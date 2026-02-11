#!/usr/bin/env python3
"""
Embedding KNN Predictor

Given a new document embedding, find the K most similar historical embeddings
and predict direction based on what happened after those historical cases.

Usage:
    python embedding_knn_predictor.py --company "Volvo" --year 2024
    python embedding_knn_predictor.py --test  # Run validation test
"""

import asyncio
import asyncpg
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
import ast
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


@dataclass
class Prediction:
    """A prediction for a document embedding."""
    company_name: str
    section_type: str
    document_date: date

    # Prediction
    predicted_label: str  # bullish, bearish, neutral, mixed
    confidence: float     # 0-1, based on neighbor agreement

    # Supporting evidence
    num_neighbors: int
    bullish_votes: int
    bearish_votes: int
    neutral_votes: int

    # Average returns of neighbors
    avg_neighbor_return_30d: Optional[float]
    avg_neighbor_return_60d: Optional[float]
    avg_neighbor_return_90d: Optional[float]

    # Top similar cases
    similar_cases: List[Dict]


@dataclass
class ValidationResult:
    """Result of backtesting the predictor."""
    total_predictions: int
    correct_predictions: int
    accuracy: float

    # By label
    bullish_accuracy: float
    bearish_accuracy: float

    # Average returns by prediction
    avg_return_when_bullish: float
    avg_return_when_bearish: float


class EmbeddingKNNPredictor:
    """KNN-based predictor using labeled document embeddings."""

    def __init__(self, k: int = 10, min_similarity: float = 0.5):
        self.k = k
        self.min_similarity = min_similarity
        self.conn = None

    async def connect(self):
        """Connect to database."""
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def close(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()

    async def predict(
        self,
        embedding: List[float],
        section_type: Optional[str] = None,
        exclude_company: Optional[str] = None,
        max_date: Optional[date] = None
    ) -> Prediction:
        """
        Predict direction based on similar historical embeddings.

        Args:
            embedding: The query embedding vector
            section_type: Filter neighbors by section type (optional)
            exclude_company: Exclude this company from neighbors (for validation)
            max_date: Only use neighbors before this date (for backtesting)

        Returns:
            Prediction with label, confidence, and supporting evidence
        """

        # Format embedding for pgvector
        embedding_str = '[' + ','.join(str(x) for x in embedding) + ']'

        # Build query with optional filters
        filters = []
        params = [embedding_str, self.k]

        if section_type:
            filters.append(f"section_type = ${len(params) + 1}")
            params.append(section_type)

        if exclude_company:
            filters.append(f"company_name != ${len(params) + 1}")
            params.append(exclude_company)

        if max_date:
            filters.append(f"document_date < ${len(params) + 1}")
            params.append(max_date)

        where_clause = "WHERE " + " AND ".join(filters) if filters else ""

        # Find K nearest neighbors using pgvector
        query = f"""
            SELECT
                company_name,
                section_type,
                document_date,
                consensus_label,
                return_30d,
                return_60d,
                return_90d,
                1 - (embedding <=> $1::vector) as similarity
            FROM embedding_labels
            {where_clause}
            ORDER BY embedding <=> $1::vector
            LIMIT $2
        """

        rows = await self.conn.fetch(query, *params)

        if not rows:
            return Prediction(
                company_name="",
                section_type=section_type or "",
                document_date=date.today(),
                predicted_label="unknown",
                confidence=0.0,
                num_neighbors=0,
                bullish_votes=0,
                bearish_votes=0,
                neutral_votes=0,
                avg_neighbor_return_30d=None,
                avg_neighbor_return_60d=None,
                avg_neighbor_return_90d=None,
                similar_cases=[]
            )

        # Filter by minimum similarity
        neighbors = [dict(row) for row in rows if row['similarity'] >= self.min_similarity]

        if not neighbors:
            neighbors = [dict(rows[0])]  # At least use closest

        # Count votes
        bullish = sum(1 for n in neighbors if n['consensus_label'] == 'bullish')
        bearish = sum(1 for n in neighbors if n['consensus_label'] == 'bearish')
        neutral = sum(1 for n in neighbors if n['consensus_label'] == 'neutral')
        total = len(neighbors)

        # Determine prediction
        if bullish > bearish and bullish > neutral:
            predicted_label = 'bullish'
            confidence = bullish / total
        elif bearish > bullish and bearish > neutral:
            predicted_label = 'bearish'
            confidence = bearish / total
        elif neutral >= bullish and neutral >= bearish:
            predicted_label = 'neutral'
            confidence = neutral / total
        else:
            predicted_label = 'mixed'
            confidence = max(bullish, bearish) / total

        # Calculate average returns of neighbors
        returns_30d = [n['return_30d'] for n in neighbors if n['return_30d'] is not None]
        returns_60d = [n['return_60d'] for n in neighbors if n['return_60d'] is not None]
        returns_90d = [n['return_90d'] for n in neighbors if n['return_90d'] is not None]

        # Format similar cases for output
        similar_cases = [
            {
                'company': n['company_name'],
                'date': str(n['document_date']),
                'label': n['consensus_label'],
                'return_60d': f"{n['return_60d']:.1%}" if n['return_60d'] else "N/A",
                'similarity': f"{n['similarity']:.3f}"
            }
            for n in neighbors[:5]  # Top 5
        ]

        return Prediction(
            company_name="",
            section_type=section_type or neighbors[0]['section_type'],
            document_date=date.today(),
            predicted_label=predicted_label,
            confidence=confidence,
            num_neighbors=total,
            bullish_votes=bullish,
            bearish_votes=bearish,
            neutral_votes=neutral,
            avg_neighbor_return_30d=np.mean(returns_30d) if returns_30d else None,
            avg_neighbor_return_60d=np.mean(returns_60d) if returns_60d else None,
            avg_neighbor_return_90d=np.mean(returns_90d) if returns_90d else None,
            similar_cases=similar_cases
        )

    async def predict_for_company(
        self,
        company_name: str,
        year: int,
        section_type: Optional[str] = None
    ) -> List[Prediction]:
        """Get predictions for a company's documents."""

        # Get embeddings for this company
        query = """
            SELECT
                se.embedding,
                se.section_type,
                ed.filing_date,
                ed.company_name
            FROM section_embeddings se
            JOIN extracted_documents ed ON ed.id = se.extracted_document_id
            WHERE ed.company_name ILIKE $1
            AND ed.year = $2
        """
        params = [f"%{company_name}%", year]

        if section_type:
            query += " AND se.section_type = $3"
            params.append(section_type)

        rows = await self.conn.fetch(query, *params)

        predictions = []
        for row in rows:
            # Parse embedding
            emb_str = row['embedding']
            if isinstance(emb_str, str):
                try:
                    embedding = json.loads(emb_str)
                except json.JSONDecodeError:
                    embedding = ast.literal_eval(emb_str)
            else:
                embedding = list(emb_str)

            # Get prediction (exclude same company to avoid data leakage)
            pred = await self.predict(
                embedding=embedding,
                section_type=row['section_type'],
                exclude_company=row['company_name']
            )

            pred.company_name = row['company_name']
            pred.section_type = row['section_type']
            pred.document_date = row['filing_date'] or date(year + 1, 3, 1)

            predictions.append(pred)

        return predictions

    async def validate(
        self,
        test_fraction: float = 0.2,
        section_type: Optional[str] = None
    ) -> ValidationResult:
        """
        Validate predictor using leave-one-out style testing.

        For each labeled embedding, predict using all others and check accuracy.
        """

        print("Running validation...")

        # Get labeled embeddings
        query = """
            SELECT
                id, embedding, section_type, company_name,
                document_date, consensus_label, return_60d
            FROM embedding_labels
            WHERE consensus_label IN ('bullish', 'bearish')
        """

        if section_type:
            query += f" AND section_type = '{section_type}'"

        # Random sample for speed
        query += " ORDER BY RANDOM() LIMIT 500"

        rows = await self.conn.fetch(query)
        print(f"Testing on {len(rows)} samples...")

        results = []
        for i, row in enumerate(rows):
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(rows)}")

            # Parse embedding
            emb_str = row['embedding']
            if isinstance(emb_str, str):
                try:
                    embedding = json.loads(emb_str)
                except:
                    continue
            else:
                embedding = list(emb_str)

            # Predict (exclude this company to avoid data leakage)
            pred = await self.predict(
                embedding=embedding,
                section_type=row['section_type'],
                exclude_company=row['company_name'],
                max_date=row['document_date']  # No future peeking
            )

            actual_label = row['consensus_label']
            actual_return = row['return_60d']

            results.append({
                'predicted': pred.predicted_label,
                'actual': actual_label,
                'return_60d': actual_return,
                'confidence': pred.confidence
            })

        # Calculate metrics
        total = len(results)
        correct = sum(1 for r in results if r['predicted'] == r['actual'])
        accuracy = correct / total if total > 0 else 0

        # By label
        bullish_results = [r for r in results if r['predicted'] == 'bullish']
        bearish_results = [r for r in results if r['predicted'] == 'bearish']

        bullish_correct = sum(1 for r in bullish_results if r['actual'] == 'bullish')
        bearish_correct = sum(1 for r in bearish_results if r['actual'] == 'bearish')

        bullish_accuracy = bullish_correct / len(bullish_results) if bullish_results else 0
        bearish_accuracy = bearish_correct / len(bearish_results) if bearish_results else 0

        # Average returns
        bullish_returns = [r['return_60d'] for r in bullish_results if r['return_60d']]
        bearish_returns = [r['return_60d'] for r in bearish_results if r['return_60d']]

        return ValidationResult(
            total_predictions=total,
            correct_predictions=correct,
            accuracy=accuracy,
            bullish_accuracy=bullish_accuracy,
            bearish_accuracy=bearish_accuracy,
            avg_return_when_bullish=np.mean(bullish_returns) if bullish_returns else 0,
            avg_return_when_bearish=np.mean(bearish_returns) if bearish_returns else 0
        )


async def main():
    """Run predictor from command line."""

    parser = argparse.ArgumentParser(description="Embedding KNN Predictor")
    parser.add_argument("--company", type=str, help="Company to predict")
    parser.add_argument("--year", type=int, default=2024, help="Year to analyze")
    parser.add_argument("--section", type=str, help="Section type filter")
    parser.add_argument("--test", action="store_true", help="Run validation test")
    parser.add_argument("--k", type=int, default=10, help="Number of neighbors")

    args = parser.parse_args()

    predictor = EmbeddingKNNPredictor(k=args.k)
    await predictor.connect()

    try:
        if args.test:
            # Run validation
            print("=" * 70)
            print("EMBEDDING KNN PREDICTOR - VALIDATION")
            print("=" * 70)

            result = await predictor.validate(section_type=args.section)

            print(f"\n--- Results ---")
            print(f"Total predictions: {result.total_predictions}")
            print(f"Correct predictions: {result.correct_predictions}")
            print(f"Overall accuracy: {result.accuracy:.1%}")
            print(f"\nBullish predictions accuracy: {result.bullish_accuracy:.1%}")
            print(f"Bearish predictions accuracy: {result.bearish_accuracy:.1%}")
            print(f"\nAvg return when predicting bullish: {result.avg_return_when_bullish:.1%}")
            print(f"Avg return when predicting bearish: {result.avg_return_when_bearish:.1%}")

            # Key metric: does bullish beat bearish?
            spread = result.avg_return_when_bullish - result.avg_return_when_bearish
            print(f"\n🎯 Bullish-Bearish spread: {spread:.1%}")
            if spread > 0.05:
                print("   ✅ Strong signal! Bullish predictions significantly outperform bearish.")
            elif spread > 0:
                print("   ⚠️  Weak signal. Some predictive power but not strong.")
            else:
                print("   ❌ No signal. Predictions don't differentiate returns.")

        elif args.company:
            # Predict for specific company
            print("=" * 70)
            print(f"PREDICTIONS FOR {args.company.upper()} ({args.year})")
            print("=" * 70)

            predictions = await predictor.predict_for_company(
                company_name=args.company,
                year=args.year,
                section_type=args.section
            )

            if not predictions:
                print(f"No embeddings found for {args.company} in {args.year}")
                return

            print(f"\nFound {len(predictions)} sections\n")

            for pred in predictions:
                emoji = {"bullish": "🟢", "bearish": "🔴", "neutral": "⚪", "mixed": "🟡"}.get(pred.predicted_label, "❓")
                print(f"{emoji} {pred.section_type}")
                print(f"   Prediction: {pred.predicted_label.upper()} ({pred.confidence:.0%} confidence)")
                print(f"   Votes: 🟢{pred.bullish_votes} 🔴{pred.bearish_votes} ⚪{pred.neutral_votes}")
                if pred.avg_neighbor_return_60d:
                    print(f"   Avg neighbor 60d return: {pred.avg_neighbor_return_60d:.1%}")
                print(f"   Similar cases:")
                for case in pred.similar_cases[:3]:
                    print(f"      {case['company'][:20]:<20} | {case['date']} | {case['label']:<8} | {case['return_60d']}")
                print()

        else:
            parser.print_help()

    finally:
        await predictor.close()


if __name__ == "__main__":
    asyncio.run(main())
