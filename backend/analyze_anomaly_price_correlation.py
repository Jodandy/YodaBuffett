#!/usr/bin/env python3
"""
Anomaly-Price Correlation Analysis

Simple script to check if document embedding anomalies predict price movements.

For each company:
1. Calculate temporal anomalies (similarity to prior year's documents)
2. Get price data around anomaly dates
3. Calculate forward returns (30, 60, 90 days)
4. Analyze correlation between anomaly score and returns
"""

import asyncio
import asyncpg
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
import ast


@dataclass
class AnomalyEvent:
    """A detected anomaly with price context."""
    company_name: str
    symbol: str
    document_date: date
    section_type: str
    anomaly_score: float  # 0-1, higher = more anomalous
    similarity_to_prior: float  # 0-1, lower = more different
    forward_return_30d: Optional[float] = None
    forward_return_60d: Optional[float] = None
    forward_return_90d: Optional[float] = None
    price_at_detection: Optional[float] = None


async def get_company_symbol_mapping(conn) -> Dict[str, str]:
    """Get mapping from company_name to stock symbol."""
    rows = await conn.fetch("""
        SELECT company_name, primary_ticker, yahoo_symbol
        FROM company_master
        WHERE primary_ticker IS NOT NULL OR yahoo_symbol IS NOT NULL
    """)

    mapping = {}
    for row in rows:
        name = row['company_name']
        symbol = row['primary_ticker'] or row['yahoo_symbol'].replace('.ST', '').replace('.OL', '')
        if name and symbol:
            mapping[name.lower()] = symbol

    return mapping


async def calculate_temporal_anomalies(
    conn,
    company_name: str,
    min_year: int = 2018,
    max_year: int = 2024
) -> List[AnomalyEvent]:
    """
    Calculate temporal anomalies for a company.

    Compare each year's document embeddings to the prior year.
    Returns anomaly events where similarity drops below threshold.
    """

    # Get section embeddings by year
    # Note: section_embeddings has section_type directly, no need to join document_sections
    rows = await conn.fetch("""
        SELECT
            ed.year,
            ed.company_name,
            se.section_type,
            se.embedding,
            ed.filing_date
        FROM section_embeddings se
        JOIN extracted_documents ed ON ed.id = se.extracted_document_id
        WHERE ed.company_name ILIKE $1
        AND ed.year BETWEEN $2 AND $3
        AND se.section_type IN (
            'balance_sheet', 'income_statement', 'cash_flow',
            'risk_factors', 'management_discussion'
        )
        ORDER BY ed.year, se.section_type
    """, company_name, min_year - 1, max_year)

    if not rows:
        return []

    # Group by year and section type
    by_year_section: Dict[Tuple[int, str], List] = {}
    for row in rows:
        key = (row['year'], row['section_type'])
        if key not in by_year_section:
            by_year_section[key] = []
        # Parse embedding string to list
        emb_str = row['embedding']
        if isinstance(emb_str, str):
            try:
                embedding = json.loads(emb_str)
            except json.JSONDecodeError:
                embedding = ast.literal_eval(emb_str)
        else:
            embedding = emb_str

        by_year_section[key].append({
            'embedding': embedding,
            'filing_date': row['filing_date'],
            'company_name': row['company_name']
        })

    anomalies = []

    # Compare each year to prior year
    for year in range(min_year, max_year + 1):
        for section_type in ['balance_sheet', 'income_statement', 'cash_flow', 'risk_factors', 'management_discussion']:
            current_key = (year, section_type)
            prior_key = (year - 1, section_type)

            if current_key not in by_year_section or prior_key not in by_year_section:
                continue

            current_data = by_year_section[current_key]
            prior_data = by_year_section[prior_key]

            if not current_data or not prior_data:
                continue

            # Average embedding for each year
            current_avg = np.mean([d['embedding'] for d in current_data], axis=0)
            prior_avg = np.mean([d['embedding'] for d in prior_data], axis=0)

            # Calculate cosine similarity
            similarity = np.dot(current_avg, prior_avg) / (
                np.linalg.norm(current_avg) * np.linalg.norm(prior_avg)
            )

            # Anomaly score = 1 - similarity (higher = more anomalous)
            anomaly_score = 1 - similarity

            # Get filing date (approximate)
            filing_date = current_data[0]['filing_date']
            if filing_date:
                doc_date = filing_date if isinstance(filing_date, date) else filing_date.date()
            else:
                # Approximate: annual reports typically in Feb-March of next year
                doc_date = date(year + 1, 3, 1)

            anomalies.append(AnomalyEvent(
                company_name=current_data[0]['company_name'],
                symbol='',  # Will be filled later
                document_date=doc_date,
                section_type=section_type,
                anomaly_score=float(anomaly_score),
                similarity_to_prior=float(similarity)
            ))

    return anomalies


async def add_price_data(conn, anomalies: List[AnomalyEvent], symbol_mapping: Dict[str, str]):
    """Add price data and forward returns to anomaly events."""

    for anomaly in anomalies:
        # Find symbol
        company_lower = anomaly.company_name.lower()
        symbol = symbol_mapping.get(company_lower)

        if not symbol:
            # Try partial matching
            for name, sym in symbol_mapping.items():
                if company_lower in name or name in company_lower:
                    symbol = sym
                    break

        if not symbol:
            continue

        anomaly.symbol = symbol

        # Get price at detection date
        price_row = await conn.fetchrow("""
            SELECT close_price, date
            FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC
            LIMIT 1
        """, symbol, anomaly.document_date)

        if not price_row:
            continue

        anomaly.price_at_detection = float(price_row['close_price'])
        base_date = price_row['date']

        # Get forward prices
        for days, attr in [(30, 'forward_return_30d'), (60, 'forward_return_60d'), (90, 'forward_return_90d')]:
            target_date = base_date + timedelta(days=days)
            future_price = await conn.fetchrow("""
                SELECT close_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2
                ORDER BY date
                LIMIT 1
            """, symbol, target_date)

            if future_price:
                ret = (float(future_price['close_price']) - anomaly.price_at_detection) / anomaly.price_at_detection
                setattr(anomaly, attr, ret)


def analyze_correlations(anomalies: List[AnomalyEvent]) -> Dict:
    """Analyze correlation between anomaly scores and returns."""

    # Filter to anomalies with complete price data
    complete = [a for a in anomalies if a.forward_return_60d is not None]

    if len(complete) < 5:
        return {"error": "Not enough data points"}

    # Separate by severity
    high_anomalies = [a for a in complete if a.anomaly_score >= 0.4]
    low_anomalies = [a for a in complete if a.anomaly_score < 0.4]

    # Section-specific analysis
    section_results = {}
    for section in ['balance_sheet', 'income_statement', 'risk_factors', 'management_discussion']:
        section_anomalies = [a for a in complete if a.section_type == section]
        if len(section_anomalies) >= 3:
            section_results[section] = {
                'count': len(section_anomalies),
                'avg_anomaly_score': np.mean([a.anomaly_score for a in section_anomalies]),
                'avg_return_60d': np.mean([a.forward_return_60d for a in section_anomalies]),
                'positive_return_rate': sum(1 for a in section_anomalies if a.forward_return_60d > 0) / len(section_anomalies)
            }

    # Calculate correlation
    scores = [a.anomaly_score for a in complete]
    returns_60d = [a.forward_return_60d for a in complete]

    correlation = np.corrcoef(scores, returns_60d)[0, 1] if len(scores) > 2 else 0

    return {
        'total_anomalies': len(complete),
        'high_anomaly_count': len(high_anomalies),
        'low_anomaly_count': len(low_anomalies),
        'high_anomaly_avg_return_60d': np.mean([a.forward_return_60d for a in high_anomalies]) if high_anomalies else None,
        'low_anomaly_avg_return_60d': np.mean([a.forward_return_60d for a in low_anomalies]) if low_anomalies else None,
        'correlation_score_vs_return': correlation,
        'section_analysis': section_results,
        'top_anomalies': sorted(complete, key=lambda a: a.anomaly_score, reverse=True)[:10]
    }


async def main():
    """Run anomaly-price correlation analysis."""

    print("=" * 70)
    print("ANOMALY-PRICE CORRELATION ANALYSIS")
    print("=" * 70)

    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')

    try:
        # Get symbol mapping
        print("\n1. Loading company-symbol mapping...")
        symbol_mapping = await get_company_symbol_mapping(conn)
        print(f"   Found {len(symbol_mapping)} company-symbol mappings")

        # Get companies with most documents
        print("\n2. Finding companies with rich document history...")
        companies = await conn.fetch("""
            SELECT company_name, COUNT(*) as doc_count
            FROM extracted_documents
            WHERE company_name IS NOT NULL
            GROUP BY company_name
            HAVING COUNT(*) >= 20
            ORDER BY doc_count DESC
            LIMIT 30
        """)

        print(f"   Found {len(companies)} companies with 20+ documents")

        all_anomalies = []

        # Calculate anomalies for each company
        print("\n3. Calculating temporal anomalies...")
        for i, company in enumerate(companies):
            company_name = company['company_name']
            anomalies = await calculate_temporal_anomalies(conn, company_name)

            if anomalies:
                print(f"   [{i+1}/{len(companies)}] {company_name}: {len(anomalies)} anomaly events")
                all_anomalies.extend(anomalies)

        print(f"\n   Total anomaly events: {len(all_anomalies)}")

        # Add price data
        print("\n4. Adding price data to anomalies...")
        await add_price_data(conn, all_anomalies, symbol_mapping)

        with_prices = [a for a in all_anomalies if a.forward_return_60d is not None]
        print(f"   Anomalies with price data: {len(with_prices)}")

        # Analyze correlations
        print("\n5. Analyzing correlations...")
        results = analyze_correlations(all_anomalies)

        # Print results
        print("\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)

        print(f"\nTotal anomalies analyzed: {results.get('total_anomalies', 0)}")
        print(f"High anomalies (score >= 0.4): {results.get('high_anomaly_count', 0)}")
        print(f"Low anomalies (score < 0.4): {results.get('low_anomaly_count', 0)}")

        print(f"\nCorrelation (anomaly score vs 60d return): {results.get('correlation_score_vs_return', 0):.4f}")

        if results.get('high_anomaly_avg_return_60d') is not None:
            print(f"\nHigh anomaly avg 60d return: {results['high_anomaly_avg_return_60d']:.2%}")
        if results.get('low_anomaly_avg_return_60d') is not None:
            print(f"Low anomaly avg 60d return: {results['low_anomaly_avg_return_60d']:.2%}")

        print("\n--- Section Analysis ---")
        for section, data in results.get('section_analysis', {}).items():
            print(f"\n{section}:")
            print(f"  Count: {data['count']}")
            print(f"  Avg anomaly score: {data['avg_anomaly_score']:.3f}")
            print(f"  Avg 60d return: {data['avg_return_60d']:.2%}")
            print(f"  Positive return rate: {data['positive_return_rate']:.1%}")

        print("\n--- Top 10 Highest Anomalies ---")
        for i, a in enumerate(results.get('top_anomalies', [])[:10], 1):
            ret_str = f"{a.forward_return_60d:.1%}" if a.forward_return_60d else "N/A"
            print(f"{i:2}. {a.company_name[:25]:<25} | {a.section_type:<20} | "
                  f"Score: {a.anomaly_score:.3f} | 60d: {ret_str}")

        # Save results
        output_file = 'data/anomaly_price_correlation.json'
        with open(output_file, 'w') as f:
            # Convert to serializable format
            output = {
                'total_anomalies': results.get('total_anomalies'),
                'high_anomaly_count': results.get('high_anomaly_count'),
                'low_anomaly_count': results.get('low_anomaly_count'),
                'correlation': results.get('correlation_score_vs_return'),
                'high_anomaly_avg_return': results.get('high_anomaly_avg_return_60d'),
                'low_anomaly_avg_return': results.get('low_anomaly_avg_return_60d'),
                'section_analysis': results.get('section_analysis', {}),
                'anomalies': [
                    {
                        'company': a.company_name,
                        'symbol': a.symbol,
                        'date': str(a.document_date),
                        'section': a.section_type,
                        'anomaly_score': a.anomaly_score,
                        'similarity': a.similarity_to_prior,
                        'return_30d': a.forward_return_30d,
                        'return_60d': a.forward_return_60d,
                        'return_90d': a.forward_return_90d
                    }
                    for a in all_anomalies if a.forward_return_60d is not None
                ]
            }
            json.dump(output, f, indent=2)

        print(f"\n\nResults saved to: {output_file}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
