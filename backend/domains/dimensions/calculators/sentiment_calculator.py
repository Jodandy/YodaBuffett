"""
Sentiment Dimension Calculator

Analyzes communication patterns using document embeddings and NLP.
This is an example of an AI-derived dimension that uses completely
different methodology than traditional factor-based dimensions.

Methodology:
- Fetch recent document embeddings for the company
- Compare to historical baseline embeddings
- Detect anomalies (significant communication shifts)
- Analyze section-specific patterns (risk factors, MD&A, financials)
- Combine signals into overall sentiment score

Higher scores indicate:
- Stable, consistent communication (no alarming changes)
- Positive tone trends in management discussion
- No unexpected shifts in risk factor language
- Consistent financial reporting patterns

This demonstrates how dimensions can be "black boxes" with any internal logic.
"""

from typing import Dict, List, Optional, Any, Union
from datetime import date, timedelta
import numpy as np
import json
import logging

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class SentimentCalculator(BaseDimensionCalculator):
    """
    Sentiment dimension calculator using embedding-based analysis.

    Uses the temporal anomaly detection system to identify
    communication pattern shifts in company filings.
    """

    @property
    def dimension_code(self) -> str:
        return "sentiment"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code="sentiment",
            display_name="Sentiment",
            description="AI-derived analysis of communication patterns and document anomalies",
            category="ai_derived",
            data_sources=["document_embeddings", "section_embeddings", "extracted_documents"],
            update_frequency="daily",
            requires_external_api=False,
            config={
                "methodology": "embedding_anomaly_detection",
                "baseline_window_days": 730,
                "anomaly_threshold": 0.3,
            },
        )

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self._version = "1.0.0"

        # Configuration
        self.baseline_window_days = self.get_config("baseline_window_days", 730)
        self.anomaly_threshold = self.get_config("anomaly_threshold", 0.3)

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> DimensionScore:
        """
        Calculate sentiment score using embedding analysis.

        The methodology:
        1. Get recent document embedding
        2. Compare to historical baseline
        3. Check section-specific changes
        4. Calculate stability score
        5. Combine into overall sentiment
        """

        # Get company name for embedding queries
        company_name = await self._get_company_name(company_id)

        if not company_name:
            return self._no_data_score(company_id, score_date, "company_not_found")

        metadata = {
            "company_name": company_name,
            "analysis_components": [],
        }

        # Component scores (each 0-100, will be combined)
        component_scores = {}
        component_weights = {
            "document_stability": 0.30,
            "risk_factor_stability": 0.25,
            "mda_tone": 0.20,
            "financial_consistency": 0.15,
            "communication_regularity": 0.10,
        }

        # 1. Document-level stability (overall embedding similarity to baseline)
        doc_stability = await self._calculate_document_stability(company_name, score_date)
        if doc_stability is not None:
            component_scores["document_stability"] = doc_stability["score"]
            metadata["document_stability"] = doc_stability
            metadata["analysis_components"].append("document_stability")

        # 2. Risk factor section stability
        risk_stability = await self._calculate_section_stability(
            company_name, score_date, "risk_factors"
        )
        if risk_stability is not None:
            component_scores["risk_factor_stability"] = risk_stability["score"]
            metadata["risk_factor_stability"] = risk_stability
            metadata["analysis_components"].append("risk_factor_stability")

        # 3. MD&A tone trend
        mda_tone = await self._calculate_mda_tone_trend(company_name, score_date)
        if mda_tone is not None:
            component_scores["mda_tone"] = mda_tone["score"]
            metadata["mda_tone"] = mda_tone
            metadata["analysis_components"].append("mda_tone")

        # 4. Financial statement consistency
        fin_consistency = await self._calculate_financial_consistency(company_name, score_date)
        if fin_consistency is not None:
            component_scores["financial_consistency"] = fin_consistency["score"]
            metadata["financial_consistency"] = fin_consistency
            metadata["analysis_components"].append("financial_consistency")

        # 5. Communication regularity (are they filing consistently?)
        comm_regularity = await self._calculate_communication_regularity(company_name, score_date)
        if comm_regularity is not None:
            component_scores["communication_regularity"] = comm_regularity["score"]
            metadata["communication_regularity"] = comm_regularity
            metadata["analysis_components"].append("communication_regularity")

        # Calculate weighted score
        if not component_scores:
            return self._no_data_score(company_id, score_date, "no_embedding_data")

        total_weight = sum(
            component_weights[k] for k in component_scores
        )
        weighted_sum = sum(
            component_scores[k] * component_weights[k]
            for k in component_scores
        )
        score = weighted_sum / total_weight if total_weight > 0 else 50.0

        # Calculate confidence and data quality
        data_quality = len(component_scores) / len(component_weights)
        confidence = self._calculate_confidence(component_scores, data_quality)

        # Calculate uncertainty range
        if component_scores:
            score_values = list(component_scores.values())
            std_dev = np.std(score_values) if len(score_values) > 1 else 15.0
            uncertainty_factor = 2.0 * (1.0 - confidence) + 1.0
            score_low = max(0, score - std_dev * uncertainty_factor)
            score_high = min(100, score + std_dev * uncertainty_factor)
        else:
            score_low, score_high = 0.0, 100.0

        # Add summary to metadata
        metadata["component_scores"] = component_scores
        metadata["component_weights"] = {k: component_weights[k] for k in component_scores}
        metadata["components_available"] = len(component_scores)
        metadata["components_total"] = len(component_weights)

        # Determine overall sentiment interpretation
        if score >= 70:
            metadata["interpretation"] = "stable_positive"
            metadata["summary"] = "Communication patterns are stable with positive indicators"
        elif score >= 50:
            metadata["interpretation"] = "neutral"
            metadata["summary"] = "Communication patterns are within normal range"
        elif score >= 30:
            metadata["interpretation"] = "caution"
            metadata["summary"] = "Some shifts detected in communication patterns"
        else:
            metadata["interpretation"] = "alert"
            metadata["summary"] = "Significant changes detected in communication patterns"

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=float(score),
            confidence=confidence,
            data_quality=data_quality,
            score_low=score_low,
            score_high=score_high,
            metadata=metadata,
            definition_version=1,
        )

    async def _get_company_name(self, company_id: str) -> Optional[str]:
        """Get company name from company_master."""
        row = await self.db_conn.fetchrow(
            "SELECT company_name FROM company_master WHERE id = $1",
            company_id
        )
        return row["company_name"] if row else None

    async def _calculate_document_stability(
        self,
        company_name: str,
        score_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate document-level stability score.

        Compares recent document embedding to historical baseline.
        High similarity = stable communication = higher score.
        """

        query = """
        WITH recent_doc AS (
            SELECT de.embedding, ed.filing_date
            FROM document_embeddings de
            JOIN extracted_documents ed ON de.extracted_document_id = ed.id
            WHERE ed.company_name = $1
            AND ed.filing_date <= $2
            AND de.embedding_model LIKE 'local/%'
            ORDER BY ed.filing_date DESC
            LIMIT 1
        ),
        baseline_docs AS (
            SELECT de.embedding
            FROM document_embeddings de
            JOIN extracted_documents ed ON de.extracted_document_id = ed.id
            WHERE ed.company_name = $1
            AND ed.filing_date < $2 - INTERVAL '180 days'
            AND ed.filing_date > $2 - INTERVAL '730 days'
            AND de.embedding_model LIKE 'local/%'
            ORDER BY ed.filing_date DESC
            LIMIT 5
        )
        SELECT
            (SELECT embedding FROM recent_doc) as recent_embedding,
            (SELECT filing_date FROM recent_doc) as recent_date,
            (SELECT array_agg(embedding) FROM baseline_docs) as baseline_embeddings
        """

        try:
            row = await self.db_conn.fetchrow(query, company_name, score_date)

            if not row or not row["recent_embedding"] or not row["baseline_embeddings"]:
                return None

            # Parse embeddings (handle pgvector string format)
            recent_emb = self._parse_embedding(row["recent_embedding"])
            baseline_embs = [
                self._parse_embedding(e)
                for e in row["baseline_embeddings"]
                if e
            ]
            baseline_embs = [e for e in baseline_embs if e is not None]

            if recent_emb is None or not baseline_embs:
                return None

            # Calculate average similarity to baseline
            similarities = [
                self._cosine_similarity(recent_emb, baseline_emb)
                for baseline_emb in baseline_embs
            ]
            avg_similarity = np.mean(similarities)

            # Convert similarity to score (higher similarity = higher score)
            # Similarity of 0.9+ = excellent, 0.7-0.9 = good, 0.5-0.7 = concerning, <0.5 = alert
            if avg_similarity >= 0.9:
                score = 90 + (avg_similarity - 0.9) * 100  # 90-100
            elif avg_similarity >= 0.7:
                score = 60 + (avg_similarity - 0.7) * 150  # 60-90
            elif avg_similarity >= 0.5:
                score = 30 + (avg_similarity - 0.5) * 150  # 30-60
            else:
                score = avg_similarity * 60  # 0-30

            return {
                "score": float(score),
                "avg_similarity": float(avg_similarity),
                "min_similarity": float(min(similarities)),
                "max_similarity": float(max(similarities)),
                "baseline_count": len(baseline_embs),
                "recent_doc_date": str(row["recent_date"]) if row["recent_date"] else None,
            }

        except Exception as e:
            logger.warning(f"Document stability calculation failed for {company_name}: {e}")
            return None

    async def _calculate_section_stability(
        self,
        company_name: str,
        score_date: date,
        section_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate stability for a specific document section type.

        Compares recent section embedding to historical baseline.
        """

        query = """
        WITH recent_section AS (
            SELECT se.embedding
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE ed.company_name = $1
            AND ds.section_type = $2
            AND ed.filing_date <= $3
            AND se.embedding_model LIKE 'local/%'
            ORDER BY ed.filing_date DESC
            LIMIT 1
        ),
        baseline_sections AS (
            SELECT se.embedding
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE ed.company_name = $1
            AND ds.section_type = $2
            AND ed.filing_date < $3 - INTERVAL '365 days'
            AND se.embedding_model LIKE 'local/%'
            ORDER BY ed.filing_date DESC
            LIMIT 3
        )
        SELECT
            (SELECT embedding FROM recent_section) as recent_embedding,
            (SELECT array_agg(embedding) FROM baseline_sections) as baseline_embeddings
        """

        try:
            row = await self.db_conn.fetchrow(query, company_name, section_type, score_date)

            if not row or not row["recent_embedding"] or not row["baseline_embeddings"]:
                return None

            # Parse embeddings (handle pgvector string format)
            recent_emb = self._parse_embedding(row["recent_embedding"])
            baseline_embs = [
                self._parse_embedding(e)
                for e in row["baseline_embeddings"]
                if e
            ]
            baseline_embs = [e for e in baseline_embs if e is not None]

            if recent_emb is None or not baseline_embs:
                return None

            similarities = [
                self._cosine_similarity(recent_emb, baseline_emb)
                for baseline_emb in baseline_embs
            ]
            avg_similarity = np.mean(similarities)

            # Convert to score
            score = avg_similarity * 100

            return {
                "score": float(score),
                "section_type": section_type,
                "avg_similarity": float(avg_similarity),
                "baseline_count": len(baseline_embs),
            }

        except Exception as e:
            logger.warning(f"Section stability calculation failed: {e}")
            return None

    async def _calculate_mda_tone_trend(
        self,
        company_name: str,
        score_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate trend in Management Discussion & Analysis tone.

        Compares recent MD&A sections to detect positive/negative shifts.
        """

        query = """
        SELECT se.embedding, ed.filing_date
        FROM section_embeddings se
        JOIN document_sections ds ON se.document_section_id = ds.id
        JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
        WHERE ed.company_name = $1
        AND ds.section_type IN ('management_discussion', 'management_discussion_and_analysis', 'mda')
        AND ed.filing_date <= $2
        AND se.embedding_model LIKE 'local/%'
        ORDER BY ed.filing_date DESC
        LIMIT 4
        """

        try:
            rows = await self.db_conn.fetch(query, company_name, score_date)

            if len(rows) < 2:
                return None

            # Parse embeddings (handle pgvector string format)
            embeddings = [self._parse_embedding(r["embedding"]) for r in rows]
            embeddings = [e for e in embeddings if e is not None]

            if len(embeddings) < 2:
                return None

            # Calculate trend: are recent docs more similar to each other than to older ones?
            # This indicates consistency (or lack of sudden shifts)
            if len(embeddings) >= 3:
                # Recent pair similarity
                recent_sim = self._cosine_similarity(embeddings[0], embeddings[1])
                # Older pair similarity
                older_sim = self._cosine_similarity(embeddings[1], embeddings[2])

                # Trend: positive if recent similarity maintained or improved
                trend = recent_sim - older_sim + 0.5  # Shift to 0-1 range

                # Score: 50 is neutral, higher is more stable/positive
                score = 50 + (trend - 0.5) * 100
            else:
                # Just compare the two available
                sim = self._cosine_similarity(embeddings[0], embeddings[1])
                score = sim * 100

            score = max(0, min(100, score))

            return {
                "score": float(score),
                "documents_analyzed": len(rows),
                "recent_similarity": float(self._cosine_similarity(embeddings[0], embeddings[1])) if len(embeddings) >= 2 else None,
            }

        except Exception as e:
            logger.warning(f"MD&A tone calculation failed: {e}")
            return None

    async def _calculate_financial_consistency(
        self,
        company_name: str,
        score_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate consistency of financial statement sections.

        Checks balance sheet and income statement sections for anomalies.
        """

        # Check both balance sheet and income statement sections
        section_types = ["balance_sheet", "income_statement"]
        similarities = []

        for section_type in section_types:
            result = await self._calculate_section_stability(company_name, score_date, section_type)
            if result:
                similarities.append(result["avg_similarity"])

        if not similarities:
            return None

        avg_sim = np.mean(similarities)
        score = avg_sim * 100

        return {
            "score": float(score),
            "sections_analyzed": len(similarities),
            "avg_similarity": float(avg_sim),
        }

    async def _calculate_communication_regularity(
        self,
        company_name: str,
        score_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate communication regularity score.

        Checks if company is filing documents at expected intervals.
        """

        query = """
        SELECT filing_date
        FROM extracted_documents
        WHERE company_name = $1
        AND filing_date <= $2
        AND filing_date > $2 - INTERVAL '2 years'
        ORDER BY filing_date DESC
        """

        try:
            rows = await self.db_conn.fetch(query, company_name, score_date)

            if len(rows) < 2:
                return None

            dates = [r["filing_date"] for r in rows]

            # Calculate gaps between filings
            gaps = []
            for i in range(len(dates) - 1):
                gap = (dates[i] - dates[i + 1]).days
                gaps.append(gap)

            if not gaps:
                return None

            # Calculate regularity metrics
            avg_gap = np.mean(gaps)
            std_gap = np.std(gaps) if len(gaps) > 1 else 0

            # Score based on regularity
            # Ideal: quarterly filings (~90 day gaps) with low variance
            # Penalize if gaps are too long or too irregular

            # Coefficient of variation (lower is more regular)
            cv = std_gap / avg_gap if avg_gap > 0 else 1.0

            # Score: lower CV = higher score
            if cv < 0.2:
                score = 90 + (0.2 - cv) * 50  # Very regular
            elif cv < 0.5:
                score = 60 + (0.5 - cv) * 100  # Regular
            elif cv < 1.0:
                score = 30 + (1.0 - cv) * 60  # Somewhat irregular
            else:
                score = max(0, 30 - (cv - 1.0) * 30)  # Irregular

            # Also penalize if filing frequency is very low
            if avg_gap > 180:  # Less than twice a year
                score *= 0.8

            return {
                "score": float(max(0, min(100, score))),
                "document_count": len(dates),
                "avg_gap_days": float(avg_gap),
                "gap_std_days": float(std_gap),
                "regularity_cv": float(cv),
            }

        except Exception as e:
            logger.warning(f"Communication regularity calculation failed: {e}")
            return None

    def _parse_embedding(self, embedding_data: Union[str, list, np.ndarray, None]) -> Optional[np.ndarray]:
        """
        Parse embedding from various database formats.

        pgvector returns embeddings as strings like "[0.1, 0.2, ...]"
        This method handles string, list, and array inputs.
        """
        if embedding_data is None:
            return None

        # Already a numpy array
        if isinstance(embedding_data, np.ndarray):
            return embedding_data

        # Already a list of floats
        if isinstance(embedding_data, list):
            return np.array(embedding_data, dtype=np.float32)

        # String representation - parse it
        if isinstance(embedding_data, str):
            try:
                # Try JSON first (most common)
                parsed = json.loads(embedding_data)
                return np.array(parsed, dtype=np.float32)
            except json.JSONDecodeError:
                pass

            # Try pgvector format: [0.1,0.2,0.3]
            try:
                # Remove brackets and split
                cleaned = embedding_data.strip('[]')
                values = [float(x.strip()) for x in cleaned.split(',')]
                return np.array(values, dtype=np.float32)
            except (ValueError, AttributeError):
                pass

        return None

    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors."""
        if vec1 is None or vec2 is None:
            return 0.0

        dot = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(dot / (norm1 * norm2))

    def _calculate_confidence(
        self,
        component_scores: Dict[str, float],
        data_quality: float
    ) -> float:
        """Calculate confidence based on data availability."""
        # Base confidence from data quality
        confidence = data_quality

        # Boost if components agree
        if len(component_scores) >= 3:
            spread = np.std(list(component_scores.values()))
            if spread < 15:
                confidence *= 1.1
            elif spread > 30:
                confidence *= 0.9

        return max(0.0, min(1.0, confidence))

    def _no_data_score(
        self,
        company_id: str,
        score_date: date,
        reason: str
    ) -> DimensionScore:
        """Return a score indicating no data available."""
        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=50.0,  # Neutral
            confidence=0.0,
            data_quality=0.0,
            score_low=0.0,
            score_high=100.0,
            metadata={
                "no_data": True,
                "reason": reason,
                "interpretation": "insufficient_data",
                "summary": "Insufficient embedding data for sentiment analysis",
            },
            definition_version=1,
        )
