"""
Dimension Repository

Database access layer for dimension scores.
Handles storage, retrieval, and querying of dimension data.
"""

from typing import Dict, List, Optional, Any
from datetime import date, datetime
import json
import logging

from ..models.dimension import DimensionScore, CompositeScore, ComputationResult

logger = logging.getLogger(__name__)


class DimensionRepository:
    """Repository for dimension score persistence and retrieval."""

    def __init__(self, db_conn):
        """
        Initialize repository.

        Args:
            db_conn: asyncpg connection or pool
        """
        self.db_conn = db_conn

    # ==================== STORAGE ====================

    async def store_dimension_score(self, score: DimensionScore) -> int:
        """
        Store a single dimension score.

        Returns:
            The ID of the inserted/updated record
        """
        query = """
        INSERT INTO daily_dimension_scores (
            company_id, score_date, dimension_code, definition_version,
            score, confidence, data_quality,
            percentile_rank, universe_size, universe_filter,
            score_low, score_high,
            metadata, computed_at, computation_time_ms, calculator_version
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10,
            $11, $12,
            $13, $14, $15, $16
        )
        ON CONFLICT (company_id, score_date, dimension_code)
        DO UPDATE SET
            definition_version = EXCLUDED.definition_version,
            score = EXCLUDED.score,
            confidence = EXCLUDED.confidence,
            data_quality = EXCLUDED.data_quality,
            percentile_rank = EXCLUDED.percentile_rank,
            universe_size = EXCLUDED.universe_size,
            universe_filter = EXCLUDED.universe_filter,
            score_low = EXCLUDED.score_low,
            score_high = EXCLUDED.score_high,
            metadata = EXCLUDED.metadata,
            computed_at = EXCLUDED.computed_at,
            computation_time_ms = EXCLUDED.computation_time_ms,
            calculator_version = EXCLUDED.calculator_version
        RETURNING id
        """

        row = await self.db_conn.fetchrow(
            query,
            score.company_id,
            score.score_date,
            score.dimension_code,
            score.definition_version,
            score.score,
            score.confidence,
            score.data_quality,
            score.percentile_rank,
            score.universe_size,
            json.dumps(score.universe_filter) if score.universe_filter else None,
            score.score_low,
            score.score_high,
            json.dumps(score.metadata),
            score.computed_at,
            score.computation_time_ms,
            score.calculator_version,
        )

        return row["id"]

    async def store_dimension_scores(self, scores: List[DimensionScore]) -> int:
        """
        Store multiple dimension scores in a batch.

        Returns:
            Number of scores stored
        """
        if not scores:
            return 0

        # Use COPY for bulk insert would be faster, but upsert logic is needed
        count = 0
        for score in scores:
            try:
                await self.store_dimension_score(score)
                count += 1
            except Exception as e:
                logger.warning(f"Failed to store score for {score.company_id}: {e}")

        return count

    async def store_composite_score(
        self,
        company_id: str,
        score_date: date,
        composite_code: str,
        score: float,
        dimension_scores: Dict[str, float],
        dimension_weights: Dict[str, float],
        confidence: Optional[float] = None,
        percentile_rank: Optional[float] = None,
        missing_dimensions: Optional[List[str]] = None,
    ) -> int:
        """Store a composite score."""

        query = """
        INSERT INTO composite_scores (
            company_id, score_date, composite_code,
            score, confidence, percentile_rank,
            dimension_scores, dimension_weights, missing_dimensions
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (company_id, score_date, composite_code)
        DO UPDATE SET
            score = EXCLUDED.score,
            confidence = EXCLUDED.confidence,
            percentile_rank = EXCLUDED.percentile_rank,
            dimension_scores = EXCLUDED.dimension_scores,
            dimension_weights = EXCLUDED.dimension_weights,
            missing_dimensions = EXCLUDED.missing_dimensions,
            computed_at = NOW()
        RETURNING id
        """

        row = await self.db_conn.fetchrow(
            query,
            company_id,
            score_date,
            composite_code,
            score,
            confidence,
            percentile_rank,
            json.dumps(dimension_scores),
            json.dumps(dimension_weights),
            missing_dimensions or [],
        )

        return row["id"]

    async def log_computation(self, result: ComputationResult) -> None:
        """Log a computation run for monitoring."""

        query = """
        INSERT INTO dimension_computation_log (
            dimension_code, score_date,
            started_at, completed_at, status,
            companies_processed, companies_succeeded, companies_failed, companies_skipped,
            total_duration_ms, avg_company_time_ms,
            error_summary
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """

        avg_time = None
        if result.companies_succeeded > 0 and result.total_duration_ms:
            avg_time = result.total_duration_ms // result.companies_succeeded

        await self.db_conn.execute(
            query,
            result.dimension_code,
            result.score_date,
            result.started_at,
            result.completed_at,
            result.status,
            result.companies_processed,
            result.companies_succeeded,
            result.companies_failed,
            result.companies_skipped,
            result.total_duration_ms,
            avg_time,
            json.dumps(result.errors) if result.errors else None,
        )

    # ==================== RETRIEVAL ====================

    async def get_company_scores(
        self,
        company_id: str,
        score_date: Optional[date] = None,
    ) -> Dict[str, DimensionScore]:
        """
        Get all dimension scores for a company.

        Returns:
            Dict mapping dimension_code to DimensionScore
        """
        if score_date is None:
            # Get latest scores
            query = """
            SELECT DISTINCT ON (dimension_code) *
            FROM daily_dimension_scores
            WHERE company_id = $1
            ORDER BY dimension_code, score_date DESC
            """
            rows = await self.db_conn.fetch(query, company_id)
        else:
            query = """
            SELECT * FROM daily_dimension_scores
            WHERE company_id = $1 AND score_date = $2
            """
            rows = await self.db_conn.fetch(query, company_id, score_date)

        return {
            row["dimension_code"]: self._row_to_score(row)
            for row in rows
        }

    async def get_dimension_score(
        self,
        company_id: str,
        dimension_code: str,
        score_date: Optional[date] = None,
    ) -> Optional[DimensionScore]:
        """Get a specific dimension score for a company."""

        if score_date is None:
            query = """
            SELECT * FROM daily_dimension_scores
            WHERE company_id = $1 AND dimension_code = $2
            ORDER BY score_date DESC
            LIMIT 1
            """
            row = await self.db_conn.fetchrow(query, company_id, dimension_code)
        else:
            query = """
            SELECT * FROM daily_dimension_scores
            WHERE company_id = $1 AND dimension_code = $2 AND score_date = $3
            """
            row = await self.db_conn.fetchrow(query, company_id, dimension_code, score_date)

        return self._row_to_score(row) if row else None

    async def get_dimension_history(
        self,
        company_id: str,
        dimension_code: str,
        start_date: date,
        end_date: date,
    ) -> List[DimensionScore]:
        """Get historical dimension scores for a company."""

        query = """
        SELECT * FROM daily_dimension_scores
        WHERE company_id = $1
        AND dimension_code = $2
        AND score_date BETWEEN $3 AND $4
        ORDER BY score_date ASC
        """

        rows = await self.db_conn.fetch(query, company_id, dimension_code, start_date, end_date)
        return [self._row_to_score(row) for row in rows]

    async def get_dimension_rankings(
        self,
        dimension_code: str,
        score_date: Optional[date] = None,
        limit: int = 50,
        sector: Optional[str] = None,
        country: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get top companies ranked by a dimension."""

        # Build filters
        filters = ["ds.dimension_code = $1"]
        params = [dimension_code]
        param_idx = 2

        if score_date:
            filters.append(f"ds.score_date = ${param_idx}")
            params.append(score_date)
            param_idx += 1
        else:
            filters.append("ds.score_date = (SELECT MAX(score_date) FROM daily_dimension_scores WHERE dimension_code = $1)")

        if sector:
            filters.append(f"cm.sector = ${param_idx}")
            params.append(sector)
            param_idx += 1

        if country:
            filters.append(f"cm.country = ${param_idx}")
            params.append(country)
            param_idx += 1

        query = f"""
        SELECT
            ds.*,
            cm.company_name,
            cm.primary_ticker,
            cm.sector,
            cm.country
        FROM daily_dimension_scores ds
        JOIN company_master cm ON ds.company_id = cm.id
        WHERE {' AND '.join(filters)}
        ORDER BY ds.score DESC
        LIMIT {limit}
        """

        rows = await self.db_conn.fetch(query, *params)

        return [
            {
                "company_id": str(row["company_id"]),
                "company_name": row["company_name"],
                "ticker": row["primary_ticker"],
                "sector": row["sector"],
                "country": row["country"],
                "score": float(row["score"]),
                "percentile_rank": float(row["percentile_rank"]) if row["percentile_rank"] else None,
                "confidence": float(row["confidence"]) if row["confidence"] else None,
                "score_date": row["score_date"],
            }
            for row in rows
        ]

    async def get_all_dimension_scores(
        self,
        company_ids: List[str],
        score_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """
        Get all dimension scores for multiple companies.

        Returns:
            {company_id: {dimension_code: score}}
        """
        query = """
        SELECT company_id::text, dimension_code, score
        FROM daily_dimension_scores
        WHERE company_id = ANY($1)
        AND score_date = $2
        """

        rows = await self.db_conn.fetch(query, company_ids, score_date)

        result = {}
        for row in rows:
            company_id = row["company_id"]
            if company_id not in result:
                result[company_id] = {}
            result[company_id][row["dimension_code"]] = float(row["score"])

        return result

    async def screen_companies(
        self,
        filters: Dict[str, Any],
        score_date: Optional[date] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Screen companies by dimension score filters.

        Args:
            filters: Dict with keys like 'value_min', 'value_max', 'momentum_min', etc.
            score_date: Date to screen (defaults to latest)
            limit: Max results
            offset: Pagination offset

        Returns:
            List of company summaries with scores
        """
        # This would be implemented based on the specific filter structure
        # For now, return a placeholder
        raise NotImplementedError("Screen functionality to be implemented")

    # ==================== HELPERS ====================

    def _row_to_score(self, row) -> DimensionScore:
        """Convert a database row to DimensionScore."""
        return DimensionScore(
            company_id=str(row["company_id"]),
            score_date=row["score_date"],
            dimension_code=row["dimension_code"],
            score=float(row["score"]),
            confidence=float(row["confidence"]) if row["confidence"] else None,
            data_quality=float(row["data_quality"]) if row["data_quality"] else None,
            percentile_rank=float(row["percentile_rank"]) if row["percentile_rank"] else None,
            universe_size=row["universe_size"],
            universe_filter=json.loads(row["universe_filter"]) if row["universe_filter"] else None,
            score_low=float(row["score_low"]) if row["score_low"] else None,
            score_high=float(row["score_high"]) if row["score_high"] else None,
            metadata=json.loads(row["metadata"]) if row["metadata"] else {},
            computed_at=row["computed_at"],
            computation_time_ms=row["computation_time_ms"],
            calculator_version=row["calculator_version"],
            definition_version=row["definition_version"],
        )

    async def get_active_companies(self, countries: List[str] = None) -> List[str]:
        """Get list of active company IDs."""

        if countries is None:
            # Include both short codes and full country names
            countries = ["SE", "NO", "DK", "FI", "Sverige", "Norge", "Danmark", "Finland", "Nordic"]

        query = """
        SELECT id::text
        FROM company_master
        WHERE country = ANY($1)
        ORDER BY market_cap_usd DESC NULLS LAST
        """

        rows = await self.db_conn.fetch(query, countries)
        return [row["id"] for row in rows]

    async def check_already_computed(self, dimension_code: str, score_date: date) -> bool:
        """Check if dimension was already computed for this date."""

        count = await self.db_conn.fetchval(
            """
            SELECT COUNT(*) FROM daily_dimension_scores
            WHERE dimension_code = $1 AND score_date = $2
            """,
            dimension_code,
            score_date,
        )
        return count > 0
