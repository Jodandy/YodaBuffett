"""
Fat Pitch Service

The main service that orchestrates the fat pitch pitching machine.

Usage:
    service = FatPitchService(db_conn)

    # Get all pitches
    pitches = await service.get_all_pitches()

    # Get pitches for a specific stage
    growth_pitches = await service.get_stage_pitches(BusinessStage.GROWTH_STAGE)

    # Analyze a specific company
    analysis = await service.analyze_company(company_id)
"""

from datetime import date
from typing import Dict, List, Optional
import logging

from .models import BusinessStage, FatPitch, PitchRanking, CompanyFinancials
from .business_router import BusinessRouter
from .scorer import FatPitchScorer, STAGE_PROFILES

logger = logging.getLogger(__name__)


class FatPitchService:
    """
    Main service for the fat pitch pitching machine.

    Orchestrates routing, scoring, and ranking of investment opportunities.
    """

    def __init__(self, db_conn):
        self.db_conn = db_conn
        self.router = BusinessRouter(db_conn)
        self.scorer = FatPitchScorer(db_conn)

    # =========================================================================
    # MAIN API
    # =========================================================================

    async def get_all_pitches(
        self,
        score_date: date = None,
        min_quality_score: float = 0.0,
        limit: int = 100,
    ) -> List[FatPitch]:
        """
        Get all fat pitches across all stages, ranked by score.

        Args:
            score_date: Date for scoring (default: today)
            min_quality_score: Minimum quality score filter
            limit: Max pitches to return

        Returns:
            List of FatPitch objects sorted by fat_pitch_score
        """
        score_date = score_date or date.today()

        # Get all companies
        company_ids = await self._get_all_companies()
        logger.info(f"Found {len(company_ids)} companies")

        # Route and score
        pitches = await self._route_and_score(company_ids, score_date)

        # Filter by quality
        pitches = [p for p in pitches if p.quality_score >= min_quality_score]

        # Sort and limit
        pitches.sort(key=lambda p: p.fat_pitch_score, reverse=True)

        return pitches[:limit]

    async def get_stage_pitches(
        self,
        stage: BusinessStage,
        score_date: date = None,
        limit: int = 50,
    ) -> PitchRanking:
        """
        Get ranked pitches for a specific stage.

        Args:
            stage: Business stage to filter
            score_date: Date for scoring
            limit: Max pitches to return

        Returns:
            PitchRanking with sorted pitches
        """
        score_date = score_date or date.today()

        # Get all companies
        company_ids = await self._get_all_companies()

        # Route and score
        all_pitches = await self._route_and_score(company_ids, score_date)

        # Filter by stage
        stage_pitches = [p for p in all_pitches if p.stage == stage]
        stage_pitches.sort(key=lambda p: p.fat_pitch_score, reverse=True)
        stage_pitches = stage_pitches[:limit]

        return PitchRanking(
            stage=stage,
            score_date=score_date,
            pitches=stage_pitches,
            total_companies=len(company_ids),
            companies_with_data=len(all_pitches),
        )

    async def get_all_stage_rankings(
        self,
        score_date: date = None,
        pitches_per_stage: int = 20,
    ) -> Dict[BusinessStage, PitchRanking]:
        """
        Get rankings for all stages.

        Returns:
            Dict mapping stage to PitchRanking
        """
        score_date = score_date or date.today()

        # Get and score all companies
        company_ids = await self._get_all_companies()
        all_pitches = await self._route_and_score(company_ids, score_date)

        # Create ranking for each stage
        rankings = {}
        for stage in BusinessStage:
            if stage == BusinessStage.UNKNOWN:
                continue

            stage_pitches = [p for p in all_pitches if p.stage == stage]
            stage_pitches.sort(key=lambda p: p.fat_pitch_score, reverse=True)

            rankings[stage] = PitchRanking(
                stage=stage,
                score_date=score_date,
                pitches=stage_pitches[:pitches_per_stage],
                total_companies=len(company_ids),
                companies_with_data=len([p for p in stage_pitches if p.dimension_scores]),
            )

        return rankings

    async def analyze_company(
        self,
        company_id: str,
        score_date: date = None,
    ) -> Optional[FatPitch]:
        """
        Analyze a specific company as a fat pitch candidate.

        Returns detailed scoring and analysis.
        """
        score_date = score_date or date.today()

        pitches = await self._route_and_score([company_id], score_date)

        return pitches[0] if pitches else None

    async def get_top_actionable(
        self,
        limit: int = 10,
        score_date: date = None,
    ) -> List[FatPitch]:
        """
        Get the top actionable pitches (high quality + cheap).

        These are the pitches worth investigating now.
        """
        score_date = score_date or date.today()

        all_pitches = await self.get_all_pitches(score_date=score_date, limit=500)

        # Filter for actionable
        actionable = [p for p in all_pitches if p.is_actionable]

        return actionable[:limit]

    # =========================================================================
    # INTERNAL METHODS
    # =========================================================================

    async def _route_and_score(
        self,
        company_ids: List[str],
        score_date: date,
    ) -> List[FatPitch]:
        """Route companies and score them."""

        if not company_ids:
            return []

        # 1. Route companies to stages
        routes = await self.router.route_companies(company_ids, score_date)

        # 2. Get dimension scores for all companies
        dimension_scores = await self._get_dimension_scores(company_ids, score_date)

        # 3. Score each company
        companies_to_score = [
            (cid, stage, confidence)
            for cid, (stage, confidence) in routes.items()
            if stage != BusinessStage.UNKNOWN
        ]

        pitches = await self.scorer.score_companies(
            companies=companies_to_score,
            dimension_scores=dimension_scores,
            score_date=score_date,
        )

        return pitches

    async def _get_all_companies(self) -> List[str]:
        """Get all active Nordic company IDs."""
        # Include both short codes and full country names
        query = """
        SELECT id::text
        FROM company_master
        WHERE country IN ('SE', 'NO', 'DK', 'FI', 'Sverige', 'Norge', 'Danmark', 'Finland', 'Nordic')
        ORDER BY market_cap_usd DESC NULLS LAST
        """
        rows = await self.db_conn.fetch(query)
        return [row["id"] for row in rows]

    async def _get_dimension_scores(
        self,
        company_ids: List[str],
        score_date: date,
    ) -> Dict[str, Dict[str, float]]:
        """
        Get dimension scores for companies.

        Returns:
            Dict mapping company_id -> dimension_code -> score
        """
        if not company_ids:
            return {}

        # Fetch latest dimension scores for each company
        query = """
        SELECT
            company_id::text,
            dimension_code,
            score
        FROM daily_dimension_scores
        WHERE company_id = ANY($1::uuid[])
        AND score_date <= $2
        AND (company_id, dimension_code, score_date) IN (
            SELECT company_id, dimension_code, MAX(score_date)
            FROM daily_dimension_scores
            WHERE company_id = ANY($1::uuid[])
            AND score_date <= $2
            GROUP BY company_id, dimension_code
        )
        """

        try:
            rows = await self.db_conn.fetch(query, company_ids, score_date)
        except Exception as e:
            logger.error(f"Error fetching dimension scores: {e}")
            return {}

        # Organize by company
        result: Dict[str, Dict[str, float]] = {}
        for row in rows:
            company_id = row["company_id"]
            if company_id not in result:
                result[company_id] = {}
            result[company_id][row["dimension_code"]] = float(row["score"])

        return result

    # =========================================================================
    # SUMMARY / STATS
    # =========================================================================

    async def get_stage_summary(self, score_date: date = None) -> Dict[str, any]:
        """
        Get summary statistics for all stages.

        Returns counts and average scores per stage.
        """
        score_date = score_date or date.today()

        company_ids = await self._get_all_companies()
        all_pitches = await self._route_and_score(company_ids, score_date)

        summary = {
            "total_companies": len(company_ids),
            "companies_scored": len(all_pitches),
            "score_date": str(score_date),
            "stages": {},
        }

        for stage in BusinessStage:
            if stage == BusinessStage.UNKNOWN:
                continue

            stage_pitches = [p for p in all_pitches if p.stage == stage]

            if stage_pitches:
                summary["stages"][stage.value] = {
                    "count": len(stage_pitches),
                    "avg_quality": round(
                        sum(p.quality_score for p in stage_pitches) / len(stage_pitches), 1
                    ),
                    "avg_cheapness": round(
                        sum(p.cheapness_score for p in stage_pitches) / len(stage_pitches), 1
                    ),
                    "avg_fat_pitch_score": round(
                        sum(p.fat_pitch_score for p in stage_pitches) / len(stage_pitches), 1
                    ),
                    "actionable": len([p for p in stage_pitches if p.is_actionable]),
                    "tier_1_count": len([p for p in stage_pitches if p.quality_tier == 1]),
                    "tier_2_count": len([p for p in stage_pitches if p.quality_tier == 2]),
                }
            else:
                summary["stages"][stage.value] = {"count": 0}

        return summary

    async def get_profile_info(self, stage: BusinessStage) -> Dict:
        """Get scoring profile info for a stage."""
        profile = STAGE_PROFILES.get(stage)
        if not profile:
            return {}

        return {
            "stage": stage.value,
            "display_name": profile.display_name,
            "description": profile.description,
            "dimension_weights": profile.dimension_weights,
            "min_quality_score": profile.min_quality_score,
            "cheapness_weight": profile.cheapness_weight,
            "tier_thresholds": profile.tier_thresholds,
        }

    async def get_dimension_details(
        self,
        company_id: str,
        score_date: date = None,
    ) -> List[Dict]:
        """
        Get full dimension details including metadata for a company.

        Returns all dimension scores with their underlying metrics.
        """
        import json

        score_date = score_date or date.today()

        # Use DISTINCT ON for efficient "latest per dimension" query
        query = """
        SELECT DISTINCT ON (dimension_code)
            dimension_code,
            score,
            confidence,
            data_quality,
            score_low,
            score_high,
            metadata
        FROM daily_dimension_scores
        WHERE company_id = $1::uuid
        AND score_date <= $2
        ORDER BY dimension_code, score_date DESC
        """

        try:
            rows = await self.db_conn.fetch(query, company_id, score_date)
        except Exception as e:
            logger.error(f"Error fetching dimension details for {company_id}: {e}")
            return []

        result = []
        for row in rows:
            # Parse metadata if it's a string (asyncpg sometimes returns JSONB as string)
            metadata = row["metadata"]
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except json.JSONDecodeError:
                    metadata = {}
            elif metadata is None:
                metadata = {}

            result.append({
                "dimension_code": row["dimension_code"],
                "score": float(row["score"]) if row["score"] else None,
                "confidence": float(row["confidence"]) if row["confidence"] else None,
                "data_quality": float(row["data_quality"]) if row["data_quality"] else None,
                "score_low": float(row["score_low"]) if row["score_low"] else None,
                "score_high": float(row["score_high"]) if row["score_high"] else None,
                "metadata": metadata,
            })

        return result
