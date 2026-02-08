"""
Fat Pitch Scorer

Scores companies as investment opportunities based on their lifecycle stage.

Each stage has different dimension weights reflecting what matters most:
- Early Stage: Growth trajectory, momentum (most qualitative, LLM territory)
- Growth Stage: Growth quality, profitability, returns, capital allocation
- Mature Yield: Dividend safety, financial health, earnings quality, value trap avoidance
- Compounder: Moat/quality, returns, profitability, growth stability
"""

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional
import logging
import numpy as np

from .models import (
    BusinessStage,
    StageProfile,
    FatPitch,
    PitchRanking,
    CompanyFinancials,
)

logger = logging.getLogger(__name__)


# ============================================================================
# STAGE SCORING PROFILES
# ============================================================================

STAGE_PROFILES: Dict[BusinessStage, StageProfile] = {

    # -------------------------------------------------------------------------
    # EARLY STAGE
    # Focus: Growth trajectory, momentum as traction signal
    # Note: Most early-stage analysis is qualitative (TAM, team, product)
    # Our dimensions can only capture the quantitative signals
    # -------------------------------------------------------------------------
    BusinessStage.EARLY_STAGE: StageProfile(
        stage=BusinessStage.EARLY_STAGE,
        display_name="Early Stage",
        description="High-growth companies, pre-profit or small revenue",
        dimension_weights={
            # Growth signals (60%)
            "growth": 0.30,           # Revenue trajectory
            "momentum": 0.20,         # Price momentum as traction proxy
            "capital_allocation": 0.10,  # Are they investing well?

            # Quality signals (25%)
            "earnings_quality": 0.15,  # Even early, watch for manipulation
            "profitability": 0.10,     # Path to profitability

            # Risk signals (15%)
            "financial_health": 0.10,  # Runway, not going bankrupt
            "beneish_mscore": 0.05,    # Fraud check
        },
        min_quality_score=40.0,  # Lower bar - early stage is speculative
        cheapness_weight=0.30,   # Quality matters more than cheapness here
    ),

    # -------------------------------------------------------------------------
    # GROWTH STAGE
    # Focus: Unit economics, Rule of 40, moat formation, execution
    # -------------------------------------------------------------------------
    BusinessStage.GROWTH_STAGE: StageProfile(
        stage=BusinessStage.GROWTH_STAGE,
        display_name="Growth Stage",
        description="Profitable, high-growth companies (>20% growth)",
        dimension_weights={
            # Growth & Profitability (45%)
            "growth": 0.20,            # Revenue/earnings growth
            "profitability": 0.15,     # Margin trajectory
            "returns": 0.10,           # ROIC, capital efficiency

            # Quality & Efficiency (35%)
            "earnings_quality": 0.15,  # Cash backing, accruals
            "capital_allocation": 0.10,  # Reinvestment quality
            "quality": 0.10,           # Overall quality metrics

            # Safety (20%)
            "financial_health": 0.10,  # Balance sheet
            "beneish_mscore": 0.05,    # Fraud check
            "risk": 0.05,              # Volatility
        },
        min_quality_score=50.0,
        cheapness_weight=0.35,
    ),

    # -------------------------------------------------------------------------
    # MATURE YIELD
    # Focus: Dividend safety, value trap avoidance, capital returns
    # -------------------------------------------------------------------------
    BusinessStage.MATURE_YIELD: StageProfile(
        stage=BusinessStage.MATURE_YIELD,
        display_name="Mature Yield",
        description="Dividend-focused, low-growth companies",
        dimension_weights={
            # Dividend Safety (40%)
            "capital_allocation": 0.20,  # Dividend coverage, FCF payout
            "financial_health": 0.15,    # Debt, interest coverage
            "earnings_quality": 0.10,    # Cash backing of earnings

            # Value Trap Avoidance (30%)
            "beneish_mscore": 0.10,    # Manipulation check
            "profitability": 0.10,     # Margin stability
            "returns": 0.10,           # Sustainable returns

            # Valuation (20%)
            "value": 0.10,              # Fundamental value
            "valuation_percentile": 0.10,  # Historical cheapness

            # Other (10%)
            "quality": 0.05,
            "risk": 0.05,
        },
        min_quality_score=55.0,  # Higher bar - yield traps are dangerous
        cheapness_weight=0.45,   # Yield investors care about price/yield
    ),

    # -------------------------------------------------------------------------
    # COMPOUNDER
    # Focus: Moat, returns on capital, management, reinvestment
    # The classic Buffett-style quality compounder
    # -------------------------------------------------------------------------
    BusinessStage.COMPOUNDER: StageProfile(
        stage=BusinessStage.COMPOUNDER,
        display_name="Compounder",
        description="High-quality businesses with stable, profitable growth",
        dimension_weights={
            # Moat & Quality (40%)
            "quality": 0.15,           # Overall quality
            "returns": 0.15,           # ROE, ROIC, DuPont
            "profitability": 0.10,     # Margins

            # Capital Efficiency (25%)
            "capital_allocation": 0.15,  # Reinvestment, self-funding
            "earnings_quality": 0.10,    # Accounting quality

            # Growth & Stability (20%)
            "growth": 0.10,            # Sustainable growth
            "financial_health": 0.10,  # Fortress balance sheet

            # Safety (15%)
            "beneish_mscore": 0.05,    # Fraud check
            "risk": 0.05,              # Low volatility preferred
            "working_capital": 0.05,   # Operating efficiency
        },
        min_quality_score=60.0,  # High bar - compounders must be quality
        cheapness_weight=0.35,   # Will pay up for quality, but price matters
    ),

    # -------------------------------------------------------------------------
    # ESTABLISHED
    # Catch-all for profitable companies that don't fit other categories
    # General quality assessment
    # -------------------------------------------------------------------------
    BusinessStage.ESTABLISHED: StageProfile(
        stage=BusinessStage.ESTABLISHED,
        display_name="Established",
        description="Profitable businesses - general quality assessment",
        dimension_weights={
            # Balanced weights across all dimensions
            "quality": 0.12,
            "profitability": 0.12,
            "returns": 0.12,
            "growth": 0.10,
            "financial_health": 0.10,
            "earnings_quality": 0.10,
            "capital_allocation": 0.10,
            "value": 0.08,
            "valuation_percentile": 0.06,
            "beneish_mscore": 0.05,
            "risk": 0.05,
        },
        min_quality_score=50.0,
        cheapness_weight=0.40,
    ),
}


class FatPitchScorer:
    """
    Scores companies as fat pitch candidates based on their stage.
    """

    # Dimensions used for cheapness score
    CHEAPNESS_DIMENSIONS = ["value", "valuation_percentile"]

    def __init__(self, db_conn=None):
        self.db_conn = db_conn
        self.profiles = STAGE_PROFILES

    async def score_company(
        self,
        company_id: str,
        stage: BusinessStage,
        dimension_scores: Dict[str, float],
        financials: Optional[CompanyFinancials] = None,
        stage_confidence: float = 1.0,
    ) -> Optional[FatPitch]:
        """
        Score a single company as a fat pitch candidate.

        Args:
            company_id: Company UUID
            stage: Assigned business stage
            dimension_scores: Dict of dimension_code -> score (0-100)
            financials: Optional company financials
            stage_confidence: Confidence in stage assignment (0-1)

        Returns:
            FatPitch object or None if insufficient data
        """

        if stage == BusinessStage.UNKNOWN:
            return None

        profile = self.profiles.get(stage)
        if not profile:
            logger.warning(f"No profile for stage {stage}")
            return None

        # Calculate quality score (weighted dimensions)
        quality_score, contributions = self._calculate_quality_score(
            dimension_scores, profile
        )

        # Calculate cheapness score
        cheapness_score = self._calculate_cheapness_score(dimension_scores)

        # Calculate fat pitch score
        fat_pitch_score = self._calculate_fat_pitch_score(
            quality_score, cheapness_score, profile
        )

        # Determine quality tier
        quality_tier = profile.get_tier(quality_score)

        # Generate flags and warnings
        flags = self._generate_flags(dimension_scores, quality_score, cheapness_score)
        warnings = self._generate_warnings(dimension_scores, stage)

        # Get company info
        company_info = await self._get_company_info(company_id)

        return FatPitch(
            company_id=company_id,
            symbol=company_info.get("symbol", ""),
            company_name=company_info.get("company_name", ""),
            stage=stage,
            stage_confidence=stage_confidence,
            quality_score=round(quality_score, 1),
            cheapness_score=round(cheapness_score, 1),
            fat_pitch_score=round(fat_pitch_score, 1),
            quality_tier=quality_tier,
            dimension_scores=dimension_scores,
            dimension_contributions=contributions,
            flags=flags,
            warnings=warnings,
            financials=financials,
        )

    def _calculate_quality_score(
        self,
        dimension_scores: Dict[str, float],
        profile: StageProfile
    ) -> tuple[float, Dict[str, float]]:
        """
        Calculate weighted quality score based on stage profile.

        Returns:
            Tuple of (quality_score, contributions_dict)
        """
        contributions = {}
        weighted_sum = 0.0
        total_weight = 0.0

        for dim, weight in profile.dimension_weights.items():
            if dim in dimension_scores:
                score = dimension_scores[dim]
                contribution = score * weight
                contributions[dim] = round(contribution, 2)
                weighted_sum += contribution
                total_weight += weight

        if total_weight == 0:
            return 50.0, contributions

        # Normalize by actual weights used
        quality_score = weighted_sum / total_weight * (total_weight / sum(profile.dimension_weights.values()))

        # Scale to account for missing dimensions
        completeness = total_weight / sum(profile.dimension_weights.values())
        # Penalize incomplete data slightly
        quality_score = quality_score * (0.8 + 0.2 * completeness)

        return quality_score, contributions

    def _calculate_cheapness_score(
        self,
        dimension_scores: Dict[str, float]
    ) -> float:
        """
        Calculate cheapness score from value-related dimensions.

        Higher score = cheaper/more attractive valuation.
        """
        cheapness_scores = []

        for dim in self.CHEAPNESS_DIMENSIONS:
            if dim in dimension_scores:
                cheapness_scores.append(dimension_scores[dim])

        if not cheapness_scores:
            return 50.0  # Neutral if no data

        return np.mean(cheapness_scores)

    def _calculate_fat_pitch_score(
        self,
        quality_score: float,
        cheapness_score: float,
        profile: StageProfile
    ) -> float:
        """
        Combine quality and cheapness into fat pitch score.

        Fat Pitch = Quality business + Cheap price
        """
        quality_weight = 1.0 - profile.cheapness_weight
        cheapness_weight = profile.cheapness_weight

        return (quality_score * quality_weight) + (cheapness_score * cheapness_weight)

    def _generate_flags(
        self,
        dimension_scores: Dict[str, float],
        quality_score: float,
        cheapness_score: float
    ) -> List[str]:
        """Generate notable characteristic flags."""
        flags = []

        if quality_score >= 75:
            flags.append("High quality")
        if cheapness_score >= 75:
            flags.append("Historically cheap")
        if cheapness_score >= 80 and quality_score >= 60:
            flags.append("Quality at discount")

        # Check individual dimensions
        if dimension_scores.get("returns", 0) >= 80:
            flags.append("High returns on capital")
        if dimension_scores.get("growth", 0) >= 75:
            flags.append("Strong growth")
        if dimension_scores.get("earnings_quality", 0) >= 80:
            flags.append("Clean earnings")
        if dimension_scores.get("financial_health", 0) >= 80:
            flags.append("Fortress balance sheet")

        return flags

    def _generate_warnings(
        self,
        dimension_scores: Dict[str, float],
        stage: BusinessStage
    ) -> List[str]:
        """Generate warning flags."""
        warnings = []

        # Beneish M-Score warning
        beneish = dimension_scores.get("beneish_mscore", 100)
        if beneish < 40:
            warnings.append("Manipulation risk (Beneish)")

        # Financial health warning
        health = dimension_scores.get("financial_health", 100)
        if health < 35:
            warnings.append("Weak financial health")

        # Earnings quality warning
        eq = dimension_scores.get("earnings_quality", 100)
        if eq < 35:
            warnings.append("Low earnings quality")

        # Stage-specific warnings
        if stage == BusinessStage.MATURE_YIELD:
            capital = dimension_scores.get("capital_allocation", 100)
            if capital < 40:
                warnings.append("Dividend sustainability concern")

        if stage == BusinessStage.GROWTH_STAGE:
            prof = dimension_scores.get("profitability", 100)
            if prof < 35:
                warnings.append("Profitability declining")

        return warnings

    async def _get_company_info(self, company_id: str) -> Dict:
        """Get basic company info."""
        if not self.db_conn:
            return {}

        row = await self.db_conn.fetchrow("""
            SELECT primary_ticker as symbol, company_name
            FROM company_master WHERE id = $1
        """, company_id)

        return dict(row) if row else {}

    async def score_companies(
        self,
        companies: List[tuple],  # List of (company_id, stage, confidence)
        dimension_scores: Dict[str, Dict[str, float]],  # company_id -> dim_code -> score
        score_date: date = None,
    ) -> List[FatPitch]:
        """
        Score multiple companies.

        Args:
            companies: List of (company_id, stage, confidence) tuples
            dimension_scores: Nested dict of company_id -> dimension_code -> score
            score_date: Date for scoring

        Returns:
            List of FatPitch objects, sorted by fat_pitch_score descending
        """
        score_date = score_date or date.today()

        pitches = []
        for company_id, stage, confidence in companies:
            if company_id not in dimension_scores:
                continue

            pitch = await self.score_company(
                company_id=company_id,
                stage=stage,
                dimension_scores=dimension_scores[company_id],
                stage_confidence=confidence,
            )

            if pitch:
                pitch.score_date = score_date
                pitches.append(pitch)

        # Sort by fat pitch score
        pitches.sort(key=lambda p: p.fat_pitch_score, reverse=True)

        return pitches

    async def create_ranking(
        self,
        stage: BusinessStage,
        pitches: List[FatPitch],
        score_date: date = None,
    ) -> PitchRanking:
        """Create a ranked list of pitches for a stage."""
        score_date = score_date or date.today()

        stage_pitches = [p for p in pitches if p.stage == stage]
        stage_pitches.sort(key=lambda p: p.fat_pitch_score, reverse=True)

        return PitchRanking(
            stage=stage,
            score_date=score_date,
            pitches=stage_pitches,
            total_companies=len(stage_pitches),
            companies_with_data=len([p for p in stage_pitches if p.dimension_scores]),
        )
