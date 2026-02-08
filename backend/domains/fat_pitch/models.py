"""
Fat Pitch Models

Data structures for the fat pitch pitching machine.
"""

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Dict, List, Optional, Any


class BusinessStage(Enum):
    """Business lifecycle stages for routing."""

    EARLY_STAGE = "early_stage"
    GROWTH_STAGE = "growth_stage"
    MATURE_YIELD = "mature_yield"
    COMPOUNDER = "compounder"
    ESTABLISHED = "established"
    UNKNOWN = "unknown"  # Insufficient data to route


@dataclass
class CompanyFinancials:
    """Key financials used for routing and scoring."""

    company_id: str
    symbol: str
    company_name: str

    # Revenue metrics
    revenue_ttm: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    revenue_growth_3yr_cagr: Optional[float] = None

    # Profitability
    is_profitable: Optional[bool] = None
    net_income_ttm: Optional[float] = None
    ebit_margin: Optional[float] = None
    fcf_margin: Optional[float] = None

    # Dividends
    dividend_yield: Optional[float] = None
    dividend_payout_ratio: Optional[float] = None

    # Growth stability
    growth_volatility: Optional[float] = None  # StdDev of growth rates

    # Market data
    market_cap: Optional[float] = None

    def __post_init__(self):
        """Derive is_profitable if not set."""
        if self.is_profitable is None and self.net_income_ttm is not None:
            self.is_profitable = self.net_income_ttm > 0


@dataclass
class StageProfile:
    """
    Scoring profile for a business stage.

    Defines which dimensions matter and their weights for each stage.
    """

    stage: BusinessStage
    display_name: str
    description: str

    # Dimension weights (must sum to 1.0)
    dimension_weights: Dict[str, float]

    # Quality threshold for "investable" (0-100)
    min_quality_score: float = 50.0

    # Cheapness weight in final score (vs quality)
    cheapness_weight: float = 0.4  # 40% cheapness, 60% quality

    # Tier thresholds (quality score ranges)
    tier_thresholds: Dict[int, float] = field(default_factory=lambda: {
        1: 80,  # Tier 1: 80+
        2: 65,  # Tier 2: 65-79
        3: 50,  # Tier 3: 50-64
        4: 35,  # Tier 4: 35-49
        5: 0,   # Tier 5: 0-34
    })

    def get_tier(self, quality_score: float) -> int:
        """Get quality tier from score."""
        for tier, threshold in sorted(self.tier_thresholds.items()):
            if quality_score >= threshold:
                return tier
        return 5


@dataclass
class FatPitch:
    """
    A fat pitch candidate - a company that may be worth investigating.
    """

    company_id: str
    symbol: str
    company_name: str

    # Routing
    stage: BusinessStage
    stage_confidence: float  # 0-1, how confident are we in the stage assignment

    # Scores
    quality_score: float  # 0-100, stage-weighted dimension score
    cheapness_score: float  # 0-100, value + valuation percentile
    fat_pitch_score: float  # 0-100, combined score

    # Quality tier (1-5, 1 is best)
    quality_tier: int

    # Dimension breakdown
    dimension_scores: Dict[str, float]  # Individual dimension scores
    dimension_contributions: Dict[str, float]  # Weighted contributions

    # Flags and signals
    flags: List[str] = field(default_factory=list)  # Notable characteristics
    warnings: List[str] = field(default_factory=list)  # Red flags

    # Metadata
    score_date: date = None
    financials: Optional[CompanyFinancials] = None

    def __post_init__(self):
        if self.score_date is None:
            self.score_date = date.today()

    @property
    def is_actionable(self) -> bool:
        """Is this pitch worth acting on now?"""
        return self.quality_tier <= 3 and self.cheapness_score >= 60

    @property
    def pitch_summary(self) -> str:
        """One-line summary of the pitch."""
        stage_name = self.stage.value.replace("_", " ").title()
        return f"{stage_name} | Tier {self.quality_tier} | Quality: {self.quality_score:.0f} | Cheap: {self.cheapness_score:.0f}"


@dataclass
class PitchRanking:
    """Ranked list of pitches for a stage."""

    stage: BusinessStage
    score_date: date
    pitches: List[FatPitch]

    # Summary stats
    total_companies: int = 0
    companies_with_data: int = 0
    actionable_pitches: int = 0

    def __post_init__(self):
        self.actionable_pitches = sum(1 for p in self.pitches if p.is_actionable)

    @property
    def top_pitches(self) -> List[FatPitch]:
        """Top 10 actionable pitches."""
        return [p for p in self.pitches if p.is_actionable][:10]
