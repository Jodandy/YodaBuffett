"""
Quality Screener Models

Standalone models - no Fat Pitch dependencies.
"""

from dataclasses import dataclass, field
from typing import Optional, List
from datetime import date
from enum import Enum


class BusinessModel(str, Enum):
    """Business model classification based on cash conversion."""
    CASH_COW = "Cash Cow"
    COMPOUNDER = "Compounder"
    CAUTION = "Caution"
    RED_FLAG = "Red Flag"
    UNCLEAR = "Unclear"
    UNKNOWN = "Unknown"


class SizeCategory(str, Enum):
    """Market cap size categories."""
    MICRO = "Micro"      # < $25M
    SMALL = "Small"      # $25M - $100M
    MID = "Mid"          # $100M - $1B
    LARGE = "Large"      # $1B - $10B
    MEGA = "Mega"        # > $10B


class CashQuality(str, Enum):
    """Cash conversion quality (OCF/NI ratio)."""
    EXCELLENT = "Excellent"  # > 100%
    GOOD = "Good"            # > 80%
    MODERATE = "Moderate"    # > 70%
    WEAK = "Weak"            # > 50%
    POOR = "Poor"            # < 50%
    UNKNOWN = "Unknown"


class QualityTier(int, Enum):
    """Quality tier ranking."""
    TIER_1 = 1  # Potential Compounder
    TIER_2 = 2  # Solid Quality
    TIER_3 = 3  # Decent Business
    TIER_4 = 4  # Mixed Signals
    TIER_5 = 5  # Not Quality


# Size category thresholds
SIZE_THRESHOLDS = {
    SizeCategory.MICRO: (0, 25e6),
    SizeCategory.SMALL: (25e6, 100e6),
    SizeCategory.MID: (100e6, 1e9),
    SizeCategory.LARGE: (1e9, 10e9),
    SizeCategory.MEGA: (10e9, float('inf')),
}

# Cash quality thresholds (OCF/NI ratio)
CASH_THRESHOLDS = {
    CashQuality.EXCELLENT: 1.0,
    CashQuality.GOOD: 0.8,
    CashQuality.MODERATE: 0.7,
    CashQuality.WEAK: 0.5,
    CashQuality.POOR: 0.0,
}


@dataclass
class QualityCandidate:
    """A company with quality classification."""
    # Identity
    ticker: str
    company_name: str

    # Categories (main filters)
    tier: int
    tier_description: str
    business_model: str
    business_model_reason: str
    size_category: str
    cash_quality: str
    is_profitable: bool

    # Score
    quality_score: int

    # Key Metrics
    market_cap: float
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roic: Optional[float] = None
    roe: Optional[float] = None
    revenue_cagr: Optional[float] = None

    # Cash Conversion
    ocf_to_ni: Optional[float] = None
    fcf_to_ni: Optional[float] = None
    fcf_yield: Optional[float] = None

    # Capital Intensity
    capex_to_revenue: Optional[float] = None
    capex_to_da: Optional[float] = None

    # Working Capital
    negative_working_capital: bool = False
    receivables_vs_revenue: Optional[float] = None

    # Trends
    gross_margin_trend: Optional[float] = None
    share_dilution: Optional[float] = None

    # Financial Strength
    net_cash: bool = False
    net_debt_to_ebitda: Optional[float] = None

    # Valuation
    pe_ratio: Optional[float] = None
    ev_to_ebitda: Optional[float] = None

    # Qualitative
    reasons: List[str] = field(default_factory=list)
    concerns: List[str] = field(default_factory=list)


@dataclass
class ScreenerFilters:
    """Filter options for the screener."""
    tiers: Optional[List[int]] = None
    business_models: Optional[List[str]] = None
    size_categories: Optional[List[str]] = None
    cash_qualities: Optional[List[str]] = None
    profitable_only: bool = False
    min_market_cap: float = 0
    max_market_cap: float = float('inf')


@dataclass
class ScreenerSummary:
    """Summary statistics for the screener."""
    total_companies: int
    by_tier: dict
    by_business_model: dict
    by_size: dict
    by_cash_quality: dict
    profitable_count: int
    unprofitable_count: int


@dataclass
class CategoryOptions:
    """Available filter options."""
    tiers: List[dict]
    business_models: List[dict]
    size_categories: List[dict]
    cash_qualities: List[dict]
