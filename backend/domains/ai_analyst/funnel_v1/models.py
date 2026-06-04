"""
Data models for the Focus-Narrowing Engine
"""
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class Side(str, Enum):
    """Two-sided classification"""
    FEAR_PREMIUM = "FEAR_PREMIUM"                  # Cheap, pessimism priced in
    UNDERPRICED_DURABILITY = "UNDERPRICED_DURABILITY"  # Quality compounder that's also cheap
    QUALITY_WATCH = "QUALITY_WATCH"                # Quality compounder at full/rich price - watchlist
    EXPENSIVE_FRAGILE = "EXPENSIVE_FRAGILE"        # Overpriced, can't deliver
    FAIR_NO_EDGE = "FAIR_NO_EDGE"                  # No material mispricing
    ASSET_ONLY = "ASSET_ONLY"                      # No earnings; NAV-based
    DATA_SUSPECT = "DATA_SUSPECT"                  # Distressed/corrupted data


@dataclass
class CompanyInput:
    """Input data per company (one row)"""
    ticker: str
    name: str
    price: float
    eps_norm: Optional[float]          # Normalized earnings power per share
    growth_hist: Optional[float]       # Historical CAGR (as decimal, e.g. 0.08 = 8%)
    roic: Optional[float]              # Return on invested capital (as decimal)
    nav_ps: Optional[float]            # NAV/book per share
    div_ps: Optional[float] = None     # Dividend per share (optional)
    sector: Optional[str] = None       # Sector/category (optional)

    # Durable compounder data (optional - for reference growth logic)
    growth_cagr_robust: Optional[float] = None     # Robust multi-year CAGR
    track_record_years: Optional[int] = None       # Years of financial history
    growth_consistency_score: Optional[float] = None  # Fraction of positive growth years
    goodwill_fraction: Optional[float] = None      # Goodwill / invested capital
    organic_roic: Optional[float] = None           # ROIC excluding goodwill


@dataclass
class TriageResult:
    """Output per company after triage"""
    ticker: str
    name: str
    price: float

    # Implied growth rates
    g_short: Optional[float]           # Implied growth at primary horizon
    g_long: Optional[float]            # Implied growth at long horizon

    # Reference and gap
    ref_growth: Optional[float]        # What we believe the business can do
    ceiling: Optional[float]           # ROIC-based sustainable growth ceiling
    gap: Optional[float]               # ref_growth - g_short (positive = cheap)

    # Asset play
    disc_to_nav: Optional[float]       # Discount to NAV (positive = cheap)

    # Compounder diagnostic
    duration_dependence: Optional[float]  # g_short - g_long

    # Classification
    side: Side
    triage_priority: float             # Ranking score (abs(gap) or disc_to_nav)
    hinge: str                         # "What you'd have to believe" summary

    # Passthrough
    roic: Optional[float]
    growth_hist: Optional[float]
