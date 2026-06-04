"""
Data models for NAV-Quality Evaluator
"""
from dataclasses import dataclass
from typing import Optional
from enum import Enum


class NAVRealityStatus(str, Enum):
    """Is the NAV real and recoverable?"""
    REAL = "REAL"              # Marked-to-market, liquid
    PARTLY = "PARTLY"          # Mix of real and suspect
    SUSPECT = "SUSPECT"        # Stale appraisals, mark-to-model


class CatalystStatus(str, Enum):
    """Is there a catalyst for discount closure?"""
    PRESENT = "PRESENT"        # Buyback, dividend, activist, sale process
    ABSENT = "ABSENT"          # Controlled float, entrenched insiders


class JudgmentFlag(str, Enum):
    """Final judgment classification"""
    CANDIDATE = "CANDIDATE"                # Real NAV, has catalyst, discount unwarranted
    VALUE_TRAP_RISK = "VALUE_TRAP_RISK"    # No catalyst or structural issues
    NAV_SUSPECT = "NAV_SUSPECT"            # NAV quality questionable
    DATA_SUSPECT = "DATA_SUSPECT"          # Corrupted/implausible data


@dataclass
class CompanyAssetInput:
    """Input data for asset-backed company (Layer 1)"""
    ticker: str
    name: str
    price: float

    # Balance sheet (point-in-time)
    total_assets: float
    total_liabilities: float
    total_equity: float
    cash_and_equivalents: float
    marketable_securities: Optional[float]
    receivables: Optional[float]
    investment_property: Optional[float]
    goodwill: Optional[float]
    intangibles: Optional[float]

    # Cash burn
    quarterly_cash_burn: Optional[float]  # Negative OCF

    # Share count
    shares_outstanding: float

    # Recent dilution
    recent_share_issuance: Optional[float]  # Fraction, e.g., 0.10 = 10% dilution


@dataclass
class Layer1Result:
    """Layer 1 math output - deterministic NAV analysis"""
    ticker: str
    name: str
    price: float

    # NAV calculations
    book_nav_ps: float                      # Total equity / shares
    hard_nav_ps: float                      # (Hard assets - liabilities) / shares
    soft_fraction: float                    # Soft assets / total assets

    # Discounts
    disc_to_hard_nav: float                 # Positive = margin of safety
    disc_to_book: float                     # Discount to book value

    # Survival metrics
    runway_quarters: Optional[float]        # Cash / quarterly burn
    dilution_flag: bool                     # True = at risk of dilution

    # Candidate status (Layer 1 filter)
    is_candidate: bool                      # Passed all Layer 1 filters


@dataclass
class Layer2Judgment:
    """Layer 2 LLM judgment - qualitative assessment"""
    ticker: str

    # Three questions with citations
    nav_reality: NAVRealityStatus
    nav_reality_reason: str                 # One-line reason + citation

    catalyst_status: CatalystStatus
    catalyst_reason: str                    # What it is (or why absent) + citation

    why_cheap: str                          # Short reason + citation

    # Final flag
    judgment_flag: JudgmentFlag

    # Evidence sources used
    sources: list[str]                      # List of documents/sections cited


@dataclass
class NAVEvaluationResult:
    """
    Complete NAV evaluation result combining Layer 1 and Layer 2.

    Workflow:
    - Layer 1 runs first (deterministic math)
    - If Layer 1 passes, Layer 2 runs (LLM judgment)
    - If Layer 1 fails, no Layer 2 needed
    """
    ticker: str
    name: str

    # Layer 1 results (always present)
    layer1: Layer1Result

    # Layer 2 results (only if Layer 1 passed)
    layer2: Optional[Layer2Judgment] = None

    # Final status
    passed_layer1: bool = False             # Deterministic filters
    final_judgment: Optional[JudgmentFlag] = None  # From Layer 2 if available
