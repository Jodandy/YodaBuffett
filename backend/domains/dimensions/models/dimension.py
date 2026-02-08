"""
Dimension scoring data models.

These dataclasses define the standardized output format for all dimensions.
Each dimension calculator is a black box that can use any methodology internally,
but must produce a DimensionScore with these fields.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import date, datetime
from decimal import Decimal


@dataclass
class DimensionScore:
    """
    Standardized output from any dimension calculator.

    This is the "contract" that all dimension calculators must fulfill.
    The internal calculation can be anything - weighted factors, ML models,
    NLP analysis, external API calls, etc. - but the output is always this format.

    Examples of what `metadata` might contain for different dimensions:

    Value dimension:
        {"pe_ratio": 12.4, "pe_contribution": 18.5, "pb_ratio": 1.8, "dcf_upside": 0.23}

    Sentiment dimension:
        {"news_sentiment": 0.72, "social_sentiment": 0.45, "filing_tone_change": -0.12,
         "articles_analyzed": 47, "key_topics": ["earnings", "expansion"]}

    Moat dimension:
        {"moat_type": "wide", "durability_score": 85, "key_sources": ["brand", "network_effects"],
         "competitive_threats": ["new_entrants"], "confidence_reasoning": "Strong brand loyalty..."}

    Regulatory Risk dimension:
        {"risk_level": "medium", "key_regulations": ["GDPR", "Basel III"],
         "pending_litigation": 2, "compliance_score": 78, "sector_regulatory_pressure": "high"}

    Credit dimension:
        {"external_rating": "BBB+", "outlook": "stable", "altman_z": 3.2,
         "interest_coverage": 8.5, "debt_to_equity": 0.45, "probability_of_default": 0.02}
    """

    # Identity
    company_id: str
    score_date: date
    dimension_code: str

    # Core score (0-100 scale, standardized across all dimensions)
    score: float

    # Confidence in the score (0-1)
    # - High confidence: abundant, high-quality data, stable methodology
    # - Low confidence: sparse data, extrapolation, high uncertainty
    confidence: Optional[float] = None

    # Data quality indicator (0-1)
    # - What percentage of ideal inputs were available?
    data_quality: Optional[float] = None

    # Percentile ranking (calculated post-hoc across universe)
    percentile_rank: Optional[float] = None
    universe_size: Optional[int] = None
    universe_filter: Optional[Dict[str, Any]] = None  # e.g., {"sector": "Technology", "country": "SE"}

    # Score uncertainty range
    score_low: Optional[float] = None
    score_high: Optional[float] = None

    # Dimension-specific metadata (the flexible part)
    # Each dimension puts whatever is relevant here
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Computation info
    computed_at: datetime = field(default_factory=datetime.now)
    computation_time_ms: Optional[int] = None
    calculator_version: Optional[str] = None
    definition_version: int = 1

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "company_id": self.company_id,
            "score_date": self.score_date,
            "dimension_code": self.dimension_code,
            "score": self.score,
            "confidence": self.confidence,
            "data_quality": self.data_quality,
            "percentile_rank": self.percentile_rank,
            "universe_size": self.universe_size,
            "universe_filter": self.universe_filter,
            "score_low": self.score_low,
            "score_high": self.score_high,
            "metadata": self.metadata,
            "computed_at": self.computed_at,
            "computation_time_ms": self.computation_time_ms,
            "calculator_version": self.calculator_version,
            "definition_version": self.definition_version,
        }


@dataclass
class DimensionDefinition:
    """
    Registration/metadata for a dimension.

    This describes WHAT a dimension is, not HOW it's calculated.
    The calculation logic lives in the calculator class.
    """

    dimension_code: str
    display_name: str
    description: str
    category: str  # 'fundamental', 'technical', 'alternative', 'ai_derived', 'external'

    # What data sources does this dimension use?
    data_sources: List[str] = field(default_factory=list)

    # How often should it be updated?
    update_frequency: str = "daily"  # 'realtime', 'daily', 'weekly', 'quarterly'

    # Does it need external API calls?
    requires_external_api: bool = False

    # Flexible config for the calculator
    config: Dict[str, Any] = field(default_factory=dict)

    # Version tracking
    version: int = 1
    is_active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "dimension_code": self.dimension_code,
            "display_name": self.display_name,
            "description": self.description,
            "category": self.category,
            "data_sources": self.data_sources,
            "update_frequency": self.update_frequency,
            "requires_external_api": self.requires_external_api,
            "config": self.config,
            "version": self.version,
            "is_active": self.is_active,
        }


@dataclass
class CompositeScore:
    """
    A combined score from multiple dimensions.

    Used for things like "Overall Score", "Quality-Value Combo", etc.
    """

    company_id: str
    score_date: date
    composite_code: str  # 'overall', 'quality_value', 'risk_adjusted_momentum'

    # Combined score
    score: float
    confidence: Optional[float] = None
    percentile_rank: Optional[float] = None

    # What went into it
    dimension_scores: Dict[str, float] = field(default_factory=dict)  # {"value": 72.5, "momentum": 68.3}
    dimension_weights: Dict[str, float] = field(default_factory=dict)  # {"value": 0.25, "momentum": 0.20}
    missing_dimensions: List[str] = field(default_factory=list)

    computed_at: datetime = field(default_factory=datetime.now)


@dataclass
class ScoreHistoryPoint:
    """A single point in a dimension's score history."""

    date: date
    score: float
    percentile_rank: Optional[float] = None
    confidence: Optional[float] = None


@dataclass
class ComputationResult:
    """
    Result of a batch dimension computation run.

    Used by the daily worker to track what happened.
    """

    dimension_code: str
    score_date: date

    # Counts
    companies_processed: int = 0
    companies_succeeded: int = 0
    companies_failed: int = 0
    companies_skipped: int = 0

    # Timing
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    total_duration_ms: Optional[int] = None

    # Status
    status: str = "running"  # 'running', 'completed', 'failed', 'partial'

    # Errors encountered
    errors: Dict[str, int] = field(default_factory=dict)  # {"timeout": 5, "missing_data": 12}
    error_details: List[Dict[str, Any]] = field(default_factory=list)

    # The scores that were computed
    scores: List[DimensionScore] = field(default_factory=list)

    def mark_completed(self):
        """Mark this computation as completed."""
        self.completed_at = datetime.now()
        self.status = "completed" if self.companies_failed == 0 else "partial"
        if self.started_at:
            self.total_duration_ms = int((self.completed_at - self.started_at).total_seconds() * 1000)

    def add_error(self, error_type: str, company_id: str, details: Optional[str] = None):
        """Record an error."""
        self.errors[error_type] = self.errors.get(error_type, 0) + 1
        self.error_details.append({
            "company_id": company_id,
            "error_type": error_type,
            "details": details,
            "timestamp": datetime.now().isoformat(),
        })
