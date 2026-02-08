"""
Analysis helpers for sophisticated dimension calculations.

Provides reusable functions for:
- Historical analysis (trends, averages, volatility)
- Peer comparison (sector percentiles, distance from median)
- Quality scoring (data completeness, outlier detection)
- Score normalization (0-100 scaling with proper handling)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from datetime import date, timedelta
from decimal import Decimal
import numpy as np
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TrendDirection(Enum):
    """Trend direction classification."""
    STRONG_UP = "strong_improvement"
    UP = "improving"
    STABLE = "stable"
    DOWN = "declining"
    STRONG_DOWN = "strong_decline"
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass
class MetricAnalysis:
    """Complete analysis of a single metric."""

    # Current values
    current: Optional[float] = None
    ttm: Optional[float] = None  # Trailing 12 months

    # Historical context
    avg_3yr: Optional[float] = None
    avg_5yr: Optional[float] = None
    min_historical: Optional[float] = None
    max_historical: Optional[float] = None
    historical_percentile: Optional[float] = None  # Where current sits in own history

    # Trend analysis
    trend_direction: TrendDirection = TrendDirection.INSUFFICIENT_DATA
    trend_score: Optional[float] = None  # 0-100, higher = improving faster
    yoy_change: Optional[float] = None  # Year-over-year change
    qoq_change: Optional[float] = None  # Quarter-over-quarter change
    cagr_3yr: Optional[float] = None  # 3-year CAGR

    # Volatility/stability
    volatility: Optional[float] = None  # Std dev of historical values
    stability_score: Optional[float] = None  # 0-100, higher = more stable

    # Peer comparison
    sector_percentile: Optional[float] = None  # 0-100
    sector_median: Optional[float] = None
    vs_sector_median: Optional[float] = None  # Distance from median
    industry_percentile: Optional[float] = None

    # Data quality
    data_points: int = 0
    data_quality_score: Optional[float] = None  # 0-1
    is_outlier: bool = False

    # Raw score (before combining with other metrics)
    raw_score: Optional[float] = None


@dataclass
class DimensionAnalysis:
    """Complete analysis for a dimension with multiple metrics."""

    company_id: str
    score_date: date
    dimension_code: str

    # Individual metric analyses
    metrics: Dict[str, MetricAnalysis] = field(default_factory=dict)

    # Composite scores
    raw_score: Optional[float] = None  # Based on absolute levels
    trend_score: Optional[float] = None  # Based on improvement
    peer_score: Optional[float] = None  # Based on sector comparison
    stability_score: Optional[float] = None  # Based on consistency

    # Final combined score
    composite_score: Optional[float] = None

    # Confidence
    data_quality: float = 0.0
    confidence: float = 0.0

    # Context
    sector: Optional[str] = None
    industry: Optional[str] = None
    currency: Optional[str] = None

    # Score ranges (uncertainty)
    score_low: Optional[float] = None
    score_high: Optional[float] = None


class HistoricalAnalyzer:
    """Analyze historical trends and patterns."""

    @staticmethod
    def calculate_trend(
        values: List[Tuple[date, float]],
        min_periods: int = 4
    ) -> Tuple[TrendDirection, Optional[float]]:
        """
        Calculate trend direction and score from time series.

        Args:
            values: List of (date, value) tuples, oldest first
            min_periods: Minimum data points required

        Returns:
            (TrendDirection, trend_score 0-100)
        """
        if len(values) < min_periods:
            return TrendDirection.INSUFFICIENT_DATA, None

        # Sort by date
        sorted_values = sorted(values, key=lambda x: x[0])
        y = np.array([v[1] for v in sorted_values])
        x = np.arange(len(y))

        # Calculate linear regression slope
        slope, intercept = np.polyfit(x, y, 1)

        # Normalize slope by mean value to get percentage change per period
        mean_val = np.mean(y)
        if mean_val == 0:
            return TrendDirection.STABLE, 50.0

        normalized_slope = slope / abs(mean_val)

        # Classify direction
        if normalized_slope > 0.05:  # >5% improvement per period
            direction = TrendDirection.STRONG_UP
        elif normalized_slope > 0.01:  # >1% improvement
            direction = TrendDirection.UP
        elif normalized_slope > -0.01:  # Stable
            direction = TrendDirection.STABLE
        elif normalized_slope > -0.05:  # Declining
            direction = TrendDirection.DOWN
        else:
            direction = TrendDirection.STRONG_DOWN

        # Convert to 0-100 score (50 = stable, 100 = strong improvement)
        # Cap at ±10% normalized slope for scoring
        capped_slope = max(-0.10, min(0.10, normalized_slope))
        trend_score = 50 + (capped_slope * 500)  # -10% → 0, +10% → 100

        return direction, round(trend_score, 1)

    @staticmethod
    def calculate_cagr(
        start_value: float,
        end_value: float,
        years: float
    ) -> Optional[float]:
        """Calculate Compound Annual Growth Rate."""
        if start_value <= 0 or end_value <= 0 or years <= 0:
            return None

        return (end_value / start_value) ** (1 / years) - 1

    @staticmethod
    def calculate_volatility(values: List[float]) -> Optional[float]:
        """Calculate coefficient of variation (normalized volatility)."""
        if len(values) < 3:
            return None

        mean_val = np.mean(values)
        if mean_val == 0:
            return None

        return np.std(values) / abs(mean_val)

    @staticmethod
    def calculate_stability_score(volatility: Optional[float]) -> Optional[float]:
        """
        Convert volatility to stability score (0-100).
        Lower volatility = higher stability score.
        """
        if volatility is None:
            return None

        # CoV of 0 = 100 score, CoV of 0.5+ = 0 score
        score = max(0, min(100, 100 - (volatility * 200)))
        return round(score, 1)


class PeerAnalyzer:
    """Analyze company metrics relative to peers."""

    @staticmethod
    def calculate_percentile(
        value: float,
        peer_values: List[float],
        higher_is_better: bool = True
    ) -> float:
        """
        Calculate percentile rank within peer group.

        Args:
            value: The company's value
            peer_values: Values for all peers (including the company)
            higher_is_better: If True, higher values get higher percentiles

        Returns:
            Percentile 0-100
        """
        if not peer_values or len(peer_values) < 2:
            return 50.0  # Default to median if no peers

        # Filter out None/NaN - handle both float and Decimal types
        def is_valid(v):
            if v is None:
                return False
            try:
                fv = float(v)
                return not np.isnan(fv)
            except (TypeError, ValueError):
                return False

        clean_peers = [float(v) for v in peer_values if is_valid(v)]

        if len(clean_peers) < 2:
            return 50.0

        value_float = float(value)
        sorted_peers = sorted(clean_peers)
        rank = sum(1 for v in sorted_peers if v < value_float)
        percentile = (rank / len(sorted_peers)) * 100

        if not higher_is_better:
            percentile = 100 - percentile

        return round(percentile, 1)

    @staticmethod
    def calculate_vs_median(
        value: float,
        peer_values: List[float]
    ) -> Optional[float]:
        """Calculate how far value is from peer median (as %)."""
        if not peer_values:
            return None

        # Filter out None/NaN - handle both float and Decimal types
        def is_valid(v):
            if v is None:
                return False
            try:
                fv = float(v)
                return not np.isnan(fv)
            except (TypeError, ValueError):
                return False

        clean_peers = [float(v) for v in peer_values if is_valid(v)]
        if not clean_peers:
            return None

        median = np.median(clean_peers)
        if median == 0:
            return None

        return (float(value) - median) / abs(median)


class QualityScorer:
    """Score data quality and detect issues."""

    @staticmethod
    def calculate_data_quality(
        data_points: int,
        expected_points: int = 12,  # 3 years of quarterly data
        has_recent_data: bool = True,
        days_since_update: int = 0
    ) -> float:
        """
        Calculate data quality score (0-1).

        Args:
            data_points: Number of historical periods available
            expected_points: Ideal number of periods
            has_recent_data: Whether most recent quarter is available
            days_since_update: Days since last data update
        """
        # Coverage score (0-0.5)
        coverage = min(1.0, data_points / expected_points) * 0.5

        # Recency score (0-0.3)
        recency = 0.3 if has_recent_data else 0.0
        if days_since_update > 180:
            recency *= 0.5  # Penalize stale data

        # Completeness bonus (0-0.2)
        if data_points >= expected_points:
            completeness = 0.2
        elif data_points >= expected_points * 0.75:
            completeness = 0.1
        else:
            completeness = 0.0

        return round(coverage + recency + completeness, 2)

    @staticmethod
    def detect_outlier(
        value: float,
        historical_values: List[float],
        threshold_std: float = 3.0
    ) -> bool:
        """Detect if value is a statistical outlier."""
        if len(historical_values) < 5:
            return False

        mean = np.mean(historical_values)
        std = np.std(historical_values)

        if std == 0:
            return False

        z_score = abs(value - mean) / std
        return z_score > threshold_std


class ScoreNormalizer:
    """Normalize raw metrics to 0-100 scores."""

    @staticmethod
    def normalize_metric(
        value: float,
        low_threshold: float,
        high_threshold: float,
        higher_is_better: bool = True,
        curve: str = "linear"  # "linear", "log", "sigmoid"
    ) -> float:
        """
        Normalize a metric value to 0-100 score.

        Args:
            value: The raw metric value
            low_threshold: Value that maps to score 0 (or 100 if higher_is_better=False)
            high_threshold: Value that maps to score 100 (or 0 if higher_is_better=False)
            higher_is_better: Whether higher values should get higher scores
            curve: Transformation curve type

        Returns:
            Score 0-100
        """
        # Handle edge cases
        if value is None or np.isnan(value):
            return 50.0  # Default to middle

        # Calculate position in range
        range_size = high_threshold - low_threshold
        if range_size == 0:
            return 50.0

        position = (value - low_threshold) / range_size

        # Apply curve transformation
        if curve == "log":
            # Logarithmic: more sensitive at low end
            position = np.log1p(position * 9) / np.log(10)
        elif curve == "sigmoid":
            # Sigmoid: S-curve, sensitive in middle
            position = 1 / (1 + np.exp(-10 * (position - 0.5)))
        # else: linear (no transformation)

        # Clamp to 0-1
        position = max(0, min(1, position))

        # Convert to 0-100
        score = position * 100

        # Invert if lower is better
        if not higher_is_better:
            score = 100 - score

        return round(score, 1)

    @staticmethod
    def combine_scores(
        scores: Dict[str, float],
        weights: Dict[str, float],
        min_score: float = 0.0,
        max_score: float = 100.0
    ) -> float:
        """
        Combine multiple scores with weights.

        Args:
            scores: Dict of score_name -> score_value
            weights: Dict of score_name -> weight (will be normalized)
            min_score: Minimum output score
            max_score: Maximum output score

        Returns:
            Weighted average score
        """
        if not scores:
            return (min_score + max_score) / 2

        # Only include scores that have weights
        valid_scores = {k: v for k, v in scores.items() if k in weights and v is not None}

        if not valid_scores:
            return (min_score + max_score) / 2

        # Normalize weights
        total_weight = sum(weights[k] for k in valid_scores)
        if total_weight == 0:
            return (min_score + max_score) / 2

        # Calculate weighted average
        weighted_sum = sum(valid_scores[k] * weights[k] for k in valid_scores)
        result = weighted_sum / total_weight

        # Clamp to range
        return round(max(min_score, min(max_score, result)), 1)


# Thresholds for common financial metrics (for normalization)
# These define what's "bad" (low_threshold) vs "excellent" (high_threshold)
METRIC_THRESHOLDS = {
    # Profitability (higher is better)
    "gross_margin": {"low": 0.10, "high": 0.60, "higher_is_better": True},
    "operating_margin": {"low": -0.05, "high": 0.25, "higher_is_better": True},
    "net_margin": {"low": -0.10, "high": 0.20, "higher_is_better": True},
    "ebitda_margin": {"low": 0.05, "high": 0.35, "higher_is_better": True},

    # Returns (higher is better)
    "roe": {"low": 0.0, "high": 0.25, "higher_is_better": True},
    "roa": {"low": 0.0, "high": 0.15, "higher_is_better": True},
    "roic": {"low": 0.0, "high": 0.20, "higher_is_better": True},

    # Growth (higher is better, but cap extremes)
    "revenue_growth_yoy": {"low": -0.10, "high": 0.30, "higher_is_better": True},
    "earnings_growth_yoy": {"low": -0.20, "high": 0.40, "higher_is_better": True},
    "revenue_growth": {"low": -0.10, "high": 0.30, "higher_is_better": True},
    "earnings_growth": {"low": -0.20, "high": 0.50, "higher_is_better": True},
    "operating_income_growth": {"low": -0.20, "high": 0.40, "higher_is_better": True},
    "ebitda_growth": {"low": -0.15, "high": 0.35, "higher_is_better": True},
    "debt_growth": {"low": -0.20, "high": 0.30, "higher_is_better": False},  # Lower debt growth is better

    # Financial health (varies)
    "current_ratio": {"low": 0.5, "high": 2.5, "higher_is_better": True},
    "quick_ratio": {"low": 0.3, "high": 2.0, "higher_is_better": True},
    "debt_to_equity": {"low": 0.0, "high": 2.0, "higher_is_better": False},
    "interest_coverage": {"low": 1.0, "high": 10.0, "higher_is_better": True},

    # Cash generation (higher is better)
    "fcf_margin": {"low": -0.05, "high": 0.20, "higher_is_better": True},
    "cash_conversion": {"low": 0.5, "high": 1.5, "higher_is_better": True},

    # Valuation (lower is better, but too low is suspicious)
    "pe_ratio": {"low": 5.0, "high": 40.0, "higher_is_better": False},
    "pb_ratio": {"low": 0.5, "high": 5.0, "higher_is_better": False},
    "ev_ebitda": {"low": 3.0, "high": 20.0, "higher_is_better": False},
}


def get_metric_threshold(metric_name: str) -> Dict[str, Any]:
    """Get normalization thresholds for a metric."""
    return METRIC_THRESHOLDS.get(
        metric_name,
        {"low": 0.0, "high": 1.0, "higher_is_better": True}
    )
