"""
Profitability Dimension Calculator

Measures how efficiently a company converts revenue into profit.
This is a BUSINESS dimension (price-independent).

Metrics analyzed:
- Gross margin (revenue quality, pricing power)
- Operating margin (operational efficiency)
- Net margin (bottom-line profitability)
- EBITDA margin (cash-generating ability)

Each metric is analyzed for:
- Current level (vs thresholds)
- Historical trend (improving/declining)
- Peer comparison (vs sector)
- Stability (consistency over time)
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging

from .base import BaseDimensionCalculator, register_calculator
from .analysis_helpers import (
    MetricAnalysis,
    DimensionAnalysis,
    HistoricalAnalyzer,
    PeerAnalyzer,
    QualityScorer,
    ScoreNormalizer,
    TrendDirection,
    get_metric_threshold,
)
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class ProfitabilityCalculator(BaseDimensionCalculator):
    """
    Sophisticated profitability dimension calculator.

    Analyzes multiple margin metrics with:
    - Multi-period historical analysis
    - Trend detection and scoring
    - Sector-relative comparisons
    - Stability/consistency scoring
    - Quality-adjusted confidence
    """

    # Metric weights for composite score
    METRIC_WEIGHTS = {
        "gross_margin": 0.20,
        "operating_margin": 0.30,
        "net_margin": 0.25,
        "ebitda_margin": 0.25,
    }

    # Component weights for final score
    COMPONENT_WEIGHTS = {
        "raw_score": 0.40,      # Absolute level matters most
        "trend_score": 0.25,    # Improvement trajectory
        "peer_score": 0.25,     # Sector comparison
        "stability_score": 0.10,  # Consistency bonus
    }

    @property
    def dimension_code(self) -> str:
        return "profitability"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Profitability",
            description="Measures profit margin quality, trends, and consistency relative to peers",
            category="fundamental",
            data_sources=["financial_statements"],
            update_frequency="daily",
            version="2.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate profitability dimension for a company."""

        # Get company info (sector, ticker, currency)
        company_info = await self._get_company_info(company_id)
        if not company_info:
            logger.warning(f"Company not found: {company_id}")
            return None

        symbol = company_info["primary_ticker"]
        sector = company_info.get("sector")
        currency = company_info.get("report_currency", "SEK")

        # Get historical financial data (up to 5 years)
        financials = await self._get_historical_financials(symbol, score_date, years=5)
        if not financials or len(financials) < 2:
            logger.info(f"Insufficient financial data for {symbol}")
            return None

        # Analyze each margin metric
        analysis = DimensionAnalysis(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            sector=sector,
            currency=currency,
        )

        # Calculate all margin metrics
        margin_metrics = {
            "gross_margin": self._calc_margin(financials, "gross_profit", "total_revenue"),
            "operating_margin": self._calc_margin(financials, "operating_income", "total_revenue"),
            "net_margin": self._calc_margin(financials, "net_income", "total_revenue"),
            "ebitda_margin": self._calc_margin(financials, "ebitda", "total_revenue"),
        }

        # Analyze each metric
        for metric_name, values in margin_metrics.items():
            if values:
                metric_analysis = await self._analyze_metric(
                    metric_name=metric_name,
                    values=values,
                    sector=sector,
                    score_date=score_date,
                )
                analysis.metrics[metric_name] = metric_analysis

        # Skip if not enough metrics analyzed
        if len(analysis.metrics) < 2:
            logger.info(f"Insufficient margin data for {symbol}")
            return None

        # Calculate component scores
        analysis.raw_score = self._calculate_raw_score(analysis.metrics)
        analysis.trend_score = self._calculate_trend_score(analysis.metrics)
        analysis.peer_score = self._calculate_peer_score(analysis.metrics)
        analysis.stability_score = self._calculate_stability_score(analysis.metrics)

        # Calculate composite score
        component_scores = {
            "raw_score": analysis.raw_score,
            "trend_score": analysis.trend_score,
            "peer_score": analysis.peer_score,
            "stability_score": analysis.stability_score,
        }
        analysis.composite_score = ScoreNormalizer.combine_scores(
            component_scores, self.COMPONENT_WEIGHTS
        )

        # Calculate confidence based on data quality
        analysis.data_quality = self._calculate_data_quality(analysis.metrics)
        analysis.confidence = analysis.data_quality

        # Calculate score range (uncertainty)
        uncertainty = (1 - analysis.confidence) * 15  # Max ±15 points if low confidence
        analysis.score_low = max(0, analysis.composite_score - uncertainty)
        analysis.score_high = min(100, analysis.composite_score + uncertainty)

        # Build the DimensionScore
        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=analysis.composite_score,
            confidence=analysis.confidence,
            data_quality=analysis.data_quality,
            percentile_rank=None,  # Calculated in batch
            score_low=analysis.score_low,
            score_high=analysis.score_high,
            metadata=self._build_metadata(analysis, company_info),
        )

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        """Get company info from company_master."""
        row = await self.db_conn.fetchrow("""
            SELECT
                id, company_name, primary_ticker, yahoo_symbol,
                sector, industry, report_currency, country
            FROM company_master
            WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_historical_financials(
        self,
        symbol: str,
        score_date: date,
        years: int = 5
    ) -> List[Dict]:
        """Get historical financial statements."""
        start_date = score_date - timedelta(days=years * 365)

        rows = await self.db_conn.fetch("""
            SELECT
                period_date,
                statement_type,
                total_revenue,
                gross_profit,
                operating_income,
                net_income,
                ebitda,
                currency
            FROM financial_statements
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $3
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """, symbol, score_date, start_date)

        return [dict(row) for row in rows]

    def _calc_margin(
        self,
        financials: List[Dict],
        numerator_field: str,
        denominator_field: str
    ) -> List[Tuple[date, float]]:
        """Calculate margin ratio from financials."""
        values = []
        for f in financials:
            num = f.get(numerator_field)
            denom = f.get(denominator_field)
            if num is not None and denom is not None and denom != 0:
                margin = float(num) / float(denom)
                values.append((f["period_date"], margin))
        return values

    async def _analyze_metric(
        self,
        metric_name: str,
        values: List[Tuple[date, float]],
        sector: Optional[str],
        score_date: date,
    ) -> MetricAnalysis:
        """Perform full analysis on a single metric."""

        analysis = MetricAnalysis()
        analysis.data_points = len(values)

        if not values:
            return analysis

        # Sort by date (newest first)
        sorted_values = sorted(values, key=lambda x: x[0], reverse=True)
        raw_values = [v[1] for v in sorted_values]

        # Current and TTM
        analysis.current = raw_values[0]
        if len(raw_values) >= 4:
            analysis.ttm = sum(raw_values[:4]) / 4

        # Historical stats
        if len(raw_values) >= 4:
            analysis.avg_3yr = sum(raw_values[:min(12, len(raw_values))]) / min(12, len(raw_values))
        if len(raw_values) >= 8:
            analysis.avg_5yr = sum(raw_values) / len(raw_values)

        analysis.min_historical = min(raw_values)
        analysis.max_historical = max(raw_values)

        # Where does current sit in own history?
        if len(raw_values) >= 4:
            analysis.historical_percentile = PeerAnalyzer.calculate_percentile(
                analysis.current, raw_values, higher_is_better=True
            )

        # Trend analysis
        if len(values) >= 4:
            # Use oldest-first for trend
            trend_values = sorted(values, key=lambda x: x[0])
            analysis.trend_direction, analysis.trend_score = \
                HistoricalAnalyzer.calculate_trend(trend_values)

        # YoY change (if we have data from ~1 year ago)
        if len(sorted_values) >= 4:
            current_val = sorted_values[0][1]
            # Find value from ~1 year ago
            for d, v in sorted_values:
                days_ago = (score_date - d).days
                if 300 <= days_ago <= 400:
                    analysis.yoy_change = (current_val - v) / abs(v) if v != 0 else None
                    break

        # Volatility and stability
        if len(raw_values) >= 4:
            analysis.volatility = HistoricalAnalyzer.calculate_volatility(raw_values)
            analysis.stability_score = HistoricalAnalyzer.calculate_stability_score(
                analysis.volatility
            )

        # Peer comparison (if sector available)
        if sector:
            peer_values = await self._get_sector_peer_values(metric_name, sector, score_date)
            if peer_values and len(peer_values) >= 5:
                analysis.sector_percentile = PeerAnalyzer.calculate_percentile(
                    analysis.current, peer_values, higher_is_better=True
                )
                analysis.sector_median = float(sorted(peer_values)[len(peer_values) // 2])
                analysis.vs_sector_median = PeerAnalyzer.calculate_vs_median(
                    analysis.current, peer_values
                )

        # Normalize to raw score
        thresholds = get_metric_threshold(metric_name)
        if analysis.current is not None:
            analysis.raw_score = ScoreNormalizer.normalize_metric(
                analysis.current,
                thresholds["low"],
                thresholds["high"],
                thresholds["higher_is_better"],
            )

        # Data quality
        analysis.data_quality_score = QualityScorer.calculate_data_quality(
            data_points=len(values),
            expected_points=12,
            has_recent_data=True,  # Already filtered by score_date
        )

        # Outlier detection
        if len(raw_values) >= 5:
            analysis.is_outlier = QualityScorer.detect_outlier(
                analysis.current, raw_values[1:]
            )

        return analysis

    async def _get_sector_peer_values(
        self,
        metric_name: str,
        sector: str,
        score_date: date
    ) -> List[float]:
        """Get latest metric values for all companies in sector."""

        # Map metric name to SQL calculation
        metric_sql = {
            "gross_margin": "fs.gross_profit::float / NULLIF(fs.total_revenue, 0)",
            "operating_margin": "fs.operating_income::float / NULLIF(fs.total_revenue, 0)",
            "net_margin": "fs.net_income::float / NULLIF(fs.total_revenue, 0)",
            "ebitda_margin": "fs.ebitda::float / NULLIF(fs.total_revenue, 0)",
        }.get(metric_name)

        if not metric_sql:
            return []

        rows = await self.db_conn.fetch(f"""
            WITH latest_financials AS (
                SELECT DISTINCT ON (fs.symbol)
                    fs.symbol,
                    {metric_sql} as metric_value
                FROM financial_statements fs
                JOIN company_master cm ON fs.symbol = cm.primary_ticker
                WHERE cm.sector = $1
                AND fs.period_date <= $2
                AND fs.total_revenue > 0
                AND fs.statement_type = 'annual'
                ORDER BY fs.symbol, fs.period_date DESC
            )
            SELECT metric_value FROM latest_financials
            WHERE metric_value IS NOT NULL
            AND metric_value BETWEEN -1 AND 2
        """, sector, score_date)

        return [row["metric_value"] for row in rows]

    def _calculate_raw_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted raw score from all metrics."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.raw_score is not None:
                scores[name] = analysis.raw_score
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_trend_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted trend score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.trend_score is not None:
                scores[name] = analysis.trend_score
        if not scores:
            return 50.0  # Neutral if no trend data
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_peer_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted peer comparison score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.sector_percentile is not None:
                scores[name] = analysis.sector_percentile
        if not scores:
            return 50.0  # Neutral if no peer data
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_stability_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted stability score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.stability_score is not None:
                scores[name] = analysis.stability_score
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_data_quality(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate overall data quality score."""
        qualities = [
            m.data_quality_score for m in metrics.values()
            if m.data_quality_score is not None
        ]
        if not qualities:
            return 0.5
        return sum(qualities) / len(qualities)

    def _build_metadata(
        self,
        analysis: DimensionAnalysis,
        company_info: Dict
    ) -> Dict[str, Any]:
        """Build detailed metadata for the score."""

        metadata = {
            "dimension_version": "2.0.0",
            "company_name": company_info.get("company_name"),
            "sector": analysis.sector,
            "currency": analysis.currency,
            "component_scores": {
                "raw": analysis.raw_score,
                "trend": analysis.trend_score,
                "peer": analysis.peer_score,
                "stability": analysis.stability_score,
            },
            "metrics": {},
        }

        # Add per-metric details
        for name, m in analysis.metrics.items():
            metadata["metrics"][name] = {
                "current": round(m.current, 4) if m.current else None,
                "ttm": round(m.ttm, 4) if m.ttm else None,
                "avg_3yr": round(m.avg_3yr, 4) if m.avg_3yr else None,
                "trend": m.trend_direction.value if m.trend_direction else None,
                "trend_score": m.trend_score,
                "sector_percentile": m.sector_percentile,
                "vs_sector_median": round(m.vs_sector_median, 4) if m.vs_sector_median else None,
                "stability": m.stability_score,
                "raw_score": m.raw_score,
                "data_points": m.data_points,
            }

        return metadata
