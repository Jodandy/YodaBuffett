"""
Growth Dimension Calculator

Measures how fast a company is growing its business.
This is a BUSINESS dimension (price-independent).

Metrics analyzed:
- Revenue growth (top-line expansion)
- Earnings growth (bottom-line expansion)
- Operating income growth (core business growth)
- EBITDA growth (cash-generating ability growth)
- Debt growth (financing quality indicator)

Growth Quality Analysis:
- YoY growth rates (most recent)
- 3-year CAGR (sustainability)
- Growth consistency (not one-time spikes)
- Organic vs acquisition indicators
- Debt-funded growth detection (revenue growth vs debt growth)

For ML/LLM integration:
- Growth quality score (revenue growth with margin preservation)
- Growth acceleration/deceleration patterns
- Sector-relative growth ranking
- Debt-to-growth ratio (is growth self-funded or leveraged?)
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
import numpy as np

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
class GrowthCalculator(BaseDimensionCalculator):
    """
    Sophisticated growth dimension calculator.

    Analyzes business growth with:
    - Multi-period CAGR calculations
    - Growth consistency scoring
    - Growth quality analysis (revenue + margin)
    - Sector-relative rankings
    - Acceleration/deceleration detection

    Growth quality is key for LLM reasoning:
    - High revenue growth + stable margins = quality growth
    - High revenue growth + declining margins = buying growth (risky)
    - Low revenue growth + improving margins = optimization phase
    """

    # Metric weights for composite score
    METRIC_WEIGHTS = {
        "revenue_growth": 0.35,      # Top-line is most important
        "earnings_growth": 0.25,     # Bottom-line matters
        "operating_income_growth": 0.25,  # Core business
        "ebitda_growth": 0.15,       # Cash generation growth
    }

    # Component weights for final score
    COMPONENT_WEIGHTS = {
        "raw_score": 0.35,           # Absolute growth level
        "trend_score": 0.20,         # Growth acceleration
        "peer_score": 0.25,          # Sector comparison
        "consistency_score": 0.15,   # Growth stability
        "quality_bonus": 0.05,       # Growth quality indicator
    }

    @property
    def dimension_code(self) -> str:
        return "growth"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Growth",
            description="Measures business growth rate, consistency, quality, and debt-funded growth detection",
            category="fundamental",
            data_sources=["financial_statements", "balance_sheet_data"],
            update_frequency="daily",
            version="2.1.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate growth dimension for a company."""

        # Get company info
        company_info = await self._get_company_info(company_id)
        if not company_info:
            logger.warning(f"Company not found: {company_id}")
            return None

        symbol = company_info["primary_ticker"]
        sector = company_info.get("sector")
        currency = company_info.get("report_currency", "SEK")

        # Get historical financial data (need more for growth calculations)
        financials = await self._get_historical_financials(symbol, score_date, years=5)
        if not financials or len(financials) < 4:
            logger.info(f"Insufficient financial data for growth analysis: {symbol}")
            return None

        # Get historical balance sheet data for debt growth analysis
        balance_sheets = await self._get_historical_balance_sheets(symbol, score_date, years=5)

        # Create analysis container
        analysis = DimensionAnalysis(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            sector=sector,
            currency=currency,
        )

        # Calculate growth metrics (including debt growth if balance sheet data available)
        growth_metrics = self._calculate_growth_metrics(financials, score_date, balance_sheets)

        # Analyze each metric
        for metric_name, values in growth_metrics.items():
            if values and metric_name in self.METRIC_WEIGHTS:
                metric_analysis = await self._analyze_growth_metric(
                    metric_name=metric_name,
                    values=values,
                    sector=sector,
                    score_date=score_date,
                )
                analysis.metrics[metric_name] = metric_analysis

        if len(analysis.metrics) < 2:
            logger.info(f"Insufficient growth metrics for {symbol}")
            return None

        # Calculate growth quality analysis (includes debt-funded growth detection)
        growth_quality = self._analyze_growth_quality(financials, growth_metrics)

        # Calculate component scores
        analysis.raw_score = self._calculate_raw_score(analysis.metrics)
        analysis.trend_score = self._calculate_trend_score(analysis.metrics)
        analysis.peer_score = self._calculate_peer_score(analysis.metrics)
        consistency_score = self._calculate_consistency_score(analysis.metrics)

        # Quality bonus based on growth quality
        quality_bonus = self._calculate_growth_quality_bonus(growth_quality)

        # Calculate composite score
        component_scores = {
            "raw_score": analysis.raw_score,
            "trend_score": analysis.trend_score,
            "peer_score": analysis.peer_score,
            "consistency_score": consistency_score,
            "quality_bonus": quality_bonus,
        }
        analysis.composite_score = ScoreNormalizer.combine_scores(
            component_scores, self.COMPONENT_WEIGHTS
        )

        # Store stability score for metadata
        analysis.stability_score = consistency_score

        # Calculate confidence
        analysis.data_quality = self._calculate_data_quality(analysis.metrics)
        analysis.confidence = analysis.data_quality

        # Score range
        uncertainty = (1 - analysis.confidence) * 15
        analysis.score_low = max(0, analysis.composite_score - uncertainty)
        analysis.score_high = min(100, analysis.composite_score + uncertainty)

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=analysis.composite_score,
            confidence=analysis.confidence,
            data_quality=analysis.data_quality,
            percentile_rank=None,
            score_low=analysis.score_low,
            score_high=analysis.score_high,
            metadata=self._build_metadata(analysis, company_info, growth_quality),
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
                net_income,
                operating_income,
                gross_profit,
                ebitda,
                currency
            FROM financial_statements
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $3
            ORDER BY period_date DESC
        """, symbol, score_date, start_date)

        return [dict(row) for row in rows]

    async def _get_historical_balance_sheets(
        self,
        symbol: str,
        score_date: date,
        years: int = 5
    ) -> List[Dict]:
        """Get historical balance sheet data for debt growth analysis."""
        start_date = score_date - timedelta(days=years * 365)

        rows = await self.db_conn.fetch("""
            SELECT
                period_date,
                total_debt,
                total_equity,
                total_assets
            FROM balance_sheet_data
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $3
            ORDER BY period_date DESC
        """, symbol, score_date, start_date)

        return [dict(row) for row in rows]

    def _calculate_growth_metrics(
        self,
        financials: List[Dict],
        score_date: date,
        balance_sheets: Optional[List[Dict]] = None
    ) -> Dict[str, List[Tuple[date, float]]]:
        """Calculate YoY growth rates for each metric including debt growth."""

        # Sort by date descending
        sorted_fin = sorted(financials, key=lambda x: x["period_date"], reverse=True)

        # Build time series for each metric
        metrics = {
            "revenue_growth": [],
            "earnings_growth": [],
            "operating_income_growth": [],
            "ebitda_growth": [],
            "debt_growth": [],  # Important for detecting debt-funded growth
        }

        # Index balance sheets by period for debt growth calculation
        bs_by_period = {}
        if balance_sheets:
            for bs in balance_sheets:
                bs_by_period[bs["period_date"]] = bs

        # For each period, calculate YoY growth
        for i, fin in enumerate(sorted_fin):
            period = fin["period_date"]

            # Find same period from previous year (approximately 1 year ago)
            prior_year = None
            for j in range(i + 1, len(sorted_fin)):
                days_diff = (period - sorted_fin[j]["period_date"]).days
                if 300 <= days_diff <= 400:  # Roughly 1 year
                    prior_year = sorted_fin[j]
                    break

            if not prior_year:
                continue

            # Revenue growth
            rev_curr = fin.get("total_revenue")
            rev_prior = prior_year.get("total_revenue")
            if rev_curr and rev_prior and rev_prior != 0:
                growth = (rev_curr - rev_prior) / abs(rev_prior)
                if -2 <= growth <= 5:  # Cap extreme values
                    metrics["revenue_growth"].append((period, growth))

            # Earnings growth
            earn_curr = fin.get("net_income")
            earn_prior = prior_year.get("net_income")
            if earn_curr and earn_prior and earn_prior != 0:
                # Handle sign changes carefully
                if earn_prior > 0:
                    growth = (earn_curr - earn_prior) / abs(earn_prior)
                else:
                    # Negative base: improvement is getting less negative
                    growth = (earn_curr - earn_prior) / abs(earn_prior)
                if -3 <= growth <= 10:
                    metrics["earnings_growth"].append((period, growth))

            # Operating income growth
            op_curr = fin.get("operating_income")
            op_prior = prior_year.get("operating_income")
            if op_curr and op_prior and op_prior != 0:
                if op_prior > 0:
                    growth = (op_curr - op_prior) / abs(op_prior)
                else:
                    growth = (op_curr - op_prior) / abs(op_prior)
                if -3 <= growth <= 10:
                    metrics["operating_income_growth"].append((period, growth))

            # EBITDA growth
            ebitda_curr = fin.get("ebitda")
            ebitda_prior = prior_year.get("ebitda")
            if ebitda_curr and ebitda_prior and ebitda_prior != 0:
                if ebitda_prior > 0:
                    growth = (ebitda_curr - ebitda_prior) / abs(ebitda_prior)
                else:
                    growth = (ebitda_curr - ebitda_prior) / abs(ebitda_prior)
                if -3 <= growth <= 10:
                    metrics["ebitda_growth"].append((period, growth))

            # Debt growth - crucial for detecting debt-funded growth
            if bs_by_period:
                # Find balance sheet for current period (within 60 days)
                bs_curr = None
                bs_prior = None
                for bs_period, bs_data in bs_by_period.items():
                    if abs((bs_period - period).days) <= 60:
                        bs_curr = bs_data
                    prior_period = prior_year.get("period_date")
                    if prior_period and abs((bs_period - prior_period).days) <= 60:
                        bs_prior = bs_data

                if bs_curr and bs_prior:
                    debt_curr = bs_curr.get("total_debt")
                    debt_prior = bs_prior.get("total_debt")
                    if debt_curr is not None and debt_prior is not None and debt_prior > 0:
                        debt_growth = (float(debt_curr) - float(debt_prior)) / float(debt_prior)
                        if -1 <= debt_growth <= 5:  # Cap extreme values
                            metrics["debt_growth"].append((period, debt_growth))

        return metrics

    def _analyze_growth_quality(
        self,
        financials: List[Dict],
        growth_metrics: Optional[Dict[str, List[Tuple[date, float]]]] = None
    ) -> Dict[str, Any]:
        """
        Analyze growth quality - is the company growing sustainably?

        Quality indicators:
        - Revenue growth with stable/improving margins = quality growth
        - Revenue growth with declining margins = buying growth
        - Revenue growth funded by debt = debt-funded growth (risky)
        - Consistent growth over multiple periods = sustainable
        - Erratic growth = one-time events
        """
        if len(financials) < 4:
            return {"quality_score": 50.0, "interpretation": "insufficient_data"}

        sorted_fin = sorted(financials, key=lambda x: x["period_date"], reverse=True)

        # Get recent trends
        revenues = []
        margins = []

        for fin in sorted_fin[:8]:  # Last 8 periods
            rev = fin.get("total_revenue")
            ni = fin.get("net_income")
            if rev and rev > 0:
                revenues.append(rev)
                if ni is not None:
                    margins.append(ni / rev)

        if len(revenues) < 4:
            return {"quality_score": 50.0, "interpretation": "insufficient_data"}

        # Calculate revenue CAGR
        if revenues[-1] > 0:
            years = len(revenues) / 4  # Assuming quarterly
            rev_cagr = (revenues[0] / revenues[-1]) ** (1 / years) - 1 if years > 0 else 0
        else:
            rev_cagr = 0

        # Calculate margin trend
        if len(margins) >= 4:
            margin_trend = (margins[0] - margins[-1])  # Positive = improving margins
        else:
            margin_trend = 0

        # Analyze debt-funded growth
        debt_funded_growth = False
        debt_to_revenue_growth_ratio = None
        avg_debt_growth = None
        avg_rev_growth = None

        if growth_metrics:
            debt_growth_values = growth_metrics.get("debt_growth", [])
            rev_growth_values = growth_metrics.get("revenue_growth", [])

            if debt_growth_values and rev_growth_values:
                # Calculate average growth rates
                avg_debt_growth = sum(v[1] for v in debt_growth_values) / len(debt_growth_values)
                avg_rev_growth = sum(v[1] for v in rev_growth_values) / len(rev_growth_values)

                # If debt is growing significantly faster than revenue, it's debt-funded growth
                if avg_rev_growth > 0.01:  # Only if there's meaningful revenue growth
                    debt_to_revenue_growth_ratio = avg_debt_growth / avg_rev_growth if avg_rev_growth != 0 else None

                    # Flag if debt growing >1.5x faster than revenue
                    if debt_to_revenue_growth_ratio and debt_to_revenue_growth_ratio > 1.5:
                        debt_funded_growth = True
                elif avg_debt_growth > 0.10:  # Growing debt >10% with minimal revenue growth
                    debt_funded_growth = True

        # Determine growth quality category
        if debt_funded_growth:
            quality_category = "debt_funded_growth"
            quality_score = 35.0
            interpretation = "Growth appears funded by increasing debt - unsustainable pattern, high risk"
        elif rev_cagr > 0.10 and margin_trend >= 0:
            quality_category = "profitable_growth"
            quality_score = 85.0
            interpretation = "Strong revenue growth with stable/improving margins - high quality growth"
        elif rev_cagr > 0.10 and margin_trend < -0.03:
            quality_category = "buying_growth"
            quality_score = 45.0
            interpretation = "Revenue growth but declining margins - potentially buying growth at expense of profitability"
        elif rev_cagr > 0.05 and margin_trend > 0.02:
            quality_category = "balanced_growth"
            quality_score = 75.0
            interpretation = "Moderate revenue growth with margin expansion - balanced quality growth"
        elif rev_cagr < 0.02 and margin_trend > 0.02:
            quality_category = "optimization"
            quality_score = 60.0
            interpretation = "Low revenue growth but margin improvement - optimization/efficiency phase"
        elif rev_cagr < 0:
            quality_category = "declining"
            quality_score = 25.0
            interpretation = "Revenue decline - business contraction"
        else:
            quality_category = "stable"
            quality_score = 55.0
            interpretation = "Modest growth with stable margins - mature business pattern"

        # Growth consistency (volatility of growth rates)
        if len(revenues) >= 4:
            growth_rates = [(revenues[i] - revenues[i+1]) / abs(revenues[i+1])
                          for i in range(len(revenues)-1) if revenues[i+1] != 0]
            if growth_rates:
                consistency = 1 - min(1, np.std(growth_rates))  # Lower volatility = higher consistency
            else:
                consistency = 0.5
        else:
            consistency = 0.5

        return {
            "quality_category": quality_category,
            "quality_score": quality_score,
            "interpretation": interpretation,
            "revenue_cagr": round(rev_cagr, 4),
            "margin_trend": round(margin_trend, 4) if margin_trend else None,
            "growth_consistency": round(consistency, 2),
            # Debt-funded growth analysis
            "debt_funded_growth": debt_funded_growth,
            "avg_debt_growth": round(avg_debt_growth, 4) if avg_debt_growth is not None else None,
            "avg_revenue_growth": round(avg_rev_growth, 4) if avg_rev_growth is not None else None,
            "debt_to_revenue_growth_ratio": round(debt_to_revenue_growth_ratio, 2) if debt_to_revenue_growth_ratio is not None else None,
        }

    async def _analyze_growth_metric(
        self,
        metric_name: str,
        values: List[Tuple[date, float]],
        sector: Optional[str],
        score_date: date,
    ) -> MetricAnalysis:
        """Analyze a growth metric."""

        analysis = MetricAnalysis()
        analysis.data_points = len(values)

        if not values:
            return analysis

        # Sort by date (newest first)
        sorted_values = sorted(values, key=lambda x: x[0], reverse=True)
        raw_values = [v[1] for v in sorted_values]

        # Current (most recent YoY growth)
        analysis.current = raw_values[0]

        # Average growth rates
        if len(raw_values) >= 2:
            analysis.ttm = sum(raw_values[:min(4, len(raw_values))]) / min(4, len(raw_values))
        if len(raw_values) >= 4:
            analysis.avg_3yr = sum(raw_values) / len(raw_values)

        analysis.min_historical = min(raw_values)
        analysis.max_historical = max(raw_values)

        # Historical percentile
        if len(raw_values) >= 4:
            analysis.historical_percentile = PeerAnalyzer.calculate_percentile(
                analysis.current, raw_values, higher_is_better=True
            )

        # Growth acceleration/deceleration (trend of growth)
        if len(values) >= 3:
            trend_values = sorted(values, key=lambda x: x[0])
            analysis.trend_direction, analysis.trend_score = \
                HistoricalAnalyzer.calculate_trend(trend_values)

        # Volatility of growth (consistency)
        if len(raw_values) >= 3:
            analysis.volatility = HistoricalAnalyzer.calculate_volatility(raw_values)
            analysis.stability_score = HistoricalAnalyzer.calculate_stability_score(
                analysis.volatility
            )

        # Peer comparison
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

        # Normalize to raw score (using growth thresholds)
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
            expected_points=8,  # 2 years of quarterly YoY growth
            has_recent_data=True,
        )

        return analysis

    async def _get_sector_peer_values(
        self,
        metric_name: str,
        sector: str,
        score_date: date
    ) -> List[float]:
        """Get latest growth values for sector peers."""

        # Calculate YoY revenue growth for peers
        if metric_name == "revenue_growth":
            query = """
                WITH latest_two_years AS (
                    SELECT
                        fs.symbol,
                        fs.period_date,
                        fs.total_revenue,
                        LAG(fs.total_revenue, 4) OVER (PARTITION BY fs.symbol ORDER BY fs.period_date) as prior_revenue
                    FROM financial_statements fs
                    JOIN company_master cm ON fs.symbol = cm.primary_ticker
                    WHERE cm.sector = $1
                    AND fs.period_date <= $2
                    AND fs.total_revenue > 0
                ),
                latest_growth AS (
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        (total_revenue - prior_revenue)::float / NULLIF(prior_revenue, 0) as growth_rate
                    FROM latest_two_years
                    WHERE prior_revenue IS NOT NULL AND prior_revenue > 0
                    ORDER BY symbol, period_date DESC
                )
                SELECT growth_rate FROM latest_growth
                WHERE growth_rate BETWEEN -1 AND 2
            """
        elif metric_name == "earnings_growth":
            query = """
                WITH latest_two_years AS (
                    SELECT
                        fs.symbol,
                        fs.period_date,
                        fs.net_income,
                        LAG(fs.net_income, 4) OVER (PARTITION BY fs.symbol ORDER BY fs.period_date) as prior_income
                    FROM financial_statements fs
                    JOIN company_master cm ON fs.symbol = cm.primary_ticker
                    WHERE cm.sector = $1
                    AND fs.period_date <= $2
                    AND fs.net_income IS NOT NULL
                ),
                latest_growth AS (
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        (net_income - prior_income)::float / NULLIF(ABS(prior_income), 0) as growth_rate
                    FROM latest_two_years
                    WHERE prior_income IS NOT NULL AND prior_income != 0
                    ORDER BY symbol, period_date DESC
                )
                SELECT growth_rate FROM latest_growth
                WHERE growth_rate BETWEEN -2 AND 5
            """
        else:
            return []

        rows = await self.db_conn.fetch(query, sector, score_date)
        return [row["growth_rate"] for row in rows]

    def _calculate_raw_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted raw score from all metrics."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.raw_score is not None:
                scores[name] = analysis.raw_score
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_trend_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate growth acceleration/deceleration score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.trend_score is not None:
                scores[name] = analysis.trend_score
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_peer_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate sector peer comparison score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.sector_percentile is not None:
                scores[name] = analysis.sector_percentile
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_consistency_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate growth consistency score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.stability_score is not None:
                scores[name] = analysis.stability_score
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_growth_quality_bonus(self, growth_quality: Dict) -> float:
        """Get the quality bonus from growth quality analysis."""
        return growth_quality.get("quality_score", 50.0)

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
        company_info: Dict,
        growth_quality: Dict
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
                "consistency": analysis.stability_score,
            },
            "growth_quality": growth_quality,  # Rich data for LLM reasoning
            "metrics": {},
        }

        for name, m in analysis.metrics.items():
            metadata["metrics"][name] = {
                "current_yoy": round(m.current, 4) if m.current else None,
                "avg_growth": round(m.avg_3yr, 4) if m.avg_3yr else None,
                "trend": m.trend_direction.value if m.trend_direction else None,
                "trend_score": m.trend_score,
                "sector_percentile": m.sector_percentile,
                "vs_sector_median": round(m.vs_sector_median, 4) if m.vs_sector_median else None,
                "consistency": m.stability_score,
                "raw_score": m.raw_score,
                "data_points": m.data_points,
            }

        return metadata
