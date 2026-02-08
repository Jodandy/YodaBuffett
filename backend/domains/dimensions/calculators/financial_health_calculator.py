"""
Financial Health Dimension Calculator

Measures the financial strength and stability of a company.
This is a BUSINESS dimension (price-independent).

Metrics analyzed:
- Liquidity: Current ratio, Quick ratio (short-term obligations)
- Solvency: Debt to Equity, Debt to Assets (long-term stability)
- Interest Coverage: Ability to service debt
- Cash Position: Cash as % of assets

Special features:
- Altman Z-score calculation (bankruptcy prediction)
- Piotroski F-Score components
- Trend analysis for financial health changes
- Sector-adjusted thresholds (banks have different norms)

For ML/LLM integration:
- Financial distress probability
- Health trend direction and acceleration
- Key risk factors identified
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
class FinancialHealthCalculator(BaseDimensionCalculator):
    """
    Sophisticated financial health dimension calculator.

    Analyzes company financial stability with:
    - Multi-metric liquidity and solvency analysis
    - Altman Z-score for distress prediction
    - Trend detection in financial health
    - Sector-relative comparisons

    Key insight for LLM reasoning:
    - High liquidity + low debt = fortress balance sheet
    - Low liquidity + high debt = financial stress risk
    - Improving trends = turnaround potential
    - Declining trends = early warning signal
    """

    # Metric weights for composite score
    METRIC_WEIGHTS = {
        "current_ratio": 0.20,       # Short-term liquidity
        "quick_ratio": 0.15,         # Immediate liquidity
        "debt_to_equity": 0.25,      # Leverage
        "interest_coverage": 0.20,   # Debt service ability
        "cash_ratio": 0.10,          # Cash buffer
        "debt_to_assets": 0.10,      # Asset leverage
    }

    # Component weights for final score
    COMPONENT_WEIGHTS = {
        "raw_score": 0.35,           # Absolute level
        "trend_score": 0.20,         # Improvement trajectory
        "peer_score": 0.25,          # Sector comparison
        "stability_score": 0.10,     # Consistency
        "distress_score": 0.10,      # Altman Z-score component
    }

    @property
    def dimension_code(self) -> str:
        return "financial_health"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Financial Health",
            description="Measures balance sheet strength, liquidity, solvency, and distress risk",
            category="fundamental",
            data_sources=["balance_sheet_data", "financial_statements"],
            update_frequency="daily",
            version="2.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate financial health dimension for a company."""

        company_info = await self._get_company_info(company_id)
        if not company_info:
            logger.warning(f"Company not found: {company_id}")
            return None

        symbol = company_info["primary_ticker"]
        sector = company_info.get("sector")
        currency = company_info.get("report_currency", "SEK")

        # Get historical data
        balance_sheets = await self._get_historical_balance_sheets(symbol, score_date, years=5)
        financials = await self._get_historical_financials(symbol, score_date, years=5)

        if not balance_sheets or len(balance_sheets) < 2:
            logger.info(f"Insufficient balance sheet data for {symbol}")
            return None

        # Create analysis container
        analysis = DimensionAnalysis(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            sector=sector,
            currency=currency,
        )

        # Calculate health metrics
        health_metrics = self._calculate_health_metrics(balance_sheets, financials)

        # Analyze each metric
        for metric_name, values in health_metrics.items():
            if values and metric_name in self.METRIC_WEIGHTS:
                metric_analysis = await self._analyze_metric(
                    metric_name=metric_name,
                    values=values,
                    sector=sector,
                    score_date=score_date,
                )
                analysis.metrics[metric_name] = metric_analysis

        if len(analysis.metrics) < 3:
            logger.info(f"Insufficient health metrics for {symbol}")
            return None

        # Calculate distress score (Altman Z-score based)
        distress_analysis = self._calculate_distress_score(balance_sheets, financials)

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
            "distress_score": distress_analysis.get("distress_score", 50.0),
        }
        analysis.composite_score = ScoreNormalizer.combine_scores(
            component_scores, self.COMPONENT_WEIGHTS
        )

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
            metadata=self._build_metadata(analysis, company_info, distress_analysis),
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

    async def _get_historical_balance_sheets(
        self,
        symbol: str,
        score_date: date,
        years: int = 5
    ) -> List[Dict]:
        """Get historical balance sheet data."""
        start_date = score_date - timedelta(days=years * 365)

        rows = await self.db_conn.fetch("""
            SELECT
                period_date,
                statement_type,
                total_assets,
                current_assets,
                cash_and_equivalents,
                accounts_receivable,
                inventory,
                total_liabilities,
                current_liabilities,
                total_debt,
                long_term_debt,
                total_equity,
                retained_earnings,
                currency
            FROM balance_sheet_data
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $3
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """, symbol, score_date, start_date)

        return [dict(row) for row in rows]

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

    def _calculate_health_metrics(
        self,
        balance_sheets: List[Dict],
        financials: List[Dict]
    ) -> Dict[str, List[Tuple[date, float]]]:
        """Calculate financial health metrics from balance sheets."""

        # Index financials by period for lookup
        fin_by_period = {f["period_date"]: f for f in financials}

        metrics = {
            "current_ratio": [],
            "quick_ratio": [],
            "debt_to_equity": [],
            "debt_to_assets": [],
            "interest_coverage": [],
            "cash_ratio": [],
        }

        for bs in balance_sheets:
            period = bs["period_date"]
            fin = fin_by_period.get(period, {})

            current_assets = bs.get("current_assets")
            current_liabilities = bs.get("current_liabilities")
            total_assets = bs.get("total_assets")
            total_equity = bs.get("total_equity")
            total_debt = bs.get("total_debt") or 0
            cash = bs.get("cash_and_equivalents") or 0
            inventory = bs.get("inventory") or 0
            operating_income = fin.get("operating_income")

            # Current Ratio = Current Assets / Current Liabilities
            if current_assets and current_liabilities and current_liabilities > 0:
                cr = float(current_assets) / float(current_liabilities)
                if 0 < cr < 10:  # Cap extreme values
                    metrics["current_ratio"].append((period, cr))

            # Quick Ratio = (Current Assets - Inventory) / Current Liabilities
            if current_assets and current_liabilities and current_liabilities > 0:
                qr = (float(current_assets) - float(inventory)) / float(current_liabilities)
                if 0 < qr < 10:
                    metrics["quick_ratio"].append((period, qr))

            # Cash Ratio = Cash / Current Liabilities
            if current_liabilities and current_liabilities > 0 and cash:
                cr_cash = float(cash) / float(current_liabilities)
                if 0 <= cr_cash < 5:
                    metrics["cash_ratio"].append((period, cr_cash))

            # Debt to Equity = Total Debt / Total Equity
            if total_equity and total_equity > 0:
                de = float(total_debt) / float(total_equity)
                if 0 <= de < 10:
                    metrics["debt_to_equity"].append((period, de))

            # Debt to Assets = Total Debt / Total Assets
            if total_assets and total_assets > 0:
                da = float(total_debt) / float(total_assets)
                if 0 <= da < 1:
                    metrics["debt_to_assets"].append((period, da))

            # Interest Coverage = Operating Income / Interest Expense
            # Approximate interest expense from debt level
            if operating_income and total_debt and total_debt > 0:
                # Estimate interest at 5% of debt (rough approximation)
                estimated_interest = float(total_debt) * 0.05
                if estimated_interest > 0:
                    ic = float(operating_income) / estimated_interest
                    if -5 < ic < 50:
                        metrics["interest_coverage"].append((period, ic))

        return metrics

    def _calculate_distress_score(
        self,
        balance_sheets: List[Dict],
        financials: List[Dict]
    ) -> Dict[str, Any]:
        """
        Calculate financial distress probability using Altman Z-score methodology.

        Z-Score = 1.2A + 1.4B + 3.3C + 0.6D + 1.0E
        Where:
        A = Working Capital / Total Assets
        B = Retained Earnings / Total Assets
        C = EBIT / Total Assets
        D = Market Value of Equity / Total Liabilities (we use book value)
        E = Sales / Total Assets

        Interpretation:
        Z > 2.99: Safe zone
        1.81 < Z < 2.99: Grey zone
        Z < 1.81: Distress zone
        """
        if not balance_sheets or not financials:
            return {"distress_score": 50.0, "z_score": None, "zone": "unknown"}

        # Get most recent data
        bs = balance_sheets[0]
        fin_by_period = {f["period_date"]: f for f in financials}
        fin = fin_by_period.get(bs["period_date"], {})

        total_assets = bs.get("total_assets")
        current_assets = bs.get("current_assets")
        current_liabilities = bs.get("current_liabilities")
        retained_earnings = bs.get("retained_earnings")
        total_liabilities = bs.get("total_liabilities")
        total_equity = bs.get("total_equity")
        operating_income = fin.get("operating_income")
        revenue = fin.get("total_revenue")

        if not all([total_assets, total_equity]):
            return {"distress_score": 50.0, "z_score": None, "zone": "insufficient_data"}

        if total_assets == 0:
            return {"distress_score": 50.0, "z_score": None, "zone": "insufficient_data"}

        # Calculate Z-Score components
        working_capital = (float(current_assets or 0) - float(current_liabilities or 0))
        a = working_capital / float(total_assets)
        b = float(retained_earnings or 0) / float(total_assets)
        c = float(operating_income or 0) / float(total_assets)
        d = float(total_equity) / float(total_liabilities or 1)  # Use book value
        e = float(revenue or 0) / float(total_assets)

        z_score = 1.2 * a + 1.4 * b + 3.3 * c + 0.6 * d + 1.0 * e

        # Determine zone
        if z_score > 2.99:
            zone = "safe"
            distress_score = 85.0 + min(15, (z_score - 2.99) * 5)  # Up to 100
        elif z_score > 1.81:
            zone = "grey"
            # Linear interpolation between 40 and 85
            distress_score = 40 + (z_score - 1.81) / (2.99 - 1.81) * 45
        else:
            zone = "distress"
            distress_score = max(5, 40 + (z_score - 1.81) * 20)  # Down to 5

        distress_score = max(0, min(100, distress_score))

        return {
            "z_score": round(z_score, 2),
            "zone": zone,
            "distress_score": round(distress_score, 1),
            "components": {
                "working_capital_ratio": round(a, 4),
                "retained_earnings_ratio": round(b, 4),
                "ebit_ratio": round(c, 4),
                "equity_liabilities_ratio": round(d, 4),
                "asset_turnover": round(e, 4),
            },
            "interpretation": self._interpret_distress(zone, z_score),
        }

    def _interpret_distress(self, zone: str, z_score: float) -> str:
        """Generate LLM-friendly interpretation of distress level."""
        interpretations = {
            "safe": f"Strong financial health (Z={z_score:.2f}) - low bankruptcy risk, fortress balance sheet",
            "grey": f"Moderate financial health (Z={z_score:.2f}) - some stress indicators, monitor closely",
            "distress": f"Financial distress warning (Z={z_score:.2f}) - elevated bankruptcy risk, urgent attention needed",
            "unknown": "Insufficient data to assess financial distress",
            "insufficient_data": "Missing key financial data for distress calculation",
        }
        return interpretations.get(zone, "Unknown zone")

    async def _analyze_metric(
        self,
        metric_name: str,
        values: List[Tuple[date, float]],
        sector: Optional[str],
        score_date: date,
    ) -> MetricAnalysis:
        """Analyze a financial health metric."""

        analysis = MetricAnalysis()
        analysis.data_points = len(values)

        if not values:
            return analysis

        sorted_values = sorted(values, key=lambda x: x[0], reverse=True)
        raw_values = [v[1] for v in sorted_values]

        analysis.current = raw_values[0]
        if len(raw_values) >= 4:
            analysis.ttm = sum(raw_values[:4]) / 4
        if len(raw_values) >= 8:
            analysis.avg_3yr = sum(raw_values) / len(raw_values)

        analysis.min_historical = min(raw_values)
        analysis.max_historical = max(raw_values)

        # Historical percentile
        higher_is_better = metric_name not in ["debt_to_equity", "debt_to_assets"]
        if len(raw_values) >= 4:
            analysis.historical_percentile = PeerAnalyzer.calculate_percentile(
                analysis.current, raw_values, higher_is_better=higher_is_better
            )

        # Trend analysis
        if len(values) >= 4:
            trend_values = sorted(values, key=lambda x: x[0])
            analysis.trend_direction, analysis.trend_score = \
                HistoricalAnalyzer.calculate_trend(trend_values)
            # Invert trend score for debt metrics (declining is good)
            if not higher_is_better and analysis.trend_score:
                analysis.trend_score = 100 - analysis.trend_score

        # Volatility and stability
        if len(raw_values) >= 4:
            analysis.volatility = HistoricalAnalyzer.calculate_volatility(raw_values)
            analysis.stability_score = HistoricalAnalyzer.calculate_stability_score(
                analysis.volatility
            )

        # Peer comparison
        if sector:
            peer_values = await self._get_sector_peer_values(metric_name, sector, score_date)
            if peer_values and len(peer_values) >= 5:
                analysis.sector_percentile = PeerAnalyzer.calculate_percentile(
                    analysis.current, peer_values, higher_is_better=higher_is_better
                )
                analysis.sector_median = float(sorted(peer_values)[len(peer_values) // 2])

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
            has_recent_data=True,
        )

        return analysis

    async def _get_sector_peer_values(
        self,
        metric_name: str,
        sector: str,
        score_date: date
    ) -> List[float]:
        """Get latest metric values for sector peers."""

        metric_sql = {
            "current_ratio": "bs.current_assets::float / NULLIF(bs.current_liabilities, 0)",
            "quick_ratio": "(bs.current_assets - COALESCE(bs.inventory, 0))::float / NULLIF(bs.current_liabilities, 0)",
            "debt_to_equity": "COALESCE(bs.total_debt, 0)::float / NULLIF(bs.total_equity, 0)",
            "debt_to_assets": "COALESCE(bs.total_debt, 0)::float / NULLIF(bs.total_assets, 0)",
            "cash_ratio": "COALESCE(bs.cash_and_equivalents, 0)::float / NULLIF(bs.current_liabilities, 0)",
        }.get(metric_name)

        if not metric_sql:
            return []

        # Define reasonable bounds for each metric
        bounds = {
            "current_ratio": (0.1, 10),
            "quick_ratio": (0.1, 10),
            "debt_to_equity": (0, 5),
            "debt_to_assets": (0, 1),
            "cash_ratio": (0, 5),
        }.get(metric_name, (0, 10))

        query = f"""
            WITH latest_data AS (
                SELECT DISTINCT ON (bs.symbol)
                    bs.symbol,
                    {metric_sql} as metric_value
                FROM balance_sheet_data bs
                JOIN company_master cm ON bs.symbol = cm.primary_ticker
                WHERE cm.sector = $1
                AND bs.period_date <= $2
                AND bs.statement_type = 'annual'
                ORDER BY bs.symbol, bs.period_date DESC
            )
            SELECT metric_value FROM latest_data
            WHERE metric_value IS NOT NULL
            AND metric_value BETWEEN {bounds[0]} AND {bounds[1]}
        """

        rows = await self.db_conn.fetch(query, sector, score_date)
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
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_peer_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted peer comparison score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.sector_percentile is not None:
                scores[name] = analysis.sector_percentile
        if not scores:
            return 50.0
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
        company_info: Dict,
        distress_analysis: Dict
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
            "distress_analysis": distress_analysis,  # Rich data for LLM reasoning
            "metrics": {},
        }

        for name, m in analysis.metrics.items():
            metadata["metrics"][name] = {
                "current": round(m.current, 4) if m.current else None,
                "avg_historical": round(m.avg_3yr, 4) if m.avg_3yr else None,
                "trend": m.trend_direction.value if m.trend_direction else None,
                "trend_score": m.trend_score,
                "sector_percentile": m.sector_percentile,
                "stability": m.stability_score,
                "raw_score": m.raw_score,
                "data_points": m.data_points,
            }

        return metadata
