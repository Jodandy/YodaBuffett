"""
Earnings Quality Dimension Calculator

Measures how "real" and sustainable a company's earnings are.
This is a critical dimension for detecting earnings manipulation and fraud.

Key Metrics:
- Accrual Ratio: (Net Income - Operating CF) / Total Assets
  * High accruals = earnings driven by accounting, not cash (red flag)
  * Healthy range: -10% to +10%

- Cash Conversion: Operating CF / Net Income
  * Should be >80% for quality earnings
  * <50% is a major red flag

- Earnings Persistence: How stable are earnings over time?
  * High volatility = lower quality

- OCF/EBIT Ratio: Cash backing of operating profits
  * Should be >70%

For fraud detection:
- Accrual ratio > 20% with declining cash conversion = major warning
- Sudden accrual spikes in otherwise stable companies = investigate
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
)
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class EarningsQualityCalculator(BaseDimensionCalculator):
    """
    Earnings Quality dimension calculator.

    Identifies companies with sustainable, cash-backed earnings vs
    companies with aggressive accounting or potential manipulation.

    High score = Cash-backed, sustainable earnings
    Low score = Accrual-heavy, potentially manipulated earnings

    Academic basis:
    - Sloan (1996): Accrual component of earnings less persistent than cash component
    - Richardson et al. (2005): Accrual reliability and earnings persistence
    - Dechow & Dichev (2002): Quality of accruals and earnings

    Weights are equal by default - tune based on backtesting results.
    """

    # Equal weights by default - no assumptions without backtesting
    # Override via config parameter for tuning
    DEFAULT_METRIC_WEIGHTS = {
        "accrual_ratio": 0.25,         # Sloan accrual anomaly
        "cash_conversion": 0.25,       # OCF / Net Income
        "ocf_ebit_ratio": 0.25,        # Cash backing of operating profit
        "earnings_stability": 0.25,    # Consistency over time
    }

    # Component weights also equal by default
    DEFAULT_COMPONENT_WEIGHTS = {
        "raw_score": 0.25,
        "trend_score": 0.25,
        "peer_score": 0.25,
        "stability_score": 0.25,
    }

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        # Allow weight overrides via config
        self.metric_weights = (config or {}).get("metric_weights", self.DEFAULT_METRIC_WEIGHTS)
        self.component_weights = (config or {}).get("component_weights", self.DEFAULT_COMPONENT_WEIGHTS)

    @property
    def dimension_code(self) -> str:
        return "earnings_quality"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Earnings Quality",
            description="Measures cash backing and sustainability of reported earnings",
            category="fundamental",
            data_sources=["financial_statements", "cash_flow_data", "balance_sheet_data"],
            update_frequency="daily",
            version="1.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate earnings quality for a company."""

        company_info = await self._get_company_info(company_id)
        if not company_info:
            return None

        symbol = company_info["primary_ticker"]
        if not symbol:
            return None

        # Fetch historical data
        financials = await self._get_historical_financials(symbol, score_date, years=5)
        cash_flows = await self._get_historical_cash_flows(symbol, score_date, years=5)
        balance_sheets = await self._get_historical_balance_sheets(symbol, score_date, years=5)

        if len(financials) < 2 or len(cash_flows) < 2 or len(balance_sheets) < 2:
            return None

        # Calculate earnings quality metrics
        metrics = self._calculate_earnings_quality_metrics(financials, cash_flows, balance_sheets)

        if not metrics:
            return None

        # Get peer data for comparison
        peer_data = await self._get_peer_earnings_quality(score_date, company_info.get("sector"))

        # Analyze each metric
        analysis = self._analyze_metrics(metrics, peer_data, score_date)

        # Calculate composite score
        composite = self._calculate_composite_score(analysis)

        # Build warning flags
        warnings = self._detect_warnings(metrics)

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=composite["score"],
            confidence=composite["confidence"],
            data_quality=composite["data_quality"],
            score_low=composite["score_low"],
            score_high=composite["score_high"],
            metadata={
                "metrics": metrics,
                "analysis": analysis,
                "warnings": warnings,
                "interpretation": self._interpret_score(composite["score"], warnings),
            },
            definition_version=1,
        )

    def _calculate_earnings_quality_metrics(
        self,
        financials: List[Dict],
        cash_flows: List[Dict],
        balance_sheets: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate earnings quality metrics from financial data."""

        # Match periods across all three statements
        metrics_by_period = {}

        for fs in financials:
            period = fs["period_date"]
            cf = next((c for c in cash_flows if c["period_date"] == period), None)
            bs = next((b for b in balance_sheets if b["period_date"] == period), None)

            if cf and bs:
                net_income = float(fs.get("net_income") or 0)
                ocf = float(cf.get("operating_cash_flow") or 0)
                total_assets = float(bs.get("total_assets") or 1)
                ebit = float(fs.get("ebit") or fs.get("operating_income") or 0)

                if total_assets > 0:
                    accrual_ratio = (net_income - ocf) / total_assets
                else:
                    accrual_ratio = None

                if net_income != 0:
                    cash_conversion = ocf / net_income if net_income > 0 else None
                else:
                    cash_conversion = None

                if ebit != 0:
                    ocf_ebit = ocf / ebit if ebit > 0 else None
                else:
                    ocf_ebit = None

                metrics_by_period[period] = {
                    "accrual_ratio": accrual_ratio,
                    "cash_conversion": cash_conversion,
                    "ocf_ebit_ratio": ocf_ebit,
                    "net_income": net_income,
                    "ocf": ocf,
                }

        if not metrics_by_period:
            return {}

        # Get most recent metrics
        sorted_periods = sorted(metrics_by_period.keys(), reverse=True)
        current = metrics_by_period[sorted_periods[0]]

        # Calculate historical averages and trends
        accrual_history = [m["accrual_ratio"] for m in metrics_by_period.values() if m["accrual_ratio"] is not None]
        cash_conv_history = [m["cash_conversion"] for m in metrics_by_period.values() if m["cash_conversion"] is not None]
        ni_history = [m["net_income"] for m in metrics_by_period.values()]

        # Calculate earnings stability (coefficient of variation of net income)
        if len(ni_history) >= 3 and np.mean(ni_history) != 0:
            earnings_stability = 1 - min(1, abs(np.std(ni_history) / abs(np.mean(ni_history))))
        else:
            earnings_stability = None

        return {
            "current": {
                "accrual_ratio": current.get("accrual_ratio"),
                "cash_conversion": current.get("cash_conversion"),
                "ocf_ebit_ratio": current.get("ocf_ebit_ratio"),
            },
            "historical": {
                "avg_accrual_ratio": np.mean(accrual_history) if accrual_history else None,
                "avg_cash_conversion": np.mean(cash_conv_history) if cash_conv_history else None,
                "accrual_trend": self._calculate_trend(accrual_history),
                "cash_conversion_trend": self._calculate_trend(cash_conv_history),
            },
            "earnings_stability": earnings_stability,
            "periods_analyzed": len(metrics_by_period),
        }

    def _calculate_trend(self, values: List[float]) -> Optional[str]:
        """Calculate trend direction from a list of values (oldest to newest)."""
        if len(values) < 3:
            return None

        # Simple linear regression
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]

        if slope > 0.02:
            return "improving"
        elif slope < -0.02:
            return "deteriorating"
        else:
            return "stable"

    def _analyze_metrics(
        self,
        metrics: Dict,
        peer_data: Dict,
        score_date: date
    ) -> Dict[str, Any]:
        """Analyze metrics and convert to scores."""

        current = metrics.get("current", {})
        historical = metrics.get("historical", {})

        analysis = {}

        # Accrual Ratio: Lower (or negative) is better
        # Range: -0.20 (excellent) to +0.20 (poor)
        accrual = current.get("accrual_ratio")
        if accrual is not None:
            # Invert score - negative accruals are good
            accrual_score = ScoreNormalizer.normalize_metric(
                accrual, low_threshold=0.20, high_threshold=-0.15, higher_is_better=False
            )
            analysis["accrual_ratio"] = {
                "value": round(accrual, 4),
                "score": accrual_score,
                "interpretation": self._interpret_accrual(accrual),
            }

        # Cash Conversion: Higher is better (>1.0 is excellent)
        cash_conv = current.get("cash_conversion")
        if cash_conv is not None:
            cash_score = ScoreNormalizer.normalize_metric(
                min(cash_conv, 2.0),  # Cap at 2.0 to avoid outlier distortion
                low_threshold=0.3, high_threshold=1.2, higher_is_better=True
            )
            analysis["cash_conversion"] = {
                "value": round(cash_conv, 4),
                "score": cash_score,
                "interpretation": self._interpret_cash_conversion(cash_conv),
            }

        # OCF/EBIT: Higher is better
        ocf_ebit = current.get("ocf_ebit_ratio")
        if ocf_ebit is not None:
            ocf_score = ScoreNormalizer.normalize_metric(
                min(ocf_ebit, 2.0),
                low_threshold=0.3, high_threshold=1.2, higher_is_better=True
            )
            analysis["ocf_ebit_ratio"] = {
                "value": round(ocf_ebit, 4),
                "score": ocf_score,
            }

        # Earnings Stability
        stability = metrics.get("earnings_stability")
        if stability is not None:
            stability_score = stability * 100  # Already 0-1, convert to 0-100
            analysis["earnings_stability"] = {
                "value": round(stability, 4),
                "score": stability_score,
            }

        return analysis

    def _calculate_composite_score(self, analysis: Dict) -> Dict[str, float]:
        """Calculate weighted composite score."""

        scores = {}
        weights_used = {}

        for metric, config in self.metric_weights.items():
            if metric in analysis and "score" in analysis[metric]:
                scores[metric] = analysis[metric]["score"]
                weights_used[metric] = config

        if not scores:
            return {"score": 50.0, "confidence": 0.0, "data_quality": 0.0,
                    "score_low": 0.0, "score_high": 100.0}

        # Weighted average
        total_weight = sum(weights_used.values())
        weighted_sum = sum(scores[m] * weights_used[m] for m in scores)
        composite_score = weighted_sum / total_weight

        # Data quality based on metric availability
        data_quality = len(scores) / len(self.metric_weights)

        # Confidence based on data quality and score spread
        score_spread = max(scores.values()) - min(scores.values()) if len(scores) > 1 else 0
        confidence = data_quality * (1 - min(0.5, score_spread / 100))

        # Score range
        std_dev = np.std(list(scores.values())) if len(scores) > 1 else 15
        score_low = max(0, composite_score - std_dev * 1.5)
        score_high = min(100, composite_score + std_dev * 1.5)

        return {
            "score": round(composite_score, 1),
            "confidence": round(confidence, 2),
            "data_quality": round(data_quality, 2),
            "score_low": round(score_low, 1),
            "score_high": round(score_high, 1),
        }

    def _detect_warnings(self, metrics: Dict) -> List[Dict[str, str]]:
        """Detect warning signals in earnings quality."""

        warnings = []
        current = metrics.get("current", {})
        historical = metrics.get("historical", {})

        # High accruals
        accrual = current.get("accrual_ratio")
        if accrual is not None and accrual > 0.15:
            warnings.append({
                "type": "high_accruals",
                "severity": "high" if accrual > 0.25 else "medium",
                "message": f"High accrual ratio ({accrual:.1%}) - earnings may not be cash-backed",
            })

        # Low cash conversion
        cash_conv = current.get("cash_conversion")
        if cash_conv is not None and cash_conv < 0.5:
            warnings.append({
                "type": "low_cash_conversion",
                "severity": "high" if cash_conv < 0.3 else "medium",
                "message": f"Low cash conversion ({cash_conv:.1%}) - earnings not converting to cash",
            })

        # Deteriorating trend
        accrual_trend = historical.get("accrual_trend")
        if accrual_trend == "deteriorating":
            warnings.append({
                "type": "deteriorating_quality",
                "severity": "medium",
                "message": "Accrual ratio trending higher - earnings quality declining",
            })

        # Combined red flag
        if accrual is not None and cash_conv is not None:
            if accrual > 0.10 and cash_conv < 0.6:
                warnings.append({
                    "type": "quality_divergence",
                    "severity": "high",
                    "message": "High accruals combined with low cash conversion - potential manipulation",
                })

        return warnings

    def _interpret_accrual(self, accrual: float) -> str:
        if accrual < -0.05:
            return "Excellent - cash exceeds reported earnings"
        elif accrual < 0.05:
            return "Good - earnings well-backed by cash"
        elif accrual < 0.15:
            return "Moderate - some accrual build-up"
        else:
            return "Concerning - high accruals, investigate"

    def _interpret_cash_conversion(self, cash_conv: float) -> str:
        if cash_conv > 1.0:
            return "Excellent - cash exceeds earnings"
        elif cash_conv > 0.8:
            return "Good - strong cash conversion"
        elif cash_conv > 0.5:
            return "Moderate - acceptable conversion"
        else:
            return "Poor - earnings not converting to cash"

    def _interpret_score(self, score: float, warnings: List) -> str:
        high_severity = sum(1 for w in warnings if w.get("severity") == "high")

        if score >= 75 and high_severity == 0:
            return "High quality, cash-backed earnings"
        elif score >= 60:
            return "Acceptable earnings quality"
        elif score >= 40:
            return "Mixed quality - monitor closely"
        else:
            return "Low quality earnings - significant red flags"

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        """Get company information."""
        row = await self.db_conn.fetchrow("""
            SELECT
                id, company_name, primary_ticker, yahoo_symbol,
                sector, industry, country
            FROM company_master
            WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_historical_financials(
        self, symbol: str, score_date: date, years: int = 5
    ) -> List[Dict]:
        """Get historical income statement data."""
        rows = await self.db_conn.fetch("""
            SELECT
                period_date, statement_type,
                total_revenue, gross_profit, operating_income,
                net_income, ebit, ebitda, interest_expense
            FROM financial_statements
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_historical_cash_flows(
        self, symbol: str, score_date: date, years: int = 5
    ) -> List[Dict]:
        """Get historical cash flow data."""
        rows = await self.db_conn.fetch("""
            SELECT
                period_date, operating_cash_flow, net_income,
                capital_expenditure, free_cash_flow,
                depreciation_amortization
            FROM cash_flow_data
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_historical_balance_sheets(
        self, symbol: str, score_date: date, years: int = 5
    ) -> List[Dict]:
        """Get historical balance sheet data."""
        rows = await self.db_conn.fetch("""
            SELECT
                period_date, total_assets, current_assets,
                total_liabilities, current_liabilities,
                total_equity, total_debt, accounts_receivable, inventory
            FROM balance_sheet_data
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_peer_earnings_quality(
        self, score_date: date, sector: Optional[str]
    ) -> Dict[str, List[float]]:
        """Get peer earnings quality metrics for comparison."""
        # Simplified - could be expanded for sector-specific comparison
        return {}
