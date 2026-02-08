"""
Working Capital Efficiency Calculator

Measures how efficiently a company manages its operating cycle.
This is a BUSINESS dimension (price-independent).

Key Metrics:
- DSO (Days Sales Outstanding): How fast receivables are collected
- DIO (Days Inventory Outstanding): How fast inventory turns
- DPO (Days Payable Outstanding): How long to pay suppliers
- CCC (Cash Conversion Cycle): DSO + DIO - DPO

The Cash Conversion Cycle is critical:
- Negative CCC = Gets paid before paying suppliers (ideal, like Amazon)
- Low positive CCC = Efficient working capital
- High CCC = Cash tied up in operations

Academic/Practitioner Basis:
- Richards & Laughlin (1980): Cash Conversion Cycle
- Shin & Soenen (1998): Working capital and profitability
- Deloof (2003): Working capital management and firm profitability

Equal weights by default - tune based on backtesting.
"""

from datetime import date
from typing import Dict, List, Optional, Any
import logging
import numpy as np

from .base import BaseDimensionCalculator, register_calculator
from .analysis_helpers import ScoreNormalizer, HistoricalAnalyzer, TrendDirection
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class WorkingCapitalCalculator(BaseDimensionCalculator):
    """
    Working Capital Efficiency calculator.

    Measures operating cycle efficiency through DSO, DIO, DPO, and CCC.

    High score = Efficient working capital, fast cash conversion
    Low score = Cash tied up in operations, slow conversion
    """

    DEFAULT_METRIC_WEIGHTS = {
        "dso": 0.25,              # Days Sales Outstanding
        "dio": 0.25,              # Days Inventory Outstanding
        "dpo": 0.25,              # Days Payable Outstanding
        "cash_conversion_cycle": 0.25,  # The composite metric
    }

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self.metric_weights = (config or {}).get("metric_weights", self.DEFAULT_METRIC_WEIGHTS)

    @property
    def dimension_code(self) -> str:
        return "working_capital"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Working Capital Efficiency",
            description="Measures operating cycle efficiency through cash conversion cycle",
            category="fundamental",
            data_sources=["financial_statements", "balance_sheet_data"],
            update_frequency="daily",
            version="1.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate working capital efficiency score."""

        company_info = await self._get_company_info(company_id)
        if not company_info:
            return None

        symbol = company_info["primary_ticker"]
        if not symbol:
            return None

        # Fetch historical data
        financials = await self._get_historical_financials(symbol, score_date, years=5)
        balance_sheets = await self._get_historical_balance_sheets(symbol, score_date, years=5)

        if len(financials) < 2 or len(balance_sheets) < 2:
            return None

        # Calculate working capital metrics
        metrics = self._calculate_metrics(financials, balance_sheets)

        if not metrics or not metrics.get("current"):
            return None

        # Get peer data for comparison
        peer_data = await self._get_peer_wc_metrics(score_date, company_info.get("sector"))

        # Analyze and score
        analysis = self._analyze_metrics(metrics, peer_data)
        composite = self._calculate_composite_score(analysis)

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
                "interpretation": self._interpret(metrics),
                "trend": metrics.get("trend"),
            },
            definition_version=1,
        )

    def _calculate_metrics(
        self,
        financials: List[Dict],
        balance_sheets: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate working capital metrics for each period."""

        metrics_by_period = []

        for fs in financials:
            period = fs["period_date"]
            bs = next((b for b in balance_sheets if b["period_date"] == period), None)

            if not bs:
                continue

            revenue = float(fs.get("total_revenue") or 0)
            cogs = revenue - float(fs.get("gross_profit") or 0)  # Approximate COGS

            receivables = float(bs.get("accounts_receivable") or 0)
            inventory = float(bs.get("inventory") or 0)
            payables = float(bs.get("accounts_payable") or 0)

            # Use daily rates (annual / 365)
            daily_revenue = revenue / 365 if revenue > 0 else 1
            daily_cogs = cogs / 365 if cogs > 0 else 1

            period_metrics = {
                "period": str(period),
            }

            # DSO = Receivables / (Revenue / 365)
            if daily_revenue > 0:
                period_metrics["dso"] = receivables / daily_revenue

            # DIO = Inventory / (COGS / 365)
            if daily_cogs > 0:
                period_metrics["dio"] = inventory / daily_cogs

            # DPO = Payables / (COGS / 365)
            if daily_cogs > 0:
                period_metrics["dpo"] = payables / daily_cogs

            # CCC = DSO + DIO - DPO
            dso = period_metrics.get("dso", 0)
            dio = period_metrics.get("dio", 0)
            dpo = period_metrics.get("dpo", 0)
            period_metrics["cash_conversion_cycle"] = dso + dio - dpo

            metrics_by_period.append(period_metrics)

        if not metrics_by_period:
            return {}

        # Current is most recent
        current = metrics_by_period[0]

        # Calculate historical stats
        historical_ccc = [m.get("cash_conversion_cycle") for m in metrics_by_period if m.get("cash_conversion_cycle") is not None]

        result = {
            "current": {
                "dso": round(current.get("dso", 0), 1),
                "dio": round(current.get("dio", 0), 1),
                "dpo": round(current.get("dpo", 0), 1),
                "cash_conversion_cycle": round(current.get("cash_conversion_cycle", 0), 1),
            },
            "periods_analyzed": len(metrics_by_period),
        }

        if len(historical_ccc) >= 3:
            result["historical"] = {
                "avg_ccc": round(np.mean(historical_ccc), 1),
                "min_ccc": round(min(historical_ccc), 1),
                "max_ccc": round(max(historical_ccc), 1),
            }

            # Trend (newest first, so reverse for trend calc)
            trend_values = list(reversed(historical_ccc))
            x = np.arange(len(trend_values))
            slope = np.polyfit(x, trend_values, 1)[0]

            if slope < -2:
                result["trend"] = "improving"  # CCC decreasing
            elif slope > 2:
                result["trend"] = "deteriorating"  # CCC increasing
            else:
                result["trend"] = "stable"

        return result

    def _analyze_metrics(self, metrics: Dict, peer_data: Dict) -> Dict[str, Any]:
        """Analyze metrics and convert to scores."""

        current = metrics.get("current", {})
        analysis = {}

        # DSO: Lower is better. 30-45 days is good for most industries
        dso = current.get("dso")
        if dso is not None:
            score = ScoreNormalizer.normalize_metric(
                dso, low_threshold=90, high_threshold=20, higher_is_better=False
            )
            analysis["dso"] = {
                "value": dso,
                "score": score,
                "interpretation": self._interpret_dso(dso),
            }

        # DIO: Lower is better. Varies a lot by industry
        dio = current.get("dio")
        if dio is not None:
            score = ScoreNormalizer.normalize_metric(
                dio, low_threshold=120, high_threshold=30, higher_is_better=False
            )
            analysis["dio"] = {
                "value": dio,
                "score": score,
                "interpretation": self._interpret_dio(dio),
            }

        # DPO: Higher is generally better (using suppliers' cash)
        # But too high can indicate payment issues
        dpo = current.get("dpo")
        if dpo is not None:
            # Optimal around 45-60 days
            if dpo < 30:
                score = 60  # Paying too fast
            elif dpo < 60:
                score = 80  # Good range
            elif dpo < 90:
                score = 70  # Using supplier credit well
            else:
                score = 50  # Maybe straining supplier relationships
            analysis["dpo"] = {
                "value": dpo,
                "score": score,
                "interpretation": self._interpret_dpo(dpo),
            }

        # CCC: Lower (even negative) is better
        ccc = current.get("cash_conversion_cycle")
        if ccc is not None:
            score = ScoreNormalizer.normalize_metric(
                ccc, low_threshold=120, high_threshold=-30, higher_is_better=False
            )
            analysis["cash_conversion_cycle"] = {
                "value": ccc,
                "score": score,
                "interpretation": self._interpret_ccc(ccc),
            }

        return analysis

    def _calculate_composite_score(self, analysis: Dict) -> Dict[str, float]:
        """Calculate weighted composite score."""

        scores = {}
        for metric in self.metric_weights:
            if metric in analysis and "score" in analysis[metric]:
                scores[metric] = analysis[metric]["score"]

        if not scores:
            return {"score": 50.0, "confidence": 0.0, "data_quality": 0.0,
                    "score_low": 0.0, "score_high": 100.0}

        composite_score = np.mean(list(scores.values()))
        data_quality = len(scores) / len(self.metric_weights)

        std_dev = np.std(list(scores.values())) if len(scores) > 1 else 15
        score_low = max(0, composite_score - std_dev)
        score_high = min(100, composite_score + std_dev)

        return {
            "score": round(composite_score, 1),
            "confidence": round(data_quality, 2),
            "data_quality": round(data_quality, 2),
            "score_low": round(score_low, 1),
            "score_high": round(score_high, 1),
        }

    def _interpret_dso(self, dso: float) -> str:
        if dso < 30:
            return "Excellent - collecting receivables quickly"
        elif dso < 45:
            return "Good - healthy collection period"
        elif dso < 60:
            return "Average - acceptable but monitor"
        elif dso < 90:
            return "Slow - receivables building up"
        else:
            return "Concerning - very slow collections"

    def _interpret_dio(self, dio: float) -> str:
        if dio < 30:
            return "Very fast - minimal inventory (or service business)"
        elif dio < 60:
            return "Efficient - good inventory turnover"
        elif dio < 90:
            return "Average - typical for many industries"
        else:
            return "Slow - inventory may be building up"

    def _interpret_dpo(self, dpo: float) -> str:
        if dpo < 30:
            return "Paying suppliers very quickly"
        elif dpo < 45:
            return "Standard payment terms"
        elif dpo < 60:
            return "Using supplier credit well"
        elif dpo < 90:
            return "Extended payment terms"
        else:
            return "Very slow payments - may strain suppliers"

    def _interpret_ccc(self, ccc: float) -> str:
        if ccc < 0:
            return "Negative CCC - excellent! Gets paid before paying suppliers"
        elif ccc < 30:
            return "Very efficient working capital"
        elif ccc < 60:
            return "Good working capital efficiency"
        elif ccc < 90:
            return "Average - cash tied up for 2-3 months"
        else:
            return "High CCC - significant cash tied up in operations"

    def _interpret(self, metrics: Dict) -> str:
        """Overall interpretation."""
        current = metrics.get("current", {})
        ccc = current.get("cash_conversion_cycle")

        if ccc is None:
            return "Insufficient data"

        if ccc < 0:
            return "Outstanding working capital management - negative cash cycle"
        elif ccc < 45:
            return "Efficient working capital management"
        elif ccc < 75:
            return "Average working capital efficiency"
        else:
            return "Working capital intensive - significant cash tied up"

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        row = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker, sector
            FROM company_master WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_historical_financials(self, symbol: str, score_date: date, years: int = 5) -> List[Dict]:
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_revenue, gross_profit
            FROM financial_statements
            WHERE symbol = $1 AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_historical_balance_sheets(self, symbol: str, score_date: date, years: int = 5) -> List[Dict]:
        rows = await self.db_conn.fetch("""
            SELECT period_date, accounts_receivable, inventory, accounts_payable
            FROM balance_sheet_data
            WHERE symbol = $1 AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_peer_wc_metrics(self, score_date: date, sector: Optional[str]) -> Dict:
        """Get peer working capital metrics for comparison."""
        # Simplified - could expand for sector-specific comparison
        return {}
