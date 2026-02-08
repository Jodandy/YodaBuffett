"""
Capital Allocation Dimension Calculator

Measures how effectively management deploys capital.
This is a BUSINESS dimension (price-independent).

Key Metrics:
- CapEx/Depreciation: >1 = investing for growth, <1 = harvesting
- Self-Funding Ratio: Operating CF / CapEx (ability to fund growth internally)
- Dividend Coverage: FCF / Dividends (sustainability of payouts)
- Reinvestment Rate: CapEx / Operating CF (how much goes back into business)
- Debt Management: Changes in debt relative to cash generation

Academic Basis:
- Jensen (1986): Free cash flow agency problem
- Richardson (2006): Over-investment of free cash flow
- Capital allocation is critical per Buffett, Greenblatt, Klarman

Weights are equal by default - tune based on backtesting.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Any
import logging
import numpy as np

from .base import BaseDimensionCalculator, register_calculator
from .analysis_helpers import (
    ScoreNormalizer,
    HistoricalAnalyzer,
    TrendDirection,
)
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class CapitalAllocationCalculator(BaseDimensionCalculator):
    """
    Capital Allocation dimension calculator.

    Identifies companies with disciplined, value-creating capital allocation
    vs companies that waste cash or over-leverage.

    High score = Disciplined allocation, self-funding growth
    Low score = Poor capital discipline, over-reliance on debt
    """

    # Equal weights by default - no assumptions without backtesting
    DEFAULT_METRIC_WEIGHTS = {
        "capex_to_depreciation": 0.20,  # Investment vs maintenance
        "self_funding_ratio": 0.20,     # OCF covers CapEx
        "fcf_margin": 0.20,             # FCF as % of revenue
        "dividend_coverage": 0.20,      # Can it afford dividends
        "debt_change_ratio": 0.20,      # Is debt growing vs cash
    }

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self.metric_weights = (config or {}).get("metric_weights", self.DEFAULT_METRIC_WEIGHTS)

    @property
    def dimension_code(self) -> str:
        return "capital_allocation"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Capital Allocation",
            description="Measures management's effectiveness in deploying capital",
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
        """Calculate capital allocation score for a company."""

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

        if len(cash_flows) < 2 or len(balance_sheets) < 2:
            return None

        # Calculate capital allocation metrics
        metrics = self._calculate_metrics(financials, cash_flows, balance_sheets)

        if not metrics:
            return None

        # Analyze and score
        analysis = self._analyze_metrics(metrics)
        composite = self._calculate_composite_score(analysis)

        # Determine allocation style
        allocation_style = self._determine_allocation_style(metrics)

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
                "allocation_style": allocation_style,
            },
            definition_version=1,
        )

    def _calculate_metrics(
        self,
        financials: List[Dict],
        cash_flows: List[Dict],
        balance_sheets: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate capital allocation metrics."""

        if not cash_flows:
            return {}

        # Most recent period
        current_cf = cash_flows[0]
        current_bs = balance_sheets[0] if balance_sheets else {}
        current_fs = financials[0] if financials else {}

        # Previous period for debt change
        prev_bs = balance_sheets[1] if len(balance_sheets) > 1 else {}

        ocf = float(current_cf.get("operating_cash_flow") or 0)
        capex = abs(float(current_cf.get("capital_expenditure") or 0))
        fcf = float(current_cf.get("free_cash_flow") or 0)
        depreciation = float(current_cf.get("depreciation_amortization") or 0)
        dividends = abs(float(current_cf.get("dividends_paid") or 0))
        revenue = float(current_fs.get("total_revenue") or 1)

        current_debt = float(current_bs.get("total_debt") or 0)
        prev_debt = float(prev_bs.get("total_debt") or current_debt)

        # Calculate ratios
        metrics = {
            "current_period": str(current_cf.get("period_date")),
        }

        # CapEx / Depreciation: >1 = investing, <1 = harvesting
        if depreciation > 0:
            metrics["capex_to_depreciation"] = capex / depreciation
        else:
            metrics["capex_to_depreciation"] = None

        # Self-funding ratio: OCF / CapEx
        if capex > 0:
            metrics["self_funding_ratio"] = ocf / capex
        else:
            metrics["self_funding_ratio"] = None if ocf <= 0 else 999  # No capex needed

        # FCF Margin: FCF / Revenue
        if revenue > 0:
            metrics["fcf_margin"] = fcf / revenue
        else:
            metrics["fcf_margin"] = None

        # Dividend coverage: FCF / Dividends
        if dividends > 0:
            metrics["dividend_coverage"] = fcf / dividends
        else:
            metrics["dividend_coverage"] = None  # No dividends

        # Debt change ratio: (Current Debt - Prev Debt) / OCF
        # Positive = taking on debt, negative = paying down
        if ocf != 0:
            debt_change = current_debt - prev_debt
            metrics["debt_change_ratio"] = debt_change / abs(ocf)
        else:
            metrics["debt_change_ratio"] = None

        # Historical averages
        historical_fcf_margins = []
        for cf, fs in zip(cash_flows, financials):
            fcf_val = float(cf.get("free_cash_flow") or 0)
            rev_val = float(fs.get("total_revenue") or 1)
            if rev_val > 0:
                historical_fcf_margins.append(fcf_val / rev_val)

        if historical_fcf_margins:
            metrics["avg_fcf_margin"] = np.mean(historical_fcf_margins)
            metrics["fcf_margin_trend"] = self._simple_trend(historical_fcf_margins)

        return metrics

    def _simple_trend(self, values: List[float]) -> str:
        """Simple trend detection."""
        if len(values) < 3:
            return "insufficient_data"

        # Values are newest first, reverse for trend calc
        values = list(reversed(values))
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]

        if slope > 0.01:
            return "improving"
        elif slope < -0.01:
            return "declining"
        return "stable"

    def _analyze_metrics(self, metrics: Dict) -> Dict[str, Any]:
        """Analyze metrics and convert to scores."""

        analysis = {}

        # CapEx/Depreciation: 1.0-1.5 is ideal (maintaining + modest growth)
        # <0.8 = underinvesting, >2.0 = potentially over-investing
        capex_dep = metrics.get("capex_to_depreciation")
        if capex_dep is not None:
            # Score peaks at 1.2, falls off on both sides
            if capex_dep < 0.5:
                score = 30  # Severely underinvesting
            elif capex_dep < 0.8:
                score = 50  # Underinvesting
            elif capex_dep < 1.5:
                score = 80 + (1.2 - abs(capex_dep - 1.2)) * 50  # Optimal range
            elif capex_dep < 2.5:
                score = 70  # Heavy investing (may or may not be good)
            else:
                score = 50  # Potentially over-investing
            analysis["capex_to_depreciation"] = {"value": round(capex_dep, 2), "score": min(100, score)}

        # Self-funding: >1 means OCF covers CapEx (self-funded growth)
        self_fund = metrics.get("self_funding_ratio")
        if self_fund is not None and self_fund < 100:  # Ignore the 999 placeholder
            score = ScoreNormalizer.normalize_metric(
                min(self_fund, 3.0), low_threshold=0.5, high_threshold=2.0, higher_is_better=True
            )
            analysis["self_funding_ratio"] = {"value": round(self_fund, 2), "score": score}

        # FCF Margin: Higher is better, but realistic range
        fcf_margin = metrics.get("fcf_margin")
        if fcf_margin is not None:
            score = ScoreNormalizer.normalize_metric(
                fcf_margin, low_threshold=-0.05, high_threshold=0.20, higher_is_better=True
            )
            analysis["fcf_margin"] = {"value": round(fcf_margin, 4), "score": score}

        # Dividend coverage: >1.5 is safe, <1 is unsustainable
        div_cov = metrics.get("dividend_coverage")
        if div_cov is not None:
            score = ScoreNormalizer.normalize_metric(
                min(div_cov, 5.0), low_threshold=0.5, high_threshold=2.5, higher_is_better=True
            )
            analysis["dividend_coverage"] = {"value": round(div_cov, 2), "score": score}

        # Debt change: negative (paying down) is better, unless company can invest well
        debt_change = metrics.get("debt_change_ratio")
        if debt_change is not None:
            # Paying down debt = good, taking on debt moderately = ok, heavy debt = bad
            score = ScoreNormalizer.normalize_metric(
                debt_change, low_threshold=0.5, high_threshold=-0.3, higher_is_better=False
            )
            analysis["debt_change_ratio"] = {"value": round(debt_change, 3), "score": score}

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

        # Equal weight for available metrics
        composite_score = np.mean(list(scores.values()))
        data_quality = len(scores) / len(self.metric_weights)
        confidence = data_quality

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

    def _determine_allocation_style(self, metrics: Dict) -> Dict[str, str]:
        """Classify the company's capital allocation style."""

        capex_dep = metrics.get("capex_to_depreciation", 1.0)
        self_fund = metrics.get("self_funding_ratio", 1.0)
        fcf_margin = metrics.get("fcf_margin", 0)
        div_cov = metrics.get("dividend_coverage")

        styles = []

        # Investment style
        if capex_dep and capex_dep > 1.5:
            styles.append("growth_investor")
        elif capex_dep and capex_dep < 0.8:
            styles.append("harvesting")
        else:
            styles.append("maintaining")

        # Funding style
        if self_fund and self_fund > 1.5:
            styles.append("self_funded")
        elif self_fund and self_fund < 0.8:
            styles.append("external_funding_dependent")

        # Cash generation
        if fcf_margin and fcf_margin > 0.15:
            styles.append("cash_cow")
        elif fcf_margin and fcf_margin < 0:
            styles.append("cash_burner")

        # Shareholder returns
        if div_cov and div_cov > 2:
            styles.append("sustainable_dividend")
        elif div_cov and div_cov < 1:
            styles.append("dividend_at_risk")

        return {
            "primary_style": styles[0] if styles else "unknown",
            "all_styles": styles,
        }

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        row = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker, yahoo_symbol, sector
            FROM company_master WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_historical_financials(self, symbol: str, score_date: date, years: int = 5) -> List[Dict]:
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_revenue, operating_income, net_income
            FROM financial_statements
            WHERE symbol = $1 AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_historical_cash_flows(self, symbol: str, score_date: date, years: int = 5) -> List[Dict]:
        rows = await self.db_conn.fetch("""
            SELECT period_date, operating_cash_flow, capital_expenditure,
                   free_cash_flow, dividends_paid, depreciation_amortization
            FROM cash_flow_data
            WHERE symbol = $1 AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]

    async def _get_historical_balance_sheets(self, symbol: str, score_date: date, years: int = 5) -> List[Dict]:
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_assets, total_debt, total_equity
            FROM balance_sheet_data
            WHERE symbol = $1 AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """ % years, symbol, score_date)
        return [dict(r) for r in rows]
