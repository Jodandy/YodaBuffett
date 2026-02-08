"""
Historical Valuation Percentile Calculator

Measures where current valuation sits in the company's own history.
This is a MARKET dimension (price-dependent).

Key Metrics:
- P/E Percentile: Current P/E vs 5-year history
- P/B Percentile: Current P/B vs 5-year history
- EV/EBITDA Percentile: Enterprise value multiple vs history
- Price/Sales Percentile: Revenue multiple vs history

Why This Matters:
- A company trading at its own 10th percentile P/E is historically cheap
- A company at its 95th percentile is historically expensive
- Normalizes for industry differences by using own history

Weights are equal by default - tune based on backtesting.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Any
import logging
import numpy as np
from scipy import stats

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class ValuationPercentileCalculator(BaseDimensionCalculator):
    """
    Historical Valuation Percentile calculator.

    Compares current valuation multiples against the company's own history.
    High score = Currently cheap vs own history
    Low score = Currently expensive vs own history
    """

    DEFAULT_METRIC_WEIGHTS = {
        "pe_percentile": 0.25,
        "pb_percentile": 0.25,
        "ps_percentile": 0.25,
        "ev_ebitda_percentile": 0.25,
    }

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self.metric_weights = (config or {}).get("metric_weights", self.DEFAULT_METRIC_WEIGHTS)

    @property
    def dimension_code(self) -> str:
        return "valuation_percentile"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Valuation Percentile",
            description="Current valuation vs company's own historical range",
            category="technical",  # Price-dependent
            data_sources=["daily_price_data", "financial_statements", "balance_sheet_data"],
            update_frequency="daily",
            version="1.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate valuation percentile score."""

        company_info = await self._get_company_info(company_id)
        if not company_info:
            return None

        symbol = company_info["primary_ticker"]

        if not symbol:
            return None

        # Get current price (price data uses short ticker format)
        current_price = await self._get_price(symbol, score_date)
        if not current_price:
            return None

        # Get financial data
        financials = await self._get_financials(symbol, score_date)
        balance_sheet = await self._get_balance_sheet(symbol, score_date)

        if not financials or not balance_sheet:
            return None

        # Calculate current multiples
        current_multiples = self._calculate_multiples(
            current_price, financials, balance_sheet
        )

        if not current_multiples:
            return None

        # Get historical multiples (5 years)
        historical = await self._get_historical_multiples(
            symbol, symbol, score_date, years=5
        )

        if len(historical) < 20:  # Need enough history
            return None

        # Calculate percentiles
        percentiles = self._calculate_percentiles(current_multiples, historical)

        if not percentiles:
            return None

        # Score: Lower percentile = cheaper = higher score
        composite = self._calculate_composite_score(percentiles)

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
                "current_multiples": current_multiples,
                "percentiles": percentiles,
                "historical_periods": len(historical),
                "interpretation": self._interpret(percentiles),
            },
            definition_version=1,
        )

    def _calculate_multiples(
        self,
        price: float,
        financials: Dict,
        balance_sheet: Dict
    ) -> Dict[str, float]:
        """Calculate valuation multiples."""

        multiples = {}
        shares = float(balance_sheet.get("shares_outstanding") or 0)

        if shares <= 0:
            return {}

        market_cap = price * shares

        # P/E
        net_income = float(financials.get("net_income") or 0)
        if net_income > 0:
            eps = net_income / shares
            multiples["pe"] = price / eps

        # P/B
        equity = float(balance_sheet.get("total_equity") or 0)
        if equity > 0:
            book_per_share = equity / shares
            multiples["pb"] = price / book_per_share

        # P/S
        revenue = float(financials.get("total_revenue") or 0)
        if revenue > 0:
            multiples["ps"] = market_cap / revenue

        # EV/EBITDA
        ebitda = float(financials.get("ebitda") or 0)
        debt = float(balance_sheet.get("total_debt") or 0)
        cash = float(balance_sheet.get("cash_and_equivalents") or 0)

        if ebitda > 0:
            ev = market_cap + debt - cash
            multiples["ev_ebitda"] = ev / ebitda

        return multiples

    def _calculate_percentiles(
        self,
        current: Dict[str, float],
        historical: List[Dict]
    ) -> Dict[str, Any]:
        """Calculate where current multiples sit in historical distribution."""

        percentiles = {}

        for metric in ["pe", "pb", "ps", "ev_ebitda"]:
            current_val = current.get(metric)
            if current_val is None:
                continue

            historical_vals = [
                h.get(metric) for h in historical
                if h.get(metric) is not None and h.get(metric) > 0
            ]

            if len(historical_vals) < 10:
                continue

            # Calculate percentile (where does current sit?)
            pct = stats.percentileofscore(historical_vals, current_val, kind='rank')

            percentiles[f"{metric}_percentile"] = {
                "value": round(pct, 1),
                "current": round(current_val, 2),
                "median": round(np.median(historical_vals), 2),
                "min": round(min(historical_vals), 2),
                "max": round(max(historical_vals), 2),
            }

        return percentiles

    def _calculate_composite_score(self, percentiles: Dict) -> Dict[str, float]:
        """Calculate composite score - lower percentile = higher score."""

        scores = {}
        for key, data in percentiles.items():
            # Invert: 10th percentile = 90 score (cheap), 90th percentile = 10 score
            scores[key] = 100 - data["value"]

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

    def _interpret(self, percentiles: Dict) -> str:
        """Interpret the valuation percentile."""
        if not percentiles:
            return "Insufficient data"

        avg_pct = np.mean([p["value"] for p in percentiles.values()])

        if avg_pct <= 20:
            return "Historically very cheap - near multi-year lows"
        elif avg_pct <= 40:
            return "Below historical average - potentially undervalued"
        elif avg_pct <= 60:
            return "Near historical average"
        elif avg_pct <= 80:
            return "Above historical average - getting expensive"
        else:
            return "Historically very expensive - near multi-year highs"

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        row = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker, yahoo_symbol
            FROM company_master WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_price(self, symbol: str, score_date: date) -> Optional[float]:
        row = await self.db_conn.fetchrow("""
            SELECT close_price FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC LIMIT 1
        """, symbol, score_date)
        return float(row["close_price"]) if row else None

    async def _get_financials(self, symbol: str, score_date: date) -> Optional[Dict]:
        row = await self.db_conn.fetchrow("""
            SELECT total_revenue, net_income, ebitda
            FROM financial_statements
            WHERE symbol = $1 AND period_date <= $2
            ORDER BY period_date DESC LIMIT 1
        """, symbol, score_date)
        return dict(row) if row else None

    async def _get_balance_sheet(self, symbol: str, score_date: date) -> Optional[Dict]:
        row = await self.db_conn.fetchrow("""
            SELECT total_equity, total_debt, cash_and_equivalents, shares_outstanding
            FROM balance_sheet_data
            WHERE symbol = $1 AND period_date <= $2
            ORDER BY period_date DESC LIMIT 1
        """, symbol, score_date)
        return dict(row) if row else None

    async def _get_historical_multiples(
        self,
        price_symbol: str,
        fin_symbol: str,
        score_date: date,
        years: int = 5
    ) -> List[Dict]:
        """Get historical valuation multiples."""

        # Get all price points
        prices = await self.db_conn.fetch("""
            SELECT date, close_price FROM daily_price_data
            WHERE symbol = $1
            AND date <= $2
            AND date >= $2 - INTERVAL '%s years'
            ORDER BY date
        """ % years, price_symbol, score_date)

        if not prices:
            return []

        # Get all financial periods
        financials = await self.db_conn.fetch("""
            SELECT period_date, total_revenue, net_income, ebitda
            FROM financial_statements
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            ORDER BY period_date
        """ % years, fin_symbol, score_date)

        balance_sheets = await self.db_conn.fetch("""
            SELECT period_date, total_equity, total_debt, cash_and_equivalents, shares_outstanding
            FROM balance_sheet_data
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $2 - INTERVAL '%s years'
            ORDER BY period_date
        """ % years, fin_symbol, score_date)

        if not financials or not balance_sheets:
            return []

        # Sample monthly to avoid too much data
        historical = []
        seen_months = set()

        for price_row in prices:
            price_date = price_row["date"]
            month_key = (price_date.year, price_date.month)

            if month_key in seen_months:
                continue
            seen_months.add(month_key)

            # Find applicable financials (most recent before price date)
            applicable_fin = None
            for fin in reversed(list(financials)):
                if fin["period_date"] <= price_date:
                    applicable_fin = dict(fin)
                    break

            applicable_bs = None
            for bs in reversed(list(balance_sheets)):
                if bs["period_date"] <= price_date:
                    applicable_bs = dict(bs)
                    break

            if applicable_fin and applicable_bs:
                multiples = self._calculate_multiples(
                    float(price_row["close_price"]),
                    applicable_fin,
                    applicable_bs
                )
                if multiples:
                    multiples["date"] = price_date
                    historical.append(multiples)

        return historical
