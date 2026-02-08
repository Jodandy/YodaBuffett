"""
Risk Dimension Calculator

Comprehensive risk assessment combining market risk and business risk factors.
IMPORTANT: Higher scores indicate LOWER risk (inverted for consistency).

MARKET RISK FACTORS:
- Volatility (20-day) - price fluctuation
- Max drawdown (1-year) - worst peak-to-trough decline
- Beta - market sensitivity

BUSINESS RISK FACTORS:
- Leverage risk (debt-to-equity) - financial leverage
- Financing risk (short-term debt ratio, debt to cash) - refinancing exposure
- Operating leverage - profit sensitivity to revenue changes
- Liquidity stress - ability to meet short-term obligations
- Receivables quality - customer payment risk proxy

All factors are inverted so that higher dimension score = lower risk = better.
Designed to integrate with qualitative risk analysis (LLM analysis of risk factor sections).
"""

from typing import Dict, List, Optional, Any
from datetime import date, timedelta
import numpy as np
from scipy import stats
import logging

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class RiskCalculator(BaseDimensionCalculator):
    """
    Risk dimension calculator.

    Measures downside risk through multiple factors.
    Higher score = lower risk (inverted scale for consistency).
    """

    @property
    def dimension_code(self) -> str:
        return "risk"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code="risk",
            display_name="Risk",
            description="Comprehensive risk assessment: market risk (volatility, drawdown, beta) + business risk (leverage, financing, liquidity, receivables)",
            category="market",  # Price-dependent but includes business risk
            data_sources=["daily_price_data", "balance_sheet_data", "financial_statements"],
            update_frequency="daily",
            requires_external_api=False,
            version="2.0.0",
            config={
                "volatility_window": 20,
                "drawdown_window": 252,
                "market_risk_weight": 0.50,
                "business_risk_weight": 0.50,
                "note": "Higher score = lower risk. Combines market and business risk factors.",
            },
        )

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self._version = "2.0.0"

        self.volatility_window = self.get_config("volatility_window", 20)
        self.drawdown_window = self.get_config("drawdown_window", 252)

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> DimensionScore:
        """
        Calculate comprehensive risk score for a company.

        Combines market risk and business risk factors:
        - Market Risk: volatility, drawdown, beta
        - Business Risk: leverage, financing risk, liquidity, receivables
        """

        metadata = {
            "note": "Higher score indicates LOWER risk",
            "version": "2.0.0",
        }

        market_risk_scores = {}
        business_risk_scores = {}

        # ==================================================
        # MARKET RISK FACTORS (50% weight)
        # ==================================================

        # 1. Volatility (annualized)
        vol_data = await self._calculate_volatility(company_id, score_date)
        if vol_data:
            vol = vol_data["annualized_volatility"]
            if vol <= 0.15:
                vol_score = 90 + (0.15 - vol) * 100
            elif vol <= 0.30:
                vol_score = 60 + (0.30 - vol) * 200
            elif vol <= 0.50:
                vol_score = 30 + (0.50 - vol) * 150
            else:
                vol_score = max(0, 30 - (vol - 0.50) * 60)
            market_risk_scores["volatility"] = vol_score
            metadata["volatility"] = vol_data

        # 2. Max Drawdown
        dd_data = await self._calculate_max_drawdown(company_id, score_date)
        if dd_data:
            max_dd = abs(dd_data["max_drawdown"])
            if max_dd <= 0.10:
                dd_score = 85 + (0.10 - max_dd) * 150
            elif max_dd <= 0.25:
                dd_score = 55 + (0.25 - max_dd) * 200
            elif max_dd <= 0.40:
                dd_score = 25 + (0.40 - max_dd) * 200
            else:
                dd_score = max(0, 25 - (max_dd - 0.40) * 62.5)
            market_risk_scores["max_drawdown"] = dd_score
            metadata["max_drawdown"] = dd_data

        # 3. Beta (market sensitivity)
        beta_data = await self._calculate_beta(company_id, score_date)
        if beta_data:
            beta = beta_data["beta"]
            if beta <= 0.5:
                beta_score = 90 + (0.5 - beta) * 20
            elif beta <= 1.0:
                beta_score = 60 + (1.0 - beta) * 60
            elif beta <= 1.5:
                beta_score = 30 + (1.5 - beta) * 60
            else:
                beta_score = max(0, 30 - (beta - 1.5) * 30)
            market_risk_scores["beta"] = beta_score
            metadata["beta"] = beta_data

        # ==================================================
        # BUSINESS RISK FACTORS (50% weight)
        # ==================================================

        # 4. Leverage Risk (debt-to-equity)
        leverage_data = await self._get_leverage(company_id, score_date)
        if leverage_data:
            d_e = leverage_data.get("debt_to_equity")
            if d_e is not None and d_e >= 0:
                if d_e <= 0.3:
                    leverage_score = 90 + (0.3 - d_e) * 33
                elif d_e <= 1.0:
                    leverage_score = 55 + (1.0 - d_e) * 50
                elif d_e <= 2.0:
                    leverage_score = 25 + (2.0 - d_e) * 30
                else:
                    leverage_score = max(0, 25 - (d_e - 2.0) * 12.5)
                business_risk_scores["leverage"] = leverage_score
                metadata["leverage"] = leverage_data

        # 5. Financing Risk (short-term debt exposure, debt to cash)
        financing_data = await self._calculate_financing_risk(company_id, score_date)
        if financing_data:
            # Lower short-term debt ratio = lower risk
            short_term_ratio = financing_data.get("short_term_debt_ratio")
            debt_to_cash = financing_data.get("debt_to_cash")

            financing_score = 50.0  # Default neutral

            if short_term_ratio is not None:
                # Lower ratio = better (less refinancing risk)
                if short_term_ratio <= 0.2:
                    financing_score = 85
                elif short_term_ratio <= 0.4:
                    financing_score = 65
                elif short_term_ratio <= 0.6:
                    financing_score = 45
                else:
                    financing_score = 25

            # Adjust for debt-to-cash (can company pay off debt with cash?)
            if debt_to_cash is not None:
                if debt_to_cash <= 1.0:
                    financing_score = min(100, financing_score + 15)  # Can pay all debt
                elif debt_to_cash <= 3.0:
                    financing_score = financing_score  # Neutral
                elif debt_to_cash <= 10.0:
                    financing_score = max(0, financing_score - 10)
                else:
                    financing_score = max(0, financing_score - 20)  # High risk

            business_risk_scores["financing_risk"] = financing_score
            metadata["financing_risk"] = financing_data

        # 6. Liquidity Stress (current ratio, quick ratio coverage)
        liquidity_data = await self._calculate_liquidity_stress(company_id, score_date)
        if liquidity_data:
            current_ratio = liquidity_data.get("current_ratio")
            quick_ratio = liquidity_data.get("quick_ratio")

            liquidity_score = 50.0
            if current_ratio is not None:
                if current_ratio >= 2.0:
                    liquidity_score = 85
                elif current_ratio >= 1.5:
                    liquidity_score = 70
                elif current_ratio >= 1.0:
                    liquidity_score = 50
                elif current_ratio >= 0.7:
                    liquidity_score = 30
                else:
                    liquidity_score = 15  # Severe liquidity stress

            # Adjust for quick ratio (more conservative)
            if quick_ratio is not None and quick_ratio < 1.0:
                liquidity_score = max(0, liquidity_score - 10)

            business_risk_scores["liquidity"] = liquidity_score
            metadata["liquidity"] = liquidity_data

        # 7. Receivables Quality (DSO, AR growth vs revenue growth)
        receivables_data = await self._calculate_receivables_risk(company_id, score_date)
        if receivables_data:
            dso = receivables_data.get("days_sales_outstanding")
            ar_growth_ratio = receivables_data.get("ar_to_revenue_growth_ratio")

            receivables_score = 60.0  # Default slightly positive

            # DSO scoring (lower = better)
            if dso is not None:
                if dso <= 30:
                    receivables_score = 85
                elif dso <= 45:
                    receivables_score = 70
                elif dso <= 60:
                    receivables_score = 55
                elif dso <= 90:
                    receivables_score = 40
                elif dso <= 120:
                    receivables_score = 25
                else:
                    receivables_score = 10  # Very concerning (>120 days)

            # If AR growing much faster than revenue, penalize (customer concentration/quality risk)
            if ar_growth_ratio is not None and ar_growth_ratio > 1.5:
                receivables_score = max(0, receivables_score - 15)

            business_risk_scores["receivables_quality"] = receivables_score
            metadata["receivables"] = receivables_data

        # ==================================================
        # AGGREGATE SCORES
        # ==================================================

        # Combine market and business risk
        market_weights = {"volatility": 0.35, "max_drawdown": 0.35, "beta": 0.30}
        business_weights = {
            "leverage": 0.30,
            "financing_risk": 0.25,
            "liquidity": 0.25,
            "receivables_quality": 0.20,
        }

        # Calculate market risk composite
        if market_risk_scores:
            market_total_weight = sum(market_weights.get(k, 0) for k in market_risk_scores)
            market_weighted_sum = sum(
                market_risk_scores[k] * market_weights.get(k, 0)
                for k in market_risk_scores
            )
            market_risk_score = market_weighted_sum / market_total_weight if market_total_weight > 0 else 50.0
        else:
            market_risk_score = None

        # Calculate business risk composite
        if business_risk_scores:
            business_total_weight = sum(business_weights.get(k, 0) for k in business_risk_scores)
            business_weighted_sum = sum(
                business_risk_scores[k] * business_weights.get(k, 0)
                for k in business_risk_scores
            )
            business_risk_score = business_weighted_sum / business_total_weight if business_total_weight > 0 else 50.0
        else:
            business_risk_score = None

        # Combine market (50%) and business (50%) risk
        if market_risk_score is not None and business_risk_score is not None:
            score = 0.50 * market_risk_score + 0.50 * business_risk_score
        elif market_risk_score is not None:
            score = market_risk_score
        elif business_risk_score is not None:
            score = business_risk_score
        else:
            return self._no_data_score(company_id, score_date)

        # All component scores combined
        all_scores = {**market_risk_scores, **business_risk_scores}
        total_possible = len(market_weights) + len(business_weights)
        data_quality = len(all_scores) / total_possible
        confidence = data_quality * 0.85

        # Uncertainty range
        if all_scores:
            std_dev = np.std(list(all_scores.values()))
            score_low = max(0, score - std_dev * 1.5)
            score_high = min(100, score + std_dev * 1.5)
        else:
            score_low, score_high = 0.0, 100.0

        # Store breakdown in metadata
        metadata["market_risk"] = {
            "score": round(market_risk_score, 1) if market_risk_score else None,
            "components": market_risk_scores,
            "weight": 0.50,
        }
        metadata["business_risk"] = {
            "score": round(business_risk_score, 1) if business_risk_score else None,
            "components": business_risk_scores,
            "weight": 0.50,
        }
        metadata["component_scores"] = all_scores

        # Risk interpretation
        if score >= 70:
            metadata["risk_level"] = "low"
            metadata["interpretation"] = "Low risk profile - defensive market and business characteristics"
        elif score >= 50:
            metadata["risk_level"] = "moderate"
            metadata["interpretation"] = "Moderate risk profile - balanced market and business risk"
        elif score >= 30:
            metadata["risk_level"] = "elevated"
            metadata["interpretation"] = "Elevated risk profile - some concerning factors in market or business risk"
        else:
            metadata["risk_level"] = "high"
            metadata["interpretation"] = "High risk profile - significant market and/or business risk exposure"

        # Flag specific concerns
        concerns = []
        if market_risk_scores.get("volatility", 100) < 40:
            concerns.append("High price volatility")
        if market_risk_scores.get("max_drawdown", 100) < 40:
            concerns.append("Severe historical drawdown")
        if business_risk_scores.get("leverage", 100) < 40:
            concerns.append("High financial leverage")
        if business_risk_scores.get("financing_risk", 100) < 40:
            concerns.append("Refinancing risk exposure")
        if business_risk_scores.get("liquidity", 100) < 40:
            concerns.append("Liquidity stress")
        if business_risk_scores.get("receivables_quality", 100) < 40:
            concerns.append("Receivables quality concerns")

        if concerns:
            metadata["risk_concerns"] = concerns

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=float(score),
            confidence=confidence,
            data_quality=data_quality,
            score_low=score_low,
            score_high=score_high,
            metadata=metadata,
            definition_version=1,
        )

    async def _calculate_volatility(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate annualized volatility."""

        query = """
        SELECT dpd.close_price
        FROM daily_price_data dpd
        JOIN company_master cm ON dpd.symbol = cm.primary_ticker
        WHERE cm.id = $1
        AND dpd.date <= $2
        ORDER BY dpd.date DESC
        LIMIT $3
        """

        try:
            rows = await self.db_conn.fetch(query, company_id, score_date, self.volatility_window + 1)

            if len(rows) < 10:
                return None

            prices = [float(r["close_price"]) for r in rows]
            returns = np.diff(prices) / prices[:-1]

            daily_vol = np.std(returns)
            annualized_vol = daily_vol * np.sqrt(252)

            return {
                "daily_volatility": float(daily_vol),
                "annualized_volatility": float(annualized_vol),
                "window_days": len(rows) - 1,
            }
        except Exception as e:
            logger.warning(f"Volatility calculation failed: {e}")
            return None

    async def _calculate_max_drawdown(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate maximum drawdown over the window period."""

        query = """
        SELECT dpd.date, dpd.close_price
        FROM daily_price_data dpd
        JOIN company_master cm ON dpd.symbol = cm.primary_ticker
        WHERE cm.id = $1
        AND dpd.date <= $2
        AND dpd.date >= $2 - INTERVAL '1 year'
        ORDER BY dpd.date ASC
        """

        try:
            rows = await self.db_conn.fetch(query, company_id, score_date)

            if len(rows) < 20:
                return None

            prices = [float(r["close_price"]) for r in rows]

            # Calculate running max and drawdown
            running_max = prices[0]
            max_drawdown = 0
            max_dd_start = None
            max_dd_end = None

            for i, price in enumerate(prices):
                if price > running_max:
                    running_max = price
                drawdown = (price - running_max) / running_max
                if drawdown < max_drawdown:
                    max_drawdown = drawdown
                    max_dd_end = rows[i]["date"]

            return {
                "max_drawdown": float(max_drawdown),
                "max_drawdown_pct": float(max_drawdown * 100),
                "window_days": len(rows),
            }
        except Exception as e:
            logger.warning(f"Max drawdown calculation failed: {e}")
            return None

    async def _calculate_beta(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate beta relative to market index."""

        # Use a market index proxy - we'll use the average of all Nordic stocks
        query = """
        WITH company_returns AS (
            SELECT dpd.date,
                   (dpd.close_price - LAG(dpd.close_price) OVER (ORDER BY dpd.date))
                   / LAG(dpd.close_price) OVER (ORDER BY dpd.date) as return
            FROM daily_price_data dpd
            JOIN company_master cm ON dpd.symbol = cm.primary_ticker
            WHERE cm.id = $1
            AND dpd.date <= $2
            AND dpd.date >= $2 - INTERVAL '1 year'
        ),
        market_returns AS (
            SELECT date, AVG(daily_return) as market_return
            FROM daily_price_data
            WHERE date <= $2
            AND date >= $2 - INTERVAL '1 year'
            AND daily_return IS NOT NULL
            GROUP BY date
        )
        SELECT
            cr.return as company_return,
            mr.market_return
        FROM company_returns cr
        JOIN market_returns mr ON cr.date = mr.date
        WHERE cr.return IS NOT NULL
        """

        try:
            rows = await self.db_conn.fetch(query, company_id, score_date)

            if len(rows) < 50:
                return None

            company_returns = [float(r["company_return"]) for r in rows]
            market_returns = [float(r["market_return"]) for r in rows]

            # Calculate beta using covariance / variance
            covariance = np.cov(company_returns, market_returns)[0][1]
            market_variance = np.var(market_returns)

            beta = covariance / market_variance if market_variance > 0 else 1.0

            return {
                "beta": float(beta),
                "data_points": len(rows),
            }
        except Exception as e:
            logger.warning(f"Beta calculation failed: {e}")
            return None

    async def _get_leverage(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Get debt-to-equity ratio from balance sheet data."""

        query = """
        SELECT bs.total_debt, bs.total_equity, bs.total_assets
        FROM balance_sheet_data bs
        JOIN company_master cm ON bs.symbol = cm.primary_ticker
        WHERE cm.id = $1
        AND bs.period_date <= $2
        ORDER BY bs.period_date DESC
        LIMIT 1
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date)

            if not row:
                return None

            total_debt = row["total_debt"]
            total_equity = row["total_equity"]
            total_assets = row["total_assets"]

            # Calculate debt-to-equity
            debt_to_equity = None
            if total_equity and float(total_equity) > 0 and total_debt:
                debt_to_equity = float(total_debt) / float(total_equity)

            # Calculate debt-to-assets as backup metric
            debt_to_assets = None
            if total_assets and float(total_assets) > 0 and total_debt:
                debt_to_assets = float(total_debt) / float(total_assets)

            if debt_to_equity is None and debt_to_assets is None:
                return None

            return {
                "debt_to_equity": debt_to_equity,
                "debt_to_assets": debt_to_assets,
                "total_debt": float(total_debt) if total_debt else None,
                "total_equity": float(total_equity) if total_equity else None,
            }
        except Exception as e:
            logger.warning(f"Leverage lookup failed: {e}")
            return None

    async def _calculate_financing_risk(self, company_id: str, score_date: date) -> Optional[Dict]:
        """
        Calculate financing/refinancing risk.

        Measures:
        - Short-term debt ratio: How much debt is due soon?
        - Debt to cash ratio: Can company pay off debt with available cash?
        """
        query = """
        SELECT
            bs.total_debt,
            bs.long_term_debt,
            bs.current_liabilities,
            bs.cash_and_equivalents
        FROM balance_sheet_data bs
        JOIN company_master cm ON bs.symbol = cm.primary_ticker
        WHERE cm.id = $1
        AND bs.period_date <= $2
        ORDER BY bs.period_date DESC
        LIMIT 1
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date)

            if not row:
                return None

            total_debt = float(row["total_debt"]) if row["total_debt"] else 0
            long_term_debt = float(row["long_term_debt"]) if row["long_term_debt"] else 0
            cash = float(row["cash_and_equivalents"]) if row["cash_and_equivalents"] else 0

            # Calculate short-term debt (total - long-term)
            short_term_debt = max(0, total_debt - long_term_debt)

            # Short-term debt ratio
            short_term_ratio = short_term_debt / total_debt if total_debt > 0 else 0

            # Debt to cash ratio (how many times debt exceeds cash)
            debt_to_cash = total_debt / cash if cash > 0 else None

            return {
                "short_term_debt": short_term_debt,
                "long_term_debt": long_term_debt,
                "total_debt": total_debt,
                "cash": cash,
                "short_term_debt_ratio": round(short_term_ratio, 3),
                "debt_to_cash": round(debt_to_cash, 2) if debt_to_cash else None,
            }
        except Exception as e:
            logger.warning(f"Financing risk calculation failed: {e}")
            return None

    async def _calculate_liquidity_stress(self, company_id: str, score_date: date) -> Optional[Dict]:
        """
        Calculate liquidity stress indicators.

        Measures ability to meet short-term obligations.
        """
        query = """
        SELECT
            bs.current_assets,
            bs.current_liabilities,
            bs.inventory,
            bs.accounts_receivable,
            bs.cash_and_equivalents
        FROM balance_sheet_data bs
        JOIN company_master cm ON bs.symbol = cm.primary_ticker
        WHERE cm.id = $1
        AND bs.period_date <= $2
        ORDER BY bs.period_date DESC
        LIMIT 1
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date)

            if not row:
                return None

            current_assets = float(row["current_assets"]) if row["current_assets"] else 0
            current_liabilities = float(row["current_liabilities"]) if row["current_liabilities"] else 0
            inventory = float(row["inventory"]) if row["inventory"] else 0
            cash = float(row["cash_and_equivalents"]) if row["cash_and_equivalents"] else 0

            # Current ratio
            current_ratio = current_assets / current_liabilities if current_liabilities > 0 else None

            # Quick ratio (excluding inventory)
            quick_assets = current_assets - inventory
            quick_ratio = quick_assets / current_liabilities if current_liabilities > 0 else None

            # Cash ratio (most conservative)
            cash_ratio = cash / current_liabilities if current_liabilities > 0 else None

            return {
                "current_ratio": round(current_ratio, 2) if current_ratio else None,
                "quick_ratio": round(quick_ratio, 2) if quick_ratio else None,
                "cash_ratio": round(cash_ratio, 2) if cash_ratio else None,
                "current_assets": current_assets,
                "current_liabilities": current_liabilities,
            }
        except Exception as e:
            logger.warning(f"Liquidity stress calculation failed: {e}")
            return None

    async def _calculate_receivables_risk(self, company_id: str, score_date: date) -> Optional[Dict]:
        """
        Calculate receivables quality risk.

        High DSO or AR growing faster than revenue can indicate:
        - Customer concentration risk
        - Customer payment issues
        - Potential revenue quality issues
        """
        query = """
        WITH latest_bs AS (
            SELECT
                bs.accounts_receivable,
                bs.period_date
            FROM balance_sheet_data bs
            JOIN company_master cm ON bs.symbol = cm.primary_ticker
            WHERE cm.id = $1
            AND bs.period_date <= $2
            ORDER BY bs.period_date DESC
            LIMIT 2
        ),
        latest_fs AS (
            SELECT
                fs.total_revenue,
                fs.period_date
            FROM financial_statements fs
            JOIN company_master cm ON fs.symbol = cm.primary_ticker
            WHERE cm.id = $1
            AND fs.period_date <= $2
            ORDER BY fs.period_date DESC
            LIMIT 2
        )
        SELECT
            (SELECT accounts_receivable FROM latest_bs ORDER BY period_date DESC LIMIT 1) as current_ar,
            (SELECT accounts_receivable FROM latest_bs ORDER BY period_date ASC LIMIT 1) as prior_ar,
            (SELECT total_revenue FROM latest_fs ORDER BY period_date DESC LIMIT 1) as current_revenue,
            (SELECT total_revenue FROM latest_fs ORDER BY period_date ASC LIMIT 1) as prior_revenue
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date)

            if not row or not row["current_ar"] or not row["current_revenue"]:
                return None

            current_ar = float(row["current_ar"])
            current_revenue = float(row["current_revenue"])
            prior_ar = float(row["prior_ar"]) if row["prior_ar"] else None
            prior_revenue = float(row["prior_revenue"]) if row["prior_revenue"] else None

            # Days Sales Outstanding (annualized revenue assumption)
            annual_revenue = current_revenue * 4  # Assuming quarterly data
            dso = (current_ar / annual_revenue) * 365 if annual_revenue > 0 else None

            # AR to revenue growth ratio
            ar_to_revenue_growth_ratio = None
            if prior_ar and prior_revenue and prior_ar > 0 and prior_revenue > 0:
                ar_growth = (current_ar - prior_ar) / prior_ar
                revenue_growth = (current_revenue - prior_revenue) / prior_revenue
                if revenue_growth != 0:
                    ar_to_revenue_growth_ratio = ar_growth / revenue_growth

            return {
                "days_sales_outstanding": round(dso, 1) if dso else None,
                "accounts_receivable": current_ar,
                "ar_growth": round((current_ar - prior_ar) / prior_ar, 3) if prior_ar and prior_ar > 0 else None,
                "revenue_growth": round((current_revenue - prior_revenue) / prior_revenue, 3) if prior_revenue and prior_revenue > 0 else None,
                "ar_to_revenue_growth_ratio": round(ar_to_revenue_growth_ratio, 2) if ar_to_revenue_growth_ratio else None,
            }
        except Exception as e:
            logger.warning(f"Receivables risk calculation failed: {e}")
            return None

    def _no_data_score(self, company_id: str, score_date: date) -> DimensionScore:
        """Return neutral score when no data available."""
        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=50.0,
            confidence=0.0,
            data_quality=0.0,
            metadata={"no_data": True, "note": "Higher score = lower risk"},
            definition_version=1,
        )
