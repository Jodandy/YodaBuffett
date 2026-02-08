"""
Business Stage Router

Routes companies to their appropriate lifecycle stage based on financials.

Decision Tree (from QUICK-BUSINESS-ROUTER.md):

1. Revenue < $50M OR Growth > 40%? → EARLY_STAGE
2. Profitable AND Growth > 20%? → GROWTH_STAGE
3. Dividend > 3% AND Growth < 15%? → MATURE_YIELD
4. Profitable AND Stable growth? → COMPOUNDER
5. Else → ESTABLISHED (general quality assessment)
"""

from datetime import date
from typing import Dict, List, Optional, Tuple
import logging

from .models import BusinessStage, CompanyFinancials

logger = logging.getLogger(__name__)


class BusinessRouter:
    """
    Routes companies to business stages based on financial characteristics.
    """

    # Thresholds for routing (configurable)
    EARLY_STAGE_REVENUE_MAX = 50_000_000  # $50M
    EARLY_STAGE_GROWTH_MIN = 0.40  # 40%

    GROWTH_STAGE_GROWTH_MIN = 0.20  # 20%

    MATURE_YIELD_DIVIDEND_MIN = 0.03  # 3%
    MATURE_YIELD_GROWTH_MAX = 0.15  # 15%

    COMPOUNDER_GROWTH_VOLATILITY_MAX = 0.10  # 10% std dev = "stable"

    def __init__(self, db_conn=None):
        self.db_conn = db_conn

    async def route_company(
        self,
        financials: CompanyFinancials
    ) -> Tuple[BusinessStage, float]:
        """
        Route a company to its stage based on financials.

        Returns:
            Tuple of (stage, confidence)
        """

        # Check data availability
        has_revenue = financials.revenue_ttm is not None
        has_growth = financials.revenue_growth_yoy is not None
        has_profitability = financials.is_profitable is not None
        has_dividend = financials.dividend_yield is not None

        if not has_revenue and not has_growth:
            return BusinessStage.UNKNOWN, 0.0

        # Decision tree
        revenue = financials.revenue_ttm or 0
        growth = financials.revenue_growth_yoy or 0
        is_profitable = financials.is_profitable or False
        dividend_yield = financials.dividend_yield or 0
        growth_volatility = financials.growth_volatility or 0.5

        # 1. Early Stage: Small or hypergrowth
        if revenue < self.EARLY_STAGE_REVENUE_MAX or growth > self.EARLY_STAGE_GROWTH_MIN:
            confidence = self._calculate_confidence(has_revenue, has_growth)
            return BusinessStage.EARLY_STAGE, confidence

        # 2. Growth Stage: Profitable with strong growth
        if is_profitable and growth > self.GROWTH_STAGE_GROWTH_MIN:
            confidence = self._calculate_confidence(has_revenue, has_growth, has_profitability)
            return BusinessStage.GROWTH_STAGE, confidence

        # 3. Mature Yield: High dividend, low growth
        if dividend_yield > self.MATURE_YIELD_DIVIDEND_MIN and growth < self.MATURE_YIELD_GROWTH_MAX:
            confidence = self._calculate_confidence(has_revenue, has_growth, has_dividend)
            return BusinessStage.MATURE_YIELD, confidence

        # 4. Compounder: Profitable with stable growth
        if is_profitable and growth_volatility < self.COMPOUNDER_GROWTH_VOLATILITY_MAX:
            confidence = self._calculate_confidence(has_revenue, has_growth, has_profitability)
            return BusinessStage.COMPOUNDER, confidence

        # 5. Default: Established business
        confidence = self._calculate_confidence(has_revenue, has_growth)
        return BusinessStage.ESTABLISHED, confidence

    def _calculate_confidence(self, *data_flags: bool) -> float:
        """Calculate routing confidence based on data availability."""
        if not data_flags:
            return 0.5
        return sum(1 for f in data_flags if f) / len(data_flags)

    async def route_companies(
        self,
        company_ids: List[str],
        score_date: date = None
    ) -> Dict[str, Tuple[BusinessStage, float]]:
        """
        Route multiple companies to their stages.

        Returns:
            Dict mapping company_id to (stage, confidence)
        """
        score_date = score_date or date.today()

        # Fetch financials for all companies
        financials = await self._get_company_financials(company_ids, score_date)

        results = {}
        for company_id in company_ids:
            if company_id in financials:
                stage, confidence = await self.route_company(financials[company_id])
                results[company_id] = (stage, confidence)
            else:
                results[company_id] = (BusinessStage.UNKNOWN, 0.0)

        return results

    async def get_companies_by_stage(
        self,
        stage: BusinessStage,
        score_date: date = None
    ) -> List[str]:
        """Get all company IDs for a specific stage."""
        score_date = score_date or date.today()

        # Get all active companies
        company_ids = await self._get_active_companies()

        # Route them all
        routes = await self.route_companies(company_ids, score_date)

        # Filter by stage
        return [cid for cid, (s, _) in routes.items() if s == stage]

    async def _get_company_financials(
        self,
        company_ids: List[str],
        score_date: date
    ) -> Dict[str, CompanyFinancials]:
        """Fetch financials needed for routing."""

        if not company_ids:
            return {}

        # Fetch from financial_statements and company_master
        query = """
        WITH latest_financials AS (
            SELECT DISTINCT ON (fs.symbol)
                cm.id as company_id,
                cm.primary_ticker as symbol,
                cm.company_name,
                cm.market_cap_usd as market_cap,
                fs.total_revenue as revenue_ttm,
                fs.net_income as net_income_ttm,
                CASE WHEN fs.total_revenue > 0
                     THEN fs.operating_income / fs.total_revenue
                     ELSE NULL END as ebit_margin
            FROM company_master cm
            LEFT JOIN financial_statements fs ON cm.primary_ticker = fs.symbol
            WHERE cm.id = ANY($1::uuid[])
            ORDER BY fs.symbol, fs.period_date DESC
        ),
        growth_calc AS (
            SELECT
                cm.id as company_id,
                (
                    SELECT (f1.total_revenue - f2.total_revenue) / NULLIF(f2.total_revenue, 0)
                    FROM financial_statements f1
                    JOIN financial_statements f2
                        ON f1.symbol = f2.symbol
                        AND f2.period_date = (
                            SELECT MAX(period_date)
                            FROM financial_statements
                            WHERE symbol = f1.symbol
                            AND period_date < f1.period_date
                        )
                    WHERE f1.symbol = cm.primary_ticker
                    ORDER BY f1.period_date DESC
                    LIMIT 1
                ) as revenue_growth_yoy
            FROM company_master cm
            WHERE cm.id = ANY($1::uuid[])
        )
        SELECT
            lf.*,
            gc.revenue_growth_yoy
        FROM latest_financials lf
        LEFT JOIN growth_calc gc ON lf.company_id = gc.company_id
        """

        try:
            rows = await self.db_conn.fetch(query, company_ids)
        except Exception as e:
            logger.error(f"Error fetching financials for routing: {e}")
            return {}

        results = {}
        for row in rows:
            company_id = str(row["company_id"])
            results[company_id] = CompanyFinancials(
                company_id=company_id,
                symbol=row["symbol"] or "",
                company_name=row["company_name"] or "",
                revenue_ttm=float(row["revenue_ttm"]) if row["revenue_ttm"] else None,
                revenue_growth_yoy=float(row["revenue_growth_yoy"]) if row["revenue_growth_yoy"] else None,
                net_income_ttm=float(row["net_income_ttm"]) if row["net_income_ttm"] else None,
                is_profitable=float(row["net_income_ttm"]) > 0 if row["net_income_ttm"] else None,
                ebit_margin=float(row["ebit_margin"]) if row["ebit_margin"] else None,
                market_cap=float(row["market_cap"]) if row["market_cap"] else None,
            )

        return results

    async def _get_active_companies(self) -> List[str]:
        """Get all active Nordic company IDs."""
        # Include both short codes and full country names
        query = """
        SELECT id::text
        FROM company_master
        WHERE country IN ('SE', 'NO', 'DK', 'FI', 'Sverige', 'Norge', 'Danmark', 'Finland', 'Nordic')
        ORDER BY market_cap_usd DESC NULLS LAST
        """
        rows = await self.db_conn.fetch(query)
        return [row["id"] for row in rows]
