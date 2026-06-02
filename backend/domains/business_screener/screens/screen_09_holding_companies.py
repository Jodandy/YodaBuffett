"""
Screen 9: Holding Company Discounts — Portfolios Below Sum of Parts

Identifies investment holding companies and conglomerates trading at
significant discounts to their net asset value or sum-of-parts value.

Tier: A + B (needs manual portfolio identification)
Frequency: Daily

Backtesting: Fully supported via score_date parameter.

Note: This Tier A screen identifies POTENTIAL holding companies based
on balance sheet characteristics. Tier B/manual analysis is needed to
verify holdings and calculate true NAV.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class HoldingCompanyDiscountsScreen(BaseScreen):
    """
    Screen 9: Holding Company Discounts — Portfolios Below Sum of Parts

    Criteria for identifying potential holding companies:
    - High investment assets relative to total assets
    - Low operating margin (income comes from investments)
    - Price to book significantly below 1
    - Positive equity (not distressed)
    - Investment income or dividend income on income statement

    For confirmed holding companies:
    - Compare market cap to estimated NAV
    - Look for 20%+ discount to NAV

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 9

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: identify potential holding companies at discount.

        This identifies companies with holding company characteristics
        (high investments, low operating margin, P/B discount).
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use yahoo_financials CTE
        combined_cte = self.get_yahoo_combined_financials_cte()
        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, f.report_currency)', 'c.trading_currency')

        query = f"""
            WITH {combined_cte},
            pit_financials AS (
                -- Select from yahoo_combined_financials
                SELECT
                    symbol,
                    period_date,
                    report_currency,
                    is_ttm,
                    total_revenue,
                    operating_income,
                    net_income,
                    total_assets,
                    current_assets,
                    cash_and_equivalents,
                    total_liabilities,
                    total_debt,
                    total_equity,
                    shares_outstanding
                FROM yahoo_combined_financials
            ),
            pit_prices AS (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    close_price,
                    date AS price_date
                FROM daily_price_data
                WHERE date <= $1
                ORDER BY symbol, date DESC
            ),
            companies_with_data AS (
                SELECT
                    cm.id AS company_id,
                    cm.company_name,
                    cm.primary_ticker,
                    cm.sector,
                    cm.currency AS trading_currency,
                    cm.report_currency,  -- For FX conversion
                    REPLACE(cm.primary_ticker, '-', ' ') AS financial_symbol
                FROM company_master cm
                WHERE cm.listing_status = 'active'
            )
            SELECT
                c.company_id,
                c.company_name,
                c.primary_ticker,
                c.sector,
                c.trading_currency,

                -- Price data (in trading_currency)
                p.close_price AS price,
                p.price_date,

                -- Financial data
                f.period_date AS financial_date,
                f.is_ttm,
                f.report_currency,
                f.shares_outstanding,
                f.total_revenue,
                f.operating_income,
                f.net_income,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Balance sheet (raw values in report_currency)
                f.total_assets,
                f.current_assets,
                f.cash_and_equivalents,
                f.total_liabilities,
                f.total_debt,
                f.total_equity,

                -- Non-current assets (proxy for investments)
                f.total_assets - f.current_assets AS non_current_assets,

                -- Market cap (in trading_currency)
                p.close_price * f.shares_outstanding AS market_cap,

                -- Price to book (with FX conversion)
                CASE WHEN f.total_equity > 0 AND f.shares_outstanding > 0
                    THEN p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding)
                    ELSE NULL END AS price_to_book,

                -- Non-current assets intensity (no FX needed - same currency ratio)
                CASE WHEN f.total_assets > 0
                    THEN (f.total_assets - f.current_assets)::DECIMAL / f.total_assets
                    ELSE NULL END AS non_current_intensity,

                -- Operating margin (no FX needed - same currency ratio)
                CASE WHEN f.total_revenue > 0
                    THEN f.operating_income::DECIMAL / f.total_revenue
                    ELSE NULL END AS operating_margin,

                -- Non-operating income ratio (no FX needed)
                CASE WHEN f.net_income > 0 AND f.operating_income IS NOT NULL
                    THEN (f.net_income - f.operating_income)::DECIMAL / f.net_income
                    ELSE NULL END AS non_operating_income_ratio,

                -- NAV estimate (converted to trading_currency)
                f.total_equity * ({fx_rate_sql}) AS nav_estimate,

                -- Discount to NAV (with FX conversion)
                CASE WHEN f.total_equity > 0
                    THEN 1 - (p.close_price * f.shares_outstanding)::DECIMAL / (f.total_equity * ({fx_rate_sql}))
                    ELSE NULL END AS nav_discount

            FROM companies_with_data c
            JOIN pit_financials f ON c.financial_symbol = f.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE f.shares_outstanding > 0
              AND p.close_price > 0
              AND f.total_equity > 0
              AND f.total_assets > 0
              -- Price to book < 0.8 (with FX conversion)
              AND p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) < 0.8
              -- Net income positive (not distressed)
              AND f.net_income > 0
              -- Low operating margin OR high non-operating income OR high non-current assets
              AND (
                  (f.total_revenue > 0 AND f.operating_income::DECIMAL / f.total_revenue < 0.10)
                  OR (f.operating_income IS NOT NULL AND f.net_income > 1.5 * f.operating_income)
                  OR (f.total_assets - f.current_assets)::DECIMAL / f.total_assets > 0.70
              )
            ORDER BY
                -- Sort by NAV discount (with FX conversion)
                1 - (p.close_price * f.shares_outstanding)::DECIMAL / (f.total_equity * ({fx_rate_sql})) DESC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            nav_discount = float(row['nav_discount']) if row['nav_discount'] else 0
            p_b = float(row['price_to_book']) if row['price_to_book'] else 0
            non_current_intensity = float(row['non_current_intensity']) if row['non_current_intensity'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,

                # Valuation
                'price_to_book': p_b,
                'nav_discount': nav_discount,
                'nav_estimate': float(row['nav_estimate']) if row['nav_estimate'] else None,

                # Balance sheet
                'total_assets': float(row['total_assets']) if row['total_assets'] else None,
                'total_equity': float(row['total_equity']) if row['total_equity'] else None,
                'non_current_assets': float(row['non_current_assets']) if row['non_current_assets'] else None,
                'cash_and_equivalents': float(row['cash_and_equivalents']) if row['cash_and_equivalents'] else None,
                'total_debt': float(row['total_debt']) if row['total_debt'] else 0,

                # Holding company indicators
                'non_current_intensity': non_current_intensity,
                'operating_margin': float(row['operating_margin']) if row['operating_margin'] else None,
                'non_operating_income_ratio': float(row['non_operating_income_ratio']) if row['non_operating_income_ratio'] else None,

                # Income
                'net_income': float(row['net_income']) if row['net_income'] else None,
                'operating_income': float(row['operating_income']) if row['operating_income'] else None,

                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'is_ttm': row['is_ttm'],
            }

            # Build flags
            flags = []

            # Data source flag
            if row['is_ttm']:
                flags.append("TTM_DATA: Using trailing 12 months (fresher data)")
            else:
                flags.append("ANNUAL_DATA: Using latest annual report")

            if nav_discount >= 0.40:
                flags.append("DEEP_DISCOUNT: 40%+ below NAV")
            elif nav_discount >= 0.30:
                flags.append("SIGNIFICANT_DISCOUNT: 30%+ below NAV")
            elif nav_discount >= 0.20:
                flags.append("NOTABLE_DISCOUNT: 20%+ below NAV")

            if non_current_intensity >= 0.80:
                flags.append("HIGH_NON_CURRENT: 80%+ non-current assets")
            elif non_current_intensity >= 0.70:
                flags.append("NON_CURRENT_HEAVY: 70%+ non-current assets")

            op_margin = metrics.get('operating_margin')
            if op_margin is not None and op_margin < 0.05:
                flags.append("LOW_OPERATING: Holding company profile")

            non_op_ratio = metrics.get('non_operating_income_ratio')
            if non_op_ratio and non_op_ratio > 0.3:
                flags.append("NON_OPERATING_INCOME: 30%+ from investments/subs")

            flags.append("NEEDS_TIER_B: Verify holdings and calculate true NAV")

            # Currency mismatch warning
            self.add_currency_warning(flags, row['report_currency'], row['trading_currency'])

            result = self.create_result(
                company_id=row['company_id'],
                metrics=metrics,
                tier='A',
                flags=flags
            )

            result.company_name = row['company_name']
            result.primary_ticker = row['primary_ticker']

            results.append(result)

        return results

    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate score based on discount and holding company characteristics.
        """
        score = 0.0

        nav_discount = metrics.get('nav_discount', 0) or 0
        non_current_intensity = metrics.get('non_current_intensity', 0) or 0
        op_margin = metrics.get('operating_margin', 0.1) or 0.1
        non_op_ratio = metrics.get('non_operating_income_ratio', 0) or 0

        # NAV discount scoring (up to 35 points)
        if nav_discount >= 0.50:
            score += 35
        elif nav_discount >= 0.40:
            score += 30
        elif nav_discount >= 0.35:
            score += 25
        elif nav_discount >= 0.30:
            score += 20
        elif nav_discount >= 0.25:
            score += 15
        elif nav_discount >= 0.20:
            score += 10

        # Non-current asset intensity - proxy for holding company (up to 25 points)
        if non_current_intensity >= 0.85:
            score += 25
        elif non_current_intensity >= 0.80:
            score += 20
        elif non_current_intensity >= 0.75:
            score += 15
        elif non_current_intensity >= 0.70:
            score += 10
        elif non_current_intensity >= 0.60:
            score += 5

        # Low operating margin indicator (up to 20 points)
        if op_margin < 0.02:
            score += 20  # Very low = classic holding company
        elif op_margin < 0.05:
            score += 15
        elif op_margin < 0.08:
            score += 10
        elif op_margin < 0.10:
            score += 5

        # Non-operating income ratio (up to 20 points)
        # Higher ratio suggests income from investments/subsidiaries
        if non_op_ratio >= 0.50:
            score += 20
        elif non_op_ratio >= 0.40:
            score += 15
        elif non_op_ratio >= 0.30:
            score += 10
        elif non_op_ratio >= 0.20:
            score += 5

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for holding company NAV calculation.
        """
        return f"""Calculate the Net Asset Value and discount for this potential holding company.

Company: {company_name}
Market cap: {metrics.get('market_cap', 'N/A')} ({metrics.get('trading_currency', 'SEK')})
Book value: {metrics.get('total_equity', 'N/A')} ({metrics.get('report_currency', 'SEK')})
Price to book: {metrics.get('price_to_book', 'N/A')}
Current NAV discount (vs book): {(metrics.get('nav_discount', 0) or 0) * 100:.1f}%

Investment characteristics:
- Non-current asset intensity: {(metrics.get('non_current_intensity', 0) or 0) * 100:.1f}% of assets
- Operating margin: {(metrics.get('operating_margin', 0) or 0) * 100:.1f}%
- Non-operating income ratio: {(metrics.get('non_operating_income_ratio', 0) or 0) * 100:.1f}%

From the annual report and notes:

1. PORTFOLIO IDENTIFICATION: List the major holdings
   - Listed equity holdings (with market values if disclosed)
   - Unlisted holdings (with book values)
   - Real estate holdings
   - Other investments

2. NAV CALCULATION: Calculate true Net Asset Value
   - Market value of listed holdings
   - Estimated value of unlisted holdings
   - Plus: Cash and liquid assets
   - Less: Total debt
   - Less: Estimated tax liabilities on gains
   = Net Asset Value

3. DISCOUNT ANALYSIS: Calculate true discount
   - Current market cap vs calculated NAV
   - Historical discount range
   - Reasons for discount (complexity, illiquidity, governance?)

4. CATALYST POTENTIAL: What could close the discount?
   - Asset sales planned?
   - Dividend policy changes?
   - Share buybacks?
   - Strategic alternatives?

Respond in JSON:
{{
  "is_holding_company": "PURE_HOLDING|HYBRID|OPERATING_WITH_INVESTMENTS",
  "major_holdings": [
    {{"name": "string", "type": "LISTED|UNLISTED|REAL_ESTATE", "value": "number", "ownership_pct": "number"}}
  ],
  "calculated_nav": "number",
  "nav_per_share": "number",
  "true_discount_pct": "number",
  "discount_reasons": ["string array"],
  "historical_discount_avg": "number or null",
  "catalysts": ["string array"],
  "summary": "string"
}}"""
