"""
Screen 1: Net-Nets — Below Liquidation Value

Identifies companies trading below their Net Current Asset Value (NCAV).
NCAV = Current Assets - Total Liabilities

This is a classic Benjamin Graham screen. A company trading below NCAV
is theoretically worth more dead (liquidated) than alive at current prices.

Tier: A (fully mechanical) + B (asset quality assessment)
Frequency: Weekly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any
from uuid import UUID

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class NetNetsScreen(BaseScreen):
    """
    Screen 1: Net-Nets — Below Liquidation Value

    Criteria:
    - NCAV (Current Assets - Total Liabilities) is positive
    - Market cap < NCAV (trading below liquidation value)
    - Profitable OR minimal cash burn relative to NCAV

    Graham's original threshold was NCAV/Price > 1.33 (33% discount).

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 1

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: pure SQL/math based on NCAV.

        Returns companies where:
        - NCAV > 0
        - Price < NCAV per share
        - Either profitable or cash burn < 10% of NCAV annually

        Uses point-in-time data based on self.score_date.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use yahoo_financials CTE for richer data
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
                    quarters_count,
                    total_revenue,
                    gross_profit,
                    operating_income,
                    net_income,
                    total_assets,
                    current_assets,
                    cash_and_equivalents,
                    accounts_receivable,
                    inventory,
                    total_liabilities,
                    current_liabilities,
                    total_debt,
                    total_equity,
                    shares_outstanding,
                    free_cash_flow
                FROM yahoo_combined_financials
            ),
            pit_prices AS (
                -- Get latest price on or before score_date
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
                    cm.yahoo_symbol,
                    cm.sector,
                    cm.currency AS trading_currency,
                    COALESCE(cm.report_currency, cm.currency) AS report_currency,
                    -- Convert ticker format: VOLV-B -> VOLV B for yahoo_financials
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
                c.report_currency,

                -- Price data
                p.close_price AS price,
                p.price_date,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Financial data
                f.period_date AS financial_date,
                f.is_ttm,
                f.report_currency AS yahoo_report_currency,
                f.shares_outstanding,

                -- Market cap (price * shares, in trading_currency)
                p.close_price * f.shares_outstanding AS market_cap,

                -- NCAV calculation (converted to trading_currency)
                f.current_assets * ({fx_rate_sql}) AS current_assets,
                f.cash_and_equivalents * ({fx_rate_sql}) AS cash_and_equivalents,
                f.accounts_receivable * ({fx_rate_sql}) AS accounts_receivable,
                f.inventory * ({fx_rate_sql}) AS inventory,
                f.total_liabilities * ({fx_rate_sql}) AS total_liabilities,
                (f.current_assets - f.total_liabilities) * ({fx_rate_sql}) AS ncav,
                CASE WHEN f.shares_outstanding > 0
                    THEN ((f.current_assets - f.total_liabilities) * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding
                    ELSE NULL END AS ncav_per_share,

                -- Profitability (converted to trading_currency)
                f.net_income * ({fx_rate_sql}) AS net_income,
                f.free_cash_flow * ({fx_rate_sql}) AS free_cash_flow,

                -- NCAV quality metrics (ratio, no conversion needed)
                CASE WHEN (f.current_assets - f.total_liabilities) > 0
                    THEN f.cash_and_equivalents::DECIMAL / (f.current_assets - f.total_liabilities)
                    ELSE NULL END AS cash_pct_of_ncav

            FROM companies_with_data c
            JOIN pit_financials f ON c.financial_symbol = f.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE f.current_assets IS NOT NULL
              AND f.total_liabilities IS NOT NULL
              AND f.shares_outstanding > 0
              AND p.close_price > 0
              -- Core net-net criteria (with FX conversion)
              AND (f.current_assets - f.total_liabilities) > 0                    -- NCAV positive
              AND ((f.current_assets - f.total_liabilities) * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding > p.close_price  -- Below NCAV (converted)
              AND (
                  f.net_income > 0                                                 -- Profitable
                  OR f.net_income > -0.1 * (f.current_assets - f.total_liabilities) -- Or burn < 10% NCAV
              )
            ORDER BY
                (((f.current_assets - f.total_liabilities) * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) / NULLIF(p.close_price, 0) DESC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            ncav = float(row['ncav']) if row['ncav'] else 0
            ncav_per_share = float(row['ncav_per_share']) if row['ncav_per_share'] else 0
            price = float(row['price']) if row['price'] else 0
            ncav_to_price = ncav_per_share / price if price > 0 else 0

            # Extract asset components for haircut calculation
            cash = float(row['cash_and_equivalents']) if row['cash_and_equivalents'] else 0
            receivables = float(row['accounts_receivable']) if row['accounts_receivable'] else 0
            inventory = float(row['inventory']) if row['inventory'] else 0
            current_assets = float(row['current_assets']) if row['current_assets'] else 0
            total_liabilities = float(row['total_liabilities']) if row['total_liabilities'] else 0
            shares = int(row['shares_outstanding']) if row['shares_outstanding'] else 0

            # Calculate "other current assets" (prepaid expenses, etc.)
            other_current = current_assets - cash - receivables - inventory

            # Graham-style haircuts for liquidation value:
            # - Cash: 100% (fully liquid)
            # - Receivables: 75% (some uncollectible)
            # - Inventory: 50% (fire sale discount)
            # - Other current: 50% (conservative)
            discounted_current_assets = (
                cash * 1.00 +
                receivables * 0.75 +
                inventory * 0.50 +
                max(0, other_current) * 0.50  # Don't let negative other drag it down
            )
            discounted_ncav = discounted_current_assets - total_liabilities
            discounted_ncav_per_share = discounted_ncav / shares if shares > 0 else 0
            discounted_ncav_to_price = discounted_ncav_per_share / price if price > 0 else 0

            # Calculate how much the ratio drops after haircuts
            ncav_ratio_drop = ncav_to_price - discounted_ncav_to_price if ncav_to_price > 0 else 0
            ncav_ratio_drop_pct = (ncav_ratio_drop / ncav_to_price * 100) if ncav_to_price > 0 else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': price,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'shares_outstanding': shares,
                'ncav': ncav,
                'ncav_per_share': ncav_per_share,
                'ncav_to_price_ratio': ncav_to_price,
                # Graham haircut metrics (secondary)
                'discounted_ncav': discounted_ncav,
                'discounted_ncav_per_share': discounted_ncav_per_share,
                'discounted_ncav_to_price_ratio': discounted_ncav_to_price,
                'ncav_ratio_drop_pct': ncav_ratio_drop_pct,
                # Asset breakdown
                'current_assets': current_assets,
                'cash_and_equivalents': cash,
                'accounts_receivable': receivables,
                'inventory': inventory,
                'other_current_assets': other_current,
                'total_liabilities': total_liabilities,
                'net_income': float(row['net_income']) if row['net_income'] else None,
                'free_cash_flow': float(row['free_cash_flow']) if row['free_cash_flow'] else None,
                'cash_pct_of_ncav': float(row['cash_pct_of_ncav']) if row['cash_pct_of_ncav'] else None,
                'inventory_pct_of_current': (inventory / current_assets * 100) if current_assets > 0 else 0,
                'receivables_pct_of_current': (receivables / current_assets * 100) if current_assets > 0 else 0,
                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'is_ttm': row['is_ttm'],
                'is_profitable': row['net_income'] is not None and row['net_income'] > 0,
            }

            # Build flags
            flags = []

            # Data source flag
            if row['is_ttm']:
                flags.append("TTM_DATA: Using trailing 12 months (fresher data)")
            else:
                flags.append("ANNUAL_DATA: Using latest annual report")

            if ncav_to_price >= 1.50:
                flags.append("DEEP_DISCOUNT: Trading at 50%+ discount to NCAV")
            elif ncav_to_price >= 1.33:
                flags.append("GRAHAM_THRESHOLD: Trading at 33%+ discount to NCAV")

            cash_pct = metrics.get('cash_pct_of_ncav') or 0
            if cash_pct >= 0.50:
                flags.append("HIGH_QUALITY_NCAV: Cash is 50%+ of NCAV")

            # Graham haircut quality assessment
            if discounted_ncav_to_price >= 1.33:
                flags.append(f"SURVIVES_HAIRCUTS: Still {discounted_ncav_to_price:.2f}x after Graham discounts")
            elif discounted_ncav_to_price >= 1.0:
                flags.append(f"PASSES_HAIRCUTS: {discounted_ncav_to_price:.2f}x after discounts (marginal)")
            elif discounted_ncav_to_price > 0:
                flags.append(f"FAILS_HAIRCUTS: Only {discounted_ncav_to_price:.2f}x after discounts - inventory dependent")
            else:
                flags.append("FAILS_HAIRCUTS: Negative discounted NCAV - very inventory dependent")

            # Inventory concentration warning
            inv_pct = metrics.get('inventory_pct_of_current') or 0
            if inv_pct >= 40:
                flags.append(f"INVENTORY_HEAVY: {inv_pct:.0f}% of current assets is inventory")
            elif inv_pct >= 25:
                flags.append(f"MODERATE_INVENTORY: {inv_pct:.0f}% of current assets is inventory")

            if not metrics['is_profitable']:
                flags.append("WARNING: Not profitable, relying on burn rate")

            # Currency mismatch warning
            self.add_currency_warning(flags, row['report_currency'], row['trading_currency'])

            result = self.create_result(
                company_id=row['company_id'],
                metrics=metrics,
                tier='A',
                flags=flags
            )

            # Add company info to result
            result.company_name = row['company_name']
            result.primary_ticker = row['primary_ticker']

            results.append(result)

        return results

    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate score based on:
        - NCAV/Price ratio (higher = better)
        - Discounted NCAV/Price (Graham haircuts applied)
        - Profitability
        - Cash quality of NCAV
        """
        score = 0.0

        ncav_ratio = metrics.get('ncav_to_price_ratio', 0) or 0
        discounted_ratio = metrics.get('discounted_ncav_to_price_ratio', 0) or 0
        is_profitable = metrics.get('is_profitable', False)
        cash_pct = metrics.get('cash_pct_of_ncav', 0) or 0
        inv_pct = metrics.get('inventory_pct_of_current', 0) or 0

        # NCAV to price ratio scoring (up to 40 points, reduced from 50)
        # Ratio of 1.0 means trading at NCAV (barely qualifies)
        # Ratio of 1.33 is Graham's threshold
        # Ratio of 2.0+ is exceptional
        if ncav_ratio >= 2.0:
            score += 40
        elif ncav_ratio >= 1.50:
            score += 32
        elif ncav_ratio >= 1.33:
            score += 24
        elif ncav_ratio >= 1.20:
            score += 16
        elif ncav_ratio >= 1.0:
            score += 8

        # Discounted NCAV quality (up to 20 points) - NEW
        # Does it still pass after Graham haircuts?
        if discounted_ratio >= 1.33:
            score += 20  # Still a great net-net after haircuts
        elif discounted_ratio >= 1.0:
            score += 12  # Passes but marginal
        elif discounted_ratio > 0:
            score += 4   # Relies on inventory
        # else: 0 points - negative discounted NCAV

        # Profitability (15 points, reduced from 20)
        if is_profitable:
            score += 15

        # Cash quality of NCAV (up to 15 points, reduced from 20)
        # Higher cash % means more reliable liquidation value
        if cash_pct >= 0.70:
            score += 15
        elif cash_pct >= 0.50:
            score += 12
        elif cash_pct >= 0.30:
            score += 8
        elif cash_pct >= 0.10:
            score += 4

        # Bonus for exceptional cases (up to 10 points)
        if ncav_ratio >= 1.33 and is_profitable:
            score += 5  # Graham's ideal: discount + profitable

        if discounted_ratio >= 1.33 and is_profitable:
            score += 5  # Survives haircuts AND profitable - best quality

        # Inventory concentration penalty (0 to -10 points)
        if inv_pct >= 50:
            score -= 10  # Very inventory dependent
        elif inv_pct >= 40:
            score -= 6
        elif inv_pct >= 30:
            score -= 3

        return self.clamp_score(score)

    def should_run_tier_b(self, metrics: Dict[str, Any], score: float) -> bool:
        """
        All net-nets should get Tier B analysis to verify asset quality.
        Net-nets are rare and warrant deeper investigation.
        """
        return True

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for asset quality assessment.
        """
        return f"""Review the balance sheet notes for this company and assess the quality
of current assets:

Company: {company_name}
Current assets breakdown:
- Cash: {metrics.get('cash_and_equivalents', 'N/A')} ({metrics.get('report_currency', 'SEK')})
- Receivables: {metrics.get('accounts_receivable', 'N/A')} ({metrics.get('report_currency', 'SEK')})
- Inventory: {metrics.get('inventory', 'N/A')} ({metrics.get('report_currency', 'SEK')})
- Total current assets: {metrics.get('current_assets', 'N/A')} ({metrics.get('report_currency', 'SEK')})
Total liabilities: {metrics.get('total_liabilities', 'N/A')} ({metrics.get('report_currency', 'SEK')})
NCAV: {metrics.get('ncav', 'N/A')} ({metrics.get('report_currency', 'SEK')})
Market cap: {metrics.get('market_cap', 'N/A')} ({metrics.get('trading_currency', 'SEK')})

From the financial report notes, answer:
1. RECEIVABLES QUALITY: Is there a significant allowance for doubtful
   accounts? Are receivables concentrated in few customers? Any mention
   of overdue receivables?
2. INVENTORY RISK: Is inventory described as slow-moving, obsolete, or
   requiring write-downs? What type of inventory is it?
3. HIDDEN LIABILITIES: Are there significant off-balance-sheet obligations,
   guarantees, or contingent liabilities in the notes?
4. CASH BURN RATE: Based on the last 4 quarters, how quickly is the
   company consuming cash? How many quarters of runway at current burn?

Respond in JSON:
{{
  "receivables_quality": "HIGH|MEDIUM|LOW",
  "receivables_concerns": "string or null",
  "inventory_risk": "HIGH|MEDIUM|LOW",
  "inventory_concerns": "string or null",
  "hidden_liabilities_amount": "number or null",
  "cash_runway_quarters": "number",
  "overall_ncav_reliability": "HIGH|MEDIUM|LOW",
  "adjusted_ncav_estimate": "number",
  "summary": "string"
}}"""
