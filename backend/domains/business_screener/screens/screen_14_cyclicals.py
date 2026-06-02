"""
Screen 14: Cyclicals — Inverted Screen for Cyclical Companies

Identifies cyclical companies at the trough of their earnings cycle.
For cyclicals, traditional valuation metrics are INVERTED:
- High P/E is GOOD (depressed earnings = trough)
- Low P/B is GOOD (market has given up)
- Low margins vs history is GOOD (reversion expected)

Tier: A + B (needs classification of cyclical vs stable)
Frequency: Monthly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class CyclicalsScreen(BaseScreen):
    """
    Screen 14: Cyclicals — Buy at the Trough

    Criteria:
    - Current operating margin significantly below 7-year average (trough)
    - Price to book < 1.5 (cheap on asset basis)
    - Company has demonstrated cyclicality (margin variance > 5pp)
    - Has survived multiple cycles (4+ years of data, most profitable)
    - Balance sheet can survive downturn (debt/equity < 1.5)
    - Current earnings exist (not losing money catastrophically)

    The key insight: cyclical companies revert to mid-cycle earnings.
    When margins are at trough, forward P/E on normalized earnings is
    actually very low. We want to buy when everyone is pessimistic.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 14

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find cyclicals at trough.

        Uses yahoo_financials for richer data including:
        - More accurate EBITDA directly from Yahoo
        - Invested capital for ROIC calculations
        - TTM preferred, annual fallback for 7-year cycle analysis
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        financial_filter = self.get_financial_date_filter('fs')
        # IMPORTANT: Use c.report_currency from company_master (correct), not cm.report_currency
        # from yahoo_financials (often incorrect for USD-reporting companies on Oslo exchange)
        fx_rate_sql = self.get_fx_rate_sql('c.report_currency', 'c.trading_currency')

        # Get yahoo_financials CTE for richer data
        combined_cte = self.get_yahoo_combined_financials_cte()

        query = f"""
            WITH {combined_cte},
            -- Current period: TTM preferred, annual fallback
            current_financials AS (
                SELECT
                    symbol,
                    period_date,
                    report_currency,
                    is_ttm,
                    total_revenue,
                    gross_profit,
                    operating_income,
                    net_income,
                    ebitda,
                    total_assets,
                    current_assets,
                    current_liabilities,
                    total_debt,
                    total_equity,
                    shares_outstanding,
                    operating_cash_flow,
                    free_cash_flow,
                    -- Margins
                    CASE WHEN total_revenue > 0
                        THEN operating_income::DECIMAL / total_revenue
                        ELSE NULL END AS operating_margin,
                    CASE WHEN total_revenue > 0
                        THEN ebitda::DECIMAL / total_revenue
                        ELSE NULL END AS ebitda_margin,
                    -- ROE
                    CASE WHEN total_equity > 0
                        THEN net_income::DECIMAL / total_equity
                        ELSE NULL END AS roe
                FROM yahoo_combined_financials
            ),
            -- Historical annual data for 7-year cycle analysis
            -- Uses yahoo_financials for consistent symbol matching with current data
            historical_annual AS (
                SELECT
                    yf.symbol,
                    yf.period_date,
                    yf.fiscal_year,
                    (yf.income_statement->>'total_revenue')::NUMERIC AS total_revenue,
                    (yf.income_statement->>'operating_income')::NUMERIC AS operating_income,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.income_statement->>'ebitda')::NUMERIC AS ebitda,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    -- Margins
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC /
                             (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS operating_margin,
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'ebitda')::NUMERIC /
                             (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS ebitda_margin,
                    -- ROE
                    CASE WHEN (yf.balance_sheet->>'stockholders_equity')::NUMERIC > 0
                        THEN (yf.income_statement->>'net_income')::NUMERIC /
                             (yf.balance_sheet->>'stockholders_equity')::NUMERIC
                        ELSE NULL END AS roe,
                    ROW_NUMBER() OVER (PARTITION BY yf.symbol ORDER BY yf.period_date DESC) AS rn
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '7 years'
            ),
            cycle_metrics AS (
                -- Combine current (TTM/annual) with historical annual for cycle analysis
                SELECT
                    cf.symbol,
                    -- Current period data (TTM or latest annual)
                    EXTRACT(YEAR FROM cf.period_date)::INT AS year_current,
                    cf.period_date AS latest_period_date,
                    cf.report_currency,
                    cf.is_ttm,
                    cf.total_revenue AS revenue_current,
                    cf.operating_income AS operating_income_current,
                    cf.net_income AS net_income_current,
                    cf.ebitda AS ebitda_current,
                    cf.operating_margin AS operating_margin_current,
                    cf.ebitda_margin AS ebitda_margin_current,
                    cf.roe AS roe_current,
                    cf.shares_outstanding AS shares_current,
                    cf.total_debt AS total_debt_current,
                    cf.total_equity AS total_equity_current,
                    cf.total_assets AS total_assets_current,

                    -- 7-year averages from historical annual (mid-cycle proxy)
                    (COALESCE(ha1.operating_margin, 0) + COALESCE(ha2.operating_margin, 0) + COALESCE(ha3.operating_margin, 0) +
                     COALESCE(ha4.operating_margin, 0) + COALESCE(ha5.operating_margin, 0) + COALESCE(ha6.operating_margin, 0) + COALESCE(ha7.operating_margin, 0)) /
                        NULLIF(
                            CASE WHEN ha1.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha6.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha7.operating_margin IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_operating_margin_7yr,
                    (COALESCE(ha1.ebitda_margin, 0) + COALESCE(ha2.ebitda_margin, 0) + COALESCE(ha3.ebitda_margin, 0) +
                     COALESCE(ha4.ebitda_margin, 0) + COALESCE(ha5.ebitda_margin, 0) + COALESCE(ha6.ebitda_margin, 0) + COALESCE(ha7.ebitda_margin, 0)) /
                        NULLIF(
                            CASE WHEN ha1.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha6.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha7.ebitda_margin IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_ebitda_margin_7yr,
                    (COALESCE(ha1.roe, 0) + COALESCE(ha2.roe, 0) + COALESCE(ha3.roe, 0) +
                     COALESCE(ha4.roe, 0) + COALESCE(ha5.roe, 0) + COALESCE(ha6.roe, 0) + COALESCE(ha7.roe, 0)) /
                        NULLIF(
                            CASE WHEN ha1.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha6.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha7.roe IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_roe_7yr,
                    (COALESCE(ha1.net_income, 0) + COALESCE(ha2.net_income, 0) + COALESCE(ha3.net_income, 0) +
                     COALESCE(ha4.net_income, 0) + COALESCE(ha5.net_income, 0) + COALESCE(ha6.net_income, 0) + COALESCE(ha7.net_income, 0)) /
                        NULLIF(
                            CASE WHEN ha1.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha6.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha7.net_income IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_net_income_7yr,

                    -- Peak and trough margins (to confirm cyclicality)
                    GREATEST(ha1.operating_margin, ha2.operating_margin, ha3.operating_margin, ha4.operating_margin, ha5.operating_margin, ha6.operating_margin, ha7.operating_margin) AS peak_operating_margin,
                    LEAST(ha1.operating_margin, ha2.operating_margin, ha3.operating_margin, ha4.operating_margin, ha5.operating_margin, ha6.operating_margin, ha7.operating_margin) AS trough_operating_margin,

                    -- Margin volatility approximation (simplified stddev)
                    -- Using range as a proxy: (max - min) / 4 ≈ stddev for uniform distribution
                    (GREATEST(ha1.operating_margin, ha2.operating_margin, ha3.operating_margin, ha4.operating_margin, ha5.operating_margin, ha6.operating_margin, ha7.operating_margin) -
                     LEAST(ha1.operating_margin, ha2.operating_margin, ha3.operating_margin, ha4.operating_margin, ha5.operating_margin, ha6.operating_margin, ha7.operating_margin)) / 4.0 AS margin_volatility,

                    -- Years of data
                    CASE WHEN ha1.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha2.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha3.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha4.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha5.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha6.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha7.symbol IS NOT NULL THEN 1 ELSE 0 END AS total_years,

                    -- Profitable years
                    CASE WHEN ha1.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha2.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha3.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha4.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha5.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha6.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha7.net_income > 0 THEN 1 ELSE 0 END AS profitable_years

                FROM current_financials cf
                LEFT JOIN historical_annual ha1 ON cf.symbol = ha1.symbol AND ha1.rn = 1
                LEFT JOIN historical_annual ha2 ON cf.symbol = ha2.symbol AND ha2.rn = 2
                LEFT JOIN historical_annual ha3 ON cf.symbol = ha3.symbol AND ha3.rn = 3
                LEFT JOIN historical_annual ha4 ON cf.symbol = ha4.symbol AND ha4.rn = 4
                LEFT JOIN historical_annual ha5 ON cf.symbol = ha5.symbol AND ha5.rn = 5
                LEFT JOIN historical_annual ha6 ON cf.symbol = ha6.symbol AND ha6.rn = 6
                LEFT JOIN historical_annual ha7 ON cf.symbol = ha7.symbol AND ha7.rn = 7
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
                    cm.report_currency AS report_currency,  -- CORRECT source for FX conversion
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

                -- Price data
                p.close_price AS price,
                p.price_date,

                -- Current financials
                cm.year_current AS fiscal_year,
                cm.latest_period_date AS financial_date,
                cm.report_currency,
                cm.is_ttm,
                cm.shares_current,
                cm.revenue_current,
                cm.operating_income_current,
                cm.net_income_current,
                cm.ebitda_current,
                cm.total_debt_current,
                cm.total_equity_current,
                cm.total_assets_current,

                -- Current margins
                cm.operating_margin_current,
                cm.ebitda_margin_current,
                cm.roe_current,

                -- Historical averages (mid-cycle)
                cm.avg_operating_margin_7yr,
                cm.avg_ebitda_margin_7yr,
                cm.avg_roe_7yr,
                cm.avg_net_income_7yr,

                -- Cycle extremes
                cm.peak_operating_margin,
                cm.trough_operating_margin,
                cm.margin_volatility,

                -- Margin gap (negative = below mid-cycle = trough)
                cm.operating_margin_current - cm.avg_operating_margin_7yr AS margin_gap,

                -- Cycle position estimate (0 = trough, 100 = peak)
                CASE WHEN cm.peak_operating_margin > cm.trough_operating_margin
                    THEN ((cm.operating_margin_current - cm.trough_operating_margin) /
                          (cm.peak_operating_margin - cm.trough_operating_margin)) * 100
                    ELSE 50 END AS cycle_position_pct,

                -- Track record
                cm.total_years,
                cm.profitable_years,

                -- Market cap
                p.close_price * cm.shares_current AS market_cap,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Valuation ratios (with FX conversion)
                CASE WHEN cm.net_income_current > 0
                    THEN (p.close_price * cm.shares_current)::DECIMAL / (cm.net_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,

                CASE WHEN cm.total_equity_current > 0 AND cm.shares_current > 0
                    THEN p.close_price::DECIMAL / ((cm.total_equity_current * ({fx_rate_sql}))::DECIMAL / cm.shares_current)
                    ELSE NULL END AS price_to_book,

                -- Normalized P/E (using mid-cycle earnings, with FX conversion)
                CASE WHEN cm.avg_net_income_7yr > 0
                    THEN (p.close_price * cm.shares_current)::DECIMAL / (cm.avg_net_income_7yr * ({fx_rate_sql}))
                    ELSE NULL END AS normalized_pe,

                -- Debt to equity (no FX - same currency)
                CASE WHEN cm.total_equity_current > 0
                    THEN COALESCE(cm.total_debt_current, 0)::DECIMAL / cm.total_equity_current
                    ELSE NULL END AS debt_to_equity

            FROM companies_with_data c
            JOIN cycle_metrics cm ON c.financial_symbol = cm.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE cm.shares_current > 0
              AND p.close_price > 0
              AND cm.total_equity_current > 0
              -- Has enough history to determine cyclicality (4 years = max available from Yahoo)
              AND cm.total_years >= 4
              -- Demonstrates cyclicality (margin variance > 5pp)
              AND cm.peak_operating_margin - cm.trough_operating_margin > 0.05
              -- Currently at trough (margin at least 3pp below average)
              AND cm.operating_margin_current < cm.avg_operating_margin_7yr - 0.03
              -- But still making some money (not catastrophic loss)
              AND (cm.net_income_current > 0 OR cm.operating_income_current > 0)
              -- Price to book < 1.5 (cheap on assets, with FX conversion)
              AND p.close_price::DECIMAL / ((cm.total_equity_current * ({fx_rate_sql}))::DECIMAL / cm.shares_current) < 1.5
              -- Balance sheet can survive (debt/equity < 1.5)
              AND COALESCE(cm.total_debt_current, 0)::DECIMAL / cm.total_equity_current < 1.5
              -- Has survived cycles (mostly profitable)
              AND cm.profitable_years::DECIMAL / cm.total_years >= 0.6
            ORDER BY
                -- Sort by how deep in trough (most depressed margins first)
                cm.operating_margin_current - cm.avg_operating_margin_7yr ASC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            margin_gap = float(row['margin_gap']) if row['margin_gap'] else 0
            cycle_position = float(row['cycle_position_pct']) if row['cycle_position_pct'] else 50
            p_b = float(row['price_to_book']) if row['price_to_book'] else 0
            normalized_pe = float(row['normalized_pe']) if row['normalized_pe'] else None

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,

                # Current margins
                'operating_margin_current': float(row['operating_margin_current']) if row['operating_margin_current'] else None,
                'ebitda_margin_current': float(row['ebitda_margin_current']) if row['ebitda_margin_current'] else None,
                'roe_current': float(row['roe_current']) if row['roe_current'] else None,

                # Mid-cycle (7yr avg)
                'avg_operating_margin_7yr': float(row['avg_operating_margin_7yr']) if row['avg_operating_margin_7yr'] else None,
                'avg_ebitda_margin_7yr': float(row['avg_ebitda_margin_7yr']) if row['avg_ebitda_margin_7yr'] else None,
                'avg_roe_7yr': float(row['avg_roe_7yr']) if row['avg_roe_7yr'] else None,

                # Cycle metrics
                'peak_operating_margin': float(row['peak_operating_margin']) if row['peak_operating_margin'] else None,
                'trough_operating_margin': float(row['trough_operating_margin']) if row['trough_operating_margin'] else None,
                'margin_volatility': float(row['margin_volatility']) if row['margin_volatility'] else None,
                'margin_gap': margin_gap,
                'cycle_position_pct': cycle_position,

                # Track record
                'total_years': int(row['total_years']) if row['total_years'] else 0,
                'profitable_years': int(row['profitable_years']) if row['profitable_years'] else 0,

                # Valuation
                'pe_ratio': float(row['pe_ratio']) if row['pe_ratio'] else None,
                'price_to_book': p_b,
                'normalized_pe': normalized_pe,

                # Balance sheet
                'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,
                'total_equity': float(row['total_equity_current']) if row['total_equity_current'] else None,

                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],

                # Data freshness indicators
                'is_ttm': row['is_ttm'],
                'data_source': 'TTM (4 quarters)' if row['is_ttm'] else 'Annual report',
            }

            # Build flags
            flags = []

            # Data freshness flag
            if row['is_ttm']:
                flags.append("TTM_DATA: Using trailing 12 months (fresher)")
            else:
                flags.append("ANNUAL_DATA: Using latest annual report")

            if cycle_position < 20:
                flags.append("DEEP_TROUGH: Near cycle low")
            elif cycle_position < 35:
                flags.append("TROUGH: Below mid-cycle")

            if margin_gap < -0.08:
                flags.append("SEVERE_COMPRESSION: Margins 8pp+ below normal")
            elif margin_gap < -0.05:
                flags.append("SIGNIFICANT_COMPRESSION: Margins 5pp+ below normal")

            if p_b < 0.7:
                flags.append("VERY_CHEAP: P/B under 0.7x")
            elif p_b < 1.0:
                flags.append("CHEAP: P/B under 1.0x")

            if normalized_pe and normalized_pe < 8:
                flags.append("CHEAP_NORMALIZED: Normalized P/E under 8")
            elif normalized_pe and normalized_pe < 12:
                flags.append("FAIR_NORMALIZED: Normalized P/E under 12")

            if metrics['profitable_years'] == metrics['total_years']:
                flags.append("SURVIVOR: Profitable through all cycles")

            # Short history warning (cyclicals ideally need 7+ years to capture full cycle)
            total_years = metrics.get('total_years', 0) or 0
            if total_years > 0 and total_years < 7:
                flags.append(f"SHORT_CYCLE_HISTORY: Only {total_years} years - mid-cycle estimate may not cover full commodity cycle")

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
        Calculate score based on cycle position and valuation.
        """
        score = 0.0

        cycle_position = metrics.get('cycle_position_pct', 50) or 50
        margin_gap = metrics.get('margin_gap', 0) or 0
        p_b = metrics.get('price_to_book', 1.5) or 1.5
        normalized_pe = metrics.get('normalized_pe', 15) or 15
        profitable_years = metrics.get('profitable_years', 0) or 0
        total_years = metrics.get('total_years', 5) or 5
        debt_to_equity = metrics.get('debt_to_equity', 1) or 1

        # Cycle position (lower = better, up to 25 points)
        if cycle_position < 15:
            score += 25
        elif cycle_position < 25:
            score += 20
        elif cycle_position < 35:
            score += 15
        elif cycle_position < 45:
            score += 10
        elif cycle_position < 50:
            score += 5

        # Margin compression (more compressed = better, up to 20 points)
        if margin_gap < -0.10:
            score += 20
        elif margin_gap < -0.07:
            score += 16
        elif margin_gap < -0.05:
            score += 12
        elif margin_gap < -0.03:
            score += 8

        # Price to book (up to 20 points)
        if p_b < 0.5:
            score += 20
        elif p_b < 0.7:
            score += 16
        elif p_b < 0.9:
            score += 12
        elif p_b < 1.2:
            score += 8
        elif p_b < 1.5:
            score += 5

        # Normalized P/E (up to 15 points)
        if normalized_pe < 6:
            score += 15
        elif normalized_pe < 8:
            score += 12
        elif normalized_pe < 10:
            score += 9
        elif normalized_pe < 12:
            score += 6
        elif normalized_pe < 15:
            score += 3

        # Survival track record (up to 10 points)
        profitability_rate = profitable_years / total_years if total_years > 0 else 0
        if profitability_rate >= 0.95:
            score += 10
        elif profitability_rate >= 0.85:
            score += 8
        elif profitability_rate >= 0.75:
            score += 5
        elif profitability_rate >= 0.60:
            score += 3

        # Balance sheet strength (up to 10 points)
        if debt_to_equity < 0.3:
            score += 10
        elif debt_to_equity < 0.5:
            score += 8
        elif debt_to_equity < 0.8:
            score += 5
        elif debt_to_equity < 1.0:
            score += 3

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for cycle analysis.
        """
        return f"""Analyze the cyclical nature and recovery potential for this company.

Company: {company_name}
Current operating margin: {(metrics.get('operating_margin_current', 0) or 0) * 100:.1f}%
7-year avg operating margin: {(metrics.get('avg_operating_margin_7yr', 0) or 0) * 100:.1f}%
Margin gap (vs avg): {(metrics.get('margin_gap', 0) or 0) * 100:.1f}pp
Cycle position: {metrics.get('cycle_position_pct', 50):.0f}% (0=trough, 100=peak)
Price to book: {metrics.get('price_to_book', 0):.2f}x
Normalized P/E: {metrics.get('normalized_pe', 'N/A')}

From the latest reports and industry analysis:

1. CYCLE DRIVER: What drives this company's cyclicality?
   - Commodity prices?
   - Economic activity (GDP)?
   - Industry capex cycles?
   - Interest rates?
   - Other?

2. CURRENT CYCLE POSITION: Where are we in the cycle?
   - What's causing current margin compression?
   - How does current compare to previous troughs?
   - What would trigger recovery?

3. RECOVERY CATALYST: What would normalize margins?
   - Industry supply cuts?
   - Demand recovery?
   - Pricing power restoration?
   - Cost restructuring?

4. BALANCE SHEET RISK: Can they survive an extended downturn?
   - Debt maturity schedule?
   - Covenant headroom?
   - Cash generation even at trough?

5. STRUCTURAL CHANGE: Is this cycle or structural decline?
   - Technology disruption risk?
   - Permanent demand destruction?
   - Competitive position shifts?

Respond in JSON:
{{
  "cycle_driver": "COMMODITY|GDP|CAPEX|RATES|OTHER",
  "cycle_driver_detail": "string",
  "current_vs_historical_trough": "WORSE|SIMILAR|BETTER",
  "recovery_catalysts": ["string array"],
  "expected_recovery_quarters": "number or null",
  "balance_sheet_survival": "STRONG|ADEQUATE|AT_RISK",
  "structural_decline_risk": "LOW|MEDIUM|HIGH",
  "is_cyclical_or_structural": "CYCLICAL|STRUCTURAL|UNCERTAIN",
  "summary": "string"
}}"""
