"""
Screen 5: Distressed Stable Earners — Temporary Margin Compression

Identifies quality businesses with historically good returns that are
experiencing temporary margin compression. These are "coiled springs" —
good businesses at depressed valuations due to temporary headwinds.

Tier: A + B
Frequency: Monthly

Criteria (from spec):
1. Historical ROIC > 12% (5yr avg - proven quality)
2. Current margin at least 3pp below historical avg (temporarily distressed)
3. Still profitable (net income > 0)
4. FREE CASH FLOW > 0 (must be generating real cash, not just accounting profits)
5. Revenue stable (YoY decline < 15%)
6. P/E < 12 (NOT 15 - this is a distressed screen)
7. Consistently profitable (4 of 5 years, ideally 5/5 - it's a STABLE earner)

This screen is stricter than Screen 7 (Compressed Fundamentals) because
we're buying on financial metrics alone. Screen 7 uses LLM to identify
specific temporary factors.

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class DistressedStableEarnersScreen(BaseScreen):
    """
    Screen 5: Distressed Stable Earners — Temporary Margin Compression

    Criteria (from spec - STRICT):
    - Historical ROIC > 12% (5-year average, proving quality)
    - Current margin at least 3pp below 5-year average (distressed)
    - Net income > 0 (still profitable)
    - FREE CASH FLOW > 0 (generating real cash)
    - Revenue stable (YoY decline < 15%)
    - P/E < 12 (deeply distressed valuation - NOT 15)
    - Profitable 4+ of last 5 years (STABLE earner, not volatile)

    The key insight: This is a STRICT screen for genuinely cheap,
    genuinely stable businesses. P/E < 12 ensures real distress.
    FCF > 0 ensures cash generation, not just accounting profits.

    Differentiation from Screen 7:
    - Screen 5: Strict financial metrics, no LLM needed
    - Screen 7: Looser metrics, LLM identifies specific temporary factors

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 5

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find distressed stable earners.

        Uses yahoo_financials for richer data including:
        - Invested capital directly from Yahoo
        - Working capital calculations
        - TTM preferred, annual fallback
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        financial_filter = self.get_financial_date_filter('fs')

        # Get yahoo_financials CTE for richer data
        combined_cte = self.get_yahoo_combined_financials_cte()

        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, mh.report_currency)', 'c.trading_currency')

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
                    -- Operating margin
                    CASE WHEN total_revenue > 0
                        THEN operating_income::DECIMAL / total_revenue
                        ELSE NULL END AS operating_margin,
                    -- Net margin
                    CASE WHEN total_revenue > 0
                        THEN net_income::DECIMAL / total_revenue
                        ELSE NULL END AS net_margin,
                    -- ROIC (operating income / invested capital)
                    CASE WHEN (total_assets - current_liabilities) > 0
                        THEN operating_income::DECIMAL / (total_assets - current_liabilities)
                        ELSE NULL END AS roic,
                    -- ROE
                    CASE WHEN total_equity > 0
                        THEN net_income::DECIMAL / total_equity
                        ELSE NULL END AS roe
                FROM yahoo_combined_financials
            ),
            -- Historical annual data for 5-year averages and profitability tracking
            -- Uses yahoo_financials for consistent symbol matching with current data
            historical_annual AS (
                SELECT
                    yf.symbol,
                    yf.period_date,
                    yf.fiscal_year,
                    (yf.income_statement->>'total_revenue')::NUMERIC AS total_revenue,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.income_statement->>'operating_income')::NUMERIC AS operating_income,
                    (yf.balance_sheet->>'total_assets')::NUMERIC AS total_assets,
                    (yf.balance_sheet->>'current_liabilities')::NUMERIC AS current_liabilities,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    -- Operating margin
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC /
                             (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS operating_margin,
                    -- Net margin
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'net_income')::NUMERIC /
                             (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS net_margin,
                    -- ROIC (using invested_capital from Yahoo if available)
                    CASE WHEN COALESCE((yf.balance_sheet->>'invested_capital')::NUMERIC,
                                       (yf.balance_sheet->>'total_assets')::NUMERIC -
                                       (yf.balance_sheet->>'current_liabilities')::NUMERIC) > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC /
                             COALESCE((yf.balance_sheet->>'invested_capital')::NUMERIC,
                                      (yf.balance_sheet->>'total_assets')::NUMERIC -
                                      (yf.balance_sheet->>'current_liabilities')::NUMERIC)
                        ELSE NULL END AS roic,
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
            margin_history AS (
                -- Combine current (TTM/annual) with historical annual for averages
                SELECT
                    cf.symbol,
                    -- Current period data (TTM or latest annual)
                    EXTRACT(YEAR FROM cf.period_date)::INT AS year_current,
                    cf.period_date AS latest_period_date,
                    cf.report_currency,
                    cf.is_ttm,
                    cf.total_revenue AS revenue_current,
                    cf.net_income AS net_income_current,
                    cf.operating_income AS operating_income_current,
                    cf.operating_margin AS operating_margin_current,
                    cf.net_margin AS net_margin_current,
                    cf.roic AS roic_current,
                    cf.roe AS roe_current,
                    cf.shares_outstanding AS shares_current,
                    cf.total_debt AS total_debt_current,
                    cf.total_equity AS total_equity_current,
                    cf.free_cash_flow AS fcf_current,

                    -- Prior year revenue (for YoY comparison) from historical annual
                    ha1.total_revenue AS revenue_1yr_ago,

                    -- 5-year averages from historical annual (ha1-ha5)
                    (COALESCE(ha1.operating_margin, 0) + COALESCE(ha2.operating_margin, 0) + COALESCE(ha3.operating_margin, 0) + COALESCE(ha4.operating_margin, 0) + COALESCE(ha5.operating_margin, 0)) /
                        NULLIF(
                            CASE WHEN ha1.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.operating_margin IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_operating_margin_5yr,
                    (COALESCE(ha1.net_margin, 0) + COALESCE(ha2.net_margin, 0) + COALESCE(ha3.net_margin, 0) + COALESCE(ha4.net_margin, 0) + COALESCE(ha5.net_margin, 0)) /
                        NULLIF(
                            CASE WHEN ha1.net_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.net_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.net_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.net_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.net_margin IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_net_margin_5yr,
                    (COALESCE(ha1.roic, 0) + COALESCE(ha2.roic, 0) + COALESCE(ha3.roic, 0) + COALESCE(ha4.roic, 0) + COALESCE(ha5.roic, 0)) /
                        NULLIF(
                            CASE WHEN ha1.roic IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.roic IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.roic IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.roic IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.roic IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_roic_5yr,
                    (COALESCE(ha1.roe, 0) + COALESCE(ha2.roe, 0) + COALESCE(ha3.roe, 0) + COALESCE(ha4.roe, 0) + COALESCE(ha5.roe, 0)) /
                        NULLIF(
                            CASE WHEN ha1.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.roe IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.roe IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_roe_5yr,

                    -- Peak historical margins
                    GREATEST(ha1.operating_margin, ha2.operating_margin, ha3.operating_margin, ha4.operating_margin, ha5.operating_margin) AS peak_operating_margin,
                    GREATEST(ha1.roic, ha2.roic, ha3.roic, ha4.roic, ha5.roic) AS peak_roic,

                    -- 3-year average net income (for inflation guard)
                    (COALESCE(ha1.net_income, 0) + COALESCE(ha2.net_income, 0) + COALESCE(ha3.net_income, 0)) /
                        NULLIF(
                            CASE WHEN ha1.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.net_income IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.net_income IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_net_income_3yr,

                    -- Count profitable years (for stability check)
                    CASE WHEN ha1.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha2.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha3.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha4.net_income > 0 THEN 1 ELSE 0 END +
                    CASE WHEN ha5.net_income > 0 THEN 1 ELSE 0 END AS profitable_years_count,

                    -- Total years of data
                    CASE WHEN ha1.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha2.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha3.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha4.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha5.symbol IS NOT NULL THEN 1 ELSE 0 END AS total_years_count

                FROM current_financials cf
                LEFT JOIN historical_annual ha1 ON cf.symbol = ha1.symbol AND ha1.rn = 1
                LEFT JOIN historical_annual ha2 ON cf.symbol = ha2.symbol AND ha2.rn = 2
                LEFT JOIN historical_annual ha3 ON cf.symbol = ha3.symbol AND ha3.rn = 3
                LEFT JOIN historical_annual ha4 ON cf.symbol = ha4.symbol AND ha4.rn = 4
                LEFT JOIN historical_annual ha5 ON cf.symbol = ha5.symbol AND ha5.rn = 5
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
                    COALESCE(cm.report_currency, cm.currency) AS report_currency,
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

                -- Current financials
                mh.year_current AS fiscal_year,
                mh.latest_period_date AS financial_date,
                mh.report_currency AS yahoo_report_currency,
                mh.is_ttm,
                mh.shares_current,
                mh.revenue_current * ({fx_rate_sql}) AS revenue_current,
                mh.net_income_current * ({fx_rate_sql}) AS net_income_current,
                mh.operating_income_current * ({fx_rate_sql}) AS operating_income_current,
                mh.fcf_current * ({fx_rate_sql}) AS fcf_current,
                mh.total_debt_current * ({fx_rate_sql}) AS total_debt_current,
                mh.total_equity_current * ({fx_rate_sql}) AS total_equity_current,

                -- Current margins (ratios, no conversion needed)
                mh.operating_margin_current,
                mh.net_margin_current,
                mh.roic_current,
                mh.roe_current,

                -- Historical averages (ratios, no conversion needed)
                mh.avg_operating_margin_5yr,
                mh.avg_net_margin_5yr,
                mh.avg_roic_5yr,
                mh.avg_roe_5yr,
                mh.peak_operating_margin,
                mh.peak_roic,

                -- Margin compression (negative = compressed)
                mh.operating_margin_current - mh.avg_operating_margin_5yr AS margin_compression,
                mh.roic_current - mh.avg_roic_5yr AS roic_compression,

                -- Revenue stability (ratio, no conversion needed)
                CASE WHEN mh.revenue_1yr_ago > 0
                    THEN (mh.revenue_current - mh.revenue_1yr_ago)::DECIMAL / mh.revenue_1yr_ago
                    ELSE NULL END AS revenue_yoy,

                -- Profitability track record
                mh.profitable_years_count,
                mh.total_years_count,

                -- 3-year average net income (converted for display)
                mh.avg_net_income_3yr * ({fx_rate_sql}) AS avg_net_income_3yr,

                -- Market cap and EV (with FX conversion)
                p.close_price * mh.shares_current AS market_cap,
                p.close_price * mh.shares_current + COALESCE(mh.total_debt_current * ({fx_rate_sql}), 0) AS enterprise_value,

                -- Valuation ratios (with FX conversion)
                CASE WHEN mh.net_income_current > 0
                    THEN (p.close_price * mh.shares_current)::DECIMAL / (mh.net_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,
                -- Use operating_income as EBIT (operating income ≈ EBIT)
                CASE WHEN mh.operating_income_current > 0
                    THEN (p.close_price * mh.shares_current + COALESCE(mh.total_debt_current * ({fx_rate_sql}), 0))::DECIMAL / (mh.operating_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS ev_to_ebit,

                -- Price to normalized earnings (with FX conversion)
                CASE WHEN mh.avg_net_margin_5yr > 0 AND mh.revenue_current > 0
                    THEN (p.close_price * mh.shares_current)::DECIMAL /
                         (mh.revenue_current * ({fx_rate_sql}) * mh.avg_net_margin_5yr)
                    ELSE NULL END AS pe_normalized

            FROM companies_with_data c
            JOIN margin_history mh ON c.financial_symbol = mh.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            -- Exclude cyclicals - they don't belong on "stable earner" screens
            LEFT JOIN bsd_company_classifications cc ON c.company_id = cc.company_id
            WHERE mh.shares_current > 0
              -- CYCLICAL EXCLUSION: Oil, mining, shipping, forestry etc. have volatile margins by nature
              AND (cc.classification IS NULL OR cc.classification != 'CYCLICAL')
              AND p.close_price > 0
              AND mh.revenue_current > 0
              -- SPEC FILTER 1: Historical ROIC > 12% (quality business)
              AND mh.avg_roic_5yr > 0.12
              -- SPEC FILTER 2: At least 4 years of history
              AND mh.total_years_count >= 4
              -- SPEC FILTER 3: STABLE earner - profitable 4+ of 5 years (was 3/5)
              AND mh.profitable_years_count >= 4
              -- SPEC FILTER 4: Current margin at least 3pp below historical average
              AND mh.operating_margin_current < mh.avg_operating_margin_5yr - 0.03
              -- SPEC FILTER 5: Still profitable (net income > 0, NOT minimal loss)
              AND mh.net_income_current > 0
              -- SPEC FILTER 6: FREE CASH FLOW > 0 (real cash generation)
              AND mh.fcf_current > 0
              -- SPEC FILTER 7: Revenue not collapsing (< 15% decline YoY)
              AND (mh.revenue_1yr_ago IS NULL
                   OR mh.revenue_current >= mh.revenue_1yr_ago * 0.85)
              -- SPEC FILTER 8: P/E < 12 (distressed valuation, with FX conversion)
              AND (p.close_price * mh.shares_current)::DECIMAL / (mh.net_income_current * ({fx_rate_sql})) < 12
            ORDER BY
                -- Sort by margin compression (most compressed first)
                mh.operating_margin_current - mh.avg_operating_margin_5yr ASC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            margin_compression = float(row['margin_compression']) if row['margin_compression'] else 0
            avg_roic = float(row['avg_roic_5yr']) if row['avg_roic_5yr'] else 0
            pe = float(row['pe_ratio']) if row['pe_ratio'] else None
            pe_normalized = float(row['pe_normalized']) if row['pe_normalized'] else None

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'enterprise_value': float(row['enterprise_value']) if row['enterprise_value'] else None,

                # Current margins
                'operating_margin_current': float(row['operating_margin_current']) if row['operating_margin_current'] else None,
                'net_margin_current': float(row['net_margin_current']) if row['net_margin_current'] else None,
                'roic_current': float(row['roic_current']) if row['roic_current'] else None,
                'roe_current': float(row['roe_current']) if row['roe_current'] else None,

                # Historical averages
                'avg_operating_margin_5yr': float(row['avg_operating_margin_5yr']) if row['avg_operating_margin_5yr'] else None,
                'avg_net_margin_5yr': float(row['avg_net_margin_5yr']) if row['avg_net_margin_5yr'] else None,
                'avg_roic_5yr': avg_roic,
                'avg_roe_5yr': float(row['avg_roe_5yr']) if row['avg_roe_5yr'] else None,
                'peak_operating_margin': float(row['peak_operating_margin']) if row['peak_operating_margin'] else None,
                'peak_roic': float(row['peak_roic']) if row['peak_roic'] else None,

                # Compression metrics
                'margin_compression': margin_compression,
                'roic_compression': float(row['roic_compression']) if row['roic_compression'] else None,

                # Revenue stability
                'revenue_yoy': float(row['revenue_yoy']) if row['revenue_yoy'] else None,

                # Profitability track record
                'profitable_years_count': int(row['profitable_years_count']) if row['profitable_years_count'] else 0,
                'total_years_count': int(row['total_years_count']) if row['total_years_count'] else 0,

                # Valuations
                'pe_ratio': pe,
                'ev_to_ebit': float(row['ev_to_ebit']) if row['ev_to_ebit'] else None,
                'pe_normalized': pe_normalized,

                # Other
                'free_cash_flow': float(row['fcf_current']) if row['fcf_current'] else None,
                'net_income': float(row['net_income_current']) if row['net_income_current'] else None,
                'avg_net_income_3yr': float(row['avg_net_income_3yr']) if row['avg_net_income_3yr'] else None,
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

            # Compression severity flags
            if margin_compression <= -0.08:
                flags.append("DEEP_DISTRESS: Margins 8pp+ below normal")
            elif margin_compression <= -0.05:
                flags.append("SIGNIFICANT_COMPRESSION: Margins 5pp+ below normal")

            # Quality flags
            if avg_roic >= 0.20:
                flags.append("HIGH_QUALITY: Historical ROIC 20%+")
            elif avg_roic >= 0.15:
                flags.append("GOOD_QUALITY: Historical ROIC 15%+")

            # Valuation flags
            if pe_normalized and pe_normalized < 5:
                flags.append("VERY_CHEAP: P/E normalized under 5")
            elif pe_normalized and pe_normalized < 8:
                flags.append("CHEAP: P/E normalized under 8")
            elif pe_normalized and pe_normalized < 12:
                flags.append("REASONABLE: P/E normalized under 12")

            # Recovery signals
            if row['profitable_years_count'] == row['total_years_count']:
                flags.append("CONSISTENT: Profitable every year")

            fcf = metrics.get('free_cash_flow')
            if fcf and fcf > 0 and metrics.get('net_income_current', 0) <= 0:
                flags.append("FCF_POSITIVE: Still generating cash despite loss")

            # INFLATION GUARD: Warn if current earnings are significantly above 3-year average
            # This catches companies where a one-off gain makes P/E look artificially low
            net_income_current = float(row['net_income_current']) if row['net_income_current'] else 0
            avg_net_income_3yr = float(row['avg_net_income_3yr']) if row['avg_net_income_3yr'] else 0
            if avg_net_income_3yr > 0 and net_income_current > avg_net_income_3yr * 1.5:
                inflation_ratio = net_income_current / avg_net_income_3yr
                flags.insert(0, f"⚠️ EARNINGS_INFLATED: Current NI is {inflation_ratio:.1f}x 3yr avg - possible one-off gain")

            # Short history warning
            total_years = metrics.get('total_years_count', 0) or 0
            if total_years > 0 and total_years < 5:
                flags.append(f"SHORT_HISTORY: Only {total_years} years of data available")

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
        Calculate score based on quality, compression, and valuation.
        """
        score = 0.0

        avg_roic = metrics.get('avg_roic_5yr', 0) or 0
        margin_compression = metrics.get('margin_compression', 0) or 0
        pe_normalized = metrics.get('pe_normalized', 20) or 20
        ev_ebit = metrics.get('ev_to_ebit', 15) or 15
        profitable_years = metrics.get('profitable_years_count', 0) or 0
        total_years = metrics.get('total_years_count', 5) or 5

        # Historical quality scoring (up to 25 points)
        if avg_roic >= 0.25:
            score += 25
        elif avg_roic >= 0.20:
            score += 22
        elif avg_roic >= 0.15:
            score += 18
        elif avg_roic >= 0.12:
            score += 12

        # Margin compression scoring (up to 25 points)
        # More compressed = higher score (more upside potential)
        if margin_compression <= -0.10:
            score += 25
        elif margin_compression <= -0.07:
            score += 20
        elif margin_compression <= -0.05:
            score += 15
        elif margin_compression <= -0.03:
            score += 10

        # Normalized valuation scoring (up to 20 points)
        if pe_normalized < 5:
            score += 20
        elif pe_normalized < 7:
            score += 16
        elif pe_normalized < 9:
            score += 12
        elif pe_normalized < 12:
            score += 8

        # EV/EBIT scoring (up to 15 points)
        if ev_ebit < 6:
            score += 15
        elif ev_ebit < 8:
            score += 12
        elif ev_ebit < 10:
            score += 8
        elif ev_ebit < 12:
            score += 5

        # Consistency scoring (up to 15 points)
        profitability_rate = profitable_years / total_years if total_years > 0 else 0
        if profitability_rate >= 1.0:
            score += 15
        elif profitability_rate >= 0.8:
            score += 12
        elif profitability_rate >= 0.6:
            score += 8

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for margin compression analysis.
        """
        return f"""Analyze why this company's margins are compressed and assess recovery potential.

Company: {company_name}
Current operating margin: {(metrics.get('operating_margin_current', 0) or 0) * 100:.1f}%
Historical avg operating margin (5yr): {(metrics.get('avg_operating_margin_5yr', 0) or 0) * 100:.1f}%
Margin compression: {(metrics.get('margin_compression', 0) or 0) * 100:.1f}pp
Historical ROIC (5yr avg): {(metrics.get('avg_roic_5yr', 0) or 0) * 100:.1f}%
Current ROIC: {(metrics.get('roic_current', 0) or 0) * 100:.1f}%

From the latest reports, determine:
1. COMPRESSION CAUSE: What is driving margin compression?
   - Input cost inflation?
   - Competitive pricing pressure?
   - One-time costs/restructuring?
   - Investment in growth?
   - Currency headwinds?

2. MANAGEMENT ACTION: What is management doing about it?
   - Cost cutting programs?
   - Price increases announced?
   - Capacity adjustments?

3. STRUCTURAL vs TEMPORARY: Is this compression:
   - Temporary (industry cycle, one-time costs)?
   - Permanent (structural industry change, lost competitive position)?

4. RECOVERY TIMELINE: What does management guidance suggest?
   - Any specific margin targets mentioned?
   - Timeline for normalization?

5. BALANCE SHEET STRENGTH: Can they survive the downturn?
   - Debt levels relative to cash generation
   - Covenant headroom

Respond in JSON:
{{
  "compression_causes": ["string array"],
  "primary_cause": "COST_INFLATION|PRICING_PRESSURE|INVESTMENTS|RESTRUCTURING|CURRENCY|OTHER",
  "management_actions": ["string array"],
  "structural_or_temporary": "STRUCTURAL|TEMPORARY|UNCERTAIN",
  "recovery_timeline_quarters": "number or null",
  "margin_target_mentioned": "number or null",
  "balance_sheet_risk": "LOW|MEDIUM|HIGH",
  "recovery_probability": "HIGH|MEDIUM|LOW",
  "summary": "string"
}}"""
