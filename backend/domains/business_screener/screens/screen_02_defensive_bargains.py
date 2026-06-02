"""
Screen 2: Defensive Bargains — Graham's Multi-Factor Safety

Implements Benjamin Graham's defensive investor criteria from "The Intelligent Investor".
These are high-quality, conservatively financed companies trading at reasonable valuations.

Tier: A (fully mechanical)
Frequency: Monthly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class DefensiveBargainsScreen(BaseScreen):
    """
    Screen 2: Defensive Bargains — Graham's Multi-Factor Safety

    Graham's criteria for defensive investors:
    - P/E ratio under 15
    - P/B ratio under 1.5
    - P/E × P/B < 22.5 (combined criterion)
    - Pays dividends
    - 4+ consecutive profitable years (adapted from Graham's 10yr)
    - Current ratio > 1.5 (adequate liquidity)
    - Total debt < Total equity (conservative leverage)
    - Earnings growth of 33%+ over 10 years (or ~3% CAGR)

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 2

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: Graham's defensive criteria.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use yahoo_financials CTE
        combined_cte = self.get_yahoo_combined_financials_cte()

        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, f.report_currency)', 'c.trading_currency')

        query = f"""
            WITH {combined_cte},
            pit_financials AS (
                -- Select from yahoo_combined_financials with dividend data
                SELECT
                    yf.symbol,
                    yf.period_date,
                    yf.report_currency,
                    yf.is_ttm,
                    yf.total_revenue,
                    yf.net_income,
                    yf.total_assets,
                    yf.current_assets,
                    yf.current_liabilities,
                    yf.total_debt,
                    yf.total_equity,
                    yf.shares_outstanding
                FROM yahoo_combined_financials yf
            ),
            -- Get dividend data from yahoo_financials cash_flow (separate CTE for latest value)
            dividend_data AS (
                SELECT DISTINCT ON (yf.symbol)
                    yf.symbol,
                    COALESCE(
                        (yf.cash_flow->>'cash_dividends_paid')::NUMERIC,
                        (yf.cash_flow->>'common_stock_dividend_paid')::NUMERIC
                    ) AS dividends_paid
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                ORDER BY yf.symbol, yf.period_date DESC
            ),
            earnings_history AS (
                -- Check profitability over available history (4-5 years typical, max 10)
                SELECT
                    yf.symbol,
                    COUNT(CASE WHEN (yf.income_statement->>'net_income')::NUMERIC > 0 THEN 1 END) AS profitable_years,
                    COUNT(*) AS total_years,
                    MIN(CASE WHEN yf.fiscal_year = (
                        SELECT MAX(fiscal_year) - 9 FROM yahoo_financials
                        WHERE symbol = yf.symbol AND statement_type = 'annual'
                    ) THEN (yf.income_statement->>'net_income')::NUMERIC END) AS earnings_10yr_ago,
                    MAX(CASE WHEN yf.fiscal_year = (
                        SELECT MAX(fiscal_year) FROM yahoo_financials
                        WHERE symbol = yf.symbol AND statement_type = 'annual'
                    ) THEN (yf.income_statement->>'net_income')::NUMERIC END) AS earnings_latest
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.fiscal_year >= (SELECT MAX(fiscal_year) - 9 FROM yahoo_financials WHERE symbol = yf.symbol)
                GROUP BY yf.symbol
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
                  -- DEFENSIVE SCREEN: Exclude cyclicals (shipping, oil, aquaculture, commodities)
                  -- Cyclicals show low P/E at peak earnings, but these are NOT defensive businesses
                  AND NOT EXISTS (
                      SELECT 1 FROM bsd_company_classifications bcc
                      WHERE bcc.company_id = cm.id
                      AND bcc.classification = 'CYCLICAL'
                  )
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
                f.net_income * ({fx_rate_sql}) AS net_income,
                f.total_revenue * ({fx_rate_sql}) AS total_revenue,
                f.total_equity * ({fx_rate_sql}) AS total_equity,
                f.total_debt * ({fx_rate_sql}) AS total_debt,
                f.current_assets * ({fx_rate_sql}) AS current_assets,
                f.current_liabilities * ({fx_rate_sql}) AS current_liabilities,
                d.dividends_paid * ({fx_rate_sql}) AS dividends_paid,

                -- Market cap (in trading_currency)
                p.close_price * f.shares_outstanding AS market_cap,

                -- Valuation ratios (with FX conversion)
                CASE WHEN f.net_income > 0
                    THEN (p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,

                CASE WHEN f.total_equity > 0 AND f.shares_outstanding > 0
                    THEN p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding)
                    ELSE NULL END AS pb_ratio,

                -- Book value per share (converted to trading_currency)
                CASE WHEN f.shares_outstanding > 0
                    THEN (f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding
                    ELSE NULL END AS book_value_per_share,

                -- Current ratio (ratio, no conversion needed)
                CASE WHEN f.current_liabilities > 0
                    THEN f.current_assets::DECIMAL / f.current_liabilities
                    ELSE NULL END AS current_ratio,

                -- Dividend yield (with FX conversion)
                CASE WHEN p.close_price * f.shares_outstanding > 0 AND d.dividends_paid IS NOT NULL
                    THEN ABS(d.dividends_paid * ({fx_rate_sql}))::DECIMAL / (p.close_price * f.shares_outstanding)
                    ELSE 0 END AS dividend_yield,

                -- Earnings history
                eh.profitable_years,
                eh.total_years,
                eh.earnings_10yr_ago,
                eh.earnings_latest,
                CASE WHEN eh.earnings_10yr_ago > 0 AND eh.earnings_latest > 0
                    THEN (eh.earnings_latest - eh.earnings_10yr_ago)::DECIMAL / eh.earnings_10yr_ago
                    ELSE NULL END AS earnings_growth_10yr

            FROM companies_with_data c
            JOIN pit_financials f ON c.financial_symbol = f.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            LEFT JOIN dividend_data d ON c.financial_symbol = d.symbol
            LEFT JOIN earnings_history eh ON c.financial_symbol = eh.symbol
            WHERE f.shares_outstanding > 0
              AND p.close_price > 0
              AND f.net_income > 0
              AND f.total_equity > 0
              -- P/E under 15 (with FX conversion)
              AND (p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql})) > 0
              AND (p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql})) < 15
              -- P/B under 1.5 (with FX conversion)
              AND p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) > 0
              AND p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) < 1.5
              -- P/E × P/B < 22.5 (with FX conversion)
              AND ((p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql})))
                  * (p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding)) < 22.5
              -- Pays dividend (dividends_paid is negative = cash outflow)
              AND d.dividends_paid IS NOT NULL
              AND d.dividends_paid < 0
              -- Current ratio > 1.5 (ratio, no conversion needed)
              AND f.current_liabilities > 0
              AND f.current_assets::DECIMAL / f.current_liabilities > 1.5
              -- Debt < Equity (same currency ratio)
              AND COALESCE(f.total_debt, 0) < f.total_equity
              -- 4+ profitable years (Graham used 10yr, adapted for data availability)
              AND eh.profitable_years >= 4
            ORDER BY
                ((p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql})))
                * (p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding)) ASC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            pe = float(row['pe_ratio']) if row['pe_ratio'] else 0
            pb = float(row['pb_ratio']) if row['pb_ratio'] else 0
            pe_times_pb = pe * pb if pe and pb else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'pe_ratio': pe,
                'pb_ratio': pb,
                'pe_times_pb': pe_times_pb,
                'book_value_per_share': float(row['book_value_per_share']) if row['book_value_per_share'] else None,
                'current_ratio': float(row['current_ratio']) if row['current_ratio'] else None,
                'dividend_yield': float(row['dividend_yield']) if row['dividend_yield'] else 0,
                'total_debt': float(row['total_debt']) if row['total_debt'] else 0,
                'total_equity': float(row['total_equity']) if row['total_equity'] else None,
                'debt_to_equity': float(row['total_debt']) / float(row['total_equity']) if row['total_equity'] and row['total_debt'] else 0,
                'profitable_years': int(row['profitable_years']) if row['profitable_years'] else 0,
                'total_years': int(row['total_years']) if row['total_years'] else 0,
                'earnings_growth_10yr': float(row['earnings_growth_10yr']) if row['earnings_growth_10yr'] else None,
                'net_income': float(row['net_income']) if row['net_income'] else None,
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

            if pe < 10:
                flags.append("VERY_CHEAP: P/E under 10")
            if pb < 0.8:
                flags.append("DEEP_VALUE: P/B under 0.8")
            if metrics['dividend_yield'] > 0.05:
                flags.append("HIGH_YIELD: Dividend yield over 5%")
            if metrics['current_ratio'] and metrics['current_ratio'] > 2.5:
                flags.append("FORTRESS: Current ratio over 2.5")
            if metrics['profitable_years'] >= metrics.get('total_years', 5):
                flags.append("ALWAYS_PROFITABLE: All years profitable")
            elif metrics['profitable_years'] >= 4:
                flags.append("CONSISTENT: 4+ profitable years")
            if metrics['debt_to_equity'] < 0.3:
                flags.append("LOW_DEBT: Debt/Equity under 30%")

            # Short history warning - critical for defensive screen
            # With only 4-5 years, we can't distinguish "always profitable" from "profitable during upcycle"
            if metrics['total_years'] < 5:
                flags.append(
                    f"SHORT_HISTORY_WARNING: Only {metrics['total_years']} years of data — "
                    f"profitability consistency may not capture full business cycle"
                )
            elif metrics['total_years'] < 10:
                flags.append(f"REDUCED_HISTORY: Graham criteria adapted from 10yr to {metrics['total_years']}yr")

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
        Calculate score based on Graham's criteria quality.
        """
        score = 0.0

        pe = metrics.get('pe_ratio', 15) or 15
        pb = metrics.get('pb_ratio', 1.5) or 1.5
        dividend_yield = metrics.get('dividend_yield', 0) or 0
        profitable_years = metrics.get('profitable_years', 0) or 0
        current_ratio = metrics.get('current_ratio', 1.5) or 1.5
        debt_to_equity = metrics.get('debt_to_equity', 1) or 1

        # P/E scoring (up to 25 points)
        if pe < 8:
            score += 25
        elif pe < 10:
            score += 20
        elif pe < 12:
            score += 15
        elif pe < 15:
            score += 10

        # P/B scoring (up to 25 points)
        if pb < 0.6:
            score += 25
        elif pb < 0.8:
            score += 20
        elif pb < 1.0:
            score += 15
        elif pb < 1.5:
            score += 10

        # Dividend yield (up to 15 points)
        if dividend_yield > 0.06:
            score += 15
        elif dividend_yield > 0.04:
            score += 12
        elif dividend_yield > 0.03:
            score += 8
        elif dividend_yield > 0:
            score += 5

        # Profitable years (up to 15 points) - adapted for 4-5yr data availability
        total_years = metrics.get('total_years', 5) or 5
        if profitable_years >= total_years:  # All years profitable
            score += 15
        elif profitable_years >= 4:
            score += 12
        elif profitable_years >= 3:
            score += 8

        # History length bonus/penalty (up to +10 or -5 points)
        # Longer history = more confidence in "defensive" classification
        if total_years >= 10:
            score += 10  # Full Graham history - very confident
        elif total_years >= 7:
            score += 5   # Good history
        elif total_years >= 5:
            score += 0   # Acceptable
        else:
            score -= 5   # Short history - less confidence in defensive classification

        # Current ratio (up to 10 points)
        if current_ratio > 2.5:
            score += 10
        elif current_ratio > 2.0:
            score += 7
        elif current_ratio > 1.5:
            score += 5

        # Low debt (up to 10 points)
        if debt_to_equity < 0.2:
            score += 10
        elif debt_to_equity < 0.4:
            score += 7
        elif debt_to_equity < 0.6:
            score += 5

        return self.clamp_score(score)
