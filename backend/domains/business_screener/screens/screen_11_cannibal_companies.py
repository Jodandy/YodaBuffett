"""
Screen 11: Cannibal Companies — Buyback Compounders

Identifies companies aggressively buying back their own shares, funded by
strong free cash flow. These companies are "eating themselves" by reducing
share count, concentrating ownership for remaining shareholders.

Tier: A (fully mechanical)
Frequency: Quarterly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class CannibalCompaniesScreen(BaseScreen):
    """
    Screen 11: Cannibal Companies — Buyback Compounders

    Criteria:
    - Share count reduced 3%+ in last year
    - FCF yield > 8% (strong cash generation)
    - Free cash flow positive (buybacks funded by FCF, not debt)
    - Consistent share count decline (2+ consecutive years)

    The best cannibals are cheap (high FCF yield), shrinking fast,
    and funding buybacks from operations rather than debt.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 11

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find aggressive share repurchasers.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, sh.report_currency)', 'c.trading_currency')

        query = f"""
            WITH pit_financials AS (
                -- Get annual financials from yahoo_financials
                SELECT
                    yf.symbol,
                    yf.period_date,
                    yf.fiscal_year,
                    yf.currency AS report_currency,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.income_statement->>'basic_average_shares')::NUMERIC AS shares_outstanding,
                    (yf.balance_sheet->>'total_debt')::NUMERIC AS total_debt,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    (yf.cash_flow->>'free_cash_flow')::NUMERIC AS free_cash_flow,
                    (yf.cash_flow->>'operating_cash_flow')::NUMERIC AS operating_cash_flow
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '5 years'
            ),
            share_history AS (
                -- Get share counts for current, 1yr ago, 2yr ago, 3yr ago
                SELECT
                    symbol,
                    MAX(CASE WHEN rn = 1 THEN fiscal_year END) AS year_current,
                    MAX(CASE WHEN rn = 1 THEN shares_outstanding END) AS shares_current,
                    MAX(CASE WHEN rn = 2 THEN shares_outstanding END) AS shares_1yr_ago,
                    MAX(CASE WHEN rn = 3 THEN shares_outstanding END) AS shares_2yr_ago,
                    MAX(CASE WHEN rn = 4 THEN shares_outstanding END) AS shares_3yr_ago,
                    MAX(CASE WHEN rn = 1 THEN free_cash_flow END) AS fcf_current,
                    MAX(CASE WHEN rn = 1 THEN net_income END) AS net_income_current,
                    MAX(CASE WHEN rn = 1 THEN total_debt END) AS total_debt_current,
                    MAX(CASE WHEN rn = 1 THEN total_equity END) AS total_equity_current,
                    MAX(CASE WHEN rn = 1 THEN report_currency END) AS report_currency,
                    MAX(CASE WHEN rn = 1 THEN period_date END) AS latest_period_date
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_year DESC) AS rn
                    FROM pit_financials
                    WHERE shares_outstanding > 0
                ) ranked
                WHERE rn <= 4
                GROUP BY symbol
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

                -- Share history
                sh.year_current AS fiscal_year,
                sh.latest_period_date AS financial_date,
                sh.report_currency AS yahoo_report_currency,
                sh.shares_current,
                sh.shares_1yr_ago,
                sh.shares_2yr_ago,
                sh.shares_3yr_ago,

                -- Buyback metrics (share counts are currency-agnostic)
                CASE WHEN sh.shares_1yr_ago > 0
                    THEN (sh.shares_1yr_ago - sh.shares_current)::DECIMAL / sh.shares_1yr_ago
                    ELSE 0 END AS buyback_yield_1yr,
                CASE WHEN sh.shares_2yr_ago > 0
                    THEN (sh.shares_2yr_ago - sh.shares_current)::DECIMAL / sh.shares_2yr_ago
                    ELSE 0 END AS buyback_yield_2yr,
                CASE WHEN sh.shares_3yr_ago > 0
                    THEN (sh.shares_3yr_ago - sh.shares_current)::DECIMAL / sh.shares_3yr_ago
                    ELSE 0 END AS buyback_yield_3yr,

                -- Consistency check
                CASE WHEN sh.shares_current < sh.shares_1yr_ago
                          AND sh.shares_1yr_ago < sh.shares_2yr_ago
                     THEN TRUE ELSE FALSE END AS consistent_2yr_decline,
                CASE WHEN sh.shares_current < sh.shares_1yr_ago
                          AND sh.shares_1yr_ago < sh.shares_2yr_ago
                          AND sh.shares_2yr_ago < sh.shares_3yr_ago
                     THEN TRUE ELSE FALSE END AS consistent_3yr_decline,

                -- Market cap (in trading_currency) and FCF (converted to trading_currency)
                p.close_price * sh.shares_current AS market_cap,
                sh.fcf_current * ({fx_rate_sql}) AS free_cash_flow,
                sh.net_income_current * ({fx_rate_sql}) AS net_income,

                -- FCF yield (with FX conversion)
                CASE WHEN p.close_price * sh.shares_current > 0 AND sh.fcf_current IS NOT NULL
                    THEN (sh.fcf_current * ({fx_rate_sql}))::DECIMAL / (p.close_price * sh.shares_current)
                    ELSE 0 END AS fcf_yield,

                -- P/E (with FX conversion)
                CASE WHEN sh.net_income_current > 0
                    THEN (p.close_price * sh.shares_current)::DECIMAL / (sh.net_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,

                -- Debt metrics (converted to trading_currency)
                sh.total_debt_current * ({fx_rate_sql}) AS total_debt,
                sh.total_equity_current * ({fx_rate_sql}) AS total_equity

            FROM companies_with_data c
            JOIN share_history sh ON c.financial_symbol = sh.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE sh.shares_current > 0
              AND sh.shares_1yr_ago > 0
              AND p.close_price > 0
              -- 3%+ buyback yield in last year
              AND (sh.shares_1yr_ago - sh.shares_current)::DECIMAL / sh.shares_1yr_ago > 0.03
              -- FCF yield > 8% (with FX conversion)
              AND sh.fcf_current > 0
              AND (sh.fcf_current * ({fx_rate_sql}))::DECIMAL / (p.close_price * sh.shares_current) > 0.08
              -- Shares actually declining (not just current < 1yr ago due to stock splits)
              AND sh.shares_current < sh.shares_1yr_ago
              -- At least 2 consecutive years of decline
              AND sh.shares_1yr_ago < sh.shares_2yr_ago
            ORDER BY
                (sh.shares_1yr_ago - sh.shares_current)::DECIMAL / sh.shares_1yr_ago DESC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            buyback_1yr = float(row['buyback_yield_1yr']) if row['buyback_yield_1yr'] else 0
            buyback_2yr = float(row['buyback_yield_2yr']) if row['buyback_yield_2yr'] else 0
            fcf_yield = float(row['fcf_yield']) if row['fcf_yield'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'shares_current': int(row['shares_current']) if row['shares_current'] else None,
                'shares_1yr_ago': int(row['shares_1yr_ago']) if row['shares_1yr_ago'] else None,
                'shares_2yr_ago': int(row['shares_2yr_ago']) if row['shares_2yr_ago'] else None,
                'buyback_yield_1yr': buyback_1yr,
                'buyback_yield_2yr': buyback_2yr,
                'buyback_yield_3yr': float(row['buyback_yield_3yr']) if row['buyback_yield_3yr'] else 0,
                'consistent_2yr_decline': row['consistent_2yr_decline'],
                'consistent_3yr_decline': row['consistent_3yr_decline'],
                'free_cash_flow': float(row['free_cash_flow']) if row['free_cash_flow'] else None,
                'fcf_yield': fcf_yield,
                'pe_ratio': float(row['pe_ratio']) if row['pe_ratio'] else None,
                'net_income': float(row['net_income']) if row['net_income'] else None,
                'total_debt': float(row['total_debt']) if row['total_debt'] else 0,
                'total_equity': float(row['total_equity']) if row['total_equity'] else None,
                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],
            }

            # Build flags
            flags = []
            if buyback_1yr >= 0.08:
                flags.append("AGGRESSIVE: 8%+ buyback yield")
            elif buyback_1yr >= 0.05:
                flags.append("STRONG: 5%+ buyback yield")

            if fcf_yield >= 0.15:
                flags.append("HIGH_FCF: 15%+ FCF yield")
            elif fcf_yield >= 0.12:
                flags.append("GOOD_FCF: 12%+ FCF yield")

            if row['consistent_3yr_decline']:
                flags.append("CONSISTENT: 3yr share decline")
            elif row['consistent_2yr_decline']:
                flags.append("TRENDING: 2yr share decline")

            pe = metrics.get('pe_ratio')
            if pe and pe < 10:
                flags.append("CHEAP: P/E under 10")

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
        Calculate score based on buyback intensity and FCF quality.
        """
        score = 0.0

        buyback_1yr = metrics.get('buyback_yield_1yr', 0) or 0
        fcf_yield = metrics.get('fcf_yield', 0) or 0
        consistent_3yr = metrics.get('consistent_3yr_decline', False)
        consistent_2yr = metrics.get('consistent_2yr_decline', False)
        pe = metrics.get('pe_ratio', 20) or 20

        # Buyback yield scoring (up to 30 points)
        if buyback_1yr >= 0.10:
            score += 30
        elif buyback_1yr >= 0.08:
            score += 25
        elif buyback_1yr >= 0.05:
            score += 20
        elif buyback_1yr >= 0.03:
            score += 10

        # FCF yield scoring (up to 25 points)
        if fcf_yield >= 0.20:
            score += 25
        elif fcf_yield >= 0.15:
            score += 20
        elif fcf_yield >= 0.12:
            score += 15
        elif fcf_yield >= 0.08:
            score += 10

        # Consistency scoring (up to 20 points)
        if consistent_3yr:
            score += 20
        elif consistent_2yr:
            score += 12

        # Valuation (P/E) scoring (up to 15 points)
        if pe < 8:
            score += 15
        elif pe < 10:
            score += 12
        elif pe < 12:
            score += 8
        elif pe < 15:
            score += 5

        # Bonus for extreme cases (up to 10 points)
        if buyback_1yr >= 0.08 and fcf_yield >= 0.15:
            score += 10  # Aggressive buyer with high FCF

        return self.clamp_score(score)
