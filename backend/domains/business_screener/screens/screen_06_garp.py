"""
Screen 6: Growth at Reasonable Prices (GARP)

Identifies companies with demonstrated (not hypothetical) growth that are
still reasonably priced. This is the Peter Lynch approach - finding growth
without overpaying for it.

Tier: A + light B
Frequency: Monthly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class GARPScreen(BaseScreen):
    """
    Screen 6: Growth at Reasonable Prices — Demonstrated, Not Hypothetical

    Criteria:
    - Revenue CAGR > 15% over 3 years (proven growth)
    - Gross margin stable or improving (not buying growth)
    - Operating cash flow positive (real earnings)
    - PEG ratio < 1 OR EV/EBIT < 15 (reasonable price)
    - ROIC > 15% (quality growth)

    The key insight: we want companies that HAVE grown, not ones that
    promise to grow. Gross margin stability proves they're not sacrificing
    profitability for growth.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 6

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: GARP criteria.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, gm.report_currency)', 'c.trading_currency')

        query = f"""
            WITH historical_annual AS (
                -- Get 6 years of annual financials from yahoo_financials for growth calculations
                SELECT
                    yf.symbol,
                    yf.period_date,
                    yf.fiscal_year,
                    yf.currency AS report_currency,
                    (yf.income_statement->>'total_revenue')::NUMERIC AS total_revenue,
                    (yf.income_statement->>'gross_profit')::NUMERIC AS gross_profit,
                    (yf.income_statement->>'operating_income')::NUMERIC AS operating_income,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.income_statement->>'ebit')::NUMERIC AS ebit,
                    (yf.balance_sheet->>'total_assets')::NUMERIC AS total_assets,
                    (yf.balance_sheet->>'current_assets')::NUMERIC AS current_assets,
                    (yf.balance_sheet->>'current_liabilities')::NUMERIC AS current_liabilities,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    (yf.balance_sheet->>'total_debt')::NUMERIC AS total_debt,
                    (yf.income_statement->>'basic_average_shares')::NUMERIC AS shares_outstanding,
                    (yf.cash_flow->>'operating_cash_flow')::NUMERIC AS operating_cash_flow,
                    (yf.cash_flow->>'free_cash_flow')::NUMERIC AS free_cash_flow,
                    -- Gross margin
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'gross_profit')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS gross_margin,
                    -- ROIC
                    CASE WHEN ((yf.balance_sheet->>'total_assets')::NUMERIC - COALESCE((yf.balance_sheet->>'current_liabilities')::NUMERIC, 0)) > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / ((yf.balance_sheet->>'total_assets')::NUMERIC - COALESCE((yf.balance_sheet->>'current_liabilities')::NUMERIC, 0))
                        ELSE NULL END AS roic
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '7 years'
            ),
            pit_financials AS (
                -- Latest annual financials only (for current period data)
                SELECT * FROM historical_annual
                WHERE period_date >= '{self._score_date}'::date - INTERVAL '18 months'
            ),
            growth_metrics AS (
                -- Calculate growth rates and margin trends using historical data
                SELECT
                    symbol,
                    -- Current year data
                    MAX(CASE WHEN rn = 1 THEN fiscal_year END) AS year_current,
                    MAX(CASE WHEN rn = 1 THEN period_date END) AS latest_period_date,
                    MAX(CASE WHEN rn = 1 THEN report_currency END) AS report_currency,
                    MAX(CASE WHEN rn = 1 THEN total_revenue END) AS revenue_current,
                    MAX(CASE WHEN rn = 1 THEN net_income END) AS net_income_current,
                    MAX(CASE WHEN rn = 1 THEN operating_income END) AS operating_income_current,
                    MAX(CASE WHEN rn = 1 THEN ebit END) AS ebit_current,
                    MAX(CASE WHEN rn = 1 THEN gross_margin END) AS gross_margin_current,
                    MAX(CASE WHEN rn = 1 THEN roic END) AS roic_current,
                    MAX(CASE WHEN rn = 1 THEN operating_cash_flow END) AS ocf_current,
                    MAX(CASE WHEN rn = 1 THEN free_cash_flow END) AS fcf_current,
                    MAX(CASE WHEN rn = 1 THEN shares_outstanding END) AS shares_current,
                    MAX(CASE WHEN rn = 1 THEN total_debt END) AS total_debt_current,
                    MAX(CASE WHEN rn = 1 THEN total_equity END) AS total_equity_current,
                    -- 3 years ago data
                    MAX(CASE WHEN rn = 4 THEN total_revenue END) AS revenue_3yr_ago,
                    MAX(CASE WHEN rn = 4 THEN net_income END) AS net_income_3yr_ago,
                    MAX(CASE WHEN rn = 4 THEN gross_margin END) AS gross_margin_3yr_ago,
                    -- 5 years ago data
                    MAX(CASE WHEN rn = 6 THEN total_revenue END) AS revenue_5yr_ago,
                    MAX(CASE WHEN rn = 6 THEN net_income END) AS net_income_5yr_ago,
                    -- Average ROIC over 5 years
                    AVG(roic) FILTER (WHERE rn <= 5) AS avg_roic_5yr
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_year DESC) AS rn
                    FROM historical_annual
                ) ranked
                WHERE rn <= 6
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

                -- Growth metrics
                gm.year_current AS fiscal_year,
                gm.latest_period_date AS financial_date,
                gm.report_currency AS yahoo_report_currency,
                gm.shares_current,
                gm.revenue_current * ({fx_rate_sql}) AS revenue_current,
                gm.net_income_current * ({fx_rate_sql}) AS net_income_current,
                gm.operating_income_current * ({fx_rate_sql}) AS operating_income_current,
                gm.ebit_current * ({fx_rate_sql}) AS ebit_current,
                gm.ocf_current * ({fx_rate_sql}) AS ocf_current,
                gm.fcf_current * ({fx_rate_sql}) AS fcf_current,
                gm.total_debt_current * ({fx_rate_sql}) AS total_debt_current,
                gm.total_equity_current * ({fx_rate_sql}) AS total_equity_current,

                -- Margins (ratios, no conversion needed)
                gm.gross_margin_current,
                gm.gross_margin_3yr_ago,
                gm.gross_margin_current - COALESCE(gm.gross_margin_3yr_ago, gm.gross_margin_current) AS gross_margin_change,

                -- Returns (ratios, no conversion needed)
                gm.roic_current,
                gm.avg_roic_5yr,

                -- Market cap and EV (with FX conversion)
                p.close_price * gm.shares_current AS market_cap,
                p.close_price * gm.shares_current + COALESCE(gm.total_debt_current * ({fx_rate_sql}), 0) AS enterprise_value,

                -- Growth rates (3-year CAGR) - same currency ratios, no conversion needed
                CASE WHEN gm.revenue_3yr_ago > 0 AND gm.revenue_current > 0
                    THEN POWER(gm.revenue_current::DECIMAL / gm.revenue_3yr_ago, 1.0/3) - 1
                    ELSE NULL END AS revenue_cagr_3yr,
                CASE WHEN gm.net_income_3yr_ago > 0 AND gm.net_income_current > 0
                    THEN POWER(gm.net_income_current::DECIMAL / gm.net_income_3yr_ago, 1.0/3) - 1
                    ELSE NULL END AS earnings_cagr_3yr,

                -- 5-year CAGR
                CASE WHEN gm.revenue_5yr_ago > 0 AND gm.revenue_current > 0
                    THEN POWER(gm.revenue_current::DECIMAL / gm.revenue_5yr_ago, 1.0/5) - 1
                    ELSE NULL END AS revenue_cagr_5yr,
                CASE WHEN gm.net_income_5yr_ago > 0 AND gm.net_income_current > 0
                    THEN POWER(gm.net_income_current::DECIMAL / gm.net_income_5yr_ago, 1.0/5) - 1
                    ELSE NULL END AS earnings_cagr_5yr,

                -- P/E ratio (with FX conversion)
                CASE WHEN gm.net_income_current > 0
                    THEN (p.close_price * gm.shares_current)::DECIMAL / (gm.net_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,

                -- EV/EBIT (with FX conversion)
                CASE WHEN gm.ebit_current > 0
                    THEN (p.close_price * gm.shares_current + COALESCE(gm.total_debt_current * ({fx_rate_sql}), 0))::DECIMAL / (gm.ebit_current * ({fx_rate_sql}))
                    ELSE NULL END AS ev_to_ebit

            FROM companies_with_data c
            JOIN growth_metrics gm ON c.financial_symbol = gm.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE gm.shares_current > 0
              AND p.close_price > 0
              AND gm.revenue_current > 0
              AND gm.revenue_3yr_ago > 0
              -- Revenue CAGR > 15% over 3 years
              AND POWER(gm.revenue_current::DECIMAL / gm.revenue_3yr_ago, 1.0/3) - 1 > 0.15
              -- Gross margin stable or improving (not declining more than 2pp)
              AND (gm.gross_margin_current IS NULL
                   OR gm.gross_margin_3yr_ago IS NULL
                   OR gm.gross_margin_current >= gm.gross_margin_3yr_ago - 0.02)
              -- Operating cash flow positive
              AND gm.ocf_current > 0
              -- ROIC > 15% (average over 5 years, or current if limited history)
              AND COALESCE(gm.avg_roic_5yr, gm.roic_current) > 0.15
              -- Valuation: PEG < 1 OR EV/EBIT < 15 (with FX conversion)
              AND (
                  -- PEG < 1 (P/E divided by earnings growth rate as percentage)
                  (gm.net_income_current > 0
                   AND gm.net_income_3yr_ago > 0
                   AND POWER(gm.net_income_current::DECIMAL / gm.net_income_3yr_ago, 1.0/3) - 1 > 0.05
                   AND ((p.close_price * gm.shares_current)::DECIMAL / (gm.net_income_current * ({fx_rate_sql})))
                       / ((POWER(gm.net_income_current::DECIMAL / gm.net_income_3yr_ago, 1.0/3) - 1) * 100) < 1)
                  OR
                  -- EV/EBIT < 15 (with FX conversion)
                  (gm.ebit_current > 0
                   AND (p.close_price * gm.shares_current + COALESCE(gm.total_debt_current * ({fx_rate_sql}), 0))::DECIMAL / (gm.ebit_current * ({fx_rate_sql})) < 15)
              )
            ORDER BY
                -- Sort by PEG ratio (lower is better)
                COALESCE(
                    CASE WHEN gm.net_income_current > 0
                              AND gm.net_income_3yr_ago > 0
                              AND POWER(gm.net_income_current::DECIMAL / gm.net_income_3yr_ago, 1.0/3) - 1 > 0.05
                         THEN ((p.close_price * gm.shares_current)::DECIMAL / (gm.net_income_current * ({fx_rate_sql})))
                              / ((POWER(gm.net_income_current::DECIMAL / gm.net_income_3yr_ago, 1.0/3) - 1) * 100)
                         ELSE 99 END,
                    99
                ) ASC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            revenue_cagr = float(row['revenue_cagr_3yr']) if row['revenue_cagr_3yr'] else 0
            earnings_cagr = float(row['earnings_cagr_3yr']) if row['earnings_cagr_3yr'] else 0
            pe = float(row['pe_ratio']) if row['pe_ratio'] else None
            peg = pe / (earnings_cagr * 100) if pe and earnings_cagr and earnings_cagr > 0.05 else None

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'enterprise_value': float(row['enterprise_value']) if row['enterprise_value'] else None,
                'revenue_cagr_3yr': revenue_cagr,
                'earnings_cagr_3yr': earnings_cagr,
                'revenue_cagr_5yr': float(row['revenue_cagr_5yr']) if row['revenue_cagr_5yr'] else None,
                'earnings_cagr_5yr': float(row['earnings_cagr_5yr']) if row['earnings_cagr_5yr'] else None,
                'gross_margin_current': float(row['gross_margin_current']) if row['gross_margin_current'] else None,
                'gross_margin_3yr_ago': float(row['gross_margin_3yr_ago']) if row['gross_margin_3yr_ago'] else None,
                'gross_margin_change': float(row['gross_margin_change']) if row['gross_margin_change'] else None,
                'roic_current': float(row['roic_current']) if row['roic_current'] else None,
                'avg_roic_5yr': float(row['avg_roic_5yr']) if row['avg_roic_5yr'] else None,
                'pe_ratio': pe,
                'peg_ratio': peg,
                'ev_to_ebit': float(row['ev_to_ebit']) if row['ev_to_ebit'] else None,
                'operating_cash_flow': float(row['ocf_current']) if row['ocf_current'] else None,
                'free_cash_flow': float(row['fcf_current']) if row['fcf_current'] else None,
                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],
            }

            # Build flags
            flags = []

            # Data source flag (GARP uses annual data for growth calculations)
            flags.append("ANNUAL_GROWTH: Using annual data for growth CAGR")

            if peg and peg < 0.5:
                flags.append("VERY_CHEAP: PEG under 0.5")
            elif peg and peg < 0.75:
                flags.append("CHEAP: PEG under 0.75")

            if revenue_cagr >= 0.25:
                flags.append("HIGH_GROWTH: 25%+ revenue CAGR")
            elif revenue_cagr >= 0.20:
                flags.append("STRONG_GROWTH: 20%+ revenue CAGR")

            gm_change = metrics.get('gross_margin_change')
            if gm_change and gm_change > 0.02:
                flags.append("IMPROVING_MARGINS: GM expanding")

            roic = metrics.get('avg_roic_5yr') or metrics.get('roic_current')
            if roic and roic > 0.25:
                flags.append("HIGH_ROIC: 25%+ returns")

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
        Calculate score based on growth quality and valuation.
        """
        score = 0.0

        revenue_cagr = metrics.get('revenue_cagr_3yr', 0) or 0
        peg = metrics.get('peg_ratio')
        ev_ebit = metrics.get('ev_to_ebit', 20) or 20
        roic = metrics.get('avg_roic_5yr') or metrics.get('roic_current', 0) or 0
        gm_change = metrics.get('gross_margin_change', 0) or 0

        # Growth rate scoring (up to 25 points)
        if revenue_cagr >= 0.30:
            score += 25
        elif revenue_cagr >= 0.25:
            score += 22
        elif revenue_cagr >= 0.20:
            score += 18
        elif revenue_cagr >= 0.15:
            score += 12

        # PEG ratio scoring (up to 25 points)
        if peg is not None:
            if peg < 0.5:
                score += 25
            elif peg < 0.75:
                score += 20
            elif peg < 1.0:
                score += 15
            elif peg < 1.5:
                score += 8

        # EV/EBIT scoring (up to 15 points)
        if ev_ebit < 8:
            score += 15
        elif ev_ebit < 10:
            score += 12
        elif ev_ebit < 12:
            score += 8
        elif ev_ebit < 15:
            score += 5

        # ROIC scoring (up to 20 points)
        if roic >= 0.30:
            score += 20
        elif roic >= 0.25:
            score += 16
        elif roic >= 0.20:
            score += 12
        elif roic >= 0.15:
            score += 8

        # Gross margin trend (up to 15 points)
        if gm_change > 0.05:
            score += 15  # Expanding significantly
        elif gm_change > 0.02:
            score += 12  # Expanding
        elif gm_change >= 0:
            score += 8   # Stable
        elif gm_change >= -0.02:
            score += 4   # Slight decline (still acceptable)

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for growth sustainability assessment.
        """
        return f"""Assess the quality and sustainability of this company's growth.

Company: {company_name}
Revenue CAGR (3yr): {metrics.get('revenue_cagr_3yr', 0) * 100:.1f}%
Gross margin trend: {metrics.get('gross_margin_3yr_ago', 0) * 100:.1f}% → {metrics.get('gross_margin_current', 0) * 100:.1f}%
ROIC (5yr avg): {metrics.get('avg_roic_5yr', 0) * 100:.1f}%
PEG ratio: {metrics.get('peg_ratio', 'N/A')}

From the reports, determine:
1. GROWTH SOURCE: Is growth organic or acquisition-driven?
   If acquisitions, what multiple are they paying?
2. CUSTOMER DYNAMICS: Are they adding new customers, expanding
   existing accounts, or raising prices?
3. ADDRESSABLE MARKET: How large is TAM vs current revenue?
4. COMPETITIVE POSITION: Are they gaining or losing market share?
5. DECELERATION RISK: Is quarterly growth rate slowing even if
   3-year CAGR still looks strong?

Respond in JSON:
{{
  "growth_source": "ORGANIC|ACQUISITION|MIXED",
  "organic_growth_pct": "number or null",
  "acquisition_multiples": "string or null",
  "customer_growth_driver": "NEW_CUSTOMERS|EXPANSION|PRICING|MIXED",
  "tam_vs_revenue_ratio": "number or null",
  "market_share_trend": "GAINING|STABLE|LOSING",
  "quarterly_growth_decelerating": "boolean",
  "growth_sustainability": "HIGH|MEDIUM|LOW",
  "summary": "string"
}}"""
