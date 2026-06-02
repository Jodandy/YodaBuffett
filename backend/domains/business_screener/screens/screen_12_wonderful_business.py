"""
Screen 12: Wonderful Business at Fair Price — Munger's Compounders

Identifies high-quality businesses with durable competitive advantages,
consistent high returns on capital, and strong growth - available at
reasonable (not necessarily cheap) valuations.

Tier: A + C (requires Claude for moat assessment)
Frequency: Quarterly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class WonderfulBusinessScreen(BaseScreen):
    """
    Screen 12: Wonderful Business at Fair Price — Munger's Compounders

    Criteria:
    - ROIC > 15% consistently (5-year average)
    - Gross margin > 40% (pricing power / moat indicator)
    - Revenue CAGR > 5% (growing, not stagnant)
    - Earnings CAGR > 5% (profitable growth)
    - FCF positive and growing
    - Debt to equity < 1 (not over-leveraged)
    - P/E < 25 (fair price, not extreme)
    - PEG ratio < 2 (growth is reasonably priced)

    The key insight: a wonderful business compounds value over time.
    We don't need it to be cheap - just fairly priced. Time is the
    friend of the wonderful business.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 12

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find wonderful businesses.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use company_master.report_currency (correct) instead of yahoo_financials.currency
        # which is often wrong for companies reporting in EUR/USD on Nordic exchanges
        fx_rate_sql = self.get_fx_rate_sql('c.report_currency', 'c.trading_currency')

        query = f"""
            WITH pit_financials AS (
                -- Get annual financials from yahoo_financials
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
                    -- Margins
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'gross_profit')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS gross_margin,
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS operating_margin,
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'net_income')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS net_margin,
                    -- Returns
                    CASE WHEN ((yf.balance_sheet->>'total_assets')::NUMERIC - COALESCE((yf.balance_sheet->>'current_liabilities')::NUMERIC, 0)) > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / ((yf.balance_sheet->>'total_assets')::NUMERIC - COALESCE((yf.balance_sheet->>'current_liabilities')::NUMERIC, 0))
                        ELSE NULL END AS roic,
                    CASE WHEN (yf.balance_sheet->>'stockholders_equity')::NUMERIC > 0
                        THEN (yf.income_statement->>'net_income')::NUMERIC / (yf.balance_sheet->>'stockholders_equity')::NUMERIC
                        ELSE NULL END AS roe
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '7 years'
            ),
            quality_metrics AS (
                -- Calculate quality and growth metrics
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
                    MAX(CASE WHEN rn = 1 THEN operating_margin END) AS operating_margin_current,
                    MAX(CASE WHEN rn = 1 THEN roic END) AS roic_current,
                    MAX(CASE WHEN rn = 1 THEN roe END) AS roe_current,
                    -- Use most recent non-NULL shares (fallback to previous years if current is NULL)
                    COALESCE(
                        MAX(CASE WHEN rn = 1 THEN shares_outstanding END),
                        MAX(CASE WHEN rn = 2 THEN shares_outstanding END),
                        MAX(CASE WHEN rn = 3 THEN shares_outstanding END)
                    ) AS shares_current,
                    MAX(CASE WHEN rn = 1 THEN total_debt END) AS total_debt_current,
                    MAX(CASE WHEN rn = 1 THEN total_equity END) AS total_equity_current,
                    MAX(CASE WHEN rn = 1 THEN free_cash_flow END) AS fcf_current,
                    MAX(CASE WHEN rn = 1 THEN operating_cash_flow END) AS ocf_current,

                    -- 5 years ago data (for CAGR)
                    MAX(CASE WHEN rn = 6 THEN total_revenue END) AS revenue_5yr_ago,
                    MAX(CASE WHEN rn = 6 THEN net_income END) AS net_income_5yr_ago,
                    MAX(CASE WHEN rn = 6 THEN free_cash_flow END) AS fcf_5yr_ago,

                    -- 3 years ago (for shorter CAGR)
                    MAX(CASE WHEN rn = 4 THEN total_revenue END) AS revenue_3yr_ago,
                    MAX(CASE WHEN rn = 4 THEN net_income END) AS net_income_3yr_ago,

                    -- 2 years ago (fallback if 3yr data is NULL)
                    MAX(CASE WHEN rn = 3 THEN total_revenue END) AS revenue_2yr_ago,

                    -- 5-year averages
                    AVG(gross_margin) FILTER (WHERE rn <= 5) AS avg_gross_margin_5yr,
                    AVG(operating_margin) FILTER (WHERE rn <= 5) AS avg_operating_margin_5yr,
                    AVG(roic) FILTER (WHERE rn <= 5) AS avg_roic_5yr,
                    AVG(roe) FILTER (WHERE rn <= 5) AS avg_roe_5yr,

                    -- Consistency checks
                    MIN(roic) FILTER (WHERE rn <= 5) AS min_roic_5yr,
                    MIN(gross_margin) FILTER (WHERE rn <= 5) AS min_gross_margin_5yr,
                    COUNT(*) FILTER (WHERE rn <= 5 AND net_income > 0) AS profitable_years,
                    COUNT(*) FILTER (WHERE rn <= 5 AND free_cash_flow > 0) AS fcf_positive_years,
                    COUNT(*) FILTER (WHERE rn <= 5) AS total_years

                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_year DESC) AS rn
                    FROM pit_financials
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
                    -- Use report_currency from company_master (correct) instead of yahoo_financials
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

                -- Price data
                p.close_price AS price,
                p.price_date,

                -- Current financials
                qm.year_current AS fiscal_year,
                qm.latest_period_date AS financial_date,
                c.report_currency,  -- From company_master (correct)
                qm.shares_current,
                qm.revenue_current,
                qm.net_income_current,
                qm.operating_income_current,
                qm.ebit_current,
                qm.fcf_current,
                qm.ocf_current,
                qm.total_debt_current,
                qm.total_equity_current,

                -- Current margins and returns
                qm.gross_margin_current,
                qm.operating_margin_current,
                qm.roic_current,
                qm.roe_current,

                -- 5-year averages
                qm.avg_gross_margin_5yr,
                qm.avg_operating_margin_5yr,
                qm.avg_roic_5yr,
                qm.avg_roe_5yr,

                -- Consistency
                qm.min_roic_5yr,
                qm.min_gross_margin_5yr,
                qm.profitable_years,
                qm.fcf_positive_years,
                qm.total_years,

                -- Market cap
                p.close_price * qm.shares_current AS market_cap,

                -- Enterprise value
                p.close_price * qm.shares_current + COALESCE(qm.total_debt_current, 0) AS enterprise_value,

                -- Growth rates
                CASE WHEN qm.revenue_5yr_ago > 0 AND qm.revenue_current > 0
                    THEN POWER(qm.revenue_current::DECIMAL / qm.revenue_5yr_ago, 1.0/5) - 1
                    ELSE NULL END AS revenue_cagr_5yr,
                CASE WHEN qm.net_income_5yr_ago > 0 AND qm.net_income_current > 0
                    THEN POWER(qm.net_income_current::DECIMAL / qm.net_income_5yr_ago, 1.0/5) - 1
                    ELSE NULL END AS earnings_cagr_5yr,
                CASE WHEN qm.revenue_3yr_ago > 0 AND qm.revenue_current > 0
                    THEN POWER(qm.revenue_current::DECIMAL / qm.revenue_3yr_ago, 1.0/3) - 1
                    ELSE NULL END AS revenue_cagr_3yr,
                CASE WHEN qm.net_income_3yr_ago > 0 AND qm.net_income_current > 0
                    THEN POWER(qm.net_income_current::DECIMAL / qm.net_income_3yr_ago, 1.0/3) - 1
                    ELSE NULL END AS earnings_cagr_3yr,
                -- 2yr CAGR fallback
                CASE WHEN qm.revenue_2yr_ago > 0 AND qm.revenue_current > 0
                    THEN POWER(qm.revenue_current::DECIMAL / qm.revenue_2yr_ago, 1.0/2) - 1
                    ELSE NULL END AS revenue_cagr_2yr,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Valuation ratios (with FX conversion)
                CASE WHEN qm.net_income_current > 0
                    THEN (p.close_price * qm.shares_current)::DECIMAL / (qm.net_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,
                CASE WHEN qm.ebit_current > 0
                    THEN (p.close_price * qm.shares_current + COALESCE(qm.total_debt_current, 0) * ({fx_rate_sql}))::DECIMAL / (qm.ebit_current * ({fx_rate_sql}))
                    ELSE NULL END AS ev_to_ebit,
                CASE WHEN qm.fcf_current > 0
                    THEN (p.close_price * qm.shares_current)::DECIMAL / (qm.fcf_current * ({fx_rate_sql}))
                    ELSE NULL END AS price_to_fcf,

                -- Debt to equity (no FX - same currency)
                CASE WHEN qm.total_equity_current > 0
                    THEN COALESCE(qm.total_debt_current, 0)::DECIMAL / qm.total_equity_current
                    ELSE NULL END AS debt_to_equity,

                -- FCF yield (with FX conversion)
                CASE WHEN p.close_price * qm.shares_current > 0 AND qm.fcf_current > 0
                    THEN (qm.fcf_current * ({fx_rate_sql}))::DECIMAL / (p.close_price * qm.shares_current)
                    ELSE NULL END AS fcf_yield

            FROM companies_with_data c
            JOIN quality_metrics qm ON c.financial_symbol = qm.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE qm.shares_current > 0
              AND p.close_price > 0
              AND qm.revenue_current > 0
              AND qm.net_income_current > 0
              -- ROIC > 15% average
              AND qm.avg_roic_5yr > 0.15
              -- Consistent ROIC (minimum > 10%)
              AND COALESCE(qm.min_roic_5yr, qm.roic_current) > 0.10
              -- Gross margin > 40% (moat indicator)
              AND qm.avg_gross_margin_5yr > 0.40
              -- Revenue growth > 5% (use longest available timeframe)
              AND (
                  (qm.revenue_5yr_ago > 0 AND POWER(qm.revenue_current::DECIMAL / qm.revenue_5yr_ago, 1.0/5) - 1 > 0.05)
                  OR (qm.revenue_3yr_ago > 0 AND POWER(qm.revenue_current::DECIMAL / qm.revenue_3yr_ago, 1.0/3) - 1 > 0.05)
                  OR (qm.revenue_2yr_ago > 0 AND POWER(qm.revenue_current::DECIMAL / qm.revenue_2yr_ago, 1.0/2) - 1 > 0.05)
              )
              -- Profitable most years (75%+ of available years, minimum 3)
              AND qm.profitable_years >= GREATEST(3, qm.total_years * 0.75)
              -- FCF positive most years (60%+ of available years, minimum 2)
              AND qm.fcf_positive_years >= GREATEST(2, qm.total_years * 0.60)
              -- Debt to equity < 1
              AND COALESCE(qm.total_debt_current, 0)::DECIMAL / qm.total_equity_current < 1.0
              -- P/E < 25 (with FX conversion)
              AND (p.close_price * qm.shares_current)::DECIMAL / (qm.net_income_current * ({fx_rate_sql})) < 25
            ORDER BY
                -- Sort by quality-adjusted valuation (ROIC / P/E) with FX
                qm.avg_roic_5yr / ((p.close_price * qm.shares_current)::DECIMAL / (qm.net_income_current * ({fx_rate_sql}))) DESC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            avg_roic = float(row['avg_roic_5yr']) if row['avg_roic_5yr'] else 0
            avg_gm = float(row['avg_gross_margin_5yr']) if row['avg_gross_margin_5yr'] else 0
            pe = float(row['pe_ratio']) if row['pe_ratio'] else None
            earnings_cagr = float(row['earnings_cagr_5yr']) if row['earnings_cagr_5yr'] else (
                float(row['earnings_cagr_3yr']) if row['earnings_cagr_3yr'] else 0
            )
            peg = pe / (earnings_cagr * 100) if pe and earnings_cagr and earnings_cagr > 0.03 else None

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'enterprise_value': float(row['enterprise_value']) if row['enterprise_value'] else None,

                # Quality metrics
                'avg_roic_5yr': avg_roic,
                'roic_current': float(row['roic_current']) if row['roic_current'] else None,
                'min_roic_5yr': float(row['min_roic_5yr']) if row['min_roic_5yr'] else None,
                'avg_roe_5yr': float(row['avg_roe_5yr']) if row['avg_roe_5yr'] else None,
                'roe_current': float(row['roe_current']) if row['roe_current'] else None,

                # Margins
                'avg_gross_margin_5yr': avg_gm,
                'gross_margin_current': float(row['gross_margin_current']) if row['gross_margin_current'] else None,
                'avg_operating_margin_5yr': float(row['avg_operating_margin_5yr']) if row['avg_operating_margin_5yr'] else None,
                'operating_margin_current': float(row['operating_margin_current']) if row['operating_margin_current'] else None,

                # Growth
                'revenue_cagr_5yr': float(row['revenue_cagr_5yr']) if row['revenue_cagr_5yr'] else None,
                'revenue_cagr_3yr': float(row['revenue_cagr_3yr']) if row['revenue_cagr_3yr'] else None,
                'earnings_cagr_5yr': float(row['earnings_cagr_5yr']) if row['earnings_cagr_5yr'] else None,
                'earnings_cagr_3yr': float(row['earnings_cagr_3yr']) if row['earnings_cagr_3yr'] else None,

                # Consistency
                'profitable_years': int(row['profitable_years']) if row['profitable_years'] else 0,
                'fcf_positive_years': int(row['fcf_positive_years']) if row['fcf_positive_years'] else 0,
                'total_years': int(row['total_years']) if row['total_years'] else 0,

                # Valuation
                'pe_ratio': pe,
                'peg_ratio': peg,
                'ev_to_ebit': float(row['ev_to_ebit']) if row['ev_to_ebit'] else None,
                'price_to_fcf': float(row['price_to_fcf']) if row['price_to_fcf'] else None,
                'fcf_yield': float(row['fcf_yield']) if row['fcf_yield'] else None,

                # Leverage
                'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,

                # Cash flow
                'free_cash_flow': float(row['fcf_current']) if row['fcf_current'] else None,
                'net_income': float(row['net_income_current']) if row['net_income_current'] else None,

                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],
            }

            # Build flags
            flags = []

            if avg_roic >= 0.25:
                flags.append("EXCEPTIONAL_ROIC: 25%+ average returns")
            elif avg_roic >= 0.20:
                flags.append("HIGH_ROIC: 20%+ average returns")

            if avg_gm >= 0.60:
                flags.append("WIDE_MOAT: 60%+ gross margins")
            elif avg_gm >= 0.50:
                flags.append("STRONG_MOAT: 50%+ gross margins")

            if pe and pe < 15:
                flags.append("CHEAP: P/E under 15")
            elif pe and pe < 18:
                flags.append("FAIRLY_PRICED: P/E under 18")

            if peg and peg < 1:
                flags.append("PEG_ATTRACTIVE: Under 1x")

            revenue_cagr = metrics.get('revenue_cagr_5yr') or metrics.get('revenue_cagr_3yr')
            if revenue_cagr and revenue_cagr > 0.15:
                flags.append("FAST_GROWER: 15%+ revenue CAGR")
            elif revenue_cagr and revenue_cagr > 0.10:
                flags.append("SOLID_GROWTH: 10%+ revenue CAGR")

            if metrics.get('profitable_years') == metrics.get('total_years'):
                flags.append("CONSISTENT: Profitable every year")

            # Short history warning
            total_years = metrics.get('total_years', 0) or 0
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
        Calculate score based on quality, growth, and valuation.
        """
        score = 0.0

        avg_roic = metrics.get('avg_roic_5yr', 0.15) or 0.15
        avg_gm = metrics.get('avg_gross_margin_5yr', 0.4) or 0.4
        pe = metrics.get('pe_ratio', 20) or 20
        peg = metrics.get('peg_ratio')
        revenue_cagr = metrics.get('revenue_cagr_5yr') or metrics.get('revenue_cagr_3yr') or 0.05
        fcf_yield = metrics.get('fcf_yield', 0) or 0
        profitable_years = metrics.get('profitable_years', 0) or 0
        total_years = metrics.get('total_years', 5) or 5

        # ROIC scoring (up to 25 points)
        if avg_roic >= 0.30:
            score += 25
        elif avg_roic >= 0.25:
            score += 22
        elif avg_roic >= 0.20:
            score += 18
        elif avg_roic >= 0.17:
            score += 14
        elif avg_roic >= 0.15:
            score += 10

        # Gross margin / moat scoring (up to 20 points)
        if avg_gm >= 0.70:
            score += 20
        elif avg_gm >= 0.60:
            score += 17
        elif avg_gm >= 0.50:
            score += 14
        elif avg_gm >= 0.45:
            score += 10
        elif avg_gm >= 0.40:
            score += 7

        # Valuation scoring (up to 20 points)
        if pe < 12:
            score += 20
        elif pe < 15:
            score += 16
        elif pe < 18:
            score += 12
        elif pe < 22:
            score += 8
        elif pe < 25:
            score += 5

        # Growth scoring (up to 15 points)
        if revenue_cagr > 0.20:
            score += 15
        elif revenue_cagr > 0.15:
            score += 12
        elif revenue_cagr > 0.10:
            score += 9
        elif revenue_cagr > 0.07:
            score += 6
        elif revenue_cagr > 0.05:
            score += 4

        # Consistency scoring (up to 10 points)
        profitability_rate = profitable_years / total_years if total_years > 0 else 0
        if profitability_rate >= 1.0:
            score += 10
        elif profitability_rate >= 0.8:
            score += 7
        elif profitability_rate >= 0.6:
            score += 4

        # FCF yield bonus (up to 10 points)
        if fcf_yield > 0.08:
            score += 10
        elif fcf_yield > 0.06:
            score += 7
        elif fcf_yield > 0.04:
            score += 5
        elif fcf_yield > 0.02:
            score += 3

        return self.clamp_score(score)

    def get_tier_c_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier C prompt for deep moat assessment (requires Claude API).
        """
        return f"""Perform a deep competitive moat analysis for this company.

Company: {company_name}
Avg ROIC (5yr): {(metrics.get('avg_roic_5yr', 0) or 0) * 100:.1f}%
Avg Gross Margin (5yr): {(metrics.get('avg_gross_margin_5yr', 0) or 0) * 100:.1f}%
Revenue CAGR (5yr): {(metrics.get('revenue_cagr_5yr', 0) or 0) * 100:.1f}%
P/E Ratio: {metrics.get('pe_ratio', 'N/A')}
FCF Yield: {(metrics.get('fcf_yield', 0) or 0) * 100:.1f}%

Analyze the company's competitive moat by examining:

1. SWITCHING COSTS: How difficult is it for customers to switch?
   - Contract lengths and terms?
   - Integration depth?
   - Learning curve?
   - Data lock-in?

2. NETWORK EFFECTS: Does value increase with more users?
   - Two-sided marketplace dynamics?
   - Community effects?
   - Data advantages?

3. COST ADVANTAGES: What structural cost advantages exist?
   - Scale economies?
   - Process advantages?
   - Location advantages?
   - Resource access?

4. INTANGIBLE ASSETS: What intangibles create barriers?
   - Brand strength and pricing power?
   - Patents and IP?
   - Regulatory licenses?
   - Unique capabilities?

5. EFFICIENT SCALE: Is the market naturally limited?
   - Geographic monopolies?
   - Capacity constraints?
   - Niche domination?

6. MOAT TRAJECTORY: Is the moat widening or narrowing?
   - New competitive threats?
   - Technology disruption risk?
   - Market share trends?

7. REINVESTMENT OPPORTUNITIES: Can high ROIC be sustained?
   - Addressable market size?
   - Adjacent expansion potential?
   - Capital allocation track record?

Respond in JSON:
{{
  "switching_costs": "NONE|LOW|MEDIUM|HIGH",
  "switching_costs_detail": "string",
  "network_effects": "NONE|WEAK|MODERATE|STRONG",
  "network_effects_detail": "string",
  "cost_advantages": "NONE|SMALL|MODERATE|LARGE",
  "cost_advantages_detail": "string",
  "intangible_assets": "WEAK|MODERATE|STRONG",
  "intangible_assets_detail": "string",
  "efficient_scale": "NO|PARTIAL|YES",
  "efficient_scale_detail": "string",
  "moat_trajectory": "NARROWING|STABLE|WIDENING",
  "disruption_risk": "LOW|MEDIUM|HIGH",
  "reinvestment_runway_years": "number",
  "overall_moat_strength": "NONE|NARROW|WIDE|VERY_WIDE",
  "confidence": "LOW|MEDIUM|HIGH",
  "summary": "string"
}}"""
