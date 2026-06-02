"""
Screen 15: Stalwarts — Blue Chip Dip Buys

Identifies large, established quality companies experiencing temporary
price weakness. These are steady compounders available at a discount.

Tier: A + B
Frequency: Weekly

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class StalwartsScreen(BaseScreen):
    """
    Screen 15: Stalwarts — Blue Chip Dip Buys

    Criteria:
    - Large cap (market cap > 5B SEK or equivalent)
    - Profitable every year for 5+ years
    - ROIC > 12% on average
    - Debt to equity < 0.8
    - Dividend paying (cash flow positive)
    - Price down 20%+ from 52-week high (temporary dip)
    - Fundamentals intact (no major margin deterioration)

    The key insight: quality companies rarely go on sale. When they do,
    it's often due to temporary factors or market sentiment, not
    fundamental deterioration.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 15

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find stalwarts on sale.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use company_master.report_currency (correct) instead of yahoo_financials.currency
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
                    (yf.balance_sheet->>'total_assets')::NUMERIC AS total_assets,
                    (yf.balance_sheet->>'current_assets')::NUMERIC AS current_assets,
                    (yf.balance_sheet->>'current_liabilities')::NUMERIC AS current_liabilities,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    (yf.balance_sheet->>'total_debt')::NUMERIC AS total_debt,
                    (yf.income_statement->>'basic_average_shares')::NUMERIC AS shares_outstanding,
                    (yf.cash_flow->>'operating_cash_flow')::NUMERIC AS operating_cash_flow,
                    (yf.cash_flow->>'free_cash_flow')::NUMERIC AS free_cash_flow,
                    COALESCE(
                        (yf.cash_flow->>'cash_dividends_paid')::NUMERIC,
                        (yf.cash_flow->>'common_stock_dividend_paid')::NUMERIC
                    ) AS dividends_paid,
                    -- Margins
                    CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END AS operating_margin,
                    -- ROIC
                    CASE WHEN ((yf.balance_sheet->>'total_assets')::NUMERIC - COALESCE((yf.balance_sheet->>'current_liabilities')::NUMERIC, 0)) > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / ((yf.balance_sheet->>'total_assets')::NUMERIC - COALESCE((yf.balance_sheet->>'current_liabilities')::NUMERIC, 0))
                        ELSE NULL END AS roic,
                    -- ROE
                    CASE WHEN (yf.balance_sheet->>'stockholders_equity')::NUMERIC > 0
                        THEN (yf.income_statement->>'net_income')::NUMERIC / (yf.balance_sheet->>'stockholders_equity')::NUMERIC
                        ELSE NULL END AS roe
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '6 years'
            ),
            quality_metrics AS (
                -- Calculate quality metrics
                SELECT
                    symbol,
                    -- Current year data
                    MAX(CASE WHEN rn = 1 THEN fiscal_year END) AS year_current,
                    MAX(CASE WHEN rn = 1 THEN period_date END) AS latest_period_date,
                    MAX(CASE WHEN rn = 1 THEN report_currency END) AS report_currency,
                    MAX(CASE WHEN rn = 1 THEN total_revenue END) AS revenue_current,
                    MAX(CASE WHEN rn = 1 THEN net_income END) AS net_income_current,
                    MAX(CASE WHEN rn = 1 THEN operating_income END) AS operating_income_current,
                    MAX(CASE WHEN rn = 1 THEN operating_margin END) AS operating_margin_current,
                    MAX(CASE WHEN rn = 1 THEN roic END) AS roic_current,
                    MAX(CASE WHEN rn = 1 THEN roe END) AS roe_current,
                    MAX(CASE WHEN rn = 1 THEN shares_outstanding END) AS shares_current,
                    MAX(CASE WHEN rn = 1 THEN total_debt END) AS total_debt_current,
                    MAX(CASE WHEN rn = 1 THEN total_equity END) AS total_equity_current,
                    MAX(CASE WHEN rn = 1 THEN free_cash_flow END) AS fcf_current,
                    MAX(CASE WHEN rn = 1 THEN dividends_paid END) AS dividends_paid_current,

                    -- Prior year revenue (for YoY)
                    MAX(CASE WHEN rn = 2 THEN total_revenue END) AS revenue_1yr_ago,

                    -- 5-year averages
                    AVG(operating_margin) FILTER (WHERE rn <= 5) AS avg_operating_margin_5yr,
                    AVG(roic) FILTER (WHERE rn <= 5) AS avg_roic_5yr,
                    AVG(roe) FILTER (WHERE rn <= 5) AS avg_roe_5yr,

                    -- Consistency
                    COUNT(*) FILTER (WHERE rn <= 5 AND net_income > 0) AS profitable_years,
                    COUNT(*) FILTER (WHERE rn <= 5 AND free_cash_flow > 0) AS fcf_positive_years,
                    COUNT(*) FILTER (WHERE rn <= 5 AND dividends_paid IS NOT NULL AND dividends_paid < 0) AS dividend_paying_years,
                    COUNT(*) FILTER (WHERE rn <= 5) AS total_years

                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fiscal_year DESC) AS rn
                    FROM pit_financials
                ) ranked
                WHERE rn <= 5
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
            price_history AS (
                -- Get 52-week high
                SELECT
                    symbol,
                    MAX(close_price) AS high_52w,
                    MIN(close_price) AS low_52w
                FROM daily_price_data
                WHERE date <= $1
                  AND date > $1 - INTERVAL '365 days'
                GROUP BY symbol
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

                -- Price data
                p.close_price AS price,
                p.price_date,
                ph.high_52w,
                ph.low_52w,

                -- Price metrics
                CASE WHEN ph.high_52w > 0
                    THEN (ph.high_52w - p.close_price) / ph.high_52w
                    ELSE NULL END AS drawdown_from_high,
                CASE WHEN ph.low_52w > 0
                    THEN (p.close_price - ph.low_52w) / ph.low_52w
                    ELSE NULL END AS gain_from_low,

                -- Current financials
                qm.year_current AS fiscal_year,
                qm.latest_period_date AS financial_date,
                c.report_currency,  -- From company_master (correct)
                qm.shares_current,
                qm.revenue_current,
                qm.net_income_current,
                qm.operating_income_current,
                qm.fcf_current,
                qm.dividends_paid_current,
                qm.total_debt_current,
                qm.total_equity_current,

                -- Current margins/returns
                qm.operating_margin_current,
                qm.roic_current,
                qm.roe_current,

                -- Averages
                qm.avg_operating_margin_5yr,
                qm.avg_roic_5yr,
                qm.avg_roe_5yr,

                -- Margin change (current vs average)
                qm.operating_margin_current - qm.avg_operating_margin_5yr AS margin_change,

                -- Revenue growth
                CASE WHEN qm.revenue_1yr_ago > 0
                    THEN (qm.revenue_current - qm.revenue_1yr_ago)::DECIMAL / qm.revenue_1yr_ago
                    ELSE NULL END AS revenue_yoy,

                -- Consistency
                qm.profitable_years,
                qm.fcf_positive_years,
                qm.dividend_paying_years,
                qm.total_years,

                -- Market cap
                p.close_price * qm.shares_current AS market_cap,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Valuation (with FX conversion)
                CASE WHEN qm.net_income_current > 0
                    THEN (p.close_price * qm.shares_current)::DECIMAL / (qm.net_income_current * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,
                CASE WHEN qm.fcf_current > 0
                    THEN (p.close_price * qm.shares_current)::DECIMAL / (qm.fcf_current * ({fx_rate_sql}))
                    ELSE NULL END AS price_to_fcf,

                -- Dividend yield (with FX conversion)
                CASE WHEN p.close_price * qm.shares_current > 0 AND qm.dividends_paid_current IS NOT NULL
                    THEN (ABS(qm.dividends_paid_current) * ({fx_rate_sql}))::DECIMAL / (p.close_price * qm.shares_current)
                    ELSE NULL END AS dividend_yield,

                -- Debt to equity (no FX - same currency)
                CASE WHEN qm.total_equity_current > 0
                    THEN COALESCE(qm.total_debt_current, 0)::DECIMAL / qm.total_equity_current
                    ELSE NULL END AS debt_to_equity

            FROM companies_with_data c
            JOIN quality_metrics qm ON c.financial_symbol = qm.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            JOIN price_history ph ON c.primary_ticker = ph.symbol
            WHERE qm.shares_current > 0
              AND p.close_price > 0
              AND qm.total_equity_current > 0
              AND qm.net_income_current > 0
              -- Large cap (market cap > 5B SEK)
              AND p.close_price * qm.shares_current > 5000000000
              -- Profitable every year (or 4 of 5)
              AND qm.profitable_years >= 4
              -- ROIC > 12% average
              AND qm.avg_roic_5yr > 0.12
              -- Low debt (debt/equity < 0.8)
              AND COALESCE(qm.total_debt_current, 0)::DECIMAL / qm.total_equity_current < 0.8
              -- Dividend paying (at least 3 of 5 years)
              AND qm.dividend_paying_years >= 3
              -- Price down 20%+ from 52-week high
              AND (ph.high_52w - p.close_price) / ph.high_52w > 0.20
              -- Fundamentals intact (margin not collapsed more than 3pp vs average)
              AND qm.operating_margin_current > qm.avg_operating_margin_5yr - 0.03
            ORDER BY
                -- Sort by drawdown from high (biggest dip first)
                (ph.high_52w - p.close_price) / ph.high_52w DESC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            drawdown = float(row['drawdown_from_high']) if row['drawdown_from_high'] else 0
            avg_roic = float(row['avg_roic_5yr']) if row['avg_roic_5yr'] else 0
            pe = float(row['pe_ratio']) if row['pe_ratio'] else None
            div_yield = float(row['dividend_yield']) if row['dividend_yield'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,

                # Price metrics
                'high_52w': float(row['high_52w']) if row['high_52w'] else None,
                'low_52w': float(row['low_52w']) if row['low_52w'] else None,
                'drawdown_from_high': drawdown,
                'gain_from_low': float(row['gain_from_low']) if row['gain_from_low'] else None,

                # Quality metrics
                'avg_roic_5yr': avg_roic,
                'roic_current': float(row['roic_current']) if row['roic_current'] else None,
                'avg_roe_5yr': float(row['avg_roe_5yr']) if row['avg_roe_5yr'] else None,
                'roe_current': float(row['roe_current']) if row['roe_current'] else None,

                # Margins
                'operating_margin_current': float(row['operating_margin_current']) if row['operating_margin_current'] else None,
                'avg_operating_margin_5yr': float(row['avg_operating_margin_5yr']) if row['avg_operating_margin_5yr'] else None,
                'margin_change': float(row['margin_change']) if row['margin_change'] else None,

                # Revenue
                'revenue_yoy': float(row['revenue_yoy']) if row['revenue_yoy'] else None,

                # Consistency
                'profitable_years': int(row['profitable_years']) if row['profitable_years'] else 0,
                'fcf_positive_years': int(row['fcf_positive_years']) if row['fcf_positive_years'] else 0,
                'dividend_paying_years': int(row['dividend_paying_years']) if row['dividend_paying_years'] else 0,
                'total_years': int(row['total_years']) if row['total_years'] else 0,

                # Valuation
                'pe_ratio': pe,
                'price_to_fcf': float(row['price_to_fcf']) if row['price_to_fcf'] else None,
                'dividend_yield': div_yield,

                # Balance sheet
                'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,
                'free_cash_flow': float(row['fcf_current']) if row['fcf_current'] else None,

                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],
            }

            # Build flags
            flags = []

            if drawdown >= 0.35:
                flags.append("DEEP_DIP: 35%+ off highs")
            elif drawdown >= 0.30:
                flags.append("SIGNIFICANT_DIP: 30%+ off highs")
            elif drawdown >= 0.25:
                flags.append("NOTABLE_DIP: 25%+ off highs")

            if avg_roic >= 0.20:
                flags.append("HIGH_QUALITY: ROIC 20%+")
            elif avg_roic >= 0.15:
                flags.append("QUALITY: ROIC 15%+")

            if div_yield >= 0.05:
                flags.append("HIGH_YIELD: 5%+ dividend")
            elif div_yield >= 0.03:
                flags.append("SOLID_YIELD: 3%+ dividend")

            if pe and pe < 12:
                flags.append("CHEAP: P/E under 12")
            elif pe and pe < 15:
                flags.append("FAIR: P/E under 15")

            if metrics['profitable_years'] == metrics['total_years']:
                flags.append("CONSISTENT: Profitable every year")

            margin_change = metrics.get('margin_change', 0)
            if margin_change and margin_change > 0:
                flags.append("IMPROVING: Margins expanding")

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
        Calculate score based on quality, dip severity, and valuation.
        """
        score = 0.0

        drawdown = metrics.get('drawdown_from_high', 0.2) or 0.2
        avg_roic = metrics.get('avg_roic_5yr', 0.12) or 0.12
        pe = metrics.get('pe_ratio', 18) or 18
        div_yield = metrics.get('dividend_yield', 0) or 0
        profitable_years = metrics.get('profitable_years', 0) or 0
        total_years = metrics.get('total_years', 5) or 5
        margin_change = metrics.get('margin_change', 0) or 0

        # Drawdown severity (up to 25 points)
        if drawdown >= 0.40:
            score += 25
        elif drawdown >= 0.35:
            score += 22
        elif drawdown >= 0.30:
            score += 18
        elif drawdown >= 0.25:
            score += 14
        elif drawdown >= 0.20:
            score += 10

        # Quality (ROIC, up to 20 points)
        if avg_roic >= 0.25:
            score += 20
        elif avg_roic >= 0.20:
            score += 17
        elif avg_roic >= 0.17:
            score += 14
        elif avg_roic >= 0.15:
            score += 11
        elif avg_roic >= 0.12:
            score += 8

        # Valuation (P/E, up to 20 points)
        if pe < 10:
            score += 20
        elif pe < 12:
            score += 16
        elif pe < 14:
            score += 12
        elif pe < 16:
            score += 8
        elif pe < 18:
            score += 5

        # Dividend yield (up to 15 points)
        if div_yield >= 0.06:
            score += 15
        elif div_yield >= 0.05:
            score += 12
        elif div_yield >= 0.04:
            score += 9
        elif div_yield >= 0.03:
            score += 6
        elif div_yield >= 0.02:
            score += 3

        # Consistency (up to 10 points)
        profitability_rate = profitable_years / total_years if total_years > 0 else 0
        if profitability_rate >= 1.0:
            score += 10
        elif profitability_rate >= 0.8:
            score += 7
        elif profitability_rate >= 0.6:
            score += 4

        # Fundamentals intact bonus (up to 10 points)
        if margin_change >= 0.02:
            score += 10  # Margins improving
        elif margin_change >= 0:
            score += 7   # Margins stable
        elif margin_change >= -0.02:
            score += 4   # Minor decline acceptable

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for dip analysis.
        """
        return f"""Analyze why this quality company's stock has declined and assess recovery potential.

Company: {company_name}
Drawdown from 52-week high: {metrics.get('drawdown_from_high', 0) * 100:.1f}%
Current price: {metrics.get('price', 'N/A')} ({metrics.get('trading_currency', 'SEK')})
52-week high: {metrics.get('high_52w', 'N/A')} ({metrics.get('trading_currency', 'SEK')})
P/E ratio: {metrics.get('pe_ratio', 'N/A')}
Dividend yield: {(metrics.get('dividend_yield', 0) or 0) * 100:.2f}%
Avg ROIC (5yr): {(metrics.get('avg_roic_5yr', 0) or 0) * 100:.1f}%
Margin change vs avg: {(metrics.get('margin_change', 0) or 0) * 100:.1f}pp

From recent news and reports:

1. DECLINE CAUSE: Why has the stock declined?
   - Company-specific issues?
   - Sector-wide selloff?
   - Market-wide correction?
   - Guidance cut or miss?
   - Analyst downgrades?

2. TEMPORARY OR PERMANENT: Is the cause temporary or structural?
   - One-time events?
   - Cyclical factors?
   - Competitive threats?
   - Regulatory issues?

3. FUNDAMENTALS CHECK: Are fundamentals intact?
   - Revenue trend?
   - Margin trend?
   - Order book / backlog?
   - Customer retention?

4. MANAGEMENT RESPONSE: How is management responding?
   - Buybacks announced?
   - Dividend maintained/raised?
   - Cost actions?
   - Strategic initiatives?

5. RECOVERY CATALYST: What would drive price recovery?
   - Earnings beat?
   - Guidance raise?
   - Industry recovery?
   - Sentiment change?

Respond in JSON:
{{
  "decline_cause": "COMPANY_SPECIFIC|SECTOR|MARKET|GUIDANCE|ANALYST|OTHER",
  "decline_cause_detail": "string",
  "temporary_or_structural": "TEMPORARY|STRUCTURAL|UNCERTAIN",
  "fundamentals_intact": "YES|MOSTLY|DETERIORATING",
  "management_actions": ["string array"],
  "buyback_activity": "AGGRESSIVE|MODERATE|NONE",
  "dividend_status": "RAISED|MAINTAINED|CUT",
  "recovery_catalysts": ["string array"],
  "expected_recovery_months": "number or null",
  "buy_rating": "STRONG_BUY|BUY|HOLD|AVOID",
  "summary": "string"
}}"""
