"""
Screen 13: Crisis Bargains — Oversold on Bad News

Identifies companies with sharp price drops where the market appears
to have overreacted. The crisis may be priced in, creating opportunity.

Tier: A + B
Frequency: Daily (fast-moving opportunities)

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class CrisisBargainsScreen(BaseScreen):
    """
    Screen 13: Crisis Bargains — Oversold on Bad News

    Criteria:
    - Price down 30%+ in last 3 months (crisis/event-driven drop)
    - But still profitable (fundamentals not destroyed)
    - Operating margin still positive
    - Price to earnings < 12 OR Price to book < 1.0 (oversold)
    - Debt to equity < 1.5 (can survive crisis)
    - Has history of profitability (5+ years, mostly profitable)

    The key insight: markets often overreact to negative news, creating
    buying opportunities in fundamentally sound companies.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 13

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find crisis bargains.
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, f.report_currency)', 'c.trading_currency')

        query = f"""
            WITH pit_financials AS (
                -- Get annual financials from yahoo_financials
                SELECT
                    yf.symbol,
                    yf.period_date,
                    yf.fiscal_year,
                    yf.currency AS report_currency,
                    (yf.income_statement->>'total_revenue')::NUMERIC AS total_revenue,
                    (yf.income_statement->>'operating_income')::NUMERIC AS operating_income,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.balance_sheet->>'total_assets')::NUMERIC AS total_assets,
                    (yf.balance_sheet->>'current_assets')::NUMERIC AS current_assets,
                    (yf.balance_sheet->>'cash_and_cash_equivalents')::NUMERIC AS cash_and_equivalents,
                    (yf.balance_sheet->>'current_liabilities')::NUMERIC AS current_liabilities,
                    (yf.balance_sheet->>'total_debt')::NUMERIC AS total_debt,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    (yf.income_statement->>'basic_average_shares')::NUMERIC AS shares_outstanding,
                    (yf.cash_flow->>'free_cash_flow')::NUMERIC AS free_cash_flow
                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '6 years'
            ),
            latest_financials AS (
                -- Get latest financials per company
                SELECT DISTINCT ON (symbol)
                    symbol,
                    period_date,
                    fiscal_year,
                    report_currency,
                    total_revenue,
                    operating_income,
                    net_income,
                    shares_outstanding,
                    total_debt,
                    total_equity,
                    cash_and_equivalents,
                    free_cash_flow,
                    CASE WHEN total_revenue > 0
                        THEN operating_income::DECIMAL / total_revenue
                        ELSE NULL END AS operating_margin
                FROM pit_financials
                ORDER BY symbol, period_date DESC
            ),
            historical_profitability AS (
                -- Check profitability history
                SELECT
                    symbol,
                    COUNT(*) AS total_years,
                    COUNT(*) FILTER (WHERE net_income > 0) AS profitable_years
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
            price_3m_ago AS (
                -- Get price 3 months ago
                SELECT DISTINCT ON (symbol)
                    symbol,
                    close_price AS price_3m_ago,
                    date AS date_3m_ago
                FROM daily_price_data
                WHERE date <= $1 - INTERVAL '90 days'
                  AND date > $1 - INTERVAL '100 days'
                ORDER BY symbol, date DESC
            ),
            price_6m_ago AS (
                -- Get price 6 months ago
                SELECT DISTINCT ON (symbol)
                    symbol,
                    close_price AS price_6m_ago,
                    date AS date_6m_ago
                FROM daily_price_data
                WHERE date <= $1 - INTERVAL '180 days'
                  AND date > $1 - INTERVAL '190 days'
                ORDER BY symbol, date DESC
            ),
            price_52w_high AS (
                -- Get 52-week high
                SELECT
                    symbol,
                    MAX(close_price) AS high_52w
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

                -- Current price
                p.close_price AS price,
                p.price_date,

                -- Historical prices
                p3.price_3m_ago,
                p6.price_6m_ago,
                ph.high_52w,

                -- Price changes
                CASE WHEN p3.price_3m_ago > 0
                    THEN (p.close_price - p3.price_3m_ago) / p3.price_3m_ago
                    ELSE NULL END AS return_3m,
                CASE WHEN p6.price_6m_ago > 0
                    THEN (p.close_price - p6.price_6m_ago) / p6.price_6m_ago
                    ELSE NULL END AS return_6m,
                CASE WHEN ph.high_52w > 0
                    THEN (ph.high_52w - p.close_price) / ph.high_52w
                    ELSE NULL END AS drawdown_from_high,

                -- Current financials
                f.period_date AS financial_date,
                f.fiscal_year,
                f.report_currency,
                f.shares_outstanding,
                f.total_revenue,
                f.operating_income,
                f.net_income,
                f.free_cash_flow,
                f.operating_margin,
                f.total_debt,
                f.total_equity,
                f.cash_and_equivalents,

                -- Historical profitability
                hp.total_years,
                hp.profitable_years,

                -- Market cap
                p.close_price * f.shares_outstanding AS market_cap,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Valuation ratios (with FX conversion)
                CASE WHEN f.net_income > 0
                    THEN (p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql}))
                    ELSE NULL END AS pe_ratio,
                CASE WHEN f.total_equity > 0 AND f.shares_outstanding > 0
                    THEN p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding)
                    ELSE NULL END AS price_to_book,

                -- Debt to equity (no FX - same currency)
                CASE WHEN f.total_equity > 0
                    THEN COALESCE(f.total_debt, 0)::DECIMAL / f.total_equity
                    ELSE NULL END AS debt_to_equity,

                -- Cash as % of market cap (with FX conversion)
                CASE WHEN p.close_price * f.shares_outstanding > 0
                    THEN (COALESCE(f.cash_and_equivalents, 0) * ({fx_rate_sql}))::DECIMAL / (p.close_price * f.shares_outstanding)
                    ELSE NULL END AS cash_to_mcap

            FROM companies_with_data c
            JOIN latest_financials f ON c.financial_symbol = f.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            LEFT JOIN price_3m_ago p3 ON c.primary_ticker = p3.symbol
            LEFT JOIN price_6m_ago p6 ON c.primary_ticker = p6.symbol
            LEFT JOIN price_52w_high ph ON c.primary_ticker = ph.symbol
            LEFT JOIN historical_profitability hp ON c.financial_symbol = hp.symbol
            WHERE f.shares_outstanding > 0
              AND p.close_price > 0
              AND f.total_equity > 0
              AND f.net_income > 0
              -- Price down 30%+ in last 3 months OR 40%+ from 52w high
              AND (
                  (p3.price_3m_ago IS NOT NULL AND (p.close_price - p3.price_3m_ago) / p3.price_3m_ago < -0.30)
                  OR (ph.high_52w IS NOT NULL AND (ph.high_52w - p.close_price) / ph.high_52w > 0.40)
              )
              -- Still profitable
              AND f.net_income > 0
              -- Operating margin positive
              AND f.operating_margin > 0
              -- Cheap valuation (P/E < 12 OR P/B < 1) (with FX conversion)
              AND (
                  (p.close_price * f.shares_outstanding)::DECIMAL / (f.net_income * ({fx_rate_sql})) < 12
                  OR p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) < 1.0
              )
              -- Not over-leveraged
              AND COALESCE(f.total_debt, 0)::DECIMAL / f.total_equity < 1.5
              -- Has profitability track record
              AND COALESCE(hp.profitable_years, 0) >= 3
            ORDER BY
                -- Sort by 3-month return (biggest drop first)
                COALESCE((p.close_price - p3.price_3m_ago) / NULLIF(p3.price_3m_ago, 0), -0.4) ASC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            return_3m = float(row['return_3m']) if row['return_3m'] else 0
            return_6m = float(row['return_6m']) if row['return_6m'] else 0
            drawdown = float(row['drawdown_from_high']) if row['drawdown_from_high'] else 0
            pe = float(row['pe_ratio']) if row['pe_ratio'] else None
            p_b = float(row['price_to_book']) if row['price_to_book'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,

                # Price history
                'price_3m_ago': float(row['price_3m_ago']) if row['price_3m_ago'] else None,
                'price_6m_ago': float(row['price_6m_ago']) if row['price_6m_ago'] else None,
                'high_52w': float(row['high_52w']) if row['high_52w'] else None,

                # Returns
                'return_3m': return_3m,
                'return_6m': return_6m,
                'drawdown_from_high': drawdown,

                # Valuation
                'pe_ratio': pe,
                'price_to_book': p_b,

                # Fundamentals
                'operating_margin': float(row['operating_margin']) if row['operating_margin'] else None,
                'net_income': float(row['net_income']) if row['net_income'] else None,
                'free_cash_flow': float(row['free_cash_flow']) if row['free_cash_flow'] else None,

                # Balance sheet
                'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,
                'cash_to_mcap': float(row['cash_to_mcap']) if row['cash_to_mcap'] else 0,
                'cash_and_equivalents': float(row['cash_and_equivalents']) if row['cash_and_equivalents'] else None,
                'total_equity': float(row['total_equity']) if row['total_equity'] else None,

                # Track record
                'profitable_years': int(row['profitable_years']) if row['profitable_years'] else 0,
                'total_years': int(row['total_years']) if row['total_years'] else 0,

                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],
            }

            # Build flags
            flags = []

            if return_3m < -0.50:
                flags.append("CRASH: Down 50%+ in 3 months")
            elif return_3m < -0.40:
                flags.append("SEVERE_DROP: Down 40%+ in 3 months")
            elif return_3m < -0.30:
                flags.append("SIGNIFICANT_DROP: Down 30%+ in 3 months")

            if drawdown >= 0.50:
                flags.append("HALF_OFF: 50%+ off 52w high")
            elif drawdown >= 0.40:
                flags.append("DEEP_DISCOUNT: 40%+ off 52w high")

            if pe and pe < 5:
                flags.append("VERY_CHEAP: P/E under 5")
            elif pe and pe < 8:
                flags.append("CHEAP: P/E under 8")

            if p_b < 0.5:
                flags.append("BELOW_LIQUIDATION: P/B under 0.5")
            elif p_b < 0.8:
                flags.append("ASSET_DISCOUNT: P/B under 0.8")

            cash_to_mcap = metrics.get('cash_to_mcap', 0)
            if cash_to_mcap > 0.3:
                flags.append("CASH_RICH: Cash > 30% of market cap")

            flags.append("NEEDS_NEWS_CHECK: Verify crisis cause")

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
        Calculate score based on drop severity, valuation, and fundamentals.
        """
        score = 0.0

        return_3m = metrics.get('return_3m', 0) or 0
        drawdown = metrics.get('drawdown_from_high', 0) or 0
        pe = metrics.get('pe_ratio', 10) or 10
        p_b = metrics.get('price_to_book', 1) or 1
        op_margin = metrics.get('operating_margin', 0) or 0
        cash_to_mcap = metrics.get('cash_to_mcap', 0) or 0
        debt_to_equity = metrics.get('debt_to_equity', 1) or 1

        # Drop severity (up to 25 points)
        # More severe drop = potentially more opportunity
        if return_3m < -0.50:
            score += 25
        elif return_3m < -0.40:
            score += 20
        elif return_3m < -0.35:
            score += 15
        elif return_3m < -0.30:
            score += 10

        # Drawdown from high (up to 20 points)
        if drawdown >= 0.60:
            score += 20
        elif drawdown >= 0.50:
            score += 16
        elif drawdown >= 0.40:
            score += 12
        elif drawdown >= 0.30:
            score += 8

        # Valuation (P/E, up to 20 points)
        if pe < 5:
            score += 20
        elif pe < 7:
            score += 16
        elif pe < 9:
            score += 12
        elif pe < 11:
            score += 8
        elif pe < 12:
            score += 5

        # P/B (up to 15 points)
        if p_b < 0.4:
            score += 15
        elif p_b < 0.6:
            score += 12
        elif p_b < 0.8:
            score += 9
        elif p_b < 1.0:
            score += 6

        # Fundamentals intact bonus (up to 10 points)
        if op_margin > 0.15:
            score += 10
        elif op_margin > 0.10:
            score += 7
        elif op_margin > 0.05:
            score += 4

        # Balance sheet strength (up to 10 points)
        if cash_to_mcap > 0.4:
            score += 5  # Lots of cash relative to market cap
        if debt_to_equity < 0.5:
            score += 5  # Low leverage

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for crisis analysis.
        """
        return f"""Analyze the crisis affecting this company and assess recovery potential.

Company: {company_name}
3-month return: {metrics.get('return_3m', 0) * 100:.1f}%
Drawdown from 52w high: {(metrics.get('drawdown_from_high', 0) or 0) * 100:.1f}%
Current P/E: {metrics.get('pe_ratio', 'N/A')}
Current P/B: {metrics.get('price_to_book', 0):.2f}x
Operating margin: {(metrics.get('operating_margin', 0) or 0) * 100:.1f}%
Debt to equity: {metrics.get('debt_to_equity', 0):.2f}x
Cash as % of market cap: {(metrics.get('cash_to_mcap', 0) or 0) * 100:.1f}%

From recent news and reports:

1. CRISIS IDENTIFICATION: What caused the price drop?
   - Earnings miss or guidance cut?
   - Legal or regulatory issue?
   - Key customer/contract loss?
   - Management scandal?
   - Industry-wide problem?
   - Fraud allegations?

2. SEVERITY ASSESSMENT: How bad is the situation?
   - Quantify the impact if possible
   - Is the worst case priced in?
   - What's the downside scenario?

3. RESOLUTION TIMELINE: When might this resolve?
   - Legal proceedings timeline?
   - Management changes?
   - Operational fixes?

4. BUSINESS IMPACT: Is the core business damaged?
   - Customer relationships intact?
   - Competitive position?
   - Brand damage?

5. BALANCE SHEET: Can they survive?
   - Cash runway?
   - Debt covenants?
   - Asset liquidation value?

6. UPSIDE SCENARIO: What happens if resolved positively?
   - Normalized earnings estimate?
   - Target multiple?
   - Potential return?

Respond in JSON:
{{
  "crisis_type": "EARNINGS|LEGAL|CUSTOMER_LOSS|SCANDAL|INDUSTRY|FRAUD|OTHER",
  "crisis_description": "string",
  "severity": "MINOR|MODERATE|SEVERE|EXISTENTIAL",
  "worst_case_priced_in": "YES|PARTIALLY|NO",
  "resolution_timeline_months": "number or null",
  "core_business_intact": "YES|MOSTLY|DAMAGED|UNKNOWN",
  "survival_probability": "HIGH|MEDIUM|LOW",
  "normalized_pe_estimate": "number or null",
  "potential_upside_pct": "number or null",
  "key_risks": ["string array"],
  "buy_rating": "STRONG_BUY|BUY|SPECULATIVE_BUY|AVOID",
  "summary": "string"
}}"""
