"""
Screen 4: Revenue Turnarounds — Intact Unit Economics at Death Prices

Identifies companies with strong gross margins (proving unit economics work)
trading at distressed P/S valuations. The thesis is that gross margin stability
proves the core business model is intact despite revenue pressure.

Tier: A + B
Frequency: Weekly

Criteria (from spec):
1. Gross margin > 35% (strong unit economics - NOT just 20%)
2. Gross margin stable (YoY change > -3pp - not collapsing)
3. Revenue YoY decline < 15% (not in freefall)
4. P/S < 0.5 (death price)
5. EBITDA > 0 OR EBITDA improving YoY (turnaround signal)

NOTE: This screen does NOT filter on operating margin. A company with
45% gross margin and 8% operating margin at 0.3x P/S is a valid candidate.
Operating margin filters belong to Screen 5 (Distressed Stable Earners).

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class RevenueTurnaroundsScreen(BaseScreen):
    """
    Screen 4: Revenue Turnarounds — Intact Unit Economics at Death Prices

    Criteria (from spec):
    - Gross margin > 35% (strong unit economics prove pricing power)
    - Gross margin stable (YoY change > -3pp, not collapsing)
    - Revenue not collapsing (YoY decline < 15%)
    - Price to sales < 0.5 (death price)
    - EBITDA > 0 OR EBITDA improving YoY (turnaround signal)

    The key insight: A 35%+ gross margin proves the business has real
    pricing power. Revenue can decline, but if gross margin holds, the
    core economics are intact. We buy at "give-up" valuations.

    NOTE: No operating margin filter - that's Screen 5's territory.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 4

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find revenue turnarounds.

        Uses yahoo_financials for richer data including:
        - More accurate EBITDA directly from Yahoo
        - Working capital and net debt calculations
        - TTM preferred, annual fallback
        """
        self.log("Running Tier A screen (using yahoo_financials)...")

        financial_filter = self.get_financial_date_filter('fs')
        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, mh.report_currency)', 'c.trading_currency')

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
                    cash_and_equivalents,
                    current_liabilities,
                    total_debt,
                    total_equity,
                    shares_outstanding,
                    operating_cash_flow,
                    free_cash_flow,
                    -- Margins
                    CASE WHEN total_revenue > 0
                        THEN gross_profit::DECIMAL / total_revenue
                        ELSE NULL END AS gross_margin,
                    CASE WHEN total_revenue > 0
                        THEN operating_income::DECIMAL / total_revenue
                        ELSE NULL END AS operating_margin,
                    CASE WHEN total_revenue > 0
                        THEN net_income::DECIMAL / total_revenue
                        ELSE NULL END AS net_margin
                FROM yahoo_combined_financials
            ),
            -- Historical annual data for comparisons (1yr ago, averages)
            historical_annual AS (
                SELECT
                    fs.symbol,
                    fs.period_date,
                    fs.total_revenue,
                    fs.gross_profit,
                    fs.operating_income,
                    fs.ebitda,
                    CASE WHEN fs.total_revenue > 0
                        THEN fs.gross_profit::DECIMAL / fs.total_revenue
                        ELSE NULL END AS gross_margin,
                    CASE WHEN fs.total_revenue > 0
                        THEN fs.operating_income::DECIMAL / fs.total_revenue
                        ELSE NULL END AS operating_margin,
                    ROW_NUMBER() OVER (PARTITION BY fs.symbol ORDER BY fs.period_date DESC) AS rn
                FROM financial_statements fs
                WHERE fs.statement_type = 'annual'
                  AND {financial_filter}
            ),
            margin_history AS (
                -- Combine current (TTM/annual) with historical annual for YoY and averages
                SELECT
                    cf.symbol,
                    -- Current period data (TTM or latest annual)
                    EXTRACT(YEAR FROM cf.period_date)::INT AS year_current,
                    cf.period_date AS latest_period_date,
                    cf.report_currency,
                    cf.is_ttm,
                    cf.total_revenue AS revenue_current,
                    cf.gross_profit AS gross_profit_current,
                    cf.operating_income AS operating_income_current,
                    cf.net_income AS net_income_current,
                    cf.ebitda AS ebitda_current,
                    cf.gross_margin AS gross_margin_current,
                    cf.operating_margin AS operating_margin_current,
                    cf.net_margin AS net_margin_current,
                    cf.shares_outstanding AS shares_current,
                    cf.cash_and_equivalents AS cash_current,
                    cf.total_debt AS total_debt_current,
                    cf.total_equity AS total_equity_current,
                    cf.free_cash_flow AS fcf_current,
                    cf.operating_cash_flow AS ocf_current,

                    -- Prior year data for YoY comparisons (from historical annual)
                    ha1.total_revenue AS revenue_1yr_ago,
                    ha1.gross_margin AS gross_margin_1yr_ago,
                    ha1.ebitda AS ebitda_1yr_ago,

                    -- Historical margins (for context, not filtering)
                    GREATEST(ha1.operating_margin, ha2.operating_margin, ha3.operating_margin, ha4.operating_margin, ha5.operating_margin) AS peak_operating_margin,
                    (COALESCE(ha1.operating_margin, 0) + COALESCE(ha2.operating_margin, 0) + COALESCE(ha3.operating_margin, 0) + COALESCE(ha4.operating_margin, 0) + COALESCE(ha5.operating_margin, 0)) /
                        NULLIF(
                            CASE WHEN ha1.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.operating_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.operating_margin IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_operating_margin_historical,
                    (COALESCE(ha1.gross_margin, 0) + COALESCE(ha2.gross_margin, 0) + COALESCE(ha3.gross_margin, 0) + COALESCE(ha4.gross_margin, 0) + COALESCE(ha5.gross_margin, 0)) /
                        NULLIF(
                            CASE WHEN ha1.gross_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha2.gross_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha3.gross_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha4.gross_margin IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN ha5.gross_margin IS NOT NULL THEN 1 ELSE 0 END, 0
                        ) AS avg_gross_margin_historical,

                    -- Years of historical data
                    CASE WHEN ha1.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha2.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha3.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha4.symbol IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN ha5.symbol IS NOT NULL THEN 1 ELSE 0 END AS years_of_data

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

                -- Price data
                p.close_price AS price,
                p.price_date,

                -- Current financials
                mh.year_current AS fiscal_year,
                mh.latest_period_date AS financial_date,
                mh.report_currency,
                mh.is_ttm,
                mh.shares_current,
                mh.revenue_current,
                mh.gross_profit_current,
                mh.operating_income_current,
                mh.net_income_current,
                mh.ebitda_current,
                mh.cash_current,
                mh.total_debt_current,
                mh.total_equity_current,
                mh.fcf_current,
                mh.ocf_current,

                -- Current margins
                mh.gross_margin_current,
                mh.operating_margin_current,
                mh.net_margin_current,

                -- Historical margins
                mh.peak_operating_margin,
                mh.avg_operating_margin_historical,
                mh.avg_gross_margin_historical,

                -- Revenue trend (no FX - same currency comparison)
                CASE WHEN mh.revenue_1yr_ago > 0
                    THEN (mh.revenue_current - mh.revenue_1yr_ago)::DECIMAL / mh.revenue_1yr_ago
                    ELSE NULL END AS revenue_yoy,

                -- Gross margin stability check (YoY change)
                mh.gross_margin_1yr_ago,
                CASE WHEN mh.gross_margin_1yr_ago IS NOT NULL
                    THEN mh.gross_margin_current - mh.gross_margin_1yr_ago
                    ELSE NULL END AS gross_margin_change_yoy,

                -- EBITDA trend
                mh.ebitda_1yr_ago,
                CASE WHEN mh.ebitda_1yr_ago IS NOT NULL AND mh.ebitda_1yr_ago != 0
                    THEN (mh.ebitda_current - mh.ebitda_1yr_ago)::DECIMAL / ABS(mh.ebitda_1yr_ago)
                    ELSE NULL END AS ebitda_change_yoy,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- Market cap (in trading_currency)
                p.close_price * mh.shares_current AS market_cap,

                -- Enterprise value (with FX conversion for debt/cash)
                p.close_price * mh.shares_current + (COALESCE(mh.total_debt_current, 0) - COALESCE(mh.cash_current, 0)) * ({fx_rate_sql}) AS enterprise_value,

                -- Price to sales (with FX conversion)
                CASE WHEN mh.revenue_current > 0
                    THEN (p.close_price * mh.shares_current)::DECIMAL / (mh.revenue_current * ({fx_rate_sql}))
                    ELSE NULL END AS price_to_sales,

                -- EV to sales (with FX conversion)
                CASE WHEN mh.revenue_current > 0
                    THEN (p.close_price * mh.shares_current + (COALESCE(mh.total_debt_current, 0) - COALESCE(mh.cash_current, 0)) * ({fx_rate_sql}))::DECIMAL / (mh.revenue_current * ({fx_rate_sql}))
                    ELSE NULL END AS ev_to_sales,

                -- Cash runway (no FX - same currency comparison)
                CASE WHEN mh.ocf_current < 0 AND mh.cash_current > 0
                    THEN mh.cash_current::DECIMAL / (ABS(mh.ocf_current) / 12)
                    ELSE 999 END AS cash_runway_months,

                -- Margin improvement potential
                mh.peak_operating_margin - mh.operating_margin_current AS margin_recovery_potential,

                mh.years_of_data

            FROM companies_with_data c
            JOIN margin_history mh ON c.financial_symbol = mh.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE mh.shares_current > 0
              AND p.close_price > 0
              AND mh.revenue_current > 0
              -- SPEC FILTER 1: Gross margin > 35% (strong unit economics, NOT 20%)
              AND mh.gross_margin_current > 0.35
              -- SPEC FILTER 2: Gross margin stable (YoY change > -3pp, not collapsing)
              AND (mh.gross_margin_1yr_ago IS NULL
                   OR mh.gross_margin_current - mh.gross_margin_1yr_ago > -0.03)
              -- SPEC FILTER 3: Revenue not collapsing (YoY decline < 15%)
              AND (mh.revenue_1yr_ago IS NULL
                   OR mh.revenue_current >= mh.revenue_1yr_ago * 0.85)
              -- SPEC FILTER 4: Price to sales < 0.5 (death price)
              AND (p.close_price * mh.shares_current)::DECIMAL / (mh.revenue_current * ({fx_rate_sql})) < 0.5
              -- SPEC FILTER 5: EBITDA > 0 OR EBITDA improving YoY (turnaround signal)
              AND (mh.ebitda_current > 0
                   OR (mh.ebitda_1yr_ago IS NOT NULL
                       AND mh.ebitda_current > mh.ebitda_1yr_ago))
            ORDER BY
                -- Sort by margin recovery potential × valuation discount (with FX)
                (mh.peak_operating_margin - mh.operating_margin_current) *
                (1 - (p.close_price * mh.shares_current)::DECIMAL / (mh.revenue_current * ({fx_rate_sql}))) DESC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            p_s = float(row['price_to_sales']) if row['price_to_sales'] else 0
            margin_potential = float(row['margin_recovery_potential']) if row['margin_recovery_potential'] else 0
            gross_margin = float(row['gross_margin_current']) if row['gross_margin_current'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'enterprise_value': float(row['enterprise_value']) if row['enterprise_value'] else None,

                # Valuation
                'price_to_sales': p_s,
                'ev_to_sales': float(row['ev_to_sales']) if row['ev_to_sales'] else None,

                # Current margins
                'gross_margin_current': gross_margin,
                'operating_margin_current': float(row['operating_margin_current']) if row['operating_margin_current'] else None,
                'net_margin_current': float(row['net_margin_current']) if row['net_margin_current'] else None,

                # Historical margins
                'peak_operating_margin': float(row['peak_operating_margin']) if row['peak_operating_margin'] else None,
                'avg_operating_margin_historical': float(row['avg_operating_margin_historical']) if row['avg_operating_margin_historical'] else None,
                'avg_gross_margin_historical': float(row['avg_gross_margin_historical']) if row['avg_gross_margin_historical'] else None,

                # Recovery potential
                'margin_recovery_potential': margin_potential,

                # Revenue trend
                'revenue_current': float(row['revenue_current']) if row['revenue_current'] else None,
                'revenue_yoy': float(row['revenue_yoy']) if row['revenue_yoy'] else None,

                # Gross margin stability
                'gross_margin_1yr_ago': float(row['gross_margin_1yr_ago']) if row['gross_margin_1yr_ago'] else None,
                'gross_margin_change_yoy': float(row['gross_margin_change_yoy']) if row['gross_margin_change_yoy'] else None,

                # EBITDA trend
                'ebitda_current': float(row['ebitda_current']) if row['ebitda_current'] else None,
                'ebitda_1yr_ago': float(row['ebitda_1yr_ago']) if row['ebitda_1yr_ago'] else None,
                'ebitda_change_yoy': float(row['ebitda_change_yoy']) if row['ebitda_change_yoy'] else None,

                # Cash position
                'cash_current': float(row['cash_current']) if row['cash_current'] else None,
                'cash_runway_months': float(row['cash_runway_months']) if row['cash_runway_months'] else None,
                'free_cash_flow': float(row['fcf_current']) if row['fcf_current'] else None,

                # Other
                'total_debt': float(row['total_debt_current']) if row['total_debt_current'] else 0,
                'net_income': float(row['net_income_current']) if row['net_income_current'] else None,
                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'fiscal_year': row['fiscal_year'],

                # Data freshness indicators
                'is_ttm': row['is_ttm'],
                'data_source': 'TTM (4 quarters)' if row['is_ttm'] else 'Annual report',
                'years_of_data': row['years_of_data'],
            }

            # Build flags
            flags = []

            # Data freshness flag
            if row['is_ttm']:
                flags.append("TTM_DATA: Using trailing 12 months (fresher)")
            else:
                flags.append("ANNUAL_DATA: Using latest annual report")

            # Valuation flags
            if p_s < 0.2:
                flags.append("EXTREME_DISCOUNT: P/S under 0.2x")
            elif p_s < 0.3:
                flags.append("DEEP_DISCOUNT: P/S under 0.3x")

            # Unit economics flags (higher bar now - 35% is minimum)
            if gross_margin > 0.50:
                flags.append("EXCEPTIONAL_UNIT_ECONOMICS: Gross margin > 50%")
            elif gross_margin > 0.45:
                flags.append("STRONG_UNIT_ECONOMICS: Gross margin > 45%")

            # Gross margin stability
            gm_change = metrics.get('gross_margin_change_yoy')
            if gm_change is not None:
                if gm_change > 0.02:
                    flags.append("MARGIN_IMPROVING: Gross margin up YoY")
                elif gm_change > -0.01:
                    flags.append("MARGIN_STABLE: Gross margin holding")

            # EBITDA trend - key turnaround signal
            ebitda = metrics.get('ebitda_current', 0) or 0
            ebitda_change = metrics.get('ebitda_change_yoy')
            if ebitda > 0:
                flags.append("EBITDA_POSITIVE: Generating EBITDA")
            elif ebitda_change is not None and ebitda_change > 0:
                flags.append("EBITDA_IMPROVING: Turnaround signal")

            # Revenue trend
            revenue_yoy = metrics.get('revenue_yoy')
            if revenue_yoy and revenue_yoy > 0:
                flags.append("GROWING: Revenue increasing YoY")
            elif revenue_yoy and revenue_yoy > -0.05:
                flags.append("STABLE: Revenue decline < 5%")

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
        Calculate score based on valuation, unit economics, and recovery potential.
        """
        score = 0.0

        p_s = metrics.get('price_to_sales', 0.5) or 0.5
        gross_margin = metrics.get('gross_margin_current', 0.2) or 0.2
        margin_potential = metrics.get('margin_recovery_potential', 0) or 0
        revenue_yoy = metrics.get('revenue_yoy', 0) or 0
        fcf = metrics.get('free_cash_flow', 0) or 0

        # Price to sales scoring (up to 25 points)
        if p_s < 0.15:
            score += 25
        elif p_s < 0.20:
            score += 22
        elif p_s < 0.25:
            score += 18
        elif p_s < 0.30:
            score += 14
        elif p_s < 0.40:
            score += 10
        elif p_s < 0.50:
            score += 5

        # Gross margin (unit economics) scoring (up to 25 points)
        # Note: 35% is now minimum, so scoring starts higher
        if gross_margin > 0.60:
            score += 25
        elif gross_margin > 0.50:
            score += 22
        elif gross_margin > 0.45:
            score += 18
        elif gross_margin > 0.40:
            score += 14
        elif gross_margin > 0.35:
            score += 10  # Minimum threshold

        # Margin recovery potential (up to 20 points)
        if margin_potential > 0.15:
            score += 20
        elif margin_potential > 0.10:
            score += 16
        elif margin_potential > 0.07:
            score += 12
        elif margin_potential > 0.05:
            score += 8
        elif margin_potential > 0.03:
            score += 5

        # Revenue trend (up to 15 points)
        if revenue_yoy > 0.10:
            score += 15  # Growing fast
        elif revenue_yoy > 0.05:
            score += 12  # Growing
        elif revenue_yoy > 0:
            score += 10  # Slight growth
        elif revenue_yoy > -0.05:
            score += 6   # Stable
        elif revenue_yoy > -0.10:
            score += 3   # Small decline

        # Cash flow (up to 15 points)
        if fcf > 0:
            score += 15  # Generating cash - major positive
        elif metrics.get('cash_runway_months', 0) and metrics['cash_runway_months'] > 24:
            score += 10  # Long runway
        elif metrics.get('cash_runway_months', 0) and metrics['cash_runway_months'] > 12:
            score += 5   # Decent runway

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for turnaround assessment.
        """
        return f"""Assess the turnaround potential and risks for this company.

Company: {company_name}
Current P/S: {metrics.get('price_to_sales', 0):.2f}x
Gross margin: {(metrics.get('gross_margin_current', 0) or 0) * 100:.1f}%
Operating margin: {(metrics.get('operating_margin_current', 0) or 0) * 100:.1f}%
Peak historical operating margin: {(metrics.get('peak_operating_margin', 0) or 0) * 100:.1f}%
Margin recovery potential: {(metrics.get('margin_recovery_potential', 0) or 0) * 100:.1f}pp
Revenue YoY: {(metrics.get('revenue_yoy', 0) or 0) * 100:.1f}%
Cash runway: {metrics.get('cash_runway_months', 'N/A')} months

From the latest reports, determine:
1. CAUSE OF DECLINE: What caused operating margin compression?
   - Pricing pressure?
   - Cost inflation?
   - Overinvestment/expansion?
   - One-time charges?
   - Product transition?

2. MANAGEMENT PLAN: What is management doing to fix it?
   - Cost restructuring?
   - Price increases?
   - New products/markets?
   - Asset sales?

3. GROSS MARGIN SUSTAINABILITY: Is the gross margin stable or at risk?
   - Competitive threats?
   - Input cost trends?
   - Mix shift?

4. REVENUE TRAJECTORY: Is revenue stabilizing, growing, or declining?
   - Order book trends?
   - Customer retention?
   - Market share?

5. TIMELINE: When does management expect return to profitability?

6. RISKS: What could go wrong with the turnaround?
   - Competitive threats?
   - Debt maturity?
   - Key customer concentration?

Respond in JSON:
{{
  "decline_causes": ["string array"],
  "primary_cause": "PRICING|COSTS|INVESTMENT|CHARGES|TRANSITION|OTHER",
  "management_actions": ["string array"],
  "gross_margin_trend": "STABLE|IMPROVING|DECLINING",
  "revenue_trajectory": "GROWING|STABLE|DECLINING",
  "profitability_timeline_quarters": "number or null",
  "key_risks": ["string array"],
  "turnaround_probability": "HIGH|MEDIUM|LOW",
  "summary": "string"
}}"""
