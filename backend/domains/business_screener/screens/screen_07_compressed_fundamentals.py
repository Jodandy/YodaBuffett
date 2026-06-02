"""
Screen 7: Compressed Fundamentals

Identifies "coiled spring" opportunities - companies with temporarily suppressed
earnings that are likely to recover.

This is a Tier B only screen (no SQL/math filter). The LLM analyzes:
- One-time charges or extraordinary items
- Industry cyclicality vs company-specific issues
- Investment phase (R&D, expansion) vs structural decline
- Management commentary on margin outlook
- Historical margin patterns

Tier: B (local LLM) + C (API LLM for deep analysis)
Frequency: Quarterly
"""

from datetime import date
from typing import List, Dict, Any, Optional
from uuid import UUID

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class CompressedFundamentalsScreen(BaseScreen):
    """
    Screen 7: Compressed Fundamentals

    Finds companies with temporarily suppressed earnings that are likely to recover.
    No Tier A SQL filter - relies on LLM analysis of annual reports.

    The "coiled spring" thesis:
    - Current margins well below historical norms
    - Suppression appears temporary (not structural)
    - Clear catalyst or reversion expected

    Key LLM analysis areas:
    1. One-time charges: Restructuring, impairments, legal settlements
    2. Industry cycle: Sector-wide downturn vs company-specific
    3. Investment phase: R&D surge, geographic expansion, new products
    4. Cost structure: Temporary inflation vs permanent increases
    5. Management signals: Guidance on margin recovery
    """

    screen_type = 7

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Get candidates for Tier B LLM analysis.

        Tight pre-filter for "coiled spring" opportunities:
        - Margin compression (5pp below avg OR 40% below peak)
        - Gross margin still healthy (>30%) - core business intact
        - Revenue not collapsing (>80% of prior year)
        - Minimum market cap (200M SEK) - avoid micro-cap noise
        - Valuation gate (P/E < 25 OR P/S < 2) - market hasn't priced in recovery
        """
        # Get currency conversion SQL
        fx_rate = self.get_fx_rate_sql('COALESCE(c.report_currency, yf.currency)', 'c.stock_currency')

        query = f"""
            WITH latest_annual AS (
                -- Get latest annual financials from yahoo_financials
                SELECT DISTINCT ON (c.id)
                    c.id AS company_id,
                    c.company_name,
                    c.primary_ticker,
                    c.yahoo_symbol,
                    yf.currency AS report_currency,
                    CASE
                        WHEN c.yahoo_symbol LIKE '%%.ST' THEN 'SEK'
                        WHEN c.yahoo_symbol LIKE '%%.OL' THEN 'NOK'
                        WHEN c.yahoo_symbol LIKE '%%.CO' THEN 'DKK'
                        WHEN c.yahoo_symbol LIKE '%%.HE' THEN 'EUR'
                        ELSE 'SEK'
                    END AS stock_currency,
                    yf.period_date,
                    yf.fiscal_year,
                    (yf.income_statement->>'total_revenue')::NUMERIC AS total_revenue,
                    (yf.income_statement->>'gross_profit')::NUMERIC AS gross_profit,
                    (yf.income_statement->>'operating_income')::NUMERIC AS operating_income,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.income_statement->>'ebitda')::NUMERIC AS ebitda,
                    (yf.balance_sheet->>'ordinary_shares_number')::NUMERIC AS shares_outstanding,
                    (yf.cash_flow->>'free_cash_flow')::NUMERIC AS free_cash_flow,
                    ({fx_rate}) AS fx_rate
                FROM company_master c
                JOIN yahoo_financials yf ON REPLACE(c.primary_ticker, '-', ' ') = yf.symbol
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '18 months'
                  AND (yf.income_statement->>'total_revenue')::NUMERIC > 0
                  AND c.listing_status = 'active'
                ORDER BY c.id, yf.period_date DESC
            ),
            prior_year_revenue AS (
                -- Get revenue from 1 year ago for YoY comparison
                SELECT DISTINCT ON (c.id)
                    c.id AS company_id,
                    (yf.income_statement->>'total_revenue')::NUMERIC AS revenue_1yr_ago
                FROM company_master c
                JOIN yahoo_financials yf ON REPLACE(c.primary_ticker, '-', ' ') = yf.symbol
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '30 months'
                  AND yf.period_date < '{self._score_date}'::date - INTERVAL '6 months'
                  AND (yf.income_statement->>'total_revenue')::NUMERIC > 0
                ORDER BY c.id, yf.period_date DESC
            ),
            historical_margins AS (
                -- Get historical average margins (1-5 years back) from yahoo_financials
                SELECT
                    c.id AS company_id,
                    AVG(CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END) AS avg_operating_margin,
                    AVG(CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'ebitda')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END) AS avg_ebitda_margin,
                    MAX(CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC > 0
                        THEN (yf.income_statement->>'operating_income')::NUMERIC / (yf.income_statement->>'total_revenue')::NUMERIC
                        ELSE NULL END) AS peak_operating_margin,
                    COUNT(*) AS years_of_data
                FROM company_master c
                JOIN yahoo_financials yf ON REPLACE(c.primary_ticker, '-', ' ') = yf.symbol
                WHERE yf.statement_type = 'annual'
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '5 years'
                  AND yf.period_date < '{self._score_date}'::date - INTERVAL '1 year'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND (yf.income_statement->>'total_revenue')::NUMERIC > 0
                GROUP BY c.id
                HAVING COUNT(*) >= 2
            ),
            pit_prices AS (
                -- Get latest price on or before score_date for market cap
                SELECT DISTINCT ON (symbol)
                    symbol,
                    close_price,
                    date AS price_date
                FROM daily_price_data
                WHERE date <= '{self._score_date}'::date
                ORDER BY symbol, date DESC
            )
            SELECT
                la.company_id,
                la.company_name,
                la.primary_ticker,
                la.yahoo_symbol,
                la.report_currency,
                la.stock_currency,
                la.fx_rate,
                la.fiscal_year,
                la.period_date AS financial_date,
                la.total_revenue * la.fx_rate AS revenue,
                la.gross_profit * la.fx_rate AS gross_profit,
                la.operating_income * la.fx_rate AS operating_income,
                la.net_income * la.fx_rate AS net_income,
                la.ebitda * la.fx_rate AS ebitda,
                la.free_cash_flow * la.fx_rate AS free_cash_flow,
                la.shares_outstanding,
                p.close_price AS price,
                p.price_date,
                -- Market cap (in stock currency)
                p.close_price * la.shares_outstanding AS market_cap,
                -- Prior year revenue for YoY comparison
                pyr.revenue_1yr_ago * la.fx_rate AS revenue_1yr_ago,
                -- Revenue change YoY
                CASE WHEN pyr.revenue_1yr_ago > 0
                    THEN la.total_revenue / pyr.revenue_1yr_ago
                    ELSE NULL END AS revenue_yoy_ratio,
                -- Gross margin
                CASE WHEN la.total_revenue > 0 AND la.gross_profit IS NOT NULL
                    THEN la.gross_profit / la.total_revenue
                    ELSE NULL END AS gross_margin,
                -- Current margins
                CASE WHEN la.total_revenue > 0 THEN la.operating_income / la.total_revenue ELSE NULL END AS current_operating_margin,
                CASE WHEN la.total_revenue > 0 THEN la.ebitda / la.total_revenue ELSE NULL END AS current_ebitda_margin,
                -- Historical comparison
                hm.avg_operating_margin,
                hm.avg_ebitda_margin,
                hm.peak_operating_margin,
                hm.years_of_data,
                -- Compression metrics
                CASE
                    WHEN la.total_revenue > 0 AND hm.avg_operating_margin IS NOT NULL THEN
                        (la.operating_income / la.total_revenue) - hm.avg_operating_margin
                    ELSE NULL
                END AS margin_compression,
                CASE
                    WHEN hm.peak_operating_margin != 0 THEN
                        (hm.peak_operating_margin - (la.operating_income / la.total_revenue)) / ABS(hm.peak_operating_margin)
                    ELSE NULL
                END AS margin_vs_peak_pct,
                -- Valuation metrics
                CASE WHEN la.net_income > 0
                    THEN (p.close_price * la.shares_outstanding) / (la.net_income * la.fx_rate)
                    ELSE NULL END AS pe_ratio,
                CASE WHEN la.total_revenue > 0
                    THEN (p.close_price * la.shares_outstanding) / (la.total_revenue * la.fx_rate)
                    ELSE NULL END AS ps_ratio,
                TRUE AS has_annual_report,
                -- Cyclical classification
                cc.classification AS cyclical_classification,
                cc.cycle_position
            FROM latest_annual la
            JOIN historical_margins hm ON la.company_id = hm.company_id
            JOIN pit_prices p ON la.primary_ticker = p.symbol
            LEFT JOIN prior_year_revenue pyr ON la.company_id = pyr.company_id
            LEFT JOIN bsd_company_classifications cc ON la.company_id = cc.company_id
            WHERE la.shares_outstanding > 0
              AND p.close_price > 0
              -- FILTER 1: Compression detection (tightened from 3pp/30% to 5pp/40%)
              AND (
                  -- 5pp below historical average
                  (la.operating_income / NULLIF(la.total_revenue, 0)) < (hm.avg_operating_margin - 0.05)
                  -- OR 40% below peak
                  OR (la.operating_income / NULLIF(la.total_revenue, 0)) < (hm.peak_operating_margin * 0.60)
              )
              -- FILTER 2: Gross margin still healthy (core business intact)
              AND la.gross_profit IS NOT NULL
              AND (la.gross_profit / NULLIF(la.total_revenue, 0)) > 0.30
              -- FILTER 3: Revenue not collapsing (max 20% decline YoY)
              AND (
                  pyr.revenue_1yr_ago IS NULL  -- Allow if no prior year data
                  OR (la.total_revenue / NULLIF(pyr.revenue_1yr_ago, 0)) > 0.80
              )
              -- FILTER 4: Minimum market cap (200M SEK equivalent)
              AND (p.close_price * la.shares_outstanding) > 200000000
              -- FILTER 5: Valuation gate (market hasn't priced in recovery)
              AND (
                  -- P/E < 25 (for profitable companies)
                  (la.net_income > 0 AND (p.close_price * la.shares_outstanding) / (la.net_income * la.fx_rate) < 25)
                  -- OR P/S < 2 (for unprofitable or low-margin companies)
                  OR (p.close_price * la.shares_outstanding) / (la.total_revenue * la.fx_rate) < 2.0
              )
            ORDER BY
                -- Prioritize largest margin compression
                (la.operating_income / NULLIF(la.total_revenue, 0)) - hm.avg_operating_margin ASC
        """

        rows = await self.conn.fetch(query)
        results = []

        for row in rows:
            row_dict = dict(row)

            metrics = {
                'company_name': row_dict['company_name'],
                'primary_ticker': row_dict['primary_ticker'],
                'yahoo_symbol': row_dict['yahoo_symbol'],
                'fiscal_year': row_dict['fiscal_year'],
                'financial_date': row_dict['financial_date'].isoformat() if row_dict.get('financial_date') else None,
                'price': float(row_dict['price']) if row_dict.get('price') else None,
                'price_date': row_dict['price_date'].isoformat() if row_dict.get('price_date') else None,
                'market_cap': float(row_dict['market_cap']) if row_dict.get('market_cap') else None,
                'revenue': float(row_dict['revenue']) if row_dict['revenue'] else None,
                'revenue_1yr_ago': float(row_dict['revenue_1yr_ago']) if row_dict.get('revenue_1yr_ago') else None,
                'revenue_yoy_ratio': float(row_dict['revenue_yoy_ratio']) if row_dict.get('revenue_yoy_ratio') else None,
                'gross_profit': float(row_dict['gross_profit']) if row_dict.get('gross_profit') else None,
                'gross_margin': float(row_dict['gross_margin']) if row_dict.get('gross_margin') else None,
                'operating_income': float(row_dict['operating_income']) if row_dict['operating_income'] else None,
                'net_income': float(row_dict['net_income']) if row_dict.get('net_income') else None,
                'ebitda': float(row_dict['ebitda']) if row_dict['ebitda'] else None,
                'free_cash_flow': float(row_dict['free_cash_flow']) if row_dict.get('free_cash_flow') else None,
                'current_operating_margin': float(row_dict['current_operating_margin']) if row_dict['current_operating_margin'] else None,
                'current_ebitda_margin': float(row_dict['current_ebitda_margin']) if row_dict['current_ebitda_margin'] else None,
                'avg_operating_margin': float(row_dict['avg_operating_margin']) if row_dict['avg_operating_margin'] else None,
                'avg_ebitda_margin': float(row_dict['avg_ebitda_margin']) if row_dict['avg_ebitda_margin'] else None,
                'peak_operating_margin': float(row_dict['peak_operating_margin']) if row_dict['peak_operating_margin'] else None,
                'margin_compression': float(row_dict['margin_compression']) if row_dict['margin_compression'] else None,
                'margin_vs_peak_pct': float(row_dict['margin_vs_peak_pct']) if row_dict.get('margin_vs_peak_pct') else None,
                'pe_ratio': float(row_dict['pe_ratio']) if row_dict.get('pe_ratio') else None,
                'ps_ratio': float(row_dict['ps_ratio']) if row_dict.get('ps_ratio') else None,
                'years_of_data': row_dict['years_of_data'],
                'has_annual_report': row_dict.get('has_annual_report', False),
                'report_currency': row_dict['report_currency'],
                'stock_currency': row_dict['stock_currency'],
                'cyclical_classification': row_dict.get('cyclical_classification'),
                'cycle_position': row_dict.get('cycle_position'),
            }

            flags = []

            # CRITICAL: Cyclical warning - compression may be normal cycle, not coiled spring
            cyclical_class = row_dict.get('cyclical_classification')
            if cyclical_class == 'CYCLICAL':
                cycle_pos = row_dict.get('cycle_position')
                if cycle_pos:
                    flags.append(f'CYCLICAL_WARNING: Classified as cyclical ({cycle_pos}) - compression may be normal')
                else:
                    flags.append('CYCLICAL_WARNING: Classified as cyclical - compression may be normal cycle behavior')
            else:
                # Sector-based cyclical detection for unclassified companies
                company_name = row_dict.get('company_name', '').lower()
                ticker = row_dict.get('primary_ticker', '').upper()

                # Oil & Gas indicators
                oil_keywords = ['oil', 'energy', 'petro', 'gas', 'equinor', 'aker bp', 'dno', 'okea']
                # Mining/Materials indicators
                mining_keywords = ['mining', 'gruber', 'rana', 'boliden', 'lundin']
                # Forestry/Paper indicators
                forestry_keywords = ['forest', 'skog', 'holmen', 'billerud', 'sca', 'rottneros', 'paper']
                # Shipping indicators
                shipping_keywords = ['shipping', 'tanker', 'bulk', 'freight']

                is_likely_cyclical = any(kw in company_name for kw in oil_keywords + mining_keywords + forestry_keywords + shipping_keywords)

                if is_likely_cyclical:
                    flags.append('LIKELY_CYCLICAL: Sector suggests cyclical business - verify compression is not normal')

            # Compression severity flags
            margin_compression = row_dict.get('margin_compression')
            if margin_compression and margin_compression < -0.10:
                flags.append('SEVERE_COMPRESSION: >10pp below average')
            elif margin_compression and margin_compression < -0.07:
                flags.append('STRONG_COMPRESSION: >7pp below average')

            margin_vs_peak = row_dict.get('margin_vs_peak_pct')
            if margin_vs_peak and margin_vs_peak > 0.60:
                flags.append('DEEP_BELOW_PEAK: >60% below peak margin')

            # Profitability status
            if row_dict['current_operating_margin'] and row_dict['current_operating_margin'] < 0:
                flags.append('CURRENTLY_UNPROFITABLE')
            elif row_dict['current_operating_margin'] and row_dict['current_operating_margin'] < 0.03:
                flags.append('NEAR_BREAKEVEN: <3% operating margin')

            # Gross margin quality
            gross_margin = row_dict.get('gross_margin')
            if gross_margin and gross_margin > 0.50:
                flags.append('HIGH_GROSS_MARGIN: >50% (strong pricing power)')
            elif gross_margin and gross_margin > 0.40:
                flags.append('GOOD_GROSS_MARGIN: >40%')

            # Revenue trend
            revenue_yoy = row_dict.get('revenue_yoy_ratio')
            if revenue_yoy:
                if revenue_yoy > 1.10:
                    flags.append('GROWING_REVENUE: >10% YoY growth')
                elif revenue_yoy < 0.90:
                    flags.append('DECLINING_REVENUE: >10% YoY decline')

            # Valuation context
            pe = row_dict.get('pe_ratio')
            ps = row_dict.get('ps_ratio')
            if pe and pe < 10:
                flags.append('CHEAP_PE: P/E < 10')
            elif pe and pe < 15:
                flags.append('REASONABLE_PE: P/E < 15')
            if ps and ps < 1.0:
                flags.append('CHEAP_PS: P/S < 1')

            # FCF status
            fcf = row_dict.get('free_cash_flow')
            if fcf and fcf > 0:
                flags.append('FCF_POSITIVE: Generating cash despite compressed margins')
            elif fcf and fcf < 0:
                flags.append('FCF_NEGATIVE: Burning cash')

            # Currency conversion warning
            self.add_currency_warning(flags, row_dict['report_currency'], row_dict['stock_currency'])

            results.append(self.create_result(
                company_id=row_dict['company_id'],
                metrics=metrics,
                tier='A',  # Technically pre-filter, but stored as A
                flags=flags
            ))

        self.log(f"Found {len(results)} candidates with margin compression for Tier B analysis")
        return results

    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Rebalanced scoring formula - every point is earned, no base score.

        Components (max 100 total, no clamping needed):
        - Compression magnitude: 0-20 points
        - Peak comparison: 0-10 points
        - Gross margin quality: 0-20 points (doubled - key quality signal)
        - Revenue trend: -5 to +15 points
        - Valuation: 0-20 points (doubled - cheaper = more upside)
        - FCF positive: 0 or 10 points (binary)
        - Cyclical penalty: 0 or -15 points
        - Data quality: 0-5 points
        """
        score = 0.0  # No free points - every point is earned

        # === COMPRESSION MAGNITUDE (0-20 points) ===
        margin_compression = metrics.get('margin_compression', 0) or 0
        if margin_compression <= -0.15:  # >= 15pp below average
            score += 20
        elif margin_compression <= -0.10:  # >= 10pp below average
            score += 16
        elif margin_compression <= -0.07:  # >= 7pp below average
            score += 12
        elif margin_compression <= -0.05:  # >= 5pp below average
            score += 8
        else:
            score += 4  # Minimum (passed 5pp filter)

        # === PEAK COMPARISON (0-10 points) ===
        margin_vs_peak = metrics.get('margin_vs_peak_pct', 0) or 0
        if margin_vs_peak >= 0.60:  # >= 60% below peak
            score += 10
        elif margin_vs_peak >= 0.40:  # >= 40% below peak
            score += 7
        elif margin_vs_peak >= 0.30:  # >= 30% below peak
            score += 4
        # else: 0 points

        # === GROSS MARGIN QUALITY (0-20 points) - Key quality signal ===
        gross_margin = metrics.get('gross_margin', 0) or 0
        if gross_margin >= 0.70:
            score += 20
        elif gross_margin >= 0.60:
            score += 16
        elif gross_margin >= 0.50:
            score += 12
        elif gross_margin >= 0.40:
            score += 8
        elif gross_margin >= 0.30:
            score += 4  # Minimum (passed 30% filter)

        # === REVENUE TREND (-5 to +15 points) ===
        revenue_yoy = metrics.get('revenue_yoy_ratio', 1.0) or 1.0
        if revenue_yoy >= 1.10:  # Growing >= 10% YoY
            score += 15
        elif revenue_yoy >= 1.05:  # Growing >= 5% YoY
            score += 12
        elif revenue_yoy >= 1.0:  # Stable or slight growth
            score += 8
        elif revenue_yoy >= 0.90:  # Declining 0-10% YoY
            score += 3
        elif revenue_yoy >= 0.80:  # Declining 10-20% YoY
            score += 0
        else:  # Declining > 20% YoY
            score -= 5

        # === VALUATION (0-20 points) - Use P/E if positive, else P/S ===
        pe = metrics.get('pe_ratio')
        ps = metrics.get('ps_ratio', 2.0) or 2.0

        if pe and pe > 0:
            # Use P/E for profitable companies
            if pe < 8:
                score += 20
            elif pe < 12:
                score += 15
            elif pe < 15:
                score += 10
            elif pe < 20:
                score += 5
            # else: 0 points
        else:
            # Use P/S for unprofitable companies
            if ps < 0.5:
                score += 20
            elif ps < 1.0:
                score += 15
            elif ps < 1.5:
                score += 10
            elif ps < 2.0:
                score += 5
            # else: 0 points

        # === FCF POSITIVE (0 or 10 points) - Binary ===
        fcf = metrics.get('free_cash_flow')
        if fcf and fcf > 0:
            score += 10
        # else: 0 points

        # === CYCLICAL PENALTY (0 or -15 points) ===
        cyclical_class = metrics.get('cyclical_classification')
        if cyclical_class == 'CYCLICAL':
            score -= 15

        # === DATA QUALITY (0-5 points) ===
        years = metrics.get('years_of_data', 0) or 0
        if years >= 4:
            score += 5
        elif years >= 3:
            score += 3
        elif years >= 2:
            score += 1

        # Ensure score stays in 0-100 range
        return max(0.0, min(100.0, score))

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """Generate Tier B prompt for compressed fundamentals analysis."""
        current_margin = metrics.get('current_operating_margin', 0) or 0
        avg_margin = metrics.get('avg_operating_margin', 0) or 0
        peak_margin = metrics.get('peak_operating_margin', 0) or 0
        compression = metrics.get('margin_compression_pct', 0) or 0

        return f"""Analyze {company_name}'s margin compression to determine if it's temporary or structural.

CURRENT SITUATION:
- Current Operating Margin: {current_margin:.1%}
- Historical Average Margin: {avg_margin:.1%}
- Peak Historical Margin: {peak_margin:.1%}
- Compression vs Average: {compression:.0%}

ANALYSIS REQUIRED:

1. ONE-TIME ITEMS: Are there restructuring charges, impairments, legal settlements, or other one-time items depressing current margins?

2. INDUSTRY VS COMPANY: Is this an industry-wide downturn or company-specific issues? Industry cycles suggest recovery; company-specific may be structural.

3. INVESTMENT PHASE: Is the company investing in R&D, expansion, or new products that temporarily depress margins? These can be positive if executed well.

4. COST STRUCTURE: Are cost increases (wages, materials, energy) temporary or permanent? Have competitors faced similar increases?

5. MANAGEMENT SIGNALS: What is management saying about margin recovery? Are there concrete targets or timelines?

6. RECOVERY CATALYST: What specific event or trend would normalize margins?

Respond in JSON:
{{
    "compression_type": "TEMPORARY|STRUCTURAL|MIXED",
    "primary_cause": "<one sentence>",
    "one_time_items_detected": true|false,
    "one_time_items_detail": "<description or null>",
    "industry_wide": true|false,
    "investment_phase": true|false,
    "management_guidance": "OPTIMISTIC|NEUTRAL|PESSIMISTIC|NONE",
    "recovery_timeline": "6_MONTHS|1_YEAR|2_YEARS|UNCLEAR|UNLIKELY",
    "recovery_catalyst": "<description or null>",
    "margin_recovery_potential": "HIGH|MEDIUM|LOW",
    "confidence": "HIGH|MEDIUM|LOW",
    "risk_factors": ["<list of key risks>"],
    "summary": "<2-3 sentence investment thesis>"
}}"""

    def get_tier_c_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """Generate Tier C prompt for deep compressed fundamentals analysis."""
        return f"""Perform a deep analysis of {company_name}'s margin compression and recovery potential.

This is a Tier C deep dive for a promising compressed fundamentals candidate.

COMPREHENSIVE ANALYSIS REQUIRED:

1. HISTORICAL CONTEXT
   - Map margin history over the past 5-7 years
   - Identify previous compression episodes and recovery patterns
   - Quantify the magnitude of current compression vs historical

2. ROOT CAUSE ANALYSIS
   - Detail each factor contributing to margin compression
   - Estimate the margin impact of each factor
   - Classify factors as temporary vs structural

3. PEER COMPARISON
   - How do margins compare to industry peers?
   - Are peers experiencing similar compression?
   - What do peer recovery patterns suggest?

4. MANAGEMENT TRACK RECORD
   - Has management successfully navigated margin recovery before?
   - Are cost reduction initiatives credible?
   - What is their track record on guidance?

5. QUANTIFIED RECOVERY SCENARIO
   - If temporary factors reverse, what margin is achievable?
   - What is the timeline for normalization?
   - What would earnings look like at normalized margins?

6. RISK ASSESSMENT
   - What could make compression permanent?
   - Competitive threats to margin recovery
   - Balance sheet constraints on turnaround

Respond in JSON:
{{
    "historical_margin_range": {{"low": <pct>, "high": <pct>, "avg": <pct>}},
    "compression_factors": [
        {{"factor": "<name>", "margin_impact_pct": <number>, "type": "TEMPORARY|STRUCTURAL"}}
    ],
    "peer_comparison": {{
        "vs_peers": "BETTER|IN_LINE|WORSE",
        "peers_compressed": true|false
    }},
    "management_credibility": "HIGH|MEDIUM|LOW",
    "normalized_margin_estimate": <pct>,
    "recovery_probability": <0-100>,
    "recovery_timeline_months": <number or null>,
    "earnings_at_normal_margin": {{
        "current_earnings": <number or null>,
        "normalized_earnings": <number or null>,
        "upside_pct": <number or null>
    }},
    "key_risks": ["<list>"],
    "key_catalysts": ["<list>"],
    "investment_recommendation": "STRONG_BUY|BUY|HOLD|AVOID",
    "thesis": "<detailed investment thesis - 3-5 sentences>"
}}"""
