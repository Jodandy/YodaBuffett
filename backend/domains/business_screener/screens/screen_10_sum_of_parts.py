"""
Screen 10: Sum-of-Parts (SoTP)

Identifies conglomerates and multi-segment businesses trading below
the sum of their parts. Hidden value often exists in:
- Segment reporting in footnotes
- Non-core assets
- Real estate holdings
- Minority stakes in other companies
- Intellectual property/patents

This is a Tier B only screen. The LLM analyzes segment data
and identifies hidden value opportunities.

Tier: B (local LLM) + C (API LLM for deep analysis)
Frequency: Annually (segment data changes slowly)
"""

from datetime import date
from typing import List, Dict, Any, Optional
from uuid import UUID

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class SumOfPartsScreen(BaseScreen):
    """
    Screen 10: Sum-of-Parts

    Finds multi-segment businesses trading below the sum of their parts.
    No simple Tier A SQL filter - relies on LLM analysis of segment reporting.

    Sum-of-parts opportunities often arise when:
    1. Conglomerate discount: Market applies blanket discount
    2. Hidden segments: Small but valuable business lines obscured
    3. Non-core assets: Real estate, investments not reflected in stock price
    4. Misunderstood mix: High-value segment obscured by low-value segment

    LLM analysis reads segment reporting to:
    - Identify distinct business segments
    - Value each segment independently
    - Compare to traded peers
    - Calculate conglomerate discount
    """

    screen_type = 10

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Get candidates for Tier B LLM analysis.

        Since this is a Tier B only screen, we return companies that:
        - Are likely conglomerates or multi-segment businesses
        - Have significant assets relative to market cap (potential hidden value)
        - Have annual reports available for segment analysis

        Heuristics for multi-segment companies:
        - Large companies (more likely to have multiple segments)
        - Trading below book value (potential hidden assets)
        - High asset intensity
        """
        # Get currency conversion SQL - use company_master.report_currency (correct)
        # instead of yf.currency which is often wrong for EUR/USD reporters
        fx_rate = self.get_fx_rate_sql('COALESCE(c.report_currency, yf.currency)', 'c.stock_currency')

        query = f"""
            WITH latest_annual AS (
                -- Get latest annual financials from yahoo_financials (includes goodwill, intangibles!)
                SELECT DISTINCT ON (c.id)
                    c.id AS company_id,
                    c.company_name,
                    c.primary_ticker,
                    c.yahoo_symbol,
                    c.sector,
                    c.industry,
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
                    (yf.income_statement->>'operating_income')::NUMERIC AS operating_income,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.income_statement->>'ebitda')::NUMERIC AS ebitda,
                    (yf.balance_sheet->>'total_assets')::NUMERIC AS total_assets,
                    (yf.balance_sheet->>'current_assets')::NUMERIC AS current_assets,
                    (yf.balance_sheet->>'cash_and_cash_equivalents')::NUMERIC AS cash_and_equivalents,
                    (yf.balance_sheet->>'investments_and_advances')::NUMERIC AS investments,
                    (yf.balance_sheet->>'net_ppe')::NUMERIC AS property_plant_equipment,
                    (yf.balance_sheet->>'goodwill')::NUMERIC AS goodwill,
                    (yf.balance_sheet->>'other_intangible_assets')::NUMERIC AS intangible_assets,
                    (yf.balance_sheet->>'total_liabilities_net_minority_interest')::NUMERIC AS total_liabilities,
                    (yf.balance_sheet->>'total_debt')::NUMERIC AS total_debt,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    (yf.income_statement->>'basic_average_shares')::NUMERIC AS shares_outstanding,
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
            price_data AS (
                -- Get latest price
                SELECT DISTINCT ON (c.id)
                    c.id AS company_id,
                    dpd.close_price,
                    dpd.date AS price_date
                FROM company_master c
                JOIN daily_price_data dpd ON c.primary_ticker = dpd.symbol
                WHERE {self.get_price_date_filter('dpd')}
                ORDER BY c.id, dpd.date DESC
            )
            SELECT
                la.company_id,
                la.company_name,
                la.primary_ticker,
                la.yahoo_symbol,
                la.sector,
                la.industry,
                la.report_currency,
                la.stock_currency,
                la.fx_rate,
                la.fiscal_year,
                -- Apply FX conversion to all financial values
                la.total_revenue * la.fx_rate AS revenue,
                la.operating_income * la.fx_rate AS operating_income,
                la.net_income * la.fx_rate AS net_income,
                la.ebitda * la.fx_rate AS ebitda,
                la.total_assets * la.fx_rate AS total_assets,
                la.current_assets * la.fx_rate AS current_assets,
                la.cash_and_equivalents * la.fx_rate AS cash,
                COALESCE(la.investments, 0) * la.fx_rate AS investments,
                la.property_plant_equipment * la.fx_rate AS ppe,
                COALESCE(la.goodwill, 0) * la.fx_rate AS goodwill,
                COALESCE(la.intangible_assets, 0) * la.fx_rate AS intangibles,
                la.total_liabilities * la.fx_rate AS total_liabilities,
                la.total_debt * la.fx_rate AS total_debt,
                la.total_equity * la.fx_rate AS book_value,
                la.shares_outstanding,
                pd.close_price,
                -- Calculate market cap
                pd.close_price * la.shares_outstanding AS market_cap,
                -- Calculate P/B ratio
                CASE
                    WHEN la.total_equity > 0 THEN
                        (pd.close_price * la.shares_outstanding) / (la.total_equity * la.fx_rate)
                    ELSE NULL
                END AS price_to_book,
                -- Calculate asset coverage
                CASE
                    WHEN pd.close_price * la.shares_outstanding > 0 THEN
                        la.total_assets * la.fx_rate / (pd.close_price * la.shares_outstanding)
                    ELSE NULL
                END AS asset_coverage,
                -- Non-operating assets ratio
                CASE
                    WHEN la.total_assets > 0 THEN
                        (COALESCE(la.investments, 0) + COALESCE(la.goodwill, 0) + COALESCE(la.intangible_assets, 0)) / la.total_assets
                    ELSE NULL
                END AS non_operating_asset_ratio,
                TRUE AS has_annual_report  -- Assume documents available for LLM analysis
            FROM latest_annual la
            JOIN price_data pd ON la.company_id = pd.company_id
            WHERE
                -- Reasonable size companies (more likely to have multiple segments)
                pd.close_price * la.shares_outstanding > 1e9  -- >1B market cap in local currency
                -- AND some indicator of potential hidden value:
                AND (
                    -- Trading below book value
                    (pd.close_price * la.shares_outstanding) < (la.total_equity * la.fx_rate)
                    -- OR significant non-operating assets
                    OR (COALESCE(la.investments, 0) + COALESCE(la.goodwill, 0) + COALESCE(la.intangible_assets, 0)) > (pd.close_price * la.shares_outstanding * 0.3)
                    -- OR high asset coverage
                    OR la.total_assets * la.fx_rate > (pd.close_price * la.shares_outstanding * 2)
                    -- OR in sectors typically having multiple segments
                    OR la.sector ILIKE '%industrial%'
                    OR la.sector ILIKE '%conglomerate%'
                    OR la.sector ILIKE '%holding%'
                    OR la.industry ILIKE '%diversified%'
                )
            ORDER BY
                -- Prioritize by discount to book and asset coverage
                (pd.close_price * la.shares_outstanding) / NULLIF(la.total_equity * la.fx_rate, 0) ASC,
                la.total_assets * la.fx_rate / NULLIF(pd.close_price * la.shares_outstanding, 0) DESC
            LIMIT 50
        """

        rows = await self.conn.fetch(query)
        results = []

        for row in rows:
            row_dict = dict(row)

            metrics = {
                'company_name': row_dict['company_name'],
                'primary_ticker': row_dict['primary_ticker'],
                'yahoo_symbol': row_dict['yahoo_symbol'],
                'sector': row_dict.get('sector'),
                'industry': row_dict.get('industry'),
                'fiscal_year': row_dict['fiscal_year'],
                'market_cap': float(row_dict['market_cap']) if row_dict.get('market_cap') else None,
                'revenue': float(row_dict['revenue']) if row_dict.get('revenue') else None,
                'operating_income': float(row_dict['operating_income']) if row_dict.get('operating_income') else None,
                'ebitda': float(row_dict['ebitda']) if row_dict.get('ebitda') else None,
                'total_assets': float(row_dict['total_assets']) if row_dict.get('total_assets') else None,
                'book_value': float(row_dict['book_value']) if row_dict.get('book_value') else None,
                'cash': float(row_dict['cash']) if row_dict.get('cash') else None,
                'investments': float(row_dict['investments']) if row_dict.get('investments') else None,
                'ppe': float(row_dict['ppe']) if row_dict.get('ppe') else None,
                'goodwill': float(row_dict['goodwill']) if row_dict.get('goodwill') else None,
                'intangibles': float(row_dict['intangibles']) if row_dict.get('intangibles') else None,
                'total_debt': float(row_dict['total_debt']) if row_dict.get('total_debt') else None,
                'price_to_book': float(row_dict['price_to_book']) if row_dict.get('price_to_book') else None,
                'asset_coverage': float(row_dict['asset_coverage']) if row_dict.get('asset_coverage') else None,
                'non_operating_asset_ratio': float(row_dict['non_operating_asset_ratio']) if row_dict.get('non_operating_asset_ratio') else None,
                'has_annual_report': row_dict.get('has_annual_report', False),
                'report_currency': row_dict['report_currency'],
                'stock_currency': row_dict['stock_currency'],
            }

            flags = []

            # Classify potential opportunity type
            pb = row_dict.get('price_to_book')
            if pb and pb < 0.7:
                flags.append('DEEP_DISCOUNT_TO_BOOK')
            elif pb and pb < 1.0:
                flags.append('BELOW_BOOK')

            asset_ratio = row_dict.get('non_operating_asset_ratio') or 0
            if asset_ratio > 0.4:
                flags.append('HIGH_NON_OPERATING_ASSETS')

            if row_dict.get('investments') and float(row_dict['investments']) > 0:
                flags.append('HAS_INVESTMENTS')

            if row_dict.get('goodwill') and float(row_dict['goodwill']) > float(row_dict.get('market_cap', 1) or 1) * 0.3:
                flags.append('HIGH_GOODWILL')

            if not row_dict.get('has_annual_report'):
                flags.append('NO_ANNUAL_REPORT_FOR_LLM')

            # Currency conversion warning
            self.add_currency_warning(flags, row_dict['report_currency'], row_dict['stock_currency'])

            results.append(self.create_result(
                company_id=row_dict['company_id'],
                metrics=metrics,
                tier='A',
                flags=flags
            ))

        self.log(f"Found {len(results)} candidates for sum-of-parts analysis")
        return results

    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate preliminary score based on potential hidden value.

        Final score is determined by Tier B LLM analysis.
        """
        score = 45.0  # Base score

        # Discount to book value
        pb = metrics.get('price_to_book')
        if pb:
            if pb < 0.5:
                score += 25
            elif pb < 0.7:
                score += 20
            elif pb < 0.85:
                score += 15
            elif pb < 1.0:
                score += 10

        # Asset coverage
        asset_coverage = metrics.get('asset_coverage')
        if asset_coverage:
            if asset_coverage > 3.0:
                score += 15
            elif asset_coverage > 2.0:
                score += 10
            elif asset_coverage > 1.5:
                score += 5

        # Non-operating assets suggest potential hidden value
        non_op_ratio = metrics.get('non_operating_asset_ratio')
        if non_op_ratio:
            if non_op_ratio > 0.4:
                score += 10
            elif non_op_ratio > 0.2:
                score += 5

        # Has investments (potential stake in other companies)
        if metrics.get('investments') and metrics['investments'] > 0:
            score += 5

        # Penalty if no documents for LLM analysis
        if not metrics.get('has_annual_report'):
            score -= 15

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """Generate Tier B prompt for sum-of-parts analysis."""
        market_cap = metrics.get('market_cap')
        book_value = metrics.get('book_value')
        pb = metrics.get('price_to_book')
        total_assets = metrics.get('total_assets')
        investments = metrics.get('investments') or 0
        goodwill = metrics.get('goodwill') or 0

        market_cap_str = f"${market_cap/1e6:.0f}M" if market_cap else "Unknown"
        book_value_str = f"${book_value/1e6:.0f}M" if book_value else "Unknown"
        pb_str = f"{pb:.2f}x" if pb else "Unknown"
        total_assets_str = f"${total_assets/1e6:.0f}M" if total_assets else "Unknown"
        investments_str = f"${investments/1e6:.0f}M" if investments else "$0M"
        goodwill_str = f"${goodwill/1e6:.0f}M" if goodwill else "$0M"

        return f"""Perform a sum-of-parts analysis of {company_name} to identify potential hidden value.

COMPANY OVERVIEW:
- Market Cap: {market_cap_str}
- Book Value: {book_value_str}
- Price/Book: {pb_str}
- Total Assets: {total_assets_str}
- Recorded Investments: {investments_str}
- Goodwill: {goodwill_str}
- Sector: {metrics.get('sector', 'Unknown')}
- Industry: {metrics.get('industry', 'Unknown')}

ANALYSIS REQUIRED:

1. BUSINESS SEGMENTS: Identify all distinct business segments from segment reporting.
   - What businesses does the company operate?
   - What is the revenue/profit contribution of each segment?

2. SEGMENT VALUATION: For each segment:
   - What are comparable pure-play companies?
   - What multiples do peers trade at?
   - What is a reasonable standalone value?

3. NON-CORE ASSETS: Identify assets not reflected in operating earnings:
   - Real estate holdings (owned vs rented)
   - Investment stakes in other companies
   - Intellectual property/patents
   - Brand value
   - Excess cash

4. HIDDEN LIABILITIES: Any off-balance sheet items or contingencies?

5. SUM-OF-PARTS CALCULATION:
   - Sum segment values
   - Add non-core asset values
   - Subtract net debt
   - Compare to market cap

Respond in JSON:
{{
    "segments": [
        {{
            "name": "<segment name>",
            "revenue_contribution": "<percentage or amount>",
            "profit_contribution": "<percentage or amount>",
            "comparable_peers": ["<list>"],
            "implied_multiple": "<EV/EBITDA or other>",
            "estimated_value": "<amount or range>"
        }}
    ],
    "non_core_assets": [
        {{
            "asset_type": "<real_estate|investment|ip|brand|cash|other>",
            "description": "<brief description>",
            "estimated_value": "<amount or range>",
            "valuation_method": "<how valued>"
        }}
    ],
    "sum_of_parts_value": "<total estimated value>",
    "net_debt": "<net debt amount>",
    "equity_value_sotp": "<sum_of_parts minus net_debt>",
    "market_cap": "{market_cap_str}",
    "implied_discount": "<percentage discount to SOTP>",
    "discount_reasons": ["<why might discount exist>"],
    "catalyst_for_value_realization": "<how might value be unlocked>",
    "confidence": "HIGH|MEDIUM|LOW",
    "summary": "<2-3 sentence investment thesis>"
}}"""

    def get_tier_c_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """Generate Tier C prompt for deep sum-of-parts analysis."""
        return f"""Perform a comprehensive sum-of-parts valuation of {company_name}.

This is a Tier C deep dive for a promising sum-of-parts opportunity.

DETAILED ANALYSIS REQUIRED:

1. SEGMENT DEEP DIVE
   For each business segment:
   - Detailed financial analysis (revenue, margins, growth)
   - Competitive position assessment
   - Industry dynamics and outlook
   - Appropriate valuation methodology
   - Range of values based on different assumptions

2. COMPARABLE COMPANY ANALYSIS
   For each segment:
   - List 3-5 most relevant public comparables
   - Analyze why each is comparable
   - Detail trading multiples
   - Apply appropriate discount/premium for size, quality

3. HIDDEN ASSET INVENTORY
   Comprehensive search for:
   - Real estate: Owned properties, book vs market value
   - Investments: Listed and unlisted stakes
   - Intellectual property: Patents, trademarks, trade secrets
   - Tax assets: NOLs, credits
   - Other: Overfunded pensions, valuable contracts

4. SCENARIO ANALYSIS
   Build out three scenarios:
   - Conservative: Bearish assumptions
   - Base case: Most likely outcome
   - Optimistic: Bull case
   Include specific value for each scenario

5. CATALYST ANALYSIS
   What would unlock the sum-of-parts value?
   - Spin-off or split
   - Activist involvement
   - M&A interest
   - Management change
   - Asset sales
   Timeline and probability for each catalyst

6. RISK ASSESSMENT
   What could destroy the thesis?
   - Integration value loss if segments separated
   - Execution risk
   - Leverage constraints
   - Management resistance

Respond in JSON:
{{
    "segment_valuations": [
        {{
            "segment": "<name>",
            "revenue": <number>,
            "ebitda": <number>,
            "ebitda_margin": <pct>,
            "growth_rate": <pct>,
            "comparables": [
                {{"name": "<company>", "ev_ebitda": <multiple>, "relevance": "HIGH|MEDIUM|LOW"}}
            ],
            "selected_multiple": <number>,
            "segment_ev": <number>,
            "confidence": "HIGH|MEDIUM|LOW"
        }}
    ],
    "hidden_assets": [
        {{
            "type": "<category>",
            "description": "<details>",
            "book_value": <number or null>,
            "estimated_market_value": <number>,
            "methodology": "<how valued>"
        }}
    ],
    "scenarios": {{
        "conservative": {{"sotp_value": <number>, "upside_pct": <number>}},
        "base_case": {{"sotp_value": <number>, "upside_pct": <number>}},
        "optimistic": {{"sotp_value": <number>, "upside_pct": <number>}}
    }},
    "catalysts": [
        {{
            "catalyst": "<description>",
            "probability": "HIGH|MEDIUM|LOW",
            "timeline": "<when>",
            "value_impact": "<description>"
        }}
    ],
    "risks": [
        {{
            "risk": "<description>",
            "severity": "HIGH|MEDIUM|LOW",
            "mitigation": "<if any>"
        }}
    ],
    "investment_recommendation": "STRONG_BUY|BUY|HOLD|AVOID",
    "target_price": <number or null>,
    "thesis": "<detailed investment thesis - 4-6 sentences>"
}}"""
