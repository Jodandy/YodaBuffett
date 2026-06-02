"""
Screen 8: Special Situations

Identifies event-driven opportunities with defined timelines:
- Spin-offs
- Mergers and acquisitions
- Restructurings
- Activist involvement
- Litigation outcomes
- Regulatory decisions

This is a Tier B only screen. The LLM analyzes recent filings and news
to identify actionable special situations.

Tier: B (local LLM)
Frequency: Daily (event-driven)
"""

from datetime import date
from typing import List, Dict, Any, Optional
from uuid import UUID

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


@register_screen
class SpecialSituationsScreen(BaseScreen):
    """
    Screen 8: Special Situations

    Finds event-driven investment opportunities with defined catalysts.
    No Tier A SQL filter - relies on LLM analysis of recent documents.

    Special situation types:
    1. Spin-offs: Parent or SpinCo may be mispriced
    2. M&A: Merger arbitrage or post-merger integration plays
    3. Restructuring: Debt workouts, asset sales, turnarounds
    4. Activism: Activist investor involvement
    5. Litigation: Resolution of major legal matters
    6. Regulatory: Pending decisions with binary outcomes

    Key characteristics:
    - Defined timeline or catalyst
    - Identifiable event completion
    - Market may be mispricing complexity
    """

    screen_type = 8

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Get candidates for Tier B LLM analysis.

        Since this is a Tier B only screen, we return companies with:
        - Recent press releases or filings (potential event announcements)
        - Recent significant price moves (market reaction to events)

        This is a loose filter - Tier B does the real work of identifying
        actual special situations.
        """
        # Get currency conversion SQL - use company_master.report_currency (correct)
        # instead of yf.currency which is often wrong for EUR/USD reporters
        fx_rate = self.get_fx_rate_sql('COALESCE(c.report_currency, yf.currency)', 'c.stock_currency')

        query = f"""
            WITH price_volatility AS (
                -- Companies with recent significant price moves
                SELECT
                    c.id AS company_id,
                    c.primary_ticker,
                    -- 30-day price change
                    (SELECT close_price FROM daily_price_data
                     WHERE symbol = c.primary_ticker AND date <= '{self._score_date}'
                     ORDER BY date DESC LIMIT 1) /
                    NULLIF((SELECT close_price FROM daily_price_data
                            WHERE symbol = c.primary_ticker
                            AND date <= '{self._score_date}'::date - INTERVAL '30 days'
                            ORDER BY date DESC LIMIT 1), 0) - 1 AS price_change_30d,
                    -- 7-day price change (more recent)
                    (SELECT close_price FROM daily_price_data
                     WHERE symbol = c.primary_ticker AND date <= '{self._score_date}'
                     ORDER BY date DESC LIMIT 1) /
                    NULLIF((SELECT close_price FROM daily_price_data
                            WHERE symbol = c.primary_ticker
                            AND date <= '{self._score_date}'::date - INTERVAL '7 days'
                            ORDER BY date DESC LIMIT 1), 0) - 1 AS price_change_7d
                FROM company_master c
                WHERE c.listing_status = 'active'
            ),
            latest_financials AS (
                -- Basic financial context from yahoo_financials
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
                    (yf.income_statement->>'total_revenue')::NUMERIC AS total_revenue,
                    (yf.income_statement->>'net_income')::NUMERIC AS net_income,
                    (yf.balance_sheet->>'total_assets')::NUMERIC AS total_assets,
                    (yf.balance_sheet->>'total_debt')::NUMERIC AS total_debt,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC AS total_equity,
                    (yf.income_statement->>'basic_average_shares')::NUMERIC AS shares_outstanding,
                    ({fx_rate}) AS fx_rate
                FROM company_master c
                LEFT JOIN yahoo_financials yf ON REPLACE(c.primary_ticker, '-', ' ') = yf.symbol
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '18 months'
                  AND c.listing_status = 'active'
                ORDER BY c.id, yf.period_date DESC
            )
            SELECT
                lf.company_id,
                lf.company_name,
                lf.primary_ticker,
                lf.yahoo_symbol,
                lf.report_currency,
                lf.stock_currency,
                lf.fx_rate,
                lf.total_revenue * COALESCE(lf.fx_rate, 1) AS revenue,
                lf.net_income * COALESCE(lf.fx_rate, 1) AS net_income,
                lf.total_assets * COALESCE(lf.fx_rate, 1) AS total_assets,
                lf.total_debt * COALESCE(lf.fx_rate, 1) AS total_debt,
                lf.total_equity * COALESCE(lf.fx_rate, 1) AS total_equity,
                lf.shares_outstanding,
                0 AS recent_doc_count,  -- Document analysis not available
                0 AS event_hint_count,
                ARRAY[]::text[] AS event_hints,
                NULL::date AS latest_doc_date,
                pv.price_change_30d,
                pv.price_change_7d,
                -- Latest price for market cap
                (SELECT close_price FROM daily_price_data
                 WHERE symbol = lf.primary_ticker AND date <= '{self._score_date}'
                 ORDER BY date DESC LIMIT 1) AS latest_price
            FROM latest_financials lf
            LEFT JOIN price_volatility pv ON lf.company_id = pv.company_id
            WHERE
                -- Significant recent price moves suggest potential special situations
                ABS(pv.price_change_30d) > 0.15
                OR ABS(pv.price_change_7d) > 0.10
            ORDER BY
                -- Prioritize by price volatility (most dramatic moves first)
                ABS(COALESCE(pv.price_change_30d, 0)) DESC
            LIMIT 100
        """

        rows = await self.conn.fetch(query)
        results = []

        for row in rows:
            row_dict = dict(row)

            # Calculate market cap
            market_cap = None
            if row_dict.get('latest_price') and row_dict.get('shares_outstanding'):
                market_cap = float(row_dict['latest_price']) * float(row_dict['shares_outstanding'])

            # Parse event hints array
            event_hints = row_dict.get('event_hints') or []
            if isinstance(event_hints, str):
                event_hints = [event_hints]

            metrics = {
                'company_name': row_dict['company_name'],
                'primary_ticker': row_dict['primary_ticker'],
                'yahoo_symbol': row_dict['yahoo_symbol'],
                'market_cap': market_cap,
                'revenue': float(row_dict['revenue']) if row_dict.get('revenue') else None,
                'total_assets': float(row_dict['total_assets']) if row_dict.get('total_assets') else None,
                'total_debt': float(row_dict['total_debt']) if row_dict.get('total_debt') else None,
                'total_equity': float(row_dict['total_equity']) if row_dict.get('total_equity') else None,
                'recent_doc_count': row_dict.get('recent_doc_count', 0),
                'event_hint_count': row_dict.get('event_hint_count', 0),
                'event_hints': event_hints,
                'latest_doc_date': row_dict.get('latest_doc_date'),
                'price_change_30d': float(row_dict['price_change_30d']) if row_dict.get('price_change_30d') else None,
                'price_change_7d': float(row_dict['price_change_7d']) if row_dict.get('price_change_7d') else None,
                'report_currency': row_dict['report_currency'],
                'stock_currency': row_dict['stock_currency'],
            }

            flags = []

            # Flag based on event hints
            if 'merger' in event_hints or 'acquisition' in event_hints:
                flags.append('POTENTIAL_M&A')
            if 'spin-off' in event_hints:
                flags.append('POTENTIAL_SPINOFF')
            if 'restructuring' in event_hints:
                flags.append('POTENTIAL_RESTRUCTURING')
            if 'activism' in event_hints:
                flags.append('POTENTIAL_ACTIVISM')
            if 'litigation' in event_hints or 'settlement' in event_hints:
                flags.append('POTENTIAL_LEGAL')

            # Flag significant price moves
            price_30d = row_dict.get('price_change_30d') or 0
            if price_30d > 0.20:
                flags.append('PRICE_SURGE_30D')
            elif price_30d < -0.20:
                flags.append('PRICE_DROP_30D')

            # Currency conversion warning
            self.add_currency_warning(flags, row_dict['report_currency'], row_dict['stock_currency'])

            results.append(self.create_result(
                company_id=row_dict['company_id'],
                metrics=metrics,
                tier='A',
                flags=flags
            ))

        self.log(f"Found {len(results)} candidates with potential special situations for Tier B analysis")
        return results

    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate preliminary score based on event indicators.

        Final score is determined by Tier B LLM analysis.
        """
        score = 40.0  # Base score

        # Event hints add significant score
        event_hints = metrics.get('event_hints', [])
        if event_hints:
            # Each event type adds points
            high_value_events = ['spin-off', 'merger', 'acquisition', 'tender-offer']
            medium_value_events = ['restructuring', 'strategic-review', 'divestiture']

            for hint in event_hints:
                if hint in high_value_events:
                    score += 15
                elif hint in medium_value_events:
                    score += 10
                else:
                    score += 5

        # Recent document activity
        doc_count = metrics.get('recent_doc_count', 0)
        if doc_count >= 10:
            score += 10
        elif doc_count >= 5:
            score += 5

        # Price volatility suggests market is pricing something
        price_30d = abs(metrics.get('price_change_30d') or 0)
        if price_30d > 0.30:
            score += 15
        elif price_30d > 0.20:
            score += 10
        elif price_30d > 0.10:
            score += 5

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """Generate Tier B prompt for special situations analysis."""
        event_hints = metrics.get('event_hints', [])
        price_30d = metrics.get('price_change_30d', 0) or 0
        doc_count = metrics.get('recent_doc_count', 0)
        market_cap = metrics.get('market_cap')

        market_cap_str = f"${market_cap/1e6:.0f}M" if market_cap else "Unknown"

        return f"""Analyze {company_name} for actionable special situation opportunities.

INITIAL INDICATORS:
- Event Hints from Documents: {event_hints if event_hints else 'None detected'}
- Recent Document Activity: {doc_count} filings in last 90 days
- 30-Day Price Change: {price_30d:.1%}
- Market Cap: {market_cap_str}

IDENTIFY AND ANALYZE:

1. SPIN-OFF: Is there a spin-off announced or rumored?
   - Expected timing
   - Parent vs SpinCo attractiveness
   - Forced selling opportunity

2. M&A: Is there merger or acquisition activity?
   - Deal terms and timeline
   - Spread to deal price
   - Deal completion probability

3. RESTRUCTURING: Is there a major restructuring underway?
   - Asset sales or divestitures
   - Cost reduction programs
   - Debt restructuring

4. ACTIVISM: Is an activist investor involved?
   - Who is the activist and their track record?
   - What changes are they pushing for?
   - Management response

5. LEGAL/REGULATORY: Are there pending legal or regulatory matters?
   - Expected resolution timeline
   - Potential outcome scenarios
   - Current market pricing of risk

6. OTHER EVENTS: Any other identifiable catalysts?
   - Management changes
   - Strategic alternatives process
   - Large contract wins/losses

Respond in JSON:
{{
    "special_situation_type": "SPIN_OFF|M_AND_A|RESTRUCTURING|ACTIVISM|LEGAL|REGULATORY|OTHER|NONE",
    "situation_description": "<brief description>",
    "is_actionable": true|false,
    "timeline": "<expected timeline or 'UNCLEAR'>",
    "catalyst": "<specific catalyst event>",
    "current_market_pricing": "UNDERPRICED|FAIRLY_PRICED|OVERPRICED",
    "probability_of_favorable_outcome": "HIGH|MEDIUM|LOW",
    "risk_factors": ["<list of key risks>"],
    "opportunity_size": "LARGE|MEDIUM|SMALL",
    "recommended_action": "BUY|WAIT|MONITOR|AVOID",
    "confidence": "HIGH|MEDIUM|LOW",
    "summary": "<2-3 sentence investment thesis>"
}}"""

    def get_tier_c_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Screen 8 doesn't have Tier C enabled (tier_c_enabled=False).
        This method won't be called, but we include it for completeness.
        """
        return ""
