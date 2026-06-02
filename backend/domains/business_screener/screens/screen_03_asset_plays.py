"""
Screen 3: Asset Plays — Real Assets Below Book Value

Identifies companies where market value is significantly below book value,
with real/tangible assets providing downside protection. Special focus on
asset-heavy sectors like real estate, forestry, investment, and shipping.

Tier: A + B
Frequency: Monthly

Criteria (from spec):
1. P/B < 0.5 (deep discount to book value)
2. Operating income > 0 (business is profitable at operating level)
3. Total debt / total assets < 0.65 (reasonable leverage for asset base)
4. Sector is Real Estate/Forestry/Investment/Shipping OR high tangible assets

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


# Asset-heavy sectors that qualify automatically (Swedish + English names)
ASSET_HEAVY_SECTORS = [
    'Real Estate', 'Fastighetsbolag', 'Fastigheter',
    'Forestry', 'Skogsbruk', 'Skog',
    'Investment', 'Investmentbolag',
    'Shipping', 'Sjöfart', 'Sjöfart & Rederi', 'Rederi',
]


@register_screen
class AssetPlaysScreen(BaseScreen):
    """
    Screen 3: Asset Plays — Real Assets Below Book Value

    Criteria (from spec):
    - Price to Book < 0.5 (deep discount to book value)
    - Operating income > 0 (profitable at operating level)
    - Total debt / total assets < 0.65 (not over-leveraged on asset base)
    - Sector is asset-heavy (Real Estate, Forestry, Investment, Shipping)
      OR tangible assets > 60% of total assets

    The key insight: real estate, forestry, and shipping companies have
    tangible assets (property, ships, land) that provide downside protection.
    Standard current-asset filters don't work for these sectors.

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 3

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find asset plays with correct spec filters.

        Uses yahoo_financials data for ACCURATE tangible asset calculations:
        - goodwill, other_intangible_assets directly from Yahoo
        - net_tangible_assets, tangible_book_value pre-calculated by Yahoo
        - net_ppe, working_capital, net_debt directly available

        Falls back to legacy tables if yahoo_financials is empty.
        """
        self.log("Running Tier A screen (using yahoo_financials for accurate tangible assets)...")

        # Get FX rate SQL for currency conversion
        # Use company_master.report_currency (correct) with fallback to f.report_currency
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, f.report_currency)', 'c.stock_currency')

        # Build sector filter for asset-heavy sectors
        sector_conditions = " OR ".join([f"c.sector ILIKE '%{s}%'" for s in ASSET_HEAVY_SECTORS])

        # Use yahoo_financials CTE for richer data (goodwill, intangibles, net_tangible_assets)
        combined_cte = self.get_yahoo_combined_financials_cte()

        query = f"""
            WITH {combined_cte},
            pit_financials AS (
                -- Select from yahoo_combined_financials with rich asset data
                SELECT
                    symbol,
                    period_date,
                    report_currency,
                    is_ttm,
                    quarters_count,
                    total_revenue,
                    operating_income,
                    net_income,
                    total_assets,
                    current_assets,
                    cash_and_equivalents,
                    accounts_receivable,
                    inventory,
                    total_liabilities,
                    current_liabilities,
                    total_debt,
                    total_equity,
                    shares_outstanding,
                    free_cash_flow,
                    operating_cash_flow,
                    -- NEW: Rich asset data from yahoo_financials
                    goodwill,
                    other_intangible_assets,
                    total_intangibles,
                    net_tangible_assets,
                    tangible_book_value,
                    net_ppe,
                    working_capital,
                    net_debt,
                    invested_capital
                FROM yahoo_combined_financials
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
                    cm.industry,
                    COALESCE(cm.stock_currency,
                        CASE
                            WHEN cm.yahoo_symbol LIKE '%%.ST' THEN 'SEK'
                            WHEN cm.yahoo_symbol LIKE '%%.OL' THEN 'NOK'
                            WHEN cm.yahoo_symbol LIKE '%%.CO' THEN 'DKK'
                            WHEN cm.yahoo_symbol LIKE '%%.HE' THEN 'EUR'
                            ELSE 'SEK'
                        END
                    ) AS stock_currency,
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
                c.industry,
                c.stock_currency,

                -- Price data
                p.close_price AS price,
                p.price_date,

                -- Financial data
                f.period_date AS financial_date,
                f.is_ttm,
                f.quarters_count,
                f.report_currency,
                f.shares_outstanding,

                -- FX rate: converts report_currency to stock_currency
                ({fx_rate_sql}) AS fx_rate,

                -- Raw financial values (in report_currency)
                f.operating_income,
                f.net_income,
                f.total_revenue,
                f.free_cash_flow,
                f.total_assets,
                f.current_assets,
                f.cash_and_equivalents,
                f.accounts_receivable,
                f.inventory,
                f.total_liabilities,
                f.total_debt,
                f.total_equity,

                -- Market cap (in stock_currency)
                p.close_price * f.shares_outstanding AS market_cap,

                -- Book value converted to stock_currency
                f.total_equity * ({fx_rate_sql}) AS book_value,

                -- Book value per share (converted)
                CASE WHEN f.shares_outstanding > 0
                    THEN (f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding
                    ELSE NULL END AS bv_per_share,

                -- Price to book (with FX conversion)
                CASE WHEN f.total_equity > 0 AND f.shares_outstanding > 0
                    THEN p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding)
                    ELSE NULL END AS price_to_book,

                -- Debt to assets ratio (no FX needed)
                CASE WHEN f.total_assets > 0
                    THEN COALESCE(f.total_debt, 0)::DECIMAL / f.total_assets
                    ELSE NULL END AS debt_to_assets,

                -- Debt to equity ratio (no FX needed)
                CASE WHEN f.total_equity > 0
                    THEN COALESCE(f.total_debt, 0)::DECIMAL / f.total_equity
                    ELSE NULL END AS debt_to_equity,

                -- ACCURATE tangible assets from Yahoo data
                -- Uses net_tangible_assets directly, or calculates from goodwill/intangibles
                CASE WHEN f.total_assets > 0
                    THEN COALESCE(
                        f.net_tangible_assets::DECIMAL / f.total_assets,
                        (f.total_assets - COALESCE(f.total_intangibles, COALESCE(f.goodwill, 0) + COALESCE(f.other_intangible_assets, 0)))::DECIMAL / f.total_assets
                    )
                    ELSE NULL END AS tangible_asset_pct,

                -- NEW: Raw intangible data for transparency
                f.goodwill,
                f.other_intangible_assets,
                f.total_intangibles,
                f.net_tangible_assets,
                f.tangible_book_value,
                f.net_ppe,
                f.net_debt AS yahoo_net_debt,
                f.working_capital AS yahoo_working_capital,

                -- Is this an asset-heavy sector?
                CASE WHEN ({sector_conditions}) THEN TRUE ELSE FALSE END AS is_asset_heavy_sector

            FROM companies_with_data c
            JOIN pit_financials f ON c.financial_symbol = f.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE f.shares_outstanding > 0
              AND p.close_price > 0
              AND f.total_equity > 0
              AND f.total_assets > 0
              -- SPEC FILTER 1: P/B < 0.5
              AND p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) < 0.5
              -- SPEC FILTER 2: Operating income > 0
              AND f.operating_income > 0
              -- SPEC FILTER 3: Total debt / total assets < 0.65
              AND COALESCE(f.total_debt, 0)::DECIMAL / f.total_assets < 0.65
              -- SPEC FILTER 4: Asset-heavy sector OR high ACTUAL tangible assets (>60%)
              -- Now uses real intangibles data instead of approximation!
              AND (
                  ({sector_conditions})
                  OR COALESCE(
                      f.net_tangible_assets::DECIMAL / f.total_assets,
                      (f.total_assets - COALESCE(f.total_intangibles, COALESCE(f.goodwill, 0) + COALESCE(f.other_intangible_assets, 0)))::DECIMAL / f.total_assets
                  ) > 0.60
              )
            ORDER BY
                -- Sort by price to book (cheapest first)
                p.close_price::DECIMAL / ((f.total_equity * ({fx_rate_sql}))::DECIMAL / f.shares_outstanding) ASC
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates")

        results = []
        for row in rows:
            p_b = float(row['price_to_book']) if row['price_to_book'] else 0
            debt_to_assets = float(row['debt_to_assets']) if row['debt_to_assets'] else 0
            tangible_pct = float(row['tangible_asset_pct']) if row['tangible_asset_pct'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'industry': row['industry'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,

                # Book values
                'book_value': float(row['book_value']) if row['book_value'] else None,
                'bv_per_share': float(row['bv_per_share']) if row['bv_per_share'] else None,
                'price_to_book': p_b,

                # Asset composition (now with REAL intangibles data from Yahoo!)
                'total_assets': float(row['total_assets']) if row['total_assets'] else None,
                'current_assets': float(row['current_assets']) if row['current_assets'] else None,
                'tangible_asset_pct': tangible_pct,
                'cash_and_equivalents': float(row['cash_and_equivalents']) if row['cash_and_equivalents'] else None,
                'accounts_receivable': float(row['accounts_receivable']) if row['accounts_receivable'] else None,
                'inventory': float(row['inventory']) if row['inventory'] else None,

                # NEW: Actual intangibles breakdown from Yahoo Finance
                'goodwill': float(row['goodwill']) if row['goodwill'] else 0,
                'other_intangible_assets': float(row['other_intangible_assets']) if row['other_intangible_assets'] else 0,
                'total_intangibles': float(row['total_intangibles']) if row['total_intangibles'] else 0,
                'net_tangible_assets': float(row['net_tangible_assets']) if row['net_tangible_assets'] else None,
                'tangible_book_value': float(row['tangible_book_value']) if row['tangible_book_value'] else None,
                'net_ppe': float(row['net_ppe']) if row['net_ppe'] else None,
                'yahoo_net_debt': float(row['yahoo_net_debt']) if row['yahoo_net_debt'] else None,
                'yahoo_working_capital': float(row['yahoo_working_capital']) if row['yahoo_working_capital'] else None,

                # Leverage ratios
                'total_debt': float(row['total_debt']) if row['total_debt'] else 0,
                'total_equity': float(row['total_equity']) if row['total_equity'] else None,
                'debt_to_assets': debt_to_assets,
                'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,

                # Profitability
                'operating_income': float(row['operating_income']) if row['operating_income'] else None,
                'net_income': float(row['net_income']) if row['net_income'] else None,
                'free_cash_flow': float(row['free_cash_flow']) if row['free_cash_flow'] else None,

                # Sector classification
                'is_asset_heavy_sector': row['is_asset_heavy_sector'],

                'report_currency': row['report_currency'],
                'stock_currency': row['stock_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,

                # Data freshness indicators
                'is_ttm': row['is_ttm'],
                'data_source': 'TTM (4 quarters)' if row['is_ttm'] else 'Annual report',
                'quarters_count': row['quarters_count'] if row['quarters_count'] else 1,
            }

            # Build flags
            flags = []

            # Data freshness flag
            if row['is_ttm']:
                flags.append("TTM_DATA: Using trailing 12 months (fresher)")
            else:
                flags.append("ANNUAL_DATA: Using latest annual report")

            # P/B flags
            if p_b < 0.3:
                flags.append("EXTREME_DISCOUNT: P/B under 0.3x")
            elif p_b < 0.4:
                flags.append("DEEP_DISCOUNT: P/B under 0.4x")
            else:
                flags.append("DISCOUNT: P/B under 0.5x")

            # Sector flags
            if row['is_asset_heavy_sector']:
                flags.append(f"ASSET_SECTOR: {row['sector']}")
            elif tangible_pct > 0.8:
                flags.append("HIGH_TANGIBLES: >80% tangible assets")
            elif tangible_pct > 0.6:
                flags.append("TANGIBLE_ASSETS: >60% tangible assets")

            # NEW: Intangibles transparency flags
            goodwill = row['goodwill'] or 0
            other_intangibles = row['other_intangible_assets'] or 0
            total_assets = row['total_assets'] or 1
            intangibles_pct = (goodwill + other_intangibles) / total_assets if total_assets > 0 else 0

            if intangibles_pct < 0.05:
                flags.append("MINIMAL_INTANGIBLES: <5% of assets")
            elif intangibles_pct < 0.15:
                flags.append("LOW_INTANGIBLES: <15% of assets")
            elif intangibles_pct > 0.30:
                flags.append("HIGH_INTANGIBLES: >30% of assets (caution)")

            # Flag if we have rich data
            if row['net_tangible_assets'] is not None:
                flags.append("YAHOO_TANGIBLES: Using actual net_tangible_assets")

            # Leverage flags
            if debt_to_assets < 0.3:
                flags.append("LOW_LEVERAGE: Debt/Assets under 30%")
            elif debt_to_assets < 0.5:
                flags.append("MODERATE_LEVERAGE: Debt/Assets under 50%")

            # Profitability flags
            if row['operating_income'] and row['operating_income'] > 0:
                flags.append("OPERATING_PROFIT: Positive operating income")

            if row['net_income'] and row['net_income'] > 0:
                flags.append("NET_PROFITABLE: Positive net income")

            # Currency mismatch warning
            self.add_currency_warning(flags, row['report_currency'], row['stock_currency'])

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
        Calculate score based on asset discount, earnings yield, and quality.

        Scoring (100 points max):
        - P/B discount: up to 35 points (40 for real estate - most important metric)
        - Earnings yield: up to 25 points (uses operating income for real estate)
        - Low leverage: up to 20 points (adjusted thresholds for real estate)
        - Tangible assets: up to 10 points (real estate gets full points)
        - Operating profitability (ROA): up to 10 points
        """
        score = 0.0

        p_b = metrics.get('price_to_book', 1) or 1
        debt_to_assets = metrics.get('debt_to_assets', 1) or 1
        tangible_pct = metrics.get('tangible_asset_pct', 0) or 0
        operating_income = metrics.get('operating_income', 0) or 0
        net_income = metrics.get('net_income', 0) or 0
        total_assets = metrics.get('total_assets', 1) or 1
        market_cap = metrics.get('market_cap', 1) or 1
        sector = metrics.get('sector', '') or ''

        # Check if real estate sector
        is_real_estate = any(s.lower() in sector.lower() for s in ['fastighet', 'real estate'])

        # For real estate: calculate true tangible % as (total_assets - goodwill - intangibles) / total_assets
        # Yahoo classifies investment properties separately from PP&E, but they ARE tangible assets
        if is_real_estate:
            goodwill = metrics.get('goodwill', 0) or 0
            intangibles = metrics.get('other_intangible_assets', 0) or 0
            total_intangibles = metrics.get('total_intangibles', 0) or 0
            # Use total_intangibles if available, otherwise sum goodwill + other
            intangible_total = total_intangibles if total_intangibles > 0 else (goodwill + intangibles)
            if total_assets > 0:
                tangible_pct = (total_assets - intangible_total) / total_assets

        # P/B scoring - CRITICAL for real estate (book = appraised property values)
        # Real estate: up to 40 points (P/B is the key metric)
        # Other sectors: up to 35 points
        if is_real_estate:
            # Real estate P/B scoring (up to 40 points)
            if p_b < 0.35:
                score += 40
            elif p_b < 0.40:
                score += 36
            elif p_b < 0.45:
                score += 32
            elif p_b < 0.50:
                score += 28
            elif p_b < 0.55:
                score += 22
            elif p_b < 0.60:
                score += 16
        else:
            # Non-real-estate P/B scoring (up to 35 points)
            if p_b < 0.25:
                score += 35
            elif p_b < 0.3:
                score += 30
            elif p_b < 0.35:
                score += 25
            elif p_b < 0.4:
                score += 20
            elif p_b < 0.45:
                score += 14
            elif p_b < 0.5:
                score += 8

        # Earnings yield scoring (up to 25 points)
        # CRITICAL: For real estate, use operating income (excludes unrealized revaluations)
        if is_real_estate:
            earnings_for_yield = operating_income
        else:
            earnings_for_yield = net_income

        if market_cap > 0 and earnings_for_yield > 0:
            earnings_yield = earnings_for_yield / market_cap
            if earnings_yield > 0.20:  # 20%+ yield
                score += 25
            elif earnings_yield > 0.15:  # 15-20%
                score += 20
            elif earnings_yield > 0.10:  # 10-15%
                score += 15
            elif earnings_yield > 0.07:  # 7-10%
                score += 10
            elif earnings_yield > 0.05:  # 5-7%
                score += 5

        # Leverage scoring (up to 20 points)
        # Real estate uses structural leverage (mortgages) - adjust thresholds
        if is_real_estate:
            # Real estate leverage scoring
            # Screen threshold is 0.65, so anything below is acceptable
            if debt_to_assets < 0.50:
                score += 20  # Very conservative for RE
            elif debt_to_assets < 0.55:
                score += 16
            elif debt_to_assets < 0.60:
                score += 12
            elif debt_to_assets < 0.65:
                score += 8   # At screen threshold - still gets points
            elif debt_to_assets < 0.70:
                score += 4   # Above threshold but not dangerous
            # Above 0.70: 0 points
        else:
            # Non-real-estate leverage scoring
            if debt_to_assets < 0.2:
                score += 20
            elif debt_to_assets < 0.3:
                score += 16
            elif debt_to_assets < 0.4:
                score += 12
            elif debt_to_assets < 0.5:
                score += 8
            elif debt_to_assets < 0.6:
                score += 4

        # Tangible assets scoring (up to 10 points)
        # Real estate: investment properties ARE tangible (physical buildings)
        if tangible_pct > 0.95:
            score += 10
        elif tangible_pct > 0.85:
            score += 8
        elif tangible_pct > 0.75:
            score += 6
        elif tangible_pct > 0.60:
            score += 4

        # Operating profitability scoring (up to 10 points)
        # ROA = operating_income / total_assets
        if total_assets > 0:
            asset_return = operating_income / total_assets
            if asset_return > 0.10:
                score += 10
            elif asset_return > 0.07:
                score += 7
            elif asset_return > 0.05:
                score += 5
            elif asset_return > 0.02:
                score += 2

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for asset quality assessment.
        """
        sector = metrics.get('sector', 'Unknown')
        is_asset_sector = metrics.get('is_asset_heavy_sector', False)

        sector_context = ""
        if is_asset_sector:
            if 'fastighet' in sector.lower() or 'real estate' in sector.lower():
                sector_context = """
This is a REAL ESTATE company. Focus on:
- Property portfolio quality (location, occupancy, lease terms)
- NAV calculation (market value of properties vs book value)
- Rental yield and operating margins
- Loan-to-value ratios and refinancing risk"""
            elif 'investment' in sector.lower():
                sector_context = """
This is an INVESTMENT company. Focus on:
- Portfolio holdings and their current market values
- Discount to NAV
- Management fees and cost structure
- Dividend policy and shareholder returns"""
            elif 'sjöfart' in sector.lower() or 'shipping' in sector.lower():
                sector_context = """
This is a SHIPPING company. Focus on:
- Fleet composition and age
- Charter rates vs spot rates
- Vessel valuations (book vs broker estimates)
- Order book and newbuild exposure"""

        return f"""Assess the quality and realizability of this company's assets.

Company: {company_name}
Sector: {sector}
Price to Book: {metrics.get('price_to_book', 0):.2f}x
Debt/Assets: {metrics.get('debt_to_assets', 0):.1%}
Tangible Assets: {metrics.get('tangible_asset_pct', 0):.1%} of total
{sector_context}

Asset composition:
- Total Assets: {metrics.get('total_assets', 'N/A'):,.0f} {metrics.get('report_currency', 'SEK')}
- Total Equity: {metrics.get('total_equity', 'N/A'):,.0f} {metrics.get('report_currency', 'SEK')}
- Total Debt: {metrics.get('total_debt', 0):,.0f} {metrics.get('report_currency', 'SEK')}
- Cash: {metrics.get('cash_and_equivalents', 'N/A')} {metrics.get('report_currency', 'SEK')}
- Operating Income: {metrics.get('operating_income', 'N/A'):,.0f} {metrics.get('report_currency', 'SEK')}
Market cap: {metrics.get('market_cap', 'N/A'):,.0f} {metrics.get('stock_currency', 'SEK')}

From the financial report, answer:
1. ASSET QUALITY: What do the assets consist of? Are they stated at cost or fair value?
2. HIDDEN VALUE: Are there any assets carried at historical cost that may be worth more?
   (Land, older properties, long-held investments)
3. LEVERAGE SAFETY: Is the debt level manageable? What are the maturities?
4. LIQUIDATION VALUE: If liquidated today, what would shareholders receive?
5. CATALYST: What could unlock the discount? (Spin-off, sale, dividend, buyback)

Respond in JSON:
{{
  "asset_types": ["list of main asset categories"],
  "valuation_method": "COST|FAIR_VALUE|MIXED",
  "hidden_value_estimate": "SIGNIFICANT|MODERATE|MINIMAL|NONE",
  "hidden_value_details": "string or null",
  "debt_safety": "HIGH|MEDIUM|LOW",
  "debt_concerns": "string or null",
  "estimated_nav_premium": "number (% above/below book)",
  "liquidation_estimate": "number or null",
  "potential_catalysts": ["list"],
  "key_risks": ["list"],
  "overall_attractiveness": "HIGH|MEDIUM|LOW",
  "summary": "2-3 sentence investment thesis"
}}"""
