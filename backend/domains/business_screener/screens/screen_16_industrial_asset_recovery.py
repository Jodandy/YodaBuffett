"""
Screen 16: Industrial Asset Recovery

Identifies asset-heavy industrial businesses with real revenue trading at
liquidation-level valuations, where profitability is temporarily depressed
but the underlying business model is intact.

Thesis: Industrial companies with gross margins of 12-30% that fail every
other screen's margin thresholds (typically 35%+). Companies like NKT (8x
from trough), Duroc, and Strongpoint - real businesses with real assets
that are temporarily unprofitable or earning well below normalized levels.

Archetype: A 50-year-old manufacturer or industrial services company with
EUR 100M+ in revenue, trading at P/S 0.3x and P/B 0.7x, currently earning
thin margins or breakeven after a cyclical downturn or a one-off drag.
The factories, order books, and customer relationships haven't changed -
just the current P&L.

What this screen catches that other screens miss:
- Industrial companies with gross margins of 12-30% (excluded by 35%+ filters)
- Companies cheap on MULTIPLE dimensions (P/S + P/B + EV/Revenue)
- Stable/improving gross margins despite revenue pressure (unit economics intact)

Tier: A + B
Frequency: Quarterly

Criteria:
TIER 1 (Valuation - "Nobody Wants This"):
- P/S < 0.5
- P/B < 1.0
- EV/Revenue < 0.6
- All three required

TIER 2 (Business Intact - "Not Dying"):
- Revenue > 20M EUR
- Revenue YoY > -20%
- Gross margin > 12%
- Gross margin change YoY > -3pp

TIER 3 (Financial Survival):
- Current ratio > 1.2
- Net debt/EBITDA < 4.0 (or net cash, or net debt < 30% of equity if EBITDA negative)
- Equity ratio > 25%

TIER 4 (Turnaround Signal) - At least ONE:
- EBITDA > 0
- EBITDA margin improving > 1pp YoY
- Operating loss narrowing
- Revenue YoY > 0% after prior year decline

Exclusions:
- Banks & Financial Services
- REITs
- Oil & Gas E&P
- Mining/Basic Materials (pure-play)
- Sports clubs/Entertainment
- Biotech/Pharma (pre-revenue)
- Shipping (pure-play)
- Market cap < 10M EUR

Backtesting: Fully supported via score_date parameter.
"""

from typing import List, Dict, Any

from .base import BaseScreen, register_screen
from ..models.screen_result import ScreenResult


# Sector exclusions - companies in these sectors are filtered out
EXCLUDED_SECTORS = {
    'banks', 'bank', 'banking',
    'financial services', 'financials', 'finance',
    'insurance',
    'real estate investment trust', 'reit', 'reits', 'real estate investment',
    'oil & gas', 'oil and gas', 'oil gas exploration', 'oil & gas exploration',
    'mining', 'basic materials', 'metals & mining', 'metals and mining',
    'biotech', 'biotechnology', 'pharmaceutical', 'pharmaceuticals', 'pharma',
    'shipping', 'marine', 'tanker', 'bulk carrier',
    'sports', 'entertainment', 'gaming',
}

# Industry exclusions for more granular filtering
EXCLUDED_INDUSTRIES = {
    'banks', 'diversified banks', 'regional banks',
    'insurance', 'life insurance', 'property insurance',
    'reits', 'equity real estate investment trusts',
    'oil & gas exploration', 'integrated oil',
    'gold', 'silver', 'copper', 'iron ore', 'coal',
    'marine shipping', 'tanker shipping',
    'sports franchises', 'casinos',
}

# Minimum revenue in EUR (20M)
MIN_REVENUE_EUR = 20_000_000

# Minimum market cap in EUR (10M)
MIN_MARKET_CAP_EUR = 10_000_000


@register_screen
class IndustrialAssetRecoveryScreen(BaseScreen):
    """
    Screen 16: Industrial Asset Recovery

    Finds asset-heavy industrials at liquidation valuations with intact
    unit economics. The key discriminator: gross margin stability proves
    the business model is intact despite temporary profitability issues.

    Criteria:
    - P/S < 0.5, P/B < 1.0, EV/Revenue < 0.6 (all required)
    - Revenue > 20M EUR, Revenue YoY > -20%
    - Gross margin > 12%, Gross margin stable (YoY > -3pp)
    - Current ratio > 1.2, manageable leverage, equity ratio > 25%
    - At least one turnaround signal firing

    Point-in-time safe: Uses publish_date for financials, date <= score_date for prices.
    """

    screen_type = 16

    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A screen: find industrial asset recovery candidates.

        Uses yahoo_financials for richer data including:
        - More accurate EBITDA directly from Yahoo
        - Working capital and net debt calculations
        - TTM preferred, annual fallback
        """
        self.log("Running Tier A screen (Industrial Asset Recovery)...")

        financial_filter = self.get_financial_date_filter('fs')
        # Use company_master.report_currency (correct) with fallback
        fx_rate_sql = self.get_fx_rate_sql('COALESCE(c.report_currency, mh.report_currency)', 'c.trading_currency')

        # FX rate to EUR for minimum revenue/market cap checks
        fx_to_eur_sql = """
            CASE
                WHEN c.trading_currency = 'EUR' THEN 1.0
                WHEN c.trading_currency = 'SEK' THEN 0.087
                WHEN c.trading_currency = 'NOK' THEN 0.083
                WHEN c.trading_currency = 'DKK' THEN 0.134
                ELSE 0.087
            END
        """

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
                    net_debt,
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
                        THEN ebitda::DECIMAL / total_revenue
                        ELSE NULL END AS ebitda_margin,
                    CASE WHEN total_revenue > 0
                        THEN net_income::DECIMAL / total_revenue
                        ELSE NULL END AS net_margin,
                    -- Balance sheet ratios
                    CASE WHEN current_liabilities > 0
                        THEN current_assets::DECIMAL / current_liabilities
                        ELSE NULL END AS current_ratio,
                    CASE WHEN total_assets > 0
                        THEN total_equity::DECIMAL / total_assets
                        ELSE NULL END AS equity_ratio
                FROM yahoo_combined_financials
            ),
            -- Historical annual data for comparisons (1yr ago, 2yr ago)
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
                    CASE WHEN fs.total_revenue > 0
                        THEN fs.ebitda::DECIMAL / fs.total_revenue
                        ELSE NULL END AS ebitda_margin,
                    ROW_NUMBER() OVER (PARTITION BY fs.symbol ORDER BY fs.period_date DESC) AS rn
                FROM financial_statements fs
                WHERE fs.statement_type = 'annual'
                  AND {financial_filter}
            ),
            margin_history AS (
                SELECT
                    cf.symbol,
                    -- Current period data (TTM or latest annual)
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
                    cf.ebitda_margin AS ebitda_margin_current,
                    cf.net_margin AS net_margin_current,
                    cf.shares_outstanding AS shares_current,
                    cf.cash_and_equivalents AS cash_current,
                    cf.total_debt AS total_debt_current,
                    cf.total_equity AS total_equity_current,
                    cf.total_assets AS total_assets_current,
                    cf.current_assets AS current_assets_current,
                    cf.current_liabilities AS current_liabilities_current,
                    cf.net_debt AS net_debt_current,
                    cf.free_cash_flow AS fcf_current,
                    cf.operating_cash_flow AS ocf_current,
                    cf.current_ratio AS current_ratio,
                    cf.equity_ratio AS equity_ratio,

                    -- Prior year data for YoY comparisons
                    ha1.total_revenue AS revenue_1yr_ago,
                    ha1.gross_margin AS gross_margin_1yr_ago,
                    ha1.operating_income AS operating_income_1yr_ago,
                    ha1.operating_margin AS operating_margin_1yr_ago,
                    ha1.ebitda AS ebitda_1yr_ago,
                    ha1.ebitda_margin AS ebitda_margin_1yr_ago,

                    -- 2 years ago for trend analysis
                    ha2.total_revenue AS revenue_2yr_ago,
                    ha2.gross_margin AS gross_margin_2yr_ago

                FROM current_financials cf
                LEFT JOIN historical_annual ha1 ON cf.symbol = ha1.symbol AND ha1.rn = 1
                LEFT JOIN historical_annual ha2 ON cf.symbol = ha2.symbol AND ha2.rn = 2
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
                    cm.currency AS trading_currency,
                    cm.report_currency,
                    REPLACE(cm.primary_ticker, '-', ' ') AS financial_symbol,
                    LOWER(COALESCE(cm.sector, '')) AS sector_lower,
                    LOWER(COALESCE(cm.industry, '')) AS industry_lower
                FROM company_master cm
                WHERE cm.listing_status = 'active'
            )
            SELECT
                c.company_id,
                c.company_name,
                c.primary_ticker,
                c.sector,
                c.industry,
                c.trading_currency,
                c.sector_lower,
                c.industry_lower,

                -- Price data
                p.close_price AS price,
                p.price_date,

                -- Current financials
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
                mh.total_assets_current,
                mh.current_assets_current,
                mh.current_liabilities_current,
                mh.net_debt_current,
                mh.fcf_current,
                mh.ocf_current,

                -- Current margins and ratios
                mh.gross_margin_current,
                mh.operating_margin_current,
                mh.ebitda_margin_current,
                mh.net_margin_current,
                mh.current_ratio,
                mh.equity_ratio,

                -- Historical data
                mh.revenue_1yr_ago,
                mh.gross_margin_1yr_ago,
                mh.operating_income_1yr_ago,
                mh.operating_margin_1yr_ago,
                mh.ebitda_1yr_ago,
                mh.ebitda_margin_1yr_ago,
                mh.revenue_2yr_ago,
                mh.gross_margin_2yr_ago,

                -- FX rate for currency conversion
                ({fx_rate_sql}) AS fx_rate,

                -- FX rate to EUR for threshold checks
                ({fx_to_eur_sql}) AS fx_to_eur,

                -- Market cap (in trading_currency)
                p.close_price * mh.shares_current AS market_cap,

                -- Book value per share (with FX conversion)
                CASE WHEN mh.shares_current > 0
                    THEN mh.total_equity_current::DECIMAL / mh.shares_current
                    ELSE NULL END AS book_value_per_share,

                -- Enterprise value (with FX conversion for debt/cash)
                p.close_price * mh.shares_current + (COALESCE(mh.total_debt_current, 0) - COALESCE(mh.cash_current, 0)) * ({fx_rate_sql}) AS enterprise_value,

                -- TIER 1: Valuation ratios (with FX conversion)
                -- Price to Sales
                CASE WHEN mh.revenue_current > 0
                    THEN (p.close_price * mh.shares_current)::DECIMAL / (mh.revenue_current * ({fx_rate_sql}))
                    ELSE NULL END AS price_to_sales,

                -- Price to Book
                CASE WHEN mh.total_equity_current > 0
                    THEN (p.close_price * mh.shares_current)::DECIMAL / (mh.total_equity_current * ({fx_rate_sql}))
                    ELSE NULL END AS price_to_book,

                -- EV to Revenue
                CASE WHEN mh.revenue_current > 0
                    THEN (p.close_price * mh.shares_current + (COALESCE(mh.total_debt_current, 0) - COALESCE(mh.cash_current, 0)) * ({fx_rate_sql}))::DECIMAL / (mh.revenue_current * ({fx_rate_sql}))
                    ELSE NULL END AS ev_to_revenue,

                -- Revenue YoY change
                CASE WHEN mh.revenue_1yr_ago > 0
                    THEN (mh.revenue_current - mh.revenue_1yr_ago)::DECIMAL / mh.revenue_1yr_ago
                    ELSE NULL END AS revenue_yoy,

                -- Revenue 2yr trend (for TIER 4 check)
                CASE WHEN mh.revenue_2yr_ago > 0
                    THEN (mh.revenue_1yr_ago - mh.revenue_2yr_ago)::DECIMAL / mh.revenue_2yr_ago
                    ELSE NULL END AS revenue_prior_yoy,

                -- Gross margin stability check (YoY change in pp)
                CASE WHEN mh.gross_margin_1yr_ago IS NOT NULL
                    THEN mh.gross_margin_current - mh.gross_margin_1yr_ago
                    ELSE NULL END AS gross_margin_change_yoy,

                -- EBITDA margin change (for TIER 4)
                CASE WHEN mh.ebitda_margin_1yr_ago IS NOT NULL
                    THEN mh.ebitda_margin_current - mh.ebitda_margin_1yr_ago
                    ELSE NULL END AS ebitda_margin_change_yoy,

                -- Operating income change (for TIER 4 - loss narrowing)
                CASE WHEN mh.operating_income_1yr_ago IS NOT NULL
                    THEN mh.operating_income_current - mh.operating_income_1yr_ago
                    ELSE NULL END AS operating_income_change,

                -- Net debt to EBITDA
                CASE WHEN mh.ebitda_current > 0
                    THEN COALESCE(mh.net_debt_current, mh.total_debt_current - mh.cash_current, 0)::DECIMAL / mh.ebitda_current
                    ELSE NULL END AS net_debt_to_ebitda,

                -- Net debt to equity (for EBITDA-negative cases)
                CASE WHEN mh.total_equity_current > 0
                    THEN COALESCE(mh.net_debt_current, mh.total_debt_current - mh.cash_current, 0)::DECIMAL / mh.total_equity_current
                    ELSE NULL END AS net_debt_to_equity

            FROM companies_with_data c
            JOIN margin_history mh ON c.financial_symbol = mh.symbol
            JOIN pit_prices p ON c.primary_ticker = p.symbol
            WHERE mh.shares_current > 0
              AND p.close_price > 0
              AND mh.revenue_current > 0
              AND mh.total_equity_current > 0

              -- TIER 1: Valuation - "Nobody Wants This" (ALL required)
              -- P/S < 0.5
              AND (p.close_price * mh.shares_current)::DECIMAL / (mh.revenue_current * ({fx_rate_sql})) < 0.5
              -- P/B < 1.0
              AND (p.close_price * mh.shares_current)::DECIMAL / (mh.total_equity_current * ({fx_rate_sql})) < 1.0
              -- EV/Revenue < 0.6
              AND (p.close_price * mh.shares_current + (COALESCE(mh.total_debt_current, 0) - COALESCE(mh.cash_current, 0)) * ({fx_rate_sql}))::DECIMAL / (mh.revenue_current * ({fx_rate_sql})) < 0.6

              -- TIER 2: Business Intact - "Not Dying"
              -- Revenue > 20M EUR
              AND mh.revenue_current * ({fx_rate_sql}) * ({fx_to_eur_sql}) > {MIN_REVENUE_EUR}
              -- Revenue YoY > -20%
              AND (mh.revenue_1yr_ago IS NULL
                   OR mh.revenue_current >= mh.revenue_1yr_ago * 0.80)
              -- Gross margin > 12%
              AND mh.gross_margin_current > 0.12
              -- Gross margin stable (YoY change > -3pp)
              AND (mh.gross_margin_1yr_ago IS NULL
                   OR mh.gross_margin_current - mh.gross_margin_1yr_ago > -0.03)

              -- TIER 3: Financial Survival
              -- Current ratio > 1.2
              AND mh.current_ratio > 1.2
              -- Equity ratio > 25%
              AND mh.equity_ratio > 0.25
              -- Leverage check: Net debt/EBITDA < 4.0 OR net cash OR (EBITDA negative AND net debt < 30% equity)
              AND (
                  -- Net cash position (no leverage concern)
                  COALESCE(mh.net_debt_current, mh.total_debt_current - mh.cash_current, 0) < 0
                  -- OR Net debt/EBITDA < 4.0
                  OR (mh.ebitda_current > 0
                      AND COALESCE(mh.net_debt_current, mh.total_debt_current - mh.cash_current, 0)::DECIMAL / mh.ebitda_current < 4.0)
                  -- OR EBITDA negative but net debt < 30% of equity
                  OR (mh.ebitda_current <= 0
                      AND mh.total_equity_current > 0
                      AND COALESCE(mh.net_debt_current, mh.total_debt_current - mh.cash_current, 0)::DECIMAL / mh.total_equity_current < 0.30)
              )

              -- TIER 4: Turnaround Signal - At least ONE must be true
              AND (
                  -- Signal 1: EBITDA positive
                  mh.ebitda_current > 0
                  -- Signal 2: EBITDA margin improving > 1pp
                  OR (mh.ebitda_margin_1yr_ago IS NOT NULL
                      AND mh.ebitda_margin_current - mh.ebitda_margin_1yr_ago > 0.01)
                  -- Signal 3: Operating loss narrowing
                  OR (mh.operating_income_current < 0
                      AND mh.operating_income_1yr_ago < 0
                      AND mh.operating_income_current > mh.operating_income_1yr_ago)
                  -- Signal 4: Revenue growth resuming after prior decline
                  OR (mh.revenue_1yr_ago IS NOT NULL
                      AND mh.revenue_2yr_ago IS NOT NULL
                      AND mh.revenue_1yr_ago < mh.revenue_2yr_ago  -- Prior year was decline
                      AND mh.revenue_current > mh.revenue_1yr_ago)  -- Now growing
              )

              -- Minimum market cap (10M EUR)
              AND p.close_price * mh.shares_current * ({fx_to_eur_sql}) > {MIN_MARKET_CAP_EUR}

            ORDER BY
                -- Sort by combined valuation discount (lower = more attractive)
                (p.close_price * mh.shares_current)::DECIMAL / (mh.revenue_current * ({fx_rate_sql})) +
                (p.close_price * mh.shares_current)::DECIMAL / (mh.total_equity_current * ({fx_rate_sql}))
        """

        rows = await self.conn.fetch(query, self.score_date)

        self.log(f"Found {len(rows)} candidates before sector exclusions")

        results = []
        for row in rows:
            # Apply sector exclusions
            sector_lower = row['sector_lower'] or ''
            industry_lower = row['industry_lower'] or ''

            excluded = False
            for excluded_term in EXCLUDED_SECTORS:
                if excluded_term in sector_lower or excluded_term in industry_lower:
                    excluded = True
                    break

            if not excluded:
                for excluded_term in EXCLUDED_INDUSTRIES:
                    if excluded_term in industry_lower:
                        excluded = True
                        break

            if excluded:
                continue

            # Calculate turnaround signals count
            turnaround_signals = []
            ebitda = float(row['ebitda_current']) if row['ebitda_current'] else 0
            ebitda_margin_change = float(row['ebitda_margin_change_yoy']) if row['ebitda_margin_change_yoy'] else None
            op_income_change = float(row['operating_income_change']) if row['operating_income_change'] else None
            op_income_current = float(row['operating_income_current']) if row['operating_income_current'] else 0
            op_income_1yr = float(row['operating_income_1yr_ago']) if row['operating_income_1yr_ago'] else None
            revenue_yoy = float(row['revenue_yoy']) if row['revenue_yoy'] else None
            revenue_prior_yoy = float(row['revenue_prior_yoy']) if row['revenue_prior_yoy'] else None

            if ebitda > 0:
                turnaround_signals.append("EBITDA_POSITIVE")
            if ebitda_margin_change is not None and ebitda_margin_change > 0.01:
                turnaround_signals.append("EBITDA_MARGIN_IMPROVING")
            if op_income_current < 0 and op_income_1yr is not None and op_income_1yr < 0 and op_income_current > op_income_1yr:
                turnaround_signals.append("LOSS_NARROWING")
            if revenue_yoy is not None and revenue_yoy > 0 and revenue_prior_yoy is not None and revenue_prior_yoy < 0:
                turnaround_signals.append("REVENUE_GROWTH_RESUMING")

            p_s = float(row['price_to_sales']) if row['price_to_sales'] else 0
            p_b = float(row['price_to_book']) if row['price_to_book'] else 0
            ev_rev = float(row['ev_to_revenue']) if row['ev_to_revenue'] else 0
            gross_margin = float(row['gross_margin_current']) if row['gross_margin_current'] else 0
            gm_change = float(row['gross_margin_change_yoy']) if row['gross_margin_change_yoy'] else 0
            net_debt_ebitda = float(row['net_debt_to_ebitda']) if row['net_debt_to_ebitda'] else None
            net_debt_equity = float(row['net_debt_to_equity']) if row['net_debt_to_equity'] else 0

            metrics = {
                'primary_ticker': row['primary_ticker'],
                'company_name': row['company_name'],
                'sector': row['sector'],
                'industry': row['industry'],
                'price': float(row['price']) if row['price'] else None,
                'price_date': row['price_date'].isoformat() if row['price_date'] else None,
                'market_cap': float(row['market_cap']) if row['market_cap'] else None,
                'enterprise_value': float(row['enterprise_value']) if row['enterprise_value'] else None,

                # TIER 1: Valuation
                'price_to_sales': p_s,
                'price_to_book': p_b,
                'ev_to_revenue': ev_rev,

                # TIER 2: Business quality
                'revenue_current': float(row['revenue_current']) if row['revenue_current'] else None,
                'revenue_yoy': revenue_yoy,
                'gross_margin_current': gross_margin,
                'gross_margin_1yr_ago': float(row['gross_margin_1yr_ago']) if row['gross_margin_1yr_ago'] else None,
                'gross_margin_change_yoy': gm_change,

                # TIER 3: Financial health
                'current_ratio': float(row['current_ratio']) if row['current_ratio'] else None,
                'equity_ratio': float(row['equity_ratio']) if row['equity_ratio'] else None,
                'net_debt_to_ebitda': net_debt_ebitda,
                'net_debt_to_equity': net_debt_equity,
                'is_net_cash': net_debt_equity < 0 if net_debt_equity else False,

                # TIER 4: Turnaround signals
                'ebitda_current': ebitda,
                'ebitda_margin_change_yoy': ebitda_margin_change,
                'operating_income_current': op_income_current,
                'operating_income_1yr_ago': op_income_1yr,
                'turnaround_signals': turnaround_signals,
                'turnaround_signal_count': len(turnaround_signals),

                # Other metrics
                'operating_margin_current': float(row['operating_margin_current']) if row['operating_margin_current'] else None,
                'net_margin_current': float(row['net_margin_current']) if row['net_margin_current'] else None,
                'total_debt': float(row['total_debt_current']) if row['total_debt_current'] else 0,
                'total_equity': float(row['total_equity_current']) if row['total_equity_current'] else None,
                'cash_current': float(row['cash_current']) if row['cash_current'] else None,
                'free_cash_flow': float(row['fcf_current']) if row['fcf_current'] else None,

                # Data metadata
                'report_currency': row['report_currency'],
                'trading_currency': row['trading_currency'],
                'fx_rate': float(row['fx_rate']) if row['fx_rate'] else 1.0,
                'financial_date': row['financial_date'].isoformat() if row['financial_date'] else None,
                'is_ttm': row['is_ttm'],
                'data_source': 'TTM (4 quarters)' if row['is_ttm'] else 'Annual report',
            }

            # Build flags
            flags = []

            # Data freshness flag
            if row['is_ttm']:
                flags.append("TTM_DATA: Using trailing 12 months (fresher)")
            else:
                flags.append("ANNUAL_DATA: Using latest annual report")

            # TIER 1: Valuation flags
            if p_s < 0.2:
                flags.append("EXTREME_PS_DISCOUNT: P/S under 0.2x")
            elif p_s < 0.3:
                flags.append("DEEP_PS_DISCOUNT: P/S under 0.3x")

            if p_b < 0.5:
                flags.append("EXTREME_PB_DISCOUNT: P/B under 0.5x")
            elif p_b < 0.7:
                flags.append("DEEP_PB_DISCOUNT: P/B under 0.7x")

            if ev_rev < 0.3:
                flags.append("EXTREME_EV_DISCOUNT: EV/Revenue under 0.3x")

            # TIER 2: Business quality flags
            if gross_margin > 0.25:
                flags.append("STRONG_GROSS_MARGIN: GM > 25% (exceptional for industrial)")
            elif gross_margin > 0.20:
                flags.append("GOOD_GROSS_MARGIN: GM > 20%")
            elif gross_margin > 0.15:
                flags.append("DECENT_GROSS_MARGIN: GM > 15%")

            if gm_change is not None:
                if gm_change > 0.02:
                    flags.append("MARGIN_IMPROVING: Gross margin up > 2pp YoY")
                elif gm_change > 0:
                    flags.append("MARGIN_STABLE_UP: Gross margin slightly up YoY")
                elif gm_change > -0.01:
                    flags.append("MARGIN_STABLE: Gross margin holding")

            # TIER 3: Financial health flags
            if net_debt_equity < 0:
                flags.append("NET_CASH: Company has net cash position")
            elif net_debt_ebitda is not None and net_debt_ebitda < 1.0:
                flags.append("LOW_LEVERAGE: Net debt < 1x EBITDA")
            elif net_debt_ebitda is not None and net_debt_ebitda < 2.0:
                flags.append("MODERATE_LEVERAGE: Net debt 1-2x EBITDA")

            # TIER 4: Turnaround signals
            for signal in turnaround_signals:
                if signal == "EBITDA_POSITIVE":
                    flags.append("TURNAROUND_EBITDA_POSITIVE: Generating EBITDA")
                elif signal == "EBITDA_MARGIN_IMPROVING":
                    flags.append("TURNAROUND_MARGIN_IMPROVING: EBITDA margin up > 1pp")
                elif signal == "LOSS_NARROWING":
                    flags.append("TURNAROUND_LOSS_NARROWING: Operating losses shrinking")
                elif signal == "REVENUE_GROWTH_RESUMING":
                    flags.append("TURNAROUND_REVENUE_RESUMING: Growth after prior decline")

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

        self.log(f"After sector exclusions: {len(results)} candidates")

        return results

    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate score based on:
        1. Valuation depth (30%): How cheap relative to thresholds
        2. Gross margin quality (20%): Higher gross margin = better business
        3. Gross margin stability (15%): YoY change in gross margin
        4. Balance sheet strength (15%): Net cash/debt position
        5. Turnaround momentum (20%): How many turnaround signals firing
        """
        score = 0.0

        p_s = metrics.get('price_to_sales', 0.5) or 0.5
        p_b = metrics.get('price_to_book', 1.0) or 1.0
        gross_margin = metrics.get('gross_margin_current', 0.12) or 0.12
        gm_change = metrics.get('gross_margin_change_yoy', 0) or 0
        net_debt_ebitda = metrics.get('net_debt_to_ebitda')
        is_net_cash = metrics.get('is_net_cash', False)
        signal_count = metrics.get('turnaround_signal_count', 0) or 0

        # Component 1: Valuation depth (up to 30 points)
        # Split between P/S (20) and P/B (10)
        # P/S scoring
        if p_s < 0.2:
            score += 20  # Extreme discount
        elif p_s < 0.25:
            score += 17
        elif p_s < 0.3:
            score += 14
        elif p_s < 0.35:
            score += 11
        elif p_s < 0.4:
            score += 8
        elif p_s < 0.5:
            score += 5

        # P/B scoring
        if p_b < 0.5:
            score += 10  # Extreme discount
        elif p_b < 0.6:
            score += 8
        elif p_b < 0.7:
            score += 6
        elif p_b < 0.8:
            score += 4
        elif p_b < 1.0:
            score += 2

        # Component 2: Gross margin quality (up to 20 points)
        if gross_margin > 0.25:
            score += 20  # Exceptional for industrial
        elif gross_margin > 0.22:
            score += 17
        elif gross_margin > 0.20:
            score += 14
        elif gross_margin > 0.17:
            score += 10
        elif gross_margin > 0.15:
            score += 7
        elif gross_margin > 0.12:
            score += 4

        # Component 3: Gross margin stability (up to 15 points)
        if gm_change > 0.02:
            score += 15  # Improving > 2pp
        elif gm_change > 0.01:
            score += 12  # Improving 1-2pp
        elif gm_change > 0:
            score += 10  # Slightly improving
        elif gm_change > -0.01:
            score += 7   # Stable
        elif gm_change > -0.02:
            score += 4   # Small decline
        elif gm_change > -0.03:
            score += 2   # Larger decline (but still passes)

        # Component 4: Balance sheet strength (up to 15 points)
        if is_net_cash:
            score += 15  # Net cash position
        elif net_debt_ebitda is not None:
            if net_debt_ebitda < 1.0:
                score += 12
            elif net_debt_ebitda < 2.0:
                score += 9
            elif net_debt_ebitda < 3.0:
                score += 5
            elif net_debt_ebitda < 4.0:
                score += 2

        # Component 5: Turnaround momentum (up to 20 points)
        if signal_count >= 4:
            score += 20
        elif signal_count == 3:
            score += 16
        elif signal_count == 2:
            score += 12
        elif signal_count == 1:
            score += 8

        return self.clamp_score(score)

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate Tier B prompt for industrial asset recovery assessment.
        """
        turnaround_signals = metrics.get('turnaround_signals', [])
        signals_str = ', '.join(turnaround_signals) if turnaround_signals else 'None identified'

        return f"""Assess the industrial asset recovery potential for this company.

Company: {company_name}
Sector: {metrics.get('sector', 'N/A')}
Industry: {metrics.get('industry', 'N/A')}

VALUATION:
- Price/Sales: {metrics.get('price_to_sales', 0):.2f}x
- Price/Book: {metrics.get('price_to_book', 0):.2f}x
- EV/Revenue: {metrics.get('ev_to_revenue', 0):.2f}x

BUSINESS QUALITY:
- Gross margin: {(metrics.get('gross_margin_current', 0) or 0) * 100:.1f}%
- Gross margin change YoY: {(metrics.get('gross_margin_change_yoy', 0) or 0) * 100:.1f}pp
- Operating margin: {(metrics.get('operating_margin_current', 0) or 0) * 100:.1f}%
- Revenue YoY: {(metrics.get('revenue_yoy', 0) or 0) * 100:.1f}%

FINANCIAL HEALTH:
- Current ratio: {metrics.get('current_ratio', 0):.2f}x
- Equity ratio: {(metrics.get('equity_ratio', 0) or 0) * 100:.1f}%
- Net debt/EBITDA: {metrics.get('net_debt_to_ebitda', 'N/A')}
- Net cash position: {'Yes' if metrics.get('is_net_cash') else 'No'}

TURNAROUND SIGNALS: {signals_str}

From the latest reports, determine:

1. ASSET QUALITY: What are the core physical assets?
   - Manufacturing facilities/equipment?
   - Real estate?
   - Inventory value?
   - Customer contracts/order book?

2. CAUSE OF DEPRESSION: Why is profitability low?
   - Cyclical downturn?
   - Restructuring costs?
   - Bad subsidiary/product line?
   - Commodity price pressure?
   - Temporary demand weakness?

3. GROSS MARGIN ASSESSMENT: Is the 12%+ gross margin sustainable?
   - Competitive position?
   - Pricing power?
   - Input cost trends?

4. TURNAROUND CATALYST: What could drive recovery?
   - Cost restructuring underway?
   - Capacity rationalization?
   - End-market recovery?
   - New products/contracts?
   - Asset sales?

5. HIDDEN VALUE: Any non-operating assets?
   - Real estate at book value?
   - Stakes in other companies?
   - Tax loss carryforwards?

6. RISKS: What could prevent recovery?
   - Debt maturities?
   - Customer concentration?
   - Technological obsolescence?
   - Competitive threats?

Respond in JSON:
{{
  "asset_type": "MANUFACTURING|SERVICES|DISTRIBUTION|INFRASTRUCTURE|OTHER",
  "key_assets": ["string array"],
  "depression_cause": "CYCLICAL|RESTRUCTURING|BAD_SUBSIDIARY|COMMODITY|DEMAND|OTHER",
  "depression_explanation": "string",
  "gross_margin_sustainability": "HIGH|MEDIUM|LOW",
  "turnaround_catalysts": ["string array"],
  "primary_catalyst": "string",
  "hidden_value_items": ["string array"],
  "key_risks": ["string array"],
  "recovery_probability": "HIGH|MEDIUM|LOW",
  "recovery_timeline_quarters": "number or null",
  "summary": "string"
}}"""
