#!/usr/bin/env python3
"""
Business Screener Deluxe - Database Migration

Creates all tables and views needed for the screening system.
Safe to run multiple times (idempotent).

Run: python domains/business_screener/migration_setup.py
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def migrate():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("🚀 Business Screener Deluxe - Database Migration")
        print("=" * 60)

        # ============================================================
        # CORE TABLES
        # ============================================================

        print("\n📦 Creating core tables...")

        # Screen definitions
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bsd_screen_definitions (
                screen_type INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                short_name VARCHAR(50) NOT NULL,
                description TEXT,
                tier_a_enabled BOOLEAN DEFAULT TRUE,
                tier_b_enabled BOOLEAN DEFAULT FALSE,
                tier_c_enabled BOOLEAN DEFAULT FALSE,
                run_frequency VARCHAR(20),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("  ✓ bsd_screen_definitions")

        # Screen results
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bsd_screen_results (
                id SERIAL PRIMARY KEY,
                company_id UUID NOT NULL,
                screen_type INT NOT NULL,
                tier CHAR(1) NOT NULL,
                score DECIMAL(5,2),
                metrics JSONB,
                flags TEXT[],
                is_active BOOLEAN DEFAULT TRUE,
                triggered_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                CONSTRAINT fk_screen_type FOREIGN KEY (screen_type)
                    REFERENCES bsd_screen_definitions(screen_type)
            )
        """)
        print("  ✓ bsd_screen_results")

        # Company classifications (for cyclical detection)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bsd_company_classifications (
                company_id UUID PRIMARY KEY,
                classification VARCHAR(20) NOT NULL,
                cycle_driver TEXT,
                cycle_position VARCHAR(20),
                mid_cycle_ebitda DECIMAL(15,2),
                peak_to_trough_ratio DECIMAL(5,2),
                classified_at TIMESTAMP DEFAULT NOW(),
                classification_source VARCHAR(20) DEFAULT 'MANUAL',
                confidence_score DECIMAL(3,2),
                metadata JSONB
            )
        """)
        print("  ✓ bsd_company_classifications")

        # LLM analysis results
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bsd_llm_analysis_results (
                id SERIAL PRIMARY KEY,
                company_id UUID NOT NULL,
                screen_type INT,
                analysis_type VARCHAR(50) NOT NULL,
                tier CHAR(1) NOT NULL,
                model_used VARCHAR(50),
                prompt_template VARCHAR(100),
                raw_response TEXT,
                parsed_response JSONB,
                confidence_score DECIMAL(3,2),
                report_period VARCHAR(10),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("  ✓ bsd_llm_analysis_results")

        # Holding company portfolios (for Screen 9)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bsd_holding_company_portfolios (
                id SERIAL PRIMARY KEY,
                holding_company_id UUID NOT NULL,
                held_company_id UUID,
                held_company_name VARCHAR(200),
                shares_held BIGINT,
                market_value DECIMAL(15,2),
                book_value DECIMAL(15,2),
                ownership_pct DECIMAL(5,2),
                is_listed BOOLEAN DEFAULT TRUE,
                last_updated TIMESTAMP DEFAULT NOW()
            )
        """)
        print("  ✓ bsd_holding_company_portfolios")

        # ============================================================
        # INDEXES
        # ============================================================

        print("\n📇 Creating indexes...")

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bsd_results_company
            ON bsd_screen_results(company_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bsd_results_type
            ON bsd_screen_results(screen_type)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bsd_results_active
            ON bsd_screen_results(is_active, screen_type)
            WHERE is_active = TRUE
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bsd_results_triggered
            ON bsd_screen_results(triggered_at DESC)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bsd_llm_company
            ON bsd_llm_analysis_results(company_id, analysis_type)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_bsd_holdings_company
            ON bsd_holding_company_portfolios(holding_company_id)
        """)

        print("  ✓ All indexes created")

        # ============================================================
        # HELPER VIEWS
        # ============================================================

        print("\n👁️ Creating helper views...")

        # Drop existing views first (they depend on each other)
        views_to_drop = [
            'bsd_v_multi_screen_hits',
            'bsd_v_price_changes',
            'bsd_v_price_metrics',
            'bsd_v_growth_rates',
            'bsd_v_7yr_averages',
            'bsd_v_5yr_averages',
            'bsd_v_latest_quarterly',
            'bsd_v_latest_annual',
            'bsd_v_companies'
        ]

        for view in views_to_drop:
            await conn.execute(f"DROP VIEW IF EXISTS {view} CASCADE")

        # Companies view - unified company info with current market cap
        # Note: market_cap needs to be calculated from price * shares
        await conn.execute("""
            CREATE VIEW bsd_v_companies AS
            SELECT
                cm.id,
                cm.company_name,
                cm.primary_ticker,
                cm.yahoo_symbol,
                cm.sector,
                cm.industry,
                cm.country,
                cm.currency,
                cm.market_cap_usd,
                -- Convert primary_ticker to space format for financial data joins
                REPLACE(cm.primary_ticker, '-', ' ') AS financial_symbol,
                -- Get latest price
                latest_price.close_price AS price,
                latest_price.price_date,
                -- Calculate market cap from latest price and shares
                COALESCE(
                    latest_price.close_price * latest_bs.shares_outstanding,
                    cm.market_cap_usd
                ) AS market_cap,
                latest_bs.shares_outstanding
            FROM company_master cm
            LEFT JOIN LATERAL (
                SELECT close_price, date AS price_date
                FROM daily_price_data dpd
                WHERE dpd.symbol = cm.primary_ticker
                ORDER BY date DESC
                LIMIT 1
            ) latest_price ON TRUE
            LEFT JOIN LATERAL (
                SELECT shares_outstanding
                FROM balance_sheet_data bs
                WHERE bs.symbol = REPLACE(cm.primary_ticker, '-', ' ')
                  AND bs.statement_type = 'annual'
                ORDER BY period_date DESC
                LIMIT 1
            ) latest_bs ON TRUE
            WHERE cm.listing_status = 'active'
        """)
        print("  ✓ bsd_v_companies")

        # Latest annual financials per company
        await conn.execute("""
            CREATE VIEW bsd_v_latest_annual AS
            SELECT DISTINCT ON (fs.symbol)
                fs.symbol,
                fs.period_date,
                fs.fiscal_year,
                fs.publish_date,
                fs.currency AS report_currency,
                -- Income statement
                fs.total_revenue,
                fs.gross_profit,
                fs.operating_income,
                fs.net_income,
                fs.ebit,
                fs.ebitda,
                fs.basic_eps,
                fs.interest_expense,
                -- Balance sheet (join)
                bs.total_assets,
                bs.current_assets,
                bs.cash_and_equivalents,
                bs.accounts_receivable,
                bs.inventory,
                bs.total_liabilities,
                bs.current_liabilities,
                bs.total_debt,
                bs.long_term_debt,
                bs.total_equity,
                bs.shares_outstanding,
                -- Cash flow (join)
                cf.operating_cash_flow,
                cf.capital_expenditure,
                cf.dividends_paid,
                cf.free_cash_flow,
                cf.depreciation_amortization,
                -- Calculated fields
                COALESCE(bs.total_debt, 0) - COALESCE(bs.cash_and_equivalents, 0) AS net_debt,
                CASE WHEN bs.shares_outstanding > 0
                    THEN bs.total_equity::DECIMAL / bs.shares_outstanding
                    ELSE NULL END AS book_value_per_share,
                -- Margins
                CASE WHEN fs.total_revenue > 0
                    THEN fs.gross_profit::DECIMAL / fs.total_revenue
                    ELSE NULL END AS gross_margin,
                CASE WHEN fs.total_revenue > 0
                    THEN fs.operating_income::DECIMAL / fs.total_revenue
                    ELSE NULL END AS operating_margin,
                CASE WHEN fs.total_revenue > 0
                    THEN fs.net_income::DECIMAL / fs.total_revenue
                    ELSE NULL END AS net_margin,
                CASE WHEN fs.total_revenue > 0
                    THEN cf.free_cash_flow::DECIMAL / fs.total_revenue
                    ELSE NULL END AS fcf_margin,
                -- Returns
                CASE WHEN bs.total_equity > 0
                    THEN fs.net_income::DECIMAL / bs.total_equity
                    ELSE NULL END AS roe,
                CASE WHEN (bs.total_assets - bs.current_liabilities) > 0
                    THEN fs.operating_income::DECIMAL / (bs.total_assets - bs.current_liabilities)
                    ELSE NULL END AS roic,
                -- Liquidity
                CASE WHEN bs.current_liabilities > 0
                    THEN bs.current_assets::DECIMAL / bs.current_liabilities
                    ELSE NULL END AS current_ratio,
                -- Leverage
                CASE WHEN fs.ebitda > 0
                    THEN (COALESCE(bs.total_debt, 0) - COALESCE(bs.cash_and_equivalents, 0))::DECIMAL / fs.ebitda
                    ELSE NULL END AS net_debt_to_ebitda,
                CASE WHEN bs.total_assets > 0
                    THEN bs.total_debt::DECIMAL / bs.total_assets
                    ELSE NULL END AS debt_to_assets
            FROM financial_statements fs
            LEFT JOIN balance_sheet_data bs
                ON fs.symbol = bs.symbol
                AND fs.period_date = bs.period_date
                AND fs.statement_type = bs.statement_type
            LEFT JOIN cash_flow_data cf
                ON fs.symbol = cf.symbol
                AND fs.period_date = cf.period_date
                AND fs.statement_type = cf.statement_type
            WHERE fs.statement_type = 'annual'
            ORDER BY fs.symbol, fs.period_date DESC
        """)
        print("  ✓ bsd_v_latest_annual")

        # Latest quarterly financials
        await conn.execute("""
            CREATE VIEW bsd_v_latest_quarterly AS
            SELECT DISTINCT ON (fs.symbol)
                fs.symbol,
                fs.period_date,
                fs.fiscal_year,
                fs.fiscal_quarter,
                fs.publish_date,
                fs.currency AS report_currency,
                fs.total_revenue,
                fs.gross_profit,
                fs.operating_income,
                fs.net_income,
                fs.ebitda,
                bs.total_assets,
                bs.current_assets,
                bs.cash_and_equivalents,
                bs.total_liabilities,
                bs.current_liabilities,
                bs.total_debt,
                bs.total_equity,
                bs.shares_outstanding,
                cf.operating_cash_flow,
                cf.free_cash_flow
            FROM financial_statements fs
            LEFT JOIN balance_sheet_data bs
                ON fs.symbol = bs.symbol
                AND fs.period_date = bs.period_date
                AND fs.statement_type = bs.statement_type
            LEFT JOIN cash_flow_data cf
                ON fs.symbol = cf.symbol
                AND fs.period_date = cf.period_date
                AND fs.statement_type = cf.statement_type
            WHERE fs.statement_type = 'quarterly'
            ORDER BY fs.symbol, fs.period_date DESC
        """)
        print("  ✓ bsd_v_latest_quarterly")

        # 5-year averages
        await conn.execute("""
            CREATE VIEW bsd_v_5yr_averages AS
            SELECT
                fs.symbol,
                AVG(fs.total_revenue) AS avg_revenue_5yr,
                AVG(fs.operating_income) AS avg_operating_income_5yr,
                AVG(fs.net_income) AS avg_net_income_5yr,
                AVG(fs.ebitda) AS avg_ebitda_5yr,
                AVG(CASE WHEN fs.total_revenue > 0
                    THEN fs.operating_income::DECIMAL / fs.total_revenue
                    ELSE NULL END) AS avg_operating_margin_5yr,
                AVG(CASE WHEN bs.total_equity > 0
                    THEN fs.net_income::DECIMAL / bs.total_equity
                    ELSE NULL END) AS avg_roe_5yr,
                AVG(CASE WHEN (bs.total_assets - bs.current_liabilities) > 0
                    THEN fs.operating_income::DECIMAL / (bs.total_assets - bs.current_liabilities)
                    ELSE NULL END) AS avg_roic_5yr,
                MIN(fs.net_income) AS min_net_income_5yr,
                MAX(fs.net_income) AS max_net_income_5yr,
                COUNT(CASE WHEN fs.net_income > 0 THEN 1 END) AS profitable_years_5yr,
                COUNT(*) AS years_available_5yr
            FROM financial_statements fs
            LEFT JOIN balance_sheet_data bs
                ON fs.symbol = bs.symbol
                AND fs.period_date = bs.period_date
                AND fs.statement_type = bs.statement_type
            WHERE fs.statement_type = 'annual'
              AND fs.fiscal_year >= EXTRACT(YEAR FROM NOW()) - 5
            GROUP BY fs.symbol
        """)
        print("  ✓ bsd_v_5yr_averages")

        # 7-year averages (for cyclicals)
        await conn.execute("""
            CREATE VIEW bsd_v_7yr_averages AS
            SELECT
                fs.symbol,
                AVG(fs.ebitda) AS avg_ebitda_7yr,
                MAX(fs.ebitda) AS peak_ebitda_7yr,
                MIN(fs.ebitda) AS trough_ebitda_7yr,
                AVG(fs.total_revenue) AS avg_revenue_7yr,
                MAX(fs.total_revenue) AS peak_revenue_7yr,
                MIN(fs.total_revenue) AS trough_revenue_7yr,
                AVG(fs.net_income) AS avg_net_income_7yr,
                COUNT(*) AS years_available_7yr
            FROM financial_statements fs
            WHERE fs.statement_type = 'annual'
              AND fs.fiscal_year >= EXTRACT(YEAR FROM NOW()) - 7
            GROUP BY fs.symbol
        """)
        print("  ✓ bsd_v_7yr_averages")

        # Growth rates
        await conn.execute("""
            CREATE VIEW bsd_v_growth_rates AS
            WITH annual_data AS (
                SELECT
                    fs.symbol,
                    fs.fiscal_year,
                    fs.total_revenue,
                    fs.net_income,
                    fs.gross_profit,
                    bs.total_assets,
                    bs.shares_outstanding,
                    CASE WHEN fs.total_revenue > 0
                        THEN fs.gross_profit::DECIMAL / fs.total_revenue
                        ELSE NULL END AS gross_margin
                FROM financial_statements fs
                LEFT JOIN balance_sheet_data bs
                    ON fs.symbol = bs.symbol
                    AND fs.period_date = bs.period_date
                    AND fs.statement_type = bs.statement_type
                WHERE fs.statement_type = 'annual'
            ),
            current_year AS (
                SELECT MAX(fiscal_year) AS max_year FROM annual_data
            )
            SELECT
                curr.symbol,
                -- 1-year growth
                CASE WHEN ABS(prev1.total_revenue) > 0
                    THEN (curr.total_revenue - prev1.total_revenue)::DECIMAL / ABS(prev1.total_revenue)
                    ELSE NULL END AS revenue_growth_1yr,
                CASE WHEN ABS(prev1.net_income) > 0
                    THEN (curr.net_income - prev1.net_income)::DECIMAL / ABS(prev1.net_income)
                    ELSE NULL END AS earnings_growth_1yr,
                curr.gross_margin - prev1.gross_margin AS gross_margin_change_1yr,
                -- 3-year CAGR
                CASE WHEN prev3.total_revenue > 0 AND curr.total_revenue > 0
                    THEN POWER(curr.total_revenue::DECIMAL / prev3.total_revenue, 1.0/3) - 1
                    ELSE NULL END AS revenue_cagr_3yr,
                -- 5-year CAGR
                CASE WHEN prev5.total_revenue > 0 AND curr.total_revenue > 0
                    THEN POWER(curr.total_revenue::DECIMAL / prev5.total_revenue, 1.0/5) - 1
                    ELSE NULL END AS revenue_cagr_5yr,
                CASE WHEN prev5.net_income > 0 AND curr.net_income > 0
                    THEN POWER(curr.net_income::DECIMAL / prev5.net_income, 1.0/5) - 1
                    ELSE NULL END AS earnings_cagr_5yr,
                -- Gross margins
                curr.gross_margin AS gross_margin_current,
                prev3.gross_margin AS gross_margin_3yr_ago,
                -- Total assets change
                CASE WHEN ABS(prev1.total_assets) > 0
                    THEN (curr.total_assets - prev1.total_assets)::DECIMAL / ABS(prev1.total_assets)
                    ELSE NULL END AS total_assets_change_1yr,
                -- Buyback yield (share count reduction)
                CASE WHEN prev1.shares_outstanding > 0
                    THEN (prev1.shares_outstanding - curr.shares_outstanding)::DECIMAL / prev1.shares_outstanding
                    ELSE NULL END AS buyback_yield_1yr
            FROM annual_data curr
            CROSS JOIN current_year cy
            LEFT JOIN annual_data prev1 ON curr.symbol = prev1.symbol AND prev1.fiscal_year = cy.max_year - 1
            LEFT JOIN annual_data prev3 ON curr.symbol = prev3.symbol AND prev3.fiscal_year = cy.max_year - 3
            LEFT JOIN annual_data prev5 ON curr.symbol = prev5.symbol AND prev5.fiscal_year = cy.max_year - 5
            WHERE curr.fiscal_year = cy.max_year
        """)
        print("  ✓ bsd_v_growth_rates")

        # Price metrics (valuation ratios)
        await conn.execute("""
            CREATE VIEW bsd_v_price_metrics AS
            SELECT
                c.id AS company_id,
                c.primary_ticker,
                c.company_name,
                c.price,
                c.market_cap,
                c.shares_outstanding,
                la.report_currency,
                -- P/E ratio
                CASE WHEN la.net_income > 0
                    THEN c.market_cap::DECIMAL / la.net_income
                    ELSE NULL END AS pe_ratio,
                -- P/S ratio
                CASE WHEN la.total_revenue > 0
                    THEN c.market_cap::DECIMAL / la.total_revenue
                    ELSE NULL END AS ps_ratio,
                -- P/B ratio
                CASE WHEN la.book_value_per_share > 0
                    THEN c.price::DECIMAL / la.book_value_per_share
                    ELSE NULL END AS pb_ratio,
                -- Enterprise Value
                c.market_cap + COALESCE(la.net_debt, 0) AS enterprise_value,
                -- EV/EBITDA
                CASE WHEN la.ebitda > 0
                    THEN (c.market_cap + COALESCE(la.net_debt, 0))::DECIMAL / la.ebitda
                    ELSE NULL END AS ev_to_ebitda,
                -- EV/EBIT
                CASE WHEN la.operating_income > 0
                    THEN (c.market_cap + COALESCE(la.net_debt, 0))::DECIMAL / la.operating_income
                    ELSE NULL END AS ev_to_ebit,
                -- Earnings yield (inverse P/E)
                CASE WHEN c.market_cap > 0
                    THEN la.net_income::DECIMAL / c.market_cap
                    ELSE NULL END AS earnings_yield,
                -- FCF yield
                CASE WHEN c.market_cap > 0
                    THEN la.free_cash_flow::DECIMAL / c.market_cap
                    ELSE NULL END AS fcf_yield,
                -- Dividend yield
                CASE WHEN c.market_cap > 0 AND la.dividends_paid IS NOT NULL
                    THEN ABS(la.dividends_paid)::DECIMAL / c.market_cap
                    ELSE NULL END AS dividend_yield,
                -- NCAV (Net Current Asset Value) - Graham's liquidation value
                la.current_assets - la.total_liabilities AS ncav,
                CASE WHEN c.shares_outstanding > 0
                    THEN (la.current_assets - la.total_liabilities)::DECIMAL / c.shares_outstanding
                    ELSE NULL END AS ncav_per_share,
                -- P/E * P/B for Graham screen
                CASE WHEN la.net_income > 0 AND la.book_value_per_share > 0
                    THEN (c.market_cap::DECIMAL / la.net_income) * (c.price::DECIMAL / la.book_value_per_share)
                    ELSE NULL END AS pe_times_pb,
                -- Pass through key financial metrics
                la.net_income,
                la.total_revenue,
                la.ebitda,
                la.operating_income,
                la.free_cash_flow,
                la.current_assets,
                la.total_liabilities,
                la.total_equity,
                la.total_debt,
                la.cash_and_equivalents,
                la.net_debt,
                la.book_value_per_share,
                la.gross_margin,
                la.operating_margin,
                la.current_ratio,
                la.net_debt_to_ebitda,
                la.roe,
                la.roic
            FROM bsd_v_companies c
            JOIN bsd_v_latest_annual la ON c.financial_symbol = la.symbol
            WHERE c.price IS NOT NULL
              AND c.market_cap IS NOT NULL
              AND c.market_cap > 0
        """)
        print("  ✓ bsd_v_price_metrics")

        # Price changes (recent performance)
        await conn.execute("""
            CREATE VIEW bsd_v_price_changes AS
            WITH price_history AS (
                SELECT
                    symbol,
                    date,
                    close_price,
                    FIRST_VALUE(close_price) OVER (
                        PARTITION BY symbol ORDER BY date DESC
                    ) AS latest_price,
                    LAG(close_price, 30) OVER (
                        PARTITION BY symbol ORDER BY date
                    ) AS price_30d_ago,
                    LAG(close_price, 90) OVER (
                        PARTITION BY symbol ORDER BY date
                    ) AS price_90d_ago,
                    LAG(close_price, 252) OVER (
                        PARTITION BY symbol ORDER BY date
                    ) AS price_1yr_ago
                FROM daily_price_data
                WHERE date >= NOW() - INTERVAL '400 days'
            ),
            latest_prices AS (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    latest_price,
                    price_30d_ago,
                    price_90d_ago,
                    price_1yr_ago
                FROM price_history
                ORDER BY symbol, date DESC
            )
            SELECT
                lp.symbol,
                lp.latest_price,
                -- 30-day change
                CASE WHEN lp.price_30d_ago > 0
                    THEN (lp.latest_price - lp.price_30d_ago) / lp.price_30d_ago
                    ELSE NULL END AS price_change_30d,
                -- 90-day change
                CASE WHEN lp.price_90d_ago > 0
                    THEN (lp.latest_price - lp.price_90d_ago) / lp.price_90d_ago
                    ELSE NULL END AS price_change_90d,
                -- 1-year change
                CASE WHEN lp.price_1yr_ago > 0
                    THEN (lp.latest_price - lp.price_1yr_ago) / lp.price_1yr_ago
                    ELSE NULL END AS price_change_1yr
            FROM latest_prices lp
        """)
        print("  ✓ bsd_v_price_changes")

        # Multi-screen hits view
        await conn.execute("""
            CREATE VIEW bsd_v_multi_screen_hits AS
            SELECT
                sr.company_id,
                c.company_name,
                c.primary_ticker,
                COUNT(DISTINCT sr.screen_type) AS screens_triggered,
                ARRAY_AGG(DISTINCT sr.screen_type ORDER BY sr.screen_type) AS screen_types,
                AVG(sr.score) AS avg_score,
                MAX(sr.score) AS max_score,
                SUM(sr.score) AS total_score,
                BOOL_OR(sr.flags IS NOT NULL AND ARRAY_LENGTH(sr.flags, 1) > 0) AS has_warnings,
                MAX(sr.triggered_at) AS latest_trigger
            FROM bsd_screen_results sr
            JOIN bsd_v_companies c ON sr.company_id = c.id
            WHERE sr.is_active = TRUE
              AND sr.score > 0
            GROUP BY sr.company_id, c.company_name, c.primary_ticker
            HAVING COUNT(DISTINCT sr.screen_type) >= 2
            ORDER BY COUNT(DISTINCT sr.screen_type) DESC, SUM(sr.score) DESC
        """)
        print("  ✓ bsd_v_multi_screen_hits")

        # ============================================================
        # SEED DATA
        # ============================================================

        print("\n🌱 Seeding screen definitions...")

        screen_definitions = [
            (1, 'Net-Nets', 'Net-Nets', 'Below liquidation value (NCAV > market cap)', True, True, False, 'weekly'),
            (2, 'Defensive Bargains', 'Defensive', "Graham's multi-factor safety screen", True, False, False, 'monthly'),
            (3, 'Asset Plays', 'Asset Plays', 'Real assets below book value', True, True, False, 'monthly'),
            (4, 'Revenue Turnarounds', 'Turnarounds', 'Intact unit economics at death prices', True, True, False, 'weekly'),
            (5, 'Distressed Stable Earners', 'Distressed', 'Temporary margin compression', True, True, False, 'monthly'),
            (6, 'Growth at Reasonable Prices', 'GARP', 'Demonstrated growth, not hypothetical', True, True, False, 'monthly'),
            (7, 'Compressed Fundamentals', 'Compressed', 'Coiled spring - temporary earnings suppression', False, True, True, 'quarterly'),
            (8, 'Special Situations', 'Special Sit', 'Event-driven with defined timelines', False, True, False, 'daily'),
            (9, 'Holding Company Discounts', 'Holdings', 'Portfolios below sum of parts', True, True, False, 'daily'),
            (10, 'Sum-of-Parts', 'SoTP', 'Hidden value in the footnotes', False, True, True, 'annually'),
            (11, 'Cannibal Companies', 'Cannibals', 'Buyback compounders', True, False, False, 'quarterly'),
            (12, 'Wonderful Business at Fair Price', 'Wonderful', "Munger's compounders", True, False, True, 'quarterly'),
            (13, 'Crisis Bargains', 'Crisis', 'Legal or regulatory overhang', True, True, False, 'daily'),
            (14, 'Cyclicals', 'Cyclicals', 'Inverted screen for cyclical companies', True, True, False, 'monthly'),
            (15, 'Stalwarts', 'Stalwarts', 'Blue chip dip buys', True, True, False, 'weekly'),
        ]

        for sd in screen_definitions:
            await conn.execute("""
                INSERT INTO bsd_screen_definitions
                    (screen_type, name, short_name, description, tier_a_enabled, tier_b_enabled, tier_c_enabled, run_frequency)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (screen_type) DO UPDATE SET
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    description = EXCLUDED.description,
                    tier_a_enabled = EXCLUDED.tier_a_enabled,
                    tier_b_enabled = EXCLUDED.tier_b_enabled,
                    tier_c_enabled = EXCLUDED.tier_c_enabled,
                    run_frequency = EXCLUDED.run_frequency,
                    updated_at = NOW()
            """, *sd)

        print(f"  ✓ {len(screen_definitions)} screen definitions seeded")

        # ============================================================
        # VERIFICATION
        # ============================================================

        print("\n🔍 Verifying setup...")

        # Check tables
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'bsd_%'
            ORDER BY tablename
        """)
        print(f"  Tables: {len(tables)}")
        for t in tables:
            print(f"    - {t['tablename']}")

        # Check views
        views = await conn.fetch("""
            SELECT viewname FROM pg_views
            WHERE schemaname = 'public' AND viewname LIKE 'bsd_v_%'
            ORDER BY viewname
        """)
        print(f"  Views: {len(views)}")
        for v in views:
            print(f"    - {v['viewname']}")

        # Test bsd_v_companies
        company_count = await conn.fetchval("SELECT COUNT(*) FROM bsd_v_companies WHERE price IS NOT NULL")
        print(f"  Companies with prices: {company_count}")

        # Test bsd_v_price_metrics
        metrics_count = await conn.fetchval("SELECT COUNT(*) FROM bsd_v_price_metrics")
        print(f"  Companies with price metrics: {metrics_count}")

        print("\n" + "=" * 60)
        print("✅ Business Screener Deluxe migration complete!")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
