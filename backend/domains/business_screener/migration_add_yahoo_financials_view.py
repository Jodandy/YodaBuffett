#!/usr/bin/env python3
"""
Migration: Add helper view for yahoo_financials data extraction.

This view extracts key fields from the JSONB columns in yahoo_financials,
providing a more complete picture than the legacy financial_statements tables.

Key improvements over legacy tables:
- Actual goodwill and intangible assets (not approximated)
- Net tangible assets calculated by Yahoo
- Net PPE, working capital, net debt directly available
- Publish dates from earnings calendar
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def migrate():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Create comprehensive view that extracts all useful fields from yahoo_financials JSONB
        await conn.execute("""
            CREATE OR REPLACE VIEW bsd_v_yahoo_financials AS
            SELECT
                yf.id,
                yf.symbol,
                yf.period_date,
                yf.statement_type,
                yf.fiscal_year,
                yf.fiscal_quarter,
                yf.publish_date,
                yf.currency AS report_currency,

                -- Income Statement (extracted from JSONB)
                (yf.income_statement->>'total_revenue')::NUMERIC::BIGINT AS total_revenue,
                (yf.income_statement->>'gross_profit')::NUMERIC::BIGINT AS gross_profit,
                (yf.income_statement->>'operating_income')::NUMERIC::BIGINT AS operating_income,
                (yf.income_statement->>'net_income')::NUMERIC::BIGINT AS net_income,
                (yf.income_statement->>'ebitda')::NUMERIC::BIGINT AS ebitda,
                (yf.income_statement->>'ebit')::NUMERIC::BIGINT AS ebit,
                (yf.income_statement->>'interest_expense')::NUMERIC::BIGINT AS interest_expense,
                (yf.income_statement->>'cost_of_revenue')::NUMERIC::BIGINT AS cost_of_revenue,
                (yf.income_statement->>'selling_general_and_administration')::NUMERIC::BIGINT AS sga_expense,
                (yf.income_statement->>'research_and_development')::NUMERIC::BIGINT AS rd_expense,
                (yf.income_statement->>'basic_eps')::FLOAT AS basic_eps,
                (yf.income_statement->>'diluted_eps')::FLOAT AS diluted_eps,
                (yf.income_statement->>'basic_average_shares')::NUMERIC::BIGINT AS shares_outstanding,
                (yf.income_statement->>'diluted_average_shares')::NUMERIC::BIGINT AS diluted_shares,

                -- Balance Sheet (extracted from JSONB) - CORE FIELDS
                (yf.balance_sheet->>'total_assets')::NUMERIC::BIGINT AS total_assets,
                (yf.balance_sheet->>'total_liabilities_net_minority_interest')::NUMERIC::BIGINT AS total_liabilities,
                (yf.balance_sheet->>'stockholders_equity')::NUMERIC::BIGINT AS total_equity,
                (yf.balance_sheet->>'common_stock_equity')::NUMERIC::BIGINT AS common_equity,
                (yf.balance_sheet->>'total_debt')::NUMERIC::BIGINT AS total_debt,

                -- Balance Sheet - CURRENT ASSETS
                (yf.balance_sheet->>'current_assets')::NUMERIC::BIGINT AS current_assets,
                (yf.balance_sheet->>'cash_and_cash_equivalents')::NUMERIC::BIGINT AS cash_and_equivalents,
                (yf.balance_sheet->>'cash_cash_equivalents_and_short_term_investments')::NUMERIC::BIGINT AS cash_and_investments,
                (yf.balance_sheet->>'accounts_receivable')::NUMERIC::BIGINT AS accounts_receivable,
                (yf.balance_sheet->>'receivables')::NUMERIC::BIGINT AS total_receivables,
                (yf.balance_sheet->>'inventory')::NUMERIC::BIGINT AS inventory,
                (yf.balance_sheet->>'other_short_term_investments')::NUMERIC::BIGINT AS short_term_investments,

                -- Balance Sheet - NON-CURRENT ASSETS (KEY FOR ASSET PLAYS!)
                (yf.balance_sheet->>'total_non_current_assets')::NUMERIC::BIGINT AS non_current_assets,
                (yf.balance_sheet->>'net_ppe')::NUMERIC::BIGINT AS net_ppe,
                (yf.balance_sheet->>'gross_ppe')::NUMERIC::BIGINT AS gross_ppe,
                (yf.balance_sheet->>'accumulated_depreciation')::NUMERIC::BIGINT AS accumulated_depreciation,
                (yf.balance_sheet->>'properties')::NUMERIC::BIGINT AS properties,
                (yf.balance_sheet->>'construction_in_progress')::NUMERIC::BIGINT AS construction_in_progress,
                (yf.balance_sheet->>'machinery_furniture_equipment')::NUMERIC::BIGINT AS machinery_equipment,
                (yf.balance_sheet->>'long_term_equity_investment')::NUMERIC::BIGINT AS equity_investments,
                (yf.balance_sheet->>'investments_and_advances')::NUMERIC::BIGINT AS investments_and_advances,

                -- Balance Sheet - INTANGIBLES (KEY FOR TRUE TANGIBLE ASSETS!)
                (yf.balance_sheet->>'goodwill')::NUMERIC::BIGINT AS goodwill,
                (yf.balance_sheet->>'other_intangible_assets')::NUMERIC::BIGINT AS other_intangible_assets,
                (yf.balance_sheet->>'goodwill_and_other_intangible_assets')::NUMERIC::BIGINT AS total_intangibles,
                (yf.balance_sheet->>'net_tangible_assets')::NUMERIC::BIGINT AS net_tangible_assets,
                (yf.balance_sheet->>'tangible_book_value')::NUMERIC::BIGINT AS tangible_book_value,

                -- Balance Sheet - LIABILITIES
                (yf.balance_sheet->>'current_liabilities')::NUMERIC::BIGINT AS current_liabilities,
                (yf.balance_sheet->>'current_debt')::NUMERIC::BIGINT AS current_debt,
                (yf.balance_sheet->>'long_term_debt')::NUMERIC::BIGINT AS long_term_debt,
                (yf.balance_sheet->>'accounts_payable')::NUMERIC::BIGINT AS accounts_payable,
                (yf.balance_sheet->>'payables')::NUMERIC::BIGINT AS total_payables,
                (yf.balance_sheet->>'total_non_current_liabilities_net_minority_interest')::NUMERIC::BIGINT AS non_current_liabilities,

                -- Balance Sheet - CALCULATED BY YAHOO (VERY USEFUL!)
                (yf.balance_sheet->>'working_capital')::NUMERIC::BIGINT AS working_capital,
                (yf.balance_sheet->>'net_debt')::NUMERIC::BIGINT AS net_debt,
                (yf.balance_sheet->>'invested_capital')::NUMERIC::BIGINT AS invested_capital,
                (yf.balance_sheet->>'total_capitalization')::NUMERIC::BIGINT AS total_capitalization,

                -- Balance Sheet - SHARE DATA
                (yf.balance_sheet->>'ordinary_shares_number')::NUMERIC::BIGINT AS shares_issued,
                (yf.balance_sheet->>'treasury_shares_number')::NUMERIC::BIGINT AS treasury_shares,
                (yf.balance_sheet->>'retained_earnings')::NUMERIC::BIGINT AS retained_earnings,

                -- Cash Flow (extracted from JSONB)
                (yf.cash_flow->>'operating_cash_flow')::NUMERIC::BIGINT AS operating_cash_flow,
                (yf.cash_flow->>'free_cash_flow')::NUMERIC::BIGINT AS free_cash_flow,
                (yf.cash_flow->>'capital_expenditure')::NUMERIC::BIGINT AS capex,
                (yf.cash_flow->>'depreciation_and_amortization')::NUMERIC::BIGINT AS depreciation,
                (yf.cash_flow->>'cash_dividends_paid')::NUMERIC::BIGINT AS dividends_paid,
                (yf.cash_flow->>'repurchase_of_capital_stock')::NUMERIC::BIGINT AS share_repurchases,
                (yf.cash_flow->>'issuance_of_debt')::NUMERIC::BIGINT AS debt_issued,
                (yf.cash_flow->>'repayment_of_debt')::NUMERIC::BIGINT AS debt_repaid,
                (yf.cash_flow->>'net_income_from_continuing_operations')::NUMERIC::BIGINT AS cf_net_income,
                (yf.cash_flow->>'change_in_working_capital')::NUMERIC::BIGINT AS change_in_working_capital,
                (yf.cash_flow->>'change_in_inventory')::NUMERIC::BIGINT AS change_in_inventory,
                (yf.cash_flow->>'change_in_receivables')::NUMERIC::BIGINT AS change_in_receivables,
                (yf.cash_flow->>'change_in_payable')::NUMERIC::BIGINT AS change_in_payables,

                -- Calculated margins (for convenience)
                CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC::BIGINT > 0
                    THEN (yf.income_statement->>'gross_profit')::DECIMAL /
                         (yf.income_statement->>'total_revenue')::DECIMAL
                    ELSE NULL END AS gross_margin,
                CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC::BIGINT > 0
                    THEN (yf.income_statement->>'operating_income')::DECIMAL /
                         (yf.income_statement->>'total_revenue')::DECIMAL
                    ELSE NULL END AS operating_margin,
                CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC::BIGINT > 0
                    THEN (yf.income_statement->>'net_income')::DECIMAL /
                         (yf.income_statement->>'total_revenue')::DECIMAL
                    ELSE NULL END AS net_margin,
                CASE WHEN (yf.income_statement->>'total_revenue')::NUMERIC::BIGINT > 0
                    THEN (yf.income_statement->>'ebitda')::DECIMAL /
                         (yf.income_statement->>'total_revenue')::DECIMAL
                    ELSE NULL END AS ebitda_margin,

                -- Calculated asset metrics
                CASE WHEN (yf.balance_sheet->>'total_assets')::NUMERIC::BIGINT > 0
                    THEN COALESCE((yf.balance_sheet->>'net_tangible_assets')::DECIMAL,
                                  (yf.balance_sheet->>'total_assets')::DECIMAL -
                                  COALESCE((yf.balance_sheet->>'goodwill_and_other_intangible_assets')::DECIMAL, 0)) /
                         (yf.balance_sheet->>'total_assets')::DECIMAL
                    ELSE NULL END AS tangible_asset_pct,

                -- Raw JSONB for advanced queries
                yf.income_statement AS income_statement_json,
                yf.balance_sheet AS balance_sheet_json,
                yf.cash_flow AS cash_flow_json,

                yf.created_at,
                yf.updated_at

            FROM yahoo_financials yf;
        """)
        print("✅ Created view: bsd_v_yahoo_financials")

        # Create view for latest annual data per company
        await conn.execute("""
            CREATE OR REPLACE VIEW bsd_v_yahoo_latest_annual AS
            SELECT DISTINCT ON (symbol)
                *
            FROM bsd_v_yahoo_financials
            WHERE statement_type = 'annual'
              AND total_revenue IS NOT NULL
            ORDER BY symbol, period_date DESC;
        """)
        print("✅ Created view: bsd_v_yahoo_latest_annual")

        # Create view for latest quarterly data per company
        await conn.execute("""
            CREATE OR REPLACE VIEW bsd_v_yahoo_latest_quarterly AS
            SELECT DISTINCT ON (symbol)
                *
            FROM bsd_v_yahoo_financials
            WHERE statement_type = 'quarterly'
              AND total_revenue IS NOT NULL
            ORDER BY symbol, period_date DESC;
        """)
        print("✅ Created view: bsd_v_yahoo_latest_quarterly")

        # Create TTM (trailing twelve months) view by summing last 4 quarters
        await conn.execute("""
            CREATE OR REPLACE VIEW bsd_v_yahoo_ttm AS
            WITH ranked_quarters AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_date DESC) AS rn
                FROM bsd_v_yahoo_financials
                WHERE statement_type = 'quarterly'
                  AND total_revenue IS NOT NULL
            ),
            last_4_quarters AS (
                SELECT symbol
                FROM ranked_quarters
                WHERE rn <= 4
                GROUP BY symbol
                HAVING COUNT(*) = 4
            )
            SELECT
                rq.symbol,
                MAX(rq.period_date) AS period_date,
                'ttm' AS statement_type,
                MAX(rq.fiscal_year) AS fiscal_year,
                MAX(rq.publish_date) AS publish_date,
                MAX(rq.report_currency) AS report_currency,

                -- Sum flow metrics (income statement, cash flow)
                SUM(rq.total_revenue) AS total_revenue,
                SUM(rq.gross_profit) AS gross_profit,
                SUM(rq.operating_income) AS operating_income,
                SUM(rq.net_income) AS net_income,
                SUM(rq.ebitda) AS ebitda,
                SUM(rq.ebit) AS ebit,
                SUM(rq.interest_expense) AS interest_expense,
                SUM(rq.cost_of_revenue) AS cost_of_revenue,
                SUM(rq.sga_expense) AS sga_expense,
                SUM(rq.rd_expense) AS rd_expense,
                SUM(rq.operating_cash_flow) AS operating_cash_flow,
                SUM(rq.free_cash_flow) AS free_cash_flow,
                SUM(rq.capex) AS capex,
                SUM(rq.depreciation) AS depreciation,
                SUM(rq.dividends_paid) AS dividends_paid,
                SUM(rq.share_repurchases) AS share_repurchases,

                -- Latest point-in-time for balance sheet items
                (array_agg(rq.total_assets ORDER BY rq.period_date DESC))[1] AS total_assets,
                (array_agg(rq.total_liabilities ORDER BY rq.period_date DESC))[1] AS total_liabilities,
                (array_agg(rq.total_equity ORDER BY rq.period_date DESC))[1] AS total_equity,
                (array_agg(rq.total_debt ORDER BY rq.period_date DESC))[1] AS total_debt,
                (array_agg(rq.current_assets ORDER BY rq.period_date DESC))[1] AS current_assets,
                (array_agg(rq.cash_and_equivalents ORDER BY rq.period_date DESC))[1] AS cash_and_equivalents,
                (array_agg(rq.current_liabilities ORDER BY rq.period_date DESC))[1] AS current_liabilities,
                (array_agg(rq.inventory ORDER BY rq.period_date DESC))[1] AS inventory,
                (array_agg(rq.accounts_receivable ORDER BY rq.period_date DESC))[1] AS accounts_receivable,
                (array_agg(rq.shares_outstanding ORDER BY rq.period_date DESC))[1] AS shares_outstanding,

                -- Key asset play fields (latest)
                (array_agg(rq.net_ppe ORDER BY rq.period_date DESC))[1] AS net_ppe,
                (array_agg(rq.goodwill ORDER BY rq.period_date DESC))[1] AS goodwill,
                (array_agg(rq.other_intangible_assets ORDER BY rq.period_date DESC))[1] AS other_intangible_assets,
                (array_agg(rq.total_intangibles ORDER BY rq.period_date DESC))[1] AS total_intangibles,
                (array_agg(rq.net_tangible_assets ORDER BY rq.period_date DESC))[1] AS net_tangible_assets,
                (array_agg(rq.tangible_book_value ORDER BY rq.period_date DESC))[1] AS tangible_book_value,
                (array_agg(rq.working_capital ORDER BY rq.period_date DESC))[1] AS working_capital,
                (array_agg(rq.net_debt ORDER BY rq.period_date DESC))[1] AS net_debt,
                (array_agg(rq.invested_capital ORDER BY rq.period_date DESC))[1] AS invested_capital,

                -- TTM margins
                CASE WHEN SUM(rq.total_revenue) > 0
                    THEN SUM(rq.gross_profit)::DECIMAL / SUM(rq.total_revenue)
                    ELSE NULL END AS gross_margin,
                CASE WHEN SUM(rq.total_revenue) > 0
                    THEN SUM(rq.operating_income)::DECIMAL / SUM(rq.total_revenue)
                    ELSE NULL END AS operating_margin,
                CASE WHEN SUM(rq.total_revenue) > 0
                    THEN SUM(rq.net_income)::DECIMAL / SUM(rq.total_revenue)
                    ELSE NULL END AS net_margin,
                CASE WHEN SUM(rq.total_revenue) > 0
                    THEN SUM(rq.ebitda)::DECIMAL / SUM(rq.total_revenue)
                    ELSE NULL END AS ebitda_margin,

                -- Tangible asset percentage
                CASE WHEN (array_agg(rq.total_assets ORDER BY rq.period_date DESC))[1] > 0
                    THEN COALESCE(
                        (array_agg(rq.net_tangible_assets ORDER BY rq.period_date DESC))[1]::DECIMAL,
                        (array_agg(rq.total_assets ORDER BY rq.period_date DESC))[1]::DECIMAL -
                        COALESCE((array_agg(rq.total_intangibles ORDER BY rq.period_date DESC))[1]::DECIMAL, 0)
                    ) / (array_agg(rq.total_assets ORDER BY rq.period_date DESC))[1]::DECIMAL
                    ELSE NULL END AS tangible_asset_pct,

                4 AS quarters_count,
                TRUE AS is_ttm

            FROM ranked_quarters rq
            INNER JOIN last_4_quarters l4q ON rq.symbol = l4q.symbol
            WHERE rq.rn <= 4
            GROUP BY rq.symbol;
        """)
        print("✅ Created view: bsd_v_yahoo_ttm")

        print("\n✅ All yahoo_financials views created successfully!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
