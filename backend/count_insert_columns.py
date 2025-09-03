#!/usr/bin/env python3
"""
Debug column count mismatch in INSERT statement
"""
import asyncio
import asyncpg
from shared.config import settings

async def count_columns():
    """Count columns in database vs INSERT statement"""
    
    print("🔍 COLUMN COUNT DEBUGGING")
    print("=" * 50)
    
    # Connect to database
    conn = await asyncpg.connect(settings.database_url)
    
    # Get all columns from database (excluding 'id' which is auto-generated)
    db_columns = await conn.fetch("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = 'financial_metrics'
        AND column_name != 'id'
        ORDER BY ordinal_position;
    """)
    
    print(f"✅ Database columns (excluding 'id'): {len(db_columns)}")
    
    # The INSERT columns from our fixed service
    insert_columns = [
        "document_id", "company_name", "report_period", "report_type", "fiscal_year", "report_date",
        "revenue_reported", "revenue_adjusted", "revenue_adjustments", "revenue_currency", "revenue_growth_pct", "revenue_growth_qoq_pct",
        "gross_profit", "gross_margin_pct", "cost_of_goods_sold", "operating_expenses",
        "operating_profit_reported", "operating_profit_adjusted", "operating_adjustments", "operating_margin_pct",
        "ebitda_reported", "ebitda_adjusted", "ebitda_adjustments", "ebitda_margin_pct",
        "depreciation_amortization", "interest_expense", "tax_expense", "other_income",
        "net_income_reported", "net_income_adjusted", "net_income_adjustments", "net_margin_pct",
        "operating_cash_flow", "investing_cash_flow", "financing_cash_flow", "free_cash_flow", "capex", "dividends_paid",
        "total_assets", "current_assets", "non_current_assets", "total_equity", "retained_earnings",
        "total_liabilities", "current_liabilities", "non_current_liabilities", "total_debt", "cash_and_equivalents",
        "inventory", "accounts_receivable", "accounts_payable", "working_capital",
        "debt_to_equity", "current_ratio", "quick_ratio", "inventory_turnover", "asset_turnover", "interest_coverage",
        "return_on_equity_pct", "return_on_assets_pct",
        "earnings_per_share_reported", "earnings_per_share_adjusted", "eps_adjustments", "book_value_per_share",
        "dividend_per_share", "shares_outstanding", "payout_ratio", "dividend_yield_pct",
        "operational_metrics", "extraction_method", "extraction_confidence", "extraction_date", "model_used",
        "has_revenue", "has_profitability", "has_cash_flow", "has_balance_sheet", "data_quality_score",
        "extraction_notes", "data_warnings"
    ]
    
    print(f"✅ INSERT statement columns: {len(insert_columns)}")
    
    # Find missing and extra columns
    db_column_names = [col['column_name'] for col in db_columns]
    
    missing_from_insert = [col for col in db_column_names if col not in insert_columns]
    extra_in_insert = [col for col in insert_columns if col not in db_column_names]
    
    if missing_from_insert:
        print(f"\n❌ Missing from INSERT ({len(missing_from_insert)} columns):")
        for col in missing_from_insert:
            print(f"  - {col}")
    
    if extra_in_insert:
        print(f"\n❌ Extra in INSERT ({len(extra_in_insert)} columns):")
        for col in extra_in_insert:
            print(f"  + {col}")
    
    if not missing_from_insert and not extra_in_insert:
        print("✅ Column counts match perfectly!")
    
    # Show column-by-column comparison
    print(f"\n📊 COLUMN COMPARISON:")
    print("DB Column".ljust(30) + " | " + "INSERT Column".ljust(30) + " | Match")
    print("-" * 80)
    
    for i, db_col in enumerate(db_column_names):
        insert_col = insert_columns[i] if i < len(insert_columns) else "MISSING"
        match = "✅" if db_col == insert_col else "❌"
        print(db_col.ljust(30) + " | " + insert_col.ljust(30) + " | " + match)
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(count_columns())