"""
Create Financial Metrics Table
Database schema for storing extracted financial data from PDFs
"""
import asyncio
import asyncpg
import json
from datetime import datetime
from typing import Optional


CREATE_FINANCIAL_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS financial_metrics (
    id SERIAL PRIMARY KEY,
    
    -- Document reference
    document_id UUID REFERENCES nordic_documents(id),
    company_name TEXT NOT NULL,
    report_period TEXT, -- e.g., "Q3 2024", "2024", "H1 2024"
    report_type TEXT,   -- e.g., "quarterly", "annual", "interim"
    fiscal_year INTEGER,
    report_date DATE,
    
    -- Revenue metrics (in local currency) - dual extraction
    revenue_reported DECIMAL(15,2),         -- Statutory revenue figure
    revenue_adjusted DECIMAL(15,2),         -- Adjusted/normalized revenue
    revenue_adjustments TEXT,               -- What adjustments were made
    revenue_currency TEXT DEFAULT 'SEK',
    revenue_growth_pct DECIMAL(5,2),        -- Year-over-year growth percentage
    revenue_growth_qoq_pct DECIMAL(5,2),    -- Quarter-over-quarter growth
    
    -- Profitability metrics - selective dual extraction
    gross_profit DECIMAL(15,2),
    gross_margin_pct DECIMAL(5,2),
    cost_of_goods_sold DECIMAL(15,2),       -- COGS
    operating_expenses DECIMAL(15,2),       -- OpEx
    
    -- Operating profit (EBIT) - dual extraction
    operating_profit_reported DECIMAL(15,2),
    operating_profit_adjusted DECIMAL(15,2),
    operating_adjustments TEXT,
    operating_margin_pct DECIMAL(5,2),
    
    -- EBITDA - dual extraction  
    ebitda_reported DECIMAL(15,2),
    ebitda_adjusted DECIMAL(15,2),
    ebitda_adjustments TEXT,
    ebitda_margin_pct DECIMAL(5,2),
    
    depreciation_amortization DECIMAL(15,2),
    interest_expense DECIMAL(15,2),
    tax_expense DECIMAL(15,2),
    other_income DECIMAL(15,2),
    
    -- Net income - dual extraction
    net_income_reported DECIMAL(15,2),
    net_income_adjusted DECIMAL(15,2),
    net_income_adjustments TEXT,
    net_margin_pct DECIMAL(5,2),
    
    -- Cash flow metrics
    operating_cash_flow DECIMAL(15,2),
    investing_cash_flow DECIMAL(15,2),
    financing_cash_flow DECIMAL(15,2),
    free_cash_flow DECIMAL(15,2),
    capex DECIMAL(15,2),                    -- Capital expenditures
    dividends_paid DECIMAL(15,2),
    
    -- Balance sheet metrics
    total_assets DECIMAL(15,2),
    current_assets DECIMAL(15,2),
    non_current_assets DECIMAL(15,2),
    total_equity DECIMAL(15,2),
    retained_earnings DECIMAL(15,2),
    total_liabilities DECIMAL(15,2),
    current_liabilities DECIMAL(15,2),
    non_current_liabilities DECIMAL(15,2),
    total_debt DECIMAL(15,2),
    cash_and_equivalents DECIMAL(15,2),
    inventory DECIMAL(15,2),
    accounts_receivable DECIMAL(15,2),
    accounts_payable DECIMAL(15,2),
    working_capital DECIMAL(15,2),
    
    -- Key ratios
    debt_to_equity DECIMAL(8,4),
    current_ratio DECIMAL(8,4),
    quick_ratio DECIMAL(8,4),               -- Acid test ratio
    inventory_turnover DECIMAL(8,4),
    asset_turnover DECIMAL(8,4),
    interest_coverage DECIMAL(8,4),         -- EBIT / Interest Expense
    return_on_equity_pct DECIMAL(5,2),
    return_on_assets_pct DECIMAL(5,2),
    
    -- Per share metrics - dual extraction for EPS
    earnings_per_share_reported DECIMAL(10,4),   -- 2.47 (statutory)
    earnings_per_share_adjusted DECIMAL(10,4),   -- 3.26 (excluding items)
    eps_adjustments TEXT,                         -- What was excluded
    book_value_per_share DECIMAL(10,4),
    dividend_per_share DECIMAL(10,4),
    shares_outstanding BIGINT,
    
    -- Nordic-specific ratios
    payout_ratio DECIMAL(5,2),              -- Dividends / Net Income
    dividend_yield_pct DECIMAL(5,2),        -- If stock price available
    
    -- Operational metrics (company-specific)
    operational_metrics JSONB,              -- Store additional metrics as JSON
    
    -- Extraction metadata
    extraction_method TEXT DEFAULT 'local_llm',   -- 'local_llm', 'manual', 'api'
    extraction_confidence DECIMAL(3,2),           -- 0.0 to 1.0
    extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_used TEXT,                              -- e.g., 'llama3:latest'
    
    -- Data quality flags
    has_revenue BOOLEAN DEFAULT FALSE,
    has_profitability BOOLEAN DEFAULT FALSE,
    has_cash_flow BOOLEAN DEFAULT FALSE,
    has_balance_sheet BOOLEAN DEFAULT FALSE,
    data_quality_score DECIMAL(3,2),             -- Overall quality score 0-1
    
    -- Notes and warnings
    extraction_notes TEXT,
    data_warnings TEXT[],                         -- Array of warning messages
    
    UNIQUE(document_id, report_period)           -- One extraction per document per period
);

-- Create indexes separately
CREATE INDEX IF NOT EXISTS idx_financial_metrics_company ON financial_metrics(company_name);
CREATE INDEX IF NOT EXISTS idx_financial_metrics_period ON financial_metrics(report_period);
CREATE INDEX IF NOT EXISTS idx_financial_metrics_year ON financial_metrics(fiscal_year);
CREATE INDEX IF NOT EXISTS idx_financial_metrics_extraction_date ON financial_metrics(extraction_date);
"""

# Add comments for better documentation
ADD_TABLE_COMMENTS = """
COMMENT ON TABLE financial_metrics IS 'Structured financial data extracted from company reports using AI with dual extraction';

COMMENT ON COLUMN financial_metrics.revenue_reported IS 'Total revenue/net sales in local currency (reported/statutory figure)';
COMMENT ON COLUMN financial_metrics.revenue_adjusted IS 'Total revenue/net sales adjusted for one-time items';
COMMENT ON COLUMN financial_metrics.ebitda_reported IS 'EBITDA reported figure';
COMMENT ON COLUMN financial_metrics.ebitda_adjusted IS 'EBITDA adjusted for one-time items';
COMMENT ON COLUMN financial_metrics.extraction_confidence IS 'AI confidence score for extraction accuracy (0.0-1.0)';
COMMENT ON COLUMN financial_metrics.operational_metrics IS 'Company-specific metrics stored as JSON (e.g., production volumes, customer counts)';
COMMENT ON COLUMN financial_metrics.data_quality_score IS 'Overall data quality assessment (0.0-1.0)';
"""


async def create_financial_metrics_table():
    """Create the financial metrics table"""
    
    # Database connection - use shared config
    from shared.config import settings
    DATABASE_URL = settings.database_url
    
    try:
        # Connect to database
        conn = await asyncpg.connect(DATABASE_URL)
        
        print("🗄️  Creating financial_metrics table...")
        
        # Create table
        await conn.execute(CREATE_FINANCIAL_METRICS_TABLE)
        
        # Add comments
        await conn.execute(ADD_TABLE_COMMENTS)
        
        # Verify creation
        result = await conn.fetch("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'financial_metrics'
            ORDER BY ordinal_position;
        """)
        
        print(f"✅ Table created successfully with {len(result)} columns:")
        
        # Show key columns (updated for dual extraction schema)
        key_columns = [
            'company_name', 'report_period', 'revenue_reported', 'ebitda_reported', 
            'net_income_reported', 'extraction_confidence', 'extraction_date'
        ]
        
        for row in result:
            if row['column_name'] in key_columns:
                nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                print(f"  • {row['column_name']}: {row['data_type']} {nullable}")
        
        print(f"  ... and {len(result) - len(key_columns)} more columns")
        
        await conn.close()
        
        print("\n🎯 Ready for financial extraction!")
        print("Next step: Create extraction service to populate this table")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return False


async def show_table_info():
    """Show information about the created table"""
    from shared.config import settings
    DATABASE_URL = settings.database_url
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Check if table exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'financial_metrics'
            );
        """)
        
        if exists:
            print("✅ financial_metrics table exists")
            
            # Show sample structure
            print("\n📊 Table Structure Overview:")
            print("Revenue Metrics: revenue, revenue_growth_pct, revenue_currency")
            print("Profitability: gross_profit, ebitda, net_income + margins")
            print("Cash Flow: operating_cash_flow, free_cash_flow, capex")
            print("Balance Sheet: total_assets, total_equity, total_debt")
            print("Ratios: debt_to_equity, current_ratio, ROE, ROA")
            print("Metadata: extraction_confidence, model_used, data_quality_score")
            
        else:
            print("❌ financial_metrics table does not exist")
            
        await conn.close()
        
    except Exception as e:
        print(f"❌ Error checking table: {e}")


if __name__ == "__main__":
    print("🏗️  Financial Metrics Table Setup")
    print("=" * 50)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Create table
        success = loop.run_until_complete(create_financial_metrics_table())
        
        if success:
            # Show info
            loop.run_until_complete(show_table_info())
            
    finally:
        loop.close()