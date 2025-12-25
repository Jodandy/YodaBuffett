#!/usr/bin/env python3
"""
Direct Publish Date Setter

Direct approach: Use the publish dates from documents and infer
the financial period, then set them directly on financial tables.
"""

import asyncio
import asyncpg
from datetime import datetime, date

async def analyze_current_situation():
    """Analyze current publish date situation"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('🔍 CURRENT SITUATION ANALYSIS')
    print('=' * 70)
    
    # Check documents
    docs_stats = await conn.fetchrow("""
        SELECT 
            COUNT(*) as total_docs,
            COUNT(CASE WHEN document_type = 'annual_report' THEN 1 END) as annual_docs,
            COUNT(CASE WHEN document_type = 'quarterly_report' THEN 1 END) as quarterly_docs,
            MIN(publish_date) as earliest_date,
            MAX(publish_date) as latest_date
        FROM nordic_documents
        WHERE document_type IN ('annual_report', 'quarterly_report')
    """)
    
    print(f'📄 NORDIC DOCUMENTS:')
    print(f'   Total reports: {docs_stats["total_docs"]:,}')
    print(f'   Annual reports: {docs_stats["annual_docs"]:,}')
    print(f'   Quarterly reports: {docs_stats["quarterly_docs"]:,}')
    print(f'   Date range: {docs_stats["earliest_date"]} to {docs_stats["latest_date"]}')
    
    # Check financial statements
    fs_stats = await conn.fetchrow("""
        SELECT 
            COUNT(*) as total_fs,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(publish_date) as with_publish_date,
            MIN(period_date) as earliest_period,
            MAX(period_date) as latest_period
        FROM financial_statements
    """)
    
    print(f'\n💰 FINANCIAL STATEMENTS:')
    print(f'   Total statements: {fs_stats["total_fs"]:,}')
    print(f'   Unique symbols: {fs_stats["unique_symbols"]:,}')
    print(f'   With publish dates: {fs_stats["with_publish_date"]:,} ({fs_stats["with_publish_date"]/fs_stats["total_fs"]*100:.1f}%)')
    print(f'   Period range: {fs_stats["earliest_period"]} to {fs_stats["latest_period"]}')
    
    # Check symbol overlap potential
    symbols_query = """
    SELECT 
        COUNT(DISTINCT symbol) as fs_symbols
    FROM financial_statements
    """
    
    symbols_result = await conn.fetchrow(symbols_query)
    print(f'   Unique symbols in financial_statements: {symbols_result["fs_symbols"]}')
    
    await conn.close()
    return fs_stats

async def set_approximate_publish_dates():
    """Set approximate publish dates based on period_date + typical reporting lag"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('\n📅 SETTING APPROXIMATE PUBLISH DATES')
    print('=' * 70)
    
    # Set publish dates based on period_date + typical reporting lag
    # Annual reports: typically published 2-4 months after year-end
    # Quarterly reports: typically published 1-2 months after quarter-end
    
    print('Setting annual report publish dates (period_date + 90 days)...')
    annual_update = await conn.execute("""
        UPDATE financial_statements 
        SET publish_date = period_date + INTERVAL '90 days'
        WHERE fiscal_quarter IS NULL 
        AND publish_date IS NULL
        AND period_date IS NOT NULL
    """)
    
    annual_count = int(annual_update.split()[-1]) if annual_update.startswith('UPDATE') else 0
    
    print('Setting quarterly report publish dates (period_date + 45 days)...')
    quarterly_update = await conn.execute("""
        UPDATE financial_statements 
        SET publish_date = period_date + INTERVAL '45 days'
        WHERE fiscal_quarter IS NOT NULL 
        AND publish_date IS NULL
        AND period_date IS NOT NULL
    """)
    
    quarterly_count = int(quarterly_update.split()[-1]) if quarterly_update.startswith('UPDATE') else 0
    
    print(f'✅ Updated {annual_count:,} annual and {quarterly_count:,} quarterly statements')
    
    # Update balance_sheet_data and cash_flow_data if they have the columns
    try:
        print('Updating balance_sheet_data...')
        bs_annual = await conn.execute("""
            UPDATE balance_sheet_data 
            SET publish_date = period_date + INTERVAL '90 days'
            WHERE fiscal_quarter IS NULL 
            AND publish_date IS NULL
            AND period_date IS NOT NULL
        """)
        
        bs_quarterly = await conn.execute("""
            UPDATE balance_sheet_data 
            SET publish_date = period_date + INTERVAL '45 days'
            WHERE fiscal_quarter IS NOT NULL 
            AND publish_date IS NULL
            AND period_date IS NOT NULL
        """)
        
        bs_annual_count = int(bs_annual.split()[-1]) if bs_annual.startswith('UPDATE') else 0
        bs_quarterly_count = int(bs_quarterly.split()[-1]) if bs_quarterly.startswith('UPDATE') else 0
        
        print(f'✅ Updated {bs_annual_count:,} annual and {bs_quarterly_count:,} quarterly balance sheets')
    except Exception as e:
        print(f'⚠️ Could not update balance_sheet_data: {e}')
    
    try:
        print('Updating cash_flow_data...')
        cf_annual = await conn.execute("""
            UPDATE cash_flow_data 
            SET publish_date = period_date + INTERVAL '90 days'
            WHERE fiscal_quarter IS NULL 
            AND publish_date IS NULL
            AND period_date IS NOT NULL
        """)
        
        cf_quarterly = await conn.execute("""
            UPDATE cash_flow_data 
            SET publish_date = period_date + INTERVAL '45 days'
            WHERE fiscal_quarter IS NOT NULL 
            AND publish_date IS NULL
            AND period_date IS NOT NULL
        """)
        
        cf_annual_count = int(cf_annual.split()[-1]) if cf_annual.startswith('UPDATE') else 0
        cf_quarterly_count = int(cf_quarterly.split()[-1]) if cf_quarterly.startswith('UPDATE') else 0
        
        print(f'✅ Updated {cf_annual_count:,} annual and {cf_quarterly_count:,} quarterly cash flows')
    except Exception as e:
        print(f'⚠️ Could not update cash_flow_data: {e}')
    
    # Check final coverage
    print(f'\n📊 FINAL COVERAGE:')
    
    coverage_check = await conn.fetchrow("""
        SELECT 
            COUNT(*) as total,
            COUNT(publish_date) as with_dates,
            COUNT(publish_date)::float / COUNT(*) * 100 as coverage_pct
        FROM financial_statements
    """)
    
    print(f'   Financial statements: {coverage_check["with_dates"]:,}/{coverage_check["total"]:,} ({coverage_check["coverage_pct"]:.1f}%)')
    
    # Sample results
    sample_query = """
    SELECT symbol, fiscal_year, fiscal_quarter, period_date, publish_date, total_revenue/1e6 as revenue_m
    FROM financial_statements
    WHERE publish_date IS NOT NULL
    ORDER BY publish_date DESC
    LIMIT 15
    """
    
    samples = await conn.fetch(sample_query)
    
    print(f'\n📋 SAMPLE RESULTS WITH PUBLISH DATES:')
    for row in samples:
        quarter = f"Q{row['fiscal_quarter']}" if row['fiscal_quarter'] else "Annual"
        revenue = f"${row['revenue_m']:.0f}M" if row['revenue_m'] else "N/A"
        lag_days = (row['publish_date'] - row['period_date']).days if row['publish_date'] and row['period_date'] else 0
        print(f'   {row["symbol"]:8} | {row["fiscal_year"]} {quarter:7} | {row["period_date"]} → {row["publish_date"]} (+{lag_days}d) | {revenue:>8}')
    
    await conn.close()
    
    return {
        'annual_updated': annual_count,
        'quarterly_updated': quarterly_count,
        'total_updated': annual_count + quarterly_count
    }

async def main():
    """Main function"""
    
    print('📅 DIRECT PUBLISH DATE SETTER')
    print('=' * 70)
    
    # Analyze current situation
    await analyze_current_situation()
    
    # Set approximate publish dates
    stats = await set_approximate_publish_dates()
    
    print(f'\n🎯 PUBLISH DATE SETTING COMPLETE')
    print(f'   Set publish dates on {stats["total_updated"]:,} financial statements')
    print(f'   Using period_date + reporting lag approximation')
    print(f'   Annual reports: +90 days | Quarterly reports: +45 days')

if __name__ == "__main__":
    asyncio.run(main())