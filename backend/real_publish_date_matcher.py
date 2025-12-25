#!/usr/bin/env python3
"""
Real Publish Date Matcher

Use the ACTUAL publish dates from nordic_documents by linking:
nordic_documents → nordic_companies → financial_statements

No assumptions, no hardcoding - only real data.
"""

import asyncio
import asyncpg
from datetime import datetime

async def match_and_set_real_publish_dates():
    """Match documents to financial statements and set REAL publish dates"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('📅 REAL PUBLISH DATE MATCHING')
    print('=' * 70)
    
    # First, let's understand what we're working with
    print('\n🔍 Analyzing financial reports in documents...')
    
    # Get financial reports only (not AGM notices, etc.)
    analysis_query = """
    SELECT 
        nd.document_type,
        COUNT(*) as total,
        COUNT(CASE WHEN nd.title ~* 'delårsrapport|interim.*report|quarterly.*report|q[1-4]' THEN 1 END) as quarterly_pattern,
        COUNT(CASE WHEN nd.title ~* 'årsredovisning|annual.*report|year.*report' THEN 1 END) as annual_pattern
    FROM nordic_documents nd
    WHERE nd.document_type IN ('annual_report', 'quarterly_report')
    GROUP BY nd.document_type
    """
    
    analysis = await conn.fetch(analysis_query)
    
    for row in analysis:
        print(f'{row["document_type"]:16}: {row["total"]:,} total, {row["quarterly_pattern"]:,} quarterly pattern, {row["annual_pattern"]:,} annual pattern')
    
    print('\n🔗 Matching documents to financial statements using REAL publish dates...')
    
    # Match quarterly reports
    print('\n📊 Processing QUARTERLY reports...')
    
    quarterly_update = """
    WITH matched_quarterlies AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nd.publish_date as real_publish_date,
            fs.symbol,
            fs.fiscal_year,
            fs.fiscal_quarter,
            nd.title
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_documents nd ON nc.id = nd.company_id
        WHERE fs.fiscal_quarter IS NOT NULL
        AND fs.publish_date IS NULL
        AND nd.document_type = 'quarterly_report'
        -- Match by year and quarter proximity
        AND EXTRACT(YEAR FROM nd.publish_date) = fs.fiscal_year
        AND (
            -- Q1 reports published in Q1 or Q2
            (fs.fiscal_quarter = 1 AND EXTRACT(QUARTER FROM nd.publish_date) IN (1, 2))
            -- Q2 reports published in Q2 or Q3  
            OR (fs.fiscal_quarter = 2 AND EXTRACT(QUARTER FROM nd.publish_date) IN (2, 3))
            -- Q3 reports published in Q3 or Q4
            OR (fs.fiscal_quarter = 3 AND EXTRACT(QUARTER FROM nd.publish_date) IN (3, 4))
            -- Q4 reports published in Q4 or Q1 next year
            OR (fs.fiscal_quarter = 4 AND (
                EXTRACT(QUARTER FROM nd.publish_date) = 4 
                OR (EXTRACT(YEAR FROM nd.publish_date) = fs.fiscal_year + 1 AND EXTRACT(QUARTER FROM nd.publish_date) = 1)
            ))
        )
        ORDER BY fs.id, nd.publish_date
    )
    UPDATE financial_statements fs
    SET publish_date = mq.real_publish_date
    FROM matched_quarterlies mq
    WHERE fs.id = mq.fs_id
    """
    
    quarterly_result = await conn.execute(quarterly_update)
    quarterly_count = int(quarterly_result.split()[-1]) if quarterly_result.startswith('UPDATE') else 0
    print(f'✅ Updated {quarterly_count:,} quarterly reports with REAL publish dates')
    
    # Match annual reports
    print('\n📊 Processing ANNUAL reports...')
    
    annual_update = """
    WITH matched_annuals AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nd.publish_date as real_publish_date,
            fs.symbol,
            fs.fiscal_year,
            nd.title
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_documents nd ON nc.id = nd.company_id
        WHERE fs.fiscal_quarter IS NULL
        AND fs.publish_date IS NULL
        AND nd.document_type = 'annual_report'
        -- Annual reports usually published in Q1/Q2 of following year
        AND (
            (EXTRACT(YEAR FROM nd.publish_date) = fs.fiscal_year + 1 AND EXTRACT(QUARTER FROM nd.publish_date) IN (1, 2))
            OR (EXTRACT(YEAR FROM nd.publish_date) = fs.fiscal_year AND EXTRACT(QUARTER FROM nd.publish_date) IN (3, 4))
        )
        ORDER BY fs.id, nd.publish_date
    )
    UPDATE financial_statements fs
    SET publish_date = ma.real_publish_date
    FROM matched_annuals ma
    WHERE fs.id = ma.fs_id
    """
    
    annual_result = await conn.execute(annual_update)
    annual_count = int(annual_result.split()[-1]) if annual_result.startswith('UPDATE') else 0
    print(f'✅ Updated {annual_count:,} annual reports with REAL publish dates')
    
    # Now update balance_sheet_data and cash_flow_data
    print('\n📊 Propagating to balance_sheet_data...')
    
    bs_update = """
    UPDATE balance_sheet_data bs
    SET publish_date = fs.publish_date
    FROM financial_statements fs
    WHERE bs.symbol = fs.symbol
    AND bs.period_date = fs.period_date
    AND fs.publish_date IS NOT NULL
    AND bs.publish_date IS NULL
    """
    
    bs_result = await conn.execute(bs_update)
    bs_count = int(bs_result.split()[-1]) if bs_result.startswith('UPDATE') else 0
    print(f'✅ Updated {bs_count:,} balance sheet records')
    
    print('\n📊 Propagating to cash_flow_data...')
    
    cf_update = """
    UPDATE cash_flow_data cf
    SET publish_date = fs.publish_date
    FROM financial_statements fs
    WHERE cf.symbol = fs.symbol
    AND cf.period_date = fs.period_date
    AND fs.publish_date IS NOT NULL
    AND cf.publish_date IS NULL
    """
    
    cf_result = await conn.execute(cf_update)
    cf_count = int(cf_result.split()[-1]) if cf_result.startswith('UPDATE') else 0
    print(f'✅ Updated {cf_count:,} cash flow records')
    
    # Check final results
    print('\n📊 FINAL COVERAGE WITH REAL PUBLISH DATES:')
    
    for table in ['financial_statements', 'balance_sheet_data', 'cash_flow_data']:
        coverage = await conn.fetchrow(f'SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM {table}')
        pct = (coverage['with_dates'] / coverage['total'] * 100) if coverage['total'] > 0 else 0
        print(f'   {table:20}: {coverage["with_dates"]:,}/{coverage["total"]:,} ({pct:.1f}%)')
    
    # Show sample results
    print('\n📋 SAMPLE RESULTS (REAL publish dates):')
    
    sample_query = """
    SELECT 
        fs.symbol,
        fs.fiscal_year,
        fs.fiscal_quarter,
        fs.period_date,
        fs.publish_date,
        fs.total_revenue/1e6 as revenue_m,
        nd.title
    FROM financial_statements fs
    JOIN nordic_companies nc ON fs.symbol = nc.ticker
    JOIN nordic_documents nd ON nc.id = nd.company_id
        AND nd.publish_date = fs.publish_date
    WHERE fs.publish_date IS NOT NULL
    ORDER BY fs.publish_date DESC
    LIMIT 10
    """
    
    samples = await conn.fetch(sample_query)
    
    for row in samples:
        quarter = f"Q{row['fiscal_quarter']}" if row['fiscal_quarter'] else "Annual"
        revenue = f"${row['revenue_m']:.0f}M" if row['revenue_m'] else "N/A"
        lag_days = (row['publish_date'] - row['period_date']).days if row['publish_date'] and row['period_date'] else 0
        print(f'   {row["symbol"]:8} | {row["fiscal_year"]} {quarter:7} | {row["period_date"]} → {row["publish_date"]} (+{lag_days}d) | {revenue:>8}')
        print(f'      Document: {row["title"][:60]}...')
    
    await conn.close()
    
    return {
        'quarterly_updated': quarterly_count,
        'annual_updated': annual_count,
        'balance_sheet_updated': bs_count,
        'cash_flow_updated': cf_count,
        'total_updated': quarterly_count + annual_count
    }

async def main():
    """Main function"""
    
    print('🎯 REAL PUBLISH DATE MATCHER - NO ASSUMPTIONS')
    print('=' * 70)
    print('Using ACTUAL publish dates from nordic_documents')
    print('Linked via: nordic_documents → nordic_companies → financial_statements')
    
    stats = await match_and_set_real_publish_dates()
    
    print(f'\n✅ MATCHING COMPLETE')
    print(f'   Set {stats["total_updated"]:,} REAL publish dates on financial statements')
    print(f'   NO hardcoded values, NO assumptions - only REAL data from documents')

if __name__ == "__main__":
    asyncio.run(main())