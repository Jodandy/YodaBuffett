#!/usr/bin/env python3
"""
Corrected Earnings Calendar Publish Date Matcher

FIXED the timing logic:
- Annual reports for year N → published Jan-Mar of year N+1
- Q1 reports → published Apr-May of same year  
- Q2 reports → published Jul-Aug of same year
- Q3 reports → published Oct-Nov of same year
- Q4 reports → published Jan-Feb of following year
"""

import asyncio
import asyncpg
from datetime import datetime

async def match_earnings_to_financials_corrected():
    """Match earnings calendar events to financial statements with CORRECT timing"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('📅 CORRECTED EARNINGS CALENDAR MATCHING')
    print('=' * 70)
    
    # ANNUAL REPORTS: Published Jan-Mar of year N+1 for fiscal year N
    print('\n📊 Processing ANNUAL earnings events (CORRECTED)...')
    
    annual_update = """
    WITH annual_earnings AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nce.event_date as publish_date,
            fs.symbol,
            fs.fiscal_year,
            nce.title
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_calendar_events nce ON nc.id = nce.company_id
        WHERE fs.fiscal_quarter IS NULL  -- Annual reports
        AND fs.publish_date IS NULL
        AND nce.event_type = 'earnings'
        -- CORRECTED: Annual for year N published in year N+1 (Jan-Mar)
        AND EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year + 1 
        AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 1 AND 3
        ORDER BY fs.id, nce.event_date
    )
    UPDATE financial_statements fs
    SET publish_date = ae.publish_date
    FROM annual_earnings ae
    WHERE fs.id = ae.fs_id
    """
    
    annual_result = await conn.execute(annual_update)
    annual_count = int(annual_result.split()[-1]) if annual_result.startswith('UPDATE') else 0
    print(f'✅ Updated {annual_count:,} annual reports (Year N → Published Jan-Mar Year N+1)')
    
    # Q1 REPORTS: Published Apr-May of same year
    print('\n📊 Processing Q1 earnings events...')
    
    q1_update = """
    WITH q1_earnings AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nce.event_date as publish_date,
            fs.symbol,
            fs.fiscal_year,
            fs.fiscal_quarter
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_calendar_events nce ON nc.id = nce.company_id
        WHERE fs.fiscal_quarter = 1
        AND fs.publish_date IS NULL
        AND nce.event_type = 'earnings'
        AND EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year
        AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 4 AND 5  -- Apr-May
        ORDER BY fs.id, nce.event_date
    )
    UPDATE financial_statements fs
    SET publish_date = q1e.publish_date
    FROM q1_earnings q1e
    WHERE fs.id = q1e.fs_id
    """
    
    q1_result = await conn.execute(q1_update)
    q1_count = int(q1_result.split()[-1]) if q1_result.startswith('UPDATE') else 0
    print(f'✅ Updated {q1_count:,} Q1 reports (Apr-May same year)')
    
    # Q2 REPORTS: Published Jul-Aug of same year
    print('\n📊 Processing Q2 earnings events...')
    
    q2_update = """
    WITH q2_earnings AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nce.event_date as publish_date,
            fs.symbol,
            fs.fiscal_year,
            fs.fiscal_quarter
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_calendar_events nce ON nc.id = nce.company_id
        WHERE fs.fiscal_quarter = 2
        AND fs.publish_date IS NULL
        AND nce.event_type = 'earnings'
        AND EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year
        AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 7 AND 8  -- Jul-Aug
        ORDER BY fs.id, nce.event_date
    )
    UPDATE financial_statements fs
    SET publish_date = q2e.publish_date
    FROM q2_earnings q2e
    WHERE fs.id = q2e.fs_id
    """
    
    q2_result = await conn.execute(q2_update)
    q2_count = int(q2_result.split()[-1]) if q2_result.startswith('UPDATE') else 0
    print(f'✅ Updated {q2_count:,} Q2 reports (Jul-Aug same year)')
    
    # Q3 REPORTS: Published Oct-Nov of same year
    print('\n📊 Processing Q3 earnings events...')
    
    q3_update = """
    WITH q3_earnings AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nce.event_date as publish_date,
            fs.symbol,
            fs.fiscal_year,
            fs.fiscal_quarter
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_calendar_events nce ON nc.id = nce.company_id
        WHERE fs.fiscal_quarter = 3
        AND fs.publish_date IS NULL
        AND nce.event_type = 'earnings'
        AND EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year
        AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 10 AND 11  -- Oct-Nov
        ORDER BY fs.id, nce.event_date
    )
    UPDATE financial_statements fs
    SET publish_date = q3e.publish_date
    FROM q3_earnings q3e
    WHERE fs.id = q3e.fs_id
    """
    
    q3_result = await conn.execute(q3_update)
    q3_count = int(q3_result.split()[-1]) if q3_result.startswith('UPDATE') else 0
    print(f'✅ Updated {q3_count:,} Q3 reports (Oct-Nov same year)')
    
    # Q4 REPORTS: Published Jan-Feb of following year
    print('\n📊 Processing Q4 earnings events...')
    
    q4_update = """
    WITH q4_earnings AS (
        SELECT DISTINCT ON (fs.id)
            fs.id as fs_id,
            nce.event_date as publish_date,
            fs.symbol,
            fs.fiscal_year,
            fs.fiscal_quarter
        FROM financial_statements fs
        JOIN nordic_companies nc ON fs.symbol = nc.ticker
        JOIN nordic_calendar_events nce ON nc.id = nce.company_id
        WHERE fs.fiscal_quarter = 4
        AND fs.publish_date IS NULL
        AND nce.event_type = 'earnings'
        AND EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year + 1
        AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 1 AND 2  -- Jan-Feb next year
        ORDER BY fs.id, nce.event_date
    )
    UPDATE financial_statements fs
    SET publish_date = q4e.publish_date
    FROM q4_earnings q4e
    WHERE fs.id = q4e.fs_id
    """
    
    q4_result = await conn.execute(q4_update)
    q4_count = int(q4_result.split()[-1]) if q4_result.startswith('UPDATE') else 0
    print(f'✅ Updated {q4_count:,} Q4 reports (Jan-Feb following year)')
    
    # Propagate to other tables
    print('\n📊 Propagating to balance_sheet_data and cash_flow_data...')
    
    # Balance sheet
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
    
    # Cash flow
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
    
    await conn.close()
    
    return {
        'annual_updated': annual_count,
        'q1_updated': q1_count,
        'q2_updated': q2_count, 
        'q3_updated': q3_count,
        'q4_updated': q4_count,
        'balance_sheet_updated': bs_count,
        'cash_flow_updated': cf_count,
        'total_financial_updated': annual_count + q1_count + q2_count + q3_count + q4_count
    }

async def validate_corrected_results():
    """Show sample results with the corrected timing"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('\n📊 FINAL COVERAGE WITH CORRECTED TIMING:')
    
    for table in ['financial_statements', 'balance_sheet_data', 'cash_flow_data']:
        coverage = await conn.fetchrow(f'SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM {table}')
        pct = (coverage['with_dates'] / coverage['total'] * 100) if coverage['total'] > 0 else 0
        print(f'   {table:20}: {coverage["with_dates"]:,}/{coverage["total"]:,} ({pct:.1f}%)')
    
    # Show sample ABB to verify the fix
    print('\n📋 SAMPLE ABB RESULTS (checking the fix):')
    
    abb_query = """
    SELECT 
        fs.symbol,
        fs.fiscal_year,
        fs.fiscal_quarter,
        fs.period_date,
        fs.publish_date,
        fs.total_revenue/1e6 as revenue_m
    FROM financial_statements fs
    WHERE fs.symbol = 'ABB' 
    AND fs.publish_date IS NOT NULL
    ORDER BY fs.fiscal_year DESC, fs.fiscal_quarter NULLS LAST
    LIMIT 5
    """
    
    abb_samples = await conn.fetch(abb_query)
    
    for row in abb_samples:
        quarter = f"Q{row['fiscal_quarter']}" if row['fiscal_quarter'] else "Annual"
        revenue = f"${row['revenue_m']:.0f}M" if row['revenue_m'] else "N/A"
        lag_days = (row['publish_date'] - row['period_date']).days if row['publish_date'] and row['period_date'] else 0
        print(f'   {row["symbol"]:8} | {row["fiscal_year"]} {quarter:7} | {row["period_date"]} → {row["publish_date"]} (+{lag_days}d) | {revenue:>8}')
    
    await conn.close()

async def main():
    """Main function with corrected timing logic"""
    
    print('🎯 CORRECTED EARNINGS CALENDAR PUBLISH DATE MATCHER')
    print('=' * 70)
    print('FIXED: Annual reports Year N → Published Jan-Mar Year N+1')
    print('Chain: nordic_calendar_events → nordic_companies → financial_statements')
    
    # Match with corrected timing
    stats = await match_earnings_to_financials_corrected()
    
    # Validate results
    await validate_corrected_results()
    
    print(f'\n✅ CORRECTED EARNINGS CALENDAR MATCHING COMPLETE')
    print(f'   Updated {stats["total_financial_updated"]:,} financial statements with CORRECT earnings dates')
    print(f'   Annual: {stats["annual_updated"]:,}, Q1: {stats["q1_updated"]:,}, Q2: {stats["q2_updated"]:,}, Q3: {stats["q3_updated"]:,}, Q4: {stats["q4_updated"]:,}')
    print(f'   Also updated {stats["balance_sheet_updated"]:,} balance sheet + {stats["cash_flow_updated"]:,} cash flow records')
    
    if stats['annual_updated'] == 0:
        print('\n⚠️  WARNING: No annual reports matched - check earnings calendar data availability')

if __name__ == "__main__":
    asyncio.run(main())