#!/usr/bin/env python3
"""
Earnings Calendar Publish Date Matcher

Use nordic_calendar_events with event_type='earnings' to set REAL publish dates
on financial statements. Much cleaner than parsing document titles.

Chain: nordic_calendar_events → nordic_companies → financial_statements
"""

import asyncio
import asyncpg
from datetime import datetime

async def analyze_earnings_calendar():
    """Analyze earnings calendar events to understand the data"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('🔍 ANALYZING EARNINGS CALENDAR EVENTS')
    print('=' * 70)
    
    # Check earnings events
    earnings_query = """
    SELECT 
        COUNT(*) as total_earnings_events,
        COUNT(DISTINCT company_id) as unique_companies,
        MIN(event_date) as earliest_event,
        MAX(event_date) as latest_event
    FROM nordic_calendar_events
    WHERE event_type = 'earnings'
    """
    
    earnings_stats = await conn.fetchrow(earnings_query)
    
    print(f'📊 EARNINGS EVENTS:')
    print(f'   Total earnings events: {earnings_stats["total_earnings_events"]:,}')
    print(f'   Unique companies: {earnings_stats["unique_companies"]:,}')
    print(f'   Date range: {earnings_stats["earliest_event"]} to {earnings_stats["latest_event"]}')
    
    # Check event types
    event_types_query = """
    SELECT event_type, COUNT(*) as count
    FROM nordic_calendar_events
    GROUP BY event_type
    ORDER BY count DESC
    """
    
    event_types = await conn.fetch(event_types_query)
    
    print(f'\n📋 ALL EVENT TYPES:')
    for row in event_types:
        print(f'   {row["event_type"]:15}: {row["count"]:,} events')
    
    # Sample earnings events
    sample_query = """
    SELECT 
        nce.event_date,
        nce.title,
        nce.description,
        nc.ticker,
        nc.name
    FROM nordic_calendar_events nce
    JOIN nordic_companies nc ON nce.company_id = nc.id
    WHERE nce.event_type = 'earnings'
    ORDER BY nce.event_date DESC
    LIMIT 10
    """
    
    samples = await conn.fetch(sample_query)
    
    print(f'\n📋 SAMPLE EARNINGS EVENTS:')
    for row in samples:
        print(f'   {row["ticker"]:8} | {row["event_date"]} | {row["title"]}')
        if row['description']:
            print(f'      {row["description"][:60]}...')
    
    # Check match potential with financial_statements
    match_potential_query = """
    SELECT 
        COUNT(DISTINCT nce.company_id) as earnings_companies,
        COUNT(DISTINCT fs.symbol) as financial_companies,
        COUNT(DISTINCT CASE WHEN fs.symbol IS NOT NULL THEN nce.company_id END) as matchable_companies
    FROM nordic_calendar_events nce
    JOIN nordic_companies nc ON nce.company_id = nc.id
    LEFT JOIN financial_statements fs ON nc.ticker = fs.symbol
    WHERE nce.event_type = 'earnings'
    """
    
    match_stats = await conn.fetchrow(match_potential_query)
    
    match_rate = (match_stats['matchable_companies'] / match_stats['earnings_companies'] * 100) if match_stats['earnings_companies'] > 0 else 0
    
    print(f'\n🔗 MATCH POTENTIAL:')
    print(f'   Companies with earnings events: {match_stats["earnings_companies"]:,}')
    print(f'   Companies with financial statements: {match_stats["financial_companies"]:,}')
    print(f'   Matchable companies: {match_stats["matchable_companies"]:,} ({match_rate:.1f}%)')
    
    await conn.close()
    
    return match_stats

async def match_earnings_to_financials():
    """Match earnings calendar events to financial statements using timing rules"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('\n📅 MATCHING EARNINGS EVENTS TO FINANCIAL STATEMENTS')
    print('=' * 70)
    
    # Match annual reports (earnings published Jan-Mar for previous year)
    print('\n📊 Processing ANNUAL earnings events...')
    
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
        -- Annual earnings published Jan-Mar for previous year
        AND (
            (EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year + 1 
             AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 1 AND 3)
            -- Or published late in same year (Q4/year-end reports)
            OR (EXTRACT(YEAR FROM nce.event_date) = fs.fiscal_year 
                AND EXTRACT(MONTH FROM nce.event_date) BETWEEN 10 AND 12)
        )
        ORDER BY fs.id, nce.event_date
    )
    UPDATE financial_statements fs
    SET publish_date = ae.publish_date
    FROM annual_earnings ae
    WHERE fs.id = ae.fs_id
    """
    
    annual_result = await conn.execute(annual_update)
    annual_count = int(annual_result.split()[-1]) if annual_result.startswith('UPDATE') else 0
    print(f'✅ Updated {annual_count:,} annual reports with earnings calendar dates')
    
    # Match Q1 reports (published Apr-May)
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
    print(f'✅ Updated {q1_count:,} Q1 reports with earnings calendar dates')
    
    # Match Q2 reports (published Jul-Aug)
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
    print(f'✅ Updated {q2_count:,} Q2 reports with earnings calendar dates')
    
    # Match Q3 reports (published Oct-Nov)
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
    print(f'✅ Updated {q3_count:,} Q3 reports with earnings calendar dates')
    
    # Match Q4 reports (published Jan-Feb next year)
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
    print(f'✅ Updated {q4_count:,} Q4 reports with earnings calendar dates')
    
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

async def validate_results():
    """Show sample results to validate the matching"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('\n📊 FINAL COVERAGE WITH EARNINGS CALENDAR DATES:')
    
    for table in ['financial_statements', 'balance_sheet_data', 'cash_flow_data']:
        coverage = await conn.fetchrow(f'SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM {table}')
        pct = (coverage['with_dates'] / coverage['total'] * 100) if coverage['total'] > 0 else 0
        print(f'   {table:20}: {coverage["with_dates"]:,}/{coverage["total"]:,} ({pct:.1f}%)')
    
    # Show sample results with earnings event details
    print('\n📋 SAMPLE RESULTS WITH EARNINGS CALENDAR DATES:')
    
    sample_query = """
    SELECT 
        fs.symbol,
        fs.fiscal_year,
        fs.fiscal_quarter,
        fs.period_date,
        fs.publish_date,
        fs.total_revenue/1e6 as revenue_m,
        nce.title as earnings_title
    FROM financial_statements fs
    JOIN nordic_companies nc ON fs.symbol = nc.ticker
    JOIN nordic_calendar_events nce ON nc.id = nce.company_id 
        AND nce.event_date = fs.publish_date
        AND nce.event_type = 'earnings'
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
        if row['earnings_title']:
            print(f'      Earnings: {row["earnings_title"][:60]}...')
    
    await conn.close()

async def main():
    """Main function to run earnings calendar matching"""
    
    print('🎯 EARNINGS CALENDAR PUBLISH DATE MATCHER')
    print('=' * 70)
    print('Using nordic_calendar_events with event_type=\'earnings\'')
    print('Chain: nordic_calendar_events → nordic_companies → financial_statements')
    
    # Analyze the calendar data first
    await analyze_earnings_calendar()
    
    # Match and set publish dates
    stats = await match_earnings_to_financials()
    
    # Validate results
    await validate_results()
    
    print(f'\n✅ EARNINGS CALENDAR MATCHING COMPLETE')
    print(f'   Updated {stats["total_financial_updated"]:,} financial statements with REAL earnings dates')
    print(f'   Annual: {stats["annual_updated"]:,}, Q1: {stats["q1_updated"]:,}, Q2: {stats["q2_updated"]:,}, Q3: {stats["q3_updated"]:,}, Q4: {stats["q4_updated"]:,}')
    print(f'   Also updated {stats["balance_sheet_updated"]:,} balance sheet + {stats["cash_flow_updated"]:,} cash flow records')

if __name__ == "__main__":
    asyncio.run(main())