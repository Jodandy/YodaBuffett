#!/usr/bin/env python3
"""
Explore the historical_fundamentals_daily table to understand
what metrics we have available for fundamental anomaly detection.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta

async def explore_fundamentals_data():
    """Explore the available fundamental data."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print("🔍 EXPLORING HISTORICAL FUNDAMENTALS DATA")
    print("="*60)
    
    # 1. Check table structure
    print("\n1️⃣ TABLE STRUCTURE:")
    schema_query = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns 
    WHERE table_name = 'historical_fundamentals_daily'
    ORDER BY ordinal_position
    """
    
    schema_results = await conn.fetch(schema_query)
    for row in schema_results:
        print(f"   {row['column_name']:<30} {row['data_type']:<20} {row['is_nullable']}")
    
    # 2. Check data availability
    print("\n2️⃣ DATA OVERVIEW:")
    overview_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT symbol) as unique_symbols,
        MIN(date) as earliest_date,
        MAX(date) as latest_date,
        COUNT(DISTINCT date) as unique_dates
    FROM historical_fundamentals_daily
    """
    
    overview = await conn.fetchrow(overview_query)
    print(f"   Total records:     {overview['total_records']:,}")
    print(f"   Unique symbols:    {overview['unique_symbols']:,}")
    print(f"   Date range:        {overview['earliest_date']} to {overview['latest_date']}")
    print(f"   Unique dates:      {overview['unique_dates']:,}")
    
    # 3. Sample a few companies with most data
    print("\n3️⃣ TOP COMPANIES BY DATA AVAILABILITY:")
    companies_query = """
    SELECT 
        symbol,
        COUNT(*) as records,
        MIN(date) as first_date,
        MAX(date) as last_date,
        COUNT(DISTINCT date) as unique_dates
    FROM historical_fundamentals_daily
    GROUP BY symbol
    ORDER BY records DESC
    LIMIT 10
    """
    
    companies = await conn.fetch(companies_query)
    print(f"   {'Symbol':<10} {'Records':<8} {'First Date':<12} {'Last Date':<12} {'Date Range'}")
    print(f"   {'-'*65}")
    for company in companies:
        date_range = company['last_date'] - company['first_date']
        print(f"   {company['symbol']:<10} {company['records']:<8} {company['first_date']:<12} {company['last_date']:<12} {date_range.days:>4} days")
    
    # 4. Explore available metrics for a sample company
    print(f"\n4️⃣ SAMPLE DATA FOR {companies[0]['symbol']}:")
    sample_query = """
    SELECT *
    FROM historical_fundamentals_daily
    WHERE symbol = $1
    ORDER BY date DESC
    LIMIT 3
    """
    
    sample_data = await conn.fetch(sample_query, companies[0]['symbol'])
    if sample_data:
        # Get column names
        columns = list(sample_data[0].keys())
        print(f"   Available columns: {len(columns)}")
        
        # Show first record with non-null values
        first_record = dict(sample_data[0])
        print(f"\n   Latest record ({first_record['date']}):")
        for key, value in first_record.items():
            if value is not None and key not in ['symbol', 'date']:
                if isinstance(value, (int, float)):
                    print(f"   {key:<30}: {value:>15,.2f}")
                else:
                    print(f"   {key:<30}: {str(value):<15}")
    
    # 5. Check data completeness for key metrics
    print(f"\n5️⃣ DATA COMPLETENESS FOR KEY METRICS:")
    completeness_query = """
    SELECT 
        'revenue_per_share' as metric,
        COUNT(*) as total_records,
        COUNT(revenue_per_share) as non_null_records,
        ROUND(100.0 * COUNT(revenue_per_share) / COUNT(*), 2) as completeness_pct
    FROM historical_fundamentals_daily
    UNION ALL
    SELECT 
        'pe_ratio',
        COUNT(*),
        COUNT(pe_ratio),
        ROUND(100.0 * COUNT(pe_ratio) / COUNT(*), 2)
    FROM historical_fundamentals_daily
    UNION ALL
    SELECT 
        'pb_ratio',
        COUNT(*),
        COUNT(pb_ratio),
        ROUND(100.0 * COUNT(pb_ratio) / COUNT(*), 2)
    FROM historical_fundamentals_daily
    UNION ALL
    SELECT 
        'cash_per_share',
        COUNT(*),
        COUNT(cash_per_share),
        ROUND(100.0 * COUNT(cash_per_share) / COUNT(*), 2)
    FROM historical_fundamentals_daily
    UNION ALL
    SELECT 
        'book_value_per_share',
        COUNT(*),
        COUNT(book_value_per_share),
        ROUND(100.0 * COUNT(book_value_per_share) / COUNT(*), 2)
    FROM historical_fundamentals_daily
    UNION ALL
    SELECT 
        'debt_to_equity',
        COUNT(*),
        COUNT(debt_to_equity),
        ROUND(100.0 * COUNT(debt_to_equity) / COUNT(*), 2)
    FROM historical_fundamentals_daily
    """
    
    completeness = await conn.fetch(completeness_query)
    print(f"   {'Metric':<30} {'Non-null':<10} {'Total':<10} {'Complete %'}")
    print(f"   {'-'*65}")
    for metric in completeness:
        print(f"   {metric['metric']:<30} {metric['non_null_records']:<10} {metric['total_records']:<10} {metric['completeness_pct']}%")
    
    # 6. Sample time series for one company
    print(f"\n6️⃣ TIME SERIES SAMPLE - {companies[0]['symbol']} METRICS:")
    timeseries_query = """
    SELECT 
        date,
        revenue_per_share,
        pe_ratio,
        book_value_per_share,
        pb_ratio
    FROM historical_fundamentals_daily
    WHERE symbol = $1 
    AND revenue_per_share IS NOT NULL
    ORDER BY date DESC
    LIMIT 10
    """
    
    timeseries = await conn.fetch(timeseries_query, companies[0]['symbol'])
    print(f"   {'Date':<12} {'Rev/Share':<10} {'P/E':<8} {'Book Val':<10} {'P/B':<8}")
    print(f"   {'-'*55}")
    for record in timeseries:
        rev_ps = f"{record['revenue_per_share']:.2f}" if record['revenue_per_share'] else "N/A"
        pe = f"{record['pe_ratio']:.2f}" if record['pe_ratio'] else "N/A"
        book_val = f"{record['book_value_per_share']:.2f}" if record['book_value_per_share'] else "N/A"
        pb = f"{record['pb_ratio']:.2f}" if record['pb_ratio'] else "N/A"
        print(f"   {record['date']:<12} {rev_ps:<10} {pe:<8} {book_val:<10} {pb:<8}")
    
    # 7. Check if we can match with price data
    print(f"\n7️⃣ MATCHING WITH PRICE DATA:")
    
    # Get a company that exists in both tables
    matching_query = """
    SELECT 
        hf.symbol,
        COUNT(hf.*) as fundamental_records,
        COUNT(pd.*) as price_records
    FROM historical_fundamentals_daily hf
    LEFT JOIN daily_price_data pd ON hf.symbol = pd.symbol AND hf.date = pd.date
    WHERE hf.symbol IN (
        SELECT symbol FROM historical_fundamentals_daily 
        GROUP BY symbol 
        ORDER BY COUNT(*) DESC 
        LIMIT 5
    )
    GROUP BY hf.symbol
    ORDER BY price_records DESC
    """
    
    matching = await conn.fetch(matching_query)
    print(f"   {'Symbol':<10} {'Fundamentals':<12} {'Price Data':<11} {'Match Rate'}")
    print(f"   {'-'*50}")
    for match in matching:
        match_rate = (match['price_records'] / match['fundamental_records']) * 100 if match['fundamental_records'] > 0 else 0
        print(f"   {match['symbol']:<10} {match['fundamental_records']:<12} {match['price_records']:<11} {match_rate:>6.1f}%")
    
    await conn.close()
    
    print(f"\n✅ EXPLORATION COMPLETE!")
    print(f"\n💡 NEXT STEPS FOR FUNDAMENTAL STRATEGY:")
    print(f"   1. Focus on companies with good data completeness")
    print(f"   2. Calculate rolling averages for key metrics")
    print(f"   3. Detect when current fundamentals exceed historical thresholds")
    print(f"   4. Correlate fundamental changes with subsequent price movements")

if __name__ == "__main__":
    asyncio.run(explore_fundamentals_data())