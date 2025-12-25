#!/usr/bin/env python3
"""
Analyze DCF Data Coverage
Check what data we have for DCF backtesting and identify gaps
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta

async def analyze_dcf_coverage():
    # Database connection
    db_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(db_url)
    
    try:
        print("="*80)
        print("DCF BACKTEST DATA COVERAGE ANALYSIS")
        print("="*80)
        
        # 1. Check total companies with different types of data
        print("\n1. COMPANY DATA COVERAGE")
        print("-" * 40)
        
        # Total companies in master list
        total_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM company_master")
        print(f"Total companies in master: {total_companies}")
        
        # Companies with price data
        companies_with_prices = await conn.fetchval("""
            SELECT COUNT(DISTINCT symbol) FROM daily_price_data 
            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
        """)
        print(f"Companies with recent price data: {companies_with_prices}")
        
        # Companies with fundamental data
        companies_with_fundamentals = await conn.fetchval("""
            SELECT COUNT(DISTINCT symbol) FROM historical_fundamentals_daily
            WHERE date >= CURRENT_DATE - INTERVAL '2 years'
        """)
        print(f"Companies with fundamentals: {companies_with_fundamentals}")
        
        # Companies with DCF valuations
        companies_with_dcf = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM dcf_valuations")
        print(f"Companies with DCF valuations: {companies_with_dcf}")
        
        # 2. Check overlap - companies that have ALL required data
        print("\n2. DATA OVERLAP ANALYSIS")
        print("-" * 40)
        
        overlap_query = """
        SELECT COUNT(*) as complete_companies
        FROM (
            SELECT DISTINCT d.symbol
            FROM dcf_valuations d
            INNER JOIN daily_price_data p ON d.symbol = p.symbol
            WHERE d.valuation_date >= CURRENT_DATE - INTERVAL '3 years'
              AND p.date >= d.valuation_date
              AND p.date <= d.valuation_date + INTERVAL '365 days'
        ) complete
        """
        complete_companies = await conn.fetchval(overlap_query)
        print(f"Companies with DCF + sufficient price data for backtesting: {complete_companies}")
        
        # 3. Check DCF coverage by date range
        print("\n3. DCF VALUATION COVERAGE BY TIME")
        print("-" * 40)
        
        dcf_date_range = await conn.fetch("""
            SELECT 
                DATE_TRUNC('year', valuation_date) as year,
                COUNT(*) as valuations,
                COUNT(DISTINCT symbol) as unique_companies
            FROM dcf_valuations
            WHERE valuation_date >= '2020-01-01'
            GROUP BY 1
            ORDER BY 1
        """)
        
        for row in dcf_date_range:
            print(f"  {int(row['year'].year)}: {row['valuations']} valuations across {row['unique_companies']} companies")
        
        # 4. Find companies missing from DCF backtest
        print("\n4. MISSING DATA ANALYSIS")
        print("-" * 40)
        
        # Companies with prices but no DCF
        missing_dcf = await conn.fetch("""
            SELECT DISTINCT p.symbol, COUNT(*) as price_records
            FROM daily_price_data p
            LEFT JOIN dcf_valuations d ON p.symbol = d.symbol
            WHERE d.symbol IS NULL
              AND p.date >= CURRENT_DATE - INTERVAL '2 years'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 20
        """)
        
        print(f"\nTop 20 companies with price data but NO DCF valuations:")
        for row in missing_dcf:
            print(f"  {row['symbol']}: {row['price_records']} price records")
        
        # 5. Check data quality issues
        print("\n5. DATA QUALITY ISSUES")
        print("-" * 40)
        
        # DCF valuations with invalid fair values
        invalid_dcf = await conn.fetchval("""
            SELECT COUNT(*) FROM dcf_valuations 
            WHERE fair_value_median IS NULL OR fair_value_median <= 0
        """)
        print(f"DCF valuations with invalid fair values: {invalid_dcf}")
        
        # Companies with price gaps
        price_gaps = await conn.fetch("""
            SELECT symbol, MIN(date) as first_date, MAX(date) as last_date,
                   COUNT(*) as total_records,
                   (MAX(date) - MIN(date))::int as date_span
            FROM daily_price_data 
            WHERE symbol IN (SELECT DISTINCT symbol FROM dcf_valuations)
            GROUP BY symbol
            HAVING COUNT(*) < (MAX(date) - MIN(date))::int * 0.7  -- Less than 70% coverage
            ORDER BY date_span DESC
            LIMIT 10
        """)
        
        if price_gaps:
            print(f"\nCompanies with potential price data gaps:")
            for row in price_gaps:
                expected_days = row['date_span']
                actual_records = row['total_records']
                coverage = (actual_records / expected_days) * 100 if expected_days > 0 else 0
                print(f"  {row['symbol']}: {coverage:.1f}% coverage ({actual_records}/{expected_days} days)")
        
        # 6. Check recent data freshness
        print("\n6. DATA FRESHNESS")
        print("-" * 40)
        
        latest_dcf = await conn.fetchval("SELECT MAX(valuation_date) FROM dcf_valuations")
        latest_price = await conn.fetchval("SELECT MAX(date) FROM daily_price_data")
        
        print(f"Latest DCF valuation: {latest_dcf}")
        print(f"Latest price data: {latest_price}")
        
        # 7. Recommendations
        print("\n7. EXPANSION OPPORTUNITIES")  
        print("-" * 40)
        
        # Companies with fundamentals but no DCF
        expansion_candidates = await conn.fetch("""
            SELECT DISTINCT h.symbol, COUNT(*) as fundamental_records
            FROM historical_fundamentals_daily h
            LEFT JOIN dcf_valuations d ON h.symbol = d.symbol
            INNER JOIN daily_price_data p ON h.symbol = p.symbol
            WHERE d.symbol IS NULL
              AND h.date >= CURRENT_DATE - INTERVAL '1 year'
              AND p.date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY 1
            HAVING COUNT(*) >= 100  -- At least 100 fundamental records
            ORDER BY 2 DESC
            LIMIT 15
        """)
        
        print(f"\nTop expansion candidates (have fundamentals + prices, missing DCF):")
        for row in expansion_candidates:
            print(f"  {row['symbol']}: {row['fundamental_records']} fundamental records")
        
        print("\n" + "="*80)
        print("RECOMMENDATIONS")
        print("="*80)
        print("1. Current DCF backtest covers", complete_companies, "companies - good foundation")
        print("2. Run DCF analysis on", len(expansion_candidates), "additional companies with complete data")
        print("3. Check symbol mapping for companies with prices but no fundamentals")
        print("4. Consider extending historical DCF analysis to capture more time periods")
        
        # Save detailed analysis
        detailed_data = await conn.fetch("""
            SELECT 
                d.symbol,
                COUNT(DISTINCT d.valuation_date) as dcf_count,
                MIN(d.valuation_date) as first_dcf,
                MAX(d.valuation_date) as last_dcf,
                COUNT(DISTINCT p.date) as price_count,
                MIN(p.date) as first_price,
                MAX(p.date) as last_price
            FROM dcf_valuations d
            INNER JOIN daily_price_data p ON d.symbol = p.symbol
            WHERE d.valuation_date >= CURRENT_DATE - INTERVAL '3 years'
            GROUP BY 1
            ORDER BY 2 DESC
        """)
        
        df = pd.DataFrame(detailed_data)
        df.to_csv('dcf_backtest_data_coverage.csv', index=False)
        print(f"\nDetailed coverage analysis saved to dcf_backtest_data_coverage.csv")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_dcf_coverage())