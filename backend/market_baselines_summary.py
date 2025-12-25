#!/usr/bin/env python3
"""
Market Baselines Summary

Quick summary of key market statistics and baselines from the YodaBuffett database.
Provides essential metrics for investment analysis and comparison.
"""

import asyncio
import asyncpg
from datetime import date, timedelta
import pandas as pd
import numpy as np

async def generate_market_baselines():
    """Generate comprehensive market baselines summary."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print("=" * 100)
    print("📊 YODABUFFETT MARKET BASELINES & STATISTICS")
    print("=" * 100)
    
    # 1. Data Coverage
    coverage_query = """
    SELECT 
        (SELECT COUNT(DISTINCT symbol) FROM historical_fundamentals_daily WHERE pe_ratio IS NOT NULL) as companies_with_fundamentals,
        (SELECT COUNT(DISTINCT symbol) FROM daily_price_data) as companies_with_prices,
        (SELECT COUNT(DISTINCT primary_ticker) FROM company_master) as total_companies,
        (SELECT MIN(date) FROM historical_fundamentals_daily) as earliest_fundamental_date,
        (SELECT MAX(date) FROM historical_fundamentals_daily) as latest_fundamental_date,
        (SELECT COUNT(*) FROM historical_fundamentals_daily) as total_fundamental_records
    """
    
    coverage = await conn.fetchrow(coverage_query)
    
    print("\n📈 DATA COVERAGE:")
    print(f"   Total companies tracked: {coverage['total_companies']:,}")
    print(f"   Companies with fundamentals: {coverage['companies_with_fundamentals']:,}")
    print(f"   Companies with price data: {coverage['companies_with_prices']:,}")
    print(f"   Fundamental records: {coverage['total_fundamental_records']:,}")
    print(f"   Date range: {coverage['earliest_fundamental_date']} to {coverage['latest_fundamental_date']}")
    
    # 2. Current Market Baselines (most recent comprehensive data)
    recent_date_query = """
    SELECT date FROM historical_fundamentals_daily
    WHERE pe_ratio IS NOT NULL
    GROUP BY date
    HAVING COUNT(DISTINCT symbol) > 100
    ORDER BY date DESC
    LIMIT 1
    """
    recent_date = await conn.fetchval(recent_date_query)
    
    baselines_query = """
    SELECT 
        COUNT(DISTINCT symbol) as companies,
        -- P/E Ratios
        PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY pe_ratio) as pe_10th,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pe_ratio) as pe_25th,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pe_ratio) as pe_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pe_ratio) as pe_75th,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pe_ratio) as pe_90th,
        AVG(pe_ratio) as pe_mean,
        -- P/B Ratios  
        PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY pb_ratio) as pb_10th,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pb_ratio) as pb_25th,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pb_ratio) as pb_median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pb_ratio) as pb_75th,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pb_ratio) as pb_90th,
        AVG(pb_ratio) as pb_mean,
        -- Market Cap (in millions)
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY market_cap)/1000000 as cap_25th_mm,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY market_cap)/1000000 as cap_median_mm,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY market_cap)/1000000 as cap_75th_mm,
        -- Financial Health
        AVG(debt_to_equity) as avg_debt_equity,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY debt_to_equity) as median_debt_equity,
        AVG(current_ratio) as avg_current_ratio,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY current_ratio) as median_current_ratio
    FROM historical_fundamentals_daily
    WHERE date = $1
    AND pe_ratio > 0 AND pe_ratio < 100
    AND pb_ratio > 0 AND pb_ratio < 20
    """
    
    baselines = await conn.fetchrow(baselines_query, recent_date)
    
    print(f"\n💰 MARKET BASELINES (as of {recent_date}, {baselines['companies']} companies):")
    print("\n   P/E RATIO DISTRIBUTION:")
    print(f"     10th percentile: {baselines['pe_10th']:.1f} (Deep value territory)")
    print(f"     25th percentile: {baselines['pe_25th']:.1f} (Value stocks)")
    print(f"     Median (50th):   {baselines['pe_median']:.1f} (Market average)")
    print(f"     Mean:            {baselines['pe_mean']:.1f}")
    print(f"     75th percentile: {baselines['pe_75th']:.1f} (Growth stocks)")
    print(f"     90th percentile: {baselines['pe_90th']:.1f} (High growth/expensive)")
    
    print("\n   P/B RATIO DISTRIBUTION:")
    print(f"     10th percentile: {baselines['pb_10th']:.1f} (Potential value)")
    print(f"     25th percentile: {baselines['pb_25th']:.1f}")
    print(f"     Median (50th):   {baselines['pb_median']:.1f} (Market average)")
    print(f"     Mean:            {baselines['pb_mean']:.1f}")
    print(f"     75th percentile: {baselines['pb_75th']:.1f}")
    print(f"     90th percentile: {baselines['pb_90th']:.1f} (Premium valuation)")
    
    print("\n   MARKET CAP DISTRIBUTION:")
    print(f"     Small cap (25th percentile):  ${baselines['cap_25th_mm']:.0f}M")
    print(f"     Mid cap (median):              ${baselines['cap_median_mm']:.0f}M")  
    print(f"     Large cap (75th percentile):   ${baselines['cap_75th_mm']:.0f}M")
    
    print("\n   FINANCIAL HEALTH METRICS:")
    print(f"     Average Debt/Equity: {baselines['avg_debt_equity']:.2f} (Median: {baselines['median_debt_equity']:.2f})")
    print(f"     Average Current Ratio: {baselines['avg_current_ratio']:.2f} (Median: {baselines['median_current_ratio']:.2f})")
    
    # 3. Historical Trends
    trends_query = """
    WITH monthly_avg AS (
        SELECT 
            DATE_TRUNC('month', date) as month,
            AVG(pe_ratio) as avg_pe,
            AVG(pb_ratio) as avg_pb,
            COUNT(DISTINCT symbol) as companies
        FROM historical_fundamentals_daily
        WHERE pe_ratio > 0 AND pe_ratio < 100
        GROUP BY DATE_TRUNC('month', date)
        HAVING COUNT(DISTINCT symbol) > 50
    )
    SELECT 
        MIN(avg_pe) as min_pe,
        MAX(avg_pe) as max_pe,
        AVG(avg_pe) as historical_avg_pe,
        MIN(avg_pb) as min_pb,
        MAX(avg_pb) as max_pb,
        AVG(avg_pb) as historical_avg_pb,
        COUNT(*) as months_of_data
    FROM monthly_avg
    """
    
    trends = await conn.fetchrow(trends_query)
    
    print("\n📊 HISTORICAL RANGES (monthly averages):")
    print(f"   P/E Range: {trends['min_pe']:.1f} - {trends['max_pe']:.1f} (Historical avg: {trends['historical_avg_pe']:.1f})")
    print(f"   P/B Range: {trends['min_pb']:.1f} - {trends['max_pb']:.1f} (Historical avg: {trends['historical_avg_pb']:.1f})")
    print(f"   Based on {trends['months_of_data']} months of data")
    
    # 4. Sector Comparisons
    sector_query = """
    SELECT 
        COALESCE(cm.industry, 'Unknown') as sector,
        COUNT(DISTINCT h.symbol) as companies,
        AVG(h.pe_ratio) as avg_pe,
        AVG(h.pb_ratio) as avg_pb,
        AVG(h.debt_to_equity) as avg_debt_equity
    FROM historical_fundamentals_daily h
    LEFT JOIN company_master cm ON h.symbol = cm.primary_ticker
    WHERE h.date = $1
    AND h.pe_ratio > 0 AND h.pe_ratio < 100
    GROUP BY COALESCE(cm.industry, 'Unknown')
    HAVING COUNT(DISTINCT h.symbol) >= 5
    ORDER BY AVG(h.pe_ratio)
    LIMIT 10
    """
    
    sectors = await conn.fetch(sector_query, recent_date)
    
    if sectors:
        print("\n🏭 SECTOR P/E RANKINGS (lowest to highest):")
        for i, sector in enumerate(sectors, 1):
            print(f"   {i:2d}. {sector['sector']:<30} P/E: {sector['avg_pe']:>5.1f} ({sector['companies']} companies)")
    
    # 5. Value vs Growth Classification
    value_growth_query = """
    SELECT 
        COUNT(CASE WHEN pe_ratio < $2 THEN 1 END) as value_stocks,
        COUNT(CASE WHEN pe_ratio >= $2 AND pe_ratio < $3 THEN 1 END) as fair_value_stocks,
        COUNT(CASE WHEN pe_ratio >= $3 THEN 1 END) as growth_stocks,
        COUNT(*) as total_stocks
    FROM historical_fundamentals_daily
    WHERE date = $1
    AND pe_ratio > 0 AND pe_ratio < 100
    """
    
    # Using median as fair value center
    median_pe = baselines['pe_median']
    value_threshold = median_pe * 0.8
    growth_threshold = median_pe * 1.2
    
    classification = await conn.fetchrow(value_growth_query, recent_date, value_threshold, growth_threshold)
    
    print("\n🎯 MARKET CLASSIFICATION:")
    print(f"   Value stocks (P/E < {value_threshold:.1f}): {classification['value_stocks']} ({classification['value_stocks']/classification['total_stocks']*100:.1f}%)")
    print(f"   Fair value (P/E {value_threshold:.1f}-{growth_threshold:.1f}): {classification['fair_value_stocks']} ({classification['fair_value_stocks']/classification['total_stocks']*100:.1f}%)")
    print(f"   Growth stocks (P/E > {growth_threshold:.1f}): {classification['growth_stocks']} ({classification['growth_stocks']/classification['total_stocks']*100:.1f}%)")
    
    # 6. Quick Investment Screens
    print("\n🔍 QUICK INVESTMENT SCREENS:")
    
    # Deep Value Screen
    deep_value = await conn.fetch("""
        SELECT h.symbol, cm.company_name, h.pe_ratio, h.pb_ratio, h.market_cap
        FROM historical_fundamentals_daily h
        LEFT JOIN company_master cm ON h.symbol = cm.primary_ticker
        WHERE h.date = $1
        AND h.pe_ratio > 0 AND h.pe_ratio < $2
        AND h.pb_ratio < 1.5
        AND h.debt_to_equity < 1.0
        ORDER BY h.pe_ratio
        LIMIT 5
    """, recent_date, baselines['pe_25th'])
    
    print("\n   📉 DEEP VALUE OPPORTUNITIES (Low P/E + Low P/B + Low Debt):")
    for stock in deep_value:
        name = stock['company_name'] or stock['symbol']
        print(f"      {stock['symbol']:<8} P/E: {stock['pe_ratio']:>5.1f} | P/B: {stock['pb_ratio']:>4.1f} | {name[:30]}")
    
    # Quality at Fair Price
    quality_fair = await conn.fetch("""
        SELECT h.symbol, cm.company_name, h.pe_ratio, h.debt_to_equity, h.current_ratio
        FROM historical_fundamentals_daily h
        LEFT JOIN company_master cm ON h.symbol = cm.primary_ticker
        WHERE h.date = $1
        AND h.pe_ratio > $2 AND h.pe_ratio < $3
        AND h.debt_to_equity < 0.5
        AND h.current_ratio > 1.5
        ORDER BY h.debt_to_equity
        LIMIT 5
    """, recent_date, baselines['pe_25th'], baselines['pe_median'])
    
    print("\n   ⭐ QUALITY AT FAIR PRICE (Moderate P/E + Strong Balance Sheet):")
    for stock in quality_fair:
        name = stock['company_name'] or stock['symbol']
        print(f"      {stock['symbol']:<8} P/E: {stock['pe_ratio']:>5.1f} | D/E: {stock['debt_to_equity']:>4.2f} | Current: {stock['current_ratio']:>4.1f} | {name[:30]}")
    
    print("\n" + "=" * 100)
    print("Use these baselines to evaluate individual stocks and identify opportunities! 🎯")
    print("=" * 100)
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(generate_market_baselines())