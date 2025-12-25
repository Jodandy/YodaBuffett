#!/usr/bin/env python3
"""
Data Coverage Analyzer

Investigates data coverage issues across price data, fundamentals, and company master tables.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, date, timedelta
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCoverageAnalyzer:
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def analyze_data_coverage(self):
        """Comprehensive data coverage analysis."""
        
        print("\n" + "="*100)
        print("🔍 YODABUFFETT DATA COVERAGE ANALYSIS")
        print("="*100)
        
        # 1. Table sizes and date ranges
        await self._analyze_table_coverage()
        
        # 2. Symbol overlap analysis
        await self._analyze_symbol_overlap()
        
        # 3. Recent data availability
        await self._analyze_recent_data()
        
        # 4. Missing data patterns
        await self._analyze_missing_patterns()
        
    async def _analyze_table_coverage(self):
        """Analyze basic table statistics."""
        
        print(f"\n📊 TABLE COVERAGE OVERVIEW:")
        
        # Company master
        company_stats = await self.db_conn.fetchrow("""
        SELECT 
            COUNT(*) as total_companies,
            COUNT(DISTINCT primary_ticker) as unique_tickers,
            COUNT(DISTINCT yahoo_symbol) as yahoo_symbols,
            COUNT(company_name) as named_companies
        FROM company_master
        """)
        
        print(f"   Company Master: {company_stats['total_companies']:,} total")
        print(f"     - Unique tickers: {company_stats['unique_tickers']:,}")
        print(f"     - Yahoo symbols: {company_stats['yahoo_symbols']:,}")
        print(f"     - Named companies: {company_stats['named_companies']:,}")
        
        # Price data
        price_stats = await self.db_conn.fetchrow("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols,
            COUNT(*) as total_records,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM daily_price_data
        """)
        
        print(f"\n   Price Data: {price_stats['symbols']:,} symbols, {price_stats['total_records']:,} records")
        print(f"     - Date range: {price_stats['earliest_date']} to {price_stats['latest_date']}")
        
        # Fundamentals
        fund_stats = await self.db_conn.fetchrow("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols,
            COUNT(*) as total_records,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
        FROM historical_fundamentals_daily
        """)
        
        print(f"\n   Fundamentals: {fund_stats['symbols']:,} symbols, {fund_stats['total_records']:,} records")
        print(f"     - Date range: {fund_stats['earliest_date']} to {fund_stats['latest_date']}")
        
    async def _analyze_symbol_overlap(self):
        """Analyze symbol overlap between tables."""
        
        print(f"\n🔗 SYMBOL OVERLAP ANALYSIS:")
        
        # Get symbols from each table
        company_symbols = await self.db_conn.fetch("SELECT DISTINCT primary_ticker FROM company_master WHERE primary_ticker IS NOT NULL")
        price_symbols = await self.db_conn.fetch("SELECT DISTINCT symbol FROM daily_price_data")
        fund_symbols = await self.db_conn.fetch("SELECT DISTINCT symbol FROM historical_fundamentals_daily")
        
        company_set = {row['primary_ticker'] for row in company_symbols}
        price_set = {row['symbol'] for row in price_symbols}
        fund_set = {row['symbol'] for row in fund_symbols}
        
        print(f"   Company Master symbols: {len(company_set):,}")
        print(f"   Price Data symbols: {len(price_set):,}")
        print(f"   Fundamentals symbols: {len(fund_set):,}")
        
        # Overlaps
        price_and_fund = price_set & fund_set
        all_three = company_set & price_set & fund_set
        price_only = price_set - fund_set
        fund_only = fund_set - price_set
        
        print(f"\n   📈 Complete overlap (all 3 tables): {len(all_three):,} symbols")
        print(f"   📊 Price + Fundamentals: {len(price_and_fund):,} symbols")
        print(f"   💰 Price data only: {len(price_only):,} symbols")
        print(f"   📋 Fundamentals only: {len(fund_only):,} symbols")
        
        # Show examples of missing data
        if price_only:
            print(f"\n   💰 Examples with PRICE but no fundamentals: {list(price_only)[:10]}")
        if fund_only:
            print(f"   📋 Examples with FUNDAMENTALS but no prices: {list(fund_only)[:10]}")
            
        return {
            'complete_overlap': all_three,
            'price_and_fund': price_and_fund,
            'price_only': price_only,
            'fund_only': fund_only
        }
        
    async def _analyze_recent_data(self):
        """Check recent data availability."""
        
        print(f"\n📅 RECENT DATA AVAILABILITY (last 7 days):")
        
        # Recent dates with data
        recent_dates = await self.db_conn.fetch("""
        SELECT 
            p.date,
            COUNT(DISTINCT p.symbol) as price_symbols,
            COUNT(DISTINCT f.symbol) as fund_symbols
        FROM daily_price_data p
        LEFT JOIN historical_fundamentals_daily f ON p.symbol = f.symbol AND p.date = f.date
        WHERE p.date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY p.date
        ORDER BY p.date DESC
        """)
        
        print(f"   {'Date':<12} {'Price Symbols':<15} {'Fund Symbols':<15} {'Coverage %'}")
        print("-" * 60)
        
        for row in recent_dates:
            coverage_pct = (row['fund_symbols'] / row['price_symbols'] * 100) if row['price_symbols'] > 0 else 0
            print(f"   {row['date']:<12} {row['price_symbols']:<15,} {row['fund_symbols']:<15,} {coverage_pct:>8.1f}%")
            
    async def _analyze_missing_patterns(self):
        """Analyze patterns in missing data."""
        
        print(f"\n🕵️ MISSING DATA PATTERNS:")
        
        # Companies with price data but missing recent fundamentals
        missing_fundamentals = await self.db_conn.fetch("""
        SELECT 
            p.symbol,
            cm.company_name,
            MAX(p.date) as latest_price_date,
            MAX(f.date) as latest_fund_date,
            p.close_price as current_price
        FROM daily_price_data p
        LEFT JOIN company_master cm ON p.symbol = cm.primary_ticker
        LEFT JOIN historical_fundamentals_daily f ON p.symbol = f.symbol
        WHERE p.date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY p.symbol, cm.company_name, p.close_price
        HAVING MAX(f.date) IS NULL OR MAX(f.date) < CURRENT_DATE - INTERVAL '30 days'
        ORDER BY p.close_price DESC
        LIMIT 20
        """)
        
        print(f"\n   📊 TOP 20 COMPANIES WITH PRICES BUT MISSING/OLD FUNDAMENTALS:")
        print(f"   {'Symbol':<10} {'Company':<30} {'Price':<10} {'Latest Fund Date'}")
        print("-" * 70)
        
        for row in missing_fundamentals:
            company_name = (row['company_name'] or 'Unknown')[:28]
            fund_date = row['latest_fund_date'] or 'Never'
            price = row['current_price'] or 0
            
            print(f"   {row['symbol']:<10} {company_name:<30} ${price:<9.2f} {fund_date}")
            
        # Symbol format analysis
        print(f"\n   🔤 SYMBOL FORMAT ANALYSIS:")
        
        symbol_analysis = await self.db_conn.fetch("""
        SELECT 
            CASE 
                WHEN LENGTH(p.symbol) <= 3 THEN 'Short (≤3)'
                WHEN LENGTH(p.symbol) <= 5 THEN 'Medium (4-5)'
                ELSE 'Long (>5)'
            END as symbol_length,
            CASE
                WHEN p.symbol ~ '^[A-Z]+$' THEN 'Letters only'
                WHEN p.symbol ~ '^[A-Z]+[0-9]+$' THEN 'Letters + numbers'
                WHEN p.symbol ~ ' ' THEN 'Contains space'
                WHEN p.symbol ~ '-' THEN 'Contains dash'
                ELSE 'Other format'
            END as symbol_format,
            COUNT(*) as count,
            COUNT(CASE WHEN f.symbol IS NOT NULL THEN 1 END) as has_fundamentals
        FROM daily_price_data p
        LEFT JOIN historical_fundamentals_daily f ON p.symbol = f.symbol
        WHERE p.date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY 1, 2
        ORDER BY count DESC
        """)
        
        for row in symbol_analysis:
            coverage = (row['has_fundamentals'] / row['count'] * 100) if row['count'] > 0 else 0
            print(f"     {row['symbol_length']}, {row['symbol_format']}: {row['count']:,} symbols ({coverage:.1f}% with fundamentals)")
            
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    analyzer = DataCoverageAnalyzer()
    
    try:
        await analyzer.setup()
        await analyzer.analyze_data_coverage()
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())