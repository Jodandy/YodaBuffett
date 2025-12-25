#!/usr/bin/env python3
"""
Comprehensive A/B Share Historical Data Backfill

Backfills 3+ years of historical data for all A/B pairs to enable
comprehensive backtesting across multiple pairs.
"""

import asyncio
import asyncpg
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ComprehensiveABBackfill:
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_ab_pairs_needing_backfill(self) -> List[Dict]:
        """Get A/B pairs that need historical backfill."""
        
        query = """
        SELECT 
            CASE 
                WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
            END as company_base,
            cm.primary_ticker,
            cm.yahoo_symbol,
            cm.company_name,
            COUNT(pd.date) as days_of_data,
            MIN(pd.date) as first_date,
            MAX(pd.date) as last_date
        FROM company_master cm
        LEFT JOIN daily_price_data pd ON cm.primary_ticker = pd.symbol
        WHERE cm.primary_ticker ~ '-[AB]$' OR cm.primary_ticker ~ ' [AB]$'
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2
        """
        
        records = await self.db_conn.fetch(query)
        
        # Group by company and find those needing backfill
        companies = {}
        for record in records:
            company = record['company_base']
            if company not in companies:
                companies[company] = []
            companies[company].append(dict(record))
        
        backfill_needed = []
        target_date = datetime(2022, 1, 1)  # Need data back to 2022
        
        for company, symbols in companies.items():
            if len(symbols) == 2:  # Both A and B exist
                for symbol_data in symbols:
                    # Check if this symbol needs more historical data
                    days = symbol_data['days_of_data'] or 0
                    first_date = symbol_data['first_date']
                    
                    needs_backfill = (
                        days < 700 or  # Less than ~2 years of data
                        first_date is None or 
                        first_date > target_date.date()
                    )
                    
                    if needs_backfill and symbol_data['yahoo_symbol']:
                        backfill_needed.append(symbol_data)
        
        logger.info(f"📊 Found {len(backfill_needed)} symbols needing historical backfill")
        
        return backfill_needed
        
    async def backfill_historical_data(self, symbol_info: Dict, years: int = 3) -> bool:
        """Backfill historical data for a symbol."""
        
        ticker = symbol_info['primary_ticker']
        yahoo_symbol = symbol_info['yahoo_symbol']
        
        try:
            # Get 3+ years of data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=years*365 + 100)  # Extra buffer
            
            logger.info(f"   📈 Fetching {ticker} ({yahoo_symbol}) from {start_date.date()}")
            
            yf_ticker = yf.Ticker(yahoo_symbol)
            hist_data = yf_ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval='1d'
            )
            
            if hist_data.empty:
                logger.warning(f"   ❌ No data available for {ticker}")
                return False
                
            # Insert data with conflict resolution
            insert_query = """
            INSERT INTO daily_price_data 
            (symbol, date, open_price, high_price, low_price, close_price, volume, adjusted_close, provider)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (symbol, date, provider) DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                adjusted_close = EXCLUDED.adjusted_close
            """
            
            inserted_count = 0
            for date, row in hist_data.iterrows():
                if pd.notna(row['Close']) and row['Close'] > 0:
                    try:
                        await self.db_conn.execute(
                            insert_query,
                            ticker,
                            date.date(),
                            float(row['Open']) if pd.notna(row['Open']) else None,
                            float(row['High']) if pd.notna(row['High']) else None,
                            float(row['Low']) if pd.notna(row['Low']) else None,
                            float(row['Close']),
                            int(row['Volume']) if pd.notna(row['Volume']) else 0,
                            float(row['Close']),
                            'yahoo_finance'
                        )
                        inserted_count += 1
                    except Exception:
                        pass  # Skip conflicts/errors
                        
            logger.info(f"   ✅ {ticker}: {inserted_count} records inserted")
            return True
            
        except Exception as e:
            logger.error(f"   ❌ {ticker}: Error - {e}")
            return False
            
    async def run_comprehensive_backfill(self) -> Dict:
        """Run the complete backfill process."""
        
        logger.info("🚀 Starting Comprehensive A/B Share Historical Backfill")
        
        # Get symbols needing backfill
        symbols_needing_backfill = await self.get_ab_pairs_needing_backfill()
        
        if not symbols_needing_backfill:
            logger.info("✅ All A/B symbols already have sufficient historical data!")
            return await self.check_final_coverage()
            
        logger.info(f"📈 Backfilling {len(symbols_needing_backfill)} symbols with 3+ years of data...")
        
        successful = 0
        failed = 0
        
        for i, symbol_info in enumerate(symbols_needing_backfill):
            ticker = symbol_info['primary_ticker']
            company = symbol_info['company_base']
            
            logger.info(f"\n{i+1}/{len(symbols_needing_backfill)} - {company}: {ticker}")
            
            if await self.backfill_historical_data(symbol_info, years=3):
                successful += 1
            else:
                failed += 1
                
            # Rate limiting - be respectful to Yahoo Finance
            await asyncio.sleep(2)
            
        logger.info(f"\n🎉 BACKFILL COMPLETE:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📈 Success rate: {successful/(successful+failed)*100:.1f}%")
        
        return await self.check_final_coverage()
        
    async def check_final_coverage(self) -> Dict:
        """Check final A/B pair coverage for backtesting."""
        
        backtest_start = datetime(2022, 10, 1)
        backtest_end = datetime(2024, 12, 1)
        
        query = """
        SELECT 
            CASE 
                WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
            END as company_base,
            COUNT(DISTINCT cm.primary_ticker) as total_classes,
            COUNT(DISTINCT CASE WHEN backtest_data.symbol IS NOT NULL THEN cm.primary_ticker END) as classes_with_sufficient_data
        FROM company_master cm
        LEFT JOIN (
            SELECT DISTINCT symbol 
            FROM daily_price_data 
            WHERE date BETWEEN $1 AND $2
            GROUP BY symbol
            HAVING COUNT(*) >= 400  -- At least 400 days in backtest period
        ) backtest_data ON cm.primary_ticker = backtest_data.symbol
        WHERE cm.primary_ticker ~ '-[AB]$' OR cm.primary_ticker ~ ' [AB]$'
        GROUP BY 1
        HAVING COUNT(DISTINCT cm.primary_ticker) = 2
        ORDER BY COUNT(DISTINCT CASE WHEN backtest_data.symbol IS NOT NULL THEN cm.primary_ticker END) DESC
        """
        
        pairs = await self.db_conn.fetch(query, backtest_start.date(), backtest_end.date())
        
        suitable_pairs = [p for p in pairs if p['classes_with_sufficient_data'] == 2]
        partial_pairs = [p for p in pairs if p['classes_with_sufficient_data'] == 1]
        no_data_pairs = [p for p in pairs if p['classes_with_sufficient_data'] == 0]
        
        print(f"\n🎯 FINAL A/B PAIRS COVERAGE FOR BACKTESTING:")
        print(f"   Period: {backtest_start.date()} to {backtest_end.date()}")
        print(f"   ✅ Suitable pairs (both A & B): {len(suitable_pairs)}")
        print(f"   ⚠️  Partial pairs (one class): {len(partial_pairs)}")
        print(f"   ❌ Insufficient pairs: {len(no_data_pairs)}")
        
        if suitable_pairs:
            print(f"\n🚀 READY FOR COMPREHENSIVE BACKTESTING:")
            for pair in suitable_pairs:
                print(f"   ✅ {pair['company_base']}")
                
        return {
            'suitable_pairs': len(suitable_pairs),
            'suitable_companies': [p['company_base'] for p in suitable_pairs],
            'partial_pairs': len(partial_pairs),
            'total_pairs': len(pairs)
        }
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run comprehensive A/B share historical backfill."""
    
    import sys
    
    dry_run = '--dry-run' in sys.argv
    
    backfill = ComprehensiveABBackfill()
    
    try:
        await backfill.setup()
        
        if dry_run:
            symbols_needed = await backfill.get_ab_pairs_needing_backfill()
            print(f"\n🔍 DRY RUN: Would backfill {len(symbols_needed)} symbols")
            
            companies = {}
            for symbol in symbols_needed:
                company = symbol['company_base']
                if company not in companies:
                    companies[company] = []
                companies[company].append(symbol)
                
            for company, symbols in list(companies.items())[:10]:
                print(f"\n  {company}:")
                for symbol in symbols:
                    days = symbol['days_of_data'] or 0
                    print(f"    {symbol['primary_ticker']:<12}: {days:3d} days → Needs 3+ years")
                    
            print(f"\n💡 Remove --dry-run to start comprehensive backfill")
            return
            
        # Run the backfill
        results = await backfill.run_comprehensive_backfill()
        
        print(f"\n🎉 COMPREHENSIVE BACKFILL COMPLETE!")
        print(f"   📈 {results['suitable_pairs']} A/B pairs ready for backtesting")
        
        if results['suitable_pairs'] > 1:
            print(f"\n🚀 READY FOR MULTI-PAIR A/B ARBITRAGE BACKTESTING!")
            print(f"   Companies: {', '.join(results['suitable_companies'])}")
            
    except Exception as e:
        logger.error(f"Error during backfill: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backfill.cleanup()

if __name__ == "__main__":
    asyncio.run(main())