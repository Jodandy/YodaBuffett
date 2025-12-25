#!/usr/bin/env python3
"""
Backfill A/B Share Price Data

Fetches Yahoo Finance price data specifically for the 65 A/B share symbols
that are missing price data after the symbol format fix.
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

class ABPriceDataBackfill:
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_missing_ab_symbols(self) -> List[Dict]:
        """Get A/B symbols that need price data backfill."""
        
        query = """
        SELECT 
            cm.primary_ticker,
            cm.yahoo_symbol,
            cm.company_name,
            CASE 
                WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
            END as company_base
        FROM company_master cm
        LEFT JOIN daily_price_data pd ON cm.primary_ticker = pd.symbol
        WHERE (cm.primary_ticker ~ '-[AB]$' OR cm.primary_ticker ~ ' [AB]$')
        AND pd.symbol IS NULL
        AND cm.yahoo_symbol IS NOT NULL
        ORDER BY company_base, cm.primary_ticker
        """
        
        records = await self.db_conn.fetch(query)
        
        missing_symbols = []
        for record in records:
            missing_symbols.append({
                'ticker': record['primary_ticker'],
                'yahoo_symbol': record['yahoo_symbol'],
                'company_name': record['company_name'],
                'company_base': record['company_base']
            })
            
        logger.info(f"📊 Found {len(missing_symbols)} A/B symbols needing price data")
        
        # Group by company for better visibility
        by_company = {}
        for symbol in missing_symbols:
            base = symbol['company_base']
            if base not in by_company:
                by_company[base] = []
            by_company[base].append(symbol)
            
        logger.info(f"📈 Across {len(by_company)} companies")
        
        return missing_symbols
        
    async def test_yahoo_symbol(self, symbol_info: Dict) -> bool:
        """Test if a Yahoo symbol exists and has data."""
        
        ticker = symbol_info['ticker']
        yahoo_symbol = symbol_info['yahoo_symbol']
        
        try:
            yf_ticker = yf.Ticker(yahoo_symbol)
            hist = yf_ticker.history(period='5d')
            
            if not hist.empty and hist['Close'].iloc[-1] > 0:
                latest_price = float(hist['Close'].iloc[-1])
                logger.info(f"   ✅ {ticker:<12} ({yahoo_symbol:<15}) → ${latest_price:.2f}")
                return True
            else:
                logger.info(f"   ❌ {ticker:<12} ({yahoo_symbol:<15}) → No data")
                return False
                
        except Exception as e:
            logger.info(f"   ❌ {ticker:<12} ({yahoo_symbol:<15}) → Error: {str(e)[:50]}")
            return False
            
    async def backfill_price_data(self, symbol_info: Dict) -> bool:
        """Backfill historical price data for a symbol."""
        
        ticker = symbol_info['ticker']
        yahoo_symbol = symbol_info['yahoo_symbol']
        
        try:
            # Get 2 years of data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=2*365)
            
            yf_ticker = yf.Ticker(yahoo_symbol)
            hist_data = yf_ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval='1d'
            )
            
            if hist_data.empty:
                return False
                
            # Insert data
            insert_query = """
            INSERT INTO daily_price_data 
            (symbol, date, open_price, high_price, low_price, close_price, volume, adj_close_price)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol, date) DO NOTHING
            """
            
            inserted_count = 0
            for date, row in hist_data.iterrows():
                if pd.notna(row['Close']) and row['Close'] > 0:
                    try:
                        await self.db_conn.execute(
                            insert_query,
                            ticker,  # Use the database ticker format
                            date.date(),
                            float(row['Open']) if pd.notna(row['Open']) else None,
                            float(row['High']) if pd.notna(row['High']) else None,
                            float(row['Low']) if pd.notna(row['Low']) else None,
                            float(row['Close']),
                            int(row['Volume']) if pd.notna(row['Volume']) else 0,
                            float(row['Close'])
                        )
                        inserted_count += 1
                    except Exception:
                        pass  # Skip duplicates
                        
            logger.info(f"   ✅ {ticker:<12} → {inserted_count} records")
            return True
            
        except Exception as e:
            logger.error(f"   ❌ {ticker:<12} → Error: {e}")
            return False
            
    async def run_backfill(self, missing_symbols: List[Dict], test_first: bool = True):
        """Run the complete backfill process."""
        
        if test_first:
            logger.info(f"🧪 TESTING {len(missing_symbols)} symbols first...")
            
            working_symbols = []
            failed_symbols = []
            
            for i, symbol_info in enumerate(missing_symbols):
                if i > 0 and i % 10 == 0:
                    logger.info(f"   Progress: {i}/{len(missing_symbols)}")
                    
                if await self.test_yahoo_symbol(symbol_info):
                    working_symbols.append(symbol_info)
                else:
                    failed_symbols.append(symbol_info)
                    
                # Rate limiting
                await asyncio.sleep(0.5)
                
            logger.info(f"\n📊 TEST RESULTS:")
            logger.info(f"   ✅ Working symbols: {len(working_symbols)}")
            logger.info(f"   ❌ Failed symbols: {len(failed_symbols)}")
            
            if not working_symbols:
                logger.info("❌ No working symbols found. Check Yahoo symbol formats.")
                return
                
            symbols_to_fetch = working_symbols
        else:
            symbols_to_fetch = missing_symbols
            
        # Backfill data for working symbols
        logger.info(f"\n📈 BACKFILLING {len(symbols_to_fetch)} symbols...")
        
        successful = 0
        failed = 0
        
        for i, symbol_info in enumerate(symbols_to_fetch):
            if i > 0 and i % 5 == 0:
                logger.info(f"   Progress: {i}/{len(symbols_to_fetch)} ({successful} successful)")
                
            if await self.backfill_price_data(symbol_info):
                successful += 1
            else:
                failed += 1
                
            # Rate limiting
            await asyncio.sleep(1)
            
        logger.info(f"\n🎉 BACKFILL COMPLETE:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📈 Success rate: {successful/(successful+failed)*100:.1f}%")
        
        return successful
        
    async def verify_ab_pairs_after_backfill(self):
        """Check A/B pairs status after backfill."""
        
        query = """
        SELECT 
            CASE 
                WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
            END as company_base,
            STRING_AGG(DISTINCT cm.primary_ticker || CASE WHEN pd.symbol IS NOT NULL THEN ' ✓' ELSE ' ✗' END, ', ' ORDER BY cm.primary_ticker || CASE WHEN pd.symbol IS NOT NULL THEN ' ✓' ELSE ' ✗' END) as share_classes,
            COUNT(DISTINCT cm.primary_ticker) as total_classes,
            COUNT(DISTINCT pd.symbol) as classes_with_data
        FROM company_master cm
        LEFT JOIN daily_price_data pd ON cm.primary_ticker = pd.symbol
        WHERE cm.primary_ticker ~ '-[AB]$' OR cm.primary_ticker ~ ' [AB]$'
        GROUP BY 1
        HAVING COUNT(DISTINCT cm.primary_ticker) > 1
        ORDER BY COUNT(DISTINCT pd.symbol) DESC, COUNT(DISTINCT cm.primary_ticker) DESC
        """
        
        pairs = await self.db_conn.fetch(query)
        
        complete = [p for p in pairs if p['classes_with_data'] == p['total_classes']]
        partial = [p for p in pairs if 0 < p['classes_with_data'] < p['total_classes']]
        none = [p for p in pairs if p['classes_with_data'] == 0]
        
        print(f"\n🎯 A/B PAIRS STATUS AFTER BACKFILL:")
        print(f"   Total companies: {len(pairs)}")
        print(f"   ✅ Complete pairs: {len(complete)}")
        print(f"   ⚠️  Partial pairs: {len(partial)}")
        print(f"   ❌ No data pairs: {len(none)}")
        
        if complete:
            print(f"\n🚀 COMPLETE A/B PAIRS (READY FOR ARBITRAGE ANALYSIS):")
            for pair in complete:
                print(f"   {pair['company_base']:<12} → {pair['share_classes']}")
                
        return len(complete)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the A/B share price data backfill."""
    
    import sys
    
    skip_test = '--skip-test' in sys.argv
    dry_run = '--dry-run' in sys.argv
    
    backfill = ABPriceDataBackfill()
    
    try:
        await backfill.setup()
        
        # Get missing symbols
        missing_symbols = await backfill.get_missing_ab_symbols()
        
        if not missing_symbols:
            print("✅ All A/B symbols already have price data!")
            complete_pairs = await backfill.verify_ab_pairs_after_backfill()
            print(f"\n🎉 Ready to analyze {complete_pairs} complete A/B pairs!")
            return
            
        if dry_run:
            print(f"\n🔍 DRY RUN: Would backfill {len(missing_symbols)} symbols")
            
            # Group by company
            by_company = {}
            for symbol in missing_symbols:
                base = symbol['company_base']
                if base not in by_company:
                    by_company[base] = []
                by_company[base].append(symbol)
                
            for company, symbols in list(by_company.items())[:10]:
                print(f"   {company}:")
                for symbol in symbols:
                    print(f"     {symbol['ticker']:<12} → {symbol['yahoo_symbol']}")
                    
            print(f"\n💡 Remove --dry-run to start backfilling")
            return
            
        # Run backfill
        successful = await backfill.run_backfill(missing_symbols, test_first=not skip_test)
        
        if successful > 0:
            complete_pairs = await backfill.verify_ab_pairs_after_backfill()
            
            print(f"\n🎉 BACKFILL SUCCESS!")
            print(f"   📈 {successful} symbols now have price data")
            print(f"   🎯 {complete_pairs} complete A/B pairs ready for analysis")
            
            if complete_pairs > 0:
                print(f"\n🚀 NEXT STEP: Run A/B spread arbitrage analysis!")
                print(f"   python3 update_ab_share_analyzer.py")
        
    except Exception as e:
        logger.error(f"Error during backfill: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backfill.cleanup()

if __name__ == "__main__":
    asyncio.run(main())