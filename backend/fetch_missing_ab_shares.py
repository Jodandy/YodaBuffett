#!/usr/bin/env python3
"""
Fetch Missing A/B Share Price Data

Focuses on collecting price data specifically for A/B share classes 
that are missing from daily_price_data.
"""

import asyncio
import asyncpg
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ABShareDataFetcher:
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def find_missing_ab_shares(self) -> List[Dict]:
        """Find A/B share symbols missing price data."""
        
        # Get A/B shares from company_master that don't have price data
        query = """
        SELECT 
            cm.primary_ticker,
            cm.company_name,
            cm.yahoo_symbol,
            cm.region,
            cm.country,
            CASE WHEN EXISTS(SELECT 1 FROM daily_price_data WHERE symbol = cm.primary_ticker) THEN 'has_data' ELSE 'missing' END as status
        FROM company_master cm
        WHERE (
            cm.primary_ticker ~ ' [ABC]$' 
            OR cm.primary_ticker ~ '-[ABC]$'
        )
        AND cm.yahoo_symbol IS NOT NULL
        ORDER BY cm.primary_ticker
        """
        
        records = await self.db_conn.fetch(query)
        
        all_shares = [dict(row) for row in records]
        missing_shares = [s for s in all_shares if s['status'] == 'missing']
        
        logger.info(f"📊 A/B Share Analysis:")
        logger.info(f"   Total A/B shares in master: {len(all_shares)}")
        logger.info(f"   Missing price data: {len(missing_shares)}")
        logger.info(f"   Already have price data: {len(all_shares) - len(missing_shares)}")
        
        return missing_shares
        
    async def fetch_price_data_for_symbol(self, symbol_info: Dict) -> bool:
        """Fetch and store price data for a single symbol."""
        
        ticker = symbol_info['primary_ticker']
        yahoo_symbol = symbol_info['yahoo_symbol']
        company_name = symbol_info['company_name']
        
        try:
            # Download data for the last 5 years
            end_date = datetime.now()
            start_date = end_date - timedelta(days=5*365)
            
            logger.info(f"   📈 Fetching {ticker} ({yahoo_symbol})...")
            
            # Create yfinance ticker
            yf_ticker = yf.Ticker(yahoo_symbol)
            
            # Get historical data
            hist_data = yf_ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval='1d'
            )
            
            if hist_data.empty:
                logger.warning(f"   ❌ No data found for {ticker}")
                return False
                
            # Prepare data for database
            records_to_insert = []
            
            for date, row in hist_data.iterrows():
                if pd.notna(row['Close']) and row['Close'] > 0:
                    records_to_insert.append({
                        'symbol': ticker,
                        'date': date.date(),
                        'open_price': float(row['Open']) if pd.notna(row['Open']) else None,
                        'high_price': float(row['High']) if pd.notna(row['High']) else None,
                        'low_price': float(row['Low']) if pd.notna(row['Low']) else None,
                        'close_price': float(row['Close']),
                        'volume': int(row['Volume']) if pd.notna(row['Volume']) else 0,
                        'adj_close_price': float(row['Close'])  # Using close as adj_close for simplicity
                    })
            
            if not records_to_insert:
                logger.warning(f"   ❌ No valid data for {ticker}")
                return False
                
            # Insert into database
            insert_query = """
            INSERT INTO daily_price_data 
            (symbol, date, open_price, high_price, low_price, close_price, volume, adj_close_price)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol, date) DO NOTHING
            """
            
            inserted_count = 0
            for record in records_to_insert:
                try:
                    await self.db_conn.execute(
                        insert_query,
                        record['symbol'],
                        record['date'],
                        record['open_price'],
                        record['high_price'],
                        record['low_price'],
                        record['close_price'],
                        record['volume'],
                        record['adj_close_price']
                    )
                    inserted_count += 1
                except Exception as e:
                    # Skip duplicates silently
                    pass
                    
            logger.info(f"   ✅ Inserted {inserted_count} records for {ticker}")
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Error fetching {ticker}: {e}")
            return False
            
    async def fetch_missing_ab_data(self, missing_shares: List[Dict], batch_size: int = 5):
        """Fetch price data for missing A/B shares in batches."""
        
        logger.info(f"🚀 Starting price data collection for {len(missing_shares)} symbols...")
        
        successful = 0
        failed = 0
        
        for i in range(0, len(missing_shares), batch_size):
            batch = missing_shares[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(missing_shares) + batch_size - 1) // batch_size
            
            logger.info(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} symbols)...")
            
            for symbol_info in batch:
                success = await self.fetch_price_data_for_symbol(symbol_info)
                if success:
                    successful += 1
                else:
                    failed += 1
                    
                # Rate limiting
                await asyncio.sleep(1)
                
            # Longer pause between batches
            if i + batch_size < len(missing_shares):
                logger.info(f"   💤 Waiting 5 seconds before next batch...")
                await asyncio.sleep(5)
        
        logger.info(f"\n📊 COLLECTION SUMMARY:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   📈 Success rate: {successful/len(missing_shares)*100:.1f}%")
        
        return successful, failed
        
    async def verify_ab_pairs_after_fetch(self):
        """Check how many A/B pairs we can now analyze."""
        
        # Check A/B pairs with price data
        query = """
        SELECT 
            CASE 
                WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
                WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                ELSE cm.primary_ticker
            END as company_base,
            STRING_AGG(cm.primary_ticker, ', ' ORDER BY cm.primary_ticker) as share_classes,
            COUNT(*) as class_count,
            COUNT(CASE WHEN p.symbol IS NOT NULL THEN 1 END) as classes_with_data
        FROM company_master cm
        LEFT JOIN daily_price_data p ON cm.primary_ticker = p.symbol
        WHERE cm.primary_ticker ~ ' [AB]$' OR cm.primary_ticker ~ '-[AB]$'
        GROUP BY 1
        HAVING COUNT(*) > 1
        ORDER BY classes_with_data DESC, class_count DESC
        """
        
        pairs = await self.db_conn.fetch(query)
        
        print(f"\n🎯 A/B SHARE PAIRS ANALYSIS:")
        print(f"   Companies with multiple share classes: {len(pairs)}")
        
        complete_pairs = [p for p in pairs if p['classes_with_data'] == p['class_count']]
        partial_pairs = [p for p in pairs if 0 < p['classes_with_data'] < p['class_count']]
        no_data_pairs = [p for p in pairs if p['classes_with_data'] == 0]
        
        print(f"   Complete pairs (both classes have data): {len(complete_pairs)}")
        print(f"   Partial pairs (only one class has data): {len(partial_pairs)}")
        print(f"   No data pairs: {len(no_data_pairs)}")
        
        if complete_pairs:
            print(f"\n✅ COMPLETE A/B PAIRS:")
            for pair in complete_pairs[:10]:
                print(f"   {pair['company_base']:<15} → {pair['share_classes']}")
                
        if partial_pairs:
            print(f"\n⚠️  PARTIAL A/B PAIRS:")
            for pair in partial_pairs[:10]:
                print(f"   {pair['company_base']:<15} → {pair['share_classes']} ({pair['classes_with_data']}/{pair['class_count']} have data)")
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the A/B share data collection."""
    
    import sys
    
    # Check for batch size argument
    batch_size = 5
    if '--batch-size' in sys.argv:
        idx = sys.argv.index('--batch-size')
        if idx + 1 < len(sys.argv):
            batch_size = int(sys.argv[idx + 1])
    
    dry_run = '--dry-run' in sys.argv
    
    fetcher = ABShareDataFetcher()
    
    try:
        await fetcher.setup()
        
        # Find missing A/B shares
        missing_shares = await fetcher.find_missing_ab_shares()
        
        if not missing_shares:
            print("✅ All A/B shares already have price data!")
            await fetcher.verify_ab_pairs_after_fetch()
            return
            
        if dry_run:
            print(f"\n🔍 DRY RUN: Would fetch data for {len(missing_shares)} symbols")
            for i, share in enumerate(missing_shares[:10]):
                print(f"   {share['primary_ticker']:<15} → {share['yahoo_symbol']}")
            if len(missing_shares) > 10:
                print(f"   ... and {len(missing_shares) - 10} more")
            print(f"\n💡 Add --execute to actually fetch the data")
            return
            
        # Fetch missing data
        successful, failed = await fetcher.fetch_missing_ab_data(missing_shares, batch_size)
        
        # Verify results
        if successful > 0:
            print(f"\n🔍 Verifying A/B pairs after data collection...")
            await fetcher.verify_ab_pairs_after_fetch()
        
    except Exception as e:
        logger.error(f"Error during data collection: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await fetcher.cleanup()

if __name__ == "__main__":
    asyncio.run(main())