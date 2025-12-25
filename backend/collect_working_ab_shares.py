#!/usr/bin/env python3
"""
Collect Working A/B Shares

Tests and collects only the A/B shares that actually exist on Yahoo Finance.
"""

import asyncio
import asyncpg
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WorkingABCollector:
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_ab_candidates(self) -> List[Dict]:
        """Get A/B share candidates from company_master."""
        
        query = """
        SELECT 
            primary_ticker,
            yahoo_symbol,
            company_name,
            CASE WHEN EXISTS(SELECT 1 FROM daily_price_data WHERE symbol = primary_ticker) THEN true ELSE false END as has_data
        FROM company_master
        WHERE (
            primary_ticker ~ ' [AB]$' 
            OR primary_ticker ~ '-[AB]$'
        )
        AND yahoo_symbol IS NOT NULL
        AND yahoo_symbol != ''
        ORDER BY primary_ticker
        """
        
        records = await self.db_conn.fetch(query)
        candidates = [dict(row) for row in records]
        
        logger.info(f"📊 Found {len(candidates)} A/B share candidates")
        missing_data = [c for c in candidates if not c['has_data']]
        logger.info(f"📊 {len(missing_data)} missing price data")
        
        return missing_data
        
    def test_yahoo_symbol(self, symbol: str) -> Tuple[bool, Dict]:
        """Test if a Yahoo Finance symbol exists and has data."""
        
        try:
            ticker = yf.Ticker(symbol)
            
            # Try to get recent data
            hist = ticker.history(period='5d')
            
            if hist.empty:
                return False, {}
                
            latest_price = float(hist['Close'].iloc[-1])
            latest_date = hist.index[-1].strftime('%Y-%m-%d')
            record_count = len(hist)
            
            # Try to get company info
            try:
                info = ticker.info
                company_name = info.get('longName', 'Unknown')
            except:
                company_name = 'Unknown'
                
            return True, {
                'latest_price': latest_price,
                'latest_date': latest_date,
                'record_count': record_count,
                'company_name': company_name
            }
            
        except Exception as e:
            return False, {'error': str(e)}
            
    async def test_and_collect_ab_shares(self, candidates: List[Dict]) -> Dict:
        """Test candidates and collect data for working symbols."""
        
        working_symbols = []
        failed_symbols = []
        
        logger.info(f"🧪 Testing {len(candidates)} A/B share symbols...")
        
        for i, candidate in enumerate(candidates):
            ticker = candidate['primary_ticker']
            yahoo_symbol = candidate['yahoo_symbol']
            
            if i > 0 and i % 10 == 0:
                logger.info(f"   Progress: {i}/{len(candidates)}")
            
            # Test the symbol
            exists, data = self.test_yahoo_symbol(yahoo_symbol)
            
            if exists:
                working_symbols.append({
                    'ticker': ticker,
                    'yahoo_symbol': yahoo_symbol,
                    'company_name': candidate['company_name'],
                    'test_data': data
                })
                logger.info(f"   ✅ {ticker:<12} → ${data['latest_price']:.2f}")
            else:
                failed_symbols.append({
                    'ticker': ticker,
                    'yahoo_symbol': yahoo_symbol,
                    'reason': data.get('error', 'No data')
                })
                
            # Rate limiting
            await asyncio.sleep(0.5)
            
        logger.info(f"📊 Test Results:")
        logger.info(f"   ✅ Working symbols: {len(working_symbols)}")
        logger.info(f"   ❌ Failed symbols: {len(failed_symbols)}")
        
        return {
            'working': working_symbols,
            'failed': failed_symbols
        }
        
    async def collect_historical_data(self, working_symbols: List[Dict]) -> int:
        """Collect historical data for working symbols."""
        
        logger.info(f"📈 Collecting historical data for {len(working_symbols)} symbols...")
        
        successful = 0
        
        for symbol_info in working_symbols:
            ticker = symbol_info['ticker']
            yahoo_symbol = symbol_info['yahoo_symbol']
            
            try:
                # Download 2 years of data
                end_date = datetime.now()
                start_date = end_date - timedelta(days=2*365)
                
                logger.info(f"   📈 Collecting {ticker} ({yahoo_symbol})...")
                
                yf_ticker = yf.Ticker(yahoo_symbol)
                hist_data = yf_ticker.history(
                    start=start_date.strftime('%Y-%m-%d'),
                    end=end_date.strftime('%Y-%m-%d'),
                    interval='1d'
                )
                
                if hist_data.empty:
                    logger.warning(f"   ❌ No data for {ticker}")
                    continue
                    
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
                                ticker,
                                date.date(),
                                float(row['Open']) if pd.notna(row['Open']) else None,
                                float(row['High']) if pd.notna(row['High']) else None,
                                float(row['Low']) if pd.notna(row['Low']) else None,
                                float(row['Close']),
                                int(row['Volume']) if pd.notna(row['Volume']) else 0,
                                float(row['Close'])
                            )
                            inserted_count += 1
                        except:
                            pass  # Skip duplicates
                            
                logger.info(f"   ✅ Inserted {inserted_count} records for {ticker}")
                successful += 1
                
                # Rate limiting
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"   ❌ Error collecting {ticker}: {e}")
                
        return successful
        
    async def analyze_ab_pairs_after_collection(self):
        """Analyze A/B pairs after data collection."""
        
        logger.info("🔍 Analyzing A/B pairs after data collection...")
        
        query = """
        WITH ab_pairs AS (
            SELECT 
                CASE 
                    WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                    WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
                    WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                    WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                END as company_base,
                cm.primary_ticker,
                cm.company_name,
                CASE WHEN p.symbol IS NOT NULL THEN true ELSE false END as has_price_data
            FROM company_master cm
            LEFT JOIN daily_price_data p ON cm.primary_ticker = p.symbol
            WHERE cm.primary_ticker ~ ' [AB]$' OR cm.primary_ticker ~ '-[AB]$'
        )
        SELECT 
            company_base,
            STRING_AGG(primary_ticker, ', ' ORDER BY primary_ticker) as share_classes,
            COUNT(*) as total_classes,
            COUNT(*) FILTER (WHERE has_price_data) as classes_with_data,
            STRING_AGG(company_name, ', ') as names
        FROM ab_pairs
        GROUP BY company_base
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) FILTER (WHERE has_price_data) DESC, COUNT(*) DESC
        """
        
        pairs = await self.db_conn.fetch(query)
        
        complete_pairs = [p for p in pairs if p['classes_with_data'] == p['total_classes']]
        partial_pairs = [p for p in pairs if 0 < p['classes_with_data'] < p['total_classes']]
        
        print(f"\n🎯 A/B PAIRS ANALYSIS:")
        print(f"   Total companies with multiple share classes: {len(pairs)}")
        print(f"   Complete pairs (both classes have data): {len(complete_pairs)}")
        print(f"   Partial pairs (some data missing): {len(partial_pairs)}")
        
        if complete_pairs:
            print(f"\n✅ COMPLETE A/B PAIRS:")
            for pair in complete_pairs:
                print(f"   {pair['company_base']:<15} → {pair['share_classes']}")
        
        return complete_pairs
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the working A/B share collection."""
    
    collector = WorkingABCollector()
    
    try:
        await collector.setup()
        
        # Get candidates
        candidates = await collector.get_ab_candidates()
        
        if not candidates:
            print("✅ All A/B shares already have price data!")
            complete_pairs = await collector.analyze_ab_pairs_after_collection()
            print(f"\n🎉 Found {len(complete_pairs)} complete A/B pairs ready for analysis!")
            return
            
        # Test symbols
        results = await collector.test_and_collect_ab_shares(candidates)
        
        working_symbols = results['working']
        failed_symbols = results['failed']
        
        if not working_symbols:
            print("❌ No working A/B share symbols found")
            return
            
        print(f"\n🎯 WORKING A/B SHARES:")
        for symbol in working_symbols:
            print(f"   {symbol['ticker']:<12} → {symbol['yahoo_symbol']:<15} ${symbol['test_data']['latest_price']:.2f}")
            
        # Collect data for working symbols
        successful = await collector.collect_historical_data(working_symbols)
        
        print(f"\n🎉 Successfully collected data for {successful}/{len(working_symbols)} symbols!")
        
        # Final analysis
        complete_pairs = await collector.analyze_ab_pairs_after_collection()
        
        if complete_pairs:
            print(f"\n🚀 Ready to run A/B share analysis with {len(complete_pairs)} complete pairs!")
        
    except Exception as e:
        logger.error(f"Error during collection: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await collector.cleanup()

if __name__ == "__main__":
    asyncio.run(main())