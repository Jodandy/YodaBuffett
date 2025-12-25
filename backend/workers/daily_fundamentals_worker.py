#!/usr/bin/env python3
"""
Daily Fundamentals Worker

Collects fundamental data from Yahoo Finance daily, similar to the market data worker.
Designed to run as part of the daily automation system.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import date, datetime, timedelta
import argparse
from typing import List

from yahoo_fundamentals_daily_collector import YahooDailyFundamentalsCollector
import asyncpg

# Setup logging
LOG_DIR = '/Users/jdandemar/Documents/YodaBuffett/logs'
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOG_DIR}/daily-fundamentals-worker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DailyFundamentalsWorker:
    """Worker for daily fundamental data collection."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.collector = YahooDailyFundamentalsCollector()
        
    async def get_active_symbols(self) -> List[str]:
        """Get symbols from company_master that have recent price data."""
        try:
            # Use the collector's method to get symbols properly
            symbols = await self.collector.get_symbols_from_company_master()
            logger.info(f"Found {len(symbols)} active symbols from company_master")
            return symbols
            
        except Exception as e:
            logger.error(f"Error getting active symbols: {e}")
            return []
                
    async def check_if_already_collected(self, date: date) -> bool:
        """Check if we already collected fundamentals for this date."""
        conn = None
        try:
            DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
            conn = await asyncpg.connect(DATABASE_URL)
            
            query = """
            SELECT COUNT(*) as count 
            FROM daily_fundamentals 
            WHERE date = $1
            """
            
            result = await conn.fetchval(query, date)
            return result > 0
            
        except Exception as e:
            logger.error(f"Error checking collection status: {e}")
            return False
        finally:
            if conn:
                await conn.close()
                
    async def run(self):
        """Run the daily fundamentals collection."""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("🚀 Starting Daily Fundamentals Worker")
        logger.info(f"   Time: {start_time}")
        logger.info(f"   Mode: {'DRY RUN' if self.dry_run else 'PRODUCTION'}")
        
        try:
            # Setup collector
            await self.collector.setup()
            
            # Check if already collected today
            today = date.today()
            if await self.check_if_already_collected(today):
                logger.info(f"✅ Fundamentals already collected for {today}")
                return
                
            # Get active symbols
            symbols = await self.get_active_symbols()
            
            if not symbols:
                logger.warning("No active symbols found")
                return
                
            # Filter to manageable batch (fundamentals change less frequently)
            # We can afford to update different subsets each day
            batch_size = min(100, len(symbols))  # Max 100 per day
            
            # Rotate through symbols (simple rotation based on day of month)
            day_offset = today.day % len(symbols)
            rotated_symbols = symbols[day_offset:] + symbols[:day_offset]
            batch_symbols = rotated_symbols[:batch_size]
            
            logger.info(f"📊 Collecting fundamentals for {len(batch_symbols)} symbols")
            
            if self.dry_run:
                logger.info("DRY RUN - Would collect fundamentals for:")
                for symbol in batch_symbols[:10]:
                    logger.info(f"  - {symbol}")
                logger.info(f"  ... and {len(batch_symbols) - 10} more")
            else:
                # Collect fundamentals
                await self.collector.collect_daily_fundamentals(batch_symbols, today)
                
                # Log summary
                conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
                
                count_query = """
                SELECT COUNT(*) FROM daily_fundamentals WHERE date = $1
                """
                count = await conn.fetchval(count_query, today)
                
                metrics_query = """
                SELECT 
                    AVG(CASE WHEN trailing_pe > 0 THEN trailing_pe END) as avg_pe,
                    AVG(CASE WHEN return_on_equity > 0 THEN return_on_equity END) as avg_roe,
                    COUNT(CASE WHEN dividend_yield > 0 THEN 1 END) as dividend_payers
                FROM daily_fundamentals 
                WHERE date = $1
                """
                
                row = await conn.fetchrow(metrics_query, today)
                
                logger.info(f"\n📈 Collection Summary:")
                logger.info(f"   Total symbols updated: {count}")
                logger.info(f"   Average P/E: {row['avg_pe']:.1f}" if row['avg_pe'] else "   Average P/E: N/A")
                logger.info(f"   Average ROE: {row['avg_roe']:.1%}" if row['avg_roe'] else "   Average ROE: N/A")
                logger.info(f"   Dividend payers: {row['dividend_payers']}")
                
                await conn.close()
                
        except Exception as e:
            logger.error(f"❌ Error in fundamentals worker: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"\n⏱️  Duration: {duration}")
            logger.info(f"✅ Daily Fundamentals Worker completed")
            logger.info("=" * 60)
            
            await self.collector.cleanup()

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Daily Fundamentals Worker')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run in dry-run mode (no data collection)')
    parser.add_argument('--run-now', action='store_true',
                       help='Run immediately (bypass schedule check)')
    
    args = parser.parse_args()
    
    # Check if we should run (only between 3:30 AM and 4:00 AM unless forced)
    current_hour = datetime.now().hour
    current_minute = datetime.now().minute
    
    if not args.run_now:
        if not (current_hour == 3 and 30 <= current_minute <= 59):
            logger.info(f"Outside scheduled window (3:30-4:00 AM). Current time: {current_hour}:{current_minute:02d}")
            logger.info("Use --run-now to force execution")
            return
            
    worker = DailyFundamentalsWorker(dry_run=args.dry_run)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())