#!/usr/bin/env python3
"""
Rebuild Corrected Historical Fundamentals

Rebuilds historical fundamentals with the corrected logic:
- Only calculates metrics when fundamental data was actually available
- Uses point-in-time fundamental data, not future data
- Results in realistic historical coverage starting from ~2020-2022
"""

import asyncio
import logging
from datetime import datetime
from historical_fundamentals_backfill import HistoricalFundamentalsBackfill

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/corrected_fundamentals_rebuild.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def rebuild_corrected():
    """Rebuild historical fundamentals with corrected logic."""
    
    start_time = datetime.now()
    backfill = HistoricalFundamentalsBackfill()
    
    try:
        await backfill.setup()
        
        # Get symbols that already have financial statement data
        symbols_query = """
        SELECT DISTINCT symbol
        FROM financial_statements
        ORDER BY symbol
        """
        
        rows = await backfill.db_conn.fetch(symbols_query)
        symbols = [row['symbol'] for row in rows]
        
        logger.info("🔧 REBUILDING CORRECTED HISTORICAL FUNDAMENTALS")
        logger.info("=" * 60)
        logger.info(f"📊 Processing {len(symbols)} symbols with financial data...")
        logger.info(f"⏰ Started at: {start_time}")
        logger.info("✅ Using corrected logic (no future fundamental data)")
        
        successful = 0
        failed = 0
        total_metrics_created = 0
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"\n[{i}/{len(symbols)}] Recalculating {symbol}...")
            
            try:
                # Only recalculate the daily metrics (statements already exist)
                # Get price data range
                price_range = await backfill.db_conn.fetchrow("""
                    SELECT MIN(date) as start_date, MAX(date) as end_date
                    FROM daily_price_data WHERE symbol = $1
                """, symbol)
                
                if price_range and price_range['start_date']:
                    result = await backfill.calculate_historical_metrics(
                        symbol, price_range['start_date'], price_range['end_date']
                    )
                    
                    if result:
                        # Count metrics created for this symbol
                        metrics_count = await backfill.db_conn.fetchval(
                            "SELECT COUNT(*) FROM historical_fundamentals_daily WHERE symbol = $1", 
                            symbol
                        )
                        total_metrics_created += metrics_count
                        successful += 1
                        logger.info(f"   ✅ SUCCESS: {metrics_count:,} daily metrics created")
                    else:
                        failed += 1
                        logger.info(f"   ⚠️ No metrics created (no fundamental data overlap with prices)")
                else:
                    failed += 1
                    logger.info(f"   ❌ No price data found")
                    
                # Progress update every 20 symbols
                if i % 20 == 0:
                    elapsed = datetime.now() - start_time
                    remaining_symbols = len(symbols) - i
                    avg_time_per_symbol = elapsed.total_seconds() / i
                    estimated_remaining = remaining_symbols * avg_time_per_symbol / 60
                    
                    logger.info(f"\n📈 Progress Update:")
                    logger.info(f"   Processed: {i}/{len(symbols)} ({i/len(symbols)*100:.1f}%)")
                    logger.info(f"   Successful: {successful}")
                    logger.info(f"   Total daily metrics created: {total_metrics_created:,}")
                    logger.info(f"   Estimated remaining: {estimated_remaining:.1f} minutes")
                    
            except Exception as e:
                logger.error(f"   ❌ ERROR processing {symbol}: {e}")
                failed += 1
                
        # Final summary
        end_time = datetime.now()
        total_duration = end_time - start_time
        
        logger.info(f"\n🎯 REBUILD COMPLETE:")
        logger.info(f"   Total symbols processed: {len(symbols)}")
        logger.info(f"   Successful: {successful}")
        logger.info(f"   Failed/No data: {failed}")
        logger.info(f"   Success rate: {successful/len(symbols)*100:.1f}%")
        logger.info(f"   Total duration: {total_duration}")
        logger.info(f"   Total corrected daily metrics: {total_metrics_created:,}")
        
        # Verification
        logger.info(f"\n✅ VERIFICATION:")
        
        # Check for any corruption (future data used)
        corrupt_count = await backfill.db_conn.fetchval("""
            SELECT COUNT(*) FROM historical_fundamentals_daily
            WHERE financial_data_date > date
        """)
        
        if corrupt_count == 0:
            logger.info(f"   ✅ No corrupted data (future fundamentals used for past prices)")
        else:
            logger.error(f"   ❌ Found {corrupt_count} corrupted records!")
            
        # Show realistic date ranges
        sample_ranges = await backfill.db_conn.fetch("""
            SELECT 
                symbol,
                MIN(date) as earliest_metrics,
                MAX(date) as latest_metrics,
                COUNT(*) as daily_count
            FROM historical_fundamentals_daily
            GROUP BY symbol
            ORDER BY earliest_metrics
            LIMIT 10
        """)
        
        logger.info(f"   Sample realistic date ranges:")
        for row in sample_ranges:
            logger.info(f"     {row['symbol']}: {row['earliest_metrics']} to {row['latest_metrics']} ({row['daily_count']:,} days)")
            
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backfill.cleanup()
        logger.info("✅ Rebuild complete, database connection closed")

if __name__ == "__main__":
    asyncio.run(rebuild_corrected())