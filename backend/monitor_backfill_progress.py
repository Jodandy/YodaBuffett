#!/usr/bin/env python3
"""
Monitor Historical Fundamentals Backfill Progress

Tracks the progress of the full backfill and provides estimates.
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
import time

async def monitor_progress():
    """Monitor backfill progress and provide estimates."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print("🔍 HISTORICAL FUNDAMENTALS BACKFILL MONITOR")
    print("=" * 60)
    
    # Get baseline data
    start_time = datetime.now()
    
    while True:
        try:
            # Current progress
            statements = await conn.fetchval('SELECT COUNT(*) FROM financial_statements')
            balance_sheet = await conn.fetchval('SELECT COUNT(*) FROM balance_sheet_data') 
            cash_flow = await conn.fetchval('SELECT COUNT(*) FROM cash_flow_data')
            daily_metrics = await conn.fetchval('SELECT COUNT(*) FROM historical_fundamentals_daily')
            unique_symbols = await conn.fetchval('SELECT COUNT(DISTINCT symbol) FROM financial_statements')
            
            # Latest additions (companies processed in last 5 minutes)
            recent_companies = await conn.fetch('''
                SELECT DISTINCT symbol 
                FROM financial_statements 
                WHERE created_at > NOW() - INTERVAL '5 minutes'
                ORDER BY symbol
            ''')
            
            # Total target (744 companies from the log)
            target_companies = 744
            companies_processed = unique_symbols
            remaining = target_companies - companies_processed
            
            # Time estimates
            elapsed = datetime.now() - start_time
            if companies_processed > 0:
                avg_time_per_company = elapsed.total_seconds() / companies_processed
                estimated_remaining_seconds = remaining * avg_time_per_company
                estimated_completion = datetime.now() + timedelta(seconds=estimated_remaining_seconds)
            else:
                estimated_completion = None
                
            # Clear screen and show progress
            print("\033[2J\033[H")  # Clear screen
            print("🔍 HISTORICAL FUNDAMENTALS BACKFILL MONITOR")
            print("=" * 60)
            print(f"⏰ Started: {start_time.strftime('%H:%M:%S')}")
            print(f"⏱️  Elapsed: {str(elapsed).split('.')[0]}")
            print(f"📊 Current Time: {datetime.now().strftime('%H:%M:%S')}")
            
            print(f"\n📈 PROGRESS:")
            print(f"   Companies processed: {companies_processed:,} / {target_companies:,} ({companies_processed/target_companies*100:.1f}%)")
            print(f"   Companies remaining: {remaining:,}")
            
            if estimated_completion:
                print(f"   Estimated completion: {estimated_completion.strftime('%H:%M:%S')} ({estimated_remaining_seconds/60:.0f} minutes remaining)")
                print(f"   Average time per company: {avg_time_per_company:.1f} seconds")
            
            print(f"\n📊 DATA COLLECTED:")
            print(f"   Financial statements: {statements:,}")
            print(f"   Balance sheet records: {balance_sheet:,}")
            print(f"   Cash flow records: {cash_flow:,}")
            print(f"   Daily calculated metrics: {daily_metrics:,}")
            
            if recent_companies:
                recent_list = [r['symbol'] for r in recent_companies]
                print(f"\n🔄 Recently processed ({len(recent_list)} in last 5 min):")
                # Show in rows of 8
                for i in range(0, len(recent_list), 8):
                    row = recent_list[i:i+8]
                    print(f"   {' | '.join(row)}")
            
            # Progress bar
            progress_pct = companies_processed / target_companies
            bar_length = 50
            filled_length = int(bar_length * progress_pct)
            bar = "█" * filled_length + "░" * (bar_length - filled_length)
            print(f"\n[{bar}] {progress_pct*100:.1f}%")
            
            # Check if complete
            if companies_processed >= target_companies:
                print(f"\n🎉 BACKFILL COMPLETE!")
                print(f"   Total time: {elapsed}")
                print(f"   Companies processed: {companies_processed:,}")
                print(f"   Total records created: {statements + balance_sheet + cash_flow + daily_metrics:,}")
                break
                
            # Wait before next update
            await asyncio.sleep(30)  # Update every 30 seconds
            
        except KeyboardInterrupt:
            print(f"\n⏹️  Monitoring stopped by user")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            await asyncio.sleep(10)
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(monitor_progress())