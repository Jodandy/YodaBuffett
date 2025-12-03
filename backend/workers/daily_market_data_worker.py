#!/usr/bin/env python3
"""
Production Daily Market Data Worker

Daily market data collection worker that runs every day to update
price data for all companies in the company_master table.

Features:
- Daily price data updates for all companies
- Incremental updates (only fetch recent data)
- Production logging and monitoring
- Health check endpoint
- Graceful error handling and recovery
- Docker-friendly configuration
- Progress persistence for resume capability
"""

import asyncio
import aiohttp
import sys
import os
import json
import signal
import time
import schedule
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import logging
from contextlib import asynccontextmanager
import traceback

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from workers.worker_config import get_config, setup_worker_logging
from domains.market_data.services.historical_data_ingestor import HistoricalDataIngestor

class DailyMarketDataWorker:
    """
    Production worker for daily market data updates
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = setup_worker_logging()
        
        self.ingestor = HistoricalDataIngestor()
        self.health_server = None
        self.is_running = False
        self.shutdown_requested = False
        
        # Statistics
        self.stats = {
            'total_runs': 0,
            'successful_companies': 0,
            'failed_companies': 0,
            'last_run': None,
            'last_success': None,
            'next_scheduled_run': None
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
    
    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"📡 Received shutdown signal {signum}")
        self.shutdown_requested = True
    
    async def start_health_server(self):
        """Start HTTP health check server"""
        from aiohttp import web, web_runner
        
        async def health_check(request):
            """Health check endpoint"""
            status = {
                'status': 'healthy' if not self.shutdown_requested else 'shutting_down',
                'worker_type': 'daily_market_data_worker',
                'is_running': self.is_running,
                'stats': self.stats,
                'timestamp': datetime.now().isoformat()
            }
            return web.json_response(status)
        
        app = web.Application()
        app.router.add_get('/health', health_check)
        
        # Start server
        runner = web_runner.AppRunner(app)
        await runner.setup()
        
        port = int(os.getenv('HEALTH_CHECK_PORT', 8086))
        site = web_runner.TCPSite(runner, '0.0.0.0', port)
        await site.start()
        
        self.logger.info(f"🏥 Health check server started on port {port}")
        return runner
    
    async def get_companies_for_daily_update(self) -> List[Dict]:
        """Get all companies that need daily market data updates"""
        
        await self.ingestor.connect()
        
        try:
            # Get all companies from company_master with valid tickers
            companies = await self.ingestor.conn.fetch("""
                SELECT 
                    id,
                    company_name,
                    primary_ticker,
                    yahoo_symbol,
                    document_count,
                    symbol_confidence,
                    country,
                    updated_at
                FROM company_master
                WHERE primary_ticker IS NOT NULL
                AND yahoo_symbol IS NOT NULL
                AND yahoo_symbol LIKE '%.ST'  -- Focus on Swedish companies for now
                ORDER BY 
                    document_count DESC NULLS LAST,
                    symbol_confidence DESC
            """)
            
            return [dict(row) for row in companies]
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching companies: {e}")
            return []
    
    async def update_single_company_data(self, company: Dict) -> Dict:
        """Update market data for a single company"""
        
        result = {
            'company_name': company['company_name'],
            'ticker': company['primary_ticker'],
            'yahoo_symbol': company['yahoo_symbol'],
            'success': False,
            'price_points_added': 0,
            'error': None,
            'processing_time': 0
        }
        
        start_time = time.time()
        
        try:
            # Get recent market data (last 7 days to catch weekends/holidays)
            success = await self.ingestor.ingest_historical_data(
                symbol=company['primary_ticker'],
                days_back=7,  # Only get recent data
                calculate_metrics=True
            )
            
            if success:
                # Count how many price points we have for this company
                price_count = await self.ingestor.conn.fetchval("""
                    SELECT COUNT(*) 
                    FROM daily_price_data 
                    WHERE symbol = $1
                """, company['primary_ticker'])
                
                result['success'] = True
                result['price_points_total'] = price_count
                self.logger.info(f"           ✅ Updated {company['primary_ticker']} (now has {price_count} total points)")
            else:
                result['error'] = "No data retrieved from Yahoo Finance"
                self.logger.warning(f"           ❌ Failed to update {company['primary_ticker']}")
        
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"           ❌ Error updating {company['primary_ticker']}: {e}")
        
        result['processing_time'] = time.time() - start_time
        return result
    
    async def run_daily_update(self) -> Dict:
        """Run the daily market data update for all companies"""
        
        self.logger.info(f"🚀 Starting Daily Market Data Update")
        self.logger.info(f"📅 Date: {date.today()}")
        self.stats['last_run'] = datetime.now().isoformat()
        
        run_stats = {
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'companies_processed': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'total_companies': 0,
            'duration_minutes': 0,
            'company_results': []
        }
        
        try:
            # Get companies to update
            companies = await self.get_companies_for_daily_update()
            run_stats['total_companies'] = len(companies)
            
            self.logger.info(f"📊 Found {len(companies)} companies to update")
            
            if not companies:
                self.logger.warning("⚠️ No companies found for update")
                return run_stats
            
            # Process each company
            for i, company in enumerate(companies, 1):
                if self.shutdown_requested:
                    self.logger.info("🛑 Shutdown requested - stopping updates")
                    break
                
                doc_count = company.get('document_count', 0) or 0
                priority_icon = "🔥" if doc_count >= 50 else "📈" if doc_count >= 20 else "📄"
                
                self.logger.info(f"[{i:3}/{len(companies)}] {priority_icon} {company['primary_ticker']:8} | {company['company_name'][:40]:40}")
                
                # Update company data
                result = await self.update_single_company_data(company)
                run_stats['company_results'].append(result)
                run_stats['companies_processed'] += 1
                
                if result['success']:
                    run_stats['successful_updates'] += 1
                    self.stats['successful_companies'] += 1
                else:
                    run_stats['failed_updates'] += 1
                    self.stats['failed_companies'] += 1
                
                # Progress update every 25 companies
                if i % 25 == 0:
                    success_rate = run_stats['successful_updates'] / run_stats['companies_processed'] * 100
                    self.logger.info(f"📊 Progress: {i}/{len(companies)} ({i/len(companies)*100:.1f}%) | Success: {success_rate:.1f}%")
                
                # Small delay to be respectful to Yahoo Finance
                await asyncio.sleep(1)
            
        except Exception as e:
            self.logger.error(f"❌ Critical error during daily update: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        finally:
            await self.ingestor.disconnect()
        
        # Finalize stats
        run_stats['end_time'] = datetime.now().isoformat()
        if run_stats['start_time']:
            start_dt = datetime.fromisoformat(run_stats['start_time'])
            end_dt = datetime.fromisoformat(run_stats['end_time'])
            run_stats['duration_minutes'] = (end_dt - start_dt).total_seconds() / 60
        
        # Update worker stats
        self.stats['total_runs'] += 1
        if run_stats['successful_updates'] > 0:
            self.stats['last_success'] = run_stats['end_time']
        
        # Save results
        await self.save_run_results(run_stats)
        await self.print_run_summary(run_stats)
        
        return run_stats
    
    async def save_run_results(self, run_stats: Dict):
        """Save detailed results of the daily run"""
        
        results_file = f"daily_market_data_{date.today().strftime('%Y%m%d')}_{datetime.now().strftime('%H%M%S')}.json"
        
        # Add metadata
        complete_results = {
            'metadata': {
                'worker_type': 'daily_market_data_worker',
                'run_date': date.today().isoformat(),
                'timestamp': datetime.now().isoformat()
            },
            'run_summary': run_stats,
            'worker_lifetime_stats': self.stats
        }
        
        data_dir = os.getenv('DATA_VOLUME_PATH', 'data')
        os.makedirs(data_dir, exist_ok=True)
        results_path = os.path.join(data_dir, results_file)
        
        with open(results_path, 'w') as f:
            json.dump(complete_results, f, indent=2, default=str)
        
        self.logger.info(f"💾 Results saved to: {results_path}")
    
    async def print_run_summary(self, run_stats: Dict):
        """Print summary of the daily run"""
        
        print(f"\n" + "=" * 80)
        print(f"📈 DAILY MARKET DATA UPDATE COMPLETE")
        print(f"=" * 80)
        print(f"📅 Date: {date.today()}")
        print(f"📊 Companies Processed: {run_stats['companies_processed']}/{run_stats['total_companies']}")
        print(f"✅ Successful Updates: {run_stats['successful_updates']}")
        print(f"❌ Failed Updates: {run_stats['failed_updates']}")
        print(f"📈 Success Rate: {run_stats['successful_updates']/max(run_stats['companies_processed'],1)*100:.1f}%")
        print(f"⏱️  Duration: {run_stats['duration_minutes']:.1f} minutes")
        
        if run_stats['failed_updates'] > 0:
            print(f"\n❌ Failed Companies:")
            failed_companies = [r for r in run_stats['company_results'] if not r['success']]
            for company in failed_companies[:5]:  # Show first 5 failures
                print(f"   {company['ticker']:8} | {company['company_name'][:40]:40} | {company['error'][:30]}")
            if len(failed_companies) > 5:
                print(f"   ... and {len(failed_companies) - 5} more (see JSON results for full list)")
        
        print(f"\n💡 Next scheduled run: Tomorrow at {os.getenv('DAILY_RUN_TIME', '06:00')}")
    
    def schedule_daily_runs(self):
        """Setup daily scheduling"""
        
        run_time = os.getenv('DAILY_RUN_TIME', '06:00')
        self.stats['next_scheduled_run'] = f"Daily at {run_time}"
        
        schedule.every().day.at(run_time).do(
            lambda: asyncio.create_task(self.run_daily_update())
        )
        
        self.logger.info(f"📅 Scheduled daily market data updates at {run_time}")
    
    async def run_scheduler_loop(self):
        """Main scheduler loop"""
        
        self.logger.info(f"🔄 Starting daily market data scheduler loop")
        self.is_running = True
        
        # Setup daily scheduling
        self.schedule_daily_runs()
        
        # Start health check server
        health_runner = await self.start_health_server()
        
        try:
            while not self.shutdown_requested:
                # Run pending scheduled jobs
                schedule.run_pending()
                
                # Sleep for a minute before checking again
                await asyncio.sleep(60)
                
        except Exception as e:
            self.logger.error(f"❌ Error in scheduler loop: {e}")
        
        finally:
            self.is_running = False
            if health_runner:
                await health_runner.cleanup()
            self.logger.info("🛑 Daily market data scheduler stopped")

async def main():
    """Main entry point"""
    
    print("📈 DAILY MARKET DATA WORKER")
    print("Production daily market data updates for all companies")
    print("=" * 60)
    
    # Check if we should run immediately or start scheduler
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        worker = DailyMarketDataWorker()
        
        if command == "--run-now":
            print("🚀 Running market data update immediately")
            await worker.run_daily_update()
        elif command == "--dry-run":
            print("🧪 Dry run - checking companies to update")
            companies = await worker.get_companies_for_daily_update()
            print(f"📊 Found {len(companies)} companies for daily updates:")
            for i, company in enumerate(companies[:10], 1):
                print(f"   {i:2}. {company['primary_ticker']:8} | {company['company_name'][:40]:40}")
            if len(companies) > 10:
                print(f"   ... and {len(companies) - 10} more companies")
        else:
            print("❌ Unknown command. Use --run-now or --dry-run or run without args for scheduler")
    else:
        # Start the daily scheduler
        worker = DailyMarketDataWorker()
        await worker.run_scheduler_loop()

if __name__ == "__main__":
    asyncio.run(main())