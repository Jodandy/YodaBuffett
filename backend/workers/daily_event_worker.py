#!/usr/bin/env python3
"""
Production Daily Event Worker

Calendar-driven Swedish financial data collection worker that runs daily
to scrape companies with upcoming or recent financial events.

Features:
- Event-driven targeting using calendar database
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
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import logging
from dataclasses import asdict
from contextlib import asynccontextmanager
import traceback

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.event_scheduler import EventScheduler, ScheduledScrape
from workers.worker_config import get_config, setup_worker_logging
from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events

class DailyEventWorker:
    """
    Production worker for event-driven daily data collection
    """
    
    def __init__(self):
        self.config = get_config()
        self.logger = setup_worker_logging()
        
        self.scheduler = EventScheduler(
            look_ahead_days=self.config.scheduler.look_ahead_days,
            look_back_days=self.config.scheduler.look_back_days
        )
        
        self.collector = MFNCollector(
            rate_limit_delay=self.config.scraping.rate_limit_delay
        )
        
        # Execution state
        self.start_time = datetime.now()
        self.session_id = self.start_time.strftime("%Y%m%d_%H%M%S")
        self.is_running = False
        self.should_stop = False
        
        # Progress tracking
        self.results_file = f"{self.config.data_volume_path}/daily_worker_{self.session_id}.json"
        self.progress = {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "config": {
                "mode": self.config.mode.value,
                "look_ahead_days": self.config.scheduler.look_ahead_days,
                "look_back_days": self.config.scheduler.look_back_days,
                "rate_limit": self.config.scraping.rate_limit_delay
            },
            "scheduled_scrapes": [],
            "completed_scrapes": [],
            "failed_scrapes": [],
            "statistics": {
                "total_companies": 0,
                "successful_companies": 0,
                "failed_companies": 0,
                "total_documents": 0,
                "total_events": 0,
                "total_processing_time": 0
            }
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"📡 Received signal {signum}, initiating graceful shutdown...")
        self.should_stop = True
    
    async def run_daily_collection(self, target_date: Optional[date] = None) -> Dict:
        """
        Run the daily event-driven collection
        
        Args:
            target_date: Date to run collection for (defaults to today)
            
        Returns:
            Dictionary with execution results
        """
        if target_date is None:
            target_date = date.today()
            
        self.is_running = True
        self.logger.info(f"🚀 Starting daily event worker for {target_date}")
        self.logger.info(f"📋 Session ID: {self.session_id}")
        
        try:
            # Step 1: Get scheduled scrape targets
            self.logger.info("🗓️  Generating event-driven scrape targets...")
            scheduled_scrapes = await self.scheduler.get_daily_scrape_targets(target_date)
            
            if not scheduled_scrapes:
                self.logger.info("📭 No events scheduled for today - worker completing early")
                return await self._finalize_results("no_targets")
                
            self.progress["scheduled_scrapes"] = [asdict(scrape) for scrape in scheduled_scrapes]
            self.progress["statistics"]["total_companies"] = len(scheduled_scrapes)
            
            self.logger.info(f"🎯 Found {len(scheduled_scrapes)} companies to scrape")
            self._log_scrape_summary(scheduled_scrapes)
            
            # Step 2: Execute scraping with progress tracking
            await self._execute_scheduled_scrapes(scheduled_scrapes)
            
            # Step 3: Finalize and return results
            return await self._finalize_results("completed")
            
        except Exception as e:
            self.logger.error(f"❌ Daily worker failed: {e}")
            self.logger.error(traceback.format_exc())
            return await self._finalize_results("error", str(e))
        
        finally:
            self.is_running = False
            self.logger.info("🏁 Daily event worker execution completed")
    
    async def _execute_scheduled_scrapes(self, scheduled_scrapes: List[ScheduledScrape]):
        """Execute all scheduled scrapes with proper error handling"""
        
        async with aiohttp.ClientSession(
            headers=self.collector.session_headers,
            timeout=aiohttp.ClientTimeout(total=self.config.scraping.request_timeout)
        ) as session:
            
            for i, scrape in enumerate(scheduled_scrapes, 1):
                if self.should_stop:
                    self.logger.info("⏹️  Graceful shutdown requested, stopping scraping")
                    break
                
                self.logger.info(f"🏢 [{i}/{len(scheduled_scrapes)}] Processing {scrape.company_name}")
                self.logger.info(f"   📅 Event: {scrape.event_title} ({scrape.event_type})")
                self.logger.info(f"   🎯 Priority: {scrape.priority.value} | Reason: {scrape.reason}")
                
                scrape_start = time.time()
                
                try:
                    # Collect company data
                    company_slug = self._generate_company_slug(scrape.company_name)
                    items = await self.collector.collect_company_news(
                        session, 
                        company_slug, 
                        limit=200  # Get more items for event-driven scrapes
                    )
                    
                    if not items:
                        self.logger.warning(f"⚠️  No items found for {scrape.company_name}")
                        await self._record_failed_scrape(scrape, "no_items", scrape_start)
                        continue
                    
                    self.logger.info(f"📊 Collected {len(items)} items from MFN")
                    
                    # Store documents and events
                    doc_stats = await catalog_mfn_documents(items)
                    event_stats = await store_mfn_calendar_events(items)
                    
                    scrape_time = time.time() - scrape_start
                    
                    # Record successful scrape
                    success_record = {
                        "company_id": scrape.company_id,
                        "company_name": scrape.company_name,
                        "company_ticker": scrape.company_ticker,
                        "event_type": scrape.event_type,
                        "event_date": scrape.event_date.isoformat(),
                        "priority": scrape.priority.value,
                        "reason": scrape.reason,
                        "processing_time": scrape_time,
                        "items_collected": len(items),
                        "documents_stored": doc_stats.get('stored', 0),
                        "events_stored": event_stats.get('calendar_events_created', 0),
                        "completed_at": datetime.now().isoformat()
                    }
                    
                    self.progress["completed_scrapes"].append(success_record)
                    self.progress["statistics"]["successful_companies"] += 1
                    self.progress["statistics"]["total_documents"] += doc_stats.get('stored', 0)
                    self.progress["statistics"]["total_events"] += event_stats.get('calendar_events_created', 0)
                    self.progress["statistics"]["total_processing_time"] += scrape_time
                    
                    self.logger.info(f"✅ Completed {scrape.company_name} in {scrape_time:.1f}s")
                    self.logger.info(f"   📄 Documents: {doc_stats.get('stored', 0)} | Events: {event_stats.get('calendar_events_created', 0)}")
                    
                    # Save progress after each company
                    await self._save_progress()
                    
                    # Rate limiting
                    if i < len(scheduled_scrapes):  # Don't delay after last company
                        await asyncio.sleep(self.config.scraping.rate_limit_delay)
                
                except Exception as e:
                    self.logger.error(f"❌ Failed to process {scrape.company_name}: {e}")
                    self.logger.error(traceback.format_exc())
                    await self._record_failed_scrape(scrape, str(e), scrape_start)
    
    async def _record_failed_scrape(self, scrape: ScheduledScrape, error: str, start_time: float):
        """Record failed scrape attempt"""
        failure_record = {
            "company_id": scrape.company_id,
            "company_name": scrape.company_name,
            "company_ticker": scrape.company_ticker,
            "event_type": scrape.event_type,
            "event_date": scrape.event_date.isoformat(),
            "priority": scrape.priority.value,
            "reason": scrape.reason,
            "error": error,
            "processing_time": time.time() - start_time,
            "failed_at": datetime.now().isoformat()
        }
        
        self.progress["failed_scrapes"].append(failure_record)
        self.progress["statistics"]["failed_companies"] += 1
    
    def _generate_company_slug(self, company_name: str) -> str:
        """Generate MFN-compatible company slug from company name"""
        # Simple slug generation - convert to lowercase and replace spaces/special chars
        slug = company_name.lower()
        slug = slug.replace(' ', '-')
        slug = slug.replace('&', 'and')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        return slug.strip('-')
    
    def _log_scrape_summary(self, scrapes: List[ScheduledScrape]):
        """Log summary of scheduled scrapes"""
        
        by_priority = {}
        by_event_type = {}
        
        for scrape in scrapes:
            # Count by priority
            priority = scrape.priority.value
            by_priority[priority] = by_priority.get(priority, 0) + 1
            
            # Count by event type
            event_type = scrape.event_type
            by_event_type[event_type] = by_event_type.get(event_type, 0) + 1
        
        self.logger.info("📊 Scrape Target Summary:")
        self.logger.info(f"   By Priority: {dict(by_priority)}")
        self.logger.info(f"   By Event Type: {dict(by_event_type)}")
        
        # Log first few companies for visibility
        self.logger.info("🎯 Top Priority Targets:")
        for scrape in scrapes[:5]:
            self.logger.info(f"   🏢 {scrape.company_name} - {scrape.event_title}")
    
    async def _save_progress(self):
        """Save current progress to file"""
        try:
            os.makedirs(os.path.dirname(self.results_file), exist_ok=True)
            with open(self.results_file, 'w') as f:
                json.dump(self.progress, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"⚠️  Failed to save progress: {e}")
    
    async def _finalize_results(self, status: str, error_message: str = None) -> Dict:
        """Finalize execution and return results"""
        
        end_time = datetime.now()
        total_time = (end_time - self.start_time).total_seconds()
        
        final_results = {
            "session_id": self.session_id,
            "status": status,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "total_execution_time": total_time,
            "statistics": self.progress["statistics"],
            "results_file": self.results_file
        }
        
        if error_message:
            final_results["error"] = error_message
        
        # Update progress with final results
        self.progress.update(final_results)
        await self._save_progress()
        
        # Log final summary
        stats = self.progress["statistics"]
        self.logger.info("📈 Daily Worker Execution Summary:")
        self.logger.info(f"   Status: {status}")
        self.logger.info(f"   Total Companies: {stats['total_companies']}")
        self.logger.info(f"   Successful: {stats['successful_companies']}")
        self.logger.info(f"   Failed: {stats['failed_companies']}")
        self.logger.info(f"   Documents Stored: {stats['total_documents']}")
        self.logger.info(f"   Events Created: {stats['total_events']}")
        self.logger.info(f"   Total Time: {total_time:.1f} seconds")
        
        return final_results

# Health check and monitoring
async def health_check():
    """Simple health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "worker_type": "daily-event-worker",
        "version": "1.0.0"
    }

# CLI interface
async def main():
    """Main CLI interface for the daily worker"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Daily Event-Driven Data Collection Worker')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--dry-run', action='store_true', help='Show targets without scraping')
    
    args = parser.parse_args()
    
    # Parse target date
    target_date = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD")
            return 1
    
    worker = DailyEventWorker()
    
    if args.dry_run:
        # Dry run - show targets only
        print("🧪 DRY RUN MODE - Showing targets without scraping")
        print("=" * 60)
        
        scheduler = EventScheduler()
        targets = await scheduler.get_daily_scrape_targets(target_date)
        
        if not targets:
            print("📭 No events scheduled for target date")
            return 0
            
        print(f"🎯 Found {len(targets)} companies scheduled for scraping:")
        for target in targets:
            print(f"  🏢 {target.company_name} ({target.company_ticker})")
            print(f"     📅 Event: {target.event_title} on {target.event_date}")
            print(f"     🎯 Priority: {target.priority.value} | Reason: {target.reason}")
            print()
        
        return 0
    
    # Production run
    results = await worker.run_daily_collection(target_date)
    
    # Return appropriate exit code
    if results.get("status") == "completed":
        return 0
    else:
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⏹️  Worker interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Worker failed: {e}")
        sys.exit(1)