#!/usr/bin/env python3
"""
Historical Document Catch-Up Runner

Comprehensive document collection to catch up on missed daily updates.
Re-runs the full scraping process for a specified time period.
"""

import asyncio
import aiohttp
import sys
import os
from datetime import datetime, date, timedelta
from typing import Optional
import json
import logging

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from workers.event_scheduler import EventScheduler
from workers.worker_config import get_config, setup_worker_logging
from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events

class HistoricalDocumentCatchup:
    """
    Comprehensive catch-up for missed document collection periods
    """
    
    def __init__(self, start_date: date, end_date: Optional[date] = None):
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.config = get_config()
        self.logger = setup_worker_logging()
        
        # Statistics tracking
        self.stats = {
            'start_time': None,
            'end_time': None,
            'companies_processed': 0,
            'documents_found': 0,
            'calendar_events_found': 0,
            'errors': 0,
            'daily_summaries': []
        }
    
    async def run_catchup(self):
        """Run comprehensive catch-up for the specified date range"""
        
        self.logger.info(f"🚀 Starting Historical Document Catch-Up")
        self.logger.info(f"📅 Date Range: {self.start_date} to {self.end_date}")
        self.logger.info(f"📊 Period: {(self.end_date - self.start_date).days} days")
        
        self.stats['start_time'] = datetime.now().isoformat()
        
        # Calculate daily catch-up approach
        current_date = self.start_date
        day_count = 0
        
        while current_date <= self.end_date:
            day_count += 1
            daily_stats = await self.process_single_day(current_date, day_count)
            self.stats['daily_summaries'].append(daily_stats)
            
            current_date += timedelta(days=1)
            
            # Small delay between days to be respectful to servers
            await asyncio.sleep(2)
        
        self.stats['end_time'] = datetime.now().isoformat()
        
        # Save comprehensive results
        await self.save_results()
        await self.print_summary()
    
    async def process_single_day(self, target_date: date, day_number: int) -> dict:
        """Process documents for a single day"""
        
        total_days = (self.end_date - self.start_date).days + 1
        self.logger.info(f"\n[Day {day_number:3}/{total_days}] Processing {target_date}")
        
        daily_stats = {
            'date': target_date.isoformat(),
            'companies_processed': 0,
            'documents_found': 0,
            'calendar_events': 0,
            'errors': 0,
            'processing_time_minutes': 0
        }
        
        day_start_time = datetime.now()
        
        try:
            # Use the event scheduler to find companies with events on this date
            scheduler = EventScheduler(
                look_ahead_days=0,  # Only this specific day
                look_back_days=0
            )
            
            scheduled_scrapes = await scheduler.get_daily_scrape_targets(target_date)
            
            if not scheduled_scrapes:
                self.logger.info(f"           📋 No companies with events on {target_date}")
                return daily_stats
            
            self.logger.info(f"           📋 Found {len(scheduled_scrapes)} companies with events")
            
            # Process each company with events on this date using the actual MFN collector approach
            async with aiohttp.ClientSession() as session:
                for i, scrape in enumerate(scheduled_scrapes, 1):
                    try:
                        company_name = scrape.company_name
                        
                        self.logger.info(f"           [{i:2}/{len(scheduled_scrapes)}] {company_name[:40]:40}")
                        self.logger.info(f"                     📅 Event: {scrape.event_title} ({scrape.event_type})")
                        
                        # Generate company slug for MFN.se
                        company_slug = self._generate_company_slug(company_name)
                        
                        # Use MFNCollector to collect company news
                        collector = MFNCollector()
                        items = await collector.collect_company_news(
                            session, 
                            company_slug, 
                            limit=200  # Get more items for event-driven scrapes
                        )
                        
                        if items:
                            # Store documents in database - catalog_mfn_documents expects MFNNewsItem objects directly
                            catalog_stats = await catalog_mfn_documents(items)
                            documents_stored = catalog_stats.get('stored', 0)
                            daily_stats['documents_found'] += documents_stored
                            
                            self.logger.info(f"                     ✅ {len(items)} items found, {documents_stored} cataloged")
                        else:
                            self.logger.info(f"                     📄 No new items found")
                        
                        daily_stats['companies_processed'] += 1
                        
                        # Rate limiting
                        await asyncio.sleep(1.5)
                        
                    except Exception as e:
                        self.logger.error(f"                     ❌ Error processing {company_name}: {e}")
                        daily_stats['errors'] += 1
            
            # Update global stats
            self.stats['companies_processed'] += daily_stats['companies_processed']
            self.stats['documents_found'] += daily_stats['documents_found']
            self.stats['calendar_events_found'] += daily_stats['calendar_events']
            self.stats['errors'] += daily_stats['errors']
            
        except Exception as e:
            self.logger.error(f"           ❌ Day processing error: {e}")
            daily_stats['errors'] += 1
        
        # Calculate processing time
        daily_stats['processing_time_minutes'] = (datetime.now() - day_start_time).total_seconds() / 60
        
        self.logger.info(f"           📊 Day {day_number} summary: {daily_stats['companies_processed']} companies, {daily_stats['documents_found']} docs, {daily_stats['errors']} errors")
        
        return daily_stats
    
    def _generate_company_slug(self, company_name: str) -> str:
        """Generate company slug for MFN.se URLs (same logic as daily worker)"""
        import re
        
        # Convert to lowercase and replace spaces/special chars with hyphens
        slug = company_name.lower()
        slug = re.sub(r'[^\w\s-]', '', slug)  # Remove special characters
        slug = re.sub(r'[-\s]+', '-', slug)   # Replace spaces/hyphens with single hyphen
        slug = slug.strip('-')                # Remove leading/trailing hyphens
        
        return slug
    
    async def save_results(self):
        """Save comprehensive catch-up results"""
        
        results_file = f"historical_catchup_{self.start_date}_{self.end_date}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        complete_results = {
            'metadata': {
                'start_date': self.start_date.isoformat(),
                'end_date': self.end_date.isoformat(),
                'total_days': (self.end_date - self.start_date).days + 1,
                'run_timestamp': datetime.now().isoformat()
            },
            'summary': self.stats,
            'daily_details': self.stats['daily_summaries']
        }
        
        with open(results_file, 'w') as f:
            json.dump(complete_results, f, indent=2, default=str)
        
        self.logger.info(f"💾 Complete results saved to: {results_file}")
        return results_file
    
    async def print_summary(self):
        """Print comprehensive summary"""
        
        duration_minutes = 0
        if self.stats['start_time'] and self.stats['end_time']:
            start_dt = datetime.fromisoformat(self.stats['start_time'])
            end_dt = datetime.fromisoformat(self.stats['end_time'])
            duration_minutes = (end_dt - start_dt).total_seconds() / 60
        
        print(f"\n" + "=" * 80)
        print(f"🎉 HISTORICAL DOCUMENT CATCH-UP COMPLETE!")
        print(f"=" * 80)
        print(f"📅 Period: {self.start_date} → {self.end_date} ({(self.end_date - self.start_date).days + 1} days)")
        print(f"🏢 Companies Processed: {self.stats['companies_processed']}")
        print(f"📄 Documents Found: {self.stats['documents_found']}")
        print(f"📅 Calendar Events: {self.stats['calendar_events_found']}")
        print(f"❌ Errors: {self.stats['errors']}")
        print(f"⏱️  Total Duration: {duration_minutes:.1f} minutes")
        
        if self.stats['companies_processed'] > 0:
            print(f"📊 Average: {self.stats['documents_found']/self.stats['companies_processed']:.1f} docs per company")
        
        # Show top days by activity
        top_days = sorted(self.stats['daily_summaries'], key=lambda x: x['documents_found'], reverse=True)[:5]
        
        print(f"\n📈 Most Active Days:")
        for day in top_days:
            if day['documents_found'] > 0:
                print(f"   {day['date']}: {day['documents_found']} documents from {day['companies_processed']} companies")
        
        print(f"\n💡 Next Steps:")
        print(f"   1. Restart daily updater: docker-compose up daily-event-scheduler -d")
        print(f"   2. Verify document embeddings are current")
        print(f"   3. Run temporal anomaly analysis with complete dataset")

async def main():
    """Main execution with CLI arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Historical Document Catch-Up")
    parser.add_argument('--start-date', type=str, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, 
                       help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--days-back', type=int,
                       help='Number of days back from today (alternative to start-date)')
    
    args = parser.parse_args()
    
    # Parse dates - either --days-back OR --start-date is required
    if args.days_back:
        start_date = date.today() - timedelta(days=args.days_back)
        end_date = date.today()
    elif args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else date.today()
    else:
        print("❌ Error: Either --days-back or --start-date must be provided")
        print("\nExamples:")
        print("  python3 historical_document_catchup.py --days-back 7")
        print("  python3 historical_document_catchup.py --start-date 2024-11-25")
        return
    
    # Validate date range
    if start_date > end_date:
        print("❌ Error: Start date must be before end date")
        return
    
    if start_date > date.today():
        print("❌ Error: Start date cannot be in the future")
        return
    
    # Run catch-up
    catchup = HistoricalDocumentCatchup(start_date, end_date)
    await catchup.run_catchup()

if __name__ == "__main__":
    print("📚 HISTORICAL DOCUMENT CATCH-UP SYSTEM")
    print("Comprehensive document collection for missed periods")
    print("=" * 60)
    print("")
    print("Usage examples:")
    print("  python3 historical_document_catchup.py --days-back 30")
    print("  python3 historical_document_catchup.py --start-date 2024-11-01 --end-date 2024-12-01")
    print("  python3 historical_document_catchup.py --start-date 2024-10-15")
    print("")
    
    asyncio.run(main())