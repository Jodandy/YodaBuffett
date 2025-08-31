#!/usr/bin/env python3
"""
Historical Ingestion Batch Processor
Systematically processes all Swedish companies with full historical data collection

Features:
- Processes one company at a time (respectful to MFN.se)
- 90-second timeout per company
- Progress tracking with success/failure lists
- Resume capability (skip already completed)
- Detailed logging to files
"""
import asyncio
import aiohttp
import sys
import os
import json
import time
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Set
import logging

# Disable SQLAlchemy logging noise
logging.getLogger('sqlalchemy').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.engine').setLevel(logging.CRITICAL)
logging.getLogger('sqlalchemy.pool').setLevel(logging.CRITICAL)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events
from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from sqlalchemy import select

class HistoricalIngestionBatch:
    """
    Batch processor for historical financial document collection
    """
    
    def __init__(self):
        self.start_time = datetime.now()
        self.session_id = self.start_time.strftime("%Y%m%d_%H%M%S")
        
        # File paths
        self.results_file = f"historical_ingestion_{self.session_id}.json"
        self.log_file = f"historical_ingestion_{self.session_id}.log"
        
        # 15-minute timeout per company (extended for large document sets)
        self.company_timeout = 900  # 15 minutes
        
        # Optional: specific companies to target (if None, processes all)
        self.target_companies = None
        
        # Results tracking
        self.results = {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "completed": [],
            "failed": [],
            "skipped": [],
            "in_progress": None,
            "stats": {
                "total_companies": 0,
                "completed_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "total_documents": 0,
                "total_calendar_events": 0,
                "processing_time_seconds": 0
            }
        }
        
        # Will be loaded from database - ALL Swedish companies
        self.swedish_companies = []  # Will be populated from nordic_companies table
        
        # Graceful shutdown handling
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    async def load_swedish_companies_from_db(self):
        """Load ALL Swedish companies from nordic_companies table"""
        try:
            async with AsyncSessionLocal() as db:
                # Get all Swedish companies from database
                result = await db.execute(
                    select(NordicCompany.name, NordicCompany.ticker, NordicCompany.id)
                    .where(NordicCompany.country == 'SE')
                    .order_by(NordicCompany.name)
                )
                companies = result.fetchall()
                
                self.logger.info(f"📊 Found {len(companies)} Swedish companies in database")
                
                # Convert company names to MFN slugs
                for company_name, ticker, company_id in companies:
                    # Convert company name to MFN URL slug format
                    mfn_slug = self._convert_to_mfn_slug(company_name, ticker)
                    if mfn_slug:
                        self.swedish_companies.append({
                            'slug': mfn_slug,
                            'name': company_name,
                            'ticker': ticker,
                            'id': str(company_id)
                        })
                
                self.logger.info(f"✅ Mapped {len(self.swedish_companies)} companies to MFN slugs")
                
                # Show sample companies
                for i, company in enumerate(self.swedish_companies[:10]):
                    self.logger.info(f"   {i+1}. {company['name']} ({company['ticker']}) -> {company['slug']}")
                if len(self.swedish_companies) > 10:
                    self.logger.info(f"   ... and {len(self.swedish_companies) - 10} more companies")
                    
        except Exception as e:
            self.logger.error(f"❌ Error loading companies from database: {e}")
            # Fallback to hardcoded list
            self.swedish_companies = [
                {'slug': 'hexagon', 'name': 'Hexagon AB', 'ticker': 'HEXA B', 'id': 'fallback'}
            ]
            
    def _convert_to_mfn_slug(self, company_name: str, ticker: str) -> str:
        """Convert company name to MFN URL slug format"""
        # Use only TESTED, WORKING mappings - no more guesses!
        known_mappings = {
            # These are the ONLY ones we know actually work
            "AstraZeneca": "astrazeneca", 
            "Atlas Copco AB": "atlas-copco",
            "ABB Ltd": "abb",
            "Hexagon AB": "hexagon",
            "Nordea Bank Abp": "nordea",  # ✅ Worked - got 6 docs
            "Peab AB": "peab",            # ✅ Worked - got 8 docs
            # More can be added as we verify them
        }
        
        if company_name in known_mappings:
            return known_mappings[company_name]
        
        # Enhanced slug generation
        return self._generate_slug_from_name(company_name, ticker)
    
    def _generate_slug_from_name(self, company_name: str, ticker: str = None) -> str:
        """Enhanced slug generation with better Swedish company patterns"""
        if not company_name:
            return None
        
        slug = company_name.lower()
        
        # Remove common Swedish company suffixes
        import re
        suffixes_to_remove = [
            r'\s+ab$', r'\s+aktiebolag$', r'\s+aktiebolaget$',
            r'\s+group$', r'\s+holding$', r'\s+holdings$', 
            r'\s+ltd$', r'\s+plc$', r'\s+corp$', r'\s+corporation$',
            r'\s+inc$', r'\s+incorporated$', r'\s+company$',
            r'\s+publ$', r'\s+public$'
        ]
        
        for suffix in suffixes_to_remove:
            slug = re.sub(suffix, '', slug)
        
        # Remove common Swedish prefixes
        prefixes_to_remove = [
            r'^aktiebolaget\s+', r'^telefonaktiebolaget\s+',
            r'^fastighets\s+ab\s+', r'^svenska\s+'
        ]
        
        for prefix in prefixes_to_remove:
            slug = re.sub(prefix, '', slug)
        
        # Clean up special characters
        char_mappings = {
            'å': 'a', 'ä': 'a', 'ö': 'o',
            'é': 'e', 'è': 'e', 'ë': 'e',
            'ü': 'u', 'ú': 'u', 'ù': 'u',
            '&': 'and', '+': 'plus'
        }
        
        for old_char, new_char in char_mappings.items():
            slug = slug.replace(old_char, new_char)
        
        # Handle spaces and special characters
        slug = re.sub(r'[^\w\s-]', '', slug)  # Remove special chars except spaces and dashes
        slug = re.sub(r'\s+', '-', slug)      # Replace spaces with dashes
        slug = re.sub(r'-+', '-', slug)       # Remove multiple dashes
        slug = slug.strip('-')                # Remove leading/trailing dashes
        
        # If ticker provided, also try ticker-based approaches as fallback
        if ticker and (not slug or len(slug) <= 2):
            ticker_clean = ticker.replace(' A', '').replace(' B', '').replace(' SDB', '').replace(' PREF', '')
            ticker_slug = ticker_clean.lower().replace(' ', '-')
            if len(ticker_slug) > 1:
                slug = ticker_slug
        
        return slug if slug and len(slug) > 1 else None
        
    def _signal_handler(self, signum, frame):
        """Handle graceful shutdown"""
        self.logger.info(f"🛑 Shutdown signal received ({signum})")
        self.shutdown_requested = True
        
    def load_previous_results(self) -> bool:
        """Load results from previous run to enable resume"""
        try:
            # Look for most recent results file
            import glob
            result_files = glob.glob("historical_ingestion_*.json")
            if not result_files:
                return False
                
            latest_file = max(result_files, key=os.path.getctime)
            
            with open(latest_file, 'r') as f:
                previous_results = json.load(f)
                
            # Ask user if they want to resume
            completed_count = len(previous_results.get('completed', []))
            failed_count = len(previous_results.get('failed', []))
            
            print(f"📄 Found previous run: {latest_file}")
            print(f"   ✅ Completed: {completed_count}")
            print(f"   ❌ Failed: {failed_count}")
            
            resume = input("🔄 Resume from previous run? (y/n): ").lower().strip()
            if resume in ['y', 'yes']:
                # Load previous results but start fresh session
                completed_companies = set(previous_results.get('completed', []))
                failed_companies = set(previous_results.get('failed', []))
                
                # Skip companies that were already completed
                self.results['skipped'] = list(completed_companies)
                self.results['stats']['skipped_count'] = len(completed_companies)
                
                # Could retry failed companies or skip them
                retry_failed = input("🔄 Retry previously failed companies? (y/n): ").lower().strip()
                if retry_failed not in ['y', 'yes']:
                    self.results['skipped'].extend(failed_companies)
                    self.results['stats']['skipped_count'] += len(failed_companies)
                
                self.logger.info(f"📄 Resuming: {len(self.results['skipped'])} companies skipped")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Error loading previous results: {e}")
            
        return False
        
    def save_results(self):
        """Save current results to file"""
        try:
            self.results['stats']['processing_time_seconds'] = (datetime.now() - self.start_time).total_seconds()
            
            with open(self.results_file, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
                
            self.logger.info(f"💾 Results saved to {self.results_file}")
            
        except Exception as e:
            self.logger.error(f"❌ Error saving results: {e}")
            
    async def process_company(
        self, 
        session: aiohttp.ClientSession, 
        company: Dict[str, str]
    ) -> Dict[str, any]:
        """
        Process a single company with timeout and error handling
        
        Returns:
            Dict with processing results
        """
        start_time = time.time()
        
        try:
            company_name = f"{company['name']} ({company['ticker']})"
            company_slug = company['slug']
            
            self.logger.info(f"🏢 Processing {company_name}...")
            self.results['in_progress'] = company_slug
            self.save_results()  # Save progress
            
            collector = MFNCollector(rate_limit_delay=3.0)  # Conservative for batch
            
            # Collect news items - NO TIMEOUT, let it finish
            try:
                items = await collector.collect_company_news(session, company_slug, limit=480, full_backfill=True)
            except Exception as e:
                return {
                    "company": company_slug,
                    "company_name": company_name,
                    "success": False,
                    "error": f"Collection error: {str(e)}",
                    "failure_reason": "collection_error",
                    "documents": 0,
                    "events": 0,
                    "processing_time": time.time() - start_time
                }
            
            self.logger.info(f"📄 {company_name}: Found {len(items)} items")
            
            if not items:
                return {
                    "company": company_slug,
                    "company_name": company_name,
                    "success": False,
                    "error": "No items found - company may not exist on MFN or have no financial documents",
                    "failure_reason": "no_items_found",
                    "documents": 0,
                    "events": 0,
                    "processing_time": time.time() - start_time
                }
            
            # Store documents - NO TIMEOUT, let it finish
            try:
                storage_stats = await catalog_mfn_documents(items)
            except Exception as e:
                return {
                    "company": company_slug,
                    "company_name": company_name,
                    "success": False,
                    "error": f"Document storage error: {str(e)}",
                    "failure_reason": "storage_error",
                    "documents": 0,
                    "events": 0,
                    "processing_time": time.time() - start_time
                }
            
            # Store calendar events - NO TIMEOUT, let it finish
            try:
                calendar_stats = await store_mfn_calendar_events(items)
            except Exception as e:
                calendar_stats = {"calendar_events_created": 0, "dividend_events": 0, "errors": 1}
                self.logger.warning(f"⚠️  {company}: Calendar storage error: {e}")
            
            processing_time = time.time() - start_time
            
            result = {
                "company": company_slug,
                "company_name": company_name,
                "success": True,
                "error": None,
                "documents": storage_stats.get('stored', 0),
                "events": calendar_stats.get('calendar_events_created', 0),
                "processing_time": processing_time,
                "details": {
                    "items_found": len(items),
                    "documents_stored": storage_stats.get('stored', 0),
                    "document_duplicates": storage_stats.get('duplicates', 0),
                    "document_errors": storage_stats.get('errors', 0),
                    "calendar_events": calendar_stats.get('calendar_events_created', 0),
                    "dividend_events": calendar_stats.get('dividend_events', 0),
                    "calendar_errors": calendar_stats.get('errors', 0)
                }
            }
            
            self.logger.info(f"✅ {company_name}: {result['documents']} docs, {result['events']} events ({processing_time:.1f}s)")
            return result
            
        except Exception as e:
            processing_time = time.time() - start_time
            company_name = f"{company['name']} ({company['ticker']})" if 'name' in company else str(company)
            company_slug = company['slug'] if 'slug' in company else str(company)
            self.logger.error(f"❌ {company_name}: Unexpected error: {e}")
            
            return {
                "company": company_slug,
                "company_name": company_name,
                "success": False,
                "error": str(e),
                "failure_reason": "unexpected_error",
                "documents": 0,
                "events": 0,
                "processing_time": processing_time
            }
            
    async def run_batch_ingestion(self):
        """
        Run the complete batch ingestion process
        """
        self.logger.info(f"🚀 Starting Historical Ingestion Batch")
        
        # Load ALL Swedish companies from database
        await self.load_swedish_companies_from_db()
        
        self.logger.info(f"📊 Processing {len(self.swedish_companies)} Swedish companies")
        self.logger.info(f"📄 Limit: 480 documents per company (maximum historical coverage)")
        self.logger.info(f"⏱️  15-minute timeout per company (extended for large datasets)")
        self.logger.info(f"💾 Results will be saved to {self.results_file}")
        
        # Load previous results if available
        self.load_previous_results()
        
        self.results['stats']['total_companies'] = len(self.swedish_companies)
        
        # Get companies to process (excluding skipped ones)
        skipped_set = set(self.results['skipped'])
        companies_to_process = [c for c in self.swedish_companies if c['slug'] not in skipped_set]
        
        # Filter by target companies if specified
        if self.target_companies:
            target_set = set(self.target_companies)
            companies_to_process = [c for c in companies_to_process if c['slug'] in target_set]
            self.logger.info(f"🎯 Filtered to {len(companies_to_process)} target companies")
        
        self.logger.info(f"📋 Processing {len(companies_to_process)} companies (skipping {len(skipped_set)})")
        
        async with aiohttp.ClientSession(
            headers={
                'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            },
            timeout=aiohttp.ClientTimeout(total=self.company_timeout + 60)  # Add buffer
        ) as session:
            
            for i, company in enumerate(companies_to_process, 1):
                if self.shutdown_requested:
                    self.logger.info("🛑 Shutdown requested, stopping batch processing")
                    break
                
                company_display = f"{company['name']} ({company['ticker']})"
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"🏢 [{i}/{len(companies_to_process)}] Processing: {company_display}")
                self.logger.info(f"🔗 MFN URL: https://mfn.se/all/a/{company['slug']}?limit=480")
                self.logger.info(f"{'='*80}")
                
                try:
                    # Process with 5-minute timeout (safety measure)
                    result = await asyncio.wait_for(
                        self.process_company(session, company),
                        timeout=self.company_timeout
                    )
                    
                    if result['success']:
                        self.results['completed'].append(result)
                        self.results['stats']['completed_count'] += 1
                        self.results['stats']['total_documents'] += result['documents']
                        self.results['stats']['total_calendar_events'] += result['events']
                    else:
                        self.results['failed'].append(result)
                        self.results['stats']['failed_count'] += 1
                        
                except asyncio.TimeoutError:
                    company_name = f"{company['name']} ({company['ticker']})" if isinstance(company, dict) else str(company)
                    self.logger.error(f"❌ {company_name}: Overall timeout ({self.company_timeout}s)")
                    self.results['failed'].append({
                        "company": company['slug'] if isinstance(company, dict) else str(company),
                        "company_name": company_name,
                        "success": False,
                        "error": f"Overall timeout ({self.company_timeout}s) - large dataset processing",
                        "failure_reason": "timeout",
                        "documents": 0,
                        "events": 0,
                        "processing_time": self.company_timeout
                    })
                    self.results['stats']['failed_count'] += 1
                    
                except Exception as e:
                    company_name = f"{company['name']} ({company['ticker']})" if isinstance(company, dict) else str(company)
                    self.logger.error(f"❌ {company_name}: Critical error: {e}")
                    self.results['failed'].append({
                        "company": company['slug'] if isinstance(company, dict) else str(company),
                        "company_name": company_name,
                        "success": False,
                        "error": f"Critical error: {e}",
                        "failure_reason": "critical_error",
                        "documents": 0,
                        "events": 0,
                        "processing_time": 0
                    })
                    self.results['stats']['failed_count'] += 1
                
                # Clear in_progress
                self.results['in_progress'] = None
                
                # Save progress after each company
                self.save_results()
                
                # Extended pause between companies (be very respectful to MFN.se)
                if i < len(companies_to_process):
                    self.logger.info("⏱️  Waiting 15 seconds before next company...")
                    await asyncio.sleep(15)
            
        # Final results
        self.print_final_summary()
        self.save_results()
        
    def print_final_summary(self):
        """Print comprehensive final summary"""
        total_time = datetime.now() - self.start_time
        stats = self.results['stats']
        
        print(f"\n{'='*70}")
        print(f"🎉 HISTORICAL INGESTION COMPLETE")
        print(f"{'='*70}")
        print(f"⏱️  Total Time: {total_time}")
        print(f"📊 Total Companies: {stats['total_companies']}")
        print(f"✅ Completed: {stats['completed_count']}")
        print(f"❌ Failed: {stats['failed_count']}")
        print(f"⏭️  Skipped: {stats['skipped_count']}")
        print(f"📄 Total Documents: {stats['total_documents']}")
        print(f"📅 Total Calendar Events: {stats['total_calendar_events']}")
        
        print(f"\n📁 FILES GENERATED:")
        print(f"   📊 Results: {self.results_file}")
        print(f"   📝 Logs: {self.log_file}")
        
        if self.results['completed']:
            print(f"\n✅ SUCCESSFUL COMPANIES:")
            for result in self.results['completed']:
                company_name = result.get('company_name', result.get('company', 'Unknown'))
                print(f"   {company_name}: {result['documents']} docs, {result['events']} events ({result['processing_time']:.1f}s)")
        
        if self.results['failed']:
            print(f"\n❌ FAILED COMPANIES:")
            # Group by failure reason for better overview
            failure_groups = {}
            for result in self.results['failed']:
                reason = result.get('failure_reason', 'unknown')
                if reason not in failure_groups:
                    failure_groups[reason] = []
                failure_groups[reason].append(result)
            
            for reason, failures in failure_groups.items():
                print(f"\n   🔴 {reason.upper().replace('_', ' ')} ({len(failures)} companies):")
                for result in failures:
                    company_name = result.get('company_name', result.get('company', 'Unknown'))
                    print(f"      • {company_name}: {result.get('error', 'No details')}")
                
        print(f"\n💡 To retry failed companies, run this script again and choose 'resume'")

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Historical Ingestion Batch Processor')
    parser.add_argument('--companies', help='File with list of companies to process (one per line)')
    parser.add_argument('--delay', type=int, default=3, help='Delay between companies in seconds')
    
    args = parser.parse_args()
    
    batch_processor = HistoricalIngestionBatch()
    
    # If specific companies file provided, load it
    if args.companies:
        print(f"📄 Loading companies from: {args.companies}")
        try:
            with open(args.companies, 'r') as f:
                target_companies = [line.strip() for line in f if line.strip()]
            print(f"🎯 Targeting {len(target_companies)} specific companies")
            batch_processor.target_companies = target_companies
        except Exception as e:
            print(f"❌ Error loading companies file: {e}")
            return
    
    await batch_processor.run_batch_ingestion()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()