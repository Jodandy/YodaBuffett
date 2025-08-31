#!/usr/bin/env python3
"""
Weekly Surprise Scanner Worker

Broad-spectrum scanner for detecting unexpected news and announcements
from Swedish companies that haven't had recent calendar events.

Features:
- Targets companies with no recent activity
- Randomized sampling to avoid predictable patterns  
- Lower priority scanning for resource efficiency
- Surprise detection algorithms
- Production monitoring and logging
"""

import asyncio
import aiohttp
import sys
import os
import json
import time
import random
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional
import logging
from dataclasses import asdict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.event_scheduler import EventScheduler, ScheduledScrape, CompanyTier
from workers.worker_config import get_config, setup_worker_logging
from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events

class WeeklyScanner:
    """
    Weekly surprise news scanner for comprehensive monitoring
    """
    
    def __init__(self, sample_size: int = 50):
        self.config = get_config()
        self.logger = setup_worker_logging()
        self.sample_size = sample_size
        
        self.scheduler = EventScheduler()
        self.collector = MFNCollector(
            rate_limit_delay=self.config.scraping.rate_limit_delay * 1.5  # Slower for weekly
        )
        
        # Execution state
        self.start_time = datetime.now()
        self.session_id = f"weekly_{self.start_time.strftime('%Y%m%d_%H%M%S')}"
        
        # Progress tracking
        self.results_file = f"{self.config.data_volume_path}/weekly_scanner_{self.session_id}.json"
        self.progress = {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "scanner_type": "weekly_surprise",
            "config": {
                "sample_size": self.sample_size,
                "quiet_period_days": 14,
                "rate_limit": self.config.scraping.rate_limit_delay * 1.5
            },
            "target_companies": [],
            "surprise_discoveries": [],
            "completed_scans": [],
            "statistics": {
                "companies_scanned": 0,
                "surprises_found": 0,
                "new_documents": 0,
                "new_events": 0,
                "total_processing_time": 0
            }
        }
    
    async def run_weekly_scan(self) -> Dict:
        """
        Execute the weekly surprise scanning
        
        Returns:
            Dictionary with execution results
        """
        self.logger.info(f"🎲 Starting weekly surprise scanner")
        self.logger.info(f"📋 Session ID: {self.session_id}")
        self.logger.info(f"🎯 Target sample size: {self.sample_size}")
        
        try:
            # Step 1: Get surprise scan targets
            self.logger.info("🔍 Identifying companies for surprise scanning...")
            targets = await self.scheduler.get_weekly_surprise_targets(self.sample_size)
            
            if not targets:
                self.logger.info("📭 No companies identified for surprise scanning")
                return await self._finalize_results("no_targets")
            
            self.progress["target_companies"] = [asdict(target) for target in targets]
            self.logger.info(f"🎯 Selected {len(targets)} companies for surprise scanning")
            self._log_target_summary(targets)
            
            # Step 2: Execute surprise scanning
            await self._execute_surprise_scans(targets)
            
            # Step 3: Analyze for surprises
            await self._analyze_surprises()
            
            # Step 4: Finalize results
            return await self._finalize_results("completed")
            
        except Exception as e:
            self.logger.error(f"❌ Weekly scanner failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return await self._finalize_results("error", str(e))
    
    async def _execute_surprise_scans(self, targets: List[ScheduledScrape]):
        """Execute surprise scans for all target companies"""
        
        async with aiohttp.ClientSession(
            headers=self.collector.session_headers,
            timeout=aiohttp.ClientTimeout(total=self.config.scraping.request_timeout)
        ) as session:
            
            for i, target in enumerate(targets, 1):
                self.logger.info(f"🏢 [{i}/{len(targets)}] Scanning {target.company_name}")
                
                scan_start = time.time()
                
                try:
                    # Generate company slug for MFN
                    company_slug = self._generate_company_slug(target.company_name)
                    
                    # Collect recent items (last 30 days)
                    items = await self.collector.collect_company_news(
                        session, 
                        company_slug, 
                        limit=50  # Smaller limit for surprise scans
                    )
                    
                    scan_time = time.time() - scan_start
                    
                    if not items:
                        self.logger.info(f"   📭 No recent items found")
                        await self._record_scan_result(target, 0, 0, 0, scan_time)
                        continue
                    
                    # Filter for recent items only (last 30 days)
                    recent_items = self._filter_recent_items(items, days=30)
                    
                    if not recent_items:
                        self.logger.info(f"   📅 No items from last 30 days")
                        await self._record_scan_result(target, len(items), 0, 0, scan_time)
                        continue
                    
                    self.logger.info(f"   📊 Found {len(recent_items)} recent items (from {len(items)} total)")
                    
                    # Store documents and events
                    doc_stats = await catalog_mfn_documents(recent_items)
                    event_stats = await store_mfn_calendar_events(recent_items)
                    
                    new_docs = doc_stats.get('stored', 0)
                    new_events = event_stats.get('calendar_events_created', 0)
                    
                    # Check if this qualifies as a "surprise" (new activity)
                    if new_docs > 0 or new_events > 0:
                        await self._record_surprise_discovery(target, recent_items, new_docs, new_events)
                    
                    await self._record_scan_result(target, len(items), new_docs, new_events, scan_time)
                    
                    self.logger.info(f"   ✅ Completed scan in {scan_time:.1f}s | Docs: {new_docs} | Events: {new_events}")
                    
                    # Rate limiting between companies
                    if i < len(targets):
                        await asyncio.sleep(self.config.scraping.rate_limit_delay * 1.5)
                
                except Exception as e:
                    self.logger.error(f"❌ Failed to scan {target.company_name}: {e}")
                    await self._record_scan_result(target, 0, 0, 0, time.time() - scan_start, str(e))
    
    def _filter_recent_items(self, items, days: int = 30):
        """Filter items to only include recent ones"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_items = []
        for item in items:
            if item.date_published and item.date_published >= cutoff_date:
                recent_items.append(item)
        
        return recent_items
    
    async def _record_surprise_discovery(self, target: ScheduledScrape, items, new_docs: int, new_events: int):
        """Record a surprise discovery"""
        
        # Analyze the type of surprise
        surprise_types = []
        
        if new_docs > 0:
            surprise_types.append("new_documents")
        if new_events > 0:
            surprise_types.append("new_events")
        
        # Categorize document types found
        doc_types = set()
        for item in items:
            if item.document_type:
                doc_types.add(item.document_type)
        
        surprise_record = {
            "company_id": target.company_id,
            "company_name": target.company_name,
            "company_ticker": target.company_ticker,
            "company_tier": target.company_tier.value,
            "surprise_types": surprise_types,
            "document_types_found": list(doc_types),
            "new_documents": new_docs,
            "new_events": new_events,
            "items_analyzed": len(items),
            "discovered_at": datetime.now().isoformat(),
            "significance": self._assess_surprise_significance(new_docs, new_events, doc_types)
        }
        
        self.progress["surprise_discoveries"].append(surprise_record)
        self.progress["statistics"]["surprises_found"] += 1
        
        self.logger.info(f"🎉 SURPRISE DISCOVERED: {target.company_name}")
        self.logger.info(f"   📄 New documents: {new_docs}")
        self.logger.info(f"   📅 New events: {new_events}")
        self.logger.info(f"   📋 Document types: {list(doc_types)}")
    
    def _assess_surprise_significance(self, new_docs: int, new_events: int, doc_types: Set[str]) -> str:
        """Assess the significance of a surprise discovery"""
        
        high_value_types = {'annual_report', 'quarterly_report', 'earnings', 'corporate_action'}
        
        if doc_types.intersection(high_value_types):
            return "high"
        elif new_docs >= 3 or new_events >= 2:
            return "medium"
        else:
            return "low"
    
    async def _record_scan_result(self, target: ScheduledScrape, total_items: int, 
                                new_docs: int, new_events: int, scan_time: float, 
                                error: str = None):
        """Record the result of a company scan"""
        
        scan_record = {
            "company_id": target.company_id,
            "company_name": target.company_name,
            "company_ticker": target.company_ticker,
            "company_tier": target.company_tier.value,
            "total_items_found": total_items,
            "new_documents": new_docs,
            "new_events": new_events,
            "processing_time": scan_time,
            "scanned_at": datetime.now().isoformat(),
            "success": error is None
        }
        
        if error:
            scan_record["error"] = error
        
        self.progress["completed_scans"].append(scan_record)
        self.progress["statistics"]["companies_scanned"] += 1
        self.progress["statistics"]["new_documents"] += new_docs
        self.progress["statistics"]["new_events"] += new_events
        self.progress["statistics"]["total_processing_time"] += scan_time
        
        # Save progress periodically
        await self._save_progress()
    
    async def _analyze_surprises(self):
        """Analyze discovered surprises for patterns"""
        
        surprises = self.progress["surprise_discoveries"]
        if not surprises:
            self.logger.info("📊 No surprises discovered in this scan")
            return
        
        self.logger.info(f"🔍 Analyzing {len(surprises)} surprise discoveries:")
        
        # Group by significance
        by_significance = {}
        by_company_tier = {}
        by_doc_type = {}
        
        for surprise in surprises:
            # Count by significance
            sig = surprise["significance"]
            by_significance[sig] = by_significance.get(sig, 0) + 1
            
            # Count by company tier
            tier = surprise["company_tier"]
            by_company_tier[tier] = by_company_tier.get(tier, 0) + 1
            
            # Count by document types
            for doc_type in surprise["document_types_found"]:
                by_doc_type[doc_type] = by_doc_type.get(doc_type, 0) + 1
        
        self.logger.info(f"   📊 By significance: {dict(by_significance)}")
        self.logger.info(f"   🏢 By company tier: {dict(by_company_tier)}")
        self.logger.info(f"   📄 Top document types: {dict(sorted(by_doc_type.items(), key=lambda x: x[1], reverse=True)[:5])}")
        
        # Log high-significance surprises
        high_sig_surprises = [s for s in surprises if s["significance"] == "high"]
        if high_sig_surprises:
            self.logger.info(f"🚨 High-significance surprises:")
            for surprise in high_sig_surprises:
                self.logger.info(f"   🏢 {surprise['company_name']}: {surprise['document_types_found']}")
    
    def _generate_company_slug(self, company_name: str) -> str:
        """Generate MFN-compatible company slug from company name"""
        slug = company_name.lower()
        slug = slug.replace(' ', '-')
        slug = slug.replace('&', 'and')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        return slug.strip('-')
    
    def _log_target_summary(self, targets: List[ScheduledScrape]):
        """Log summary of target companies"""
        
        by_tier = {}
        for target in targets:
            tier = target.company_tier.value
            by_tier[tier] = by_tier.get(tier, 0) + 1
        
        self.logger.info("📊 Weekly Scan Target Summary:")
        self.logger.info(f"   By Company Tier: {dict(by_tier)}")
        self.logger.info(f"   Sample Strategy: Random selection from quiet companies")
        
        # Show a few sample companies
        sample_companies = random.sample(targets, min(5, len(targets)))
        self.logger.info("🎯 Sample Target Companies:")
        for target in sample_companies:
            self.logger.info(f"   🏢 {target.company_name} ({target.company_ticker}) - {target.company_tier.value}")
    
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
            "scanner_type": "weekly_surprise",
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
        self.logger.info("📈 Weekly Scanner Execution Summary:")
        self.logger.info(f"   Status: {status}")
        self.logger.info(f"   Companies Scanned: {stats['companies_scanned']}")
        self.logger.info(f"   Surprises Found: {stats['surprises_found']}")
        self.logger.info(f"   New Documents: {stats['new_documents']}")
        self.logger.info(f"   New Events: {stats['new_events']}")
        self.logger.info(f"   Total Time: {total_time:.1f} seconds")
        
        if stats['surprises_found'] > 0:
            self.logger.info(f"🎉 Discovered unexpected activity in {stats['surprises_found']} companies!")
        
        return final_results

# CLI interface
async def main():
    """Main CLI interface for the weekly scanner"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Weekly Surprise News Scanner')
    parser.add_argument('--sample-size', type=int, default=50, 
                       help='Number of companies to scan (default: 50)')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show targets without scanning')
    
    args = parser.parse_args()
    
    if args.dry_run:
        # Dry run - show targets only
        print("🧪 DRY RUN MODE - Showing surprise scan targets")
        print("=" * 60)
        
        scheduler = EventScheduler()
        targets = await scheduler.get_weekly_surprise_targets(args.sample_size)
        
        if not targets:
            print("📭 No companies identified for surprise scanning")
            return 0
        
        print(f"🎯 Would scan {len(targets)} companies:")
        
        by_tier = {}
        for target in targets:
            tier = target.company_tier.value
            by_tier[tier] = by_tier.get(tier, 0) + 1
        
        print(f"📊 By company tier: {dict(by_tier)}")
        print(f"\n🏢 Sample companies:")
        
        for target in targets[:10]:
            print(f"  • {target.company_name} ({target.company_ticker}) - {target.company_tier.value}")
        
        if len(targets) > 10:
            print(f"  ... and {len(targets) - 10} more companies")
        
        return 0
    
    # Production run
    scanner = WeeklyScanner(sample_size=args.sample_size)
    results = await scanner.run_weekly_scan()
    
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
        print("\n⏹️  Scanner interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Scanner failed: {e}")
        sys.exit(1)