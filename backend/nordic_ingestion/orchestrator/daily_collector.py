"""
Daily Collection Orchestrator
Schedules and coordinates all Nordic data collection activities
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import schedule
import time
from threading import Thread

from shared.database import AsyncSessionLocal
from shared.monitoring import record_collection_attempt
from ..collectors.rss.swedish_rss_collector import collect_swedish_rss_feeds
from ..collectors.calendar.swedish_calendar_collector import collect_swedish_financial_calendars
from ..storage.document_downloader import download_pending_documents


class DailyCollectionOrchestrator:
    """
    Production orchestrator for daily Nordic financial data collection
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.scheduler_thread: Thread = None
        
    def start_scheduler(self):
        """Start the collection scheduler in background thread"""
        if self.running:
            self.logger.warning("Scheduler already running")
            return
        
        self.running = True
        
        # Schedule daily collection at 6 AM
        schedule.every().day.at("06:00").do(self._run_daily_collection)
        
        # Schedule document downloads every hour
        schedule.every().hour.do(self._run_document_downloads)
        
        # Schedule calendar collection weekly on Mondays
        schedule.every().monday.at("05:00").do(self._run_calendar_collection)
        
        # Start scheduler in background thread
        self.scheduler_thread = Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("🕐 Daily collection scheduler started")
        self.logger.info("📋 Schedule:")
        self.logger.info("  - Daily RSS collection: 06:00")
        self.logger.info("  - Document downloads: Every hour")
        self.logger.info("  - Calendar collection: Mondays 05:00")
    
    def stop_scheduler(self):
        """Stop the collection scheduler"""
        self.running = False
        schedule.clear()
        self.logger.info("🛑 Daily collection scheduler stopped")
    
    def _scheduler_loop(self):
        """Background scheduler loop"""
        while self.running:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def _run_daily_collection(self):
        """Run daily RSS collection (scheduled function)"""
        asyncio.run(self.run_daily_collection())
    
    def _run_document_downloads(self):
        """Run document downloads (scheduled function)"""
        asyncio.run(self.run_document_downloads())
    
    def _run_calendar_collection(self):
        """Run calendar collection (scheduled function)"""
        asyncio.run(self.run_calendar_collection())
    
    async def run_daily_collection(self) -> Dict[str, Any]:
        """
        Run complete daily collection workflow
        Returns collection statistics
        """
        self.logger.info("🚀 Starting daily Nordic collection")
        start_time = datetime.utcnow()
        
        results = {
            "start_time": start_time.isoformat(),
            "rss_collection": {},
            "document_downloads": {},
            "errors": [],
            "total_documents_found": 0,
            "total_documents_downloaded": 0
        }
        
        try:
            # Phase 1: RSS Collection
            self.logger.info("📡 Phase 1: RSS Collection")
            rss_results = await collect_swedish_rss_feeds()
            results["rss_collection"] = rss_results
            results["total_documents_found"] = sum(rss_results.values())
            
            self.logger.info(f"📊 RSS Collection: {results['total_documents_found']} documents found from {len(rss_results)} companies")
            
            # Phase 2: Document Downloads
            self.logger.info("📥 Phase 2: Document Downloads")
            download_results = await download_pending_documents(limit=50)
            results["document_downloads"] = download_results
            results["total_documents_downloaded"] = download_results.get("downloaded", 0)
            
            self.logger.info(f"📊 Downloads: {results['total_documents_downloaded']} successful, {download_results.get('failed', 0)} failed")
            
            # Phase 3: Manual Task Creation (if needed)
            if download_results.get("failed", 0) > 0:
                self.logger.info("👤 Phase 3: Creating Manual Tasks")
                manual_tasks = await self._create_manual_tasks_for_failures()
                results["manual_tasks_created"] = manual_tasks
            
            # Calculate total time
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            results["end_time"] = end_time.isoformat()
            results["duration_seconds"] = duration
            
            # Generate summary report
            await self._generate_daily_report(results)
            
            self.logger.info(f"✅ Daily collection completed in {duration:.1f}s")
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Daily collection failed: {e}", exc_info=True)
            results["errors"].append(str(e))
            return results
    
    async def run_document_downloads(self) -> Dict[str, int]:
        """Run document downloads for pending documents"""
        
        try:
            self.logger.info("📥 Running hourly document downloads")
            
            # Download up to 20 pending documents
            results = await download_pending_documents(limit=20)
            
            if results["downloaded"] > 0 or results["failed"] > 0:
                self.logger.info(f"📊 Download results: {results['downloaded']} downloaded, {results['failed']} failed")
            else:
                self.logger.debug("📊 No pending documents to download")
            
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Document download failed: {e}", exc_info=True)
            return {"downloaded": 0, "failed": 0, "error": str(e)}
    
    async def run_calendar_collection(self) -> Dict[str, int]:
        """Run calendar collection for Swedish companies"""
        
        try:
            self.logger.info("🗓️ Running weekly calendar collection")
            
            calendar_results = await collect_swedish_financial_calendars()
            
            total_events = sum(calendar_results.values())
            self.logger.info(f"📊 Calendar collection: {total_events} events found from {len(calendar_results)} companies")
            
            return {
                "companies_processed": len(calendar_results),
                "total_events": total_events,
                "results": calendar_results
            }
            
        except Exception as e:
            self.logger.error(f"❌ Calendar collection failed: {e}", exc_info=True)
            return {"companies_processed": 0, "total_events": 0, "error": str(e)}
    
    async def _create_manual_tasks_for_failures(self) -> int:
        """Create manual collection tasks for failed downloads"""
        
        try:
            # This would create GitHub issues for failed downloads
            # For now, just log that manual tasks would be created
            
            async with AsyncSessionLocal() as db:
                from sqlalchemy import select, text
                
                # Find documents that failed to download
                failed_query = text("""
                    SELECT d.id, d.company_id, d.document_type, d.report_period, c.name as company_name
                    FROM nordic_documents d
                    JOIN nordic_companies c ON d.company_id = c.id
                    WHERE d.processing_status = 'failed' 
                    AND d.metadata->>'error' IS NOT NULL
                    AND DATE(d.ingestion_date) = CURRENT_DATE
                """)
                
                result = await db.execute(failed_query)
                failed_docs = result.fetchall()
                
                manual_tasks_created = 0
                
                for doc in failed_docs:
                    self.logger.warning(f"📝 Would create manual task: {doc.company_name} {doc.document_type} {doc.report_period}")
                    # TODO: Implement actual GitHub issue creation
                    manual_tasks_created += 1
                
                return manual_tasks_created
                
        except Exception as e:
            self.logger.error(f"Manual task creation failed: {e}")
            return 0
    
    async def _generate_daily_report(self, results: Dict[str, Any]):
        """Generate and log daily collection report"""
        
        report = f"""
📊 Daily Nordic Collection Report - {results['start_time'][:10]}

📈 Collection Summary:
  • RSS Documents Found: {results['total_documents_found']}
  • Documents Downloaded: {results['total_documents_downloaded']}
  • Processing Time: {results.get('duration_seconds', 0):.1f}s

📡 RSS Collection:
"""
        
        rss_results = results.get("rss_collection", {})
        for company_id, count in rss_results.items():
            if count > 0:
                report += f"  • Company {company_id[:8]}: {count} documents\n"
        
        download_results = results.get("document_downloads", {})
        if download_results:
            report += f"""
📥 Download Results:
  • Successful: {download_results.get('downloaded', 0)}
  • Failed: {download_results.get('failed', 0)}
"""
        
        if results.get("manual_tasks_created", 0) > 0:
            report += f"👤 Manual Tasks Created: {results['manual_tasks_created']}\n"
        
        if results.get("errors"):
            report += f"❌ Errors: {len(results['errors'])}\n"
            for error in results['errors'][:3]:  # Show first 3 errors
                report += f"  • {error}\n"
        
        self.logger.info(report)
        
        # TODO: Send to Slack/email for operations team
        # await self._send_daily_report_notification(report)
    
    async def run_manual_collection(self) -> Dict[str, Any]:
        """Run collection manually (for testing/debugging)"""
        self.logger.info("🔧 Running manual collection (debug mode)")
        return await self.run_daily_collection()


# Global orchestrator instance
_orchestrator_instance: DailyCollectionOrchestrator = None


def get_orchestrator() -> DailyCollectionOrchestrator:
    """Get global orchestrator instance"""
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = DailyCollectionOrchestrator()
    return _orchestrator_instance


def start_daily_scheduler():
    """Start the daily collection scheduler"""
    orchestrator = get_orchestrator()
    orchestrator.start_scheduler()


def stop_daily_scheduler():
    """Stop the daily collection scheduler"""
    orchestrator = get_orchestrator()
    orchestrator.stop_scheduler()


# Convenience functions for external use
async def run_collection_now():
    """Run collection immediately (for testing)"""
    orchestrator = get_orchestrator()
    return await orchestrator.run_manual_collection()