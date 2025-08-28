"""
Automated Fallback Orchestrator
Integrates all collection methods with intelligent fallback logic
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from rss_monitor import RSSMonitor
from download_orchestrator import DownloadOrchestrator
from company_pipeline import CompanyPipeline
from manual_scraper import ManualScraper


class CollectionStatus(Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    MANUAL_REQUIRED = "manual_required"


@dataclass
class CollectionResult:
    status: CollectionStatus
    documents_found: int
    documents_downloaded: int
    method_used: str
    errors: List[str]
    next_attempt_time: Optional[datetime] = None
    requires_manual: bool = False


class AutomatedFallbackOrchestrator:
    """
    Coordinates all collection methods with intelligent fallback
    """
    
    def __init__(self):
        self.rss_monitor = RSSMonitor()
        self.download_orchestrator = DownloadOrchestrator()
        self.manual_scraper = ManualScraper()
        self.company_pipelines = {}  # company_id -> CompanyPipeline
        
        # Configure logging
        self.logger = logging.getLogger(__name__)
        
    async def initialize_company_pipelines(self):
        """Load company-specific pipeline configurations"""
        # This would load from the companies/ directory structure we designed
        companies = await self.load_nordic_companies()
        
        for company in companies:
            pipeline = CompanyPipeline(company)
            
            # Configure company-specific tiers
            if company.country == "SE":  # Sweden
                # Tier 1: RSS feeds (Swedish companies have good RSS)
                pipeline.add_tier("rss_feeds", {"priority": 1, "confidence": 0.9})
                
                # Tier 2: IR email monitoring  
                pipeline.add_tier("email_monitoring", {"priority": 2, "confidence": 0.8})
                
                # Tier 3: Web scraping with URL prediction
                pipeline.add_tier("intelligent_scraping", {"priority": 3, "confidence": 0.7})
                
                # Tier 4: Playwright browser automation
                pipeline.add_tier("playwright_automation", {"priority": 4, "confidence": 0.4})
                
                # Tier 5: Manual collection
                pipeline.add_tier("manual_collection", {"priority": 5, "confidence": 1.0})
            
            self.company_pipelines[company.id] = pipeline
    
    async def run_daily_collection(self) -> Dict[str, CollectionResult]:
        """
        Daily collection orchestration
        """
        self.logger.info("🚀 Starting daily Nordic financial data collection")
        
        results = {}
        
        # Step 1: RSS Feed Monitoring (fastest, highest confidence)
        self.logger.info("📡 Phase 1: RSS Feed Monitoring")
        rss_results = await self.collect_via_rss()
        
        # Step 2: Email Inbox Processing
        self.logger.info("📧 Phase 2: Email Processing")  
        email_results = await self.collect_via_email()
        
        # Step 3: Scheduled Report Collection (based on financial calendar)
        self.logger.info("📅 Phase 3: Scheduled Report Collection")
        scheduled_results = await self.collect_scheduled_reports()
        
        # Step 4: Failed Collection Retry
        self.logger.info("🔄 Phase 4: Retry Failed Collections")
        retry_results = await self.retry_failed_collections()
        
        # Combine all results
        all_results = {**rss_results, **email_results, **scheduled_results, **retry_results}
        
        # Step 5: Create manual tasks for remaining failures
        await self.create_manual_tasks(all_results)
        
        # Step 6: Generate collection report
        await self.generate_collection_report(all_results)
        
        return all_results
    
    async def collect_via_rss(self) -> Dict[str, CollectionResult]:
        """Collect documents discovered via RSS feeds"""
        results = {}
        
        # Get new RSS items from last 24 hours
        rss_items = await self.rss_monitor.check_all_feeds()
        
        for company_id, items in rss_items.items():
            if not items:
                continue
                
            self.logger.info(f"📡 Processing {len(items)} RSS items for company {company_id}")
            
            documents_downloaded = 0
            errors = []
            
            for item in items:
                try:
                    # Try to download each discovered document
                    download_result = await self.download_orchestrator.download_document(
                        url=item.pdf_url,
                        company_id=company_id,
                        document_type=item.document_type,
                        metadata={
                            "source": "rss_feed",
                            "rss_title": item.title,
                            "published_date": item.published_date
                        }
                    )
                    
                    if download_result.success:
                        documents_downloaded += 1
                        self.logger.info(f"✅ Downloaded: {item.title}")
                    else:
                        errors.append(f"Failed to download {item.title}: {download_result.error}")
                        
                except Exception as e:
                    errors.append(f"RSS processing error: {str(e)}")
            
            results[company_id] = CollectionResult(
                status=CollectionStatus.SUCCESS if documents_downloaded > 0 else CollectionStatus.FAILED,
                documents_found=len(items),
                documents_downloaded=documents_downloaded,
                method_used="rss_feeds",
                errors=errors
            )
        
        return results
    
    async def collect_via_email(self) -> Dict[str, CollectionResult]:
        """Process IR email subscriptions"""
        results = {}
        
        try:
            # This would connect to yodabuffett.ir@gmail.com
            # Parse unread emails for financial report announcements
            email_notifications = await self.parse_ir_emails()
            
            for notification in email_notifications:
                company_id = notification.company_id
                
                # Try to download documents mentioned in email
                download_result = await self.download_orchestrator.process_email_notification(
                    notification
                )
                
                results[company_id] = CollectionResult(
                    status=CollectionStatus.SUCCESS if download_result.success else CollectionStatus.FAILED,
                    documents_found=1,
                    documents_downloaded=1 if download_result.success else 0,
                    method_used="email_subscription",
                    errors=[] if download_result.success else [download_result.error]
                )
                
        except Exception as e:
            self.logger.error(f"Email collection failed: {e}")
            
        return results
    
    async def collect_scheduled_reports(self) -> Dict[str, CollectionResult]:
        """
        Collect reports that are scheduled to be released today
        Based on financial calendar predictions
        """
        results = {}
        
        # Get companies with reports expected today (±3 days variance)
        today = datetime.now().date()
        expected_reports = await self.get_expected_reports(
            date_range=(today - timedelta(days=3), today + timedelta(days=3))
        )
        
        for report in expected_reports:
            company_id = report.company_id
            
            self.logger.info(f"📅 Checking for expected {report.report_type} report: {report.company_name}")
            
            try:
                # Use company-specific pipeline
                pipeline = self.company_pipelines[company_id]
                collection_result = await pipeline.collect_document(
                    report_type=report.report_type,
                    expected_period=report.period
                )
                
                results[company_id] = CollectionResult(
                    status=collection_result.status,
                    documents_found=1,
                    documents_downloaded=1 if collection_result.success else 0,
                    method_used=collection_result.method_used,
                    errors=collection_result.errors,
                    requires_manual=collection_result.requires_manual
                )
                
                if collection_result.success:
                    self.logger.info(f"✅ Found expected report via {collection_result.method_used}")
                else:
                    self.logger.warning(f"⚠️ Could not find expected report, will try again tomorrow")
                    
            except Exception as e:
                self.logger.error(f"Scheduled collection failed for {company_id}: {e}")
                results[company_id] = CollectionResult(
                    status=CollectionStatus.FAILED,
                    documents_found=0,
                    documents_downloaded=0,
                    method_used="scheduled_collection",
                    errors=[str(e)]
                )
        
        return results
    
    async def retry_failed_collections(self) -> Dict[str, CollectionResult]:
        """Retry previously failed collection attempts"""
        results = {}
        
        # Get failed collections from last 7 days that haven't been retried today
        failed_tasks = await self.get_retry_candidates()
        
        for task in failed_tasks:
            company_id = task.company_id
            
            self.logger.info(f"🔄 Retrying failed collection: {task.company_name} {task.report_type}")
            
            # Use higher-tier methods for retries
            pipeline = self.company_pipelines[company_id]
            
            # Skip methods that already failed, try next tier
            result = await pipeline.collect_document(
                report_type=task.report_type,
                expected_period=task.period,
                skip_failed_methods=task.failed_methods
            )
            
            results[company_id] = CollectionResult(
                status=result.status,
                documents_found=1,
                documents_downloaded=1 if result.success else 0,
                method_used=f"retry_{result.method_used}",
                errors=result.errors,
                requires_manual=result.requires_manual
            )
            
            if result.success:
                self.logger.info(f"✅ Retry successful via {result.method_used}")
                # Mark original task as resolved
                await self.mark_task_resolved(task.id)
            else:
                # Update failed methods list
                await self.update_failed_methods(task.id, result.method_used)
        
        return results
    
    async def create_manual_tasks(self, collection_results: Dict[str, CollectionResult]):
        """Create manual collection tasks for failed automated attempts"""
        
        manual_tasks_created = 0
        
        for company_id, result in collection_results.items():
            if result.requires_manual or result.status == CollectionStatus.FAILED:
                
                company = await self.get_company(company_id)
                
                # Create GitHub issue for manual collection
                issue_data = {
                    "title": f"Manual Collection Required: {company.name} {result.method_used}",
                    "body": f"""
**Company**: {company.name} ({company.ticker})
**Report Type**: {result.method_used}
**Automated Attempts**: {len(result.errors)} failed
**Errors**: {', '.join(result.errors[:3])}

**Manual Steps**:
1. Visit {company.ir_website}
2. Look for recent financial reports
3. Download any new documents
4. Upload via: `yb upload --company-ticker {company.ticker} --file report.pdf`

**Deadline**: {datetime.now() + timedelta(days=2)}
**Priority**: {'high' if result.method_used == 'scheduled_collection' else 'medium'}
                    """,
                    "labels": ["manual-collection", f"country-{company.country}", "automated-failure"]
                }
                
                await self.create_github_issue(issue_data)
                manual_tasks_created += 1
                
        if manual_tasks_created > 0:
            self.logger.warning(f"👤 Created {manual_tasks_created} manual collection tasks")
            # Send Slack notification
            await self.send_slack_notification(f"🚨 {manual_tasks_created} documents require manual collection")
    
    async def generate_collection_report(self, results: Dict[str, CollectionResult]):
        """Generate daily collection summary report"""
        
        # Calculate metrics
        total_companies = len(results)
        successful_companies = len([r for r in results.values() if r.status == CollectionStatus.SUCCESS])
        total_documents = sum(r.documents_downloaded for r in results.values())
        failed_collections = len([r for r in results.values() if r.status == CollectionStatus.FAILED])
        manual_required = len([r for r in results.values() if r.requires_manual])
        
        # Method effectiveness
        method_stats = {}
        for result in results.values():
            method = result.method_used
            method_stats[method] = method_stats.get(method, {"success": 0, "total": 0})
            method_stats[method]["total"] += 1
            if result.status == CollectionStatus.SUCCESS:
                method_stats[method]["success"] += 1
        
        # Generate report
        report = f"""
# 📊 YodaBuffett Nordic Collection Report - {datetime.now().strftime('%Y-%m-%d')}

## Summary
- **Companies Processed**: {total_companies}
- **Successful Collections**: {successful_companies} ({successful_companies/total_companies*100:.1f}%)
- **Documents Downloaded**: {total_documents}
- **Failed Collections**: {failed_collections}
- **Manual Tasks Created**: {manual_required}

## Method Effectiveness
"""
        
        for method, stats in method_stats.items():
            success_rate = stats["success"] / stats["total"] * 100 if stats["total"] > 0 else 0
            report += f"- **{method}**: {success_rate:.1f}% ({stats['success']}/{stats['total']})\n"
        
        report += "\n## Next Steps\n"
        if manual_required > 0:
            report += f"- Process {manual_required} manual collection tasks\n"
        if failed_collections > 0:
            report += f"- Investigate {failed_collections} failed collections\n"
        
        # Save report and send notifications
        await self.save_collection_report(report)
        await self.send_daily_report_notification(report)
        
        self.logger.info(f"📋 Collection complete: {successful_companies}/{total_companies} companies, {total_documents} documents")
    
    # Helper methods
    async def load_nordic_companies(self):
        """Load company configurations from filesystem"""
        # Implementation would load from companies/ directory structure
        pass
    
    async def parse_ir_emails(self):
        """Parse IR email inbox for report notifications"""
        # Implementation would connect to yodabuffett.ir@gmail.com
        pass
    
    async def get_expected_reports(self, date_range):
        """Get reports expected in date range based on calendar"""
        # Implementation would query nordic_calendar_events table
        pass
    
    async def get_retry_candidates(self):
        """Get failed tasks eligible for retry"""
        # Implementation would query failed collection logs
        pass
    
    async def create_github_issue(self, issue_data):
        """Create GitHub issue for manual task"""
        # Implementation would use GitHub API
        pass
    
    async def send_slack_notification(self, message):
        """Send notification to operations Slack channel"""
        # Implementation would use Slack API
        pass


async def main():
    """Main orchestration entry point"""
    orchestrator = AutomatedFallbackOrchestrator()
    
    try:
        # Initialize company pipelines
        await orchestrator.initialize_company_pipelines()
        
        # Run daily collection
        results = await orchestrator.run_daily_collection()
        
        print("✅ Daily collection completed successfully")
        
    except Exception as e:
        logging.error(f"❌ Daily collection failed: {e}")
        # Send alert notification
        await orchestrator.send_slack_notification(f"🚨 Daily collection failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())