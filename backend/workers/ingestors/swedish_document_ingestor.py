#!/usr/bin/env python3
"""
Swedish Document Ingestor

Specialized document collector for the Swedish market.
Integrates with MFN.se and other Swedish financial data sources.
"""

import asyncio
import aiohttp
import sys
import os
from datetime import datetime, date
from typing import Dict, Any, List, Optional

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from workers.base.document_ingestor import (
    DocumentIngestor, DocumentSource, DocumentType, DocumentMetadata
)
from workers.event_scheduler import EventScheduler
from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events
from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from sqlalchemy import select

class SwedishDocumentIngestor(DocumentIngestor):
    """
    Document ingestor for Swedish financial markets
    
    Data Sources:
    - MFN.se (primary source)
    - Company RSS feeds
    - Nasdaq Stockholm
    - Swedish Financial Supervisory Authority (Finansinspektionen)
    
    Features:
    - Calendar-driven targeting for efficiency
    - Swedish language document classification
    - SEK currency handling
    - Swedish regulatory compliance
    """
    
    def __init__(self, 
                 use_calendar_targeting: bool = True,
                 document_types: List[DocumentType] = None):
        """
        Initialize Swedish document ingestor
        
        Args:
            use_calendar_targeting: Use calendar events to target companies
            document_types: Specific document types to collect
        """
        # Default to high-value document types
        if document_types is None:
            document_types = [
                DocumentType.ANNUAL_REPORT,
                DocumentType.QUARTERLY_REPORT,
                DocumentType.INTERIM_REPORT,
                DocumentType.CORPORATE_ACTION
            ]
        
        super().__init__(
            worker_name="swedish-document-ingestor",
            market="swedish",
            document_sources=[
                DocumentSource.WEB_SCRAPING,  # MFN.se
                DocumentSource.RSS_FEED,       # Company feeds
                DocumentSource.API             # Future: Nasdaq API
            ],
            document_types=document_types
        )
        
        self.use_calendar_targeting = use_calendar_targeting
        self.mfn_collector = MFNCollector(rate_limit_delay=self.rate_limit_delay)
        self.event_scheduler = EventScheduler() if use_calendar_targeting else None
        
        # Swedish-specific configuration
        self.swedish_document_keywords = {
            'annual_report': ['årsredovisning', 'annual report', 'årsbokslut'],
            'quarterly_report': ['kvartalsrapport', 'quarterly report', 'q1', 'q2', 'q3', 'q4'],
            'interim_report': ['delårsrapport', 'halvårsrapport', 'interim report'],
            'corporate_action': ['förvärv', 'fusion', 'utdelning', 'emission', 'acquisition', 'merger']
        }
    
    async def connect_to_sources(self):
        """Connect to Swedish data sources"""
        self.logger.info("🇸🇪 Connecting to Swedish financial data sources...")
        
        # Test MFN.se connectivity
        try:
            async with aiohttp.ClientSession() as session:
                test_url = "https://mfn.se/all"
                async with session.get(test_url, timeout=10) as response:
                    if response.status == 200:
                        self.logger.info("✅ MFN.se connection successful")
                    else:
                        self.logger.warning(f"⚠️  MFN.se returned status {response.status}")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to MFN.se: {e}")
    
    async def run(self) -> Dict[str, Any]:
        """Run Swedish document ingestion with optional calendar targeting"""
        
        # Get target companies
        if self.use_calendar_targeting:
            target_companies = await self._get_calendar_targeted_companies()
            self.logger.info(f"📅 Calendar targeting: {len(target_companies)} companies")
        else:
            target_companies = await self._get_all_swedish_companies()
            self.logger.info(f"🏢 Processing all {len(target_companies)} Swedish companies")
        
        # Process companies
        results = {
            "market": "swedish",
            "companies_processed": 0,
            "documents_discovered": 0,
            "documents_stored": 0,
            "events_created": 0,
            "errors": 0,
            "targeting_mode": "calendar" if self.use_calendar_targeting else "all"
        }
        
        async with aiohttp.ClientSession(
            headers=self.session_headers,
            timeout=aiohttp.ClientTimeout(total=self.config.scraping.request_timeout)
        ) as session:
            
            for company in target_companies:
                if self.should_stop:
                    break
                
                try:
                    # Process company
                    company_results = await self._process_company(session, company)
                    
                    # Update results
                    results["companies_processed"] += 1
                    results["documents_discovered"] += company_results["documents_discovered"]
                    results["documents_stored"] += company_results["documents_stored"]
                    results["events_created"] += company_results["events_created"]
                    
                    # Update metrics
                    self.update_metrics(
                        processed=1,
                        succeeded=1 if company_results["documents_stored"] > 0 else 0,
                        failed=0 if company_results["documents_stored"] > 0 else 1
                    )
                    
                    # Save progress
                    await self.add_checkpoint(f"company_{company['slug']}", company_results)
                    
                    # Rate limiting
                    await asyncio.sleep(self.rate_limit_delay)
                    
                except Exception as e:
                    self.logger.error(f"❌ Error processing {company['name']}: {e}")
                    results["errors"] += 1
                    await self.record_error("company_processing", str(e), {"company": company})
        
        return results
    
    async def discover_documents(self, 
                               session: aiohttp.ClientSession,
                               source: DocumentSource) -> List[DocumentMetadata]:
        """Discover documents from Swedish sources"""
        
        # This is handled by _process_company for MFN.se
        # Future: Add RSS feed discovery, API discovery
        return []
    
    async def store_document(self, document: DocumentMetadata) -> bool:
        """Store document using existing Nordic ingestion system"""
        # This is handled by catalog_mfn_documents
        return True
    
    async def _get_calendar_targeted_companies(self) -> List[Dict[str, Any]]:
        """Get companies with upcoming financial events"""
        
        # Get scheduled scrape targets
        targets = await self.event_scheduler.get_daily_scrape_targets()
        
        # Convert to company list format
        companies = []
        for target in targets:
            companies.append({
                "id": target.company_id,
                "name": target.company_name,
                "ticker": target.company_ticker,
                "slug": self._generate_mfn_slug(target.company_name),
                "event_context": {
                    "event_type": target.event_type,
                    "event_date": target.event_date.isoformat(),
                    "priority": target.priority.value
                }
            })
        
        return companies
    
    async def _get_all_swedish_companies(self) -> List[Dict[str, Any]]:
        """Get all Swedish companies from database"""
        
        companies = []
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(NordicCompany).where(
                    NordicCompany.country == "SE"
                ).order_by(NordicCompany.name)
            )
            
            for company in result.scalars().all():
                companies.append({
                    "id": str(company.id),
                    "name": company.name,
                    "ticker": company.ticker,
                    "slug": self._generate_mfn_slug(company.name)
                })
        
        return companies
    
    async def _process_company(self, 
                             session: aiohttp.ClientSession,
                             company: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single Swedish company"""
        
        self.logger.info(f"🏢 Processing {company['name']} ({company['ticker']})")
        
        results = {
            "company": company['name'],
            "documents_discovered": 0,
            "documents_stored": 0,
            "events_created": 0
        }
        
        try:
            # Collect from MFN.se
            items = await self.mfn_collector.collect_company_news(
                session,
                company['slug'],
                limit=200 if self.use_calendar_targeting else 50
            )
            
            if not items:
                self.logger.info(f"   📭 No items found")
                return results
            
            results["documents_discovered"] = len(items)
            self.logger.info(f"   📊 Found {len(items)} items")
            
            # Store documents
            doc_stats = await catalog_mfn_documents(items)
            results["documents_stored"] = doc_stats.get('stored', 0)
            
            # Store calendar events
            event_stats = await store_mfn_calendar_events(items)
            results["events_created"] = event_stats.get('calendar_events_created', 0)
            
            self.logger.info(
                f"   ✅ Stored {results['documents_stored']} documents, "
                f"{results['events_created']} events"
            )
            
        except Exception as e:
            self.logger.error(f"   ❌ Error: {e}")
            raise
        
        return results
    
    def _generate_mfn_slug(self, company_name: str) -> str:
        """Generate MFN-compatible slug from company name"""
        slug = company_name.lower()
        slug = slug.replace(' ', '-')
        slug = slug.replace('&', 'and')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        return slug.strip('-')
    
    def classify_document_type(self, raw_data: Dict[str, Any]) -> DocumentType:
        """Swedish-specific document classification"""
        
        title = str(raw_data.get('title', '')).lower()
        content = str(raw_data.get('content', '')).lower()
        combined_text = f"{title} {content}"
        
        # Check Swedish keywords
        for doc_type, keywords in self.swedish_document_keywords.items():
            if any(keyword in combined_text for keyword in keywords):
                if doc_type == 'annual_report':
                    return DocumentType.ANNUAL_REPORT
                elif doc_type == 'quarterly_report':
                    return DocumentType.QUARTERLY_REPORT
                elif doc_type == 'interim_report':
                    return DocumentType.INTERIM_REPORT
                elif doc_type == 'corporate_action':
                    return DocumentType.CORPORATE_ACTION
        
        # Fallback to base classification
        return super().classify_document_type(raw_data)

# CLI interface for testing
async def main():
    """Test Swedish document ingestor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Swedish Document Ingestor')
    parser.add_argument('--calendar', action='store_true', 
                       help='Use calendar-based targeting')
    parser.add_argument('--limit', type=int, default=5,
                       help='Limit number of companies to process')
    parser.add_argument('--all-types', action='store_true',
                       help='Collect all document types')
    
    args = parser.parse_args()
    
    # Configure document types
    if args.all_types:
        doc_types = None
    else:
        doc_types = [DocumentType.ANNUAL_REPORT, DocumentType.QUARTERLY_REPORT]
    
    # Create and run ingestor
    ingestor = SwedishDocumentIngestor(
        use_calendar_targeting=args.calendar,
        document_types=doc_types
    )
    
    results = await ingestor.start()
    ingestor.log_summary()
    
    print(f"\n📊 Results:")
    for key, value in results.items():
        print(f"   {key}: {value}")

if __name__ == "__main__":
    asyncio.run(main())