#!/usr/bin/env python3
"""
Norwegian Document Ingestor

Specialized document collector for the Norwegian market.
Integrates with Newsweb.no, Oslo Børs, and other Norwegian financial data sources.
"""

import asyncio
import aiohttp
import sys
import os
import json
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Set
from bs4 import BeautifulSoup

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from workers.base.document_ingestor import (
    DocumentIngestor, DocumentSource, DocumentType, DocumentMetadata
)
from workers.event_scheduler import EventScheduler
from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany, NordicDocument
from sqlalchemy import select
from nordic_ingestion.storage.document_catalog import DocumentCatalog
from nordic_ingestion.storage.calendar_storage import CalendarStorage

class NorwegianDocumentIngestor(DocumentIngestor):
    """
    Document ingestor for Norwegian financial markets
    
    Data Sources:
    - Newsweb.no (primary source - Oslo Børs news platform)
    - Oslo Børs API
    - Norwegian Financial Supervisory Authority (Finanstilsynet)
    - Company RSS feeds
    
    Features:
    - Calendar-driven targeting for efficiency
    - Norwegian/English language document classification
    - NOK currency handling
    - Norwegian regulatory compliance (1 hour insider trading delay)
    """
    
    def __init__(self, 
                 use_calendar_targeting: bool = True,
                 document_types: List[DocumentType] = None):
        """
        Initialize Norwegian document ingestor
        
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
                DocumentType.CORPORATE_ACTION,
                DocumentType.INSIDER_TRADING
            ]
        
        super().__init__(
            worker_name="norwegian-document-ingestor",
            market="norwegian",
            document_sources=[
                DocumentSource.WEB_SCRAPING,  # Newsweb.no
                DocumentSource.API,           # Oslo Børs API
                DocumentSource.RSS_FEED       # Company feeds
            ],
            document_types=document_types
        )
        
        self.use_calendar_targeting = use_calendar_targeting
        self.event_scheduler = EventScheduler() if use_calendar_targeting else None
        
        # Norwegian-specific configuration
        self.newsweb_base_url = "https://newsweb.oslobors.no"
        self.norwegian_document_keywords = {
            'annual_report': ['årsrapport', 'annual report', 'årsregnskap'],
            'quarterly_report': ['kvartalsrapport', 'quarterly report', 'q1', 'q2', 'q3', 'q4'],
            'interim_report': ['halvårsrapport', 'half-year report', 'delårsrapport'],
            'corporate_action': ['oppkjøp', 'fusjon', 'utbytte', 'emisjon', 'merger', 'acquisition'],
            'insider_trading': ['innsidehandel', 'insider trading', 'primærinnsider'],
            'press_release': ['pressemelding', 'press release', 'børsmelding']
        }
        
        self.document_catalog = DocumentCatalog()
        self.calendar_storage = CalendarStorage()
    
    async def connect_to_sources(self):
        """Connect to Norwegian data sources"""
        self.logger.info("🇳🇴 Connecting to Norwegian financial data sources...")
        
        # Test Newsweb connectivity
        try:
            async with aiohttp.ClientSession() as session:
                test_url = f"{self.newsweb_base_url}/"
                async with session.get(test_url, timeout=10) as response:
                    if response.status == 200:
                        self.logger.info("✅ Newsweb.no connection successful")
                    else:
                        self.logger.warning(f"⚠️  Newsweb.no returned status {response.status}")
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to Newsweb.no: {e}")
    
    async def run(self) -> Dict[str, Any]:
        """Run Norwegian document ingestion with optional calendar targeting"""
        
        # Get target companies
        if self.use_calendar_targeting:
            target_companies = await self._get_calendar_targeted_companies()
            self.logger.info(f"📅 Calendar targeting: {len(target_companies)} companies")
        else:
            target_companies = await self._get_all_norwegian_companies()
            self.logger.info(f"🏢 Processing all {len(target_companies)} Norwegian companies")
        
        # Process companies
        results = {
            "market": "norwegian",
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
                    await self.add_checkpoint(f"company_{company['ticker']}", company_results)
                    
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
        """Discover documents from Norwegian sources"""
        
        if source == DocumentSource.WEB_SCRAPING:
            # Get recent news from Newsweb
            return await self._discover_newsweb_documents(session)
        elif source == DocumentSource.API:
            # Future: Oslo Børs API integration
            return []
        elif source == DocumentSource.RSS_FEED:
            # Future: Company RSS feeds
            return []
        
        return []
    
    async def store_document(self, document: DocumentMetadata) -> bool:
        """Store document using Nordic ingestion system"""
        
        # Convert to Nordic document format
        nordic_doc = {
            "company_id": document.company_id,
            "title": document.title,
            "published_date": document.published_date,
            "document_type": document.document_type.value,
            "url": document.url,
            "pdf_url": document.pdf_url,
            "metadata": {
                "source": document.source.value,
                "language": document.language,
                "market": "norwegian",
                "collected_at": datetime.now().isoformat()
            }
        }
        
        # Store using catalog
        stats = await self.document_catalog.store_catalogued_documents([nordic_doc])
        return stats.get('stored', 0) > 0
    
    async def _get_calendar_targeted_companies(self) -> List[Dict[str, Any]]:
        """Get companies with upcoming financial events"""
        
        # Get scheduled scrape targets
        targets = await self.event_scheduler.get_daily_scrape_targets()
        
        # Convert to company list format
        companies = []
        for target in targets:
            # Only include Norwegian companies
            if target.company_country == "NO":
                companies.append({
                    "id": target.company_id,
                    "name": target.company_name,
                    "ticker": target.company_ticker,
                    "newsweb_id": await self._get_newsweb_company_id(target.company_ticker),
                    "event_context": {
                        "event_type": target.event_type,
                        "event_date": target.event_date.isoformat(),
                        "priority": target.priority.value
                    }
                })
        
        return companies
    
    async def _get_all_norwegian_companies(self) -> List[Dict[str, Any]]:
        """Get all Norwegian companies from database"""
        
        companies = []
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(NordicCompany).where(
                    NordicCompany.country == "NO"
                ).order_by(NordicCompany.name)
            )
            
            for company in result.scalars().all():
                companies.append({
                    "id": str(company.id),
                    "name": company.name,
                    "ticker": company.ticker,
                    "newsweb_id": await self._get_newsweb_company_id(company.ticker)
                })
        
        return companies
    
    async def _process_company(self, 
                             session: aiohttp.ClientSession,
                             company: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single Norwegian company"""
        
        self.logger.info(f"🏢 Processing {company['name']} ({company['ticker']})")
        
        results = {
            "company": company['name'],
            "documents_discovered": 0,
            "documents_stored": 0,
            "events_created": 0
        }
        
        try:
            # Collect from Newsweb
            documents = await self._collect_newsweb_documents(session, company)
            
            if not documents:
                self.logger.info(f"   📭 No documents found")
                return results
            
            results["documents_discovered"] = len(documents)
            self.logger.info(f"   📊 Found {len(documents)} documents")
            
            # Store documents
            stored_count = 0
            calendar_events = []
            
            for doc in documents:
                # Store document
                if await self.store_document(doc):
                    stored_count += 1
                
                # Extract calendar event if applicable
                event = self._extract_calendar_event(doc)
                if event:
                    calendar_events.append(event)
            
            results["documents_stored"] = stored_count
            
            # Store calendar events
            if calendar_events:
                event_stats = await self.calendar_storage.store_calendar_events(
                    company['id'], 
                    calendar_events
                )
                results["events_created"] = event_stats.get('created', 0)
            
            self.logger.info(
                f"   ✅ Stored {results['documents_stored']} documents, "
                f"{results['events_created']} events"
            )
            
        except Exception as e:
            self.logger.error(f"   ❌ Error: {e}")
            raise
        
        return results
    
    async def _collect_newsweb_documents(self,
                                       session: aiohttp.ClientSession,
                                       company: Dict[str, Any]) -> List[DocumentMetadata]:
        """Collect documents from Newsweb for a specific company"""
        
        documents = []
        
        if not company.get('newsweb_id'):
            return documents
        
        # Build search URL
        search_url = f"{self.newsweb_base_url}/search?category=&issuer={company['newsweb_id']}"
        
        try:
            async with session.get(search_url) as response:
                if response.status != 200:
                    self.logger.warning(f"Failed to fetch from Newsweb: {response.status}")
                    return documents
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find news items
                news_items = soup.find_all('article', class_='news-item')
                
                for item in news_items[:50]:  # Limit to recent 50
                    try:
                        doc_metadata = self._parse_newsweb_item(item, company)
                        if doc_metadata:
                            documents.append(doc_metadata)
                    except Exception as e:
                        self.logger.debug(f"Failed to parse news item: {e}")
                
        except Exception as e:
            self.logger.error(f"Error fetching from Newsweb: {e}")
        
        return documents
    
    def _parse_newsweb_item(self, item_element, company: Dict[str, Any]) -> Optional[DocumentMetadata]:
        """Parse a single Newsweb news item"""
        
        try:
            # Extract title
            title_elem = item_element.find('h3', class_='news-title')
            if not title_elem:
                return None
            title = title_elem.get_text(strip=True)
            
            # Extract URL
            link_elem = item_element.find('a', href=True)
            if not link_elem:
                return None
            url = f"{self.newsweb_base_url}{link_elem['href']}"
            
            # Extract date
            date_elem = item_element.find('time')
            if date_elem and date_elem.get('datetime'):
                published_date = datetime.fromisoformat(date_elem['datetime'].replace('Z', '+00:00'))
            else:
                published_date = datetime.now()
            
            # Extract PDF URL if available
            pdf_url = None
            pdf_elem = item_element.find('a', class_='pdf-link')
            if pdf_elem and pdf_elem.get('href'):
                pdf_url = f"{self.newsweb_base_url}{pdf_elem['href']}"
            
            # Classify document type
            doc_type = self.classify_document_type({
                'title': title,
                'content': item_element.get_text()
            })
            
            return DocumentMetadata(
                company_id=company['id'],
                company_name=company['name'],
                title=title,
                url=url,
                pdf_url=pdf_url,
                published_date=published_date,
                document_type=doc_type,
                source=DocumentSource.WEB_SCRAPING,
                language="no" if self._is_norwegian_text(title) else "en",
                metadata={
                    "ticker": company['ticker'],
                    "newsweb_id": company['newsweb_id']
                }
            )
            
        except Exception as e:
            self.logger.debug(f"Error parsing Newsweb item: {e}")
            return None
    
    async def _discover_newsweb_documents(self, session: aiohttp.ClientSession) -> List[DocumentMetadata]:
        """Discover recent documents from Newsweb main page"""
        
        documents = []
        url = f"{self.newsweb_base_url}/"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return documents
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Parse recent news items
                # Implementation depends on Newsweb structure
                
        except Exception as e:
            self.logger.error(f"Error discovering Newsweb documents: {e}")
        
        return documents
    
    async def _get_newsweb_company_id(self, ticker: str) -> Optional[str]:
        """Get Newsweb company ID from ticker"""
        # This would need to be mapped from a database or configuration
        # For now, return the ticker as a placeholder
        return ticker
    
    def _extract_calendar_event(self, document: DocumentMetadata) -> Optional[Dict[str, Any]]:
        """Extract calendar event from document if applicable"""
        
        # Check if this is a calendar-worthy document
        calendar_types = [
            DocumentType.ANNUAL_REPORT,
            DocumentType.QUARTERLY_REPORT,
            DocumentType.INTERIM_REPORT
        ]
        
        if document.document_type not in calendar_types:
            return None
        
        # Try to extract event date from title or content
        event_date = self._extract_event_date(document.title)
        if not event_date:
            event_date = document.published_date.date()
        
        return {
            "event_type": document.document_type.value,
            "event_date": event_date.isoformat(),
            "title": document.title,
            "description": f"Source: {document.url}",
            "metadata": {
                "document_url": document.url,
                "pdf_url": document.pdf_url
            }
        }
    
    def _extract_event_date(self, text: str) -> Optional[date]:
        """Try to extract date from text"""
        # Simple implementation - could be enhanced
        import re
        
        # Look for Q1-Q4 patterns
        quarter_match = re.search(r'Q([1-4])\s*(\d{4})', text, re.IGNORECASE)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year = int(quarter_match.group(2))
            # Approximate quarter end dates
            quarter_ends = {
                1: date(year, 3, 31),
                2: date(year, 6, 30),
                3: date(year, 9, 30),
                4: date(year, 12, 31)
            }
            return quarter_ends.get(quarter)
        
        return None
    
    def _is_norwegian_text(self, text: str) -> bool:
        """Check if text is in Norwegian"""
        norwegian_chars = set('æøåÆØÅ')
        return any(char in norwegian_chars for char in text)
    
    def classify_document_type(self, raw_data: Dict[str, Any]) -> DocumentType:
        """Norwegian-specific document classification"""
        
        title = str(raw_data.get('title', '')).lower()
        content = str(raw_data.get('content', '')).lower()
        combined_text = f"{title} {content}"
        
        # Check Norwegian keywords
        for doc_type, keywords in self.norwegian_document_keywords.items():
            if any(keyword in combined_text for keyword in keywords):
                if doc_type == 'annual_report':
                    return DocumentType.ANNUAL_REPORT
                elif doc_type == 'quarterly_report':
                    return DocumentType.QUARTERLY_REPORT
                elif doc_type == 'interim_report':
                    return DocumentType.INTERIM_REPORT
                elif doc_type == 'corporate_action':
                    return DocumentType.CORPORATE_ACTION
                elif doc_type == 'insider_trading':
                    return DocumentType.INSIDER_TRADING
                elif doc_type == 'press_release':
                    return DocumentType.PRESS_RELEASE
        
        # Fallback to base classification
        return super().classify_document_type(raw_data)

# CLI interface for testing
async def main():
    """Test Norwegian document ingestor"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Norwegian Document Ingestor')
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
    ingestor = NorwegianDocumentIngestor(
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