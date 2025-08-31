#!/usr/bin/env python3
"""
Document Ingestor Base Class

Foundation for all document collection workers across different markets.
Handles the common patterns of document discovery, filtering, and storage.
"""

import asyncio
import aiohttp
from abc import abstractmethod
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Set
from enum import Enum
from dataclasses import dataclass

from .base_worker import BaseWorker, WorkerType

class DocumentSource(Enum):
    """Types of document sources"""
    RSS_FEED = "rss_feed"
    WEB_SCRAPING = "web_scraping"
    API = "api"
    EMAIL = "email"
    FTP = "ftp"
    MANUAL = "manual"

class DocumentType(Enum):
    """Standard document types across markets"""
    ANNUAL_REPORT = "annual_report"
    QUARTERLY_REPORT = "quarterly_report"
    INTERIM_REPORT = "interim_report"
    PRESS_RELEASE = "press_release"
    CORPORATE_ACTION = "corporate_action"
    GOVERNANCE = "governance"
    PROSPECTUS = "prospectus"
    INSIDER_TRADING = "insider_trading"
    OTHER = "other"

@dataclass
class DocumentMetadata:
    """Standard metadata for financial documents"""
    company_id: str
    company_name: str
    document_type: DocumentType
    source_url: str
    publish_date: Optional[date] = None
    language: Optional[str] = None
    file_size: Optional[int] = None
    pages: Optional[int] = None
    fiscal_year: Optional[int] = None
    fiscal_period: Optional[str] = None
    market_specific_data: Dict[str, Any] = None

class DocumentIngestor(BaseWorker):
    """
    Base class for document ingestion workers
    
    Provides common functionality for:
    - Document discovery from various sources
    - Filtering and deduplication
    - Metadata extraction
    - Storage coordination
    - Rate limiting and retry logic
    """
    
    def __init__(self, 
                 worker_name: str,
                 market: str,
                 document_sources: List[DocumentSource],
                 document_types: List[DocumentType] = None):
        """
        Initialize document ingestor
        
        Args:
            worker_name: Name of this ingestor instance
            market: Market this ingestor serves (e.g., 'swedish', 'norwegian')
            document_sources: List of sources this ingestor uses
            document_types: Document types to collect (None = all types)
        """
        super().__init__(worker_name, WorkerType.DOCUMENT_INGESTOR, market)
        
        self.document_sources = document_sources
        self.document_types = document_types or list(DocumentType)
        
        # Rate limiting
        self.rate_limit_delay = self.config.scraping.rate_limit_delay
        self.session_headers = {
            'User-Agent': self.config.scraping.user_agent
        }
        
        # Tracking
        self.discovered_documents: List[DocumentMetadata] = []
        self.processed_urls: Set[str] = set()
        
        self.logger.info(f"📚 Document Ingestor initialized")
        self.logger.info(f"   Sources: {[s.value for s in document_sources]}")
        self.logger.info(f"   Document Types: {len(self.document_types)}")
    
    async def on_startup(self):
        """Startup tasks for document ingestor"""
        self.logger.info(f"🔌 Connecting to {self.market} data sources...")
        
        # Market-specific startup
        await self.connect_to_sources()
        
        # Load previous state if resuming
        await self.load_processing_state()
    
    async def run(self) -> Dict[str, Any]:
        """Main document ingestion workflow"""
        self.logger.info(f"📥 Starting document ingestion for {self.market} market")
        
        start_time = datetime.now()
        results = {
            "market": self.market,
            "sources_checked": 0,
            "documents_discovered": 0,
            "documents_stored": 0,
            "documents_skipped": 0,
            "errors": 0
        }
        
        # Create HTTP session for all requests
        timeout = aiohttp.ClientTimeout(total=self.config.scraping.request_timeout)
        async with aiohttp.ClientSession(
            headers=self.session_headers,
            timeout=timeout
        ) as session:
            
            # Process each document source
            for source in self.document_sources:
                if self.should_stop:
                    self.logger.info("⏹️  Stopping document ingestion...")
                    break
                
                try:
                    self.logger.info(f"🔍 Checking {source.value} for {self.market} documents...")
                    results["sources_checked"] += 1
                    
                    # Discover documents from this source
                    documents = await self.discover_documents(session, source)
                    results["documents_discovered"] += len(documents)
                    
                    # Process discovered documents
                    for doc in documents:
                        if self.should_stop:
                            break
                        
                        if doc.source_url in self.processed_urls:
                            results["documents_skipped"] += 1
                            continue
                        
                        # Store document metadata
                        success = await self.store_document(doc)
                        if success:
                            results["documents_stored"] += 1
                            self.processed_urls.add(doc.source_url)
                            self.update_metrics(processed=1, succeeded=1)
                        else:
                            results["errors"] += 1
                            self.update_metrics(processed=1, failed=1)
                        
                        # Rate limiting
                        await asyncio.sleep(self.rate_limit_delay)
                    
                    # Save progress after each source
                    await self.save_progress()
                    
                except Exception as e:
                    self.logger.error(f"❌ Error processing {source.value}: {e}")
                    results["errors"] += 1
                    await self.record_error(f"source_{source.value}", str(e))
        
        # Final summary
        duration = (datetime.now() - start_time).total_seconds()
        results["duration_seconds"] = duration
        
        self.logger.info(f"✅ Document ingestion completed")
        self.logger.info(f"   Documents discovered: {results['documents_discovered']}")
        self.logger.info(f"   Documents stored: {results['documents_stored']}")
        self.logger.info(f"   Duration: {duration:.1f}s")
        
        return results
    
    async def on_shutdown(self):
        """Cleanup tasks for document ingestor"""
        self.logger.info("🧹 Cleaning up document ingestor...")
        await self.save_processing_state()
    
    @abstractmethod
    async def connect_to_sources(self):
        """Connect to market-specific data sources - implement in subclasses"""
        pass
    
    @abstractmethod
    async def discover_documents(self, 
                               session: aiohttp.ClientSession,
                               source: DocumentSource) -> List[DocumentMetadata]:
        """Discover documents from a specific source - implement in subclasses"""
        pass
    
    @abstractmethod
    async def store_document(self, document: DocumentMetadata) -> bool:
        """Store document metadata - implement in subclasses"""
        pass
    
    async def load_processing_state(self):
        """Load previously processed URLs for resume capability"""
        try:
            # Check for previous session data
            if hasattr(self, 'progress_data') and 'processed_urls' in self.progress_data:
                self.processed_urls = set(self.progress_data['processed_urls'])
                self.logger.info(f"📂 Loaded {len(self.processed_urls)} previously processed URLs")
        except Exception as e:
            self.logger.warning(f"⚠️  Could not load previous state: {e}")
    
    async def save_processing_state(self):
        """Save processed URLs for resume capability"""
        try:
            self.progress_data['processed_urls'] = list(self.processed_urls)
            await self.save_progress()
        except Exception as e:
            self.logger.error(f"⚠️  Could not save processing state: {e}")
    
    def filter_documents_by_type(self, 
                               documents: List[DocumentMetadata]) -> List[DocumentMetadata]:
        """Filter documents by configured document types"""
        if not self.document_types:
            return documents
        
        filtered = [
            doc for doc in documents 
            if doc.document_type in self.document_types
        ]
        
        self.logger.info(f"🔽 Filtered {len(documents)} → {len(filtered)} documents")
        return filtered
    
    def filter_documents_by_date(self,
                               documents: List[DocumentMetadata],
                               days_back: int = 30) -> List[DocumentMetadata]:
        """Filter documents by publish date"""
        cutoff_date = date.today() - timedelta(days=days_back)
        
        filtered = [
            doc for doc in documents
            if doc.publish_date and doc.publish_date >= cutoff_date
        ]
        
        self.logger.info(f"📅 Date filter: {len(documents)} → {len(filtered)} documents")
        return filtered
    
    async def extract_metadata(self, 
                             url: str, 
                             raw_data: Dict[str, Any]) -> DocumentMetadata:
        """Extract standardized metadata from raw document data"""
        # Base implementation - override for market-specific extraction
        return DocumentMetadata(
            company_id=raw_data.get('company_id', 'unknown'),
            company_name=raw_data.get('company_name', 'Unknown'),
            document_type=self.classify_document_type(raw_data),
            source_url=url,
            publish_date=self.parse_date(raw_data.get('date')),
            language=raw_data.get('language'),
            market_specific_data=raw_data
        )
    
    def classify_document_type(self, raw_data: Dict[str, Any]) -> DocumentType:
        """Classify document type from raw data"""
        # Simple keyword-based classification - override for better accuracy
        title = str(raw_data.get('title', '')).lower()
        
        if any(term in title for term in ['annual', 'yearly', 'årsrapport']):
            return DocumentType.ANNUAL_REPORT
        elif any(term in title for term in ['quarterly', 'q1', 'q2', 'q3', 'q4', 'kvartal']):
            return DocumentType.QUARTERLY_REPORT
        elif any(term in title for term in ['interim', 'half-year', 'halvår']):
            return DocumentType.INTERIM_REPORT
        elif any(term in title for term in ['press', 'news', 'announcement']):
            return DocumentType.PRESS_RELEASE
        elif any(term in title for term in ['merger', 'acquisition', 'dividend']):
            return DocumentType.CORPORATE_ACTION
        else:
            return DocumentType.OTHER
    
    def parse_date(self, date_string: Any) -> Optional[date]:
        """Parse date from various formats"""
        if not date_string:
            return None
        
        if isinstance(date_string, date):
            return date_string
        
        # Add more date parsing logic as needed
        try:
            return datetime.fromisoformat(str(date_string)).date()
        except:
            return None