"""
Swedish RSS Collector
Production-ready collector for Swedish company RSS feeds and press releases
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from dataclasses import dataclass
import hashlib
import re
from urllib.parse import urljoin, urlparse

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, update

from shared.database import AsyncSessionLocal
from shared.monitoring import record_collection_attempt, record_document_processed
from ...models import NordicCompany, NordicDocument, NordicDataSource, NordicIngestionLog


@dataclass
class RSSItem:
    """RSS item data structure"""
    company_id: str
    company_name: str
    title: str
    description: str
    published_date: datetime
    source_url: str
    pdf_urls: List[str]
    document_type: str
    language: str = "sv"
    priority: int = 3  # 1=urgent, 5=low


class SwedishRSSCollector:
    """
    Production-ready Swedish RSS collector
    Monitors RSS feeds from Swedish companies for financial reports and news
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session: Optional[aiohttp.ClientSession] = None
        self.processed_items: Set[str] = set()  # Deduplication
        
        # Swedish financial keywords for classification
        self.financial_keywords = {
            'quarterly_report': [
                'delårsrapport', 'kvartalsrapport', 'q1', 'q2', 'q3', 'q4',
                'interim report', 'quarterly report', 'första kvartalet',
                'andra kvartalet', 'tredje kvartalet', 'fjärde kvartalet'
            ],
            'annual_report': [
                'årsredovisning', 'annual report', 'årsbokslut',
                'helårsrapport', 'full year', 'yearly report'
            ],
            'press_release': [
                'pressmeddelande', 'press release', 'announcement',
                'meddelande', 'tillkännagivande', 'börsmeddelande'
            ],
            'earnings': [
                'resultat', 'earnings', 'vinst', 'förlust', 'profit', 'loss',
                'omsättning', 'revenue', 'intäkter'
            ]
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/rss+xml, application/xml, text/xml, */*',
                'Accept-Language': 'sv-SE,sv;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def collect_all_swedish_rss(self) -> Dict[str, int]:
        """
        Collect RSS feeds from all Swedish companies
        Returns: {company_id: items_found}
        """
        results = {}
        
        async with AsyncSessionLocal() as db:
            # Get all Swedish companies with RSS sources
            query = select(NordicCompany, NordicDataSource).join(
                NordicDataSource, NordicCompany.id == NordicDataSource.company_id
            ).where(
                NordicCompany.country == 'SE',
                NordicDataSource.source_type == 'rss_feed',
                NordicDataSource.status == 'active'
            )
            
            result = await db.execute(query)
            companies_with_rss = result.all()
            
            self.logger.info(f"📡 Collecting RSS from {len(companies_with_rss)} Swedish companies")
            
            # Process companies in batches
            batch_size = 5
            for i in range(0, len(companies_with_rss), batch_size):
                batch = companies_with_rss[i:i + batch_size]
                
                batch_tasks = [
                    self._collect_company_rss(company, source, db)
                    for company, source in batch
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for j, result in enumerate(batch_results):
                    company, source = batch[j]
                    company_id = str(company.id)
                    
                    if isinstance(result, Exception):
                        self.logger.error(f"RSS collection failed for {company.name}: {result}")
                        results[company_id] = 0
                        record_collection_attempt("rss_feed", "failed", company.name)
                    else:
                        results[company_id] = result
                        record_collection_attempt("rss_feed", "success", company.name)
                
                # Rate limiting between batches
                await asyncio.sleep(1)
            
            await db.commit()
        
        total_items = sum(results.values())
        self.logger.info(f"📰 Collected {total_items} RSS items from {len(results)} companies")
        
        return results
    
    async def _collect_company_rss(
        self,
        company: NordicCompany,
        source: NordicDataSource,
        db: AsyncSession
    ) -> int:
        """Collect RSS feed for a single Swedish company"""
        
        items_processed = 0
        
        try:
            config = source.config or {}
            rss_urls = config.get('urls', [])
            
            if isinstance(rss_urls, str):
                rss_urls = [rss_urls]
            
            if not rss_urls:
                self.logger.warning(f"No RSS URLs configured for {company.name}")
                return 0
            
            # Process each RSS feed
            for rss_url in rss_urls:
                try:
                    feed_items = await self._parse_rss_feed(rss_url, company)
                    
                    for item in feed_items:
                        if await self._process_rss_item(item, db):
                            items_processed += 1
                            
                except Exception as e:
                    self.logger.error(f"RSS feed processing failed for {company.name} ({rss_url}): {e}")
                    continue
            
            # Update source success status
            await db.execute(
                update(NordicDataSource)
                .where(NordicDataSource.id == source.id)
                .values(
                    last_success=datetime.utcnow(),
                    failure_count=0
                )
            )
            
            # Log collection result
            log_entry = NordicIngestionLog(
                source_id=source.id,
                collection_type='scheduled',
                status='success' if items_processed > 0 else 'partial',
                reports_found=items_processed,
                reports_downloaded=0,  # Downloads happen separately
                news_items_found=items_processed,
                processing_time_seconds=None
            )
            db.add(log_entry)
            
            self.logger.info(f"✅ {company.name}: Processed {items_processed} RSS items")
            return items_processed
            
        except Exception as e:
            self.logger.error(f"RSS collection failed for {company.name}: {e}")
            
            # Update source failure count
            await db.execute(
                update(NordicDataSource)
                .where(NordicDataSource.id == source.id)
                .values(failure_count=source.failure_count + 1)
            )
            
            return 0
    
    async def _parse_rss_feed(self, rss_url: str, company: NordicCompany) -> List[RSSItem]:
        """Parse RSS feed and extract relevant items"""
        
        items = []
        
        try:
            self.logger.debug(f"📡 Parsing RSS feed: {rss_url}")
            
            # Fetch RSS feed
            async with self.session.get(rss_url) as response:
                if response.status != 200:
                    self.logger.warning(f"RSS feed returned {response.status}: {rss_url}")
                    return items
                
                feed_content = await response.text()
            
            # Parse with feedparser
            feed = feedparser.parse(feed_content)
            
            if feed.bozo:
                self.logger.warning(f"RSS feed has parsing issues: {rss_url}")
            
            # Process recent entries (last 90 days for financial reports)
            cutoff_date = datetime.now() - timedelta(days=90)
            
            for entry in feed.entries:
                try:
                    # Parse published date
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        published = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        published = datetime(*entry.updated_parsed[:6])
                    else:
                        published = datetime.now()
                    
                    # Skip old entries
                    if published < cutoff_date:
                        continue
                    
                    # Create unique ID for deduplication
                    item_id = hashlib.md5(
                        f"{company.id}_{entry.link}_{published}".encode()
                    ).hexdigest()
                    
                    if item_id in self.processed_items:
                        continue
                    
                    # Extract content
                    title = entry.title if hasattr(entry, 'title') else ""
                    description = entry.description if hasattr(entry, 'description') else ""
                    source_url = entry.link if hasattr(entry, 'link') else ""
                    
                    # Skip if not relevant
                    if not self._is_financial_relevant(title, description):
                        continue
                    
                    # Extract PDF URLs
                    pdf_urls = await self._extract_pdf_urls(description, source_url)
                    
                    # Classify document type
                    document_type = self._classify_document_type(title, description)
                    
                    # Determine priority
                    priority = self._calculate_priority(document_type, title, company.name)
                    
                    item = RSSItem(
                        company_id=str(company.id),
                        company_name=company.name,
                        title=title,
                        description=description,
                        published_date=published,
                        source_url=source_url,
                        pdf_urls=pdf_urls,
                        document_type=document_type,
                        language=company.reporting_language,
                        priority=priority
                    )
                    
                    items.append(item)
                    self.processed_items.add(item_id)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to process RSS entry: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"RSS parsing failed for {rss_url}: {e}")
        
        return items
    
    def _is_financial_relevant(self, title: str, description: str) -> bool:
        """Check if RSS item is financially relevant"""
        
        text = f"{title} {description}".lower()
        
        # Check for financial keywords
        for category_keywords in self.financial_keywords.values():
            for keyword in category_keywords:
                if keyword in text:
                    return True
        
        # Additional relevance indicators
        relevance_indicators = [
            'investor', 'investerare', 'finansiell', 'financial',
            'rapport', 'report', 'resultat', 'earnings', 'utdelning',
            'dividend', 'börsmedelande', 'ir-', 'investor relations'
        ]
        
        return any(indicator in text for indicator in relevance_indicators)
    
    def _classify_document_type(self, title: str, description: str) -> str:
        """Classify the type of document"""
        
        text = f"{title} {description}".lower()
        
        # Check each category
        for doc_type, keywords in self.financial_keywords.items():
            for keyword in keywords:
                if keyword in text:
                    if doc_type == 'quarterly_report':
                        # Determine which quarter
                        if any(q in text for q in ['q1', 'första kvartalet']):
                            return 'Q1'
                        elif any(q in text for q in ['q2', 'andra kvartalet']):
                            return 'Q2'
                        elif any(q in text for q in ['q3', 'tredje kvartalet']):
                            return 'Q3'
                        else:
                            return 'Q2'  # Default quarterly to Q2
                    elif doc_type == 'annual_report':
                        return 'annual'
                    elif doc_type == 'press_release':
                        return 'press_release'
        
        # Default to press release
        return 'press_release'
    
    def _calculate_priority(self, document_type: str, title: str, company_name: str) -> int:
        """Calculate item priority (1=urgent, 5=low)"""
        
        # Financial reports are high priority
        if document_type in ['Q1', 'Q2', 'Q3', 'annual']:
            return 1  # Urgent
        
        # Earnings-related press releases
        title_lower = title.lower()
        if any(keyword in title_lower for keyword in ['resultat', 'earnings', 'profit', 'vinst']):
            return 2  # High
        
        # Major companies get higher priority
        major_companies = ['volvo', 'h&m', 'ericsson', 'atlas copco', 'sandvik']
        if any(company.lower() in company_name.lower() for company in major_companies):
            return 2  # High
        
        # Default priority
        return 3  # Medium
    
    async def _extract_pdf_urls(self, description: str, source_url: str) -> List[str]:
        """Extract PDF URLs from RSS item content"""
        
        pdf_urls = []
        
        # Find PDF links in description
        pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
        matches = re.findall(pdf_pattern, description, re.IGNORECASE)
        
        for match in matches:
            if match.startswith('http'):
                pdf_urls.append(match)
            elif match.startswith('/'):
                # Relative URL, make absolute
                base_url = f"{urlparse(source_url).scheme}://{urlparse(source_url).netloc}"
                pdf_urls.append(urljoin(base_url, match))
        
        # If no PDFs in description, check if source URL is a direct PDF
        if not pdf_urls and source_url.lower().endswith('.pdf'):
            pdf_urls.append(source_url)
        
        return pdf_urls
    
    async def _process_rss_item(self, item: RSSItem, db: AsyncSession) -> bool:
        """Process and store RSS item"""
        
        try:
            # Check for duplicates
            file_hash = hashlib.sha256(
                f"{item.company_id}_{item.title}_{item.published_date}".encode()
            ).hexdigest()
            
            existing_query = select(NordicDocument).where(
                NordicDocument.file_hash == file_hash
            )
            result = await db.execute(existing_query)
            
            if result.scalar_one_or_none():
                self.logger.debug(f"Duplicate RSS item skipped: {item.title}")
                return False
            
            # Create new document record
            document = NordicDocument(
                company_id=item.company_id,
                document_type=item.document_type,
                report_period=self._determine_report_period(item),
                title=item.title,
                source_url=item.source_url,
                storage_path=None,  # Will be set when document is downloaded
                file_hash=file_hash,
                language=item.language,
                processing_status='pending',
                metadata_={
                    'rss_source': True,
                    'pdf_urls': item.pdf_urls,
                    'priority': item.priority,
                    'description': item.description
                }
            )
            
            db.add(document)
            
            # Record metrics
            record_document_processed(
                item.company_name,
                item.document_type,
                'discovered'
            )
            
            self.logger.debug(f"Stored RSS item: {item.title}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process RSS item: {e}")
            return False
    
    def _determine_report_period(self, item: RSSItem) -> str:
        """Determine report period from RSS item"""
        
        # Extract year from published date
        year = item.published_date.year
        
        if item.document_type in ['Q1', 'Q2', 'Q3']:
            return f"{item.document_type}_{year}"
        elif item.document_type == 'annual':
            return f"FY_{year}"
        else:
            # Press releases use date
            return item.published_date.strftime('%Y-%m-%d')


# Convenience function for external use
async def collect_swedish_rss_feeds() -> Dict[str, int]:
    """
    Collect all Swedish RSS feeds
    Returns: {company_id: items_found}
    """
    async with SwedishRSSCollector() as collector:
        return await collector.collect_all_swedish_rss()