"""
MFN.se Universal Financial News Collector
Single source for all Swedish company financial news and reports
"""
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import re
from dataclasses import dataclass
from nordic_ingestion.common.company_mappings import get_company_name
from nordic_ingestion.storage.slug_manager import SlugManager
from shared.database import AsyncSessionLocal

@dataclass
class MFNNewsItem:
    """Financial news item from MFN.se"""
    company_name: str
    title: str
    date_published: datetime
    content: str
    pdf_urls: List[str]
    source_url: str
    document_type: str  # earnings, m&a, governance, etc.
    calendar_info: Dict[str, Any] = None  # Earnings dates, webcast info, etc.

class MFNCollector:
    """
    Universal collector for Swedish financial news via MFN.se
    
    Benefits:
    - Single source for ALL Swedish companies
    - Direct PDF links included
    - Pre-parsed financial news
    - Standardized format
    """
    
    def __init__(self, rate_limit_delay: float = 2.0):
        self.base_url = "https://mfn.se/all/a"
        self.rate_limit_delay = rate_limit_delay  # Seconds between requests
        self.enable_slug_resolution = True  # Auto-retry with slug variations
        # Company name mapping: database_name -> mfn_url_slug
        self.company_url_mapping = {
            "2cureX AB": "2curex",
            "Volvo Group": "volvo",
            "AstraZeneca": "astrazeneca", 
            "Atlas Copco AB": "atlas-copco",
            "Telefonaktiebolaget LM Ericsson": "ericsson",
            "H&M Hennes & Mauritz AB": "handm",  # Note: handm not hm!
            "Sandvik AB": "sandvik",
            "Nordea Bank Abp": "nordea",
            "Investor AB": "investor",
            "ABB Ltd": "abb",
            "Hexagon AB": "hexagon"
        }
        
        # Default list for testing
        self.swedish_companies = list(self.company_url_mapping.values())
        self.session_headers = {
            'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
    async def collect_all_companies(self) -> List[MFNNewsItem]:
        """
        Collect financial news for all Swedish companies
        
        Returns:
            List of financial news items with PDF links
        """
        all_items = []
        
        async with aiohttp.ClientSession(
            headers=self.session_headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            
            # Sequential processing with rate limiting (be respectful!)
            results = []
            for company in self.swedish_companies:
                print(f"📡 Collecting {company}...")
                try:
                    result = await self.collect_company_news(session, company)
                    results.append(result)
                    
                    # Rate limit: wait between requests
                    if company != self.swedish_companies[-1]:  # Don't wait after last
                        print(f"⏱️  Rate limiting... waiting {self.rate_limit_delay}s")
                        await asyncio.sleep(self.rate_limit_delay)
                        
                except Exception as e:
                    print(f"❌ Error collecting {company}: {e}")
                    results.append([])
            
            for result in results:
                if isinstance(result, list):
                    all_items.extend(result)
                    
        return all_items
        
    async def collect_company_news(
        self, 
        session: aiohttp.ClientSession, 
        company: str,
        limit: int = 50,
        full_backfill: bool = True,
        chunk_size: int = 50,
        save_callback = None,
        _recursion_depth: int = 0
    ) -> List[MFNNewsItem]:
        """
        Collect news for a specific company with chunked processing support
        
        Args:
            session: HTTP session
            company: Company slug (e.g., 'volvo', 'astrazeneca')
            limit: Number of items to fetch (default: 50 for daily updates, 0 = no limit)
            full_backfill: If True, fetch 1000+ items for historical data
            chunk_size: Process items in chunks of this size (default: 50)
            save_callback: Optional function to call for saving chunks (items) -> None
            
        Returns:
            List of news items for the company
        """
        if full_backfill:
            url_limit = 240  # ~5 years of data (30 events/year × 5 years)
        elif limit == 0:
            url_limit = 1000  # Large number to get all items when limit=0
        else:
            url_limit = limit
            
        url = f"{self.base_url}/{company}?limit={url_limit}"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    # If 404 and slug resolution enabled, try to find correct slug
                    # But limit recursion to prevent infinite loops
                    if response.status == 404 and self.enable_slug_resolution and _recursion_depth < 2:
                        print(f"🔄 404 for {company}, attempting slug resolution...")
                        resolved_slug = await self._resolve_company_slug(session, company)
                        if resolved_slug and resolved_slug != company:
                            print(f"🎯 Retrying with resolved slug: {resolved_slug}")
                            return await self.collect_company_news(session, resolved_slug, limit, full_backfill, chunk_size, save_callback, _recursion_depth + 1)
                        else:
                            print(f"🚫 No valid slug variation found for {company}")
                    return []
                    
                html = await response.text()
                result = self._parse_mfn_page(company, html, url, limit, chunk_size, save_callback)
                
                # If we got no results and slug resolution is enabled, try variations
                # But limit recursion to prevent infinite loops
                if (not result or len(result) == 0) and self.enable_slug_resolution and _recursion_depth < 2:
                    print(f"🔄 No results for {company}, attempting slug resolution...")
                    resolved_slug = await self._resolve_company_slug(session, company)
                    if resolved_slug and resolved_slug != company:
                        print(f"🎯 Retrying with resolved slug: {resolved_slug}")
                        return await self.collect_company_news(session, resolved_slug, limit, full_backfill, chunk_size, save_callback, _recursion_depth + 1)
                    else:
                        print(f"🚫 No valid slug variation found for {company}")
                
                return result
                
        except Exception as e:
            print(f"Failed to collect {company}: {e}")
            return []
    
    async def _resolve_company_slug(self, session: aiohttp.ClientSession, original_slug: str) -> Optional[str]:
        """
        Resolve company slug by testing common variations
        
        Examples:
        - "absolent-air-care" → tries "absolent-air-care-group"
        - "yubico" → tries "yubico-ab"
        """
        
        # Cache for resolved slugs
        if not hasattr(self, '_slug_cache'):
            self._slug_cache = {}
        
        if original_slug in self._slug_cache:
            return self._slug_cache[original_slug]
        
        # ENHANCEMENT: Check database for stored slug first
        try:
            async with AsyncSessionLocal() as db:
                # Try to find stored slug by company name derived from slug
                company_name = get_company_name(original_slug)
                stored_slug = await SlugManager.get_slug_for_company_name(db, company_name)
                if stored_slug and stored_slug != original_slug:
                    print(f"📋 Using stored slug: {original_slug} → {stored_slug}")
                    self._slug_cache[original_slug] = stored_slug
                    return stored_slug
        except Exception as e:
            print(f"⚠️ Error checking stored slug: {e}")
            # Continue with normal resolution
        
        print(f"🔍 Resolving slug variations for: {original_slug}")
        
        # FIRST: Remove stock class suffixes (-a, -b) as they're not part of company name
        base_slug = original_slug
        if original_slug.endswith('-a') or original_slug.endswith('-b'):
            base_slug = original_slug[:-2]
            print(f"   📊 Removed stock class suffix: {original_slug} → {base_slug}")
        
        # TARGETED approach: Based on your observation that it's "usually either -group or -holding"
        primary_suffixes = ["-group", "-holding"]  # Most common
        secondary_suffixes = ["-ab", "-publ"]      # Less common but still seen
        
        variations = []
        
        # Strategy 1: If slug already has a suffix, try removing it and adding primary ones
        suffix_found = False
        all_suffixes = primary_suffixes + secondary_suffixes + ["-international", "-systems", "-tech"]
        
        for suffix in all_suffixes:
            if base_slug.endswith(suffix):
                company_base = base_slug[:-len(suffix)]
                # Try with primary suffixes first
                for primary_suffix in primary_suffixes:
                    if primary_suffix != suffix:  # Don't try the same suffix
                        variations.append(company_base + primary_suffix)
                # Then try without any suffix
                variations.append(company_base)
                suffix_found = True
                break
        
        # Strategy 2: If no existing suffix, try adding the most common ones first
        if not suffix_found:
            # FIRST: Try the simple hyphenated version (for cases like "Atlas Copco" → "atlas-copco")
            if base_slug != original_slug:  # Only if we changed something (removed -a, -b)
                variations.append(base_slug)
            
            # Then try primary suffixes (most likely to work)
            for suffix in primary_suffixes:
                variations.append(base_slug + suffix)
            # Finally try secondary suffixes
            for suffix in secondary_suffixes:
                variations.append(base_slug + suffix)
        
        # Limit to 4 attempts max (fast resolution)
        variations = variations[:4]
        print(f"   📝 Testing variations: {variations}")
        
        # Test each variation
        for i, variation in enumerate(variations):
            try:
                test_url = f"{self.base_url}/{variation}"
                async with session.get(test_url, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # Quick validation - look for Swedish financial terms
                        if any(term in html.lower() for term in ['kvartalsrapport', 'årsrapport', 'aktie', 'rapport', 'short-item']):
                            print(f"   ✅ Found working variation: {variation}")
                            self._slug_cache[original_slug] = variation
                            
                            # ENHANCEMENT: Store successful slug in database for future use
                            try:
                                async with AsyncSessionLocal() as db:
                                    company_name = get_company_name(original_slug)
                                    # Find the company to store slug
                                    from sqlalchemy import select, func
                                    from nordic_ingestion.models import NordicCompany
                                    result = await db.execute(
                                        select(NordicCompany).where(
                                            func.lower(NordicCompany.name) == func.lower(company_name)
                                        ).order_by(NordicCompany.created_at).limit(1)
                                    )
                                    company = result.scalar_one_or_none()
                                    if company:
                                        await SlugManager.store_successful_slug(db, company.id, variation)
                            except Exception as e:
                                print(f"   ⚠️ Could not store slug in database: {e}")
                            
                            return variation
                        else:
                            print(f"   ⚠️  {variation} returned 200 but no financial content")
                    else:
                        print(f"   ❌ {variation} returned {response.status}")
                
                # Rate limit between attempts
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Error testing {variation}: {e}")
                continue
        
        print(f"   🚫 No working variations found for {original_slug}")
        return None
            
    def _parse_mfn_page(
        self, 
        company: str, 
        html: str, 
        source_url: str,
        limit: int = 50,
        chunk_size: int = 50,
        save_callback = None
    ) -> List[MFNNewsItem]:
        """
        Parse MFN.se page for financial news items and calendar events
        
        Args:
            company: Company name
            html: Page HTML
            source_url: Original URL
            
        Returns:
            List of parsed news items with calendar information
        """
        soup = BeautifulSoup(html, 'html.parser')
        items = []
        
        # First, extract calendar information from the entire page
        page_calendar_info = self._extract_calendar_info(soup, company)
        
        # Find MFN-specific document containers
        # MFN uses <div class="short-item compressible"> for each document
        mfn_items = soup.find_all('div', class_='short-item compressible')
        print(f"🔍 Found {len(mfn_items)} MFN short-item containers")
        
        # ⭐ CRITICAL FIX: Filter MFN items to only include the target company
        # This prevents collecting documents from other companies when MFN shows a generic feed
        company_mfn_items = []
        if mfn_items:
            print(f"🔍 Filtering containers for company: {company}")
            for item in mfn_items:
                # Look for author attribute in the item
                author_link = item.find('a', {'author': True})
                if author_link:
                    item_author = author_link.get('author')
                    if item_author == company:
                        company_mfn_items.append(item)
                        print(f"   ✅ Found document from {item_author}")
                    else:
                        print(f"   🔄 Skipping document from {item_author} (not target company)")
                else:
                    # If no author found, include item (fallback for older format)
                    company_mfn_items.append(item)
            
            print(f"🔍 After filtering: {len(company_mfn_items)} containers belong to {company}")
            mfn_items = company_mfn_items
        
        # Also try generic article/news patterns as fallback
        generic_articles = soup.find_all(['article', 'div'], class_=re.compile(r'(article|news|item|post)', re.I))
        print(f"🔍 Found {len(generic_articles)} articles with standard selectors")
        
        # Combine both approaches
        articles = mfn_items + generic_articles
        print(f"🔍 Total containers to process: {len(articles)}")
        
        # Also check for table rows as final fallback
        if not articles:
            table_rows = soup.find_all('tr')
            if table_rows and len(table_rows) > 1:  # Skip header row
                print(f"📋 Fallback: Found {len(table_rows)} table rows, checking for links...")
                rows_with_links = 0
                for tr in table_rows[1:]:  # Skip first row (usually header)
                    if tr.find('a', href=True):  # Has links
                        articles.append(tr)
                        rows_with_links += 1
                print(f"📋 Added {rows_with_links} table rows with links as articles")
        
        # Final fallback - use entire page
        if not articles:
            print(f"⚠️  No structured containers found, using entire page as fallback")
            articles = [soup]
        
        print(f"📄 Processing {len(articles)} total articles for document extraction")
        
        articles_processed = 0
        items_created = 0
        current_chunk = []
        all_items = []
        
        # Process articles with chunking support
        for article in articles:  # Process all articles found
            articles_processed += 1
            
            # Check if we've reached the limit (0 means no limit)
            if limit > 0 and items_created >= limit:
                print(f"   📊 Reached limit of {limit} items, stopping collection")
                break
                
            try:
                # DEBUG: Show article type (only for first few)
                if hasattr(article, 'name') and articles_processed <= 5:
                    article_type = f"{article.name}" + (f".{article.get('class')}" if article.get('class') else "")
                    print(f"🔍 Processing article {articles_processed}: {article_type}")
                
                # Extract PDF links from this article - FIXED to only get actual PDFs
                pdf_links = []
                for link in article.find_all('a', href=True):
                    href = link['href']
                    # Only accept files that actually end with .pdf
                    if href.endswith('.pdf'):
                        if href.startswith('http'):
                            pdf_links.append(href)
                        elif href.startswith('/'):
                            pdf_links.append('https://mfn.se' + href)
                
                # Also check for PDF patterns in the article HTML - but ONLY actual PDFs
                article_html = str(article)
                
                # Common PDF hosting patterns - INCLUDING STORAGE.MFN.SE
                pdf_patterns = [
                    r'https://storage\.mfn\.se/[^"\'>\s]+\.pdf',  # MFN Storage PDFs ⭐ NEW!
                    r'https://mb\.cision\.com/[^"\'>\s]+\.pdf',  # Cision PDFs
                    r'https://[^"\'>\s]*\.pdf',  # Any HTTPS PDF
                    r'http://[^"\'>\s]*\.pdf',   # Any HTTP PDF  
                    r'/[^"\'>\s]*\.pdf',         # Relative PDF paths
                ]
                
                for pattern in pdf_patterns:
                    found_pdfs = re.findall(pattern, article_html)
                    pdf_links.extend(found_pdfs)
                
                # Also look for common document hosting domains even without .pdf extension
                document_patterns = [
                    r'https://storage\.mfn\.se/[^"\'>\s]+',  # MFN Storage documents ⭐ NEW!
                    r'https://mb\.cision\.com/[^"\'>\s]+',  # Cision documents
                    r'https://ml-eu\.globenewswire\.com/Resource/Download/[^"\'>\s]+',  # GlobeNewswire documents
                    r'https://[^"\'>\s]*rapport[^"\'>\s]*',  # Swedish "rapport" documents
                    r'https://[^"\'>\s]*financial[^"\'>\s]*',  # Financial documents
                    r'https://[^"\'>\s]*investor[^"\'>\s]*',  # Investor documents
                ]
                
                potential_docs = []
                for pattern in document_patterns:
                    found_docs = re.findall(pattern, article_html, re.IGNORECASE)
                    potential_docs.extend(found_docs)
                
                # DEBUG: Show potential document links (only for first few)
                if potential_docs and article != soup and articles_processed <= 5:
                    print(f"🔍 Potential document links: {potential_docs[:2]}...")  # Show first 2
                
                # Filter and normalize PDF links - INCLUDE document hosting links
                actual_pdf_links = []
                for link in pdf_links:
                    # Accept .pdf files and known document hosting domains
                    if (link.endswith('.pdf') and 
                        not any(img_ext in link.lower() for img_ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg'])):
                        
                        # Fix protocol-relative URLs (starting with //)
                        if link.startswith('//'):
                            link = 'https:' + link
                        elif link.startswith('/'):
                            link = 'https://mfn.se' + link
                        
                        actual_pdf_links.append(link)
                
                # ALSO INCLUDE potential document links from hosting services
                for link in potential_docs:
                    # Skip image files
                    if any(img_ext in link.lower() for img_ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp']):
                        continue
                        
                    # Known document hosting services that serve PDFs without .pdf extension
                    if any(host in link.lower() for host in ['storage.mfn.se', 'mb.cision.com', 'cision.com', 'ir.hexagon.com', 'investors.', 'investor.', 'globenewswire.com']):
                        # Fix protocol-relative URLs
                        if link.startswith('//'):
                            link = 'https:' + link
                        elif link.startswith('/'):
                            link = 'https://mfn.se' + link
                        
                        actual_pdf_links.append(link)
                
                pdf_links = list(set(actual_pdf_links))  # Remove duplicates
                
                # DEBUG: Show what we found in this article (only for first few)
                if article != soup and articles_processed <= 5:  # Don't spam for fallback case
                    all_links = [link.get('href') for link in article.find_all('a', href=True)]
                    if all_links:
                        print(f"🔍 Article links found: {all_links[:3]}...")  # Show first 3
                    if pdf_links:
                        print(f"📄 PDF links extracted: {pdf_links}")
                    else:
                        print(f"⚠️  No PDF links in article with {len(all_links)} total links")
                
                if pdf_links:
                    # Try to extract title - ENHANCED for MFN.se structure
                    title = "Financial Document"
                    
                    # Look for titles in various MFN.se patterns
                    title_sources = [
                        # Standard HTML headings
                        article.find(['h1', 'h2', 'h3', 'h4', '.title', '.headline']),
                        # MFN.se anchor titles (like "AB Volvo: Volvokoncernen - det andra kvartalet 2025")
                        article.find('a', title=True),
                        # Any anchor with Swedish quarterly terms
                        article.find('a', string=re.compile(r'(kvartal|kvartalet|delårsrapport|q[1-4])', re.I)),
                        # Any element containing Swedish quarterly terms
                        article.find(text=re.compile(r'(det \w+ kvartalet|kvartal|delårsrapport)', re.I))
                    ]
                    
                    for source in title_sources:
                        if source:
                            if hasattr(source, 'get') and source.get('title'):
                                # Extract from title attribute
                                title = source.get('title').strip()[:200]
                                break
                            elif hasattr(source, 'get_text'):
                                # Extract from element text
                                text = source.get_text(strip=True)
                                if text and len(text) > 5:
                                    title = text[:200]
                                    break
                            elif isinstance(source, str) and len(source.strip()) > 5:
                                # Direct string match
                                title = source.strip()[:200]
                                break
                    
                    # Try to extract date - ENHANCED for MFN.se structure
                    date_published = None
                    
                    # First, try MFN.se specific date format
                    date_span = article.find('span', class_='compressed-date')
                    if date_span:
                        date_text = date_span.get_text(strip=True)
                        try:
                            date_published = datetime.strptime(date_text, '%Y-%m-%d')
                        except:
                            pass
                    
                    # Check if this article is a table row or within one
                    if not date_published and article.name == 'tr':
                        # Article IS a table row, get first td
                        first_td = article.find('td')
                        if first_td:
                            date_text = first_td.get_text(strip=True)
                            # Try Swedish date format first (common on MFN)
                            for fmt in ['%Y-%m-%d', '%d %b %Y', '%d %B %Y', '%Y-%m-%dT%H:%M:%S', '%d.%m.%Y']:
                                try:
                                    # Handle Swedish month names
                                    date_text_en = date_text.replace('jan', 'Jan').replace('feb', 'Feb').replace('mar', 'Mar').replace('apr', 'Apr').replace('maj', 'May').replace('jun', 'Jun').replace('jul', 'Jul').replace('aug', 'Aug').replace('sep', 'Sep').replace('okt', 'Oct').replace('nov', 'Nov').replace('dec', 'Dec')
                                    date_published = datetime.strptime(date_text_en[:10], fmt)
                                    break
                                except:
                                    continue
                    elif not date_published:
                        # Check if article is within a table row
                        parent_tr = article.find_parent('tr') if hasattr(article, 'find_parent') else None
                        if parent_tr:
                            # Look for the first td in the row (first column = date)
                            first_td = parent_tr.find('td')
                            if first_td:
                                date_text = first_td.get_text(strip=True)
                                # Try Swedish date format first (common on MFN)
                                for fmt in ['%Y-%m-%d', '%d %b %Y', '%d %B %Y', '%Y-%m-%dT%H:%M:%S', '%d.%m.%Y']:
                                    try:
                                        # Handle Swedish month names
                                        date_text_en = date_text.replace('jan', 'Jan').replace('feb', 'Feb').replace('mar', 'Mar').replace('apr', 'Apr').replace('maj', 'May').replace('jun', 'Jun').replace('jul', 'Jul').replace('aug', 'Aug').replace('sep', 'Sep').replace('okt', 'Oct').replace('nov', 'Nov').replace('dec', 'Dec')
                                        date_published = datetime.strptime(date_text_en[:10], fmt)
                                        break
                                    except:
                                        continue
                    
                    # Fallback to standard date finding
                    if not date_published:
                        date_elem = article.find(['time', '.date', '.published'])
                        if date_elem:
                            date_text = date_elem.get('datetime') or date_elem.get_text(strip=True)
                            try:
                                # Try to parse common date formats
                                for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%d/%m/%Y', '%m/%d/%Y']:
                                    try:
                                        date_published = datetime.strptime(date_text[:19], fmt)
                                        break
                                    except:
                                        continue
                            except:
                                pass
                    
                    # Final fallback - use current date if nothing found
                    if not date_published:
                        date_published = datetime.now()
                    
                    # Extract content preview
                    content = ""
                    content_elem = article.find(['p', '.content', '.summary', '.excerpt'])
                    if content_elem:
                        content = content_elem.get_text(strip=True)[:500]
                    
                    # Extract calendar info specific to this article
                    article_calendar_info = self._extract_article_calendar_info(article, title, content)
                    
                    # Merge page-level and article-level calendar info
                    combined_calendar_info = {**page_calendar_info}
                    if article_calendar_info:
                        combined_calendar_info.update(article_calendar_info)
                    
                    # Classify document type
                    doc_type = self._classify_news_type(title, content)
                    
                    # Create news item with proper company name mapping
                    proper_company_name = self._map_to_database_name(company)
                    item = MFNNewsItem(
                        company_name=proper_company_name,
                        title=title,
                        date_published=date_published,
                        content=content,
                        pdf_urls=pdf_links,
                        source_url=source_url,
                        document_type=doc_type,
                        calendar_info=combined_calendar_info if combined_calendar_info else None
                    )
                    current_chunk.append(item)
                    items_created += 1
                    
                    # Check if we need to save a chunk
                    if len(current_chunk) >= chunk_size:
                        if save_callback:
                            print(f"   💾 Saving chunk of {len(current_chunk)} items...")
                            save_callback(current_chunk)
                        all_items.extend(current_chunk)
                        print(f"   📊 Progress: {items_created} items processed")
                        current_chunk = []
                    
            except Exception as e:
                print(f"⚠️  Error parsing article {articles_processed} for {company}: {e}")
                continue
                
        # If no structured parsing worked, use fallback method - FIXED to only get PDFs
        if not items:
            # Look for PDFs from all known hosting services
            pdf_pattern_storage = r'https://storage\.mfn\.se/[^"\'>\s]+\.pdf'  # ⭐ NEW!
            pdf_pattern_cision = r'https://mb\.cision\.com/[^"\'>\s]+\.pdf'
            pdf_pattern_any = r'https?://[^"\'>\s]+\.pdf'
            
            pdf_urls = []
            pdf_urls.extend(re.findall(pdf_pattern_storage, html))  # ⭐ NEW!
            pdf_urls.extend(re.findall(pdf_pattern_cision, html))
            pdf_urls.extend(re.findall(pdf_pattern_any, html))
            
            # Filter out image files that might have .pdf in the URL path but aren't actually PDFs
            actual_pdf_urls = []
            for url in pdf_urls:
                if (url.endswith('.pdf') and 
                    not any(img_ext in url.lower() for img_ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg'])):
                    actual_pdf_urls.append(url)
            pdf_urls = actual_pdf_urls
            
            pdf_urls = list(set(pdf_urls))  # Remove duplicates
            
            if pdf_urls:
                proper_company_name = self._map_to_database_name(company)
                item = MFNNewsItem(
                    company_name=proper_company_name,
                    title=f"Financial Documents Found ({len(pdf_urls)} PDFs)",
                    date_published=datetime.now(),
                    content=f"Found {len(pdf_urls)} PDF documents from {company}",
                    pdf_urls=pdf_urls,
                    source_url=source_url,
                    document_type="mixed",
                    calendar_info=page_calendar_info if page_calendar_info else None
                )
                current_chunk.append(item)
                items_created += 1
        
        # Save any remaining items in the final chunk
        if current_chunk:
            if save_callback:
                print(f"   💾 Saving final chunk of {len(current_chunk)} items...")
                save_callback(current_chunk)
            all_items.extend(current_chunk)
        
        # If all_items is empty (no chunking used), return items as before
        final_items = all_items if all_items else items
        
        # DEBUG: Final summary
        print(f"📊 {company} extraction summary:")
        print(f"   📄 Articles processed: {articles_processed}")
        print(f"   🏗️  Items created: {items_created}")
        print(f"   📋 Total items returned: {len(final_items)}")
        
        return final_items
        
    def _map_to_database_name(self, mfn_slug: str) -> str:
        """Map MFN company slug to database company name using centralized mapping"""
        return get_company_name(mfn_slug)
        
    def _extract_calendar_info(self, soup: BeautifulSoup, company: str) -> Dict[str, Any]:
        """
        Extract financial calendar information from MFN page calendar table
        
        Args:
            soup: BeautifulSoup object of the page
            company: Company name
            
        Returns:
            Dictionary with structured calendar information
        """
        calendar_info = {}
        
        try:
            # Find the official MFN calendar table
            calendar_table = soup.find('table', class_='table-calender')
            
            if not calendar_table:
                print(f"⚠️  No calendar table found for {company}")
                return calendar_info
            
            # Extract calendar events from table rows
            calendar_events = []
            earnings_events = []
            dividend_events = []
            
            tbody = calendar_table.find('tbody')
            if not tbody:
                print(f"⚠️  No tbody found in calendar table for {company}")
                return calendar_info
            
            rows = tbody.find_all('tr')
            print(f"📅 Found {len(rows)} calendar rows for {company}")
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 3:  # Date, Time, Event columns
                    date_cell = cells[0].get_text(strip=True)
                    time_cell = cells[1].get_text(strip=True)  # Usually empty
                    event_cell = cells[2].get_text(strip=True)
                    
                    if not date_cell or not event_cell:
                        continue
                    
                    # Parse the date
                    parsed_date = self._parse_mfn_calendar_date(date_cell)
                    if not parsed_date:
                        continue
                    
                    event_lower = event_cell.lower()
                    
                    # Classify the event type
                    if any(keyword in event_lower for keyword in ['kvartalsrapport', 'quarterly', 'delårsrapport', 'interim']):
                        # Extract quarter and year from event
                        quarter_match = re.search(r'(q[1-4]|[1-4]q|\d{4}-q[1-4])', event_lower)
                        year_match = re.search(r'(20\d{2})', event_cell)
                        
                        earnings_events.append({
                            'date': parsed_date,
                            'event': event_cell,
                            'type': 'quarterly_report',
                            'quarter': quarter_match.group(1) if quarter_match else None,
                            'year': year_match.group(1) if year_match else None,
                            'time': time_cell if time_cell else None
                        })
                        
                    elif any(keyword in event_lower for keyword in ['bokslutskommuniké', 'annual', 'årsbokslut', 'year-end']):
                        year_match = re.search(r'(20\d{2})', event_cell)
                        
                        earnings_events.append({
                            'date': parsed_date,
                            'event': event_cell,
                            'type': 'annual_report',
                            'year': year_match.group(1) if year_match else None,
                            'time': time_cell if time_cell else None
                        })
                        
                    elif any(keyword in event_lower for keyword in ['x-dag', 'utdelning', 'dividend']):
                        # Parse dividend info: "X-dag ordinarie utdelning HEXA B 4.54 SEK"
                        amount_match = re.search(r'(\d+[.,]\d+)\s*(sek|eur|usd)', event_lower)
                        ticker_match = re.search(r'\b([A-Z]{3,6})\s*([AB]?)\b', event_cell)
                        
                        dividend_events.append({
                            'date': parsed_date,
                            'event': event_cell,
                            'type': 'ex_dividend',
                            'amount': amount_match.group(1).replace(',', '.') if amount_match else None,
                            'currency': amount_match.group(2).upper() if amount_match else None,
                            'ticker': ticker_match.group(1) if ticker_match else None,
                            'share_class': ticker_match.group(2) if ticker_match and ticker_match.group(2) else None,
                            'time': time_cell if time_cell else None
                        })
                        
                    elif any(keyword in event_lower for keyword in ['årsstämma', 'agm', 'annual general meeting']):
                        calendar_events.append({
                            'date': parsed_date,
                            'event': event_cell,
                            'type': 'agm',
                            'time': time_cell if time_cell else None
                        })
                        
                    else:
                        # Other events (presentations, etc.)
                        calendar_events.append({
                            'date': parsed_date,
                            'event': event_cell,
                            'type': 'other',
                            'time': time_cell if time_cell else None
                        })
            
            # Structure the extracted data
            if earnings_events:
                calendar_info['earnings'] = {
                    'events': earnings_events,
                    'count': len(earnings_events)
                }
                
            if dividend_events:
                calendar_info['dividend'] = {
                    'events': dividend_events,
                    'count': len(dividend_events),
                    # For compatibility with existing code
                    'parsed_amounts': [
                        {
                            'amount': float(evt['amount']),
                            'currency': evt['currency'],
                            'raw_text': f"{evt['amount']} {evt['currency']}"
                        } for evt in dividend_events if evt['amount'] and evt['currency']
                    ],
                    'x_dag_dates': {
                        'ex_dividend_dates': [evt['date'].strftime('%Y-%m-%d') for evt in dividend_events]
                    }
                }
                
            if calendar_events:
                calendar_info['other_events'] = {
                    'events': calendar_events,
                    'count': len(calendar_events)
                }
                
            # Add extraction metadata
            calendar_info['extracted_at'] = datetime.now().isoformat()
            calendar_info['source'] = 'mfn_calendar_table'
            calendar_info['total_events'] = len(earnings_events) + len(dividend_events) + len(calendar_events)
            
            print(f"✅ Extracted {calendar_info['total_events']} events from calendar table for {company}")
            
        except Exception as e:
            print(f"❌ Error extracting calendar info for {company}: {e}")
            import traceback
            traceback.print_exc()
            calendar_info['error'] = str(e)
        
        return calendar_info
    
    def _parse_mfn_calendar_date(self, date_string: str) -> Optional[date]:
        """Parse MFN calendar date format (YYYY-MM-DD)"""
        try:
            date_string = date_string.strip()
            # MFN typically uses YYYY-MM-DD format
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_string):
                year, month, day = map(int, date_string.split('-'))
                return date(year, month, day)
        except Exception as e:
            print(f"⚠️  Could not parse MFN date '{date_string}': {e}")
        return None
    
    def _extract_article_calendar_info(self, article: BeautifulSoup, title: str, content: str) -> Dict[str, Any]:
        """
        Extract calendar information specific to an article
        
        Args:
            article: BeautifulSoup object of the article
            title: Article title
            content: Article content
            
        Returns:
            Dictionary with article-specific calendar information
        """
        calendar_info = {}
        
        try:
            article_text = f"{title} {content}"
            
            # Check if this is an earnings announcement
            if any(word in article_text.lower() for word in ['earnings', 'quarterly', 'q1', 'q2', 'q3', 'q4', 'rapport']):
                calendar_info['event_type'] = 'earnings_report'
                
                # Extract quarter info
                quarter_match = re.search(r'(Q[1-4]|[1-4]Q|kvartal\s*[1-4])', article_text, re.IGNORECASE)
                if quarter_match:
                    calendar_info['quarter'] = quarter_match.group(1)
                
                # Extract year
                year_match = re.search(r'\b(20\d{2})\b', article_text)
                if year_match:
                    calendar_info['year'] = year_match.group(1)
            
            # Check if this is about upcoming events
            if any(word in article_text.lower() for word in ['webcast', 'conference', 'call', 'presentation']):
                calendar_info['has_webcast'] = True
                
                # Look for specific times in the article
                time_match = re.search(r'(\d{1,2}[:.]\d{2})', article_text)
                if time_match:
                    calendar_info['webcast_time'] = time_match.group(1)
            
            # Check for AGM/EGM
            if any(word in article_text.lower() for word in ['agm', 'egm', 'annual general meeting', 'bolagsstämma']):
                calendar_info['event_type'] = 'shareholder_meeting'
            
            # Check for dividend announcements
            if any(word in article_text.lower() for word in ['dividend', 'utdelning']):
                calendar_info['has_dividend_info'] = True
                
                # Try to extract dividend amount
                dividend_match = re.search(r'(\d+[.,]\d+\s*(?:SEK|EUR|USD|kr))', article_text, re.IGNORECASE)
                if dividend_match:
                    calendar_info['dividend_amount'] = dividend_match.group(1)
            
        except Exception as e:
            print(f"⚠️  Error extracting article calendar info: {e}")
        
        return calendar_info
    
    def _classify_news_type(self, title: str, content: str) -> str:
        """
        Classify the type of financial news - ENHANCED with Swedish terms
        
        Args:
            title: News title
            content: News content
            
        Returns:
            Document type: earnings, m&a, governance, etc.
        """
        text = f"{title} {content}".lower()
        
        # Quarterly reports - English and Swedish
        quarterly_keywords = [
            # English
            "q1", "q2", "q3", "q4", "quarterly", "interim", "quarter", 
            # Swedish 
            "kvartal", "kvartalet", "första kvartalet", "andra kvartalet", 
            "tredje kvartalet", "fjärde kvartalet", "delårsrapport"
        ]
        if any(word in text for word in quarterly_keywords):
            return "quarterly_report"
        
        # Annual reports - English and Swedish  
        annual_keywords = [
            # English
            "annual", "yearly", "full year", "year-end",
            # Swedish
            "årsrapport", "helår", "årsstämma", "årsredovisning"
        ]
        if any(word in text for word in annual_keywords):
            return "annual_report"
            
        # Corporate actions - English and Swedish
        corporate_keywords = [
            # English
            "acqui", "merger", "divest", "invests", "acquisition", "investment",
            # Swedish  
            "förvärv", "köper", "investering", "avyttring", "fusion"
        ]
        if any(word in text for word in corporate_keywords):
            return "corporate_action"
            
        # Governance - English and Swedish
        governance_keywords = [
            # English
            "board", "agm", "voting", "shares", "governance", "directors",
            # Swedish
            "styrelse", "bolagsstämma", "röstning", "aktier", "vd", "ledning"
        ]
        if any(word in text for word in governance_keywords):
            return "governance"
        else:
            return "press_release"

# Example usage:
"""
mfn_collector = MFNCollector()
all_news = await mfn_collector.collect_all_companies()

for item in all_news:
    print(f"{item.company_name}: {item.title}")
    print(f"PDFs: {item.pdf_urls}")
    print("---")
"""