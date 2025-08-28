"""
MFN.se Universal Financial News Collector
Single source for all Swedish company financial news and reports
"""
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Any
from datetime import datetime
import re
from dataclasses import dataclass

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
        # Company name mapping: database_name -> mfn_url_slug
        self.company_url_mapping = {
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
        full_backfill: bool = False
    ) -> List[MFNNewsItem]:
        """
        Collect news for a specific company with pagination support
        
        Args:
            session: HTTP session
            company: Company slug (e.g., 'volvo', 'astrazeneca')
            limit: Number of items to fetch (default: 50 for daily updates)
            full_backfill: If True, fetch 1000+ items for historical data
            
        Returns:
            List of news items for the company
        """
        if full_backfill:
            limit = 150  # ~5 years of data (30 events/year × 5 years)
            
        url = f"{self.base_url}/{company}?limit={limit}"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return []
                    
                html = await response.text()
                return self._parse_mfn_page(company, html, url)
                
        except Exception as e:
            print(f"Failed to collect {company}: {e}")
            return []
            
    def _parse_mfn_page(
        self, 
        company: str, 
        html: str, 
        source_url: str
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
        
        # Find all news articles - MFN uses article tags or similar containers
        articles = soup.find_all(['article', 'div'], class_=re.compile(r'(article|news|item|post)', re.I))
        
        # If no structured articles found, look for any content with PDF links
        if not articles:
            articles = [soup]  # Use entire page as fallback
        
        for article in articles[:50]:  # Limit to prevent too many items
            try:
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
                pdf_pattern = r'https://mb\.cision\.com/[^"\'>\s]+\.pdf'  # Must end with .pdf
                cision_pdfs = re.findall(pdf_pattern, article_html)
                pdf_links.extend(cision_pdfs)
                
                # Filter out any non-PDF files that might have slipped through
                actual_pdf_links = []
                for link in pdf_links:
                    # Double-check: must end with .pdf and not be an image
                    if (link.endswith('.pdf') and 
                        not any(img_ext in link.lower() for img_ext in ['.png', '.jpg', '.jpeg', '.gif', '.svg'])):
                        actual_pdf_links.append(link)
                
                pdf_links = actual_pdf_links
                
                # Remove duplicates
                pdf_links = list(set(pdf_links))
                
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
                    
                    # Try to extract date
                    date_published = datetime.now()
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
                    items.append(item)
                    
            except Exception as e:
                print(f"⚠️  Error parsing article for {company}: {e}")
                continue
                
        # If no structured parsing worked, use fallback method - FIXED to only get PDFs
        if not items:
            # Only look for actual PDF files from Cision
            pdf_pattern = r'https://mb\.cision\.com/[^"\'>\s]+\.pdf'
            pdf_urls = re.findall(pdf_pattern, html)
            
            # Also look for any other .pdf links
            pdf_pattern2 = r'https?://[^"\'>\s]+\.pdf'
            pdf_urls.extend(re.findall(pdf_pattern2, html))
            
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
                items.append(item)
        
        return items
        
    def _map_to_database_name(self, mfn_slug: str) -> str:
        """Map MFN company slug to database company name"""
        # Reverse mapping from company_url_mapping
        slug_to_name = {
            "volvo": "Volvo Group",
            "astrazeneca": "AstraZeneca", 
            "atlas-copco": "Atlas Copco AB",
            "ericsson": "Telefonaktiebolaget LM Ericsson",
            "handm": "H&M Hennes & Mauritz AB",
            "sandvik": "Sandvik AB",
            "nordea": "Nordea Bank Abp",
            "investor": "Investor AB",
            "abb": "ABB Ltd",
            "hexagon": "Hexagon AB"
        }
        
        return slug_to_name.get(mfn_slug.lower(), mfn_slug.title())
        
    def _extract_calendar_info(self, soup: BeautifulSoup, company: str) -> Dict[str, Any]:
        """
        Extract financial calendar information from MFN page
        
        Args:
            soup: BeautifulSoup object of the page
            company: Company name
            
        Returns:
            Dictionary with calendar information
        """
        calendar_info = {}
        
        try:
            # Look for common calendar indicators
            calendar_keywords = [
                'earnings', 'report', 'quarterly', 'annual', 'interim',
                'webcast', 'conference', 'call', 'presentation',
                'agm', 'annual general meeting', 'dividend'
            ]
            
            # Search for dates and times in various formats
            date_patterns = [
                r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{4})\b',  # DD/MM/YYYY or DD-MM-YYYY
                r'\b(\d{4}[-/]\d{1,2}[-/]\d{1,2})\b',  # YYYY/MM/DD or YYYY-MM-DD
                r'\b(\d{1,2}\s+(?:januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\s+\d{4})\b',  # Swedish months
                r'\b(\d{1,2}\s+(?:january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4})\b',  # English months
            ]
            
            # Search for time patterns (excluding dividend amounts)
            time_patterns = [
                r'\b(\d{1,2}:\d{2})\b',  # HH:MM format only
            ]
            
            page_text = soup.get_text()
            
            # Extract upcoming dates
            upcoming_dates = []
            for pattern in date_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                upcoming_dates.extend(matches)
            
            # Extract times (filter out dividend amounts)
            times = []
            for pattern in time_patterns:
                matches = re.findall(pattern, page_text)
                # Filter out anything that looks like a dividend amount (has currency context nearby)
                filtered_times = []
                for match in matches:
                    # Look for currency context around this match
                    match_pos = page_text.find(match)
                    if match_pos >= 0:
                        context = page_text[max(0, match_pos-50):match_pos+50].lower()
                        # Skip if currency keywords are nearby
                        if not any(currency in context for currency in ['sek', 'eur', 'usd', 'kr', 'kronor', 'utdelning', 'dividend']):
                            filtered_times.append(match)
                times.extend(filtered_times)
            
            # Look for earnings-related information
            earnings_info = {}
            if any(keyword in page_text.lower() for keyword in ['earnings', 'quarterly', 'rapport', 'delårsrapport']):
                # Try to find Q1, Q2, Q3, Q4 mentions
                quarter_pattern = r'(Q[1-4]|[1-4]Q|kvartal\s*[1-4])'
                quarters = re.findall(quarter_pattern, page_text, re.IGNORECASE)
                if quarters:
                    earnings_info['quarters_mentioned'] = list(set(quarters))
                
                # Look for year mentions
                year_pattern = r'\b(20\d{2})\b'
                years = re.findall(year_pattern, page_text)
                if years:
                    earnings_info['years_mentioned'] = list(set(years))
            
            # Look for webcast/conference call info
            webcast_info = {}
            if any(keyword in page_text.lower() for keyword in ['webcast', 'conference', 'call', 'presentation']):
                # Look for webcast URLs
                webcast_urls = re.findall(r'https?://[^\s<>"`]+(?:webcast|stream|call)', page_text, re.IGNORECASE)
                if webcast_urls:
                    webcast_info['urls'] = webcast_urls
                
                # Look for registration info
                if any(word in page_text.lower() for word in ['register', 'registration', 'registrera']):
                    webcast_info['registration_required'] = True
            
            # Look for dividend information with enhanced parsing
            dividend_info = {}
            if any(keyword in page_text.lower() for keyword in ['dividend', 'utdelning', 'utdeln']):
                
                # Enhanced dividend amount parsing - only look for dividend-specific contexts
                dividend_patterns = [
                    r'(?:utdelning|dividend)[^\d]*(\d+[.,]\d+)\s*(SEK|EUR|USD|kr|kronor)\s*(?:per|/)\s*(?:aktie|share|stock)',  # "utdelning 2.50 SEK per aktie"
                    r'(?:utdelning|dividend)[^\d]*(\d+[.,]\d+)\s*(SEK|EUR|USD|kr|kronor)',  # "utdelning 2.50 SEK"
                    r'(\d+[.,]\d+)\s*(SEK|EUR|USD|kr|kronor)\s*(?:per|/)\s*(?:aktie|share|stock)',  # "2.50 SEK per aktie"
                ]
                
                parsed_dividends = []
                for pattern in dividend_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    for amount, currency in matches:
                        # Normalize currency
                        currency_normalized = currency.upper()
                        if currency_normalized in ['KR', 'KRONOR']:
                            currency_normalized = 'SEK'
                        
                        # Normalize amount (Swedish uses comma as decimal separator)
                        amount_normalized = amount.replace(',', '.')
                        amount_float = float(amount_normalized)
                        
                        parsed_dividends.append({
                            'amount': amount_float,
                            'currency': currency_normalized,
                            'raw_text': f"{amount} {currency}"
                        })
                
                if parsed_dividends:
                    dividend_info['parsed_amounts'] = parsed_dividends
                    # Keep original for backwards compatibility
                    dividend_info['amounts_mentioned'] = [d['raw_text'] for d in parsed_dividends]
                
                # Look for dividend type indicators
                dividend_type = 'regular'  # default
                if any(word in page_text.lower() for word in ['extra', 'särskild', 'special']):
                    dividend_type = 'special'
                elif any(word in page_text.lower() for word in ['interim', 'delårs']):
                    dividend_type = 'interim'
                dividend_info['dividend_type'] = dividend_type
                
                # Look for Swedish X-dag dates
                x_dag_info = {}
                
                # Ex-dividend date patterns
                ex_patterns = [
                    r'ex[-\s]?(?:dag|dividend)[\s:]*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                    r'ex[-\s]?(?:dag|dividend)[\s:]*(\d{1,2}\s+\w+\s+\d{4})',
                ]
                
                for pattern in ex_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    if matches:
                        x_dag_info['ex_dividend_dates'] = matches
                        break
                
                # Record date (Avstämningsdag) patterns
                record_patterns = [
                    r'avstämningsdag[\s:]*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                    r'avstämningsdag[\s:]*(\d{1,2}\s+\w+\s+\d{4})',
                    r'record\s+date[\s:]*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                ]
                
                for pattern in record_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    if matches:
                        x_dag_info['record_dates'] = matches
                        break
                
                # Payment date (Utbetalningsdag) patterns  
                payment_patterns = [
                    r'utbetalning(?:sdag)?[\s:]*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                    r'utbetalning(?:sdag)?[\s:]*(\d{1,2}\s+\w+\s+\d{4})',
                    r'payment\s+date[\s:]*(\d{1,2}[-/]\d{1,2}[-/]\d{4})',
                ]
                
                for pattern in payment_patterns:
                    matches = re.findall(pattern, page_text, re.IGNORECASE)
                    if matches:
                        x_dag_info['payment_dates'] = matches
                        break
                
                if x_dag_info:
                    dividend_info['x_dag_dates'] = x_dag_info
                    
                # General dividend mention flag
                if any(word in page_text.lower() for word in ['ex-dividend', 'avstämningsdag']):
                    dividend_info['ex_dividend_mentioned'] = True
            
            # Compile calendar info
            if upcoming_dates:
                calendar_info['upcoming_dates'] = list(set(upcoming_dates))
            if times:
                calendar_info['times_mentioned'] = list(set(times))
            if earnings_info:
                calendar_info['earnings'] = earnings_info
            if webcast_info:
                calendar_info['webcast'] = webcast_info
            if dividend_info:
                calendar_info['dividend'] = dividend_info
            
            # Add extraction metadata
            calendar_info['extracted_at'] = datetime.now().isoformat()
            calendar_info['source'] = 'mfn_page_scan'
            
        except Exception as e:
            print(f"⚠️  Error extracting calendar info for {company}: {e}")
            calendar_info['error'] = str(e)
        
        return calendar_info
    
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