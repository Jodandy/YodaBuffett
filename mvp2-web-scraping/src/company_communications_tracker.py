"""
Company Communications Tracker
Monitors all communication channels from Nordic companies for financial updates
"""
import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import hashlib

import feedparser
import imaplib
import email
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup


class CommunicationType(Enum):
    QUARTERLY_REPORT = "quarterly_report"
    ANNUAL_REPORT = "annual_report"
    PRESS_RELEASE = "press_release"
    IR_ANNOUNCEMENT = "ir_announcement"
    CALENDAR_UPDATE = "calendar_update"
    DIVIDEND_ANNOUNCEMENT = "dividend_announcement"
    EARNINGS_CALL = "earnings_call"
    OTHER = "other"


class CommunicationChannel(Enum):
    RSS_FEED = "rss_feed"
    EMAIL_SUBSCRIPTION = "email_subscription"
    PRESS_RELEASE_SITE = "press_release_site"
    IR_CALENDAR = "ir_calendar"
    SOCIAL_MEDIA = "social_media"
    REGULATORY_FILING = "regulatory_filing"


@dataclass
class CommunicationItem:
    id: str
    company_id: str
    company_name: str
    channel: CommunicationChannel
    type: CommunicationType
    title: str
    content: str
    published_date: datetime
    source_url: str
    pdf_urls: List[str]
    language: str
    priority: int  # 1=urgent, 2=high, 3=normal, 4=low
    processed: bool = False
    metadata: Dict = None


class CompanyCommunicationsTracker:
    """
    Comprehensive tracking of all communications from Nordic companies
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tracked_items = set()  # For deduplication
        self.communication_patterns = self._load_communication_patterns()
        
    def _load_communication_patterns(self) -> Dict[str, List[str]]:
        """Load patterns for classifying communications"""
        return {
            "quarterly_report": [
                r"delårsrapport", r"kvartalsrapport", r"q[1-4].*\d{4}",
                r"interim.*report", r"quarterly.*report", r"first.*quarter",
                r"second.*quarter", r"third.*quarter", r"fourth.*quarter"
            ],
            "annual_report": [
                r"årsredovisning", r"annual.*report", r"yearly.*report",
                r"årsbokslut", r"helårsrapport", r"full.*year"
            ],
            "press_release": [
                r"pressmeddelande", r"press.*release", r"announcement",
                r"meddelande", r"tillkännagivande"
            ],
            "earnings_call": [
                r"resultatkonferens", r"earnings.*call", r"investor.*call",
                r"telefonkonferens", r"webcast", r"presentation"
            ],
            "dividend": [
                r"utdelning", r"dividend", r"actionnaire", r"aktieägare"
            ]
        }
    
    async def track_all_communications(self, companies: List[Dict]) -> List[CommunicationItem]:
        """
        Monitor all communication channels for multiple companies
        """
        self.logger.info(f"🔍 Tracking communications for {len(companies)} companies")
        
        all_communications = []
        
        # Process companies in batches to avoid overwhelming systems
        batch_size = 5
        for i in range(0, len(companies), batch_size):
            batch = companies[i:i + batch_size]
            
            # Process batch concurrently
            batch_tasks = [
                self.track_company_communications(company) for company in batch
            ]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    self.logger.error(f"Batch processing error: {result}")
                else:
                    all_communications.extend(result)
            
            # Rate limiting between batches
            await asyncio.sleep(2)
        
        # Sort by priority and date
        all_communications.sort(key=lambda x: (x.priority, -x.published_date.timestamp()))
        
        self.logger.info(f"📡 Found {len(all_communications)} new communications")
        return all_communications
    
    async def track_company_communications(self, company: Dict) -> List[CommunicationItem]:
        """
        Track all communication channels for a single company
        """
        communications = []
        company_id = company["id"]
        company_name = company["name"]
        
        self.logger.debug(f"📡 Tracking {company_name}")
        
        # Channel 1: RSS Feeds
        rss_communications = await self.track_rss_feeds(company)
        communications.extend(rss_communications)
        
        # Channel 2: Email Subscriptions
        email_communications = await self.track_email_subscriptions(company)
        communications.extend(email_communications)
        
        # Channel 3: Press Release Pages
        press_communications = await self.track_press_releases(company)
        communications.extend(press_communications)
        
        # Channel 4: IR Calendar Updates
        calendar_communications = await self.track_ir_calendar(company)
        communications.extend(calendar_communications)
        
        # Channel 5: Regulatory Filings (if available)
        regulatory_communications = await self.track_regulatory_filings(company)
        communications.extend(regulatory_communications)
        
        return communications
    
    async def track_rss_feeds(self, company: Dict) -> List[CommunicationItem]:
        """Track company RSS feeds for financial communications"""
        communications = []
        
        rss_feeds = company.get("rss_feeds", [])
        if not rss_feeds:
            return communications
        
        for feed_config in rss_feeds:
            try:
                feed_url = feed_config["url"]
                feed_type = feed_config.get("type", "general")
                
                self.logger.debug(f"📡 Checking RSS: {feed_url}")
                
                # Parse RSS feed
                feed = feedparser.parse(feed_url)
                
                if feed.bozo:
                    self.logger.warning(f"RSS feed parsing error: {feed_url}")
                    continue
                
                # Process entries from last 7 days
                cutoff_date = datetime.now() - timedelta(days=7)
                
                for entry in feed.entries:
                    try:
                        # Parse published date
                        published = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') and entry.published_parsed else datetime.now()
                        
                        if published < cutoff_date:
                            continue
                        
                        # Create unique ID for deduplication
                        item_id = hashlib.md5(f"{company['id']}_{entry.link}_{published}".encode()).hexdigest()
                        
                        if item_id in self.tracked_items:
                            continue
                        
                        # Classify communication type
                        comm_type = self._classify_communication(entry.title, entry.description if hasattr(entry, 'description') else "")
                        
                        # Extract PDF URLs
                        pdf_urls = self._extract_pdf_urls(entry.description if hasattr(entry, 'description') else "", entry.link)
                        
                        # Determine priority
                        priority = self._calculate_priority(comm_type, entry.title)
                        
                        communication = CommunicationItem(
                            id=item_id,
                            company_id=company["id"],
                            company_name=company["name"],
                            channel=CommunicationChannel.RSS_FEED,
                            type=comm_type,
                            title=entry.title,
                            content=entry.description if hasattr(entry, 'description') else "",
                            published_date=published,
                            source_url=entry.link,
                            pdf_urls=pdf_urls,
                            language=company.get("language", "sv"),
                            priority=priority,
                            metadata={
                                "rss_feed": feed_url,
                                "feed_type": feed_type
                            }
                        )
                        
                        communications.append(communication)
                        self.tracked_items.add(item_id)
                        
                    except Exception as e:
                        self.logger.error(f"RSS entry processing error: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"RSS feed error for {company['name']}: {e}")
                continue
        
        return communications
    
    async def track_email_subscriptions(self, company: Dict) -> List[CommunicationItem]:
        """Monitor IR email subscriptions"""
        communications = []
        
        # This would connect to yodabuffett.ir@gmail.com
        # Parse recent emails for this company
        
        try:
            email_config = company.get("email_subscription")
            if not email_config:
                return communications
            
            # Connect to email account
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login("yodabuffett.ir@gmail.com", "!BuffayTime3214")  # From docs/operations/human-operator-guide.md
            mail.select("inbox")
            
            # Search for emails from this company (last 7 days)
            company_domains = self._extract_company_domains(company)
            date_filter = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
            
            for domain in company_domains:
                search_query = f'(FROM "{domain}" SINCE {date_filter})'
                typ, data = mail.search(None, search_query)
                
                for num in data[0].split():
                    typ, data = mail.fetch(num, '(RFC822)')
                    raw_email = data[0][1]
                    
                    email_message = email.message_from_bytes(raw_email)
                    
                    # Parse email content
                    subject = email_message["Subject"]
                    from_addr = email_message["From"]
                    date_header = email_message["Date"]
                    
                    # Get email body
                    body = self._get_email_body(email_message)
                    
                    # Classify and extract information
                    comm_type = self._classify_communication(subject, body)
                    pdf_urls = self._extract_pdf_urls(body, "")
                    
                    # Create communication item
                    item_id = hashlib.md5(f"{company['id']}_email_{subject}_{date_header}".encode()).hexdigest()
                    
                    if item_id not in self.tracked_items:
                        communication = CommunicationItem(
                            id=item_id,
                            company_id=company["id"],
                            company_name=company["name"],
                            channel=CommunicationChannel.EMAIL_SUBSCRIPTION,
                            type=comm_type,
                            title=subject,
                            content=body,
                            published_date=datetime.strptime(date_header, "%a, %d %b %Y %H:%M:%S %z").replace(tzinfo=None),
                            source_url="",
                            pdf_urls=pdf_urls,
                            language=company.get("language", "sv"),
                            priority=self._calculate_priority(comm_type, subject),
                            metadata={
                                "from_email": from_addr,
                                "email_channel": "ir_subscription"
                            }
                        )
                        
                        communications.append(communication)
                        self.tracked_items.add(item_id)
            
            mail.close()
            mail.logout()
            
        except Exception as e:
            self.logger.error(f"Email tracking error for {company['name']}: {e}")
        
        return communications
    
    async def track_press_releases(self, company: Dict) -> List[CommunicationItem]:
        """Scrape company press release pages"""
        communications = []
        
        press_config = company.get("press_releases")
        if not press_config:
            return communications
        
        try:
            press_url = press_config["url"]
            selectors = press_config.get("selectors", {})
            
            # Fetch press release page
            headers = {
                "User-Agent": "YodaBuffett-Nordic/1.0 (+https://yodabuffett.com/about)",
                "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"
            }
            
            response = requests.get(press_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract press releases
            release_elements = soup.select(selectors.get("press_items", ".press-release"))
            
            for element in release_elements[:10]:  # Limit to recent 10
                try:
                    title_elem = element.select_one(selectors.get("title", "h3"))
                    date_elem = element.select_one(selectors.get("date", ".date"))
                    link_elem = element.select_one(selectors.get("link", "a"))
                    
                    if not all([title_elem, date_elem, link_elem]):
                        continue
                    
                    title = title_elem.get_text(strip=True)
                    date_text = date_elem.get_text(strip=True)
                    link = link_elem.get("href")
                    
                    # Parse date
                    published = self._parse_swedish_date(date_text)
                    if not published or published < datetime.now() - timedelta(days=7):
                        continue
                    
                    # Make link absolute
                    if link.startswith("/"):
                        link = f"{company['website']}{link}"
                    
                    # Create communication item
                    item_id = hashlib.md5(f"{company['id']}_press_{link}".encode()).hexdigest()
                    
                    if item_id not in self.tracked_items:
                        comm_type = self._classify_communication(title, "")
                        
                        communication = CommunicationItem(
                            id=item_id,
                            company_id=company["id"],
                            company_name=company["name"],
                            channel=CommunicationChannel.PRESS_RELEASE_SITE,
                            type=comm_type,
                            title=title,
                            content="",
                            published_date=published,
                            source_url=link,
                            pdf_urls=[],
                            language=company.get("language", "sv"),
                            priority=self._calculate_priority(comm_type, title),
                            metadata={
                                "press_page": press_url
                            }
                        )
                        
                        communications.append(communication)
                        self.tracked_items.add(item_id)
                        
                except Exception as e:
                    self.logger.error(f"Press release parsing error: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Press release tracking error for {company['name']}: {e}")
        
        return communications
    
    async def track_ir_calendar(self, company: Dict) -> List[CommunicationItem]:
        """Monitor IR calendar for event updates"""
        communications = []
        
        ir_calendar = company.get("ir_calendar")
        if not ir_calendar:
            return communications
        
        try:
            calendar_url = ir_calendar["url"]
            
            # Fetch calendar page
            headers = {
                "User-Agent": "YodaBuffett-Nordic/1.0 (+https://yodabuffett.com/about)",
                "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8"
            }
            
            response = requests.get(calendar_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract calendar events (next 90 days)
            selectors = ir_calendar.get("selectors", {})
            event_elements = soup.select(selectors.get("events", ".calendar-event"))
            
            for element in event_elements:
                try:
                    date_elem = element.select_one(selectors.get("date", ".event-date"))
                    title_elem = element.select_one(selectors.get("title", ".event-title"))
                    type_elem = element.select_one(selectors.get("type", ".event-type"))
                    
                    if not all([date_elem, title_elem]):
                        continue
                    
                    event_date = self._parse_swedish_date(date_elem.get_text(strip=True))
                    title = title_elem.get_text(strip=True)
                    event_type = type_elem.get_text(strip=True) if type_elem else ""
                    
                    # Only track future events within 90 days
                    if not event_date or event_date < datetime.now() or event_date > datetime.now() + timedelta(days=90):
                        continue
                    
                    # Create communication item for calendar update
                    item_id = hashlib.md5(f"{company['id']}_calendar_{title}_{event_date}".encode()).hexdigest()
                    
                    if item_id not in self.tracked_items:
                        comm_type = self._classify_calendar_event(title, event_type)
                        
                        communication = CommunicationItem(
                            id=item_id,
                            company_id=company["id"],
                            company_name=company["name"],
                            channel=CommunicationChannel.IR_CALENDAR,
                            type=comm_type,
                            title=f"Calendar Update: {title}",
                            content=f"Event scheduled for {event_date.strftime('%Y-%m-%d')}",
                            published_date=datetime.now(),  # Discovery date
                            source_url=calendar_url,
                            pdf_urls=[],
                            language=company.get("language", "sv"),
                            priority=self._calculate_priority(comm_type, title),
                            metadata={
                                "event_date": event_date.isoformat(),
                                "event_type": event_type
                            }
                        )
                        
                        communications.append(communication)
                        self.tracked_items.add(item_id)
                        
                except Exception as e:
                    self.logger.error(f"Calendar event parsing error: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"IR calendar tracking error for {company['name']}: {e}")
        
        return communications
    
    async def track_regulatory_filings(self, company: Dict) -> List[CommunicationItem]:
        """Track regulatory filings if available"""
        # For Nordic countries, this might include:
        # - Finansinspektionen (Sweden)
        # - Finanstilsynet (Norway, Denmark)
        # - Finanssivalvonta (Finland)
        
        # Implementation would depend on available APIs
        return []
    
    def _classify_communication(self, title: str, content: str) -> CommunicationType:
        """Classify communication type based on content"""
        text = f"{title} {content}".lower()
        
        for comm_type, patterns in self.communication_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    if comm_type == "quarterly_report":
                        return CommunicationType.QUARTERLY_REPORT
                    elif comm_type == "annual_report":
                        return CommunicationType.ANNUAL_REPORT
                    elif comm_type == "press_release":
                        return CommunicationType.PRESS_RELEASE
                    elif comm_type == "earnings_call":
                        return CommunicationType.EARNINGS_CALL
                    elif comm_type == "dividend":
                        return CommunicationType.DIVIDEND_ANNOUNCEMENT
        
        return CommunicationType.OTHER
    
    def _classify_calendar_event(self, title: str, event_type: str) -> CommunicationType:
        """Classify calendar events"""
        text = f"{title} {event_type}".lower()
        
        if any(term in text for term in ["q1", "q2", "q3", "delårs", "interim"]):
            return CommunicationType.QUARTERLY_REPORT
        elif any(term in text for term in ["annual", "års", "helår"]):
            return CommunicationType.ANNUAL_REPORT
        elif any(term in text for term in ["call", "konferens", "presentation"]):
            return CommunicationType.EARNINGS_CALL
        else:
            return CommunicationType.CALENDAR_UPDATE
    
    def _calculate_priority(self, comm_type: CommunicationType, title: str) -> int:
        """Calculate priority (1=urgent, 4=low)"""
        if comm_type in [CommunicationType.QUARTERLY_REPORT, CommunicationType.ANNUAL_REPORT]:
            return 1  # Urgent
        elif comm_type in [CommunicationType.EARNINGS_CALL, CommunicationType.DIVIDEND_ANNOUNCEMENT]:
            return 2  # High
        elif comm_type == CommunicationType.PRESS_RELEASE:
            # Check for urgent keywords
            urgent_keywords = ["resultat", "earnings", "profit", "förvärv", "acquisition", "konkurs", "bankruptcy"]
            if any(keyword in title.lower() for keyword in urgent_keywords):
                return 2  # High
            return 3  # Normal
        else:
            return 4  # Low
    
    def _extract_pdf_urls(self, content: str, base_url: str) -> List[str]:
        """Extract PDF URLs from content"""
        pdf_urls = []
        
        # Find PDF links
        pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
        matches = re.findall(pdf_pattern, content, re.IGNORECASE)
        
        for match in matches:
            if match.startswith("http"):
                pdf_urls.append(match)
            elif match.startswith("/"):
                # Relative URL, make absolute
                if base_url:
                    from urllib.parse import urljoin
                    pdf_urls.append(urljoin(base_url, match))
        
        return pdf_urls
    
    def _extract_company_domains(self, company: Dict) -> List[str]:
        """Extract email domains for the company"""
        domains = []
        
        if company.get("ir_email"):
            domain = company["ir_email"].split("@")[-1]
            domains.append(domain)
        
        if company.get("website"):
            from urllib.parse import urlparse
            domain = urlparse(company["website"]).netloc
            domains.append(domain)
        
        return domains
    
    def _get_email_body(self, email_message):
        """Extract email body text"""
        body = ""
        
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    body += part.get_payload(decode=True).decode("utf-8", errors="ignore")
        else:
            body = email_message.get_payload(decode=True).decode("utf-8", errors="ignore")
        
        return body
    
    def _parse_swedish_date(self, date_text: str) -> Optional[datetime]:
        """Parse Swedish date formats"""
        # Common Swedish date patterns
        patterns = [
            r"(\d{1,2})\s+(januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\s+(\d{4})",
            r"(\d{4})-(\d{2})-(\d{2})",
            r"(\d{2})/(\d{2})/(\d{4})",
            r"(\d{1,2})\.(\d{1,2})\.(\d{4})"
        ]
        
        swedish_months = {
            "januari": 1, "februari": 2, "mars": 3, "april": 4,
            "maj": 5, "juni": 6, "juli": 7, "augusti": 8,
            "september": 9, "oktober": 10, "november": 11, "december": 12
        }
        
        for pattern in patterns:
            match = re.search(pattern, date_text.lower())
            if match:
                try:
                    if "januari" in pattern or any(month in date_text.lower() for month in swedish_months.keys()):
                        # Swedish month names
                        day, month_name, year = match.groups()
                        month = swedish_months[month_name]
                        return datetime(int(year), month, int(day))
                    else:
                        # Numeric formats
                        parts = match.groups()
                        if len(parts) == 3:
                            if len(parts[0]) == 4:  # YYYY-MM-DD
                                return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                            else:  # DD/MM/YYYY or DD.MM.YYYY
                                return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                except ValueError:
                    continue
        
        return None


# Example usage
async def main():
    """Example of tracking company communications"""
    tracker = CompanyCommunicationsTracker()
    
    # Example company configurations
    companies = [
        {
            "id": "volvo-group",
            "name": "Volvo Group",
            "website": "https://www.volvogroup.com",
            "language": "sv",
            "rss_feeds": [
                {
                    "url": "https://www.volvogroup.com/se/news-and-media/events/_jcr_content/root/responsivegrid/eventlist.feed.xml",
                    "type": "financial_events"
                }
            ],
            "email_subscription": {
                "ir_email": "investor.relations@volvo.com"
            },
            "press_releases": {
                "url": "https://www.volvogroup.com/se/news-and-media/press-releases/",
                "selectors": {
                    "press_items": ".press-release-item",
                    "title": ".title",
                    "date": ".date",
                    "link": "a"
                }
            },
            "ir_calendar": {
                "url": "https://www.volvogroup.com/investors/calendar/",
                "selectors": {
                    "events": ".calendar-event",
                    "date": ".event-date",
                    "title": ".event-title",
                    "type": ".event-type"
                }
            }
        }
    ]
    
    # Track communications
    communications = await tracker.track_all_communications(companies)
    
    print(f"📡 Found {len(communications)} communications:")
    for comm in communications[:5]:  # Show first 5
        print(f"  • {comm.company_name}: {comm.title} [{comm.type.value}]")


if __name__ == "__main__":
    asyncio.run(main())