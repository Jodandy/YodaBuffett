"""
Swedish Financial Calendar Collector
Production-ready collector for Swedish company IR calendars
"""
import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import re

import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, update

from shared.database import AsyncSessionLocal
from shared.monitoring import record_collection_attempt
from ...models import NordicCompany, NordicCalendarEvent, NordicDataSource


@dataclass
class CalendarEventData:
    """Calendar event data structure"""
    company_id: str
    event_type: str
    event_date: date
    event_time: Optional[str]
    title: str
    description: Optional[str] = None
    location: Optional[str] = None
    webcast_url: Optional[str] = None
    source_url: Optional[str] = None
    confirmed: bool = False


class SwedishCalendarCollector:
    """
    Production-ready Swedish financial calendar collector
    Collects upcoming financial events from Swedish company IR pages
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session: Optional[aiohttp.ClientSession] = None
        self.swedish_months = {
            'januari': 1, 'februari': 2, 'mars': 3, 'april': 4,
            'maj': 5, 'juni': 6, 'juli': 7, 'augusti': 8,
            'september': 9, 'oktober': 10, 'november': 11, 'december': 12
        }
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                'User-Agent': 'YodaBuffett-Nordic/1.0 (+https://yodabuffett.com/about)',
                'Accept-Language': 'sv-SE,sv;q=0.9,en;q=0.8'
            }
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def collect_all_swedish_calendars(self) -> Dict[str, int]:
        """
        Collect calendars from all Swedish companies
        Returns: {company_id: events_found}
        """
        results = {}
        
        async with AsyncSessionLocal() as db:
            # Get all Swedish companies with calendar sources
            query = select(NordicCompany, NordicDataSource).join(
                NordicDataSource, NordicCompany.id == NordicDataSource.company_id
            ).where(
                NordicCompany.country == 'SE',
                NordicDataSource.source_type == 'ir_calendar',
                NordicDataSource.status == 'active'
            )
            
            result = await db.execute(query)
            companies_with_sources = result.all()
            
            self.logger.info(f"🗓️ Collecting calendars from {len(companies_with_sources)} Swedish companies")
            
            # Process companies in batches to avoid overwhelming servers
            batch_size = 3
            for i in range(0, len(companies_with_sources), batch_size):
                batch = companies_with_sources[i:i + batch_size]
                
                batch_tasks = [
                    self._collect_company_calendar(company, source, db)
                    for company, source in batch
                ]
                
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for j, result in enumerate(batch_results):
                    company, source = batch[j]
                    company_id = str(company.id)
                    
                    if isinstance(result, Exception):
                        self.logger.error(f"Calendar collection failed for {company.name}: {result}")
                        results[company_id] = 0
                        record_collection_attempt("ir_calendar", "failed", company.name)
                    else:
                        results[company_id] = result
                        record_collection_attempt("ir_calendar", "success", company.name)
                
                # Rate limiting between batches
                await asyncio.sleep(2)
            
            await db.commit()
        
        total_events = sum(results.values())
        self.logger.info(f"📅 Collected {total_events} calendar events from {len(results)} companies")
        
        return results
    
    async def _collect_company_calendar(
        self, 
        company: NordicCompany, 
        source: NordicDataSource,
        db: AsyncSession
    ) -> int:
        """Collect calendar events for a single Swedish company"""
        
        try:
            config = source.config or {}
            calendar_url = config.get('url', f"{company.ir_website}/calendar/")
            selectors = config.get('selectors', {})
            
            self.logger.debug(f"🗓️ Collecting calendar for {company.name}: {calendar_url}")
            
            # Fetch calendar page
            async with self.session.get(calendar_url) as response:
                if response.status != 200:
                    self.logger.warning(f"Calendar page returned {response.status} for {company.name}")
                    return 0
                
                html_content = await response.text()
            
            # Parse events
            events = await self._parse_calendar_page(
                html_content, 
                company.id, 
                calendar_url, 
                selectors,
                company.name
            )
            
            # Store events in database
            events_stored = await self._store_calendar_events(events, db)
            
            # Update source last_success
            await db.execute(
                update(NordicDataSource)
                .where(NordicDataSource.id == source.id)
                .values(
                    last_success=datetime.utcnow(),
                    failure_count=0
                )
            )
            
            self.logger.info(f"✅ {company.name}: Found {len(events)} events, stored {events_stored}")
            return events_stored
            
        except Exception as e:
            self.logger.error(f"Calendar collection failed for {company.name}: {e}")
            
            # Update source failure count
            await db.execute(
                update(NordicDataSource)
                .where(NordicDataSource.id == source.id)
                .values(failure_count=source.failure_count + 1)
            )
            
            return 0
    
    async def _parse_calendar_page(
        self,
        html_content: str,
        company_id: str,
        source_url: str,
        selectors: Dict[str, str],
        company_name: str
    ) -> List[CalendarEventData]:
        """Parse calendar events from HTML content"""
        
        events = []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Use company-specific selectors or defaults
            event_selector = selectors.get('events', '.calendar-event, .event-item, .ir-event')
            date_selector = selectors.get('date', '.date, .event-date, .datum')
            title_selector = selectors.get('title', '.title, .event-title, .rubrik')
            type_selector = selectors.get('type', '.type, .event-type, .kategori')
            
            # Find event elements
            event_elements = soup.select(event_selector)
            
            if not event_elements:
                # Try alternative parsing methods for different website structures
                event_elements = self._try_alternative_parsing(soup, company_name)
            
            for element in event_elements:
                try:
                    # Extract event data
                    date_elem = element.select_one(date_selector)
                    title_elem = element.select_one(title_selector)
                    type_elem = element.select_one(type_selector)
                    
                    if not date_elem or not title_elem:
                        continue
                    
                    # Parse date
                    date_text = date_elem.get_text(strip=True)
                    event_date = self._parse_swedish_date(date_text)
                    
                    if not event_date:
                        continue
                    
                    # Only collect future events within next 12 months
                    if event_date < date.today() or event_date > date.today() + timedelta(days=365):
                        continue
                    
                    # Extract title and type
                    title = title_elem.get_text(strip=True)
                    event_type_text = type_elem.get_text(strip=True) if type_elem else ""
                    
                    # Classify event type
                    event_type = self._classify_event_type(title, event_type_text)
                    
                    # Extract additional information
                    description = self._extract_description(element)
                    location = self._extract_location(element)
                    webcast_url = self._extract_webcast_url(element)
                    
                    # Determine if this is a confirmed event
                    confirmed = self._is_confirmed_event(element, title, company_name)
                    
                    event = CalendarEventData(
                        company_id=company_id,
                        event_type=event_type,
                        event_date=event_date,
                        event_time=None,  # Could parse time if available
                        title=title,
                        description=description,
                        location=location,
                        webcast_url=webcast_url,
                        source_url=source_url,
                        confirmed=confirmed
                    )
                    
                    events.append(event)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to parse event element for {company_name}: {e}")
                    continue
            
        except Exception as e:
            self.logger.error(f"Failed to parse calendar page for {company_name}: {e}")
        
        return events
    
    def _try_alternative_parsing(self, soup: BeautifulSoup, company_name: str) -> List:
        """Try alternative parsing methods for different website structures"""
        
        # Common alternative selectors
        alternative_selectors = [
            'tr.event-row',
            '.event-list li',
            '.calendar-item',
            '.ir-calendar-event',
            'table tbody tr',
            '.timeline-item'
        ]
        
        for selector in alternative_selectors:
            elements = soup.select(selector)
            if elements:
                self.logger.debug(f"Using alternative selector '{selector}' for {company_name}")
                return elements
        
        return []
    
    def _parse_swedish_date(self, date_text: str) -> Optional[date]:
        """Parse Swedish date formats"""
        
        # Clean up date text
        date_text = re.sub(r'\s+', ' ', date_text.strip().lower())
        
        # Common Swedish date patterns
        patterns = [
            # "15 januari 2025", "3 mars 2025"
            r'(\d{1,2})\s+(januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december)\s+(\d{4})',
            # "2025-01-15"
            r'(\d{4})-(\d{2})-(\d{2})',
            # "15/1/2025", "3/12/2025"
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            # "15.1.2025"
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, date_text)
            if match:
                try:
                    groups = match.groups()
                    
                    if any(month in date_text for month in self.swedish_months.keys()):
                        # Swedish month names
                        day, month_name, year = groups
                        month = self.swedish_months[month_name]
                        return date(int(year), month, int(day))
                    else:
                        # Numeric formats
                        if len(groups[0]) == 4:  # YYYY-MM-DD
                            year, month, day = groups
                        else:  # DD/MM/YYYY or DD.MM.YYYY
                            day, month, year = groups
                        
                        return date(int(year), int(month), int(day))
                        
                except (ValueError, KeyError) as e:
                    self.logger.debug(f"Date parsing error: {e}")
                    continue
        
        return None
    
    def _classify_event_type(self, title: str, type_text: str) -> str:
        """Classify the type of financial event"""
        
        text = f"{title} {type_text}".lower()
        
        # Financial report patterns
        if any(pattern in text for pattern in ['q1', 'första kvartalet', 'first quarter']):
            return 'Q1_report'
        elif any(pattern in text for pattern in ['q2', 'andra kvartalet', 'second quarter']):
            return 'Q2_report'
        elif any(pattern in text for pattern in ['q3', 'tredje kvartalet', 'third quarter']):
            return 'Q3_report'
        elif any(pattern in text for pattern in ['q4', 'fjärde kvartalet', 'fourth quarter']):
            return 'Q3_report'  # We treat Q4 as Q3 for consistency
        elif any(pattern in text for pattern in ['delårsrapport', 'interim', 'kvartalsrapport']):
            return 'Q2_report'  # Default quarterly to Q2
        elif any(pattern in text for pattern in ['årsredovisning', 'annual', 'årsbokslut', 'helårsrapport']):
            return 'annual_report'
        elif any(pattern in text for pattern in ['årsstämma', 'agm', 'annual general meeting']):
            return 'agm'
        elif any(pattern in text for pattern in ['resultatkonferens', 'earnings', 'telefonkonferens', 'webcast']):
            return 'earnings_call'
        else:
            return 'other'
    
    def _extract_description(self, element) -> Optional[str]:
        """Extract event description"""
        desc_selectors = ['.description', '.event-description', '.details', '.info']
        
        for selector in desc_selectors:
            desc_elem = element.select_one(selector)
            if desc_elem:
                return desc_elem.get_text(strip=True)
        
        return None
    
    def _extract_location(self, element) -> Optional[str]:
        """Extract event location"""
        location_selectors = ['.location', '.venue', '.address', '.plats']
        
        for selector in location_selectors:
            loc_elem = element.select_one(selector)
            if loc_elem:
                return loc_elem.get_text(strip=True)
        
        return None
    
    def _extract_webcast_url(self, element) -> Optional[str]:
        """Extract webcast URL"""
        webcast_selectors = ['a[href*="webcast"]', 'a[href*="stream"]', '.webcast-link']
        
        for selector in webcast_selectors:
            link_elem = element.select_one(selector)
            if link_elem:
                return link_elem.get('href')
        
        return None
    
    def _is_confirmed_event(self, element, title: str, company_name: str) -> bool:
        """Determine if event is confirmed"""
        
        # Events are considered confirmed if they have specific markers
        confirmed_indicators = [
            'bekräftat', 'confirmed', 'fastställt', 'preliminär',
            'preliminary', 'estimated'
        ]
        
        element_text = element.get_text().lower()
        title_lower = title.lower()
        
        # Check for confirmed indicators
        for indicator in confirmed_indicators:
            if indicator in element_text or indicator in title_lower:
                return 'preliminär' not in element_text and 'preliminary' not in element_text
        
        # Default to confirmed for major companies
        major_companies = ['volvo', 'h&m', 'ericsson', 'atlas copco', 'sandvik']
        if any(company.lower() in company_name.lower() for company in major_companies):
            return True
        
        return False
    
    async def _store_calendar_events(
        self, 
        events: List[CalendarEventData], 
        db: AsyncSession
    ) -> int:
        """Store calendar events in database"""
        
        stored_count = 0
        
        for event_data in events:
            try:
                # Check if event already exists
                existing_query = select(NordicCalendarEvent).where(
                    NordicCalendarEvent.company_id == event_data.company_id,
                    NordicCalendarEvent.event_date == event_data.event_date,
                    NordicCalendarEvent.title == event_data.title
                )
                
                result = await db.execute(existing_query)
                existing_event = result.scalar_one_or_none()
                
                if existing_event:
                    # Update existing event if new data is more complete
                    if event_data.confirmed and not existing_event.confirmed:
                        existing_event.confirmed = True
                        existing_event.updated_date = datetime.utcnow()
                        stored_count += 1
                else:
                    # Create new event
                    new_event = NordicCalendarEvent(
                        company_id=event_data.company_id,
                        event_type=event_data.event_type,
                        event_date=event_data.event_date,
                        event_time=None,  # Could add time parsing
                        title=event_data.title,
                        description=event_data.description,
                        location=event_data.location,
                        webcast_url=event_data.webcast_url,
                        source_url=event_data.source_url,
                        confirmed=event_data.confirmed
                    )
                    
                    db.add(new_event)
                    stored_count += 1
                    
            except Exception as e:
                self.logger.error(f"Failed to store calendar event: {e}")
                continue
        
        return stored_count


# Convenience function for external use
async def collect_swedish_financial_calendars() -> Dict[str, int]:
    """
    Collect all Swedish financial calendars
    Returns: {company_id: events_found}
    """
    async with SwedishCalendarCollector() as collector:
        return await collector.collect_all_swedish_calendars()