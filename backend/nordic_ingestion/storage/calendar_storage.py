"""
Calendar Event Storage for Nordic Ingestion
Handles split storage: document metadata + dedicated calendar events
"""
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from decimal import Decimal

from shared.database import AsyncSessionLocal
from ..models import NordicCalendarEvent, NordicCompany
from ..collectors.aggregator.mfn_collector import MFNNewsItem


class CalendarEventStorage:
    """
    Manages calendar events extracted from financial news
    Implements split storage strategy:
    - Document metadata: report-specific calendar info
    - Calendar table: company-wide events (earnings dates, dividends, AGM)
    """
    
    async def store_calendar_events_from_mfn(
        self, 
        mfn_items: List[MFNNewsItem]
    ) -> Dict[str, int]:
        """
        Extract and store calendar events from MFN news items
        
        Args:
            mfn_items: List of MFN news items with calendar info
            
        Returns:
            {"calendar_events_created": count, "dividend_events": count, "errors": count}
        """
        stats = {"calendar_events_created": 0, "dividend_events": 0, "errors": 0}
        
        for item in mfn_items:
            # Process each item in its own transaction to avoid cascading failures
            async with AsyncSessionLocal() as db:
                try:
                    if not item.calendar_info:
                        continue
                    
                    # Find company
                    company = await self._find_company_by_name(db, item.company_name)
                    if not company:
                        print(f"⚠️  Company not found for calendar: {item.company_name}")
                        stats["errors"] += 1
                        continue
                    
                    # Extract calendar events from the calendar_info
                    events = await self._extract_calendar_events(
                        item.calendar_info, 
                        company.id, 
                        item.source_url,
                        item.title
                    )
                    
                    events_added_in_this_transaction = 0
                    
                    for event in events:
                        try:
                            # Check for duplicates
                            existing = await self._check_existing_event(
                                db, company.id, event["event_type"], event["event_date"]
                            )
                            
                            if existing:
                                continue  # Skip duplicates
                            
                            # Create calendar event
                            calendar_event = NordicCalendarEvent(
                                company_id=company.id,
                                event_type=event["event_type"],
                                event_date=event["event_date"],
                                event_time=event.get("event_time"),
                                title=event["title"],
                                description=event.get("description"),
                                webcast_url=event.get("webcast_url"),
                                source_url=item.source_url,
                                confirmed=False,  # MFN is secondary source
                                
                                # Dividend-specific fields
                                dividend_amount=event.get("dividend_amount"),
                                dividend_currency=event.get("dividend_currency"),
                                dividend_type=event.get("dividend_type"),
                                ex_dividend_date=event.get("ex_dividend_date"),
                                record_date=event.get("record_date"),
                                payment_date=event.get("payment_date"),
                                
                                metadata_=event.get("metadata", {})
                            )
                            
                            db.add(calendar_event)
                            events_added_in_this_transaction += 1
                            
                            if event["event_type"] == "dividend":
                                stats["dividend_events"] += 1
                                
                        except Exception as e:
                            print(f"⚠️  Error creating calendar event: {e}")
                            stats["errors"] += 1
                            continue
                    
                    # Commit this item's events
                    if events_added_in_this_transaction > 0:
                        await db.commit()
                        stats["calendar_events_created"] += events_added_in_this_transaction
                        
                except Exception as e:
                    print(f"❌ Error storing calendar events for {item.company_name}: {e}")
                    stats["errors"] += 1
                    await db.rollback()
        
        return stats
    
    async def _extract_calendar_events(
        self, 
        calendar_info: Dict[str, Any], 
        company_id: str,
        source_url: str,
        item_title: str
    ) -> List[Dict[str, Any]]:
        """
        Extract calendar events from parsed calendar information
        
        Returns:
            List of calendar event dictionaries
        """
        events = []
        
        try:
            # Extract dividend events
            if 'dividend' in calendar_info:
                dividend_info = calendar_info['dividend']
                events.extend(await self._create_dividend_events(
                    dividend_info, company_id, source_url, item_title
                ))
            
            # Extract earnings events
            if 'earnings' in calendar_info:
                earnings_info = calendar_info['earnings']
                events.extend(await self._create_earnings_events(
                    earnings_info, company_id, source_url, item_title
                ))
            
            # Extract webcast events
            if 'webcast' in calendar_info:
                webcast_info = calendar_info['webcast']
                events.extend(await self._create_webcast_events(
                    webcast_info, company_id, source_url, item_title
                ))
            
        except Exception as e:
            print(f"⚠️  Error extracting calendar events: {e}")
        
        return events
    
    async def _create_dividend_events(
        self, 
        dividend_info: Dict[str, Any], 
        company_id: str,
        source_url: str,
        item_title: str
    ) -> List[Dict[str, Any]]:
        """Create dividend calendar events from dividend info - FIXED to not make up fake dates"""
        events = []
        
        # Get parsed dividend amounts
        parsed_amounts = dividend_info.get('parsed_amounts', [])
        dividend_type = dividend_info.get('dividend_type', 'regular')
        x_dag_dates = dividend_info.get('x_dag_dates', {})
        
        print(f"📊 Dividend parsing: Found {len(parsed_amounts)} amounts, X-dag dates: {bool(x_dag_dates)}")
        
        # CRITICAL FIX: Only create dividend events if we have REAL dates from X-dag parsing
        if not x_dag_dates or 'ex_dividend_dates' not in x_dag_dates:
            print(f"⚠️  No ex-dividend dates found - skipping dividend events (avoiding fake dates)")
            return []
        
        # Only create events if we can pair dividends with actual dates
        ex_dates = x_dag_dates.get('ex_dividend_dates', [])
        if not ex_dates:
            print(f"⚠️  No valid ex-dividend dates - skipping dividend events")
            return []
        
        print(f"✅ Found {len(ex_dates)} real ex-dividend dates: {ex_dates[:3]}")
        
        # Take only recent, significant dividend amounts (avoid historical spam)
        significant_dividends = []
        for div in parsed_amounts:
            if div['amount'] > 5.0 and div['currency'] == 'SEK':  # Only recent, substantial SEK dividends
                significant_dividends.append(div)
        
        if not significant_dividends:
            print(f"⚠️  No significant recent dividends found")
            return []
        
        # Take the top dividend and pair it with the first ex-dividend date
        top_dividend = max(significant_dividends, key=lambda x: x['amount'])
        first_ex_date_str = ex_dates[0]
        
        try:
            ex_date = self._parse_date(first_ex_date_str)
            if not ex_date:
                print(f"⚠️  Could not parse ex-dividend date: {first_ex_date_str}")
                return []
            
            # Only create event if the date makes sense (not way in the future)
            from datetime import timedelta
            if ex_date > date.today() + timedelta(days=365):
                print(f"⚠️  Ex-dividend date too far in future: {ex_date}")
                return []
            
            event = {
                "event_type": "dividend",
                "event_date": ex_date,  # Use REAL parsed date
                "title": f"Ex-Dividend {top_dividend['amount']:.2f} {top_dividend['currency']}",
                "description": f"Ex-dividend date from {item_title}",
                "dividend_amount": Decimal(str(top_dividend['amount'])),
                "dividend_currency": top_dividend['currency'],
                "dividend_type": dividend_type,
                "ex_dividend_date": ex_date,
                "metadata": {
                    "source": "mfn_x_dag_parsing",
                    "raw_dividend_text": top_dividend['raw_text'],
                    "raw_ex_date": first_ex_date_str,
                    "item_title": item_title
                }
            }
            
            # Add record and payment dates if available
            if 'record_dates' in x_dag_dates:
                try:
                    record_date = self._parse_date(x_dag_dates['record_dates'][0])
                    if record_date:
                        event['record_date'] = record_date
                except:
                    pass
            
            if 'payment_dates' in x_dag_dates:
                try:
                    payment_date = self._parse_date(x_dag_dates['payment_dates'][0])
                    if payment_date:
                        event['payment_date'] = payment_date
                except:
                    pass
            
            events.append(event)
            print(f"✅ Created dividend event: {ex_date} - {top_dividend['amount']} {top_dividend['currency']}")
            
        except Exception as e:
            print(f"❌ Error creating dividend event: {e}")
        
        return events
    
    async def _create_earnings_events(
        self, 
        earnings_info: Dict[str, Any], 
        company_id: str,
        source_url: str,
        item_title: str
    ) -> List[Dict[str, Any]]:
        """Create earnings calendar events"""
        events = []
        
        quarters = earnings_info.get('quarters_mentioned', [])
        years = earnings_info.get('years_mentioned', [])
        
        # Only create events for recent/future years (avoid creating 100+ historical events)
        current_year = date.today().year
        relevant_years = [y for y in years if int(y) >= current_year - 1]  # This year and last year only
        
        # Limit to max 2 events to avoid spam
        created_events = 0
        for quarter in quarters[:2]:  # Only first 2 quarters
            for year in relevant_years[:2]:  # Only first 2 years
                if created_events >= 2:  # Max 2 earnings events
                    break
                    
                try:
                    # Try to estimate a reasonable date for this quarter/year
                    estimated_date = self._estimate_earnings_date(quarter, int(year))
                    
                    event = {
                        "event_type": "earnings",
                        "event_date": estimated_date,
                        "title": f"{quarter} {year} Earnings Report",
                        "description": f"Earnings report mentioned in {item_title}",
                        "report_period": f"{quarter}_{year}",
                        "metadata": {
                            "source": "mfn_extraction",
                            "quarter": quarter,
                            "year": year,
                            "item_title": item_title,
                            "estimated_date": True
                        }
                    }
                    events.append(event)
                    created_events += 1
                    
                except Exception as e:
                    print(f"⚠️  Error creating earnings event: {e}")
            
            if created_events >= 2:
                break
        
        return events
    
    def _estimate_earnings_date(self, quarter: str, year: int) -> date:
        """Estimate earnings date based on quarter and year"""
        try:
            # Standard earnings release months by quarter
            quarter_months = {
                'Q1': 4,   # Q1 results usually released in April
                'Q2': 7,   # Q2 results usually released in July  
                'Q3': 10,  # Q3 results usually released in October
                'Q4': 2,   # Q4 results usually released in February (next year)
            }
            
            month = quarter_months.get(quarter.upper(), date.today().month)
            
            # For Q4, results are released in February of the next year
            if quarter.upper() == 'Q4':
                year += 1
            
            # Use middle of the month as estimate
            return date(year, month, 15)
            
        except:
            # Fallback to today if estimation fails
            return date.today()
    
    async def _create_webcast_events(
        self, 
        webcast_info: Dict[str, Any], 
        company_id: str,
        source_url: str,
        item_title: str
    ) -> List[Dict[str, Any]]:
        """Create webcast calendar events"""
        events = []
        
        webcast_urls = webcast_info.get('urls', [])
        registration_required = webcast_info.get('registration_required', False)
        
        if webcast_urls:
            try:
                event = {
                    "event_type": "webcast",
                    "event_date": date.today(),  # Default
                    "title": f"Webcast/Conference Call",
                    "description": f"Webcast mentioned in {item_title}",
                    "webcast_url": webcast_urls[0],  # Use first URL
                    "metadata": {
                        "source": "mfn_extraction",
                        "all_urls": webcast_urls,
                        "registration_required": registration_required,
                        "item_title": item_title
                    }
                }
                events.append(event)
                
            except Exception as e:
                print(f"⚠️  Error creating webcast event: {e}")
        
        return events
    
    def _parse_date(self, date_string: str) -> Optional[date]:
        """Parse various date formats to date object"""
        try:
            # Try various date formats
            formats = [
                '%d/%m/%Y', '%d-%m-%Y',
                '%Y/%m/%d', '%Y-%m-%d',
                '%d %B %Y', '%d %b %Y',
                '%d %m %Y'
            ]
            
            date_string = date_string.strip()
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_string, fmt).date()
                except:
                    continue
            
            # Try Swedish months
            swedish_months = {
                'januari': 'January', 'februari': 'February', 'mars': 'March',
                'april': 'April', 'maj': 'May', 'juni': 'June',
                'juli': 'July', 'augusti': 'August', 'september': 'September',
                'oktober': 'October', 'november': 'November', 'december': 'December'
            }
            
            date_lower = date_string.lower()
            for swedish, english in swedish_months.items():
                if swedish in date_lower:
                    english_date = date_lower.replace(swedish, english.lower())
                    try:
                        return datetime.strptime(english_date, '%d %B %Y').date()
                    except:
                        continue
            
        except Exception as e:
            print(f"⚠️  Could not parse date '{date_string}': {e}")
        
        return None
    
    async def _find_company_by_name(
        self, 
        db: AsyncSession, 
        company_name: str
    ) -> Optional[NordicCompany]:
        """Find company by name (using same logic as document catalog)"""
        # Direct name match first
        result = await db.execute(
            select(NordicCompany).where(NordicCompany.name == company_name)
        )
        company = result.scalar_one_or_none()
        if company:
            return company
            
        # Fuzzy matching for common variations (MFN slug to database name)
        name_variations = {
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
        
        # Check if company_name is a slug we know about
        company_name_lower = company_name.lower()
        if company_name_lower in name_variations:
            target_name = name_variations[company_name_lower]
            result = await db.execute(
                select(NordicCompany).where(NordicCompany.name == target_name)
            )
            return result.scalar_one_or_none()
        
        return None
    
    async def _check_existing_event(
        self, 
        db: AsyncSession, 
        company_id: str, 
        event_type: str, 
        event_date: date
    ) -> bool:
        """Check if similar calendar event already exists"""
        result = await db.execute(
            select(NordicCalendarEvent).where(
                and_(
                    NordicCalendarEvent.company_id == company_id,
                    NordicCalendarEvent.event_type == event_type,
                    NordicCalendarEvent.event_date == event_date
                )
            )
        )
        return result.scalar_one_or_none() is not None


# Convenience functions
async def store_mfn_calendar_events(mfn_items: List[MFNNewsItem]) -> Dict[str, int]:
    """Store calendar events extracted from MFN items"""
    storage = CalendarEventStorage()
    return await storage.store_calendar_events_from_mfn(mfn_items)


async def get_upcoming_calendar_events(
    company_id: Optional[str] = None,
    event_type: Optional[str] = None,
    days_ahead: int = 30
) -> List[NordicCalendarEvent]:
    """Get upcoming calendar events"""
    async with AsyncSessionLocal() as db:
        query = select(NordicCalendarEvent).where(
            NordicCalendarEvent.event_date >= date.today()
        ).where(
            NordicCalendarEvent.event_date <= date.today().replace(day=date.today().day + days_ahead)
        )
        
        if company_id:
            query = query.where(NordicCalendarEvent.company_id == company_id)
        
        if event_type:
            query = query.where(NordicCalendarEvent.event_type == event_type)
        
        query = query.order_by(NordicCalendarEvent.event_date)
        
        result = await db.execute(query)
        return result.scalars().all()