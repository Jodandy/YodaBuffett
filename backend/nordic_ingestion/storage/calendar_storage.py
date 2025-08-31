"""
Calendar Event Storage for Nordic Ingestion
Handles split storage: document metadata + dedicated calendar events
"""
from datetime import datetime, date, time
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
                    
                    # BATCH OPTIMIZATION: Check all events for duplicates at once
                    print(f"🚀 Batch checking {len(events)} calendar events for duplicates...")
                    existing_events = await self._batch_check_existing_events(db, company.id, events)
                    print(f"📊 Found {len(existing_events)} existing events")
                    
                    for event in events:
                        try:
                            # OPTIMIZED: Use pre-loaded batch results instead of individual queries
                            event_key = (event["event_type"], event["event_date"], event.get("title"))
                            if event_key in existing_events:
                                print(f"  🔄 Skipping duplicate event: {event.get('title', 'Untitled')}")
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
            # Check if this is the new MFN calendar table format
            if calendar_info.get('source') == 'mfn_calendar_table':
                # Handle new structured calendar table data directly
                events.extend(await self._create_events_from_calendar_table(
                    calendar_info, company_id, source_url, item_title
                ))
            else:
                # Handle old format for backward compatibility
                # Extract dividend events
                if 'dividend' in calendar_info:
                    dividend_info = calendar_info['dividend']
                    events.extend(await self._create_dividend_events(
                        dividend_info, company_id, source_url, item_title
                    ))
                
                # Extract earnings events
                if 'earnings' in calendar_info:
                    earnings_info = calendar_info['earnings']
                    # Pass through any upcoming dates found
                    if 'upcoming_dates' in calendar_info:
                        earnings_info['upcoming_dates'] = calendar_info['upcoming_dates']
                    events.extend(await self._create_earnings_events(
                        earnings_info, company_id, source_url, item_title
                    ))
                
                # Extract webcast events
                if 'webcast' in calendar_info:
                    webcast_info = calendar_info['webcast']
                    # Pass through any upcoming dates found
                    if 'upcoming_dates' in calendar_info:
                        webcast_info['upcoming_dates'] = calendar_info['upcoming_dates']
                    events.extend(await self._create_webcast_events(
                        webcast_info, company_id, source_url, item_title
                    ))
            
        except Exception as e:
            print(f"⚠️  Error extracting calendar events: {e}")
        
        return events
    
    async def _create_events_from_calendar_table(
        self, 
        calendar_info: Dict[str, Any], 
        company_id: str,
        source_url: str,
        item_title: str
    ) -> List[Dict[str, Any]]:
        """
        Create calendar events directly from structured calendar table data
        
        Args:
            calendar_info: Parsed calendar info from MFN calendar table
            
        Returns:
            List of calendar event dictionaries
        """
        events = []
        
        try:
            # Handle dividend events directly from calendar table
            if 'dividend' in calendar_info and 'events' in calendar_info['dividend']:
                for dividend_event in calendar_info['dividend']['events']:
                    try:
                        # Convert the structured dividend event to database format
                        event = {
                            "event_type": "dividend",
                            "event_date": dividend_event['date'],
                            "title": f"Ex-Dividend {dividend_event['amount']} {dividend_event['currency']}",
                            "description": f"Dividend event: {dividend_event['event']}",
                            "dividend_amount": Decimal(str(dividend_event['amount'])) if dividend_event['amount'] else None,
                            "dividend_currency": dividend_event['currency'],
                            "dividend_type": dividend_event['type'],
                            "ex_dividend_date": dividend_event['date'],
                            "metadata": {
                                "source": "mfn_calendar_table",
                                "raw_event_text": dividend_event['event'],
                                "ticker": dividend_event.get('ticker'),
                                "share_class": dividend_event.get('share_class'),
                                "item_title": item_title
                            }
                        }
                        
                        # Add event time if available and valid (not just '-' or empty)
                        if dividend_event.get('time') and dividend_event['time'].strip() != '-' and dividend_event['time'].strip():
                            # Parse time string to time object if needed
                            try:
                                time_str = dividend_event['time'].strip()
                                # Handle common time formats like "14:00", "14:00:00"
                                if ':' in time_str:
                                    time_parts = time_str.split(':')
                                    if len(time_parts) >= 2:
                                        hour = int(time_parts[0])
                                        minute = int(time_parts[1])
                                        second = int(time_parts[2]) if len(time_parts) > 2 else 0
                                        event['event_time'] = time(hour, minute, second)
                            except Exception as e:
                                print(f"⚠️  Could not parse time '{dividend_event['time']}': {e}")
                                # Don't set event_time if parsing fails
                        
                        events.append(event)
                        
                    except Exception as e:
                        print(f"⚠️  Error creating dividend event from calendar table: {e}")
                        continue
            
            # Handle earnings events directly from calendar table
            if 'earnings' in calendar_info and 'events' in calendar_info['earnings']:
                for earnings_event in calendar_info['earnings']['events']:
                    try:
                        event_title = earnings_event['event']
                        if earnings_event['type'] == 'quarterly_report':
                            event_title = f"Q{earnings_event.get('quarter', '?')} {earnings_event.get('year', '?')} Report"
                        elif earnings_event['type'] == 'annual_report':
                            event_title = f"{earnings_event.get('year', '?')} Annual Report"
                        
                        event = {
                            "event_type": "earnings",
                            "event_date": earnings_event['date'],
                            "title": event_title,
                            "description": f"Earnings event: {earnings_event['event']}",
                            "report_period": f"Q{earnings_event.get('quarter', '?')}_{earnings_event.get('year', '?')}" if earnings_event.get('quarter') else None,
                            "metadata": {
                                "source": "mfn_calendar_table",
                                "raw_event_text": earnings_event['event'],
                                "event_type": earnings_event['type'],
                                "quarter": earnings_event.get('quarter'),
                                "year": earnings_event.get('year'),
                                "item_title": item_title
                            }
                        }
                        
                        # Add event time if available and valid (not just '-' or empty)
                        if earnings_event.get('time') and earnings_event['time'].strip() != '-' and earnings_event['time'].strip():
                            # Parse time string to time object if needed
                            try:
                                time_str = earnings_event['time'].strip()
                                # Handle common time formats like "14:00", "14:00:00"
                                if ':' in time_str:
                                    time_parts = time_str.split(':')
                                    if len(time_parts) >= 2:
                                        hour = int(time_parts[0])
                                        minute = int(time_parts[1])
                                        second = int(time_parts[2]) if len(time_parts) > 2 else 0
                                        event['event_time'] = time(hour, minute, second)
                            except Exception as e:
                                print(f"⚠️  Could not parse time '{earnings_event['time']}': {e}")
                                # Don't set event_time if parsing fails
                        
                        events.append(event)
                        
                    except Exception as e:
                        print(f"⚠️  Error creating earnings event from calendar table: {e}")
                        continue
            
            # Handle other calendar events
            if 'other_events' in calendar_info and 'events' in calendar_info['other_events']:
                for calendar_event in calendar_info['other_events']['events']:
                    try:
                        event = {
                            "event_type": calendar_event['type'],
                            "event_date": calendar_event['date'],
                            "title": calendar_event['event'],
                            "description": f"Calendar event: {calendar_event['event']}",
                            "metadata": {
                                "source": "mfn_calendar_table",
                                "raw_event_text": calendar_event['event'],
                                "event_type": calendar_event['type'],
                                "item_title": item_title
                            }
                        }
                        
                        # Add event time if available and valid (not just '-' or empty)
                        if calendar_event.get('time') and calendar_event['time'].strip() != '-' and calendar_event['time'].strip():
                            # Parse time string to time object if needed
                            try:
                                time_str = calendar_event['time'].strip()
                                # Handle common time formats like "14:00", "14:00:00"
                                if ':' in time_str:
                                    time_parts = time_str.split(':')
                                    if len(time_parts) >= 2:
                                        hour = int(time_parts[0])
                                        minute = int(time_parts[1])
                                        second = int(time_parts[2]) if len(time_parts) > 2 else 0
                                        event['event_time'] = time(hour, minute, second)
                            except Exception as e:
                                print(f"⚠️  Could not parse time '{calendar_event['time']}': {e}")
                                # Don't set event_time if parsing fails
                        
                        events.append(event)
                        
                    except Exception as e:
                        print(f"⚠️  Error creating calendar event from calendar table: {e}")
                        continue
            
            print(f"✅ Created {len(events)} events from calendar table data")
            
        except Exception as e:
            print(f"❌ Error processing calendar table data: {e}")
        
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
        """Create earnings calendar events - FIXED to not create fake future dates"""
        events = []
        
        # Extract actual dates from the calendar info first
        upcoming_dates = earnings_info.get('upcoming_dates', [])
        
        # If we have REAL dates from the calendar, use those
        if upcoming_dates:
            for date_str in upcoming_dates[:2]:  # Max 2 to avoid spam
                parsed_date = self._parse_date(date_str)
                if parsed_date:
                    # Only create events for reasonable dates (within 1 year)
                    days_ahead = (parsed_date - date.today()).days
                    if -30 <= days_ahead <= 365:  # 30 days past to 1 year future
                        event = {
                            "event_type": "earnings",
                            "event_date": parsed_date,
                            "title": f"Earnings Report - {date_str}",
                            "description": f"Earnings date found in {item_title}",
                            "metadata": {
                                "source": "mfn_calendar_extraction",
                                "raw_date": date_str,
                                "item_title": item_title,
                                "estimated_date": False
                            }
                        }
                        events.append(event)
            return events
        
        # Only estimate dates if we're dealing with CURRENT or NEXT quarter
        quarters = earnings_info.get('quarters_mentioned', [])
        years = earnings_info.get('years_mentioned', [])
        
        if not quarters or not years:
            return []
        
        current_year = date.today().year
        current_month = date.today().month
        current_quarter = (current_month - 1) // 3 + 1  # 1-4
        
        # Only process current year or next year
        valid_years = [str(y) for y in [current_year, current_year + 1] if str(y) in years]
        
        if not valid_years:
            print(f"⚠️  No current/next year found in earnings info, skipping")
            return []
        
        # Only create ONE event for the most relevant quarter
        for year in valid_years[:1]:  # Just first valid year
            for quarter in quarters[:1]:  # Just first quarter
                quarter_num = self._parse_quarter_number(quarter)
                if not quarter_num:
                    continue
                
                # Only create event if it's current or next quarter
                if int(year) == current_year and quarter_num < current_quarter - 1:
                    continue  # Skip old quarters
                
                try:
                    estimated_date = self._estimate_earnings_date_conservative(quarter_num, int(year))
                    
                    # Final sanity check - must be within reasonable range
                    days_ahead = (estimated_date - date.today()).days
                    if days_ahead < -30 or days_ahead > 180:  # 30 days past to 6 months future
                        print(f"⚠️  Estimated date {estimated_date} out of range, skipping")
                        continue
                    
                    event = {
                        "event_type": "earnings",
                        "event_date": estimated_date,
                        "title": f"Q{quarter_num} {year} Earnings Report (Estimated)",
                        "description": f"Earnings period mentioned in {item_title}",
                        "report_period": f"Q{quarter_num}_{year}",
                        "metadata": {
                            "source": "mfn_extraction",
                            "quarter": f"Q{quarter_num}",
                            "year": year,
                            "item_title": item_title,
                            "estimated_date": True,
                            "estimation_note": "Date estimated based on typical reporting schedule"
                        }
                    }
                    events.append(event)
                    return events  # Only ONE event
                    
                except Exception as e:
                    print(f"⚠️  Error creating earnings event: {e}")
        
        return events
    
    def _parse_quarter_number(self, quarter_str: str) -> Optional[int]:
        """Parse quarter string to number (1-4)"""
        quarter_str = quarter_str.upper()
        if 'Q1' in quarter_str or '1Q' in quarter_str:
            return 1
        elif 'Q2' in quarter_str or '2Q' in quarter_str:
            return 2
        elif 'Q3' in quarter_str or '3Q' in quarter_str:
            return 3
        elif 'Q4' in quarter_str or '4Q' in quarter_str:
            return 4
        elif 'KVARTAL 1' in quarter_str:
            return 1
        elif 'KVARTAL 2' in quarter_str:
            return 2
        elif 'KVARTAL 3' in quarter_str:
            return 3
        elif 'KVARTAL 4' in quarter_str:
            return 4
        return None
    
    def _estimate_earnings_date_conservative(self, quarter: int, year: int) -> date:
        """Conservative earnings date estimation - only for near-term dates"""
        try:
            # Typical Swedish earnings release schedule
            # Q1: Late April/Early May
            # Q2: Late July
            # Q3: Late October
            # Q4: Late January/Early February (next year)
            
            today = date.today()
            
            if quarter == 1:
                # Q1 reports typically in late April
                estimated = date(year, 4, 25)
            elif quarter == 2:
                # Q2 reports typically in late July
                estimated = date(year, 7, 20)
            elif quarter == 3:
                # Q3 reports typically in late October
                estimated = date(year, 10, 25)
            elif quarter == 4:
                # Q4 reports typically in late January/early February of NEXT year
                estimated = date(year + 1, 1, 25)
            else:
                return today
            
            # If the estimated date is more than 6 months away, it's too speculative
            days_diff = (estimated - today).days
            if days_diff > 180:
                print(f"⚠️  Estimated date {estimated} too far in future")
                return today
            
            return estimated
            
        except:
            return date.today()
    
    async def _create_webcast_events(
        self, 
        webcast_info: Dict[str, Any], 
        company_id: str,
        source_url: str,
        item_title: str
    ) -> List[Dict[str, Any]]:
        """Create webcast calendar events - FIXED to not create events without dates"""
        events = []
        
        # Don't create webcast events unless we have an actual date
        # Webcasts are usually tied to earnings releases, so they should come with dates
        
        webcast_urls = webcast_info.get('urls', [])
        if not webcast_urls:
            return []
        
        # Check if we have a date in the webcast info or nearby context
        upcoming_dates = webcast_info.get('upcoming_dates', [])
        if not upcoming_dates:
            print(f"⚠️  Webcast found but no date available - skipping event creation")
            return []
        
        # Use the first valid date found
        for date_str in upcoming_dates[:1]:  # Just first date
            parsed_date = self._parse_date(date_str)
            if parsed_date:
                # Sanity check - must be reasonable
                days_ahead = (parsed_date - date.today()).days
                if -7 <= days_ahead <= 90:  # 1 week past to 3 months future
                    try:
                        event = {
                            "event_type": "webcast",
                            "event_date": parsed_date,
                            "title": f"Earnings Webcast/Conference Call",
                            "description": f"Webcast on {date_str} from {item_title}",
                            "webcast_url": webcast_urls[0],  # Use first URL
                            "metadata": {
                                "source": "mfn_extraction",
                                "all_urls": webcast_urls,
                                "registration_required": webcast_info.get('registration_required', False),
                                "item_title": item_title,
                                "raw_date": date_str
                            }
                        }
                        events.append(event)
                        break
                        
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
        # Direct name match first (case-insensitive)
        from sqlalchemy import func
        result = await db.execute(
            select(NordicCompany).where(func.lower(NordicCompany.name) == func.lower(company_name))
            .order_by(NordicCompany.created_at)  # Pick the first created
            .limit(1)  # Handle multi-listed companies
        )
        company = result.scalar_one_or_none()
        if company:
            return company
            
        # SYSTEMATIC FIX: Try without ticker suffix if company_name has (TICKER) pattern
        import re
        ticker_pattern = r'^(.+?)\s*\([A-Z0-9\s]+\)$'
        match = re.match(ticker_pattern, company_name)
        if match:
            base_name = match.group(1).strip()
            result = await db.execute(
                select(NordicCompany).where(func.lower(NordicCompany.name) == func.lower(base_name))
                .order_by(NordicCompany.created_at).limit(1)
            )
            company = result.scalar_one_or_none()
            if company:
                return company
            
        # ⭐ SYSTEMATIC FIX: Use centralized company mappings
        from ..common.company_mappings import get_company_name, COMPANY_SLUG_TO_NAME
        
        # First try centralized mapping by treating company_name as potential slug
        potential_slug = company_name.lower().replace(" ", "-").replace("(", "").replace(")", "").replace("&", "").strip("-")
        mapped_name = get_company_name(potential_slug)
        if mapped_name:
            # Try finding by mapped name
            result = await db.execute(
                select(NordicCompany).where(func.lower(NordicCompany.name) == func.lower(mapped_name))
                .order_by(NordicCompany.created_at).limit(1)
            )
            company = result.scalar_one_or_none()
            if company:
                return company
        
        # Fallback: Check if company_name matches any known MFN company names in our mapping
        for slug, name in COMPANY_SLUG_TO_NAME.items():
            if company_name.lower() in name.lower() or name.lower() in company_name.lower():
                result = await db.execute(
                    select(NordicCompany).where(func.lower(NordicCompany.name).contains(name.lower()))
                    .order_by(NordicCompany.created_at).limit(1)
                )
                company = result.scalar_one_or_none()
                if company:
                    return company
        
        # Legacy fuzzy matching for backward compatibility
        name_variations = {
            # Major companies (keeping for now)
            "volvo": "Volvo Group",
            "astrazeneca": "AstraZeneca", 
            "atlas-copco": "Atlas Copco AB",
            "ericsson": "Telefonaktiebolaget LM Ericsson",
            "handm": "H&M Hennes & Mauritz AB",
            "sandvik": "Sandvik AB",
            "nordea": "Nordea Bank Abp",
            "investor": "Investor AB",
            "abb": "ABB Ltd",
            "hexagon": "Hexagon AB", 
            "aak": "AAK",
            "abas-protect": "ABAS Protect",
            "abera-bioscience": "Abera Bioscience", 
            "active-biotech": "Active Biotech",
            "africa-energy": "Africa Energy",
            "ages-industri": "Ages Industri",
            "aik-fotboll": "AIK Fotboll",
            "aino-health": "Aino Health",
            "alfa-laval": "Alfa Laval AB",
            "alligator-bioscience": "Alligator Bioscience",
            "alm-equity": "ALM Equity",
            "alzecure-pharma": "AlzeCure Pharma",
            "amhult-2": "Amhult 2",
            "addlife": "AddLife",
            "addnode": "Addnode", 
            "addtech": "Addtech"
        }
        
        # Check if company_name is a slug we know about
        company_name_lower = company_name.lower()
        if company_name_lower in name_variations:
            target_name = name_variations[company_name_lower]
            result = await db.execute(
                select(NordicCompany).where(func.lower(NordicCompany.name) == func.lower(target_name))
            )
            return result.scalar_one_or_none()
        
        return None
    
    async def _batch_check_existing_events(
        self, 
        db: AsyncSession, 
        company_id: str, 
        events: List[Dict[str, Any]]
    ) -> set:
        """Batch check which calendar events already exist for this company - MUCH faster than individual queries"""
        
        if not events:
            return set()
        
        try:
            print(f"🚀 Executing batch calendar query for {len(events)} events...")
            
            # Get all existing events for this company in one query
            result = await db.execute(
                select(NordicCalendarEvent.event_type, 
                       NordicCalendarEvent.event_date, 
                       NordicCalendarEvent.title).where(
                    NordicCalendarEvent.company_id == company_id
                )
            )
            
            # Build set of existing event tuples for fast lookup
            existing_events = set()
            for row in result.fetchall():
                event_key = (row[0], row[1], row[2])  # (event_type, event_date, title)
                existing_events.add(event_key)
            
            print(f"✅ Batch calendar query found {len(existing_events)} existing events for company")
            return existing_events
            
        except Exception as e:
            print(f"❌ Batch calendar query failed: {e}")
            import traceback
            traceback.print_exc()
            print(f"🔄 Falling back to individual event queries...")
            
            # Fallback to individual queries if batch fails
            existing_events = set()
            for event in events:
                try:
                    exists = await self._check_existing_event(
                        db, company_id, event["event_type"], event["event_date"], event.get("title")
                    )
                    if exists:
                        event_key = (event["event_type"], event["event_date"], event.get("title"))
                        existing_events.add(event_key)
                except:
                    pass
            return existing_events
    
    async def _check_existing_event(
        self, 
        db: AsyncSession, 
        company_id: str, 
        event_type: str, 
        event_date: date,
        event_title: str = None
    ) -> bool:
        """Check if similar calendar event already exists"""
        
        # Use title for more specific duplicate detection
        if event_title:
            result = await db.execute(
                select(NordicCalendarEvent).where(
                    and_(
                        NordicCalendarEvent.company_id == company_id,
                        NordicCalendarEvent.event_type == event_type,
                        NordicCalendarEvent.event_date == event_date,
                        NordicCalendarEvent.title == event_title
                    )
                )
            )
        else:
            # Fallback to old logic
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