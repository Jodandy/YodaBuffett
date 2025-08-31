#!/usr/bin/env python3
"""
Event Scheduler for Calendar-Driven Data Collection

This service queries the calendar events database to identify companies
that should be scraped based on upcoming financial events (earnings, reports).

Features:
- Smart event filtering (earnings, annual/quarterly reports only)
- Timing optimization (scrape day-of and day-after events)
- Company prioritization based on market cap and importance
- Configurable look-ahead windows
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCalendarEvent, NordicCompany
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EventPriority(Enum):
    """Event priority levels for scheduling"""
    HIGH = "high"      # Earnings, quarterly/annual reports
    MEDIUM = "medium"  # Dividend announcements, AGMs
    LOW = "low"        # Other corporate events

class CompanyTier(Enum):
    """Company importance tiers for resource allocation"""
    TIER_1 = "tier_1"  # Large cap, high importance
    TIER_2 = "tier_2"  # Mid cap, medium importance  
    TIER_3 = "tier_3"  # Small cap, low importance

@dataclass
class ScheduledScrape:
    """Represents a company scheduled for scraping"""
    company_id: str
    company_name: str
    company_ticker: str
    event_date: date
    event_type: str
    event_title: str
    priority: EventPriority
    company_tier: CompanyTier
    scrape_date: date
    reason: str

class EventScheduler:
    """
    Smart scheduler that uses calendar events to target data collection
    """
    
    def __init__(self, 
                 look_ahead_days: int = 3,
                 look_back_days: int = 1):
        self.look_ahead_days = look_ahead_days
        self.look_back_days = look_back_days
        
        # Event types that trigger scraping
        self.high_priority_events = {
            'earnings', 'quarterly_report', 'annual_report'
        }
        
        self.medium_priority_events = {
            'dividend', 'agm', 'extraordinary_general_meeting'
        }
    
    async def get_daily_scrape_targets(self, 
                                     target_date: Optional[date] = None) -> List[ScheduledScrape]:
        """
        Get list of companies to scrape based on calendar events
        
        Args:
            target_date: Date to schedule for (defaults to today)
            
        Returns:
            List of ScheduledScrape objects ordered by priority
        """
        if target_date is None:
            target_date = date.today()
            
        logger.info(f"🗓️  Generating scrape targets for {target_date}")
        
        async with AsyncSessionLocal() as db:
            # Get relevant events in our time window
            start_date = target_date - timedelta(days=self.look_back_days)
            end_date = target_date + timedelta(days=self.look_ahead_days)
            
            events = await self._get_relevant_events(db, start_date, end_date)
            logger.info(f"📊 Found {len(events)} relevant events in date range")
            
            # Convert events to scheduled scrapes
            scheduled_scrapes = []
            
            for event in events:
                company = await self._get_company_details(db, event.company_id)
                if not company:
                    continue
                
                priority = self._determine_event_priority(event.event_type)
                company_tier = self._determine_company_tier(company)
                scrape_date = self._calculate_optimal_scrape_date(event.event_date, target_date)
                
                # Only schedule if scrape date matches target date
                if scrape_date == target_date:
                    reason = self._generate_scrape_reason(event, target_date)
                    
                    scheduled_scrape = ScheduledScrape(
                        company_id=str(event.company_id),
                        company_name=company.name,
                        company_ticker=company.ticker,
                        event_date=event.event_date,
                        event_type=event.event_type,
                        event_title=event.title or "Untitled Event",
                        priority=priority,
                        company_tier=company_tier,
                        scrape_date=scrape_date,
                        reason=reason
                    )
                    
                    scheduled_scrapes.append(scheduled_scrape)
            
            # Sort by priority and company tier
            scheduled_scrapes.sort(key=lambda x: (
                x.priority.value,
                x.company_tier.value,
                x.event_date
            ))
            
            logger.info(f"✅ Generated {len(scheduled_scrapes)} scheduled scrapes")
            return scheduled_scrapes
    
    async def _get_relevant_events(self, 
                                 db: AsyncSession, 
                                 start_date: date, 
                                 end_date: date) -> List[NordicCalendarEvent]:
        """Get calendar events that should trigger scraping"""
        
        relevant_event_types = (
            list(self.high_priority_events) + 
            list(self.medium_priority_events)
        )
        
        result = await db.execute(
            select(NordicCalendarEvent).where(
                and_(
                    NordicCalendarEvent.event_date >= start_date,
                    NordicCalendarEvent.event_date <= end_date,
                    NordicCalendarEvent.event_type.in_(relevant_event_types)
                )
            ).order_by(NordicCalendarEvent.event_date, NordicCalendarEvent.company_id)
        )
        
        return result.scalars().all()
    
    async def _get_company_details(self, 
                                 db: AsyncSession, 
                                 company_id: str) -> Optional[NordicCompany]:
        """Get company details for scheduling"""
        result = await db.execute(
            select(NordicCompany).where(NordicCompany.id == company_id)
        )
        return result.scalar_one_or_none()
    
    def _determine_event_priority(self, event_type: str) -> EventPriority:
        """Determine priority level for event type"""
        if event_type in self.high_priority_events:
            return EventPriority.HIGH
        elif event_type in self.medium_priority_events:
            return EventPriority.MEDIUM
        else:
            return EventPriority.LOW
    
    def _determine_company_tier(self, company: NordicCompany) -> CompanyTier:
        """Determine company tier based on market cap and importance"""
        
        # Use market cap category if available
        if hasattr(company, 'market_cap_category') and company.market_cap_category:
            if company.market_cap_category.lower() in ['large_cap', 'large']:
                return CompanyTier.TIER_1
            elif company.market_cap_category.lower() in ['mid_cap', 'medium']:
                return CompanyTier.TIER_2
            else:
                return CompanyTier.TIER_3
        
        # Fallback to ticker-based heuristics for major Swedish companies
        major_companies = {
            'VOLV-B', 'ERIC-B', 'HM-B', 'ATCO-A', 'SAND', 'ASSA-B',
            'SEB-A', 'SWED-A', 'TEL2-B', 'KINV-B'
        }
        
        if company.ticker in major_companies:
            return CompanyTier.TIER_1
        else:
            return CompanyTier.TIER_2  # Default to tier 2 for unknown companies
    
    def _calculate_optimal_scrape_date(self, 
                                     event_date: date, 
                                     target_date: date) -> date:
        """
        Calculate optimal date to scrape for an event
        
        Strategy:
        - For future events: scrape on event date
        - For recent events: scrape day after (documents likely published)
        - For today's events: scrape today
        """
        
        if event_date > target_date:
            # Future event - scrape on the day of the event
            return event_date
        elif event_date == target_date:
            # Event is today - scrape today
            return target_date
        else:
            # Recent event - scrape day after for document availability
            return event_date + timedelta(days=1)
    
    def _generate_scrape_reason(self, event: NordicCalendarEvent, target_date: date) -> str:
        """Generate human-readable reason for scraping"""
        
        if event.event_date == target_date:
            return f"{event.event_type.replace('_', ' ').title()} event today"
        elif event.event_date < target_date:
            days_ago = (target_date - event.event_date).days
            return f"{event.event_type.replace('_', ' ').title()} event {days_ago} day(s) ago"
        else:
            days_ahead = (event.event_date - target_date).days
            return f"{event.event_type.replace('_', ' ').title()} event in {days_ahead} day(s)"
    
    async def get_weekly_surprise_targets(self, 
                                        sample_size: int = 50) -> List[ScheduledScrape]:
        """
        Get companies for weekly surprise news scanning
        
        Targets companies that:
        - Haven't been active recently (no events in last 14 days)
        - Haven't been scraped recently
        - Randomized selection for unpredictability
        """
        logger.info(f"🎲 Generating {sample_size} surprise scan targets")
        
        async with AsyncSessionLocal() as db:
            # Get companies without recent calendar events
            cutoff_date = date.today() - timedelta(days=14)
            
            # Find companies with no recent events
            result = await db.execute(
                select(NordicCompany).where(
                    ~NordicCompany.id.in_(
                        select(NordicCalendarEvent.company_id).where(
                            NordicCalendarEvent.event_date >= cutoff_date
                        )
                    )
                ).order_by(NordicCompany.name)
            )
            
            quiet_companies = result.scalars().all()
            logger.info(f"📊 Found {len(quiet_companies)} companies without recent events")
            
            # Randomly sample companies for surprise scanning
            import random
            selected_companies = random.sample(
                quiet_companies, 
                min(sample_size, len(quiet_companies))
            )
            
            # Convert to ScheduledScrape objects
            scheduled_scrapes = []
            for company in selected_companies:
                scheduled_scrape = ScheduledScrape(
                    company_id=str(company.id),
                    company_name=company.name,
                    company_ticker=company.ticker,
                    event_date=date.today(),
                    event_type="surprise_scan",
                    event_title="Weekly surprise news scan",
                    priority=EventPriority.LOW,
                    company_tier=self._determine_company_tier(company),
                    scrape_date=date.today(),
                    reason="Weekly surprise news detection"
                )
                scheduled_scrapes.append(scheduled_scrape)
            
            logger.info(f"✅ Generated {len(scheduled_scrapes)} surprise scan targets")
            return scheduled_scrapes

# CLI and testing functions
async def main():
    """Test the event scheduler"""
    scheduler = EventScheduler(look_ahead_days=3, look_back_days=1)
    
    print("🗓️  Event-Driven Scheduler Test")
    print("=" * 50)
    
    # Test daily targets
    targets = await scheduler.get_daily_scrape_targets()
    
    print(f"\n📊 Daily Scrape Targets ({len(targets)} companies):")
    for target in targets[:10]:  # Show first 10
        print(f"  🏢 {target.company_name} ({target.company_ticker})")
        print(f"     📅 Event: {target.event_title} on {target.event_date}")
        print(f"     🎯 Priority: {target.priority.value} | Tier: {target.company_tier.value}")
        print(f"     📝 Reason: {target.reason}")
        print()
    
    if len(targets) > 10:
        print(f"  ... and {len(targets) - 10} more companies")
    
    # Test weekly surprise targets
    surprise_targets = await scheduler.get_weekly_surprise_targets(sample_size=10)
    
    print(f"\n🎲 Weekly Surprise Targets ({len(surprise_targets)} companies):")
    for target in surprise_targets[:5]:  # Show first 5
        print(f"  🏢 {target.company_name} ({target.company_ticker})")
        print(f"     🎯 Tier: {target.company_tier.value}")
        print(f"     📝 Reason: {target.reason}")
    
    print(f"\n✅ Event scheduling test completed!")

if __name__ == "__main__":
    asyncio.run(main())