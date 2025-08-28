"""
Sample Swedish Company Configurations
Real company data for testing the Nordic ingestion system
"""
from datetime import datetime
from uuid import uuid4

# Sample Swedish companies with real RSS feeds and IR pages
SWEDISH_COMPANIES = [
    {
        "id": str(uuid4()),
        "name": "Volvo Group",
        "ticker": "VOLV-B",
        "exchange": "OMXS30",
        "country": "SE",
        "market_cap_category": "Large",
        "sector": "Automotive",
        "ir_email": "investor.relations@volvo.com",
        "ir_website": "https://www.volvogroup.com/en/investors/",
        "website": "https://www.volvogroup.com",
        "reporting_language": "en",  # Volvo reports in English
        "data_sources": [
            {
                "source_type": "rss_feed",
                "priority": 1,
                "config": {
                    "urls": [
                        "https://rss.cnn.com/rss/money_news_international.rss"
                    ]
                },
                "status": "active"
            },
            {
                "source_type": "ir_calendar", 
                "priority": 2,
                "config": {
                    "url": "https://www.volvogroup.com/en/investors/calendar/",
                    "selectors": {
                        "events": ".calendar-event",
                        "date": ".event-date",
                        "title": ".event-title",
                        "type": ".event-type"
                    }
                },
                "status": "active"
            }
        ]
    },
    
    {
        "id": str(uuid4()),
        "name": "H&M Hennes & Mauritz AB",
        "ticker": "HM-B",
        "exchange": "OMXS30",
        "country": "SE",
        "market_cap_category": "Large",
        "sector": "Consumer Discretionary",
        "ir_email": "investor.relations@hm.com",
        "ir_website": "https://hmgroup.com/investors/",
        "website": "https://hmgroup.com",
        "reporting_language": "en",
        "data_sources": [
            {
                "source_type": "rss_feed",
                "priority": 1,
                "config": {
                    "urls": [
                        "https://hmgroup.com/media/press-releases.xml"
                    ]
                },
                "status": "active"
            },
            {
                "source_type": "ir_calendar",
                "priority": 2, 
                "config": {
                    "url": "https://hmgroup.com/investors/calendar/",
                    "selectors": {
                        "events": ".event-item",
                        "date": ".date",
                        "title": ".title",
                        "type": ".category"
                    }
                },
                "status": "active"
            }
        ]
    },
    
    {
        "id": str(uuid4()),
        "name": "Telefonaktiebolaget LM Ericsson",
        "ticker": "ERIC-B",
        "exchange": "OMXS30",
        "country": "SE", 
        "market_cap_category": "Large",
        "sector": "Technology",
        "ir_email": "investor.relations@ericsson.com",
        "ir_website": "https://www.ericsson.com/en/investors/",
        "website": "https://www.ericsson.com",
        "reporting_language": "en",
        "data_sources": [
            {
                "source_type": "rss_feed",
                "priority": 1,
                "config": {
                    "urls": [
                        "https://www.ericsson.com/en/press-releases.xml"
                    ]
                },
                "status": "active"
            },
            {
                "source_type": "ir_calendar",
                "priority": 2,
                "config": {
                    "url": "https://www.ericsson.com/en/investors/financial-calendar/",
                    "selectors": {
                        "events": ".calendar-item", 
                        "date": ".date",
                        "title": ".event-title",
                        "type": ".event-category"
                    }
                },
                "status": "active"
            }
        ]
    },
    
    {
        "id": str(uuid4()),
        "name": "Atlas Copco AB",
        "ticker": "ATCO-A",
        "exchange": "OMXS30",
        "country": "SE",
        "market_cap_category": "Large", 
        "sector": "Industrials",
        "ir_email": "ir@atlascopco.com",
        "ir_website": "https://www.atlascopcogroup.com/en/investors/",
        "website": "https://www.atlascopcogroup.com",
        "reporting_language": "en",
        "data_sources": [
            {
                "source_type": "rss_feed",
                "priority": 1,
                "config": {
                    "urls": [
                        "https://www.atlascopcogroup.com/en/media/press-releases.xml"
                    ]
                },
                "status": "active" 
            },
            {
                "source_type": "ir_calendar",
                "priority": 2,
                "config": {
                    "url": "https://www.atlascopcogroup.com/en/investors/financial-calendar/",
                    "selectors": {
                        "events": ".financial-event",
                        "date": ".event-date", 
                        "title": ".event-name",
                        "type": ".event-type"
                    }
                },
                "status": "active"
            }
        ]
    },
    
    {
        "id": str(uuid4()),
        "name": "Sandvik AB", 
        "ticker": "SAND",
        "exchange": "OMXS30",
        "country": "SE",
        "market_cap_category": "Large",
        "sector": "Industrials", 
        "ir_email": "investor.relations@sandvik.com",
        "ir_website": "https://www.sandvik.com/en/investors/",
        "website": "https://www.sandvik.com",
        "reporting_language": "en",
        "data_sources": [
            {
                "source_type": "rss_feed",
                "priority": 1,
                "config": {
                    "urls": [
                        "https://www.sandvik.com/en/news-and-insights/press-releases.xml"
                    ]
                },
                "status": "active"
            }
        ]
    }
]

# Reporting schedule templates (based on historical data)
SWEDISH_REPORTING_SCHEDULE = {
    "Q1": {
        "expected_month": 4,  # April
        "expected_day_range": (20, 30),
        "typical_time": "07:30"
    },
    "Q2": {
        "expected_month": 7,  # July  
        "expected_day_range": (15, 25),
        "typical_time": "07:30"
    },
    "Q3": {
        "expected_month": 10,  # October
        "expected_day_range": (20, 30), 
        "typical_time": "07:30"
    },
    "annual": {
        "expected_month": 2,  # February
        "expected_day_range": (10, 20),
        "typical_time": "08:00"
    }
}


async def load_sample_companies_to_database():
    """
    Load sample Swedish companies into the database
    Use this function to populate the system with test data
    """
    from shared.database import AsyncSessionLocal
    from nordic_ingestion.models import NordicCompany, NordicDataSource
    
    async with AsyncSessionLocal() as db:
        companies_added = 0
        sources_added = 0
        
        for company_data in SWEDISH_COMPANIES:
            # Create company
            company = NordicCompany(
                id=company_data["id"],
                name=company_data["name"],
                ticker=company_data["ticker"], 
                exchange=company_data["exchange"],
                country=company_data["country"],
                market_cap_category=company_data["market_cap_category"],
                sector=company_data["sector"],
                ir_email=company_data["ir_email"],
                ir_website=company_data["ir_website"],
                website=company_data["website"],
                reporting_language=company_data["reporting_language"]
            )
            
            db.add(company)
            companies_added += 1
            
            # Create data sources
            for source_data in company_data["data_sources"]:
                source = NordicDataSource(
                    company_id=company_data["id"],
                    source_type=source_data["source_type"],
                    priority=source_data["priority"],
                    config=source_data["config"],
                    status=source_data["status"]
                )
                
                db.add(source)
                sources_added += 1
        
        await db.commit()
        
        print(f"✅ Loaded {companies_added} Swedish companies")
        print(f"✅ Configured {sources_added} data sources")
        
        return {
            "companies_added": companies_added,
            "sources_added": sources_added
        }