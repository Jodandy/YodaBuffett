#!/usr/bin/env python3
"""
Market-Specific Configurations

Defines configuration for each Nordic market including:
- Data sources and endpoints
- Market hours and holidays
- Language and currency settings
- Regulatory requirements
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import time
from enum import Enum

class Market(Enum):
    """Supported Nordic markets"""
    SWEDISH = "swedish"
    NORWEGIAN = "norwegian"
    DANISH = "danish"
    FINNISH = "finnish"

class Currency(Enum):
    """Nordic currencies"""
    SEK = "SEK"  # Swedish Krona
    NOK = "NOK"  # Norwegian Krone
    DKK = "DKK"  # Danish Krone
    EUR = "EUR"  # Euro (Finland)

@dataclass
class TradingHours:
    """Market trading hours"""
    market_open: time
    market_close: time
    pre_market_open: Optional[time] = None
    post_market_close: Optional[time] = None
    timezone: str = "Europe/Stockholm"

@dataclass
class DataSourceConfig:
    """Configuration for a data source"""
    name: str
    source_type: str  # 'rss', 'api', 'web_scraping', 'email'
    base_url: Optional[str] = None
    endpoints: Dict[str, str] = None
    credentials: Dict[str, str] = None
    rate_limit: float = 2.0  # seconds between requests
    timeout: int = 30  # request timeout
    headers: Dict[str, str] = None

@dataclass
class MarketConfig:
    """Complete configuration for a market"""
    market: Market
    full_name: str
    country_code: str
    currency: Currency
    languages: List[str]
    trading_hours: TradingHours
    data_sources: List[DataSourceConfig]
    
    # Market-specific settings
    fiscal_year_end: str  # "MM-DD" format
    quarterly_reporting_months: List[int]
    
    # Document type mappings
    document_type_keywords: Dict[str, List[str]]
    
    # Regulatory settings
    regulatory_authority: str
    insider_trading_delay_hours: int = 0

# Swedish Market Configuration
SWEDISH_CONFIG = MarketConfig(
    market=Market.SWEDISH,
    full_name="Swedish Financial Market",
    country_code="SE",
    currency=Currency.SEK,
    languages=["sv", "en"],
    trading_hours=TradingHours(
        market_open=time(9, 0),
        market_close=time(17, 30),
        timezone="Europe/Stockholm"
    ),
    data_sources=[
        DataSourceConfig(
            name="MFN.se",
            source_type="web_scraping",
            base_url="https://mfn.se",
            endpoints={
                "company": "/all/a/{company_slug}",
                "calendar": "/kalender",
                "news": "/nyheter"
            },
            rate_limit=2.0
        ),
        DataSourceConfig(
            name="Nasdaq Stockholm",
            source_type="api",
            base_url="https://www.nasdaqomxnordic.com",
            endpoints={
                "companies": "/shares/listed-companies",
                "news": "/news/companynews"
            },
            rate_limit=3.0
        ),
        DataSourceConfig(
            name="Finansinspektionen",
            source_type="rss",
            base_url="https://www.fi.se",
            endpoints={
                "rss": "/sv/publicerat/nyheter/rss/"
            },
            rate_limit=5.0
        )
    ],
    fiscal_year_end="12-31",
    quarterly_reporting_months=[2, 5, 8, 11],
    document_type_keywords={
        "annual_report": ["årsredovisning", "annual report", "årsbokslut", "årsrapport"],
        "quarterly_report": ["kvartalsrapport", "quarterly report", "delårsrapport", "q1", "q2", "q3", "q4"],
        "interim_report": ["halvårsrapport", "half-year report", "sexmånadersrapport"],
        "press_release": ["pressmeddelande", "press release", "pressrelease"],
        "insider_trading": ["insynshandel", "insider trading", "insynsperson"],
        "prospectus": ["prospekt", "prospectus", "emissionsprospekt"],
        "corporate_action": ["förvärv", "fusion", "utdelning", "emission", "merger", "acquisition"]
    },
    regulatory_authority="Finansinspektionen",
    insider_trading_delay_hours=0
)

# Norwegian Market Configuration
NORWEGIAN_CONFIG = MarketConfig(
    market=Market.NORWEGIAN,
    full_name="Norwegian Financial Market",
    country_code="NO",
    currency=Currency.NOK,
    languages=["no", "en"],
    trading_hours=TradingHours(
        market_open=time(9, 0),
        market_close=time(16, 25),
        timezone="Europe/Oslo"
    ),
    data_sources=[
        DataSourceConfig(
            name="Newsweb",
            source_type="web_scraping",
            base_url="https://newsweb.oslobors.no",
            endpoints={
                "company": "/search?category=&issuer={company_id}",
                "all_news": "/",
                "api": "/api/news/all"
            },
            rate_limit=2.0
        ),
        DataSourceConfig(
            name="Oslo Børs",
            source_type="api",
            base_url="https://www.oslobors.no",
            endpoints={
                "companies": "/ob/servlets/components/searchEngine/search",
                "news": "/ob/servlets/newslist"
            },
            rate_limit=3.0
        ),
        DataSourceConfig(
            name="Finanstilsynet",
            source_type="rss",
            base_url="https://www.finanstilsynet.no",
            endpoints={
                "rss": "/rss/nyheter"
            },
            rate_limit=5.0
        )
    ],
    fiscal_year_end="12-31",
    quarterly_reporting_months=[2, 5, 8, 11],
    document_type_keywords={
        "annual_report": ["årsrapport", "annual report", "årsregnskap"],
        "quarterly_report": ["kvartalsrapport", "quarterly report", "q1", "q2", "q3", "q4"],
        "interim_report": ["halvårsrapport", "half-year report", "delårsrapport"],
        "press_release": ["pressemelding", "press release", "børsmelding"],
        "insider_trading": ["innsidehandel", "insider trading", "primærinnsider"],
        "prospectus": ["prospekt", "prospectus", "emisjonsprospekt"],
        "corporate_action": ["oppkjøp", "fusjon", "utbytte", "emisjon", "merger", "acquisition"]
    },
    regulatory_authority="Finanstilsynet",
    insider_trading_delay_hours=1
)

# Danish Market Configuration
DANISH_CONFIG = MarketConfig(
    market=Market.DANISH,
    full_name="Danish Financial Market",
    country_code="DK",
    currency=Currency.DKK,
    languages=["da", "en"],
    trading_hours=TradingHours(
        market_open=time(9, 0),
        market_close=time(17, 0),
        timezone="Europe/Copenhagen"
    ),
    data_sources=[
        DataSourceConfig(
            name="Nasdaq Copenhagen",
            source_type="api",
            base_url="https://www.nasdaqomxnordic.com",
            endpoints={
                "companies": "/shares/listed-companies/copenhagen",
                "news": "/news/companynews"
            },
            rate_limit=3.0
        ),
        DataSourceConfig(
            name="Finanstilsynet DK",
            source_type="web_scraping",
            base_url="https://www.finanstilsynet.dk",
            endpoints={
                "companies": "/kapitalmarked/boerser-og-udstedere"
            },
            rate_limit=5.0
        )
    ],
    fiscal_year_end="12-31",
    quarterly_reporting_months=[2, 5, 8, 11],
    document_type_keywords={
        "annual_report": ["årsrapport", "annual report", "årsregnskab"],
        "quarterly_report": ["kvartalsrapport", "quarterly report", "delårsrapport"],
        "interim_report": ["halvårsrapport", "half-year report"],
        "press_release": ["pressemeddelelse", "press release", "selskabsmeddelelse"],
        "prospectus": ["prospekt", "prospectus"],
        "corporate_action": ["opkøb", "fusion", "udbytte", "emission", "merger", "acquisition"]
    },
    regulatory_authority="Finanstilsynet",
    insider_trading_delay_hours=0
)

# Finnish Market Configuration
FINNISH_CONFIG = MarketConfig(
    market=Market.FINNISH,
    full_name="Finnish Financial Market",
    country_code="FI",
    currency=Currency.EUR,
    languages=["fi", "sv", "en"],
    trading_hours=TradingHours(
        market_open=time(10, 0),  # Note: Finnish time is +1 hour from Stockholm
        market_close=time(18, 30),
        timezone="Europe/Helsinki"
    ),
    data_sources=[
        DataSourceConfig(
            name="Nasdaq Helsinki",
            source_type="api",
            base_url="https://www.nasdaqomxnordic.com",
            endpoints={
                "companies": "/shares/listed-companies/helsinki",
                "news": "/news/companynews"
            },
            rate_limit=3.0
        ),
        DataSourceConfig(
            name="Finanssivalvonta",
            source_type="rss",
            base_url="https://www.finanssivalvonta.fi",
            endpoints={
                "rss": "/en/news/rss"
            },
            rate_limit=5.0
        )
    ],
    fiscal_year_end="12-31",
    quarterly_reporting_months=[2, 5, 8, 11],
    document_type_keywords={
        "annual_report": ["vuosikertomus", "annual report", "årsberättelse", "tilinpäätös"],
        "quarterly_report": ["osavuosikatsaus", "quarterly report", "kvartalsrapport"],
        "interim_report": ["puolivuosikatsaus", "half-year report", "halvårsrapport"],
        "press_release": ["pörssitiedote", "press release", "pressmeddelande", "tiedote"],
        "insider_trading": ["sisäpiirikauppa", "insider trading", "insiderhandel"],
        "prospectus": ["esite", "prospectus", "prospekt"],
        "corporate_action": ["yritysosto", "fuusio", "osinko", "osakeanti", "merger", "acquisition"]
    },
    regulatory_authority="Finanssivalvonta",
    insider_trading_delay_hours=0
)

# Market registry
MARKET_CONFIGS = {
    Market.SWEDISH: SWEDISH_CONFIG,
    Market.NORWEGIAN: NORWEGIAN_CONFIG,
    Market.DANISH: DANISH_CONFIG,
    Market.FINNISH: FINNISH_CONFIG
}

def get_market_config(market: Market) -> MarketConfig:
    """Get configuration for a specific market"""
    return MARKET_CONFIGS.get(market)

def get_all_markets() -> List[Market]:
    """Get list of all supported markets"""
    return list(Market)

def get_data_source(market: Market, source_name: str) -> Optional[DataSourceConfig]:
    """Get specific data source configuration for a market"""
    config = get_market_config(market)
    if config:
        for source in config.data_sources:
            if source.name == source_name:
                return source
    return None

# Market holidays (simplified - extend as needed)
MARKET_HOLIDAYS = {
    Market.SWEDISH: [
        "01-01",  # New Year's Day
        "01-06",  # Epiphany
        "05-01",  # Labour Day
        "06-06",  # National Day
        "12-24",  # Christmas Eve
        "12-25",  # Christmas Day
        "12-26",  # Boxing Day
        "12-31",  # New Year's Eve
        # Add Easter, Midsummer (moveable feasts) dynamically
    ],
    Market.NORWEGIAN: [
        "01-01",  # New Year's Day
        "05-01",  # Labour Day
        "05-17",  # Constitution Day
        "12-24",  # Christmas Eve
        "12-25",  # Christmas Day
        "12-26",  # Boxing Day
        "12-31",  # New Year's Eve
    ],
    Market.DANISH: [
        "01-01",  # New Year's Day
        "06-05",  # Constitution Day
        "12-24",  # Christmas Eve
        "12-25",  # Christmas Day
        "12-26",  # Boxing Day
        "12-31",  # New Year's Eve
    ],
    Market.FINNISH: [
        "01-01",  # New Year's Day
        "01-06",  # Epiphany
        "05-01",  # Labour Day
        "12-06",  # Independence Day
        "12-24",  # Christmas Eve
        "12-25",  # Christmas Day
        "12-26",  # Boxing Day
    ]
}