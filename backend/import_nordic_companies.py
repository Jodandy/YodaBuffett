"""
Import Nordic Companies from JSON file
Load comprehensive Nordic company data into nordic_companies table
"""
import asyncio
import sys
import os
import json
import uuid
from datetime import datetime
from typing import List, Dict, Any
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from sqlalchemy import select

# Mapping dictionaries for data normalization
COUNTRY_MAP = {
    1: "SE",  # Sweden
    2: "NO",  # Norway  
    3: "DK",  # Denmark
    4: "FI",  # Finland
    # Add more as needed
}

SECTOR_MAP = {
    1: "Basic Materials",
    2: "Consumer Goods", 
    3: "Consumer Services",
    4: "Energy",
    5: "Financials",
    6: "Health Care",
    7: "Industrials",
    8: "Technology",
    9: "Telecommunications",
    10: "Utilities",
    # Add more mappings as needed
}

# Instrument type mappings - filter out non-stocks
INSTRUMENT_TYPES = {
    0: "Common Stock",           # Regular stocks - INCLUDE
    1: "Preferred Stock",        # Preferred shares - INCLUDE  
    2: "Rights",                 # Rights/Warrants - EXCLUDE
    3: "Bond",                   # Bonds - EXCLUDE
    4: "Fund",                   # Funds/ETFs - EXCLUDE
    5: "Warrant",                # Warrants - EXCLUDE
    8: "Certificate",            # Certificates - EXCLUDE
    13: "Index",                 # Indexes - EXCLUDE
    # Add more as needed
}

# Only include these instrument types (stocks)
STOCK_INSTRUMENT_TYPES = [0, 1]  # Common and Preferred stocks

MARKET_MAP = {
    1: "NASDAQ Stockholm",
    2: "Oslo Børs",
    3: "NASDAQ Copenhagen", 
    4: "NASDAQ Helsinki",
    # Add more as needed
}

def determine_market_cap(listing_date: str, ticker: str) -> str:
    """Determine market cap category based on available info"""
    # Simple heuristic - can be improved with actual market cap data
    try:
        listing_year = int(listing_date[:4])
        if listing_year < 2010:
            return "Large Cap"  # Older listings tend to be larger companies
        elif listing_year < 2015:
            return "Mid Cap"
        else:
            return "Small Cap"
    except:
        return "Mid Cap"  # Default

def generate_mfn_slug(url_name: str, name: str, ticker: str) -> str:
    """Generate likely MFN.se slug from available data"""
    # Use urlName if available, otherwise derive from name/ticker
    if url_name and url_name != "":
        return url_name.lower()
    
    # Fallback: use ticker or simplified name
    if ticker:
        return ticker.lower().replace("-", "").replace(".", "")
    
    # Last resort: simplified company name
    return name.lower().replace(" ", "-").replace("ab", "").replace("ltd", "").strip("-")

async def import_nordic_companies(json_file_path: str, limit_countries: List[str] = None):
    print("🌍 Importing Nordic Companies from JSON")
    print("=" * 60)
    
    # Load JSON data
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Handle both array format and object with "instruments" key
        if isinstance(json_data, dict) and "instruments" in json_data:
            companies_data = json_data["instruments"]
        else:
            companies_data = json_data
            
        print(f"📄 Loaded {len(companies_data)} instruments from {json_file_path}")
    except FileNotFoundError:
        print(f"❌ File not found: {json_file_path}")
        return
    except json.JSONDecodeError as e:
        print(f"❌ JSON parsing error: {e}")
        return
    
    async with AsyncSessionLocal() as db:
        # Check existing companies
        existing_result = await db.execute(
            select(NordicCompany.ticker, NordicCompany.name)
        )
        existing_tickers = set(ticker for (ticker, name) in existing_result.all())
        
        print(f"📊 Found {len(existing_tickers)} existing companies in database")
        
        # Process and filter companies
        new_companies = []
        skipped_companies = []
        country_stats = {}
        instrument_stats = {}
        filtered_out = {
            'non_stocks': 0,
            'wrong_country': 0,
            'existing': 0,
            'incomplete': 0
        }
        
        for company_data in companies_data:
            try:
                # Filter by instrument type - only stocks
                instrument_type = company_data.get('instrument', 0)
                instrument_name = INSTRUMENT_TYPES.get(instrument_type, f"Unknown ({instrument_type})")
                instrument_stats[instrument_name] = instrument_stats.get(instrument_name, 0) + 1
                
                if instrument_type not in STOCK_INSTRUMENT_TYPES:
                    filtered_out['non_stocks'] += 1
                    continue
                
                # Get country code
                country_id = company_data.get('countryId', 1)
                country_code = COUNTRY_MAP.get(country_id, 'Unknown')
                
                # Filter by countries if specified
                if limit_countries and country_code not in limit_countries:
                    filtered_out['wrong_country'] += 1
                    continue
                    
                # Count countries (only for stocks)
                country_stats[country_code] = country_stats.get(country_code, 0) + 1
                
                # Check if already exists
                ticker = company_data.get('ticker', '')
                if ticker in existing_tickers:
                    skipped_companies.append(f"{company_data.get('name', 'Unknown')} ({ticker})")
                    filtered_out['existing'] += 1
                    continue
                
                # Extract and normalize data
                name = company_data.get('name', '').strip()
                url_name = company_data.get('urlName', '').strip()
                isin = company_data.get('isin', '')
                yahoo_ticker = company_data.get('yahoo', '')
                sector_id = company_data.get('sectorId', 1)
                market_id = company_data.get('marketId', 1)
                listing_date = company_data.get('listingDate', '')
                
                if not name or not ticker:
                    filtered_out['incomplete'] += 1
                    continue  # Skip incomplete entries
                
                # Generate MFN slug
                mfn_slug = generate_mfn_slug(url_name, name, ticker)
                
                # Create company entry
                new_company = NordicCompany(
                    id=uuid.uuid4(),
                    name=name,
                    ticker=ticker,
                    exchange=MARKET_MAP.get(market_id, 'Unknown Exchange'),
                    country=country_code,
                    market_cap_category=determine_market_cap(listing_date, ticker),
                    sector=SECTOR_MAP.get(sector_id, 'Unknown Sector'),
                    ir_email=None,
                    ir_website=f"https://mfn.se/all/a/{mfn_slug}" if country_code == 'SE' else None,
                    website=None,
                    reporting_language='sv' if country_code == 'SE' else 
                                    'no' if country_code == 'NO' else
                                    'da' if country_code == 'DK' else
                                    'fi' if country_code == 'FI' else 'en',
                    metadata_={
                        "insId": company_data.get('insId'),
                        "mfn_slug": mfn_slug if country_code == 'SE' else None,
                        "mfn_url": f"https://mfn.se/all/a/{mfn_slug}" if country_code == 'SE' else None,
                        "isin": isin,
                        "yahoo_ticker": yahoo_ticker,
                        "listing_date": listing_date,
                        "stock_currency": company_data.get('stockPriceCurrency', ''),
                        "report_currency": company_data.get('reportCurrency', ''),
                        "data_source": "nordic_companies_json",
                        "original_sector_id": sector_id,
                        "original_market_id": market_id,
                        "original_country_id": country_id,
                        "collection_priority": "high" if determine_market_cap(listing_date, ticker) == "Large Cap" else "medium"
                    },
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                
                new_companies.append(new_company)
                
            except Exception as e:
                print(f"⚠️  Error processing company {company_data.get('name', 'Unknown')}: {e}")
                continue
        
        # Show statistics
        print(f"\n📊 INSTRUMENT TYPE ANALYSIS:")
        print(f"  📈 All instruments found in JSON:")
        for instrument, count in sorted(instrument_stats.items()):
            included = "✅" if any(name in instrument for name in ["Common Stock", "Preferred Stock"]) else "❌"
            print(f"    {included} {instrument}: {count}")
        
        print(f"\n🔍 FILTERING SUMMARY:")
        print(f"  ❌ Non-stocks filtered out: {filtered_out['non_stocks']}")
        print(f"  🌍 Wrong country filtered out: {filtered_out['wrong_country']}")
        print(f"  🔄 Already existing: {filtered_out['existing']}")
        print(f"  📝 Incomplete data: {filtered_out['incomplete']}")
        print(f"  ✅ Stocks ready for import: {len(new_companies)}")
        
        print(f"\n📊 STOCK COMPANIES BY COUNTRY:")
        print(f"  🌍 Countries found (stocks only):")
        for country, count in sorted(country_stats.items()):
            flag = {"SE": "🇸🇪", "NO": "🇳🇴", "DK": "🇩🇰", "FI": "🇫🇮"}.get(country, "🌍")
            print(f"    {flag} {country}: {count} companies")
        
        print(f"\n📥 IMPORT SUMMARY:")
        print(f"  ➕ New companies to add: {len(new_companies)}")
        print(f"  ⏭️  Existing companies skipped: {len(skipped_companies)}")
        
        # Show sample of new companies by country
        if new_companies:
            print(f"\n📋 SAMPLE NEW COMPANIES:")
            by_country = {}
            for company in new_companies:
                if company.country not in by_country:
                    by_country[company.country] = []
                by_country[company.country].append(company)
            
            for country_code, companies in sorted(by_country.items()):
                flag = {"SE": "🇸🇪", "NO": "🇳🇴", "DK": "🇩🇰", "FI": "🇫🇮"}.get(country_code, "🌍")
                print(f"  {flag} {country_code} ({len(companies)} companies):")
                
                # Show first 5 companies as sample
                for company in companies[:5]:
                    mfn_info = f" (mfn: {company.metadata_.get('mfn_slug', 'N/A')})" if company.country == 'SE' else ""
                    print(f"    🏢 {company.name} ({company.ticker}){mfn_info}")
                
                if len(companies) > 5:
                    print(f"    ... and {len(companies) - 5} more")
        
        # Confirm before importing
        if new_companies:
            print(f"\n❓ Import {len(new_companies)} new companies? (y/n): ", end="")
            try:
                confirmation = input().strip().lower()
            except EOFError:
                # Non-interactive mode - default to yes
                confirmation = 'y'
                print("y (auto-confirmed)")
            
            if confirmation in ['y', 'yes']:
                # Add companies to database
                for company in new_companies:
                    db.add(company)
                
                await db.commit()
                print(f"✅ Successfully imported {len(new_companies)} companies!")
                
                # Show final database stats
                total_result = await db.execute(
                    select(NordicCompany.country, NordicCompany.market_cap_category)
                )
                all_companies = total_result.all()
                
                final_stats = {}
                cap_stats = {}
                
                for country, market_cap in all_companies:
                    final_stats[country] = final_stats.get(country, 0) + 1
                    cap_stats[market_cap] = cap_stats.get(market_cap, 0) + 1
                
                print(f"\n🎉 FINAL DATABASE STATUS:")
                print(f"  📊 Total companies: {len(all_companies)}")
                print(f"  🌍 By country:")
                for country, count in sorted(final_stats.items()):
                    flag = {"SE": "🇸🇪", "NO": "🇳🇴", "DK": "🇩🇰", "FI": "🇫🇮"}.get(country, "🌍")
                    print(f"    {flag} {country}: {count}")
                print(f"  💰 By market cap:")
                for cap, count in sorted(cap_stats.items()):
                    print(f"    📈 {cap}: {count}")
                
                print(f"\n🚀 Ready for systematic indexing!")
                
            else:
                print(f"❌ Import cancelled")
        else:
            print(f"\n✅ No new companies to import!")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Import Nordic companies from JSON')
    parser.add_argument('json_file', help='Path to JSON file with company data')
    parser.add_argument('--countries', nargs='+', default=['SE'], 
                       help='Country codes to import (default: SE only)')
    
    args = parser.parse_args()
    
    print(f"📄 JSON file: {args.json_file}")
    print(f"🌍 Countries: {', '.join(args.countries)}")
    print()
    
    asyncio.run(import_nordic_companies(args.json_file, args.countries))

if __name__ == "__main__":
    main()