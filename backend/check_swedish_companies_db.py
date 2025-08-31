#!/usr/bin/env python3
"""
Check Swedish companies in database to build better mappings
"""
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from sqlalchemy import select

async def check_swedish_companies():
    """Check what Swedish companies are in the database"""
    
    print("🔍 CHECKING SWEDISH COMPANIES IN DATABASE")
    print("=" * 60)
    
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(NordicCompany.name, NordicCompany.ticker, NordicCompany.id)
                .where(NordicCompany.country == 'SE')
                .order_by(NordicCompany.name)
            )
            companies = result.fetchall()
            
            print(f"📊 Found {len(companies)} Swedish companies in database\n")
            
            # Test some problematic cases
            test_cases = {
                'aac-clyde-space': ['aac', 'clyde', 'space'],
                'aak': ['aak'],
                'active-biotech': ['active', 'biotech'],
                'adecco': ['adecco'],
                'addlife': ['addlife', 'add', 'life']
            }
            
            print("🎯 TESTING PROBLEMATIC CASES:")
            for slug, search_terms in test_cases.items():
                print(f"\n📋 MFN Slug: '{slug}'")
                print(f"🔍 Search terms: {search_terms}")
                
                matches = []
                for name, ticker, company_id in companies:
                    name_lower = name.lower()
                    ticker_lower = ticker.lower() if ticker else ""
                    
                    # Check if any search term matches
                    if any(term in name_lower or term in ticker_lower for term in search_terms):
                        matches.append((name, ticker, company_id))
                
                if matches:
                    print(f"✅ Potential matches:")
                    for name, ticker, company_id in matches[:5]:
                        print(f"   • {name} ({ticker})")
                else:
                    print(f"❌ No matches found")
            
            print(f"\n📋 FIRST 30 COMPANIES (for reference):")
            for i, (name, ticker, company_id) in enumerate(companies[:30], 1):
                print(f"{i:2d}. {name} ({ticker})")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_swedish_companies())