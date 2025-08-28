"""
Add Swedish Companies to Nordic Database
Populate nordic_companies table with major Swedish public companies
"""
import asyncio
import sys
import os
import uuid
from datetime import datetime
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from sqlalchemy import select

# Major Swedish companies with their MFN.se slugs and basic info
SWEDISH_COMPANIES = [
    # OMXS30 - Top 30 Swedish companies
    {
        "name": "Volvo Group",
        "ticker": "VOLV-B",
        "mfn_slug": "volvo",
        "sector": "Industrials",
        "market_cap": "Large Cap"
    },
    {
        "name": "AstraZeneca",
        "ticker": "AZN",
        "mfn_slug": "astrazeneca", 
        "sector": "Healthcare",
        "market_cap": "Large Cap"
    },
    {
        "name": "Atlas Copco AB",
        "ticker": "ATCO-A",
        "mfn_slug": "atlas-copco",
        "sector": "Industrials", 
        "market_cap": "Large Cap"
    },
    {
        "name": "Telefonaktiebolaget LM Ericsson",
        "ticker": "ERIC-B",
        "mfn_slug": "ericsson",
        "sector": "Technology",
        "market_cap": "Large Cap"
    },
    {
        "name": "H&M Hennes & Mauritz AB", 
        "ticker": "HM-B",
        "mfn_slug": "handm",
        "sector": "Consumer Discretionary",
        "market_cap": "Large Cap"
    },
    {
        "name": "Sandvik AB",
        "ticker": "SAND",
        "mfn_slug": "sandvik",
        "sector": "Industrials",
        "market_cap": "Large Cap"
    },
    {
        "name": "Nordea Bank Abp",
        "ticker": "NDA-SE", 
        "mfn_slug": "nordea",
        "sector": "Financial Services",
        "market_cap": "Large Cap"
    },
    {
        "name": "Investor AB",
        "ticker": "INVE-B",
        "mfn_slug": "investor",
        "sector": "Financial Services",
        "market_cap": "Large Cap"
    },
    {
        "name": "ABB Ltd",
        "ticker": "ABB",
        "mfn_slug": "abb",
        "sector": "Industrials",
        "market_cap": "Large Cap"
    },
    {
        "name": "Hexagon AB",
        "ticker": "HEXA-B",
        "mfn_slug": "hexagon",
        "sector": "Technology",
        "market_cap": "Large Cap"
    },
    
    # Additional prominent Swedish companies
    {
        "name": "Spotify Technology SA",
        "ticker": "SPOT",
        "mfn_slug": "spotify",
        "sector": "Technology",
        "market_cap": "Large Cap"
    },
    {
        "name": "Svenska Cellulosa Aktiebolaget SCA",
        "ticker": "SCA-B",
        "mfn_slug": "sca",
        "sector": "Materials",
        "market_cap": "Large Cap"
    },
    {
        "name": "Boliden AB",
        "ticker": "BOL",
        "mfn_slug": "boliden",
        "sector": "Materials",
        "market_cap": "Large Cap"
    },
    {
        "name": "Electrolux AB",
        "ticker": "ELUX-B",
        "mfn_slug": "electrolux",
        "sector": "Consumer Discretionary", 
        "market_cap": "Large Cap"
    },
    {
        "name": "Skanska AB",
        "ticker": "SKA-B",
        "mfn_slug": "skanska",
        "sector": "Industrials",
        "market_cap": "Large Cap"
    },
    {
        "name": "ICA Gruppen AB",
        "ticker": "ICA",
        "mfn_slug": "ica",
        "sector": "Consumer Staples",
        "market_cap": "Large Cap"
    },
    {
        "name": "Tele2 AB",
        "ticker": "TEL2-B",
        "mfn_slug": "tele2",
        "sector": "Telecommunications",
        "market_cap": "Mid Cap"
    },
    {
        "name": "Alfa Laval AB",
        "ticker": "ALFA",
        "mfn_slug": "alfa-laval",
        "sector": "Industrials",
        "market_cap": "Mid Cap"
    },
    {
        "name": "Kinnevik AB",
        "ticker": "KINV-B",
        "mfn_slug": "kinnevik",
        "sector": "Financial Services",
        "market_cap": "Mid Cap"
    },
    {
        "name": "Svenska Handelsbanken AB",
        "ticker": "SHB-A",
        "mfn_slug": "handelsbanken",
        "sector": "Financial Services",
        "market_cap": "Large Cap"
    },
    {
        "name": "Securitas AB",
        "ticker": "SECU-B",
        "mfn_slug": "securitas",
        "sector": "Industrials",
        "market_cap": "Mid Cap"
    },
    {
        "name": "Getinge AB",
        "ticker": "GETI-B", 
        "mfn_slug": "getinge",
        "sector": "Healthcare",
        "market_cap": "Mid Cap"
    },
    {
        "name": "Evolution AB",
        "ticker": "EVO",
        "mfn_slug": "evolution",
        "sector": "Consumer Discretionary",
        "market_cap": "Large Cap"
    },
    {
        "name": "Epiroc AB",
        "ticker": "EPI-A",
        "mfn_slug": "epiroc", 
        "sector": "Industrials",
        "market_cap": "Large Cap"
    },
    {
        "name": "SSAB AB",
        "ticker": "SSAB-A",
        "mfn_slug": "ssab",
        "sector": "Materials",
        "market_cap": "Mid Cap"
    }
]

async def add_swedish_companies():
    print("🏢 Adding Swedish Companies to Nordic Database")
    print("=" * 60)
    
    async with AsyncSessionLocal() as db:
        # Check existing companies
        existing_result = await db.execute(
            select(NordicCompany.name)
        )
        existing_names = set(name for (name,) in existing_result.all())
        
        print(f"📊 Found {len(existing_names)} existing companies:")
        for name in sorted(existing_names):
            print(f"  ✅ {name}")
        
        # Add new companies
        new_companies = []
        skipped_companies = []
        
        for company_data in SWEDISH_COMPANIES:
            if company_data["name"] in existing_names:
                skipped_companies.append(company_data["name"])
                continue
            
            # Create new company
            new_company = NordicCompany(
                id=uuid.uuid4(),
                name=company_data["name"],
                ticker=company_data["ticker"],
                exchange="NASDAQ Stockholm",
                country="SE",
                market_cap_category=company_data["market_cap"],
                sector=company_data["sector"],
                ir_email=None,  # To be populated later
                ir_website=f"https://mfn.se/all/a/{company_data['mfn_slug']}",
                website=None,   # To be populated later
                reporting_language="sv",  # Swedish
                metadata_={
                    "mfn_slug": company_data["mfn_slug"],
                    "mfn_url": f"https://mfn.se/all/a/{company_data['mfn_slug']}",
                    "data_source": "mfn.se",
                    "collection_priority": "high" if company_data["market_cap"] == "Large Cap" else "medium"
                },
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            new_companies.append(new_company)
        
        print(f"\n📥 Adding {len(new_companies)} new companies:")
        for company in new_companies:
            print(f"  ➕ {company.name} ({company.ticker}) - {company.sector}")
            db.add(company)
        
        if skipped_companies:
            print(f"\n⏭️  Skipped {len(skipped_companies)} existing companies:")
            for name in sorted(skipped_companies):
                print(f"  ✅ {name}")
        
        # Commit new companies
        if new_companies:
            await db.commit()
            print(f"\n✅ Successfully added {len(new_companies)} companies!")
        else:
            print(f"\n✅ No new companies to add!")
        
        # Show final summary with indexing recommendations
        total_result = await db.execute(
            select(NordicCompany.name, NordicCompany.market_cap_category, NordicCompany.metadata_)
            .order_by(NordicCompany.name)
        )
        all_companies = total_result.all()
        
        print(f"\n📊 FINAL COMPANY DATABASE ({len(all_companies)} companies):")
        print("-" * 50)
        
        # Group by market cap for indexing strategy
        large_cap = []
        mid_cap = []
        
        for name, market_cap, metadata in all_companies:
            mfn_slug = metadata.get('mfn_slug', 'unknown') if metadata else 'unknown'
            
            if market_cap == "Large Cap":
                large_cap.append((name, mfn_slug))
            else:
                mid_cap.append((name, mfn_slug))
        
        print(f"🏆 LARGE CAP COMPANIES ({len(large_cap)}) - Priority for indexing:")
        for name, slug in large_cap:
            print(f"  🔥 {name} (mfn: {slug})")
        
        print(f"\n📈 MID CAP COMPANIES ({len(mid_cap)}) - Secondary priority:")
        for name, slug in mid_cap:
            print(f"  📊 {name} (mfn: {slug})")
        
        # Indexing recommendations
        print(f"\n🎯 INDEXING STRATEGY RECOMMENDATIONS:")
        print(f"  📋 Chunk 1: Start with top 5 Large Cap companies")
        print(f"  📋 Chunk 2: Next 5 Large Cap companies") 
        print(f"  📋 Chunk 3: Remaining Large Cap companies")
        print(f"  📋 Chunk 4+: Mid Cap companies")
        print(f"\n⚡ Ready to start systematic indexing!")
        print(f"🔧 Next: Update test_mfn_collector.py with company chunks")

if __name__ == "__main__":
    asyncio.run(add_swedish_companies())