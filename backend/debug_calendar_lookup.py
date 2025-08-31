#!/usr/bin/env python3
"""
Debug calendar company lookup for abelco-investment
"""
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def main():
    # Test the MFN collector to see what company name it generates
    from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
    
    collector = MFNCollector()
    
    # Test the mapping directly
    mapped_name = collector._map_to_database_name("abelco-investment")
    print(f"🔄 MFN slug 'abelco-investment' maps to: '{mapped_name}'")
    
    # Test calendar company lookup  
    from shared.database import AsyncSessionLocal
    from nordic_ingestion.storage.calendar_storage import CalendarEventStorage
    
    storage = CalendarEventStorage()
    
    async with AsyncSessionLocal() as db:
        company = await storage._find_company_by_name(db, mapped_name)
        if company:
            print(f"✅ Calendar found company: {company.name} (ID: {company.id})")
        else:
            print(f"❌ Calendar could not find company: '{mapped_name}'")
            
            # List all companies with 'abelco' in the name
            from sqlalchemy import select, func
            from nordic_ingestion.models import NordicCompany
            
            result = await db.execute(
                select(NordicCompany.name, NordicCompany.id).where(
                    func.lower(NordicCompany.name).contains('abelco')
                )
            )
            companies = result.fetchall()
            print(f"📋 Companies in DB with 'abelco':")
            for company in companies:
                print(f"   - '{company.name}' (ID: {company.id})")

if __name__ == "__main__":
    asyncio.run(main())