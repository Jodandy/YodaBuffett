#!/usr/bin/env python3
"""
Check for duplicate company names in the database
"""
import asyncio
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany

async def find_duplicate_companies():
    """Find companies with duplicate names"""
    
    print("🔍 Checking for duplicate company names in database...")
    
    async with AsyncSessionLocal() as db:
        # Find companies with the same name (case-insensitive)
        subquery = select(
            func.lower(NordicCompany.name).label('name_lower'),
            func.count(NordicCompany.id).label('count')
        ).group_by(
            func.lower(NordicCompany.name)
        ).having(
            func.count(NordicCompany.id) > 1
        ).subquery()
        
        # Get full details of duplicates
        query = select(
            NordicCompany.id,
            NordicCompany.name,
            NordicCompany.ticker,
            NordicCompany.exchange,
            NordicCompany.country,
            NordicCompany.created_at
        ).where(
            func.lower(NordicCompany.name).in_(
                select(subquery.c.name_lower)
            )
        ).order_by(
            func.lower(NordicCompany.name),
            NordicCompany.id
        )
        
        result = await db.execute(query)
        duplicates = result.fetchall()
        
        if not duplicates:
            print("✅ No duplicate company names found!")
            return
        
        # Group by company name
        duplicate_groups = {}
        for dup in duplicates:
            name_lower = dup.name.lower()
            if name_lower not in duplicate_groups:
                duplicate_groups[name_lower] = []
            duplicate_groups[name_lower].append(dup)
        
        print(f"\n❌ Found {len(duplicate_groups)} companies with duplicates:")
        print("=" * 80)
        
        for name_lower, companies in duplicate_groups.items():
            print(f"\n🏢 '{companies[0].name}' has {len(companies)} entries:")
            for i, company in enumerate(companies, 1):
                print(f"   {i}. ID: {company.id}")
                print(f"      Name: {company.name}")
                print(f"      Ticker: {company.ticker}")
                print(f"      Exchange: {company.exchange}")
                print(f"      Country: {company.country}")
                print(f"      Created: {company.created_at}")
        
        print("\n" + "=" * 80)
        print("⚠️  RECOMMENDED ACTION:")
        print("1. Review each duplicate group")
        print("2. Decide which entry to keep (usually the one with more data)")
        print("3. Delete the duplicate entries")
        print("4. Consider adding a unique constraint on company name")
        
        # Also check for exact query that's failing
        print("\n🔍 Checking 'Better Collective' specifically...")
        query = select(NordicCompany).where(
            func.lower(NordicCompany.name) == 'better collective'
        )
        result = await db.execute(query)
        better_collective = result.fetchall()
        
        if len(better_collective) > 1:
            print(f"\n⚠️  'Better Collective' has {len(better_collective)} entries:")
            for i, (company,) in enumerate(better_collective, 1):
                print(f"   {i}. ID: {company.id}, Ticker: {company.ticker}, Exchange: {company.exchange}")

async def main():
    await find_duplicate_companies()

if __name__ == "__main__":
    asyncio.run(main())