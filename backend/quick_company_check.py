#!/usr/bin/env python3
"""
Quick check of companies in our database and which ones have documents
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def check_companies():
    # Import here to avoid dependency issues
    try:
        from shared.database import AsyncSessionLocal
        from nordic_ingestion.models import NordicDocument, NordicCompany
        from sqlalchemy import select, func
        
        async with AsyncSessionLocal() as db:
            # Get companies with document counts
            result = await db.execute(
                select(
                    NordicCompany.name, 
                    NordicCompany.ticker,
                    func.count(NordicDocument.id).label('doc_count')
                )
                .outerjoin(NordicDocument)
                .where(NordicCompany.country == 'SE')
                .group_by(NordicCompany.name, NordicCompany.ticker)
                .order_by(func.count(NordicDocument.id).desc())
            )
            companies = result.fetchall()
            
            print(f'📊 Found {len(companies)} Swedish companies in database')
            
            # Show companies with documents
            with_docs = [c for c in companies if c.doc_count > 0]
            without_docs = [c for c in companies if c.doc_count == 0]
            
            print(f'✅ {len(with_docs)} companies have documents')
            print(f'❌ {len(without_docs)} companies have NO documents')
            
            if with_docs:
                print(f'\n📄 Companies WITH documents:')
                for i, (name, ticker, docs) in enumerate(with_docs):
                    print(f'{i+1:2}. {name} ({ticker}) - {docs} docs')
            
            # Show sample of companies without docs
            print(f'\n🔍 Sample companies WITHOUT documents (first 20):')
            for i, (name, ticker, docs) in enumerate(without_docs[:20]):
                print(f'{i+1:2}. {name} ({ticker}) - {docs} docs')
            
            return companies
            
    except ImportError as e:
        print(f"❌ Cannot import database modules: {e}")
        print("💡 Try running from the backend directory with proper Python environment")
        return []

if __name__ == "__main__":
    asyncio.run(check_companies())