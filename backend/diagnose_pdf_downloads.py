#!/usr/bin/env python3
"""
Diagnose PDF Download Issues
Check what's actually in the database and why PDF downloads are limited
"""
import asyncio
import sys
import os
from datetime import datetime
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def diagnose_database():
    try:
        from shared.database import AsyncSessionLocal
        from nordic_ingestion.models import NordicDocument, NordicCompany
        from sqlalchemy import select, func, and_, or_, text
        from datetime import date
        
        async with AsyncSessionLocal() as db:
            print("🔍 DIAGNOSING PDF DOWNLOAD DATABASE STATE")
            print("="*60)
            
            # 1. Total documents
            total_result = await db.execute(select(func.count(NordicDocument.id)))
            total_docs = total_result.scalar()
            print(f"📊 Total documents in database: {total_docs:,}")
            
            # 2. Documents by status
            print(f"\n📋 Documents by processing status:")
            status_result = await db.execute(
                select(
                    NordicDocument.processing_status,
                    func.count(NordicDocument.id).label('count')
                ).group_by(NordicDocument.processing_status)
                .order_by(func.count(NordicDocument.id).desc())
            )
            
            for status, count in status_result.fetchall():
                print(f"   {status}: {count:,} documents")
            
            # 3. Documents by year
            print(f"\n📅 Documents by year (from publish_date):")
            year_result = await db.execute(
                select(
                    func.extract('year', NordicDocument.publish_date).label('year'),
                    func.count(NordicDocument.id).label('count')
                ).where(NordicDocument.publish_date.is_not(None))
                .group_by(func.extract('year', NordicDocument.publish_date))
                .order_by(func.extract('year', NordicDocument.publish_date).desc())
            )
            
            for year, count in year_result.fetchall():
                year_int = int(year) if year else None
                print(f"   {year_int}: {count:,} documents")
            
            # 4. Documents with PDF URLs
            print(f"\n🔗 Documents with PDF URLs:")
            pdf_url_result = await db.execute(
                select(func.count(NordicDocument.id))
                .where(NordicDocument.metadata_.op('->>')('pdf_url').is_not(None))
            )
            pdf_url_count = pdf_url_result.scalar()
            print(f"   Documents with PDF URLs: {pdf_url_count:,}")
            
            # 5. What would PDF download batch find? (2025 filter)
            print(f"\n🎯 What PDF download batch would find (2025 filter):")
            current_year = datetime.now().year
            
            pdf_batch_result = await db.execute(
                select(func.count(NordicDocument.id))
                .join(NordicCompany)
                .where(
                    and_(
                        NordicDocument.processing_status == "catalogued",
                        or_(
                            NordicDocument.publish_date.between(
                                date(current_year, 1, 1),
                                date(current_year, 12, 31)
                            ),
                            NordicDocument.title.contains(str(current_year))
                        ),
                        NordicDocument.metadata_.op('->>')('pdf_url').is_not(None)
                    )
                )
            )
            
            pdf_batch_count = pdf_batch_result.scalar()
            print(f"   Catalogued, {current_year}, with PDF URL: {pdf_batch_count:,} documents")
            
            # 6. What if we remove year filter?
            print(f"\n🌐 What if we remove year filter:")
            no_year_result = await db.execute(
                select(func.count(NordicDocument.id))
                .join(NordicCompany)
                .where(
                    and_(
                        NordicDocument.processing_status == "catalogued",
                        NordicDocument.metadata_.op('->>')('pdf_url').is_not(None)
                    )
                )
            )
            
            no_year_count = no_year_result.scalar()
            print(f"   Catalogued (any year) with PDF URL: {no_year_count:,} documents")
            
            # 7. Companies with documents by first letter
            print(f"\n🔤 Companies with documents by first letter:")
            letter_result = await db.execute(
                select(
                    func.substr(NordicCompany.name, 1, 1).label('first_letter'),
                    func.count(func.distinct(NordicCompany.id)).label('company_count'),
                    func.count(NordicDocument.id).label('doc_count')
                ).join(NordicDocument)
                .group_by(func.substr(NordicCompany.name, 1, 1))
                .order_by(func.substr(NordicCompany.name, 1, 1))
            )
            
            for letter, companies, docs in letter_result.fetchall():
                print(f"   {letter}: {companies} companies, {docs:,} documents")
            
            # 8. Sample companies with catalogued documents
            print(f"\n👥 Sample companies with catalogued documents:")
            sample_result = await db.execute(
                select(
                    NordicCompany.name,
                    NordicCompany.ticker,
                    func.count(NordicDocument.id).label('doc_count')
                ).join(NordicDocument)
                .where(NordicDocument.processing_status == "catalogued")
                .group_by(NordicCompany.name, NordicCompany.ticker)
                .order_by(func.count(NordicDocument.id).desc())
                .limit(20)
            )
            
            for name, ticker, count in sample_result.fetchall():
                first_letter = name[0] if name else '?'
                print(f"   [{first_letter}] {name} ({ticker}): {count} catalogued docs")
            
            # 9. Recommendations
            print(f"\n💡 RECOMMENDATIONS:")
            if pdf_batch_count < 1000:
                print(f"   🎯 Try removing year filter: --year=None or don't specify year")
            if no_year_count > pdf_batch_count * 5:
                print(f"   📅 Most documents are from previous years, not 2025")
            
            print(f"\n🚀 Try running: python3 pdf_download_batch.py --year=2024")
            print(f"🚀 Or try: python3 pdf_download_batch.py (without year filter)")
            
    except ImportError as e:
        print(f"❌ Cannot import database modules: {e}")
        print("💡 Run this in proper Python environment with SQLAlchemy installed")
    except Exception as e:
        print(f"❌ Database error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(diagnose_database())