#!/usr/bin/env python3
"""
Check what documents are actually catalogued in the database
This will show us what companies have documents ready for PDF download
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def check_catalogued_docs():
    try:
        from shared.database import AsyncSessionLocal
        from nordic_ingestion.models import NordicDocument, NordicCompany
        from sqlalchemy import select, func, and_, or_
        from datetime import date
        
        async with AsyncSessionLocal() as db:
            # Get documents that are catalogued (ready for PDF download)
            result = await db.execute(
                select(
                    NordicCompany.name.label('company_name'),
                    NordicCompany.ticker,
                    NordicDocument.processing_status,
                    func.count(NordicDocument.id).label('doc_count')
                ).join(NordicCompany).group_by(
                    NordicCompany.name, 
                    NordicCompany.ticker,
                    NordicDocument.processing_status
                ).order_by(NordicCompany.name)
            )
            
            all_results = result.fetchall()
            
            print(f'📊 Document Status Summary')
            print(f'='*50)
            
            # Group by status
            status_groups = {}
            for company_name, ticker, status, count in all_results:
                if status not in status_groups:
                    status_groups[status] = []
                status_groups[status].append((company_name, ticker, count))
            
            for status, companies in status_groups.items():
                total_docs = sum(count for _, _, count in companies)
                print(f'\n📋 Status: {status} ({len(companies)} companies, {total_docs} documents)')
                
                # Show first 10 companies for each status
                for i, (name, ticker, count) in enumerate(companies[:10]):
                    first_letter = name[0] if name else '?'
                    print(f'   {i+1:2}. [{first_letter}] {name} ({ticker}) - {count} docs')
                
                if len(companies) > 10:
                    print(f'   ... and {len(companies) - 10} more companies')
            
            # Check specifically for catalogued documents ready for download
            print(f'\n🔍 CATALOGUED DOCUMENTS (Ready for PDF download):')
            catalogued_result = await db.execute(
                select(
                    NordicCompany.name.label('company_name'),
                    NordicCompany.ticker,
                    func.count(NordicDocument.id).label('doc_count'),
                    func.min(NordicDocument.publish_date).label('earliest_date'),
                    func.max(NordicDocument.publish_date).label('latest_date')
                ).join(NordicCompany).where(
                    NordicDocument.processing_status == "catalogued"
                ).group_by(
                    NordicCompany.name, 
                    NordicCompany.ticker
                ).order_by(func.count(NordicDocument.id).desc())
            )
            
            catalogued_companies = catalogued_result.fetchall()
            
            if catalogued_companies:
                print(f'Found {len(catalogued_companies)} companies with catalogued documents:')
                for i, (name, ticker, count, earliest, latest) in enumerate(catalogued_companies):
                    first_letter = name[0] if name else '?'
                    print(f'{i+1:2}. [{first_letter}] {name} ({ticker}) - {count} docs ({earliest} to {latest})')
            else:
                print('❌ No catalogued documents found!')
                
            # Check for documents with PDF URLs
            print(f'\n📄 Documents with PDF URLs:')
            pdf_url_result = await db.execute(
                select(
                    NordicCompany.name.label('company_name'),
                    NordicCompany.ticker,
                    func.count(NordicDocument.id).label('doc_count')
                ).join(NordicCompany).where(
                    and_(
                        NordicDocument.processing_status == "catalogued",
                        NordicDocument.metadata_.op('->>')('pdf_url') != None
                    )
                ).group_by(
                    NordicCompany.name, 
                    NordicCompany.ticker
                ).order_by(NordicCompany.name)
            )
            
            pdf_companies = pdf_url_result.fetchall()
            
            if pdf_companies:
                print(f'Found {len(pdf_companies)} companies with PDF URLs:')
                for i, (name, ticker, count) in enumerate(pdf_companies):
                    first_letter = name[0] if name else '?'
                    print(f'{i+1:2}. [{first_letter}] {name} ({ticker}) - {count} docs with PDF URLs')
            else:
                print('❌ No documents with PDF URLs found!')
                
    except ImportError as e:
        print(f"❌ Cannot import database modules: {e}")
        print("💡 This script needs to run in the proper Python environment")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_catalogued_docs())