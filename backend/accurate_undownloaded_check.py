#!/usr/bin/env python3
"""
Accurate Undownloaded Document Check
Reconcile database status with actual file system to get precise undownloaded count
"""
import asyncio
import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def check_accurate_undownloaded_status():
    try:
        from shared.database import AsyncSessionLocal
        from nordic_ingestion.models import NordicDocument, NordicCompany
        from sqlalchemy import select, func, and_, or_
        from datetime import date
        
        async with AsyncSessionLocal() as db:
            print(f'🔍 ACCURATE UNDOWNLOADED DOCUMENT ANALYSIS')
            print(f'='*60)
            
            # 1. Get all documents with PDF URLs that are "catalogued"
            print(f'\n📊 Catalogued Documents Analysis:')
            
            catalogued_result = await db.execute(
                select(
                    NordicDocument.id,
                    NordicDocument.title,
                    NordicDocument.document_type,
                    NordicDocument.publish_date,
                    NordicDocument.processing_status,
                    NordicDocument.storage_path,
                    NordicDocument.metadata_,
                    NordicCompany.name.label('company_name'),
                    NordicCompany.ticker
                ).join(NordicCompany).where(
                    and_(
                        NordicDocument.processing_status == "catalogued",
                        NordicDocument.metadata_.op('->>')('pdf_url').is_not(None)
                    )
                ).order_by(NordicCompany.name, NordicDocument.publish_date.desc())
            )
            
            catalogued_docs = catalogued_result.fetchall()
            print(f'📋 Total catalogued documents with PDF URLs: {len(catalogued_docs):,}')
            
            # 2. Check which ones actually exist on file system
            truly_undownloaded = []
            already_downloaded_but_not_updated = []
            base_storage = Path("data/companies/SE")
            
            print(f'\n🔍 Checking file system vs database status...')
            
            for i, doc in enumerate(catalogued_docs):
                if i % 100 == 0:
                    print(f'   Checked {i:,} / {len(catalogued_docs):,} documents...')
                
                # Generate expected file path
                company_name = doc.company_name
                year = doc.publish_date.year if doc.publish_date else 2025
                doc_type = doc.document_type or "unknown"
                
                # Clean company name for filesystem
                company_clean = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).strip()
                company_clean = company_clean.replace(' ', '_')
                first_letter = company_clean[0].upper() if company_clean else 'Z'
                
                # Check if any PDF exists in the expected company directory
                company_dir = base_storage / first_letter / company_clean
                
                if company_dir.exists():
                    # Look for any PDFs in the company directory (any year/type)
                    pdf_files = list(company_dir.rglob("*.pdf"))
                    
                    if pdf_files:
                        # PDFs exist but database shows "catalogued" - needs status update
                        already_downloaded_but_not_updated.append({
                            'id': doc.id,
                            'company_name': company_name,
                            'title': doc.title[:60] + '...',
                            'year': year,
                            'pdf_count': len(pdf_files),
                            'expected_dir': str(company_dir)
                        })
                    else:
                        # No PDFs found - truly undownloaded
                        truly_undownloaded.append({
                            'id': doc.id,
                            'company_name': company_name,
                            'title': doc.title[:60] + '...',
                            'year': year,
                            'pdf_url': doc.metadata_.get('pdf_url') if doc.metadata_ else None
                        })
                else:
                    # Company directory doesn't exist - definitely undownloaded
                    truly_undownloaded.append({
                        'id': doc.id,
                        'company_name': company_name,
                        'title': doc.title[:60] + '...',
                        'year': year,
                        'pdf_url': doc.metadata_.get('pdf_url') if doc.metadata_ else None
                    })
            
            # 3. Results summary
            print(f'\n📊 ACCURATE RESULTS:')
            print(f'✅ Already downloaded (but DB not updated): {len(already_downloaded_but_not_updated):,} documents')
            print(f'❌ Truly undownloaded: {len(truly_undownloaded):,} documents')
            print(f'📋 Total catalogued checked: {len(catalogued_docs):,} documents')
            
            # 4. Show samples
            if already_downloaded_but_not_updated:
                print(f'\n💾 Sample already downloaded (DB needs updating):')
                for i, doc in enumerate(already_downloaded_but_not_updated[:10], 1):
                    print(f'   {i:2}. {doc["company_name"]} ({doc["year"]}): {doc["pdf_count"]} PDFs in {doc["expected_dir"]}')
                if len(already_downloaded_but_not_updated) > 10:
                    print(f'   ... and {len(already_downloaded_but_not_updated) - 10} more companies with downloaded PDFs')
            
            if truly_undownloaded:
                print(f'\n📥 Sample truly undownloaded documents:')
                for i, doc in enumerate(truly_undownloaded[:10], 1):
                    print(f'   {i:2}. {doc["company_name"]} ({doc["year"]}): {doc["title"]}')
                if len(truly_undownloaded) > 10:
                    print(f'   ... and {len(truly_undownloaded) - 10} more truly undownloaded documents')
            
            # 5. Companies breakdown
            print(f'\n🏢 Company Analysis:')
            
            # Count unique companies in each category
            companies_with_downloads = set()
            companies_needing_downloads = set()
            
            for doc in already_downloaded_but_not_updated:
                companies_with_downloads.add(doc['company_name'])
                
            for doc in truly_undownloaded:
                companies_needing_downloads.add(doc['company_name'])
            
            print(f'   📁 Companies with downloaded PDFs: {len(companies_with_downloads):,}')
            print(f'   📥 Companies needing downloads: {len(companies_needing_downloads):,}')
            print(f'   🔄 Overlap (companies with both): {len(companies_with_downloads & companies_needing_downloads):,}')
            
            # 6. Recommendations
            print(f'\n💡 RECOMMENDATIONS:')
            
            if len(already_downloaded_but_not_updated) > len(truly_undownloaded):
                print(f'   🔄 Most "catalogued" documents are already downloaded!')
                print(f'   💾 Run: python3 pdf_download_batch.py --year=2025')
                print(f'      This will update DB status for existing files')
            
            if len(truly_undownloaded) > 0:
                print(f'   📥 {len(truly_undownloaded):,} documents truly need downloading')
                print(f'   🚀 Consider running without year filter for historical docs:')
                print(f'      python3 pdf_download_batch.py --all-types')
            
            if len(already_downloaded_but_not_updated) > 1000:
                print(f'   ⚡ Large number of DB updates needed - consider batch status update script')
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_accurate_undownloaded_status())