"""
Nordic Ingestion Dashboard
Comprehensive tracking of what we have indexed and downloaded
"""
import asyncio
import sys
import os
from collections import defaultdict
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany, NordicCalendarEvent
from sqlalchemy import select, func

async def nordic_dashboard():
    print("🇸🇪 NORDIC INGESTION DASHBOARD")
    print("=" * 80)
    
    async with AsyncSessionLocal() as db:
        # 1. COMPANIES OVERVIEW
        print("\n📊 1. COMPANIES OVERVIEW")
        print("-" * 40)
        
        companies_result = await db.execute(
            select(NordicCompany.name, NordicCompany.country, NordicCompany.ticker)
            .order_by(NordicCompany.name)
        )
        companies = companies_result.all()
        
        country_counts = defaultdict(int)
        for name, country, ticker in companies:
            country_counts[country or "Unknown"] += 1
            print(f"  🏢 {name} ({ticker}) - {country or 'Unknown'}")
        
        print(f"\n📈 SUMMARY:")
        for country, count in sorted(country_counts.items()):
            flag = "🇸🇪" if country == "SE" else "🌍"
            print(f"  {flag} {country}: {count} companies")
        print(f"  📊 Total: {sum(country_counts.values())} companies")
        
        # 2. INDEXING STATUS (CATALOGUED DOCUMENTS)
        print(f"\n📚 2. INDEXING STATUS (CATALOGUED DOCUMENTS)")
        print("-" * 50)
        
        # Get catalogued documents by company and type
        catalogued_result = await db.execute(
            select(
                NordicCompany.name,
                NordicDocument.document_type,
                func.count(NordicDocument.id).label('count')
            )
            .join(NordicCompany, NordicDocument.company_id == NordicCompany.id)
            .where(NordicDocument.processing_status.in_(["catalogued", "pending", "downloaded"]))
            .group_by(NordicCompany.name, NordicDocument.document_type)
            .order_by(NordicCompany.name, NordicDocument.document_type)
        )
        catalogued_docs = catalogued_result.all()
        
        # Organize by company
        company_index = defaultdict(lambda: defaultdict(int))
        total_indexed = 0
        
        for company_name, doc_type, count in catalogued_docs:
            company_index[company_name][doc_type] = count
            total_indexed += count
        
        for company_name, doc_types in sorted(company_index.items()):
            company_total = sum(doc_types.values())
            print(f"  🏢 {company_name} ({company_total} documents)")
            
            for doc_type, count in sorted(doc_types.items()):
                emoji = "📊" if "quarterly" in doc_type else "📰" if "press" in doc_type else "📄"
                print(f"    {emoji} {doc_type}: {count}")
        
        print(f"\n📈 INDEXING SUMMARY:")
        print(f"  📚 Total indexed: {total_indexed} documents")
        print(f"  🏢 Companies with documents: {len(company_index)}")
        
        # 3. DOWNLOAD STATUS
        print(f"\n📥 3. DOWNLOAD STATUS")
        print("-" * 30)
        
        # Get status breakdown
        status_result = await db.execute(
            select(
                NordicDocument.processing_status,
                func.count(NordicDocument.id).label('count')
            )
            .group_by(NordicDocument.processing_status)
        )
        status_counts = dict(status_result.all())
        
        # Get downloads by company
        downloaded_result = await db.execute(
            select(
                NordicCompany.name,
                NordicDocument.document_type,
                func.count(NordicDocument.id).label('count'),
                func.sum(NordicDocument.file_size_mb).label('total_size_mb')
            )
            .join(NordicCompany, NordicDocument.company_id == NordicCompany.id)
            .where(NordicDocument.processing_status == "downloaded")
            .group_by(NordicCompany.name, NordicDocument.document_type)
            .order_by(NordicCompany.name, NordicDocument.document_type)
        )
        downloaded_docs = downloaded_result.all()
        
        # Status overview
        print(f"  📋 Catalogued (ready to download): {status_counts.get('catalogued', 0)}")
        print(f"  ⏳ Pending download: {status_counts.get('pending', 0)}")
        print(f"  ✅ Downloaded: {status_counts.get('downloaded', 0)}")
        print(f"  ❌ Failed downloads: {status_counts.get('failed', 0)}")
        
        # Downloaded breakdown by company
        if downloaded_docs:
            print(f"\n📥 DOWNLOADED BY COMPANY:")
            company_downloads = defaultdict(lambda: {'count': 0, 'size_mb': 0, 'types': defaultdict(int)})
            
            for company_name, doc_type, count, size_mb in downloaded_docs:
                company_downloads[company_name]['count'] += count
                company_downloads[company_name]['size_mb'] += size_mb or 0
                company_downloads[company_name]['types'][doc_type] = count
            
            for company_name, stats in sorted(company_downloads.items()):
                print(f"  🏢 {company_name}: {stats['count']} files ({stats['size_mb']:.1f}MB)")
                for doc_type, count in sorted(stats['types'].items()):
                    emoji = "📊" if "quarterly" in doc_type else "📰" if "press" in doc_type else "📄"
                    print(f"    {emoji} {doc_type}: {count}")
        
        # 4. CALENDAR EVENTS
        print(f"\n📅 4. CALENDAR EVENTS")
        print("-" * 25)
        
        calendar_result = await db.execute(
            select(
                NordicCompany.name,
                NordicCalendarEvent.event_type,
                func.count(NordicCalendarEvent.id).label('count')
            )
            .join(NordicCompany, NordicCalendarEvent.company_id == NordicCompany.id)
            .group_by(NordicCompany.name, NordicCalendarEvent.event_type)
            .order_by(NordicCompany.name, NordicCalendarEvent.event_type)
        )
        calendar_events = calendar_result.all()
        
        if calendar_events:
            company_events = defaultdict(lambda: defaultdict(int))
            total_events = 0
            
            for company_name, event_type, count in calendar_events:
                company_events[company_name][event_type] = count
                total_events += count
            
            for company_name, event_types in sorted(company_events.items()):
                company_total = sum(event_types.values())
                print(f"  🏢 {company_name} ({company_total} events)")
                
                for event_type, count in sorted(event_types.items()):
                    emoji = "📊" if event_type == "earnings" else "💰" if event_type == "dividend" else "📅"
                    print(f"    {emoji} {event_type}: {count}")
            
            print(f"\n📈 CALENDAR SUMMARY: {total_events} events tracked")
        else:
            print(f"  📅 No calendar events found")
        
        # 5. STORAGE ANALYSIS
        print(f"\n💾 5. STORAGE ANALYSIS")
        print("-" * 25)
        
        # Analyze storage paths to check for the path fix
        paths_result = await db.execute(
            select(
                NordicDocument.storage_path,
                NordicDocument.metadata_
            )
            .where(NordicDocument.processing_status == "downloaded")
        )
        storage_paths = paths_result.all()
        
        if storage_paths:
            path_analysis = defaultdict(list)
            sandvik_in_volvo = 0
            total_files = len(storage_paths)
            
            for storage_path, metadata in storage_paths:
                if storage_path:
                    # Extract company from path
                    path_parts = storage_path.split('/')
                    if len(path_parts) >= 6:  # data/documents/companies/SE/X/company/...
                        company_in_path = path_parts[5]
                        
                        # Check if it's a Sandvik document in Volvo folder (bug)
                        mfn_source = metadata.get('mfn_source', '') if metadata else ''
                        if 'sandvik' in mfn_source.lower() and company_in_path == 'volvo':
                            sandvik_in_volvo += 1
                        
                        path_analysis[company_in_path].append(storage_path)
            
            print(f"  📂 Files stored by company folder:")
            for company_folder, paths in sorted(path_analysis.items()):
                print(f"    📁 {company_folder}/: {len(paths)} files")
            
            if sandvik_in_volvo > 0:
                print(f"\n⚠️  STORAGE ISSUE DETECTED:")
                print(f"    🐛 {sandvik_in_volvo} Sandvik documents in Volvo folder")
                print(f"    ✅ Fix applied - new downloads will use correct paths")
            else:
                print(f"\n✅ Storage paths look correct!")
                
            total_size_result = await db.execute(
                select(func.sum(NordicDocument.file_size_mb))
                .where(NordicDocument.processing_status == "downloaded")
            )
            total_size = total_size_result.scalar() or 0
            
            print(f"  📊 Total storage: {total_size:.1f}MB across {total_files} files")
        else:
            print(f"  📂 No downloaded files to analyze")
        
        # 6. RECOMMENDATIONS
        print(f"\n🎯 6. RECOMMENDATIONS")
        print("-" * 25)
        
        catalogued_count = status_counts.get('catalogued', 0)
        downloaded_count = status_counts.get('downloaded', 0)
        companies_with_docs = len(company_index)
        total_companies = len(companies)
        
        if catalogued_count > 0:
            print(f"  📥 Ready to download: {catalogued_count} documents waiting")
            print(f"  🚀 Run downloader: python3 test_download_documents.py")
        
        if companies_with_docs < total_companies:
            missing_companies = total_companies - companies_with_docs
            print(f"  📊 {missing_companies} companies have no documents yet")
            print(f"  🔍 Run collector for more companies")
        
        if downloaded_count > 0:
            print(f"  ✅ Ready for analysis: {downloaded_count} PDFs available")
            print(f"  🧠 Can feed to MVP1 analysis system")
        
        # Overall system health
        coverage_pct = (companies_with_docs / total_companies * 100) if total_companies > 0 else 0
        download_rate = (downloaded_count / total_indexed * 100) if total_indexed > 0 else 0
        
        print(f"\n📈 SYSTEM HEALTH:")
        print(f"  🏢 Company coverage: {coverage_pct:.1f}% ({companies_with_docs}/{total_companies})")
        print(f"  📥 Download completion: {download_rate:.1f}% ({downloaded_count}/{total_indexed})")
        
        if coverage_pct >= 80 and download_rate >= 80:
            print(f"  🎉 System status: EXCELLENT")
        elif coverage_pct >= 60 and download_rate >= 60:
            print(f"  ✅ System status: GOOD")
        elif coverage_pct >= 40 or download_rate >= 40:
            print(f"  ⚡ System status: GROWING")
        else:
            print(f"  🔄 System status: EARLY STAGE")

if __name__ == "__main__":
    asyncio.run(nordic_dashboard())