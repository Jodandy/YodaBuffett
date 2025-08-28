"""
Test script for MFN.se collector
Start small and respectful - test with just 2 companies first
"""
import asyncio
import aiohttp
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents, get_catalog_summary
from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events

async def test_mfn_collector():
    """Test MFN collector with just 2 companies first"""
    
    print("🧪 Testing MFN.se collector...")
    print("📋 Starting with top 10 OMXS30 companies")
    print("⏱️  Using 5-second rate limiting (respectful but efficient)")
    print("📅 Will test 5-year historical limit for comprehensive data")
    
    # Create collector with reasonable rate limiting (2 seconds)
    collector = MFNCollector(rate_limit_delay=5.0)
    
    # Test with top 10 OMXS30 companies for index building (with correct URLs)
    # collector.swedish_companies = [
    #     "volvo", "astrazeneca", "atlas-copco", "ericsson", "handm", 
    #     "sandvik", "nordea", "investor", "abb", "hexagon"
    # ]
    collector.swedish_companies = [
        "sandvik", 'ericsson'
    ]
    
    try:
        # Choose collection mode
        mode = "index_building"  # Just catalog PDFs, don't download yet
        
        if mode == "index_building":
            print("\n📚 Starting INDEX BUILDING (catalog all PDFs, don't download)")
            print("🏢 Companies: 10 top OMXS30 companies")
            print("📄 Limit: 50 items per company (2-3 years of comprehensive data)")
            print("⏱️  Estimated time: ~20 seconds with 2s rate limiting")
            print("🎯 Goal: Build searchable catalog of ~5,000+ financial documents")
        elif mode == "historical":
            print("\n📡 Starting HISTORICAL collection (5 years, ~150 items per company)...")
            print("⚠️  This will take ~20 seconds with rate limiting")
        else:
            print("\n📡 Starting RECENT collection (last 10 items per company)...")
            print("⚡ This will take ~20 seconds with rate limiting")
        
        # Test collection with specified companies and limits
        all_news = []
        async with aiohttp.ClientSession(
            headers=collector.session_headers,
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            for company in collector.swedish_companies:
                print(f"📡 Collecting {company}...")
                # Set limit based on mode
                if mode == "index_building":
                    limit = 5  # Small test to match debug
                elif mode == "historical":
                    limit = 150  # 5 years full historical
                else:
                    limit = 10   # Recent items only
                    
                items = await collector.collect_company_news(session, company, limit=limit)
                all_news.extend(items)
                if company != collector.swedish_companies[-1]:  # Don't wait after last
                    print(f"⏱️  Waiting {collector.rate_limit_delay}s...")
                    await asyncio.sleep(collector.rate_limit_delay)
        
        print(f"\n✅ Collection complete!")
        print(f"📊 Found {len(all_news)} news items total")
        
        # Show INDEX BUILDING results
        if mode == "index_building":
            print("\n📊 INDEX BUILDING SUMMARY:")
            company_stats = {}
            total_pdfs = 0
            
            for item in all_news:
                company = item.company_name
                pdf_count = len(item.pdf_urls)
                
                if company not in company_stats:
                    company_stats[company] = {"items": 0, "pdfs": 0}
                
                company_stats[company]["items"] += 1
                company_stats[company]["pdfs"] += pdf_count
                total_pdfs += pdf_count
            
            print(f"\n📚 CATALOG BUILT:")
            print(f"   🏢 Companies: {len(company_stats)}")
            print(f"   📄 Total PDFs cataloged: {total_pdfs}")
            print(f"   📊 Average PDFs per company: {total_pdfs // len(company_stats) if company_stats else 0}")
            
            print(f"\n🏢 PER-COMPANY BREAKDOWN:")
            for company, stats in company_stats.items():
                print(f"   {company}: {stats['items']} items, {stats['pdfs']} PDFs")
                
            print(f"\n💾 STORING DATA IN DATABASE...")
            print(f"   Items to store: {len(all_news)}")
            print(f"   Sample item PDFs: {len(all_news[0].pdf_urls) if all_news else 0}")
            
            # Store catalogued documents in database  
            storage_stats = await catalog_mfn_documents(all_news)
            
            print(f"\n📊 DOCUMENT STORAGE RESULTS:")
            print(f"   ✅ Stored: {storage_stats['stored']} documents")
            print(f"   🔄 Duplicates skipped: {storage_stats['duplicates']}")
            print(f"   ❌ Errors: {storage_stats['errors']}")
            
            # Store calendar events in database (SPLIT STORAGE!)
            print(f"\n📅 STORING CALENDAR EVENTS...")
            calendar_stats = await store_mfn_calendar_events(all_news)
            
            print(f"\n📅 CALENDAR STORAGE RESULTS:")
            print(f"   📅 Calendar events created: {calendar_stats['calendar_events_created']}")
            print(f"   💰 Dividend events: {calendar_stats['dividend_events']}")
            print(f"   ❌ Calendar errors: {calendar_stats['errors']}")
            
            # Get catalog summary
            catalog_summary = await get_catalog_summary()
            print(f"\n📚 CATALOG SUMMARY (Total Database):")
            print(f"   📋 Catalogued (not downloaded): {catalog_summary['catalogued']}")
            print(f"   ⏳ Pending download: {catalog_summary['pending_download']}")
            print(f"   ✅ Downloaded: {catalog_summary['downloaded']}")
            print(f"   ❌ Failed downloads: {catalog_summary['failed']}")
            print(f"   📊 Total discovered: {catalog_summary['total_discovered']}")
            
            # Summary of calendar extraction (removed detailed output to reduce clutter)
            calendar_items = sum(1 for item in all_news if item.calendar_info)
            if calendar_items == 0:
                print(f"\n📅 No calendar information extracted")
            else:
                print(f"\n📅 ✅ Calendar info extracted from {calendar_items}/{len(all_news)} items")
            
            print(f"\n✅ INDEX BUILDING COMPLETE!")
            print(f"🔍 You can now search for specific documents without downloading")
            print(f"📥 Download only what you need for analysis")
            print(f"📅 Calendar information extracted and stored in database")
            
        else:
            # Show detailed results for other modes
            for item in all_news:
                print(f"\n🏢 {item.company_name}")
                print(f"📰 {item.title}")
                print(f"📅 {item.date_published}")
                print(f"🔗 {len(item.pdf_urls)} PDF links")
                if item.pdf_urls:
                    for pdf in item.pdf_urls[:2]:  # Show first 2 PDFs
                        print(f"   📄 {pdf}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_mfn_collector())