#!/usr/bin/env python3
"""
Nordic Ingestion Management CLI
Production management tool for Nordic financial data ingestion
"""
import asyncio
import sys
import argparse
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.database import init_database
from nordic_ingestion.companies.sweden.sample_companies import load_sample_companies_to_database
from nordic_ingestion.orchestrator.daily_collector import run_collection_now, start_daily_scheduler, stop_daily_scheduler
from nordic_ingestion.storage.document_downloader import download_pending_documents


async def setup_database():
    """Initialize database tables"""
    print("🗄️ Setting up database...")
    try:
        await init_database()
        print("✅ Database setup complete")
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False
    return True


async def load_sample_companies():
    """Load sample Swedish companies"""
    print("🏢 Loading sample Swedish companies...")
    try:
        result = await load_sample_companies_to_database()
        print(f"✅ Added {result['companies_added']} companies with {result['sources_added']} data sources")
    except Exception as e:
        print(f"❌ Failed to load companies: {e}")
        return False
    return True


async def run_rss_collection():
    """Test RSS collection"""
    print("📡 Running RSS collection test...")
    try:
        from nordic_ingestion.collectors.rss.swedish_rss_collector import collect_swedish_rss_feeds
        results = await collect_swedish_rss_feeds()
        
        total_items = sum(results.values())
        print(f"✅ RSS collection complete: {total_items} items from {len(results)} companies")
        
        for company_id, count in results.items():
            if count > 0:
                print(f"  • Company {company_id[:8]}: {count} items")
        
    except Exception as e:
        print(f"❌ RSS collection failed: {e}")
        return False
    return True


async def run_calendar_collection():
    """Test calendar collection"""
    print("🗓️ Running calendar collection test...")
    try:
        from nordic_ingestion.collectors.calendar.swedish_calendar_collector import collect_swedish_financial_calendars
        results = await collect_swedish_financial_calendars()
        
        total_events = sum(results.values())
        print(f"✅ Calendar collection complete: {total_events} events from {len(results)} companies")
        
        for company_id, count in results.items():
            if count > 0:
                print(f"  • Company {company_id[:8]}: {count} events")
        
    except Exception as e:
        print(f"❌ Calendar collection failed: {e}")
        return False
    return True


async def run_document_downloads():
    """Test document downloads"""
    print("📥 Running document downloads test...")
    try:
        results = await download_pending_documents(limit=10)
        print(f"✅ Download complete: {results['downloaded']} downloaded, {results['failed']} failed")
    except Exception as e:
        print(f"❌ Document downloads failed: {e}")
        return False
    return True


async def run_full_collection():
    """Run complete collection workflow"""
    print("🚀 Running full collection workflow...")
    try:
        results = await run_collection_now()
        
        print("✅ Full collection complete!")
        print(f"📊 Summary:")
        print(f"  • Documents found: {results.get('total_documents_found', 0)}")
        print(f"  • Documents downloaded: {results.get('total_documents_downloaded', 0)}")
        print(f"  • Duration: {results.get('duration_seconds', 0):.1f}s")
        
        if results.get("errors"):
            print(f"  • Errors: {len(results['errors'])}")
            for error in results['errors'][:3]:
                print(f"    - {error}")
        
    except Exception as e:
        print(f"❌ Full collection failed: {e}")
        return False
    return True


async def show_status():
    """Show system status"""
    print("📊 Nordic Ingestion System Status")
    print("=" * 40)
    
    try:
        from shared.database import AsyncSessionLocal
        from sqlalchemy import text
        
        async with AsyncSessionLocal() as db:
            # Count companies
            result = await db.execute(text("SELECT COUNT(*) FROM nordic_companies"))
            company_count = result.scalar()
            
            # Count data sources
            result = await db.execute(text("SELECT COUNT(*) FROM nordic_data_sources"))
            source_count = result.scalar()
            
            # Count documents
            result = await db.execute(text("SELECT COUNT(*) FROM nordic_documents"))
            doc_count = result.scalar()
            
            # Count pending documents
            result = await db.execute(text("SELECT COUNT(*) FROM nordic_documents WHERE processing_status = 'pending'"))
            pending_count = result.scalar()
            
            # Count downloaded documents  
            result = await db.execute(text("SELECT COUNT(*) FROM nordic_documents WHERE processing_status = 'downloaded'"))
            downloaded_count = result.scalar()
            
            # Count calendar events
            result = await db.execute(text("SELECT COUNT(*) FROM nordic_calendar_events"))
            event_count = result.scalar()
            
            print(f"🏢 Companies: {company_count}")
            print(f"📡 Data Sources: {source_count}")
            print(f"📄 Total Documents: {doc_count}")
            print(f"⏳ Pending Downloads: {pending_count}")
            print(f"✅ Downloaded: {downloaded_count}")
            print(f"🗓️ Calendar Events: {event_count}")
            
            # Show recent activity
            result = await db.execute(text("""
                SELECT COUNT(*) FROM nordic_documents 
                WHERE DATE(ingestion_date) = CURRENT_DATE
            """))
            today_count = result.scalar()
            print(f"📈 Documents Today: {today_count}")
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")


def start_scheduler():
    """Start the daily scheduler"""
    print("🕐 Starting daily collection scheduler...")
    try:
        start_daily_scheduler()
        print("✅ Scheduler started successfully")
        print("Press Ctrl+C to stop...")
        
        # Keep running until interrupted
        import time
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping scheduler...")
        stop_daily_scheduler()
        print("✅ Scheduler stopped")


async def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Nordic Ingestion Management CLI")
    parser.add_argument("command", choices=[
        "setup", "load-companies", "test-rss", "test-calendar", 
        "test-downloads", "run-collection", "status", "start-scheduler"
    ], help="Command to run")
    
    args = parser.parse_args()
    
    if args.command == "setup":
        success = await setup_database()
        if success:
            print("\n🎯 Next steps:")
            print("1. Run: python scripts/manage_nordic.py load-companies")
            print("2. Test: python scripts/manage_nordic.py test-rss")
            print("3. Run:  python scripts/manage_nordic.py run-collection")
    
    elif args.command == "load-companies":
        await load_sample_companies()
    
    elif args.command == "test-rss":
        await run_rss_collection()
    
    elif args.command == "test-calendar":
        await run_calendar_collection()
    
    elif args.command == "test-downloads":
        await run_document_downloads()
    
    elif args.command == "run-collection":
        await run_full_collection()
    
    elif args.command == "status":
        await show_status()
    
    elif args.command == "start-scheduler":
        start_scheduler()


if __name__ == "__main__":
    asyncio.run(main())