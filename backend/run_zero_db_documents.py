#!/usr/bin/env python3
"""
Run Historical Ingestion for Companies with 0 Documents OR 0 Events
Queries the database to find companies missing documents and/or calendar events
This will help fix calendar mapping issues after the recent fixes
"""
import asyncio
from datetime import datetime
import sys
import os
import subprocess

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany, NordicDocument, NordicCalendarEvent
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

async def get_companies_with_zero_documents_or_events():
    """Query database for companies with zero documents OR zero events - PURE DATABASE QUERY"""
    
    print("🔍 Querying database directly for companies with zero documents OR zero events...")
    print("   (Ignoring any ingestion run results)")
    
    async with AsyncSessionLocal() as db:
        # Step 1: Get companies with zero documents OR zero events
        subquery = select(
            NordicCompany.id,
            func.count(NordicDocument.id.distinct()).label('doc_count'),
            func.count(NordicCalendarEvent.id.distinct()).label('event_count')
        ).select_from(
            NordicCompany
        ).outerjoin(
            NordicDocument, 
            NordicCompany.id == NordicDocument.company_id
        ).outerjoin(
            NordicCalendarEvent,
            NordicCompany.id == NordicCalendarEvent.company_id
        ).group_by(
            NordicCompany.id
        ).having(
            # Companies with 0 documents OR 0 events (or both)
            (func.count(NordicDocument.id.distinct()) == 0) | 
            (func.count(NordicCalendarEvent.id.distinct()) == 0)
        ).subquery()
        
        # Step 2: Get full company details for those matching criteria
        query = select(
            NordicCompany.id,
            NordicCompany.name,
            NordicCompany.ticker,
            NordicCompany.metadata_,
            subquery.c.doc_count,
            subquery.c.event_count
        ).select_from(
            NordicCompany
        ).join(
            subquery, 
            NordicCompany.id == subquery.c.id
        ).order_by(
            NordicCompany.name
        )
        
        result = await db.execute(query)
        companies = result.fetchall()
        
        # Extract slugs from metadata or generate them
        target_companies = []
        zero_docs_count = 0
        zero_events_count = 0
        
        for company in companies:
            doc_count = company.doc_count
            event_count = company.event_count
            
            if doc_count == 0:
                zero_docs_count += 1
            if event_count == 0:
                zero_events_count += 1
                
            # Try to get slug from metadata
            slug = None
            if company.metadata_ and isinstance(company.metadata_, dict):
                slug = company.metadata_.get('slug')
            
            # If no slug, try to generate from name
            if not slug:
                slug = company.name.lower()
                slug = slug.replace(' ', '-').replace('&', 'and').replace('å', 'a').replace('ä', 'a').replace('ö', 'o')
                slug = ''.join(c for c in slug if c.isalnum() or c == '-')
                slug = slug.strip('-')
            
            reason = []
            if doc_count == 0:
                reason.append("0 documents")
            if event_count == 0:
                reason.append("0 events")
            
            target_companies.append({
                'id': company.id,
                'name': company.name,
                'ticker': company.ticker,
                'slug': slug,
                'doc_count': doc_count,
                'event_count': event_count,
                'reason': " AND ".join(reason)
            })
        
        print(f"📊 Found {len(target_companies)} companies to reprocess:")
        print(f"   📄 {zero_docs_count} companies with 0 documents")
        print(f"   📅 {zero_events_count} companies with 0 events")
        print(f"   🔄 Total unique companies: {len(target_companies)}")
        
        return target_companies

async def get_all_companies_doc_stats():
    """Get document count stats for all companies"""
    
    async with AsyncSessionLocal() as db:
        query = select(
            func.count(NordicDocument.id).label('doc_count')
        ).select_from(
            NordicCompany
        ).outerjoin(
            NordicDocument, 
            NordicCompany.id == NordicDocument.company_id
        ).group_by(
            NordicCompany.id
        )
        
        result = await db.execute(query)
        doc_counts = [row.doc_count for row in result.fetchall()]
        
        total_companies = len(doc_counts)
        zero_doc_count = sum(1 for count in doc_counts if count == 0)
        has_docs_count = total_companies - zero_doc_count
        
        return {
            "total_companies": total_companies,
            "zero_documents": zero_doc_count,
            "has_documents": has_docs_count,
            "max_documents": max(doc_counts) if doc_counts else 0,
            "avg_documents": sum(doc_counts) / len(doc_counts) if doc_counts else 0
        }

def main():
    """Main execution"""
    print("🎯 Historical Ingestion for Companies with 0 Documents OR 0 Events")
    print("=" * 70)
    
    async def run():
        # Get overall stats
        stats = await get_all_companies_doc_stats()
        print(f"\n📈 Database Document Statistics:")
        print(f"   • Total companies: {stats['total_companies']}")
        print(f"   • Companies with documents: {stats['has_documents']}")
        print(f"   • Companies with ZERO documents: {stats['zero_documents']}")
        print(f"   • Average documents per company: {stats['avg_documents']:.1f}")
        print(f"   • Max documents (single company): {stats['max_documents']}")
        
        # Get companies with zero documents OR zero events
        target_companies = await get_companies_with_zero_documents_or_events()
        
        if not target_companies:
            print("\n✅ All companies have both documents AND events in the database!")
            return
        
        # Show sample companies
        print(f"\n📋 Companies to reprocess (0 documents OR 0 events):")
        for i, company in enumerate(target_companies[:15]):
            print(f"   • {company['name']} ({company['ticker']}) → {company['reason']} → slug: {company['slug']}")
        
        if len(target_companies) > 15:
            print(f"   ... and {len(target_companies) - 15} more")
        
        # Extract slugs for ingestion
        company_slugs = [company['slug'] for company in target_companies if company['slug']]
        
        # Create company list file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        company_file = f"zero_docs_or_events_{timestamp}.txt"
        
        with open(company_file, 'w') as f:
            for slug in company_slugs:
                f.write(f"{slug}\n")
        
        print(f"\n📝 Created company list: {company_file}")
        
        # Calculate estimated time
        estimated_hours = (len(company_slugs) * 25) / 3600  # ~25s per company
        print(f"⏱️  Estimated time: {estimated_hours:.1f} hours")
        
        print(f"\n🚀 Starting historical ingestion in 5 seconds...")
        print(f"   Note: Using centralized company mappings")
        print(f"   Note: Conservative delays (3s within, 15s between companies)")
        print(f"   (Press Ctrl+C to cancel)")
        
        try:
            import time
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n❌ Cancelled by user")
            os.remove(company_file)
            return
        
        # Run historical ingestion
        print("\n" + "=" * 65)
        
        cmd = [
            'python3', 
            'historical_ingestion_batch.py',
            '--companies', company_file
        ]
        
        try:
            result = subprocess.run(cmd, check=False)
            if result.returncode == 0:
                print("\n✅ Historical ingestion completed!")
            else:
                print(f"\n⚠️ Historical ingestion exited with code: {result.returncode}")
        except KeyboardInterrupt:
            print("\n⚠️ Interrupted by user")
        finally:
            # Clean up temporary file
            try:
                os.remove(company_file)
                print(f"🗑️ Cleaned up: {company_file}")
            except:
                pass
    
    asyncio.run(run())

if __name__ == "__main__":
    main()