#!/usr/bin/env python3
"""Reset documents stuck in extraction 'processing' state"""

import asyncio
import asyncpg
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def reset_extraction_processing(hours_old: int = 1):
    """Reset documents stuck in extraction 'processing' state back to 'pending'"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # Check documents stuck in extraction processing
        stuck_docs = await conn.fetch("""
            SELECT COUNT(*) as count, 
                   MIN(updated_at) as oldest,
                   MAX(updated_at) as newest
            FROM nordic_documents 
            WHERE processing_status = 'downloaded'
            AND extraction_status = 'processing'
        """)
        
        if stuck_docs[0]['count'] == 0:
            print("✅ No documents stuck in extraction processing state")
            return
        
        print(f"📊 Found {stuck_docs[0]['count']:,} documents stuck in extraction 'processing' state")
        print(f"   Oldest: {stuck_docs[0]['oldest']}")
        print(f"   Newest: {stuck_docs[0]['newest']}")
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(hours=hours_old)
        
        # Count how many we'll reset
        to_reset = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM nordic_documents 
            WHERE processing_status = 'downloaded'
            AND extraction_status = 'processing'
            AND updated_at < $1
        """, cutoff_time)
        
        print(f"\n🔄 Will reset {to_reset:,} extraction tasks older than {hours_old} hours")
        
        # Also show failed extractions
        failed_count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM nordic_documents
            WHERE extraction_status = 'failed_extraction'
        """)
        
        if failed_count > 0:
            print(f"📝 Note: There are also {failed_count:,} failed extractions")
            print(f"   Run with --include-failed to reset those too")
        
        if input("\nProceed with reset? (y/n): ").lower() != 'y':
            print("❌ Cancelled")
            return
        
        # Reset stuck documents to 'pending'
        result = await conn.execute("""
            UPDATE nordic_documents
            SET extraction_status = 'pending',
                extraction_attempts = 0,
                updated_at = NOW()
            WHERE processing_status = 'downloaded'
            AND extraction_status = 'processing'
            AND updated_at < $1
        """, cutoff_time)
        
        count = int(result.split()[-1])
        print(f"\n✅ Reset {count:,} documents from extraction 'processing' to 'pending'")
        
        # Handle failed extractions if requested
        if '--include-failed' in sys.argv:
            failed_result = await conn.execute("""
                UPDATE nordic_documents
                SET extraction_status = 'pending',
                    extraction_attempts = 0,
                    updated_at = NOW()
                WHERE extraction_status = 'failed_extraction'
            """)
            
            failed_count = int(failed_result.split()[-1])
            print(f"✅ Reset {failed_count:,} failed extractions to 'pending'")
        
        # Show new status
        status = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE extraction_status = 'pending') as pending,
                COUNT(*) FILTER (WHERE extraction_status = 'processing') as processing,
                COUNT(*) FILTER (WHERE extraction_status = 'completed') as completed,
                COUNT(*) FILTER (WHERE extraction_status = 'failed_extraction') as failed
            FROM nordic_documents
            WHERE processing_status = 'downloaded'
        """)
        
        print(f"\n📊 New Extraction Status:")
        print(f"   ⏳ Pending: {status['pending']:,}")
        print(f"   ⚙️  Processing: {status['processing']:,}")
        print(f"   ✅ Completed: {status['completed']:,}")
        print(f"   ❌ Failed: {status['failed']:,}")
        
        if status['pending'] > 0:
            print(f"\n🚀 Ready to extract {status['pending']:,} documents!")
            print(f"   Run: python domains/document_intelligence/cli_nordic_extraction.py extract 100000")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    hours = 1
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        hours = int(sys.argv[1])
    
    print(f"🔧 Resetting documents stuck in extraction 'processing' for more than {hours} hour(s)")
    print(f"   Options: --include-failed to also reset failed extractions")
    asyncio.run(reset_extraction_processing(hours))