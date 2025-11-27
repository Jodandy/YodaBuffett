#!/usr/bin/env python3
"""Reset documents stuck in 'processing' state"""

import asyncio
import asyncpg
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def reset_stuck_documents(hours_old: int = 1):
    """Reset documents that have been stuck in 'processing' state"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # First, let's see what we're dealing with
        stuck_docs = await conn.fetch("""
            SELECT COUNT(*) as count, 
                   MIN(updated_at) as oldest,
                   MAX(updated_at) as newest
            FROM nordic_documents 
            WHERE processing_status = 'processing'
        """)
        
        if stuck_docs[0]['count'] == 0:
            print("✅ No documents stuck in processing state")
            return
        
        print(f"📊 Found {stuck_docs[0]['count']:,} documents in 'processing' state")
        print(f"   Oldest: {stuck_docs[0]['oldest']}")
        print(f"   Newest: {stuck_docs[0]['newest']}")
        
        # Calculate cutoff time
        cutoff_time = datetime.now() - timedelta(hours=hours_old)
        
        # Count how many we'll reset
        to_reset = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM nordic_documents 
            WHERE processing_status = 'processing'
            AND updated_at < $1
        """, cutoff_time)
        
        print(f"\n🔄 Will reset {to_reset:,} documents older than {hours_old} hours")
        
        if input("\nProceed? (y/n): ").lower() != 'y':
            print("❌ Cancelled")
            return
        
        # Reset stuck documents to 'pending'
        result = await conn.execute("""
            UPDATE nordic_documents
            SET processing_status = 'pending',
                updated_at = NOW(),
                extraction_attempts = COALESCE(extraction_attempts, 0)
            WHERE processing_status = 'processing'
            AND updated_at < $1
        """, cutoff_time)
        
        count = int(result.split()[-1])
        print(f"\n✅ Reset {count:,} documents from 'processing' to 'pending'")
        
        # Show new status
        status = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE processing_status = 'pending') as pending,
                COUNT(*) FILTER (WHERE processing_status = 'processing') as processing,
                COUNT(*) FILTER (WHERE processing_status = 'completed') as completed,
                COUNT(*) FILTER (WHERE processing_status = 'failed') as failed
            FROM nordic_documents
        """)
        
        print(f"\n📊 New Status:")
        print(f"   ⏳ Pending: {status['pending']:,}")
        print(f"   ⚙️  Processing: {status['processing']:,}")
        print(f"   ✅ Completed: {status['completed']:,}")
        print(f"   ❌ Failed: {status['failed']:,}")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    hours = 1
    if len(sys.argv) > 1:
        try:
            hours = int(sys.argv[1])
        except ValueError:
            print("Usage: python reset_stuck_documents.py [hours_old]")
            print("  Default: 1 hour")
            sys.exit(1)
    
    print(f"🔧 Resetting documents stuck in 'processing' for more than {hours} hour(s)")
    asyncio.run(reset_stuck_documents(hours))