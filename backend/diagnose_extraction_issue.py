#!/usr/bin/env python3
"""
Comprehensive diagnosis of the extraction issue
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def diagnose():
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔍 DIAGNOSING EXTRACTION ISSUE")
        print("=" * 60)
        
        # 1. Check if extraction columns exist
        print("\n1️⃣ Checking if extraction columns exist...")
        columns = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'nordic_documents' 
            AND column_name IN ('extraction_status', 'extraction_priority', 'extraction_attempts')
        """)
        
        existing_cols = [col['column_name'] for col in columns]
        print(f"   Found columns: {existing_cols}")
        
        if 'extraction_status' not in existing_cols:
            print("   ❌ extraction_status column is MISSING!")
            print("   ➡️  You need to run: python3 domains/document_intelligence/migration_add_extraction_tracking.py")
            return
        
        # 2. Check processing_status distribution
        print("\n2️⃣ Processing status distribution:")
        proc_status = await conn.fetch("""
            SELECT processing_status, COUNT(*) as count
            FROM nordic_documents
            GROUP BY processing_status
            ORDER BY count DESC
        """)
        
        for row in proc_status:
            print(f"   {row['processing_status']}: {row['count']:,}")
        
        # 3. Check extraction_status for downloaded documents
        print("\n3️⃣ Extraction status for downloaded documents:")
        ext_status = await conn.fetch("""
            SELECT 
                CASE 
                    WHEN extraction_status IS NULL THEN 'NULL'
                    ELSE extraction_status
                END as status,
                COUNT(*) as count
            FROM nordic_documents
            WHERE processing_status = 'downloaded'
            GROUP BY extraction_status
            ORDER BY count DESC
        """)
        
        for row in ext_status:
            print(f"   {row['status']}: {row['count']:,}")
        
        # 4. Check documents that meet extraction criteria
        print("\n4️⃣ Documents meeting extraction criteria:")
        extractable = await conn.fetchval("""
            SELECT COUNT(*)
            FROM nordic_documents
            WHERE processing_status = 'downloaded'
            AND extraction_status = 'pending'
            AND extraction_attempts < 3
        """)
        print(f"   Ready for extraction: {extractable:,}")
        
        # 5. Check a few sample documents
        print("\n5️⃣ Sample of downloaded documents:")
        samples = await conn.fetch("""
            SELECT id, title, processing_status, extraction_status, 
                   extraction_attempts, extraction_priority, storage_path
            FROM nordic_documents
            WHERE processing_status = 'downloaded'
            LIMIT 5
        """)
        
        for i, row in enumerate(samples, 1):
            print(f"\n   Document {i}:")
            print(f"   - Title: {row['title'][:40]}...")
            print(f"   - Processing: {row['processing_status']}")
            print(f"   - Extraction: {row['extraction_status']}")
            print(f"   - Attempts: {row['extraction_attempts']}")
            print(f"   - Priority: {row['extraction_priority']}")
            print(f"   - Path exists: {Path(row['storage_path']).exists() if row['storage_path'] else 'No path'}")
        
        # 6. Identify the problem
        print("\n6️⃣ DIAGNOSIS:")
        
        null_count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM nordic_documents
            WHERE processing_status = 'downloaded' 
            AND extraction_status IS NULL
        """)
        
        if null_count > 0:
            print(f"   ⚠️  PROBLEM FOUND: {null_count:,} documents have NULL extraction_status!")
            print("   ➡️  SOLUTION: Run: python3 fix_extraction_status.py")
        elif extractable == 0:
            # Check other reasons
            completed = await conn.fetchval("""
                SELECT COUNT(*)
                FROM nordic_documents
                WHERE processing_status = 'downloaded' 
                AND extraction_status = 'completed'
            """)
            
            failed_perm = await conn.fetchval("""
                SELECT COUNT(*)
                FROM nordic_documents
                WHERE processing_status = 'downloaded' 
                AND extraction_status = 'failed_permanent'
            """)
            
            high_attempts = await conn.fetchval("""
                SELECT COUNT(*)
                FROM nordic_documents
                WHERE processing_status = 'downloaded' 
                AND extraction_status IN ('pending', 'failed_extraction')
                AND extraction_attempts >= 3
            """)
            
            if completed > 0:
                print(f"   ℹ️  {completed:,} documents already extracted (status='completed')")
                print("   ➡️  To re-extract: python3 fix_extraction_status.py --reset-completed")
            
            if failed_perm > 0:
                print(f"   ℹ️  {failed_perm:,} documents permanently failed")
            
            if high_attempts > 0:
                print(f"   ℹ️  {high_attempts:,} documents exceeded max attempts (3)")
                print("   ➡️  To reset: python3 domains/document_intelligence/cli_nordic_extraction.py reset-failed 5")
        else:
            print(f"   ✅ Everything looks good! {extractable:,} documents ready for extraction")
        
    except Exception as e:
        print(f"\n❌ Error during diagnosis: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(diagnose())