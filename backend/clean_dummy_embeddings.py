#!/usr/bin/env python3
"""Remove dummy embeddings so they can be regenerated"""

import asyncio
import asyncpg
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def clean_dummy_embeddings():
    """Remove dummy embeddings from database"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🧹 CLEANING DUMMY EMBEDDINGS")
        print("=" * 40)
        
        # First, let's find dummy embeddings
        print("🔍 Finding dummy embeddings...")
        
        all_embeddings = await conn.fetch("""
            SELECT id, embedding
            FROM section_embeddings
            WHERE embedding_model LIKE 'local/%'
        """)
        
        dummy_ids = []
        for row in all_embeddings:
            embedding = eval(row['embedding'])
            first_val = embedding[0]
            
            # Check if all values are the same (dummy pattern)
            if all(x == first_val for x in embedding):
                if abs(first_val - 0.2) < 0.001 or abs(first_val - 0.0) < 0.001:
                    dummy_ids.append(row['id'])
        
        print(f"📊 Found {len(dummy_ids)} dummy embeddings out of {len(all_embeddings)} total")
        
        if not dummy_ids:
            print("✅ No dummy embeddings found!")
            return
        
        print(f"🗑️  Will delete {len(dummy_ids)} dummy embeddings")
        
        if input("\nProceed with deletion? (y/n): ").lower() != 'y':
            print("❌ Cancelled")
            return
        
        # Delete dummy embeddings
        result = await conn.execute("""
            DELETE FROM section_embeddings
            WHERE id = ANY($1)
        """, dummy_ids)
        
        deleted_count = int(result.split()[-1])
        print(f"✅ Deleted {deleted_count} dummy embeddings")
        
        # Show new stats
        remaining = await conn.fetchval("""
            SELECT COUNT(*)
            FROM section_embeddings
            WHERE embedding_model LIKE 'local/%'
        """)
        
        print(f"📊 Remaining embeddings: {remaining}")
        
        print(f"\n🚀 Next step: Regenerate embeddings for the cleaned sections")
        print(f"   Run: python domains/document_intelligence/cli_multi_embeddings.py local process 1000")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(clean_dummy_embeddings())