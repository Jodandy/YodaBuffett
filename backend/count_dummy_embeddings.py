#!/usr/bin/env python3
"""Count dummy embeddings that need to be regenerated"""

import asyncio
import asyncpg
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def count_dummy_embeddings():
    """Count how many dummy embeddings we have"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔍 COUNTING DUMMY EMBEDDINGS")
        print("=" * 40)
        
        # Get all embeddings
        all_embeddings = await conn.fetch("""
            SELECT 
                se.id,
                se.embedding,
                ds.section_type,
                ed.company_name
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE se.embedding_model LIKE 'local/%'
        """)
        
        print(f"📊 Total embeddings: {len(all_embeddings)}")
        
        # Check for dummy patterns
        dummy_02_count = 0
        dummy_00_count = 0
        normal_count = 0
        dummy_ids = []
        
        for row in all_embeddings:
            embedding = eval(row['embedding'])
            first_val = embedding[0]
            
            # Check if all values are the same (dummy pattern)
            if all(x == first_val for x in embedding):
                if abs(first_val - 0.2) < 0.001:
                    dummy_02_count += 1
                    dummy_ids.append(row['id'])
                elif abs(first_val - 0.0) < 0.001:
                    dummy_00_count += 1
                    dummy_ids.append(row['id'])
                else:
                    print(f"   Unusual dummy pattern: {first_val} for {row['company_name']} {row['section_type']}")
            else:
                normal_count += 1
        
        print(f"✅ Normal embeddings: {normal_count}")
        print(f"🟡 Dummy 0.2 embeddings: {dummy_02_count}")
        print(f"🟡 Dummy 0.0 embeddings: {dummy_00_count}")
        print(f"❌ Total dummy embeddings: {dummy_02_count + dummy_00_count}")
        
        if dummy_ids:
            print(f"\n📋 Sample dummy embeddings to delete:")
            sample_dummies = await conn.fetch("""
                SELECT 
                    se.id,
                    ds.section_type,
                    ed.company_name,
                    ed.year
                FROM section_embeddings se
                JOIN document_sections ds ON se.document_section_id = ds.id
                JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
                WHERE se.id = ANY($1)
                LIMIT 5
            """, dummy_ids)
            
            for dummy in sample_dummies:
                print(f"   {dummy['company_name']} {dummy['year']} - {dummy['section_type']}")
        
        return dummy_ids
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(count_dummy_embeddings())