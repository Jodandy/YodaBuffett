#!/usr/bin/env python3
"""
Test batch updates vs individual updates
"""
import asyncio
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument
from sqlalchemy import select, update

async def test_batch_update():
    """Test batch update performance"""
    print("🧪 Testing batch update performance...")
    
    async with AsyncSessionLocal() as db:
        # Get first 10 BE Group documents
        result = await db.execute(
            select(NordicDocument.id)
            .where(NordicDocument.company_id == '2cd2e296-2ed2-4f9b-bd80-ffe5c4b1e7dd')
            .limit(10)
        )
        doc_ids = [row[0] for row in result.fetchall()]
        print(f"Found {len(doc_ids)} documents to test")
        
        # Test 1: Individual updates (current slow method)
        print("\n🐌 Testing individual updates...")
        start_time = time.time()
        
        for i, doc_id in enumerate(doc_ids[:5]):  # Just 5 for speed
            await db.execute(
                update(NordicDocument)
                .where(NordicDocument.id == doc_id)  
                .values(processing_status='test_individual')
            )
            print(f"  Updated {i+1}/5...")
            
        await db.commit()
        individual_time = time.time() - start_time
        print(f"Individual updates: {individual_time:.2f} seconds")
        
        # Test 2: Batch update (faster method)  
        print("\n🚀 Testing batch update...")
        start_time = time.time()
        
        # Update all 5 remaining documents in one query
        await db.execute(
            update(NordicDocument)
            .where(NordicDocument.id.in_(doc_ids[5:]))
            .values(processing_status='test_batch')
        )
        
        await db.commit()
        batch_time = time.time() - start_time  
        print(f"Batch update: {batch_time:.2f} seconds")
        
        print(f"\n📊 Results:")
        print(f"Individual: {individual_time:.2f}s for 5 documents ({individual_time/5:.2f}s per doc)")
        print(f"Batch: {batch_time:.2f}s for 5 documents ({batch_time/5:.2f}s per doc)")
        print(f"Speedup: {individual_time/batch_time:.1f}x faster")

if __name__ == "__main__":
    asyncio.run(test_batch_update())