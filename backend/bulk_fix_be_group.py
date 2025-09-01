#!/usr/bin/env python3
"""
BULK FIX: Process ALL BE Group documents in batches
Continues until no more BE Group documents need fixing
"""
import asyncio
import sys
import os
import time
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from nordic_ingestion.common.company_mappings import get_company_name
from sqlalchemy import select, update, func

async def fix_batch(batch_size: int = 50) -> dict:
    """Fix one batch of BE Group documents"""
    
    async with AsyncSessionLocal() as db:
        # Get batch of BE Group documents
        result = await db.execute(
            select(NordicDocument.id, NordicDocument.source_url)
            .where(NordicDocument.company_id == '2cd2e296-2ed2-4f9b-bd80-ffe5c4b1e7dd')
            .limit(batch_size)
        )
        docs = result.fetchall()
        
        if not docs:
            return {"processed": 0, "fixed": 0, "completed": True}
        
        fixes = []
        for doc_id, source_url in docs:
            # Extract company slug from URL
            if '/all/a/' in source_url:
                slug = source_url.split('/all/a/')[1].split('?')[0]
                
                # Use centralized mapping system
                target_company_name = get_company_name(slug)
                
                # Find the company in database
                company_result = await db.execute(
                    select(NordicCompany.id, NordicCompany.name).where(
                        func.lower(NordicCompany.name) == func.lower(target_company_name)
                    ).limit(1)
                )
                row = company_result.fetchone()
                
                if row:
                    company_id, actual_name = row
                    fixes.append((doc_id, company_id, slug, actual_name))
                else:
                    # Try partial match
                    company_result = await db.execute(
                        select(NordicCompany.id, NordicCompany.name).where(
                            func.lower(NordicCompany.name).contains(func.lower(target_company_name.split()[0]))
                        ).limit(1)
                    )
                    row = company_result.fetchone()
                    
                    if row:
                        company_id, actual_name = row
                        fixes.append((doc_id, company_id, slug, actual_name))
        
        # Apply all fixes
        for doc_id, company_id, slug, company_name in fixes:
            # Get current document to clean metadata
            doc_result = await db.execute(
                select(NordicDocument.metadata_).where(NordicDocument.id == doc_id)
            )
            current_metadata = doc_result.scalar_one_or_none() or {}
            
            # Clean metadata - fix company reference in llm_filter_context
            cleaned_metadata = current_metadata.copy()
            if "llm_filter_context" in cleaned_metadata and "company" in cleaned_metadata["llm_filter_context"]:
                cleaned_metadata["llm_filter_context"]["company"] = company_name
            
            # Update both company_id and cleaned metadata
            await db.execute(
                update(NordicDocument)
                .where(NordicDocument.id == doc_id)
                .values(company_id=company_id, metadata_=cleaned_metadata)
            )
        
        await db.commit()
        
        return {
            "processed": len(docs),
            "fixed": len(fixes),
            "completed": False,
            "example_fixes": [f"{fix[2]} → {fix[3]}" for fix in fixes[:3]]
        }

async def bulk_fix():
    """Process all BE Group documents in batches"""
    print("🚀 BULK BE GROUP FIX - Processing ALL documents")
    print("=" * 60)
    
    total_processed = 0
    total_fixed = 0
    batch_count = 0
    start_time = time.time()
    
    while True:
        batch_count += 1
        batch_start = time.time()
        
        result = await fix_batch()
        
        batch_time = time.time() - batch_start
        total_processed += result["processed"]
        total_fixed += result["fixed"]
        
        print(f"Batch {batch_count:3d}: Fixed {result['fixed']:2d}/{result['processed']:2d} docs in {batch_time:.2f}s")
        
        if result["example_fixes"]:
            print(f"           Examples: {', '.join(result['example_fixes'][:2])}")
        
        # Show progress every 10 batches
        if batch_count % 10 == 0:
            elapsed = time.time() - start_time
            rate = total_fixed / elapsed if elapsed > 0 else 0
            remaining_estimate = f" (~{(30000-total_fixed)/rate/60:.1f}m remaining)" if rate > 0 and total_fixed < 30000 else ""
            print(f"           Progress: {total_fixed:5d} fixed total, {rate:.1f} docs/sec{remaining_estimate}")
        
        if result["completed"]:
            break
    
    total_time = time.time() - start_time
    print("\n" + "=" * 60)
    print("🎉 BULK FIX COMPLETE!")
    print(f"📊 Total processed: {total_processed}")
    print(f"✅ Total fixed: {total_fixed}")
    print(f"⏱️  Total time: {total_time:.1f} seconds")
    print(f"🚀 Average rate: {total_fixed/total_time:.1f} documents/second")

if __name__ == "__main__":
    asyncio.run(bulk_fix())