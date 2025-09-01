#!/usr/bin/env python3
"""
QUICK FIX: Just move BE Group documents to correct companies
Simple and fast - no complex lookups
"""
import asyncio
import sys
import os
import json
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from nordic_ingestion.common.company_mappings import get_company_name
from sqlalchemy import select, update, func

async def quick_fix():
    """Quick fix for BE Group documents"""
    print("🚀 QUICK BE GROUP FIX - Processing 50 documents")
    
    async with AsyncSessionLocal() as db:
        # Get 50 BE Group documents
        result = await db.execute(
            select(NordicDocument.id, NordicDocument.source_url)
            .where(NordicDocument.company_id == '2cd2e296-2ed2-4f9b-bd80-ffe5c4b1e7dd')
            .limit(50)
        )
        docs = result.fetchall()
        
        print(f"Found {len(docs)} BE Group documents")
        
        fixes = []
        for doc_id, source_url in docs:
            # Extract company slug from URL
            if '/all/a/' in source_url:
                slug = source_url.split('/all/a/')[1].split('?')[0]
                print(f"Document {str(doc_id)[:8]}... has slug: {slug}")
                
                # Use centralized mapping system
                target_company_name = get_company_name(slug)
                print(f"  → Resolved to: {target_company_name}")
                
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
                    print(f"  ✅ Found: {actual_name}")
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
                        print(f"  ✅ Found (partial): {actual_name}")
                    else:
                        print(f"  ❌ Company not found: {target_company_name}")
        
        print(f"\n📊 Ready to fix {len(fixes)} documents")
        if fixes:
            print("Auto-applying fixes...")
            if True:  # Auto-confirm
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
                    print(f"✅ Fixed: {slug} → {company_name} (with metadata cleanup)")
                
                await db.commit()
                print(f"🎉 Fixed {len(fixes)} documents!")

if __name__ == "__main__":
    asyncio.run(quick_fix())