#!/usr/bin/env python3
"""
FIX METADATA COMPANY NAMES
Finds and fixes documents where metadata contains incorrect company names
that don't match the actual company_id
"""
import asyncio
import sys
import os
import time
import json
from datetime import datetime
from typing import Dict, List, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from sqlalchemy import select, update, func, text

class MetadataCompanyFixer:
    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "total_processed": 0,
            "total_fixed": 0,
            "batches_completed": 0,
            "mismatches_found": {},
            "companies_fixed": set(),
            "processing_rate": 0.0
        }
        self.start_time = time.time()
    
    async def find_metadata_mismatches(self) -> int:
        """First scan to find all mismatches"""
        print("🔍 Scanning for metadata company mismatches...")
        
        async with AsyncSessionLocal() as db:
            # Count total documents with metadata
            count_result = await db.execute(
                select(func.count(NordicDocument.id))
                .where(NordicDocument.metadata_.isnot(None))
            )
            total_with_metadata = count_result.scalar_one()
            print(f"📊 Total documents with metadata: {total_with_metadata:,}")
            
            # Find mismatches
            query = text("""
            SELECT 
                d.id,
                d.company_id,
                c.name as actual_company_name,
                d.metadata->>'llm_filter_context' as llm_context,
                d.metadata
            FROM nordic_documents d
            JOIN nordic_companies c ON d.company_id = c.id
            WHERE 
                d.metadata IS NOT NULL
                AND d.metadata->'llm_filter_context'->>'company' IS NOT NULL
                AND LOWER(d.metadata->'llm_filter_context'->>'company') != LOWER(c.name)
            LIMIT 1000
            """)
            
            result = await db.execute(query)
            mismatches = result.fetchall()
            
            # Analyze mismatches
            for row in mismatches:
                doc_id, company_id, actual_name, llm_context, metadata = row
                if llm_context:
                    context = json.loads(llm_context)
                    metadata_company = context.get('company', '')
                    
                    key = f"{metadata_company} → {actual_name}"
                    if key not in self.stats["mismatches_found"]:
                        self.stats["mismatches_found"][key] = 0
                    self.stats["mismatches_found"][key] += 1
            
            return len(mismatches)
    
    async def process_batch(self, offset: int) -> dict:
        """Process one batch of documents"""
        batch_result = {
            "processed": 0,
            "fixed": 0,
            "skipped": 0,
            "examples": []
        }
        
        async with AsyncSessionLocal() as db:
            # Get batch of documents with potential mismatches
            query = text("""
            SELECT 
                d.id,
                d.company_id,
                c.name as actual_company_name,
                d.metadata
            FROM nordic_documents d
            JOIN nordic_companies c ON d.company_id = c.id
            WHERE 
                d.metadata IS NOT NULL
                AND d.metadata->'llm_filter_context'->>'company' IS NOT NULL
            ORDER BY d.id
            OFFSET :offset
            LIMIT :limit
            """)
            
            result = await db.execute(
                query, 
                {"offset": offset, "limit": self.batch_size}
            )
            documents = result.fetchall()
            
            if not documents:
                return batch_result
            
            batch_result["processed"] = len(documents)
            fixes = []
            
            for doc_id, company_id, actual_company_name, current_metadata in documents:
                if not current_metadata:
                    continue
                
                # Check if llm_filter_context.company matches actual company
                llm_context = current_metadata.get('llm_filter_context', {})
                metadata_company = llm_context.get('company', '')
                
                if metadata_company and metadata_company.lower() != actual_company_name.lower():
                    # Need to fix this
                    cleaned_metadata = current_metadata.copy()
                    cleaned_metadata['llm_filter_context']['company'] = actual_company_name
                    
                    fixes.append({
                        'id': doc_id,
                        'metadata': cleaned_metadata,
                        'old_company': metadata_company,
                        'new_company': actual_company_name
                    })
                    
                    self.stats["companies_fixed"].add(str(company_id))
                    
                    if len(batch_result["examples"]) < 3:
                        batch_result["examples"].append(
                            f"{metadata_company} → {actual_company_name}"
                        )
                else:
                    batch_result["skipped"] += 1
            
            # Apply all fixes in this batch
            for fix in fixes:
                await db.execute(
                    update(NordicDocument)
                    .where(NordicDocument.id == fix['id'])
                    .values(metadata_=fix['metadata'])
                )
            
            await db.commit()
            batch_result["fixed"] = len(fixes)
            
        return batch_result
    
    async def run_metadata_fix(self):
        """Run the complete metadata fix"""
        print("🚀 METADATA COMPANY NAME FIX")
        print("Fixing documents where metadata company doesn't match actual company")
        print("=" * 80)
        
        # First, scan for mismatches
        mismatch_count = await self.find_metadata_mismatches()
        
        if self.stats["mismatches_found"]:
            print(f"\n❌ Found mismatches:")
            for mismatch, count in sorted(
                self.stats["mismatches_found"].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]:
                print(f"   {mismatch}: {count} documents")
            
            if len(self.stats["mismatches_found"]) > 10:
                print(f"   ... and {len(self.stats['mismatches_found']) - 10} more patterns")
        
        print(f"\n📊 Starting fix process...")
        
        offset = 0
        
        while True:
            batch_start = time.time()
            result = await self.process_batch(offset)
            batch_time = time.time() - batch_start
            
            if result["processed"] == 0:
                break
            
            self.stats["total_processed"] += result["processed"]
            self.stats["total_fixed"] += result["fixed"]
            self.stats["batches_completed"] += 1
            
            # Calculate processing rate
            elapsed = time.time() - self.start_time
            self.stats["processing_rate"] = self.stats["total_processed"] / elapsed if elapsed > 0 else 0
            
            # Progress output
            if result["fixed"] > 0:
                print(f"Batch {self.stats['batches_completed']:4d}: "
                      f"Fixed {result['fixed']:3d}/{result['processed']:3d} docs "
                      f"(skipped {result['skipped']:3d}) "
                      f"in {batch_time:.2f}s")
                
                if result["examples"]:
                    print(f"           Examples: {', '.join(result['examples'][:2])}")
            
            # Progress summary every 10 batches
            if self.stats["batches_completed"] % 10 == 0:
                rate = self.stats["processing_rate"]
                print(f"           Progress: {self.stats['total_fixed']:6d} fixed, "
                      f"{rate:.1f} docs/sec")
            
            offset += self.batch_size
        
        # Final statistics
        total_time = time.time() - self.start_time
        self.stats["end_time"] = datetime.now().isoformat()
        self.stats["total_time_seconds"] = total_time
        self.stats["companies_fixed"] = list(self.stats["companies_fixed"])
        
        print("\n" + "=" * 80)
        print("🎉 METADATA FIX COMPLETE!")
        print(f"📊 Total documents processed: {self.stats['total_processed']:,}")
        print(f"✅ Total metadata fixed: {self.stats['total_fixed']:,}")
        print(f"🏢 Companies affected: {len(self.stats['companies_fixed'])}")
        print(f"⏱️  Total time: {total_time:.1f} seconds")
        print(f"🚀 Average rate: {self.stats['total_processed']/total_time:.1f} documents/second")
        
        # Save detailed log
        log_file = f"metadata_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, 'w') as f:
            json.dump(self.stats, f, indent=2, default=str)
        print(f"\n📄 Detailed log saved: {log_file}")
        
        # Show mismatch patterns if any remain
        if self.stats["mismatches_found"]:
            print(f"\n📊 Mismatch patterns found and fixed:")
            for pattern, count in sorted(
                self.stats["mismatches_found"].items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:5]:
                print(f"   {pattern}: {count} documents")

async def verify_sample():
    """Verify a sample of documents to show the problem"""
    print("🔍 Sampling documents with metadata mismatches...\n")
    
    async with AsyncSessionLocal() as db:
        query = text("""
        SELECT 
            d.id,
            c.name as actual_company,
            d.metadata->'llm_filter_context'->>'company' as metadata_company,
            d.source_url
        FROM nordic_documents d
        JOIN nordic_companies c ON d.company_id = c.id
        WHERE 
            d.metadata IS NOT NULL
            AND d.metadata->'llm_filter_context'->>'company' IS NOT NULL
            AND LOWER(d.metadata->'llm_filter_context'->>'company') != LOWER(c.name)
        LIMIT 5
        """)
        
        result = await db.execute(query)
        samples = result.fetchall()
        
        if samples:
            print("Examples of metadata mismatches:")
            for i, (doc_id, actual, metadata_company, source_url) in enumerate(samples, 1):
                print(f"\n{i}. Document ID: {doc_id}")
                print(f"   Actual company: {actual}")
                print(f"   Metadata says: {metadata_company}")
                print(f"   Source: {source_url if source_url else 'N/A'}")
        else:
            print("✅ No metadata mismatches found!")

async def main():
    """Run metadata fix with options"""
    print("METADATA COMPANY NAME FIXER")
    print("=" * 60)
    print("\nThis tool fixes documents where the metadata company name")
    print("doesn't match the actual company in the database.")
    print("\nOptions:")
    print("1. Verify sample mismatches")
    print("2. Run full metadata fix")
    print("3. Exit")
    
    choice = input("\nYour choice (1-3): ").strip()
    
    if choice == "1":
        await verify_sample()
    elif choice == "2":
        batch_size = input("\nBatch size (default 100): ").strip()
        batch_size = int(batch_size) if batch_size else 100
        
        fixer = MetadataCompanyFixer(batch_size=batch_size)
        await fixer.run_metadata_fix()
    else:
        print("Exiting...")

if __name__ == "__main__":
    asyncio.run(main())