#!/usr/bin/env python3
"""
BATCH Company Attribution Fix - Process in small batches with verification
"""
import asyncio
import sys
import os
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCalendarEvent, NordicCompany
from nordic_ingestion.common.company_mappings import COMPANY_SLUG_TO_NAME
from sqlalchemy import select, update, func

class BatchCompanyFixer:
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.log_file = f"batch_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "batches_processed": 0,
            "documents_fixed": 0,
            "failures": []
        }
        
    def extract_company_slug_from_url(self, source_url: str) -> Optional[str]:
        """Extract company slug from MFN source URL"""
        if not source_url:
            return None
        match = re.search(r'mfn\.se/all/[a-z]/([^/?]+)', source_url)
        if match:
            return match.group(1)
        return None
    
    async def resolve_slug_to_company(self, db, slug: str) -> Tuple[Optional[str], Optional[str]]:
        """Resolve slug to company_id and name"""
        if not slug:
            return None, None
            
        # Strategy 1: Check centralized mappings FIRST
        if slug in COMPANY_SLUG_TO_NAME:
            target_name = COMPANY_SLUG_TO_NAME[slug]
            result = await db.execute(
                select(NordicCompany.id, NordicCompany.name).where(
                    func.lower(NordicCompany.name) == func.lower(target_name)
                ).limit(1)
            )
            row = result.fetchone()
            if row:
                return str(row[0]), row[1]
        
        # Strategy 2: Try without suffixes
        for suffix in ["-group", "-holding", "-ab", "-publ"]:
            if slug.endswith(suffix):
                base_slug = slug[:-len(suffix)]
                if base_slug in COMPANY_SLUG_TO_NAME:
                    target_name = COMPANY_SLUG_TO_NAME[base_slug]
                    result = await db.execute(
                        select(NordicCompany.id, NordicCompany.name).where(
                            func.lower(NordicCompany.name) == func.lower(target_name)
                        ).limit(1)
                    )
                    row = result.fetchone()
                    if row:
                        return str(row[0]), row[1]
            
        # Strategy 3: Try slug to title case
        company_name_guess = slug.replace('-', ' ').title()
        
        # Exact match
        result = await db.execute(
            select(NordicCompany.id, NordicCompany.name).where(
                func.lower(NordicCompany.name) == func.lower(company_name_guess)
            ).limit(1)
        )
        row = result.fetchone()
        if row:
            return str(row[0]), row[1]
            
        # Strategy 4: Contains match
        result = await db.execute(
            select(NordicCompany.id, NordicCompany.name).where(
                func.lower(NordicCompany.name).contains(func.lower(slug.split('-')[0]))
            ).limit(1)
        )
        row = result.fetchone()
        if row:
            return str(row[0]), row[1]
            
        return None, None
    
    async def process_batch(self, start_offset: int) -> Dict:
        """Process a small batch of documents"""
        batch_result = {
            "offset": start_offset,
            "processed": 0,
            "fixed": 0,
            "failed": 0,
            "skipped": 0,
            "examples": []
        }
        
        async with AsyncSessionLocal() as db:
            # Get batch of documents - focus on BE Group first
            result = await db.execute(
                select(NordicDocument)
                .where(
                    NordicDocument.source_url.isnot(None),
                    NordicDocument.company_id == '2cd2e296-2ed2-4f9b-bd80-ffe5c4b1e7dd'  # BE Group
                )
                .offset(start_offset)
                .limit(self.batch_size)
            )
            documents = result.scalars().all()
            
            if not documents:
                return batch_result
                
            print(f"\n📦 Processing batch at offset {start_offset} ({len(documents)} documents)")
            
            for doc in documents:
                batch_result["processed"] += 1
                
                # Extract slug
                slug = self.extract_company_slug_from_url(doc.source_url)
                if not slug:
                    batch_result["failed"] += 1
                    continue
                
                # Resolve to company
                correct_company_id, correct_company_name = await self.resolve_slug_to_company(db, slug)
                if not correct_company_id:
                    batch_result["failed"] += 1
                    self.stats["failures"].append({
                        "slug": slug,
                        "source_url": doc.source_url[:100]
                    })
                    # Print first few failures for visibility
                    if len(self.stats["failures"]) <= 5:
                        print(f"   ❌ Failed to resolve: '{slug}'")
                    continue
                
                # Skip if already correct
                if str(doc.company_id) == correct_company_id:
                    batch_result["skipped"] += 1
                    continue
                
                # Get current company name for comparison
                current_result = await db.execute(
                    select(NordicCompany.name).where(NordicCompany.id == doc.company_id)
                )
                current_company = current_result.scalar_one_or_none()
                
                # Add to examples (first 3)
                if len(batch_result["examples"]) < 3:
                    batch_result["examples"].append({
                        "slug": slug,
                        "from": current_company,
                        "to": correct_company_name,
                        "pdf": doc.metadata_.get('pdf_url', '')[:60] + '...' if doc.metadata_ else 'No PDF'
                    })
                
                # Update document
                await db.execute(
                    update(NordicDocument)
                    .where(NordicDocument.id == doc.id)
                    .values(company_id=correct_company_id)
                )
                
                batch_result["fixed"] += 1
                
                # Show progress within batch
                if batch_result["fixed"] % 5 == 0:
                    print(f"      Fixed {batch_result['fixed']} documents in this batch...")
            
            await db.commit()
            
        return batch_result
    
    async def run_test_batch(self) -> None:
        """Run a test batch first to verify it's working"""
        print("🧪 RUNNING TEST BATCH FIRST (10 documents)")
        print("=" * 60)
        
        test_result = await self.process_batch(0)
        
        print(f"\n📊 Test Batch Results:")
        print(f"   Processed: {test_result['processed']}")
        print(f"   Fixed: {test_result['fixed']}")
        print(f"   Failed: {test_result['failed']}")
        print(f"   Skipped (already correct): {test_result.get('skipped', 0)}")
        
        if test_result["examples"]:
            print(f"\n📋 Examples of fixes:")
            for ex in test_result["examples"]:
                print(f"   • {ex['slug']}: {ex['from']} → {ex['to']}")
                print(f"     PDF: {ex['pdf']}")
        
        if test_result["fixed"] == 0:
            print("\n❌ No documents were fixed. Check the resolution logic!")
            # Save log even on failure
            with open(self.log_file, "w") as f:
                json.dump(self.stats, f, indent=2, default=str)
            print(f"\n📄 Log file saved: {self.log_file}")
            return False
            
        print(f"\n✅ Test batch successful! Fixed {test_result['fixed']} documents.")
        response = input("Continue with full processing? (y/n): ")
        return response.lower() == 'y'
    
    async def run_full_processing(self) -> None:
        """Process all documents in batches"""
        print(f"\n🚀 Starting full processing (batch size: {self.batch_size})")
        
        offset = 0
        while True:
            batch_result = await self.process_batch(offset)
            
            if batch_result["processed"] == 0:
                break
                
            self.stats["batches_processed"] += 1
            self.stats["documents_fixed"] += batch_result["fixed"]
            
            print(f"   ✓ Batch {self.stats['batches_processed']}: "
                  f"Fixed {batch_result['fixed']}/{batch_result['processed']} documents")
            
            # Show sample every 10 batches
            if self.stats["batches_processed"] % 10 == 0 and batch_result["examples"]:
                print(f"   Sample: {batch_result['examples'][0]['slug']} → {batch_result['examples'][0]['to']}")
            
            offset += self.batch_size
            
            # Save progress
            if self.stats["batches_processed"] % 100 == 0:
                with open(self.log_file, "w") as f:
                    json.dump(self.stats, f, indent=2, default=str)
        
        # Final stats
        print(f"\n📊 FINAL RESULTS:")
        print(f"   Batches: {self.stats['batches_processed']}")
        print(f"   Documents fixed: {self.stats['documents_fixed']}")
        print(f"   Failures: {len(self.stats['failures'])}")
        
        # Save log file
        with open(self.log_file, "w") as f:
            json.dump(self.stats, f, indent=2, default=str)
        
        print(f"\n📄 Log file saved: {self.log_file}")
        print(f"   View failures: cat {self.log_file} | jq '.failures[:10]'")

async def main():
    """Run batch processing with verification"""
    
    # Ask for batch size
    batch_size = input("Batch size (default 10): ").strip()
    batch_size = int(batch_size) if batch_size else 10
    
    fixer = BatchCompanyFixer(batch_size=batch_size)
    
    # Run test batch
    if await fixer.run_test_batch():
        # Run full processing
        await fixer.run_full_processing()
    else:
        print("❌ Aborted based on test results")

if __name__ == "__main__":
    asyncio.run(main())