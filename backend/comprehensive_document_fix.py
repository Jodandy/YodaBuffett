#!/usr/bin/env python3
"""
COMPREHENSIVE DOCUMENT FIX
Processes ALL documents with source URLs to ensure correct company attribution
Fixes both company_id and metadata in batches
Continues until all documents are processed
"""
import asyncio
import sys
import os
import time
import json
import re
from datetime import datetime
from typing import Optional

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from nordic_ingestion.common.company_mappings import COMPANY_SLUG_TO_NAME, get_company_name
from sqlalchemy import select, update, func

class ComprehensiveDocumentFixer:
    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "total_processed": 0,
            "total_fixed": 0,
            "batches_completed": 0,
            "failed_resolutions": set(),  # Changed to set for unique values
            "failed_resolution_counts": {},  # Track count per slug
            "companies_fixed": {},
            "processing_rate": 0.0
        }
        self.start_time = time.time()
        
    def extract_company_slug_from_url(self, source_url: str) -> Optional[str]:
        """Extract company slug from MFN source URL"""
        if not source_url:
            return None
        match = re.search(r'mfn\.se/all/[a-z]/([^/?]+)', source_url)
        return match.group(1) if match else None
    
    async def resolve_slug_to_company(self, db, slug: str) -> tuple[Optional[str], Optional[str]]:
        """Resolve slug to company_id and name using comprehensive strategy"""
        if not slug:
            return None, None
            
        # Strategy 1: Direct mapping from centralized system
        target_name = get_company_name(slug)
        if target_name != slug.replace('-', ' ').title():  # get_company_name found a mapping
            result = await db.execute(
                select(NordicCompany.id, NordicCompany.name).where(
                    func.lower(NordicCompany.name) == func.lower(target_name)
                ).limit(1)
            )
            row = result.fetchone()
            if row:
                return str(row[0]), row[1]
        
        # Strategy 2: Try variations with suffixes
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
            
        # Strategy 3: Convert slug to title case and try exact match
        company_name_guess = slug.replace('-', ' ').title()
        result = await db.execute(
            select(NordicCompany.id, NordicCompany.name).where(
                func.lower(NordicCompany.name) == func.lower(company_name_guess)
            ).limit(1)
        )
        row = result.fetchone()
        if row:
            return str(row[0]), row[1]
            
        # Strategy 4: Partial matching with first word
        first_word = slug.split('-')[0]
        if len(first_word) > 3:
            result = await db.execute(
                select(NordicCompany.id, NordicCompany.name).where(
                    func.lower(NordicCompany.name).contains(func.lower(first_word))
                ).limit(1)
            )
            row = result.fetchone()
            if row:
                return str(row[0]), row[1]
                
        return None, None
    
    def clean_metadata(self, metadata: dict, correct_company_name: str) -> dict:
        """Clean metadata by fixing company references"""
        if not metadata:
            return metadata
            
        cleaned = metadata.copy()
        
        # Fix llm_filter_context company reference
        if "llm_filter_context" in cleaned and "company" in cleaned["llm_filter_context"]:
            cleaned["llm_filter_context"]["company"] = correct_company_name
                
        return cleaned
    
    async def process_batch(self, offset: int) -> dict:
        """Process one batch of documents"""
        batch_result = {
            "processed": 0,
            "fixed": 0,
            "failed": 0,
            "skipped": 0,
            "examples": []
        }
        
        async with AsyncSessionLocal() as db:
            # Get batch of ALL documents with source URLs
            result = await db.execute(
                select(NordicDocument.id, NordicDocument.source_url, NordicDocument.company_id, NordicDocument.metadata_)
                .where(NordicDocument.source_url.isnot(None))
                .offset(offset)
                .limit(self.batch_size)
            )
            documents = result.fetchall()
            
            if not documents:
                return batch_result
            
            batch_result["processed"] = len(documents)
            fixes = []
            
            for doc_id, source_url, current_company_id, current_metadata in documents:
                # Extract slug from source URL
                slug = self.extract_company_slug_from_url(source_url)
                if not slug:
                    batch_result["failed"] += 1
                    continue
                
                # Resolve slug to correct company
                correct_company_id, correct_company_name = await self.resolve_slug_to_company(db, slug)
                if not correct_company_id:
                    batch_result["failed"] += 1
                    # Track unique failed slugs and their counts
                    self.stats["failed_resolutions"].add(slug)
                    if slug not in self.stats["failed_resolution_counts"]:
                        self.stats["failed_resolution_counts"][slug] = 0
                    self.stats["failed_resolution_counts"][slug] += 1
                    continue
                
                # Skip if already correct
                if str(current_company_id) == correct_company_id:
                    batch_result["skipped"] += 1
                    continue
                
                # Prepare fix
                cleaned_metadata = self.clean_metadata(current_metadata or {}, correct_company_name)
                fixes.append((doc_id, correct_company_id, cleaned_metadata, slug, correct_company_name))
                
                # Track company fixes
                if slug not in self.stats["companies_fixed"]:
                    self.stats["companies_fixed"][slug] = {"company_name": correct_company_name, "count": 0}
                self.stats["companies_fixed"][slug]["count"] += 1
                
                # Add examples
                if len(batch_result["examples"]) < 3:
                    batch_result["examples"].append(f"{slug} → {correct_company_name}")
            
            # Apply all fixes in this batch
            for doc_id, company_id, metadata, slug, company_name in fixes:
                await db.execute(
                    update(NordicDocument)
                    .where(NordicDocument.id == doc_id)
                    .values(company_id=company_id, metadata_=metadata)
                )
            
            await db.commit()
            batch_result["fixed"] = len(fixes)
            
        return batch_result
    
    async def run_comprehensive_fix(self):
        """Run the comprehensive fix for all documents"""
        print("🚀 COMPREHENSIVE DOCUMENT FIX")
        print("Processing ALL documents with source URLs to fix company attribution")
        print("=" * 80)
        
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
            print(f"Batch {self.stats['batches_completed']:4d}: "
                  f"Fixed {result['fixed']:3d}/{result['processed']:3d} docs "
                  f"(skipped {result['skipped']:3d}, failed {result['failed']:2d}) "
                  f"in {batch_time:.2f}s")
            
            if result["examples"]:
                print(f"           Examples: {', '.join(result['examples'][:2])}")
            
            # Progress summary every 10 batches
            if self.stats["batches_completed"] % 10 == 0:
                rate = self.stats["processing_rate"]
                companies_count = len(self.stats["companies_fixed"])
                print(f"           Progress: {self.stats['total_fixed']:6d} fixed, "
                      f"{rate:.1f} docs/sec, {companies_count} companies")
            
            offset += self.batch_size
        
        # Final statistics
        total_time = time.time() - self.start_time
        self.stats["end_time"] = datetime.now().isoformat()
        self.stats["total_time_seconds"] = total_time
        
        print("\n" + "=" * 80)
        print("🎉 COMPREHENSIVE FIX COMPLETE!")
        print(f"📊 Total documents processed: {self.stats['total_processed']:,}")
        print(f"✅ Total documents fixed: {self.stats['total_fixed']:,}")
        print(f"🏢 Companies affected: {len(self.stats['companies_fixed'])}")
        print(f"❌ Failed slug resolutions: {len(self.stats['failed_resolutions'])}")
        print(f"⏱️  Total time: {total_time:.1f} seconds")
        print(f"🚀 Average rate: {self.stats['total_processed']/total_time:.1f} documents/second")
        
        # Show top companies fixed
        if self.stats["companies_fixed"]:
            print(f"\n📈 Top companies fixed:")
            sorted_companies = sorted(self.stats["companies_fixed"].items(), 
                                    key=lambda x: x[1]["count"], reverse=True)[:10]
            for slug, info in sorted_companies:
                print(f"   {slug}: {info['count']} docs → {info['company_name']}")
        
        # Save detailed log (convert set to list for JSON serialization)
        log_file = f"comprehensive_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        stats_for_json = self.stats.copy()
        stats_for_json["failed_resolutions"] = list(self.stats["failed_resolutions"])
        with open(log_file, 'w') as f:
            json.dump(stats_for_json, f, indent=2, default=str)
        print(f"\n📄 Detailed log saved: {log_file}")
        
        if self.stats["failed_resolutions"]:
            print(f"\n❌ Failed resolutions:")
            for slug in sorted(self.stats["failed_resolutions"]):
                count = self.stats["failed_resolution_counts"][slug]
                print(f"   {slug}: {count} documents")

async def main():
    """Run comprehensive document fix"""
    batch_size = input("Batch size (default 100): ").strip()
    batch_size = int(batch_size) if batch_size else 100
    
    fixer = ComprehensiveDocumentFixer(batch_size=batch_size)
    await fixer.run_comprehensive_fix()

if __name__ == "__main__":
    asyncio.run(main())