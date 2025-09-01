#!/usr/bin/env python3
"""
EMERGENCY DATA RECOVERY: Fix Company Attribution Disaster
Corrects company_id for all documents and events using source_url slug resolution
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

class CompanyAttributionFixer:
    def __init__(self):
        self.log_file = f"company_attribution_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        self.stats = {
            "start_time": datetime.now().isoformat(),
            "documents_processed": 0,
            "documents_fixed": 0,
            "events_processed": 0,
            "events_fixed": 0,
            "companies_resolved": {},
            "failed_resolutions": [],
            "errors": []
        }
        
    def extract_company_slug_from_url(self, source_url: str) -> Optional[str]:
        """Extract company slug from MFN source URL"""
        if not source_url:
            return None
            
        # Pattern: https://mfn.se/all/a/company-slug or https://mfn.se/all/a/company-slug?limit=240
        match = re.search(r'mfn\.se/all/[a-z]/([^/?]+)', source_url)
        if match:
            return match.group(1)
        return None
    
    async def resolve_slug_to_company_id(self, db, slug: str) -> Optional[str]:
        """Resolve company slug to correct company_id"""
        if not slug:
            return None
            
        # Debug logging for specific problematic slugs
        debug_slugs = ["embellence-group", "gomspace-group", "abelco-investment"]
        if slug in debug_slugs:
            print(f"\n🔍 DEBUG: Resolving slug '{slug}'")
            
        # 1. Try direct mapping from centralized system
        if slug in COMPANY_SLUG_TO_NAME:
            target_name = COMPANY_SLUG_TO_NAME[slug]
            
            result = await db.execute(
                select(NordicCompany.id).where(
                    func.lower(NordicCompany.name) == func.lower(target_name)
                ).limit(1)
            )
            company_id = result.scalar_one_or_none()
            if company_id:
                return company_id
        
        # 2. Try variations (with/without suffixes)
        for suffix in ["-group", "-holding", "-ab", "-publ"]:
            if slug.endswith(suffix):
                base_slug = slug[:-len(suffix)]
                if base_slug in COMPANY_SLUG_TO_NAME:
                    target_name = COMPANY_SLUG_TO_NAME[base_slug]
                    result = await db.execute(
                        select(NordicCompany.id).where(
                            func.lower(NordicCompany.name) == func.lower(target_name)
                        ).limit(1)
                    )
                    company_id = result.scalar_one_or_none()
                    if company_id:
                        return company_id
        
        # 3. Try converting slug to title case and search (exact match)
        company_name_guess = slug.replace('-', ' ').title()
        
        if slug in debug_slugs:
            print(f"   Trying exact match: '{company_name_guess}'")
            # Also check what companies exist with similar names
            check_result = await db.execute(
                select(NordicCompany.name).where(
                    func.lower(NordicCompany.name).contains(slug.split('-')[0])
                ).limit(5)
            )
            similar_companies = check_result.fetchall()
            if similar_companies:
                print(f"   Found similar companies: {[c[0] for c in similar_companies]}")
            
        result = await db.execute(
            select(NordicCompany.id).where(
                func.lower(NordicCompany.name) == func.lower(company_name_guess)
            ).limit(1)
        )
        company_id = result.scalar_one_or_none()
        if company_id:
            return company_id
            
        # 4. Try partial matching with slug parts
        result = await db.execute(
            select(NordicCompany.id).where(
                func.lower(NordicCompany.name).contains(func.lower(company_name_guess))
            ).limit(1)
        )
        company_id = result.scalar_one_or_none()
        if company_id:
            return company_id
            
        # 5. Try variations without common words
        for word_to_remove in [" Group", " Holding", " AB", " Ltd"]:
            if word_to_remove.lower() in company_name_guess.lower():
                simplified_name = company_name_guess.replace(word_to_remove, "").strip()
                result = await db.execute(
                    select(NordicCompany.id).where(
                        func.lower(NordicCompany.name).contains(func.lower(simplified_name))
                    ).limit(1)
                )
                company_id = result.scalar_one_or_none()
                if company_id:
                    return company_id
                    
        # 6. Last resort: check if any company name contains the main word from slug
        main_word = slug.split('-')[0]  # "embellence-group" -> "embellence"
        if len(main_word) > 3:  # Avoid matching short words
            result = await db.execute(
                select(NordicCompany.id).where(
                    func.lower(NordicCompany.name).contains(func.lower(main_word))
                ).limit(1)
            )
            company_id = result.scalar_one_or_none()
            if company_id:
                return company_id
            
        return None
    
    def clean_metadata(self, metadata: dict, correct_company_name: str) -> dict:
        """Clean metadata by removing incorrect company references"""
        if not metadata:
            return metadata
            
        # Remove incorrect company references in llm_filter_context
        if "llm_filter_context" in metadata:
            if "company" in metadata["llm_filter_context"]:
                # Replace with correct company name
                metadata["llm_filter_context"]["company"] = correct_company_name
                
        return metadata
    
    async def fix_documents(self) -> None:
        """Fix all document company attributions"""
        print("🔧 Starting document attribution fix...")
        
        async with AsyncSessionLocal() as db:
            # Get all documents with source_url
            result = await db.execute(
                select(NordicDocument).where(NordicDocument.source_url.isnot(None))
            )
            documents = result.scalars().all()
            
            print(f"📄 Found {len(documents)} documents to process")
            
            batch_updates = []
            
            for doc in documents:
                self.stats["documents_processed"] += 1
                
                # Extract company slug from source URL
                slug = self.extract_company_slug_from_url(doc.source_url)
                if not slug:
                    self.stats["failed_resolutions"].append({
                        "document_id": str(doc.id),
                        "source_url": doc.source_url,
                        "reason": "could_not_extract_slug"
                    })
                    continue
                
                # Resolve to correct company ID
                correct_company_id = await self.resolve_slug_to_company_id(db, slug)
                if not correct_company_id:
                    self.stats["failed_resolutions"].append({
                        "document_id": str(doc.id),
                        "slug": slug,
                        "source_url": doc.source_url,
                        "reason": "could_not_resolve_company"
                    })
                    continue
                
                # Skip if already correct
                if doc.company_id == correct_company_id:
                    continue
                
                # Get correct company name for metadata cleaning
                company_result = await db.execute(
                    select(NordicCompany.name).where(NordicCompany.id == correct_company_id)
                )
                correct_company_name = company_result.scalar_one_or_none()
                
                # CRITICAL VALIDATION
                if slug == "embracer-group" and correct_company_name == "B3 Consulting":
                    print(f"🚨 ERROR: Embracer Group resolving to B3 Consulting! STOPPING!")
                    raise Exception("Critical slug resolution failure detected")
                
                # Clean metadata
                cleaned_metadata = self.clean_metadata(doc.metadata_, correct_company_name)
                
                # Prepare batch update
                batch_updates.append({
                    "document_id": doc.id,
                    "old_company_id": doc.company_id,
                    "new_company_id": correct_company_id,
                    "slug": slug,
                    "company_name": correct_company_name,
                    "source_url": doc.source_url,
                    "title": doc.title,
                    "processing_status": doc.processing_status
                })
                
                # Track company resolutions
                if slug not in self.stats["companies_resolved"]:
                    self.stats["companies_resolved"][slug] = {
                        "company_name": correct_company_name,
                        "company_id": correct_company_id,
                        "document_count": 0
                    }
                self.stats["companies_resolved"][slug]["document_count"] += 1
                
                self.stats["documents_fixed"] += 1
                
                # Progress update
                if self.stats["documents_processed"] % 1000 == 0:
                    print(f"   📊 Processed {self.stats['documents_processed']} documents, fixed {self.stats['documents_fixed']}")
            
            # Execute batch updates
            print(f"💾 Executing {len(batch_updates)} document updates...")
            
            for update_info in batch_updates:
                try:
                    # Update document
                    await db.execute(
                        update(NordicDocument)
                        .where(NordicDocument.id == update_info["document_id"])
                        .values(
                            company_id=update_info["new_company_id"],
                            metadata_=self.clean_metadata(doc.metadata_, update_info["company_name"])
                        )
                    )
                    
                    # Update processing status if downloaded
                    if update_info["processing_status"] == "downloaded":
                        await db.execute(
                            update(NordicDocument)
                            .where(NordicDocument.id == update_info["document_id"])
                            .values(processing_status="downloaded_fixed")
                        )
                        
                except Exception as e:
                    self.stats["errors"].append({
                        "document_id": str(update_info["document_id"]),
                        "error": str(e)
                    })
            
            await db.commit()
            
            # Log all fixed documents
            with open(f"fixed_documents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
                json.dump(batch_updates, f, indent=2, default=str)
                
            print(f"✅ Fixed {self.stats['documents_fixed']} documents")
    
    async def fix_calendar_events(self) -> None:
        """Fix all calendar event company attributions"""
        print("📅 Starting calendar event attribution fix...")
        
        async with AsyncSessionLocal() as db:
            # Get all calendar events with source_url
            result = await db.execute(
                select(NordicCalendarEvent).where(NordicCalendarEvent.source_url.isnot(None))
            )
            events = result.scalars().all()
            
            print(f"📅 Found {len(events)} calendar events to process")
            
            batch_updates = []
            
            for event in events:
                self.stats["events_processed"] += 1
                
                # Extract company slug from source URL
                slug = self.extract_company_slug_from_url(event.source_url)
                if not slug:
                    continue
                
                # Resolve to correct company ID
                correct_company_id = await self.resolve_slug_to_company_id(db, slug)
                if not correct_company_id:
                    continue
                
                # Skip if already correct
                if event.company_id == correct_company_id:
                    continue
                
                # Prepare batch update
                batch_updates.append({
                    "event_id": event.id,
                    "old_company_id": event.company_id,
                    "new_company_id": correct_company_id,
                    "slug": slug,
                    "title": event.title,
                    "event_date": event.event_date
                })
                
                self.stats["events_fixed"] += 1
                
                # Progress update
                if self.stats["events_processed"] % 1000 == 0:
                    print(f"   📊 Processed {self.stats['events_processed']} events, fixed {self.stats['events_fixed']}")
            
            # Execute batch updates
            print(f"💾 Executing {len(batch_updates)} event updates...")
            
            for update_info in batch_updates:
                try:
                    await db.execute(
                        update(NordicCalendarEvent)
                        .where(NordicCalendarEvent.id == update_info["event_id"])
                        .values(company_id=update_info["new_company_id"])
                    )
                except Exception as e:
                    self.stats["errors"].append({
                        "event_id": str(update_info["event_id"]),
                        "error": str(e)
                    })
            
            await db.commit()
            
            # Log all fixed events
            with open(f"fixed_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
                json.dump(batch_updates, f, indent=2, default=str)
                
            print(f"✅ Fixed {self.stats['events_fixed']} calendar events")
    
    async def run_recovery(self) -> None:
        """Run complete recovery process"""
        print("🚨 EMERGENCY DATA RECOVERY: Company Attribution Fix")
        print("=" * 60)
        
        try:
            # Fix documents
            await self.fix_documents()
            
            # Fix calendar events  
            await self.fix_calendar_events()
            
            # Final statistics
            self.stats["end_time"] = datetime.now().isoformat()
            
            print("\n" + "=" * 60)
            print("📊 RECOVERY COMPLETE")
            print(f"📄 Documents processed: {self.stats['documents_processed']}")
            print(f"✅ Documents fixed: {self.stats['documents_fixed']}")
            print(f"📅 Events processed: {self.stats['events_processed']}")
            print(f"✅ Events fixed: {self.stats['events_fixed']}")
            print(f"🏢 Companies resolved: {len(self.stats['companies_resolved'])}")
            print(f"❌ Failed resolutions: {len(self.stats['failed_resolutions'])}")
            print(f"🚨 Errors: {len(self.stats['errors'])}")
            
            # Save final log
            with open(self.log_file, "w") as f:
                json.dump(self.stats, f, indent=2, default=str)
                
            print(f"📋 Full recovery log saved to: {self.log_file}")
            
        except Exception as e:
            print(f"🚨 RECOVERY FAILED: {e}")
            self.stats["fatal_error"] = str(e)
            with open(self.log_file, "w") as f:
                json.dump(self.stats, f, indent=2, default=str)

async def main():
    """Run the company attribution recovery"""
    fixer = CompanyAttributionFixer()
    await fixer.run_recovery()

if __name__ == "__main__":
    asyncio.run(main())