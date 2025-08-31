#!/usr/bin/env python3
"""
Find Working MFN Slugs
Systematically test actual MFN URLs to find companies that exist

Strategy:
1. Test a small batch of major Swedish companies
2. Verify each slug returns actual documents
3. Build verified mapping incrementally
4. No more guessing - only test confirmed URLs
"""
import asyncio
import aiohttp
import sys
import os
from typing import Dict, List, Optional

# Major Swedish companies to test (prioritized by importance)
COMPANIES_TO_TEST = [
    # Format: (company_name_in_db, potential_slug, priority)
    ("Volvo Group", "volvo", "high"),
    ("H&M Hennes & Mauritz AB", "handm", "high"), 
    ("Investor AB", "investor", "high"),
    ("Swedbank AB", "swedbank", "high"),
    ("Telefonaktiebolaget LM Ericsson", "ericsson", "high"),
    ("Sandvik AB", "sandvik", "high"),
    ("Skanska AB", "skanska", "medium"),
    ("Telia Company AB", "telia", "medium"),
    ("Electrolux AB", "electrolux", "medium"),
    ("Securitas AB", "securitas", "medium"),
    ("SEB AB", "seb", "medium"),
    ("Svenska Handelsbanken AB", "handelsbanken", "medium"),
    ("Trelleborg AB", "trelleborg", "low"),
    ("SKF AB", "skf", "low"),
    ("SSAB AB", "ssab", "low"),
]

class SlugTester:
    
    def __init__(self):
        self.working_slugs = {}
        self.failed_slugs = {}
        self.test_count = 0
    
    async def test_slug(self, session: aiohttp.ClientSession, company_name: str, slug: str) -> Optional[int]:
        """
        Test if a slug works and return document count
        Returns None if slug doesn't work, doc count if it works
        """
        test_url = f"https://mfn.se/all/a/{slug}?limit=50"
        self.test_count += 1
        
        print(f"🔍 Testing: {company_name} → '{slug}'")
        print(f"   URL: {test_url}")
        
        try:
            async with session.get(test_url) as response:
                if response.status != 200:
                    print(f"   ❌ HTTP {response.status}")
                    return None
                
                content = await response.text()
                
                # Check if page has actual content
                if ('no items found' in content.lower() or 
                    'inga objekt hittades' in content.lower() or
                    'inga nyheter tillgängliga' in content.lower() or
                    len(content) < 2000):  # Very short pages are usually empty
                    print(f"   ❌ No documents found (empty page)")
                    return None
                
                # Try to count documents in the response
                # MFN pages have specific patterns we can look for
                import re
                doc_patterns = [
                    r'<div[^>]*class="[^"]*short-item[^"]*"',  # MFN document containers
                    r'<article',                               # Alternative document containers
                    r'<div[^>]*class="[^"]*item[^"]*"'        # General item containers
                ]
                
                doc_count = 0
                for pattern in doc_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    doc_count = max(doc_count, len(matches))
                
                if doc_count > 0:
                    print(f"   ✅ WORKS! Found ~{doc_count} documents")
                    return doc_count
                else:
                    print(f"   ⚠️  Page loads but no documents detected")
                    return None
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return None
    
    async def test_company_variations(self, session: aiohttp.ClientSession, company_name: str, primary_slug: str):
        """Test a company and its variations"""
        print(f"\n{'='*60}")
        print(f"🏢 Testing: {company_name}")
        print(f"{'='*60}")
        
        # Test primary slug first
        doc_count = await self.test_slug(session, company_name, primary_slug)
        
        if doc_count:
            self.working_slugs[company_name] = {
                "slug": primary_slug,
                "doc_count": doc_count,
                "status": "confirmed"
            }
            return primary_slug
        
        # If primary fails, try some common variations
        variations = self.generate_slug_variations(company_name, primary_slug)
        
        for variation in variations[:3]:  # Only test top 3 variations
            if variation == primary_slug:
                continue
                
            await asyncio.sleep(1)  # Be respectful
            doc_count = await self.test_slug(session, company_name, variation)
            
            if doc_count:
                self.working_slugs[company_name] = {
                    "slug": variation,
                    "doc_count": doc_count,
                    "status": "found_variation"
                }
                return variation
        
        # No working slug found
        self.failed_slugs[company_name] = {
            "tested_slugs": [primary_slug] + variations[:3],
            "status": "not_found"
        }
        print(f"   🔴 No working slug found for {company_name}")
        return None
    
    def generate_slug_variations(self, company_name: str, primary_slug: str) -> List[str]:
        """Generate a few likely variations"""
        variations = []
        
        # Try company name based variations
        name = company_name.lower()
        name = name.replace(' ab', '').replace(' group', '').replace(' ltd', '')
        name = name.replace('å', 'a').replace('ä', 'a').replace('ö', 'o')
        
        # Simple variations
        simple_name = name.split()[0] if name.split() else ''
        if simple_name and simple_name != primary_slug:
            variations.append(simple_name)
        
        # Dash-separated
        dash_name = name.replace(' ', '-')
        if dash_name != primary_slug:
            variations.append(dash_name)
        
        # No spaces
        nospace_name = name.replace(' ', '')
        if nospace_name != primary_slug:
            variations.append(nospace_name)
        
        return variations
    
    async def run_batch_test(self):
        """Test a batch of companies systematically"""
        print(f"🚀 Testing {len(COMPANIES_TO_TEST)} Swedish company slugs on MFN.se")
        print(f"⚠️  Testing respectfully with delays between requests")
        
        async with aiohttp.ClientSession(
            headers={
                'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            
            # Test high priority companies first
            high_priority = [c for c in COMPANIES_TO_TEST if c[2] == "high"]
            
            print(f"\n🎯 Testing {len(high_priority)} HIGH PRIORITY companies first:")
            
            for company_name, slug, priority in high_priority:
                await self.test_company_variations(session, company_name, slug)
                await asyncio.sleep(2)  # 2 second delay between companies
            
            # Show results so far
            self.print_results()
            
            # Ask if user wants to continue
            if len(high_priority) < len(COMPANIES_TO_TEST):
                continue_test = input(f"\n🔄 Continue testing {len(COMPANIES_TO_TEST) - len(high_priority)} more companies? (y/n): ").lower().strip()
                
                if continue_test in ['y', 'yes']:
                    remaining = [c for c in COMPANIES_TO_TEST if c[2] != "high"]
                    
                    for company_name, slug, priority in remaining:
                        await self.test_company_variations(session, company_name, slug)
                        await asyncio.sleep(2)
        
        # Final results
        self.save_verified_mappings()
        self.print_final_summary()
    
    def print_results(self):
        """Print current results"""
        print(f"\n📊 CURRENT RESULTS:")
        print(f"   ✅ Working slugs: {len(self.working_slugs)}")
        print(f"   ❌ Failed slugs: {len(self.failed_slugs)}")
        
        if self.working_slugs:
            print(f"\n✅ WORKING SLUGS:")
            for company, data in self.working_slugs.items():
                print(f"   {company} → '{data['slug']}' (~{data['doc_count']} docs)")
    
    def save_verified_mappings(self):
        """Save only the verified, working mappings"""
        import json
        from datetime import datetime
        
        # Create clean mapping for code use
        clean_mapping = {}
        for company, data in self.working_slugs.items():
            clean_mapping[company] = data['slug']
        
        # Save verified mappings
        with open("verified_company_mappings.json", "w", encoding="utf-8") as f:
            json.dump(clean_mapping, f, indent=2, ensure_ascii=False)
        
        # Save detailed results
        results = {
            "timestamp": datetime.now().isoformat(),
            "test_count": self.test_count,
            "working_slugs": self.working_slugs,
            "failed_slugs": self.failed_slugs,
            "summary": {
                "companies_tested": len(self.working_slugs) + len(self.failed_slugs),
                "working_count": len(self.working_slugs),
                "failed_count": len(self.failed_slugs),
                "success_rate": len(self.working_slugs) / (len(self.working_slugs) + len(self.failed_slugs)) if (len(self.working_slugs) + len(self.failed_slugs)) > 0 else 0
            }
        }
        
        with open("slug_test_results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
    
    def print_final_summary(self):
        """Print final summary and next steps"""
        print(f"\n{'='*60}")
        print(f"🎉 SLUG TESTING COMPLETE")
        print(f"{'='*60}")
        print(f"🔍 Total URL Tests: {self.test_count}")
        print(f"✅ Working Slugs: {len(self.working_slugs)}")
        print(f"❌ Failed Tests: {len(self.failed_slugs)}")
        
        if self.working_slugs:
            print(f"\n✅ VERIFIED WORKING MAPPINGS:")
            for company, data in self.working_slugs.items():
                print(f"   \"{company}\": \"{data['slug']}\",  # ~{data['doc_count']} docs")
        
        print(f"\n📁 FILES CREATED:")
        print(f"   🔧 verified_company_mappings.json (for historical_ingestion_batch.py)")
        print(f"   📊 slug_test_results.json (detailed results)")
        
        print(f"\n💡 NEXT STEPS:")
        print(f"   1. Update historical_ingestion_batch.py to use verified_company_mappings.json")
        print(f"   2. Re-run historical ingestion with only verified companies")
        print(f"   3. Add more companies gradually as we verify their slugs")

async def main():
    """Main entry point"""
    tester = SlugTester()
    await tester.run_batch_test()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()