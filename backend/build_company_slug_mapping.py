#!/usr/bin/env python3
"""
Build Comprehensive Company Slug Mapping
Tests actual MFN URLs to find working slugs for all Swedish companies

This script:
1. Loads all Swedish companies from database
2. Tests multiple slug variations against MFN.se
3. Builds a comprehensive mapping file
4. Identifies companies that need manual mapping
"""
import asyncio
import aiohttp
import sys
import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
import re

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from sqlalchemy import select

class CompanySlugMapper:
    
    def __init__(self):
        self.working_mappings = {}
        self.failed_companies = []
        self.test_count = 0
        self.start_time = datetime.now()
        
    async def load_swedish_companies(self) -> List[Dict]:
        """Load all Swedish companies from database"""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(NordicCompany.name, NordicCompany.ticker, NordicCompany.id)
                .where(NordicCompany.country == 'SE')
                .order_by(NordicCompany.name)
            )
            companies = result.fetchall()
            
            company_list = []
            for name, ticker, company_id in companies:
                company_list.append({
                    'name': name,
                    'ticker': ticker,
                    'id': str(company_id)
                })
            
            print(f"📊 Loaded {len(company_list)} Swedish companies")
            return company_list
    
    def generate_slug_variations(self, company_name: str, ticker: str) -> List[str]:
        """Generate multiple possible slug variations for a company"""
        variations = []
        
        # Known working mappings from current system
        known_mappings = {
            "Volvo Group": "volvo",
            "AstraZeneca": "astrazeneca", 
            "Atlas Copco AB": "atlas-copco",
            "Telefonaktiebolaget LM Ericsson": "ericsson",
            "H&M Hennes & Mauritz AB": "handm",
            "Sandvik AB": "sandvik",
            "Nordea Bank Abp": "nordea",
            "Investor AB": "investor",
            "ABB Ltd": "abb",
            "Hexagon AB": "hexagon",
        }
        
        if company_name in known_mappings:
            variations.append(known_mappings[company_name])
            return variations
        
        # 1. Try ticker-based variations
        if ticker:
            # Remove common ticker suffixes
            clean_ticker = ticker.replace(' A', '').replace(' B', '').replace(' SDB', '').replace(' PREF', '')
            variations.extend([
                clean_ticker.lower(),
                ticker.lower(),
                ticker.lower().replace(' ', '-')
            ])
        
        # 2. Try company name variations
        name = company_name.lower()
        
        # Remove common Swedish company suffixes
        name = re.sub(r'\s+(ab|aktiebolag|aktiebolaget|group|holding|holdings|ltd|plc|corp|corporation)$', '', name)
        name = re.sub(r'^(aktiebolaget|telefonaktiebolaget)\s+', '', name)
        
        # Clean special characters
        name = name.replace('å', 'a').replace('ä', 'a').replace('ö', 'o')
        name = name.replace('&', 'and').replace(' & ', '-and-')
        
        # Generate variations
        variations.extend([
            name.replace(' ', ''),  # No spaces
            name.replace(' ', '-'), # Dash separated
            name.replace(' ', '_'), # Underscore separated
            '-'.join(name.split()),  # Each word separated by dash
        ])
        
        # 3. Try first word only
        first_word = name.split()[0] if name.split() else ''
        if first_word and len(first_word) > 2:
            variations.append(first_word)
        
        # 4. Try acronyms for multi-word names
        words = name.split()
        if len(words) > 1:
            acronym = ''.join(word[0] for word in words if word)
            if len(acronym) > 1:
                variations.append(acronym)
        
        # Clean up variations
        clean_variations = []
        for var in variations:
            # Remove multiple dashes/underscores
            var = re.sub(r'[-_]+', '-', var).strip('-_')
            if var and len(var) > 1 and var not in clean_variations:
                clean_variations.append(var)
        
        return clean_variations[:8]  # Limit to 8 variations to avoid too many requests
    
    async def test_slug(self, session: aiohttp.ClientSession, slug: str) -> bool:
        """Test if a slug works on MFN.se"""
        test_url = f"https://mfn.se/all/a/{slug}?limit=5"
        self.test_count += 1
        
        try:
            async with session.get(test_url) as response:
                if response.status == 200:
                    content = await response.text()
                    # Check if page has actual content (not a 404 page that returns 200)
                    if ('no items found' in content.lower() or 
                        'inga objekt hittades' in content.lower() or
                        len(content) < 1000):
                        return False
                    return True
                return False
                
        except Exception as e:
            print(f"   ❌ Error testing {slug}: {e}")
            return False
    
    async def find_working_slug(self, session: aiohttp.ClientSession, company: Dict) -> Optional[str]:
        """Find a working slug for a company"""
        name = company['name']
        ticker = company['ticker']
        
        print(f"\n🔍 Testing: {name} ({ticker})")
        
        variations = self.generate_slug_variations(name, ticker)
        print(f"   Testing {len(variations)} variations: {', '.join(variations[:5])}{'...' if len(variations) > 5 else ''}")
        
        for i, slug in enumerate(variations, 1):
            print(f"   [{i}/{len(variations)}] Testing '{slug}'...", end=' ')
            
            if await self.test_slug(session, slug):
                print(f"✅ WORKS!")
                self.working_mappings[name] = slug
                return slug
            else:
                print(f"❌")
            
            # Be respectful to MFN.se
            await asyncio.sleep(0.5)
        
        print(f"   🔴 No working slug found for {name}")
        self.failed_companies.append(company)
        return None
    
    async def build_mapping(self):
        """Build comprehensive company-to-slug mapping"""
        print(f"🚀 Building comprehensive MFN company slug mapping")
        print(f"⚠️  This will test multiple URL variations for each company")
        print(f"⏱️  Being respectful with 0.5s delays between requests")
        
        companies = await self.load_swedish_companies()
        
        async with aiohttp.ClientSession(
            headers={
                'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            
            print(f"\n📊 Processing {len(companies)} Swedish companies...")
            
            for i, company in enumerate(companies, 1):
                print(f"\n{'='*60}")
                print(f"[{i}/{len(companies)}] Processing companies...")
                
                await self.find_working_slug(session, company)
                
                # Save progress every 10 companies
                if i % 10 == 0:
                    await self.save_progress()
                    print(f"💾 Progress saved ({len(self.working_mappings)} mappings found)")
                
                # Brief pause between companies
                await asyncio.sleep(1)
        
        # Final save
        await self.save_progress()
        self.print_final_summary()
    
    async def save_progress(self):
        """Save current mapping progress"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        results = {
            "timestamp": timestamp,
            "test_count": self.test_count,
            "working_mappings": self.working_mappings,
            "failed_companies": self.failed_companies,
            "stats": {
                "total_companies_tested": len(self.working_mappings) + len(self.failed_companies),
                "successful_mappings": len(self.working_mappings),
                "failed_companies": len(self.failed_companies),
                "success_rate": len(self.working_mappings) / (len(self.working_mappings) + len(self.failed_companies)) if (len(self.working_mappings) + len(self.failed_companies)) > 0 else 0
            }
        }
        
        # Save detailed results
        with open(f"company_slug_mapping_{timestamp}.json", 'w') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        # Save clean mapping for code use
        with open("company_slug_mapping_clean.json", 'w') as f:
            json.dump(self.working_mappings, f, indent=2, ensure_ascii=False)
    
    def print_final_summary(self):
        """Print comprehensive final summary"""
        total_time = datetime.now() - self.start_time
        total_companies = len(self.working_mappings) + len(self.failed_companies)
        
        print(f"\n{'='*70}")
        print(f"🎉 COMPANY SLUG MAPPING COMPLETE")
        print(f"{'='*70}")
        print(f"⏱️  Total Time: {total_time}")
        print(f"🔍 Total URL Tests: {self.test_count}")
        print(f"📊 Total Companies: {total_companies}")
        print(f"✅ Successful Mappings: {len(self.working_mappings)}")
        print(f"❌ Failed Companies: {len(self.failed_companies)}")
        
        if total_companies > 0:
            success_rate = len(self.working_mappings) / total_companies * 100
            print(f"📈 Success Rate: {success_rate:.1f}%")
        
        print(f"\n📁 FILES GENERATED:")
        print(f"   📊 Detailed Results: company_slug_mapping_[timestamp].json")
        print(f"   🔧 Clean Mapping: company_slug_mapping_clean.json")
        
        if self.working_mappings:
            print(f"\n✅ SUCCESSFUL MAPPINGS (first 10):")
            for i, (name, slug) in enumerate(list(self.working_mappings.items())[:10]):
                print(f"   {i+1}. {name} → {slug}")
            if len(self.working_mappings) > 10:
                print(f"   ... and {len(self.working_mappings) - 10} more")
        
        if self.failed_companies:
            print(f"\n❌ FAILED COMPANIES (need manual mapping):")
            for company in self.failed_companies[:10]:
                print(f"   • {company['name']} ({company['ticker']})")
            if len(self.failed_companies) > 10:
                print(f"   ... and {len(self.failed_companies) - 10} more")
        
        print(f"\n💡 Next Steps:")
        print(f"   1. Review failed companies and add manual mappings")
        print(f"   2. Update historical_ingestion_batch.py with new mappings")
        print(f"   3. Re-run batch ingestion for all companies")

async def main():
    """Main entry point"""
    mapper = CompanySlugMapper()
    await mapper.build_mapping()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()