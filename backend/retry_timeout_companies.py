#!/usr/bin/env python3
"""
Retry Timeout Companies - Extended Processing
Specifically retry companies that hit timeout in previous run

Features:
- 15-minute timeout per company (3x longer)
- Only processes previously failed timeout companies
- Same comprehensive tracking and resume capability
"""
import asyncio
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from historical_ingestion_batch import HistoricalIngestionBatch

# Companies that hit timeout (300s) in previous run
TIMEOUT_COMPANIES = [
    "handm", "investor", "latour", "medivir", "mycronic", "nobia", "nolato", 
    "nordea", "obducat", "orexo", "peab", "prevas", "pricer", "rejlers", 
    "rottneros", "saab", "sandvik", "securitas", "senzime", "servana", 
    "sileon", "skanska", "smoltek", "softronic", "spiffbet", "stille", 
    "storytel", "studsvik", "svedbergs", "swedbank", "swedencare", "ericsson", 
    "tobii", "trelleborg", "trianon", "vestum", "vitrolife", "vivesto", 
    "volati", "volvo", "wallenstam", "zaplox", "zenergy", "zinzino"
]

class TimeoutRetryBatch(HistoricalIngestionBatch):
    """Extended timeout batch processor for timeout retries"""
    
    async def load_swedish_companies_from_db(self):
        """Load only the companies that hit timeout"""
        print(f"🔄 RETRY MODE: Processing {len(TIMEOUT_COMPANIES)} timeout companies")
        print(f"⏰ Extended timeout: 15 minutes per company (vs 5 minutes)")
        
        # Load all companies first
        await super().load_swedish_companies_from_db()
        
        # Filter to only timeout companies
        timeout_companies = []
        for company in self.swedish_companies:
            if company['slug'].lower() in [slug.lower() for slug in TIMEOUT_COMPANIES]:
                timeout_companies.append(company)
        
        self.swedish_companies = timeout_companies
        
        print(f"✅ Found {len(self.swedish_companies)} timeout companies to retry:")
        for i, company in enumerate(self.swedish_companies[:10]):
            print(f"   {i+1}. {company['name']} ({company['ticker']}) -> {company['slug']}")
        if len(self.swedish_companies) > 10:
            print(f"   ... and {len(self.swedish_companies) - 10} more companies")

async def main():
    """Main entry point for timeout retry"""
    
    print(f"🔄 TIMEOUT COMPANY RETRY")
    print(f"={'='*60}")
    print(f"⏰ Extended Processing: 15-minute timeout per company")
    print(f"🎯 Target: 46 companies that hit 5-minute timeout")  
    print(f"📊 Expected: Much higher success rate with extended time")
    print(f"🚀 Goal: Complete the Swedish market document collection")
    
    retry_processor = TimeoutRetryBatch()
    await retry_processor.run_batch_ingestion()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Retry interrupted by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()