#!/usr/bin/env python3
"""
Add Missing Instruments

Adds the 983 missing symbols from instruments.json to company_master
and prepares them for price data collection.
"""

import asyncio
import asyncpg
import json
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InstrumentAdder:
    
    def __init__(self):
        self.db_conn = None
        self.instruments = []
        
    async def setup(self):
        """Initialize database connection and load instruments."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Load instruments data
        with open('data/all-nordic-instruments.json', 'r') as f:
            data = json.load(f)
            self.instruments = data['instruments']
            
        logger.info(f"📋 Loaded {len(self.instruments)} instruments from JSON")
        
    async def get_missing_symbols(self) -> Set[str]:
        """Find symbols missing from company_master."""
        
        # Get current company_master symbols
        company_records = await self.db_conn.fetch("""
            SELECT primary_ticker 
            FROM company_master
            WHERE primary_ticker IS NOT NULL
        """)
        
        existing_symbols = {row['primary_ticker'] for row in company_records}
        
        # Get all instruments symbols
        instruments_symbols = {inst.get('ticker', '') for inst in self.instruments if inst.get('ticker')}
        
        missing_symbols = instruments_symbols - existing_symbols
        
        logger.info(f"📊 Found {len(missing_symbols)} missing symbols")
        return missing_symbols
        
    async def add_missing_symbols(self, missing_symbols: Set[str], dry_run: bool = True):
        """Add missing symbols to company_master."""
        
        if dry_run:
            print(f"\n🔍 DRY RUN: Would add {len(missing_symbols)} symbols to company_master")
            
            # Show examples
            examples = sorted(list(missing_symbols))[:10]
            for symbol in examples:
                inst = next((i for i in self.instruments if i.get('ticker') == symbol), None)
                if inst:
                    print(f"   Would add: {symbol:<10} - {inst.get('name', 'Unknown')}")
            
            print(f"\n💡 Re-run with --execute to actually add these symbols")
            return 0
            
        logger.info(f"🔧 Adding {len(missing_symbols)} symbols to company_master...")
        
        added_count = 0
        
        for inst in self.instruments:
            ticker = inst.get('ticker', '')
            
            if ticker in missing_symbols:
                try:
                    # Check if already exists (double check)
                    exists = await self.db_conn.fetchval(
                        "SELECT 1 FROM company_master WHERE primary_ticker = $1",
                        ticker
                    )
                    
                    if not exists:
                        # Prepare data
                        company_name = inst.get('name', ticker)
                        yahoo_symbol = inst.get('yahoo', '')
                        isin = inst.get('isin', '')
                        market_id = inst.get('marketId', 1)
                        sector_id = inst.get('sectorId')
                        
                        # Map sector to industry name
                        industry = self._map_sector_to_industry(sector_id)
                        
                        # Insert into company_master
                        await self.db_conn.execute("""
                            INSERT INTO company_master 
                            (primary_ticker, company_name, yahoo_symbol, region, country, industry, isin_code, created_at, created_by)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        """, ticker, company_name, yahoo_symbol, 'Nordic', 'Nordic', industry, isin, datetime.now(), 'instruments.json')
                        
                        added_count += 1
                        
                        if added_count <= 10:  # Show first 10
                            logger.info(f"   ✅ Added: {ticker:<10} - {company_name}")
                        elif added_count == 11:
                            logger.info("   ... (continuing silently)")
                            
                except Exception as e:
                    logger.error(f"   ❌ Failed to add {ticker}: {e}")
                    
        logger.info(f"🎉 Successfully added {added_count} new entries to company_master")
        return added_count
        
    def _map_sector_to_industry(self, sector_id: Optional[int]) -> str:
        """Map numeric sector ID to industry name."""
        
        sector_map = {
            1: 'Financial Services',
            2: 'Technology', 
            3: 'Healthcare',
            4: 'Industrials',
            5: 'Consumer Goods',
            6: 'Energy',
            7: 'Materials',
            8: 'Real Estate',
            9: 'Telecommunications',
            10: 'Utilities',
            11: 'Media',
            12: 'Transportation',
            13: 'Retail',
            14: 'Food & Beverage'
        }
        
        return sector_map.get(sector_id, 'Other')
        
    async def show_summary_stats(self):
        """Show summary statistics after update."""
        
        # Company master stats
        total_companies = await self.db_conn.fetchval("SELECT COUNT(*) FROM company_master")
        
        # Price data stats  
        total_price_symbols = await self.db_conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM daily_price_data")
        
        # Coverage analysis
        coverage = await self.db_conn.fetchrow("""
            SELECT 
                COUNT(DISTINCT cm.primary_ticker) as companies_in_master,
                COUNT(DISTINCT p.symbol) as symbols_with_prices,
                COUNT(DISTINCT CASE WHEN p.symbol IS NOT NULL THEN cm.primary_ticker END) as companies_with_prices
            FROM company_master cm
            LEFT JOIN daily_price_data p ON cm.primary_ticker = p.symbol
        """)
        
        print(f"\n📊 UPDATED DATABASE STATISTICS:")
        print(f"   Total companies in master: {total_companies:,}")
        print(f"   Symbols with price data: {total_price_symbols:,}")
        print(f"   Companies with price data: {coverage['companies_with_prices']:,}")
        print(f"   Price coverage: {coverage['companies_with_prices'] / total_companies * 100:.1f}%")
        
        # Show A/B share coverage
        ab_coverage = await self.db_conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE primary_ticker ~ ' [ABC]$' OR primary_ticker ~ '-[ABC]$') as ab_shares_in_master,
                COUNT(*) FILTER (WHERE primary_ticker ~ ' [ABC]$' OR primary_ticker ~ '-[ABC]$' 
                                 AND EXISTS(SELECT 1 FROM daily_price_data WHERE symbol = primary_ticker)) as ab_shares_with_prices
            FROM company_master
        """)
        
        print(f"\n🎯 A/B/C SHARE COVERAGE:")
        print(f"   A/B/C shares in master: {ab_coverage['ab_shares_in_master']:,}")
        print(f"   A/B/C shares with prices: {ab_coverage['ab_shares_with_prices']:,}")
        
        if ab_coverage['ab_shares_in_master'] > 0:
            ab_price_coverage = ab_coverage['ab_shares_with_prices'] / ab_coverage['ab_shares_in_master'] * 100
            print(f"   A/B/C price coverage: {ab_price_coverage:.1f}%")
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the instrument addition process."""
    
    import sys
    
    # Check for execute flag
    execute = '--execute' in sys.argv
    
    adder = InstrumentAdder()
    
    try:
        await adder.setup()
        
        # Find missing symbols
        missing_symbols = await adder.get_missing_symbols()
        
        if not missing_symbols:
            print("✅ No missing symbols found. Company master is up to date!")
            await adder.show_summary_stats()
            return
            
        # Add missing symbols (dry run by default)
        added_count = await adder.add_missing_symbols(missing_symbols, dry_run=not execute)
        
        if execute and added_count > 0:
            print(f"\n🎉 Successfully added {added_count} new companies!")
            await adder.show_summary_stats()
            
            print(f"\n💰 Next steps:")
            print(f"   1. Run price data collection for new symbols")
            print(f"   2. Update A/B share analysis")
            print(f"   3. Run fundamental data collection if needed")
        
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await adder.cleanup()

if __name__ == "__main__":
    asyncio.run(main())