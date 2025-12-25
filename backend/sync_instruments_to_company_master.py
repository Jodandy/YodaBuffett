#!/usr/bin/env python3
"""
Sync Instruments to Company Master

Uses all-nordic-instruments.json to:
1. Update company_master with all share classes
2. Identify missing price data
3. Fetch historical data for missing symbols
"""

import asyncio
import asyncpg
import json
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional
import pandas as pd
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InstrumentsSynchronizer:
    
    def __init__(self):
        self.db_conn = None
        self.instruments = {}
        self.missing_symbols = []
        
    async def setup(self):
        """Initialize database connection and load instruments."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Load instruments data
        with open('data/all-nordic-instruments.json', 'r') as f:
            data = json.load(f)
            self.instruments = data['instruments']  # Extract instruments array
            
        logger.info(f"📋 Loaded {len(self.instruments)} instruments from JSON")
        
    async def analyze_current_state(self):
        """Analyze current database state vs instruments.json."""
        
        print("\n" + "="*100)
        print("📊 INSTRUMENTS SYNCHRONIZATION ANALYSIS")
        print("="*100)
        
        # Get current company_master data
        company_records = await self.db_conn.fetch("""
            SELECT primary_ticker, yahoo_symbol, company_name
            FROM company_master
        """)
        
        company_tickers = {row['primary_ticker'] for row in company_records}
        yahoo_symbols = {row['yahoo_symbol'] for row in company_records if row['yahoo_symbol']}
        
        # Get price data symbols
        price_symbols = await self.db_conn.fetch("SELECT DISTINCT symbol FROM daily_price_data")
        price_set = {row['symbol'] for row in price_symbols}
        
        # Analyze instruments
        instruments_by_company = defaultdict(list)
        all_tickers = set()
        all_yahoo = set()
        share_classes = {'A': 0, 'B': 0, 'C': 0, 'Other': 0}
        
        for inst in self.instruments:
            ticker = inst.get('ticker', '')
            yahoo = inst.get('yahoo', '')
            name = inst.get('name', '')
            
            if ticker:
                all_tickers.add(ticker)
                
                # Extract company base name
                if ticker.endswith(' A') or ticker.endswith(' B') or ticker.endswith(' C'):
                    base_name = ticker[:-2]
                    class_letter = ticker[-1]
                    share_classes[class_letter] += 1
                    instruments_by_company[base_name].append(inst)
                else:
                    share_classes['Other'] += 1
                    instruments_by_company[ticker].append(inst)
                    
            if yahoo:
                all_yahoo.add(yahoo)
        
        # Find gaps
        missing_from_company_master = all_tickers - company_tickers
        missing_from_price_data = all_tickers - price_set
        companies_with_multiple_classes = {k: v for k, v in instruments_by_company.items() if len(v) > 1}
        
        # Print analysis
        print(f"\n📊 CURRENT STATE:")
        print(f"   Instruments in JSON: {len(self.instruments)}")
        print(f"   Unique tickers: {len(all_tickers)}")
        print(f"   Company Master entries: {len(company_tickers)}")
        print(f"   Price data symbols: {len(price_set)}")
        
        print(f"\n🏷️ SHARE CLASS DISTRIBUTION:")
        print(f"   A-shares: {share_classes['A']}")
        print(f"   B-shares: {share_classes['B']}")
        print(f"   C-shares: {share_classes['C']}")
        print(f"   Single class: {share_classes['Other']}")
        print(f"   Companies with multiple classes: {len(companies_with_multiple_classes)}")
        
        print(f"\n❌ GAPS IDENTIFIED:")
        print(f"   Missing from company_master: {len(missing_from_company_master)} symbols")
        print(f"   Missing from price data: {len(missing_from_price_data)} symbols")
        
        # Show examples of multi-class companies
        print(f"\n🎯 EXAMPLE COMPANIES WITH MULTIPLE SHARE CLASSES:")
        for company, instruments in list(companies_with_multiple_classes.items())[:10]:
            classes = [inst['ticker'].split()[-1] if ' ' in inst['ticker'] else 'Single' for inst in instruments]
            print(f"   {company}: {', '.join(set(classes))} classes")
            for inst in instruments:
                print(f"      {inst['ticker']} -> Yahoo: {inst.get('yahoo', 'N/A')}")
                
        return {
            'missing_from_company_master': missing_from_company_master,
            'missing_from_price_data': missing_from_price_data,
            'companies_with_multiple_classes': companies_with_multiple_classes,
            'all_tickers': all_tickers,
            'all_yahoo': all_yahoo
        }
        
    async def update_company_master(self, missing_symbols: Set[str]):
        """Add missing symbols to company_master."""
        
        print(f"\n🔧 UPDATING COMPANY MASTER...")
        
        added_count = 0
        
        for inst in self.instruments:
            ticker = inst.get('ticker', '')
            
            if ticker in missing_symbols:
                # Check if already exists
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
                        (primary_ticker, company_name, yahoo_symbol, market, industry, data_source, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """, ticker, company_name, yahoo_symbol, 'Nordic', industry, 'instruments.json', datetime.now())
                    
                    added_count += 1
                    logger.info(f"   Added: {ticker} - {company_name}")
                    
        print(f"   ✅ Added {added_count} new entries to company_master")
        
    def _map_sector_to_industry(self, sector_id: Optional[int]) -> str:
        """Map numeric sector ID to industry name."""
        
        # Basic mapping - expand as needed
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
            10: 'Utilities'
        }
        
        return sector_map.get(sector_id, 'Other')
        
    async def fetch_missing_price_data(self, missing_symbols: Set[str]):
        """Fetch historical price data for missing symbols."""
        
        print(f"\n📈 FETCHING MISSING PRICE DATA...")
        print(f"   Symbols to fetch: {len(missing_symbols)}")
        
        # Get Yahoo symbols for missing tickers
        yahoo_mapping = {}
        for inst in self.instruments:
            ticker = inst.get('ticker', '')
            yahoo = inst.get('yahoo', '')
            if ticker in missing_symbols and yahoo:
                yahoo_mapping[ticker] = yahoo
                
        success_count = 0
        failed_symbols = []
        
        # Process in batches
        batch_size = 10
        symbols_list = list(yahoo_mapping.items())
        
        for i in range(0, len(symbols_list), batch_size):
            batch = symbols_list[i:i+batch_size]
            
            for ticker, yahoo_symbol in batch:
                # For now, just mark for manual fetch
                logger.info(f"   To fetch: {ticker} (Yahoo: {yahoo_symbol})")
                failed_symbols.append(f"{ticker} -> {yahoo_symbol}")
                    
                # Rate limiting
                await asyncio.sleep(1)
                
        print(f"\n📊 SYMBOLS READY FOR PRICE DATA FETCH:")
        print(f"   Total symbols to fetch: {len(failed_symbols)}")
        
        if failed_symbols:
            print(f"\n   Example mappings (first 20):")
            for mapping in failed_symbols[:20]:
                print(f"     {mapping}")
            
    async def identify_ab_pairs(self):
        """Identify and report A/B share pairs from instruments."""
        
        print(f"\n🔍 A/B/C SHARE PAIRS ANALYSIS:")
        
        # Group by base company name
        company_shares = defaultdict(list)
        
        for inst in self.instruments:
            ticker = inst.get('ticker', '')
            
            # Extract base name and class
            if ' A' in ticker or ' B' in ticker or ' C' in ticker:
                parts = ticker.rsplit(' ', 1)
                if len(parts) == 2 and parts[1] in ['A', 'B', 'C']:
                    base_name = parts[0]
                    share_class = parts[1]
                    
                    company_shares[base_name].append({
                        'class': share_class,
                        'ticker': ticker,
                        'yahoo': inst.get('yahoo', ''),
                        'isin': inst.get('isin', '')
                    })
                    
        # Find companies with multiple classes
        multi_class_companies = {k: v for k, v in company_shares.items() if len(v) > 1}
        
        print(f"   Found {len(multi_class_companies)} companies with multiple share classes")
        
        # Show examples
        print(f"\n   📊 TOP 10 MULTI-CLASS COMPANIES:")
        for company, shares in list(multi_class_companies.items())[:10]:
            classes = sorted([s['class'] for s in shares])
            print(f"   {company}: {'/'.join(classes)} shares")
            for share in sorted(shares, key=lambda x: x['class']):
                print(f"      {share['class']}-share: {share['ticker']} (Yahoo: {share['yahoo']})")
                
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the instruments synchronization."""
    
    sync = InstrumentsSynchronizer()
    
    try:
        await sync.setup()
        
        # Analyze current state
        analysis = await sync.analyze_current_state()
        
        # Show what would be added
        if analysis['missing_from_company_master']:
            print(f"\n📋 MISSING FROM COMPANY MASTER: {len(analysis['missing_from_company_master'])} symbols")
            
            # Show examples
            missing_list = sorted(list(analysis['missing_from_company_master']))[:30]
            print(f"\nExamples of missing symbols:")
            for symbol in missing_list:
                # Find corresponding instrument
                inst = next((i for i in sync.instruments if i.get('ticker') == symbol), None)
                if inst:
                    print(f"   {symbol:<10} - {inst.get('name', 'Unknown'):<30} Yahoo: {inst.get('yahoo', 'N/A')}")
                    
        if analysis['missing_from_price_data']:
            print(f"\n💰 MISSING PRICE DATA: {len(analysis['missing_from_price_data'])} symbols")
            
            # Show share classes missing
            missing_ab = [s for s in analysis['missing_from_price_data'] if s.endswith(' A') or s.endswith(' B') or s.endswith(' C')]
            print(f"   Including {len(missing_ab)} A/B/C share classes")
                        
        # Show A/B pairs analysis
        await sync.identify_ab_pairs()
        
    except Exception as e:
        logger.error(f"Error during synchronization: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await sync.cleanup()

if __name__ == "__main__":
    asyncio.run(main())