#!/usr/bin/env python3
"""
Simple DCF Gap Analysis
"""

import asyncio
import asyncpg

async def simple_check():
    db_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(db_url)
    
    try:
        print("="*60)
        print("DCF PIPELINE GAP ANALYSIS")
        print("="*60)
        
        # Get basic counts
        fund_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM historical_fundamentals_daily")
        price_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM daily_price_data")
        dcf_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM dcf_valuations")
        
        print(f"Companies with fundamental data: {fund_companies}")
        print(f"Companies with price data: {price_companies}")
        print(f"Companies with DCF analysis: {dcf_companies}")
        
        # Check companies that have fundamentals but no DCF
        fund_symbols = await conn.fetch("SELECT DISTINCT symbol FROM historical_fundamentals_daily ORDER BY symbol")
        dcf_symbols = await conn.fetch("SELECT DISTINCT symbol FROM dcf_valuations ORDER BY symbol")
        price_symbols = await conn.fetch("SELECT DISTINCT symbol FROM daily_price_data ORDER BY symbol")
        
        fund_set = {row['symbol'] for row in fund_symbols}
        dcf_set = {row['symbol'] for row in dcf_symbols}
        price_set = {row['symbol'] for row in price_symbols}
        
        # Companies with fundamentals AND price data
        complete_data = fund_set & price_set
        print(f"\nCompanies with BOTH fundamental AND price data: {len(complete_data)}")
        
        # Companies with complete data but no DCF
        missing_dcf = complete_data - dcf_set
        print(f"Companies missing DCF analysis: {len(missing_dcf)}")
        
        print(f"\nFirst 20 companies missing DCF analysis:")
        for symbol in sorted(missing_dcf)[:20]:
            print(f"  {symbol}")
        
        # Check DCF failure rate
        total_dcf = await conn.fetchval("SELECT COUNT(*) FROM dcf_valuations")
        failed_dcf = await conn.fetchval("SELECT COUNT(*) FROM dcf_valuations WHERE fair_value_median IS NULL OR fair_value_median <= 0")
        
        print(f"\nDCF Success/Failure Analysis:")
        print(f"Total DCF attempts: {total_dcf:,}")
        print(f"Failed DCFs: {failed_dcf:,} ({failed_dcf/total_dcf*100:.1f}%)")
        print(f"Successful DCFs: {total_dcf-failed_dcf:,} ({(total_dcf-failed_dcf)/total_dcf*100:.1f}%)")
        
        # Expansion potential
        print(f"\n" + "="*60)
        print("EXPANSION POTENTIAL")
        print("="*60)
        
        current_coverage = dcf_companies / len(complete_data) * 100
        print(f"Current DCF coverage: {dcf_companies}/{len(complete_data)} = {current_coverage:.1f}%")
        
        potential_expansion = len(missing_dcf) / dcf_companies
        print(f"Potential expansion: {potential_expansion:.1f}x current size")
        
        # Estimate additional DCF valuations if we included missing companies
        avg_dcf_per_company = total_dcf / dcf_companies
        potential_new_dcfs = len(missing_dcf) * avg_dcf_per_company
        
        print(f"\nPotential new DCF valuations: ~{potential_new_dcfs:,.0f}")
        print(f"Total potential DCF valuations: ~{total_dcf + potential_new_dcfs:,.0f}")
        
        # Check top companies by fundamental data volume
        print(f"\n" + "="*60)
        print("TOP MISSING COMPANIES (by fundamental data volume)")
        print("="*60)
        
        for symbol in sorted(missing_dcf)[:10]:
            fund_count = await conn.fetchval(
                "SELECT COUNT(*) FROM historical_fundamentals_daily WHERE symbol = $1", 
                symbol
            )
            print(f"  {symbol}: {fund_count:,} fundamental records")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(simple_check())