#!/usr/bin/env python3
"""
Analyze DCF Pipeline Coverage
Check fundamental data → DCF creation pipeline to understand low DCF count
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta

async def analyze_dcf_pipeline():
    db_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(db_url)
    
    try:
        print("="*80)
        print("DCF PIPELINE COVERAGE ANALYSIS")
        print("="*80)
        
        # 1. Check fundamental data availability (the input for DCFs)
        print("\n1. FUNDAMENTAL DATA COVERAGE (DCF INPUTS)")
        print("-" * 60)
        
        # Historical fundamentals (daily ratios)
        fund_daily_count = await conn.fetchval("SELECT COUNT(*) FROM historical_fundamentals_daily")
        fund_daily_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM historical_fundamentals_daily")
        fund_date_range = await conn.fetch("SELECT MIN(date) as min_date, MAX(date) as max_date FROM historical_fundamentals_daily")
        
        print(f"Historical fundamentals daily: {fund_daily_count:,} records")
        print(f"Companies with fundamentals: {fund_daily_companies}")
        print(f"Date range: {fund_date_range[0]['min_date']} to {fund_date_range[0]['max_date']}")
        
        # Financial statements (quarterly/annual)
        financial_statements = await conn.fetchval("SELECT COUNT(*) FROM financial_statements")
        financial_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM financial_statements")
        
        print(f"Financial statements: {financial_statements:,} records")
        print(f"Companies with financial statements: {financial_companies}")
        
        # Balance sheet data
        try:
            balance_count = await conn.fetchval("SELECT COUNT(*) FROM balance_sheet_data")
            balance_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM balance_sheet_data")
            print(f"Balance sheet data: {balance_count:,} records")
            print(f"Companies with balance sheets: {balance_companies}")
        except:
            print("Balance sheet data: Table not found")
        
        # Cash flow data
        try:
            cashflow_count = await conn.fetchval("SELECT COUNT(*) FROM cash_flow_data")
            cashflow_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM cash_flow_data")
            print(f"Cash flow data: {cashflow_count:,} records")
            print(f"Companies with cash flows: {cashflow_companies}")
        except:
            print("Cash flow data: Table not found")
        
        # 2. Check overlap between fundamentals and price data
        print("\n2. FUNDAMENTAL + PRICE DATA OVERLAP")
        print("-" * 60)
        
        fund_price_overlap = await conn.fetchval("""
            SELECT COUNT(DISTINCT f.symbol)
            FROM historical_fundamentals_daily f
            INNER JOIN daily_price_data p ON f.symbol = p.symbol
        """)
        print(f"Companies with BOTH fundamentals AND price data: {fund_price_overlap}")
        
        # Companies with price but NO fundamentals
        price_no_fund = await conn.fetch("""
            SELECT p.symbol, COUNT(*) as price_records
            FROM daily_price_data p
            LEFT JOIN historical_fundamentals_daily f ON p.symbol = f.symbol
            WHERE f.symbol IS NULL
              AND p.date >= CURRENT_DATE - INTERVAL '2 years'
            GROUP BY p.symbol
            ORDER BY price_records DESC
            LIMIT 20
        """)
        
        print(f"\nTop 20 companies with PRICE data but NO fundamentals:")
        for row in price_no_fund:
            print(f"  {row['symbol']}: {row['price_records']:,} price records")
        
        # 3. DCF Creation Analysis
        print("\n3. DCF CREATION PIPELINE")
        print("-" * 60)
        
        print(f"Total DCF valuations created: {await conn.fetchval('SELECT COUNT(*) FROM dcf_valuations'):,}")
        print(f"Companies with DCF analysis: {await conn.fetchval('SELECT COUNT(DISTINCT symbol) FROM dcf_valuations')}")
        
        # Check DCF success vs failure rates
        dcf_valid = await conn.fetchval("SELECT COUNT(*) FROM dcf_valuations WHERE fair_value_median > 0")
        dcf_invalid = await conn.fetchval("SELECT COUNT(*) FROM dcf_valuations WHERE fair_value_median IS NULL OR fair_value_median <= 0")
        
        print(f"Valid DCF valuations: {dcf_valid:,} ({dcf_valid/(dcf_valid+dcf_invalid)*100:.1f}%)")
        print(f"Failed DCF valuations: {dcf_invalid:,} ({dcf_invalid/(dcf_valid+dcf_invalid)*100:.1f}%)")
        
        # 4. Gap Analysis: Companies with fundamentals but NO DCF
        print("\n4. GAP ANALYSIS: MISSING DCF ANALYSIS")
        print("-" * 60)
        
        missing_dcf = await conn.fetch("""
            SELECT DISTINCT f.symbol, COUNT(*) as fundamental_records
            FROM historical_fundamentals_daily f
            LEFT JOIN dcf_valuations d ON f.symbol = d.symbol
            WHERE d.symbol IS NULL
              AND f.date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY f.symbol
            ORDER BY fundamental_records DESC
            LIMIT 25
        """)
        
        print(f"Companies with FUNDAMENTALS but NO DCF analysis:")
        for row in missing_dcf:
            print(f"  {row['symbol']}: {row['fundamental_records']:,} fundamental records")
        
        # 5. Check DCF frequency and coverage over time
        print("\n5. DCF CREATION FREQUENCY ANALYSIS")
        print("-" * 60)
        
        dcf_by_month = await conn.fetch("""
            SELECT 
                DATE_TRUNC('month', valuation_date) as month,
                COUNT(*) as dcf_count,
                COUNT(DISTINCT symbol) as unique_companies
            FROM dcf_valuations
            WHERE valuation_date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY 1
            ORDER BY 1
        """)
        
        print("DCF creation by month (last 12 months):")
        for row in dcf_by_month:
            print(f"  {row['month'].strftime('%Y-%m')}: {row['dcf_count']} DCFs across {row['unique_companies']} companies")
        
        # 6. Top companies by fundamental data richness
        print("\n6. FUNDAMENTAL DATA RICHNESS")
        print("-" * 60)
        
        richest_data = await conn.fetch("""
            SELECT 
                symbol,
                COUNT(DISTINCT date) as unique_dates,
                MIN(date) as first_date,
                MAX(date) as last_date,
                COUNT(*) as total_records
            FROM historical_fundamentals_daily
            GROUP BY symbol
            ORDER BY unique_dates DESC
            LIMIT 15
        """)
        
        print("Companies with most fundamental data:")
        print("Symbol    | Days  | From       | To         | Records")
        print("-" * 55)
        for row in richest_data:
            print(f"{row['symbol']:<9} | {row['unique_dates']:<5} | {row['first_date']} | {row['last_date']} | {row['total_records']:,}")
        
        # 7. Check if DCF creation is limited by specific fundamental data requirements
        print("\n7. DCF INPUT REQUIREMENTS CHECK")
        print("-" * 60)
        
        # Check which fundamental metrics are most sparse
        try:
            sparse_metrics = await conn.fetch("""
                SELECT 
                    symbol,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN pe_ratio IS NOT NULL AND pe_ratio > 0 THEN 1 ELSE 0 END) as has_pe,
                    SUM(CASE WHEN pb_ratio IS NOT NULL AND pb_ratio > 0 THEN 1 ELSE 0 END) as has_pb,
                    SUM(CASE WHEN ps_ratio IS NOT NULL AND ps_ratio > 0 THEN 1 ELSE 0 END) as has_ps,
                    SUM(CASE WHEN ev_ebitda IS NOT NULL AND ev_ebitda > 0 THEN 1 ELSE 0 END) as has_ev_ebitda
                FROM historical_fundamentals_daily
                GROUP BY symbol
                HAVING COUNT(*) >= 100
                ORDER BY total_records DESC
                LIMIT 10
            """)
            
            print("Fundamental metrics availability (top 10 companies):")
            print("Symbol    | Total | PE%   | PB%   | PS%   | EV/EBITDA%")
            print("-" * 55)
            for row in sparse_metrics:
                total = row['total_records']
                pe_pct = (row['has_pe'] / total) * 100
                pb_pct = (row['has_pb'] / total) * 100  
                ps_pct = (row['has_ps'] / total) * 100
                ev_pct = (row['has_ev_ebitda'] / total) * 100
                print(f"{row['symbol']:<9} | {total:<5} | {pe_pct:<5.1f} | {pb_pct:<5.1f} | {ps_pct:<5.1f} | {ev_pct:<5.1f}")
        except Exception as e:
            print(f"Could not check metric sparsity: {e}")
        
        # 8. Expansion recommendations
        print("\n8. EXPANSION RECOMMENDATIONS")
        print("-" * 60)
        
        # Count potential DCF candidates
        potential_candidates = len(missing_dcf)
        current_dcf_companies = await conn.fetchval("SELECT COUNT(DISTINCT symbol) FROM dcf_valuations")
        
        print(f"Current DCF companies: {current_dcf_companies}")
        print(f"Potential new DCF companies: {potential_candidates}")
        print(f"Potential total: {current_dcf_companies + potential_candidates} companies")
        
        expansion_factor = (current_dcf_companies + potential_candidates) / current_dcf_companies
        print(f"Potential DCF expansion: {expansion_factor:.1f}x current size")
        
        # Estimate additional DCF valuations
        avg_dcf_per_company = await conn.fetchval("""
            SELECT AVG(dcf_count)::int 
            FROM (SELECT symbol, COUNT(*) as dcf_count FROM dcf_valuations GROUP BY symbol) t
        """)
        
        potential_dcf_valuations = potential_candidates * avg_dcf_per_company
        print(f"Potential additional DCF valuations: ~{potential_dcf_valuations:,}")
        print(f"(Current: {await conn.fetchval('SELECT COUNT(*) FROM dcf_valuations'):,}, Potential total: ~{potential_dcf_valuations + 4861:,})")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_dcf_pipeline())