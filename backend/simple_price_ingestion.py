#!/usr/bin/env python3
"""
Simplified price data ingestion that avoids constraint violations.
Uses minimal required fields and conservative approach.
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, date, timedelta
import json
import time

async def simple_price_ingestion():
    """Simple, conservative price data ingestion"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("🚀 Simple Price Data Ingestion")
        print("Using conservative approach to avoid constraint violations")
        print("=" * 70)
        
        # Get companies with high document counts first
        companies = await conn.fetch("""
            SELECT 
                id, company_name, primary_ticker, yahoo_symbol, document_count
            FROM company_master
            WHERE primary_ticker IS NOT NULL 
            AND yahoo_symbol IS NOT NULL
            AND yahoo_symbol LIKE '%.ST'  -- Only Swedish companies for now
            ORDER BY document_count DESC NULLS LAST
            LIMIT 50  -- Start with top 50 companies
        """)
        
        print(f"📊 Processing top {len(companies)} companies")
        
        results = {
            'attempted': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'companies': []
        }
        
        for i, company in enumerate(companies, 1):
            company_id = company['id']
            company_name = company['company_name']
            ticker = company['primary_ticker']
            yahoo_symbol = company['yahoo_symbol']
            doc_count = company['document_count'] or 0
            
            print(f"\n[{i:2}/{len(companies)}] {company_name[:40]:40} | {ticker:8} -> {yahoo_symbol}")
            print(f"             Documents: {doc_count:3}")
            
            results['attempted'] += 1
            
            # Check if we already have data
            existing = await conn.fetchval("""
                SELECT COUNT(*) FROM daily_price_data WHERE symbol = $1
            """, ticker)
            
            if existing > 50:
                print(f"             ✅ Already has {existing} price points - skipping")
                results['skipped'] += 1
                continue
            
            # Try to get Yahoo data
            try:
                print(f"             📊 Fetching from Yahoo Finance...")
                
                yf_ticker = yf.Ticker(yahoo_symbol)
                hist = yf_ticker.history(period="1y")  # Start with just 1 year
                
                if hist.empty:
                    print(f"             ❌ No data from Yahoo Finance")
                    results['failed'] += 1
                    continue
                
                print(f"             📈 Found {len(hist)} price points")
                
                # Insert data one by one with error handling
                inserted = 0
                for date_idx, row in hist.iterrows():
                    if (row['Open'] == row['Open'] and row['Close'] == row['Close'] and  # Not NaN
                        row['High'] == row['High'] and row['Low'] == row['Low']):
                        
                        try:
                            # Use minimal insert - just required fields
                            await conn.execute("""
                                INSERT INTO daily_price_data (
                                    symbol, date, open_price, high_price, low_price, 
                                    close_price, provider
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                                ON CONFLICT (symbol, date, provider) DO NOTHING
                            """, 
                                ticker,
                                date_idx.date(),
                                float(row['Open']),
                                float(row['High']),
                                float(row['Low']),
                                float(row['Close']),
                                'yahoo_finance'
                            )
                            inserted += 1
                        except Exception as e:
                            # If even this minimal insert fails, record the error
                            if inserted == 0:  # Only print error for first failure
                                print(f"             ⚠️  Insert error: {str(e)[:60]}")
                            break  # Stop trying this company
                
                if inserted > 0:
                    print(f"             ✅ Successfully stored {inserted} price points")
                    results['success'] += 1
                    results['companies'].append({
                        'company_name': company_name,
                        'ticker': ticker,
                        'yahoo_symbol': yahoo_symbol,
                        'price_points': inserted,
                        'doc_count': doc_count
                    })
                else:
                    print(f"             ❌ Failed to store any data")
                    results['failed'] += 1
                
            except Exception as e:
                print(f"             ❌ Yahoo fetch error: {str(e)[:60]}")
                results['failed'] += 1
            
            # Small delay
            await asyncio.sleep(1)
        
        # Summary
        print(f"\n" + "=" * 70)
        print(f"📊 SIMPLE INGESTION SUMMARY")
        print(f"=" * 70)
        print(f"Attempted: {results['attempted']}")
        print(f"✅ Success: {results['success']}")
        print(f"❌ Failed: {results['failed']}")
        print(f"⏭️  Skipped: {results['skipped']}")
        print(f"Success Rate: {results['success']/max(results['attempted'],1)*100:.1f}%")
        
        if results['companies']:
            print(f"\n🏆 Successful Companies:")
            for company in results['companies'][:10]:
                print(f"  ✅ {company['company_name'][:30]:30} -> {company['price_points']:4} points")
        
        # Save results
        with open('simple_ingestion_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: simple_ingestion_results.json")
        
        if results['success'] > 0:
            print(f"\n🎉 SUCCESS! Got data for {results['success']} companies")
            print(f"💡 If this worked, we can expand to more companies and longer periods")
        else:
            print(f"\n🔧 Still having issues - need to investigate constraint further")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🧪 SIMPLE CONSERVATIVE PRICE DATA INGESTION")
    print("Testing with minimal fields and top companies only")
    print("=" * 60)
    
    asyncio.run(simple_price_ingestion())