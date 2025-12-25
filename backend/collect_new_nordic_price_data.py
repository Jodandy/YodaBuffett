#!/usr/bin/env python3
"""
Targeted Price Data Collection for New Nordic Companies

Specifically collects price data for companies that were recently added
from the Nordic ingestion but don't have historical price data yet.
Uses their yahoo_symbol field to fetch from Yahoo Finance.
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, date, timedelta
import json
import time

async def collect_new_nordic_price_data():
    """Collect price data for newly added Nordic companies"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("🚀 Targeted Nordic Price Data Collection")
        print("Collecting data for companies without existing price data")
        print("=" * 70)
        
        # Find companies with Yahoo symbols but no price data
        companies = await conn.fetch("""
            SELECT 
                cm.id, cm.company_name, cm.primary_ticker, cm.yahoo_symbol, 
                cm.country, cm.document_count, cm.created_at
            FROM company_master cm
            LEFT JOIN daily_price_data dpd ON dpd.symbol = cm.primary_ticker
            WHERE cm.yahoo_symbol IS NOT NULL
            AND dpd.symbol IS NULL  -- No price data exists
        """)
        
        print(f"📊 Found {len(companies)} companies without price data")
        
        if not companies:
            print("✅ All Nordic companies already have price data!")
            return
        
        results = {
            'attempted': 0,
            'success': 0,
            'failed': 0,
            'companies': []
        }
        
        for i, company in enumerate(companies, 1):
            company_id = company['id']
            company_name = company['company_name']
            ticker = company['primary_ticker']
            yahoo_symbol = company['yahoo_symbol']
            country = company['country']
            doc_count = company['document_count'] or 0
            
            print(f"\n[{i:2}/{len(companies)}] {country:8} | {ticker:10} -> {yahoo_symbol}")
            print(f"             {company_name[:50]:50} | Docs: {doc_count:3}")
            
            results['attempted'] += 1
            
            try:
                print(f"             📊 Fetching from Yahoo Finance...")
                
                yf_ticker = yf.Ticker(yahoo_symbol)
                # Try different periods - start with 1 year, fallback to shorter periods
                hist = None
                for period in ["max"]:
                    try:
                        hist = yf_ticker.history(period=period)
                        if not hist.empty:
                            break
                    except Exception:
                        continue
                
                if hist is None or hist.empty:
                    print(f"             ❌ No data from Yahoo Finance for {yahoo_symbol}")
                    results['failed'] += 1
                    results['companies'].append({
                        'company_name': company_name,
                        'ticker': ticker,
                        'yahoo_symbol': yahoo_symbol,
                        'country': country,
                        'status': 'failed',
                        'error': 'No data from Yahoo Finance',
                        'price_points': 0
                    })
                    continue
                
                print(f"             📈 Found {len(hist)} price points")
                
                # Insert data with proper error handling
                inserted = 0
                failed_inserts = 0
                
                for date_idx, row in hist.iterrows():
                    if (row['Open'] == row['Open'] and row['Close'] == row['Close'] and  # Not NaN
                        row['High'] == row['High'] and row['Low'] == row['Low'] and
                        row['Volume'] == row['Volume']):
                        
                        try:
                            await conn.execute("""
                                INSERT INTO daily_price_data (
                                    symbol, date, open_price, high_price, low_price, 
                                    close_price, volume, provider
                                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                ON CONFLICT (symbol, date, provider) DO NOTHING
                            """, 
                                ticker,  # Use primary_ticker as symbol
                                date_idx.date(),
                                float(row['Open']),
                                float(row['High']),
                                float(row['Low']),
                                float(row['Close']),
                                int(row['Volume']) if row['Volume'] == row['Volume'] else 0,
                                'yahoo_finance'
                            )
                            inserted += 1
                        except Exception as e:
                            failed_inserts += 1
                            if failed_inserts == 1:  # Only print first error
                                print(f"             ⚠️  Insert error: {str(e)[:60]}")
                
                if inserted > 0:
                    await asyncio.sleep(2)
                    print(f"             ✅ Successfully stored {inserted} price points")
                    results['success'] += 1
                    results['companies'].append({
                        'company_name': company_name,
                        'ticker': ticker,
                        'yahoo_symbol': yahoo_symbol,
                        'country': country,
                        'status': 'success',
                        'price_points': inserted,
                        'failed_inserts': failed_inserts
                    })
                else:
                    print(f"             ❌ Failed to store any data (all inserts failed)")
                    results['failed'] += 1
                    results['companies'].append({
                        'company_name': company_name,
                        'ticker': ticker,
                        'yahoo_symbol': yahoo_symbol,
                        'country': country,
                        'status': 'failed',
                        'error': 'All insert attempts failed',
                        'price_points': 0
                    })
                
            except Exception as e:
                print(f"             ❌ Yahoo fetch error: {str(e)[:60]}")
                results['failed'] += 1
                results['companies'].append({
                    'company_name': company_name,
                    'ticker': ticker,
                    'yahoo_symbol': yahoo_symbol,
                    'country': country,
                    'status': 'failed',
                    'error': str(e),
                    'price_points': 0
                })
            
            # Respectful delay for Yahoo Finance
            await asyncio.sleep(2)
        
        # Summary
        print(f"\n" + "=" * 70)
        print(f"📊 NORDIC PRICE DATA COLLECTION SUMMARY")
        print(f"=" * 70)
        print(f"Attempted: {results['attempted']}")
        print(f"✅ Success: {results['success']}")
        print(f"❌ Failed: {results['failed']}")
        print(f"Success Rate: {results['success']/max(results['attempted'],1)*100:.1f}%")
        
        if results['success'] > 0:
            print(f"\n🏆 Successfully collected data for:")
            for company in [c for c in results['companies'] if c['status'] == 'success'][:10]:
                print(f"  ✅ {company['ticker']:10} | {company['company_name'][:30]:30} -> {company['price_points']:4} points")
        
        if results['failed'] > 0:
            print(f"\n❌ Failed companies:")
            for company in [c for c in results['companies'] if c['status'] == 'failed'][:5]:
                print(f"  ❌ {company['ticker']:10} | {company['yahoo_symbol']:12} -> {company['error'][:30]}")
        
        # Save results
        results_file = f"nordic_price_collection_{date.today().strftime('%Y%m%d')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to: {results_file}")
        
        # Check overall coverage after this run
        total_companies = await conn.fetchval("""
            SELECT COUNT(*) FROM company_master 
            WHERE country IN ('Sverige', 'Norway', 'Denmark', 'Finland', 'Sweden', 'Norge', 'Danmark')
            AND yahoo_symbol IS NOT NULL
        """)
        
        with_price_data = await conn.fetchval("""
            SELECT COUNT(DISTINCT cm.id) 
            FROM company_master cm
            JOIN daily_price_data dpd ON dpd.symbol = cm.primary_ticker
            WHERE cm.country IN ('Sverige', 'Norway', 'Denmark', 'Finland', 'Sweden', 'Norge', 'Danmark')
            AND cm.yahoo_symbol IS NOT NULL
        """)
        
        print(f"\n📈 Updated Nordic Market Coverage:")
        print(f"Total Nordic companies: {total_companies}")
        print(f"With price data: {with_price_data} ({with_price_data/total_companies*100:.1f}%)")
        
        if results['success'] > 0:
            print(f"\n🎉 SUCCESS! Added price data for {results['success']} companies")
            print(f"💡 Nordic companies now have much better price data coverage!")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🌍 TARGETED NORDIC PRICE DATA COLLECTION")
    print("Collecting historical data for companies without existing price data")
    print("=" * 60)
    
    asyncio.run(collect_new_nordic_price_data())