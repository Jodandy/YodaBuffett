#!/usr/bin/env python3
"""
Ingest ALL 787 companies from the company_master table.
This replaces the old script that only looked at market_data_symbols.
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import sys
from pathlib import Path
import time

# Add domains to path
sys.path.append(str(Path(__file__).parent))

async def get_max_history_safely(yahoo_symbol: str) -> Optional[int]:
    """Get maximum available history for a symbol with error handling"""
    try:
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(period="max")
        
        if not hist.empty:
            first_date = hist.index[0].date()
            last_date = hist.index[-1].date()
            days_available = (last_date - first_date).days
            
            # Verify we got real data
            if days_available > 30 and len(hist) > 50:
                return days_available
        
        return None
        
    except Exception:
        return None

async def store_price_data_directly(conn, company_id: str, symbol: str, yahoo_symbol: str, days_back: int) -> bool:
    """Store price data directly without using the old ingestor"""
    
    try:
        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        print(f"           📅 Fetching {yahoo_symbol} from {start_date} to {end_date}")
        
        # Fetch from Yahoo
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)
        
        if hist.empty:
            print(f"           ❌ No data returned for {yahoo_symbol}")
            return False
        
        # Store price data
        insert_count = 0
        for date_idx, row in hist.iterrows():
            if not (row['Open'] != row['Open'] or row['Close'] != row['Close']):  # Check for NaN
                try:
                    await conn.execute("""
                        INSERT INTO daily_price_data (
                            symbol, company_id, date, open_price, high_price, low_price, 
                            close_price, adjusted_close, volume, provider
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT (symbol, date, provider) DO UPDATE SET
                            open_price = EXCLUDED.open_price,
                            high_price = EXCLUDED.high_price,
                            low_price = EXCLUDED.low_price,
                            close_price = EXCLUDED.close_price,
                            adjusted_close = EXCLUDED.adjusted_close,
                            volume = EXCLUDED.volume,
                            created_at = NOW()
                    """, 
                        symbol, company_id, date_idx.date(),
                        float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']),
                        float(row['Adj Close']), 
                        int(row['Volume']) if row['Volume'] == row['Volume'] else None,  # NaN check
                        'yahoo_finance'
                    )
                    insert_count += 1
                except Exception as e:
                    if insert_count == 0:  # Only print first error
                        print(f"           ⚠️  Insert error: {str(e)[:50]}")
        
        print(f"           ✅ Stored {insert_count} price points")
        return insert_count > 0
        
    except Exception as e:
        print(f"           ❌ Error: {str(e)[:50]}")
        return False

async def ingest_all_787_companies():
    """Ingest historical data for ALL 787 companies in company_master"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get ALL companies from company_master, prioritized by document count
        companies = await conn.fetch("""
            SELECT 
                id,
                company_name,
                primary_ticker,
                yahoo_symbol,
                document_count,
                symbol_confidence,
                data_quality_score,
                country
            FROM company_master
            ORDER BY 
                document_count DESC NULLS LAST,
                data_quality_score DESC,
                symbol_confidence DESC
        """)
        
        print(f"🚀 Starting historical data ingestion for {len(companies)} companies")
        print("🎯 Prioritized by document volume and data quality")
        print("=" * 80)
        
        results = {
            'total': len(companies),
            'attempted': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'stats': []
        }
        
        start_time = time.time()
        
        for i, company in enumerate(companies, 1):
            company_id = company['id']
            company_name = company['company_name']
            symbol = company['primary_ticker']
            yahoo_symbol = company['yahoo_symbol']
            doc_count = company['document_count'] or 0
            confidence = company['symbol_confidence'] or 'unknown'
            
            # Priority indicators
            if doc_count >= 50:
                priority_icon = "🔥"
                priority = "HIGH"
            elif doc_count >= 20:
                priority_icon = "📈"
                priority = "MED"
            elif doc_count >= 5:
                priority_icon = "📄"
                priority = "LOW"
            else:
                priority_icon = "❓"
                priority = "MIN"
            
            print(f"\n[{i:3}/{len(companies)}] {priority_icon} {priority} | {symbol:8} | {company_name[:40]:40}")
            print(f"           Docs: {doc_count:3} | Confidence: {confidence:8} | Yahoo: {yahoo_symbol}")
            
            results['attempted'] += 1
            
            # Check if we already have data
            existing_count = await conn.fetchval("""
                SELECT COUNT(*) FROM daily_price_data 
                WHERE symbol = $1 OR company_id = $2
            """, symbol, company_id)
            
            if existing_count > 100:
                print(f"           ✅ Already has {existing_count} price points - skipping")
                results['skipped'] += 1
                continue
            
            # Check available history
            max_days = await get_max_history_safely(yahoo_symbol)
            
            if max_days:
                years = max_days / 365.25
                print(f"           📊 Available: {years:.1f} years ({max_days} days)")
                
                # Attempt to fetch and store
                success = await store_price_data_directly(
                    conn, company_id, symbol, yahoo_symbol, max_days + 30
                )
                
                if success:
                    results['success'] += 1
                    results['stats'].append({
                        'symbol': symbol,
                        'company': company_name,
                        'docs': doc_count,
                        'years': years,
                        'confidence': confidence
                    })
                else:
                    results['failed'] += 1
                    
            else:
                results['failed'] += 1
                print(f"           ❌ No Yahoo Finance data available")
            
            # Progress update every 50 companies
            if i % 50 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed * 60  # companies per minute
                remaining = (len(companies) - i) / rate if rate > 0 else 0
                
                print(f"\n🏃‍♂️ Progress: {i}/{len(companies)} ({i/len(companies)*100:.1f}%)")
                print(f"   ⏱️  Rate: {rate:.1f} companies/min | ETA: {remaining:.0f} min")
                print(f"   📊 Success: {results['success']} | Failed: {results['failed']} | Skipped: {results['skipped']}")
            
            # Respectful delay
            if doc_count >= 20:
                await asyncio.sleep(1)  # 1 sec for high priority
            elif doc_count >= 5:
                await asyncio.sleep(2)  # 2 sec for medium priority  
            else:
                await asyncio.sleep(3)  # 3 sec for low priority
        
        # Final summary
        elapsed_total = time.time() - start_time
        
        print(f"\n" + "=" * 80)
        print(f"🎉 ALL 787 COMPANIES INGESTION COMPLETE!")
        print(f"=" * 80)
        print(f"📊 Results:")
        print(f"   Total Companies: {results['total']}")
        print(f"   Attempted: {results['attempted']}")
        print(f"   ✅ Success: {results['success']}")
        print(f"   ❌ Failed: {results['failed']}")
        print(f"   ⏭️  Skipped (already had data): {results['skipped']}")
        print(f"   Success Rate: {results['success']/max(results['attempted'],1)*100:.1f}%")
        print(f"   ⏱️  Total Time: {elapsed_total/60:.1f} minutes")
        
        # Top companies by data volume
        if results['stats']:
            results['stats'].sort(key=lambda x: x['years'], reverse=True)
            
            print(f"\n🏆 Top 20 Companies by Historical Data:")
            print(f"{'Symbol':8} {'Company':30} {'Docs':5} {'Years':6} {'Confidence':10}")
            print("-" * 70)
            
            for stat in results['stats'][:20]:
                print(f"{stat['symbol']:8} {stat['company'][:29]:30} "
                      f"{stat['docs']:5} {stat['years']:6.1f} {stat['confidence']:10}")
        
        # Save results
        import json
        with open('all_787_companies_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n💾 Detailed results saved to: all_787_companies_results.json")
        
        # Update company master with data availability
        await conn.execute("""
            UPDATE company_master SET 
                yahoo_finance_available = EXISTS(
                    SELECT 1 FROM daily_price_data 
                    WHERE company_id = company_master.id
                ),
                updated_at = NOW()
        """)
        
        print(f"✅ Updated company_master with data availability flags")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🌟 COMPLETE 787 COMPANIES HISTORICAL DATA INGESTION")
    print("Processing ALL companies from company_master table")
    print("This will take 2-4 hours but gives comprehensive Nordic coverage!")
    print("=" * 80)
    
    asyncio.run(ingest_all_787_companies())