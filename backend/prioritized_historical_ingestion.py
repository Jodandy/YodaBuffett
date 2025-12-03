#!/usr/bin/env python3
"""
Prioritized historical data ingestion for ALL document companies.
Focuses on high-value companies first (most documents).
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, date, timedelta
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.market_data.services.historical_data_ingestor import HistoricalDataIngestor

async def get_max_history_safely(yahoo_symbol: str) -> int:
    """Get maximum available history for a symbol with error handling"""
    try:
        ticker = yf.Ticker(yahoo_symbol)
        
        # Try different approaches to get max history
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

async def prioritized_ingestion():
    """Ingest historical data prioritized by document volume"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get symbols prioritized by document count
        symbols = await conn.fetch("""
            SELECT 
                s.symbol, 
                s.company_name, 
                s.yahoo_symbol,
                COUNT(ed.id) as total_docs,
                s.last_data_fetch
            FROM market_data_symbols s
            LEFT JOIN extracted_documents ed ON ed.company_name = s.document_company_name
            GROUP BY s.symbol, s.company_name, s.yahoo_symbol, s.last_data_fetch
            ORDER BY COUNT(ed.id) DESC NULLS LAST, s.symbol
        """)
        
        print(f"📊 Found {len(symbols)} symbols in database")
        print("🎯 Starting prioritized historical data ingestion")
        print("=" * 70)
        
        # Create ingestor
        ingestor = HistoricalDataIngestor()
        await ingestor.connect()
        
        results = {
            'high_value': {'attempted': 0, 'success': 0, 'failed': 0},  # 20+ docs
            'medium_value': {'attempted': 0, 'success': 0, 'failed': 0},  # 5-19 docs  
            'low_value': {'attempted': 0, 'success': 0, 'failed': 0},    # 1-4 docs
            'no_docs': {'attempted': 0, 'success': 0, 'failed': 0},     # 0 docs
            'stats': []
        }
        
        for i, symbol_row in enumerate(symbols, 1):
            symbol = symbol_row['symbol']
            company_name = symbol_row['company_name']
            yahoo_symbol = symbol_row['yahoo_symbol']
            total_docs = symbol_row['total_docs'] or 0
            
            # Categorize by document volume
            if total_docs >= 20:
                category = 'high_value'
                priority_icon = "🔥"
            elif total_docs >= 5:
                category = 'medium_value' 
                priority_icon = "📈"
            elif total_docs >= 1:
                category = 'low_value'
                priority_icon = "📄"
            else:
                category = 'no_docs'
                priority_icon = "❓"
            
            results[category]['attempted'] += 1
            
            print(f"\n[{i:3}/{len(symbols)}] {priority_icon} {symbol:8} - {company_name[:35]:35}")
            print(f"           Documents: {total_docs:3} | Yahoo: {yahoo_symbol}")
            
            # Check if data already exists
            existing_count = await conn.fetchval("""
                SELECT COUNT(*) FROM daily_price_data WHERE symbol = $1
            """, symbol)
            
            if existing_count > 100:
                print(f"           ✅ Already has {existing_count} price points - skipping")
                results[category]['success'] += 1
                continue
            
            # Check how much history is available
            max_days = await get_max_history_safely(yahoo_symbol)
            
            if max_days:
                years = max_days / 365.25
                print(f"           📅 Available: {years:.1f} years ({max_days} days)")
                
                # Fetch with buffer
                try:
                    success = await ingestor.ingest_historical_data(
                        symbol, 
                        days_back=max_days + 30,
                        calculate_metrics=True
                    )
                    
                    if success:
                        # Verify what we actually got
                        final_count = await conn.fetchval("""
                            SELECT COUNT(*) FROM daily_price_data WHERE symbol = $1
                        """, symbol)
                        
                        results[category]['success'] += 1
                        print(f"           ✅ Success! Stored {final_count} price points")
                        
                        results['stats'].append({
                            'symbol': symbol,
                            'company': company_name,
                            'docs': total_docs,
                            'years': years,
                            'price_points': final_count
                        })
                        
                    else:
                        results[category]['failed'] += 1
                        print(f"           ❌ Failed to ingest data")
                        
                except Exception as e:
                    results[category]['failed'] += 1
                    print(f"           ❌ Error: {str(e)[:50]}")
            else:
                results[category]['failed'] += 1
                print(f"           ⚠️  No data available from Yahoo Finance")
            
            # Progressive delay - faster for high-value companies
            if category == 'high_value':
                await asyncio.sleep(1)  # 1 second for high priority
            elif category == 'medium_value':
                await asyncio.sleep(2)  # 2 seconds for medium priority
            else:
                await asyncio.sleep(3)  # 3 seconds for low priority
        
        await ingestor.disconnect()
        
        # Print comprehensive summary
        print("\n" + "=" * 70)
        print("📊 PRIORITIZED INGESTION SUMMARY")
        print("=" * 70)
        
        total_attempted = sum(r['attempted'] for r in results.values() if isinstance(r, dict))
        total_success = sum(r['success'] for r in results.values() if isinstance(r, dict))
        total_failed = sum(r['failed'] for r in results.values() if isinstance(r, dict))
        
        print(f"📈 Overall Results:")
        print(f"   Total Attempted: {total_attempted}")
        print(f"   Total Success: {total_success}")
        print(f"   Total Failed: {total_failed}")
        print(f"   Success Rate: {total_success/total_attempted*100:.1f}%")
        
        print(f"\n🎯 Results by Priority:")
        for cat_name, cat_data in results.items():
            if isinstance(cat_data, dict):
                success_rate = cat_data['success'] / max(cat_data['attempted'], 1) * 100
                print(f"   {cat_name:12}: {cat_data['success']:3}/{cat_data['attempted']:3} "
                      f"({success_rate:5.1f}%) - {cat_data['failed']} failed")
        
        # Show top performers by data volume
        if results['stats']:
            results['stats'].sort(key=lambda x: x['price_points'], reverse=True)
            
            print(f"\n🏆 Top 20 by Historical Data Volume:")
            print(f"{'Symbol':8} {'Company':25} {'Docs':5} {'Years':6} {'Points':8}")
            print("-" * 65)
            
            for stat in results['stats'][:20]:
                print(f"{stat['symbol']:8} {stat['company'][:24]:25} "
                      f"{stat['docs']:5} {stat['years']:6.1f} {stat['price_points']:8,}")
        
        # Save detailed results
        import json
        with open('prioritized_ingestion_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n💾 Detailed results saved to: prioritized_ingestion_results.json")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🎯 PRIORITIZED HISTORICAL DATA INGESTION")
    print("Processing companies by document volume priority")
    print("Higher document counts = better temporal anomaly potential")
    print("=" * 70)
    
    asyncio.run(prioritized_ingestion())