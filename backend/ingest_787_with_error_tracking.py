#!/usr/bin/env python3
"""
Improved 787 companies ingestion with error tracking and failed resolution logging.
Saves companies with wrong Yahoo tickers for manual resolution.
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import sys
from pathlib import Path
import time
import json

# Add domains to path
sys.path.append(str(Path(__file__).parent))

class CompanyIngestionTracker:
    """Track all ingestion attempts and failures for later resolution"""
    
    def __init__(self):
        self.results = {
            'total_companies': 0,
            'attempted': 0,
            'success': [],
            'failed_no_data': [],
            'failed_wrong_ticker': [],
            'failed_database_error': [],
            'failed_other': [],
            'skipped_existing': [],
            'stats': {
                'start_time': None,
                'end_time': None,
                'duration_minutes': 0
            }
        }
    
    def add_success(self, company_info: dict, price_points: int, years: float):
        """Track successful ingestion"""
        self.results['success'].append({
            **company_info,
            'price_points': price_points,
            'years_of_data': years,
            'ingested_at': datetime.now().isoformat()
        })
    
    def add_failed_no_data(self, company_info: dict):
        """Track companies where Yahoo returned no data"""
        self.results['failed_no_data'].append({
            **company_info,
            'reason': 'No data from Yahoo Finance',
            'failed_at': datetime.now().isoformat()
        })
    
    def add_failed_wrong_ticker(self, company_info: dict, error_detail: str):
        """Track companies with wrong Yahoo ticker symbols"""
        self.results['failed_wrong_ticker'].append({
            **company_info,
            'error': error_detail,
            'reason': 'Wrong/invalid Yahoo ticker',
            'needs_manual_resolution': True,
            'failed_at': datetime.now().isoformat()
        })
    
    def add_failed_database(self, company_info: dict, error_detail: str):
        """Track database constraint violations"""
        self.results['failed_database_error'].append({
            **company_info,
            'error': error_detail,
            'reason': 'Database constraint violation',
            'failed_at': datetime.now().isoformat()
        })
    
    def add_skipped(self, company_info: dict, existing_points: int):
        """Track companies skipped due to existing data"""
        self.results['skipped_existing'].append({
            **company_info,
            'existing_price_points': existing_points,
            'skipped_at': datetime.now().isoformat()
        })
    
    def save_results(self, filename: str = None):
        """Save comprehensive results to JSON file"""
        if not filename:
            filename = f'ingestion_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        return filename

async def check_daily_price_data_structure(conn):
    """Check and fix daily_price_data table structure"""
    
    print("🔧 Checking daily_price_data table structure...")
    
    # Get current columns
    columns = await conn.fetch("""
        SELECT column_name, data_type
        FROM information_schema.columns 
        WHERE table_name = 'daily_price_data'
        ORDER BY ordinal_position
    """)
    
    column_names = [col['column_name'] for col in columns]
    print(f"   Current columns: {', '.join(column_names)}")
    
    # Add missing columns if needed
    if 'company_id' not in column_names:
        print("   Adding company_id column...")
        await conn.execute("""
            ALTER TABLE daily_price_data 
            ADD COLUMN company_id UUID REFERENCES company_master(id)
        """)
    
    # Make sure we have the right structure
    await conn.execute("""
        ALTER TABLE daily_price_data 
        ALTER COLUMN symbol DROP NOT NULL;
    """)
    
    print("✅ Table structure checked and updated")

async def get_max_history_with_validation(yahoo_symbol: str) -> Dict[str, any]:
    """Get max history with detailed validation info"""
    result = {
        'days_available': None,
        'years_available': None,
        'data_points': 0,
        'first_date': None,
        'last_date': None,
        'validation_status': 'unknown',
        'error': None
    }
    
    try:
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(period="max")
        
        if hist.empty:
            result['validation_status'] = 'no_data'
            result['error'] = 'Yahoo returned empty dataset'
            return result
        
        if len(hist) < 50:
            result['validation_status'] = 'insufficient_data'
            result['error'] = f'Only {len(hist)} data points available'
            return result
        
        # Good data found
        first_date = hist.index[0].date()
        last_date = hist.index[-1].date()
        days_available = (last_date - first_date).days
        
        result.update({
            'days_available': days_available,
            'years_available': days_available / 365.25,
            'data_points': len(hist),
            'first_date': first_date,
            'last_date': last_date,
            'validation_status': 'good_data'
        })
        
        return result
        
    except Exception as e:
        result['validation_status'] = 'error'
        result['error'] = str(e)
        return result

async def store_price_data_safe(conn, company_info: dict, yahoo_symbol: str, days_back: int) -> Dict[str, any]:
    """Safely store price data with comprehensive error handling"""
    
    result = {
        'success': False,
        'price_points_stored': 0,
        'error': None,
        'error_type': 'unknown'
    }
    
    try:
        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        # Fetch from Yahoo
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)
        
        if hist.empty:
            result['error'] = 'No data returned from Yahoo Finance'
            result['error_type'] = 'no_data'
            return result
        
        # Store price data with error handling
        insert_count = 0
        first_error = None
        
        for date_idx, row in hist.iterrows():
            # Skip rows with NaN values
            if (row['Open'] != row['Open'] or row['Close'] != row['Close'] or 
                row['High'] != row['High'] or row['Low'] != row['Low']):
                continue
                
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
                        volume = EXCLUDED.volume
                """, 
                    company_info['primary_ticker'], company_info['id'], date_idx.date(),
                    float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']),
                    float(row['Adj Close']), 
                    int(row['Volume']) if row['Volume'] == row['Volume'] else None,
                    'yahoo_finance'
                )
                insert_count += 1
                
            except Exception as e:
                if not first_error:
                    first_error = str(e)
                    result['error_type'] = 'database_error'
                # Continue trying other rows
        
        if insert_count > 0:
            result['success'] = True
            result['price_points_stored'] = insert_count
        elif first_error:
            result['error'] = first_error
            result['error_type'] = 'database_error'
        
        return result
        
    except Exception as e:
        result['error'] = str(e)
        result['error_type'] = 'fetch_error'
        return result

async def ingest_787_companies_improved():
    """Improved ingestion with comprehensive error tracking"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    tracker = CompanyIngestionTracker()
    
    try:
        # Fix table structure first
        await check_daily_price_data_structure(conn)
        
        # Get ALL companies from company_master
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
                data_quality_score DESC
        """)
        
        tracker.results['total_companies'] = len(companies)
        tracker.results['stats']['start_time'] = datetime.now().isoformat()
        
        print(f"🚀 Starting improved ingestion for {len(companies)} companies")
        print("📊 With comprehensive error tracking and resolution logging")
        print("=" * 80)
        
        start_time = time.time()
        
        for i, company in enumerate(companies, 1):
            company_info = {
                'id': company['id'],
                'company_name': company['company_name'],
                'primary_ticker': company['primary_ticker'],
                'yahoo_symbol': company['yahoo_symbol'],
                'document_count': company['document_count'] or 0,
                'symbol_confidence': company['symbol_confidence'] or 'unknown',
                'data_quality_score': float(company['data_quality_score']) if company['data_quality_score'] else 0.0
            }
            
            # Priority display
            doc_count = company_info['document_count']
            if doc_count >= 50:
                priority_icon, priority = "🔥", "HIGH"
            elif doc_count >= 20:
                priority_icon, priority = "📈", "MED"
            elif doc_count >= 5:
                priority_icon, priority = "📄", "LOW"
            else:
                priority_icon, priority = "❓", "MIN"
            
            print(f"\n[{i:3}/{len(companies)}] {priority_icon} {priority} | {company_info['primary_ticker']:8} | {company_info['company_name'][:40]:40}")
            print(f"           Yahoo: {company_info['yahoo_symbol']} | Docs: {doc_count:3} | Confidence: {company_info['symbol_confidence']}")
            
            tracker.results['attempted'] += 1
            
            # Check existing data
            existing_count = await conn.fetchval("""
                SELECT COUNT(*) FROM daily_price_data 
                WHERE symbol = $1 OR company_id = $2
            """, company_info['primary_ticker'], company_info['id'])
            
            if existing_count > 100:
                print(f"           ✅ Already has {existing_count} price points - skipping")
                tracker.add_skipped(company_info, existing_count)
                continue
            
            # Validate Yahoo symbol
            validation = await get_max_history_with_validation(company_info['yahoo_symbol'])
            
            if validation['validation_status'] == 'good_data':
                years = validation['years_available']
                print(f"           📊 Available: {years:.1f} years ({validation['data_points']} points)")
                
                # Attempt to store data
                storage_result = await store_price_data_safe(
                    conn, company_info, company_info['yahoo_symbol'], 
                    validation['days_available'] + 30
                )
                
                if storage_result['success']:
                    print(f"           ✅ Success! Stored {storage_result['price_points_stored']} price points")
                    tracker.add_success(company_info, storage_result['price_points_stored'], years)
                else:
                    print(f"           ❌ Storage failed: {storage_result['error'][:50]}")
                    if storage_result['error_type'] == 'database_error':
                        tracker.add_failed_database(company_info, storage_result['error'])
                    else:
                        tracker.add_failed_other(company_info, storage_result['error'])
                        
            else:
                error_msg = validation.get('error', 'Unknown validation error')
                print(f"           ❌ Validation failed: {error_msg}")
                
                if validation['validation_status'] in ['no_data', 'insufficient_data']:
                    tracker.add_failed_wrong_ticker(company_info, error_msg)
                else:
                    tracker.add_failed_other(company_info, error_msg)
            
            # Progress updates
            if i % 25 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed * 60 if elapsed > 0 else 0
                
                print(f"\n📊 Progress: {i}/{len(companies)} ({i/len(companies)*100:.1f}%)")
                print(f"   ✅ Success: {len(tracker.results['success'])}")
                print(f"   ❌ Wrong tickers: {len(tracker.results['failed_wrong_ticker'])}")
                print(f"   🔧 DB errors: {len(tracker.results['failed_database_error'])}")
                print(f"   ⏭️  Skipped: {len(tracker.results['skipped_existing'])}")
            
            # Delay based on priority
            await asyncio.sleep(1 if doc_count >= 20 else 2)
        
        # Finalize results
        tracker.results['stats']['end_time'] = datetime.now().isoformat()
        tracker.results['stats']['duration_minutes'] = (time.time() - start_time) / 60
        
        # Save comprehensive results
        results_file = tracker.save_results()
        
        # Print summary
        print(f"\n" + "=" * 80)
        print(f"🎉 COMPREHENSIVE 787 COMPANIES INGESTION COMPLETE!")
        print(f"=" * 80)
        print(f"📊 Final Results:")
        print(f"   Total Companies: {tracker.results['total_companies']}")
        print(f"   ✅ Successful: {len(tracker.results['success'])}")
        print(f"   🎯 Wrong Tickers (need manual fix): {len(tracker.results['failed_wrong_ticker'])}")
        print(f"   🔧 Database Errors: {len(tracker.results['failed_database_error'])}")
        print(f"   ❌ Other Failures: {len(tracker.results['failed_other'])}")
        print(f"   ⏭️  Skipped (existing data): {len(tracker.results['skipped_existing'])}")
        print(f"   ⏱️  Duration: {tracker.results['stats']['duration_minutes']:.1f} minutes")
        
        print(f"\n💾 Comprehensive results saved to: {results_file}")
        print(f"\n🔧 Next steps:")
        print(f"   1. Review {len(tracker.results['failed_wrong_ticker'])} companies with wrong Yahoo tickers")
        print(f"   2. Fix database constraint issues if any")
        print(f"   3. Manually resolve ticker mappings for high-priority companies")
        
    finally:
        await conn.close()
        
    return tracker.results

if __name__ == "__main__":
    print("🌟 IMPROVED 787 COMPANIES INGESTION WITH ERROR TRACKING")
    print("Comprehensive logging for failed ticker resolutions")
    print("=" * 80)
    
    asyncio.run(ingest_787_companies_improved())