#!/usr/bin/env python3
"""
Ingest ALL available historical data for ALL 787 companies.
Gets maximum available history (up to 20+ years) for each company.
No skipping - processes every single company.
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
    
    def add_failed_other(self, company_info: dict, error_detail: str):
        """Track other failures"""
        self.results['failed_other'].append({
            **company_info,
            'error': error_detail,
            'reason': 'Other error',
            'failed_at': datetime.now().isoformat()
        })
    
    def save_results(self, filename: str = None):
        """Save comprehensive results to JSON file"""
        if not filename:
            filename = f'max_history_ingestion_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        return filename

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

async def store_max_price_data(conn, company_info: dict, yahoo_symbol: str) -> Dict[str, any]:
    """Store ALL available historical price data for the company"""
    
    result = {
        'success': False,
        'price_points_stored': 0,
        'years_stored': 0,
        'first_date': None,
        'last_date': None,
        'error': None,
        'error_type': 'unknown'
    }
    
    try:
        # Fetch ALL available historical data from Yahoo
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(period="max", auto_adjust=False)
        
        if hist.empty:
            result['error'] = 'No data returned from Yahoo Finance'
            result['error_type'] = 'no_data'
            return result
        
        # Store ALL price data
        insert_count = 0
        first_error = None
        first_date = None
        last_date = None
        
        today = date.today()

        for date_idx, row in hist.iterrows():
            # Skip today's data - only store confirmed closing prices
            if date_idx.date() >= today:
                continue

            # Skip rows with NaN values
            if (row['Open'] != row['Open'] or row['Close'] != row['Close'] or
                row['High'] != row['High'] or row['Low'] != row['Low']):
                continue
                
            try:
                # Store using existing table structure
                await conn.execute("""
                    INSERT INTO daily_price_data (
                        symbol, date, open_price, high_price, low_price, 
                        close_price, adjusted_close, volume, provider, company_id
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (symbol, date, provider) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        adjusted_close = EXCLUDED.adjusted_close,
                        volume = EXCLUDED.volume
                """, 
                    company_info['primary_ticker'],
                    date_idx.date(),
                    float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']),
                    float(row['Adj Close']), 
                    int(row['Volume']) if row['Volume'] == row['Volume'] else None,
                    'yahoo_finance',
                    company_info['id']
                )
                
                insert_count += 1
                if first_date is None:
                    first_date = date_idx.date()
                last_date = date_idx.date()
                
            except Exception as e:
                if not first_error:
                    first_error = str(e)
                    result['error_type'] = 'database_error'
                # Continue trying other rows
        
        if insert_count > 0:
            result['success'] = True
            result['price_points_stored'] = insert_count
            result['first_date'] = first_date
            result['last_date'] = last_date
            if first_date and last_date:
                result['years_stored'] = (last_date - first_date).days / 365.25
        elif first_error:
            result['error'] = first_error
            result['error_type'] = 'database_error'
        
        return result
        
    except Exception as e:
        result['error'] = str(e)
        result['error_type'] = 'fetch_error'
        return result

async def ingest_all_max_history(only_missing: bool = False, limit: int = None):
    """Ingest ALL available historical data for companies.

    Args:
        only_missing: If True, only process companies without existing price data
        limit: Max number of companies to process
    """

    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)

    tracker = CompanyIngestionTracker()

    try:
        # Build query based on mode
        if only_missing:
            query = """
                SELECT
                    cm.id,
                    cm.company_name,
                    cm.primary_ticker,
                    cm.yahoo_symbol,
                    cm.document_count,
                    cm.symbol_confidence,
                    cm.data_quality_score,
                    cm.country
                FROM company_master cm
                WHERE cm.primary_ticker IS NOT NULL
                AND cm.yahoo_symbol IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM daily_price_data dp
                    WHERE dp.symbol = cm.primary_ticker
                )
                ORDER BY
                    cm.document_count DESC NULLS LAST,
                    cm.data_quality_score DESC
            """
        else:
            query = """
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
                WHERE primary_ticker IS NOT NULL
                AND yahoo_symbol IS NOT NULL
                ORDER BY
                    document_count DESC NULLS LAST,
                    data_quality_score DESC
            """

        if limit:
            query += f" LIMIT {limit}"

        companies = await conn.fetch(query)
        
        tracker.results['total_companies'] = len(companies)
        tracker.results['stats']['start_time'] = datetime.now().isoformat()

        mode = "MISSING ONLY" if only_missing else "FULL REFRESH"
        print(f"🚀 MAXIMUM HISTORICAL DATA INGESTION - {mode}")
        print(f"📊 Processing {len(companies)} companies with ALL available historical data")
        if only_missing:
            print(f"🎯 Only companies without existing price data")
        else:
            print(f"🎯 Full refresh - getting complete dataset for every company!")
        print("=" * 100)
        
        start_time = time.time()
        total_price_points = 0
        
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
            
            print(f"\n[{i:3}/{len(companies)}] {priority_icon} {priority} | {company_info['primary_ticker']:8} | {company_info['company_name'][:45]:45}")
            print(f"           Yahoo: {company_info['yahoo_symbol']} | Docs: {doc_count:3} | Confidence: {company_info['symbol_confidence']}")
            
            tracker.results['attempted'] += 1
            
            # Check existing data (for information only - we'll get ALL available data anyway)
            existing_count = await conn.fetchval("""
                SELECT COUNT(*) FROM daily_price_data 
                WHERE symbol = $1
            """, company_info['primary_ticker'])
            
            if existing_count > 0:
                print(f"           📊 Currently has {existing_count} price points - getting ALL available data")
            else:
                print(f"           📊 No existing data - getting ALL available historical data")
            
            # Validate Yahoo symbol first
            validation = await get_max_history_with_validation(company_info['yahoo_symbol'])
            
            if validation['validation_status'] == 'good_data':
                years = validation['years_available']
                print(f"           📈 Available: {years:.1f} years ({validation['data_points']} points) from {validation['first_date']} to {validation['last_date']}")
                
                # Store ALL available historical data
                storage_result = await store_max_price_data(
                    conn, company_info, company_info['yahoo_symbol']
                )
                
                if storage_result['success']:
                    points_stored = storage_result['price_points_stored']
                    years_stored = storage_result['years_stored']
                    total_price_points += points_stored
                    print(f"           ✅ SUCCESS! Stored {points_stored:,} price points ({years_stored:.1f} years)")
                    print(f"              📅 Data range: {storage_result['first_date']} → {storage_result['last_date']}")
                    tracker.add_success(company_info, points_stored, years_stored)
                else:
                    print(f"           ❌ Storage failed: {storage_result['error'][:70]}")
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
            
            # Progress updates every 20 companies
            if i % 20 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed * 60 if elapsed > 0 else 0
                avg_points = total_price_points / max(len(tracker.results['success']), 1)
                
                print(f"\n" + "="*80)
                print(f"📊 PROGRESS REPORT: {i}/{len(companies)} ({i/len(companies)*100:.1f}%)")
                print(f"   ✅ Successful: {len(tracker.results['success'])}")
                print(f"   📈 Total price points stored: {total_price_points:,}")
                print(f"   📊 Average points per company: {avg_points:.0f}")
                print(f"   ❌ Wrong tickers: {len(tracker.results['failed_wrong_ticker'])}")
                print(f"   🔧 DB errors: {len(tracker.results['failed_database_error'])}")
                print(f"   🏃‍♂️ Processing rate: {rate:.1f} companies/min")
                print("="*80)
            
            # Small delay to be respectful to Yahoo Finance
            await asyncio.sleep(1.5)
        
        # Finalize results
        tracker.results['stats']['end_time'] = datetime.now().isoformat()
        tracker.results['stats']['duration_minutes'] = (time.time() - start_time) / 60
        tracker.results['stats']['total_price_points'] = total_price_points
        
        # Save comprehensive results
        results_file = tracker.save_results()
        
        # Print final summary
        print(f"\n" + "=" * 100)
        print(f"🎉 MAXIMUM HISTORICAL DATA INGESTION COMPLETE!")
        print(f"=" * 100)
        print(f"📊 FINAL RESULTS:")
        print(f"   📈 Total Companies Processed: {tracker.results['total_companies']}")
        print(f"   ✅ Successful Ingestions: {len(tracker.results['success'])}")
        print(f"   📊 Total Price Points Stored: {total_price_points:,}")
        print(f"   🎯 Wrong Tickers (need fixing): {len(tracker.results['failed_wrong_ticker'])}")
        print(f"   🔧 Database Errors: {len(tracker.results['failed_database_error'])}")
        print(f"   ❌ Other Failures: {len(tracker.results['failed_other'])}")
        print(f"   ⏱️  Total Duration: {tracker.results['stats']['duration_minutes']:.1f} minutes")
        print(f"   📈 Success Rate: {len(tracker.results['success'])/tracker.results['total_companies']*100:.1f}%")
        
        if len(tracker.results['success']) > 0:
            avg_points = total_price_points / len(tracker.results['success'])
            print(f"   📊 Average points per successful company: {avg_points:.0f}")
            
            # Show top successful companies
            print(f"\n🏆 TOP SUCCESSFUL COMPANIES (by data volume):")
            successful_sorted = sorted(tracker.results['success'], key=lambda x: x['price_points'], reverse=True)
            for i, company in enumerate(successful_sorted[:10], 1):
                print(f"   {i:2}. {company['company_name'][:35]:35} -> {company['price_points']:,} points ({company['years_of_data']:.1f} years)")
        
        print(f"\n💾 Detailed results saved to: {results_file}")
        print(f"\n🚀 READY FOR TEMPORAL ANOMALY ANALYSIS!")
        print(f"   - Total dataset: {total_price_points:,} price points across {len(tracker.results['success'])} companies")
        print(f"   - Can now validate document temporal anomalies against actual price movements")
        print(f"   - Run backtesting framework with this comprehensive dataset")
        
    finally:
        await conn.close()
        
    return tracker.results

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Backfill historical price data from Yahoo Finance')
    parser.add_argument('--only-missing', action='store_true',
                        help='Only process companies without existing price data')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of companies to process')
    args = parser.parse_args()

    print("🌟 MAXIMUM HISTORICAL DATA INGESTION FOR ALL COMPANIES")
    print("Gets ALL available historical data (up to 20+ years) for every single company")
    if args.only_missing:
        print("Mode: MISSING ONLY - filling gaps in price data coverage")
    else:
        print("Mode: FULL REFRESH - comprehensive dataset for all companies")
    print("=" * 100)

    asyncio.run(ingest_all_max_history(only_missing=args.only_missing, limit=args.limit))