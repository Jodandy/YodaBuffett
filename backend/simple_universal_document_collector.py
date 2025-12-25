#!/usr/bin/env python3
"""
Simple Universal Document Collector

Uses existing proven scripts to collect documents for ALL companies
from company_master, avoiding complex import dependencies.
"""

import asyncio
import asyncpg
import subprocess
import json
import time
from datetime import datetime
from pathlib import Path

async def get_companies_needing_documents():
    """Get companies that need document collection"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    # Get companies with low document counts, prioritizing Swedish and Norwegian
    query = """
    SELECT 
        id,
        primary_ticker as symbol,
        company_name as name,
        country,
        primary_exchange,
        document_count,
        yahoo_finance_available,
        CASE 
            WHEN country IN ('Sverige', 'SE') THEN 1
            WHEN country IN ('Norge', 'NO') THEN 2
            WHEN country IN ('Danmark', 'DK') THEN 3
            WHEN country IN ('Finland', 'FI') THEN 4
            ELSE 5
        END as priority
    FROM company_master
    WHERE primary_ticker IS NOT NULL
    AND company_name IS NOT NULL
    AND (document_count IS NULL OR document_count < 50)
    ORDER BY priority, document_count NULLS FIRST, primary_ticker
    """
    
    companies = await conn.fetch(query)
    await conn.close()
    
    return companies

def run_swedish_collection_for_company(symbol, name):
    """Run Swedish document collection for one company"""
    
    print(f"  🇸🇪 Running Swedish collection for {symbol}...")
    
    try:
        # Use the existing MFN collector infrastructure
        # This is what historical_ingestion_batch.py does internally
        
        result = subprocess.run([
            'python3', '-c', f'''
import asyncio
import sys
sys.path.append("/Users/jdandemar/Documents/YodaBuffett/backend")

from mfn_collector import MFNCollector

async def collect_one():
    try:
        collector = MFNCollector()
        result = await collector.collect_company_by_name("{name}", symbol="{symbol}")
        if result and "documents" in result:
            print(f"SUCCESS:{symbol}:{len(result['documents'])}")
        else:
            print(f"FAILED:{symbol}:no_documents")
    except Exception as e:
        print(f"ERROR:{symbol}:{{str(e)[:50]}}")

asyncio.run(collect_one())
            '''
        ], capture_output=True, text=True, timeout=300)
        
        output = result.stdout.strip()
        if output.startswith("SUCCESS"):
            parts = output.split(":")
            doc_count = int(parts[2]) if len(parts) > 2 else 0
            return {'status': 'success', 'documents': doc_count}
        elif output.startswith("FAILED"):
            return {'status': 'failed', 'reason': 'no_documents'}
        else:
            return {'status': 'error', 'reason': output}
            
    except subprocess.TimeoutExpired:
        return {'status': 'error', 'reason': 'timeout'}
    except Exception as e:
        return {'status': 'error', 'reason': str(e)[:100]}

def run_alternative_swedish_lookup(symbol, name):
    """Try alternative Swedish company lookup methods"""
    
    print(f"  🔍 Trying alternative lookup for {symbol}...")
    
    try:
        # Use the smart slug mapping approach
        result = subprocess.run([
            'python3', '-c', f'''
import asyncio
import requests
import time
from urllib.parse import quote

async def test_company_urls():
    base_url = "https://www.mfn.se/aktie/"
    
    # Test different slug variations
    test_slugs = [
        "{name.lower().replace(' ', '-').replace('å', 'a').replace('ä', 'a').replace('ö', 'o')}",
        "{symbol.lower()}",
        "{symbol.lower().replace(' ', '-')}",
        "{name.lower().replace(' ', '')[:10]}"
    ]
    
    for slug in test_slugs:
        try:
            url = f"{{base_url}}{{slug}}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200 and len(response.content) > 1000:
                print(f"FOUND:{symbol}:{slug}")
                return
            time.sleep(0.5)
        except:
            continue
    
    print(f"NOT_FOUND:{symbol}:no_working_slug")

asyncio.run(test_company_urls())
            '''
        ], capture_output=True, text=True, timeout=60)
        
        output = result.stdout.strip()
        if output.startswith("FOUND"):
            return {'status': 'found_slug', 'slug': output.split(":")[2]}
        else:
            return {'status': 'no_slug_found'}
            
    except Exception as e:
        return {'status': 'error', 'reason': str(e)[:100]}

async def process_companies_simple():
    """Process companies using simple subprocess approach"""
    
    print("🌍 SIMPLE UNIVERSAL DOCUMENT COLLECTION")
    print("=" * 70)
    
    companies = await get_companies_needing_documents()
    
    # Group by country for analysis
    by_country = {}
    for company in companies:
        country = company['country']
        if country not in by_country:
            by_country[country] = []
        by_country[country].append(company)
    
    print(f"📊 Found {len(companies)} companies needing document collection:")
    for country, comps in by_country.items():
        print(f"   {country}: {len(comps)} companies")
    
    # Focus on Swedish and Norwegian companies first (existing ingestors work)
    processable_companies = []
    for company in companies:
        if company['country'] in ['Sverige', 'SE', 'Norge', 'NO']:
            processable_companies.append(company)
    
    print(f"\n🚀 Processing {len(processable_companies)} Swedish/Norwegian companies")
    print(f"   (Skipping {len(companies) - len(processable_companies)} Danish/Finnish companies - no ingestor)")
    
    results = {
        'successful': [],
        'failed': [],
        'errors': [],
        'skipped': [],
        'start_time': datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    for i, company in enumerate(processable_companies, 1):
        symbol = company['symbol']
        name = company['name']
        country = company['country']
        existing_docs = company['document_count'] or 0
        
        print(f"\n📈 {i}/{len(processable_companies)} - {symbol} ({country}) [Has: {existing_docs} docs]")
        
        if existing_docs >= 50:
            print(f"   ⏭️ Skipping - already has {existing_docs} documents")
            results['skipped'].append({
                'symbol': symbol, 'country': country, 
                'reason': 'sufficient_documents', 'existing_count': existing_docs
            })
            continue
        
        processing_start = time.time()
        
        # Process based on country
        if country in ['Sverige', 'SE']:
            # Try Swedish collection
            result = run_swedish_collection_for_company(symbol, name)
            
            if result['status'] == 'success':
                processing_time = time.time() - processing_start
                print(f"   ✅ Success: {result['documents']} documents ({processing_time:.1f}s)")
                results['successful'].append({
                    'symbol': symbol, 'country': country,
                    'documents': result['documents'], 'processing_time': processing_time
                })
            elif result['status'] == 'failed':
                # Try alternative lookup
                alt_result = run_alternative_swedish_lookup(symbol, name)
                processing_time = time.time() - processing_start
                
                if alt_result['status'] == 'found_slug':
                    print(f"   ⚠️ Found working slug but need manual collection: {alt_result['slug']} ({processing_time:.1f}s)")
                    results['failed'].append({
                        'symbol': symbol, 'country': country,
                        'reason': 'found_slug_manual_needed', 'slug': alt_result['slug'],
                        'processing_time': processing_time
                    })
                else:
                    print(f"   ❌ Failed: {result['reason']} ({processing_time:.1f}s)")
                    results['failed'].append({
                        'symbol': symbol, 'country': country,
                        'reason': result['reason'], 'processing_time': processing_time
                    })
            else:
                processing_time = time.time() - processing_start
                print(f"   ❌ Error: {result['reason']} ({processing_time:.1f}s)")
                results['errors'].append({
                    'symbol': symbol, 'country': country,
                    'error': result['reason'], 'processing_time': processing_time
                })
        
        elif country in ['Norge', 'NO']:
            # Norwegian companies - would need Norwegian ingestor
            print(f"   ⚠️ Norwegian company - Norwegian ingestor needed")
            results['failed'].append({
                'symbol': symbol, 'country': country,
                'reason': 'norwegian_ingestor_needed'
            })
        
        # Progress update
        if i % 25 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(processable_companies) - i) / rate if rate > 0 else 0
            
            print(f"\n📊 PROGRESS: {i}/{len(processable_companies)} ({i/len(processable_companies)*100:.0f}%)")
            print(f"   Success: {len(results['successful'])}, Failed: {len(results['failed'])}, Errors: {len(results['errors'])}")
            print(f"   Rate: {rate:.1f}/min, ETA: {eta/60:.0f} min")
        
        # Respectful delay
        if i < len(processable_companies):
            print("   ⏱️ Waiting 10s...")
            await asyncio.sleep(10)
    
    # Final summary
    results['end_time'] = datetime.now().isoformat()
    total_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("🎯 DOCUMENT COLLECTION COMPLETE")
    print("=" * 70)
    
    print(f"\n📊 FINAL RESULTS:")
    print(f"   Companies processed: {len(processable_companies)}")
    print(f"   Successful: {len(results['successful'])}")
    print(f"   Failed: {len(results['failed'])}")
    print(f"   Errors: {len(results['errors'])}")
    print(f"   Skipped (sufficient docs): {len(results['skipped'])}")
    print(f"   Success rate: {len(results['successful'])/len(processable_companies)*100:.1f}%")
    print(f"   Total time: {total_time/60:.1f} minutes")
    
    if results['successful']:
        total_docs = sum(r['documents'] for r in results['successful'])
        print(f"   Total new documents collected: {total_docs:,}")
    
    # Failure analysis
    if results['failed']:
        failure_reasons = {}
        for failed in results['failed']:
            reason = failed.get('reason', 'unknown')
            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
        
        print(f"\n❌ FAILURE ANALYSIS:")
        for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"   {reason}: {count} companies")
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"simple_universal_collection_results_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Complete results saved to {filename}")
    
    # Show some successful examples
    if results['successful']:
        print(f"\n✅ SUCCESSFUL COLLECTIONS (sample):")
        for result in results['successful'][:10]:
            print(f"   {result['symbol']}: {result['documents']} documents")
    
    return results

if __name__ == "__main__":
    asyncio.run(process_companies_simple())