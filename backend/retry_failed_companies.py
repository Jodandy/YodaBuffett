#!/usr/bin/env python3
"""
Smart retry script for companies with 0 documents
Automatically tests suffix patterns like -holding, -group, -ab
"""
import asyncio
import aiohttp
import json
import sys
import os
import time
from datetime import datetime
from typing import List, Dict, Tuple, Optional

# Add backend path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents

class SmartRetrySystem:
    def __init__(self):
        self.collector = MFNCollector(rate_limit_delay=1.0)
        self.suffix_patterns = ['', '-holding', '-group', '-ab', '-corp']
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Results tracking
        self.retry_results = {
            'session_id': f'retry_{self.session_id}',
            'start_time': datetime.now().isoformat(),
            'companies_retried': 0,
            'successful_fixes': [],
            'still_failing': [],
            'new_mappings_found': {}
        }
        
    def load_failed_companies(self, historical_file: str) -> List[Dict]:
        """Load companies with 0 documents from historical ingestion results"""
        try:
            with open(historical_file, 'r') as f:
                data = json.load(f)
                
            failed_companies = []
            for entry in data.get('completed', []):
                # Company failed if it has 0 documents but found items (processing error)
                # OR if it had errors/exceptions
                if (entry.get('documents', 0) == 0 and 
                    (entry.get('details', {}).get('items_found', 0) > 0 or
                     entry.get('error') is not None or
                     entry.get('success') is False)):
                    
                    failed_companies.append({
                        'slug': entry['company'],
                        'name': entry.get('company_name', entry['company']),
                        'original_items_found': entry.get('details', {}).get('items_found', 0),
                        'original_error': entry.get('error'),
                        'documents_stored': entry.get('documents', 0)
                    })
                    
            print(f"📋 Found {len(failed_companies)} companies to retry")
            return failed_companies
            
        except Exception as e:
            print(f"❌ Error loading historical data: {e}")
            return []
    
    async def test_slug_variants(self, session: aiohttp.ClientSession, base_slug: str) -> Tuple[Optional[str], int]:
        """Test different suffix patterns for a company slug"""
        
        for suffix in self.suffix_patterns:
            test_slug = base_slug + suffix
            
            try:
                # Quick test to see if URL returns PDFs
                url = f"https://mfn.se/all/a/{test_slug}"
                async with session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # Quick PDF count
                        pdf_count = html.lower().count('.pdf')
                        
                        if pdf_count > 5:  # Threshold for "has documents"
                            print(f"    ✅ Found working variant: {test_slug} ({pdf_count} PDF references)")
                            return test_slug, pdf_count
                        else:
                            print(f"    ⚠️  {test_slug}: {pdf_count} PDF references")
                            
                await asyncio.sleep(0.3)  # Rate limiting
                
            except Exception as e:
                print(f"    ❌ {test_slug}: {e}")
                continue
                
        return None, 0
    
    def save_chunk_to_db(self, items):
        """Save a chunk of items to database (sync wrapper for async functions)"""
        try:
            from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
            from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events
            
            # Get the current running loop
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context, create tasks
                doc_future = asyncio.ensure_future(catalog_mfn_documents(items))
                cal_future = asyncio.ensure_future(store_mfn_calendar_events(items))
                
                # Run them concurrently
                doc_result, cal_result = loop.run_until_complete(
                    asyncio.gather(doc_future, cal_future)
                )
            except RuntimeError:
                # No event loop running, create one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # Save documents
                doc_result = loop.run_until_complete(catalog_mfn_documents(items))
                
                # Save calendar events
                cal_result = loop.run_until_complete(store_mfn_calendar_events(items))
                
                loop.close()
            
            print(f"      💾 Saved: {doc_result['stored']} docs, {doc_result['duplicates']} dups, {doc_result['errors']} errors, {cal_result.get('events', 0)} events")
            
            # Track results for summary
            if not hasattr(self, '_chunk_results'):
                self._chunk_results = []
            self._chunk_results.append({
                'stored': doc_result['stored'],
                'duplicates': doc_result['duplicates'], 
                'errors': doc_result['errors'],
                'calendar_events': cal_result.get('events', 0),
                'calendar_errors': cal_result.get('errors', 0)
            })
            
            return doc_result
            
        except Exception as e:
            print(f"      ❌ Database save error: {e}")
            import traceback
            traceback.print_exc()
            return {'stored': 0, 'duplicates': 0, 'errors': len(items)}
    
    async def retry_company(self, session: aiohttp.ClientSession, company: Dict) -> Dict:
        """Retry collection for a single company with smart slug detection"""
        
        original_slug = company['slug']
        company_name = company['name']
        
        print(f"\n🔄 Retrying: {company_name} (original: {original_slug})")
        print(f"   Original result: {company['documents_stored']} docs, {company['original_items_found']} items found")
        
        # Step 1: Test if original slug now works (maybe it was a processing issue)
        try:
            print("   Testing original slug with new collector...")
            items = await self.collector.collect_company_news(
                session, 
                original_slug, 
                limit=10,  # Small test first
                full_backfill=False,
                chunk_size=5
            )
            
            if len(items) > 0:
                print(f"   ✅ Original slug now works! Found {len(items)} items")
                
                # Test full collection
                full_items = await self.collector.collect_company_news(
                    session, 
                    original_slug, 
                    limit=0,  # Get all
                    full_backfill=False,
                    chunk_size=50,
                    save_callback=None  # We'll save after collection
                )
                
                # Save all items after collection
                doc_result = {'stored': 0, 'duplicates': 0, 'errors': 0}
                cal_result = {'events': 0, 'errors': 0}
                
                if full_items:
                    print(f"   💾 Saving {len(full_items)} items to database...")
                    from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
                    from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events
                    
                    doc_result = await catalog_mfn_documents(full_items)
                    cal_result = await store_mfn_calendar_events(full_items)
                    
                    print(f"   ✅ Saved: {doc_result['stored']} docs, {cal_result.get('events', 0)} events")
                
                return {
                    'slug': original_slug,
                    'name': company_name,
                    'status': 'fixed_original',
                    'working_slug': original_slug,
                    'items_collected': len(full_items),
                    'documents_saved': doc_result.get('stored', 0),
                    'calendar_events_saved': cal_result.get('events', 0),
                    'error': None
                }
                
        except Exception as e:
            print(f"   ❌ Original slug still fails: {e}")
        
        # Step 2: Test suffix variants
        print("   Testing suffix variants...")
        working_slug, pdf_count = await self.test_slug_variants(session, original_slug)
        
        if working_slug:
            try:
                # Full collection with working slug
                print(f"   🚀 Collecting with working slug: {working_slug}")
                
                # For now, collect without saving during collection
                full_items = await self.collector.collect_company_news(
                    session, 
                    working_slug, 
                    limit=0,  # Get all
                    full_backfill=False,
                    chunk_size=50,
                    save_callback=None  # We'll save after collection
                )
                
                # Save all items after collection
                doc_result = {'stored': 0, 'duplicates': 0, 'errors': 0}
                cal_result = {'events': 0, 'errors': 0}
                
                if full_items:
                    print(f"   💾 Saving {len(full_items)} items to database...")
                    from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
                    from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events
                    
                    doc_result = await catalog_mfn_documents(full_items)
                    cal_result = await store_mfn_calendar_events(full_items)
                    
                    print(f"   ✅ Saved: {doc_result['stored']} docs, {cal_result.get('events', 0)} events")
                
                # Track new mapping
                self.retry_results['new_mappings_found'][company_name] = working_slug
                
                return {
                    'slug': original_slug,
                    'name': company_name,
                    'status': 'fixed_variant',
                    'working_slug': working_slug,
                    'items_collected': len(full_items),
                    'documents_saved': doc_result.get('stored', 0),
                    'calendar_events_saved': cal_result.get('events', 0),
                    'error': None
                }
                
            except Exception as e:
                print(f"   ❌ Working slug collection failed: {e}")
                return {
                    'slug': original_slug,
                    'name': company_name,
                    'status': 'variant_found_but_failed',
                    'working_slug': working_slug,
                    'items_collected': 0,
                    'documents_saved': 0,
                    'calendar_events_saved': 0,
                    'error': str(e)
                }
        
        # Step 3: Still failing
        return {
            'slug': original_slug,
            'name': company_name,
            'status': 'still_failing',
            'working_slug': None,
            'items_collected': 0,
            'documents_saved': 0,
            'calendar_events_saved': 0,
            'error': 'No working variants found'
        }
    
    async def run_retry_batch(self, companies: List[Dict], max_companies: int = 20):
        """Run retry for a batch of companies"""
        
        print(f"🚀 Starting smart retry for {min(len(companies), max_companies)} companies...")
        
        async with aiohttp.ClientSession(
            headers=self.collector.session_headers,
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            
            companies_to_process = companies[:max_companies]
            
            for i, company in enumerate(companies_to_process, 1):
                print(f"\n{'='*60}")
                print(f"🏢 Company {i}/{len(companies_to_process)}: {company['name']}")
                print(f"{'='*60}")
                
                try:
                    result = await self.retry_company(session, company)
                    
                    # Track results
                    if result['status'] in ['fixed_original', 'fixed_variant']:
                        self.retry_results['successful_fixes'].append(result)
                        docs_saved = result.get('documents_saved', 0)
                        events_saved = result.get('calendar_events_saved', 0)
                        print(f"   🎉 SUCCESS: {result['items_collected']} items collected, {docs_saved} docs saved, {events_saved} events saved")
                    else:
                        self.retry_results['still_failing'].append(result)
                        print(f"   😞 STILL FAILING: {result['error']}")
                        
                    self.retry_results['companies_retried'] += 1
                    
                    # Rate limiting between companies
                    if i < len(companies_to_process):
                        print(f"   ⏱️  Rate limiting... (2s)")
                        await asyncio.sleep(2.0)
                        
                except Exception as e:
                    print(f"   💥 EXCEPTION: {e}")
                    self.retry_results['still_failing'].append({
                        'slug': company['slug'],
                        'name': company['name'],
                        'status': 'exception',
                        'working_slug': None,
                        'items_collected': 0,
                        'error': str(e)
                    })
                    
                # Save progress periodically
                if i % 5 == 0:
                    self.save_progress()
    
    def save_progress(self):
        """Save retry progress to file"""
        self.retry_results['end_time'] = datetime.now().isoformat()
        
        filename = f"retry_results_{self.session_id}.json"
        with open(filename, 'w') as f:
            json.dump(self.retry_results, f, indent=2)
            
        print(f"\n💾 Progress saved to {filename}")
    
    def print_summary(self):
        """Print summary of retry results"""
        successful = len(self.retry_results['successful_fixes'])
        still_failing = len(self.retry_results['still_failing'])
        total = self.retry_results['companies_retried']
        
        print(f"\n{'='*60}")
        print(f"🎯 RETRY SUMMARY")
        print(f"{'='*60}")
        print(f"📊 Total companies retried: {total}")
        print(f"✅ Successfully fixed: {successful}")
        print(f"❌ Still failing: {still_failing}")
        print(f"📈 Success rate: {(successful/total*100):.1f}%" if total > 0 else "N/A")
        
        if self.retry_results['new_mappings_found']:
            print(f"\n🗺️  NEW MAPPINGS DISCOVERED:")
            for company, slug in self.retry_results['new_mappings_found'].items():
                print(f"  • {company} → {slug}")
        
        print(f"\n💾 Full results saved to: retry_results_{self.session_id}.json")

async def main():
    """Main retry execution"""
    
    # Initialize retry system
    retry_system = SmartRetrySystem()
    
    # Load failed companies from historical data
    historical_file = "historical_ingestion_20250828_232443.json"
    failed_companies = retry_system.load_failed_companies(historical_file)
    
    if not failed_companies:
        print("❌ No failed companies found to retry")
        return
    
    # Ask user how many to process
    total_failed = len(failed_companies)
    print(f"\nFound {total_failed} companies that need retry.")
    
    try:
        max_retry = input(f"How many to retry? (1-{total_failed}, press Enter for 20): ").strip()
        max_retry = int(max_retry) if max_retry else 20
        max_retry = min(max_retry, total_failed)
    except:
        max_retry = 20
    
    # Run retry batch
    await retry_system.run_retry_batch(failed_companies, max_retry)
    
    # Save final results and show summary
    retry_system.save_progress()
    retry_system.print_summary()

if __name__ == "__main__":
    asyncio.run(main())