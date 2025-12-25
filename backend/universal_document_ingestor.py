#!/usr/bin/env python3
"""
Universal Multi-Market Document Ingestor

Process ALL companies from company_master across ALL Nordic markets.
Routes companies to appropriate data sources based on country.
Logs all failed resolutions for analysis.
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path
import time

# Import existing market-specific ingestors
from workers.ingestors.swedish_document_ingestor import SwedishDocumentIngestor
from workers.ingestors.norwegian_document_ingestor import NorwegianDocumentIngestor
from workers.base.document_ingestor import DocumentIngestor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UniversalDocumentIngestor:
    """Universal ingestor that routes companies to appropriate market-specific ingestors"""
    
    def __init__(self):
        self.db_conn = None
        self.results = {
            'total_companies': 0,
            'successful_companies': [],
            'failed_companies': [],
            'by_market': {},
            'processing_stats': {},
            'start_time': None,
            'end_time': None
        }
        
        # Initialize market-specific ingestors
        self.market_ingestors = {}
        
    async def setup(self):
        """Initialize database connection and market ingestors"""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        
        # Initialize market-specific ingestors
        self.market_ingestors['SE'] = SwedishDocumentIngestor()
        self.market_ingestors['Sverige'] = SwedishDocumentIngestor()
        self.market_ingestors['NO'] = NorwegianDocumentIngestor() 
        self.market_ingestors['Norge'] = NorwegianDocumentIngestor()
        
        # Setup each ingestor
        for ingestor in self.market_ingestors.values():
            await ingestor.setup()
            
        self.results['start_time'] = datetime.now().isoformat()
        
    async def cleanup(self):
        """Clean up connections"""
        if self.db_conn:
            await self.db_conn.close()
            
        # Cleanup market ingestors
        for ingestor in self.market_ingestors.values():
            await ingestor.cleanup()
    
    async def get_all_companies(self):
        """Get ALL companies from company_master across all markets"""
        
        query = """
        SELECT 
            id,
            primary_ticker as symbol,
            company_name as name,
            country,
            primary_exchange as market,
            sector,
            industry,
            document_count,
            yahoo_finance_available
        FROM company_master
        WHERE primary_ticker IS NOT NULL
        AND company_name IS NOT NULL
        ORDER BY country, primary_ticker
        """
        
        companies = await self.db_conn.fetch(query)
        
        # Group by market for analysis
        by_market = {}
        for company in companies:
            country = company['country'] or 'Unknown'
            if country not in by_market:
                by_market[country] = []
            by_market[country].append(company)
            
        return companies, by_market
    
    async def check_existing_documents(self, company_id):
        """Check if company already has documents collected"""
        
        query = """
        SELECT COUNT(*) as doc_count,
               MAX(ingestion_date) as last_collected
        FROM nordic_documents 
        WHERE company_id = $1
        """
        
        result = await self.db_conn.fetchrow(query, company_id)
        return {
            'doc_count': result['doc_count'],
            'last_collected': result['last_collected']
        }
    
    def get_market_ingestor(self, country):
        """Get appropriate ingestor for country/market"""
        
        # Map countries to ingestors
        country_mapping = {
            'SE': 'SE',
            'Sverige': 'Sverige', 
            'Sweden': 'SE',
            'NO': 'NO',
            'Norge': 'Norge',
            'Norway': 'NO',
            'Nordic': 'SE',  # Default to Swedish for generic Nordic
        }
        
        mapped_country = country_mapping.get(country)
        return self.market_ingestors.get(mapped_country)
    
    async def process_company(self, company, company_num, total_companies):
        """Process documents for one company"""
        
        company_id = company['id']
        symbol = company['symbol']
        name = company['name']
        country = company['country'] or 'Unknown'
        existing_doc_count = company['document_count'] or 0
        
        print(f"\n📈 {company_num}/{total_companies} - {symbol} ({country}) [Existing: {existing_doc_count} docs]")
        
        start_time = time.time()
        
        try:
            # Check existing documents from database
            existing = await self.check_existing_documents(company_id)
            
            if existing['doc_count'] > 50:
                print(f"   ✅ {symbol}: Already has {existing['doc_count']} documents, skipping")
                self.results['successful_companies'].append({
                    'symbol': symbol,
                    'country': country,
                    'status': 'already_collected',
                    'document_count': existing['doc_count'],
                    'last_collected': str(existing['last_collected']) if existing['last_collected'] else None
                })
                return
            
            # Get market-specific ingestor
            ingestor = self.get_market_ingestor(country)
            
            if not ingestor:
                print(f"   ❌ {symbol}: No ingestor available for market '{country}'")
                self.results['failed_companies'].append({
                    'symbol': symbol,
                    'country': country,
                    'error': 'no_ingestor_available',
                    'name': name
                })
                return
            
            # Process company with market-specific ingestor
            print(f"   🔍 Processing with {type(ingestor).__name__}...")
            
            # For Swedish companies, use historical ingestion
            if country in ['SE', 'Sverige', 'Sweden', 'Nordic']:
                documents = await self.process_swedish_company(symbol, name, ingestor)
            elif country in ['NO', 'Norge', 'Norway']:
                documents = await self.process_norwegian_company(symbol, name, ingestor)
            else:
                documents = []
                print(f"   ⚠️ {symbol}: Market '{country}' configured but not implemented")
                self.results['failed_companies'].append({
                    'symbol': symbol,
                    'country': country,
                    'error': 'market_not_implemented',
                    'name': name
                })
                return
            
            processing_time = time.time() - start_time
            
            if documents and len(documents) > 0:
                print(f"   ✅ {symbol}: {len(documents)} documents collected ({processing_time:.1f}s)")
                self.results['successful_companies'].append({
                    'symbol': symbol,
                    'country': country,
                    'status': 'new_collection',
                    'document_count': len(documents),
                    'processing_time': processing_time
                })
            else:
                print(f"   ❌ {symbol}: No documents found ({processing_time:.1f}s)")
                self.results['failed_companies'].append({
                    'symbol': symbol,
                    'country': country,
                    'error': 'no_documents_found',
                    'processing_time': processing_time,
                    'name': name
                })
                
        except Exception as e:
            processing_time = time.time() - start_time
            print(f"   ❌ {symbol}: Error - {str(e)[:60]}... ({processing_time:.1f}s)")
            self.results['failed_companies'].append({
                'symbol': symbol,
                'country': country,
                'error': 'processing_exception',
                'error_message': str(e)[:200],
                'processing_time': processing_time,
                'name': name
            })
    
    async def process_swedish_company(self, symbol, name, ingestor):
        """Process Swedish company using existing MFN infrastructure"""
        
        # Use the existing historical ingestion approach
        try:
            # Import the MFN collector
            from mfn_collector import MFNCollector
            
            collector = MFNCollector()
            
            # Try to find documents for this company
            # This uses the same logic as historical_ingestion_batch.py
            documents = await collector.collect_company_documents(symbol, name)
            return documents
            
        except Exception as e:
            logger.warning(f"Swedish collection failed for {symbol}: {e}")
            return []
    
    async def process_norwegian_company(self, symbol, name, ingestor):
        """Process Norwegian company using Norwegian ingestor"""
        
        try:
            # Use the Norwegian ingestor's collection method
            documents = await ingestor.collect_company_documents(symbol, name)
            return documents
            
        except Exception as e:
            logger.warning(f"Norwegian collection failed for {symbol}: {e}")
            return []
    
    def save_results(self):
        """Save processing results to JSON file"""
        
        self.results['end_time'] = datetime.now().isoformat()
        
        # Calculate statistics
        self.results['total_companies'] = len(self.results['successful_companies']) + len(self.results['failed_companies'])
        self.results['success_rate'] = len(self.results['successful_companies']) / self.results['total_companies'] if self.results['total_companies'] > 0 else 0
        
        # Group results by market
        for company in self.results['successful_companies'] + self.results['failed_companies']:
            country = company['country']
            if country not in self.results['by_market']:
                self.results['by_market'][country] = {'successful': 0, 'failed': 0, 'total': 0}
            
            if company in self.results['successful_companies']:
                self.results['by_market'][country]['successful'] += 1
            else:
                self.results['by_market'][country]['failed'] += 1
            self.results['by_market'][country]['total'] += 1
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"universal_ingestion_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"\n💾 Results saved to {filename}")
        return filename

async def run_universal_document_ingestion():
    """Run universal document ingestion for ALL companies"""
    
    ingestor = UniversalDocumentIngestor()
    await ingestor.setup()
    
    print("🌍 UNIVERSAL MULTI-MARKET DOCUMENT INGESTION")
    print("=" * 80)
    
    # Get all companies
    print("📊 Loading companies from company_master...")
    companies, by_market = await ingestor.get_all_companies()
    
    print(f"✅ Found {len(companies)} companies across {len(by_market)} markets:")
    for market, market_companies in by_market.items():
        ingestor_name = "✅" if ingestor.get_market_ingestor(market) else "❌"
        print(f"   {market}: {len(market_companies)} companies {ingestor_name}")
    
    # Process all companies
    print(f"\n🚀 Processing {len(companies)} companies across ALL markets")
    print(f"   Using market-specific ingestors where available")
    print(f"   15-second delays between companies for respectful processing")
    
    start_time = time.time()
    
    for i, company in enumerate(companies, 1):
        await ingestor.process_company(company, i, len(companies))
        
        # Progress updates
        if i % 50 == 0:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(companies) - i) / rate if rate > 0 else 0
            
            print(f"\n📈 PROGRESS: {i}/{len(companies)} companies ({i/len(companies)*100:.0f}%)")
            print(f"   Rate: {rate:.1f} companies/min | ETA: {eta/60:.0f} min")
            print(f"   Success: {len(ingestor.results['successful_companies'])}")
            print(f"   Failed: {len(ingestor.results['failed_companies'])}")
        
        # Respectful delay
        if i < len(companies):  # Don't delay after last company
            await asyncio.sleep(15)
    
    # Final summary
    print("\n" + "=" * 80)
    print("🌍 UNIVERSAL DOCUMENT INGESTION COMPLETE")
    print("=" * 80)
    
    total_time = time.time() - start_time
    
    print(f"\n📊 FINAL STATISTICS:")
    print(f"   Total companies processed: {len(companies)}")
    print(f"   Successful: {len(ingestor.results['successful_companies'])}")
    print(f"   Failed: {len(ingestor.results['failed_companies'])}")
    print(f"   Success rate: {len(ingestor.results['successful_companies'])/len(companies)*100:.1f}%")
    print(f"   Total time: {total_time/60:.1f} minutes")
    
    print(f"\n📋 BY MARKET:")
    for market, stats in ingestor.results['by_market'].items():
        success_rate = stats['successful'] / stats['total'] * 100 if stats['total'] > 0 else 0
        print(f"   {market}: {stats['successful']}/{stats['total']} ({success_rate:.0f}%)")
    
    print(f"\n❌ FAILED COMPANIES ANALYSIS:")
    failure_reasons = {}
    for failed in ingestor.results['failed_companies']:
        reason = failed.get('error', 'unknown')
        failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
    
    for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
        print(f"   {reason}: {count} companies")
    
    # Save results
    results_file = ingestor.save_results()
    print(f"\n💾 Complete results with company details saved to {results_file}")
    
    await ingestor.cleanup()

if __name__ == "__main__":
    asyncio.run(run_universal_document_ingestion())