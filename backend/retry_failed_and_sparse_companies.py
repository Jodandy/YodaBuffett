#!/usr/bin/env python3
"""
Retry Failed and Sparse Companies

Identifies companies that:
1. Failed completely (0 documents)
2. Have very few documents (likely missing storage.mfn.se PDFs)
3. Had collection errors

Then reruns ONLY those companies with the new fixes.
"""

import asyncio
import aiohttp
import sys
import os
from datetime import datetime
from typing import List, Dict, Any, Tuple
import json

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy import select, func, and_, or_
    from shared.database import AsyncSessionLocal
    from nordic_ingestion.models import NordicCompany, NordicDocument, NordicIngestionLog
    from nordic_ingestion.collectors.aggregator.mfn_collector import MFNCollector
    from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
    from nordic_ingestion.storage.calendar_storage import store_mfn_calendar_events
    DB_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Database imports not available: {e}")
    print("Running in analysis-only mode")
    DB_AVAILABLE = False

class FailedCompanyRetrier:
    """
    Identifies and retries companies that failed or have sparse document collections
    """
    
    def __init__(self):
        self.mfn_collector = MFNCollector(rate_limit_delay=3.0)  # Slower to be extra respectful
        self.session_headers = {
            'User-Agent': 'YodaBuffett-Retry/1.0 (Financial Research; Fixing Failed Collections)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Connection': 'keep-alive',
        }
    
    async def analyze_collection_status(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Analyze current collection status and identify problem companies
        
        Returns:
            Dictionary with categorized problem companies
        """
        if not DB_AVAILABLE:
            print("❌ Database not available, cannot analyze collection status")
            return {}
        
        print("🔍 Analyzing collection status...")
        
        results = {
            "failed_companies": [],      # 0 documents
            "sparse_companies": [],      # 1-5 documents (likely missing PDFs)  
            "error_companies": [],       # Had collection errors
            "successful_companies": [],  # Good collection (6+ documents)
        }
        
        async with AsyncSessionLocal() as db:
            # Get all Swedish companies
            company_result = await db.execute(
                select(NordicCompany).where(NordicCompany.country == "SE")
                .order_by(NordicCompany.name)
            )
            companies = company_result.scalars().all()
            
            print(f"📊 Found {len(companies)} Swedish companies")
            
            for company in companies:
                # Count documents for this company
                doc_result = await db.execute(
                    select(func.count(NordicDocument.id)).where(
                        NordicDocument.company_id == company.id
                    )
                )
                doc_count = doc_result.scalar() or 0
                
                company_info = {
                    "id": str(company.id),
                    "name": company.name,
                    "ticker": company.ticker,
                    "mfn_slug": self._generate_mfn_slug(company.name),
                    "document_count": doc_count,
                }
                
                # Categorize the company
                if doc_count == 0:
                    results["failed_companies"].append(company_info)
                elif doc_count <= 5:
                    results["sparse_companies"].append(company_info)  
                else:
                    results["successful_companies"].append(company_info)
            
            # Print summary
            print(f"\n📈 Collection Status Summary:")
            print(f"   ❌ Failed (0 docs): {len(results['failed_companies'])}")
            print(f"   📉 Sparse (1-5 docs): {len(results['sparse_companies'])}")
            print(f"   ✅ Successful (6+ docs): {len(results['successful_companies'])}")
            
            return results
    
    async def retry_problem_companies(self, 
                                    problem_companies: List[Dict[str, Any]], 
                                    category: str,
                                    limit: int = None) -> Dict[str, Any]:
        """
        Retry collection for problem companies
        
        Args:
            problem_companies: List of company info dicts
            category: Category name for logging
            limit: Limit number of companies to retry (None = all)
        
        Returns:
            Results summary
        """
        if not problem_companies:
            print(f"📭 No {category} companies to retry")
            return {"attempted": 0, "successful": 0, "still_failed": 0}
        
        companies_to_retry = problem_companies[:limit] if limit else problem_companies
        
        print(f"\n🔄 Retrying {len(companies_to_retry)} {category} companies...")
        if limit and len(problem_companies) > limit:
            print(f"   (Limited to {limit} out of {len(problem_companies)} total)")
        
        results = {
            "attempted": 0,
            "successful": 0, 
            "still_failed": 0,
            "details": []
        }
        
        async with aiohttp.ClientSession(
            headers=self.session_headers,
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            
            for i, company_info in enumerate(companies_to_retry):
                results["attempted"] += 1
                company_name = company_info["name"]
                mfn_slug = company_info["mfn_slug"]
                
                print(f"\n📊 [{i+1}/{len(companies_to_retry)}] Retrying: {company_name} ({mfn_slug})")
                print(f"   💾 Previous: {company_info['document_count']} documents")
                
                try:
                    # Try collection with enhanced MFN collector
                    items = await self.mfn_collector.collect_company_news(
                        session,
                        mfn_slug,
                        limit=200,  # Higher limit to get more historical documents
                        full_backfill=True
                    )
                    
                    if items:
                        print(f"   🎯 Found {len(items)} items")
                        
                        # Count PDFs and analyze hosting domains
                        pdf_count = 0
                        storage_mfn_count = 0
                        hosting_domains = {}
                        
                        for item in items:
                            pdf_count += len(item.pdf_urls)
                            for pdf_url in item.pdf_urls:
                                if 'storage.mfn.se' in pdf_url:
                                    storage_mfn_count += 1
                                # Extract domain for stats
                                if '://' in pdf_url:
                                    domain = pdf_url.split('://')[1].split('/')[0]
                                    hosting_domains[domain] = hosting_domains.get(domain, 0) + 1
                        
                        print(f"   📄 Total PDFs: {pdf_count}")
                        if storage_mfn_count > 0:
                            print(f"   ⭐ Storage.mfn.se PDFs: {storage_mfn_count} (NEW!)")
                        if hosting_domains:
                            top_domains = sorted(hosting_domains.items(), key=lambda x: x[1], reverse=True)[:3]
                            print(f"   🌐 Top domains: {', '.join([f'{d}({c})' for d, c in top_domains])}")
                        
                        # Store the documents if DB is available
                        if DB_AVAILABLE:
                            try:
                                doc_stats = await catalog_mfn_documents(items)
                                event_stats = await store_mfn_calendar_events(items)
                                
                                stored_docs = doc_stats.get('stored', 0)
                                created_events = event_stats.get('calendar_events_created', 0)
                                
                                print(f"   💾 Stored: {stored_docs} documents, {created_events} events")
                                
                                if stored_docs > 0:
                                    results["successful"] += 1
                                    result_status = "success"
                                else:
                                    results["still_failed"] += 1
                                    result_status = "no_new_documents"
                                    
                            except Exception as e:
                                print(f"   ❌ Storage error: {e}")
                                results["still_failed"] += 1
                                result_status = "storage_error"
                        else:
                            # Without DB, consider it successful if we found items
                            results["successful"] += 1
                            result_status = "found_items_no_db"
                    else:
                        print(f"   📭 No items found (even with fixes)")
                        results["still_failed"] += 1
                        result_status = "no_items_found"
                    
                    # Record results
                    results["details"].append({
                        "company": company_name,
                        "slug": mfn_slug,
                        "previous_docs": company_info["document_count"],
                        "items_found": len(items) if items else 0,
                        "pdf_count": pdf_count if items else 0,
                        "storage_mfn_pdfs": storage_mfn_count if items else 0,
                        "status": result_status
                    })
                    
                except Exception as e:
                    print(f"   ❌ Collection error: {e}")
                    results["still_failed"] += 1
                    results["details"].append({
                        "company": company_name,
                        "slug": mfn_slug,
                        "previous_docs": company_info["document_count"],
                        "items_found": 0,
                        "pdf_count": 0,
                        "status": "collection_error",
                        "error": str(e)
                    })
                
                # Rate limiting between companies
                if i < len(companies_to_retry) - 1:
                    print(f"   ⏱️  Rate limiting... waiting {self.mfn_collector.rate_limit_delay}s")
                    await asyncio.sleep(self.mfn_collector.rate_limit_delay)
        
        return results
    
    def _generate_mfn_slug(self, company_name: str) -> str:
        """Generate MFN-compatible slug from company name"""
        slug = company_name.lower()
        slug = slug.replace(' ab', '').replace(' group', '').replace(' & ', '-and-')
        slug = slug.replace(' ', '-').replace('&', 'and')
        slug = ''.join(c for c in slug if c.isalnum() or c == '-')
        return slug.strip('-')
    
    async def save_results(self, results: Dict[str, Any], filename: str = None):
        """Save results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"retry_results_{timestamp}.json"
        
        # Make results JSON serializable
        json_results = {}
        for category, companies in results.items():
            if isinstance(companies, list):
                json_results[category] = companies
            elif isinstance(companies, dict):
                json_results[category] = companies
        
        try:
            with open(filename, 'w') as f:
                json.dump(json_results, f, indent=2, default=str)
            print(f"💾 Results saved to: {filename}")
        except Exception as e:
            print(f"⚠️  Could not save results: {e}")

async def main():
    """Main execution function"""
    
    print("🚀 Failed & Sparse Company Retrier")
    print("=" * 50)
    print("This script will:")
    print("1. Analyze current collection status")  
    print("2. Identify failed/sparse companies")
    print("3. Retry with enhanced fixes (storage.mfn.se + slug resolution)")
    print("4. Save results for review")
    print()
    
    retrier = FailedCompanyRetrier()
    
    # Step 1: Analyze current status
    status = await retrier.analyze_collection_status()
    
    if not status:
        print("❌ Could not analyze collection status")
        return
    
    # Step 2: Show what we found
    print(f"\n🎯 Companies to Retry:")
    failed = status["failed_companies"]
    sparse = status["sparse_companies"]
    
    if failed:
        print(f"\n❌ Failed Companies (0 documents): {len(failed)}")
        for company in failed[:5]:  # Show first 5
            print(f"   • {company['name']} ({company['mfn_slug']})")
        if len(failed) > 5:
            print(f"   ... and {len(failed) - 5} more")
    
    if sparse:
        print(f"\n📉 Sparse Companies (1-5 documents): {len(sparse)}")
        for company in sparse[:5]:  # Show first 5
            print(f"   • {company['name']} ({company['mfn_slug']}) - {company['document_count']} docs")
        if len(sparse) > 5:
            print(f"   ... and {len(sparse) - 5} more")
    
    # Step 3: Ask user what to retry
    total_to_retry = len(failed) + len(sparse)
    
    if total_to_retry == 0:
        print("🎉 No companies need retrying! All collections look good.")
        return
    
    print(f"\n📊 Total companies to retry: {total_to_retry}")
    
    # For safety, ask confirmation if many companies
    if total_to_retry > 20:
        print(f"⚠️  This is a lot of companies. Consider running in smaller batches.")
        response = input("Continue with all companies? [y/N]: ").strip().lower()
        if response != 'y':
            limit = input(f"How many companies to retry? (max {total_to_retry}): ").strip()
            try:
                limit = int(limit)
                limit = min(limit, total_to_retry)
            except ValueError:
                limit = 10
                print(f"Invalid input, using limit of {limit}")
    else:
        response = input(f"Retry {total_to_retry} companies? [Y/n]: ").strip().lower()
        if response == 'n':
            print("Cancelled by user")
            return
        limit = None
    
    # Step 4: Retry the companies
    all_results = {}
    
    if failed:
        print(f"\n🔄 PHASE 1: Retrying Failed Companies")
        failed_results = await retrier.retry_problem_companies(failed, "failed", limit)
        all_results["failed_retry"] = failed_results
        limit = limit - failed_results["attempted"] if limit else None
    
    if sparse and (not limit or limit > 0):
        print(f"\n🔄 PHASE 2: Retrying Sparse Companies")
        sparse_results = await retrier.retry_problem_companies(sparse, "sparse", limit)
        all_results["sparse_retry"] = sparse_results
    
    # Step 5: Summary
    print("\n" + "=" * 50)
    print("📊 RETRY SUMMARY")
    
    total_attempted = sum(r.get("attempted", 0) for r in all_results.values())
    total_successful = sum(r.get("successful", 0) for r in all_results.values())
    total_still_failed = sum(r.get("still_failed", 0) for r in all_results.values())
    
    print(f"   📈 Companies attempted: {total_attempted}")
    print(f"   ✅ Successful fixes: {total_successful}")
    print(f"   ❌ Still failing: {total_still_failed}")
    
    if total_successful > 0:
        success_rate = (total_successful / total_attempted) * 100
        print(f"   🎯 Success rate: {success_rate:.1f}%")
        print(f"\n🎉 The fixes helped {total_successful} companies!")
    
    # Step 6: Save results  
    await retrier.save_results(all_results)
    
    print(f"\n✅ Retry completed!")
    if total_successful > 0:
        print(f"💡 Recommendation: Run historical_ingestion_batch.py again to see full improvement")

if __name__ == "__main__":
    asyncio.run(main())