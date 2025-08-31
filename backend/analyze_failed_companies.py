#!/usr/bin/env python3
"""
Simple analysis of failed companies - no external dependencies
"""

import asyncio
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy import select, func
    from shared.database import AsyncSessionLocal
    from nordic_ingestion.models import NordicCompany, NordicDocument
    DB_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Database imports not available: {e}")
    DB_AVAILABLE = False
    sys.exit(1)

async def analyze_companies():
    """Analyze company document counts to identify problems"""
    
    print("🔍 Analyzing Swedish company document collection status...")
    
    async with AsyncSessionLocal() as db:
        # Get all Swedish companies with their document counts
        result = await db.execute(
            select(
                NordicCompany.name,
                NordicCompany.ticker,
                func.count(NordicDocument.id).label('doc_count')
            )
            .outerjoin(NordicDocument, NordicCompany.id == NordicDocument.company_id)
            .where(NordicCompany.country == "SE")
            .group_by(NordicCompany.id, NordicCompany.name, NordicCompany.ticker)
            .order_by(func.count(NordicDocument.id).asc(), NordicCompany.name)
        )
        
        companies = result.all()
        
        # Categorize companies
        failed = []      # 0 documents
        sparse = []      # 1-5 documents
        decent = []      # 6-20 documents
        good = []        # 21+ documents
        
        for company_name, ticker, doc_count in companies:
            company_info = {
                'name': company_name,
                'ticker': ticker,
                'doc_count': doc_count
            }
            
            if doc_count == 0:
                failed.append(company_info)
            elif doc_count <= 5:
                sparse.append(company_info)
            elif doc_count <= 20:
                decent.append(company_info)
            else:
                good.append(company_info)
        
        # Print summary
        print(f"\n📊 Swedish Companies Analysis ({len(companies)} total):")
        print(f"   ❌ Failed (0 docs): {len(failed)} companies")
        print(f"   📉 Sparse (1-5 docs): {len(sparse)} companies")
        print(f"   📊 Decent (6-20 docs): {len(decent)} companies")
        print(f"   ✅ Good (21+ docs): {len(good)} companies")
        
        # Show failed companies
        if failed:
            print(f"\n❌ FAILED COMPANIES (0 documents):")
            for i, company in enumerate(failed[:20]):  # Show first 20
                mfn_slug = generate_mfn_slug(company['name'])
                print(f"   {i+1:2d}. {company['name']} ({company['ticker']}) → {mfn_slug}")
            if len(failed) > 20:
                print(f"   ... and {len(failed) - 20} more failed companies")
        
        # Show sparse companies
        if sparse:
            print(f"\n📉 SPARSE COMPANIES (1-5 documents):")
            for i, company in enumerate(sparse[:15]):  # Show first 15
                mfn_slug = generate_mfn_slug(company['name'])
                print(f"   {i+1:2d}. {company['name']} ({company['ticker']}) - {company['doc_count']} docs → {mfn_slug}")
            if len(sparse) > 15:
                print(f"   ... and {len(sparse) - 15} more sparse companies")
        
        # Show some good examples
        if good:
            print(f"\n✅ SUCCESSFUL COMPANIES (examples):")
            for i, company in enumerate(good[:5]):
                print(f"   {i+1}. {company['name']} ({company['ticker']}) - {company['doc_count']} docs")
        
        # Summary stats
        total_problems = len(failed) + len(sparse)
        problem_rate = (total_problems / len(companies)) * 100
        
        print(f"\n📈 SUMMARY:")
        print(f"   🎯 Companies needing retry: {total_problems}")
        print(f"   📊 Problem rate: {problem_rate:.1f}%")
        
        if total_problems > 0:
            print(f"\n💡 RECOMMENDATION:")
            print(f"   1. The MFN collector fixes should help these companies")
            print(f"   2. Failed companies likely need slug resolution")
            print(f"   3. Sparse companies likely missing storage.mfn.se PDFs")
            print(f"   4. Run a targeted retry on these {total_problems} companies")
        
        return {
            'failed': failed,
            'sparse': sparse,
            'total_problems': total_problems
        }

def generate_mfn_slug(company_name: str) -> str:
    """Generate MFN-compatible slug from company name"""
    slug = company_name.lower()
    slug = slug.replace(' ab', '').replace(' group', '').replace(' & ', '-and-')
    slug = slug.replace(' ', '-').replace('&', 'and')
    slug = ''.join(c for c in slug if c.isalnum() or c == '-')
    return slug.strip('-')

async def main():
    print("🚀 Failed Companies Analysis")
    print("=" * 50)
    
    results = await analyze_companies()
    
    print("\n" + "=" * 50)
    print("✅ Analysis complete!")

if __name__ == "__main__":
    asyncio.run(main())