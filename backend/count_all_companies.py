#!/usr/bin/env python3
"""
Count ALL companies in our document database and show the full scope
"""

import asyncio
import asyncpg

async def get_full_company_count():
    """Get complete count of all companies in our documents"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Total unique companies
        total_companies = await conn.fetchval("""
            SELECT COUNT(DISTINCT company_name) 
            FROM extracted_documents 
            WHERE company_name IS NOT NULL 
            AND company_name != '' 
            AND company_name != 'None'
        """)
        
        print(f"📊 TOTAL UNIQUE COMPANIES: {total_companies}")
        
        # Distribution by document count
        distribution = await conn.fetch("""
            WITH company_stats AS (
                SELECT 
                    company_name,
                    COUNT(*) as doc_count,
                    MIN(year) as first_year,
                    MAX(year) as last_year
                FROM extracted_documents
                WHERE company_name IS NOT NULL 
                AND company_name != '' 
                AND company_name != 'None'
                GROUP BY company_name
            )
            SELECT 
                CASE 
                    WHEN doc_count >= 100 THEN '100+ documents'
                    WHEN doc_count >= 50 THEN '50-99 documents' 
                    WHEN doc_count >= 20 THEN '20-49 documents'
                    WHEN doc_count >= 10 THEN '10-19 documents'
                    WHEN doc_count >= 5 THEN '5-9 documents'
                    ELSE '1-4 documents'
                END as category,
                COUNT(*) as company_count
            FROM company_stats
            GROUP BY 
                CASE 
                    WHEN doc_count >= 100 THEN '100+ documents'
                    WHEN doc_count >= 50 THEN '50-99 documents' 
                    WHEN doc_count >= 20 THEN '20-49 documents'
                    WHEN doc_count >= 10 THEN '10-19 documents'
                    WHEN doc_count >= 5 THEN '5-9 documents'
                    ELSE '1-4 documents'
                END
            ORDER BY MIN(doc_count) DESC
        """)
        
        print(f"\n📈 Distribution by Document Volume:")
        total_meaningful = 0
        for row in distribution:
            print(f"   {row['category']:15} - {row['company_count']:4} companies")
            if 'documents' in row['category'] and not row['category'].startswith('1-4'):
                total_meaningful += row['company_count']
        
        print(f"\n💡 Companies with 5+ documents: {total_meaningful}")
        print(f"   (These are good candidates for market data)")
        
        # Show top 50 companies by document count
        top_companies = await conn.fetch("""
            SELECT 
                company_name,
                COUNT(*) as doc_count,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT document_type) as doc_types,
                COUNT(DISTINCT year) as year_span
            FROM extracted_documents
            WHERE company_name IS NOT NULL 
            AND company_name != '' 
            AND company_name != 'None'
            GROUP BY company_name
            ORDER BY COUNT(*) DESC
            LIMIT 50
        """)
        
        print(f"\n🏆 Top 50 Companies by Document Count:")
        print(f"{'Rank':4} {'Company':35} {'Docs':5} {'Years':12} {'Types':6}")
        print("-" * 70)
        
        for i, row in enumerate(top_companies, 1):
            years_range = f"{row['first_year']}-{row['last_year']}"
            print(f"{i:4} {row['company_name'][:34]:35} {row['doc_count']:5} {years_range:12} {row['doc_types']:6}")
        
        # Show some sample company names to understand naming patterns
        samples = await conn.fetch("""
            SELECT DISTINCT company_name 
            FROM extracted_documents 
            WHERE company_name IS NOT NULL 
            ORDER BY company_name 
            LIMIT 100
        """)
        
        print(f"\n📝 Sample Company Names (first 100 alphabetically):")
        for i, row in enumerate(samples):
            if i % 4 == 0:
                print()
            print(f"{row['company_name'][:20]:22}", end="")
        
        print(f"\n\n💾 Would you like me to:")
        print(f"   1. Create symbols for ALL {total_companies} companies?")
        print(f"   2. Focus on {total_meaningful} companies with 5+ documents?") 
        print(f"   3. Create a massive mapping script for top 500 companies?")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🔍 COMPLETE COMPANY ANALYSIS")
    print("Analyzing ALL companies in our document database")
    print("=" * 60)
    
    asyncio.run(get_full_company_count())