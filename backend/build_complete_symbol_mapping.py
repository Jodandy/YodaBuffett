#!/usr/bin/env python3
"""
Build complete symbol mapping from our document database.
Maps all companies in extracted_documents to Yahoo Finance symbols.
"""

import asyncio
import asyncpg
import json
from typing import Dict, List, Tuple
import re

async def get_all_document_companies():
    """Get all unique companies from our document database"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get all unique companies with document counts
        companies = await conn.fetch("""
            SELECT 
                company_name,
                COUNT(*) as doc_count,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT document_type) as doc_types
            FROM extracted_documents
            GROUP BY company_name
            ORDER BY doc_count DESC
        """)
        
        print(f"📊 Found {len(companies)} unique companies in document database")
        print(f"   Total documents: {sum(r['doc_count'] for r in companies):,}")
        
        return companies
        
    finally:
        await conn.close()

def generate_yahoo_symbol(company_name: str) -> str:
    """Generate likely Yahoo symbol from company name"""
    
    # Clean the company name
    clean_name = company_name.replace('_', ' ').strip()
    
    # Special mappings for known companies
    special_mappings = {
        'Volvo_Group': 'VOLV-B.ST',
        'Volvo Group': 'VOLV-B.ST',
        'Atlas_Copco_AB': 'ATCO-A.ST',
        'Atlas Copco AB': 'ATCO-A.ST',
        'HM_Hennes__Mauritz_AB': 'HM-B.ST',
        'Telefonaktiebolaget_LM_Ericsson': 'ERIC-B.ST',
        'ABB_Ltd': 'ABB.ST',
        'Investor_AB': 'INVE-B.ST',
        'Swedbank': 'SWED-A.ST',
        'Handelsbanken_A': 'SHB-A.ST',
        'SEB_Bank': 'SEB-A.ST',
    }
    
    if company_name in special_mappings:
        return special_mappings[company_name]
    
    # For Swedish companies, most use ticker.ST format
    # Try to extract a reasonable ticker
    words = clean_name.split()
    
    # If company name is short (like AAK, SSAB), use as-is
    if len(clean_name) <= 4 and clean_name.isupper():
        return f"{clean_name}.ST"
    
    # For longer names, try to create a reasonable ticker
    # This is a heuristic - will need manual review
    ticker = clean_name.upper()[:4] if len(words) == 1 else words[0].upper()[:4]
    
    return f"{ticker}.ST"

async def build_comprehensive_symbol_list():
    """Build comprehensive list of all companies and their symbols"""
    
    companies = await get_all_document_companies()
    
    # Group companies by document count for prioritization
    high_priority = []  # 100+ documents
    medium_priority = []  # 10-99 documents
    low_priority = []  # <10 documents
    
    symbol_mappings = []
    
    print("\n🔍 Analyzing companies by document volume:\n")
    
    for company in companies:
        name = company['company_name']
        doc_count = company['doc_count']
        
        # Skip empty or invalid names
        if not name or name == 'None' or len(name) < 2:
            continue
        
        # Generate potential Yahoo symbol
        yahoo_symbol = generate_yahoo_symbol(name)
        
        mapping = {
            'company_name': name,
            'symbol': name.replace('_', '-')[:10].upper(),  # Short symbol for our DB
            'yahoo_symbol': yahoo_symbol,
            'doc_count': doc_count,
            'first_year': company['first_year'],
            'last_year': company['last_year'],
            'priority': 'high' if doc_count >= 100 else 'medium' if doc_count >= 10 else 'low'
        }
        
        symbol_mappings.append(mapping)
        
        if doc_count >= 100:
            high_priority.append(mapping)
        elif doc_count >= 10:
            medium_priority.append(mapping)
        else:
            low_priority.append(mapping)
    
    print(f"📊 Company Distribution:")
    print(f"   High Priority (100+ docs): {len(high_priority)} companies")
    print(f"   Medium Priority (10-99 docs): {len(medium_priority)} companies")
    print(f"   Low Priority (<10 docs): {len(low_priority)} companies")
    
    # Show high priority companies
    print("\n🎯 High Priority Companies (Top 30):")
    print(f"{'Company':30} {'Docs':>6} {'Years':>12} {'Yahoo Symbol'}")
    print("-" * 65)
    
    for company in high_priority[:30]:
        years = f"{company['first_year']}-{company['last_year']}"
        print(f"{company['company_name'][:30]:30} {company['doc_count']:6} {years:12} {company['yahoo_symbol']}")
    
    # Save comprehensive mapping
    output_file = 'complete_symbol_mappings.json'
    with open(output_file, 'w') as f:
        json.dump({
            'total_companies': len(symbol_mappings),
            'high_priority': high_priority,
            'medium_priority': medium_priority[:50],  # Top 50 medium priority
            'mappings': symbol_mappings[:200]  # Top 200 overall
        }, f, indent=2)
    
    print(f"\n💾 Saved complete mappings to: {output_file}")
    print(f"   Total companies mapped: {len(symbol_mappings)}")
    
    return symbol_mappings

async def generate_insert_statements():
    """Generate SQL statements to insert all symbols"""
    
    mappings = await build_comprehensive_symbol_list()
    
    # Create SQL file
    with open('insert_all_nordic_symbols.sql', 'w') as f:
        f.write("-- Insert all Nordic company symbols from document database\n")
        f.write("-- Generated from document database company names\n\n")
        
        for i, mapping in enumerate(mappings[:200]):  # Top 200 companies
            if mapping['priority'] in ['high', 'medium']:
                sql = f"""
INSERT INTO market_data_symbols 
(symbol, company_name, yahoo_symbol, market, country, sector, industry, document_company_name)
VALUES ('{mapping['symbol']}', '{mapping['company_name'].replace("'", "''")}', 
        '{mapping['yahoo_symbol']}', 'Stockholm', 'SE', 'Unknown', 'Unknown', 
        '{mapping['company_name'].replace("'", "''")}')
ON CONFLICT (symbol) DO NOTHING;
"""
                f.write(sql)
        
    print("\n📄 Generated SQL insert file: insert_all_nordic_symbols.sql")

async def create_bulk_ingestion_script():
    """Create Python script to insert all symbols"""
    
    mappings = await build_comprehensive_symbol_list()
    
    # Filter for high and medium priority
    to_insert = [m for m in mappings if m['priority'] in ['high', 'medium']][:150]
    
    script = '''#!/usr/bin/env python3
"""
Bulk insert all Nordic symbols from document database
Auto-generated from document company names
"""

import asyncio
import asyncpg

async def insert_all_symbols():
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    symbols = [
'''
    
    # Add symbol data
    for mapping in to_insert:
        script += f"""        ('{mapping['symbol']}', '{mapping['company_name'].replace("'", "''")}', '{mapping['yahoo_symbol']}', '{mapping['doc_count']}'),\n"""
    
    script = script.rstrip(',\n') + '\n    ]\n\n'
    
    script += '''    
    inserted = 0
    print(f"🚀 Inserting {len(symbols)} symbols...")
    
    for symbol, name, yahoo, doc_count in symbols:
        try:
            await conn.execute("""
                INSERT INTO market_data_symbols 
                (symbol, company_name, yahoo_symbol, market, country, sector, industry, document_company_name)
                VALUES ($1, $2, $3, 'Stockholm', 'SE', 'Unknown', 'Unknown', $4)
                ON CONFLICT (symbol) DO NOTHING
            """, symbol, name, yahoo, name)
            inserted += 1
            print(f"✅ {symbol}: {name} -> {yahoo} ({doc_count} docs)")
        except Exception as e:
            print(f"❌ Failed {symbol}: {e}")
    
    await conn.close()
    print(f"\\n✅ Inserted {inserted} symbols")

if __name__ == "__main__":
    asyncio.run(insert_all_symbols())
'''
    
    with open('insert_document_symbols.py', 'w') as f:
        f.write(script)
    
    print("📄 Generated Python insert script: insert_document_symbols.py")
    print(f"   Will insert {len(to_insert)} high/medium priority companies")

if __name__ == "__main__":
    print("🏗️  Building Complete Symbol Mapping from Document Database")
    print("=" * 60)
    
    asyncio.run(build_comprehensive_symbol_list())
    asyncio.run(generate_insert_statements())
    asyncio.run(create_bulk_ingestion_script())
    
    print("\n✅ Complete! Next steps:")
    print("   1. Review complete_symbol_mappings.json")
    print("   2. Run: python insert_document_symbols.py")
    print("   3. Run: python ingest_all_historical_data.py")