#!/usr/bin/env python3
"""
Smart symbol matcher for document companies and market data.
"""

import asyncio
import asyncpg
from typing import List, Tuple

async def create_symbol_mapping():
    """Create a comprehensive mapping between document companies and market symbols."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    # Get all available symbols
    all_symbols = await conn.fetch("""
        SELECT DISTINCT symbol, COUNT(*) as days
        FROM daily_price_data
        GROUP BY symbol
        ORDER BY days DESC
    """)
    
    print(f"Found {len(all_symbols)} total market symbols")
    
    # Get document companies
    doc_companies = await conn.fetch("""
        SELECT DISTINCT ed.company_name, COUNT(*) as sections
        FROM section_embeddings se
        JOIN document_sections ds ON se.document_section_id = ds.id
        JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
        WHERE se.embedding_model LIKE 'local/%'
        GROUP BY ed.company_name
        ORDER BY sections DESC
        LIMIT 30
    """)
    
    print(f"Found {len(doc_companies)} companies with documents")
    
    # Create smart mappings
    symbol_set = set(row['symbol'] for row in all_symbols)
    mappings = []
    
    # Known mappings for major Nordic companies
    known_mappings = {
        'Björn Borg': ['BORG', 'BJORNBORG', 'BB'],
        'Nordic Semiconductor': ['NOD', 'NORDIC', 'NRDSEMI'],
        'VBG Group': ['VBG', 'VBGGROUP'],
        'Dometic': ['DOM', 'DOMETIC'],
        'Handelsbanken': ['SHB', 'HANDELSBANKEN', 'SHBA', 'SEB'],  # Sometimes confused
        'Troax Group': ['TROAX'],
        'XANO Industri': ['XANO'],
        'Atrium Ljungberg': ['ATRLJ', 'ATRIUM'],
        'Bure Equity': ['BURE'],
        'Hoist Finance': ['HOIST'],
        'John Mattson': ['JOMA', 'JOHNMATTSON'],
        'Scandi Standard': ['SCANDI'],
        # Add more as needed
    }
    
    print(f"\n🔍 Searching for symbol matches...")
    
    for company_row in doc_companies[:15]:  # Top 15 companies
        company_name = company_row['company_name']
        sections = company_row['sections']
        
        # Try known mappings first
        found_symbol = None
        if company_name in known_mappings:
            for candidate in known_mappings[company_name]:
                # Try exact match
                if candidate in symbol_set:
                    found_symbol = candidate
                    break
                # Try with common suffixes
                for suffix in ['', '.ST', '-B.ST', '.OL', '.HE', '.CO']:
                    test_symbol = candidate + suffix
                    if test_symbol in symbol_set:
                        found_symbol = test_symbol
                        break
                if found_symbol:
                    break
        
        # Try fuzzy matching if no known mapping
        if not found_symbol:
            # Extract key parts of company name
            name_parts = company_name.replace(' Group', '').replace(' AB', '').replace(' ASA', '').split()
            
            for part in name_parts:
                if len(part) >= 3:  # Avoid short words
                    part_upper = part.upper()
                    
                    # Try direct substring match
                    for symbol in symbol_set:
                        if part_upper in symbol.upper() or symbol.upper() in part_upper:
                            if len(symbol) <= 10:  # Reasonable symbol length
                                found_symbol = symbol
                                break
                    if found_symbol:
                        break
        
        if found_symbol:
            mappings.append((company_name, found_symbol, sections))
            print(f"   ✅ {company_name} → {found_symbol} ({sections} sections)")
        else:
            print(f"   ❌ {company_name} (no match found, {sections} sections)")
    
    await conn.close()
    
    print(f"\n🎯 Found {len(mappings)} mappings")
    print("\nCode for backtest_document_anomaly_strategy.py:")
    print("company_to_symbol = {")
    for company, symbol, _ in mappings:
        print(f"    '{company}': '{symbol}',")
    print("}")
    
    return mappings

if __name__ == "__main__":
    asyncio.run(create_symbol_mapping())