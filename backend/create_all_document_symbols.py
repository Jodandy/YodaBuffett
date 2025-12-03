#!/usr/bin/env python3
"""
Create market symbols for ALL companies in our document database.
This will likely be 1000+ companies based on Nordic document coverage.
"""

import asyncio
import asyncpg
import re
from typing import Dict, List, Tuple

def clean_company_name(name: str) -> str:
    """Clean up company name for symbol generation"""
    if not name:
        return ""
    
    # Replace underscores with spaces
    clean = name.replace('_', ' ').strip()
    
    # Remove common corporate suffixes for symbol generation
    suffixes_to_remove = [' AB', ' ASA', ' A/S', ' Ltd', ' Inc', ' Corp', ' Corporation', ' Company', ' Group']
    for suffix in suffixes_to_remove:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)].strip()
    
    return clean

def generate_symbol_and_yahoo(company_name: str) -> Tuple[str, str]:
    """Generate both our internal symbol and Yahoo symbol for a company"""
    
    clean_name = clean_company_name(company_name)
    original_name = company_name.replace('_', ' ').strip()
    
    # Manual mappings for known companies (expanded from what we know works)
    known_mappings = {
        # Major companies we know work
        'AAK': ('AAK', 'AAK.ST'),
        'Volvo Group': ('VOLV-B', 'VOLV-B.ST'),
        'Atlas Copco AB': ('ATCO-A', 'ATCO-A.ST'),
        'Telefonaktiebolaget LM Ericsson': ('ERIC-B', 'ERIC-B.ST'),
        'ABB Ltd': ('ABB', 'ABB.ST'),
        'HM Hennes & Mauritz AB': ('HM-B', 'HM-B.ST'),
        'Electrolux AB': ('ELUX-B', 'ELUX-B.ST'),
        'Sandvik AB': ('SAND', 'SAND.ST'),
        'SSAB AB': ('SSAB-A', 'SSAB-A.ST'),
        'Hexagon AB': ('HEXA-B', 'HEXA-B.ST'),
        'Evolution AB': ('EVO', 'EVO.ST'),
        'Investor AB': ('INVE-B', 'INVE-B.ST'),
        'Kinnevik AB': ('KINV-B', 'KINV-B.ST'),
        'Swedbank': ('SWED-A', 'SWED-A.ST'),
        'Tele2 AB': ('TEL2-B', 'TEL2-B.ST'),
        'ICA Gruppen AB': ('ICA', 'ICA.ST'),
        'Getinge': ('GETI-B', 'GETI-B.ST'),
        'Addtech': ('ADDT-B', 'ADDTECH-B.ST'),
        'Attendo': ('ATT', 'ATT.ST'),
        'Avanza Bank': ('AZA', 'AZA.ST'),
        'Castellum': ('CAST', 'CAST.ST'),
        'Epiroc AB': ('EPI-A', 'EPI-A.ST'),
        
        # Add more as we discover them
        'Ericsson': ('ERIC-B', 'ERIC-B.ST'),
        'Volvo': ('VOLV-B', 'VOLV-B.ST'),
        'Atlas Copco': ('ATCO-A', 'ATCO-A.ST'),
        'H&M': ('HM-B', 'HM-B.ST'),
        'Hennes & Mauritz': ('HM-B', 'HM-B.ST'),
        'Swedish Orphan Biovitrum': ('SOBI', 'SOBI.ST'),
    }
    
    # Check known mappings first
    for known_name, (symbol, yahoo) in known_mappings.items():
        if (clean_name.lower() in known_name.lower() or 
            known_name.lower() in clean_name.lower() or
            original_name.lower() in known_name.lower()):
            return symbol, yahoo
    
    # Generate symbol for unknown companies
    # Take first letters of each word, max 8 characters
    words = clean_name.split()
    if len(words) == 1:
        # Single word - use first 4-6 characters
        symbol = clean_name[:6].upper()
    else:
        # Multiple words - take first letter of each word
        symbol = ''.join(word[0].upper() for word in words if word)[:8]
    
    # Clean symbol
    symbol = re.sub(r'[^A-Z0-9]', '', symbol)
    if len(symbol) < 2:
        symbol = clean_name[:4].upper()
    
    # Generate Yahoo symbol (assume Swedish .ST for now)
    yahoo_symbol = f"{symbol}.ST"
    
    return symbol, yahoo_symbol

async def create_symbols_for_all_companies():
    """Create symbols for ALL companies in our document database"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Get ALL companies with their document counts
        companies = await conn.fetch("""
            SELECT 
                company_name,
                COUNT(*) as doc_count,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT document_type) as doc_types
            FROM extracted_documents
            WHERE company_name IS NOT NULL 
            AND company_name != '' 
            AND company_name != 'None'
            AND LENGTH(company_name) > 1
            GROUP BY company_name
            ORDER BY COUNT(*) DESC
        """)
        
        print(f"🏢 Found {len(companies)} unique companies in document database")
        print(f"📊 Processing ALL companies for symbol creation...")
        print("=" * 70)
        
        # Process all companies
        symbol_mappings = []
        inserted = 0
        failed = 0
        
        for i, company in enumerate(companies, 1):
            name = company['company_name']
            doc_count = company['doc_count']
            
            # Generate symbols
            try:
                symbol, yahoo_symbol = generate_symbol_and_yahoo(name)
                
                # Ensure symbol is unique by appending number if needed
                base_symbol = symbol
                counter = 1
                while any(m['symbol'] == symbol for m in symbol_mappings):
                    symbol = f"{base_symbol}{counter}"
                    yahoo_symbol = f"{symbol}.ST"
                    counter += 1
                
                mapping = {
                    'symbol': symbol,
                    'company_name': name,
                    'yahoo_symbol': yahoo_symbol,
                    'doc_count': doc_count,
                    'priority': 'high' if doc_count >= 20 else 'medium' if doc_count >= 5 else 'low'
                }
                
                symbol_mappings.append(mapping)
                
                # Insert into database
                try:
                    await conn.execute("""
                        INSERT INTO market_data_symbols 
                        (symbol, company_name, yahoo_symbol, market, country, sector, industry, document_company_name)
                        VALUES ($1, $2, $3, 'Stockholm', 'SE', 'Unknown', 'Unknown', $4)
                        ON CONFLICT (symbol) DO UPDATE SET
                            company_name = EXCLUDED.company_name,
                            document_company_name = EXCLUDED.document_company_name,
                            updated_at = NOW()
                    """, symbol, name, yahoo_symbol, name)
                    
                    inserted += 1
                    
                    if i <= 50 or doc_count >= 20:  # Show first 50 or high priority
                        priority_icon = "🔥" if doc_count >= 20 else "📈" if doc_count >= 5 else "📄"
                        print(f"{priority_icon} {symbol:8} - {name[:40]:40} -> {yahoo_symbol:12} ({doc_count:3} docs)")
                    
                except Exception as e:
                    failed += 1
                    if failed <= 10:  # Show first 10 failures
                        print(f"❌ Failed {symbol}: {e}")
                
            except Exception as e:
                failed += 1
                print(f"❌ Error processing {name}: {e}")
        
        # Show summary
        print("\n" + "=" * 70)
        print(f"🎉 COMPREHENSIVE SYMBOL CREATION COMPLETE!")
        print(f"   📊 Total companies processed: {len(companies)}")
        print(f"   ✅ Successfully inserted: {inserted}")
        print(f"   ❌ Failed: {failed}")
        
        # Show priority distribution
        high_priority = sum(1 for m in symbol_mappings if m['priority'] == 'high')
        medium_priority = sum(1 for m in symbol_mappings if m['priority'] == 'medium')
        low_priority = sum(1 for m in symbol_mappings if m['priority'] == 'low')
        
        print(f"\n📈 Priority Distribution:")
        print(f"   🔥 High Priority (20+ docs): {high_priority} companies")
        print(f"   📈 Medium Priority (5-19 docs): {medium_priority} companies") 
        print(f"   📄 Low Priority (1-4 docs): {low_priority} companies")
        
        print(f"\n💡 Ready to ingest historical data for {inserted} companies!")
        print(f"   Recommendation: Start with high/medium priority ({high_priority + medium_priority} companies)")
        
        # Save mapping to file
        import json
        with open('all_document_companies_mapped.json', 'w') as f:
            json.dump(symbol_mappings, f, indent=2)
        print(f"💾 Saved complete mapping to: all_document_companies_mapped.json")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🌟 COMPLETE DOCUMENT COMPANY SYMBOL MAPPING")
    print("Creating market symbols for ALL companies in our document database")
    print("This could be 1000+ companies!")
    print("=" * 70)
    
    asyncio.run(create_symbols_for_all_companies())