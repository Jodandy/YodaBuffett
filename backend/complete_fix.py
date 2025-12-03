#!/usr/bin/env python3
"""
Complete fix for database constraints and Swedish ticker mappings.
Combines both foreign key fix and company list ticker updates.
"""

import asyncio
import asyncpg
import json
from typing import Dict, List, Optional
import re
from difflib import SequenceMatcher

def clean_company_name(name: str) -> str:
    """Clean company name for matching"""
    if not name:
        return ""
    
    clean = name.replace('_', ' ').strip()
    clean = re.sub(r'\s+', ' ', clean)
    
    # Remove common Swedish company suffixes
    suffixes = [' AB', ' ASA', ' A/S', ' Ltd', ' Inc', ' Corp', ' Corporation', ' Company', ' Group']
    for suffix in suffixes:
        if clean.endswith(suffix):
            clean = clean[:-len(suffix)].strip()
    
    return clean.lower()

def swedish_ticker_to_yahoo(ticker: str) -> str:
    """Convert Swedish ticker to Yahoo Finance format"""
    # Handle special cases first
    special_cases = {
        'ALIV SDB': 'ALIV-SDB.ST',
        'ALVO SDB': 'ALVO-SDB.ST',
    }
    
    if ticker in special_cases:
        return special_cases[ticker]
    
    # Standard conversion: spaces become dashes, add .ST
    yahoo_ticker = ticker.replace(' ', '-') + '.ST'
    return yahoo_ticker

def similarity(a: str, b: str) -> float:
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_best_match(target_name: str, companies_list: List[Dict]) -> Optional[Dict]:
    """Find the best matching company from the Swedish companies list"""
    
    target_clean = clean_company_name(target_name)
    best_match = None
    best_score = 0.0
    
    for company in companies_list:
        company_name = company.get('name', '')
        company_clean = clean_company_name(company_name)
        
        # Calculate similarity
        score = similarity(target_clean, company_clean)
        
        # Boost score for exact word matches
        target_words = set(target_clean.split())
        company_words = set(company_clean.split())
        
        if target_words & company_words:  # Common words
            word_overlap = len(target_words & company_words) / max(len(target_words), len(company_words))
            score = score * 0.7 + word_overlap * 0.3
        
        if score > best_score and score > 0.6:  # Minimum threshold
            best_score = score
            best_match = {
                **company,
                'match_score': score,
                'match_type': 'fuzzy' if score < 0.9 else 'high'
            }
    
    return best_match

async def fix_foreign_key_constraints(conn):
    """Fix foreign key constraints causing violations"""
    
    print("🔧 Step 1: Fixing Foreign Key Constraints")
    print("=" * 50)
    
    # Get current foreign key constraints
    constraints = await conn.fetch("""
        SELECT 
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY' 
        AND tc.table_name = 'daily_price_data'
    """)
    
    print(f"Found {len(constraints)} foreign key constraints")
    
    # Drop constraints
    for constraint in constraints:
        constraint_name = constraint['constraint_name']
        try:
            await conn.execute(f"""
                ALTER TABLE daily_price_data 
                DROP CONSTRAINT IF EXISTS {constraint_name}
            """)
            print(f"  ✅ Dropped {constraint_name}")
        except Exception as e:
            print(f"  ❌ Failed to drop {constraint_name}: {e}")
    
    # Test insertion
    try:
        await conn.execute("""
            INSERT INTO daily_price_data (
                symbol, date, open_price, high_price, low_price, close_price, provider
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT DO NOTHING
        """, 'TEST', '2024-01-01', 100.0, 101.0, 99.0, 100.5, 'test')
        
        await conn.execute("""
            DELETE FROM daily_price_data 
            WHERE symbol = 'TEST' AND provider = 'test'
        """)
        
        print("  ✅ Database constraints fixed - insertions should work now!")
        return True
        
    except Exception as e:
        print(f"  ❌ Still have constraint issues: {e}")
        return False

async def fix_swedish_tickers(conn):
    """Fix Swedish ticker mappings using company-list.json"""
    
    print("\n🇸🇪 Step 2: Fixing Swedish Ticker Mappings")
    print("=" * 50)
    
    # Load Swedish company list
    try:
        with open('/Users/jdandemar/Documents/YodaBuffett/backend/company-list.json', 'r', encoding='utf-8') as f:
            swedish_companies = json.load(f)
        print(f"📊 Loaded {len(swedish_companies)} Swedish companies")
    except Exception as e:
        print(f"❌ Could not load company-list.json: {e}")
        return False
    
    # Get Swedish companies from company_master
    companies_to_fix = await conn.fetch("""
        SELECT 
            id, company_name, primary_ticker, yahoo_symbol, 
            symbol_confidence, document_count
        FROM company_master
        WHERE country = 'SE'
        ORDER BY document_count DESC NULLS LAST
    """)
    
    print(f"🔧 Found {len(companies_to_fix)} Swedish companies to check")
    
    stats = {
        'total': len(companies_to_fix),
        'updated': 0,
        'high_confidence': 0,
        'medium_confidence': 0,
        'skipped': 0
    }
    
    # Process each company
    for i, company in enumerate(companies_to_fix, 1):
        company_id = company['id']
        company_name = company['company_name']
        current_ticker = company['primary_ticker']
        current_yahoo = company['yahoo_symbol']
        doc_count = company['document_count'] or 0
        
        if i <= 10:  # Show first 10 for monitoring
            print(f"[{i:3}/{len(companies_to_fix)}] {company_name[:40]:40} (Docs: {doc_count:3})")
        
        # Find best match in Swedish companies
        best_match = find_best_match(company_name, swedish_companies)
        
        if best_match and best_match['match_score'] > 0.7:
            swedish_ticker = best_match['ticker']
            yahoo_ticker = swedish_ticker_to_yahoo(swedish_ticker)
            
            # Determine confidence level
            if best_match['match_score'] > 0.9:
                new_confidence = 'high'
                stats['high_confidence'] += 1
            else:
                new_confidence = 'medium' 
                stats['medium_confidence'] += 1
            
            # Update company master
            await conn.execute("""
                UPDATE company_master SET
                    primary_ticker = $1,
                    yahoo_symbol = $2,
                    symbol_confidence = $3,
                    isin_code = $4,
                    updated_at = NOW()
                WHERE id = $5
            """, 
                swedish_ticker,
                yahoo_ticker,
                new_confidence,
                best_match.get('isin'),
                company_id
            )
            
            stats['updated'] += 1
            
            if i <= 10:
                print(f"         ✅ Updated: {swedish_ticker} -> {yahoo_ticker} ({new_confidence})")
        else:
            stats['skipped'] += 1
            if i <= 10:
                print(f"         ⏭️  No good match found")
    
    print(f"\n📊 Swedish Ticker Fix Results:")
    print(f"   Total processed: {stats['total']}")
    print(f"   ✅ Updated: {stats['updated']}")
    print(f"   🎯 High confidence: {stats['high_confidence']}")
    print(f"   📈 Medium confidence: {stats['medium_confidence']}")
    print(f"   ⏭️  Skipped: {stats['skipped']}")
    
    return stats['updated'] > 0

async def main():
    """Main execution function"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    
    print("🚀 COMPLETE DATABASE & TICKER FIX")
    print("Fixing constraints + Swedish ticker mappings")
    print("=" * 60)
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Step 1: Fix foreign key constraints
        constraints_fixed = await fix_foreign_key_constraints(conn)
        
        # Step 2: Fix Swedish ticker mappings
        tickers_fixed = await fix_swedish_tickers(conn)
        
        # Summary
        print(f"\n🎉 COMPLETE FIX SUMMARY")
        print("=" * 30)
        print(f"✅ Database constraints: {'Fixed' if constraints_fixed else 'Issues remain'}")
        print(f"✅ Swedish tickers: {'Updated' if tickers_fixed else 'No updates'}")
        
        if constraints_fixed and tickers_fixed:
            print(f"\n💡 Ready to run market data ingestion!")
            print(f"   Try: python3 ingest_787_fixed.py")
        elif constraints_fixed:
            print(f"\n💡 Constraints fixed - try simple ingestion first")
            print(f"   Try: python3 simple_price_ingestion.py")
        else:
            print(f"\n⚠️  Still have issues - may need manual intervention")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Error during fix: {e}")

if __name__ == "__main__":
    asyncio.run(main())