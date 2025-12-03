#!/usr/bin/env python3
"""
Populate the company master table from all available data sources:
1. Document database companies
2. Existing market_data_symbols  
3. Known Nordic company mappings
4. External data sources
"""

import asyncio
import asyncpg
import re
import json
from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime

def clean_company_name(name: str) -> str:
    """Clean company name for standardization"""
    if not name:
        return ""
    
    clean = name.replace('_', ' ').strip()
    clean = re.sub(r'\s+', ' ', clean)  # Multiple spaces to single
    return clean

def generate_company_slug(name: str) -> str:
    """Generate URL-safe company slug"""
    clean = clean_company_name(name)
    slug = re.sub(r'[^a-zA-Z0-9\s]', '', clean.lower())
    slug = re.sub(r'\s+', '-', slug)
    slug = slug.strip('-')
    return slug[:100]  # Limit length

def determine_exchange_info(company_name: str, country: str = None) -> Dict[str, str]:
    """Determine exchange information from company context"""
    
    exchange_mapping = {
        'SE': {
            'primary_exchange': 'Stockholm',
            'exchange_mic_code': 'XSTO', 
            'currency': 'SEK',
            'yahoo_suffix': '.ST'
        },
        'NO': {
            'primary_exchange': 'Oslo',
            'exchange_mic_code': 'XOSL',
            'currency': 'NOK', 
            'yahoo_suffix': '.OL'
        },
        'DK': {
            'primary_exchange': 'Copenhagen',
            'exchange_mic_code': 'XCSE',
            'currency': 'DKK',
            'yahoo_suffix': '.CO'
        },
        'FI': {
            'primary_exchange': 'Helsinki',
            'exchange_mic_code': 'XHEL',
            'currency': 'EUR',
            'yahoo_suffix': '.HE'
        }
    }
    
    # Default to Swedish for Nordic companies
    country_code = country or 'SE'
    return exchange_mapping.get(country_code, exchange_mapping['SE'])

def generate_ticker_symbols(company_name: str, country: str = 'SE') -> Dict[str, str]:
    """Generate ticker symbols for a company"""
    
    # Known symbol mappings
    known_symbols = {
        'AAK': ('AAK', 'AAK.ST'),
        'Volvo Group': ('VOLV-B', 'VOLV-B.ST'),
        'Atlas Copco AB': ('ATCO-A', 'ATCO-A.ST'),
        'Atlas Copco': ('ATCO-A', 'ATCO-A.ST'),
        'Telefonaktiebolaget LM Ericsson': ('ERIC-B', 'ERIC-B.ST'),
        'Ericsson': ('ERIC-B', 'ERIC-B.ST'),
        'ABB Ltd': ('ABB', 'ABB.ST'),
        'ABB': ('ABB', 'ABB.ST'),
        'H&M': ('HM-B', 'HM-B.ST'),
        'HM Hennes & Mauritz AB': ('HM-B', 'HM-B.ST'),
        'Hennes & Mauritz': ('HM-B', 'HM-B.ST'),
        'Electrolux AB': ('ELUX-B', 'ELUX-B.ST'),
        'Electrolux': ('ELUX-B', 'ELUX-B.ST'),
        'Sandvik AB': ('SAND', 'SAND.ST'),
        'Sandvik': ('SAND', 'SAND.ST'),
        'SSAB AB': ('SSAB-A', 'SSAB-A.ST'),
        'SSAB': ('SSAB-A', 'SSAB-A.ST'),
        'Hexagon AB': ('HEXA-B', 'HEXA-B.ST'),
        'Hexagon': ('HEXA-B', 'HEXA-B.ST'),
        'Evolution AB': ('EVO', 'EVO.ST'),
        'Evolution': ('EVO', 'EVO.ST'),
        'Investor AB': ('INVE-B', 'INVE-B.ST'),
        'Investor': ('INVE-B', 'INVE-B.ST'),
        'Kinnevik AB': ('KINV-B', 'KINV-B.ST'),
        'Kinnevik': ('KINV-B', 'KINV-B.ST'),
        'Swedbank': ('SWED-A', 'SWED-A.ST'),
        'Tele2 AB': ('TEL2-B', 'TEL2-B.ST'),
        'Tele2': ('TEL2-B', 'TEL2-B.ST'),
        'ICA Gruppen AB': ('ICA', 'ICA.ST'),
        'ICA Gruppen': ('ICA', 'ICA.ST'),
        'ICA': ('ICA', 'ICA.ST'),
        'Getinge': ('GETI-B', 'GETI-B.ST'),
        'Addtech': ('ADDT-B', 'ADDTECH-B.ST'),
        'Attendo': ('ATT', 'ATT.ST'),
        'Avanza Bank': ('AZA', 'AZA.ST'),
        'Avanza': ('AZA', 'AZA.ST'),
        'Castellum': ('CAST', 'CAST.ST'),
        'Epiroc AB': ('EPI-A', 'EPI-A.ST'),
        'Epiroc': ('EPI-A', 'EPI-A.ST'),
        'Swedish Orphan Biovitrum': ('SOBI', 'SOBI.ST'),
        'SOBI': ('SOBI', 'SOBI.ST'),
    }
    
    clean_name = clean_company_name(company_name)
    
    # Check known mappings first
    for known_name, (ticker, yahoo) in known_symbols.items():
        if (clean_name.lower() == known_name.lower() or 
            known_name.lower() in clean_name.lower() or
            clean_name.lower() in known_name.lower()):
            return {
                'primary_ticker': ticker,
                'yahoo_symbol': yahoo,
                'symbol_confidence': 'high'
            }
    
    # Generate ticker for unknown companies
    exchange_info = determine_exchange_info(company_name, country)
    
    words = clean_name.split()
    if len(words) == 1:
        ticker = clean_name[:6].upper()
    else:
        ticker = ''.join(word[0].upper() for word in words if word)[:8]
    
    ticker = re.sub(r'[^A-Z0-9]', '', ticker)
    if len(ticker) < 2:
        ticker = clean_name[:4].upper()
    
    yahoo_symbol = f"{ticker}{exchange_info['yahoo_suffix']}"
    
    return {
        'primary_ticker': ticker,
        'yahoo_symbol': yahoo_symbol,
        'symbol_confidence': 'low'
    }

def classify_company_size(doc_count: int) -> str:
    """Classify company by market cap tier based on document volume"""
    if doc_count >= 100:
        return 'large_cap'  # Lots of documents = probably large company
    elif doc_count >= 50:
        return 'mid_cap'
    elif doc_count >= 20:
        return 'small_cap'
    else:
        return 'micro_cap'

async def populate_from_documents():
    """Populate company master from document database"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("📊 Analyzing document database companies...")
        
        # Get all companies from documents with stats
        document_companies = await conn.fetch("""
            SELECT 
                company_name,
                COUNT(*) as doc_count,
                MIN(year) as first_year,
                MAX(year) as last_year,
                COUNT(DISTINCT year) as year_span,
                array_agg(DISTINCT company_name) as name_variants
            FROM extracted_documents
            WHERE company_name IS NOT NULL 
            AND company_name != '' 
            AND company_name != 'None'
            AND LENGTH(company_name) > 1
            GROUP BY company_name
            ORDER BY COUNT(*) DESC
        """)
        
        print(f"Found {len(document_companies)} unique companies in documents")
        
        inserted = 0
        failed = 0
        
        for company in document_companies:
            name = company['company_name']
            doc_count = company['doc_count']
            
            try:
                # Generate company information
                clean_name = clean_company_name(name)
                slug = generate_company_slug(name)
                
                # Generate symbols
                symbol_info = generate_ticker_symbols(name)
                
                # Exchange info (default to Swedish)
                exchange_info = determine_exchange_info(name)
                
                # Company classification
                market_cap_tier = classify_company_size(doc_count)
                
                # Data quality score based on document volume and naming
                quality_score = min(1.0, 0.1 + (doc_count * 0.01) + 
                                  (0.3 if symbol_info['symbol_confidence'] == 'high' else 0.0))
                
                # Insert into company master
                await conn.execute("""
                    INSERT INTO company_master (
                        company_name, company_name_clean, company_slug,
                        primary_ticker, yahoo_symbol,
                        primary_exchange, exchange_mic_code, currency,
                        country, region,
                        market_cap_tier, 
                        document_company_names, document_count, 
                        first_document_year, last_document_year,
                        symbol_confidence, data_quality_score
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
                        $11, $12, $13, $14, $15, $16, $17
                    ) ON CONFLICT (company_slug) DO UPDATE SET
                        document_count = EXCLUDED.document_count,
                        first_document_year = EXCLUDED.first_document_year,
                        last_document_year = EXCLUDED.last_document_year,
                        updated_at = NOW()
                """, 
                    name, clean_name, slug,
                    symbol_info['primary_ticker'], symbol_info['yahoo_symbol'],
                    exchange_info['primary_exchange'], exchange_info['exchange_mic_code'], 
                    exchange_info['currency'],
                    'SE', 'nordic',  # Default to Swedish/Nordic
                    market_cap_tier,
                    [name], doc_count,
                    company['first_year'], company['last_year'], 
                    symbol_info['symbol_confidence'], quality_score
                )
                
                inserted += 1
                
                if inserted <= 50 or doc_count >= 20:  # Show first 50 or high priority
                    confidence_icon = "🎯" if symbol_info['symbol_confidence'] == 'high' else "❓"
                    print(f"{confidence_icon} {name[:40]:40} -> {symbol_info['primary_ticker']:8} "
                          f"({doc_count:3} docs, quality: {quality_score:.2f})")
                
            except Exception as e:
                failed += 1
                if failed <= 10:
                    print(f"❌ Failed {name}: {e}")
        
        print(f"\n✅ Document Population Complete:")
        print(f"   Inserted: {inserted} companies")
        print(f"   Failed: {failed} companies")
        
        return inserted
        
    finally:
        await conn.close()

async def enhance_with_market_data():
    """Enhance company master with existing market data symbols"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("\n📈 Enhancing with existing market data...")
        
        # Check if old market_data_symbols table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'market_data_symbols'
            )
        """)
        
        if table_exists:
            market_symbols = await conn.fetch("""
                SELECT * FROM market_data_symbols
            """)
            
            print(f"Found {len(market_symbols)} existing market symbols")
            
            enhanced = 0
            for symbol_row in market_symbols:
                # Try to match with existing company
                company_id = await conn.fetchval("""
                    UPDATE company_master SET
                        primary_ticker = COALESCE(primary_ticker, $1),
                        yahoo_symbol = COALESCE(yahoo_symbol, $2),
                        sector = COALESCE(sector, $3),
                        industry = COALESCE(industry, $4),
                        market_cap_usd = COALESCE(market_cap_usd, $5),
                        yahoo_finance_available = true,
                        symbol_confidence = CASE 
                            WHEN symbol_confidence = 'high' THEN symbol_confidence
                            ELSE 'medium' 
                        END,
                        data_quality_score = GREATEST(data_quality_score, 0.7),
                        updated_at = NOW()
                    WHERE company_name_clean = $6 
                       OR $7 = ANY(document_company_names)
                    RETURNING id
                """, 
                    symbol_row['symbol'],
                    symbol_row['yahoo_symbol'], 
                    symbol_row.get('sector'),
                    symbol_row.get('industry'),
                    symbol_row.get('market_cap'),
                    clean_company_name(symbol_row['company_name']),
                    symbol_row['company_name']
                )
                
                if company_id:
                    enhanced += 1
            
            print(f"✅ Enhanced {enhanced} companies with market data")
        
    finally:
        await conn.close()

async def show_population_results():
    """Show results of company master population"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Overall stats
        total_companies = await conn.fetchval("SELECT COUNT(*) FROM company_master")
        
        # By confidence level
        confidence_stats = await conn.fetch("""
            SELECT symbol_confidence, COUNT(*) as count
            FROM company_master
            GROUP BY symbol_confidence
            ORDER BY COUNT(*) DESC
        """)
        
        # By document count
        priority_stats = await conn.fetch("""
            SELECT 
                CASE 
                    WHEN document_count >= 50 THEN 'High Priority (50+ docs)'
                    WHEN document_count >= 20 THEN 'Medium Priority (20-49 docs)'
                    WHEN document_count >= 5 THEN 'Low Priority (5-19 docs)'
                    ELSE 'Minimal Priority (1-4 docs)'
                END as priority,
                COUNT(*) as count,
                AVG(data_quality_score) as avg_quality
            FROM company_master
            GROUP BY 
                CASE 
                    WHEN document_count >= 50 THEN 'High Priority (50+ docs)'
                    WHEN document_count >= 20 THEN 'Medium Priority (20-49 docs)'
                    WHEN document_count >= 5 THEN 'Low Priority (5-19 docs)'
                    ELSE 'Minimal Priority (1-4 docs)'
                END
            ORDER BY MIN(document_count) DESC
        """)
        
        # Top companies
        top_companies = await conn.fetch("""
            SELECT 
                company_name, primary_ticker, yahoo_symbol,
                document_count, symbol_confidence, data_quality_score
            FROM company_master
            ORDER BY document_count DESC, data_quality_score DESC
            LIMIT 20
        """)
        
        print(f"\n📊 COMPANY MASTER POPULATION RESULTS")
        print(f"=" * 60)
        print(f"Total Companies: {total_companies}")
        
        print(f"\n🎯 Symbol Confidence Distribution:")
        for stat in confidence_stats:
            print(f"   {stat['symbol_confidence']:10}: {stat['count']:4} companies")
        
        print(f"\n📈 Priority Distribution:")
        for stat in priority_stats:
            print(f"   {stat['priority']:30}: {stat['count']:4} companies "
                  f"(avg quality: {stat['avg_quality']:.2f})")
        
        print(f"\n🏆 Top 20 Companies by Document Volume:")
        print(f"{'Company':35} {'Ticker':8} {'Yahoo':12} {'Docs':5} {'Conf':8} {'Quality':7}")
        print("-" * 85)
        
        for company in top_companies:
            print(f"{company['company_name'][:34]:35} "
                  f"{company['primary_ticker'] or 'N/A':8} "
                  f"{company['yahoo_symbol'] or 'N/A':12} "
                  f"{company['document_count']:5} "
                  f"{company['symbol_confidence']:8} "
                  f"{company['data_quality_score']:.3f}")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    print("🏗️  COMPANY MASTER TABLE POPULATION")
    print("Building comprehensive company database from all sources")
    print("=" * 60)
    
    # Run population steps
    asyncio.run(populate_from_documents())
    asyncio.run(enhance_with_market_data())
    asyncio.run(show_population_results())
    
    print(f"\n✅ Company Master Table populated successfully!")
    print(f"\n💡 Next steps:")
    print(f"   1. Review high-confidence companies")
    print(f"   2. Start historical data ingestion")
    print(f"   3. Add external data sources (websites, APIs)")
    print(f"   4. Build company matching algorithms")