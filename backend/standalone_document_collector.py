#!/usr/bin/env python3
"""
Standalone Document Collector

Direct document collection without complex worker infrastructure dependencies.
Uses simple HTTP requests to MFN.se for Swedish companies.
"""

import asyncio
import asyncpg
import aiohttp
import json
import time
from datetime import datetime, date
import re
from urllib.parse import urljoin
from pathlib import Path
import hashlib

async def get_companies_to_process():
    """Get Swedish companies that need document collection"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    query = """
    SELECT 
        id,
        primary_ticker as symbol,
        company_name as name,
        document_count
    FROM company_master
    WHERE country IN ('Sverige', 'SE')
    AND (document_count IS NULL OR document_count < 10)
    AND primary_ticker IS NOT NULL
    AND company_name IS NOT NULL
    ORDER BY COALESCE(document_count, 0), primary_ticker
    """
    
    companies = await conn.fetch(query)
    await conn.close()
    
    return [dict(c) for c in companies]

def clean_company_name_for_slug(name):
    """Convert company name to MFN slug format"""
    
    # Remove common suffixes
    cleaned = re.sub(r'\s+(AB|A|B|publ|\(publ\)|Group|AS|ASA|Ltd|Limited|Inc|Corp|Corporation)(\s|$)', ' ', name, flags=re.IGNORECASE).strip()
    
    # Convert to lowercase and handle Swedish characters
    cleaned = cleaned.lower()
    cleaned = cleaned.replace('å', 'a').replace('ä', 'a').replace('ö', 'o')
    
    # Replace spaces and special characters with hyphens
    cleaned = re.sub(r'[^a-z0-9]+', '-', cleaned)
    
    # Remove leading/trailing hyphens
    cleaned = cleaned.strip('-')
    
    return cleaned

async def test_mfn_company_url(session, symbol, name):
    """Test if company exists on MFN.se and return document links"""
    
    base_url = "https://www.mfn.se/aktie/"
    
    # Generate potential slugs
    test_slugs = [
        clean_company_name_for_slug(name),
        clean_company_name_for_slug(name.split()[0]),  # First word only
        symbol.lower().replace(' ', '-'),
        symbol.lower().replace(' ', '').replace('.', '').replace('-', ''),
        clean_company_name_for_slug(name.replace(' ', '')),
    ]
    
    # Remove duplicates while preserving order
    unique_slugs = []
    for slug in test_slugs:
        if slug and slug not in unique_slugs:
            unique_slugs.append(slug)
    
    for slug in unique_slugs:
        try:
            url = f"{base_url}{slug}"
            
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    content = await response.text()
                    
                    # Check if this is a valid company page (not 404 redirect)
                    if 'dokument' in content.lower() and len(content) > 5000:
                        print(f"   ✅ Found company page: {slug}")
                        
                        # Extract document links
                        documents = extract_document_links(content, url)
                        return {'status': 'found', 'slug': slug, 'url': url, 'documents': documents}
            
            # Small delay between requests
            await asyncio.sleep(0.5)
            
        except Exception as e:
            continue
    
    return {'status': 'not_found', 'tested_slugs': unique_slugs}

def extract_document_links(html_content, base_url):
    """Extract document links from MFN company page"""
    
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html_content, 'html.parser')
    documents = []
    
    # Look for PDF links
    pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
    
    for link in pdf_links:
        href = link.get('href')
        text = link.get_text(strip=True)
        
        if href:
            # Make absolute URL
            if href.startswith('/'):
                full_url = urljoin('https://www.mfn.se', href)
            elif href.startswith('http'):
                full_url = href
            else:
                full_url = urljoin(base_url, href)
            
            # Classify document type
            doc_type = classify_document_type(text, full_url)
            
            documents.append({
                'title': text,
                'url': full_url,
                'type': doc_type
            })
    
    return documents

def classify_document_type(title, url):
    """Classify document type from title/URL"""
    
    title_lower = title.lower()
    url_lower = url.lower()
    
    if any(word in title_lower for word in ['årsredovisning', 'annual', 'year']):
        return 'annual_report'
    elif any(word in title_lower for word in ['kvartalsrapport', 'delårsrapport', 'quarterly', 'interim', 'q1', 'q2', 'q3', 'q4']):
        return 'quarterly_report'
    elif any(word in title_lower for word in ['pressmeddelande', 'press', 'news', 'meddelande']):
        return 'press_release'
    else:
        return 'other'

async def save_documents_to_db(conn, company_id, symbol, documents):
    """Save discovered documents to nordic_documents table"""
    
    saved_count = 0
    
    for doc in documents:
        try:
            # Generate document hash
            doc_hash = hashlib.md5(doc['url'].encode()).hexdigest()
            
            # Check if document already exists
            existing = await conn.fetchval(
                "SELECT id FROM nordic_documents WHERE file_hash = $1", doc_hash
            )
            
            if existing:
                continue  # Skip duplicates
            
            # Insert new document
            await conn.execute("""
                INSERT INTO nordic_documents (
                    id, company_id, document_type, title, source_url, 
                    file_hash, language, ingestion_date, processing_status,
                    metadata, created_at, updated_at
                ) VALUES (
                    gen_random_uuid(), $1, $2, $3, $4, $5, 'sv', NOW(), 'discovered',
                    $6, NOW(), NOW()
                )
            """, 
            company_id, doc['type'], doc['title'], doc['url'], doc_hash,
            json.dumps({'discovered_by': 'standalone_collector', 'symbol': symbol})
            )
            
            saved_count += 1
            
        except Exception as e:
            print(f"     ⚠️ Error saving document {doc['title']}: {e}")
            continue
    
    return saved_count

async def process_swedish_companies():
    """Process all Swedish companies needing documents"""
    
    print("🇸🇪 STANDALONE SWEDISH DOCUMENT COLLECTION")
    print("=" * 70)
    
    companies = await get_companies_to_process()
    
    print(f"📊 Found {len(companies)} Swedish companies needing document collection")
    
    if not companies:
        print("✅ No companies need document collection!")
        return
    
    # Database connection for saving results
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    results = {
        'successful': [],
        'failed': [],
        'start_time': datetime.now().isoformat()
    }
    
    start_time = time.time()
    
    # Create HTTP session
    async with aiohttp.ClientSession() as session:
        
        for i, company in enumerate(companies, 1):
            company_id = company['id']
            symbol = company['symbol']
            name = company['name']
            existing_docs = company['document_count'] or 0
            
            print(f"\n📈 {i}/{len(companies)} - {symbol} ({name}) [Has: {existing_docs} docs]")
            
            processing_start = time.time()
            
            try:
                # Test MFN URL
                result = await test_mfn_company_url(session, symbol, name)
                
                if result['status'] == 'found':
                    documents = result['documents']
                    
                    if documents:
                        # Save documents to database
                        saved_count = await save_documents_to_db(conn, company_id, symbol, documents)
                        
                        # Update company document count
                        await conn.execute(
                            "UPDATE company_master SET document_count = COALESCE(document_count, 0) + $1, updated_at = NOW() WHERE id = $2",
                            saved_count, company_id
                        )
                        
                        processing_time = time.time() - processing_start
                        print(f"   ✅ Success: {len(documents)} documents found, {saved_count} saved ({processing_time:.1f}s)")
                        
                        results['successful'].append({
                            'symbol': symbol,
                            'name': name,
                            'documents_found': len(documents),
                            'documents_saved': saved_count,
                            'slug': result['slug'],
                            'processing_time': processing_time
                        })
                    else:
                        processing_time = time.time() - processing_start
                        print(f"   ⚠️ Page found but no documents extracted ({processing_time:.1f}s)")
                        
                        results['failed'].append({
                            'symbol': symbol,
                            'name': name,
                            'reason': 'no_documents_on_page',
                            'slug': result['slug'],
                            'processing_time': processing_time
                        })
                else:
                    processing_time = time.time() - processing_start
                    print(f"   ❌ Company page not found ({processing_time:.1f}s)")
                    
                    results['failed'].append({
                        'symbol': symbol,
                        'name': name,
                        'reason': 'page_not_found',
                        'tested_slugs': result['tested_slugs'],
                        'processing_time': processing_time
                    })
                
            except Exception as e:
                processing_time = time.time() - processing_start
                print(f"   ❌ Error: {str(e)[:60]}... ({processing_time:.1f}s)")
                
                results['failed'].append({
                    'symbol': symbol,
                    'name': name,
                    'reason': 'processing_error',
                    'error': str(e)[:200],
                    'processing_time': processing_time
                })
            
            # Progress update every 25 companies
            if i % 25 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed if elapsed > 0 else 0
                eta = (len(companies) - i) / rate if rate > 0 else 0
                
                print(f"\n📊 PROGRESS: {i}/{len(companies)} ({i/len(companies)*100:.0f}%)")
                print(f"   Success: {len(results['successful'])}, Failed: {len(results['failed'])}")
                print(f"   Rate: {rate:.1f}/min, ETA: {eta/60:.0f} min")
            
            # Respectful delay
            if i < len(companies):
                await asyncio.sleep(3)  # 3 second delay between companies
    
    await conn.close()
    
    # Final summary
    results['end_time'] = datetime.now().isoformat()
    total_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("🎯 SWEDISH DOCUMENT COLLECTION COMPLETE")
    print("=" * 70)
    
    successful_count = len(results['successful'])
    failed_count = len(results['failed'])
    
    print(f"\n📊 FINAL RESULTS:")
    print(f"   Companies processed: {len(companies)}")
    print(f"   Successful: {successful_count}")
    print(f"   Failed: {failed_count}")
    print(f"   Success rate: {successful_count/len(companies)*100:.1f}%")
    print(f"   Total time: {total_time/60:.1f} minutes")
    
    if results['successful']:
        total_docs_found = sum(r['documents_found'] for r in results['successful'])
        total_docs_saved = sum(r['documents_saved'] for r in results['successful'])
        print(f"   Total documents found: {total_docs_found:,}")
        print(f"   Total documents saved: {total_docs_saved:,}")
    
    # Failure analysis
    if results['failed']:
        failure_reasons = {}
        for failed in results['failed']:
            reason = failed.get('reason', 'unknown')
            failure_reasons[reason] = failure_reasons.get(reason, 0) + 1
        
        print(f"\n❌ FAILURE ANALYSIS:")
        for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
            print(f"   {reason}: {count} companies")
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"standalone_collection_results_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Complete results saved to {filename}")
    
    # Show some examples
    if results['successful']:
        print(f"\n✅ SUCCESSFUL COLLECTIONS (sample):")
        for result in results['successful'][:10]:
            print(f"   {result['symbol']}: {result['documents_found']} docs found, {result['documents_saved']} saved")
    
    return results

if __name__ == "__main__":
    asyncio.run(process_swedish_companies())