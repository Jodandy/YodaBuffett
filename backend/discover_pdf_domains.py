#!/usr/bin/env python3
"""
Discover Unknown PDF Domains Used by Swedish Companies

Analyzes HTML content from problematic companies to find PDF URLs
we're missing and identify new storage domains to add.
"""

import asyncio
import aiohttp
import re
from collections import Counter
from typing import Set, Dict, List
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def discover_pdf_domains(test_companies: List[str], max_companies: int = 10):
    """
    Fetch raw HTML from companies and extract ALL PDF-like URLs
    to discover new domains we're missing
    """
    
    print("🔍 DISCOVERING UNKNOWN PDF DOMAINS")
    print("=" * 50)
    print(f"Testing {min(len(test_companies), max_companies)} companies...")
    print()
    
    all_pdf_urls = []
    domain_counter = Counter()
    extension_counter = Counter()
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        headers={
            'User-Agent': 'YodaBuffett-Research/1.0 (Domain Discovery)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }
    ) as session:
        
        for i, company in enumerate(test_companies[:max_companies]):
            print(f"📊 [{i+1}/{min(len(test_companies), max_companies)}] Analyzing: {company}")
            
            url = f"https://mfn.se/all/a/{company}"
            
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # COMPREHENSIVE PDF URL EXTRACTION
                        # Look for ANY URL that might be a document
                        
                        comprehensive_patterns = [
                            # Current known patterns
                            r'https://storage\.mfn\.se/[^"\'>\s]+\.pdf',
                            r'https://mb\.cision\.com/[^"\'>\s]+\.pdf',
                            
                            # Extended domain detection
                            r'https://[^"\'>\s/]+\.se/[^"\'>\s]*\.pdf',      # Any .se domain with PDF
                            r'https://[^"\'>\s/]+\.com/[^"\'>\s]*\.pdf',     # Any .com domain with PDF
                            r'https://[^"\'>\s/]+\.org/[^"\'>\s]*\.pdf',     # Any .org domain with PDF
                            r'https://[^"\'>\s/]+\.net/[^"\'>\s]*\.pdf',     # Any .net domain with PDF
                            
                            # Document-like files (not just PDF)
                            r'https://[^"\'>\s/]+/[^"\'>\s]*\.(pdf|doc|docx|xls|xlsx|ppt|pptx)',
                            
                            # Relative paths that might be documents
                            r'/[^"\'>\s]*\.(pdf|doc|docx|xls|xlsx)',
                            
                            # Query parameters (sometimes PDFs are behind query params)
                            r'https://[^"\'>\s/]+/[^"\'>\s]*\?[^"\'>\s]*\.(pdf|doc)',
                            
                            # Common investor relations patterns
                            r'https://[^"\'>\s/]+/[^"\'>\s]*(investor|ir|report|annual|financial)[^"\'>\s]*\.(pdf|doc)',
                        ]
                        
                        company_pdfs = set()
                        
                        for pattern in comprehensive_patterns:
                            matches = re.findall(pattern, html, re.IGNORECASE)
                            for match in matches:
                                if isinstance(match, tuple):  # When capturing groups
                                    url_match = match[0] if len(match) > 0 else str(match)
                                else:
                                    url_match = match
                                
                                company_pdfs.add(url_match)
                        
                        print(f"   📄 Found {len(company_pdfs)} potential document URLs")
                        
                        # Analyze domains and extensions
                        for pdf_url in company_pdfs:
                            all_pdf_urls.append(pdf_url)
                            
                            # Extract domain
                            if pdf_url.startswith('http'):
                                try:
                                    domain = pdf_url.split('://')[1].split('/')[0]
                                    domain_counter[domain] += 1
                                except:
                                    pass
                            
                            # Extract extension
                            if '.' in pdf_url:
                                try:
                                    extension = pdf_url.split('.')[-1].split('?')[0]  # Remove query params
                                    extension_counter[extension.lower()] += 1
                                except:
                                    pass
                        
                        # Show sample URLs for this company
                        if company_pdfs:
                            print(f"   🌐 Sample URLs:")
                            for url in list(company_pdfs)[:3]:
                                url_preview = url[:80] + "..." if len(url) > 80 else url
                                print(f"      • {url_preview}")
                        else:
                            print(f"   📭 No document URLs found")
                    
                    else:
                        print(f"   ❌ HTTP {response.status} for {company}")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
            
            # Rate limiting
            if i < min(len(test_companies), max_companies) - 1:
                await asyncio.sleep(1.0)
        
        # ANALYSIS RESULTS
        print("\n" + "=" * 60)
        print("📊 DOMAIN DISCOVERY RESULTS")
        print("=" * 60)
        
        print(f"\n🌐 TOP DOMAINS HOSTING DOCUMENTS:")
        for domain, count in domain_counter.most_common(15):
            print(f"   {count:3d} URLs: {domain}")
        
        print(f"\n📄 FILE EXTENSIONS FOUND:")
        for ext, count in extension_counter.most_common(10):
            print(f"   {count:3d} files: .{ext}")
        
        print(f"\n🔍 MISSING DOMAINS TO ADD:")
        current_known_domains = ['storage.mfn.se', 'mb.cision.com']
        new_domains = []
        
        for domain, count in domain_counter.most_common(10):
            if domain not in current_known_domains and count >= 3:
                new_domains.append(domain)
        
        if new_domains:
            print("   📋 Add these domains to PDF patterns:")
            for domain in new_domains:
                pattern = f"r'https://{domain.replace('.', '\\.')}/[^\"\'>\s]+\.pdf'"
                print(f"      {pattern}")
        else:
            print("   ✅ No significant new domains found (may need broader search)")
        
        print(f"\n📋 SAMPLE URLS FOR INVESTIGATION:")
        unique_domains = set()
        for url in all_pdf_urls[:20]:  # Show sample URLs
            if url.startswith('http'):
                try:
                    domain = url.split('://')[1].split('/')[0]
                    if domain not in unique_domains:
                        unique_domains.add(domain)
                        url_preview = url[:100] + "..." if len(url) > 100 else url
                        print(f"   • {url_preview}")
                except:
                    pass
        
        return {
            'total_urls': len(all_pdf_urls),
            'domains': dict(domain_counter),
            'extensions': dict(extension_counter),
            'new_domains': new_domains,
            'sample_urls': all_pdf_urls[:50]
        }

async def main():
    """Test domain discovery on problematic companies"""
    
    # Companies with high duplicate counts but 0 documents stored
    # These are most likely to reveal unknown storage domains
    problematic_companies = [
        "2curex",        # 777 duplicates, 0 stored
        "aac-clyde-space", # 428 duplicates, 0 stored  
        "abas-protect",    # 264 duplicates, 0 stored
        "abera-bioscience", # 140 duplicates, 0 stored
        "academedia",      # 727 duplicates, 0 stored
        "acarix",          # 715 duplicates, 0 stored
        "acast",           # 744 duplicates, 0 stored
        "acconeer",        # 646 duplicates, 0 stored
        "acucort",         # 745 duplicates, 0 stored
        "acuvi",           # 704 duplicates, 0 stored
    ]
    
    print("🎯 Target Companies (High duplicates, zero documents stored):")
    for company in problematic_companies:
        print(f"   • {company}")
    print()
    
    results = await discover_pdf_domains(problematic_companies, max_companies=10)
    
    print(f"\n🎉 DISCOVERY COMPLETE!")
    print(f"   📊 Total URLs analyzed: {results['total_urls']}")
    print(f"   🌐 Unique domains: {len(results['domains'])}")
    print(f"   🆕 New domains to add: {len(results['new_domains'])}")
    
    if results['new_domains']:
        print(f"\n🚀 NEXT STEP: Add these domains to mfn_collector.py PDF patterns!")
    else:
        print(f"\n🤔 INVESTIGATION NEEDED: No obvious missing domains found.")
        print(f"   The issue might be:")
        print(f"   • PDFs embedded in JavaScript/dynamic content")
        print(f"   • PDFs behind authentication/cookies")  
        print(f"   • Document links use different file extensions")
        print(f"   • Links are relative and need base URL resolution")

if __name__ == "__main__":
    asyncio.run(main())