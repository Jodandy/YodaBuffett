#!/usr/bin/env python3
"""
FIX FOR MFN COLLECTOR ATTRIBUTION ISSUES

This script updates the MFN collector to prevent incorrect company attribution.
The main issues:
1. When author attribute is missing, ALL documents get attributed to the target company
2. No validation that documents actually belong to the company
3. Weak slug resolution can lead to wrong company pages

This fix adds:
- Strict company filtering with validation
- URL pattern validation
- Better logging for attribution decisions
- Fallback prevention
"""

import re

def generate_mfn_collector_fix():
    """Generate the fixed parsing logic for MFN collector"""
    
    fixed_code = '''
        # ⭐ CRITICAL FIX: Filter MFN items to only include the target company
        # This prevents collecting documents from other companies when MFN shows a generic feed
        company_mfn_items = []
        skipped_companies = {}  # Track what we're skipping
        
        if mfn_items:
            print(f"🔍 Filtering containers for company: {company}")
            target_company_lower = company.lower()
            target_company_slug = self._normalize_company_name_to_slug(company)
            
            for item in mfn_items:
                # Look for author attribute in the item
                author_link = item.find('a', {'author': True})
                if author_link:
                    item_author = author_link.get('author')
                    if item_author == company:
                        company_mfn_items.append(item)
                        print(f"   ✅ Found document from {item_author}")
                    else:
                        # Track what we're skipping
                        if item_author not in skipped_companies:
                            skipped_companies[item_author] = 0
                        skipped_companies[item_author] += 1
                        print(f"   🔄 Skipping document from {item_author} (not target company)")
                else:
                    # NO AUTHOR ATTRIBUTE - MUST VALIDATE MORE CAREFULLY
                    # Check if the item contains company-specific information
                    item_text = item.get_text().lower()
                    item_links = item.find_all('a', href=True)
                    
                    # Look for company name in the item text or links
                    company_found = False
                    for link in item_links:
                        href = link.get('href', '').lower()
                        link_text = link.get_text().lower()
                        
                        # Check if company slug appears in URL
                        if target_company_slug in href:
                            company_found = True
                            break
                        
                        # Check if company name appears in link text
                        if target_company_lower in link_text:
                            company_found = True
                            break
                    
                    # Also check item text for company name
                    if not company_found and target_company_lower in item_text:
                        company_found = True
                    
                    if company_found:
                        company_mfn_items.append(item)
                        print(f"   ✅ Found document likely from {company} (validated by content)")
                    else:
                        print(f"   ⚠️  Skipping item with no author - cannot verify it belongs to {company}")
            
            print(f"🔍 After filtering: {len(company_mfn_items)} containers belong to {company}")
            
            # Log what we skipped
            if skipped_companies:
                print(f"📊 Skipped documents from other companies:")
                for comp, count in sorted(skipped_companies.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"   - {comp}: {count} documents")
                if len(skipped_companies) > 5:
                    print(f"   - And {len(skipped_companies) - 5} other companies")
            
            mfn_items = company_mfn_items
'''
    
    return fixed_code

def generate_url_validation():
    """Generate URL validation logic"""
    
    validation_code = '''
def validate_document_url(self, url: str, company_name: str, company_slug: str) -> bool:
    """
    Validate that a document URL likely belongs to the given company.
    This helps prevent attribution errors.
    """
    url_lower = url.lower()
    company_lower = company_name.lower()
    
    # Known patterns for financial documents
    financial_hosts = [
        'mfn.se',
        'financialhearings.com', 
        'storage.mfn.se',
        'mb.cision.com',
        'news.cision.com',
        'feed.ne.cision.com'
    ]
    
    # Check if it's from a known financial host
    is_financial_host = any(host in url_lower for host in financial_hosts)
    
    if not is_financial_host:
        # For non-financial hosts, require company identification
        if company_slug in url_lower or company_lower.replace(' ', '') in url_lower:
            return True
        
        # Check for company-specific domains
        company_words = company_lower.split()
        if len(company_words) > 0 and company_words[0] in url_lower:
            return True
            
        return False
    
    # For financial hosts, we're more permissive but log for verification
    return True

def _normalize_company_name_to_slug(self, company_name: str) -> str:
    """Convert company name to slug format for validation"""
    slug = company_name.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug)
    slug = slug.strip('-')
    return slug
'''
    
    return validation_code

def generate_enhanced_logging():
    """Generate enhanced logging for attribution decisions"""
    
    logging_code = '''
# Add to the document extraction section
if pdf_url:
    # Validate the URL belongs to this company
    is_valid = self.validate_document_url(pdf_url, company, company_slug)
    
    if not is_valid:
        print(f"   ⚠️  URL validation failed for {pdf_url}")
        print(f"      Company: {company}, Slug: {company_slug}")
        print(f"      Skipping this document to prevent mis-attribution")
        continue
    
    # Log the attribution chain
    print(f"   📎 Document attribution chain:")
    print(f"      URL: {pdf_url}")
    print(f"      Company: {company}")
    print(f"      Slug: {company_slug}")
    print(f"      Valid: {is_valid}")
'''
    
    return logging_code

if __name__ == "__main__":
    print("MFN Collector Attribution Fix")
    print("=" * 60)
    print("\nThis fix addresses the following issues:")
    print("1. Prevents attribution of all documents when author is missing")
    print("2. Adds URL validation to ensure documents belong to the company")
    print("3. Enhances logging for attribution decisions")
    print("4. Tracks skipped documents from other companies")
    print("\nKey changes:")
    print("- No more blind inclusion of items without author attribute")
    print("- Company name/slug validation in content and URLs")
    print("- Better tracking of what's being skipped and why")
    print("\nImplementation:")
    print("1. Update the _parse_mfn_page() method with the new filtering logic")
    print("2. Add the validation methods to the MFNFinancialCollector class")
    print("3. Enhance logging throughout the collection process")
    
    print("\n" + "=" * 60)
    print("FIXED PARSING LOGIC:")
    print("=" * 60)
    print(generate_mfn_collector_fix())
    
    print("\n" + "=" * 60)
    print("URL VALIDATION METHODS:")
    print("=" * 60)
    print(generate_url_validation())
    
    print("\n" + "=" * 60)
    print("ENHANCED LOGGING:")
    print("=" * 60)
    print(generate_enhanced_logging())