#!/usr/bin/env python3
"""
Company Slug Resolver for MFN.se

Handles company name variations and slug resolution.
Companies like "absolent-air-care" might actually be "absolent-air-care-group" on MFN.se
"""

import asyncio
import aiohttp
from typing import List, Optional, Dict, Set
import re

class CompanySlugResolver:
    """
    Resolves company slugs by testing variations when the main slug fails
    
    Examples:
    - "absolent-air-care" → tries "absolent-air-care-group", "absolent-air-care-ab"
    - "yubico" → tries "yubico-ab", "yubico-group"
    """
    
    def __init__(self, base_url: str = "https://mfn.se/all/a"):
        self.base_url = base_url
        self.session_headers = {
            'User-Agent': 'YodaBuffett-SlugResolver/1.0 (Financial Research)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Connection': 'keep-alive',
        }
        
        # Common Swedish company suffixes
        self.common_suffixes = [
            "-group",
            "-ab",
            "-publ",
            "-holding",
            "-international",
            "-systems",
            "-tech",
            "-sweden",
            "-nordic",
            "-europe",
            "",  # Try without any suffix last
        ]
        
        # Cache successful resolutions
        self._resolved_cache: Dict[str, str] = {}
    
    async def resolve_company_slug(self, 
                                 session: aiohttp.ClientSession,
                                 original_slug: str,
                                 test_limit: int = 5) -> Optional[str]:
        """
        Find the correct company slug by testing variations
        
        Args:
            session: HTTP session
            original_slug: Original company slug that failed
            test_limit: Maximum number of variations to test
            
        Returns:
            Working slug or None if all variations fail
        """
        
        # Check cache first
        if original_slug in self._resolved_cache:
            return self._resolved_cache[original_slug]
        
        print(f"🔍 Resolving company slug: {original_slug}")
        
        # Generate slug variations to test
        variations = self._generate_slug_variations(original_slug, test_limit)
        
        print(f"   📝 Testing {len(variations)} variations: {variations}")
        
        # Test each variation
        for i, variation in enumerate(variations):
            try:
                url = f"{self.base_url}/{variation}"
                
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # Check if this page actually has company content
                        if self._validate_company_page(html, variation):
                            print(f"   ✅ Found working slug: {variation} (variation {i+1}/{len(variations)})")
                            self._resolved_cache[original_slug] = variation
                            return variation
                        else:
                            print(f"   ⚠️  {variation} returned 200 but no company content")
                    else:
                        print(f"   ❌ {variation} returned {response.status}")
                
                # Rate limiting between requests
                await asyncio.sleep(0.5)
                
            except Exception as e:
                print(f"   ❌ Error testing {variation}: {e}")
                continue
        
        print(f"   🚫 No working slug found for {original_slug}")
        return None
    
    def _generate_slug_variations(self, original_slug: str, limit: int) -> List[str]:
        """Generate variations of a company slug to test"""
        
        variations = []
        
        # If the slug already has a suffix, try removing it
        for suffix in self.common_suffixes[:-1]:  # Exclude empty string
            if original_slug.endswith(suffix):
                base_slug = original_slug[:-len(suffix)]
                # Try with different suffixes
                for new_suffix in self.common_suffixes:
                    candidate = base_slug + new_suffix
                    if candidate != original_slug and candidate not in variations:
                        variations.append(candidate)
                        if len(variations) >= limit:
                            return variations
                break
        
        # If no suffix was removed, try adding suffixes
        if not variations:
            for suffix in self.common_suffixes:
                candidate = original_slug + suffix
                if candidate != original_slug and candidate not in variations:
                    variations.append(candidate)
                    if len(variations) >= limit:
                        break
        
        # Also try some common transformations
        if len(variations) < limit:
            # Try replacing hyphens with underscores
            underscore_version = original_slug.replace('-', '_')
            if underscore_version != original_slug:
                variations.append(underscore_version)
            
            # Try without hyphens
            no_hyphen_version = original_slug.replace('-', '')
            if no_hyphen_version != original_slug:
                variations.append(no_hyphen_version)
        
        return variations[:limit]
    
    def _validate_company_page(self, html: str, slug: str) -> bool:
        """
        Check if the HTML page actually contains company content
        
        Args:
            html: Page HTML content
            slug: Company slug being tested
            
        Returns:
            True if page has company content, False otherwise
        """
        
        # Look for signs this is a real company page
        company_indicators = [
            # Swedish company indicators
            r'kvartalsrapport',
            r'årsrapport', 
            r'delårsrapport',
            r'pressmeddelande',
            
            # English indicators
            r'quarterly.*report',
            r'annual.*report',
            r'press.*release',
            r'financial.*report',
            
            # MFN-specific indicators
            r'table-calender',  # Calendar table
            r'short-item compressible',  # Document items
            r'compressed-date',  # Date spans
            
            # Stock-related indicators
            r'SEK|EUR|USD',  # Currencies
            r'\b[A-Z]{3,6}-[AB]\b',  # Stock tickers like VOLV-B
            r'aktie|share|stock',
        ]
        
        html_lower = html.lower()
        matches = 0
        
        for pattern in company_indicators:
            if re.search(pattern, html_lower, re.IGNORECASE):
                matches += 1
        
        # Need at least 2 indicators to consider it a valid company page
        is_valid = matches >= 2
        
        if is_valid:
            print(f"   📊 Page validation: {matches} indicators found for {slug}")
        else:
            print(f"   ⚠️  Page validation: Only {matches} indicators found for {slug}")
        
        return is_valid
    
    def get_cache_stats(self) -> Dict[str, any]:
        """Get cache statistics"""
        return {
            "cached_resolutions": len(self._resolved_cache),
            "cache_entries": dict(self._resolved_cache)
        }
    
    def clear_cache(self):
        """Clear the resolution cache"""
        self._resolved_cache.clear()


# Integration example:
async def enhanced_collect_company_news(mfn_collector, session, company, **kwargs):
    """
    Enhanced version of collect_company_news with automatic slug resolution
    """
    # Try original slug first
    result = await mfn_collector.collect_company_news(session, company, **kwargs)
    
    # If we got nothing, try slug resolution
    if not result or len(result) == 0:
        print(f"🔄 No results for {company}, trying slug resolution...")
        
        resolver = CompanySlugResolver()
        resolved_slug = await resolver.resolve_company_slug(session, company)
        
        if resolved_slug and resolved_slug != company:
            print(f"🎯 Retrying with resolved slug: {resolved_slug}")
            result = await mfn_collector.collect_company_news(session, resolved_slug, **kwargs)
        else:
            print(f"🚫 Could not resolve slug for {company}")
    
    return result