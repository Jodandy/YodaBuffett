#!/usr/bin/env python3
"""
Manual Slug Resolution Helper
Helps manually resolve failed company slugs to correct company names
"""
import asyncio
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicCompany
from nordic_ingestion.common.company_mappings import COMPANY_SLUG_TO_NAME
from sqlalchemy import select, func

async def search_companies(search_term: str, limit: int = 10):
    """Search for companies by name"""
    async with AsyncSessionLocal() as db:
        # Exact match first
        result = await db.execute(
            select(NordicCompany.id, NordicCompany.name).where(
                func.lower(NordicCompany.name) == func.lower(search_term)
            ).limit(limit)
        )
        exact_matches = result.fetchall()
        
        if exact_matches:
            return [{"id": str(row[0]), "name": row[1], "match_type": "exact"} for row in exact_matches]
        
        # Partial matches
        result = await db.execute(
            select(NordicCompany.id, NordicCompany.name).where(
                func.lower(NordicCompany.name).contains(func.lower(search_term))
            ).limit(limit)
        )
        partial_matches = result.fetchall()
        
        return [{"id": str(row[0]), "name": row[1], "match_type": "partial"} for row in partial_matches]

async def show_slug_suggestions(slug: str):
    """Show suggestions for a given slug"""
    print(f"\n🔍 Resolving slug: '{slug}'")
    
    # Check if already in mappings
    if slug in COMPANY_SLUG_TO_NAME:
        print(f"   ✅ Already mapped: {COMPANY_SLUG_TO_NAME[slug]}")
        return COMPANY_SLUG_TO_NAME[slug]
    
    # Try different variations
    suggestions = []
    
    # Basic transformation: slug to title case
    basic_name = slug.replace('-', ' ').title()
    matches = await search_companies(basic_name)
    if matches:
        suggestions.extend(matches)
    
    # Try first word
    first_word = slug.split('-')[0]
    if len(first_word) > 3:
        matches = await search_companies(first_word)
        suggestions.extend(matches)
    
    # Try without common suffixes
    for suffix in ['-group', '-holding', '-ab', '-publ']:
        if slug.endswith(suffix):
            base = slug[:-len(suffix)]
            base_name = base.replace('-', ' ').title()
            matches = await search_companies(base_name)
            suggestions.extend(matches)
    
    # Remove duplicates
    seen = set()
    unique_suggestions = []
    for s in suggestions:
        key = (s['id'], s['name'])
        if key not in seen:
            seen.add(key)
            unique_suggestions.append(s)
    
    if unique_suggestions:
        print(f"   💡 Suggestions:")
        for i, match in enumerate(unique_suggestions[:10]):
            print(f"      {i+1}. {match['name']} ({match['match_type']} match)")
        return unique_suggestions
    else:
        print(f"   ❌ No automatic suggestions found")
        return []

async def add_mapping(slug: str, company_name: str):
    """Add a new mapping to the system"""
    # Verify the company exists
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(NordicCompany.id, NordicCompany.name).where(
                func.lower(NordicCompany.name) == func.lower(company_name)
            ).limit(1)
        )
        company = result.fetchone()
        
        if not company:
            print(f"   ❌ Company '{company_name}' not found in database")
            return False
        
        print(f"   ✅ Found company: {company[1]} (ID: {company[0]})")
        
        # Show where to add the mapping
        print(f"\n   📝 Add this to company_mappings.py:")
        print(f'   "{slug}": "{company[1]}",')
        
        return True

async def manual_resolve():
    """Interactive manual resolution"""
    # Get failed slugs
    with open('company_attribution_fix_20250901_014228.json', 'r') as f:
        data = json.load(f)
        failed = data.get('failed_resolutions', [])
    
    unique_slugs = set()
    for failure in failed:
        if 'slug' in failure:
            unique_slugs.add(failure['slug'])
    
    sorted_slugs = sorted(unique_slugs)
    print(f"🚀 Manual Slug Resolution")
    print(f"Total unique failed slugs: {len(sorted_slugs)}")
    
    resolved_mappings = []
    
    for i, slug in enumerate(sorted_slugs):
        print(f"\n" + "="*60)
        print(f"Progress: {i+1}/{len(sorted_slugs)}")
        
        suggestions = await show_slug_suggestions(slug)
        
        print(f"\nOptions for '{slug}':")
        print("1. Use one of the suggestions above")
        print("2. Enter custom company name to search")
        print("3. Skip this slug")
        print("4. Quit")
        
        choice = input("\nYour choice (1-4): ").strip()
        
        if choice == '4':
            break
        elif choice == '3':
            print(f"   ⏭️  Skipping {slug}")
            continue
        elif choice == '2':
            custom_name = input("Enter company name to search: ").strip()
            if custom_name:
                matches = await search_companies(custom_name)
                if matches:
                    print("Found matches:")
                    for j, match in enumerate(matches[:5]):
                        print(f"   {j+1}. {match['name']}")
                    
                    match_choice = input("Select match number (or Enter to skip): ").strip()
                    if match_choice.isdigit() and 1 <= int(match_choice) <= len(matches):
                        selected = matches[int(match_choice)-1]
                        success = await add_mapping(slug, selected['name'])
                        if success:
                            resolved_mappings.append((slug, selected['name']))
                else:
                    print("No matches found")
        elif choice == '1' and suggestions:
            match_choice = input("Select suggestion number: ").strip()
            if match_choice.isdigit() and 1 <= int(match_choice) <= len(suggestions):
                selected = suggestions[int(match_choice)-1]
                success = await add_mapping(slug, selected['name'])
                if success:
                    resolved_mappings.append((slug, selected['name']))
    
    # Summary
    print(f"\n" + "="*60)
    print(f"🎉 Manual Resolution Complete!")
    print(f"Resolved {len(resolved_mappings)} mappings:")
    for slug, company in resolved_mappings:
        print(f'   "{slug}": "{company}",')

if __name__ == "__main__":
    asyncio.run(manual_resolve())