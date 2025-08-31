#!/usr/bin/env python3
"""
Expand Company Mappings
Add comprehensive Swedish company to MFN slug mappings

This creates an enhanced mapping file that can be imported into the batch processor
"""

# Comprehensive Swedish company mappings based on common MFN patterns
EXTENDED_COMPANY_MAPPINGS = {
    # Current working mappings
    "Volvo Group": "volvo",
    "AstraZeneca": "astrazeneca", 
    "Atlas Copco AB": "atlas-copco",
    "Telefonaktiebolaget LM Ericsson": "ericsson",
    "H&M Hennes & Mauritz AB": "handm",
    "Sandvik AB": "sandvik",
    "Nordea Bank Abp": "nordea",
    "Investor AB": "investor",
    "ABB Ltd": "abb",
    "Hexagon AB": "hexagon",
    
    # Major Swedish companies (likely MFN patterns)
    "Swedbank AB": "swedbank",
    "Handelsbanken": "handelsbanken",
    "SEB": "seb",
    "Telia Company AB": "telia",
    "Skanska AB": "skanska",
    "Electrolux AB": "electrolux",
    "SSAB AB": "ssab",
    "SKF AB": "skf",
    "Boliden AB": "boliden",
    "Swedish Match AB": "swedish-match",
    "Securitas AB": "securitas",
    "Castellum AB": "castellum",
    "Getinge AB": "getinge",
    "Alfa Laval AB": "alfa-laval",
    "Husqvarna AB": "husqvarna",
    "Trelleborg AB": "trelleborg",
    "Munters Group AB": "munters",
    "Peab AB": "peab",
    "NCC AB": "ncc",
    "JM AB": "jm",
    "Wihlborgs Fastigheter AB": "wihlborgs",
    "Sagax AB": "sagax",
    "Fastighets AB Balder": "balder",
    "Kinnevik AB": "kinnevik",
    "Lundin Energy AB": "lundin-energy",
    "Evolution Gaming": "evolution",
    "Embracer Group AB": "embracer",
    "Klarna Bank AB": "klarna",
    "Spotify Technology S.A.": "spotify",
    "H2 Green Steel": "h2-green-steel",
    
    # Gaming & Tech
    "Paradox Interactive AB": "paradox",
    "King Digital Entertainment": "king",
    "Mojang Studios": "mojang",
    "Dice": "dice",
    "NetEnt AB": "netent",
    
    # Healthcare & Biotech
    "Astra Zeneca AB": "astrazeneca",
    "Sobi": "sobi",
    "Orion Corporation": "orion",
    "BioArctic AB": "bioarctic",
    "BioInvent International AB": "bioinvent",
    
    # Automotive
    "Volvo Car AB": "volvo-cars",
    "Scania AB": "scania",
    "Polestar": "polestar",
    
    # Telecom
    "Tele2 AB": "tele2",
    "Com Hem AB": "comhem",
    
    # Real Estate
    "Fabege AB": "fabege",
    "Wallenstam AB": "wallenstam",
    "Atrium Ljungberg AB": "atrium-ljungberg",
    
    # Industrial
    "Sandvik AB": "sandvik",
    "LKAB": "lkab",
    "Höganäs AB": "hoganas",
    "Outokumpu": "outokumpu",
    
    # Retail
    "ICA Gruppen AB": "ica",
    "Axfood AB": "axfood",
    "Coop Sverige": "coop",
    
    # Fashion & Design
    "Lindex": "lindex",
    "Gina Tricot": "gina-tricot",
    "Acne Studios": "acne",
    
    # Startups & Growth Companies
    "Klarna": "klarna",
    "Spotify": "spotify", 
    "Truecaller": "truecaller",
    "Mojang": "mojang",
    "King": "king",
    
    # Add more as needed - this covers major Swedish companies across sectors
}

def generate_slug_from_name(company_name: str, ticker: str = None) -> str:
    """
    Enhanced slug generation with better Swedish company patterns
    """
    if not company_name:
        return None
    
    # Check exact mappings first
    if company_name in EXTENDED_COMPANY_MAPPINGS:
        return EXTENDED_COMPANY_MAPPINGS[company_name]
    
    # Generate slug from company name
    slug = company_name.lower()
    
    # Remove common Swedish company suffixes
    suffixes_to_remove = [
        r'\s+ab$', r'\s+aktiebolag$', r'\s+aktiebolaget$',
        r'\s+group$', r'\s+holding$', r'\s+holdings$', 
        r'\s+ltd$', r'\s+plc$', r'\s+corp$', r'\s+corporation$',
        r'\s+inc$', r'\s+incorporated$', r'\s+company$',
        r'\s+publ$', r'\s+public$'
    ]
    
    import re
    for suffix in suffixes_to_remove:
        slug = re.sub(suffix, '', slug)
    
    # Remove common Swedish prefixes
    prefixes_to_remove = [
        r'^aktiebolaget\s+', r'^telefonaktiebolaget\s+',
        r'^fastighets\s+ab\s+', r'^svenska\s+'
    ]
    
    for prefix in prefixes_to_remove:
        slug = re.sub(prefix, '', slug)
    
    # Clean up special characters
    char_mappings = {
        'å': 'a', 'ä': 'a', 'ö': 'o',
        'é': 'e', 'è': 'e', 'ë': 'e',
        'ü': 'u', 'ú': 'u', 'ù': 'u',
        '&': 'and', '+': 'plus'
    }
    
    for old_char, new_char in char_mappings.items():
        slug = slug.replace(old_char, new_char)
    
    # Handle spaces and special characters
    slug = re.sub(r'[^\w\s-]', '', slug)  # Remove special chars except spaces and dashes
    slug = re.sub(r'\s+', '-', slug)      # Replace spaces with dashes
    slug = re.sub(r'-+', '-', slug)       # Remove multiple dashes
    slug = slug.strip('-')                # Remove leading/trailing dashes
    
    # If ticker provided, also try ticker-based approaches
    if ticker and not slug:
        ticker_clean = ticker.replace(' A', '').replace(' B', '').replace(' SDB', '').replace(' PREF', '')
        slug = ticker_clean.lower()
    
    return slug if slug and len(slug) > 1 else None

def save_mappings():
    """Save comprehensive mappings to file for use in batch processor"""
    import json
    from datetime import datetime
    
    # Generate additional mappings for common patterns
    additional_generated = {}
    
    # Some common Swedish company patterns we can predict
    predictable_companies = {
        "Scania AB": "scania",
        "Tele2 AB": "tele2", 
        "ICA Gruppen AB": "ica",
        "SEB AB": "seb",
        "Handelsbanken AB": "handelsbanken",
        # Add more predictable patterns
    }
    
    # Combine all mappings
    all_mappings = {
        **EXTENDED_COMPANY_MAPPINGS,
        **predictable_companies
    }
    
    mapping_data = {
        "timestamp": datetime.now().isoformat(),
        "description": "Comprehensive Swedish company to MFN slug mappings",
        "mapping_count": len(all_mappings),
        "mappings": all_mappings,
        "generation_rules": {
            "suffixes_removed": ["AB", "Group", "Ltd", "Corporation", "Holdings"],
            "character_mappings": {"å": "a", "ä": "a", "ö": "o"},
            "space_handling": "converted to dashes",
            "fallback": "ticker-based when available"
        }
    }
    
    # Save comprehensive mapping
    with open("comprehensive_company_mappings.json", "w", encoding="utf-8") as f:
        json.dump(mapping_data, f, indent=2, ensure_ascii=False)
    
    # Save clean mappings for direct import
    with open("company_mappings_clean.json", "w", encoding="utf-8") as f:
        json.dump(all_mappings, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved {len(all_mappings)} company mappings")
    print(f"📁 Files created:")
    print(f"   📊 comprehensive_company_mappings.json (detailed)")
    print(f"   🔧 company_mappings_clean.json (for import)")
    
    # Show samples
    print(f"\n📋 Sample mappings:")
    for i, (company, slug) in enumerate(list(all_mappings.items())[:10]):
        print(f"   {i+1}. {company} → {slug}")
    
    if len(all_mappings) > 10:
        print(f"   ... and {len(all_mappings) - 10} more")

if __name__ == "__main__":
    print("🚀 Expanding Swedish company slug mappings...")
    save_mappings()
    print("\n💡 Next step: Update historical_ingestion_batch.py to use these mappings")