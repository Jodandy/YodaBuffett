"""
Centralized Company Name <-> MFN Slug Mappings
Ensures consistency across the entire ingestion pipeline
"""

# Single source of truth for company mappings
# Format: "mfn_slug": "Database Company Name"
COMPANY_SLUG_TO_NAME = {
    # Major companies with different naming
    "volvo": "Volvo Group",
    "astrazeneca": "AstraZeneca", 
    "atlas-copco": "Atlas Copco AB",
    "ericsson": "Telefonaktiebolaget LM Ericsson",
    "handm": "H&M Hennes & Mauritz AB",
    "h-m-hennes-mauritz": "H&M Hennes & Mauritz AB",  # Alternative slug
    "sandvik": "Sandvik AB",
    "nordea": "Nordea Bank Abp",
    "investor": "Investor AB",
    "abb": "ABB Ltd",
    "hexagon": "Hexagon AB",
    
    # ⭐ Companies with resolved slugs (slug resolution found these)
    "abelco-investment": "Abelco Investment",
    "abelco-investment-group": "Abelco Investment",
    
    # ⭐ Full MFN company names to database names  
    "abelco-investment-group-ab-publ": "Abelco Investment",
    
    # Companies with Swedish characters (MFN strips these)
    "wastbygg": "Wästbygg",
    "varnamo": "Värnamo",
    "klovern": "Klövern",
    "bjorn-borg": "Björn Borg",
    "hoganas": "Höganäs",
    "karo-pharma": "Karo Pharma",
    "loomis": "Loomis",
    "nibe-industrier": "NIBE Industrier",
    "ratos": "Ratos",
    "saab": "Saab",
    "skanska": "Skanska",
    "trelleborg": "Trelleborg",
    
    # Companies with spaces that need exact matching
    "arise-windpower": "Arise Windpower",
    "active-properties": "Active Properties",
    "africa-oil": "Africa Oil",
    "anoto-group": "Anoto Group",
    "arctic-gold": "Arctic Gold",
    "aspire-global": "Aspire Global",
    "be-group": "BE Group",
    "berg-propulsion": "Berg Propulsion",
    "betsson": "Betsson",
    "bio-gaia": "BioGaia",
    "black-earth-farming": "Black Earth Farming",
    "bong-ljungdahl": "Bong Ljungdahl",
    "boss-media": "Boss Media",
    "boule-diagnostics": "Boule Diagnostics",
    "brain-cool": "BrainCool",
    "bts-group": "BTS Group",
    "bulten": "Bulten",
    "bure-equity": "Bure Equity",
    "c-security-systems": "C Security Systems",
    "capacity-energy": "Capacity Energy",
    "carbon-recycling": "Carbon Recycling",
    "cash-guard": "Cash Guard",
    "cherry": "Cherry",
    "clean-motion": "Clean Motion",
    "clean-oil-technology": "Clean Oil Technology",
    
    # Number prefixes
    "2curex": "2cureX",
    "24storage": "24Storage",
    "5th-planet-games": "5th Planet Games",
    
    # Complex names
    "aac-clyde-space": "AAC Clyde Space", 
    "aak": "AAK",
    "abas-protect": "ABAS Protect",
    "abera-bioscience": "Abera Bioscience", 
    "absolent-air-care": "Absolent Air Care",
    "absolent-air-care-group": "Absolent Air Care",  # With suffix
    "active-biotech": "Active Biotech",
    "africa-energy": "Africa Energy",
    "africa-resources": "Africa Resources",
    "ages-industri": "Ages Industri",
    "aik-fotboll": "AIK Fotboll",
    "aino-health": "Aino Health",
    "alfa-laval": "Alfa Laval AB",
    "alligator-bioscience": "Alligator Bioscience",
    "alm-equity": "ALM Equity",
    "alzecure-pharma": "AlzeCure Pharma",
    "amhult-2": "Amhult 2",
    "addlife": "AddLife",
    "addnode": "Addnode Group", 
    "addtech": "Addtech",
    "better-collective": "Better Collective",
    "bio-works": "Bio-Works Technologies",
    "brain-cool": "BrainCool",
    "c-rad": "C-RAD",
    "cantargia": "Cantargia",
    "carl-lamm": "Carl Lamm",
    "castellum": "Castellum",
    "cellavision": "CellaVision",
    "chemometec": "ChemoMetec",
    "chr-hansen": "Chr. Hansen",
    "christian-berner": "Christian Berner Tech Trade",
    "cint-group": "Cint Group",
    "clas-ohlson": "Clas Ohlson",
    "clinical-laserthermia": "Clinical Laserthermia Systems",
    "cloud-peak-energy": "Cloud Peak Energy",
    "coegin-pharma": "Coegin Pharma",
    "combine-excellence": "Combine Excellence",
    "concordia-maritime": "Concordia Maritime",
    "copperstone-resources": "Copperstone Resources",
    "core-property": "Core Property",
    "cortus-energy": "Cortus Energy",
    "crunchfish": "Crunchfish",
    "ctm-media": "CTM Media",
    "cybaero": "CybAero",
    "cyber-security-1": "Cyber Security 1",
    "cybercom": "Cybercom",
    "cyxone": "Cyxone",
}

# Reverse mapping: Database Name -> MFN Slug
# Built automatically from the forward mapping
COMPANY_NAME_TO_SLUG = {v: k for k, v in COMPANY_SLUG_TO_NAME.items()}

# Function to ensure we always have both directions
def get_company_name(mfn_slug: str) -> str:
    """Get database company name from MFN slug"""
    slug_lower = mfn_slug.lower()
    
    # Direct lookup
    if slug_lower in COMPANY_SLUG_TO_NAME:
        return COMPANY_SLUG_TO_NAME[slug_lower]
    
    # Try without common suffixes
    for suffix in ["-group", "-holding", "-ab", "-publ"]:
        if slug_lower.endswith(suffix):
            base_slug = slug_lower[:-len(suffix)]
            if base_slug in COMPANY_SLUG_TO_NAME:
                return COMPANY_SLUG_TO_NAME[base_slug]
    
    # Default: convert slug to title case with spaces
    return mfn_slug.replace('-', ' ').title()

def get_mfn_slug(company_name: str) -> str:
    """Get MFN slug from database company name"""
    # Direct lookup
    if company_name in COMPANY_NAME_TO_SLUG:
        return COMPANY_NAME_TO_SLUG[company_name]
    
    # Try normalized version
    normalized = company_name.lower().replace(' ', '-')
    normalized = normalized.replace('&', 'and')
    
    # Remove common suffixes
    for suffix in [" ab", " ltd", " asa", " publ", " group", " holding"]:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
    
    return normalized