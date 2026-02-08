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
    
    # ⭐ Manually resolved failed slugs (batch 1)
    "absolicon-solar-collector": "Absolicon",
    "acrinova": "Acrinova A",
    "addnode-group": "Addnode", 
    "addvise-group": "ADDvise Group B",
    "aixia-group": "Aixia",
    
    # ⭐ Manually resolved failed slugs (batch 2)
    "alimak-group": "Alimak",
    "annexin-pharmaceuticals": "Annexin",
    "anoto-group": "Anoto",
    "antco-investment-group": "ANTCO Investment",
    "avsalt": "Avsalt Group",
    
    # ⭐ Manually resolved failed slugs (batch 3)
    "axolot-solutions-holding": "Axolot Solutions",
    "b3-consulting-group": "B3 Consulting",
    "bergman-and-beving": "Bergman & Beving",
    "blick-global-group": "Blick Global",
    "bonasudden-holding": "Bonäsudden",
    
    # ⭐ Manually resolved failed slugs (batch 4)
    "brandbee-holding": "BrandBee",
    "bravida-holding": "Bravida",
    "byggmax-group": "Byggmax",
    "case": "Case Group",
    "chordate-medical-holding": "Chordate Medical",
    
    # ⭐ Manually resolved failed slugs (batch 5) - exact matches that should work automatically
    "clavister": "Clavister",
    "crunchfish": "Crunchfish", 
    "duroc": "Duroc",
    "edyoutec": "Edyoutec",
    # "consilium" - skipped, no match found
    
    # ⭐ Manually resolved failed slugs (batch 6)
    "eltel-group": "Eltel",
    "enea": "Enea",
    "eniro-group": "Eniro", 
    "fagerhult-group": "Fagerhult",
    # "feiyu-technology" - skipped, doesn't exist on MFN or elsewhere
    
    # ⭐ Manually resolved failed slugs (batch 7)
    "flerie-invest": "Flerie",
    "fragbite-group": "Fragbite",
    "frontwalker": "Frontwalker",
    "garo-group": "Garo",
    "gasporox": "Gasporox",
    
    # ⭐ Manually resolved failed slugs (batch 8)
    "genovis": "Genovis",
    "getinge-group": "Getinge",
    "gigasun": "Gigasun",
    "grangex": "Grangex",
    "greenmerc": "Greenmerc",
    
    # ⭐ Manually resolved failed slugs (batch 9)
    # "gyenge-capital" - skipped, no match found
    "hexicon": "Hexicon",
    "hexpol": "Hexpol",
    "hoodin": "Hoodin",
    "hubbster": "Hubbster",
    
    # ⭐ Manually resolved failed slugs (batch 10)
    "iconovo": "Iconovo",
    "implantica": "Implantica",
    "infrea": "Infrea",
    "instalco-group": "Instalco",
    "integrum": "Integrum",
    
    # ⭐ Manually resolved failed slugs (batch 11) - all exact matches
    "intervacc": "Intervacc",
    "invisio": "Invisio", 
    "irisity": "Irisity",
    "kancera": "Kancera",
    "kiliaro": "Kiliaro",
    
    # ⭐ Manually resolved failed slugs (batch 12) - all exact matches
    "klimator": "Klimator",
    "kvix": "Kvix",
    "lifco": "Lifco", 
    "lipum": "Lipum",
    "litium": "Litium",
    
    # ⭐ Manually resolved failed slugs (batch 13) - all 15 exact matches
    "lovisagruvan": "Lovisagruvan", "lumito": "Lumito", "luxbright": "Luxbright",
    "magnasense": "Magnasense", "mangold": "Mangold", "mantex": "Mantex",
    "mavshack": "Mavshack", "medclair": "Medclair", "medivir": "Medivir",
    "mendus": "Mendus", "mentice": "Mentice", "metacon": "Metacon",
    "midsummer": "Midsummer", "minesto": "Minesto", "mips": "Mips",
    
    # ⭐ Manually resolved failed slugs (batch 14) - all 20 exact matches
    "mofast": "Mofast", "monivent": "Monivent", "mycronic": "Mycronic", 
    "nanexa": "Nanexa", "nanologica": "Nanologica", "nepa": "Nepa",
    "nobia": "Nobia", "nolato": "Nolato", "norditek": "Norditek",
    "nordnet": "Nordnet", "nordrest": "Nordrest", "nosium": "Nosium",
    "novotek": "Novotek", "nowonomics": "Nowonomics", "nyfosa": "Nyfosa",
    "obducat": "Obducat", "observit": "Observit", "odinwell": "Odinwell",
    "ogunsen": "Ogunsen", "oncopeptides": "Oncopeptides",
    
    # ⭐ Manually resolved failed slugs (batch 15)
    "clemondo-group": "Clemondo",
    "condo-nordic-holding": "Condo Nordic",
    "coor-service-management-holding": "Coor Service Management",
    "dedicare-group": "Dedicare",
    "desenio-group": "Desenio",
    
    # ⭐ Manually resolved failed slugs (batch 16)
    "dometic-group": "Dometic",
    "ecoclime-group": "Ecoclime",
    "electrolux": "Electrolux AB",
    "embeddedart-group": "EmbeddedArt",
    "embellence-group": "Embellence",
    
    # ⭐ Manually resolved failed slugs (batch 17)
    "embracer-group": "Embracer",
    "enity-holding": "Enity",
    "epiroc": "Epiroc AB",
    "es-energy-save-holding": "ES Energy Save",
    "everysport-group": "Everysport",
    
    # ⭐ Manually resolved failed slugs (batch 18)
    "evolution": "Evolution AB",
    "ework-group": "eWork",
    "expres2ion-biotech-holding": "ExpreS2ion Biotech",
    "fable-media-group": "Fable Media",
    "fractal-gaming-group": "Fractal Gaming",
    
    # ⭐ Manually resolved failed slugs (batch 19)
    "free2move-holding": "Free2Move",
    "freja-eid-group": "Freja eID",
    "genova-property-group": "Genova Property",
    "gomspace-group": "GomSpace",
    "goobit": "Goobit Group",
    
    # ⭐ Manually resolved failed slugs (batch 20)
    "gosol-energy-group": "Gosol Energy",
    "gullberg-and-jansson": "Gullberg & Jansson",
    "handelsbanken": "Handelsbanken A",
    "haypp": "Haypp Group",
    "hedera-group": "Hedera",
    
    # ⭐ Manually resolved failed slugs (batch 21)
    "hemnet-group": "Hemnet",
    "hexatronic-group": "Hexatronic",
    "hifab-group": "Hifab",
    "hilbert": "Hilbert Group",
    "i-tech": "I-Tech",
    
    # ⭐ Manually resolved failed slugs (batch 22)
    "ica-gruppen": "ICA Gruppen AB",
    "infracom-group": "Infracom",
    "k-fast-holding": "K-Fast Holding",
    "kabe-group": "Kabe",
    "kambi-group": "Kambi",
    
    # ⭐ Manually resolved failed slugs (batch 23)
    "karnell": "Karnell Group",
    "karnov-group": "Karnov",
    "kentima-holding": "Kentima",
    "kinnevik": "Kinnevik AB",
    "kjell": "Kjell Group",
    
    # ⭐ Manually resolved failed slugs (batch 24 - FINAL!)
    "klaria-pharma-holding": "Klaria Pharma",
    "lagercrantz-group": "Lagercrantz",
    "lammhults-design-group": "Lammhults Design",
    "lyko-group": "Lyko",
    "mediacle-group": "Mediacle",
    "miris-holding": "Miris",
    "momentum": "Momentum Group",
    "mresell-holding": "mResell",
    "munters-group": "Munters",
    
    # ⭐ Final remaining slugs from comprehensive fix
    "new-wave-group": "New Wave",
    "stendorren-fastigheter": "Stendörren Fastigheter",
    "sjostrand-coffee": "Sjöstrand Coffee",
    "q-linea": "Q-linea",
    "soder-sportfiske": "Söder Sportfiske",
    "vo2-cap-holding": "VO2 Cap",
    "sht-smart-high-tech": "SHT Smart High-Tech",
    "ramlosa-shipping": "Ramlösa Shipping",
    "orron-energy": "Orrön Energy",
    "vastra-hamnen": "Västra Hamnen",
    
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
    
    # Companies with A/B stock classes where MFN uses base name
    "transferator": "Transferator A",

    # Major companies with very different MFN slugs
    "boliden": "Boliden AB",
    "sca": "Svenska Cellulosa Aktiebolaget SCA",
    "telia": "Telia Company",
    "intrum": "Intrum",
    "lindab": "Lindab",
    "loomis": "Loomis",
    "vitec": "Vitec Software",
    "sagax": "Sagax A",
    "sensys-gatso": "Sensys Gatso",
    "platzer": "Platzer Fastigheter",
    "dios": "Diös Fastigheter",
    "corem": "Corem Property A",
    "samhallsbyggnadsbolaget": "Samhällsbyggnadsbolag B",
    "sbb": "Samhällsbyggnadsbolag B",
    "fastpartner": "Fastpartner A",
    "wastbygg": "Wästbygg",
    "xspray": "Xspray Pharma",
    "checkin-com": "checkin.com",
    "iar-systems": "I.A.R Systems",
    "industrivarden": "Industrivärden C",
    "nilorngruppen": "Nilörngruppen",
    "kopparbergs": "Kopparbergs",
    "bactiguard": "Bactiguard",
    "arise": "Arise Windpower",

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
    
    # Additional ticker suffix mappings
    "absolent-air-care (abso)": "Absolent Air Care",
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

    # FIRST: Strip stock class suffixes from original name BEFORE normalization
    # These are share classes, not part of the company name on MFN
    base_name = company_name

    # Stock class suffixes (must check longer ones first)
    stock_class_suffixes = [
        " Pref B", " Pref A", " Pref",  # Preference shares
        " SDB",  # Swedish Depositary Receipts (foreign companies)
        " A", " B", " C", " D",  # Stock classes
    ]
    for suffix in stock_class_suffixes:
        if base_name.endswith(suffix):
            base_name = base_name[:-len(suffix)].strip()
            break  # Only strip one suffix

    # Check if the base name (without stock class) has a direct mapping
    if base_name in COMPANY_NAME_TO_SLUG:
        return COMPANY_NAME_TO_SLUG[base_name]

    # Normalize: lowercase, spaces to hyphens, & to and
    normalized = base_name.lower().replace(' ', '-')
    normalized = normalized.replace('&', 'and')

    # Remove common corporate suffixes (now properly hyphenated)
    corporate_suffixes = ["-ab", "-ltd", "-asa", "-publ", "-group", "-holding"]
    for suffix in corporate_suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
            break

    return normalized