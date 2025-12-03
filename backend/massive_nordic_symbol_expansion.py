#!/usr/bin/env python3
"""
Massive expansion of Nordic symbols - hundreds of companies
Based on major Nordic exchanges and document database
"""

import asyncio
import asyncpg

async def insert_massive_nordic_symbols():
    """Insert hundreds of Nordic companies for comprehensive coverage"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Comprehensive list of Nordic companies
    # Format: (symbol, company_name, yahoo_symbol, market, country, sector, industry)
    massive_symbols = [
        # LARGE CAP SWEDISH COMPANIES (OMX Stockholm 30)
        ('VOLV-B', 'Volvo Group', 'VOLV-B.ST', 'Stockholm', 'SE', 'Industrial', 'Automotive'),
        ('ATCO-A', 'Atlas Copco A', 'ATCO-A.ST', 'Stockholm', 'SE', 'Industrial', 'Machinery'),
        ('ERIC-B', 'Ericsson B', 'ERIC-B.ST', 'Stockholm', 'SE', 'Technology', 'Telecom Equipment'),
        ('ABB', 'ABB Ltd', 'ABB.ST', 'Stockholm', 'SE', 'Industrial', 'Electrical Equipment'),
        ('AZN', 'AstraZeneca', 'AZN.ST', 'Stockholm', 'SE', 'Healthcare', 'Pharmaceuticals'),
        ('AAK', 'AAK AB', 'AAK.ST', 'Stockholm', 'SE', 'Consumer', 'Food Ingredients'),
        ('SWED-A', 'Swedbank A', 'SWED-A.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('SEB-A', 'SEB A', 'SEB-A.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('SHB-A', 'Handelsbanken A', 'SHB-A.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('NDA-SE', 'Nordea Bank', 'NDA-SE.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('TEL2-B', 'Tele2 B', 'TEL2-B.ST', 'Stockholm', 'SE', 'Telecom', 'Mobile Operator'),
        ('TELIA', 'Telia Company', 'TELIA.ST', 'Stockholm', 'SE', 'Telecom', 'Fixed Line'),
        ('SAND', 'Sandvik', 'SAND.ST', 'Stockholm', 'SE', 'Industrial', 'Mining Equipment'),
        ('SKA-B', 'Skanska B', 'SKA-B.ST', 'Stockholm', 'SE', 'Industrial', 'Construction'),
        ('SKF-B', 'SKF B', 'SKF-B.ST', 'Stockholm', 'SE', 'Industrial', 'Bearings'),
        ('SSAB-A', 'SSAB A', 'SSAB-A.ST', 'Stockholm', 'SE', 'Materials', 'Steel'),
        ('SCA-B', 'SCA B', 'SCA-B.ST', 'Stockholm', 'SE', 'Materials', 'Forest Products'),
        ('HM-B', 'H&M B', 'HM-B.ST', 'Stockholm', 'SE', 'Consumer', 'Fashion Retail'),
        ('INVE-B', 'Investor B', 'INVE-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('KINV-B', 'Kinnevik B', 'KINV-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('LATO-B', 'Latour B', 'LATO-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('INDU-C', 'Industrivarden C', 'INDU-C.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('ELUX-B', 'Electrolux B', 'ELUX-B.ST', 'Stockholm', 'SE', 'Consumer', 'Home Appliances'),
        ('ESSITY-B', 'Essity B', 'ESSITY-B.ST', 'Stockholm', 'SE', 'Consumer', 'Hygiene Products'),
        ('HEXA-B', 'Hexagon B', 'HEXA-B.ST', 'Stockholm', 'SE', 'Technology', 'Measurement Tech'),
        ('GETI-B', 'Getinge B', 'GETI-B.ST', 'Stockholm', 'SE', 'Healthcare', 'Medical Equipment'),
        ('EPI-A', 'Epiroc A', 'EPI-A.ST', 'Stockholm', 'SE', 'Industrial', 'Mining Equipment'),
        ('SOBI', 'Swedish Orphan Biovitrum', 'SOBI.ST', 'Stockholm', 'SE', 'Healthcare', 'Biopharmaceuticals'),
        ('ASSA-B', 'Assa Abloy B', 'ASSA-B.ST', 'Stockholm', 'SE', 'Industrial', 'Security Systems'),
        ('ALFA', 'Alfa Laval', 'ALFA.ST', 'Stockholm', 'SE', 'Industrial', 'Flow Technology'),
        
        # MID CAP SWEDISH COMPANIES
        ('NIBE-B', 'NIBE Industrier B', 'NIBE-B.ST', 'Stockholm', 'SE', 'Industrial', 'Heat Pumps'),
        ('HUSQ-B', 'Husqvarna B', 'HUSQ-B.ST', 'Stockholm', 'SE', 'Consumer', 'Outdoor Products'),
        ('SECU-B', 'Securitas B', 'SECU-B.ST', 'Stockholm', 'SE', 'Industrial', 'Security Services'),
        ('LUND-B', 'Lundin Energy', 'LUND-B.ST', 'Stockholm', 'SE', 'Energy', 'Oil & Gas'),
        ('EVO', 'Evolution Gaming', 'EVO.ST', 'Stockholm', 'SE', 'Technology', 'Live Casino'),
        ('BETS-B', 'Betsson B', 'BETS-B.ST', 'Stockholm', 'SE', 'Technology', 'Online Gambling'),
        ('SINCH', 'Sinch AB', 'SINCH.ST', 'Stockholm', 'SE', 'Technology', 'Cloud Communications'),
        ('CINT', 'Cint Group', 'CINT.ST', 'Stockholm', 'SE', 'Technology', 'Market Research'),
        ('CAST', 'Castellum', 'CAST.ST', 'Stockholm', 'SE', 'Real Estate', 'Commercial Property'),
        ('FABG', 'Fabege', 'FABG.ST', 'Stockholm', 'SE', 'Real Estate', 'Office Property'),
        ('ATRLJ', 'Atrium Ljungberg', 'ATRLJ.ST', 'Stockholm', 'SE', 'Real Estate', 'Property Development'),
        ('WIHL', 'Wihlborgs', 'WIHL.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('PEAB-B', 'Peab B', 'PEAB-B.ST', 'Stockholm', 'SE', 'Industrial', 'Construction'),
        ('JM', 'JM AB', 'JM.ST', 'Stockholm', 'SE', 'Consumer', 'Residential Construction'),
        ('BEIJER-B', 'Beijer Ref B', 'BEIJER-B.ST', 'Stockholm', 'SE', 'Industrial', 'Refrigeration'),
        ('LIFCO-B', 'Lifco B', 'LIFCO-B.ST', 'Stockholm', 'SE', 'Industrial', 'Industrial Solutions'),
        ('INDT', 'Indutrade', 'INDT.ST', 'Stockholm', 'SE', 'Industrial', 'Industrial Trading'),
        ('LAGR-B', 'Lagercrantz B', 'LAGR-B.ST', 'Stockholm', 'SE', 'Technology', 'Technical Trading'),
        ('ADDTECH-B', 'Addtech B', 'ADDTECH-B.ST', 'Stockholm', 'SE', 'Technology', 'Technical Solutions'),
        ('AXFO', 'Axfood', 'AXFO.ST', 'Stockholm', 'SE', 'Consumer', 'Food Retail'),
        ('ICA', 'ICA Gruppen', 'ICA.ST', 'Stockholm', 'SE', 'Consumer', 'Food Retail'),
        ('CLAS-B', 'Clas Ohlson B', 'CLAS-B.ST', 'Stockholm', 'SE', 'Consumer', 'Hardware Retail'),
        ('ELANDERS-B', 'Elanders B', 'ELANDERS-B.ST', 'Stockholm', 'SE', 'Industrial', 'Printing Services'),
        ('KINV', 'Kinnevik', 'KINV-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        
        # SMALL CAP SWEDISH COMPANIES  
        ('AVANZA', 'Avanza Bank', 'AZA.ST', 'Stockholm', 'SE', 'Financial', 'Online Banking'),
        ('NOLATO-B', 'Nolato B', 'NOLATO-B.ST', 'Stockholm', 'SE', 'Industrial', 'Plastics'),
        ('HUFV-A', 'Hufvudstaden A', 'HUFV-A.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('INWI', 'Inwido', 'INWI.ST', 'Stockholm', 'SE', 'Industrial', 'Windows & Doors'),
        ('BUFAB', 'Bufab', 'BUFAB.ST', 'Stockholm', 'SE', 'Industrial', 'Industrial Components'),
        ('BULTEN', 'Bulten', 'BULTEN.ST', 'Stockholm', 'SE', 'Industrial', 'Fasteners'),
        ('DIOS', 'Diös Fastigheter', 'DIOS.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('XANO-B', 'Xano Industri B', 'XANO-B.ST', 'Stockholm', 'SE', 'Industrial', 'Industrial Solutions'),
        ('TROAX', 'Troax Group', 'TROAX.ST', 'Stockholm', 'SE', 'Industrial', 'Safety Solutions'),
        ('LUMI', 'Lumito', 'LUMI.ST', 'Stockholm', 'SE', 'Healthcare', 'Diagnostics'),
        
        # NORWEGIAN COMPANIES
        ('EQNR', 'Equinor ASA', 'EQNR.OL', 'Oslo', 'NO', 'Energy', 'Oil & Gas'),
        ('DNB', 'DNB Bank', 'DNB.OL', 'Oslo', 'NO', 'Financial', 'Banking'),
        ('MOWI', 'Mowi ASA', 'MOWI.OL', 'Oslo', 'NO', 'Consumer', 'Seafood'),
        ('TEL', 'Telenor', 'TEL.OL', 'Oslo', 'NO', 'Telecom', 'Mobile Operator'),
        ('YAR', 'Yara International', 'YAR.OL', 'Oslo', 'NO', 'Materials', 'Fertilizers'),
        ('NHY', 'Norsk Hydro', 'NHY.OL', 'Oslo', 'NO', 'Materials', 'Aluminum'),
        ('SALM', 'SalMar ASA', 'SALM.OL', 'Oslo', 'NO', 'Consumer', 'Aquaculture'),
        ('LSG', 'Leroy Seafood', 'LSG.OL', 'Oslo', 'NO', 'Consumer', 'Seafood'),
        ('NAS', 'Norwegian Air', 'NAS.OL', 'Oslo', 'NO', 'Consumer', 'Airlines'),
        ('ORKLA', 'Orkla ASA', 'ORK.OL', 'Oslo', 'NO', 'Consumer', 'Consumer Goods'),
        
        # DANISH COMPANIES
        ('NOVO-B', 'Novo Nordisk B', 'NOVO-B.CO', 'Copenhagen', 'DK', 'Healthcare', 'Pharmaceuticals'),
        ('ORSTED', 'Orsted A/S', 'ORSTED.CO', 'Copenhagen', 'DK', 'Utilities', 'Renewable Energy'),
        ('MAERSK-B', 'A.P. Moller-Maersk B', 'MAERSK-B.CO', 'Copenhagen', 'DK', 'Industrial', 'Shipping'),
        ('CARL-B', 'Carlsberg B', 'CARL-B.CO', 'Copenhagen', 'DK', 'Consumer', 'Beverages'),
        ('COLO-B', 'Coloplast B', 'COLO-B.CO', 'Copenhagen', 'DK', 'Healthcare', 'Medical Devices'),
        ('NOVN-B', 'Novozymes B', 'NOVN-B.CO', 'Copenhagen', 'DK', 'Materials', 'Industrial Biotechnology'),
        ('DSV', 'DSV Panalpina', 'DSV.CO', 'Copenhagen', 'DK', 'Industrial', 'Logistics'),
        ('TRYG', 'Tryg A/S', 'TRYG.CO', 'Copenhagen', 'DK', 'Financial', 'Insurance'),
        ('DEMANT', 'Demant A/S', 'DEMANT.CO', 'Copenhagen', 'DK', 'Healthcare', 'Hearing Aids'),
        ('CHR', 'Chr. Hansen', 'CHR.CO', 'Copenhagen', 'DK', 'Materials', 'Bioscience'),
        
        # FINNISH COMPANIES  
        ('NOKIA', 'Nokia Corporation', 'NOKIA.HE', 'Helsinki', 'FI', 'Technology', 'Telecom Equipment'),
        ('FORTUM', 'Fortum Corporation', 'FORTUM.HE', 'Helsinki', 'FI', 'Utilities', 'Energy'),
        ('KONE', 'KONE Corporation', 'KONE.HE', 'Helsinki', 'FI', 'Industrial', 'Elevators'),
        ('NESTE', 'Neste Corporation', 'NESTE.HE', 'Helsinki', 'FI', 'Energy', 'Renewable Fuels'),
        ('UPM', 'UPM-Kymmene', 'UPM.HE', 'Helsinki', 'FI', 'Materials', 'Forest Products'),
        ('STORA-R', 'Stora Enso R', 'STERV.HE', 'Helsinki', 'FI', 'Materials', 'Forest Products'),
        ('SUPERCELL', 'Supercell', 'SUPERCELL.HE', 'Helsinki', 'FI', 'Technology', 'Mobile Games'),
        ('KESKO-B', 'Kesko B', 'KESKOB.HE', 'Helsinki', 'FI', 'Consumer', 'Retail'),
        ('ELISA', 'Elisa Corporation', 'ELISA.HE', 'Helsinki', 'FI', 'Telecom', 'Mobile Operator'),
        ('METSO', 'Metso Outotec', 'METSO.HE', 'Helsinki', 'FI', 'Industrial', 'Mining Equipment'),
        
        # TECHNOLOGY & GROWTH COMPANIES
        ('SPOTIFY', 'Spotify Technology SA', 'SPOT', 'Stockholm', 'SE', 'Technology', 'Music Streaming'),
        ('KLARNA', 'Klarna Bank AB', 'KLARNA.ST', 'Stockholm', 'SE', 'Financial', 'Payment Services'),
        ('KING', 'King Digital Entertainment', 'KING.ST', 'Stockholm', 'SE', 'Technology', 'Mobile Games'),
        ('PARADOX', 'Paradox Interactive', 'PDX.ST', 'Stockholm', 'SE', 'Technology', 'Video Games'),
        ('STORYTEL', 'Storytel AB', 'STORY-B.ST', 'Stockholm', 'SE', 'Technology', 'Audiobooks'),
        ('EMBRACER', 'Embracer Group', 'EMBRAC-B.ST', 'Stockholm', 'SE', 'Technology', 'Video Games'),
        
        # REAL ESTATE & CONSTRUCTION
        ('SAGAX-B', 'Sagax B', 'SAGAX-B.ST', 'Stockholm', 'SE', 'Real Estate', 'Logistics Property'),
        ('FASTPART', 'Fastpartner', 'FPAR-A.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('KLOVERN', 'Klövern', 'KLOV.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('COREM', 'Corem Property Group', 'COREM.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('HEMFOSA', 'Hemfosa Fastigheter', 'HEMF.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
        ('NYFOSA', 'Nyfosa', 'NYFOSA.ST', 'Stockholm', 'SE', 'Real Estate', 'Property'),
    ]
    
    print(f"🚀 Inserting {len(massive_symbols)} Nordic companies...")
    
    inserted = 0
    failed = 0
    
    for symbol, name, yahoo, market, country, sector, industry in massive_symbols:
        try:
            await conn.execute("""
                INSERT INTO market_data_symbols 
                (symbol, company_name, yahoo_symbol, market, country, sector, industry, document_company_name)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    yahoo_symbol = EXCLUDED.yahoo_symbol,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    updated_at = NOW()
            """, symbol, name, yahoo, market, country, sector, industry, name.replace(' ', '_'))
            
            inserted += 1
            print(f"✅ {symbol:8} - {name:30} -> {yahoo}")
            
        except Exception as e:
            failed += 1
            print(f"❌ Failed {symbol}: {e}")
    
    await conn.close()
    
    print(f"\n🎉 Massive Symbol Expansion Complete!")
    print(f"   ✅ Inserted/Updated: {inserted} symbols")
    print(f"   ❌ Failed: {failed} symbols")
    print(f"   🌍 Coverage: Sweden, Norway, Denmark, Finland")
    print(f"   💼 Sectors: All major Nordic industries")
    
    return inserted

if __name__ == "__main__":
    print("🌟 MASSIVE NORDIC SYMBOL EXPANSION")
    print("Adding 100+ major Nordic companies across all markets")
    print("=" * 60)
    
    asyncio.run(insert_massive_nordic_symbols())