#!/usr/bin/env python3
"""
Ingest ALL available historical data for every Nordic stock.
Fetches the maximum history available from Yahoo Finance.
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.market_data.services.historical_data_ingestor import HistoricalDataIngestor

async def get_max_history_days(yahoo_symbol: str) -> Optional[int]:
    """Determine how many days of history are available for a symbol"""
    try:
        ticker = yf.Ticker(yahoo_symbol)
        hist = ticker.history(period="max")
        
        if not hist.empty:
            first_date = hist.index[0].date()
            last_date = hist.index[-1].date()
            days_available = (last_date - first_date).days
            return days_available
        else:
            return None
            
    except Exception as e:
        print(f"❌ Error checking history for {yahoo_symbol}: {e}")
        return None

async def ingest_maximum_historical_data():
    """Ingest all available historical data for all symbols"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    
    try:
        # Get all symbols from database
        conn = await asyncpg.connect(DATABASE_URL)
        
        symbols = await conn.fetch("""
            SELECT symbol, company_name, yahoo_symbol 
            FROM market_data_symbols 
            ORDER BY symbol
        """)
        
        print(f"🚀 Starting MAXIMUM historical data ingestion for {len(symbols)} symbols")
        print("=" * 70)
        
        # Create ingestor
        ingestor = HistoricalDataIngestor()
        await ingestor.connect()
        
        results = {
            'success': [],
            'failed': [],
            'stats': {}
        }
        
        for i, symbol_row in enumerate(symbols, 1):
            symbol = symbol_row['symbol']
            company_name = symbol_row['company_name']
            yahoo_symbol = symbol_row['yahoo_symbol']
            
            print(f"\n[{i}/{len(symbols)}] 📊 {company_name} ({symbol})")
            
            # First, check how much history is available
            max_days = await get_max_history_days(yahoo_symbol)
            
            if max_days:
                years = max_days / 365.25
                print(f"   📅 Found {years:.1f} years of history ({max_days} days)")
                
                # Add some buffer days to ensure we get everything
                days_to_fetch = max_days + 30
                
                # Ingest with maximum history
                try:
                    success = await ingestor.ingest_historical_data(
                        symbol, 
                        days_back=days_to_fetch,
                        calculate_metrics=True
                    )
                    
                    if success:
                        results['success'].append(symbol)
                        results['stats'][symbol] = {
                            'company': company_name,
                            'days': max_days,
                            'years': years
                        }
                        
                        # Get actual count from database
                        count = await conn.fetchval("""
                            SELECT COUNT(*) FROM daily_price_data WHERE symbol = $1
                        """, symbol)
                        
                        print(f"   ✅ Success! Stored {count} price points")
                    else:
                        results['failed'].append(symbol)
                        print(f"   ❌ Failed to ingest data")
                        
                except Exception as e:
                    results['failed'].append(symbol)
                    print(f"   ❌ Error during ingestion: {e}")
                    
            else:
                results['failed'].append(symbol)
                print(f"   ⚠️  Could not determine available history")
            
            # Small delay to be respectful to Yahoo
            await asyncio.sleep(2)
        
        await ingestor.disconnect()
        await conn.close()
        
        # Print summary
        print("\n" + "=" * 70)
        print("📊 INGESTION SUMMARY")
        print("=" * 70)
        print(f"✅ Successful: {len(results['success'])} symbols")
        print(f"❌ Failed: {len(results['failed'])} symbols")
        
        if results['success']:
            print("\n📈 Top 10 symbols by history length:")
            sorted_stats = sorted(
                results['stats'].items(), 
                key=lambda x: x[1]['days'], 
                reverse=True
            )[:10]
            
            for symbol, stats in sorted_stats:
                print(f"   {symbol:8} - {stats['company']:25} - {stats['years']:4.1f} years")
        
        if results['failed']:
            print(f"\n❌ Failed symbols: {', '.join(results['failed'])}")
        
        # Save detailed results
        import json
        with open('historical_ingestion_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        print("\n📄 Detailed results saved to: historical_ingestion_results.json")
        
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return False
    
    return True

async def add_more_nordic_symbols():
    """Add more Nordic company symbols to the database"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Extended list of Nordic companies
    additional_companies = [
        # Major Swedish companies
        ('SWED-A', 'Swedbank A', 'SWED-A.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('SEB-A', 'SEB A', 'SEB-A.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('SHB-A', 'Handelsbanken A', 'SHB-A.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        ('TEL2-B', 'Tele2 B', 'TEL2-B.ST', 'Stockholm', 'SE', 'Telecom', 'Telecommunications'),
        ('TELIA', 'Telia Company', 'TELIA.ST', 'Stockholm', 'SE', 'Telecom', 'Telecommunications'),
        ('SAND', 'Sandvik', 'SAND.ST', 'Stockholm', 'SE', 'Industrial', 'Machinery'),
        ('SKA-B', 'Skanska B', 'SKA-B.ST', 'Stockholm', 'SE', 'Industrial', 'Construction'),
        ('SKF-B', 'SKF B', 'SKF-B.ST', 'Stockholm', 'SE', 'Industrial', 'Bearings'),
        ('SSAB-A', 'SSAB A', 'SSAB-A.ST', 'Stockholm', 'SE', 'Materials', 'Steel'),
        ('SCA-B', 'SCA B', 'SCA-B.ST', 'Stockholm', 'SE', 'Materials', 'Forest Products'),
        ('INVE-B', 'Investor B', 'INVE-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('KINV-B', 'Kinnevik B', 'KINV-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('LATO-B', 'Latour B', 'LATO-B.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('INDU-C', 'Industrivarden C', 'INDU-C.ST', 'Stockholm', 'SE', 'Financial', 'Investment Company'),
        ('ELUX-B', 'Electrolux B', 'ELUX-B.ST', 'Stockholm', 'SE', 'Consumer', 'Appliances'),
        ('ESSITY-B', 'Essity B', 'ESSITY-B.ST', 'Stockholm', 'SE', 'Consumer', 'Hygiene Products'),
        ('HEXA-B', 'Hexagon B', 'HEXA-B.ST', 'Stockholm', 'SE', 'Technology', 'Measurement Tech'),
        ('GETI-B', 'Getinge B', 'GETI-B.ST', 'Stockholm', 'SE', 'Healthcare', 'Medical Equipment'),
        ('EPI-A', 'Epiroc A', 'EPI-A.ST', 'Stockholm', 'SE', 'Industrial', 'Mining Equipment'),
        ('SOBI', 'Swedish Orphan Biovitrum', 'SOBI.ST', 'Stockholm', 'SE', 'Healthcare', 'Biopharmaceuticals'),
        
        # More industrial & tech
        ('ASSA-B', 'Assa Abloy B', 'ASSA-B.ST', 'Stockholm', 'SE', 'Industrial', 'Locks & Security'),
        ('ALFA', 'Alfa Laval', 'ALFA.ST', 'Stockholm', 'SE', 'Industrial', 'Flow Technology'),
        ('NIBE-B', 'NIBE Industrier B', 'NIBE-B.ST', 'Stockholm', 'SE', 'Industrial', 'Heating Technology'),
        ('HUSQ-B', 'Husqvarna B', 'HUSQ-B.ST', 'Stockholm', 'SE', 'Consumer', 'Outdoor Products'),
        ('SECU-B', 'Securitas B', 'SECU-B.ST', 'Stockholm', 'SE', 'Industrial', 'Security Services'),
        ('LUND-B', 'Lundin Energy', 'LUND-B.ST', 'Stockholm', 'SE', 'Energy', 'Oil & Gas'),
        ('NDA-SE', 'Nordea Bank', 'NDA-SE.ST', 'Stockholm', 'SE', 'Financial', 'Banking'),
        
        # Mid-cap interesting companies
        ('BETS-B', 'Betsson B', 'BETS-B.ST', 'Stockholm', 'SE', 'Consumer', 'Online Gambling'),
        ('EVO', 'Evolution Gaming', 'EVO.ST', 'Stockholm', 'SE', 'Technology', 'Live Casino'),
        ('SINCH', 'Sinch AB', 'SINCH.ST', 'Stockholm', 'SE', 'Technology', 'Cloud Communications'),
        ('CINT', 'Cint Group', 'CINT.ST', 'Stockholm', 'SE', 'Technology', 'Market Research'),
    ]
    
    inserted = 0
    for symbol, name, yahoo, market, country, sector, industry in additional_companies:
        try:
            await conn.execute("""
                INSERT INTO market_data_symbols 
                (symbol, company_name, yahoo_symbol, market, country, sector, industry, document_company_name)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol) DO NOTHING
            """, symbol, name, yahoo, market, country, sector, industry, name.replace(' ', '_'))
            inserted += 1
        except Exception as e:
            print(f"⚠️  Could not insert {symbol}: {e}")
    
    await conn.close()
    print(f"✅ Added {inserted} new symbols to database")
    
    return inserted > 0

if __name__ == "__main__":
    print("🌟 YodaBuffett Maximum Historical Data Ingestion")
    print("=" * 50)
    
    if len(sys.argv) > 1 and sys.argv[1] == "add-symbols":
        # First add more symbols
        asyncio.run(add_more_nordic_symbols())
    
    # Then run the maximum ingestion
    asyncio.run(ingest_maximum_historical_data())