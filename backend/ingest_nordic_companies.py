#!/usr/bin/env python3
"""
Ingest Nordic Companies from Börsdata CSV
Updates company_master with comprehensive Nordic market data
"""

import asyncio
import asyncpg
import pandas as pd
import csv
from datetime import datetime
import uuid
import os

class NordicCompanyIngestor:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.conn = None
        
    async def connect(self):
        """Connect to database"""
        self.conn = await asyncpg.connect(self.db_url)
        
    async def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            await self.conn.close()
            
    def load_csv_data(self, csv_path: str):
        """Load and parse the Nordic companies CSV"""
        print(f"Loading data from: {csv_path}")
        
        # Read CSV with semicolon separator
        df = pd.read_csv(csv_path, sep=';', encoding='utf-8')
        
        # Clean column names (remove quotes and spaces)
        df.columns = [col.strip('"') for col in df.columns]
        
        print(f"Loaded {len(df)} companies from CSV")
        print(f"Columns: {df.columns.tolist()}")
        
        # Show sample data
        print(f"\nSample data:")
        print(df.head(3).to_string())
        
        return df
    
    async def get_existing_companies(self):
        """Get current companies in database"""
        query = """
        SELECT primary_ticker, company_name, isin_code, yahoo_symbol, country, 
               currency, primary_exchange, sector, id
        FROM company_master
        ORDER BY primary_ticker
        """
        rows = await self.conn.fetch(query)
        
        existing = {}
        for row in rows:
            # Index by ticker and ISIN for matching
            if row['primary_ticker']:
                existing[row['primary_ticker']] = dict(row)
            if row['isin_code']:
                existing[row['isin_code']] = dict(row)
                
        print(f"Found {len(rows)} existing companies in database")
        return existing
    
    async def process_companies(self, df, existing_companies):
        """Process each company for insert or update"""
        new_companies = []
        updated_companies = []
        skipped_companies = []
        
        for idx, row in df.iterrows():
            try:
                # Parse the CSV row
                company_data = {
                    'borsdata_id': row.get('Börsdata ID', '').strip('"'),
                    'company_name': row.get('Bolagsnamn', '').strip('"'),
                    'instrument_type': row.get('Info - Instrument', '').strip('"'),
                    'isin': row.get('Info - ISIN', '').strip('"'),
                    'ticker': row.get('Info - Ticker', '').strip('"'),
                    'yahoo_symbol': row.get('Info - Yahoo', '').strip('"'),
                    'country': row.get('Info - Land', '').strip('"'),
                    'currency': row.get('Info - Kursvaluta', '').strip('"'),
                    'report_currency': row.get('Info - Rapportvaluta', '').strip('"'),
                    'exchange': row.get('Info - Lista', '').strip('"'),
                    'sector': row.get('Info - Bransch', '').strip('"')
                }
                
                # Skip if missing essential data
                if not company_data['ticker'] or not company_data['company_name']:
                    skipped_companies.append(f"Missing ticker/name: {company_data}")
                    continue
                
                # Check if company exists (by ticker or ISIN)
                existing_by_ticker = existing_companies.get(company_data['ticker'])
                existing_by_isin = existing_companies.get(company_data['isin']) if company_data['isin'] else None
                
                if existing_by_ticker or existing_by_isin:
                    # Update existing company
                    existing = existing_by_ticker or existing_by_isin
                    update_data = self.prepare_update_data(existing, company_data)
                    if update_data:
                        updated_companies.append({
                            'company_id': existing['id'],
                            'ticker': existing['primary_ticker'],
                            'updates': update_data,
                            'new_data': company_data
                        })
                else:
                    # New company
                    new_companies.append(company_data)
                    
            except Exception as e:
                skipped_companies.append(f"Error processing row {idx}: {e}")
        
        print(f"\nProcessing results:")
        print(f"New companies to insert: {len(new_companies)}")
        print(f"Existing companies to update: {len(updated_companies)}")
        print(f"Skipped entries: {len(skipped_companies)}")
        
        if skipped_companies and len(skipped_companies) < 10:
            print(f"\nSkipped entries:")
            for skip in skipped_companies:
                print(f"  {skip}")
        
        return new_companies, updated_companies, skipped_companies
    
    def prepare_update_data(self, existing, new_data):
        """Prepare update data for existing company"""
        updates = {}
        
        # Map new data to database fields
        field_mapping = {
            'company_name': 'company_name',
            'isin': 'isin_code', 
            'yahoo_symbol': 'yahoo_symbol',
            'country': 'country',
            'currency': 'currency',
            'exchange': 'primary_exchange',
            'sector': 'sector'
        }
        
        for new_field, db_field in field_mapping.items():
            new_value = new_data.get(new_field, '').strip()
            existing_value = existing.get(db_field, '') or ''
            
            # Update if new value is more complete or different
            if new_value and (not existing_value or existing_value != new_value):
                updates[db_field] = new_value
        
        # Always update yahoo_symbol if we have it
        if new_data.get('yahoo_symbol'):
            updates['yahoo_symbol'] = new_data['yahoo_symbol']
            
        return updates if updates else None
    
    async def insert_new_companies(self, new_companies):
        """Insert new companies into database"""
        if not new_companies:
            print("No new companies to insert")
            return
            
        print(f"\nInserting {len(new_companies)} new companies...")
        
        insert_query = """
        INSERT INTO company_master (
            id, primary_ticker, company_name, isin_code, yahoo_symbol, 
            country, currency, primary_exchange, sector, created_at, updated_at,
            created_by, region
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """
        
        inserted_count = 0
        for company in new_companies:
            try:
                company_id = uuid.uuid4()
                ticker = company['ticker']
                
                # Map country to region
                region = 'nordic'
                if company.get('country') in ['Sverige', 'Sweden']:
                    region = 'sweden'
                elif company.get('country') in ['Norge', 'Norway']:
                    region = 'norway'
                elif company.get('country') in ['Danmark', 'Denmark']:
                    region = 'denmark'
                elif company.get('country') in ['Finland']:
                    region = 'finland'
                
                await self.conn.execute(
                    insert_query,
                    company_id,
                    ticker,
                    company['company_name'],
                    company.get('isin'),
                    company.get('yahoo_symbol'),
                    company.get('country'),
                    company.get('currency'),
                    company.get('exchange'),
                    company.get('sector'),
                    datetime.now(),
                    datetime.now(),
                    'borsdata_csv_import',
                    region
                )
                
                inserted_count += 1
                
                if inserted_count <= 10:  # Show first 10
                    print(f"  ✓ {ticker}: {company['company_name']} ({company.get('country', 'Unknown')})")
                
            except Exception as e:
                print(f"  ✗ Error inserting {company['ticker']}: {e}")
        
        print(f"Successfully inserted {inserted_count} new companies")
    
    async def update_existing_companies(self, updated_companies):
        """Update existing companies with new data"""
        if not updated_companies:
            print("No companies to update")
            return
            
        print(f"\nUpdating {len(updated_companies)} existing companies...")
        
        updated_count = 0
        for update in updated_companies:
            try:
                # Build dynamic update query
                set_clauses = []
                params = []
                param_count = 1
                
                for field, value in update['updates'].items():
                    set_clauses.append(f"{field} = ${param_count}")
                    params.append(value)
                    param_count += 1
                
                # Add updated_at
                set_clauses.append(f"updated_at = ${param_count}")
                params.append(datetime.now())
                param_count += 1
                
                # Add company_id for WHERE clause
                params.append(update['company_id'])
                
                update_query = f"""
                UPDATE company_master 
                SET {', '.join(set_clauses)}
                WHERE id = ${param_count}
                """
                
                await self.conn.execute(update_query, *params)
                
                updated_count += 1
                
                if updated_count <= 10:  # Show first 10
                    print(f"  ✓ {update['ticker']}: Updated {list(update['updates'].keys())}")
                
            except Exception as e:
                print(f"  ✗ Error updating {update['ticker']}: {e}")
        
        print(f"Successfully updated {updated_count} companies")
    
    async def validate_results(self):
        """Validate the ingestion results"""
        print(f"\n" + "="*60)
        print("VALIDATION RESULTS")
        print("="*60)
        
        # Total companies
        total = await self.conn.fetchval("SELECT COUNT(*) FROM company_master")
        print(f"Total companies in database: {total:,}")
        
        # Companies by country
        countries = await self.conn.fetch("""
            SELECT country, COUNT(*) as count 
            FROM company_master 
            WHERE country IS NOT NULL 
            GROUP BY country 
            ORDER BY count DESC
        """)
        
        print(f"\nCompanies by country:")
        for row in countries:
            print(f"  {row['country']}: {row['count']:,}")
        
        # Yahoo symbols coverage
        with_yahoo = await self.conn.fetchval("""
            SELECT COUNT(*) FROM company_master 
            WHERE yahoo_symbol IS NOT NULL AND yahoo_symbol != ''
        """)
        print(f"\nCompanies with Yahoo symbols: {with_yahoo:,} ({with_yahoo/total*100:.1f}%)")
        
        # ISIN coverage
        with_isin = await self.conn.fetchval("""
            SELECT COUNT(*) FROM company_master 
            WHERE isin_code IS NOT NULL AND isin_code != ''
        """)
        print(f"Companies with ISIN: {with_isin:,} ({with_isin/total*100:.1f}%)")
        
        # Sample new entries
        print(f"\nSample recent additions:")
        recent = await self.conn.fetch("""
            SELECT primary_ticker, company_name, country, yahoo_symbol
            FROM company_master 
            WHERE created_at >= CURRENT_DATE
            ORDER BY created_at DESC
            LIMIT 5
        """)
        
        for row in recent:
            print(f"  {row['primary_ticker']}: {row['company_name']} ({row['country']}) -> {row['yahoo_symbol']}")

async def main():
    # Configuration
    db_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    csv_path = '/Users/jdandemar/Downloads/Borsdata_2025-12-08.csv'
    
    # Check if CSV exists
    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file not found at {csv_path}")
        print("Please ensure the Börsdata CSV file is at the correct location")
        return
    
    ingestor = NordicCompanyIngestor(db_url)
    
    try:
        await ingestor.connect()
        
        # Load and preview data
        df = ingestor.load_csv_data(csv_path)
        
        # Get existing companies
        existing_companies = await ingestor.get_existing_companies()
        
        # Process companies
        new_companies, updated_companies, skipped = await ingestor.process_companies(df, existing_companies)
        
        # Confirm before proceeding
        print(f"\n" + "="*60)
        print("INGESTION PLAN")
        print("="*60)
        print(f"Will insert: {len(new_companies)} new companies")
        print(f"Will update: {len(updated_companies)} existing companies")
        print(f"Will skip: {len(skipped)} problematic entries")
        
        response = input(f"\nProceed with ingestion? (y/N): ").strip().lower()
        if response != 'y':
            print("Cancelled by user")
            return
        
        # Execute ingestion
        await ingestor.insert_new_companies(new_companies)
        await ingestor.update_existing_companies(updated_companies)
        
        # Validate results
        await ingestor.validate_results()
        
        print(f"\n" + "="*60)
        print("✅ INGESTION COMPLETE!")
        print("="*60)
        print(f"Next steps:")
        print(f"1. Run Yahoo Finance validation on new companies")
        print(f"2. Start fundamental data collection")
        print(f"3. Update price data collection for new symbols")
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise
    finally:
        await ingestor.disconnect()

if __name__ == "__main__":
    asyncio.run(main())