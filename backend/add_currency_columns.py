#!/usr/bin/env python3
"""
Add Currency Columns to Database

Add stock_currency and report_currency columns to company_master and nordic_companies
using data from Borsdata CSV file.
"""

import asyncio
import asyncpg
import csv
from pathlib import Path

async def add_currency_columns():
    """Add currency columns and populate with Borsdata data"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('💱 ADDING CURRENCY COLUMNS TO DATABASE')
    print('=' * 60)
    
    # Add columns to company_master
    try:
        await conn.execute('ALTER TABLE company_master ADD COLUMN IF NOT EXISTS stock_currency VARCHAR(3)')
        await conn.execute('ALTER TABLE company_master ADD COLUMN IF NOT EXISTS report_currency VARCHAR(3)')
        print('✅ Added currency columns to company_master')
    except Exception as e:
        print(f'⚠️ Currency columns might already exist in company_master: {e}')
    
    # Add columns to nordic_companies  
    try:
        await conn.execute('ALTER TABLE nordic_companies ADD COLUMN IF NOT EXISTS stock_currency VARCHAR(3)')
        await conn.execute('ALTER TABLE nordic_companies ADD COLUMN IF NOT EXISTS report_currency VARCHAR(3)')
        print('✅ Added currency columns to nordic_companies')
    except Exception as e:
        print(f'⚠️ Currency columns might already exist in nordic_companies: {e}')
    
    # Read Borsdata CSV file
    borsdata_file = Path('/Users/jdandemar/Downloads/Borsdata_2025-12-08.csv')
    
    if not borsdata_file.exists():
        print(f'❌ Borsdata file not found: {borsdata_file}')
        return
    
    print(f'📊 Reading currency data from {borsdata_file}')
    
    # Parse CSV and create currency mapping
    currency_mapping = {}
    
    with open(borsdata_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        
        for row in reader:
            ticker = row['Info - Ticker'].strip().strip('"')
            stock_currency = row['Info - Kursvaluta'].strip().strip('"')
            report_currency = row['Info - Rapportvaluta'].strip().strip('"')
            
            currency_mapping[ticker] = {
                'stock_currency': stock_currency,
                'report_currency': report_currency
            }
    
    print(f'📈 Loaded {len(currency_mapping):,} currency mappings')
    
    # Update company_master
    cm_updated = 0
    for ticker, currencies in currency_mapping.items():
        try:
            result = await conn.execute('''
                UPDATE company_master 
                SET stock_currency = $1, report_currency = $2
                WHERE primary_ticker = $3
            ''', currencies['stock_currency'], currencies['report_currency'], ticker)
            
            if result.startswith('UPDATE') and int(result.split()[-1]) > 0:
                cm_updated += 1
                
        except Exception as e:
            continue
    
    print(f'✅ Updated {cm_updated:,} records in company_master')
    
    # Update nordic_companies
    nc_updated = 0
    for ticker, currencies in currency_mapping.items():
        try:
            result = await conn.execute('''
                UPDATE nordic_companies 
                SET stock_currency = $1, report_currency = $2
                WHERE ticker = $3
            ''', currencies['stock_currency'], currencies['report_currency'], ticker)
            
            if result.startswith('UPDATE') and int(result.split()[-1]) > 0:
                nc_updated += 1
                
        except Exception as e:
            continue
    
    print(f'✅ Updated {nc_updated:,} records in nordic_companies')
    
    # Show sample results
    print(f'\n📋 SAMPLE CURRENCY UPDATES:')
    
    sample_query = '''
    SELECT primary_ticker, stock_currency, report_currency, name
    FROM company_master
    WHERE stock_currency IS NOT NULL 
    AND report_currency IS NOT NULL
    AND primary_ticker IN ('AAK', 'ABB', 'VOLV-B', 'ERIC-B')
    ORDER BY primary_ticker
    '''
    
    samples = await conn.fetch(sample_query)
    
    print(f'{"Ticker":<10}{"Stock Curr":<12}{"Report Curr":<13}{"Company"}')
    print('-' * 60)
    
    for row in samples:
        print(f'{row["primary_ticker"]:<10}{row["stock_currency"]:<12}{row["report_currency"]:<13}{row["name"][:30]}')
    
    # Check for currency mismatches
    mismatch_query = '''
    SELECT primary_ticker, stock_currency, report_currency, name
    FROM company_master
    WHERE stock_currency != report_currency
    AND stock_currency IS NOT NULL 
    AND report_currency IS NOT NULL
    ORDER BY primary_ticker
    LIMIT 10
    '''
    
    mismatches = await conn.fetch(mismatch_query)
    
    print(f'\n⚠️ CURRENCY MISMATCHES (need conversion):')
    print(f'{"Ticker":<10}{"Stock Curr":<12}{"Report Curr":<13}{"Company"}')
    print('-' * 60)
    
    for row in mismatches:
        print(f'{row["primary_ticker"]:<10}{row["stock_currency"]:<12}{row["report_currency"]:<13}{row["name"][:30]}')
    
    await conn.close()
    
    print(f'\n✅ CURRENCY COLUMNS SUCCESSFULLY ADDED')
    print(f'   You can now use stock_currency and report_currency in DCF calculations')

if __name__ == "__main__":
    asyncio.run(add_currency_columns())