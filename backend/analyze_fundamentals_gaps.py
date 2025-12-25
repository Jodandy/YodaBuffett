#!/usr/bin/env python3
"""
Analyze companies that have price data but no fundamental data.
"""

import asyncio
import asyncpg

async def analyze_gaps():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        # Find companies with recent price data
        price_companies = await conn.fetch('''
            SELECT DISTINCT symbol
            FROM daily_price_data 
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY symbol
        ''')
        
        price_symbols = set(row['symbol'] for row in price_companies)
        print(f'Companies with price data (last 30 days): {len(price_symbols)}')
        
        # Find companies with fundamental data
        fund_companies = await conn.fetch('''
            SELECT DISTINCT symbol
            FROM historical_fundamentals_daily
            WHERE pe_ratio IS NOT NULL
            ORDER BY symbol
        ''')
        
        fund_symbols = set(row['symbol'] for row in fund_companies)
        print(f'Companies with historical fundamental data: {len(fund_symbols)}')
        
        # Find companies missing fundamental data
        missing_fundamentals = price_symbols - fund_symbols
        print(f'Companies missing fundamental data: {len(missing_fundamentals)}')
        
        # Check company_master for Yahoo symbols
        if missing_fundamentals:
            # Convert to list and check in company_master
            missing_list = list(missing_fundamentals)
            
            yahoo_check = await conn.fetch('''
                SELECT 
                    primary_ticker,
                    yahoo_symbol,
                    yahoo_finance_available,
                    company_name
                FROM company_master 
                WHERE primary_ticker = ANY($1)
                ORDER BY primary_ticker
            ''', missing_list)
            
            no_yahoo_symbol = 0
            yahoo_not_available = 0
            has_yahoo_symbol = 0
            
            print('\nFirst 20 companies missing fundamentals:')
            print(f"{'Symbol':<12} {'Status':<20} {'Yahoo Symbol':<15} {'Company Name'}")
            print("-" * 80)
            
            for i, row in enumerate(yahoo_check[:20]):
                symbol = row['primary_ticker']
                yahoo_sym = row['yahoo_symbol']
                available = row['yahoo_finance_available']
                company_name = row['company_name'] or "Unknown"
                
                if not yahoo_sym:
                    status = 'NO_YAHOO_SYMBOL'
                    no_yahoo_symbol += 1
                elif not available:
                    status = 'YAHOO_NOT_AVAILABLE'  
                    yahoo_not_available += 1
                else:
                    status = 'HAS_YAHOO_SYMBOL'
                    has_yahoo_symbol += 1
                    
                print(f'{symbol:<12} {status:<20} {yahoo_sym or "None":<15} {company_name[:30]}')
            
            print(f'\nSummary of {len(missing_fundamentals)} companies missing fundamentals:')
            print(f'- No Yahoo symbol: {no_yahoo_symbol}')
            print(f'- Yahoo not available: {yahoo_not_available}')  
            print(f'- Has Yahoo symbol: {has_yahoo_symbol}')
            
            # Check daily_fundamentals table too
            daily_fund_companies = await conn.fetch('''
                SELECT DISTINCT symbol
                FROM daily_fundamentals
                ORDER BY symbol
            ''')
            
            daily_fund_symbols = set(row['symbol'] for row in daily_fund_companies)
            print(f'\nCompanies in daily_fundamentals table: {len(daily_fund_symbols)}')
            
            # Check if daily fundamentals worker is collecting data for companies with Yahoo symbols
            companies_with_yahoo = await conn.fetch('''
                SELECT 
                    primary_ticker,
                    yahoo_symbol,
                    company_name
                FROM company_master 
                WHERE yahoo_symbol IS NOT NULL
                AND yahoo_finance_available = true
                AND primary_ticker = ANY($1)
                ORDER BY primary_ticker
                LIMIT 10
            ''', missing_list)
            
            print(f'\nCompanies missing fundamentals but have Yahoo symbols (first 10):')
            for row in companies_with_yahoo:
                print(f'  {row["primary_ticker"]:<12} -> {row["yahoo_symbol"]:<15} ({row["company_name"]})')
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_gaps())