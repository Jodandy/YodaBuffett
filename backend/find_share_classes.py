#!/usr/bin/env python3
"""
Find companies with multiple share classes
"""

import asyncio
import asyncpg
import re
from collections import defaultdict

async def find_similar_symbols():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    # Get all symbols with prices
    data = await conn.fetch("""
    SELECT DISTINCT symbol, close_price, volume
    FROM daily_price_data
    WHERE date >= CURRENT_DATE - INTERVAL '3 days'
    AND close_price > 0
    ORDER BY symbol
    """)
    
    # Group by potential base names
    groups = defaultdict(list)
    
    for row in data:
        symbol = row['symbol']
        
        # Try different extraction methods
        bases = []
        
        # Method 1: Remove space + letter
        if re.match(r'.*\s[A-Z]$', symbol):
            bases.append(symbol[:-2])
            
        # Method 2: Remove dash + letter  
        if re.match(r'.*-[A-Z]$', symbol):
            bases.append(symbol[:-2])
            
        # Method 3: Remove last letter if it looks like a class
        if len(symbol) > 3 and symbol[-1] in 'ABCDEFGH':
            bases.append(symbol[:-1])
            
        # Add original as potential base
        bases.append(symbol)
        
        for base in bases:
            groups[base].append({
                'symbol': symbol,
                'price': float(row['close_price']),
                'volume': float(row['volume']) if row['volume'] else 0
            })
    
    # Find groups with multiple symbols
    print('Companies with multiple share classes:')
    found_pairs = 0
    
    for base, stocks in groups.items():
        if len(stocks) > 1:
            # Remove duplicates
            unique_symbols = {}
            for stock in stocks:
                unique_symbols[stock['symbol']] = stock
                
            if len(unique_symbols) > 1:
                found_pairs += 1
                print(f'  {base}:')
                for symbol, data in unique_symbols.items():
                    print(f'    {symbol}: ${data["price"]:.2f} (vol: {data["volume"]:,.0f})')
                print()
                
                if found_pairs >= 15:  # Limit output
                    break
    
    print(f'Total groups with multiple symbols: {found_pairs}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(find_similar_symbols())