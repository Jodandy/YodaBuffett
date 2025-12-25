#!/usr/bin/env python3
"""
Simple test of premium-discount logic on one company
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta

async def test_simple():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    # Test TRAN over 3 months
    symbols = ['TRAN A', 'TRAN B']
    start_date = datetime(2023, 1, 1).date()
    end_date = datetime(2023, 4, 1).date()
    
    query = """
    SELECT symbol, date, close_price
    FROM daily_price_data
    WHERE symbol = ANY($1)
    AND date BETWEEN $2 AND $3
    ORDER BY date, symbol
    """
    
    records = await conn.fetch(query, symbols, start_date, end_date)
    df = pd.DataFrame([dict(record) for record in records])
    df['date'] = pd.to_datetime(df['date'])
    df['close_price'] = df['close_price'].astype(float)
    
    # Pivot
    pivot_df = df.pivot_table(
        index='date', 
        columns='symbol', 
        values='close_price',
        aggfunc='first'
    ).ffill()
    
    # Calculate premiums
    pivot_df['a_premium'] = (pivot_df['TRAN A'] - pivot_df['TRAN B']) / pivot_df['TRAN B'] * 100
    pivot_df['b_premium'] = (pivot_df['TRAN B'] - pivot_df['TRAN A']) / pivot_df['TRAN A'] * 100
    
    # Determine premium share 
    avg_a_premium = pivot_df['a_premium'].mean()
    avg_b_premium = pivot_df['b_premium'].mean()
    
    print(f"TRAN Premium Analysis:")
    print(f"A shares avg premium: {avg_a_premium:.2f}%")
    print(f"B shares avg premium: {avg_b_premium:.2f}%")
    
    if avg_a_premium > avg_b_premium:
        premium_share = 'A'
        print("A shares typically premium")
        
        # Look for A shares at discount (negative a_premium)
        discount_opps = pivot_df[pivot_df['a_premium'] <= -1.0].copy()
        
        print(f"\nA share discount opportunities (A cheaper than B by 1%+): {len(discount_opps)}")
        
        if len(discount_opps) > 0:
            # Simulate trades
            cash = 100000
            position_size = 0.12
            transaction_cost_rate = 0.002
            
            total_pnl = 0
            trade_count = 0
            
            for idx, row in discount_opps.head(5).iterrows():
                entry_price = row['TRAN A']
                discount_pct = row['a_premium']
                
                # Find exit - look for next day when A back to premium (or +5 days max)
                future_dates = pivot_df.index[pivot_df.index > idx][:5]
                exit_found = False
                
                for exit_date in future_dates:
                    if pivot_df.loc[exit_date, 'a_premium'] >= 0.5:  # Back to premium
                        exit_price = pivot_df.loc[exit_date, 'TRAN A']
                        exit_found = True
                        break
                
                if not exit_found:
                    continue  # Skip this trade
                    
                # Calculate trade
                available_capital = cash * position_size
                shares = int(available_capital / entry_price)
                investment = shares * entry_price
                
                entry_costs = investment * transaction_cost_rate
                gross_pnl = (exit_price - entry_price) * shares
                exit_costs = investment * transaction_cost_rate
                net_pnl = gross_pnl - entry_costs - exit_costs
                
                total_pnl += net_pnl
                trade_count += 1
                
                print(f"Trade {trade_count}:")
                print(f"  Entry: {idx.date()} @ ${entry_price:.3f} ({discount_pct:.2f}% discount)")
                print(f"  Exit:  {exit_date.date()} @ ${exit_price:.3f}")
                print(f"  Shares: {shares:,}, Investment: ${investment:,.0f}")
                print(f"  P&L: ${net_pnl:,.2f} ({net_pnl/investment*100:.2f}%)")
                
            print(f"\nTotal trades: {trade_count}")
            print(f"Total P&L: ${total_pnl:,.2f}")
            print(f"Average P&L per trade: ${total_pnl/trade_count:.2f}")
            
    await conn.close()

if __name__ == "__main__":
    asyncio.run(test_simple())