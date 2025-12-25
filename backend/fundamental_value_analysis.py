#!/usr/bin/env python3
"""
Fundamental Value Strategy Analysis

Quick analysis script to test the fundamental value strategy
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
import pandas as pd
from fundamental_value_strategy import FundamentalValueStrategy

async def main():
    # Connect to database
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        # Initialize strategy
        strategy = FundamentalValueStrategy(conn)
        
        # Test with a single stock
        symbol = 'AAPL'
        current_date = datetime.now()
        
        # Get current price
        price_query = "SELECT close FROM market_data WHERE symbol = $1 ORDER BY date DESC LIMIT 1"
        price_row = await conn.fetchrow(price_query, symbol)
        
        if price_row:
            current_price = float(price_row['close'])
            print(f"\nAnalyzing {symbol} at ${current_price:.2f}")
            
            # Get fundamental data
            fundamentals = await strategy.get_fundamental_data(symbol, current_date)
            print(f"\nFundamental Data:")
            for metric, value in fundamentals.items():
                if value is not None:
                    print(f"  {metric}: {value:.2f}")
            
            # Evaluate opportunity
            composite = await strategy.evaluate_opportunity(symbol, current_date, current_price)
            
            print(f"\nValuation Results:")
            if composite.fair_value:
                print(f"  Fair Value: ${composite.fair_value:.2f}")
                print(f"  Fat Pitch Price: ${composite.fat_pitch_price:.2f}")
                print(f"  Upside Target: ${composite.upside_target:.2f}")
                print(f"  Downside Target: ${composite.downside_target:.2f}")
                print(f"  Current Asymmetry: {composite.current_asymmetry:.1f}:1")
                print(f"  Methods Agreeing: {composite.method_count}")
                
                # Show each method
                print(f"\nIndividual Methods:")
                for val in composite.valuations:
                    if val.base_value:
                        print(f"  {val.method}: ${val.base_value:.2f} (confidence: {val.confidence:.0%})")
            else:
                print("  Insufficient data for valuation")
        
        # Quick backtest
        print(f"\n{'='*50}")
        print("Running 1-year backtest...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # Test with multiple symbols
        symbols = ['AAPL', 'MSFT', 'JNJ', 'WMT', 'KO']
        
        signals = await strategy.generate_signals(start_date, end_date, symbols)
        
        if len(signals) > 0:
            results = await strategy.backtest(signals)
            
            print(f"\nBacktest Results:")
            print(f"  Total Return: {results['total_return']:.2%}")
            print(f"  Sharpe Ratio: {results['sharpe_ratio']:.2f}")
            print(f"  Max Drawdown: {results['max_drawdown']:.2%}")
            print(f"  Win Rate: {results['win_rate']:.2%}")
            print(f"  Total Trades: {results['total_trades']}")
        else:
            print("No signals generated. You may need to collect fundamental data first.")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(main())