#!/usr/bin/env python3
"""
Simple test runner for document anomaly strategy
"""

import asyncio
import subprocess
import sys

async def run_backtest():
    print("🚀 Running Document Anomaly Backtest...")
    
    try:
        # Import and run the backtest directly
        from backtest_document_anomaly_strategy import DocumentAnomalyBacktester
        from datetime import date
        
        backtester = DocumentAnomalyBacktester(
            initial_capital=100000,
            transaction_cost=0.001,
            max_position_size=0.12,
            rebalance_frequency=2
        )
        
        await backtester.setup()
        
        # Use historical period with known data
        start_date = date(2024, 8, 1)
        end_date = date(2024, 11, 1)
        
        print(f"Running backtest from {start_date} to {end_date}")
        
        results = await backtester.run_backtest(start_date, end_date)
        
        if results:
            print(f"\n📊 RESULTS:")
            print(f"Total Return: {results.get('total_return', 0):.2%}")
            print(f"Sharpe Ratio: {results.get('sharpe_ratio', 0):.2f}")
            print(f"Number of Signals: {results.get('num_signals', 0)}")
            print(f"Number of Trades: {results.get('num_trades', 0)}")
        else:
            print("No results generated")
        
        await backtester.cleanup()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_backtest())