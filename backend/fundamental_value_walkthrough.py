#!/usr/bin/env python3
"""
Fundamental Value Strategy Walkthrough

Demonstrates the multi-method valuation approach step by step
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import Dict, List
import logging
from fundamental_value_strategy import (
    FundamentalValueStrategy, 
    ValuationResult, 
    CompositeValuation
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FundamentalValueWalkthrough:
    """Interactive walkthrough of the value investing strategy"""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        self.strategy = FundamentalValueStrategy(conn)
    
    async def analyze_single_stock(self, symbol: str, date: datetime):
        """Perform detailed analysis on a single stock"""
        print(f"\n{'='*60}")
        print(f"FUNDAMENTAL VALUE ANALYSIS: {symbol}")
        print(f"Analysis Date: {date.strftime('%Y-%m-%d')}")
        print(f"{'='*60}\n")
        
        # Get current price
        query = """
            SELECT close FROM market_data 
            WHERE symbol = $1 AND date <= $2 
            ORDER BY date DESC LIMIT 1
        """
        price_row = await self.conn.fetchrow(query, symbol, date)
        
        if not price_row:
            print(f"No price data available for {symbol}")
            return
            
        current_price = float(price_row['close'])
        print(f"Current Price: ${current_price:.2f}")
        
        # Get fundamental data
        fundamentals = await self.strategy.get_fundamental_data(symbol, date)
        
        print("\n📊 FUNDAMENTAL DATA:")
        print("-" * 40)
        for metric, value in fundamentals.items():
            if value is not None:
                print(f"{metric.replace('_', ' ').title()}: {value:.2f}")
        
        # Perform valuations
        composite = await self.strategy.evaluate_opportunity(symbol, date, current_price)
        
        # Show individual method results
        print("\n📈 VALUATION METHODS:")
        print("-" * 40)
        
        for val in composite.valuations:
            self._print_valuation_result(val)
        
        # Show composite results
        print("\n🎯 COMPOSITE VALUATION (FAT PITCH FRAMEWORK):")
        print("-" * 40)
        
        if composite.fat_pitch_price:
            print(f"Fat Pitch Entry: ${composite.fat_pitch_price:.2f}")
            print(f"Fair Value: ${composite.fair_value:.2f}")
            print(f"Upside Target: ${composite.upside_target:.2f}")
            print(f"Downside Risk: ${composite.downside_target:.2f}")
            print(f"Overvalued Level: ${composite.overvalued_price:.2f}")
            
            # Calculate potentials
            upside_pct = ((composite.upside_target - current_price) / current_price) * 100
            downside_pct = ((current_price - composite.downside_target) / current_price) * 100
            
            print(f"\n📊 RISK/REWARD ANALYSIS:")
            print(f"Current Asymmetry: {composite.current_asymmetry:.2f}:1")
            print(f"Upside Potential: {upside_pct:+.1f}%")
            print(f"Downside Risk: {downside_pct:.1f}%")
            print(f"Methods Agreeing: {composite.method_count}")
            
            # Investment decision
            print(f"\n💡 INVESTMENT DECISION:")
            if current_price < composite.fat_pitch_price and composite.current_asymmetry >= self.strategy.asymmetry_ratio:
                print(f"✅ FAT PITCH BUY - Asymmetric opportunity ({composite.current_asymmetry:.1f}:1)")
            elif current_price < composite.fair_value:
                print(f"🔵 UNDERVALUED - But not a fat pitch (only {composite.current_asymmetry:.1f}:1)")
            elif current_price > composite.overvalued_price:
                print(f"🔴 OVERVALUED - Consider selling")
            elif current_price > composite.upside_target:
                print(f"🎯 TARGET HIT - Consider taking profits")
            else:
                print(f"⚪ FAIRLY VALUED - No action needed")
        else:
            print("Insufficient data for composite valuation (need at least 2 methods)")
    
    def _print_valuation_result(self, val: ValuationResult):
        """Pretty print a valuation result"""
        print(f"\n{val.method}:")
        
        if val.confidence == 0:
            print(f"  ❌ {val.details.get('error', 'No data available')}")
        else:
            if val.bear_value:
                print(f"  Bear Case: ${val.bear_value:.2f}")
            if val.base_value:
                print(f"  Base Case: ${val.base_value:.2f}")
            if val.bull_value:
                print(f"  Bull Case: ${val.bull_value:.2f}")
            print(f"  Confidence: {val.confidence:.0%}")
            
            # Show formula used
            if 'formula' in val.details:
                print(f"  Formula: {val.details['formula']}")
    
    async def backtest_example(self, symbol: str, days_back: int = 252):
        """Run a backtest example for a single stock"""
        print(f"\n{'='*60}")
        print(f"BACKTESTING {symbol} - LAST {days_back} DAYS")
        print(f"{'='*60}\n")
        
        # Generate signals
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        signals = await self.strategy.generate_signals(start_date, end_date, [symbol])
        
        if len(signals) == 0:
            print("No signals generated")
            return
        
        # Run backtest
        results = await self.strategy.backtest(signals)
        
        # Display results
        print("📊 BACKTEST RESULTS:")
        print("-" * 40)
        print(f"Total Return: {results['total_return']:.2%}")
        print(f"Annualized Return: {results['annualized_return']:.2%}")
        print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {results['max_drawdown']:.2%}")
        print(f"Win Rate: {results['win_rate']:.2%}")
        print(f"Total Trades: {results['total_trades']}")
        
        # Show trades
        if 'trades_df' in results and len(results['trades_df']) > 0:
            print("\n📈 TRADE HISTORY:")
            print("-" * 40)
            trades_df = results['trades_df']
            
            for _, trade in trades_df.iterrows():
                action_emoji = "🟢" if trade['action'] == 'BUY' else "🔴"
                print(f"{action_emoji} {trade['date'].strftime('%Y-%m-%d')} - {trade['action']} "
                      f"{trade['shares']} shares @ ${trade['price']:.2f}")
                
                if trade['action'] == 'SELL' and 'return' in trade:
                    return_emoji = "📈" if trade['return'] > 0 else "📉"
                    print(f"   {return_emoji} Return: {trade['return']:.2%} "
                          f"(P&L: ${trade['pnl']:.2f})")
        
        # Show signal distribution
        print("\n📊 SIGNAL DISTRIBUTION:")
        print("-" * 40)
        signal_counts = signals['signal_type'].value_counts()
        for signal_type, count in signal_counts.items():
            if signal_type:
                print(f"{signal_type}: {count}")
    
    async def screen_opportunities(self, top_n: int = 10):
        """Screen for current value opportunities"""
        print(f"\n{'='*60}")
        print(f"VALUE OPPORTUNITY SCREENER")
        print(f"{'='*60}\n")
        
        # Get all symbols with recent data
        query = """
            SELECT DISTINCT symbol FROM market_data 
            WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        """
        rows = await self.conn.fetch(query)
        symbols = [row['symbol'] for row in rows][:50]  # Limit for demo
        
        opportunities = []
        
        print(f"Screening {len(symbols)} stocks...")
        
        for symbol in symbols:
            try:
                # Get latest price
                price_query = """
                    SELECT close FROM market_data 
                    WHERE symbol = $1 
                    ORDER BY date DESC LIMIT 1
                """
                price_row = await self.conn.fetchrow(price_query, symbol)
                
                if not price_row:
                    continue
                
                current_price = float(price_row['close'])
                
                # Evaluate opportunity
                composite = await self.strategy.evaluate_opportunity(
                    symbol, datetime.now(), current_price
                )
                
                if composite.fat_pitch_price and composite.current_asymmetry:
                    # Calculate discount to fat pitch
                    discount = (composite.fat_pitch_price - current_price) / composite.fat_pitch_price
                    
                    opportunities.append({
                        'symbol': symbol,
                        'current_price': current_price,
                        'fat_pitch_price': composite.fat_pitch_price,
                        'fair_value': composite.fair_value,
                        'asymmetry': composite.current_asymmetry,
                        'discount': discount,
                        'upside_pct': ((composite.upside_target - current_price) / current_price * 100)
                            if composite.upside_target else 0
                    })
                    
            except Exception as e:
                continue
        
        # Sort by asymmetry ratio (best opportunities first)
        opportunities.sort(key=lambda x: x['asymmetry'], reverse=True)
        
        # Display top opportunities
        print(f"\n🎯 TOP {top_n} VALUE OPPORTUNITIES:")
        print("-" * 80)
        print(f"{'Symbol':8} {'Price':>8} {'Fat Pitch':>10} {'Fair Value':>11} "
              f"{'Asymmetry':>10} {'Upside':>8}")
        print("-" * 80)
        
        for opp in opportunities[:top_n]:
            fat_pitch_indicator = "🟢" if opp['asymmetry'] >= self.strategy.asymmetry_ratio else "🟡"
            print(f"{fat_pitch_indicator} {opp['symbol']:6} ${opp['current_price']:7.2f} "
                  f"${opp['fat_pitch_price']:9.2f} ${opp['fair_value']:10.2f} "
                  f"{opp['asymmetry']:9.1f}:1 {opp['upside_pct']:7.1f}%")
        
        print("\n🟢 = Fat Pitch (3:1+ asymmetry), 🟡 = Undervalued but not Fat Pitch")


async def main():
    """Run the walkthrough examples"""
    
    # Connect to database
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        walkthrough = FundamentalValueWalkthrough(conn)
        
        # Example 1: Detailed analysis of a single stock
        print("\n" + "="*60)
        print("EXAMPLE 1: SINGLE STOCK ANALYSIS")
        print("="*60)
        
        await walkthrough.analyze_single_stock('AAPL', datetime.now())
        
        # Example 2: Backtest a stock
        print("\n" + "="*60)
        print("EXAMPLE 2: BACKTEST ANALYSIS")
        print("="*60)
        
        await walkthrough.backtest_example('MSFT', days_back=365)
        
        # Example 3: Screen for opportunities
        print("\n" + "="*60)
        print("EXAMPLE 3: OPPORTUNITY SCREENER")
        print("="*60)
        
        await walkthrough.screen_opportunities(top_n=15)
        
        # Summary
        print("\n" + "="*60)
        print("STRATEGY SUMMARY")
        print("="*60)
        print("""
The Fundamental Value Strategy uses 5 valuation methods to identify
asymmetric investment opportunities:

1. Graham Number: Classic value metric combining P/E and P/B
2. Earnings Power Value: Sustainable earnings capacity
3. Residual Income Model: Book value + excess returns
4. Free Cash Flow Yield: Cash generation ability  
5. Net Current Asset Value: Liquidation floor

The Fat Pitch Framework requires:
- At least 2 methods agreeing on valuation
- 3:1 or better upside/downside asymmetry
- High confidence in fundamental data

This creates a disciplined approach to value investing that waits
for exceptional opportunities rather than mediocre ones.
        """)
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())