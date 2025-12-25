#!/usr/bin/env python3
"""
Simple Demonstration of Debt-Adjusted Growth Strategy

Shows current opportunities and a basic performance calculation over the last 6 months.
"""

import asyncio
import asyncpg
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleGrowthAnalyzer:
    """Simplified growth analyzer for demonstration."""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        
        # Strategy parameters
        self.base_ps = 1.5  # Base P/S multiple
        self.growth_factor = 0.15  # P/S increase per 1% growth
        self.debt_penalty = 0.3  # Debt/equity penalty
        self.buy_threshold = 0.8  # Buy at 80% of fair value
        
    async def calculate_revenue_growth(self, symbol: str, current_date: date) -> Optional[float]:
        """Calculate YoY revenue growth."""
        
        query = """
        WITH revenue_data AS (
            SELECT date, revenue_per_share
            FROM historical_fundamentals_daily
            WHERE symbol = $1 
                AND revenue_per_share IS NOT NULL
                AND revenue_per_share > 0
                AND date <= $2
            ORDER BY date DESC
            LIMIT 400
        )
        SELECT 
            revenue_per_share as current_revenue,
            LAG(revenue_per_share, 252) OVER (ORDER BY date) as year_ago_revenue,
            date as current_date,
            LAG(date, 252) OVER (ORDER BY date) as year_ago_date
        FROM revenue_data
        ORDER BY date DESC
        LIMIT 1
        """
        
        row = await self.conn.fetchrow(query, symbol, current_date)
        
        if not row or not row['year_ago_revenue']:
            return None
            
        current = float(row['current_revenue'])
        year_ago = float(row['year_ago_revenue'])
        
        if year_ago <= 0:
            return None
            
        # Calculate annualized growth
        if row['year_ago_date']:
            actual_days = (row['current_date'] - row['year_ago_date']).days
            if actual_days < 200:  # Need roughly a year of data
                return None
            years = actual_days / 365.25
            growth = ((current / year_ago) ** (1/years) - 1) * 100
        else:
            growth = (current / year_ago - 1) * 100
            
        return max(-80, min(growth, 300))  # Cap extreme values
        
    async def get_current_metrics(self, symbol: str, current_date: date) -> Optional[Dict]:
        """Get current financial metrics."""
        
        query = """
        SELECT 
            ps_ratio,
            debt_to_equity,
            close_price,
            market_cap
        FROM historical_fundamentals_daily
        WHERE symbol = $1 
            AND date <= $2
            AND ps_ratio IS NOT NULL
            AND ps_ratio > 0
        ORDER BY date DESC
        LIMIT 1
        """
        
        row = await self.conn.fetchrow(query, symbol, current_date)
        
        if not row:
            return None
            
        return {
            'ps_ratio': float(row['ps_ratio']),
            'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,
            'price': float(row['close_price']) if row['close_price'] else None,
            'market_cap': float(row['market_cap']) if row['market_cap'] else None
        }
        
    def calculate_fair_ps(self, growth: float, debt_ratio: float) -> float:
        """Calculate fair P/S based on growth and debt."""
        
        # Base + growth component
        growth_component = self.base_ps + (growth * self.growth_factor)
        
        # Debt penalty
        debt_penalty = min(0.8, self.debt_penalty * debt_ratio)
        debt_adjustment = max(0.2, 1 - debt_penalty)
        
        fair_ps = growth_component * debt_adjustment
        return max(0.5, min(fair_ps, 12.0))  # Reasonable bounds
        
    async def find_opportunities(self, scan_date: date) -> List[Dict]:
        """Find current opportunities."""
        
        # Get symbols with good data coverage
        query = """
        SELECT symbol, COUNT(*) as records
        FROM historical_fundamentals_daily
        WHERE ps_ratio IS NOT NULL
            AND revenue_per_share IS NOT NULL
            AND revenue_per_share > 0
            AND date <= $1
        GROUP BY symbol
        HAVING COUNT(*) >= 100
        ORDER BY records DESC
        LIMIT 80
        """
        
        symbols_data = await self.conn.fetch(query, scan_date)
        symbols = [row['symbol'] for row in symbols_data]
        
        logger.info(f"Analyzing {len(symbols)} symbols")
        
        opportunities = []
        
        for symbol in symbols:
            try:
                # Get growth rate
                growth = await self.calculate_revenue_growth(symbol, scan_date)
                if growth is None:
                    continue
                    
                # Get current metrics
                metrics = await self.get_current_metrics(symbol, scan_date)
                if not metrics:
                    continue
                    
                # Calculate fair value
                fair_ps = self.calculate_fair_ps(growth, metrics['debt_to_equity'])
                
                # Check if undervalued
                current_ps = metrics['ps_ratio']
                discount = (fair_ps - current_ps) / fair_ps
                
                # Buy signal criteria
                is_buy = (current_ps < fair_ps * self.buy_threshold and 
                         growth > 0 and
                         discount > 0.15)
                         
                opportunities.append({
                    'symbol': symbol,
                    'growth': growth,
                    'current_ps': current_ps,
                    'fair_ps': fair_ps,
                    'debt_ratio': metrics['debt_to_equity'],
                    'discount': discount * 100,
                    'price': metrics['price'],
                    'is_buy': is_buy
                })
                
            except Exception as e:
                logger.debug(f"Error analyzing {symbol}: {e}")
                continue
                
        # Sort by discount (most undervalued first)
        opportunities.sort(key=lambda x: x['discount'], reverse=True)
        
        return opportunities


async def main():
    """Run simple growth strategy demo."""
    
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        analyzer = SimpleGrowthAnalyzer(conn)
        
        print("\n" + "="*80)
        print("DEBT-ADJUSTED TOPLINE GROWTH STRATEGY - SIMPLE DEMONSTRATION")
        print("="*80)
        
        # Get latest date with data
        latest_date_row = await conn.fetchrow("""
            SELECT MAX(date) as max_date 
            FROM historical_fundamentals_daily 
            WHERE ps_ratio IS NOT NULL
        """)
        
        if not latest_date_row:
            print("No fundamental data found")
            return
            
        latest_date = latest_date_row['max_date']
        print(f"Analysis Date: {latest_date}")
        
        # Find current opportunities
        opportunities = await analyzer.find_opportunities(latest_date)
        buy_signals = [opp for opp in opportunities if opp['is_buy']]
        
        print(f"\nMARKET SCAN RESULTS:")
        print(f"  Companies Analyzed: {len(opportunities)}")
        print(f"  Buy Signals Generated: {len(buy_signals)}")
        print(f"  Average Growth Rate: {np.mean([o['growth'] for o in opportunities]):+.1f}%")
        print(f"  Companies with Positive Growth: {len([o for o in opportunities if o['growth'] > 0])}")
        
        # Show top opportunities
        if buy_signals:
            print(f"\nTOP GROWTH OPPORTUNITIES:")
            print(f"{'Symbol':<10} {'Growth':<8} {'P/S':<6} {'Fair':<6} {'Debt':<6} {'Disc%':<7} {'Price':<8}")
            print("-" * 67)
            
            for opp in buy_signals[:15]:
                print(f"{opp['symbol']:<10} {opp['growth']:>7.1f}% "
                      f"{opp['current_ps']:>5.2f} {opp['fair_ps']:>5.2f} "
                      f"{opp['debt_ratio']:>5.2f} {opp['discount']:>6.1f}% "
                      f"{opp['price']:>7.2f}")
        
        # Simple 6-month backtest
        print(f"\n" + "="*80)
        print("SIMPLE 6-MONTH PERFORMANCE TEST")
        print("="*80)
        
        backtest_start = latest_date - timedelta(days=180)
        print(f"Period: {backtest_start} to {latest_date}")
        
        # Get signals from 6 months ago
        old_opportunities = await analyzer.find_opportunities(backtest_start)
        old_buy_signals = [opp for opp in old_opportunities if opp['is_buy']]
        
        if old_buy_signals:
            print(f"Signals generated 6 months ago: {len(old_buy_signals)}")
            
            # Check performance
            total_return = 0
            valid_tests = 0
            performance_details = []
            
            for signal in old_buy_signals[:12]:  # Test top 12 signals
                # Get current price
                current_metrics = await analyzer.get_current_metrics(signal['symbol'], latest_date)
                
                if current_metrics and current_metrics['price'] and signal['price'] > 0:
                    return_pct = (current_metrics['price'] / signal['price'] - 1) * 100
                    total_return += return_pct
                    valid_tests += 1
                    
                    performance_details.append({
                        'symbol': signal['symbol'],
                        'entry_price': signal['price'],
                        'exit_price': current_metrics['price'],
                        'return': return_pct,
                        'entry_growth': signal['growth']
                    })
            
            if valid_tests > 0:
                avg_return = total_return / valid_tests
                winning_trades = len([p for p in performance_details if p['return'] > 0])
                win_rate = winning_trades / len(performance_details) * 100
                
                print(f"\nPERFORMANCE SUMMARY:")
                print(f"  Average Return: {avg_return:+.2f}%")
                print(f"  Win Rate: {win_rate:.1f}% ({winning_trades}/{len(performance_details)})")
                print(f"  Sample Size: {valid_tests} trades")
                
                # Show individual performance
                print(f"\nINDIVIDUAL TRADE RESULTS:")
                print(f"{'Symbol':<10} {'Entry':<8} {'Exit':<8} {'Return':<8} {'Growth':<8}")
                print("-" * 50)
                
                performance_details.sort(key=lambda x: x['return'], reverse=True)
                for perf in performance_details:
                    print(f"{perf['symbol']:<10} {perf['entry_price']:>7.2f} "
                          f"{perf['exit_price']:>7.2f} {perf['return']:>+6.1f}% "
                          f"{perf['entry_growth']:>+6.1f}%")
                          
                # Calculate benchmark (equal weight of all analyzed stocks)
                benchmark_symbols = [opp['symbol'] for opp in old_opportunities[:50]]
                benchmark_return = 0
                benchmark_count = 0
                
                for symbol in benchmark_symbols:
                    old_metrics = await analyzer.get_current_metrics(symbol, backtest_start)
                    new_metrics = await analyzer.get_current_metrics(symbol, latest_date)
                    
                    if (old_metrics and new_metrics and 
                        old_metrics['price'] and new_metrics['price'] and 
                        old_metrics['price'] > 0):
                        
                        benchmark_return += (new_metrics['price'] / old_metrics['price'] - 1) * 100
                        benchmark_count += 1
                
                if benchmark_count > 0:
                    avg_benchmark = benchmark_return / benchmark_count
                    alpha = avg_return - avg_benchmark
                    
                    print(f"\nCOMPARISON TO MARKET:")
                    print(f"  Strategy Return: {avg_return:+.2f}%")
                    print(f"  Market Return: {avg_benchmark:+.2f}%")
                    print(f"  Alpha (Outperformance): {alpha:+.2f}%")
        else:
            print("No buy signals were generated 6 months ago.")
            
        # Strategy explanation
        print(f"\n" + "="*80)
        print("STRATEGY EXPLANATION")
        print("="*80)
        
        print("DEBT-ADJUSTED TOPLINE GROWTH MODEL:")
        print("1. Calculate year-over-year revenue growth rate")
        print("2. Determine fair P/S ratio based on growth:")
        print(f"   Fair P/S = {analyzer.base_ps} + (Growth% × {analyzer.growth_factor})")
        print("3. Adjust for debt levels:")
        print(f"   Debt Penalty = {analyzer.debt_penalty} × Debt/Equity ratio")
        print("   Final P/S = Fair P/S × (1 - Debt Penalty)")
        print("4. Buy when current P/S < 80% of debt-adjusted fair P/S")
        print("5. Only buy companies with positive revenue growth")
        
        print(f"\nKEY ADVANTAGES OVER DCF:")
        print("• Simple: Uses only current metrics, no complex projections")
        print("• Fast: Can analyze entire universe quickly")
        print("• Intuitive: Growth companies deserve higher multiples")
        print("• Risk-aware: Debt reduces valuation premium")
        print("• Actionable: Clear buy/sell signals")
        
        if buy_signals:
            high_growth = [s for s in buy_signals if s['growth'] > 20]
            low_debt = [s for s in buy_signals if s['debt_ratio'] < 0.5]
            
            print(f"\nCURRENT OPPORTUNITY BREAKDOWN:")
            print(f"• High Growth (>20%): {len(high_growth)} companies")
            print(f"• Low Debt (<0.5x): {len(low_debt)} companies")
            print(f"• Average Discount: {np.mean([s['discount'] for s in buy_signals]):.1f}%")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())