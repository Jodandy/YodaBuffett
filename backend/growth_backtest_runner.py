#!/usr/bin/env python3
"""
Growth Model Backtest Runner

Complete backtesting framework for the debt-adjusted growth model with:
- Multiple timeframes (3M, 6M, 1Y)
- Portfolio simulation with position sizing
- Performance rankings and charts
- Detailed trade analysis
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
from simple_growth_model import SimpleGrowthModel
# import matplotlib.pyplot as plt  # Optional for charts
# import seaborn as sns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GrowthBacktestRunner:
    """Complete backtesting framework for growth model"""
    
    def __init__(self, 
                 initial_capital: float = 100000,
                 position_size_pct: float = 0.20,  # 20% per position
                 max_positions: int = 5,
                 transaction_cost: float = 0.002):  # 0.2% total costs
        
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.max_positions = max_positions
        self.transaction_cost = transaction_cost
        
        # Models and connections
        self.growth_model = SimpleGrowthModel()
        self.db_conn = None
        
        # Results storage
        self.backtest_results = {}
    
    async def setup(self):
        """Initialize connections"""
        await self.growth_model.setup()
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    async def cleanup(self):
        """Close connections"""
        await self.growth_model.cleanup()
        if self.db_conn:
            await self.db_conn.close()
    
    async def get_future_price(self, symbol: str, entry_date: datetime, days_forward: int) -> Optional[float]:
        """Get stock price N days after entry date"""
        
        target_date = entry_date + timedelta(days=days_forward)
        
        # Get price within ±5 days of target
        price_query = """
        SELECT close_price, date
        FROM daily_price_data
        WHERE symbol = $1
        AND date >= $2
        AND date <= $3
        ORDER BY date
        LIMIT 1
        """
        
        result = await self.db_conn.fetchrow(
            price_query, 
            symbol, 
            target_date.date() - timedelta(days=5),
            target_date.date() + timedelta(days=5)
        )
        
        return float(result['close_price']) if result else None
    
    async def generate_signals_for_date(self, signal_date: datetime, top_n: int = 50) -> List[Dict]:
        """Generate top N signals for a specific date"""
        
        buy_signals = await self.growth_model.screen_universe(signal_date, 'BUY')
        
        # Filter out extreme outliers that might be data errors
        filtered_signals = []
        for signal in buy_signals:
            # Skip if growth is unrealistic (>1000% or <-90%)
            growth_pct = signal['revenue_growth'] * 100
            if -90 <= growth_pct <= 1000 and signal['discount'] <= 0.99:
                filtered_signals.append(signal)
        
        # Return top N by discount
        return filtered_signals[:top_n]
    
    async def backtest_timeframe(self, start_date: datetime, end_date: datetime, 
                                hold_days: int, timeframe_name: str) -> Dict:
        """Backtest over a specific timeframe"""
        
        logger.info(f"🔍 Running {timeframe_name} backtest: {start_date.date()} to {end_date.date()}")
        
        # Generate test dates (monthly intervals)
        test_dates = []
        current_date = start_date
        while current_date <= end_date:
            test_dates.append(current_date)
            current_date += timedelta(days=30)
        
        all_trades = []
        
        for test_date in test_dates:
            logger.info(f"   Generating signals for {test_date.date()}")
            
            # Get signals for this date
            signals = await self.generate_signals_for_date(test_date, top_n=20)
            
            if not signals:
                continue
            
            # Take top 5 by discount (best opportunities)
            selected_signals = signals[:self.max_positions]
            
            # Process each signal
            for rank, signal in enumerate(selected_signals, 1):
                symbol = signal['symbol']
                entry_price = signal['current_price']
                
                # Get exit price
                exit_price = await self.get_future_price(symbol, test_date, hold_days)
                
                if exit_price and exit_price > 0:
                    # Calculate returns
                    gross_return = (exit_price - entry_price) / entry_price
                    net_return = gross_return - self.transaction_cost
                    
                    # Filter out likely corporate actions
                    if abs(gross_return) <= 2.0:  # Less than 200% change
                        trade = {
                            'symbol': symbol,
                            'timeframe': timeframe_name,
                            'signal_date': test_date.date(),
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'hold_days': hold_days,
                            'rank': rank,
                            'predicted_discount': signal['discount'],
                            'revenue_growth': signal['revenue_growth'],
                            'debt_to_equity': signal['debt_to_equity'],
                            'fair_ps': signal['fair_ps'],
                            'current_ps': signal['current_ps'],
                            'gross_return': gross_return,
                            'net_return': net_return,
                            'success': net_return > 0
                        }
                        all_trades.append(trade)
        
        # Calculate performance metrics
        if all_trades:
            df = pd.DataFrame(all_trades)
            
            metrics = {
                'timeframe': timeframe_name,
                'hold_days': hold_days,
                'total_trades': len(df),
                'win_rate': df['success'].mean(),
                'avg_return': df['net_return'].mean(),
                'median_return': df['net_return'].median(),
                'volatility': df['net_return'].std(),
                'sharpe_ratio': df['net_return'].mean() / df['net_return'].std() if df['net_return'].std() > 0 else 0,
                'best_trade': df['net_return'].max(),
                'worst_trade': df['net_return'].min(),
                'total_return': (1 + df['net_return']).prod() - 1,  # Compound return
                'trades_df': df
            }
            
            logger.info(f"   ✅ {timeframe_name}: {len(df)} trades, {metrics['win_rate']:.1%} win rate, {metrics['avg_return']:+.1%} avg return")
            
            return metrics
        else:
            logger.warning(f"   ❌ {timeframe_name}: No valid trades")
            return None
    
    async def run_comprehensive_backtest(self) -> Dict:
        """Run backtest across multiple timeframes"""
        
        print("🚀 COMPREHENSIVE GROWTH MODEL BACKTEST")
        print("=" * 80)
        
        # Define test periods (ending at different recent dates)
        base_end_date = datetime(2024, 11, 30)  # Use data we're confident about
        
        test_configs = [
            {
                'name': '3M',
                'hold_days': 90,
                'start_date': base_end_date - timedelta(days=365),  # 1 year of signals
                'end_date': base_end_date - timedelta(days=90)      # Stop 90 days before end
            },
            {
                'name': '6M', 
                'hold_days': 180,
                'start_date': base_end_date - timedelta(days=400),  
                'end_date': base_end_date - timedelta(days=180)
            },
            {
                'name': '1Y',
                'hold_days': 365,
                'start_date': base_end_date - timedelta(days=500),
                'end_date': base_end_date - timedelta(days=365)
            }
        ]
        
        results = {}
        
        # Run backtests
        for config in test_configs:
            result = await self.backtest_timeframe(
                config['start_date'],
                config['end_date'],
                config['hold_days'],
                config['name']
            )
            
            if result:
                results[config['name']] = result
        
        # Print summary table
        self.print_summary_table(results)
        
        # Generate detailed analysis
        self.analyze_results(results)
        
        return results
    
    def print_summary_table(self, results: Dict):
        """Print summary performance table"""
        
        print(f"\n📊 PERFORMANCE SUMMARY")
        print("=" * 90)
        print(f"{'Timeframe':<10} {'Trades':<7} {'Win Rate':<9} {'Avg Return':<11} {'Volatility':<10} {'Sharpe':<7} {'Best':<8} {'Worst':<8}")
        print("-" * 90)
        
        for timeframe in ['3M', '6M', '1Y']:
            if timeframe in results:
                r = results[timeframe]
                print(f"{timeframe:<10} {r['total_trades']:<7} {r['win_rate']:7.1%} "
                      f"{r['avg_return']:+9.1%} {r['volatility']:8.1%} "
                      f"{r['sharpe_ratio']:5.2f} {r['best_trade']:+6.1%} {r['worst_trade']:+6.1%}")
            else:
                print(f"{timeframe:<10} No data")
        
        print("-" * 90)
    
    def analyze_results(self, results: Dict):
        """Generate detailed analysis and rankings"""
        
        print(f"\n🔍 DETAILED ANALYSIS")
        print("=" * 60)
        
        # Combine all trades for analysis
        all_trades_df = pd.concat([r['trades_df'] for r in results.values()], ignore_index=True)
        
        if len(all_trades_df) == 0:
            print("No trades to analyze")
            return
        
        # 1. Performance by Ranking
        print(f"\n📈 PERFORMANCE BY SIGNAL RANKING:")
        rank_analysis = all_trades_df.groupby('rank').agg({
            'net_return': ['count', 'mean'],
            'success': 'mean',
            'predicted_discount': 'mean'
        }).round(3)
        
        print(f"{'Rank':<6} {'Count':<7} {'Avg Return':<11} {'Win Rate':<9} {'Avg Discount':<12}")
        print("-" * 55)
        for rank in sorted(rank_analysis.index):
            count = int(rank_analysis.loc[rank, ('net_return', 'count')])
            avg_return = rank_analysis.loc[rank, ('net_return', 'mean')]
            win_rate = rank_analysis.loc[rank, ('success', 'mean')]
            avg_discount = rank_analysis.loc[rank, ('predicted_discount', 'mean')]
            
            print(f"{rank:<6} {count:<7} {avg_return:+9.1%} {win_rate:7.1%} {avg_discount:+10.1%}")
        
        # 2. Top Performers
        print(f"\n🏆 TOP 10 TRADES (All Timeframes):")
        top_trades = all_trades_df.nlargest(10, 'net_return')
        
        print(f"{'Symbol':<8} {'Date':<12} {'Timeframe':<10} {'Return':<8} {'Growth':<8} {'Discount':<9}")
        print("-" * 70)
        for _, trade in top_trades.iterrows():
            growth_pct = trade['revenue_growth'] * 100
            print(f"{trade['symbol']:<8} {trade['signal_date']!s:<12} {trade['timeframe']:<10} "
                  f"{trade['net_return']:+6.1%} {growth_pct:+6.1f}% {trade['predicted_discount']:+7.1%}")
        
        # 3. Worst Performers  
        print(f"\n💸 WORST 10 TRADES:")
        worst_trades = all_trades_df.nsmallest(10, 'net_return')
        
        print(f"{'Symbol':<8} {'Date':<12} {'Timeframe':<10} {'Return':<8} {'Growth':<8} {'Discount':<9}")
        print("-" * 70)
        for _, trade in worst_trades.iterrows():
            growth_pct = trade['revenue_growth'] * 100
            print(f"{trade['symbol']:<8} {trade['signal_date']!s:<12} {trade['timeframe']:<10} "
                  f"{trade['net_return']:+6.1%} {growth_pct:+6.1f}% {trade['predicted_discount']:+7.1%}")
        
        # 4. Growth vs Performance Analysis
        print(f"\n📊 GROWTH RATE vs PERFORMANCE:")
        all_trades_df['growth_bucket'] = pd.cut(all_trades_df['revenue_growth'], 
                                               bins=[-1, 0, 0.2, 0.5, 1.0, float('inf')], 
                                               labels=['Negative', '0-20%', '20-50%', '50-100%', '>100%'])
        
        growth_analysis = all_trades_df.groupby('growth_bucket').agg({
            'net_return': ['count', 'mean'],
            'success': 'mean'
        }).round(3)
        
        print(f"{'Growth Range':<12} {'Count':<7} {'Avg Return':<11} {'Win Rate':<9}")
        print("-" * 45)
        for bucket in growth_analysis.index:
            if pd.notna(bucket):
                count = int(growth_analysis.loc[bucket, ('net_return', 'count')])
                avg_return = growth_analysis.loc[bucket, ('net_return', 'mean')]
                win_rate = growth_analysis.loc[bucket, ('success', 'mean')]
                
                print(f"{bucket:<12} {count:<7} {avg_return:+9.1%} {win_rate:7.1%}")
        
        # Save detailed results
        all_trades_df.to_csv('growth_model_backtest_results.csv', index=False)
        print(f"\n💾 Detailed results saved to growth_model_backtest_results.csv")

async def main():
    """Run the comprehensive backtest"""
    
    backtest = GrowthBacktestRunner()
    await backtest.setup()
    
    try:
        results = await backtest.run_comprehensive_backtest()
        
        # Overall summary
        if results:
            print(f"\n🎯 OVERALL ASSESSMENT:")
            
            best_timeframe = max(results.keys(), key=lambda k: results[k]['avg_return'])
            best_performance = results[best_timeframe]
            
            print(f"   Best Timeframe: {best_timeframe} ({best_performance['avg_return']:+.1%} avg return)")
            print(f"   Total Trades Analyzed: {sum(r['total_trades'] for r in results.values())}")
            print(f"   Best Single Trade: {max(r['best_trade'] for r in results.values()):+.1%}")
            
            # Strategy viability
            avg_of_averages = np.mean([r['avg_return'] for r in results.values()])
            print(f"\n   Strategy Average Return: {avg_of_averages:+.1%}")
            
            if avg_of_averages > 0.05:
                print(f"   ✅ Strategy shows STRONG potential!")
            elif avg_of_averages > 0:
                print(f"   ✅ Strategy shows modest potential")
            else:
                print(f"   ❌ Strategy needs improvement")
        
    finally:
        await backtest.cleanup()

if __name__ == "__main__":
    asyncio.run(main())