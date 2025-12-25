#!/usr/bin/env python3
"""
Quick DCF Portfolio Simulator

Simplified version for rapid testing of DCF signals:
- Focuses on signal quality validation
- Lightweight backtesting for strategy validation
- Quick performance metrics
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json

from clean_dcf_engine import CleanDCFEngine, DCFConfig

@dataclass
class QuickTrade:
    symbol: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    fair_value: float
    implied_return: float
    actual_return: float
    days_held: int
    success: bool

class QuickDCFSimulator:
    """Lightweight DCF signal validator."""
    
    def __init__(self, hold_days: int = 30, min_return: float = 0.15):
        self.hold_days = hold_days
        self.min_return = min_return
        self.dcf_engine = CleanDCFEngine(DCFConfig(num_simulations=500))
        self.db_conn = None
        self.trades = []
    
    async def setup(self):
        """Initialize connections."""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        await self.dcf_engine.setup()
    
    async def cleanup(self):
        """Close connections."""
        if self.db_conn:
            await self.db_conn.close()
        await self.dcf_engine.cleanup()
    
    async def get_test_universe(self) -> List[str]:
        """Get reliable test companies."""
        query = """
        SELECT DISTINCT symbol
        FROM financial_statements
        WHERE symbol IN ('AAK', 'ABB', 'VOLV-B', 'ERIC-B', 'SEB-A', 'SWED-A')
        AND total_revenue > 0
        ORDER BY symbol
        """
        
        rows = await self.db_conn.fetch(query)
        return [row['symbol'] for row in rows]
    
    async def get_price(self, symbol: str, target_date: date) -> Optional[float]:
        """Get price for a specific date (with tolerance)."""
        query = """
        SELECT close_price
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date DESC
        LIMIT 1
        """
        
        start_date = target_date - timedelta(days=5)
        end_date = target_date + timedelta(days=5)
        
        row = await self.db_conn.fetchrow(query, symbol, start_date, end_date)
        return float(row['close_price']) if row else None
    
    async def test_dcf_signal(self, symbol: str, test_date: datetime) -> Optional[QuickTrade]:
        """Test a single DCF signal."""
        
        # Get entry price
        entry_price = await self.get_price(symbol, test_date.date())
        if not entry_price:
            return None
        
        # Generate DCF signal
        try:
            dcf_result = await self.dcf_engine.value_company(symbol, test_date, entry_price)
            if not dcf_result:
                return None
            
            implied_return = dcf_result['implied_return']
            
            # Only test undervalued signals
            if implied_return < self.min_return:
                return None
            
            # Get exit price
            exit_date = test_date.date() + timedelta(days=self.hold_days)
            exit_price = await self.get_price(symbol, exit_date)
            if not exit_price:
                return None
            
            # Calculate actual return
            actual_return = (exit_price - entry_price) / entry_price
            
            trade = QuickTrade(
                symbol=symbol,
                entry_date=test_date.date(),
                exit_date=exit_date,
                entry_price=entry_price,
                exit_price=exit_price,
                fair_value=dcf_result['fair_value_median'],
                implied_return=implied_return,
                actual_return=actual_return,
                days_held=self.hold_days,
                success=actual_return > 0
            )
            
            return trade
            
        except Exception as e:
            print(f"   Error testing {symbol}: {e}")
            return None
    
    async def run_quick_test(self, start_date: str, end_date: str):
        """Run quick DCF validation test."""
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        universe = await self.get_test_universe()
        
        print(f"🧪 Quick DCF Signal Test")
        print(f"   Period: {start_date} to {end_date}")
        print(f"   Universe: {universe}")
        print(f"   Hold Period: {self.hold_days} days")
        print(f"   Min Return Threshold: {self.min_return:.0%}")
        
        # Test monthly
        current_date = start_dt
        month_count = 0
        
        while current_date <= end_dt:
            month_count += 1
            print(f"\n📅 Testing {current_date.strftime('%Y-%m')}")
            
            # Test each symbol
            month_trades = []
            for symbol in universe:
                trade = await self.test_dcf_signal(symbol, current_date)
                if trade:
                    month_trades.append(trade)
                    self.trades.append(trade)
                    
                    print(f"   {symbol}: Fair=${trade.fair_value:.0f}, Entry=${trade.entry_price:.0f}, "
                          f"Predicted={trade.implied_return:+.0%}, Actual={trade.actual_return:+.0%}")
            
            if month_trades:
                month_win_rate = sum(1 for t in month_trades if t.success) / len(month_trades)
                month_avg_return = np.mean([t.actual_return for t in month_trades])
                print(f"   Month Summary: {len(month_trades)} trades, {month_win_rate:.0%} win rate, {month_avg_return:+.1%} avg return")
            
            # Move to next month
            current_date = current_date.replace(day=1)
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        # Final results
        self.print_results()
        
        return self.trades
    
    def print_results(self):
        """Print comprehensive test results."""
        
        if not self.trades:
            print("\n❌ No trades executed")
            return
        
        # Basic stats
        total_trades = len(self.trades)
        winning_trades = sum(1 for t in self.trades if t.success)
        win_rate = winning_trades / total_trades
        
        returns = [t.actual_return for t in self.trades]
        avg_return = np.mean(returns)
        median_return = np.median(returns)
        
        predicted_returns = [t.implied_return for t in self.trades]
        avg_predicted = np.mean(predicted_returns)
        
        print(f"\n📊 DCF SIGNAL VALIDATION RESULTS")
        print(f"   Total Signals Tested: {total_trades}")
        print(f"   Win Rate: {win_rate:.1%}")
        print(f"   Average Actual Return: {avg_return:+.1%}")
        print(f"   Median Actual Return: {median_return:+.1%}")
        print(f"   Average Predicted Return: {avg_predicted:+.1%}")
        
        # Correlation analysis
        if len(returns) > 5:
            correlation = np.corrcoef(predicted_returns, returns)[0, 1]
            print(f"   Prediction Correlation: {correlation:.2f}")
        
        # Performance by symbol
        symbol_performance = {}
        for trade in self.trades:
            if trade.symbol not in symbol_performance:
                symbol_performance[trade.symbol] = []
            symbol_performance[trade.symbol].append(trade)
        
        print(f"\n🏢 PERFORMANCE BY COMPANY:")
        for symbol, trades in symbol_performance.items():
            symbol_win_rate = sum(1 for t in trades if t.success) / len(trades)
            symbol_avg_return = np.mean([t.actual_return for t in trades])
            print(f"   {symbol}: {len(trades)} trades, {symbol_win_rate:.0%} win rate, {symbol_avg_return:+.1%} avg return")
        
        # Best/worst trades
        best_trades = sorted(self.trades, key=lambda x: x.actual_return, reverse=True)[:3]
        worst_trades = sorted(self.trades, key=lambda x: x.actual_return)[:3]
        
        print(f"\n🏆 BEST TRADES:")
        for trade in best_trades:
            print(f"   {trade.symbol} {trade.entry_date}: Predicted {trade.implied_return:+.0%}, Actual {trade.actual_return:+.0%}")
        
        print(f"\n💸 WORST TRADES:")
        for trade in worst_trades:
            print(f"   {trade.symbol} {trade.entry_date}: Predicted {trade.implied_return:+.0%}, Actual {trade.actual_return:+.0%}")

async def main():
    """Run quick DCF test."""
    
    simulator = QuickDCFSimulator(hold_days=30, min_return=0.15)
    await simulator.setup()
    
    try:
        # Test 1-year period
        trades = await simulator.run_quick_test('2023-01-01', '2024-01-01')
        
        # Save results
        if trades:
            results_data = []
            for trade in trades:
                results_data.append({
                    'symbol': trade.symbol,
                    'entry_date': trade.entry_date,
                    'exit_date': trade.exit_date,
                    'entry_price': trade.entry_price,
                    'exit_price': trade.exit_price,
                    'fair_value': trade.fair_value,
                    'implied_return': trade.implied_return,
                    'actual_return': trade.actual_return,
                    'days_held': trade.days_held,
                    'success': trade.success
                })
            
            results_df = pd.DataFrame(results_data)
            results_df.to_csv('dcf_quick_test_results.csv', index=False)
            print(f"\n💾 Results saved to dcf_quick_test_results.csv")
    
    finally:
        await simulator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())