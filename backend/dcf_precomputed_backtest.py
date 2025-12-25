#!/usr/bin/env python3
"""
Historical DCF Backtest Using Pre-Computed Valuations

Uses the 606 DCF valuations generated from 2024 financial reports
to test predictive power against actual 2024-2025 price performance.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DCFTrade:
    """DCF trade based on pre-computed valuations."""
    symbol: str
    signal_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    fair_value: float
    implied_return: float
    actual_return: float
    days_held: int
    confidence: float
    success: bool

class PrecomputedDCFBacktest:
    """Historical backtest using pre-computed DCF valuations."""
    
    def __init__(self, 
                 initial_capital: float = 100000,
                 position_size_pct: float = 0.20,  # 20% per position
                 max_positions: int = 5,
                 hold_days: int = 90,              # 3-month holding period
                 min_upside: float = 0.20,         # 20% minimum upside
                 min_confidence: float = 0.30,     # 30% minimum confidence
                 transaction_cost_pct: float = 0.002):  # 0.2% total transaction costs
        
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.min_upside = min_upside
        self.min_confidence = min_confidence
        self.transaction_cost_pct = transaction_cost_pct
        
        # Portfolio tracking
        self.cash = initial_capital
        self.trades: List[DCFTrade] = []
        
        # Database
        self.db_conn = None
        
        print(f"🎯 PRE-COMPUTED DCF BACKTEST SETTINGS:")
        print(f"   Capital: ${initial_capital:,.0f}")
        print(f"   Position Size: {position_size_pct:.0%}")
        print(f"   Hold Period: {hold_days} days")
        print(f"   Min Upside: {min_upside:.0%}")
        print(f"   Min Confidence: {min_confidence:.0%}")
        print(f"   Transaction Cost: {transaction_cost_pct:.1%}")
    
    async def setup(self):
        """Initialize connections."""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        logger.info("Pre-computed DCF backtest initialized")
    
    async def cleanup(self):
        """Close connections."""
        if self.db_conn:
            await self.db_conn.close()
    
    async def get_dcf_signals(self) -> List[Dict]:
        """Get all DCF signals from 2024 that meet our criteria."""
        
        query = """
        SELECT 
            v.symbol,
            v.publish_date,
            v.report_date,
            v.fair_value_stock_median as fair_value,
            v.valuation_confidence,
            -- Calculate entry price (next day after publish)
            p_entry.close_price as entry_price,
            -- Calculate exit price (after holding period)
            p_exit.close_price as exit_price,
            p_entry.date as entry_date,
            p_exit.date as exit_date
        FROM dcf_valuations v
        -- Get entry price (1-5 days after publish date)
        LEFT JOIN daily_price_data p_entry ON v.symbol = p_entry.symbol 
            AND p_entry.date >= v.publish_date + INTERVAL '1 day'
            AND p_entry.date <= v.publish_date + INTERVAL '5 days'
        -- Get exit price (after holding period)
        LEFT JOIN daily_price_data p_exit ON v.symbol = p_exit.symbol 
            AND p_exit.date >= p_entry.date + INTERVAL '%s days'
            AND p_exit.date <= p_entry.date + INTERVAL '%s days'
        WHERE v.model_version = 'clean_dcf_v1.0'
        AND EXTRACT(YEAR FROM v.publish_date) = 2024
        AND v.valuation_confidence >= %s
        AND p_entry.close_price IS NOT NULL
        AND p_exit.close_price IS NOT NULL
        -- Only consider undervalued signals
        AND (v.fair_value_stock_median - p_entry.close_price) / p_entry.close_price >= %s
        ORDER BY v.publish_date, v.symbol
        """ % (self.hold_days - 5, self.hold_days + 5, self.min_confidence, self.min_upside)
        
        rows = await self.db_conn.fetch(query)
        signals = []
        
        for row in rows:
            implied_return = (row['fair_value'] - row['entry_price']) / row['entry_price']
            
            signal = {
                'symbol': row['symbol'],
                'signal_date': row['publish_date'],
                'entry_date': row['entry_date'],
                'exit_date': row['exit_date'],
                'entry_price': float(row['entry_price']),
                'exit_price': float(row['exit_price']),
                'fair_value': float(row['fair_value']),
                'implied_return': implied_return,
                'confidence': float(row['valuation_confidence'])
            }
            
            signals.append(signal)
        
        logger.info(f"Found {len(signals)} qualifying DCF signals from 2024")
        return signals
    
    async def execute_portfolio_backtest(self, signals: List[Dict]) -> List[DCFTrade]:
        """Execute portfolio backtest with position sizing and constraints."""
        
        # Group signals by date to handle multiple signals on same day
        signals_by_date = {}
        for signal in signals:
            signal_date = signal['signal_date']
            if signal_date not in signals_by_date:
                signals_by_date[signal_date] = []
            signals_by_date[signal_date].append(signal)
        
        # Track active positions
        active_positions = []
        all_trades = []
        
        # Process signals chronologically
        for signal_date in sorted(signals_by_date.keys()):
            day_signals = signals_by_date[signal_date]
            
            # Close expired positions first
            positions_to_close = []
            for pos in active_positions:
                if signal_date >= pos['exit_date']:
                    positions_to_close.append(pos)
            
            # Process exits
            for pos in positions_to_close:
                trade = await self.close_position(pos)
                all_trades.append(trade)
                active_positions.remove(pos)
            
            # Sort day's signals by implied return (best first)
            day_signals.sort(key=lambda x: x['implied_return'], reverse=True)
            
            # Open new positions up to limit
            for signal in day_signals:
                if len(active_positions) >= self.max_positions:
                    break
                
                # Check if we have enough cash
                position_value = self.cash * self.position_size_pct
                total_cost = position_value * (1 + self.transaction_cost_pct)
                
                if total_cost <= self.cash:
                    position = {
                        'symbol': signal['symbol'],
                        'entry_date': signal['entry_date'],
                        'exit_date': signal['exit_date'],
                        'entry_price': signal['entry_price'],
                        'exit_price': signal['exit_price'],
                        'fair_value': signal['fair_value'],
                        'implied_return': signal['implied_return'],
                        'confidence': signal['confidence'],
                        'position_value': position_value,
                        'shares': int(position_value / signal['entry_price'])
                    }
                    
                    # Deduct cash
                    self.cash -= total_cost
                    active_positions.append(position)
                    
                    logger.info(f"   📈 {signal['symbol']}: {signal['implied_return']:+.0%} predicted, "
                               f"${position_value:,.0f} position")
        
        # Close any remaining positions
        for pos in active_positions:
            trade = await self.close_position(pos)
            all_trades.append(trade)
        
        return all_trades
    
    async def close_position(self, position: Dict) -> DCFTrade:
        """Close a position and return trade record."""
        
        # Calculate actual return
        gross_return = (position['exit_price'] - position['entry_price']) / position['entry_price']
        
        # Account for transaction costs
        exit_proceeds = position['shares'] * position['exit_price']
        net_proceeds = exit_proceeds * (1 - self.transaction_cost_pct/2)  # Half cost on exit
        
        net_return = (net_proceeds - position['position_value']) / position['position_value']
        
        # Add cash back to portfolio
        self.cash += net_proceeds
        
        # Calculate holding period
        days_held = (position['exit_date'] - position['entry_date']).days
        
        # Create trade record
        trade = DCFTrade(
            symbol=position['symbol'],
            signal_date=position['entry_date'],
            exit_date=position['exit_date'],
            entry_price=position['entry_price'],
            exit_price=position['exit_price'],
            fair_value=position['fair_value'],
            implied_return=position['implied_return'],
            actual_return=net_return,
            days_held=days_held,
            confidence=position['confidence'],
            success=net_return > 0
        )
        
        return trade
    
    def analyze_results(self, trades: List[DCFTrade]):
        """Analyze backtest results."""
        
        if not trades:
            logger.info("No trades to analyze")
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame([
            {
                'symbol': t.symbol,
                'signal_date': t.signal_date,
                'exit_date': t.exit_date,
                'predicted_return': t.implied_return,
                'actual_return': t.actual_return,
                'fair_value': t.fair_value,
                'entry_price': t.entry_price,
                'exit_price': t.exit_price,
                'success': t.success,
                'days_held': t.days_held,
                'confidence': t.confidence
            }
            for t in trades
        ])
        
        # Calculate key metrics
        total_trades = len(df)
        win_rate = df['success'].mean()
        avg_predicted = df['predicted_return'].mean()
        avg_actual = df['actual_return'].mean()
        total_return_pct = df['actual_return'].sum()
        
        final_portfolio = self.cash
        total_portfolio_return = (final_portfolio - self.initial_capital) / self.initial_capital
        
        # Risk metrics
        returns_std = df['actual_return'].std()
        sharpe_ratio = (avg_actual / returns_std) if returns_std > 0 else 0
        max_drawdown = self.calculate_max_drawdown(df)
        
        print(f"\n📊 PRE-COMPUTED DCF BACKTEST RESULTS")
        print(f"=" * 60)
        print(f"📅 Period: 2024 DCF signals → 2024-2025 performance")
        print(f"📈 Total Trades: {total_trades}")
        print(f"🎯 Win Rate: {win_rate:.1%}")
        print(f"💡 Average Predicted Return: {avg_predicted:+.1%}")
        print(f"📊 Average Actual Return: {avg_actual:+.1%}")
        print(f"💰 Portfolio Return: {total_portfolio_return:+.1%}")
        print(f"💵 Final Portfolio Value: ${final_portfolio:,.0f}")
        print(f"📈 Sharpe Ratio: {sharpe_ratio:.2f}")
        print(f"📉 Max Drawdown: {max_drawdown:.1%}")
        
        # Prediction accuracy
        if len(df) > 1:
            correlation = df['predicted_return'].corr(df['actual_return'])
            print(f"🎯 Prediction Correlation: {correlation:.3f}")
            
            # Prediction vs reality scatter
            above_threshold = df[df['predicted_return'] > 0.2]
            if len(above_threshold) > 0:
                high_conviction_win_rate = above_threshold['success'].mean()
                print(f"💪 High Conviction Win Rate (>20% predicted): {high_conviction_win_rate:.1%}")
        
        # Best and worst trades
        print(f"\n🏆 BEST TRADES:")
        best_trades = df.nlargest(5, 'actual_return')
        for _, trade in best_trades.iterrows():
            print(f"   {trade['symbol']} {trade['signal_date']}: "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%} "
                  f"(FV: {trade['fair_value']:.0f} vs Entry: {trade['entry_price']:.0f})")
        
        print(f"\n💸 WORST TRADES:")
        worst_trades = df.nsmallest(5, 'actual_return')
        for _, trade in worst_trades.iterrows():
            print(f"   {trade['symbol']} {trade['signal_date']}: "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%} "
                  f"(FV: {trade['fair_value']:.0f} vs Entry: {trade['entry_price']:.0f})")
        
        # Confidence analysis
        print(f"\n🎯 CONFIDENCE ANALYSIS:")
        high_conf = df[df['confidence'] > 0.5]
        low_conf = df[df['confidence'] <= 0.5]
        
        if len(high_conf) > 0:
            print(f"   High Confidence (>50%): {len(high_conf)} trades, "
                  f"{high_conf['success'].mean():.1%} win rate, "
                  f"{high_conf['actual_return'].mean():+.1%} avg return")
        
        if len(low_conf) > 0:
            print(f"   Low Confidence (≤50%): {len(low_conf)} trades, "
                  f"{low_conf['success'].mean():.1%} win rate, "
                  f"{low_conf['actual_return'].mean():+.1%} avg return")
        
        return df
    
    def calculate_max_drawdown(self, df: pd.DataFrame) -> float:
        """Calculate maximum drawdown."""
        df_sorted = df.sort_values('signal_date')
        cumulative_returns = (1 + df_sorted['actual_return']).cumprod()
        running_max = cumulative_returns.cummax()
        drawdown = (cumulative_returns - running_max) / running_max
        return abs(drawdown.min()) if len(drawdown) > 0 else 0.0

async def main():
    """Run pre-computed DCF backtest."""
    
    backtest = PrecomputedDCFBacktest(
        initial_capital=100000,
        position_size_pct=0.20,    # 20% per position
        hold_days=90,              # 3-month holding period
        min_upside=0.20,           # 20% minimum predicted upside
        min_confidence=0.30        # 30% minimum confidence
    )
    
    await backtest.setup()
    
    try:
        print(f"\n🚀 RUNNING PRE-COMPUTED DCF BACKTEST")
        print(f"Using 606 DCF valuations from 2024 financial reports")
        print(f"Testing against 2024-2025 actual performance")
        
        # Get qualifying DCF signals
        signals = await backtest.get_dcf_signals()
        
        if not signals:
            print("❌ No qualifying DCF signals found")
            return
        
        print(f"✅ Found {len(signals)} qualifying DCF signals")
        print(f"📊 Executing portfolio backtest...")
        
        # Execute portfolio backtest
        trades = await backtest.execute_portfolio_backtest(signals)
        backtest.trades = trades
        
        # Analyze results
        results_df = backtest.analyze_results(trades)
        
        if results_df is not None and len(results_df) > 0:
            # Save detailed results
            results_df.to_csv('dcf_precomputed_backtest.csv', index=False)
            logger.info(f"💾 Detailed results saved to dcf_precomputed_backtest.csv")
        
    finally:
        await backtest.cleanup()

if __name__ == "__main__":
    asyncio.run(main())