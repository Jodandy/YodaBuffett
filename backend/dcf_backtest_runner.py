#!/usr/bin/env python3
"""
Clean DCF Strategy Backtest Runner

Implements realistic portfolio management for DCF-based value investing:
- Quarterly rebalancing (aligned with earnings releases)
- Position sizing based on conviction (implied return magnitude)
- Portfolio concentration limits and cash management
- Realistic transaction costs and holding periods
- Value-focused investment approach

Uses the CleanDCFEngine for actual fundamental valuations.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from collections import defaultdict
import json
import logging

from clean_dcf_engine import CleanDCFEngine, DCFConfig

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

@dataclass
class DCFPosition:
    """Represents an open DCF-based position."""
    symbol: str
    entry_date: date
    entry_price: float
    shares: int
    position_value: float
    fair_value: float
    implied_return: float
    valuation_signal: str
    confidence: float
    quarters_held: int = 0

@dataclass
class DCFTrade:
    """Represents a completed DCF-based trade."""
    symbol: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    shares: int
    position_value: float
    gross_return: float
    net_return: float
    transaction_costs: float
    days_held: int
    quarters_held: int
    fair_value_entry: float
    implied_return_entry: float
    valuation_signal: str
    confidence: float
    success: bool

@dataclass
class DCFSignal:
    """Represents a DCF buy/sell signal."""
    symbol: str
    date: date
    signal_type: str  # 'BUY', 'SELL', 'HOLD'
    fair_value: float
    market_price: float
    implied_return: float
    valuation_signal: str
    confidence: float
    priority_score: float

class DCFBacktester:
    """DCF-based value investing backtest with realistic portfolio management."""
    
    def __init__(self, 
                 initial_capital: float = 500000,
                 position_size_pct: float = 0.15,  # 15% per position
                 max_positions: int = 8,           # Focus on best opportunities
                 rebalance_frequency: int = 90,    # Quarterly rebalancing
                 min_implied_return: float = 0.20, # 20% minimum upside for entry
                 transaction_cost_pct: float = 0.002, # 0.2% per side
                 min_holding_days: int = 30):      # Minimum 1 month holding
        
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.max_positions = max_positions
        self.rebalance_frequency = rebalance_frequency
        self.min_implied_return = min_implied_return
        self.transaction_cost_pct = transaction_cost_pct
        self.min_holding_days = min_holding_days
        
        # Portfolio state
        self.cash = initial_capital
        self.positions: List[DCFPosition] = []
        self.completed_trades: List[DCFTrade] = []
        self.signals_history: List[DCFSignal] = []
        
        # DCF engine
        self.dcf_config = DCFConfig(num_simulations=1000)  # Faster for backtesting
        self.dcf_engine = CleanDCFEngine(self.dcf_config)
        
        # Database connection
        self.db_conn = None
        
        print(f"🏦 DCF Value Investing Backtest Settings:")
        print(f"   Initial Capital: ${initial_capital:,.0f}")
        print(f"   Position Size: {position_size_pct:.0%} per position")
        print(f"   Max Positions: {max_positions} companies")
        print(f"   Rebalancing: Every {rebalance_frequency} days (quarterly)")
        print(f"   Min Upside: {min_implied_return:.0%} for entry")
        print(f"   Transaction Cost: {transaction_cost_pct:.1%} per side")
        print(f"   Min Holding: {min_holding_days} days")
    
    async def setup(self):
        """Initialize database connections."""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        await self.dcf_engine.setup()
    
    async def cleanup(self):
        """Close database connections."""
        if self.db_conn:
            await self.db_conn.close()
        await self.dcf_engine.cleanup()
    
    async def get_fundamentals_universe(self) -> List[str]:
        """Get symbols with sufficient fundamental data for DCF analysis."""
        query = """
        SELECT DISTINCT f.symbol
        FROM financial_statements f
        JOIN balance_sheet_data b ON f.symbol = b.symbol
        JOIN cash_flow_data c ON f.symbol = c.symbol
        JOIN daily_price_data p ON f.symbol = p.symbol
        WHERE f.total_revenue > 0
        AND f.publish_date IS NOT NULL
        GROUP BY f.symbol
        HAVING COUNT(DISTINCT f.publish_date) >= 4  -- At least 4 periods of data
        ORDER BY f.symbol
        """
        
        rows = await self.db_conn.fetch(query)
        symbols = [row['symbol'] for row in rows]
        
        print(f"📊 Found {len(symbols)} companies with sufficient fundamental data")
        return symbols
    
    async def get_price_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get historical price data for a symbol."""
        query = """
        SELECT date, close_price as close, volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        if not rows:
            return pd.DataFrame()
        
        data = [{'date': row['date'], 'close': float(row['close']), 'volume': int(row['volume'])} 
                for row in rows]
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        return df
    
    def calculate_confidence(self, dcf_result: dict) -> float:
        """Calculate confidence score based on DCF result quality."""
        
        # Factors for confidence:
        # 1. Consistency of fair value range (narrow = high confidence)
        # 2. Magnitude of implied return
        # 3. Quality of fundamental data
        
        fair_value_std = dcf_result['fair_value_std']
        fair_value_mean = dcf_result['fair_value_median']
        implied_return = abs(dcf_result['implied_return'])
        
        # Coefficient of variation (lower = more consistent = higher confidence)
        cv = fair_value_std / fair_value_mean if fair_value_mean > 0 else 1.0
        consistency_score = max(0, 1 - min(cv, 1.0))
        
        # Return magnitude score (higher returns = higher confidence up to a point)
        magnitude_score = min(implied_return / 0.5, 1.0)  # Cap at 50% return
        
        # Overall confidence (0 to 1)
        confidence = (consistency_score * 0.7) + (magnitude_score * 0.3)
        
        return max(0.1, min(1.0, confidence))
    
    async def generate_dcf_signals(self, symbols: List[str], valuation_date: datetime) -> List[DCFSignal]:
        """Generate DCF-based buy/sell signals for a given date."""
        
        signals = []
        
        for symbol in symbols:
            try:
                # Get market price on valuation date
                price_data = await self.get_price_data(symbol, valuation_date.date(), valuation_date.date())
                if price_data.empty:
                    continue
                
                market_price = price_data.iloc[0]['close']
                
                # Run DCF valuation
                dcf_result = await self.dcf_engine.value_company(symbol, valuation_date, market_price)
                
                if not dcf_result:
                    continue
                
                # Calculate confidence
                confidence = self.calculate_confidence(dcf_result)
                
                # Determine signal type
                implied_return = dcf_result['implied_return']
                valuation_signal = dcf_result['valuation_signal']
                
                if implied_return >= self.min_implied_return and valuation_signal == 'UNDERVALUED':
                    signal_type = 'BUY'
                elif implied_return <= -0.1 and valuation_signal == 'OVERVALUED':
                    signal_type = 'SELL'
                else:
                    signal_type = 'HOLD'
                
                # Priority score combines return magnitude and confidence
                priority_score = abs(implied_return) * confidence
                
                signal = DCFSignal(
                    symbol=symbol,
                    date=valuation_date.date(),
                    signal_type=signal_type,
                    fair_value=dcf_result['fair_value_median'],
                    market_price=market_price,
                    implied_return=implied_return,
                    valuation_signal=valuation_signal,
                    confidence=confidence,
                    priority_score=priority_score
                )
                
                signals.append(signal)
                
            except Exception as e:
                logger.warning(f"Error generating DCF signal for {symbol}: {e}")
                continue
        
        # Sort by priority score (best opportunities first)
        signals.sort(key=lambda x: x.priority_score, reverse=True)
        
        return signals
    
    def get_portfolio_value(self, current_date: date) -> float:
        """Calculate current portfolio value."""
        return self.cash + sum(pos.position_value for pos in self.positions)
    
    def calculate_position_size(self, signal: DCFSignal, portfolio_value: float) -> int:
        """Calculate position size based on signal strength and portfolio constraints."""
        
        # Base position size
        base_position_value = portfolio_value * self.position_size_pct
        
        # Adjust by confidence and implied return magnitude
        confidence_factor = signal.confidence
        return_factor = min(abs(signal.implied_return) / 0.3, 1.5)  # Cap at 1.5x for extreme returns
        
        adjusted_position_value = base_position_value * confidence_factor * return_factor
        
        # Ensure we don't exceed available cash (with buffer for transaction costs)
        max_position_value = self.cash * 0.95
        position_value = min(adjusted_position_value, max_position_value)
        
        # Convert to shares
        shares = int(position_value / signal.market_price)
        
        return max(shares, 0)
    
    async def execute_rebalancing(self, current_date: datetime, signals: List[DCFSignal]):
        """Execute portfolio rebalancing based on DCF signals."""
        
        portfolio_value = self.get_portfolio_value(current_date.date())
        
        print(f"\n📅 {current_date.date()} - Portfolio Rebalancing")
        print(f"   Portfolio Value: ${portfolio_value:,.0f}")
        print(f"   Cash: ${self.cash:,.0f}")
        print(f"   Positions: {len(self.positions)}")
        
        # Process sell signals first (close overvalued positions)
        sell_signals = [s for s in signals if s.signal_type == 'SELL']
        positions_to_close = []
        
        for position in self.positions:
            # Find corresponding signal
            sell_signal = next((s for s in sell_signals if s.symbol == position.symbol), None)
            
            days_held = (current_date.date() - position.entry_date).days
            
            # Close position if: overvalued, or held too long without upside
            if (sell_signal or 
                days_held > 365 or  # Max 1 year holding
                (days_held > self.min_holding_days and position.implied_return < 0.05)):  # Low upside
                
                positions_to_close.append(position)
        
        # Execute sell orders
        for position in positions_to_close:
            await self.close_position(position, current_date)
        
        # Process buy signals (open new undervalued positions)
        buy_signals = [s for s in signals if s.signal_type == 'BUY']
        available_slots = self.max_positions - len(self.positions)
        
        for i, signal in enumerate(buy_signals[:available_slots]):
            # Check if we already have this position
            existing_position = next((p for p in self.positions if p.symbol == signal.symbol), None)
            if existing_position:
                continue
            
            # Calculate position size
            shares = self.calculate_position_size(signal, portfolio_value)
            
            if shares > 0:
                await self.open_position(signal, shares, current_date)
    
    async def open_position(self, signal: DCFSignal, shares: int, current_date: datetime):
        """Open a new DCF-based position."""
        
        position_value = shares * signal.market_price
        transaction_cost = position_value * self.transaction_cost_pct
        total_cost = position_value + transaction_cost
        
        if total_cost > self.cash:
            print(f"   ❌ Insufficient cash for {signal.symbol}: ${total_cost:,.0f} needed, ${self.cash:,.0f} available")
            return
        
        # Create position
        position = DCFPosition(
            symbol=signal.symbol,
            entry_date=current_date.date(),
            entry_price=signal.market_price,
            shares=shares,
            position_value=position_value,
            fair_value=signal.fair_value,
            implied_return=signal.implied_return,
            valuation_signal=signal.valuation_signal,
            confidence=signal.confidence
        )
        
        # Update portfolio
        self.positions.append(position)
        self.cash -= total_cost
        
        print(f"   🟢 BUY {signal.symbol}: {shares:,} shares @ ${signal.market_price:.2f}")
        print(f"      Fair Value: ${signal.fair_value:.2f} (Upside: {signal.implied_return:+.0%})")
        print(f"      Confidence: {signal.confidence:.1%}, Cost: ${total_cost:,.0f}")
    
    async def close_position(self, position: DCFPosition, current_date: datetime):
        """Close an existing position."""
        
        # Get current price
        price_data = await self.get_price_data(position.symbol, current_date.date(), current_date.date())
        if price_data.empty:
            print(f"   ❌ No price data for {position.symbol} on {current_date.date()}")
            return
        
        exit_price = price_data.iloc[0]['close']
        gross_proceeds = position.shares * exit_price
        transaction_cost = gross_proceeds * self.transaction_cost_pct
        net_proceeds = gross_proceeds - transaction_cost
        
        # Calculate returns
        gross_return = (exit_price - position.entry_price) / position.entry_price
        net_return = (net_proceeds - position.position_value) / position.position_value
        
        days_held = (current_date.date() - position.entry_date).days
        quarters_held = position.quarters_held
        
        # Create trade record
        trade = DCFTrade(
            symbol=position.symbol,
            entry_date=position.entry_date,
            exit_date=current_date.date(),
            entry_price=position.entry_price,
            exit_price=exit_price,
            shares=position.shares,
            position_value=position.position_value,
            gross_return=gross_return,
            net_return=net_return,
            transaction_costs=transaction_cost + (position.position_value * self.transaction_cost_pct),
            days_held=days_held,
            quarters_held=quarters_held,
            fair_value_entry=position.fair_value,
            implied_return_entry=position.implied_return,
            valuation_signal=position.valuation_signal,
            confidence=position.confidence,
            success=net_return > 0
        )
        
        # Update portfolio
        self.positions = [p for p in self.positions if p.symbol != position.symbol]
        self.cash += net_proceeds
        self.completed_trades.append(trade)
        
        print(f"   🔴 SELL {position.symbol}: {position.shares:,} shares @ ${exit_price:.2f}")
        print(f"      Return: {net_return:+.0%}, Held: {days_held} days, Proceeds: ${net_proceeds:,.0f}")
    
    async def run_backtest(self, start_date: str, end_date: str):
        """Run complete DCF backtest."""
        
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Get universe
        universe = await self.get_fundamentals_universe()
        universe = universe[:50]  # Limit for testing
        
        print(f"\n🚀 DCF Value Investing Backtest")
        print(f"   Period: {start_date} to {end_date}")
        print(f"   Universe: {len(universe)} companies")
        print(f"   Strategy: Quarterly DCF rebalancing")
        
        # Portfolio tracking
        portfolio_history = []
        
        current_date = start_dt
        rebalance_count = 0
        
        while current_date <= end_dt:
            # Quarterly rebalancing
            if (current_date - start_dt).days % self.rebalance_frequency == 0:
                rebalance_count += 1
                
                # Generate signals
                signals = await self.generate_dcf_signals(universe, current_date)
                
                # Execute rebalancing
                await self.execute_rebalancing(current_date, signals)
                
                # Store signals for analysis
                self.signals_history.extend(signals)
            
            # Update position values and portfolio tracking
            portfolio_value = self.get_portfolio_value(current_date.date())
            portfolio_history.append({
                'date': current_date.date(),
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'positions': len(self.positions)
            })
            
            # Increment quarter count for positions
            if (current_date - start_dt).days % 90 == 0:
                for position in self.positions:
                    position.quarters_held += 1
            
            current_date += timedelta(days=self.rebalance_frequency)
        
        # Close all remaining positions at end
        final_signals = await self.generate_dcf_signals(universe, end_dt)
        for position in self.positions.copy():
            await self.close_position(position, end_dt)
        
        # Calculate performance
        final_value = self.cash
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        print(f"\n📈 BACKTEST RESULTS")
        print(f"   Rebalancing Events: {rebalance_count}")
        print(f"   Total Trades: {len(self.completed_trades)}")
        print(f"   Final Portfolio Value: ${final_value:,.0f}")
        print(f"   Total Return: {total_return:+.1%}")
        
        # Analyze trades
        if self.completed_trades:
            winning_trades = [t for t in self.completed_trades if t.success]
            win_rate = len(winning_trades) / len(self.completed_trades)
            avg_return = np.mean([t.net_return for t in self.completed_trades])
            avg_holding = np.mean([t.days_held for t in self.completed_trades])
            
            print(f"\n📊 TRADE ANALYSIS")
            print(f"   Win Rate: {win_rate:.1%}")
            print(f"   Average Return per Trade: {avg_return:+.1%}")
            print(f"   Average Holding Period: {avg_holding:.0f} days")
            
            # Top performing trades
            best_trades = sorted(self.completed_trades, key=lambda x: x.net_return, reverse=True)[:5]
            print(f"\n🏆 TOP TRADES:")
            for trade in best_trades:
                print(f"   {trade.symbol}: {trade.net_return:+.1%} ({trade.days_held} days)")
        
        return portfolio_history

async def main():
    """Run DCF backtest."""
    
    backtest = DCFBacktester(
        initial_capital=500000,
        position_size_pct=0.15,
        max_positions=8,
        min_implied_return=0.20,
        rebalance_frequency=90
    )
    
    await backtest.setup()
    
    try:
        # Run 2-year backtest
        portfolio_history = await backtest.run_backtest('2022-01-01', '2024-01-01')
        
        # Save results
        results_df = pd.DataFrame(portfolio_history)
        results_df.to_csv('dcf_backtest_results.csv', index=False)
        print(f"\n💾 Results saved to dcf_backtest_results.csv")
        
    finally:
        await backtest.cleanup()

if __name__ == "__main__":
    asyncio.run(main())