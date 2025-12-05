#!/usr/bin/env python3
"""
Realistic Portfolio Simulator with Position Sizing

Addresses the issue of unrealistic returns by implementing:
- 20% position sizing per trade (max 5 positions)
- Portfolio cash management
- Concurrent trade arbitration
- 3-day fixed holding period for simplicity
- Realistic transaction costs and constraints

Based on multi_horizon_indicator_tester.py but with proper portfolio management.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, NamedTuple
import json
from collections import defaultdict
import hashlib
import math
from dataclasses import dataclass

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, EMA, VolumeMA, MACD, BollingerBands

@dataclass
class Position:
    """Represents an open position in the portfolio."""
    symbol: str
    entry_date: date
    entry_price: float
    shares: int
    position_value: float
    indicator_name: str
    indicator_value: float
    expected_return: float
    confidence: float

@dataclass
class Trade:
    """Represents a completed trade."""
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
    indicator_name: str
    indicator_value: float
    expected_return: float
    confidence: float
    success: bool

@dataclass
class Signal:
    """Represents a buy signal."""
    symbol: str
    date: date
    indicator_name: str
    indicator_value: float
    expected_return: float
    confidence: float
    priority_score: float

class RealisticPortfolioSimulator:
    """Portfolio simulator with realistic position sizing and constraints."""
    
    def __init__(self, initial_capital: float = 100000, position_size_pct: float = 0.10, 
                 max_positions: int = 10, hold_days: int = 3, transaction_cost_pct: float = 0.001):
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.transaction_cost_pct = transaction_cost_pct
        
        # Portfolio state
        self.cash = initial_capital
        self.positions: List[Position] = []
        self.completed_trades: List[Trade] = []
        
        # Database and indicators
        self.db_conn = None
        self.indicator_engine = None
        
        print(f"💼 Portfolio Simulator Settings:")
        print(f"   Initial Capital: ${initial_capital:,.0f}")
        print(f"   Position Size: {position_size_pct:.0%} per trade")
        print(f"   Max Positions: {max_positions}")
        print(f"   Holding Period: {hold_days} days (fixed)")
        print(f"   Transaction Cost: {transaction_cost_pct:.1%} per side ({transaction_cost_pct * 2:.1%} total)")
        
    async def setup(self):
        """Initialize database and indicators."""
        print(f"\n🔧 Setting up Portfolio Simulator...")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register EMA 10 indicator (best performer from previous tests)
        indicators_to_test = [
            ('ema_10', EMA(period=10))
        ]
        
        for name, indicator in indicators_to_test:
            indicator_registry.register(indicator)
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        print(f"✅ Database connected")
        print(f"✅ Registered EMA 10 indicator")
        print(f"✅ Setup complete!")
        
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date, limit: int = 50) -> List[str]:
        """Get quality stocks with sufficient data."""
        query = """
        SELECT symbol, 
               COUNT(*) as days,
               AVG(volume::NUMERIC) as avg_volume,
               AVG(close_price::NUMERIC) as avg_price,
               MIN(close_price::NUMERIC) as min_price
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2
        AND volume > 0
        AND close_price > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 200  -- Need lots of data
        AND MIN(close_price::NUMERIC) >= 1.0  -- Exclude penny stocks
        AND AVG(close_price::NUMERIC) >= 5.0  -- Focus on higher-priced stocks
        ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        LIMIT 100
        """
        
        rows = await self.db_conn.fetch(query, start_date - timedelta(days=300), end_date)
        
        # Additional filtering for portfolio-suitable stocks
        quality_symbols = []
        for row in rows:
            if (float(row['min_price']) >= 2.0 and 
                float(row['avg_price']) >= 5.0 and
                float(row['avg_volume']) >= 100000):  # Good liquidity
                quality_symbols.append(row['symbol'])
        
        symbols = quality_symbols[:limit]
        
        print(f"📊 Selected {len(symbols)} portfolio-suitable stocks")
        print(f"   Sample: {symbols[:5]}...")
        
        return symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data with buffer for indicators."""
        buffer_start = start_date - timedelta(days=100)
        
        query = """
        SELECT date, 
               open_price::NUMERIC as open, 
               high_price::NUMERIC as high,
               low_price::NUMERIC as low, 
               close_price::NUMERIC as close, 
               volume::BIGINT as volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, buffer_start, end_date + timedelta(days=10))
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        return df.dropna()
    
    async def generate_signals(self, symbols: List[str], start_date: date, end_date: date) -> List[Signal]:
        """Generate all buy signals for the test period."""
        print(f"🔍 Generating EMA 10 signals for {len(symbols)} stocks...")
        
        all_signals = []
        
        for i, symbol in enumerate(symbols, 1):
            print(f"   Processing {symbol} ({i}/{len(symbols)})...")
            
            try:
                market_data = await self.get_market_data(symbol, start_date, end_date)
                if market_data.empty or len(market_data) < 50:
                    continue
                
                company_id = self.get_company_id(symbol)
                
                # Calculate EMA 10
                indicator_values = await self.indicator_engine.calculate_multiple(
                    ['ema_10'], company_id, market_data, start_date - timedelta(days=50), end_date
                )
                
                if 'ema_10' not in indicator_values:
                    continue
                
                result = indicator_values['ema_10']
                if not hasattr(result, 'values') or not result.values:
                    continue
                
                # Convert to DataFrame
                indicator_df = pd.DataFrame(list(result.values.items()), columns=['date', 'ema_10'])
                indicator_df['date'] = pd.to_datetime(indicator_df['date'])
                indicator_df.set_index('date', inplace=True)
                
                # Generate signals
                analysis_data = market_data[(market_data.index.date >= start_date) & 
                                          (market_data.index.date <= end_date)]
                
                for timestamp, row in analysis_data.iterrows():
                    if timestamp not in indicator_df.index:
                        continue
                    
                    ema_value = indicator_df.loc[timestamp, 'ema_10']
                    if pd.isna(ema_value):
                        continue
                    
                    current_price = row['close']
                    if current_price < 1.0 or ema_value <= 0:
                        continue
                    
                    # Calculate price to EMA ratio
                    price_ema_ratio = current_price / ema_value
                    
                    # Simple signal: buy when price is 1-2% above EMA (momentum but not overbought)
                    if 1.01 <= price_ema_ratio <= 1.03:
                        # Calculate expected return using simple mean reversion
                        # When price is slightly above EMA, expect some continuation
                        expected_return = 0.02  # 2% expected return
                        confidence = 0.7  # 70% confidence
                        
                        # Priority score for arbitration (higher = better signal)
                        priority_score = confidence * expected_return
                        
                        signal = Signal(
                            symbol=symbol,
                            date=timestamp.date(),
                            indicator_name='ema_10',
                            indicator_value=price_ema_ratio,
                            expected_return=expected_return,
                            confidence=confidence,
                            priority_score=priority_score
                        )
                        
                        all_signals.append(signal)
                        
            except Exception as e:
                print(f"      ❌ Error processing {symbol}: {e}")
                continue
        
        # Sort signals by date
        all_signals.sort(key=lambda x: x.date)
        
        print(f"✅ Generated {len(all_signals)} signals")
        return all_signals
    
    def get_portfolio_value(self) -> float:
        """Calculate current portfolio value (cash + positions)."""
        position_value = sum(pos.position_value for pos in self.positions)
        return self.cash + position_value
    
    def arbitrate_signals(self, signals_today: List[Signal]) -> List[Signal]:
        """Select best signals when we have multiple opportunities."""
        if len(signals_today) <= self.max_positions - len(self.positions):
            return signals_today  # Can take all signals
        
        # Sort by priority score and take top signals
        available_slots = self.max_positions - len(self.positions)
        sorted_signals = sorted(signals_today, key=lambda x: x.priority_score, reverse=True)
        return sorted_signals[:available_slots]
    
    async def get_entry_price(self, symbol: str, signal_date: date) -> Optional[float]:
        """Get next day's opening price for realistic entry."""
        try:
            next_date = signal_date + timedelta(days=1)
            
            query = """
            SELECT open_price::NUMERIC as open
            FROM daily_price_data
            WHERE symbol = $1 AND date = $2
            """
            
            row = await self.db_conn.fetchrow(query, symbol, next_date)
            if row and row['open']:
                return float(row['open'])
            
            # If no exact match, try next trading day
            for i in range(2, 5):  # Try up to 4 days ahead for weekends/holidays
                check_date = signal_date + timedelta(days=i)
                row = await self.db_conn.fetchrow(query, symbol, check_date)
                if row and row['open']:
                    return float(row['open'])
            
            return None
            
        except Exception:
            return None
    
    async def get_exit_price(self, symbol: str, exit_date: date) -> Optional[float]:
        """Get exit price (close on exit date)."""
        try:
            query = """
            SELECT close_price::NUMERIC as close
            FROM daily_price_data
            WHERE symbol = $1 AND date = $2
            """
            
            row = await self.db_conn.fetchrow(query, symbol, exit_date)
            if row and row['close']:
                return float(row['close'])
            
            return None
            
        except Exception:
            return None
    
    async def process_signals_for_date(self, signals_today: List[Signal]) -> int:
        """Process all signals for a given date."""
        if not signals_today:
            return 0
        
        # Arbitrate if we have too many signals
        selected_signals = self.arbitrate_signals(signals_today)
        
        positions_opened = 0
        
        for signal in selected_signals:
            # Check if we have enough cash
            position_value = self.cash * self.position_size_pct
            if position_value < 1000:  # Minimum $1000 position
                continue
            
            # Get entry price
            entry_price = await self.get_entry_price(signal.symbol, signal.date)
            if not entry_price or entry_price <= 0:
                continue
            
            # Calculate shares (accounting for transaction costs)
            transaction_cost = position_value * self.transaction_cost_pct
            net_investment = position_value - transaction_cost
            shares = int(net_investment / entry_price)
            
            if shares <= 0:
                continue
            
            # Adjust position value to actual shares purchased
            actual_position_value = shares * entry_price + transaction_cost
            
            # Check if we still have enough cash
            if actual_position_value > self.cash:
                continue
            
            # Create position
            position = Position(
                symbol=signal.symbol,
                entry_date=signal.date,
                entry_price=entry_price,
                shares=shares,
                position_value=shares * entry_price,  # Market value
                indicator_name=signal.indicator_name,
                indicator_value=signal.indicator_value,
                expected_return=signal.expected_return,
                confidence=signal.confidence
            )
            
            # Update portfolio
            self.cash -= actual_position_value
            self.positions.append(position)
            positions_opened += 1
            
        return positions_opened
    
    async def close_positions_for_date(self, current_date: date) -> int:
        """Close positions that have reached holding period."""
        positions_closed = 0
        positions_to_remove = []
        
        for i, position in enumerate(self.positions):
            days_held = (current_date - position.entry_date).days
            
            if days_held >= self.hold_days:
                # Time to close this position
                exit_price = await self.get_exit_price(position.symbol, current_date)
                
                if exit_price and exit_price > 0:
                    # Calculate trade results
                    gross_proceeds = position.shares * exit_price
                    transaction_cost = gross_proceeds * self.transaction_cost_pct
                    net_proceeds = gross_proceeds - transaction_cost
                    
                    # Calculate returns
                    total_cost = position.shares * position.entry_price + (position.position_value * self.transaction_cost_pct)
                    gross_return = (gross_proceeds / total_cost - 1) if total_cost > 0 else 0
                    net_return = (net_proceeds / total_cost - 1) if total_cost > 0 else 0
                    
                    # Create trade record
                    trade = Trade(
                        symbol=position.symbol,
                        entry_date=position.entry_date,
                        exit_date=current_date,
                        entry_price=position.entry_price,
                        exit_price=exit_price,
                        shares=position.shares,
                        position_value=position.position_value,
                        gross_return=gross_return,
                        net_return=net_return,
                        transaction_costs=transaction_cost + (position.position_value * self.transaction_cost_pct),
                        days_held=days_held,
                        indicator_name=position.indicator_name,
                        indicator_value=position.indicator_value,
                        expected_return=position.expected_return,
                        confidence=position.confidence,
                        success=(net_return > 0)
                    )
                    
                    # Update portfolio
                    self.cash += net_proceeds
                    self.completed_trades.append(trade)
                    positions_to_remove.append(i)
                    positions_closed += 1
        
        # Remove closed positions (reverse order to maintain indices)
        for i in reversed(positions_to_remove):
            self.positions.pop(i)
        
        return positions_closed
    
    async def run_simulation(self, symbols: List[str], start_date: date, end_date: date) -> Dict:
        """Run the complete portfolio simulation."""
        print(f"\n📈 Running Portfolio Simulation")
        print(f"   Period: {start_date} to {end_date}")
        print(f"   Universe: {len(symbols)} stocks")
        
        # Generate all signals
        all_signals = await self.generate_signals(symbols, start_date, end_date)
        
        if not all_signals:
            return {'error': 'No signals generated'}
        
        # Group signals by date
        signals_by_date = defaultdict(list)
        for signal in all_signals:
            signals_by_date[signal.date].append(signal)
        
        print(f"\n🎯 Simulating Daily Trading...")
        
        # Simulate day by day
        current_date = start_date
        simulation_days = 0
        
        while current_date <= end_date:
            simulation_days += 1
            
            # Close any positions that have reached holding period
            positions_closed = await self.close_positions_for_date(current_date)
            
            # Process new signals for today
            signals_today = signals_by_date.get(current_date, [])
            positions_opened = await self.process_signals_for_date(signals_today)
            
            # Progress update
            if simulation_days % 30 == 0 or positions_opened > 0 or positions_closed > 0:
                portfolio_value = self.get_portfolio_value()
                print(f"   📅 {current_date}: Portfolio=${portfolio_value:,.0f}, "
                      f"Cash=${self.cash:,.0f}, Positions={len(self.positions)}, "
                      f"Opened={positions_opened}, Closed={positions_closed}")
            
            current_date += timedelta(days=1)
        
        # Force close any remaining positions at end
        for position in self.positions[:]:
            exit_price = await self.get_exit_price(position.symbol, end_date)
            
            if exit_price and exit_price > 0:
                gross_proceeds = position.shares * exit_price
                transaction_cost = gross_proceeds * self.transaction_cost_pct
                net_proceeds = gross_proceeds - transaction_cost
                
                total_cost = position.shares * position.entry_price + (position.position_value * self.transaction_cost_pct)
                net_return = (net_proceeds / total_cost - 1) if total_cost > 0 else 0
                
                trade = Trade(
                    symbol=position.symbol,
                    entry_date=position.entry_date,
                    exit_date=end_date,
                    entry_price=position.entry_price,
                    exit_price=exit_price,
                    shares=position.shares,
                    position_value=position.position_value,
                    gross_return=(gross_proceeds / total_cost - 1) if total_cost > 0 else 0,
                    net_return=net_return,
                    transaction_costs=transaction_cost + (position.position_value * self.transaction_cost_pct),
                    days_held=(end_date - position.entry_date).days,
                    indicator_name=position.indicator_name,
                    indicator_value=position.indicator_value,
                    expected_return=position.expected_return,
                    confidence=position.confidence,
                    success=(net_return > 0)
                )
                
                self.cash += net_proceeds
                self.completed_trades.append(trade)
        
        self.positions.clear()
        
        return self.analyze_performance()
    
    def analyze_performance(self) -> Dict:
        """Analyze portfolio performance."""
        if not self.completed_trades:
            return {'error': 'No trades completed'}
        
        total_trades = len(self.completed_trades)
        winning_trades = sum(1 for t in self.completed_trades if t.success)
        win_rate = winning_trades / total_trades
        
        net_returns = [t.net_return for t in self.completed_trades]
        avg_return_per_trade = np.mean(net_returns)
        
        # Portfolio performance
        final_portfolio_value = self.cash
        total_return = (final_portfolio_value / self.initial_capital - 1)
        
        # Transaction costs
        total_transaction_costs = sum(t.transaction_costs for t in self.completed_trades)
        
        # Best and worst trades
        best_trade = max(self.completed_trades, key=lambda x: x.net_return)
        worst_trade = min(self.completed_trades, key=lambda x: x.net_return)
        
        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return_per_trade': avg_return_per_trade,
            'total_return': total_return,
            'initial_capital': self.initial_capital,
            'final_portfolio_value': final_portfolio_value,
            'total_transaction_costs': total_transaction_costs,
            'best_trade': {
                'symbol': best_trade.symbol,
                'return': best_trade.net_return,
                'entry_date': best_trade.entry_date,
                'exit_date': best_trade.exit_date
            },
            'worst_trade': {
                'symbol': worst_trade.symbol,
                'return': worst_trade.net_return,
                'entry_date': worst_trade.entry_date,
                'exit_date': worst_trade.exit_date
            }
        }
    
    def display_results(self, results: Dict):
        """Display comprehensive portfolio results."""
        print(f"\n🏆 REALISTIC PORTFOLIO SIMULATION RESULTS")
        print(f"=" * 80)
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
        
        print(f"💼 PORTFOLIO PERFORMANCE:")
        print(f"   Initial Capital: ${results['initial_capital']:,.0f}")
        print(f"   Final Portfolio Value: ${results['final_portfolio_value']:,.0f}")
        print(f"   Total Return: {results['total_return']:+.1%}")
        print(f"   Annualized Return: {results['total_return'] * 365/90:+.1%}")  # Assuming ~3 month test
        
        print(f"\n📊 TRADING STATISTICS:")
        print(f"   Total Trades: {results['total_trades']}")
        print(f"   Win Rate: {results['win_rate']:.1%}")
        print(f"   Avg Return per Trade: {results['avg_return_per_trade']:+.2%}")
        print(f"   Total Transaction Costs: ${results['total_transaction_costs']:,.0f}")
        
        print(f"\n🎯 TRADE HIGHLIGHTS:")
        print(f"   Best Trade: {results['best_trade']['symbol']} "
              f"({results['best_trade']['entry_date']} → {results['best_trade']['exit_date']}) "
              f"{results['best_trade']['return']:+.1%}")
        print(f"   Worst Trade: {results['worst_trade']['symbol']} "
              f"({results['worst_trade']['entry_date']} → {results['worst_trade']['exit_date']}) "
              f"{results['worst_trade']['return']:+.1%}")
        
        # Sample recent trades
        if self.completed_trades:
            print(f"\n📋 RECENT TRADES (Last 10):")
            for trade in self.completed_trades[-10:]:
                print(f"   {trade.entry_date} → {trade.exit_date}: {trade.symbol} "
                      f"{trade.net_return:+.1%} ({'✅' if trade.success else '❌'})")
        
        # Portfolio analysis
        if results['total_return'] > 0:
            vs_holding_cash = results['total_return']
            print(f"\n💡 PORTFOLIO INSIGHTS:")
            print(f"   Strategy beat holding cash by {vs_holding_cash:+.1%}")
            print(f"   Position size limit ({self.position_size_pct:.0%}) provided risk control")
            print(f"   Max {self.max_positions} concurrent positions managed complexity")
            print(f"   {self.hold_days}-day holding period provided discipline")
        else:
            print(f"\n⚠️ PORTFOLIO INSIGHTS:")
            print(f"   Strategy underperformed holding cash by {abs(results['total_return']):.1%}")
            print(f"   Transaction costs: ${results['total_transaction_costs']:,.0f}")
            print(f"   Consider longer holding periods or better signal selection")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run realistic portfolio simulation."""
    
    # Portfolio configuration
    simulator = RealisticPortfolioSimulator(
        initial_capital=100000,     # $100,000 starting capital
        position_size_pct=0.20,     # 20% per position
        max_positions=5,            # Max 5 concurrent positions
        hold_days=3,                # 3-day holding period
        transaction_cost_pct=0.001  # 0.1% per side
    )
    
    try:
        await simulator.setup()
        
        # Test period (shorter for realistic testing)
        end_date = date(2024, 11, 15)
        start_date = date(2024, 8, 1)  # ~3 months
        
        # Get stock universe
        symbols = await simulator.get_stock_universe(start_date, end_date, limit=30)
        
        if len(symbols) < 10:
            print("❌ Insufficient stocks for portfolio simulation")
            return
        
        # Run simulation
        results = await simulator.run_simulation(symbols, start_date, end_date)
        
        # Display results
        simulator.display_results(results)
        
        print(f"\n✅ REALISTIC PORTFOLIO SIMULATION COMPLETE!")
        print(f"This shows actual achievable returns with proper position sizing,")
        print(f"portfolio constraints, and realistic trading implementation.")
        
    except Exception as e:
        print(f"❌ Simulation failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await simulator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())