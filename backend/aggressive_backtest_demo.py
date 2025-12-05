#!/usr/bin/env python3
"""
Aggressive demo backtest that will generate signals for demonstration.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import random
import hashlib

from services.technical_analysis.strategies.document_anomaly_strategy import DocumentAnomalyStrategy
from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class AggressiveDemoBacktester:
    """Demo backtester with very aggressive parameters to ensure signal generation."""
    
    def __init__(
        self,
        initial_capital: float = 100000,
        transaction_cost: float = 0.001,
        max_position_size: float = 0.1,
        rebalance_frequency: int = 1  # Daily rebalancing
    ):
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.max_position_size = max_position_size
        self.rebalance_frequency = rebalance_frequency
        
        # Portfolio state
        self.cash = initial_capital
        self.positions = {}
        self.portfolio_value_history = []
        self.trades = []
        self.signals_history = []
        
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        print("🚀 Setting up Aggressive Demo Backtester...")
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_market_data_for_symbol(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data for specific period."""
        query = """
        SELECT date, 
               open_price as open, 
               high_price as high, 
               low_price as low, 
               close_price as close, 
               volume
        FROM daily_price_data 
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date - timedelta(days=60), end_date)
        if not rows:
            return pd.DataFrame()
        
        data = [dict(row) for row in rows]
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.dropna()
    
    def calculate_simple_signals(self, market_data: pd.DataFrame, current_date: date) -> Optional[Dict]:
        """Generate simple technical signals for demo."""
        
        # Get recent data
        recent_data = market_data[market_data.index.date <= current_date].tail(20)
        
        if len(recent_data) < 14:
            return None
        
        # Calculate simple RSI
        closes = recent_data['close'].values
        deltas = np.diff(closes)
        gains = deltas[deltas > 0]
        losses = -deltas[deltas < 0]
        
        avg_gain = np.mean(gains) if len(gains) > 0 else 0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0
        
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 100 if avg_gain > 0 else 50
        
        # Calculate price momentum
        if len(recent_data) >= 5:
            momentum = (closes[-1] - closes[-5]) / closes[-5]
        else:
            momentum = 0
        
        # Volume surge check
        recent_volume = recent_data['volume'].values
        avg_volume = np.mean(recent_volume[:-1]) if len(recent_volume) > 1 else recent_volume[0]
        volume_ratio = recent_volume[-1] / avg_volume if avg_volume > 0 else 1
        
        # Generate signals based on multiple factors
        signal = None
        confidence = 0
        
        # Strong oversold
        if rsi < 35:
            signal = 'buy'
            confidence = 0.7 + (35 - rsi) / 100
        # Moderate oversold with volume
        elif rsi < 45 and volume_ratio > 1.3:
            signal = 'buy'
            confidence = 0.6
        # Strong overbought
        elif rsi > 65:
            signal = 'sell'
            confidence = 0.7 + (rsi - 65) / 100
        # Moderate overbought with volume
        elif rsi > 55 and volume_ratio > 1.3:
            signal = 'sell'
            confidence = 0.6
        # Momentum signals
        elif momentum < -0.1:  # 10% drop
            signal = 'buy'
            confidence = 0.65
        elif momentum > 0.15:  # 15% gain
            signal = 'sell'
            confidence = 0.65
        
        if signal:
            return {
                'signal': signal,
                'confidence': min(confidence, 0.9),
                'rsi': rsi,
                'momentum': momentum,
                'volume_ratio': volume_ratio
            }
        
        return None
    
    def calculate_portfolio_value(self, market_data_dict: Dict[str, pd.DataFrame], current_date: date) -> float:
        """Calculate current portfolio value."""
        total_value = self.cash
        
        for symbol, shares in self.positions.items():
            if shares != 0 and symbol in market_data_dict:
                try:
                    market_data = market_data_dict[symbol]
                    date_data = market_data.loc[market_data.index.date == current_date]
                    if not date_data.empty:
                        current_price = float(date_data['close'].iloc[-1])
                        position_value = shares * current_price
                        total_value += position_value
                except:
                    continue
        
        return total_value
    
    def execute_trade(self, symbol: str, shares: int, price: float, trade_date: date, reason: str):
        """Execute trade."""
        if shares == 0:
            return
        
        trade_value = shares * price
        cost = abs(trade_value) * self.transaction_cost
        
        current_position = self.positions.get(symbol, 0)
        new_position = current_position + shares
        self.positions[symbol] = new_position
        
        self.cash -= trade_value + cost
        
        self.trades.append({
            'date': trade_date,
            'symbol': symbol,
            'shares': shares,
            'price': price,
            'trade_value': trade_value,
            'cost': cost,
            'reason': reason,
            'new_position': new_position
        })
        
        action = "BUY" if shares > 0 else "SELL"
        print(f"    📈 {trade_date}: {action} {abs(shares):>4} {symbol:<8} @${price:>6.2f} ({reason})")
    
    async def run_aggressive_demo(
        self, 
        start_date: date, 
        end_date: date,
        symbols: List[str]
    ) -> Dict:
        """Run aggressive demo backtest."""
        
        print(f"\n🚀 Running AGGRESSIVE DEMO Backtest")
        print(f"Period: {start_date} to {end_date} ({(end_date - start_date).days} days)")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Trading universe: {symbols}")
        
        # Load market data
        print(f"\n📊 Loading market data...")
        market_data_dict = {}
        
        for symbol in symbols:
            market_data = await self.get_market_data_for_symbol(symbol, start_date, end_date)
            if not market_data.empty and len(market_data) >= 20:
                market_data_dict[symbol] = market_data
                print(f"   ✅ {symbol}: {len(market_data)} days")
        
        if not market_data_dict:
            print("❌ No market data available")
            return {}
        
        # Generate trading calendar
        trading_dates = pd.date_range(start_date, end_date, freq='B')
        trading_dates = [d.date() for d in trading_dates]
        
        print(f"\n🔄 Starting AGGRESSIVE simulation...")
        print("   Using relaxed thresholds: RSI <45 or >55, Momentum ±10%")
        
        signal_count = 0
        
        # Main simulation loop
        for i, current_date in enumerate(trading_dates):
            
            # Progress reporting
            if i % 20 == 0:
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                pct_return = (portfolio_value / self.initial_capital - 1) * 100
                active_positions = len([p for p in self.positions.values() if p != 0])
                print(f"\n📅 {current_date}: Portfolio ${portfolio_value:,.0f} ({pct_return:+.1f}%) - {active_positions} positions")
            
            # Generate signals for all symbols
            daily_signals = []
            
            for symbol in market_data_dict.keys():
                market_data = market_data_dict[symbol]
                
                # Check data availability
                if current_date not in [d.date() for d in market_data.index]:
                    continue
                
                # Generate simple technical signals
                signal_data = self.calculate_simple_signals(market_data, current_date)
                
                if signal_data:
                    daily_signals.append((symbol, signal_data))
                    signal_count += 1
                    
                    self.signals_history.append({
                        'date': current_date,
                        'symbol': symbol,
                        'signal': signal_data['signal'],
                        'confidence': signal_data['confidence'],
                        'rsi': signal_data['rsi']
                    })
            
            # Execute trades on signals
            if daily_signals:
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                
                for symbol, signal_data in daily_signals:
                    market_data = market_data_dict[symbol]
                    date_data = market_data.loc[market_data.index.date == current_date]
                    current_price = float(date_data['close'].iloc[-1])
                    
                    current_position = self.positions.get(symbol, 0)
                    
                    # Position sizing
                    max_position_value = portfolio_value * self.max_position_size
                    confidence_adjusted_size = max_position_value * signal_data['confidence']
                    target_shares = int(confidence_adjusted_size / current_price)
                    
                    if signal_data['signal'] == 'buy':
                        shares_to_buy = max(0, target_shares - current_position)
                        
                        if shares_to_buy > 0 and self.cash > shares_to_buy * current_price * 1.001:
                            reason = f"RSI:{signal_data['rsi']:.0f} Mom:{signal_data['momentum']:.1%}"
                            self.execute_trade(symbol, shares_to_buy, current_price, current_date, reason)
                    
                    elif signal_data['signal'] == 'sell' and current_position > 0:
                        shares_to_sell = -min(current_position, int(current_position * signal_data['confidence']))
                        
                        if shares_to_sell < 0:
                            reason = f"RSI:{signal_data['rsi']:.0f} Mom:{signal_data['momentum']:.1%}"
                            self.execute_trade(symbol, shares_to_sell, current_price, current_date, reason)
            
            # Record portfolio value
            portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
            self.portfolio_value_history.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'num_positions': len([p for p in self.positions.values() if p != 0])
            })
        
        # Final reporting
        final_value = self.portfolio_value_history[-1]['portfolio_value']
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        print(f"\n\n📊 AGGRESSIVE DEMO Results")
        print(f"=" * 60)
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Final Value: ${final_value:,.2f}")
        print(f"Total Return: {total_return:.2%}")
        print(f"Total Signals Generated: {signal_count}")
        print(f"Total Trades Executed: {len(self.trades)}")
        
        # Show sample trades
        if self.trades:
            print(f"\n📈 Recent Trades:")
            for trade in self.trades[-10:]:
                action = "BUY " if trade['shares'] > 0 else "SELL"
                print(f"  {trade['date']} {action} {abs(trade['shares']):>4} {trade['symbol']:<8} "
                      f"@${trade['price']:>6.2f} ({trade['reason']})")
        
        return {
            'initial_capital': self.initial_capital,
            'final_value': final_value,
            'total_return': total_return,
            'num_signals': signal_count,
            'num_trades': len(self.trades)
        }
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run aggressive demo backtest."""
    
    backtester = AggressiveDemoBacktester(
        initial_capital=100000,
        transaction_cost=0.001,
        max_position_size=0.1,
        rebalance_frequency=1
    )
    
    try:
        await backtester.setup()
        
        # Use specific high-volume symbols we know work
        demo_symbols = ['ERIC-B', 'TELIA', 'SINCH', 'NIBE-B', 'PMED']
        
        # Shorter demo period
        end_date = date(2024, 8, 31)
        start_date = date(2024, 6, 1)
        
        results = await backtester.run_aggressive_demo(
            start_date, end_date, demo_symbols
        )
        
        print("\n\n🎯 DEMO COMPLETE!")
        print("This aggressive demo shows how the system works with relaxed parameters.")
        print("The production strategy uses more conservative thresholds and includes:")
        print("  - Document anomaly detection (requires filing dates)")
        print("  - Technical confirmation requirements")
        print("  - Risk management controls")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await backtester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())