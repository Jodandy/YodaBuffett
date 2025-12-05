#!/usr/bin/env python3
"""
Scaled Document Anomaly Strategy Backtest.
Tests on more companies and longer historical period.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import json
import hashlib

from services.technical_analysis.strategies.document_anomaly_strategy import DocumentAnomalyStrategy
from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class ScaledDocumentBacktester:
    """Scaled backtester for document anomaly strategy - more companies, longer period."""
    
    def __init__(
        self,
        initial_capital: float = 100000,
        transaction_cost: float = 0.001,
        max_position_size: float = 0.08,  # Smaller positions for more diversification
        rebalance_frequency: int = 3
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
        
        # Strategy and database
        self.strategy = None
        self.indicator_engine = None
        self.db_conn = None
        
    async def setup(self):
        """Initialize with enhanced symbol discovery."""
        print("🚀 Setting up Scaled Document Anomaly Backtester...")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Setup indicators
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(SMA(period=20))
        indicator_registry.register(VolumeMA(period=20))
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        # Create strategy with adjusted parameters for longer backtests
        self.strategy = DocumentAnomalyStrategy(
            anomaly_lookback_days=45,        # Shorter lookback for more signals
            min_anomaly_confidence=0.5,     # Moderate threshold
            anomaly_threshold=0.4,          # Easier to trigger
            require_technical_confirmation=False,  # Allow pure document signals
            rsi_oversold=30,                 # Standard levels
            rsi_overbought=70,
            volume_surge_threshold=1.5
        )
        
        await self.strategy.setup_db_connection()
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def discover_tradeable_symbols(self, min_days: int = 200) -> List[str]:
        """Discover all symbols with sufficient market data."""
        print(f"\n🔍 Discovering symbols with {min_days}+ days of data...")
        
        cutoff_date = date.today() - timedelta(days=min_days + 50)
        
        # Get symbols with good data coverage
        symbols_query = """
            SELECT symbol, COUNT(*) as days, 
                   MIN(date) as first_date, MAX(date) as last_date,
                   AVG(volume::NUMERIC) as avg_volume
            FROM daily_price_data 
            WHERE date >= $1
            AND volume > 0
            AND close_price > 0
            GROUP BY symbol
            HAVING COUNT(*) >= $2
            AND AVG(volume::NUMERIC) > 1000  -- Minimum liquidity
            ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        """
        
        symbol_rows = await self.db_conn.fetch(symbols_query, cutoff_date, min_days)
        
        print(f"   Found {len(symbol_rows)} symbols with sufficient data")
        
        # Filter to reasonable universe size and show details
        tradeable_symbols = []
        for row in symbol_rows[:25]:  # Top 25 by data quality
            symbol = row['symbol']
            days = row['days']
            avg_vol = float(row['avg_volume'])
            
            tradeable_symbols.append(symbol)
            print(f"     {symbol}: {days} days, avg volume {avg_vol:,.0f}")
        
        return tradeable_symbols
    
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
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
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
    
    def calculate_portfolio_value(self, market_data_dict: Dict[str, pd.DataFrame], current_date: date) -> float:
        """Calculate portfolio value with error handling."""
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
                    continue  # Skip problematic positions
        
        return total_value
    
    def execute_trade(self, symbol: str, shares: int, price: float, trade_date: date, reason: str):
        """Execute trade with enhanced logging."""
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
        
        print(f"    📈 {trade_date}: {shares:>4} {symbol:<8} @${price:>6.2f} ({reason[:20]}...)")
    
    async def run_scaled_backtest(
        self, 
        start_date: date, 
        end_date: date,
        max_symbols: int = 15
    ) -> Dict:
        """Run backtest on expanded universe and timeframe."""
        
        print(f"\n🚀 Running SCALED Document Anomaly Strategy Backtest")
        print(f"Period: {start_date} to {end_date} ({(end_date - start_date).days} days)")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Max Symbols: {max_symbols}")
        
        # Discover tradeable symbols
        all_symbols = await self.discover_tradeable_symbols(
            min_days=(end_date - start_date).days + 30
        )
        
        # Limit universe size
        symbols = all_symbols[:max_symbols]
        print(f"\nTrading universe: {symbols}")
        
        # Load market data
        print(f"\n📊 Loading market data...")
        market_data_dict = {}
        
        for i, symbol in enumerate(symbols):
            market_data = await self.get_market_data_for_symbol(symbol, start_date, end_date)
            if not market_data.empty and len(market_data) >= 30:
                market_data_dict[symbol] = market_data
                print(f"   {i+1:2d}. {symbol}: {len(market_data)} days")
            else:
                print(f"   {i+1:2d}. {symbol}: ❌ insufficient data")
        
        if len(market_data_dict) < 3:
            print("❌ Insufficient market data for meaningful backtest")
            return {}
        
        print(f"\n✅ Loaded data for {len(market_data_dict)} symbols")
        
        # Generate trading calendar
        trading_dates = pd.date_range(start_date, end_date, freq='B')  # Business days only
        trading_dates = [d.date() for d in trading_dates]
        
        print(f"\n🔄 Starting simulation over {len(trading_dates)} trading days...")
        
        # Track signal statistics
        signal_stats = {
            'document_only': 0, 'technical_only': 0, 'combined': 0,
            'buy_signals': 0, 'sell_signals': 0, 'total_generated': 0
        }
        
        # Main simulation loop
        for i, current_date in enumerate(trading_dates):
            
            # Progress reporting
            if i % 30 == 0 or i == len(trading_dates) - 1:
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                pct_return = (portfolio_value / self.initial_capital - 1) * 100
                print(f"  {current_date}: Portfolio ${portfolio_value:,.0f} ({pct_return:+.1f}%) - {len([p for p in self.positions.values() if p != 0])} positions")
            
            # Generate signals for all symbols
            daily_signals = []
            
            for symbol in market_data_dict.keys():
                market_data = market_data_dict[symbol]
                company_id = self.get_company_id(symbol)
                
                # Check data availability
                date_data = market_data.loc[market_data.index.date == current_date]
                if date_data.empty:
                    continue
                
                # Calculate indicators
                indicator_start = current_date - timedelta(days=60)
                try:
                    indicator_values = await self.indicator_engine.calculate_multiple(
                        ["rsi_14", "sma_20", "volume_sma_20"],
                        company_id,
                        market_data,
                        indicator_start,
                        current_date
                    )
                except:
                    continue
                
                # Generate signal
                signal = await self.strategy.generate_signal(
                    company_id, market_data, current_date, indicator_values
                )
                
                if signal:
                    daily_signals.append((symbol, signal))
                    
                    # Track signal statistics
                    signal_source = signal.contributing_factors.get('signal_source', 'unknown')
                    if 'document' in signal_source:
                        signal_stats['document_only'] += 1
                    elif 'technical' in signal_source:
                        signal_stats['technical_only'] += 1
                    elif 'combined' in signal_source:
                        signal_stats['combined'] += 1
                    
                    if signal.signal_type.value in ['buy', 'strong_buy']:
                        signal_stats['buy_signals'] += 1
                    elif signal.signal_type.value in ['sell', 'strong_sell']:
                        signal_stats['sell_signals'] += 1
                    
                    signal_stats['total_generated'] += 1
                    
                    self.signals_history.append({
                        'date': current_date,
                        'symbol': symbol,
                        'signal': signal.signal_type.value,
                        'confidence': signal.confidence,
                        'source': signal_source,
                        'strength': signal.strength
                    })
            
            # Execute trades based on signals (every N days)
            if i % self.rebalance_frequency == 0 and daily_signals:
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                
                for symbol, signal in daily_signals:
                    try:
                        market_data = market_data_dict[symbol]
                        date_data = market_data.loc[market_data.index.date == current_date]
                        current_price = float(date_data['close'].iloc[-1])
                        
                        current_position = self.positions.get(symbol, 0)
                        signal_source = signal.contributing_factors.get('signal_source', 'unknown')
                        
                        # Position sizing based on signal confidence
                        max_position_value = portfolio_value * self.max_position_size
                        confidence_adjusted_size = max_position_value * signal.confidence * signal.strength
                        target_shares = int(confidence_adjusted_size / current_price)
                        
                        if signal.signal_type.value in ['buy', 'strong_buy']:
                            shares_to_buy = max(0, target_shares - current_position)
                            
                            if (shares_to_buy > 0 and 
                                self.cash > shares_to_buy * current_price * (1 + self.transaction_cost)):
                                
                                self.execute_trade(
                                    symbol, shares_to_buy, current_price, current_date,
                                    f"{signal.signal_type.value.upper()} {signal_source[:15]} conf:{signal.confidence:.2f}"
                                )
                        
                        elif signal.signal_type.value in ['sell', 'strong_sell']:
                            if current_position > 0:
                                shares_to_sell = -int(current_position * signal.confidence)
                                if shares_to_sell < 0:
                                    self.execute_trade(
                                        symbol, shares_to_sell, current_price, current_date,
                                        f"{signal.signal_type.value.upper()} {signal_source[:15]} conf:{signal.confidence:.2f}"
                                    )
                    except Exception as e:
                        continue  # Skip problematic trades
            
            # Record portfolio value
            portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
            self.portfolio_value_history.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'num_positions': len([p for p in self.positions.values() if p != 0])
            })
        
        # Calculate performance metrics
        results = self.calculate_performance_metrics()
        results['signal_stats'] = signal_stats
        results['symbols_traded'] = list(market_data_dict.keys())
        results['trading_days'] = len(trading_dates)
        
        return results
    
    def calculate_performance_metrics(self) -> Dict:
        """Calculate comprehensive performance metrics."""
        if not self.portfolio_value_history:
            return {}
        
        values = [h['portfolio_value'] for h in self.portfolio_value_history]
        
        # Performance metrics
        final_value = values[-1]
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        # Annualized calculations
        days = len(values)
        years = days / 252.0  # Trading days per year
        annualized_return = (final_value / self.initial_capital) ** (1/years) - 1 if years > 0 else 0
        
        # Risk calculations
        portfolio_returns = pd.Series(values).pct_change().dropna()
        volatility = portfolio_returns.std() * np.sqrt(252)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Drawdown analysis
        portfolio_series = pd.Series(values)
        running_max = portfolio_series.cummax()
        drawdown = (portfolio_series - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # Trade analysis
        winning_trades = sum(1 for t in self.trades if t['shares'] * t['trade_value'] > 0)
        win_rate = winning_trades / len(self.trades) if self.trades else 0
        
        return {
            'initial_capital': self.initial_capital,
            'final_value': final_value,
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'num_trades': len(self.trades),
            'win_rate': win_rate,
            'num_signals': len(self.signals_history),
            'trading_days': len(self.portfolio_value_history),
            'avg_cash': np.mean([h['cash'] for h in self.portfolio_value_history]),
            'max_positions': max(h['num_positions'] for h in self.portfolio_value_history) if self.portfolio_value_history else 0
        }
    
    async def cleanup(self):
        if self.strategy:
            await self.strategy.cleanup()
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run scaled document anomaly backtest."""
    
    backtester = ScaledDocumentBacktester(
        initial_capital=100000,
        transaction_cost=0.001,
        max_position_size=0.08,  # 8% max per position
        rebalance_frequency=2    # Rebalance every 2 days
    )
    
    try:
        await backtester.setup()
        
        # LONGER PERIOD: 6 months instead of 3
        end_date = date(2024, 11, 1)
        start_date = date(2024, 5, 1)  # 6 months
        
        # MORE SYMBOLS: 15 instead of 5
        results = await backtester.run_scaled_backtest(
            start_date, end_date, max_symbols=15
        )
        
        if not results:
            print("❌ No results generated")
            return
        
        # Enhanced results display
        print(f"\n📊 SCALED Document Anomaly Strategy Results")
        print(f"=" * 70)
        print(f"Period: {start_date} to {end_date} ({results.get('trading_days', 0)} trading days)")
        print(f"Universe: {len(results.get('symbols_traded', []))} symbols")
        print(f"")
        print(f"💰 PERFORMANCE:")
        print(f"  Initial Capital:    ${results['initial_capital']:>10,.2f}")
        print(f"  Final Value:        ${results['final_value']:>10,.2f}")
        print(f"  Total Return:       {results['total_return']:>10.2%}")
        print(f"  Annualized Return:  {results['annualized_return']:>10.2%}")
        print(f"  Volatility:         {results['volatility']:>10.2%}")
        print(f"  Sharpe Ratio:       {results['sharpe_ratio']:>10.2f}")
        print(f"  Max Drawdown:       {results['max_drawdown']:>10.2%}")
        print(f"")
        print(f"📊 TRADING ACTIVITY:")
        print(f"  Total Signals:      {results['num_signals']:>10,}")
        print(f"  Total Trades:       {results['num_trades']:>10,}")
        print(f"  Win Rate:           {results['win_rate']:>10.2%}")
        print(f"  Max Positions:      {results.get('max_positions', 0):>10}")
        print(f"  Avg Cash:           ${results.get('avg_cash', 0):>10,.0f}")
        
        # Signal breakdown
        signal_stats = results.get('signal_stats', {})
        if signal_stats.get('total_generated', 0) > 0:
            print(f"")
            print(f"🚨 SIGNAL ANALYSIS:")
            print(f"  Document-only:      {signal_stats.get('document_only', 0):>10}")
            print(f"  Technical-only:     {signal_stats.get('technical_only', 0):>10}")
            print(f"  Combined signals:   {signal_stats.get('combined', 0):>10}")
            print(f"  Buy signals:        {signal_stats.get('buy_signals', 0):>10}")
            print(f"  Sell signals:       {signal_stats.get('sell_signals', 0):>10}")
        
        # Sample trades
        print(f"\n📈 SAMPLE TRADES:")
        for trade in backtester.trades[-8:]:
            pnl = "+" if trade['shares'] > 0 else "-"
            print(f"  {trade['date']} {pnl} {trade['shares']:>4} {trade['symbol']:<8} @${trade['price']:>6.2f} {trade['reason'][:25]}")
        
        # Sample signals  
        print(f"\n📡 RECENT SIGNALS:")
        for signal in backtester.signals_history[-8:]:
            print(f"  {signal['date']} {signal['symbol']:<8} {signal['signal'].upper():<4} conf:{signal['confidence']:>4.2f} ({signal['source'][:15]})")
        
    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await backtester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())