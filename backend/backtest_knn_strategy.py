"""
Backtest the KNN strategy using pre-computed neighbors.
Tests realistic performance with time-aware predictions.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import json
import hashlib

from services.technical_analysis.strategies.knn_strategy import KNNStrategy


class KNNBacktester:
    """Backtester for KNN strategy with realistic constraints."""
    
    def __init__(
        self,
        initial_capital: float = 100000,
        transaction_cost: float = 0.001,  # 0.1% per trade
        max_position_size: float = 0.1,   # Max 10% per position
        rebalance_frequency: int = 5      # Rebalance every 5 days
    ):
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.max_position_size = max_position_size
        self.rebalance_frequency = rebalance_frequency
        
        # Portfolio state
        self.cash = initial_capital
        self.positions = {}  # {symbol: shares}
        self.portfolio_value_history = []
        self.trades = []
        self.signals_history = []
        
    def get_company_id(self, symbol: str) -> int:
        """Get consistent company ID for symbol."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_market_data_for_symbol(self, conn, symbol: str) -> pd.DataFrame:
        """Get market data for backtesting."""
        query = """
        SELECT date, 
               open_price as open, 
               high_price as high, 
               low_price as low, 
               close_price as close, 
               volume
        FROM daily_price_data 
        WHERE symbol = $1
        ORDER BY date
        """
        
        rows = await conn.fetch(query, symbol)
        if not rows:
            return pd.DataFrame()
            
        data = [dict(row) for row in rows]
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        return df
    
    async def get_available_symbols(self, conn) -> List[str]:
        """Get symbols that have KNN neighbors data."""
        query = """
        SELECT DISTINCT metadata->>'symbol' as symbol
        FROM ml_labels
        WHERE label_type = 'price_returns'
        AND metadata->>'symbol' IS NOT NULL
        ORDER BY symbol
        """
        
        rows = await conn.fetch(query, )
        return [row['symbol'] for row in rows]
    
    def calculate_position_size(self, signal, current_price: float, portfolio_value: float) -> int:
        """Calculate position size based on signal strength and risk management."""
        # Base position size
        base_position_value = portfolio_value * self.max_position_size
        
        # Adjust by signal confidence and strength
        confidence_factor = signal.confidence
        strength_factor = min(abs(signal.strength), 2.0) / 2.0
        
        adjusted_position_value = base_position_value * confidence_factor * strength_factor
        
        # Convert to shares
        shares = int(adjusted_position_value / current_price)
        
        return max(shares, 0)
    
    def execute_trade(self, symbol: str, shares: int, price: float, trade_date: date, reason: str):
        """Execute a trade and update portfolio."""
        if shares == 0:
            return
            
        trade_value = shares * price
        cost = abs(trade_value) * self.transaction_cost
        
        # Update positions
        current_position = self.positions.get(symbol, 0)
        new_position = current_position + shares
        self.positions[symbol] = new_position
        
        # Update cash
        self.cash -= trade_value + cost
        
        # Record trade
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
        
        print(f"    TRADE: {shares} shares of {symbol} at ${price:.2f} ({reason})")
    
    def calculate_portfolio_value(self, market_data_dict: Dict[str, pd.DataFrame], current_date: date) -> float:
        """Calculate current portfolio value."""
        total_value = self.cash
        
        for symbol, shares in self.positions.items():
            if shares != 0 and symbol in market_data_dict:
                market_data = market_data_dict[symbol]
                # Get price on current date
                date_data = market_data.loc[market_data.index.date == current_date]
                if not date_data.empty:
                    current_price = float(date_data['close'].iloc[-1])
                    position_value = shares * current_price
                    total_value += position_value
        
        return total_value
    
    async def run_backtest(
        self,
        conn: asyncpg.Connection,
        strategy: KNNStrategy,
        start_date: date,
        end_date: date,
        symbols: Optional[List[str]] = None
    ) -> Dict:
        """Run full backtest of KNN strategy."""
        
        print(f"\n🚀 Running KNN Strategy Backtest")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        
        # Set up strategy connection
        await strategy.set_connection(conn)
        
        # Get symbols to trade
        if symbols is None:
            symbols = await self.get_available_symbols(conn)
            symbols = symbols[:5]  # Limit to 5 for testing
            
        print(f"Trading symbols: {symbols}")
        
        # Load market data
        print(f"\nLoading market data...")
        market_data_dict = {}
        for symbol in symbols:
            market_data = await self.get_market_data_for_symbol(conn, symbol)
            if not market_data.empty:
                market_data_dict[symbol] = market_data
        
        print(f"Loaded data for {len(market_data_dict)} symbols")
        
        # Generate trading dates
        trading_dates = pd.date_range(start_date, end_date, freq='D')
        trading_dates = [d.date() for d in trading_dates]
        
        print(f"\nStarting backtest simulation...")
        
        # Run day-by-day simulation
        for i, current_date in enumerate(trading_dates):
            if i % 30 == 0:  # Progress update every 30 days
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                print(f"  {current_date}: Portfolio value ${portfolio_value:,.2f}")
            
            # Generate signals for all symbols
            daily_signals = []
            
            for symbol in symbols:
                if symbol not in market_data_dict:
                    continue
                    
                market_data = market_data_dict[symbol]
                company_id = self.get_company_id(symbol)
                
                # Check if we have data for this date
                date_data = market_data.loc[market_data.index.date == current_date]
                if date_data.empty:
                    continue
                    
                # Generate signal
                signal = await strategy.generate_signal(
                    company_id, market_data, current_date, {}  # No indicators needed for KNN
                )
                
                if signal:
                    daily_signals.append((symbol, signal))
                    self.signals_history.append({
                        'date': current_date,
                        'symbol': symbol,
                        'signal': signal.signal_type.value,
                        'confidence': signal.confidence,
                        'predicted_return': signal.contributing_factors.get('predicted_return', 0)
                    })
            
            # Execute trades based on signals (every N days)
            if i % self.rebalance_frequency == 0 and daily_signals:
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                
                for symbol, signal in daily_signals:
                    market_data = market_data_dict[symbol]
                    date_data = market_data.loc[market_data.index.date == current_date]
                    current_price = float(date_data['close'].iloc[-1])
                    
                    current_position = self.positions.get(symbol, 0)
                    
                    if signal.signal_type in [signal.signal_type.BUY, signal.signal_type.STRONG_BUY]:
                        # Buy signal
                        target_shares = self.calculate_position_size(signal, current_price, portfolio_value)
                        shares_to_buy = target_shares - current_position
                        
                        if shares_to_buy > 0 and self.cash > shares_to_buy * current_price * (1 + self.transaction_cost):
                            self.execute_trade(
                                symbol, shares_to_buy, current_price, current_date,
                                f"{signal.signal_type.value.upper()} (conf:{signal.confidence:.2f})"
                            )
                    
                    elif signal.signal_type in [signal.signal_type.SELL, signal.signal_type.STRONG_SELL]:
                        # Sell signal
                        if current_position > 0:
                            shares_to_sell = -int(current_position * signal.confidence)  # Partial sell based on confidence
                            if shares_to_sell < 0:
                                self.execute_trade(
                                    symbol, shares_to_sell, current_price, current_date,
                                    f"{signal.signal_type.value.upper()} (conf:{signal.confidence:.2f})"
                                )
            
            # Record daily portfolio value
            portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
            self.portfolio_value_history.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'num_positions': len([p for p in self.positions.values() if p != 0])
            })
        
        # Calculate performance metrics
        return self.calculate_performance_metrics()
    
    def calculate_performance_metrics(self) -> Dict:
        """Calculate backtest performance metrics."""
        if not self.portfolio_value_history:
            return {}
            
        # Portfolio value series
        values = [h['portfolio_value'] for h in self.portfolio_value_history]
        dates = [h['date'] for h in self.portfolio_value_history]
        
        # Calculate returns
        portfolio_returns = pd.Series(values).pct_change().dropna()
        
        # Performance metrics
        final_value = values[-1]
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        # Annualized return (assuming daily data)
        days = len(values)
        annualized_return = (final_value / self.initial_capital) ** (365.25 / days) - 1
        
        # Risk metrics
        volatility = portfolio_returns.std() * np.sqrt(252)  # Annualized
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Drawdown
        portfolio_series = pd.Series(values)
        running_max = portfolio_series.cummax()
        drawdown = (portfolio_series - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # Win rate
        winning_trades = [t for t in self.trades if (t['shares'] > 0 and t['trade_value'] > 0) or (t['shares'] < 0 and t['trade_value'] < 0)]
        win_rate = len(winning_trades) / len(self.trades) if self.trades else 0
        
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
            'avg_cash': np.mean([h['cash'] for h in self.portfolio_value_history])
        }


async def main():
    """Run KNN strategy backtest."""
    
    # Connect to database
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Create KNN strategy
        strategy = KNNStrategy(
            model_name="rsi_knn_model",
            prediction_horizon="5d",
            buy_threshold=0.015,   # 1.5% expected return
            sell_threshold=-0.015,
            min_confidence=0.6
        )
        
        # Create backtester
        backtester = KNNBacktester(
            initial_capital=100000,
            transaction_cost=0.001,
            max_position_size=0.15,
            rebalance_frequency=3
        )
        
        # Define backtest period (recent months where we have KNN data)
        end_date = datetime.now().date() - timedelta(days=5)
        start_date = end_date - timedelta(days=60)  # 2-month backtest
        
        # Run backtest
        results = await backtester.run_backtest(
            conn, strategy, start_date, end_date
        )
        
        # Display results
        print(f"\n📊 KNN Strategy Backtest Results")
        print(f"=" * 50)
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${results['initial_capital']:,.2f}")
        print(f"Final Value: ${results['final_value']:,.2f}")
        print(f"Total Return: {results['total_return']:.2%}")
        print(f"Annualized Return: {results['annualized_return']:.2%}")
        print(f"Volatility: {results['volatility']:.2%}")
        print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {results['max_drawdown']:.2%}")
        print(f"Number of Trades: {results['num_trades']}")
        print(f"Win Rate: {results['win_rate']:.2%}")
        print(f"Number of Signals: {results['num_signals']}")
        
        # Show sample trades
        print(f"\n🔍 Sample Trades:")
        for trade in backtester.trades[-5:]:
            print(f"  {trade['date']}: {trade['shares']} {trade['symbol']} @ ${trade['price']:.2f} - {trade['reason']}")
        
        # Show sample signals
        print(f"\n📡 Sample Signals:")
        for signal in backtester.signals_history[-5:]:
            pred_return = signal['predicted_return']
            print(f"  {signal['date']}: {signal['symbol']} {signal['signal'].upper()} "
                  f"(conf:{signal['confidence']:.2f}, pred:{pred_return:.3f})")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())