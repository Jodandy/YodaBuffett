#!/usr/bin/env python3
"""
Backtest the Document Anomaly Strategy.

Tests the combined document intelligence + technical analysis strategy
using real historical data and document anomalies.
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


class DocumentAnomalyBacktester:
    """Backtester for document anomaly strategy with comprehensive performance analysis."""
    
    def __init__(
        self,
        initial_capital: float = 100000,
        transaction_cost: float = 0.001,  # 0.1% per trade
        max_position_size: float = 0.15,  # Max 15% per position
        rebalance_frequency: int = 3      # Rebalance every 3 days
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
        
        # Strategy and indicators
        self.strategy = None
        self.indicator_engine = None
        self.db_conn = None
        
    async def setup(self):
        """Initialize strategy and database connections."""
        print("🚀 Setting up Document Anomaly Backtester...")
        
        # Connect to database
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Setup indicators
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(SMA(period=20))
        indicator_registry.register(VolumeMA(period=20))
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        # Create document anomaly strategy
        self.strategy = DocumentAnomalyStrategy(
            anomaly_lookback_days=30,
            min_anomaly_confidence=0.6,
            require_technical_confirmation=False,  # Test both pure document and combined signals
            min_combined_confidence=0.65,
            rsi_oversold=25,  # More aggressive thresholds for Nordic market
            rsi_overbought=75,
            volume_surge_threshold=1.8
        )
        
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        """Get consistent company ID for symbol."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_market_data_for_symbol(self, symbol: str) -> pd.DataFrame:
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
        
        rows = await self.db_conn.fetch(query, symbol)
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
    
    async def get_available_symbols(self) -> List[str]:
        """Get symbols that have both market data and document data."""
        
        # Get symbols with market data
        market_symbols = await self.db_conn.fetch("""
            SELECT DISTINCT symbol
            FROM daily_price_data
            WHERE date >= $1
        """, date.today() - timedelta(days=365))
        
        market_symbol_set = set(row['symbol'] for row in market_symbols)
        
        # Get companies with document data
        doc_companies = await self.db_conn.fetch("""
            SELECT DISTINCT ed.company_name, COUNT(*) as doc_count
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE se.embedding_model LIKE 'local/%'
            GROUP BY ed.company_name
            ORDER BY doc_count DESC
            LIMIT 50
        """)
        
        print(f"   Found {len(doc_companies)} companies with document data")
        print(f"   Found {len(market_symbol_set)} symbols with market data")
        
        # Expanded mapping for Nordic companies
        company_to_symbol = {
            'Volvo Group': 'VOLV-B.ST',
            'Volvo': 'VOLV-B.ST',
            'Ericsson': 'ERIC-B.ST',
            'Telefonaktiebolaget LM Ericsson': 'ERIC-B.ST',
            'H&M': 'HM-B.ST',
            'Hennes & Mauritz': 'HM-B.ST',
            'SEB': 'SEB-A.ST',
            'Sandvik': 'SAND.ST',
            'Tele2': 'TEL2-B.ST',
            'ASSA ABLOY': 'ASSA-B.ST',
            'SKF': 'SKF-B.ST',
            'Alfa Laval': 'ALFA.ST',
            'Björn Borg': 'BORG.ST',
            'Nordic Semiconductor': 'NOD.OL',
            'VBG Group': 'VBG.ST',
            'Handelsbanken': 'SHB-A.ST',
            'Dometic': 'DOM.ST',
            'XANO Industri': 'XANO.ST',
            # Add more common Nordic companies
            'NIBE': 'NIBE-B.ST',
            'Atlas Copco': 'ATCO-A.ST',
            'Investor': 'INVE-B.ST',
            'ABB': 'ABB.ST'
        }
        
        # Also try direct symbol matching for common patterns
        nordic_suffixes = ['.ST', '.OL', '.HE', '.CO']
        
        available_symbols = []
        matched_companies = []
        
        for company_row in doc_companies:
            company_name = company_row['company_name']
            
            # Method 1: Direct mapping
            for doc_company, symbol in company_to_symbol.items():
                if (doc_company.lower() in company_name.lower() or 
                    company_name.lower() in doc_company.lower()) and symbol in market_symbol_set:
                    if symbol not in available_symbols:
                        available_symbols.append(symbol)
                        matched_companies.append(f"{company_name} → {symbol}")
                        break
            
            # Method 2: Try constructing symbol from company name
            if not any(doc_company.lower() in company_name.lower() for doc_company in company_to_symbol.keys()):
                # Try common symbol patterns
                base_name = company_name.replace(' ', '').replace('AB', '').replace('Group', '')[:4].upper()
                for suffix in nordic_suffixes:
                    potential_symbol = base_name + suffix
                    if potential_symbol in market_symbol_set and potential_symbol not in available_symbols:
                        available_symbols.append(potential_symbol)
                        matched_companies.append(f"{company_name} → {potential_symbol} (constructed)")
                        break
        
        print(f"   Matched companies:")
        for match in matched_companies[:10]:  # Show first 10 matches
            print(f"     {match}")
        
        # If still no matches, use symbols we know work from minimal test
        if not available_symbols:
            # Use symbols confirmed to work from minimal_document_test.py
            working_symbols = ['ERIC-B', 'ABB', 'CINT', 'SAND', 'SINCH']
            
            for symbol in working_symbols:
                if symbol in market_symbol_set:
                    available_symbols.append(symbol)
            
            if available_symbols:
                print(f"   Using confirmed working symbols: {available_symbols}")
            else:
                # Emergency fallback - find any symbols with sufficient data
                emergency_query = await self.db_conn.fetch("""
                    SELECT symbol, COUNT(*) as days
                    FROM daily_price_data 
                    WHERE date >= $1
                    GROUP BY symbol
                    HAVING COUNT(*) >= 100
                    ORDER BY COUNT(*) DESC
                    LIMIT 5
                """, date.today() - timedelta(days=200))
                
                emergency_symbols = [row['symbol'] for row in emergency_query]
                available_symbols.extend(emergency_symbols)
                print(f"   Emergency fallback to symbols with data: {available_symbols}")
        
        return available_symbols
    
    def calculate_position_size(self, signal, current_price: float, portfolio_value: float) -> int:
        """Calculate position size based on signal confidence and risk management."""
        # Base position size
        base_position_value = portfolio_value * self.max_position_size
        
        # Adjust by signal confidence and strength
        confidence_factor = signal.confidence
        strength_factor = min(abs(signal.strength), 2.0) / 2.0
        
        # Extra boost for document-based signals (they're rarer and higher conviction)
        signal_source = signal.contributing_factors.get('signal_source', '')
        if 'document' in signal_source or 'anomaly' in signal_source:
            confidence_factor *= 1.2  # 20% boost for document signals
        
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
        start_date: date,
        end_date: date,
        symbols: Optional[List[str]] = None
    ) -> Dict:
        """Run full backtest of document anomaly strategy."""
        
        print(f"\\n🚀 Running Document Anomaly Strategy Backtest")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        
        # Set up strategy database connection
        await self.strategy.setup_db_connection()
        
        # Get symbols to trade
        if symbols is None:
            symbols = await self.get_available_symbols()
            symbols = symbols[:8]  # Limit for testing
        
        print(f"Trading symbols: {symbols}")
        
        # Load market data
        print(f"\\nLoading market data...")
        market_data_dict = {}
        for symbol in symbols:
            market_data = await self.get_market_data_for_symbol(symbol)
            if not market_data.empty:
                market_data_dict[symbol] = market_data
        
        print(f"Loaded data for {len(market_data_dict)} symbols")
        
        if not market_data_dict:
            print("❌ No market data available for backtesting")
            return {}
        
        # Generate trading dates
        trading_dates = pd.date_range(start_date, end_date, freq='D')
        trading_dates = [d.date() for d in trading_dates]
        
        print(f"\\nStarting backtest simulation...")
        
        signal_breakdown = {'document_only': 0, 'technical_only': 0, 'combined_agreement': 0, 'no_signals': 0}
        
        # Run day-by-day simulation
        for i, current_date in enumerate(trading_dates):
            if i % 15 == 0:  # Progress update every 15 days
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
                
                # Calculate indicators for the required lookback period
                indicator_start = current_date - timedelta(days=60)
                try:
                    indicator_values = await self.indicator_engine.calculate_multiple(
                        ["rsi_14", "sma_20", "volume_sma_20"],
                        company_id,
                        market_data,
                        indicator_start,
                        current_date
                    )
                except Exception as e:
                    continue  # Skip if indicator calculation fails
                
                # Generate signal
                signal = await self.strategy.generate_signal(
                    company_id, market_data, current_date, indicator_values
                )
                
                if signal:
                    daily_signals.append((symbol, signal))
                    
                    # Track signal sources
                    signal_source = signal.contributing_factors.get('signal_source', 'unknown')
                    if signal_source in signal_breakdown:
                        signal_breakdown[signal_source] += 1
                    else:
                        signal_breakdown['no_signals'] += 1
                    
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
                    market_data = market_data_dict[symbol]
                    date_data = market_data.loc[market_data.index.date == current_date]
                    current_price = float(date_data['close'].iloc[-1])
                    
                    current_position = self.positions.get(symbol, 0)
                    signal_source = signal.contributing_factors.get('signal_source', 'unknown')
                    
                    if signal.signal_type.value in ['buy', 'strong_buy']:
                        # Buy signal
                        target_shares = self.calculate_position_size(signal, current_price, portfolio_value)
                        shares_to_buy = target_shares - current_position
                        
                        if shares_to_buy > 0 and self.cash > shares_to_buy * current_price * (1 + self.transaction_cost):
                            self.execute_trade(
                                symbol, shares_to_buy, current_price, current_date,
                                f"{signal.signal_type.value.upper()} ({signal_source}, conf:{signal.confidence:.2f})"
                            )
                    
                    elif signal.signal_type.value in ['sell', 'strong_sell']:
                        # Sell signal
                        if current_position > 0:
                            shares_to_sell = -int(current_position * signal.confidence)  # Partial sell based on confidence
                            if shares_to_sell < 0:
                                self.execute_trade(
                                    symbol, shares_to_sell, current_price, current_date,
                                    f"{signal.signal_type.value.upper()} ({signal_source}, conf:{signal.confidence:.2f})"
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
        results = self.calculate_performance_metrics()
        results['signal_breakdown'] = signal_breakdown
        
        return results
    
    def calculate_performance_metrics(self) -> Dict:
        """Calculate comprehensive backtest performance metrics."""
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
        
        # Trade analysis
        profitable_trades = [t for t in self.trades if (t['shares'] > 0 and self.get_trade_outcome(t) > 0) or (t['shares'] < 0 and self.get_trade_outcome(t) < 0)]
        win_rate = len(profitable_trades) / len(self.trades) if self.trades else 0
        
        # Signal analysis
        signal_types = [s['signal'] for s in self.signals_history]
        signal_sources = [s['source'] for s in self.signals_history]
        avg_confidence = np.mean([s['confidence'] for s in self.signals_history]) if self.signals_history else 0
        
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
            'avg_confidence': avg_confidence,
            'signal_distribution': dict(pd.Series(signal_types).value_counts()),
            'signal_source_distribution': dict(pd.Series(signal_sources).value_counts()),
            'avg_cash': np.mean([h['cash'] for h in self.portfolio_value_history])
        }
    
    def get_trade_outcome(self, trade: Dict) -> float:
        """Get the outcome of a trade (simplified for demo)."""
        # This is a simplified calculation - in practice would track full position lifecycle
        return 0.0  # Placeholder
    
    async def cleanup(self):
        """Clean up resources."""
        if self.strategy:
            await self.strategy.cleanup()
        if self.db_conn:
            await self.db_conn.close()


async def main():
    """Run document anomaly strategy backtest."""
    
    # Create backtester
    backtester = DocumentAnomalyBacktester(
        initial_capital=100000,
        transaction_cost=0.001,
        max_position_size=0.12,
        rebalance_frequency=2
    )
    
    try:
        await backtester.setup()
        
        # Define backtest period (use historical period where we have document data)
        end_date = date(2024, 11, 1)  # Use historical date with known document data
        start_date = date(2024, 8, 1)   # 3-month backtest period
        
        # Run backtest
        results = await backtester.run_backtest(start_date, end_date)
        
        if not results:
            print("❌ No backtest results generated")
            return
        
        # Display results
        print(f"\\n📊 Document Anomaly Strategy Backtest Results")
        print(f"=" * 60)
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
        print(f"Average Signal Confidence: {results['avg_confidence']:.2%}")
        
        # Signal breakdown
        print(f"\\n📡 Signal Analysis:")
        signal_breakdown = results.get('signal_breakdown', {})
        for source, count in signal_breakdown.items():
            print(f"  {source}: {count} signals")
        
        print(f"\\n📈 Signal Distribution:")
        for signal_type, count in results['signal_distribution'].items():
            print(f"  {signal_type}: {count}")
        
        print(f"\\n🎯 Signal Sources:")
        for source, count in results['signal_source_distribution'].items():
            print(f"  {source}: {count}")
        
        # Show sample trades
        print(f"\\n🔍 Sample Trades:")
        for trade in backtester.trades[-8:]:
            print(f"  {trade['date']}: {trade['shares']} {trade['symbol']} @ ${trade['price']:.2f} - {trade['reason']}")
        
        # Show sample signals
        print(f"\\n📡 Sample Signals:")
        for signal in backtester.signals_history[-8:]:
            print(f"  {signal['date']}: {signal['symbol']} {signal['signal'].upper()} "
                  f"(conf:{signal['confidence']:.2f}, source:{signal['source']})")
        
    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backtester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())