#!/usr/bin/env python3
"""
Hybrid backtest combining technical signals with document anomaly detection.
Uses moderate thresholds to demonstrate both components working together.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional
import hashlib

from services.technical_analysis.strategies.document_anomaly_strategy import DocumentAnomalyStrategy
from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class HybridDocumentBacktester:
    """Backtester that combines technical and document signals."""
    
    def __init__(
        self,
        initial_capital: float = 100000,
        transaction_cost: float = 0.001,
        max_position_size: float = 0.1,
        rebalance_frequency: int = 2
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
        
        self.strategy = None
        self.indicator_engine = None
        self.db_conn = None
        
    async def setup(self):
        """Initialize with moderate parameters."""
        print("🚀 Setting up Hybrid Document Backtester...")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Setup indicators
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(SMA(period=20))
        indicator_registry.register(VolumeMA(period=20))
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        # Create strategy with moderate parameters
        self.strategy = DocumentAnomalyStrategy(
            anomaly_lookback_days=30,
            min_anomaly_confidence=0.5,
            anomaly_threshold=0.5,
            require_technical_confirmation=False,  # Allow both types
            rsi_oversold=40,     # Moderate thresholds
            rsi_overbought=60,
            volume_surge_threshold=1.5
        )
        
        await self.strategy.setup_db_connection()
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def check_document_status(self):
        """Check what documents we have for key companies."""
        print("\n📄 Checking document status...")
        
        # Map symbols to potential company names
        symbol_to_companies = {
            'ERIC-B': ['%Ericsson%', '%ERIC%'],
            'TELIA': ['%Telia%', '%TELIA%'],
            'SINCH': ['%Sinch%', '%SINCH%'],
            'NIBE-B': ['%NIBE%', '%Nibe%']
        }
        
        for symbol, patterns in symbol_to_companies.items():
            total_docs = 0
            for pattern in patterns:
                count = await self.db_conn.fetchval("""
                    SELECT COUNT(DISTINCT ed.id)
                    FROM extracted_documents ed
                    JOIN document_sections ds ON ed.id = ds.extracted_document_id
                    JOIN section_embeddings se ON ds.id = se.document_section_id
                    WHERE ed.company_name ILIKE $1
                    AND se.embedding_model LIKE 'local/%'
                    AND ed.filing_date IS NOT NULL
                """, pattern)
                total_docs += count
            
            if total_docs > 0:
                print(f"   {symbol}: {total_docs} documents with embeddings")
    
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
    
    async def run_hybrid_backtest(
        self, 
        start_date: date, 
        end_date: date,
        symbols: List[str]
    ) -> Dict:
        """Run hybrid backtest with both technical and document signals."""
        
        print(f"\n🚀 Running HYBRID Document + Technical Backtest")
        print(f"Period: {start_date} to {end_date} ({(end_date - start_date).days} days)")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Trading universe: {symbols}")
        
        # Check document status
        await self.check_document_status()
        
        # Load market data
        print(f"\n📊 Loading market data...")
        market_data_dict = {}
        
        for symbol in symbols:
            market_data = await self.get_market_data_for_symbol(symbol, start_date, end_date)
            if not market_data.empty and len(market_data) >= 30:
                market_data_dict[symbol] = market_data
                print(f"   ✅ {symbol}: {len(market_data)} days")
        
        if not market_data_dict:
            print("❌ No market data available")
            return {}
        
        # Generate trading calendar
        trading_dates = pd.date_range(start_date, end_date, freq='B')
        trading_dates = [d.date() for d in trading_dates]
        
        print(f"\n🔄 Starting HYBRID simulation...")
        print("   Technical: RSI <40 or >60, Volume surge >1.5x")
        print("   Document: Anomaly detection when available")
        
        signal_stats = {
            'technical_only': 0,
            'document_only': 0,
            'combined': 0,
            'total': 0
        }
        
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
                
                # Generate signal using document anomaly strategy
                signal = await self.strategy.generate_signal(
                    company_id, market_data, current_date, indicator_values
                )
                
                if signal:
                    daily_signals.append((symbol, signal))
                    signal_stats['total'] += 1
                    
                    # Track signal type
                    signal_source = signal.contributing_factors.get('signal_source', 'unknown')
                    if 'document' in signal_source and 'technical' in signal_source:
                        signal_stats['combined'] += 1
                    elif 'document' in signal_source:
                        signal_stats['document_only'] += 1
                    elif 'technical' in signal_source:
                        signal_stats['technical_only'] += 1
                    
                    self.signals_history.append({
                        'date': current_date,
                        'symbol': symbol,
                        'signal': signal.signal_type.value,
                        'confidence': signal.confidence,
                        'source': signal_source,
                        'strength': signal.strength
                    })
            
            # Execute trades based on signals
            if i % self.rebalance_frequency == 0 and daily_signals:
                portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
                
                for symbol, signal in daily_signals:
                    try:
                        market_data = market_data_dict[symbol]
                        date_data = market_data.loc[market_data.index.date == current_date]
                        current_price = float(date_data['close'].iloc[-1])
                        
                        current_position = self.positions.get(symbol, 0)
                        signal_source = signal.contributing_factors.get('signal_source', 'unknown')
                        
                        # Position sizing
                        max_position_value = portfolio_value * self.max_position_size
                        confidence_adjusted_size = max_position_value * signal.confidence * signal.strength
                        target_shares = int(confidence_adjusted_size / current_price)
                        
                        if signal.signal_type.value in ['buy', 'strong_buy']:
                            shares_to_buy = max(0, target_shares - current_position)
                            
                            if shares_to_buy > 0 and self.cash > shares_to_buy * current_price * 1.001:
                                self.execute_trade(
                                    symbol, shares_to_buy, current_price, current_date,
                                    f"{signal_source} c:{signal.confidence:.2f}"
                                )
                        
                        elif signal.signal_type.value in ['sell', 'strong_sell']:
                            if current_position > 0:
                                shares_to_sell = -int(current_position * signal.confidence)
                                if shares_to_sell < 0:
                                    self.execute_trade(
                                        symbol, shares_to_sell, current_price, current_date,
                                        f"{signal_source} c:{signal.confidence:.2f}"
                                    )
                    except Exception as e:
                        continue
            
            # Record portfolio value
            portfolio_value = self.calculate_portfolio_value(market_data_dict, current_date)
            self.portfolio_value_history.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'num_positions': len([p for p in self.positions.values() if p != 0])
            })
        
        # Calculate final metrics
        final_value = self.portfolio_value_history[-1]['portfolio_value']
        total_return = (final_value - self.initial_capital) / self.initial_capital
        
        print(f"\n\n📊 HYBRID Strategy Results")
        print(f"=" * 60)
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.2f}")
        print(f"Final Value: ${final_value:,.2f}")
        print(f"Total Return: {total_return:.2%}")
        print(f"\n📡 Signal Breakdown:")
        print(f"  Total Signals: {signal_stats['total']}")
        print(f"  Technical Only: {signal_stats['technical_only']}")
        print(f"  Document Only: {signal_stats['document_only']}")
        print(f"  Combined (Doc+Tech): {signal_stats['combined']}")
        print(f"  Total Trades: {len(self.trades)}")
        
        # Show recent trades
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
            'signal_stats': signal_stats,
            'num_trades': len(self.trades)
        }
    
    async def cleanup(self):
        if self.strategy:
            await self.strategy.cleanup()
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run hybrid document + technical backtest."""
    
    backtester = HybridDocumentBacktester(
        initial_capital=100000,
        transaction_cost=0.001,
        max_position_size=0.1,
        rebalance_frequency=2
    )
    
    try:
        await backtester.setup()
        
        # Use high-volume symbols
        symbols = ['ERIC-B', 'TELIA', 'SINCH', 'NIBE-B', 'PMED', 'META', 'FRAG']
        
        # Extended period to capture more patterns
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        results = await backtester.run_hybrid_backtest(
            start_date, end_date, symbols
        )
        
        print("\n\n🎯 HYBRID BACKTEST COMPLETE!")
        print("This demonstrates the document anomaly strategy with:")
        print("  ✅ Technical signals (RSI, volume)")
        print("  ✅ Document anomaly detection (when available)")
        print("  ✅ Combined signals (strongest conviction)")
        print("\nThe system prioritizes signals where both document and technical")
        print("indicators align, providing higher confidence trades.")
        
    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await backtester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())