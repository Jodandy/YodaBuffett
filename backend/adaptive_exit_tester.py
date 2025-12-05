#!/usr/bin/env python3
"""
Adaptive Exit Strategy with KNN

Instead of fixed exit dates, use KNN to determine optimal exit timing.
Two KNN models:
1. Entry KNN: When to enter trades
2. Exit KNN: When to exit trades (daily evaluation while holding)

This should lead to more realistic returns and better risk management.
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

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, EMA, VolumeMA, MACD, BollingerBands

class TradingPosition(NamedTuple):
    """An open trading position."""
    entry_date: date
    entry_price: float
    symbol: str
    indicator_name: str
    indicator_value_at_entry: float
    predicted_direction: str
    confidence: float
    expected_return: float
    max_hold_days: int = 20  # Maximum days to hold before forced exit

class AdaptiveExitTester:
    """Tests indicators with adaptive exit timing using dual KNN."""
    
    def __init__(self, k: int = 12, entry_threshold: float = 0.65, exit_threshold: float = 0.70,
                 min_expected_return: float = 0.01, transaction_cost_pct: float = 0.001, min_hold_days: int = 2):
        self.k = k
        self.entry_threshold = entry_threshold    # Higher threshold for entry
        self.exit_threshold = exit_threshold      # Higher threshold for exit (prevent whipsaws)
        self.min_expected_return = min_expected_return
        self.transaction_cost_pct = transaction_cost_pct
        self.min_hold_days = min_hold_days        # Minimum holding period
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🚀 Setting up Adaptive Exit Tester...")
        print(f"   Entry Strategy: KNN with K={self.k}, threshold={self.entry_threshold:.1%}")
        print(f"   Exit Strategy: KNN with K={self.k}, threshold={self.exit_threshold:.1%}")
        print(f"   Min holding period: {self.min_hold_days} days")
        print(f"   Min expected return: {self.min_expected_return:.1%}")
        print(f"   Transaction cost: {self.transaction_cost_pct:.1%} per side")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register indicators
        indicators_to_test = [
            ('rsi_14', RSI(period=14)),
            ('sma_10', SMA(period=10)),
            ('sma_20', SMA(period=20)),
            ('ema_20', EMA(period=20)),
        ]
        
        for name, indicator in indicators_to_test:
            indicator_registry.register(indicator)
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date, limit: int = 15) -> List[str]:
        """Get quality stocks for testing."""
        query = """
        SELECT symbol, COUNT(*) as days, AVG(volume::NUMERIC) as avg_volume,
               AVG(close_price::NUMERIC) as avg_price, MIN(close_price::NUMERIC) as min_price
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2 AND volume > 0 AND close_price > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 150 AND MIN(close_price::NUMERIC) >= 1.0
        ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        LIMIT 100
        """
        
        rows = await self.db_conn.fetch(query, start_date - timedelta(days=250), end_date)
        quality_symbols = [row['symbol'] for row in rows 
                          if float(row['min_price']) >= 1.0 and float(row['avg_price']) >= 2.0][:limit]
        
        print(f"📊 Selected {len(quality_symbols)} quality stocks")
        return quality_symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data with buffer."""
        buffer_start = start_date - timedelta(days=250)
        
        query = """
        SELECT date, open_price::NUMERIC as open, high_price::NUMERIC as high,
               low_price::NUMERIC as low, close_price::NUMERIC as close, volume::BIGINT as volume
        FROM daily_price_data WHERE symbol = $1 AND date BETWEEN $2 AND $3 ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, buffer_start, end_date + timedelta(days=30))
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        return df.dropna()
    
    def calculate_indicator_ratio(self, indicator_name: str, indicator_value: float, current_price: float, current_volume: float) -> float:
        """Convert indicator to normalized ratio."""
        if indicator_name.startswith('rsi'):
            return indicator_value  # RSI already 0-100
        elif indicator_name.startswith('sma') or indicator_name.startswith('ema'):
            return current_price / indicator_value if indicator_value > 0 else 1.0
        elif indicator_name.startswith('volume'):
            return current_volume / indicator_value if indicator_value > 0 else 1.0
        else:
            return indicator_value
    
    def calculate_distance(self, val1: float, val2: float) -> float:
        """Calculate distance between two indicator values."""
        return abs(val1 - val2)
    
    def find_knn_patterns(self, target_value: float, historical_data: List[Tuple], target_date: date, target_symbol: str) -> List[Tuple]:
        """Find K nearest neighbors for pattern matching."""
        distances = []
        
        for hist_date, hist_symbol, hist_value, hist_return_1d, hist_return_5d, hist_direction in historical_data:
            # Skip same stock within 10 days or future data
            if ((hist_symbol == target_symbol and abs((hist_date - target_date).days) <= 10) or 
                hist_date >= target_date):
                continue
            
            distance = self.calculate_distance(target_value, hist_value)
            distances.append((hist_date, hist_symbol, hist_value, hist_return_1d, hist_return_5d, hist_direction, distance))
        
        distances.sort(key=lambda x: x[6])  # Sort by distance
        return distances[:self.k]
    
    def make_knn_prediction(self, neighbors: List[Tuple], prediction_type: str = 'entry') -> Dict:
        """Make entry or exit prediction based on neighbors."""
        if not neighbors:
            return {'prediction': 'neutral', 'confidence': 0.0, 'expected_return': 0.0}
        
        total_weight = 0
        direction_votes = {'up': 0, 'down': 0, 'neutral': 0}
        weighted_returns = []
        
        for hist_date, hist_symbol, hist_value, hist_return_1d, hist_return_5d, hist_direction, distance in neighbors:
            weight = 1 / (distance + 0.001)
            total_weight += weight
            
            # For entry: use 5-day returns, for exit: use 1-day returns
            return_val = hist_return_5d if prediction_type == 'entry' else hist_return_1d
            direction = hist_direction
            
            direction_votes[direction] += weight
            weighted_returns.append(return_val * weight)
        
        # Normalize votes
        if total_weight > 0:
            for direction in direction_votes:
                direction_votes[direction] /= total_weight
        
        predicted_direction = max(direction_votes.items(), key=lambda x: x[1])[0]
        confidence = direction_votes[predicted_direction]
        expected_return = sum(weighted_returns) / total_weight if total_weight > 0 else 0
        
        return {
            'prediction': predicted_direction,
            'confidence': confidence,
            'expected_return': expected_return,
            'neighbor_count': len(neighbors)
        }
    
    async def run_adaptive_exit_backtest(self, indicator_name: str, symbols: List[str], 
                                        start_date: date, end_date: date) -> Dict:
        """Run backtest with adaptive exit timing."""
        print(f"🧪 Testing {indicator_name} with adaptive exit strategy...")
        
        all_historical_data = []
        trades_executed = []
        
        # Collect all historical patterns first
        for symbol in symbols:
            market_data = await self.get_market_data(symbol, start_date, end_date)
            if market_data.empty or len(market_data) < 200:
                continue
            
            company_id = self.get_company_id(symbol)
            
            try:
                # Calculate indicator
                indicator_values = await self.indicator_engine.calculate_multiple(
                    [indicator_name], company_id, market_data,
                    start_date - timedelta(days=250), end_date
                )
                
                if indicator_name not in indicator_values:
                    continue
                
                indicator_result = indicator_values[indicator_name]
                if not hasattr(indicator_result, 'values') or not indicator_result.values:
                    continue
                
                # Process each day
                for date_str, indicator_value in indicator_result.values.items():
                    trade_date = pd.to_datetime(date_str).date()
                    if trade_date < start_date or trade_date > end_date:
                        continue
                    
                    # Check if date exists in market data
                    timestamp = pd.Timestamp(trade_date)
                    if timestamp not in market_data.index:
                        continue
                    
                    row = market_data.loc[pd.Timestamp(trade_date)]
                    current_price = row['close']
                    current_volume = row['volume']
                    
                    # Skip penny stocks
                    if current_price < 1.0:
                        continue
                    
                    # Calculate normalized indicator value
                    normalized_value = self.calculate_indicator_ratio(
                        indicator_name, indicator_value, current_price, current_volume
                    )
                    
                    # Calculate future returns for training
                    try:
                        # 1-day return (for exit decisions)
                        next_day_idx = market_data.index.get_loc(pd.Timestamp(trade_date)) + 1
                        if next_day_idx + 1 < len(market_data):
                            next_open = market_data.iloc[next_day_idx]['open']
                            day_after_close = market_data.iloc[next_day_idx + 1]['close']
                            return_1d = (day_after_close / next_open - 1) if next_open > 0 else 0
                        else:
                            return_1d = 0
                        
                        # 5-day return (for entry decisions)
                        if next_day_idx + 5 < len(market_data):
                            next_open = market_data.iloc[next_day_idx]['open']
                            future_close = market_data.iloc[next_day_idx + 5]['close']
                            return_5d = (future_close / next_open - 1) if next_open > 0 else 0
                        else:
                            return_5d = 0
                        
                        # Direction classification
                        direction_5d = 'up' if return_5d > 0.02 else 'down' if return_5d < -0.02 else 'neutral'
                        
                        all_historical_data.append((
                            trade_date, symbol, normalized_value, return_1d, return_5d, direction_5d
                        ))
                        
                    except (KeyError, IndexError):
                        continue
                        
            except Exception as e:
                print(f"     ⚠️ Error processing {symbol}: {str(e)}")
                continue
        
        print(f"     📊 Collected {len(all_historical_data)} historical patterns")
        
        if len(all_historical_data) < 500:
            return {'error': f'Insufficient data: {len(all_historical_data)} patterns'}
        
        # Sort by date for time-aware testing
        all_historical_data.sort(key=lambda x: x[0])
        
        # Now run the adaptive backtest
        open_positions = []  # Track open positions
        
        for i, (trade_date, symbol, normalized_value, return_1d, return_5d, direction_5d) in enumerate(all_historical_data):
            
            # Process existing positions first (daily exit evaluation)
            positions_to_close = []
            for pos_idx, position in enumerate(open_positions):
                days_held = (trade_date - position.entry_date).days
                
                # Enforce minimum holding period
                if days_held < self.min_hold_days:
                    continue
                
                # Force exit after max hold days
                if days_held >= position.max_hold_days:
                    positions_to_close.append(pos_idx)
                    continue
                
                # Use KNN to decide if we should exit
                historical_data_for_exit = all_historical_data[:i]  # Only use past data
                if len(historical_data_for_exit) < self.k * 2:
                    continue
                
                # Find exit patterns for this position
                exit_neighbors = self.find_knn_patterns(
                    normalized_value, historical_data_for_exit, trade_date, symbol
                )
                
                exit_prediction = self.make_knn_prediction(exit_neighbors, 'exit')
                
                # Exit if confidence is high and prediction suggests exit
                if (exit_prediction['confidence'] >= self.exit_threshold and 
                    exit_prediction['prediction'] != position.predicted_direction):
                    positions_to_close.append(pos_idx)
            
            # Close positions that met exit criteria
            for pos_idx in reversed(positions_to_close):  # Reverse to maintain indices
                position = open_positions.pop(pos_idx)
                
                # Calculate actual trade result
                try:
                    # Get actual market data for this exit
                    market_data = await self.get_market_data(position.symbol, position.entry_date, trade_date)
                    if not market_data.empty:
                        entry_row = market_data.loc[market_data.index.date == position.entry_date]
                        exit_row = market_data.loc[market_data.index.date == trade_date]
                        
                        if not entry_row.empty and not exit_row.empty:
                            actual_entry = entry_row.iloc[0]['open']
                            actual_exit = exit_row.iloc[0]['close']
                            
                            gross_return = (actual_exit / actual_entry - 1) if actual_entry > 0 else 0
                            transaction_costs = self.transaction_cost_pct * 2
                            net_return = gross_return - transaction_costs
                            
                            days_held = (trade_date - position.entry_date).days
                            
                            trade_record = {
                                'entry_date': position.entry_date,
                                'exit_date': trade_date,
                                'symbol': position.symbol,
                                'indicator': indicator_name,
                                'entry_price': actual_entry,
                                'exit_price': actual_exit,
                                'days_held': days_held,
                                'predicted_direction': position.predicted_direction,
                                'entry_confidence': position.confidence,
                                'gross_return': gross_return,
                                'net_return': net_return,
                                'success': (net_return > 0) if position.predicted_direction == 'up' else (net_return < 0)
                            }
                            
                            trades_executed.append(trade_record)
                            
                except Exception as e:
                    continue  # Skip if can't get exit data
            
            # Now evaluate new entry opportunities
            if len(open_positions) < 5:  # Limit concurrent positions
                historical_data_for_entry = all_historical_data[:i]
                if len(historical_data_for_entry) < self.k * 2:
                    continue
                
                # Find entry patterns
                entry_neighbors = self.find_knn_patterns(
                    normalized_value, historical_data_for_entry, trade_date, symbol
                )
                
                entry_prediction = self.make_knn_prediction(entry_neighbors, 'entry')
                
                # Enter position if criteria met
                if (entry_prediction['confidence'] >= self.entry_threshold and 
                    entry_prediction['prediction'] != 'neutral' and
                    entry_prediction['expected_return'] >= self.min_expected_return):
                    
                    new_position = TradingPosition(
                        entry_date=trade_date,
                        entry_price=0,  # Will be filled with next day's open
                        symbol=symbol,
                        indicator_name=indicator_name,
                        indicator_value_at_entry=normalized_value,
                        predicted_direction=entry_prediction['prediction'],
                        confidence=entry_prediction['confidence'],
                        expected_return=entry_prediction['expected_return'],
                        max_hold_days=20
                    )
                    
                    open_positions.append(new_position)
        
        # Force close any remaining open positions at end of backtest
        for position in open_positions:
            # Force exit at end_date
            try:
                market_data = await self.get_market_data(position.symbol, position.entry_date, end_date)
                if not market_data.empty:
                    entry_row = market_data.loc[market_data.index.date == position.entry_date]
                    exit_row = market_data.loc[market_data.index.date <= end_date].tail(1)
                    
                    if not entry_row.empty and not exit_row.empty:
                        actual_entry = entry_row.iloc[0]['open']
                        actual_exit = exit_row.iloc[0]['close']
                        
                        gross_return = (actual_exit / actual_entry - 1) if actual_entry > 0 else 0
                        net_return = gross_return - (self.transaction_cost_pct * 2)
                        
                        trade_record = {
                            'entry_date': position.entry_date,
                            'exit_date': end_date,
                            'symbol': position.symbol,
                            'indicator': indicator_name,
                            'entry_price': actual_entry,
                            'exit_price': actual_exit,
                            'days_held': (end_date - position.entry_date).days,
                            'predicted_direction': position.predicted_direction,
                            'entry_confidence': position.confidence,
                            'gross_return': gross_return,
                            'net_return': net_return,
                            'success': (net_return > 0) if position.predicted_direction == 'up' else (net_return < 0)
                        }
                        
                        trades_executed.append(trade_record)
                        
            except Exception as e:
                continue
        
        # Calculate performance metrics
        if not trades_executed:
            return {'error': 'No trades executed'}
        
        total_trades = len(trades_executed)
        winning_trades = sum(1 for t in trades_executed if t['success'])
        win_rate = winning_trades / total_trades
        
        avg_return = np.mean([t['net_return'] for t in trades_executed])
        total_return = sum([t['net_return'] for t in trades_executed])
        avg_hold_days = np.mean([t['days_held'] for t in trades_executed])
        
        return {
            'indicator': indicator_name,
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return_per_trade': avg_return,
            'total_return': total_return,
            'avg_hold_days': avg_hold_days,
            'best_trade': max([t['net_return'] for t in trades_executed]),
            'worst_trade': min([t['net_return'] for t in trades_executed]),
            'trades': trades_executed[-10:]  # Show last 10 trades as examples
        }
    
    def display_adaptive_results(self, results: Dict):
        """Display adaptive exit strategy results."""
        print(f"\n🎯 ADAPTIVE EXIT STRATEGY RESULTS")
        print(f"=" * 80)
        
        for indicator, result in results.items():
            if 'error' in result:
                print(f"❌ {indicator}: {result['error']}")
                continue
            
            print(f"\n📊 {indicator.upper()}:")
            print(f"   Total trades: {result['total_trades']:,}")
            print(f"   Win rate: {result['win_rate']:.1%}")
            print(f"   Avg return per trade: {result['avg_return_per_trade']:.2%}")
            print(f"   Total return: {result['total_return']:.1%}")
            print(f"   Avg hold time: {result['avg_hold_days']:.1f} days")
            print(f"   Best/Worst trade: {result['best_trade']:+.1%} / {result['worst_trade']:+.1%}")
            
            if 'trades' in result and result['trades']:
                print(f"   📋 Sample recent trades:")
                for trade in result['trades'][-5:]:
                    print(f"      {trade['entry_date']} → {trade['exit_date']} "
                          f"({trade['days_held']}d): "
                          f"{trade['net_return']:+.1%} {'✅' if trade['success'] else '❌'}")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run adaptive exit testing."""
    
    # Improved parameters to reduce over-trading
    tester = AdaptiveExitTester(k=12, entry_threshold=0.65, exit_threshold=0.70,
                               min_expected_return=0.01, transaction_cost_pct=0.001, min_hold_days=2)
    
    try:
        await tester.setup()
        
        # Test period
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        symbols = await tester.get_stock_universe(start_date, end_date, limit=15)
        
        if len(symbols) < 5:
            print("❌ Insufficient stocks")
            return
        
        print(f"\n🚀 ADAPTIVE EXIT STRATEGY TESTING")
        print(f"Period: {start_date} to {end_date}")
        print(f"Stocks: {len(symbols)}")
        print(f"Strategy: Dual KNN (entry + adaptive exit)")
        
        # Test best indicators from previous analysis
        indicators_to_test = ['sma_10', 'sma_20', 'ema_20', 'rsi_14']
        
        results = {}
        for indicator in indicators_to_test:
            result = await tester.run_adaptive_exit_backtest(indicator, symbols, start_date, end_date)
            results[indicator] = result
        
        tester.display_adaptive_results(results)
        
        print(f"\n🎯 ADAPTIVE EXIT TESTING COMPLETE!")
        print(f"This shows the power of using KNN for both entry AND exit timing!")
        
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())