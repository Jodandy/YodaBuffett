#!/usr/bin/env python3
"""
Isolated Indicator Testing Framework with Adaptive KNN Exit

Based on isolated_indicator_tester.py but with dynamic exit timing:
- Uses KNN to determine when to enter (same as before)
- Uses KNN to determine when to exit (new adaptive feature)
- Tests individual indicators in isolation
- Maintains all the existing structure and analysis
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

class SingleIndicatorPattern(NamedTuple):
    """A pattern with just one indicator value."""
    indicator_value: float
    date: date
    symbol: str
    price: float
    volume: float

class AdaptiveTrade(NamedTuple):
    """A trade with adaptive exit timing."""
    entry_date: date
    entry_price: float
    entry_indicator_value: float
    symbol: str
    prediction: str
    confidence: float
    expected_return: float
    max_hold_days: int = 15

class IsolatedIndicatorAdaptiveExit:
    """Tests individual indicators with adaptive exit timing using pure KNN."""
    
    def __init__(self, k: int = 10, consensus_threshold: float = 0.6, 
                 min_expected_return: float = 0.01, transaction_cost_pct: float = 0.001,
                 exit_threshold: float = 0.65, min_hold_days: int = 2):
        self.k = k
        self.consensus_threshold = consensus_threshold
        self.min_expected_return = min_expected_return
        self.transaction_cost_pct = transaction_cost_pct
        self.exit_threshold = exit_threshold  # Threshold for exit decisions
        self.min_hold_days = min_hold_days    # Minimum days to hold
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🔬 Setting up Isolated Indicator Adaptive Exit Tester...")
        print(f"   K neighbors: {self.k}")
        print(f"   Entry consensus threshold: {self.consensus_threshold:.1%}")
        print(f"   Exit threshold: {self.exit_threshold:.1%}")
        print(f"   Min holding period: {self.min_hold_days} days")
        print(f"   Min expected return: {self.min_expected_return:.1%}")
        print(f"   Transaction cost: {self.transaction_cost_pct:.1%} per side ({self.transaction_cost_pct * 2:.1%} total)")
        print(f"   Testing strategy: Each indicator in isolation with adaptive exits")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register core indicators for testing
        indicators_to_test = [
            # RSI variations
            ('rsi_14', RSI(period=14)),
            ('rsi_21', RSI(period=21)),
            
            # SMA variations
            ('sma_10', SMA(period=10)),
            ('sma_20', SMA(period=20)),
            ('sma_50', SMA(period=50)),
            
            # EMA variations
            ('ema_10', EMA(period=10)),
            ('ema_20', EMA(period=20)),
            
            # Volume indicators
            ('volume_sma_20', VolumeMA(period=20)),
        ]
        
        for name, indicator in indicators_to_test:
            indicator_registry.register(indicator)
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        print(f"✅ Registered {len(indicators_to_test)} indicators for testing")
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date, limit: int = 30) -> List[str]:
        """Get stocks with sufficient data and price above $1."""
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
        HAVING COUNT(*) >= 150  -- Need lots of data for indicators
        AND MIN(close_price::NUMERIC) >= 1.0  -- Exclude penny stocks
        ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        LIMIT 100
        """
        
        rows = await self.db_conn.fetch(query, start_date - timedelta(days=250), end_date)
        
        # Additional filtering to ensure quality stocks
        quality_symbols = []
        for row in rows:
            if float(row['min_price']) >= 1.0 and float(row['avg_price']) >= 2.0:
                quality_symbols.append(row['symbol'])
        
        symbols = quality_symbols[:limit]
        
        print(f"📊 Selected {len(symbols)} quality stocks (min price >= $1)")
        print(f"   Sample: {symbols[:5]}...")
        
        return symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data with large buffer for indicators."""
        buffer_start = start_date - timedelta(days=250)  # Big buffer for SMA 200
        
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
    
    def normalize_indicator_value(self, indicator_name: str, indicator_value: float, 
                                 current_price: float, current_volume: float) -> float:
        """Normalize indicator values for distance calculation."""
        if indicator_name.startswith('rsi'):
            return indicator_value  # RSI already 0-100
        elif indicator_name.startswith('sma') or indicator_name.startswith('ema'):
            # Use price/MA ratio
            if indicator_value > 0:
                return current_price / indicator_value
            else:
                return 1.0
        elif indicator_name.startswith('volume'):
            # Use volume ratio
            if indicator_value > 0:
                return current_volume / indicator_value
            else:
                return 1.0
        else:
            return indicator_value
    
    async def extract_all_patterns(self, symbol: str, indicator_name: str, 
                                  start_date: date, end_date: date) -> List[Tuple[SingleIndicatorPattern, Dict]]:
        """Extract all patterns with both 1d and 5d outcomes for a stock."""
        
        market_data = await self.get_market_data(symbol, start_date, end_date)
        if market_data.empty or len(market_data) < 200:
            return []
        
        company_id = self.get_company_id(symbol)
        
        # Get the specific indicator
        try:
            indicator_values = await self.indicator_engine.calculate_multiple(
                [indicator_name],
                company_id,
                market_data,
                start_date - timedelta(days=250),
                end_date
            )
            
            if indicator_name not in indicator_values:
                return []
            
            # Convert to DataFrame
            result = indicator_values[indicator_name]
            if not hasattr(result, 'values') or not result.values:
                return []
            
            indicator_df = pd.DataFrame(list(result.values.items()), columns=['date', indicator_name])
            indicator_df['date'] = pd.to_datetime(indicator_df['date'])
            indicator_df.set_index('date', inplace=True)
            
        except Exception as e:
            print(f"     ❌ Error calculating {indicator_name} for {symbol}: {str(e)}")
            return []
        
        # Extract patterns with outcomes
        patterns_and_outcomes = []
        analysis_data = market_data[(market_data.index.date >= start_date) & 
                                   (market_data.index.date <= end_date)]
        
        for timestamp, row in analysis_data.iterrows():
            try:
                # Get indicator value
                if timestamp not in indicator_df.index:
                    continue
                
                indicator_value = indicator_df.loc[timestamp, indicator_name]
                if pd.isna(indicator_value):
                    continue
                
                # Normalize indicator value
                current_price = row['close']
                current_volume = row['volume']
                
                # Skip if price is below $1
                if current_price < 1.0:
                    continue
                
                normalized_value = self.normalize_indicator_value(
                    indicator_name, indicator_value, current_price, current_volume
                )
                
                # Calculate future returns (1d for exits, 5d for entries)
                try:
                    timestamp_idx = market_data.index.get_loc(timestamp)
                    
                    # 1-day return (for exit decisions)
                    if timestamp_idx + 2 < len(market_data):
                        next_open = market_data.iloc[timestamp_idx + 1]['open']
                        next_close = market_data.iloc[timestamp_idx + 2]['close']
                        return_1d = (next_close / next_open - 1) if next_open > 0 else 0
                        direction_1d = 'up' if return_1d > 0.01 else 'down' if return_1d < -0.01 else 'neutral'
                    else:
                        return_1d = 0
                        direction_1d = 'neutral'
                    
                    # 5-day return (for entry decisions)
                    if timestamp_idx + 6 < len(market_data):
                        entry_open = market_data.iloc[timestamp_idx + 1]['open']
                        exit_close = market_data.iloc[timestamp_idx + 6]['close']
                        return_5d = (exit_close / entry_open - 1) if entry_open > 0 else 0
                        direction_5d = 'up' if return_5d > 0.02 else 'down' if return_5d < -0.02 else 'neutral'
                    else:
                        return_5d = 0
                        direction_5d = 'neutral'
                    
                except (IndexError, KeyError):
                    continue
                
                pattern = SingleIndicatorPattern(
                    indicator_value=float(normalized_value),
                    date=timestamp.date(),
                    symbol=symbol,
                    price=current_price,
                    volume=current_volume
                )
                
                outcome = {
                    'return_1d': return_1d,
                    'return_5d': return_5d,
                    'direction_1d': direction_1d,
                    'direction_5d': direction_5d
                }
                
                patterns_and_outcomes.append((pattern, outcome))
                
            except (KeyError, IndexError, ValueError) as e:
                continue
        
        return patterns_and_outcomes
    
    def calculate_distance_1d(self, val1: float, val2: float) -> float:
        """Calculate distance between two indicator values."""
        return abs(val1 - val2)
    
    def find_k_neighbors_1d(self, target_value: float, historical_patterns: List[Tuple[SingleIndicatorPattern, Dict]], 
                           target_date: date, target_symbol: str) -> List[Tuple[SingleIndicatorPattern, Dict, float]]:
        """Find K nearest neighbors based on single indicator value."""
        
        distances = []
        for pattern, outcome in historical_patterns:
            # Don't compare to patterns from same stock within 10 days
            if (pattern.symbol == target_symbol and 
                abs((pattern.date - target_date).days) <= 10):
                continue
            
            # Only use historical data (no look-ahead)
            if pattern.date >= target_date:
                continue
            
            distance = self.calculate_distance_1d(target_value, pattern.indicator_value)
            distances.append((pattern, outcome, distance))
        
        # Sort by distance and return top K
        distances.sort(key=lambda x: x[2])
        return distances[:self.k]
    
    def make_prediction_1d(self, neighbors: List[Tuple[SingleIndicatorPattern, Dict, float]], 
                          prediction_type: str = 'entry') -> Dict:
        """Make entry or exit prediction based on neighbors."""
        
        if not neighbors:
            return {'prediction': 'neutral', 'confidence': 0.0, 'expected_return': 0.0}
        
        # Use different return horizons for entry vs exit
        return_key = 'return_5d' if prediction_type == 'entry' else 'return_1d'
        direction_key = 'direction_5d' if prediction_type == 'entry' else 'direction_1d'
        
        # Weight by inverse distance
        total_weight = 0
        direction_votes = {'up': 0, 'down': 0, 'neutral': 0}
        weighted_returns = []
        
        for pattern, outcome, distance in neighbors:
            weight = 1 / (distance + 0.001)
            total_weight += weight
            
            direction = outcome[direction_key]
            return_val = outcome[return_key]
            
            direction_votes[direction] += weight
            weighted_returns.append(return_val * weight)
        
        # Normalize votes
        if total_weight > 0:
            for direction in direction_votes:
                direction_votes[direction] /= total_weight
        
        # Make prediction
        predicted_direction = max(direction_votes.items(), key=lambda x: x[1])[0]
        confidence = direction_votes[predicted_direction]
        expected_return = sum(weighted_returns) / total_weight if total_weight > 0 else 0
        
        return {
            'prediction': predicted_direction,
            'confidence': confidence,
            'expected_return': expected_return,
            'direction_votes': direction_votes,
            'neighbor_count': len(neighbors)
        }
    
    async def test_single_indicator_adaptive(self, indicator_name: str, symbols: List[str], 
                                           start_date: date, end_date: date) -> Dict:
        """Test a single indicator with adaptive exit timing."""
        
        print(f"🔬 Testing {indicator_name} with adaptive exits...")
        
        # Collect all patterns for this indicator
        all_patterns = []
        successful_stocks = 0
        
        for symbol in symbols:
            try:
                patterns = await self.extract_all_patterns(
                    symbol, indicator_name, start_date, end_date
                )
                if patterns:
                    all_patterns.extend(patterns)
                    successful_stocks += 1
                    
            except Exception as e:
                print(f"     ⚠️ Failed {symbol}: {str(e)}")
        
        if len(all_patterns) < 100:
            return {
                'indicator': indicator_name,
                'error': f'Insufficient data: {len(all_patterns)} patterns from {successful_stocks} stocks'
            }
        
        print(f"     📊 {len(all_patterns)} patterns from {successful_stocks} stocks")
        
        # Sort by date for time-aware testing
        all_patterns.sort(key=lambda x: x[0].date)
        
        # Run adaptive exit backtest
        open_trades = []
        completed_trades = []
        
        for i, (test_pattern, actual_outcome) in enumerate(all_patterns):
            current_date = test_pattern.date
            
            # Process existing trades first - check for exits
            trades_to_close = []
            for trade_idx, trade in enumerate(open_trades):
                days_held = (current_date - trade.entry_date).days
                
                # Enforce minimum holding period
                if days_held < self.min_hold_days:
                    continue
                
                # Force exit after max hold days
                if days_held >= trade.max_hold_days:
                    trades_to_close.append(trade_idx)
                    continue
                
                # Use KNN to decide if we should exit
                historical_patterns = all_patterns[:i]
                if len(historical_patterns) < self.k * 2:
                    continue
                
                # Find exit neighbors
                exit_neighbors = self.find_k_neighbors_1d(
                    test_pattern.indicator_value, historical_patterns,
                    current_date, test_pattern.symbol
                )
                
                exit_prediction = self.make_prediction_1d(exit_neighbors, 'exit')
                
                # Exit if confidence is high and prediction suggests exit
                if (exit_prediction['confidence'] >= self.exit_threshold and 
                    exit_prediction['prediction'] != trade.prediction):
                    trades_to_close.append(trade_idx)
            
            # Close trades that met exit criteria
            for trade_idx in reversed(trades_to_close):  # Reverse to maintain indices
                trade = open_trades.pop(trade_idx)
                
                # Calculate actual trade result
                try:
                    # Get actual market data for exit
                    market_data = await self.get_market_data(trade.symbol, trade.entry_date, current_date)
                    if not market_data.empty:
                        # Find entry and exit rows
                        entry_rows = market_data.loc[market_data.index.date == trade.entry_date]
                        exit_rows = market_data.loc[market_data.index.date == current_date]
                        
                        if not entry_rows.empty and not exit_rows.empty:
                            # Use next day's open as entry (realistic)
                            entry_date_idx = market_data.index.get_loc(entry_rows.index[0])
                            if entry_date_idx + 1 < len(market_data):
                                actual_entry = market_data.iloc[entry_date_idx + 1]['open']
                            else:
                                actual_entry = entry_rows.iloc[0]['close']
                            
                            actual_exit = exit_rows.iloc[0]['close']
                            
                            gross_return = (actual_exit / actual_entry - 1) if actual_entry > 0 else 0
                            transaction_costs = self.transaction_cost_pct * 2
                            net_return = gross_return - transaction_costs
                            
                            days_held = (current_date - trade.entry_date).days
                            
                            trade_record = {
                                'entry_date': trade.entry_date,
                                'exit_date': current_date,
                                'symbol': trade.symbol,
                                'indicator_value': trade.entry_indicator_value,
                                'entry_price': actual_entry,
                                'exit_price': actual_exit,
                                'days_held': days_held,
                                'prediction': trade.prediction,
                                'confidence': trade.confidence,
                                'expected_return': trade.expected_return,
                                'gross_return': gross_return,
                                'net_return': net_return,
                                'success': (net_return > 0) if trade.prediction == 'up' else (net_return < 0)
                            }
                            
                            completed_trades.append(trade_record)
                            
                except Exception as e:
                    continue  # Skip if can't calculate exit
            
            # Now evaluate new entry opportunities
            if len(open_trades) < 3:  # Limit concurrent trades
                historical_patterns = all_patterns[:i]
                if len(historical_patterns) < self.k * 2:
                    continue
                
                # Find entry neighbors
                entry_neighbors = self.find_k_neighbors_1d(
                    test_pattern.indicator_value, historical_patterns,
                    test_pattern.date, test_pattern.symbol
                )
                
                entry_prediction = self.make_prediction_1d(entry_neighbors, 'entry')
                
                # Enter trade if criteria met
                if (entry_prediction['confidence'] >= self.consensus_threshold and 
                    entry_prediction['prediction'] != 'neutral' and
                    entry_prediction['expected_return'] >= self.min_expected_return):
                    
                    new_trade = AdaptiveTrade(
                        entry_date=test_pattern.date,
                        entry_price=0,  # Will be filled with next day's open
                        entry_indicator_value=test_pattern.indicator_value,
                        symbol=test_pattern.symbol,
                        prediction=entry_prediction['prediction'],
                        confidence=entry_prediction['confidence'],
                        expected_return=entry_prediction['expected_return'],
                        max_hold_days=15
                    )
                    
                    open_trades.append(new_trade)
        
        # Force close any remaining trades at end
        for trade in open_trades:
            try:
                market_data = await self.get_market_data(trade.symbol, trade.entry_date, end_date)
                if not market_data.empty:
                    entry_rows = market_data.loc[market_data.index.date == trade.entry_date]
                    exit_rows = market_data.loc[market_data.index.date <= end_date].tail(1)
                    
                    if not entry_rows.empty and not exit_rows.empty:
                        entry_date_idx = market_data.index.get_loc(entry_rows.index[0])
                        if entry_date_idx + 1 < len(market_data):
                            actual_entry = market_data.iloc[entry_date_idx + 1]['open']
                        else:
                            actual_entry = entry_rows.iloc[0]['close']
                        
                        actual_exit = exit_rows.iloc[0]['close']
                        
                        gross_return = (actual_exit / actual_entry - 1) if actual_entry > 0 else 0
                        net_return = gross_return - (self.transaction_cost_pct * 2)
                        
                        trade_record = {
                            'entry_date': trade.entry_date,
                            'exit_date': end_date,
                            'symbol': trade.symbol,
                            'indicator_value': trade.entry_indicator_value,
                            'entry_price': actual_entry,
                            'exit_price': actual_exit,
                            'days_held': (end_date - trade.entry_date).days,
                            'prediction': trade.prediction,
                            'confidence': trade.confidence,
                            'expected_return': trade.expected_return,
                            'gross_return': gross_return,
                            'net_return': net_return,
                            'success': (net_return > 0) if trade.prediction == 'up' else (net_return < 0)
                        }
                        
                        completed_trades.append(trade_record)
                        
            except Exception as e:
                continue
        
        # Calculate performance metrics
        if not completed_trades:
            return {'error': 'No trades executed'}
        
        total_trades = len(completed_trades)
        winning_trades = sum(1 for t in completed_trades if t['success'])
        win_rate = winning_trades / total_trades
        
        # Filter out any NaN returns before calculating metrics
        valid_returns = [t['net_return'] for t in completed_trades if not pd.isna(t['net_return'])]
        
        if valid_returns:
            avg_return = np.mean(valid_returns)
            total_return = sum(valid_returns)
            avg_hold_days = np.mean([t['days_held'] for t in completed_trades])
        else:
            avg_return = 0.0
            total_return = 0.0
            avg_hold_days = 0.0
        
        # Show sample trades
        print(f"     📋 Sample recent adaptive trades:")
        for trade in completed_trades[-5:]:
            print(f"        {trade['entry_date']} → {trade['exit_date']} "
                  f"({trade['days_held']}d): {trade['net_return']:+.1%} "
                  f"{'✅' if trade['success'] else '❌'}")
        
        return {
            'indicator': indicator_name,
            'total_patterns': len(all_patterns),
            'successful_stocks': successful_stocks,
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return_per_trade': avg_return,
            'total_return': total_return,
            'avg_hold_days': avg_hold_days,
            'best_trade': max(valid_returns) if valid_returns else 0,
            'worst_trade': min(valid_returns) if valid_returns else 0
        }
    
    async def test_all_indicators_adaptive(self, symbols: List[str], start_date: date, end_date: date) -> Dict:
        """Test all indicators with adaptive exits and rank by performance."""
        
        indicators_to_test = [
            'rsi_14', 'rsi_21',
            'sma_10', 'sma_20', 'sma_50', 
            'ema_10', 'ema_20',
            'volume_sma_20'
        ]
        
        print(f"\n🔬 ADAPTIVE EXIT INDICATOR TESTING")
        print(f"Testing {len(indicators_to_test)} indicators on {len(symbols)} stocks")
        print(f"Period: {start_date} to {end_date}")
        print(f"Strategy: Pure KNN entry + Adaptive KNN exit")
        print(f"Entry threshold: {self.consensus_threshold:.0%}, Exit threshold: {self.exit_threshold:.0%}")
        
        results = {}
        
        for i, indicator in enumerate(indicators_to_test, 1):
            print(f"\n📊 [{i}/{len(indicators_to_test)}] Testing {indicator}...")
            
            try:
                result = await self.test_single_indicator_adaptive(indicator, symbols, start_date, end_date)
                results[indicator] = result
                
                # Quick summary
                if 'error' not in result:
                    print(f"     ✅ {result['total_trades']} trades, "
                          f"{result['win_rate']:.1%} win rate, "
                          f"{result['total_return']:+.1%} total return, "
                          f"{result['avg_hold_days']:.1f}d avg hold")
                else:
                    print(f"     ❌ {result['error']}")
                    
            except Exception as e:
                print(f"     ❌ Failed: {str(e)}")
                results[indicator] = {'error': str(e)}
        
        return results
    
    def display_adaptive_results_summary(self, results: Dict):
        """Display adaptive exit results ranking."""
        
        print(f"\n🏆 ADAPTIVE EXIT STRATEGY PERFORMANCE RANKING")
        print(f"=" * 100)
        
        # Create ranking table
        ranking_data = []
        
        for indicator, result in results.items():
            if 'error' in result:
                continue
            
            ranking_data.append({
                'indicator': indicator,
                'win_rate': result['win_rate'],
                'avg_return': result['avg_return_per_trade'],
                'total_return': result['total_return'],
                'trades': result['total_trades'],
                'avg_hold_days': result['avg_hold_days']
            })
        
        if not ranking_data:
            print("❌ No successful results to display")
            return
        
        # Sort by total return
        ranking_data.sort(key=lambda x: x['total_return'], reverse=True)
        
        print(f"🎯 RANKED BY TOTAL RETURN:")
        print(f"{'Indicator':<15} {'Win Rate':<10} {'Avg Ret':<10} {'Total Ret':<10} {'Trades':<8} {'Avg Hold':<10}")
        print("-" * 85)
        
        for entry in ranking_data:
            print(f"{entry['indicator']:<15} {entry['win_rate']:<10.1%} "
                  f"{entry['avg_return']:<10.2%} {entry['total_return']:<10.1%} "
                  f"{entry['trades']:<8} {entry['avg_hold_days']:<10.1f}d")
        
        # Sort by win rate
        ranking_data.sort(key=lambda x: x['win_rate'], reverse=True)
        
        print(f"\n🎯 RANKED BY WIN RATE:")
        print(f"{'Indicator':<15} {'Win Rate':<10} {'Avg Ret':<10} {'Total Ret':<10} {'Trades':<8} {'Avg Hold':<10}")
        print("-" * 85)
        
        for entry in ranking_data:
            print(f"{entry['indicator']:<15} {entry['win_rate']:<10.1%} "
                  f"{entry['avg_return']:<10.2%} {entry['total_return']:<10.1%} "
                  f"{entry['trades']:<8} {entry['avg_hold_days']:<10.1f}d")
        
        # Best performers analysis
        print(f"\n📊 ADAPTIVE EXIT INSIGHTS:")
        
        if ranking_data:
            best_return = ranking_data[0] if ranking_data else None
            best_winrate = max(ranking_data, key=lambda x: x['win_rate']) if ranking_data else None
            
            if best_return:
                print(f"   🏆 Best total return: {best_return['indicator']} ({best_return['total_return']:+.1%})")
            if best_winrate:
                print(f"   🎯 Highest win rate: {best_winrate['indicator']} ({best_winrate['win_rate']:.1%})")
            
            avg_hold_time = np.mean([d['avg_hold_days'] for d in ranking_data])
            print(f"   ⏱️ Average holding period: {avg_hold_time:.1f} days")
            
            profitable_strategies = sum(1 for d in ranking_data if d['total_return'] > 0)
            print(f"   💰 Profitable strategies: {profitable_strategies}/{len(ranking_data)}")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run isolated indicator testing with adaptive exits."""
    
    tester = IsolatedIndicatorAdaptiveExit(
        k=12, 
        consensus_threshold=0.65,  # Entry threshold
        exit_threshold=0.70,       # Exit threshold (higher = less sensitive exits)
        min_expected_return=0.01, 
        transaction_cost_pct=0.001,
        min_hold_days=2            # Minimum 2 days holding
    )
    
    try:
        await tester.setup()
        
        # Test period
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        # Get stocks
        symbols = await tester.get_stock_universe(start_date, end_date, limit=15)
        
        if len(symbols) < 10:
            print("❌ Insufficient stocks")
            return
        
        # Run testing with adaptive exits
        results = await tester.test_all_indicators_adaptive(symbols, start_date, end_date)
        
        # Display results
        tester.display_adaptive_results_summary(results)
        
        print(f"\n\n🎯 ISOLATED INDICATOR ADAPTIVE EXIT TESTING COMPLETE!")
        print(f"This shows indicator performance with dynamic, KNN-based exit timing!")
        
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())