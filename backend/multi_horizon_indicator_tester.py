#!/usr/bin/env python3
"""
Multi-Horizon Indicator Testing Framework

Uses multiple time horizons (1, 2, 3, 5, 8, 13, 21 days) for both:
1. Feature labeling: Each pattern has outcomes at ALL horizons
2. Buy decision making: KNN considers neighbors' performance across ALL horizons
3. Exit evaluation: Fixed exits at each specific horizon

This gives much richer pattern matching by incorporating multiple timeframes
into the buy decision while still evaluating performance at each specific horizon.
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

class MultiHorizonPattern(NamedTuple):
    """A pattern with outcomes across multiple time horizons."""
    indicator_value: float
    date: date
    symbol: str
    price: float
    volume: float

class MultiHorizonOutcome(NamedTuple):
    """Outcomes across all Fibonacci time horizons."""
    return_1d: float
    return_2d: float
    return_3d: float
    return_5d: float
    return_8d: float
    return_13d: float
    return_21d: float
    direction_1d: str
    direction_2d: str
    direction_3d: str
    direction_5d: str
    direction_8d: str
    direction_13d: str
    direction_21d: str
    entry_price_1d: float
    entry_price_2d: float
    entry_price_3d: float
    entry_price_5d: float
    entry_price_8d: float
    entry_price_13d: float
    entry_price_21d: float
    exit_price_1d: float
    exit_price_2d: float
    exit_price_3d: float
    exit_price_5d: float
    exit_price_8d: float
    exit_price_13d: float
    exit_price_21d: float

class MultiHorizonIndicatorTester:
    """Tests individual indicators using multi-horizon KNN for buy decisions."""
    
    def __init__(self, k: int = 10, consensus_threshold: float = 0.6, 
                 min_expected_return: float = 0.01, transaction_cost_pct: float = 0.001):
        self.k = k
        self.consensus_threshold = consensus_threshold
        self.min_expected_return = min_expected_return
        self.transaction_cost_pct = transaction_cost_pct
        self.horizons = [1, 2, 3, 5, 8, 13, 21]  # Fibonacci sequence
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🔬 Setting up Multi-Horizon Indicator Tester...")
        print(f"   K neighbors: {self.k}")
        print(f"   Consensus threshold: {self.consensus_threshold:.1%}")
        print(f"   Min expected return: {self.min_expected_return:.1%}")
        print(f"   Transaction cost: {self.transaction_cost_pct:.1%} per side ({self.transaction_cost_pct * 2:.1%} total)")
        print(f"   Horizons: {self.horizons} days (Fibonacci sequence)")
        print(f"   Strategy: Multi-horizon KNN for buy decisions, fixed exits for evaluation")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register core indicators - focusing on EMA carpet bombing around 10
        indicators_to_test = [
            ('ema_8', EMA(period=8)),
            ('ema_9', EMA(period=9)),
            ('ema_10', EMA(period=10)),
            ('ema_11', EMA(period=11)),
            ('ema_12', EMA(period=12)),
            ('ema_13', EMA(period=13)),
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
        """Get market data with buffer for long horizons."""
        buffer_start = start_date - timedelta(days=250)  # Big buffer for indicators
        buffer_end = end_date + timedelta(days=30)       # Buffer for longest horizon (21d)
        
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
        
        rows = await self.db_conn.fetch(query, symbol, buffer_start, buffer_end)
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        return df.dropna()
    
    def calculate_multi_horizon_outcomes(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate outcomes at ALL Fibonacci horizons using next-day open as entry."""
        opens = market_data['open']
        closes = market_data['close']
        highs = market_data['high']
        lows = market_data['low']
        
        outcomes = []
        
        for i in range(len(market_data)):
            outcome_data = {'date': market_data.index[i]}
            
            # Calculate returns at ALL horizons
            # Entry is NEXT DAY'S OPEN (realistic trading)
            for horizon in self.horizons:
                if i + 1 + horizon < len(market_data):  # Need next day for entry
                    entry_price = opens.iloc[i + 1]  # Next day's open = realistic entry
                    exit_price = closes.iloc[i + 1 + horizon]  # Close after horizon days
                    
                    # Skip if gap between close and next open is too large (>20%)
                    current_close = closes.iloc[i]
                    next_open = opens.iloc[i + 1]
                    gap = abs(next_open / current_close - 1) if current_close > 0 else 0
                    
                    if gap > 0.20:  # Skip if >20% gap (earnings, news, etc.)
                        outcome_data[f'return_{horizon}d'] = np.nan
                        outcome_data[f'direction_{horizon}d'] = 'neutral'
                        outcome_data[f'entry_price_{horizon}d'] = np.nan
                        outcome_data[f'exit_price_{horizon}d'] = np.nan
                        continue
                    
                    return_pct = (exit_price / entry_price - 1) if entry_price > 0 else 0
                    
                    outcome_data[f'return_{horizon}d'] = return_pct
                    outcome_data[f'entry_price_{horizon}d'] = entry_price
                    outcome_data[f'exit_price_{horizon}d'] = exit_price
                    
                    # Direction classification (±2% threshold)
                    if return_pct > 0.02:
                        direction = 'up'
                    elif return_pct < -0.02:
                        direction = 'down'
                    else:
                        direction = 'neutral'
                    
                    outcome_data[f'direction_{horizon}d'] = direction
                else:
                    outcome_data[f'return_{horizon}d'] = np.nan
                    outcome_data[f'direction_{horizon}d'] = 'neutral'
                    outcome_data[f'entry_price_{horizon}d'] = np.nan
                    outcome_data[f'exit_price_{horizon}d'] = np.nan
            
            outcomes.append(outcome_data)
        
        outcomes_df = pd.DataFrame(outcomes)
        outcomes_df.set_index('date', inplace=True)
        return outcomes_df
    
    async def extract_multi_horizon_patterns(self, symbol: str, indicator_name: str, 
                                           start_date: date, end_date: date) -> List[Tuple[MultiHorizonPattern, MultiHorizonOutcome]]:
        """Extract patterns with multi-horizon outcomes."""
        
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
        
        # Calculate multi-horizon outcomes
        outcomes_df = self.calculate_multi_horizon_outcomes(market_data)
        
        # Extract patterns
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
                
                # Handle special indicators (same as before)
                if indicator_name.startswith('macd'):
                    if hasattr(indicator_value, '__getitem__') and len(indicator_value) >= 3:
                        indicator_value = indicator_value[0]  # MACD line
                    else:
                        continue
                elif indicator_name.startswith('bb'):
                    if hasattr(indicator_value, '__getitem__') and len(indicator_value) >= 3:
                        upper, middle, lower = indicator_value
                        current_price = row['close']
                        if upper != lower:
                            indicator_value = (current_price - lower) / (upper - lower)
                        else:
                            continue
                    else:
                        continue
                elif indicator_name.startswith('volume'):
                    volume_ma = indicator_value
                    current_volume = row['volume']
                    if volume_ma > 0:
                        indicator_value = current_volume / volume_ma
                    else:
                        continue
                elif indicator_name.startswith('sma') or indicator_name.startswith('ema'):
                    ma_value = indicator_value
                    current_price = row['close']
                    if ma_value > 0:
                        indicator_value = current_price / ma_value
                    else:
                        continue
                
                # Get outcome for this timestamp
                if timestamp not in outcomes_df.index:
                    continue
                
                outcome_row = outcomes_df.loc[timestamp]
                
                # Skip if missing key outcome data (need at least 5d return)
                if pd.isna(outcome_row.get('return_5d')):
                    continue
                
                # Skip if price is below $1
                current_price = float(row['close'])
                if current_price < 1.0:
                    continue
                
                pattern = MultiHorizonPattern(
                    indicator_value=float(indicator_value),
                    date=timestamp.date(),
                    symbol=symbol,
                    price=current_price,
                    volume=float(row['volume'])
                )
                
                # Create multi-horizon outcome
                outcome = MultiHorizonOutcome(
                    return_1d=float(outcome_row.get('return_1d', 0)),
                    return_2d=float(outcome_row.get('return_2d', 0)),
                    return_3d=float(outcome_row.get('return_3d', 0)),
                    return_5d=float(outcome_row.get('return_5d', 0)),
                    return_8d=float(outcome_row.get('return_8d', 0)),
                    return_13d=float(outcome_row.get('return_13d', 0)),
                    return_21d=float(outcome_row.get('return_21d', 0)),
                    direction_1d=outcome_row.get('direction_1d', 'neutral'),
                    direction_2d=outcome_row.get('direction_2d', 'neutral'),
                    direction_3d=outcome_row.get('direction_3d', 'neutral'),
                    direction_5d=outcome_row.get('direction_5d', 'neutral'),
                    direction_8d=outcome_row.get('direction_8d', 'neutral'),
                    direction_13d=outcome_row.get('direction_13d', 'neutral'),
                    direction_21d=outcome_row.get('direction_21d', 'neutral'),
                    entry_price_1d=float(outcome_row.get('entry_price_1d', 0)),
                    entry_price_2d=float(outcome_row.get('entry_price_2d', 0)),
                    entry_price_3d=float(outcome_row.get('entry_price_3d', 0)),
                    entry_price_5d=float(outcome_row.get('entry_price_5d', 0)),
                    entry_price_8d=float(outcome_row.get('entry_price_8d', 0)),
                    entry_price_13d=float(outcome_row.get('entry_price_13d', 0)),
                    entry_price_21d=float(outcome_row.get('entry_price_21d', 0)),
                    exit_price_1d=float(outcome_row.get('exit_price_1d', 0)),
                    exit_price_2d=float(outcome_row.get('exit_price_2d', 0)),
                    exit_price_3d=float(outcome_row.get('exit_price_3d', 0)),
                    exit_price_5d=float(outcome_row.get('exit_price_5d', 0)),
                    exit_price_8d=float(outcome_row.get('exit_price_8d', 0)),
                    exit_price_13d=float(outcome_row.get('exit_price_13d', 0)),
                    exit_price_21d=float(outcome_row.get('exit_price_21d', 0))
                )
                
                patterns_and_outcomes.append((pattern, outcome))
                
            except (KeyError, IndexError, ValueError) as e:
                continue
        
        return patterns_and_outcomes
    
    def calculate_distance_1d(self, val1: float, val2: float) -> float:
        """Calculate distance between two indicator values."""
        return abs(val1 - val2)
    
    def find_k_neighbors_1d(self, target_value: float, historical_patterns: List[Tuple[MultiHorizonPattern, MultiHorizonOutcome]], 
                           target_date: date, target_symbol: str) -> List[Tuple[MultiHorizonPattern, MultiHorizonOutcome, float]]:
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
    
    def make_multi_horizon_prediction(self, neighbors: List[Tuple[MultiHorizonPattern, MultiHorizonOutcome, float]], 
                                    target_horizon: str = '5d') -> Dict:
        """Make prediction based on neighbors considering ALL horizons but targeting specific horizon."""
        
        if not neighbors:
            return {'prediction': 'neutral', 'confidence': 0.0, 'expected_return': 0.0}
        
        # Weight by inverse distance
        total_weight = 0
        direction_votes = {'up': 0, 'down': 0, 'neutral': 0}
        weighted_returns = []
        
        # Multi-horizon scoring: consider performance across all horizons
        horizon_weights = {
            '1d': 0.05,   # Very short term - low weight
            '2d': 0.10,   # Short term
            '3d': 0.15,   # Short-medium term
            '5d': 0.25,   # Medium term - higher weight for target
            '8d': 0.20,   # Medium-long term
            '13d': 0.15,  # Long term
            '21d': 0.10   # Very long term
        }
        
        for pattern, outcome, distance in neighbors:
            base_weight = 1 / (distance + 0.001)
            total_weight += base_weight
            
            # Calculate weighted scores across all horizons
            combined_return = 0
            combined_direction_score = {'up': 0, 'down': 0, 'neutral': 0}
            
            for horizon_str in [f'{h}d' for h in self.horizons]:
                return_attr = f'return_{horizon_str}'
                direction_attr = f'direction_{horizon_str}'
                
                horizon_return = getattr(outcome, return_attr, 0)
                horizon_direction = getattr(outcome, direction_attr, 'neutral')
                horizon_weight = horizon_weights.get(horizon_str, 0.1)
                
                # Boost weight for target horizon
                if horizon_str == target_horizon:
                    horizon_weight *= 2.0
                
                combined_return += horizon_return * horizon_weight
                combined_direction_score[horizon_direction] += horizon_weight
            
            # Normalize direction scores
            total_direction_weight = sum(combined_direction_score.values())
            if total_direction_weight > 0:
                for key in combined_direction_score:
                    combined_direction_score[key] /= total_direction_weight
            
            # Vote based on combined multi-horizon analysis
            best_direction = max(combined_direction_score.items(), key=lambda x: x[1])[0]
            direction_votes[best_direction] += base_weight
            weighted_returns.append(combined_return * base_weight)
        
        # Normalize votes
        if total_weight > 0:
            for direction in direction_votes:
                direction_votes[direction] /= total_weight
        
        # Make final prediction
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
    
    async def test_single_indicator_multi_horizon(self, indicator_name: str, symbols: List[str], 
                                                 start_date: date, end_date: date) -> Dict:
        """Test a single indicator with multi-horizon decision making."""
        
        print(f"🔬 Testing {indicator_name} with multi-horizon KNN...")
        
        # Collect all patterns for this indicator
        all_patterns = []
        successful_stocks = 0
        
        for symbol in symbols:
            try:
                patterns = await self.extract_multi_horizon_patterns(
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
        
        # First, make ALL buy decisions using multi-horizon analysis (targeting 5d as default)
        buy_decisions = []
        
        for i, (test_pattern, actual_outcome) in enumerate(all_patterns):
            
            # Need sufficient historical data
            historical_patterns = all_patterns[:i]
            if len(historical_patterns) < self.k * 2:
                continue
            
            # Skip if this pattern doesn't have valid 5d outcome data (we need at least this for buy decision)
            actual_return_5d = getattr(actual_outcome, 'return_5d')
            if pd.isna(actual_return_5d) or abs(actual_return_5d) > 1.0:
                continue
            
            # Find neighbors and predict using multi-horizon analysis (targeting 5d for buy decision)
            neighbors = self.find_k_neighbors_1d(
                test_pattern.indicator_value, historical_patterns,
                test_pattern.date, test_pattern.symbol
            )
            
            prediction = self.make_multi_horizon_prediction(neighbors, '5d')  # Use 5d as baseline for buy decisions
            
            # Only make buy decision with high confidence AND sufficient expected return
            if (prediction['confidence'] >= self.consensus_threshold and 
                prediction['prediction'] != 'neutral' and
                prediction['expected_return'] >= self.min_expected_return):
                
                # Store the buy decision
                buy_decisions.append({
                    'pattern': test_pattern,
                    'outcome': actual_outcome,
                    'prediction': prediction['prediction'],
                    'confidence': prediction['confidence'],
                    'expected_return': prediction['expected_return']
                })
        
        print(f"     🎯 Made {len(buy_decisions)} buy decisions using multi-horizon analysis")
        
        # Now evaluate these SAME buy decisions at all different exit horizons
        results = {}
        
        for horizon in [f'{h}d' for h in self.horizons]:
            trades = []
            
            for decision in buy_decisions:
                test_pattern = decision['pattern']
                actual_outcome = decision['outcome']
                
                # Get actual outcome for this specific exit horizon
                actual_return = getattr(actual_outcome, f'return_{horizon}')
                actual_direction = getattr(actual_outcome, f'direction_{horizon}')
                
                # Skip if this horizon doesn't have valid data
                if pd.isna(actual_return) or abs(actual_return) > 1.0:
                    continue
                
                # Get actual entry and exit prices
                actual_entry_price = getattr(actual_outcome, f'entry_price_{horizon}', None)
                actual_exit_price = getattr(actual_outcome, f'exit_price_{horizon}', None)
                
                # Fallback to calculated values if missing
                if actual_entry_price is None or actual_entry_price == 0:
                    actual_entry_price = test_pattern.price  # Use signal close as fallback
                if actual_exit_price is None or actual_exit_price == 0:
                    actual_exit_price = test_pattern.price * (1 + actual_return)  # Calculate from return
                
                # Calculate return after transaction costs
                gross_return = actual_return
                total_transaction_cost = self.transaction_cost_pct * 2  # Buy + Sell
                net_return = gross_return - total_transaction_cost
                
                # Update direction based on net return (transaction costs can flip profitable trades)
                if net_return > 0.02:
                    net_direction = 'up'
                elif net_return < -0.02:
                    net_direction = 'down'
                else:
                    net_direction = 'neutral'
                
                trade = {
                    'date': test_pattern.date,  # Date when indicator was calculated
                    'symbol': test_pattern.symbol,
                    'indicator_value': test_pattern.indicator_value,
                    'close_price': test_pattern.price,  # Close price on indicator day
                    'entry_price': actual_entry_price,  # Next day's open
                    'exit_price': actual_exit_price,   # Close after horizon days
                    'prediction': decision['prediction'],  # Same prediction for all horizons
                    'confidence': decision['confidence'],  # Same confidence for all horizons
                    'expected_return': decision['expected_return'],  # Same expected return for all horizons
                    'actual_return': net_return,  # Net return after transaction costs
                    'gross_return': gross_return,  # Gross return before costs
                    'transaction_cost': total_transaction_cost,
                    'actual_direction': net_direction,  # Direction based on net return
                    'gross_direction': actual_direction,  # Original direction
                    'success': decision['prediction'] == net_direction,  # Success based on net return
                    'volume': test_pattern.volume,
                    'horizon': horizon
                }
                
                trades.append(trade)
            
            # Calculate metrics for this horizon
            if trades:
                win_rate = sum(1 for t in trades if t['success']) / len(trades)
                
                # Filter out any NaN returns before calculating metrics
                valid_returns = [t['actual_return'] for t in trades if not pd.isna(t['actual_return'])]
                
                if valid_returns:
                    avg_return = np.mean(valid_returns)
                    total_return = sum(valid_returns)
                else:
                    avg_return = 0.0
                    total_return = 0.0
                
                # Print sample trades for 5d horizon
                if horizon == '5d' and len(trades) > 5:
                    print(f"     📋 Sample {horizon} multi-horizon trades for {indicator_name}:")
                    sorted_trades = sorted(trades, key=lambda x: x['actual_return'], reverse=True)
                    
                    for trade in sorted_trades[:3]:
                        print(f"        📅 Signal: {trade['date']} {trade['symbol']} (exit {horizon}):")
                        print(f"          Entry=${trade['entry_price']:.2f} → Exit=${trade['exit_price']:.2f}")
                        print(f"          Multi-horizon predicted={trade['prediction']} ({trade['confidence']:.1%})")
                        print(f"          Expected={trade['expected_return']:.2%}, Net={trade['actual_return']:.2%} "
                              f"{'✅' if trade['success'] else '❌'}")
                
                results[horizon] = {
                    'trades': len(trades),
                    'win_rate': win_rate,
                    'avg_return_per_trade': avg_return,
                    'total_return': total_return,
                    'best_trade': max(valid_returns) if valid_returns else 0,
                    'worst_trade': min(valid_returns) if valid_returns else 0
                }
            else:
                results[horizon] = {
                    'trades': 0,
                    'win_rate': 0,
                    'avg_return_per_trade': 0,
                    'total_return': 0
                }
        
        return {
            'indicator': indicator_name,
            'total_patterns': len(all_patterns),
            'successful_stocks': successful_stocks,
            'results_by_horizon': results
        }
    
    async def test_all_indicators_multi_horizon(self, symbols: List[str], start_date: date, end_date: date) -> Dict:
        """Test all indicators with multi-horizon decision making."""
        
        indicators_to_test = [
            'ema_10'
        ]
        
        print(f"\n🎯 EMA CARPET BOMBING - MULTI-HORIZON TESTING")
        print(f"Testing EMA periods {indicators_to_test} on {len(symbols)} stocks")
        print(f"Period: {start_date} to {end_date}")
        print(f"Strategy: Multi-horizon KNN buy decisions, fixed exits")
        print(f"Buy decision uses ALL horizons: {self.horizons}")
        print(f"Evaluation horizons: {self.horizons}")
        print(f"🎯 Goal: Find optimal EMA period around 8-13")
        
        results = {}
        
        for i, indicator in enumerate(indicators_to_test, 1):
            print(f"\n📊 [{i}/{len(indicators_to_test)}] Testing {indicator}...")
            
            try:
                result = await self.test_single_indicator_multi_horizon(indicator, symbols, start_date, end_date)
                results[indicator] = result
                
                # Quick summary - show best horizon
                if 'error' not in result:
                    best_horizon = max(result['results_by_horizon'].items(), 
                                     key=lambda x: x[1]['total_return'] if x[1]['trades'] > 0 else -999)
                    print(f"     ✅ Best: {best_horizon[0]} - {best_horizon[1]['win_rate']:.1%} win rate, "
                          f"{best_horizon[1]['total_return']:+.1%} total return, {best_horizon[1]['trades']} trades")
                else:
                    print(f"     ❌ {result['error']}")
                    
            except Exception as e:
                print(f"     ❌ Failed: {str(e)}")
                results[indicator] = {'error': str(e)}
        
        return results
    
    def display_multi_horizon_results(self, results: Dict):
        """Display multi-horizon results grouped by indicator."""
        
        print(f"\n🏆 MULTI-HORIZON STRATEGY PERFORMANCE BY INDICATOR")
        print(f"=" * 120)
        
        # Create ranking table
        ranking_data = []
        
        for indicator, result in results.items():
            if 'error' in result:
                continue
            
            for horizon, metrics in result['results_by_horizon'].items():
                if metrics['trades'] > 0:
                    ranking_data.append({
                        'indicator': indicator,
                        'horizon': horizon,
                        'win_rate': metrics['win_rate'],
                        'avg_return': metrics['avg_return_per_trade'],
                        'total_return': metrics['total_return'],
                        'trades': metrics['trades']
                    })
        
        if not ranking_data:
            print("❌ No successful results to display")
            return
        
        # Group by indicator
        indicators_data = defaultdict(list)
        for entry in ranking_data:
            indicators_data[entry['indicator']].append(entry)
        
        # Sort indicators by their best total return
        indicator_best_returns = []
        for indicator, entries in indicators_data.items():
            best_return = max(entry['total_return'] for entry in entries)
            indicator_best_returns.append((indicator, best_return))
        indicator_best_returns.sort(key=lambda x: x[1], reverse=True)
        
        # Display each indicator with all horizons
        for indicator, _ in indicator_best_returns:
            entries = indicators_data[indicator]
            # Sort horizons in order: 1d, 2d, 3d, 5d, 8d, 13d, 21d
            horizon_order = ['1d', '2d', '3d', '5d', '8d', '13d', '21d']
            entries_dict = {entry['horizon']: entry for entry in entries}
            
            print(f"\n📊 {indicator.upper()}:")
            print(f"{'Horizon':<8} {'Win Rate':<10} {'Avg Ret':<10} {'Total Ret':<12} {'Trades':<8}")
            print("-" * 60)
            
            for horizon in horizon_order:
                if horizon in entries_dict:
                    entry = entries_dict[horizon]
                    # Highlight best and worst performers
                    total_ret_str = f"{entry['total_return']:+.1%}"
                    if entry['total_return'] > 100:
                        total_ret_str = f"🟢 {total_ret_str}"
                    elif entry['total_return'] < 0:
                        total_ret_str = f"🔴 {total_ret_str}"
                    else:
                        total_ret_str = f"🟡 {total_ret_str}"
                    
                    print(f"{horizon:<8} {entry['win_rate']:<10.1%} "
                          f"{entry['avg_return']:<10.2%} {total_ret_str:<12} {entry['trades']:<8}")
                else:
                    print(f"{horizon:<8} {'N/A':<10} {'N/A':<10} {'N/A':<12} {'0':<8}")
        
        # Summary statistics
        print(f"\n📊 HORIZON PERFORMANCE SUMMARY:")
        horizon_performance = defaultdict(list)
        horizon_trades = defaultdict(list)
        for entry in ranking_data:
            horizon_performance[entry['horizon']].append(entry['total_return'])
            horizon_trades[entry['horizon']].append(entry['trades'])
        
        print(f"{'Horizon':<8} {'Avg Return':<12} {'Profitable':<12} {'Avg Trades':<12}")
        print("-" * 50)
        for horizon in [f'{h}d' for h in self.horizons]:
            if horizon in horizon_performance:
                avg_return = np.mean(horizon_performance[horizon])
                profitable = sum(1 for r in horizon_performance[horizon] if r > 0)
                total_strategies = len(horizon_performance[horizon])
                avg_trades = np.mean(horizon_trades[horizon]) if horizon in horizon_trades else 0
                
                avg_return_str = f"{avg_return:+.1%}"
                if avg_return > 50:
                    avg_return_str = f"🟢 {avg_return_str}"
                elif avg_return < 0:
                    avg_return_str = f"🔴 {avg_return_str}"
                else:
                    avg_return_str = f"🟡 {avg_return_str}"
                
                print(f"{horizon:<8} {avg_return_str:<12} {profitable}/{total_strategies:<11} {avg_trades:<12.0f}")
        
        # Best performers
        print(f"\n🏆 TOP PERFORMERS:")
        ranking_data.sort(key=lambda x: x['total_return'], reverse=True)
        best_5 = ranking_data[:5]
        
        print(f"{'Rank':<5} {'Strategy':<20} {'Horizon':<8} {'Total Return':<12} {'Win Rate':<10} {'Trades':<8}")
        print("-" * 75)
        for i, entry in enumerate(best_5, 1):
            print(f"{i:<5} {entry['indicator']:<20} {entry['horizon']:<8} "
                  f"{entry['total_return']:+.1%}     {entry['win_rate']:.1%}    {entry['trades']}")
        
        # Key insights
        print(f"\n✨ KEY INSIGHTS:")
        profitable_count = sum(1 for entry in ranking_data if entry['total_return'] > 0)
        print(f"   💰 Profitable strategies: {profitable_count}/{len(ranking_data)} ({profitable_count/len(ranking_data):.1%})")
        
        avg_fibonacci_return = np.mean([entry['total_return'] for entry in ranking_data])
        print(f"   📈 Average strategy return: {avg_fibonacci_return:+.2%}")
        
        best_indicator = max(indicators_data.items(), key=lambda x: max(e['total_return'] for e in x[1]))
        print(f"   🥇 Best indicator: {best_indicator[0]} (best return: {max(e['total_return'] for e in best_indicator[1]):+.1%})")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run multi-horizon indicator testing."""
    
    tester = MultiHorizonIndicatorTester(
        k=12, 
        consensus_threshold=0.65, 
        min_expected_return=0.01, 
        transaction_cost_pct=0.001
    )
    
    try:
        await tester.setup()
        
        # Test period
        end_date = date(2025, 10, 31)
        start_date = date(2024, 5, 1)
        
        # Get stocks
        symbols = await tester.get_stock_universe(start_date, end_date, limit=50)
        
        if len(symbols) < 10:
            print("❌ Insufficient stocks")
            return
        
        # Run multi-horizon testing
        results = await tester.test_all_indicators_multi_horizon(symbols, start_date, end_date)
        
        # Display results
        tester.display_multi_horizon_results(results)
        
        print(f"\n\n🎯 MULTI-HORIZON TESTING COMPLETE!")
        print(f"Buy decisions used ALL horizons {tester.horizons} for richer pattern matching!")
        
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())