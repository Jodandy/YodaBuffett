#!/usr/bin/env python3
"""
Isolated Indicator Testing Framework

Tests individual indicators in isolation using pure KNN to find the strongest predictors.
No combinations, no complex patterns - just single metrics vs future returns.

This will help identify:
1. Which individual indicators have the most predictive power
2. Optimal parameters for each indicator
3. Best performing metrics for building ensemble strategies
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

class Outcome(NamedTuple):
    """The outcome after a pattern."""
    return_1d: float
    return_3d: float
    return_5d: float
    return_10d: float
    direction_1d: str
    direction_3d: str 
    direction_5d: str
    direction_10d: str
    max_drawdown_5d: float
    max_runup_5d: float
    entry_price_1d: float
    entry_price_3d: float
    entry_price_5d: float
    entry_price_10d: float
    exit_price_1d: float
    exit_price_3d: float
    exit_price_5d: float
    exit_price_10d: float

class IsolatedIndicatorTester:
    """Tests individual indicators in isolation using pure KNN."""
    
    def __init__(self, k: int = 10, consensus_threshold: float = 0.6, 
                 min_expected_return: float = 0.01, transaction_cost_pct: float = 0.001):
        self.k = k
        self.consensus_threshold = consensus_threshold
        self.min_expected_return = min_expected_return  # Minimum 1% expected return
        self.transaction_cost_pct = transaction_cost_pct  # 0.1% per trade (buy + sell = 0.2% total)
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🔬 Setting up Isolated Indicator Tester...")
        print(f"   K neighbors: {self.k}")
        print(f"   Consensus threshold: {self.consensus_threshold:.1%}")
        print(f"   Min expected return: {self.min_expected_return:.1%}")
        print(f"   Transaction cost: {self.transaction_cost_pct:.1%} per side ({self.transaction_cost_pct * 2:.1%} total)")
        print(f"   Testing strategy: Each indicator in complete isolation")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register core indicators for faster initial testing
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
        HAVING COUNT(*) >= 150  -- Need lots of data for indicators like SMA 200
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
        """Get market data with large buffer for long-period indicators."""
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
        
        rows = await self.db_conn.fetch(query, symbol, buffer_start, end_date + timedelta(days=15))
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
        """Calculate outcomes at multiple time horizons using next-day open as entry."""
        opens = market_data['open']
        closes = market_data['close']
        highs = market_data['high']
        lows = market_data['low']
        
        outcomes = []
        
        for i in range(len(market_data)):
            outcome_data = {'date': market_data.index[i]}
            
            # Calculate returns at different horizons
            # Entry is NEXT DAY'S OPEN (realistic trading)
            for horizon in [1, 3, 5, 10]:
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
                        continue
                    
                    return_pct = (exit_price / entry_price - 1) if entry_price > 0 else 0
                    
                    outcome_data[f'return_{horizon}d'] = return_pct
                    outcome_data[f'entry_price_{horizon}d'] = entry_price  # Store for debugging
                    outcome_data[f'exit_price_{horizon}d'] = exit_price
                    
                    # Direction (±2% threshold)
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
            
            # Calculate 5-day max drawdown and runup using realistic entry
            if i + 1 + 5 < len(market_data):
                entry_price = opens.iloc[i + 1]  # Next day's open
                future_lows = lows.iloc[i+2:i+7]  # Days after entry
                future_highs = highs.iloc[i+2:i+7]
                
                max_drawdown = (future_lows.min() / entry_price - 1) if len(future_lows) > 0 and entry_price > 0 else 0
                max_runup = (future_highs.max() / entry_price - 1) if len(future_highs) > 0 and entry_price > 0 else 0
            else:
                max_drawdown = max_runup = 0
            
            outcome_data['max_drawdown_5d'] = max_drawdown
            outcome_data['max_runup_5d'] = max_runup
            
            outcomes.append(outcome_data)
        
        outcomes_df = pd.DataFrame(outcomes)
        outcomes_df.set_index('date', inplace=True)
        return outcomes_df
    
    async def extract_single_indicator_patterns(self, symbol: str, indicator_name: str, 
                                              start_date: date, end_date: date) -> List[Tuple[SingleIndicatorPattern, Outcome]]:
        """Extract patterns for a single indicator."""
        
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
        
        # Calculate outcomes
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
                
                # Handle special indicators
                if indicator_name.startswith('macd'):
                    # For MACD, use the MACD line value
                    if hasattr(indicator_value, '__getitem__') and len(indicator_value) >= 3:
                        indicator_value = indicator_value[0]  # MACD line
                    else:
                        continue
                elif indicator_name.startswith('bb'):
                    # For Bollinger Bands, use position within bands
                    if hasattr(indicator_value, '__getitem__') and len(indicator_value) >= 3:
                        upper, middle, lower = indicator_value
                        current_price = row['close']
                        # Calculate position within bands (0 = lower band, 1 = upper band)
                        if upper != lower:
                            indicator_value = (current_price - lower) / (upper - lower)
                        else:
                            continue
                    else:
                        continue
                elif indicator_name.startswith('volume'):
                    # For volume indicators, use ratio
                    volume_ma = indicator_value
                    current_volume = row['volume']
                    if volume_ma > 0:
                        indicator_value = current_volume / volume_ma
                    else:
                        continue
                elif indicator_name.startswith('sma') or indicator_name.startswith('ema'):
                    # For moving averages, use price/MA ratio
                    ma_value = indicator_value
                    current_price = row['close']
                    if ma_value > 0:
                        indicator_value = current_price / ma_value
                    else:
                        continue
                
                # Get outcome
                if timestamp not in outcomes_df.index:
                    continue
                
                outcome_row = outcomes_df.loc[timestamp]
                
                # Skip if missing key outcome data
                if pd.isna(outcome_row['return_5d']):
                    continue
                
                # Skip if price is below $1
                current_price = float(row['close'])
                if current_price < 1.0:
                    continue
                
                pattern = SingleIndicatorPattern(
                    indicator_value=float(indicator_value),
                    date=timestamp.date(),
                    symbol=symbol,
                    price=current_price,
                    volume=float(row['volume'])
                )
                
                outcome = Outcome(
                    return_1d=float(outcome_row.get('return_1d', 0)),
                    return_3d=float(outcome_row.get('return_3d', 0)),
                    return_5d=float(outcome_row.get('return_5d', 0)),
                    return_10d=float(outcome_row.get('return_10d', 0)),
                    direction_1d=outcome_row.get('direction_1d', 'neutral'),
                    direction_3d=outcome_row.get('direction_3d', 'neutral'),
                    direction_5d=outcome_row.get('direction_5d', 'neutral'),
                    direction_10d=outcome_row.get('direction_10d', 'neutral'),
                    max_drawdown_5d=float(outcome_row.get('max_drawdown_5d', 0)),
                    max_runup_5d=float(outcome_row.get('max_runup_5d', 0)),
                    entry_price_1d=float(outcome_row.get('entry_price_1d', 0)),
                    entry_price_3d=float(outcome_row.get('entry_price_3d', 0)),
                    entry_price_5d=float(outcome_row.get('entry_price_5d', 0)),
                    entry_price_10d=float(outcome_row.get('entry_price_10d', 0)),
                    exit_price_1d=float(outcome_row.get('exit_price_1d', 0)),
                    exit_price_3d=float(outcome_row.get('exit_price_3d', 0)),
                    exit_price_5d=float(outcome_row.get('exit_price_5d', 0)),
                    exit_price_10d=float(outcome_row.get('exit_price_10d', 0))
                )
                
                patterns_and_outcomes.append((pattern, outcome))
                
            except (KeyError, IndexError, ValueError) as e:
                continue
        
        return patterns_and_outcomes
    
    def calculate_distance_1d(self, val1: float, val2: float) -> float:
        """Calculate distance between two indicator values."""
        # Simple absolute difference - can be improved per indicator type
        return abs(val1 - val2)
    
    def find_k_neighbors_1d(self, target_value: float, historical_patterns: List[Tuple[SingleIndicatorPattern, Outcome]], 
                           target_date: date, target_symbol: str) -> List[Tuple[SingleIndicatorPattern, Outcome, float]]:
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
    
    def make_prediction_1d(self, neighbors: List[Tuple[SingleIndicatorPattern, Outcome, float]], 
                          horizon: str = '5d') -> Dict:
        """Make prediction based on neighbors for specific horizon."""
        
        if not neighbors:
            return {'prediction': 'neutral', 'confidence': 0.0, 'expected_return': 0.0}
        
        # Weight by inverse distance
        total_weight = 0
        direction_votes = {'up': 0, 'down': 0, 'neutral': 0}
        weighted_returns = []
        
        for pattern, outcome, distance in neighbors:
            weight = 1 / (distance + 0.001)
            total_weight += weight
            
            # Get direction and return for specified horizon
            direction = getattr(outcome, f'direction_{horizon}')
            return_val = getattr(outcome, f'return_{horizon}')
            
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
    
    async def test_single_indicator(self, indicator_name: str, symbols: List[str], 
                                   start_date: date, end_date: date) -> Dict:
        """Test a single indicator across all stocks."""
        
        print(f"🔬 Testing {indicator_name} in isolation...")
        
        # Collect all patterns for this indicator
        all_patterns = []
        successful_stocks = 0
        
        for symbol in symbols:
            try:
                patterns = await self.extract_single_indicator_patterns(
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
        
        # Test at multiple horizons
        results = {}
        
        for horizon in ['1d', '3d', '5d', '10d']:
            trades = []
            
            for i, (test_pattern, actual_outcome) in enumerate(all_patterns):
                
                # Need sufficient historical data
                historical_patterns = all_patterns[:i]
                if len(historical_patterns) < self.k * 2:
                    continue
                
                # Find neighbors and predict
                neighbors = self.find_k_neighbors_1d(
                    test_pattern.indicator_value, historical_patterns,
                    test_pattern.date, test_pattern.symbol
                )
                
                prediction = self.make_prediction_1d(neighbors, horizon)
                
                # Only trade with high confidence AND sufficient expected return
                if (prediction['confidence'] >= self.consensus_threshold and 
                    prediction['prediction'] != 'neutral' and
                    prediction['expected_return'] >= self.min_expected_return):
                    
                    actual_direction = getattr(actual_outcome, f'direction_{horizon}')
                    actual_return = getattr(actual_outcome, f'return_{horizon}')
                    
                    # Data quality check - skip trades with unrealistic returns
                    if pd.isna(actual_return) or abs(actual_return) > 1.0:  # Skip if NaN or return > 100%
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
                    
                    # Final validation - ensure net_return is valid
                    if pd.isna(net_return):
                        continue
                    
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
                        'prediction': prediction['prediction'],
                        'confidence': prediction['confidence'],
                        'expected_return': prediction['expected_return'],
                        'actual_return': net_return,  # Net return after transaction costs
                        'gross_return': gross_return,  # Gross return before costs
                        'transaction_cost': total_transaction_cost,
                        'actual_direction': net_direction,  # Direction based on net return
                        'gross_direction': actual_direction,  # Original direction
                        'success': prediction['prediction'] == net_direction,  # Success based on net return
                        'volume': test_pattern.volume
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
                
                # Print sample trades for debugging
                if horizon == '5d' and len(trades) > 5:
                    print(f"\n     📋 Sample {horizon} trades for {indicator_name}:")
                    # Sort by actual return to show best and worst
                    sorted_trades = sorted(trades, key=lambda x: x['actual_return'], reverse=True)
                    
                    # Show best 3 trades
                    print(f"     💚 Best trades:")
                    for trade in sorted_trades[:3]:
                        print(f"        📅 Signal: {trade['date']} {trade['symbol']} (entry next day):")
                        print(f"          Signal Close=${trade.get('close_price', 0):.2f} → Entry Open=${trade['entry_price']:.2f} → Exit=${trade['exit_price']:.2f}")
                        print(f"          Indicator={trade['indicator_value']:.2f}, "
                              f"Predicted={trade['prediction']} ({trade['confidence']:.1%})")
                        print(f"          Expected={trade['expected_return']:.2%}")
                        print(f"          Gross={trade.get('gross_return', 0):.2%}, "
                              f"Costs=-{trade.get('transaction_cost', 0):.2%}, "
                              f"Net={trade['actual_return']:.2%} "
                              f"{'✅' if trade['success'] else '❌'}")
                    
                    # Show worst 3 trades
                    print(f"     💔 Worst trades:")
                    for trade in sorted_trades[-3:]:
                        print(f"        📅 Signal: {trade['date']} {trade['symbol']} (entry next day):")
                        print(f"          Signal Close=${trade.get('close_price', 0):.2f} → Entry Open=${trade['entry_price']:.2f} → Exit=${trade['exit_price']:.2f}")
                        print(f"          Indicator={trade['indicator_value']:.2f}, "
                              f"Predicted={trade['prediction']} ({trade['confidence']:.1%})")
                        print(f"          Expected={trade['expected_return']:.2%}")
                        print(f"          Gross={trade.get('gross_return', 0):.2%}, "
                              f"Costs=-{trade.get('transaction_cost', 0):.2%}, "
                              f"Net={trade['actual_return']:.2%} "
                              f"{'✅' if trade['success'] else '❌'}")
                    
                    # Show a few recent trades with normal returns
                    print(f"     📊 Recent normal trades:")
                    normal_trades = [t for t in sorted_trades if abs(t['actual_return']) < 0.20]
                    for trade in normal_trades[-3:] if len(normal_trades) >= 3 else normal_trades:
                        print(f"        📅 {trade['date']} {trade['symbol']}: "
                              f"${trade['entry_price']:.2f} → ${trade['exit_price']:.2f} "
                              f"(Net: {trade['actual_return']:+.2%}) "
                              f"{'✅' if trade['success'] else '❌'}")
                
                results[horizon] = {
                    'trades': len(trades),
                    'win_rate': win_rate,
                    'avg_return_per_trade': avg_return,
                    'total_return': total_return,
                    'best_trade': max([t['actual_return'] for t in trades]),
                    'worst_trade': min([t['actual_return'] for t in trades])
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
    
    async def test_all_indicators(self, symbols: List[str], start_date: date, end_date: date) -> Dict:
        """Test all indicators and rank by performance."""
        
        indicators_to_test = [
            'rsi_14', 'rsi_21',
            'sma_10', 'sma_20', 'sma_50', 
            'ema_10', 'ema_20',
            'volume_sma_20'
        ]
        
        print(f"\n🔬 MASS INDICATOR TESTING")
        print(f"Testing {len(indicators_to_test)} indicators on {len(symbols)} stocks")
        print(f"Period: {start_date} to {end_date}")
        print(f"Strategy: Pure KNN with K={self.k}, consensus={self.consensus_threshold:.0%}")
        print(f"Min expected return: {self.min_expected_return:.1%}")
        
        results = {}
        
        for i, indicator in enumerate(indicators_to_test, 1):
            print(f"\n📊 [{i}/{len(indicators_to_test)}] Testing {indicator}...")
            
            try:
                result = await self.test_single_indicator(indicator, symbols, start_date, end_date)
                results[indicator] = result
                
                # Quick summary
                if 'error' not in result:
                    best_horizon = max(result['results_by_horizon'].items(), 
                                     key=lambda x: x[1]['win_rate'])
                    print(f"     ✅ Best: {best_horizon[0]} - {best_horizon[1]['win_rate']:.1%} win rate, {best_horizon[1]['trades']} trades")
                else:
                    print(f"     ❌ {result['error']}")
                    
            except Exception as e:
                print(f"     ❌ Failed: {str(e)}")
                results[indicator] = {'error': str(e)}
        
        return results
    
    def display_results_summary(self, results: Dict):
        """Display comprehensive results ranking."""
        
        print(f"\n🏆 INDICATOR PERFORMANCE RANKING")
        print(f"=" * 100)
        
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
        
        # Sort by win rate
        ranking_data.sort(key=lambda x: x['win_rate'], reverse=True)
        
        print(f"🎯 RANKED BY WIN RATE:")
        print(f"{'Indicator':<15} {'Horizon':<8} {'Win Rate':<10} {'Avg Ret':<10} {'Trades':<8} {'Total Ret':<10}")
        print("-" * 80)
        
        for entry in ranking_data[:20]:  # Top 20
            print(f"{entry['indicator']:<15} {entry['horizon']:<8} "
                  f"{entry['win_rate']:<10.1%} {entry['avg_return']:<10.2%} "
                  f"{entry['trades']:<8} {entry['total_return']:<10.1%}")
        
        # Sort by total return
        ranking_data.sort(key=lambda x: x['total_return'], reverse=True)
        
        print(f"\n💰 RANKED BY TOTAL RETURN:")
        print(f"{'Indicator':<15} {'Horizon':<8} {'Win Rate':<10} {'Avg Ret':<10} {'Trades':<8} {'Total Ret':<10}")
        print("-" * 80)
        
        for entry in ranking_data[:20]:  # Top 20
            print(f"{entry['indicator']:<15} {entry['horizon']:<8} "
                  f"{entry['win_rate']:<10.1%} {entry['avg_return']:<10.2%} "
                  f"{entry['trades']:<8} {entry['total_return']:<10.1%}")
        
        # Category analysis
        print(f"\n📊 CATEGORY ANALYSIS:")
        categories = {
            'RSI': [d for d in ranking_data if d['indicator'].startswith('rsi')],
            'SMA': [d for d in ranking_data if d['indicator'].startswith('sma')],
            'EMA': [d for d in ranking_data if d['indicator'].startswith('ema')],
            'Volume': [d for d in ranking_data if d['indicator'].startswith('volume')],
            'MACD': [d for d in ranking_data if d['indicator'].startswith('macd')],
            'Bollinger': [d for d in ranking_data if d['indicator'].startswith('bb')]
        }
        
        for category, data in categories.items():
            if data:
                avg_win_rate = np.mean([d['win_rate'] for d in data])
                best_indicator = max(data, key=lambda x: x['win_rate'])
                print(f"   {category}: {avg_win_rate:.1%} avg win rate, best = {best_indicator['indicator']} ({best_indicator['win_rate']:.1%})")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run isolated indicator testing."""
    
    tester = IsolatedIndicatorTester(k=12, consensus_threshold=0.65, min_expected_return=0.01, 
                                    transaction_cost_pct=0.001)  # 0.1% per side = 0.2% total
    
    try:
        await tester.setup()
        
        # Test period
        end_date = date(2025, 11, 20)
        start_date = date(2025, 10, 1)
        
        # Get stocks
        symbols = await tester.get_stock_universe(start_date, end_date, limit=300)
        
        if len(symbols) < 10:
            print("❌ Insufficient stocks")
            return
        
        # Run mass testing
        results = await tester.test_all_indicators(symbols, start_date, end_date)
        
        # Display results
        tester.display_results_summary(results)
        
        print(f"\n\n🔬 ISOLATED INDICATOR TESTING COMPLETE!")
        print(f"This shows the predictive power of each indicator in isolation.")
        
    except Exception as e:
        print(f"❌ Testing failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await tester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())