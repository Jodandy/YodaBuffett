#!/usr/bin/env python3
"""
Pure KNN Strategy Implementation - No Arbitrary Thresholds!

This implementation uses ONLY historical patterns to make trading decisions.
No RSI < 30 rules, no volume > 1.5x rules - just pure neighbor consensus.

Key Principles:
1. For every bar, find K most similar historical patterns
2. Let neighbors vote on what happened next (up/down/neutral)
3. Trade only when neighbors have strong consensus
4. Trust the data, not traditional rules
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
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class Pattern(NamedTuple):
    """A pattern represents the state at a particular point in time."""
    rsi_14: float
    rsi_7: float
    rsi_21: float
    sma_10_ratio: float  # price / sma_10
    sma_20_ratio: float  # price / sma_20
    volume_ratio: float  # volume / volume_sma_20
    price: float
    date: date
    symbol: str

class Outcome(NamedTuple):
    """The outcome after a pattern."""
    return_5d: float
    direction: str  # 'up', 'down', 'neutral'
    max_drawdown: float  # Worst intraday loss
    max_runup: float     # Best intraday gain

class PureKNNAnalyzer:
    """Pure KNN strategy with no threshold-based rules."""
    
    def __init__(self, lookforward_days: int = 5, k: int = 10, consensus_threshold: float = 0.6):
        self.lookforward_days = lookforward_days
        self.k = k
        self.consensus_threshold = consensus_threshold  # Need 60%+ neighbor agreement to trade
        self.db_conn = None
        self.indicator_engine = None
        self.all_patterns = []  # Store all historical patterns
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🚀 Setting up Pure KNN Analyzer...")
        print(f"   K neighbors: {self.k}")
        print(f"   Consensus threshold: {self.consensus_threshold:.1%}")
        print(f"   Lookforward period: {self.lookforward_days} days")
        print(f"   Strategy: Pure pattern matching, no thresholds!")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register all indicators
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(RSI(period=7))   
        indicator_registry.register(RSI(period=21))  
        indicator_registry.register(SMA(period=10))  
        indicator_registry.register(SMA(period=20))  
        indicator_registry.register(VolumeMA(period=20))
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date) -> List[str]:
        """Get stocks with sufficient data."""
        query = """
        SELECT symbol, 
               COUNT(*) as days,
               AVG(volume::NUMERIC) as avg_volume
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2
        AND volume > 0
        AND close_price > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 100  -- Need more data for pure KNN
        ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        LIMIT 150
        """
        
        rows = await self.db_conn.fetch(query, start_date - timedelta(days=60), end_date)
        symbols = [row['symbol'] for row in rows[:50]]  # Start with 50 for testing
        
        print(f"📊 Selected {len(symbols)} stocks for Pure KNN analysis")
        print(f"   Sample stocks: {symbols[:10]}")
        
        return symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data with larger buffer for indicators."""
        buffer_start = start_date - timedelta(days=60)
        
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
        
        rows = await self.db_conn.fetch(query, symbol, buffer_start, end_date + timedelta(days=self.lookforward_days))
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        
        return df.dropna()
    
    def calculate_future_returns_detailed(self, market_data: pd.DataFrame) -> pd.DataFrame:
        """Calculate detailed future returns and risk metrics."""
        closes = market_data['close']
        highs = market_data['high'] 
        lows = market_data['low']
        
        # 5-day forward returns
        future_closes = closes.shift(-self.lookforward_days)
        returns_5d = (future_closes / closes - 1)
        
        # Calculate max drawdown and runup over next 5 days
        max_drawdowns = []
        max_runups = []
        
        for i in range(len(market_data)):
            if i + self.lookforward_days >= len(market_data):
                max_drawdowns.append(np.nan)
                max_runups.append(np.nan)
                continue
            
            entry_price = closes.iloc[i]
            future_lows = lows.iloc[i+1:i+self.lookforward_days+1]
            future_highs = highs.iloc[i+1:i+self.lookforward_days+1]
            
            if len(future_lows) > 0 and len(future_highs) > 0:
                max_drawdown = (future_lows.min() / entry_price - 1)
                max_runup = (future_highs.max() / entry_price - 1)
            else:
                max_drawdown = max_runup = np.nan
            
            max_drawdowns.append(max_drawdown)
            max_runups.append(max_runup)
        
        # Direction labels
        directions = pd.Series(index=returns_5d.index, dtype='object')
        directions[returns_5d > 0.02] = 'up'
        directions[returns_5d < -0.02] = 'down' 
        directions[abs(returns_5d) <= 0.02] = 'neutral'
        
        return pd.DataFrame({
            'return_5d': returns_5d,
            'direction': directions,
            'max_drawdown': max_drawdowns,
            'max_runup': max_runups
        })
    
    async def extract_patterns(self, symbol: str, start_date: date, end_date: date) -> List[Tuple[Pattern, Outcome]]:
        """Extract all patterns and their outcomes for a stock."""
        print(f"   📋 Extracting patterns from {symbol}...")
        
        market_data = await self.get_market_data(symbol, start_date, end_date)
        if market_data.empty or len(market_data) < 100:
            return []
        
        # Get all indicators
        company_id = self.get_company_id(symbol)
        indicators_to_calc = ['rsi_14', 'rsi_7', 'rsi_21', 'sma_10', 'sma_20', 'volume_sma_20']
        
        indicator_values = await self.indicator_engine.calculate_multiple(
            indicators_to_calc,
            company_id,
            market_data,
            start_date - timedelta(days=60),
            end_date
        )
        
        # Convert to DataFrames
        indicator_dfs = {}
        for name, result in indicator_values.items():
            if hasattr(result, 'values') and result.values:
                df = pd.DataFrame(list(result.values.items()), columns=['date', name])
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                indicator_dfs[name] = df
        
        # Calculate outcomes
        outcomes_df = self.calculate_future_returns_detailed(market_data)
        
        # Extract patterns
        patterns_and_outcomes = []
        analysis_data = market_data[(market_data.index.date >= start_date) & 
                                   (market_data.index.date <= end_date)]
        
        for timestamp, row in analysis_data.iterrows():
            trade_date = timestamp.date()
            
            # Get all indicator values for this date
            try:
                rsi_14 = indicator_dfs['rsi_14'].loc[timestamp, 'rsi_14'] if 'rsi_14' in indicator_dfs else np.nan
                rsi_7 = indicator_dfs['rsi_7'].loc[timestamp, 'rsi_7'] if 'rsi_7' in indicator_dfs else np.nan
                rsi_21 = indicator_dfs['rsi_21'].loc[timestamp, 'rsi_21'] if 'rsi_21' in indicator_dfs else np.nan
                sma_10 = indicator_dfs['sma_10'].loc[timestamp, 'sma_10'] if 'sma_10' in indicator_dfs else np.nan
                sma_20 = indicator_dfs['sma_20'].loc[timestamp, 'sma_20'] if 'sma_20' in indicator_dfs else np.nan
                volume_sma_20 = indicator_dfs['volume_sma_20'].loc[timestamp, 'volume_sma_20'] if 'volume_sma_20' in indicator_dfs else np.nan
                
                # Calculate ratios
                price = row['close']
                sma_10_ratio = price / sma_10 if not pd.isna(sma_10) and sma_10 > 0 else np.nan
                sma_20_ratio = price / sma_20 if not pd.isna(sma_20) and sma_20 > 0 else np.nan
                volume_ratio = row['volume'] / volume_sma_20 if not pd.isna(volume_sma_20) and volume_sma_20 > 0 else np.nan
                
                # Skip if any key indicators are missing
                if any(pd.isna(x) for x in [rsi_14, rsi_7, rsi_21, sma_10_ratio, sma_20_ratio, volume_ratio]):
                    continue
                
                # Get outcome
                if timestamp in outcomes_df.index:
                    outcome_row = outcomes_df.loc[timestamp]
                    if pd.isna(outcome_row['return_5d']):
                        continue
                    
                    pattern = Pattern(
                        rsi_14=float(rsi_14),
                        rsi_7=float(rsi_7), 
                        rsi_21=float(rsi_21),
                        sma_10_ratio=float(sma_10_ratio),
                        sma_20_ratio=float(sma_20_ratio),
                        volume_ratio=float(volume_ratio),
                        price=float(price),
                        date=trade_date,
                        symbol=symbol
                    )
                    
                    outcome = Outcome(
                        return_5d=float(outcome_row['return_5d']),
                        direction=outcome_row['direction'],
                        max_drawdown=float(outcome_row['max_drawdown']) if not pd.isna(outcome_row['max_drawdown']) else 0.0,
                        max_runup=float(outcome_row['max_runup']) if not pd.isna(outcome_row['max_runup']) else 0.0
                    )
                    
                    patterns_and_outcomes.append((pattern, outcome))
                    
            except (KeyError, IndexError):
                continue
        
        print(f"     📊 Extracted {len(patterns_and_outcomes)} patterns from {symbol}")
        return patterns_and_outcomes
    
    def calculate_pattern_distance(self, p1: Pattern, p2: Pattern) -> float:
        """Calculate distance between two patterns using normalized weights."""
        
        # Normalize differences by typical ranges
        rsi_diff = abs(p1.rsi_14 - p2.rsi_14) / 100.0  # RSI range 0-100
        rsi7_diff = abs(p1.rsi_7 - p2.rsi_7) / 100.0
        rsi21_diff = abs(p1.rsi_21 - p2.rsi_21) / 100.0
        
        # Price vs MA ratios (typical range 0.8 - 1.2)
        sma10_diff = abs(p1.sma_10_ratio - p2.sma_10_ratio) / 0.4
        sma20_diff = abs(p1.sma_20_ratio - p2.sma_20_ratio) / 0.4
        
        # Volume ratio (can be 0.1 to 5.0+)
        volume_diff = abs(p1.volume_ratio - p2.volume_ratio) / 2.0
        
        # Weighted combination
        distance = (
            0.3 * rsi_diff +      # RSI is important
            0.15 * rsi7_diff +    # Short-term RSI
            0.15 * rsi21_diff +   # Long-term RSI  
            0.15 * sma10_diff +   # Short-term trend
            0.15 * sma20_diff +   # Medium-term trend
            0.1 * volume_diff     # Volume confirmation
        )
        
        return distance
    
    def find_k_nearest_neighbors(self, target_pattern: Pattern, historical_patterns: List[Tuple[Pattern, Outcome]]) -> List[Tuple[Pattern, Outcome, float]]:
        """Find K nearest neighbors to the target pattern."""
        
        # Calculate distances to all historical patterns
        distances = []
        for hist_pattern, outcome in historical_patterns:
            # Don't compare pattern to itself or patterns from same stock on nearby dates
            if (hist_pattern.symbol == target_pattern.symbol and 
                abs((hist_pattern.date - target_pattern.date).days) <= 10):
                continue
            
            distance = self.calculate_pattern_distance(target_pattern, hist_pattern)
            distances.append((hist_pattern, outcome, distance))
        
        # Sort by distance and return top K
        distances.sort(key=lambda x: x[2])
        return distances[:self.k]
    
    def make_knn_prediction(self, neighbors: List[Tuple[Pattern, Outcome, float]]) -> Dict:
        """Make prediction based on K nearest neighbors."""
        
        if not neighbors:
            return {'prediction': 'neutral', 'confidence': 0.0, 'expected_return': 0.0}
        
        # Weight by inverse distance
        total_weight = 0
        direction_votes = {'up': 0, 'down': 0, 'neutral': 0}
        weighted_returns = []
        weighted_drawdowns = []
        weighted_runups = []
        
        for pattern, outcome, distance in neighbors:
            # Use inverse distance weighting (closer patterns matter more)
            weight = 1 / (distance + 0.001)  # Avoid division by zero
            total_weight += weight
            
            direction_votes[outcome.direction] += weight
            weighted_returns.append(outcome.return_5d * weight)
            weighted_drawdowns.append(outcome.max_drawdown * weight)
            weighted_runups.append(outcome.max_runup * weight)
        
        # Normalize votes
        if total_weight > 0:
            for direction in direction_votes:
                direction_votes[direction] /= total_weight
        
        # Make prediction
        predicted_direction = max(direction_votes.items(), key=lambda x: x[1])[0]
        confidence = direction_votes[predicted_direction]
        
        # Calculate expected metrics
        expected_return = sum(weighted_returns) / total_weight if total_weight > 0 else 0
        expected_drawdown = sum(weighted_drawdowns) / total_weight if total_weight > 0 else 0
        expected_runup = sum(weighted_runups) / total_weight if total_weight > 0 else 0
        
        return {
            'prediction': predicted_direction,
            'confidence': confidence,
            'expected_return': expected_return,
            'expected_drawdown': expected_drawdown,
            'expected_runup': expected_runup,
            'neighbor_count': len(neighbors),
            'direction_votes': direction_votes,
            'avg_distance': sum(d for _, _, d in neighbors) / len(neighbors)
        }
    
    async def backtest_pure_knn(self, symbols: List[str], start_date: date, end_date: date) -> Dict:
        """Backtest the pure KNN strategy."""
        
        print(f"\n🧠 Building pattern database from {len(symbols)} stocks...")
        
        # Phase 1: Extract all patterns
        all_patterns = []
        for i, symbol in enumerate(symbols):
            try:
                patterns = await self.extract_patterns(symbol, start_date, end_date)
                all_patterns.extend(patterns)
                
                if (i + 1) % 10 == 0:
                    print(f"   Progress: {i + 1}/{len(symbols)} stocks processed")
                    print(f"   Total patterns: {len(all_patterns)}")
                    
            except Exception as e:
                print(f"     ❌ Failed to process {symbol}: {str(e)}")
        
        if len(all_patterns) < 1000:
            print(f"❌ Insufficient patterns: {len(all_patterns)} (need 1000+)")
            return {}
        
        print(f"✅ Pattern database built: {len(all_patterns)} patterns")
        
        # Phase 2: Backtest using time-aware KNN
        print(f"\n🎯 Running Pure KNN backtest...")
        
        trades = []
        daily_stats = []
        
        # Sort patterns by date for time-aware testing
        all_patterns.sort(key=lambda x: x[0].date)
        
        for i, (test_pattern, actual_outcome) in enumerate(all_patterns):
            
            # Only use patterns from BEFORE this date (no look-ahead bias)
            historical_patterns = [(p, o) for p, o in all_patterns[:i] if p.date < test_pattern.date]
            
            if len(historical_patterns) < self.k * 2:  # Need enough historical data
                continue
            
            # Find neighbors and make prediction
            neighbors = self.find_k_nearest_neighbors(test_pattern, historical_patterns)
            prediction = self.make_knn_prediction(neighbors)
            
            # Only trade if we have high confidence
            if prediction['confidence'] >= self.consensus_threshold and prediction['prediction'] != 'neutral':
                
                # Record the trade
                trade = {
                    'date': test_pattern.date,
                    'symbol': test_pattern.symbol,
                    'prediction': prediction['prediction'],
                    'confidence': prediction['confidence'],
                    'expected_return': prediction['expected_return'],
                    'actual_return': actual_outcome.return_5d,
                    'actual_direction': actual_outcome.direction,
                    'success': prediction['prediction'] == actual_outcome.direction,
                    'neighbor_count': prediction['neighbor_count'],
                    'avg_distance': prediction['avg_distance']
                }
                
                trades.append(trade)
            
            # Track daily stats
            if len(trades) > 0 and (i % 100 == 0):  # Log progress every 100 patterns
                recent_trades = [t for t in trades if (test_pattern.date - t['date']).days <= 30]
                if recent_trades:
                    win_rate = sum(1 for t in recent_trades if t['success']) / len(recent_trades)
                    avg_return = np.mean([t['actual_return'] for t in recent_trades])
                    print(f"   📊 {test_pattern.date}: {len(trades)} total trades, recent 30d win rate: {win_rate:.1%}, avg return: {avg_return:.2%}")
        
        print(f"✅ Backtest complete: {len(trades)} total trades")
        
        if not trades:
            return {'error': 'No trades generated'}
        
        # Calculate performance metrics
        return self.calculate_performance_metrics(trades)
    
    def calculate_performance_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate comprehensive performance metrics."""
        
        if not trades:
            return {}
        
        # Basic stats
        total_trades = len(trades)
        winning_trades = sum(1 for t in trades if t['success'])
        win_rate = winning_trades / total_trades
        
        # Return stats
        returns = [t['actual_return'] for t in trades]
        avg_return = np.mean(returns)
        median_return = np.median(returns)
        std_return = np.std(returns)
        
        # Directional accuracy by prediction type
        up_trades = [t for t in trades if t['prediction'] == 'up']
        down_trades = [t for t in trades if t['prediction'] == 'down']
        
        up_accuracy = sum(1 for t in up_trades if t['success']) / len(up_trades) if up_trades else 0
        down_accuracy = sum(1 for t in down_trades if t['success']) / len(down_trades) if down_trades else 0
        
        # Confidence analysis
        high_conf_trades = [t for t in trades if t['confidence'] >= 0.8]
        med_conf_trades = [t for t in trades if 0.6 <= t['confidence'] < 0.8]
        
        high_conf_accuracy = sum(1 for t in high_conf_trades if t['success']) / len(high_conf_trades) if high_conf_trades else 0
        med_conf_accuracy = sum(1 for t in med_conf_trades if t['success']) / len(med_conf_trades) if med_conf_trades else 0
        
        # Risk metrics
        winning_returns = [t['actual_return'] for t in trades if t['actual_return'] > 0]
        losing_returns = [t['actual_return'] for t in trades if t['actual_return'] < 0]
        
        avg_win = np.mean(winning_returns) if winning_returns else 0
        avg_loss = np.mean(losing_returns) if losing_returns else 0
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        
        # Sharpe-like ratio (assuming daily returns)
        sharpe = avg_return / std_return if std_return > 0 else 0
        
        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return_per_trade': avg_return,
            'median_return': median_return,
            'std_return': std_return,
            'sharpe_ratio': sharpe,
            'up_trades': len(up_trades),
            'down_trades': len(down_trades), 
            'up_accuracy': up_accuracy,
            'down_accuracy': down_accuracy,
            'high_confidence_trades': len(high_conf_trades),
            'high_conf_accuracy': high_conf_accuracy,
            'medium_conf_trades': len(med_conf_trades),
            'med_conf_accuracy': med_conf_accuracy,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'total_return': sum(returns),
            'best_trade': max(returns),
            'worst_trade': min(returns)
        }
    
    def display_results(self, results: Dict):
        """Display comprehensive results."""
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
        
        print(f"\n🎯 PURE KNN STRATEGY RESULTS")
        print(f"=" * 80)
        print(f"Strategy: Pure pattern matching with K={self.k} neighbors")
        print(f"Consensus threshold: {self.consensus_threshold:.1%}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        
        print(f"\n📊 TRADING PERFORMANCE:")
        print(f"Total trades: {results['total_trades']:,}")
        print(f"Overall win rate: {results['win_rate']:.1%}")
        print(f"Average return per trade: {results['avg_return_per_trade']:.2%}")
        print(f"Total strategy return: {results['total_return']:.2%}")
        print(f"Sharpe ratio: {results['sharpe_ratio']:.2f}")
        
        print(f"\n🎯 DIRECTIONAL ACCURACY:")
        print(f"Up predictions: {results['up_trades']} trades, {results['up_accuracy']:.1%} accuracy")
        print(f"Down predictions: {results['down_trades']} trades, {results['down_accuracy']:.1%} accuracy")
        
        print(f"\n🎲 CONFIDENCE ANALYSIS:")
        print(f"High confidence (≥80%): {results['high_confidence_trades']} trades, {results['high_conf_accuracy']:.1%} accuracy")
        print(f"Medium confidence (60-80%): {results['medium_conf_trades']} trades, {results['med_conf_accuracy']:.1%} accuracy")
        
        print(f"\n💰 RISK METRICS:")
        print(f"Average win: {results['avg_win']:.2%}")
        print(f"Average loss: {results['avg_loss']:.2%}")
        print(f"Profit factor: {results['profit_factor']:.2f}")
        print(f"Best trade: {results['best_trade']:.2%}")
        print(f"Worst trade: {results['worst_trade']:.2%}")
        
        print(f"\n✨ KEY INSIGHTS:")
        if results['win_rate'] > 0.55:
            print(f"   ✅ Strong predictive power: {results['win_rate']:.1%} win rate")
        elif results['win_rate'] > 0.45:
            print(f"   ⚖️ Moderate predictive power: {results['win_rate']:.1%} win rate") 
        else:
            print(f"   ❌ Weak predictive power: {results['win_rate']:.1%} win rate")
        
        if results['high_conf_accuracy'] > results['med_conf_accuracy']:
            print(f"   🎯 Confidence system working: higher confidence = better accuracy")
        else:
            print(f"   ⚠️ Confidence system needs tuning")
        
        if results['profit_factor'] > 1.5:
            print(f"   💰 Profitable strategy: profit factor {results['profit_factor']:.2f}")
        elif results['profit_factor'] > 1.0:
            print(f"   💼 Marginally profitable: profit factor {results['profit_factor']:.2f}")
        else:
            print(f"   📉 Unprofitable strategy: profit factor {results['profit_factor']:.2f}")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run Pure KNN analysis."""
    
    analyzer = PureKNNAnalyzer(lookforward_days=5, k=15, consensus_threshold=0.65)
    
    try:
        await analyzer.setup()
        
        # Analysis period
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        print(f"\n🧠 PURE KNN STRATEGY ANALYSIS")
        print(f"Period: {start_date} to {end_date}")
        print(f"No arbitrary thresholds - pure pattern matching!")
        
        # Get stock universe
        symbols = await analyzer.get_stock_universe(start_date, end_date)
        
        if len(symbols) < 10:
            print("❌ Insufficient stocks for analysis")
            return
        
        # Run backtest
        results = await analyzer.backtest_pure_knn(symbols, start_date, end_date)
        
        # Display results
        analyzer.display_results(results)
        
        print(f"\n\n🎯 PURE KNN ANALYSIS COMPLETE!")
        print(f"This strategy uses ONLY historical patterns - no traditional rules!")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())