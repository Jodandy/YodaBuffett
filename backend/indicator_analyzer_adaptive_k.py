#!/usr/bin/env python3
"""
Test indicators with ADAPTIVE K that grows based on available historical data.
K = sqrt(historical_data_length) for optimal bias-variance tradeoff.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
import json
from collections import defaultdict
import hashlib
import math

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class AdaptiveKAnalyzer:
    """Tests indicator predictive power with adaptive K based on data size."""
    
    def __init__(self, lookforward_days: int = 5, min_k: int = 3, max_k: int = 20):
        self.lookforward_days = lookforward_days
        self.min_k = min_k  # Minimum K value
        self.max_k = max_k  # Maximum K value (prevent overly large K)
        self.db_conn = None
        self.indicator_engine = None
        self.k_stats = {}  # Track K values used
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🚀 Setting up Adaptive K Analyzer...")
        print(f"   K formula: sqrt(historical_data_length)")
        print(f"   K range: {self.min_k} to {self.max_k}")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Same indicators as before
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(RSI(period=7))   
        indicator_registry.register(RSI(period=21))  
        indicator_registry.register(SMA(period=10))  
        indicator_registry.register(SMA(period=20))  
        indicator_registry.register(VolumeMA(period=20))
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        print("✅ Setup complete!")
    
    def calculate_adaptive_k(self, historical_length: int) -> int:
        """Calculate optimal K based on historical data length."""
        # K = sqrt(n) is a common rule for bias-variance tradeoff
        k = int(math.sqrt(historical_length))
        
        # Apply bounds
        k = max(self.min_k, min(k, self.max_k))
        
        return k
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date) -> List[str]:
        """Get the same first 15 stocks for comparison."""
        
        query = """
        SELECT symbol, 
               COUNT(*) as days,
               AVG(volume::NUMERIC) as avg_volume
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2
        AND volume > 0
        AND close_price > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 50
        ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        LIMIT 150
        """
        
        rows = await self.db_conn.fetch(query, start_date, end_date)
        symbols = [row['symbol'] for row in rows[:100]]
        
        print(f"📊 Using 100 stocks for Adaptive K analysis")
        print(f"   Stocks: {symbols[:10]}... (showing first 10)")
        
        return symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data for a symbol with buffer for indicators."""
        buffer_start = start_date - timedelta(days=30)
        
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
    
    def calculate_future_returns(self, market_data: pd.DataFrame) -> pd.Series:
        """Calculate forward-looking returns."""
        closes = market_data['close']
        future_returns = closes.shift(-self.lookforward_days) / closes - 1
        return future_returns
    
    def get_direction_labels(self, returns: pd.Series, threshold: float = 0.02) -> pd.Series:
        """Convert returns to directional labels."""
        labels = pd.Series(index=returns.index, dtype='object')
        labels[returns > threshold] = 'up'
        labels[returns < -threshold] = 'down'
        labels[abs(returns) <= threshold] = 'neutral'
        return labels
    
    def adaptive_signal_testing(self, indicator_values: pd.Series, labels: pd.Series, 
                               indicator_name: str, market_data: pd.DataFrame) -> Dict:
        """Enhanced signal testing with adaptive K."""
        
        results = {
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0,
            'avg_k_used': 0,
            'min_k_used': float('inf'),
            'max_k_used': 0,
            'k_usage_stats': []
        }
        
        if indicator_name.startswith('rsi'):
            return self.test_rsi_adaptive_k(indicator_values, labels, results)
        elif 'volume' in indicator_name:
            return self.test_volume_adaptive_k(indicator_values, labels, market_data, results)
        elif 'sma' in indicator_name:
            return self.test_ma_adaptive_k(indicator_values, labels, market_data, results)
        
        return results
    
    def test_rsi_adaptive_k(self, rsi_values: pd.Series, labels: pd.Series, results: Dict) -> Dict:
        """Test RSI with adaptive K."""
        
        oversold_threshold = 30
        overbought_threshold = 70
        
        total_correct = 0
        total_signals = 0
        k_values_used = []
        
        for i, (idx, rsi_val) in enumerate(rsi_values.items()):
            if idx not in labels.index or pd.isna(labels[idx]) or pd.isna(rsi_val):
                continue
            
            # Get historical RSI values (before this date)
            historical_rsi = rsi_values.loc[rsi_values.index < idx]
            
            if len(historical_rsi) < self.min_k:
                continue
            
            # Calculate adaptive K based on historical data available
            k = self.calculate_adaptive_k(len(historical_rsi))
            k_values_used.append(k)
            
            # Find K most similar RSI values
            rsi_diff = abs(historical_rsi - rsi_val)
            nearest_indices = rsi_diff.nsmallest(k).index
            
            # Get their corresponding labels
            nearest_labels = labels.loc[nearest_indices].dropna()
            
            if len(nearest_labels) == 0:
                continue
            
            # Check if this RSI value generates a signal
            signal_generated = False
            expected_direction = None
            
            if rsi_val < oversold_threshold:
                signal_generated = True
                expected_direction = 'up'
            elif rsi_val > overbought_threshold:
                signal_generated = True
                expected_direction = 'down'
            
            if signal_generated:
                total_signals += 1
                
                # Weighted prediction based on similarity
                distances = rsi_diff.loc[nearest_indices]
                weights = 1 / (distances + 1e-8)
                
                # Calculate weighted votes
                vote_scores = {'up': 0, 'down': 0, 'neutral': 0}
                total_weight = 0
                
                for neighbor_idx, weight in weights.items():
                    if neighbor_idx in labels.index and pd.notna(labels[neighbor_idx]):
                        vote_scores[labels[neighbor_idx]] += weight
                        total_weight += weight
                
                if total_weight > 0:
                    # Normalize and predict
                    for key in vote_scores:
                        vote_scores[key] /= total_weight
                    
                    predicted_label = max(vote_scores.items(), key=lambda x: x[1])[0]
                    
                    if predicted_label == expected_direction:
                        total_correct += 1
        
        # Update results
        if total_signals > 0:
            results['total_accuracy'] = total_correct / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = total_correct
        
        if k_values_used:
            results['avg_k_used'] = np.mean(k_values_used)
            results['min_k_used'] = min(k_values_used)
            results['max_k_used'] = max(k_values_used)
            results['k_usage_stats'] = k_values_used
        
        return results
    
    def test_volume_adaptive_k(self, volume_ma: pd.Series, labels: pd.Series, 
                               market_data: pd.DataFrame, results: Dict) -> Dict:
        """Test volume with adaptive K."""
        
        # Get actual volumes
        volumes = market_data['volume']
        common_idx = volumes.index.intersection(volume_ma.index).intersection(labels.index)
        volumes = volumes.loc[common_idx]
        volume_ma_values = volume_ma.loc[common_idx]
        labels = labels.loc[common_idx]
        
        # Calculate volume ratios
        volume_ratios = volumes / volume_ma_values
        
        total_correct = 0
        total_signals = 0
        k_values_used = []
        
        for i, (idx, vol_ratio) in enumerate(volume_ratios.items()):
            if idx not in labels.index or pd.isna(labels[idx]) or pd.isna(vol_ratio):
                continue
            
            # Only consider volume surges
            if vol_ratio <= 1.5:
                continue
            
            # Get historical volume ratios
            historical_ratios = volume_ratios.loc[volume_ratios.index < idx]
            
            if len(historical_ratios) < self.min_k:
                continue
            
            # Calculate adaptive K
            k = self.calculate_adaptive_k(len(historical_ratios))
            k_values_used.append(k)
            
            # Find K most similar volume surge patterns
            ratio_diff = abs(historical_ratios - vol_ratio)
            nearest_indices = ratio_diff.nsmallest(k).index
            
            # Get their corresponding outcomes
            nearest_labels = labels.loc[nearest_indices].dropna()
            
            if len(nearest_labels) == 0:
                continue
            
            total_signals += 1
            
            # Weighted prediction
            distances = ratio_diff.loc[nearest_indices]
            weights = 1 / (distances + 1e-8)
            
            movement_scores = {'up': 0, 'down': 0, 'neutral': 0}
            total_weight = 0
            
            for neighbor_idx, weight in weights.items():
                if neighbor_idx in labels.index and pd.notna(labels[neighbor_idx]):
                    movement_scores[labels[neighbor_idx]] += weight
                    total_weight += weight
            
            if total_weight > 0:
                # Normalize scores
                for key in movement_scores:
                    movement_scores[key] /= total_weight
                
                predicted_movement = max(movement_scores.items(), key=lambda x: x[1])[0]
                actual_movement = labels[idx]
                
                # Volume surges should predict ANY significant movement
                if actual_movement in ['up', 'down']:
                    if predicted_movement in ['up', 'down']:
                        total_correct += 1
                elif actual_movement == 'neutral':
                    if predicted_movement == 'neutral':
                        total_correct += 1
        
        # Update results
        if total_signals > 0:
            results['total_accuracy'] = total_correct / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = total_correct
        
        if k_values_used:
            results['avg_k_used'] = np.mean(k_values_used)
            results['min_k_used'] = min(k_values_used)
            results['max_k_used'] = max(k_values_used)
            results['k_usage_stats'] = k_values_used
        
        return results
    
    def test_ma_adaptive_k(self, ma_values: pd.Series, labels: pd.Series, 
                           market_data: pd.DataFrame, results: Dict) -> Dict:
        """Test moving average with adaptive K."""
        
        prices = market_data['close']
        common_idx = prices.index.intersection(ma_values.index).intersection(labels.index)
        prices = prices.loc[common_idx]
        ma_values = ma_values.loc[common_idx]
        labels = labels.loc[common_idx]
        
        # Calculate price relative to MA
        price_ma_ratio = prices / ma_values
        
        # Detect crossovers
        price_above_ma = (prices > ma_values).astype(int)
        crossovers = price_above_ma.diff()
        
        total_correct = 0
        total_signals = 0
        k_values_used = []
        
        for i, (idx, crossover) in enumerate(crossovers.items()):
            if idx not in labels.index or pd.isna(labels[idx]) or pd.isna(crossover):
                continue
            
            # Only consider actual crossovers
            if crossover not in [1, -1]:
                continue
            
            # Get historical price/MA ratios
            historical_ratios = price_ma_ratio.loc[price_ma_ratio.index < idx]
            
            if len(historical_ratios) < self.min_k:
                continue
            
            # Calculate adaptive K
            k = self.calculate_adaptive_k(len(historical_ratios))
            k_values_used.append(k)
            
            # Find K similar patterns
            current_ratio = price_ma_ratio[idx]
            ratio_diff = abs(historical_ratios - current_ratio)
            nearest_indices = ratio_diff.nsmallest(k).index
            
            nearest_labels = labels.loc[nearest_indices].dropna()
            
            if len(nearest_labels) == 0:
                continue
            
            total_signals += 1
            
            # Weighted prediction
            distances = ratio_diff.loc[nearest_indices]
            weights = 1 / (distances + 1e-8)
            
            vote_scores = {'up': 0, 'down': 0, 'neutral': 0}
            total_weight = 0
            
            for neighbor_idx, weight in weights.items():
                if neighbor_idx in labels.index and pd.notna(labels[neighbor_idx]):
                    vote_scores[labels[neighbor_idx]] += weight
                    total_weight += weight
            
            if total_weight > 0:
                for key in vote_scores:
                    vote_scores[key] /= total_weight
                
                predicted_label = max(vote_scores.items(), key=lambda x: x[1])[0]
                
                # Expected direction based on crossover
                if crossover == 1:  # Price crosses above MA
                    expected = 'up'
                else:  # Price crosses below MA
                    expected = 'down'
                
                if predicted_label == expected:
                    total_correct += 1
        
        # Update results
        if total_signals > 0:
            results['total_accuracy'] = total_correct / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = total_correct
        
        if k_values_used:
            results['avg_k_used'] = np.mean(k_values_used)
            results['min_k_used'] = min(k_values_used)
            results['max_k_used'] = max(k_values_used)
            results['k_usage_stats'] = k_values_used
        
        return results
    
    async def analyze_stock(self, symbol: str, start_date: date, end_date: date) -> Dict:
        """Analyze all indicators for a single stock using adaptive K."""
        print(f"   🔍 Analyzing {symbol} with Adaptive K...")
        
        market_data = await self.get_market_data(symbol, start_date, end_date)
        if market_data.empty or len(market_data) < 50:
            return {}
        
        analysis_data = market_data[(market_data.index.date >= start_date) & 
                                   (market_data.index.date <= end_date)]
        
        future_returns = self.calculate_future_returns(analysis_data)
        direction_labels = self.get_direction_labels(future_returns, threshold=0.02)
        
        company_id = self.get_company_id(symbol)
        stock_results = {'symbol': symbol, 'indicators': {}}
        
        indicators_to_test = ['rsi_14', 'rsi_7', 'rsi_21', 'sma_10', 'sma_20', 'volume_sma_20']
        
        for indicator_name in indicators_to_test:
            try:
                indicator_values = await self.indicator_engine.calculate_multiple(
                    [indicator_name],
                    company_id,
                    market_data,
                    start_date - timedelta(days=30),
                    end_date
                )
                
                if indicator_name not in indicator_values:
                    continue
                
                indicator_result = indicator_values[indicator_name]
                
                if hasattr(indicator_result, 'values') and indicator_result.values:
                    indicator_series = pd.Series(indicator_result.values)
                    indicator_series.index = pd.to_datetime(indicator_series.index)
                    
                    indicator_series = indicator_series[(indicator_series.index.date >= start_date) & 
                                                       (indicator_series.index.date <= end_date)]
                    
                    # Use adaptive K testing
                    results = self.adaptive_signal_testing(
                        indicator_series, direction_labels, indicator_name, analysis_data
                    )
                    
                    stock_results['indicators'][indicator_name] = results
                    
            except Exception as e:
                print(f"     ⚠️ Error testing {indicator_name}: {str(e)}")
                continue
        
        return stock_results
    
    def aggregate_and_compare(self, stock_results: List[Dict]) -> Dict:
        """Aggregate results for adaptive K."""
        print(f"\n📊 Aggregating Adaptive K results...")
        
        # Aggregate current results
        indicator_totals = defaultdict(lambda: {
            'total_signals': 0,
            'correct_predictions': 0,
            'stocks_tested': 0,
            'accuracy_sum': 0,
            'k_stats': []
        })
        
        for stock_result in stock_results:
            if not stock_result or 'indicators' not in stock_result:
                continue
            
            for indicator_name, results in stock_result['indicators'].items():
                if 'total_accuracy' in results and results['total_signals'] > 0:
                    totals = indicator_totals[indicator_name]
                    totals['total_signals'] += results['total_signals']
                    totals['correct_predictions'] += results['correct_predictions']
                    totals['stocks_tested'] += 1
                    totals['accuracy_sum'] += results['total_accuracy']
                    
                    # Collect K statistics
                    if 'avg_k_used' in results:
                        totals['k_stats'].append({
                            'avg_k': results['avg_k_used'],
                            'min_k': results['min_k_used'],
                            'max_k': results['max_k_used']
                        })
        
        # Calculate final metrics
        final_results = {}
        for indicator_name, totals in indicator_totals.items():
            if totals['stocks_tested'] > 0:
                overall_accuracy = totals['correct_predictions'] / totals['total_signals'] if totals['total_signals'] > 0 else 0
                avg_accuracy = totals['accuracy_sum'] / totals['stocks_tested']
                
                # Calculate K statistics
                if totals['k_stats']:
                    avg_k_overall = np.mean([stat['avg_k'] for stat in totals['k_stats']])
                    min_k_overall = min([stat['min_k'] for stat in totals['k_stats']])
                    max_k_overall = max([stat['max_k'] for stat in totals['k_stats']])
                else:
                    avg_k_overall = min_k_overall = max_k_overall = 0
                
                final_results[indicator_name] = {
                    'overall_accuracy': overall_accuracy,
                    'average_accuracy_per_stock': avg_accuracy,
                    'total_signals': totals['total_signals'],
                    'correct_predictions': totals['correct_predictions'],
                    'stocks_tested': totals['stocks_tested'],
                    'signals_per_stock': totals['total_signals'] / totals['stocks_tested'],
                    'avg_k_used': avg_k_overall,
                    'min_k_used': min_k_overall,
                    'max_k_used': max_k_overall
                }
        
        return final_results
    
    def display_adaptive_results(self, adaptive_results: Dict, stocks_analyzed: int):
        """Display adaptive K results with comparison."""
        
        print(f"\n📊 ADAPTIVE K ANALYSIS RESULTS")
        print(f"=" * 80)
        print(f"Stocks analyzed: {stocks_analyzed}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        print(f"K formula: sqrt(historical_data_length)")
        print(f"K bounds: {self.min_k} ≤ K ≤ {self.max_k}")
        
        # Fixed K results for comparison
        fixed_k_results = {
            'K=5': {'volume_sma_20': 80.8, 'rsi_14': 39.8, 'rsi_7': 38.4, 'rsi_21': 37.6, 'sma_10': 35.7, 'sma_20': 34.1},
            # Add K=10 results when available
        }
        
        # Sort indicators by adaptive K accuracy
        sorted_indicators = sorted(
            adaptive_results.items(),
            key=lambda x: x[1]['overall_accuracy'],
            reverse=True
        )
        
        print(f"\n🏆 ADAPTIVE K PERFORMANCE:")
        print(f"{'Indicator':<15} {'Accuracy':<10} {'Avg K':<8} {'K Range':<12} {'Signals':<10}")
        print(f"-" * 80)
        
        for indicator_name, metrics in sorted_indicators:
            accuracy = metrics['overall_accuracy'] * 100
            avg_k = metrics['avg_k_used']
            k_range = f"{int(metrics['min_k_used'])}-{int(metrics['max_k_used'])}"
            signals = metrics['total_signals']
            
            print(f"{indicator_name:<15} {accuracy:<10.1f}% {avg_k:<8.1f} {k_range:<12} {signals:<10}")
        
        # Comparison with fixed K
        print(f"\n📈 ADAPTIVE K vs FIXED K COMPARISON:")
        print(f"{'Indicator':<15} {'Adaptive':<10} {'K=5':<10} {'Improvement':<12} {'K Used':<10}")
        print(f"-" * 80)
        
        for indicator_name, metrics in adaptive_results.items():
            adaptive_acc = metrics['overall_accuracy'] * 100
            k5_acc = fixed_k_results['K=5'].get(indicator_name, 0)
            improvement = adaptive_acc - k5_acc
            avg_k = metrics['avg_k_used']
            
            # Format improvement
            if improvement > 2:
                imp_str = f"+{improvement:.1f}% ✅"
            elif improvement < -2:
                imp_str = f"{improvement:.1f}% ❌"
            else:
                imp_str = f"{improvement:.1f}% ≈"
            
            print(f"{indicator_name:<15} {adaptive_acc:<10.1f}% {k5_acc:<10.1f}% {imp_str:<12} {avg_k:<10.1f}")
        
        # Analysis insights
        print(f"\n🔍 ADAPTIVE K INSIGHTS:")
        
        improvements = sum(1 for name, metrics in adaptive_results.items() 
                          if (metrics['overall_accuracy'] * 100 - fixed_k_results['K=5'].get(name, 0)) > 2)
        
        print(f"   • {improvements}/{len(adaptive_results)} indicators improved with Adaptive K")
        
        # Volume analysis
        if 'volume_sma_20' in adaptive_results:
            vol_metrics = adaptive_results['volume_sma_20']
            vol_adaptive = vol_metrics['overall_accuracy'] * 100
            vol_k5 = fixed_k_results['K=5']['volume_sma_20']
            vol_avg_k = vol_metrics['avg_k_used']
            
            print(f"\n🔊 VOLUME PREDICTOR DEEP DIVE:")
            print(f"   • Adaptive K: {vol_adaptive:.1f}% accuracy (avg K={vol_avg_k:.1f})")
            print(f"   • Fixed K=5: {vol_k5:.1f}% accuracy")
            print(f"   • Improvement: {vol_adaptive - vol_k5:+.1f} percentage points")
            print(f"   • K range used: {int(vol_metrics['min_k_used'])}-{int(vol_metrics['max_k_used'])}")
        
        # K usage patterns
        print(f"\n📊 K USAGE PATTERNS:")
        for indicator_name, metrics in sorted_indicators[:3]:  # Top 3
            avg_k = metrics['avg_k_used']
            k_range = f"{int(metrics['min_k_used'])}-{int(metrics['max_k_used'])}"
            print(f"   • {indicator_name}: avg K={avg_k:.1f}, range={k_range}")
        
        # Recommendations
        print(f"\n🎯 RECOMMENDATIONS:")
        if improvements >= len(adaptive_results) // 2:
            print(f"   ✅ Use Adaptive K: {improvements} indicators improved")
            print(f"   💡 Formula: K = sqrt(historical_data_length)")
            print(f"   💡 Bounds: {self.min_k} ≤ K ≤ {self.max_k}")
        else:
            print(f"   🤔 Mixed results: Consider hybrid approach")
            print(f"   💡 Use Adaptive K for indicators that improved")
            print(f"   💡 Use Fixed K=5 for others")
        
        print(f"\n📈 THEORETICAL BENEFITS OF ADAPTIVE K:")
        print(f"   • Early periods: Small K (less overfitting)")
        print(f"   • Later periods: Larger K (more stable predictions)")
        print(f"   • Automatic bias-variance optimization")
        print(f"   • Scales naturally with dataset growth")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run analysis with adaptive K."""
    
    analyzer = AdaptiveKAnalyzer(lookforward_days=5, min_k=3, max_k=20)
    
    try:
        await analyzer.setup()
        
        # Same analysis period
        end_date = date(2025, 10, 31)
        start_date = date(2024, 5, 1)
        
        print(f"\n🚀 Running Indicator Analysis with ADAPTIVE K")
        print(f"Period: {start_date} to {end_date}")
        print(f"Prediction horizon: {analyzer.lookforward_days} days")
        print(f"Direction threshold: ±2%")
        
        # Use same first 15 stocks
        symbols = await analyzer.get_stock_universe(start_date, end_date)
        
        if len(symbols) < 5:
            print("❌ Insufficient stocks for analysis")
            return
        
        # Analyze each stock with adaptive K
        print(f"\n🔍 Analyzing {len(symbols)} stocks with Adaptive K...")
        stock_results = []
        
        for i, symbol in enumerate(symbols):
            try:
                result = await analyzer.analyze_stock(symbol, start_date, end_date)
                if result:
                    stock_results.append(result)
                if (i + 1) % 5 == 0:
                    print(f"   Progress: {i + 1}/{len(symbols)} stocks analyzed")
            except Exception as e:
                print(f"     ❌ Failed to analyze {symbol}: {str(e)}")
        
        if not stock_results:
            print("❌ No successful stock analyses")
            return
        
        # Aggregate and display results
        adaptive_results = analyzer.aggregate_and_compare(stock_results)
        analyzer.display_adaptive_results(adaptive_results, len(stock_results))
        
        print("\n\n🎯 ADAPTIVE K ANALYSIS COMPLETE!")
        print("This shows how dynamically adjusting K based on available")
        print("historical data affects prediction accuracy!")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())