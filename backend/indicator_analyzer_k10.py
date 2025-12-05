#!/usr/bin/env python3
"""
Test the same indicators with K=10 neighbors instead of K=5 to see impact on accuracy.
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

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class IndicatorAnalyzerK10:
    """Tests indicator predictive power with K=10 neighbors for comparison."""
    
    def __init__(self, lookforward_days: int = 5, k_neighbors: int = 10):
        self.lookforward_days = lookforward_days
        self.k_neighbors = k_neighbors  # Changed from 5 to 10
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🚀 Setting up Indicator Analyzer with K={self.k_neighbors}...")
        
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
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date) -> List[str]:
        """Get the same first 15 stocks for direct comparison with K=5 results."""
        
        # Get stocks with data in our period
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
        LIMIT 50
        """
        
        rows = await self.db_conn.fetch(query, start_date, end_date)
        
        # Take the SAME first 15 stocks for direct comparison
        symbols = [row['symbol'] for row in rows[:15]]
        
        print(f"📊 Using same 15 stocks for K=10 comparison")
        print(f"   Stocks: {symbols}")
        
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
    
    def enhanced_signal_testing(self, indicator_values: pd.Series, labels: pd.Series, 
                               indicator_name: str, market_data: pd.DataFrame) -> Dict:
        """Enhanced signal testing that considers K=10 neighbors for averaging."""
        
        results = {
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0,
            'k_neighbors_used': self.k_neighbors
        }
        
        if indicator_name.startswith('rsi'):
            return self.test_rsi_with_knn(indicator_values, labels)
        elif 'volume' in indicator_name:
            return self.test_volume_with_knn(indicator_values, labels, market_data)
        elif 'sma' in indicator_name:
            return self.test_ma_with_knn(indicator_values, labels, market_data)
        
        return results
    
    def test_rsi_with_knn(self, rsi_values: pd.Series, labels: pd.Series) -> Dict:
        """Test RSI with K=10 neighbor averaging."""
        results = {
            'oversold_accuracy': 0,
            'overbought_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        # For each RSI value, find K similar historical RSI values
        # and use their outcomes for prediction
        
        oversold_threshold = 30
        overbought_threshold = 70
        
        total_correct = 0
        total_signals = 0
        
        for i, (idx, rsi_val) in enumerate(rsi_values.items()):
            if idx not in labels.index or pd.isna(labels[idx]) or pd.isna(rsi_val):
                continue
            
            # Find K nearest RSI values from history (before this date)
            historical_rsi = rsi_values.loc[rsi_values.index < idx]
            
            if len(historical_rsi) < self.k_neighbors:
                continue
            
            # Find K most similar RSI values
            rsi_diff = abs(historical_rsi - rsi_val)
            nearest_indices = rsi_diff.nsmallest(self.k_neighbors).index
            
            # Get their corresponding labels
            nearest_labels = labels.loc[nearest_indices].dropna()
            
            if len(nearest_labels) == 0:
                continue
            
            # Predict based on majority vote of K neighbors
            label_counts = nearest_labels.value_counts()
            predicted_label = label_counts.index[0]  # Most common label
            
            # Check if this RSI value would generate a signal
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
                # Use KNN prediction instead of simple rule
                if predicted_label == expected_direction:
                    total_correct += 1
        
        if total_signals > 0:
            results['total_accuracy'] = total_correct / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = total_correct
        
        return results
    
    def test_volume_with_knn(self, volume_ma: pd.Series, labels: pd.Series, market_data: pd.DataFrame) -> Dict:
        """Test volume with K=10 neighbor averaging."""
        results = {
            'volume_surge_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
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
        
        for i, (idx, vol_ratio) in enumerate(volume_ratios.items()):
            if idx not in labels.index or pd.isna(labels[idx]) or pd.isna(vol_ratio):
                continue
            
            # Only consider volume surges
            if vol_ratio <= 1.5:
                continue
            
            # Find K nearest volume ratios from history
            historical_ratios = volume_ratios.loc[volume_ratios.index < idx]
            
            if len(historical_ratios) < self.k_neighbors:
                continue
            
            # Find K most similar volume surge patterns
            ratio_diff = abs(historical_ratios - vol_ratio)
            nearest_indices = ratio_diff.nsmallest(self.k_neighbors).index
            
            # Get their corresponding outcomes
            nearest_labels = labels.loc[nearest_indices].dropna()
            
            if len(nearest_labels) == 0:
                continue
            
            # Predict based on K neighbors - weighted by similarity
            distances = ratio_diff.loc[nearest_indices]
            weights = 1 / (distances + 1e-8)  # Inverse distance weighting
            
            # Calculate weighted prediction
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
                
                # Predict most likely outcome
                predicted_movement = max(movement_scores.items(), key=lambda x: x[1])[0]
                actual_movement = labels[idx]
                
                total_signals += 1
                
                # Volume surges should predict ANY significant movement
                if actual_movement in ['up', 'down']:
                    if predicted_movement in ['up', 'down']:
                        total_correct += 1
                elif actual_movement == 'neutral':
                    if predicted_movement == 'neutral':
                        total_correct += 1
        
        if total_signals > 0:
            results['total_accuracy'] = total_correct / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = total_correct
        
        return results
    
    def test_ma_with_knn(self, ma_values: pd.Series, labels: pd.Series, market_data: pd.DataFrame) -> Dict:
        """Test moving average with K=10 neighbor averaging."""
        results = {
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
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
        
        for i, (idx, crossover) in enumerate(crossovers.items()):
            if idx not in labels.index or pd.isna(labels[idx]) or pd.isna(crossover):
                continue
            
            # Only consider actual crossovers
            if crossover not in [1, -1]:
                continue
            
            # Find K similar crossover patterns from history
            historical_ratios = price_ma_ratio.loc[price_ma_ratio.index < idx]
            
            if len(historical_ratios) < self.k_neighbors:
                continue
            
            current_ratio = price_ma_ratio[idx]
            ratio_diff = abs(historical_ratios - current_ratio)
            nearest_indices = ratio_diff.nsmallest(self.k_neighbors).index
            
            nearest_labels = labels.loc[nearest_indices].dropna()
            
            if len(nearest_labels) == 0:
                continue
            
            # Predict based on majority vote
            label_counts = nearest_labels.value_counts()
            predicted_label = label_counts.index[0]
            
            total_signals += 1
            
            # Expected direction based on crossover
            if crossover == 1:  # Price crosses above MA
                expected = 'up'
            else:  # Price crosses below MA
                expected = 'down'
            
            # Use KNN prediction
            if predicted_label == expected:
                total_correct += 1
        
        if total_signals > 0:
            results['total_accuracy'] = total_correct / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = total_correct
        
        return results
    
    async def analyze_stock(self, symbol: str, start_date: date, end_date: date) -> Dict:
        """Analyze all indicators for a single stock using K=10."""
        print(f"   🔍 Analyzing {symbol} with K={self.k_neighbors}...")
        
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
                    
                    # Use enhanced KNN-based testing
                    results = self.enhanced_signal_testing(
                        indicator_series, direction_labels, indicator_name, analysis_data
                    )
                    
                    stock_results['indicators'][indicator_name] = results
                    
            except Exception as e:
                print(f"     ⚠️ Error testing {indicator_name}: {str(e)}")
                continue
        
        return stock_results
    
    def aggregate_and_compare(self, stock_results: List[Dict]) -> Dict:
        """Aggregate results for K=10."""
        print(f"\n📊 Aggregating K={self.k_neighbors} results...")
        
        # Aggregate current results
        indicator_totals = defaultdict(lambda: {
            'total_signals': 0,
            'correct_predictions': 0,
            'stocks_tested': 0,
            'accuracy_sum': 0
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
        
        # Calculate final metrics
        final_results = {}
        for indicator_name, totals in indicator_totals.items():
            if totals['stocks_tested'] > 0:
                overall_accuracy = totals['correct_predictions'] / totals['total_signals'] if totals['total_signals'] > 0 else 0
                avg_accuracy = totals['accuracy_sum'] / totals['stocks_tested']
                
                final_results[indicator_name] = {
                    'overall_accuracy': overall_accuracy,
                    'average_accuracy_per_stock': avg_accuracy,
                    'total_signals': totals['total_signals'],
                    'correct_predictions': totals['correct_predictions'],
                    'stocks_tested': totals['stocks_tested'],
                    'signals_per_stock': totals['total_signals'] / totals['stocks_tested']
                }
        
        return final_results
    
    def display_k10_results(self, k10_results: Dict, stocks_analyzed: int):
        """Display K=10 results with comparison to K=5 (original)."""
        
        print(f"\n📊 K=10 NEIGHBORS ANALYSIS RESULTS")
        print(f"=" * 70)
        print(f"Stocks analyzed: {stocks_analyzed}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        print(f"K neighbors used: {self.k_neighbors}")
        
        # Original K=5 results for comparison
        k5_results = {
            'volume_sma_20': 80.8,
            'rsi_14': 39.8,
            'rsi_7': 38.4,
            'rsi_21': 37.6,
            'sma_10': 35.7,
            'sma_20': 34.1
        }
        
        # Sort indicators by K=10 accuracy
        sorted_indicators = sorted(
            k10_results.items(),
            key=lambda x: x[1]['overall_accuracy'],
            reverse=True
        )
        
        print(f"\n🏆 K=10 vs K=5 COMPARISON:")
        print(f"{'Indicator':<15} {'K=10':<10} {'K=5':<10} {'Diff':<10} {'Better?':<8} {'Signals':<10}")
        print(f"-" * 75)
        
        for indicator_name, metrics in sorted_indicators:
            k10_accuracy = metrics['overall_accuracy'] * 100
            k5_accuracy = k5_results.get(indicator_name, 0)
            diff = k10_accuracy - k5_accuracy
            signals = metrics['total_signals']
            
            # Determine if K=10 is better
            if diff > 2:
                better = "📈 Yes"
            elif diff < -2:
                better = "📉 No"
            else:
                better = "≈ Same"
            
            print(f"{indicator_name:<15} {k10_accuracy:<10.1f}% {k5_accuracy:<10.1f}% {diff:<+10.1f}% {better:<8} {signals:<10}")
        
        # Analysis summary
        print(f"\n🔍 K=10 IMPACT ANALYSIS:")
        
        improvements = 0
        degradations = 0
        
        for indicator_name, metrics in k10_results.items():
            k10_acc = metrics['overall_accuracy'] * 100
            k5_acc = k5_results.get(indicator_name, 0)
            diff = k10_acc - k5_acc
            
            if diff > 2:
                improvements += 1
            elif diff < -2:
                degradations += 1
        
        print(f"   Indicators improved with K=10: {improvements}")
        print(f"   Indicators degraded with K=10: {degradations}")
        print(f"   Indicators unchanged: {len(k10_results) - improvements - degradations}")
        
        # Volume analysis
        if 'volume_sma_20' in k10_results:
            vol_k10 = k10_results['volume_sma_20']['overall_accuracy'] * 100
            vol_k5 = k5_results['volume_sma_20']
            print(f"\n📊 VOLUME PREDICTOR ANALYSIS:")
            print(f"   K=5: {vol_k5:.1f}% accuracy")
            print(f"   K=10: {vol_k10:.1f}% accuracy")
            print(f"   Change: {vol_k10 - vol_k5:+.1f} percentage points")
            
            if vol_k10 > vol_k5:
                print(f"   ✅ K=10 improves volume prediction accuracy!")
            elif vol_k10 < vol_k5:
                print(f"   ⚠️ K=10 reduces volume prediction accuracy")
            else:
                print(f"   ≈ K=10 has similar volume prediction accuracy")
        
        # Recommendations
        print(f"\n🎯 K=10 RECOMMENDATIONS:")
        if improvements > degradations:
            print(f"   ✅ Consider using K=10: {improvements} indicators improved")
        elif degradations > improvements:
            print(f"   ❌ Stick with K=5: {degradations} indicators degraded")
        else:
            print(f"   🤷 Mixed results: Test K=7 as middle ground")
        
        print(f"\n💡 INSIGHTS:")
        print(f"   • K=10 uses more neighbors, potentially reducing noise")
        print(f"   • Larger K may be better for indicators with many signals")
        print(f"   • Smaller K may be better for rare, high-conviction signals")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run analysis with K=10 neighbors."""
    
    analyzer = IndicatorAnalyzerK10(lookforward_days=5, k_neighbors=10)
    
    try:
        await analyzer.setup()
        
        # Same analysis period and parameters
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        print(f"\n🚀 Running Indicator Analysis with K=10 Neighbors")
        print(f"Period: {start_date} to {end_date}")
        print(f"Prediction horizon: {analyzer.lookforward_days} days")
        print(f"Direction threshold: ±2%")
        print(f"K neighbors: {analyzer.k_neighbors} (vs original K=5)")
        
        # Use same first 15 stocks for direct comparison
        symbols = await analyzer.get_stock_universe(start_date, end_date)
        
        if len(symbols) < 5:
            print("❌ Insufficient stocks for analysis")
            return
        
        # Analyze each stock with K=10
        print(f"\n🔍 Analyzing {len(symbols)} stocks with K=10...")
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
        k10_results = analyzer.aggregate_and_compare(stock_results)
        analyzer.display_k10_results(k10_results, len(stock_results))
        
        print("\n\n🎯 K=10 ANALYSIS COMPLETE!")
        print("This comparison shows how the number of neighbors affects")
        print("prediction accuracy for each technical indicator.")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())