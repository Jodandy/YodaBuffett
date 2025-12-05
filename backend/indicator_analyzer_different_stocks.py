#!/usr/bin/env python3
"""
Test the same indicators on a different set of 15 companies to validate findings.
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

class IndicatorValidationAnalyzer:
    """Validates indicator predictive power on different stocks."""
    
    def __init__(self, lookforward_days: int = 5):
        self.lookforward_days = lookforward_days
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print("🚀 Setting up Indicator Validation Analyzer...")
        
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
    
    async def get_different_stock_universe(self, start_date: date, end_date: date, skip_first: int = 15) -> List[str]:
        """Get a DIFFERENT set of stocks - skip the first 15 we already tested."""
        
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
        
        # Skip the first N stocks that were already tested
        symbols = [row['symbol'] for row in rows[skip_first:skip_first+15]]
        
        print(f"📊 Selected {len(symbols)} DIFFERENT stocks for validation")
        print(f"   Validation set: {symbols}")
        
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
    
    def test_rsi_signals(self, rsi_values: pd.Series, labels: pd.Series) -> Dict:
        """Test RSI indicator signals."""
        results = {
            'oversold_accuracy': 0,
            'overbought_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        oversold_threshold = 30
        overbought_threshold = 70
        
        oversold_signals = rsi_values < oversold_threshold
        overbought_signals = rsi_values > overbought_threshold
        
        oversold_correct = 0
        oversold_total = 0
        overbought_correct = 0
        overbought_total = 0
        
        for idx in rsi_values.index:
            if idx in labels.index and pd.notna(labels[idx]):
                if oversold_signals[idx]:
                    oversold_total += 1
                    if labels[idx] == 'up':
                        oversold_correct += 1
                if overbought_signals[idx]:
                    overbought_total += 1
                    if labels[idx] == 'down':
                        overbought_correct += 1
        
        total_signals = oversold_total + overbought_total
        correct_predictions = oversold_correct + overbought_correct
        
        if total_signals > 0:
            results['total_accuracy'] = correct_predictions / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = correct_predictions
        
        if oversold_total > 0:
            results['oversold_accuracy'] = oversold_correct / oversold_total
        if overbought_total > 0:
            results['overbought_accuracy'] = overbought_correct / overbought_total
        
        results['oversold_signals'] = oversold_total
        results['overbought_signals'] = overbought_total
        
        return results
    
    def test_ma_signals(self, prices: pd.Series, ma_values: pd.Series, labels: pd.Series) -> Dict:
        """Test moving average crossover signals."""
        results = {
            'bullish_accuracy': 0,
            'bearish_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        common_idx = prices.index.intersection(ma_values.index).intersection(labels.index)
        prices = prices.loc[common_idx]
        ma_values = ma_values.loc[common_idx]
        labels = labels.loc[common_idx]
        
        price_above_ma = (prices > ma_values).astype(int)
        crossovers = price_above_ma.diff()
        
        bullish_crossovers = crossovers == 1
        bearish_crossovers = crossovers == -1
        
        bullish_correct = 0
        bullish_total = 0
        bearish_correct = 0
        bearish_total = 0
        
        for idx in crossovers.index:
            if pd.notna(labels[idx]) and pd.notna(crossovers[idx]):
                if bullish_crossovers[idx]:
                    bullish_total += 1
                    if labels[idx] == 'up':
                        bullish_correct += 1
                elif bearish_crossovers[idx]:
                    bearish_total += 1
                    if labels[idx] == 'down':
                        bearish_correct += 1
        
        total_signals = bullish_total + bearish_total
        correct_predictions = bullish_correct + bearish_correct
        
        if total_signals > 0:
            results['total_accuracy'] = correct_predictions / total_signals
            results['total_signals'] = total_signals
            results['correct_predictions'] = correct_predictions
        
        if bullish_total > 0:
            results['bullish_accuracy'] = bullish_correct / bullish_total
        if bearish_total > 0:
            results['bearish_accuracy'] = bearish_correct / bearish_total
        
        results['bullish_signals'] = bullish_total
        results['bearish_signals'] = bearish_total
        
        return results
    
    def test_volume_signals(self, volumes: pd.Series, volume_ma: pd.Series, labels: pd.Series, prices: pd.Series) -> Dict:
        """Test volume surge signals."""
        results = {
            'volume_surge_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        common_idx = volumes.index.intersection(volume_ma.index).intersection(labels.index)
        volumes = volumes.loc[common_idx]
        volume_ma = volume_ma.loc[common_idx]
        labels = labels.loc[common_idx]
        
        volume_ratio = volumes / volume_ma
        volume_surges = volume_ratio > 1.5
        
        surge_correct = 0
        surge_total = 0
        
        for idx in volume_surges.index:
            if pd.notna(labels[idx]) and volume_surges[idx]:
                surge_total += 1
                if labels[idx] in ['up', 'down']:
                    surge_correct += 1
        
        if surge_total > 0:
            results['volume_surge_accuracy'] = surge_correct / surge_total
            results['total_signals'] = surge_total
            results['correct_predictions'] = surge_correct
            results['total_accuracy'] = surge_correct / surge_total
        
        return results
    
    async def analyze_stock(self, symbol: str, start_date: date, end_date: date) -> Dict:
        """Analyze all indicators for a single stock."""
        print(f"   🔍 Analyzing {symbol}...")
        
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
                    
                    if 'rsi' in indicator_name:
                        results = self.test_rsi_signals(indicator_series, direction_labels)
                    elif 'sma' in indicator_name and 'volume' not in indicator_name:
                        prices = analysis_data['close']
                        results = self.test_ma_signals(prices, indicator_series, direction_labels)
                    elif 'volume' in indicator_name:
                        volumes = analysis_data['volume']
                        prices = analysis_data['close']
                        results = self.test_volume_signals(volumes, indicator_series, direction_labels, prices)
                    else:
                        continue
                    
                    stock_results['indicators'][indicator_name] = results
                    
            except Exception as e:
                print(f"     ⚠️ Error testing {indicator_name}: {str(e)}")
                continue
        
        return stock_results
    
    def aggregate_and_compare(self, stock_results: List[Dict], first_run_results: Dict) -> Dict:
        """Aggregate results and compare with first run."""
        print("\n📊 Aggregating validation results...")
        
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
    
    def display_comparison_results(self, validation_results: Dict, stocks_analyzed: int):
        """Display the validation results with comparison to first run."""
        
        print(f"\n📊 VALIDATION RESULTS (Different 15 Stocks)")
        print(f"=" * 70)
        print(f"Stocks analyzed: {stocks_analyzed}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        
        # First run results for comparison
        first_run = {
            'volume_sma_20': 80.8,
            'rsi_14': 39.8,
            'rsi_7': 38.4,
            'rsi_21': 37.6,
            'sma_10': 35.7,
            'sma_20': 34.1
        }
        
        # Sort indicators by accuracy
        sorted_indicators = sorted(
            validation_results.items(),
            key=lambda x: x[1]['overall_accuracy'],
            reverse=True
        )
        
        print(f"\n🏆 VALIDATION SET PERFORMANCE:")
        print(f"{'Indicator':<15} {'Val.Acc':<10} {'1st.Acc':<10} {'Diff':<10} {'Signals':<10}")
        print(f"-" * 70)
        
        for indicator_name, metrics in sorted_indicators:
            val_accuracy = metrics['overall_accuracy'] * 100
            first_accuracy = first_run.get(indicator_name, 0)
            diff = val_accuracy - first_accuracy
            signals = metrics['total_signals']
            
            # Color coding for difference
            diff_str = f"{diff:+.1f}%"
            if abs(diff) < 5:
                consistency = "✅"  # Consistent
            elif diff > 0:
                consistency = "📈"  # Better
            else:
                consistency = "📉"  # Worse
            
            print(f"{indicator_name:<15} {val_accuracy:<10.1f}% {first_accuracy:<10.1f}% {diff_str:<10} {signals:<10} {consistency}")
        
        # Statistical summary
        print(f"\n📊 CONSISTENCY ANALYSIS:")
        
        for indicator_name, metrics in sorted_indicators:
            val_acc = metrics['overall_accuracy'] * 100
            first_acc = first_run.get(indicator_name, 0)
            
            if indicator_name == 'volume_sma_20':
                print(f"\n🔍 Volume Analysis:")
                print(f"   First 15 stocks: {first_acc:.1f}%")
                print(f"   Next 15 stocks: {val_acc:.1f}%")
                print(f"   Average: {(first_acc + val_acc) / 2:.1f}%")
                
                if val_acc > 70:
                    print(f"   ✅ CONFIRMED: Volume is a strong predictor across different stocks!")
                elif val_acc > 60:
                    print(f"   🟡 Volume still predictive but varies by stock set")
                else:
                    print(f"   ❌ Volume effectiveness varies significantly")
        
        # Overall recommendations
        print(f"\n🎯 FINAL RECOMMENDATIONS:")
        
        # Find consistently good indicators
        consistent_good = []
        for indicator_name, metrics in validation_results.items():
            val_acc = metrics['overall_accuracy'] * 100
            first_acc = first_run.get(indicator_name, 0)
            avg_acc = (val_acc + first_acc) / 2
            
            if avg_acc > 50 and abs(val_acc - first_acc) < 10:
                consistent_good.append((indicator_name, avg_acc))
        
        if consistent_good:
            print(f"\nConsistently Good Indicators:")
            for ind, acc in sorted(consistent_good, key=lambda x: x[1], reverse=True):
                print(f"  • {ind}: {acc:.1f}% average accuracy")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run validation on different stocks."""
    
    analyzer = IndicatorValidationAnalyzer(lookforward_days=5)
    
    try:
        await analyzer.setup()
        
        # Same analysis period
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        print(f"\n🚀 Running Indicator Validation on DIFFERENT Stocks")
        print(f"Period: {start_date} to {end_date}")
        print(f"Prediction horizon: {analyzer.lookforward_days} days")
        print(f"Direction threshold: ±2%")
        
        # Get different set of stocks (skip first 15)
        symbols = await analyzer.get_different_stock_universe(start_date, end_date, skip_first=15)
        
        if len(symbols) < 5:
            print("❌ Insufficient stocks for validation")
            return
        
        # Analyze each stock
        print(f"\n🔍 Analyzing {len(symbols)} different stocks...")
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
        
        # Aggregate and compare
        validation_results = analyzer.aggregate_and_compare(stock_results, None)
        analyzer.display_comparison_results(validation_results, len(stock_results))
        
        print("\n\n🎯 VALIDATION COMPLETE!")
        print("\n📌 ABOUT K VALUE IN KNN:")
        print("The current KNN strategy uses K=5 neighbors by default.")
        print("This means it looks at the 5 most similar historical patterns")
        print("to predict future price movements.")
        print("\nYou can adjust K in the KNN strategy configuration.")
        print("Typical values: K=3 (more responsive), K=5 (balanced), K=7+ (smoother)")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())