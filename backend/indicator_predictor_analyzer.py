#!/usr/bin/env python3
"""
Indicator Predictor Analyzer - Tests technical indicators in isolation to find 
the most effective predictors for price direction across multiple stocks.

This analyzer helps identify which indicators are best for KNN feature selection.
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

class IndicatorPredictorAnalyzer:
    """Analyzes the predictive power of technical indicators across multiple stocks."""
    
    def __init__(self, lookforward_days: int = 5):
        self.lookforward_days = lookforward_days  # How far ahead to predict
        self.db_conn = None
        self.indicator_engine = None
        
        # Results storage
        self.results = {
            'indicator_performance': {},
            'stock_results': {},
            'summary_stats': {}
        }
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print("🚀 Setting up Indicator Predictor Analyzer...")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register all indicators to test
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(RSI(period=7))   # Short-term RSI
        indicator_registry.register(RSI(period=21))  # Long-term RSI
        indicator_registry.register(SMA(period=10))  # Short MA
        indicator_registry.register(SMA(period=20))  # Medium MA
        indicator_registry.register(SMA(period=50))  # Long MA
        indicator_registry.register(VolumeMA(period=10))
        indicator_registry.register(VolumeMA(period=20))
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, min_days: int = 200) -> List[str]:
        """Get stocks with sufficient data for analysis."""
        query = """
        SELECT symbol, COUNT(*) as days
        FROM daily_price_data 
        WHERE date >= $1
        AND volume > 1000
        AND close_price > 0
        GROUP BY symbol
        HAVING COUNT(*) >= $2
        ORDER BY COUNT(*) DESC
        LIMIT 30
        """
        
        cutoff_date = date.today() - timedelta(days=min_days + 50)
        rows = await self.db_conn.fetch(query, cutoff_date, min_days)
        
        symbols = [row['symbol'] for row in rows]
        print(f"📊 Selected {len(symbols)} stocks for analysis: {symbols[:10]}...")
        
        return symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data for a symbol."""
        query = """
        SELECT date, open_price as open, high_price as high,
               low_price as low, close_price as close, volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df.dropna()
    
    def calculate_future_returns(self, market_data: pd.DataFrame) -> pd.Series:
        """Calculate forward-looking returns for prediction targets."""
        closes = market_data['close']
        future_returns = closes.shift(-self.lookforward_days) / closes - 1
        return future_returns
    
    def get_direction_labels(self, returns: pd.Series, threshold: float = 0.02) -> pd.Series:
        """Convert returns to directional labels (up/down/neutral)."""
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
            'correct_predictions': 0
        }
        
        # Test different RSI thresholds
        oversold_threshold = 30
        overbought_threshold = 70
        
        oversold_signals = rsi_values < oversold_threshold
        overbought_signals = rsi_values > overbought_threshold
        
        # Calculate accuracy for oversold signals (should predict 'up')
        oversold_correct = 0
        oversold_total = 0
        for date in rsi_values.index:
            if oversold_signals[date] and date in labels.index:
                oversold_total += 1
                if labels[date] == 'up':
                    oversold_correct += 1
        
        # Calculate accuracy for overbought signals (should predict 'down')
        overbought_correct = 0
        overbought_total = 0
        for date in rsi_values.index:
            if overbought_signals[date] and date in labels.index:
                overbought_total += 1
                if labels[date] == 'down':
                    overbought_correct += 1
        
        # Overall statistics
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
            'correct_predictions': 0
        }
        
        # Price above/below MA signals
        bullish_signals = prices > ma_values
        bearish_signals = prices < ma_values
        
        # Test crossovers (more specific signals)
        price_above_ma = (prices > ma_values).astype(int)
        crossovers = price_above_ma.diff()
        
        bullish_crossovers = crossovers == 1  # Price crosses above MA
        bearish_crossovers = crossovers == -1  # Price crosses below MA
        
        # Calculate accuracy for bullish crossovers
        bullish_correct = 0
        bullish_total = 0
        for date in prices.index:
            if bullish_crossovers[date] and date in labels.index:
                bullish_total += 1
                if labels[date] == 'up':
                    bullish_correct += 1
        
        # Calculate accuracy for bearish crossovers
        bearish_correct = 0
        bearish_total = 0
        for date in prices.index:
            if bearish_crossovers[date] and date in labels.index:
                bearish_total += 1
                if labels[date] == 'down':
                    bearish_correct += 1
        
        # Overall statistics
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
    
    def test_volume_signals(self, volumes: pd.Series, volume_ma: pd.Series, labels: pd.Series) -> Dict:
        """Test volume surge signals."""
        results = {
            'volume_surge_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0
        }
        
        # Volume surge signals (volume > 1.5x average)
        volume_ratio = volumes / volume_ma
        volume_surges = volume_ratio > 1.5
        
        # Test if volume surges predict price movements
        surge_correct = 0
        surge_total = 0
        for date in volumes.index:
            if volume_surges[date] and date in labels.index:
                surge_total += 1
                if labels[date] in ['up', 'down']:  # Any significant movement
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
        
        # Get market data
        market_data = await self.get_market_data(symbol, start_date, end_date)
        if market_data.empty or len(market_data) < 100:
            return {}
        
        # Calculate future returns and labels
        future_returns = self.calculate_future_returns(market_data)
        direction_labels = self.get_direction_labels(future_returns, threshold=0.03)
        
        company_id = self.get_company_id(symbol)
        stock_results = {'symbol': symbol, 'indicators': {}}
        
        # Test each indicator
        indicators_to_test = [
            'rsi_14', 'rsi_7', 'rsi_21',
            'sma_10', 'sma_20', 'sma_50',
            'volume_sma_10', 'volume_sma_20'
        ]
        
        for indicator_name in indicators_to_test:
            try:
                # Calculate indicator values
                indicator_values = await self.indicator_engine.calculate_multiple(
                    [indicator_name],
                    company_id,
                    market_data,
                    start_date,
                    end_date
                )
                
                if indicator_name not in indicator_values:
                    continue
                
                indicator_result = indicator_values[indicator_name]
                
                # Extract values from IndicatorResult
                if hasattr(indicator_result, 'values'):
                    values_dict = indicator_result.values
                    if values_dict:
                        # Convert to pandas Series
                        indicator_series = pd.Series(values_dict)
                        indicator_series.index = pd.to_datetime(indicator_series.index)
                        
                        # Test different signal types based on indicator
                        if 'rsi' in indicator_name:
                            results = self.test_rsi_signals(indicator_series, direction_labels)
                        elif 'sma' in indicator_name:
                            prices = market_data['close']
                            results = self.test_ma_signals(prices, indicator_series, direction_labels)
                        elif 'volume' in indicator_name:
                            volumes = market_data['volume']
                            results = self.test_volume_signals(volumes, indicator_series, direction_labels)
                        else:
                            continue
                        
                        stock_results['indicators'][indicator_name] = results
                        
            except Exception as e:
                print(f"     ⚠️ Error testing {indicator_name}: {e}")
                continue
        
        return stock_results
    
    def aggregate_results(self, stock_results: List[Dict]) -> Dict:
        """Aggregate results across all stocks."""
        print("\n📊 Aggregating results across all stocks...")
        
        # Initialize aggregation
        indicator_totals = defaultdict(lambda: {
            'total_signals': 0,
            'correct_predictions': 0,
            'stocks_tested': 0,
            'accuracy_sum': 0
        })
        
        # Aggregate by indicator
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
                overall_accuracy = totals['correct_predictions'] / totals['total_signals']
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
    
    async def run_analysis(self, start_date: date, end_date: date) -> Dict:
        """Run the complete indicator analysis."""
        
        print(f"\n🚀 Running Indicator Predictor Analysis")
        print(f"Period: {start_date} to {end_date}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        print(f"Direction threshold: ±3%")
        
        # Get stock universe
        symbols = await self.get_stock_universe()
        
        if len(symbols) < 5:
            print("❌ Insufficient stocks for analysis")
            return {}
        
        # Analyze each stock
        print(f"\n🔍 Analyzing {len(symbols)} stocks...")
        stock_results = []
        
        for symbol in symbols[:15]:  # Limit to 15 stocks for demo
            try:
                result = await self.analyze_stock(symbol, start_date, end_date)
                if result:
                    stock_results.append(result)
            except Exception as e:
                print(f"     ❌ Failed to analyze {symbol}: {e}")
        
        if not stock_results:
            print("❌ No successful stock analyses")
            return {}
        
        # Aggregate results
        aggregated_results = self.aggregate_results(stock_results)
        
        # Display results
        self.display_results(aggregated_results, len(stock_results))
        
        return {
            'aggregated_results': aggregated_results,
            'stock_results': stock_results,
            'analysis_params': {
                'lookforward_days': self.lookforward_days,
                'stocks_analyzed': len(stock_results),
                'period': f"{start_date} to {end_date}"
            }
        }
    
    def display_results(self, results: Dict, stocks_analyzed: int):
        """Display the analysis results in a readable format."""
        
        print(f"\n📊 INDICATOR PREDICTOR ANALYSIS RESULTS")
        print(f"=" * 70)
        print(f"Stocks analyzed: {stocks_analyzed}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        
        # Sort indicators by accuracy
        sorted_indicators = sorted(
            results.items(),
            key=lambda x: x[1]['overall_accuracy'],
            reverse=True
        )
        
        print(f"\n🏆 INDICATOR PERFORMANCE RANKING:")
        print(f"{'Indicator':<20} {'Accuracy':<10} {'Signals':<10} {'Stocks':<8} {'Avg/Stock':<10}")
        print(f"-" * 70)
        
        for indicator_name, metrics in sorted_indicators:
            accuracy = metrics['overall_accuracy']
            signals = metrics['total_signals']
            stocks = metrics['stocks_tested']
            avg_per_stock = metrics['signals_per_stock']
            
            print(f"{indicator_name:<20} {accuracy:<10.1%} {signals:<10} {stocks:<8} {avg_per_stock:<10.1f}")
        
        # Highlight top performers
        print(f"\n🥇 TOP PREDICTORS:")
        for i, (indicator_name, metrics) in enumerate(sorted_indicators[:3]):
            accuracy = metrics['overall_accuracy']
            signals = metrics['total_signals']
            print(f"  {i+1}. {indicator_name}: {accuracy:.1%} accuracy ({signals} signals)")
        
        # Insights
        print(f"\n💡 INSIGHTS:")
        
        best_indicator = sorted_indicators[0][0] if sorted_indicators else None
        best_accuracy = sorted_indicators[0][1]['overall_accuracy'] if sorted_indicators else 0
        
        if best_accuracy > 0.6:
            print(f"  ✅ Strong predictor found: {best_indicator} ({best_accuracy:.1%} accuracy)")
        elif best_accuracy > 0.55:
            print(f"  🟡 Moderate predictor: {best_indicator} ({best_accuracy:.1%} accuracy)")
        else:
            print(f"  ❌ No strong predictors found (best: {best_accuracy:.1%})")
        
        # Volume vs price indicators
        volume_indicators = [k for k in results.keys() if 'volume' in k]
        price_indicators = [k for k in results.keys() if 'volume' not in k]
        
        if volume_indicators and price_indicators:
            avg_volume_acc = np.mean([results[k]['overall_accuracy'] for k in volume_indicators])
            avg_price_acc = np.mean([results[k]['overall_accuracy'] for k in price_indicators])
            
            print(f"  📊 Volume indicators: {avg_volume_acc:.1%} avg accuracy")
            print(f"  📈 Price indicators: {avg_price_acc:.1%} avg accuracy")
        
        print(f"\n🔮 RECOMMENDATION FOR KNN:")
        top_3 = [name for name, _ in sorted_indicators[:3]]
        print(f"  Use these indicators as KNN features: {', '.join(top_3)}")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the indicator predictor analysis."""
    
    analyzer = IndicatorPredictorAnalyzer(lookforward_days=5)
    
    try:
        await analyzer.setup()
        
        # Analysis period
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)  # 6 months of data
        
        results = await analyzer.run_analysis(start_date, end_date)
        
        print("\n\n🎯 ANALYSIS COMPLETE!")
        print("This analysis helps identify the most predictive indicators for:")
        print("  • KNN feature selection")
        print("  • ML model inputs")  
        print("  • Strategy optimization")
        print("  • Risk management")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())