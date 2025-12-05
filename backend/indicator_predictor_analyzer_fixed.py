#!/usr/bin/env python3
"""
Fixed Indicator Predictor Analyzer - Tests technical indicators in isolation to find 
the most effective predictors for price direction across multiple stocks.
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
        self.lookforward_days = lookforward_days
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
        
        # Register indicators to test
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(RSI(period=7))   # Short-term RSI
        indicator_registry.register(RSI(period=21))  # Long-term RSI
        indicator_registry.register(SMA(period=10))  # Short MA
        indicator_registry.register(SMA(period=20))  # Medium MA
        indicator_registry.register(VolumeMA(period=20))
        
        self.indicator_engine = IndicatorEngine(indicator_registry)
        print("✅ Setup complete!")
    
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    async def get_stock_universe(self, start_date: date, end_date: date) -> List[str]:
        """Get stocks with sufficient data in the analysis period."""
        
        # First check what we have
        check_query = """
        SELECT COUNT(DISTINCT symbol) as total_symbols,
               MIN(date) as min_date,
               MAX(date) as max_date
        FROM daily_price_data
        """
        
        check_result = await self.db_conn.fetchrow(check_query)
        print(f"\n📊 Database info: {check_result['total_symbols']} symbols, "
              f"dates from {check_result['min_date']} to {check_result['max_date']}")
        
        # Get stocks with data in our period
        query = """
        SELECT symbol, 
               COUNT(*) as days,
               MIN(date) as first_date,
               MAX(date) as last_date,
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
        
        if not rows:
            print(f"❌ No stocks found with data between {start_date} and {end_date}")
            # Try without date filter
            fallback_query = """
            SELECT symbol, COUNT(*) as days
            FROM daily_price_data
            WHERE volume > 0
            GROUP BY symbol
            HAVING COUNT(*) >= 100
            ORDER BY COUNT(*) DESC
            LIMIT 20
            """
            rows = await self.db_conn.fetch(fallback_query)
        
        symbols = [row['symbol'] for row in rows]
        print(f"📊 Selected {len(symbols)} stocks for analysis")
        if symbols:
            print(f"   Top stocks: {symbols[:10]}...")
        
        return symbols
    
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data for a symbol with buffer for indicators."""
        # Add buffer for indicator calculation
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
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        # Test different RSI thresholds
        oversold_threshold = 30
        overbought_threshold = 70
        
        oversold_signals = rsi_values < oversold_threshold
        overbought_signals = rsi_values > overbought_threshold
        
        # Calculate accuracy for oversold signals (should predict 'up')
        oversold_correct = 0
        oversold_total = 0
        
        for idx in rsi_values.index:
            if idx in labels.index and pd.notna(labels[idx]):
                if oversold_signals[idx]:
                    oversold_total += 1
                    if labels[idx] == 'up':
                        oversold_correct += 1
        
        # Calculate accuracy for overbought signals (should predict 'down')
        overbought_correct = 0
        overbought_total = 0
        
        for idx in rsi_values.index:
            if idx in labels.index and pd.notna(labels[idx]):
                if overbought_signals[idx]:
                    overbought_total += 1
                    if labels[idx] == 'down':
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
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        # Ensure indices match
        common_idx = prices.index.intersection(ma_values.index).intersection(labels.index)
        prices = prices.loc[common_idx]
        ma_values = ma_values.loc[common_idx]
        labels = labels.loc[common_idx]
        
        # Price above/below MA
        price_above_ma = (prices > ma_values).astype(int)
        crossovers = price_above_ma.diff()
        
        bullish_crossovers = crossovers == 1  # Price crosses above MA
        bearish_crossovers = crossovers == -1  # Price crosses below MA
        
        # Calculate accuracy
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
    
    def test_volume_signals(self, volumes: pd.Series, volume_ma: pd.Series, labels: pd.Series, prices: pd.Series) -> Dict:
        """Test volume surge signals combined with price movement."""
        results = {
            'volume_surge_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        # Ensure indices match
        common_idx = volumes.index.intersection(volume_ma.index).intersection(labels.index)
        volumes = volumes.loc[common_idx]
        volume_ma = volume_ma.loc[common_idx]
        labels = labels.loc[common_idx]
        
        # Volume surge signals
        volume_ratio = volumes / volume_ma
        volume_surges = volume_ratio > 1.5
        
        # Test if volume surges predict any significant movement
        surge_correct = 0
        surge_total = 0
        
        for idx in volume_surges.index:
            if pd.notna(labels[idx]) and volume_surges[idx]:
                surge_total += 1
                if labels[idx] in ['up', 'down']:  # Any significant movement
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
        if market_data.empty or len(market_data) < 50:
            return {}
        
        # Filter to analysis period
        analysis_data = market_data[(market_data.index.date >= start_date) & 
                                   (market_data.index.date <= end_date)]
        
        # Calculate future returns and labels
        future_returns = self.calculate_future_returns(analysis_data)
        direction_labels = self.get_direction_labels(future_returns, threshold=0.02)
        
        company_id = self.get_company_id(symbol)
        stock_results = {'symbol': symbol, 'indicators': {}}
        
        # Test each indicator
        indicators_to_test = ['rsi_14', 'rsi_7', 'rsi_21', 'sma_10', 'sma_20', 'volume_sma_20']
        
        for indicator_name in indicators_to_test:
            try:
                # Calculate indicator values
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
                
                # Extract values from IndicatorResult
                if hasattr(indicator_result, 'values') and indicator_result.values:
                    # Convert to pandas Series
                    indicator_series = pd.Series(indicator_result.values)
                    indicator_series.index = pd.to_datetime(indicator_series.index)
                    
                    # Filter to analysis period
                    indicator_series = indicator_series[(indicator_series.index.date >= start_date) & 
                                                       (indicator_series.index.date <= end_date)]
                    
                    # Test based on indicator type
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
    
    async def run_analysis(self, start_date: date, end_date: date) -> Dict:
        """Run the complete indicator analysis."""
        
        print(f"\n🚀 Running Indicator Predictor Analysis")
        print(f"Period: {start_date} to {end_date}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        print(f"Direction threshold: ±2%")
        
        # Get stock universe
        symbols = await self.get_stock_universe(start_date, end_date)
        
        if len(symbols) < 3:
            print("❌ Insufficient stocks for analysis")
            return {}
        
        # Analyze each stock
        print(f"\n🔍 Analyzing {min(len(symbols), 15)} stocks...")
        stock_results = []
        
        for i, symbol in enumerate(symbols[:15]):  # Limit to 15 stocks
            try:
                result = await self.analyze_stock(symbol, start_date, end_date)
                if result:
                    stock_results.append(result)
                if (i + 1) % 5 == 0:
                    print(f"   Progress: {i + 1}/15 stocks analyzed")
            except Exception as e:
                print(f"     ❌ Failed to analyze {symbol}: {str(e)}")
        
        if not stock_results:
            print("❌ No successful stock analyses")
            return {}
        
        # Aggregate results
        aggregated_results = self.aggregate_results(stock_results)
        
        # Display results
        self.display_results(aggregated_results, len(stock_results))
        
        return {
            'aggregated_results': aggregated_results,
            'stocks_analyzed': len(stock_results),
            'analysis_params': {
                'lookforward_days': self.lookforward_days,
                'period': f"{start_date} to {end_date}"
            }
        }
    
    def display_results(self, results: Dict, stocks_analyzed: int):
        """Display the analysis results in a readable format."""
        
        print(f"\n📊 INDICATOR PREDICTOR ANALYSIS RESULTS")
        print(f"=" * 70)
        print(f"Stocks analyzed: {stocks_analyzed}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        
        if not results:
            print("\n❌ No results to display")
            return
        
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
        if sorted_indicators:
            print(f"\n🥇 TOP PREDICTORS:")
            for i, (indicator_name, metrics) in enumerate(sorted_indicators[:3]):
                accuracy = metrics['overall_accuracy']
                signals = metrics['total_signals']
                print(f"  {i+1}. {indicator_name}: {accuracy:.1%} accuracy ({signals} signals)")
        
        # Insights
        print(f"\n💡 INSIGHTS:")
        
        if sorted_indicators:
            best_indicator = sorted_indicators[0][0]
            best_accuracy = sorted_indicators[0][1]['overall_accuracy']
            
            if best_accuracy > 0.6:
                print(f"  ✅ Strong predictor found: {best_indicator} ({best_accuracy:.1%} accuracy)")
            elif best_accuracy > 0.55:
                print(f"  🟡 Moderate predictor: {best_indicator} ({best_accuracy:.1%} accuracy)")
            else:
                print(f"  ❌ No strong predictors found (best: {best_accuracy:.1%})")
        
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
        
        # Analysis period - use dates we know have data
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
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