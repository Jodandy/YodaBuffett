#!/usr/bin/env python3
"""
Test indicators on COMPLETELY NEW companies - skip the first 50 stocks we've tested.
This is the ultimate validation of whether our findings generalize.
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
import random

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

class FreshCompaniesAnalyzer:
    """Tests indicator predictive power on completely untested companies."""
    
    def __init__(self, lookforward_days: int = 5):
        self.lookforward_days = lookforward_days
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicator engine."""
        print(f"🚀 Setting up Fresh Companies Analyzer...")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Same indicators as always
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
    
    async def get_fresh_stock_universe(self, start_date: date, end_date: date, skip_first: int = 50) -> List[str]:
        """Get COMPLETELY FRESH stocks - skip the first 50 we've been testing."""
        
        print(f"\n🔍 Finding FRESH companies (skipping first {skip_first} tested stocks)...")
        
        # Get ALL stocks with decent data, ordered by volume/activity
        query = """
        SELECT symbol, 
               COUNT(*) as days,
               AVG(volume::NUMERIC) as avg_volume,
               MAX(volume::NUMERIC) as max_volume,
               MIN(date) as first_date,
               MAX(date) as last_date
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2
        AND volume > 1000  -- Minimum liquidity
        AND close_price > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 80  -- At least 80 days in period
        ORDER BY COUNT(*) DESC, AVG(volume::NUMERIC) DESC
        """
        
        rows = await self.db_conn.fetch(query, start_date, end_date)
        
        print(f"   Total stocks with data in period: {len(rows)}")
        
        # Show what we're skipping
        skipped_symbols = [row['symbol'] for row in rows[:skip_first]]
        print(f"   Skipping previously tested: {skipped_symbols[:10]}... (and {len(skipped_symbols)-10} more)")
        
        # Get the FRESH ones
        fresh_rows = rows[skip_first:skip_first+20]  # Next 20 stocks
        fresh_symbols = [row['symbol'] for row in fresh_rows]
        
        print(f"\n📊 Selected {len(fresh_symbols)} COMPLETELY FRESH companies:")
        for i, row in enumerate(fresh_rows):
            symbol = row['symbol']
            days = row['days']
            avg_vol = float(row['avg_volume']) if row['avg_volume'] else 0
            date_range = f"{row['first_date']} to {row['last_date']}"
            print(f"   {i+1:2d}. {symbol:<12}: {days:>3} days, avg vol {avg_vol:>12,.0f}, {date_range}")
        
        return fresh_symbols[:15]  # Take 15 fresh companies
    
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
        """Test RSI indicator signals - same method as original."""
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
        """Test volume surge signals - the critical test!"""
        results = {
            'volume_surge_accuracy': 0,
            'total_signals': 0,
            'correct_predictions': 0,
            'total_accuracy': 0
        }
        
        common_idx = volumes.index.intersection(volume_ma.index).intersection(labels.index)
        volumes = volumes.loc[common_idx]
        volume_ma_values = volume_ma.loc[common_idx]
        labels = labels.loc[common_idx]
        
        volume_ratio = volumes / volume_ma_values
        volume_surges = volume_ratio > 1.5  # Same threshold as original
        
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
        """Analyze all indicators for a single fresh stock."""
        print(f"   🔍 Analyzing fresh company {symbol}...")
        
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
                    
                    # Use same testing methods as original
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
    
    def aggregate_fresh_results(self, stock_results: List[Dict]) -> Dict:
        """Aggregate results for fresh companies."""
        print(f"\n📊 Aggregating fresh company results...")
        
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
    
    def display_fresh_results(self, fresh_results: Dict, stocks_analyzed: int, fresh_symbols: List[str]):
        """Display fresh company results with comparison to original findings."""
        
        print(f"\n📊 FRESH COMPANIES VALIDATION RESULTS")
        print(f"=" * 80)
        print(f"Fresh companies analyzed: {stocks_analyzed}")
        print(f"Fresh symbols: {fresh_symbols}")
        print(f"Prediction horizon: {self.lookforward_days} days")
        print(f"Direction threshold: ±2%")
        
        # Original results for comparison (first 15 stocks)
        original_results = {
            'volume_sma_20': 80.8,
            'rsi_14': 39.8,
            'rsi_7': 38.4,
            'rsi_21': 37.6,
            'sma_10': 35.7,
            'sma_20': 34.1
        }
        
        # Sort indicators by fresh company accuracy
        sorted_indicators = sorted(
            fresh_results.items(),
            key=lambda x: x[1]['overall_accuracy'],
            reverse=True
        )
        
        print(f"\n🏆 FRESH COMPANIES vs ORIGINAL COMPARISON:")
        print(f"{'Indicator':<15} {'Fresh':<10} {'Original':<10} {'Diff':<10} {'Generalizes?':<12} {'Signals':<10}")
        print(f"-" * 85)
        
        generalization_scores = []
        
        for indicator_name, metrics in sorted_indicators:
            fresh_accuracy = metrics['overall_accuracy'] * 100
            original_accuracy = original_results.get(indicator_name, 0)
            diff = fresh_accuracy - original_accuracy
            signals = metrics['total_signals']
            
            # Determine if finding generalizes
            if abs(diff) <= 5:  # Within 5 percentage points
                generalizes = "✅ YES"
                generalization_scores.append(1)
            elif fresh_accuracy > original_accuracy:
                generalizes = "📈 BETTER"
                generalization_scores.append(1)
            elif diff > -10:  # Not too much worse
                generalizes = "🟡 PARTIAL"
                generalization_scores.append(0.5)
            else:
                generalizes = "❌ NO"
                generalization_scores.append(0)
            
            print(f"{indicator_name:<15} {fresh_accuracy:<10.1f}% {original_accuracy:<10.1f}% {diff:<+10.1f}% {generalizes:<12} {signals:<10}")
        
        # Overall generalization score
        overall_generalization = np.mean(generalization_scores) if generalization_scores else 0
        
        print(f"\n🎯 GENERALIZATION ANALYSIS:")
        print(f"   Overall generalization score: {overall_generalization:.1%}")
        
        if overall_generalization >= 0.8:
            print(f"   ✅ EXCELLENT: Findings generalize very well to fresh companies!")
        elif overall_generalization >= 0.6:
            print(f"   🟢 GOOD: Most findings generalize to fresh companies")
        elif overall_generalization >= 0.4:
            print(f"   🟡 MODERATE: Some findings generalize")
        else:
            print(f"   ❌ POOR: Findings may be overfitted to original stock set")
        
        # Volume predictor deep dive
        print(f"\n🔊 VOLUME PREDICTOR VALIDATION:")
        if 'volume_sma_20' in fresh_results:
            fresh_vol = fresh_results['volume_sma_20']['overall_accuracy'] * 100
            original_vol = original_results['volume_sma_20']
            vol_signals = fresh_results['volume_sma_20']['total_signals']
            
            print(f"   Original 15 companies: {original_vol:.1f}% accuracy")
            print(f"   Fresh 15 companies: {fresh_vol:.1f}% accuracy")
            print(f"   Difference: {fresh_vol - original_vol:+.1f} percentage points")
            print(f"   Fresh signals generated: {vol_signals}")
            
            if fresh_vol >= 70:
                print(f"   🎉 VOLUME FINDING CONFIRMED: Still highly predictive!")
                print(f"   💡 Volume surges are a ROBUST predictor across different stocks")
            elif fresh_vol >= 60:
                print(f"   ✅ Volume still good, but varies by stock selection")
            else:
                print(f"   ⚠️ Volume effectiveness may be stock-specific")
        else:
            print(f"   ❌ No volume data available for fresh companies")
        
        # Stock-by-stock breakdown
        print(f"\n📈 FRESH COMPANY PERFORMANCE:")
        successful_stocks = len([r for r in fresh_results.values() if r['stocks_tested'] > 0])
        print(f"   Successfully analyzed: {successful_stocks}/{len(fresh_symbols)} companies")
        
        # Indicator ranking on fresh data
        print(f"\n🏅 FRESH COMPANY INDICATOR RANKING:")
        for i, (indicator_name, metrics) in enumerate(sorted_indicators[:5]):
            accuracy = metrics['overall_accuracy'] * 100
            signals = metrics['total_signals']
            print(f"   {i+1}. {indicator_name}: {accuracy:.1f}% ({signals} signals)")
        
        # Final recommendations
        print(f"\n💡 FINAL VALIDATION CONCLUSIONS:")
        
        vol_validated = False
        if 'volume_sma_20' in fresh_results:
            fresh_vol = fresh_results['volume_sma_20']['overall_accuracy'] * 100
            if fresh_vol >= 70:
                vol_validated = True
        
        if vol_validated:
            print(f"   🎯 VOLUME IS KING: Confirmed across different stock sets")
            print(f"   🚀 Recommendation: Build KNN strategies around volume features")
        
        if overall_generalization >= 0.7:
            print(f"   ✅ Indicator rankings are ROBUST and generalizable")
            print(f"   💰 Safe to use these indicators for live trading")
        elif overall_generalization >= 0.5:
            print(f"   🟡 Mixed results - consider ensemble approaches")
        else:
            print(f"   ⚠️ Findings may be overfit - need more validation")
        
        print(f"\n🔬 SCIENTIFIC VALIDATION STATUS:")
        if vol_validated and overall_generalization >= 0.7:
            print(f"   ✅ VALIDATED: Ready for production deployment")
        elif vol_validated:
            print(f"   🟡 PARTIAL: Volume confirmed, other indicators need review")
        else:
            print(f"   ❌ NEEDS MORE WORK: Expand validation to more companies")
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run validation on completely fresh companies."""
    
    analyzer = FreshCompaniesAnalyzer(lookforward_days=5)
    
    try:
        await analyzer.setup()
        
        # Same analysis period
        end_date = date(2024, 10, 31)
        start_date = date(2024, 5, 1)
        
        print(f"\n🚀 Running FRESH COMPANIES Validation")
        print(f"Period: {start_date} to {end_date}")
        print(f"Prediction horizon: {analyzer.lookforward_days} days")
        print(f"Direction threshold: ±2%")
        print(f"🎯 CRITICAL TEST: Do our findings generalize to untested stocks?")
        
        # Get completely fresh set of stocks (skip first 50)
        fresh_symbols = await analyzer.get_fresh_stock_universe(start_date, end_date, skip_first=50)
        
        if len(fresh_symbols) < 5:
            print("❌ Insufficient fresh stocks for validation")
            return
        
        # Analyze each fresh stock
        print(f"\n🔍 Analyzing {len(fresh_symbols)} fresh companies...")
        stock_results = []
        
        for i, symbol in enumerate(fresh_symbols):
            try:
                result = await analyzer.analyze_stock(symbol, start_date, end_date)
                if result:
                    stock_results.append(result)
                if (i + 1) % 5 == 0:
                    print(f"   Progress: {i + 1}/{len(fresh_symbols)} fresh companies analyzed")
            except Exception as e:
                print(f"     ❌ Failed to analyze {symbol}: {str(e)}")
        
        if not stock_results:
            print("❌ No successful fresh company analyses")
            return
        
        # Aggregate and display results
        fresh_results = analyzer.aggregate_fresh_results(stock_results)
        analyzer.display_fresh_results(fresh_results, len(stock_results), fresh_symbols)
        
        print("\n\n🏁 FRESH COMPANIES VALIDATION COMPLETE!")
        print("This is the ultimate test of whether our indicator findings")
        print("are robust and generalizable across different stocks!")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())