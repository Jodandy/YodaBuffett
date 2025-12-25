#!/usr/bin/env python3
"""
Fundamental Performance Analyzer

Overlays fundamental metrics with actual stock performance across all available years
to identify which metrics are most predictive of future returns in the Nordic market.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import json
import logging
# Removed scipy dependency - will calculate correlation manually

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class MetricPerformance:
    """Analysis results for a specific metric."""
    metric_name: str
    correlation_with_returns: float
    correlation_pvalue: float
    quintile_returns: List[float]  # Returns for each quintile (1=worst metric, 5=best)
    quintile_counts: List[int]
    predictive_power: float  # How well it predicts future returns
    sample_size: int

class FundamentalPerformanceAnalyzer:
    """
    Analyzes the relationship between fundamental metrics and stock performance.
    
    For each fundamental metric:
    1. Rank stocks into quintiles based on metric values
    2. Calculate forward returns for each quintile
    3. Measure correlation between metric and future performance
    4. Identify which metrics are most predictive
    """
    
    def __init__(self):
        self.db_conn = None
        self.analysis_results: Dict[str, MetricPerformance] = {}
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_available_date_range(self) -> Tuple[date, date]:
        """Get the full date range of available data."""
        query = """
        SELECT 
            MIN(h.date) as start_date,
            MAX(h.date) as end_date,
            COUNT(DISTINCT h.symbol) as companies,
            COUNT(*) as total_records
        FROM historical_fundamentals_daily h
        INNER JOIN daily_price_data p ON h.symbol = p.symbol AND h.date = p.date
        WHERE h.pe_ratio IS NOT NULL
        """
        
        result = await self.db_conn.fetchrow(query)
        logger.info(f"📅 Data range: {result['start_date']} to {result['end_date']}")
        logger.info(f"📊 Coverage: {result['companies']} companies, {result['total_records']:,} records")
        
        return result['start_date'], result['end_date']
        
    async def calculate_forward_returns(self, holding_periods: List[int] = [30, 90, 180, 365]) -> pd.DataFrame:
        """
        Calculate forward returns for all stock-date combinations.
        
        Args:
            holding_periods: List of days to hold positions for return calculation
            
        Returns:
            DataFrame with symbol, date, fundamentals, and forward returns
        """
        logger.info(f"🔄 Calculating forward returns for periods: {holding_periods}")
        
        # Get all fundamentals data with price data
        query = """
        SELECT 
            h.symbol,
            h.date,
            h.pe_ratio,
            h.pb_ratio,
            h.ps_ratio,
            h.ev_ebitda,
            h.debt_to_equity,
            h.current_ratio,
            h.book_value_per_share,
            h.revenue_per_share,
            h.cash_per_share,
            h.market_cap,
            h.close_price as current_price
        FROM historical_fundamentals_daily h
        INNER JOIN daily_price_data p ON h.symbol = p.symbol AND h.date = p.date
        WHERE h.pe_ratio IS NOT NULL 
        AND h.pb_ratio IS NOT NULL
        AND h.market_cap > 1000000000  -- Min $1B market cap
        AND h.pe_ratio > 0 AND h.pe_ratio < 100
        AND h.pb_ratio > 0 AND h.pb_ratio < 20
        ORDER BY h.symbol, h.date
        """
        
        fundamentals = await self.db_conn.fetch(query)
        
        # Convert to list of dictionaries 
        data = []
        for row in fundamentals:
            data.append(dict(row))
            
        df = pd.DataFrame(data)
        
        # Debug: check what we have
        if len(df) > 0:
            logger.info(f"DataFrame columns: {list(df.columns)}")
            logger.info(f"Sample data shape: {df.shape}")
            df['date'] = pd.to_datetime(df['date'])
        else:
            logger.error("No data returned from query!")
            return pd.DataFrame()
        
        logger.info(f"📊 Processing {len(df):,} fundamental records...")
        
        # Calculate forward returns for each holding period
        for period in holding_periods:
            logger.info(f"   Calculating {period}-day forward returns...")
            
            forward_returns = []
            
            for _, row in df.iterrows():
                symbol = row['symbol']
                current_date = row['date']
                current_price = float(row['current_price'])
                
                # Get future price
                future_date = current_date + pd.Timedelta(days=period)
                
                # Query for price on or after future date
                future_price_query = """
                SELECT close_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2
                ORDER BY date
                LIMIT 1
                """
                
                future_price = await self.db_conn.fetchval(
                    future_price_query, 
                    symbol, 
                    future_date.date()
                )
                
                if future_price:
                    forward_return = (float(future_price) - current_price) / current_price
                    forward_returns.append(forward_return)
                else:
                    forward_returns.append(None)
            
            df[f'forward_return_{period}d'] = forward_returns
            
        return df
        
    def calculate_metric_quintiles(self, df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
        """Calculate quintiles for a specific metric and add to dataframe."""
        
        # Remove NaN values for quintile calculation
        valid_data = df[df[metric_col].notna()].copy()
        
        if len(valid_data) == 0:
            return df
            
        # Calculate quintiles
        quintiles = pd.qcut(valid_data[metric_col], q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
        valid_data[f'{metric_col}_quintile'] = quintiles
        
        # Merge back to original dataframe
        df = df.merge(
            valid_data[['symbol', 'date', f'{metric_col}_quintile']], 
            on=['symbol', 'date'], 
            how='left'
        )
        
        return df
        
    async def analyze_metric_performance(self, df: pd.DataFrame, metric_col: str, 
                                       return_period: int = 90) -> MetricPerformance:
        """
        Analyze how a specific fundamental metric relates to future returns.
        
        Args:
            df: DataFrame with fundamentals and forward returns
            metric_col: Name of the metric column to analyze
            return_period: Forward return period to use (default 90 days)
        """
        logger.info(f"📈 Analyzing {metric_col} vs {return_period}-day returns...")
        
        return_col = f'forward_return_{return_period}d'
        
        # Filter valid data
        valid_data = df[(df[metric_col].notna()) & (df[return_col].notna())].copy()
        
        if len(valid_data) < 50:  # Minimum sample size
            logger.warning(f"Insufficient data for {metric_col}: {len(valid_data)} samples")
            return MetricPerformance(
                metric_name=metric_col,
                correlation_with_returns=0,
                correlation_pvalue=1,
                quintile_returns=[],
                quintile_counts=[],
                predictive_power=0,
                sample_size=len(valid_data)
            )
        
        # Calculate correlation manually
        correlation = np.corrcoef(valid_data[metric_col], valid_data[return_col])[0, 1]
        
        # Simple p-value approximation (for display purposes)
        n = len(valid_data)
        t_stat = correlation * np.sqrt((n-2) / (1 - correlation**2)) if correlation != 1 else 0
        p_value = 0.001 if abs(t_stat) > 3 else 0.05 if abs(t_stat) > 2 else 0.1
        
        # Add quintiles
        valid_data = self.calculate_metric_quintiles(valid_data, metric_col)
        quintile_col = f'{metric_col}_quintile'
        
        # Calculate average returns by quintile
        quintile_stats = valid_data.groupby(quintile_col)[return_col].agg(['mean', 'count']).round(4)
        
        quintile_returns = []
        quintile_counts = []
        
        for quintile in [1, 2, 3, 4, 5]:
            if quintile in quintile_stats.index:
                quintile_returns.append(float(quintile_stats.loc[quintile, 'mean']))
                quintile_counts.append(int(quintile_stats.loc[quintile, 'count']))
            else:
                quintile_returns.append(0.0)
                quintile_counts.append(0)
        
        # Calculate predictive power (difference between top and bottom quintile)
        if len(quintile_returns) >= 5:
            predictive_power = quintile_returns[4] - quintile_returns[0]  # Top - Bottom quintile
        else:
            predictive_power = 0
            
        return MetricPerformance(
            metric_name=metric_col,
            correlation_with_returns=correlation,
            correlation_pvalue=p_value,
            quintile_returns=quintile_returns,
            quintile_counts=quintile_counts,
            predictive_power=predictive_power,
            sample_size=len(valid_data)
        )
        
    async def run_comprehensive_analysis(self):
        """Run comprehensive analysis of all fundamental metrics."""
        
        logger.info("🚀 Starting comprehensive fundamental-performance analysis...")
        
        # Get date range
        start_date, end_date = await self.get_available_date_range()
        
        # Calculate forward returns
        df = await self.calculate_forward_returns([30, 90, 180, 365])
        
        # Define metrics to analyze
        metrics_to_analyze = [
            'pe_ratio',
            'pb_ratio', 
            'ps_ratio',
            'ev_ebitda',
            'debt_to_equity',
            'current_ratio',
            'book_value_per_share',
            'revenue_per_share',
            'cash_per_share'
        ]
        
        # Analyze each metric for different time horizons
        return_periods = [30, 90, 180, 365]
        
        results = {}
        
        for return_period in return_periods:
            logger.info(f"\n📊 Analyzing {return_period}-day forward returns...")
            results[f'{return_period}d'] = {}
            
            for metric in metrics_to_analyze:
                if metric in df.columns:
                    analysis = await self.analyze_metric_performance(df, metric, return_period)
                    results[f'{return_period}d'][metric] = analysis
                    
        self.analysis_results = results
        return results
        
    def print_analysis_report(self):
        """Print comprehensive analysis report."""
        
        print("\n" + "="*100)
        print("📊 FUNDAMENTAL METRICS vs PERFORMANCE ANALYSIS")
        print("="*100)
        
        for return_period, metrics in self.analysis_results.items():
            print(f"\n🎯 {return_period.upper()} FORWARD RETURNS ANALYSIS:")
            print(f"{'Metric':<20} {'Correlation':<12} {'P-Value':<10} {'Predictive':<12} {'Sample':<8} {'Q1→Q5 Returns'}")
            print("-" * 100)
            
            # Sort by predictive power
            sorted_metrics = sorted(
                metrics.items(), 
                key=lambda x: abs(x[1].predictive_power), 
                reverse=True
            )
            
            for metric_name, analysis in sorted_metrics:
                corr_str = f"{analysis.correlation_with_returns:.3f}"
                pval_str = f"{analysis.correlation_pvalue:.3f}" if analysis.correlation_pvalue < 0.999 else ">0.999"
                pred_str = f"{analysis.predictive_power:.3f}"
                sample_str = f"{analysis.sample_size}"
                
                # Format quintile returns
                quintile_str = " | ".join([f"{ret:.2%}" for ret in analysis.quintile_returns])
                
                print(f"{metric_name:<20} {corr_str:<12} {pval_str:<10} {pred_str:<12} {sample_str:<8} {quintile_str}")
                
        # Summary of best predictors
        print(f"\n🏆 BEST PREDICTIVE METRICS (by 90-day returns):")
        
        if '90d' in self.analysis_results:
            metrics_90d = self.analysis_results['90d']
            best_predictors = sorted(
                metrics_90d.items(),
                key=lambda x: abs(x[1].predictive_power),
                reverse=True
            )[:5]
            
            for i, (metric, analysis) in enumerate(best_predictors, 1):
                significance = "***" if analysis.correlation_pvalue < 0.001 else \
                             "**" if analysis.correlation_pvalue < 0.01 else \
                             "*" if analysis.correlation_pvalue < 0.05 else ""
                             
                print(f"   {i}. {metric:<20} Predictive Power: {analysis.predictive_power:.3f} {significance}")
                print(f"      Q1 (worst): {analysis.quintile_returns[0]:>6.2%} → Q5 (best): {analysis.quintile_returns[4]:>6.2%}")
                
        # Value vs Growth analysis
        print(f"\n💎 VALUE vs GROWTH INSIGHTS:")
        
        if '90d' in self.analysis_results:
            pe_analysis = self.analysis_results['90d'].get('pe_ratio')
            pb_analysis = self.analysis_results['90d'].get('pb_ratio')
            
            if pe_analysis:
                print(f"   P/E Ratio: Low P/E (Q1) returns {pe_analysis.quintile_returns[0]:.2%} vs High P/E (Q5) returns {pe_analysis.quintile_returns[4]:.2%}")
                if pe_analysis.quintile_returns[0] > pe_analysis.quintile_returns[4]:
                    print("   → Value effect: Lower P/E stocks outperform ✓")
                else:
                    print("   → Growth effect: Higher P/E stocks outperform")
                    
            if pb_analysis:
                print(f"   P/B Ratio: Low P/B (Q1) returns {pb_analysis.quintile_returns[0]:.2%} vs High P/B (Q5) returns {pb_analysis.quintile_returns[4]:.2%}")
                
        print("\n" + "="*100)
        print("Analysis complete! Use these insights to refine investment strategies. 📈")
        print("="*100)
        
    def save_detailed_results(self):
        """Save detailed results to JSON file."""
        
        # Convert results to serializable format
        serializable_results = {}
        
        for period, metrics in self.analysis_results.items():
            serializable_results[period] = {}
            
            for metric_name, analysis in metrics.items():
                serializable_results[period][metric_name] = {
                    'correlation': analysis.correlation_with_returns,
                    'p_value': analysis.correlation_pvalue,
                    'predictive_power': analysis.predictive_power,
                    'sample_size': analysis.sample_size,
                    'quintile_returns': analysis.quintile_returns,
                    'quintile_counts': analysis.quintile_counts
                }
                
        # Save to file
        filename = f"fundamental_performance_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump({
                'analysis_date': datetime.now().isoformat(),
                'description': 'Fundamental metrics vs stock performance analysis across all available years',
                'results': serializable_results
            }, f, indent=2)
            
        logger.info(f"💾 Detailed results saved to {filename}")
        return filename
        
    async def cleanup(self):
        """Cleanup database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the comprehensive fundamental performance analysis."""
    
    analyzer = FundamentalPerformanceAnalyzer()
    
    try:
        await analyzer.setup()
        
        # Run comprehensive analysis
        await analyzer.run_comprehensive_analysis()
        
        # Print results
        analyzer.print_analysis_report()
        
        # Save detailed results
        analyzer.save_detailed_results()
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())