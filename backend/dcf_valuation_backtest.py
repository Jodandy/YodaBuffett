#!/usr/bin/env python3
"""
DCF Valuation Backtest Script
Tests different upside requirements for buy signals based on DCF valuations
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
from collections import defaultdict

class DCFValuationBacktest:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.conn = None
        
    async def connect(self):
        """Connect to database"""
        self.conn = await asyncpg.connect(self.db_url)
        
    async def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            await self.conn.close()
            
    async def get_dcf_data(self) -> pd.DataFrame:
        """Fetch DCF valuations with future price data for backtesting"""
        query = """
        WITH dcf_with_future_prices AS (
            SELECT 
                d.company_id,
                d.symbol,
                d.valuation_date,
                d.market_price as entry_price,
                d.fair_value_median,
                d.fair_value_p25,
                d.fair_value_p75,
                d.implied_return,
                d.rate_sensitivity_score,
                d.debt_burden_category,
                
                -- Get future prices at different horizons
                (SELECT close_price FROM daily_price_data m1 
                 WHERE m1.symbol = d.symbol
                   AND m1.date > d.valuation_date 
                   AND m1.date <= d.valuation_date + INTERVAL '30 days'
                 ORDER BY m1.date DESC LIMIT 1) as price_30d,
                 
                (SELECT close_price FROM daily_price_data m2
                 WHERE m2.symbol = d.symbol
                   AND m2.date > d.valuation_date 
                   AND m2.date <= d.valuation_date + INTERVAL '90 days'
                 ORDER BY m2.date DESC LIMIT 1) as price_90d,
                 
                (SELECT close_price FROM daily_price_data m3
                 WHERE m3.symbol = d.symbol
                   AND m3.date > d.valuation_date 
                   AND m3.date <= d.valuation_date + INTERVAL '180 days'
                 ORDER BY m3.date DESC LIMIT 1) as price_180d,
                 
                (SELECT close_price FROM daily_price_data m4
                 WHERE m4.symbol = d.symbol
                   AND m4.date > d.valuation_date 
                   AND m4.date <= d.valuation_date + INTERVAL '365 days'
                 ORDER BY m4.date DESC LIMIT 1) as price_365d
                 
            FROM dcf_valuations d
            WHERE d.market_price IS NOT NULL
              AND d.fair_value_median IS NOT NULL
              AND d.fair_value_median > 0  -- Exclude invalid valuations
              AND d.valuation_date < CURRENT_DATE - INTERVAL '365 days'
        )
        SELECT * FROM dcf_with_future_prices
        WHERE price_365d IS NOT NULL  -- Ensure we have full year of data
        ORDER BY company_id, valuation_date;
        """
        
        rows = await self.conn.fetch(query)
        
        # Convert asyncpg Records to list of dicts
        data = [dict(row) for row in rows]
        df = pd.DataFrame(data)
        
        # Convert Decimal columns to float
        numeric_columns = ['entry_price', 'fair_value_median', 'fair_value_p25', 
                          'fair_value_p75', 'implied_return', 'rate_sensitivity_score',
                          'price_30d', 'price_90d', 'price_180d', 'price_365d']
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        return df
    
    def calculate_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate actual returns at different horizons"""
        df = df.copy()
        
        # Calculate actual returns
        for days in [30, 90, 180, 365]:
            col = f'price_{days}d'
            return_col = f'return_{days}d'
            if col in df.columns:
                df[return_col] = (df[col] - df['entry_price']) / df['entry_price']
        
        # Calculate upside to fair value
        df['upside_to_median'] = (df['fair_value_median'] - df['entry_price']) / df['entry_price']
        df['upside_to_p25'] = (df['fair_value_p25'] - df['entry_price']) / df['entry_price']
        df['upside_to_p75'] = (df['fair_value_p75'] - df['entry_price']) / df['entry_price']
        
        return df
    
    def backtest_strategy(self, df: pd.DataFrame, min_upside: float, 
                         holding_period: int = 365) -> Dict:
        """
        Backtest a strategy based on minimum upside requirement
        
        Args:
            df: DataFrame with DCF valuations and returns
            min_upside: Minimum required upside to fair value median (e.g., 0.2 for 20%)
            holding_period: Days to hold position (30, 90, 180, or 365)
        """
        # Filter for buy signals
        signals = df[df['upside_to_median'] >= min_upside].copy()
        
        if len(signals) == 0:
            return {
                'min_upside': min_upside,
                'holding_period': holding_period,
                'num_trades': 0,
                'avg_return': 0,
                'median_return': 0,
                'win_rate': 0,
                'sharpe_ratio': 0,
                'max_drawdown': 0,
                'total_return': 0
            }
        
        return_col = f'return_{holding_period}d'
        returns = signals[return_col].dropna()
        
        # Calculate metrics
        num_trades = len(returns)
        avg_return = returns.mean()
        median_return = returns.median()
        win_rate = (returns > 0).mean()
        
        # Sharpe ratio (assuming 0% risk-free rate, annualized)
        if returns.std() > 0:
            sharpe_ratio = np.sqrt(365 / holding_period) * (returns.mean() / returns.std())
        else:
            sharpe_ratio = 0
            
        # Calculate drawdown
        cumulative_returns = (1 + returns).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min()
        
        # Total return (compound)
        total_return = (1 + returns).prod() - 1
        
        return {
            'min_upside': min_upside,
            'holding_period': holding_period,
            'num_trades': num_trades,
            'avg_return': avg_return,
            'median_return': median_return,
            'win_rate': win_rate,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'total_return': total_return,
            'avg_annualized_return': avg_return * (365 / holding_period)
        }
    
    def analyze_by_sensitivity(self, df: pd.DataFrame, min_upside: float) -> Dict:
        """Analyze performance by interest rate sensitivity"""
        results = {}
        
        # Define sensitivity buckets
        df['sensitivity_bucket'] = pd.qcut(
            df['rate_sensitivity_score'].fillna(0), 
            q=[0, 0.33, 0.67, 1.0], 
            labels=['Low', 'Medium', 'High']
        )
        
        for bucket in ['Low', 'Medium', 'High']:
            subset = df[df['sensitivity_bucket'] == bucket]
            if len(subset) > 0:
                results[bucket] = self.backtest_strategy(subset, min_upside)
        
        return results
    
    def analyze_by_debt_burden(self, df: pd.DataFrame, min_upside: float) -> Dict:
        """Analyze performance by debt burden category"""
        results = {}
        
        for category in df['debt_burden_category'].unique():
            if pd.notna(category):
                subset = df[df['debt_burden_category'] == category]
                if len(subset) > 0:
                    results[category] = self.backtest_strategy(subset, min_upside)
        
        return results
    
    async def run_comprehensive_backtest(self):
        """Run backtest across multiple upside requirements and holding periods"""
        print("Loading DCF valuation data...")
        df = await self.get_dcf_data()
        print(f"Loaded {len(df)} DCF valuations")
        
        # Calculate returns
        df = self.calculate_returns(df)
        
        # Test different upside requirements (including overvalued/negative)
        upside_requirements = [-0.5, -0.3, -0.2, -0.1, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0]
        holding_periods = [30, 90, 180, 365]
        
        results = []
        
        print("\n" + "="*80)
        print("BACKTEST RESULTS BY UPSIDE REQUIREMENT AND HOLDING PERIOD")
        print("="*80)
        
        for upside in upside_requirements:
            if upside < 0:
                print(f"\nOVERVALUED Strategy - Buying at {abs(upside)*100:.0f}% ABOVE fair value")
            elif upside == 0:
                print(f"\nFAIR VALUE Strategy - Buying at exactly fair value (0% upside)")
            else:
                print(f"\nUNDERVALUED Strategy - Minimum {upside*100:.0f}% upside to fair value")
            print("-"*60)
            
            for period in holding_periods:
                result = self.backtest_strategy(df, upside, period)
                results.append(result)
                
                if result['num_trades'] > 0:
                    print(f"\n{period}-day holding period:")
                    print(f"  Trades: {result['num_trades']}")
                    print(f"  Avg Return: {result['avg_return']*100:.2f}%")
                    print(f"  Win Rate: {result['win_rate']*100:.1f}%")
                    print(f"  Sharpe Ratio: {result['sharpe_ratio']:.2f}")
                    print(f"  Max Drawdown: {result['max_drawdown']*100:.1f}%")
                    print(f"  Annualized Return: {result['avg_annualized_return']*100:.1f}%")
        
        # Analyze by rate sensitivity
        print("\n" + "="*80)
        print("PERFORMANCE BY INTEREST RATE SENSITIVITY (365-day holding)")
        print("="*80)
        
        for upside in [0.2, 0.3, 0.4]:
            print(f"\nMinimum Upside: {upside*100:.0f}%")
            sensitivity_results = self.analyze_by_sensitivity(df, upside)
            
            for bucket, result in sensitivity_results.items():
                if result['num_trades'] > 0:
                    print(f"\n{bucket} Sensitivity:")
                    print(f"  Trades: {result['num_trades']}")
                    print(f"  Avg Return: {result['avg_return']*100:.2f}%")
                    print(f"  Win Rate: {result['win_rate']*100:.1f}%")
                    print(f"  Sharpe Ratio: {result['sharpe_ratio']:.2f}")
        
        # Analyze by debt burden
        print("\n" + "="*80)
        print("PERFORMANCE BY DEBT BURDEN CATEGORY (365-day holding)")
        print("="*80)
        
        for upside in [0.2, 0.3]:
            print(f"\nMinimum Upside: {upside*100:.0f}%")
            debt_results = self.analyze_by_debt_burden(df, upside)
            
            for category, result in sorted(debt_results.items()):
                if result['num_trades'] > 0:
                    print(f"\n{category} Debt Burden:")
                    print(f"  Trades: {result['num_trades']}")
                    print(f"  Avg Return: {result['avg_return']*100:.2f}%")
                    print(f"  Win Rate: {result['win_rate']*100:.1f}%")
        
        # Create summary DataFrame
        results_df = pd.DataFrame(results)
        
        # Find optimal strategies
        print("\n" + "="*80)
        print("OPTIMAL STRATEGIES")
        print("="*80)
        
        # Best Sharpe ratio
        best_sharpe = results_df.loc[results_df['sharpe_ratio'].idxmax()]
        print(f"\nBest Sharpe Ratio: {best_sharpe['sharpe_ratio']:.2f}")
        print(f"  Upside Requirement: {best_sharpe['min_upside']*100:.0f}%")
        print(f"  Holding Period: {best_sharpe['holding_period']} days")
        print(f"  Avg Return: {best_sharpe['avg_return']*100:.2f}%")
        print(f"  Win Rate: {best_sharpe['win_rate']*100:.1f}%")
        
        # Best win rate (with minimum trades)
        viable_results = results_df[results_df['num_trades'] >= 20]
        if len(viable_results) > 0:
            best_win_rate = viable_results.loc[viable_results['win_rate'].idxmax()]
            print(f"\nBest Win Rate: {best_win_rate['win_rate']*100:.1f}%")
            print(f"  Upside Requirement: {best_win_rate['min_upside']*100:.0f}%")
            print(f"  Holding Period: {best_win_rate['holding_period']} days")
            print(f"  Avg Return: {best_win_rate['avg_return']*100:.2f}%")
            print(f"  Trades: {best_win_rate['num_trades']}")
        
        # Distribution analysis
        print("\n" + "="*80)
        print("UPSIDE DISTRIBUTION ANALYSIS")
        print("="*80)
        
        print(f"\nUpside to Fair Value Statistics:")
        print(f"  Mean: {df['upside_to_median'].mean()*100:.1f}%")
        print(f"  Median: {df['upside_to_median'].median()*100:.1f}%")
        print(f"  Std Dev: {df['upside_to_median'].std()*100:.1f}%")
        print(f"  5th percentile: {df['upside_to_median'].quantile(0.05)*100:.1f}%")
        print(f"  95th percentile: {df['upside_to_median'].quantile(0.95)*100:.1f}%")
        
        # Count opportunities at each level
        print("\nNumber of Opportunities by Upside Threshold:")
        for upside in [-0.5, -0.3, -0.2, -0.1, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5]:
            if upside <= 0:
                count = (df['upside_to_median'] <= upside).sum()
                pct = count / len(df) * 100
                if upside < 0:
                    print(f"  Overvalued by >={abs(upside)*100:.0f}%: {count} ({pct:.1f}%)")
                else:
                    print(f"  At fair value (0%): {count} ({pct:.1f}%)")
            else:
                count = (df['upside_to_median'] >= upside).sum()
                pct = count / len(df) * 100
                print(f"  Undervalued by >={upside*100:.0f}%: {count} ({pct:.1f}%)")
        
        # Analyze why undervalued might perform poorly
        print("\n" + "="*80)
        print("CONTRARIAN ANALYSIS - Why might undervalued stocks underperform?")
        print("="*80)
        
        # Identify worst performers in undervalued category
        print("\nVALUE TRAP ANALYSIS - Worst performing 'undervalued' stocks:")
        print("-" * 60)
        
        # Get stocks that appeared undervalued but lost money
        value_traps = df[(df['upside_to_median'] >= 0.3) & (df['return_365d'] < -0.2)].copy()
        if len(value_traps) > 0:
            value_traps['loss'] = value_traps['return_365d']
            worst_traps = value_traps.nsmallest(10, 'loss')[['symbol', 'valuation_date', 'upside_to_median', 'return_365d']]
            
            print(f"\nFound {len(value_traps)} value traps (>30% upside but lost >20% in 1 year):")
            print("\nWorst 10 value traps:")
            for _, trap in worst_traps.iterrows():
                print(f"  {trap['symbol']} ({trap['valuation_date']}): "
                      f"Upside {trap['upside_to_median']*100:.0f}%, "
                      f"Actual return {trap['return_365d']*100:.0f}%")
            
            # Analyze common characteristics
            print(f"\nValue trap characteristics:")
            print(f"  Average implied upside: {value_traps['upside_to_median'].mean()*100:.0f}%")
            print(f"  Average actual 1-year return: {value_traps['return_365d'].mean()*100:.0f}%")
            print(f"  Average rate sensitivity: {value_traps['rate_sensitivity_score'].mean():.2f}")
            
            # Group by company to see repeat offenders
            repeat_traps = value_traps.groupby('symbol').size().sort_values(ascending=False)
            if len(repeat_traps) > 0:
                print(f"\nRepeat value trap companies (multiple failed predictions):")
                for symbol, count in repeat_traps.head(5).items():
                    print(f"  {symbol}: {count} times")
        
        # Compare characteristics of undervalued vs overvalued
        undervalued = df[df['upside_to_median'] >= 0.3]
        overvalued = df[df['upside_to_median'] <= -0.2]
        
        if len(undervalued) > 0 and len(overvalued) > 0:
            print(f"\nUndervalued (>30% upside) vs Overvalued (>20% downside):")
            print(f"  Undervalued count: {len(undervalued)}")
            print(f"  Overvalued count: {len(overvalued)}")
            
            # Compare returns
            for period in [30, 90, 180, 365]:
                under_ret = undervalued[f'return_{period}d'].mean()
                over_ret = overvalued[f'return_{period}d'].mean()
                print(f"\n  {period}-day returns:")
                print(f"    Undervalued avg: {under_ret*100:.1f}%")
                print(f"    Overvalued avg: {over_ret*100:.1f}%")
                print(f"    Difference: {(over_ret - under_ret)*100:.1f}% (positive = overvalued outperforms)")
            
            # Check if DCF might be systematically wrong
            print(f"\n  Rate sensitivity comparison:")
            print(f"    Undervalued avg sensitivity: {undervalued['rate_sensitivity_score'].mean():.2f}")
            print(f"    Overvalued avg sensitivity: {overvalued['rate_sensitivity_score'].mean():.2f}")
            
            # Group by debt burden
            print(f"\n  Debt burden distribution:")
            under_debt = undervalued['debt_burden_category'].value_counts(normalize=True)
            over_debt = overvalued['debt_burden_category'].value_counts(normalize=True)
            for category in under_debt.index:
                if category in over_debt.index:
                    print(f"    {category}: Undervalued {under_debt[category]*100:.1f}% vs Overvalued {over_debt[category]*100:.1f}%")
        
        # Quality-filtered backtest
        print("\n" + "="*80)
        print("QUALITY-FILTERED BACKTEST - Excluding repeat value traps")
        print("="*80)
        
        # Identify companies to exclude (those that repeatedly appear as value traps)
        value_trap_companies = df[(df['upside_to_median'] >= 0.3) & (df['return_365d'] < -0.1)].groupby('symbol').size()
        bad_companies = value_trap_companies[value_trap_companies >= 2].index.tolist()
        
        if len(bad_companies) > 0:
            print(f"\nExcluding {len(bad_companies)} companies that repeatedly appeared as value traps")
            print(f"Companies excluded: {', '.join(bad_companies[:10])}")
            if len(bad_companies) > 10:
                print(f"  ... and {len(bad_companies) - 10} more")
            
            # Create filtered dataset
            df_filtered = df[~df['symbol'].isin(bad_companies)]
            print(f"\nDataset reduced from {len(df)} to {len(df_filtered)} valuations")
            
            # Re-run key strategies on filtered data
            print("\nFiltered Results (365-day holding period):")
            for upside in [0.2, 0.3, 0.4]:
                result = self.backtest_strategy(df_filtered, upside, 365)
                if result['num_trades'] > 0:
                    print(f"\n  {upside*100:.0f}% minimum upside:")
                    print(f"    Trades: {result['num_trades']}")
                    print(f"    Avg Return: {result['avg_return']*100:.2f}%")
                    print(f"    Win Rate: {result['win_rate']*100:.1f}%")
                    print(f"    Sharpe Ratio: {result['sharpe_ratio']:.2f}")
        
        return results_df

async def main():
    # Database connection
    db_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    
    backtest = DCFValuationBacktest(db_url)
    
    try:
        await backtest.connect()
        results = await backtest.run_comprehensive_backtest()
        
        # Save results
        results.to_csv('dcf_backtest_results.csv', index=False)
        print("\n\nResults saved to dcf_backtest_results.csv")
        
    finally:
        await backtest.disconnect()

if __name__ == "__main__":
    asyncio.run(main())