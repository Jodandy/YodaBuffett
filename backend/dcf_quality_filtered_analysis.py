#!/usr/bin/env python3
"""
DCF Quality-Filtered Analysis
Deep dive into the successful undervalued strategy after filtering out value traps
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json

class DCFQualityFilteredAnalysis:
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
            
    def calculate_max_drawdown(self, returns_series):
        """Calculate maximum drawdown from a series of returns"""
        if len(returns_series) == 0:
            return 0
        
        cumulative_returns = (1 + returns_series).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        return drawdown.min()
    
    def calculate_individual_max_drawdown(self, df, entry_price_col, periods):
        """Calculate max drawdown for individual trades using true minimum prices during each period"""
        for period in periods:
            drawdown_col = f'max_drawdown_{period}d'
            min_price_col = f'min_price_{period}d'
            
            if min_price_col in df.columns:
                # True drawdown: (minimum_price - entry_price) / entry_price
                df[drawdown_col] = (df[min_price_col] - df[entry_price_col]) / df[entry_price_col]
            else:
                df[drawdown_col] = 0.0
        
        return df
    
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
                
                -- Get future prices at different horizons (end points)
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
                 ORDER BY m4.date DESC LIMIT 1) as price_365d,
                 
                -- Get minimum prices during each period (for true drawdown calculation)
                (SELECT MIN(close_price) FROM daily_price_data m5
                 WHERE m5.symbol = d.symbol
                   AND m5.date > d.valuation_date 
                   AND m5.date <= d.valuation_date + INTERVAL '90 days') as min_price_90d,
                   
                (SELECT MIN(close_price) FROM daily_price_data m6
                 WHERE m6.symbol = d.symbol
                   AND m6.date > d.valuation_date 
                   AND m6.date <= d.valuation_date + INTERVAL '180 days') as min_price_180d,
                   
                (SELECT MIN(close_price) FROM daily_price_data m7
                 WHERE m7.symbol = d.symbol
                   AND m7.date > d.valuation_date 
                   AND m7.date <= d.valuation_date + INTERVAL '365 days') as min_price_365d
                 
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
                          'price_30d', 'price_90d', 'price_180d', 'price_365d',
                          'min_price_90d', 'min_price_180d', 'min_price_365d']
        
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
        
        # Calculate individual trade max drawdowns (include 30-day for better accuracy)
        df = self.calculate_individual_max_drawdown(df, 'entry_price', [90, 180, 365])
        
        return df
    
    async def analyze_value_traps_and_quality_filter(self):
        """Detailed analysis of value traps and quality-filtered results"""
        print("Loading DCF valuation data...")
        df = await self.get_dcf_data()
        print(f"Loaded {len(df)} DCF valuations")
        
        # Calculate returns
        df = self.calculate_returns(df)
        
        # Identify value traps - companies that repeatedly appeared undervalued but lost money
        print("\n" + "="*80)
        print("VALUE TRAP IDENTIFICATION")
        print("="*80)
        
        # Find all instances where stock appeared undervalued (>30% upside) but lost money
        value_trap_instances = df[(df['upside_to_median'] >= 0.3) & (df['return_365d'] < -0.1)]
        
        # Count how many times each company appeared as a value trap
        value_trap_counts = value_trap_instances.groupby('symbol').agg({
            'valuation_date': 'count',
            'upside_to_median': 'mean',
            'return_365d': 'mean'
        }).rename(columns={
            'valuation_date': 'trap_count',
            'upside_to_median': 'avg_implied_upside',
            'return_365d': 'avg_actual_return'
        })
        
        # Filter for companies that were value traps 2+ times
        repeat_value_traps = value_trap_counts[value_trap_counts['trap_count'] >= 2].sort_values('trap_count', ascending=False)
        
        print(f"\nFound {len(repeat_value_traps)} companies that were value traps 2+ times:")
        print("\n{:<10} {:<15} {:<20} {:<20}".format("Symbol", "Times Trapped", "Avg Implied Upside", "Avg Actual Return"))
        print("-" * 70)
        
        for symbol, row in repeat_value_traps.iterrows():
            print("{:<10} {:<15} {:<20.1%} {:<20.1%}".format(
                symbol, 
                int(row['trap_count']), 
                row['avg_implied_upside'],
                row['avg_actual_return']
            ))
        
        # Show all value trap instances for these companies
        bad_companies = repeat_value_traps.index.tolist()
        
        print("\n" + "="*80)
        print("DETAILED VALUE TRAP INSTANCES")
        print("="*80)
        
        for symbol in bad_companies[:5]:  # Show top 5 worst offenders
            company_traps = value_trap_instances[value_trap_instances['symbol'] == symbol]
            print(f"\n{symbol} - Failed predictions:")
            print("{:<15} {:<15} {:<20} {:<20}".format("Date", "Entry Price", "Implied Upside", "Actual 1Y Return"))
            print("-" * 70)
            for _, trap in company_traps.iterrows():
                print("{:<15} {:<15.2f} {:<20.1%} {:<20.1%}".format(
                    str(trap['valuation_date']),
                    trap['entry_price'],
                    trap['upside_to_median'],
                    trap['return_365d']
                ))
        
        # Create filtered dataset
        df_filtered = df[~df['symbol'].isin(bad_companies)]
        
        print("\n" + "="*80)
        print("QUALITY-FILTERED UNDERVALUED STRATEGY RESULTS")
        print("="*80)
        
        print(f"\nFiltered out {len(bad_companies)} companies")
        print(f"Dataset reduced from {len(df)} to {len(df_filtered)} valuations")
        
        # Analyze different upside thresholds
        upside_thresholds = [0.2, 0.3, 0.4, 0.5]
        holding_periods = [90, 180, 365]
        
        for threshold in upside_thresholds:
            print(f"\n\nMINIMUM {threshold*100:.0f}% UPSIDE STRATEGY")
            print("=" * 80)
            
            # Get all trades for this threshold
            trades = df_filtered[df_filtered['upside_to_median'] >= threshold].copy()
            
            if len(trades) == 0:
                print("No trades found for this threshold")
                continue
            
            # Performance summary for each holding period
            print(f"\nPerformance Summary by Holding Period:")
            print("{:<12} {:<12} {:<12} {:<10} {:<12} {:<12} {:<15}".format(
                "Period", "Avg Return", "Median Ret", "Win Rate", "Best Trade", "Worst Trade", "Max Drawdown"
            ))
            print("-" * 95)
            
            for period in holding_periods:
                return_col = f'return_{period}d'
                period_returns = trades[return_col]
                
                avg_return = period_returns.mean()
                median_return = period_returns.median()
                win_rate = (period_returns > 0).mean()
                best_return = period_returns.max()
                worst_return = period_returns.min()
                
                # Portfolio max drawdown (aggregate strategy performance)
                portfolio_max_drawdown = self.calculate_max_drawdown(period_returns)
                
                print("{:<12} {:<12.1%} {:<12.1%} {:<10.1%} {:<12.1%} {:<12.1%} {:<15.1%}".format(
                    f"{period} days",
                    avg_return,
                    median_return,
                    win_rate,
                    best_return,
                    worst_return,
                    portfolio_max_drawdown
                ))
            
            print(f"\nTotal Trades: {len(trades)}")
            
            # Show all trades sorted by 365-day return
            trades = trades.sort_values('return_365d', ascending=False)
            
            # Debug the drawdown calculation for a few trades
            debug_trades = trades[trades['symbol'] == 'CEDER'].head(3)
            if len(debug_trades) > 0:
                print(f"\nDEBUG - CEDER trades price analysis (TRUE DRAWDOWN):")
                for _, trade in debug_trades.iterrows():
                    print(f"  {trade['symbol']} ({trade['valuation_date']}):")
                    print(f"    Entry: {trade['entry_price']:.2f}")
                    if 'price_30d' in trade:
                        print(f"    30d: {trade['price_30d']:.2f} ({trade['return_30d']*100:.1f}%)")
                    print(f"    90d: {trade['price_90d']:.2f} ({trade['return_90d']*100:.1f}%)")
                    print(f"    180d: {trade['price_180d']:.2f} ({trade['return_180d']*100:.1f}%)")
                    print(f"    365d: {trade['price_365d']:.2f} ({trade['return_365d']*100:.1f}%)")
                    print(f"    Min price 90d: {trade['min_price_90d']:.2f}")
                    print(f"    Min price 180d: {trade['min_price_180d']:.2f}")
                    print(f"    Min price 365d: {trade['min_price_365d']:.2f}")
                    print(f"    True Max DD 90d: {trade['max_drawdown_90d']*100:.1f}%")
                    print(f"    True Max DD 180d: {trade['max_drawdown_180d']*100:.1f}%")
                    print(f"    True Max DD 365d: {trade['max_drawdown_365d']*100:.1f}%")
                    print()

            print(f"\nDETAILED TRADES (sorted by 365-day return):")
            print("\n{:<8} {:<10} {:<8} {:<8} {:<8} {:<9} {:<9} {:<9} {:<10} {:<10} {:<10}".format(
                "Symbol", "Buy Date", "Buy $", "Fair $", "Upside", "90d Ret", "180d Ret", "365d Ret", "90d DD", "180d DD", "365d DD"
            ))
            print("-" * 115)
            
            for _, trade in trades.iterrows():
                print("{:<8} {:<10} {:<8.2f} {:<8.2f} {:<8.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<10.1%} {:<10.1%} {:<10.1%}".format(
                    trade['symbol'],
                    str(trade['valuation_date'])[:10],
                    trade['entry_price'],
                    trade['fair_value_median'],
                    trade['upside_to_median'],
                    trade['return_90d'],
                    trade['return_180d'],
                    trade['return_365d'],
                    trade['max_drawdown_90d'],
                    trade['max_drawdown_180d'],
                    trade['max_drawdown_365d']
                ))
            
            # Show best and worst trades with multi-period returns and drawdowns
            print(f"\nBest 5 Trades (by 365-day return):")
            best_trades = trades.nlargest(5, 'return_365d')
            print("{:<15} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9}".format(
                "Symbol (Date)", "90d Ret", "180d Ret", "365d Ret", "90d DD", "180d DD", "365d DD"
            ))
            print("-" * 75)
            for _, trade in best_trades.iterrows():
                print("{:<15} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%}".format(
                    f"{trade['symbol']} ({str(trade['valuation_date'])[:7]})",
                    trade['return_90d'],
                    trade['return_180d'],
                    trade['return_365d'],
                    trade['max_drawdown_90d'],
                    trade['max_drawdown_180d'],
                    trade['max_drawdown_365d']
                ))
            
            print(f"\nWorst 5 Trades (by 365-day return):")
            worst_trades = trades.nsmallest(5, 'return_365d')
            print("{:<15} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9}".format(
                "Symbol (Date)", "90d Ret", "180d Ret", "365d Ret", "90d DD", "180d DD", "365d DD"
            ))
            print("-" * 75)
            for _, trade in worst_trades.iterrows():
                print("{:<15} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%}".format(
                    f"{trade['symbol']} ({str(trade['valuation_date'])[:7]})",
                    trade['return_90d'],
                    trade['return_180d'],
                    trade['return_365d'],
                    trade['max_drawdown_90d'],
                    trade['max_drawdown_180d'],
                    trade['max_drawdown_365d']
                ))
            
            # Company performance summary with multi-period returns and drawdowns
            print(f"\nCompany Performance Summary (by 365-day returns):")
            company_performance = trades.groupby('symbol').agg({
                'return_90d': 'mean',
                'return_180d': 'mean', 
                'return_365d': ['mean', 'count'],
                'max_drawdown_90d': 'mean',
                'max_drawdown_180d': 'mean',
                'max_drawdown_365d': 'mean',
                'upside_to_median': 'mean'
            })
            company_performance.columns = ['avg_90d_ret', 'avg_180d_ret', 'avg_365d_ret', 'trade_count', 
                                          'avg_90d_dd', 'avg_180d_dd', 'avg_365d_dd', 'avg_upside']
            company_performance = company_performance.sort_values('avg_365d_ret', ascending=False)
            
            print("\n{:<8} {:<9} {:<9} {:<9} {:<9} {:<9} {:<9} {:<8} {:<8}".format(
                "Symbol", "90d Ret", "180d Ret", "365d Ret", "90d DD", "180d DD", "365d DD", "Trades", "Upside"
            ))
            print("-" * 85)
            for symbol, row in company_performance.head(10).iterrows():
                print("{:<8} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<9.1%} {:<8} {:<8.1%}".format(
                    symbol,
                    row['avg_90d_ret'],
                    row['avg_180d_ret'],
                    row['avg_365d_ret'],
                    row['avg_90d_dd'],
                    row['avg_180d_dd'],
                    row['avg_365d_dd'],
                    int(row['trade_count']),
                    row['avg_upside']
                ))
        
        # Save detailed results with all timeframes
        all_trades_df = pd.DataFrame()
        for threshold in upside_thresholds:
            trades = df_filtered[df_filtered['upside_to_median'] >= threshold].copy()
            trades['threshold'] = threshold
            all_trades_df = pd.concat([all_trades_df, trades])
        
        # Include relevant columns for export (with drawdown data)
        export_columns = [
            'symbol', 'valuation_date', 'entry_price', 'fair_value_median', 
            'upside_to_median', 'return_90d', 'return_180d', 'return_365d',
            'price_90d', 'price_180d', 'price_365d', 'threshold',
            'max_drawdown_90d', 'max_drawdown_180d', 'max_drawdown_365d',
            'rate_sensitivity_score', 'debt_burden_category'
        ]
        
        all_trades_df[export_columns].to_csv('dcf_quality_filtered_trades.csv', index=False)
        print("\n\nDetailed trade data (all timeframes) saved to dcf_quality_filtered_trades.csv")
        
        # Save value trap list
        value_trap_df = pd.DataFrame(repeat_value_traps)
        value_trap_df.to_csv('dcf_value_traps.csv')
        print("Value trap companies saved to dcf_value_traps.csv")

async def main():
    # Database connection
    db_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    
    analyzer = DCFQualityFilteredAnalysis(db_url)
    
    try:
        await analyzer.connect()
        await analyzer.analyze_value_traps_and_quality_filter()
        
    finally:
        await analyzer.disconnect()

if __name__ == "__main__":
    asyncio.run(main())