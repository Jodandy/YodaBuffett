#!/usr/bin/env python3
"""
Clean DCF Backtest - Final Analysis

Analyzes the 2024 DCF predictions against actual performance with proper filters.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, date

async def analyze_dcf_performance():
    """Analyze DCF performance with clean data filtering."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🎯 CLEAN DCF BACKTEST ANALYSIS")
        print("=" * 60)
        
        # Get clean DCF signals with proper price data
        query = """
        WITH dcf_signals AS (
            SELECT DISTINCT
                v.symbol,
                v.publish_date,
                v.fair_value_stock_median as fair_value,
                v.valuation_confidence,
                cm.company_name,
                cm.sector,
                -- Get entry price (1-7 days after publish)
                (
                    SELECT p.close_price 
                    FROM daily_price_data p 
                    WHERE p.symbol = v.symbol 
                    AND p.date >= v.publish_date + INTERVAL '1 day'
                    AND p.date <= v.publish_date + INTERVAL '7 days'
                    ORDER BY p.date LIMIT 1
                ) as entry_price,
                -- Get 90-day exit price
                (
                    SELECT p.close_price 
                    FROM daily_price_data p 
                    WHERE p.symbol = v.symbol 
                    AND p.date >= v.publish_date + INTERVAL '85 days'
                    AND p.date <= v.publish_date + INTERVAL '95 days'
                    ORDER BY p.date 
                    LIMIT 1
                ) as exit_price
            FROM dcf_valuations v
            JOIN company_master cm ON v.symbol = cm.primary_ticker
            WHERE v.model_version = 'clean_dcf_v1.0'
            AND EXTRACT(YEAR FROM v.publish_date) = 2024
            AND v.valuation_confidence >= 0.30
        )
        SELECT *
        FROM dcf_signals
        WHERE entry_price IS NOT NULL 
        AND exit_price IS NOT NULL
        AND entry_price > 0
        AND exit_price > 0
        AND fair_value > 0
        AND (fair_value - entry_price) / entry_price >= 0.20  -- 20% minimum upside
        ORDER BY publish_date, symbol
        """
        
        rows = await conn.fetch(query)
        
        print(f"📊 Found {len(rows)} clean DCF signals with complete data")
        
        # Convert to analysis dataframe
        data = []
        for row in rows:
            implied_return = (row['fair_value'] - row['entry_price']) / row['entry_price']
            actual_return = (row['exit_price'] - row['entry_price']) / row['entry_price']
            
            data.append({
                'symbol': row['symbol'],
                'company': row['company_name'],
                'sector': row['sector'],
                'signal_date': row['publish_date'],
                'entry_price': float(row['entry_price']),
                'exit_price': float(row['exit_price']),
                'fair_value': float(row['fair_value']),
                'confidence': float(row['valuation_confidence']),
                'predicted_return': float(implied_return),
                'actual_return': float(actual_return),
                'success': bool(actual_return > 0),
                'prediction_error': float(actual_return - implied_return)
            })
        
        df = pd.DataFrame(data)
        
        if len(df) == 0:
            print("❌ No qualifying signals found")
            return
        
        # Filter out extreme outliers for analysis
        df_clean = df[
            (df['predicted_return'] <= 5.0) &  # Max 500% predicted
            (df['actual_return'] <= 5.0) &     # Max 500% actual
            (df['actual_return'] >= -0.8)      # Max 80% loss
        ].copy()
        
        print(f"📈 Analyzing {len(df_clean)} signals (filtered from {len(df)} total)")
        
        # Log all trades first
        print(f"\n📋 ALL DCF TRADES LOG:")
        print(f"{'Symbol':<8} {'Company':<20} {'Signal Date':<12} {'Entry':<8} {'Exit':<8} {'Fair Value':<10} {'Predicted':<10} {'Actual':<10} {'Success':<8}")
        print("-" * 110)
        
        for _, trade in df_clean.iterrows():
            success_mark = "✅" if trade['success'] else "❌"
            print(f"{trade['symbol']:<8} {trade['company'][:19]:<20} {trade['signal_date']:<12} "
                  f"{trade['entry_price']:<8.2f} {trade['exit_price']:<8.2f} {trade['fair_value']:<10.2f} "
                  f"{trade['predicted_return']:+8.0%} {trade['actual_return']:+8.0%} {success_mark:<8}")
        
        # Key metrics
        win_rate = df_clean['success'].mean()
        avg_predicted = df_clean['predicted_return'].mean()
        avg_actual = df_clean['actual_return'].mean()
        
        print(f"\n📊 PERFORMANCE METRICS:")
        print(f"   Win Rate: {win_rate:.1%}")
        print(f"   Average Predicted Return: {avg_predicted:+.1%}")
        print(f"   Average Actual Return: {avg_actual:+.1%}")
        
        # Prediction accuracy
        if len(df_clean) > 1:
            correlation = df_clean['predicted_return'].corr(df_clean['actual_return'])
            print(f"   Prediction Correlation: {correlation:.3f}")
        
        # Confidence analysis
        high_conf = df_clean[df_clean['confidence'] > 0.5]
        if len(high_conf) > 0:
            print(f"   High Confidence (>50%): {len(high_conf)} signals, "
                  f"{high_conf['success'].mean():.1%} win rate")
        
        # Show top performers
        print(f"\n🏆 TOP 10 PERFORMERS (Actual Returns):")
        top_performers = df_clean.nlargest(10, 'actual_return')
        for _, trade in top_performers.iterrows():
            print(f"   {trade['symbol']:6} ({trade['company'][:20]:20}): "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%}")
        
        # Show notable predictions
        print(f"\n🎯 MOST ACCURATE PREDICTIONS:")
        df_clean['abs_error'] = abs(df_clean['prediction_error'])
        accurate_predictions = df_clean.nsmallest(5, 'abs_error')
        for _, trade in accurate_predictions.iterrows():
            print(f"   {trade['symbol']:6}: Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%}, "
                  f"Error: {trade['prediction_error']:+.1%}")
        
        # Sector analysis
        print(f"\n🏭 SECTOR PERFORMANCE:")
        sector_stats = df_clean.groupby('sector').agg({
            'success': 'mean',
            'actual_return': 'mean',
            'symbol': 'count'
        }).round(3)
        sector_stats.columns = ['Win_Rate', 'Avg_Return', 'Count']
        sector_stats = sector_stats[sector_stats['Count'] >= 2]  # At least 2 signals
        sector_stats = sector_stats.sort_values('Avg_Return', ascending=False)
        
        for sector, stats in sector_stats.head(8).iterrows():
            print(f"   {sector[:25]:25}: {stats['Win_Rate']:5.1%} win rate, "
                  f"{stats['Avg_Return']:+6.1%} avg return ({int(stats['Count'])} signals)")
        
        # Portfolio simulation (simple equal weight)
        print(f"\n💰 SIMPLE PORTFOLIO SIMULATION:")
        portfolio_return = df_clean['actual_return'].mean()
        portfolio_win_rate = df_clean['success'].mean()
        
        print(f"   Equal Weight Portfolio Return: {portfolio_return:+.1%}")
        print(f"   Portfolio Win Rate: {portfolio_win_rate:.1%}")
        
        if len(df_clean) > 1:
            portfolio_volatility = df_clean['actual_return'].std()
            sharpe_ratio = portfolio_return / portfolio_volatility if portfolio_volatility > 0 else 0
            print(f"   Portfolio Volatility: {portfolio_volatility:.1%}")
            print(f"   Sharpe Ratio: {sharpe_ratio:.2f}")
        
        # Save results
        df_clean.to_csv('clean_dcf_backtest_analysis.csv', index=False)
        print(f"\n💾 Results saved to clean_dcf_backtest_analysis.csv")
        
        # Show ALL trades including extremes
        print(f"\n📋 COMPLETE TRADE LOG (including extremes):")
        print(f"{'Symbol':<8} {'Company':<20} {'Signal Date':<12} {'Entry':<8} {'Exit':<8} {'Fair Value':<10} {'Predicted':<10} {'Actual':<10} {'Success':<8}")
        print("-" * 110)
        
        df_sorted = df.sort_values('actual_return', ascending=False)
        for _, trade in df_sorted.iterrows():
            success_mark = "✅" if trade['success'] else "❌"
            extreme_mark = "🚀" if abs(trade['actual_return']) > 5.0 else ""
            print(f"{trade['symbol']:<8} {trade['company'][:19]:<20} {trade['signal_date']:<12} "
                  f"{trade['entry_price']:<8.2f} {trade['exit_price']:<8.2f} {trade['fair_value']:<10.2f} "
                  f"{trade['predicted_return']:+8.0%} {trade['actual_return']:+8.0%} {success_mark:<8} {extreme_mark}")
        
        # Show extreme cases summary
        extremes = df[
            (df['predicted_return'] > 5.0) | 
            (df['actual_return'] > 5.0)
        ]
        
        if len(extremes) > 0:
            print(f"\n🚀 EXTREME WINNERS SUMMARY:")
            for _, trade in extremes.head(5).iterrows():
                print(f"   {trade['symbol']:6} ({trade['company'][:20]:20}): "
                      f"Predicted {trade['predicted_return']:+.0%}, "
                      f"Actual {trade['actual_return']:+.0%}")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_dcf_performance())