#!/usr/bin/env python3
"""
Simple Clean DCF Backtest

Clean approach that filters corporate actions and provides realistic results.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, date, timedelta

async def simple_clean_dcf_backtest():
    """Run simple clean DCF backtest."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🧹 SIMPLE CLEAN DCF BACKTEST")
        print("=" * 50)
        
        # Get all DCF signals from 2024
        dcf_query = """
        SELECT 
            v.symbol,
            v.publish_date,
            v.fair_value_stock_median as fair_value,
            v.valuation_confidence,
            cm.company_name,
            cm.sector
        FROM dcf_valuations v
        JOIN company_master cm ON v.symbol = cm.primary_ticker
        WHERE v.model_version = 'clean_dcf_v1.0'
        AND v.publish_date >= '2024-01-01'
        AND v.publish_date <= '2024-12-31'
        AND v.valuation_confidence >= 0.30
        ORDER BY v.publish_date, v.symbol
        """
        
        dcf_rows = await conn.fetch(dcf_query)
        print(f"📊 Found {len(dcf_rows)} DCF signals from 2024")
        
        clean_trades = []
        
        for row in dcf_rows:
            symbol = row['symbol']
            signal_date = row['publish_date']
            fair_value = float(row['fair_value'])
            
            # Get entry price (3-7 days after signal)
            entry_query = """
            SELECT close_price, date
            FROM daily_price_data
            WHERE symbol = $1
            AND date >= $2
            AND date <= $3
            ORDER BY date
            LIMIT 1
            """
            
            entry_result = await conn.fetchrow(
                entry_query, 
                symbol, 
                signal_date + timedelta(days=1),
                signal_date + timedelta(days=7)
            )
            
            if not entry_result:
                continue
            
            entry_price = float(entry_result['close_price'])
            entry_date = entry_result['date']
            
            # Check if there's reasonable upside (20%+)
            predicted_return = (fair_value - entry_price) / entry_price
            if predicted_return < 0.20:
                continue
            
            # Get exit price (80-100 days later)
            exit_query = """
            SELECT close_price, date
            FROM daily_price_data
            WHERE symbol = $1
            AND date >= $2
            AND date <= $3
            ORDER BY date
            LIMIT 1
            """
            
            exit_result = await conn.fetchrow(
                exit_query,
                symbol,
                entry_date + timedelta(days=80),
                entry_date + timedelta(days=100)
            )
            
            if not exit_result:
                continue
                
            exit_price = float(exit_result['close_price'])
            exit_date = exit_result['date']
            actual_return = (exit_price - entry_price) / entry_price
            
            # Filter suspicious returns (likely corporate actions)
            if abs(actual_return) > 2.0:  # >200% return is suspicious
                print(f"   ⚠️  {symbol}: Suspicious return {actual_return:+.0%} - filtering out")
                continue
            
            # Check for obvious price discontinuities (reverse splits)
            discontinuity_query = """
            SELECT date, close_price
            FROM daily_price_data
            WHERE symbol = $1
            AND date >= $2
            AND date <= $3
            ORDER BY date
            """
            
            price_history = await conn.fetch(
                discontinuity_query,
                symbol,
                entry_date,
                exit_date
            )
            
            # Look for jumps >100% in a day
            has_discontinuity = False
            for i in range(1, len(price_history)):
                prev_price = float(price_history[i-1]['close_price'])
                curr_price = float(price_history[i]['close_price'])
                
                if prev_price > 0:
                    jump = abs((curr_price - prev_price) / prev_price)
                    if jump > 1.0:  # >100% jump
                        has_discontinuity = True
                        break
            
            if has_discontinuity:
                print(f"   ⚠️  {symbol}: Price discontinuity detected - filtering out")
                continue
            
            # This is a clean trade
            clean_trades.append({
                'symbol': symbol,
                'company': row['company_name'],
                'sector': row['sector'],
                'signal_date': signal_date,
                'entry_date': entry_date,
                'exit_date': exit_date,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'fair_value': fair_value,
                'confidence': float(row['valuation_confidence']),
                'predicted_return': predicted_return,
                'actual_return': actual_return,
                'success': actual_return > 0,
                'days_held': (exit_date - entry_date).days
            })
        
        print(f"✅ {len(clean_trades)} clean trades after filtering")
        
        if not clean_trades:
            print("❌ No clean trades found")
            return
        
        # Analysis
        df = pd.DataFrame(clean_trades)
        
        # Display all trades
        print(f"\n📋 ALL CLEAN TRADES:")
        print(f"{'Symbol':<8} {'Company':<20} {'Date':<12} {'Entry':<7} {'Exit':<7} {'Fair':<7} {'Pred':<7} {'Actual':<7} {'Days':<5} {'Success':<7}")
        print("-" * 95)
        
        for _, trade in df.iterrows():
            success_mark = "✅" if trade['success'] else "❌"
            print(f"{trade['symbol']:<8} {trade['company'][:19]:<20} {trade['signal_date']!s:<12} "
                  f"{trade['entry_price']:<7.2f} {trade['exit_price']:<7.2f} {trade['fair_value']:<7.2f} "
                  f"{trade['predicted_return']:+6.0%} {trade['actual_return']:+6.0%} "
                  f"{trade['days_held']:<5} {success_mark:<7}")
        
        # Summary statistics
        total_trades = len(df)
        win_rate = df['success'].mean()
        avg_predicted = df['predicted_return'].mean()
        avg_actual = df['actual_return'].mean()
        avg_days = df['days_held'].mean()
        
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"   Total Trades: {total_trades}")
        print(f"   Win Rate: {win_rate:.1%}")
        print(f"   Average Predicted Return: {avg_predicted:+.1%}")
        print(f"   Average Actual Return: {avg_actual:+.1%}")
        print(f"   Average Holding Period: {avg_days:.0f} days")
        
        if len(df) > 1:
            correlation = df['predicted_return'].corr(df['actual_return'])
            volatility = df['actual_return'].std()
            sharpe = avg_actual / volatility if volatility > 0 else 0
            
            print(f"   Prediction Correlation: {correlation:.3f}")
            print(f"   Return Volatility: {volatility:.1%}")
            print(f"   Sharpe Ratio: {sharpe:.2f}")
        
        # Sector breakdown
        if len(df) >= 3:
            print(f"\n🏭 SECTOR BREAKDOWN:")
            sector_stats = df.groupby('sector').agg({
                'success': ['count', 'mean'],
                'actual_return': 'mean'
            }).round(3)
            
            for sector in sector_stats.index:
                count = sector_stats.loc[sector, ('success', 'count')]
                win_rate = sector_stats.loc[sector, ('success', 'mean')]
                avg_return = sector_stats.loc[sector, ('actual_return', 'mean')]
                
                if count >= 2:  # At least 2 trades
                    print(f"   {sector[:25]:25}: {count} trades, {win_rate:4.0%} win rate, {avg_return:+5.1%} avg return")
        
        # Best and worst
        df_sorted = df.sort_values('actual_return', ascending=False)
        
        print(f"\n🏆 BEST PERFORMERS:")
        for _, trade in df_sorted.head(3).iterrows():
            print(f"   {trade['symbol']:6}: {trade['actual_return']:+5.0%} actual ({trade['predicted_return']:+5.0%} predicted)")
        
        print(f"\n💸 WORST PERFORMERS:")
        for _, trade in df_sorted.tail(3).iterrows():
            print(f"   {trade['symbol']:6}: {trade['actual_return']:+5.0%} actual ({trade['predicted_return']:+5.0%} predicted)")
        
        # Save results
        df.to_csv('simple_clean_dcf_backtest.csv', index=False)
        print(f"\n💾 Results saved to simple_clean_dcf_backtest.csv")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(simple_clean_dcf_backtest())