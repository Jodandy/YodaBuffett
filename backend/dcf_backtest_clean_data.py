#!/usr/bin/env python3
"""
DCF Backtest with Corporate Actions Filtering

Filters out stocks with reverse splits, stock splits, and other corporate actions
that create artificial returns.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, date

async def clean_dcf_backtest():
    """Run DCF backtest with corporate actions filtering."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🧹 CLEAN DCF BACKTEST (Corporate Actions Filtered)")
        print("=" * 65)
        
        # Get DCF signals first
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
        AND EXTRACT(YEAR FROM v.publish_date) = 2024
        AND v.valuation_confidence >= 0.30
        AND (v.fair_value_stock_median / (
            SELECT p.close_price 
            FROM daily_price_data p 
            WHERE p.symbol = v.symbol 
            AND p.date <= v.publish_date + INTERVAL '7 days'
            AND p.date >= v.publish_date - INTERVAL '7 days'
            ORDER BY p.date
            LIMIT 1
        )) >= 1.20  -- At least 20% upside
        ORDER BY v.publish_date, v.symbol
        """
        
        dcf_rows = await conn.fetch(dcf_query)
        print(f"📊 Found {len(dcf_rows)} DCF signals with 20%+ upside")
        
        # Analyze each signal for corporate actions
        clean_trades = []
        
        for row in dcf_rows:
            symbol = row['symbol']
            signal_date = row['publish_date']
            
            # Get price history around signal
            price_query = """
            SELECT date, close_price, volume
            FROM daily_price_data
            WHERE symbol = $1
            AND date BETWEEN $2 - INTERVAL '10 days' AND $3 + INTERVAL '100 days'
            ORDER BY date
            """
            
            prices = await conn.fetch(price_query, symbol, signal_date, signal_date)
            
            if len(prices) < 20:  # Need sufficient price history
                continue
            
            # Check for corporate actions (suspicious jumps >200% in a day)
            has_corporate_action = False
            entry_price = None
            exit_price = None
            
            for i, price_row in enumerate(prices):
                price_date = price_row['date']
                price = float(price_row['close_price'])
                
                # Find entry price (first price after signal date)
                if not entry_price and price_date >= signal_date:
                    entry_price = price
                    entry_date = price_date
                
                # Check for suspicious jumps
                if i > 0:
                    prev_price = float(prices[i-1]['close_price'])
                    if prev_price > 0:
                        jump = abs((price - prev_price) / prev_price)
                        if jump > 2.0:  # >200% jump = likely corporate action
                            has_corporate_action = True
                            break
                
                # Find exit price (around 90 days after entry)
                if entry_price and not exit_price:
                    if (price_date - entry_date).days >= 85:
                        exit_price = price
                        exit_date = price_date
                        break
            
            # Skip if corporate action detected
            if has_corporate_action:
                print(f"   ⚠️  {symbol}: Corporate action detected - skipping")
                continue
            
            # Skip if missing entry/exit prices
            if not entry_price or not exit_price:
                continue
            
            # Calculate returns
            fair_value = float(row['fair_value'])
            predicted_return = (fair_value - entry_price) / entry_price
            actual_return = (exit_price - entry_price) / entry_price
            
            # Additional sanity checks
            if abs(actual_return) > 3.0:  # >300% return still suspicious
                print(f"   ⚠️  {symbol}: Extreme return ({actual_return:+.0%}) - skipping")
                continue
            
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
                'prediction_error': actual_return - predicted_return
            })
        
        print(f"✅ {len(clean_trades)} clean trades after filtering")
        
        if not clean_trades:
            print("❌ No clean trades found")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(clean_trades)
        
        # Display all clean trades
        print(f"\n📋 ALL CLEAN DCF TRADES:")
        print(f"{'Symbol':<8} {'Company':<20} {'Signal Date':<12} {'Entry':<8} {'Exit':<8} {'Fair Value':<10} {'Predicted':<10} {'Actual':<10} {'Success':<8}")
        print("-" * 110)
        
        for _, trade in df.iterrows():
            success_mark = "✅" if trade['success'] else "❌"
            print(f"{trade['symbol']:<8} {trade['company'][:19]:<20} {trade['signal_date']:<12} "
                  f"{trade['entry_price']:<8.2f} {trade['exit_price']:<8.2f} {trade['fair_value']:<10.2f} "
                  f"{trade['predicted_return']:+8.0%} {trade['actual_return']:+8.0%} {success_mark:<8}")
        
        # Performance metrics
        win_rate = df['success'].mean()
        avg_predicted = df['predicted_return'].mean()
        avg_actual = df['actual_return'].mean()
        
        print(f"\n📊 CLEAN PERFORMANCE METRICS:")
        print(f"   Total Clean Trades: {len(df)}")
        print(f"   Win Rate: {win_rate:.1%}")
        print(f"   Average Predicted Return: {avg_predicted:+.1%}")
        print(f"   Average Actual Return: {avg_actual:+.1%}")
        
        if len(df) > 1:
            correlation = df['predicted_return'].corr(df['actual_return'])
            volatility = df['actual_return'].std()
            sharpe = avg_actual / volatility if volatility > 0 else 0
            
            print(f"   Prediction Correlation: {correlation:.3f}")
            print(f"   Return Volatility: {volatility:.1%}")
            print(f"   Sharpe Ratio: {sharpe:.2f}")
        
        # Best and worst performers
        df_sorted = df.sort_values('actual_return', ascending=False)
        
        print(f"\n🏆 BEST PERFORMERS:")
        for _, trade in df_sorted.head(5).iterrows():
            print(f"   {trade['symbol']:6} ({trade['company'][:20]:20}): "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%}")
        
        print(f"\n💸 WORST PERFORMERS:")
        for _, trade in df_sorted.tail(5).iterrows():
            print(f"   {trade['symbol']:6} ({trade['company'][:20]:20}): "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%}")
        
        # Sector analysis
        if len(df) > 3:
            print(f"\n🏭 SECTOR PERFORMANCE:")
            sector_stats = df.groupby('sector').agg({
                'success': 'mean',
                'actual_return': 'mean',
                'symbol': 'count'
            }).round(3)
            sector_stats.columns = ['Win_Rate', 'Avg_Return', 'Count']
            sector_stats = sector_stats[sector_stats['Count'] >= 2]
            
            for sector, stats in sector_stats.iterrows():
                print(f"   {sector[:30]:30}: {stats['Win_Rate']:5.1%} win rate, "
                      f"{stats['Avg_Return']:+6.1%} avg return ({int(stats['Count'])} signals)")
        
        # Save results
        df.to_csv('dcf_clean_backtest.csv', index=False)
        print(f"\n💾 Clean results saved to dcf_clean_backtest.csv")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(clean_dcf_backtest())