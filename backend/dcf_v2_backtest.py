#!/usr/bin/env python3
"""
DCF v2.0 Backtest

Test the improved DCF model on 2024 data.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta
from dcf_v2_simple import SimpleDCF2

async def backtest_dcf_v2():
    """Backtest DCF v2.0 model"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    dcf = SimpleDCF2()
    await dcf.setup()
    
    try:
        print("🚀 DCF v2.0 BACKTEST")
        print("=" * 60)
        
        # Get test dates throughout 2024
        test_dates = [
            datetime(2024, 1, 15),
            datetime(2024, 2, 15),
            datetime(2024, 3, 15),
            datetime(2024, 4, 15),
            datetime(2024, 5, 15),
            datetime(2024, 6, 15),
        ]
        
        all_trades = []
        
        for test_date in test_dates:
            print(f"\n📅 Testing signals from {test_date.date()}")
            
            # Get companies with recent financial reports
            companies_query = """
            SELECT DISTINCT 
                fs.symbol,
                cm.sector
            FROM financial_statements fs
            JOIN company_master cm ON fs.symbol = cm.primary_ticker
            WHERE fs.publish_date <= $1
            AND fs.publish_date >= $1 - INTERVAL '90 days'
            AND fs.total_revenue > 0
            AND EXISTS (
                SELECT 1 FROM balance_sheet_data bs 
                WHERE bs.symbol = fs.symbol 
                AND bs.shares_outstanding > 0
            )
            ORDER BY fs.symbol
            """
            
            companies = await conn.fetch(companies_query, test_date.date())
            
            print(f"   Found {len(companies)} companies with recent reports")
            
            signals_generated = 0
            
            for company in companies:
                symbol = company['symbol']
                
                # Get market price
                price_result = await conn.fetchrow("""
                    SELECT close_price 
                    FROM daily_price_data 
                    WHERE symbol = $1 
                    AND date <= $2 
                    AND date >= $2 - INTERVAL '7 days'
                    ORDER BY date DESC 
                    LIMIT 1
                """, symbol, test_date.date())
                
                if not price_result:
                    continue
                
                market_price = float(price_result['close_price'])
                
                # Run DCF v2
                try:
                    result = await dcf.value_company(symbol, test_date, market_price)
                    
                    if result and result['implied_return'] > 0.2 and result['confidence'] > 0.3:
                        signals_generated += 1
                        
                        # Get exit price 90 days later
                        exit_result = await conn.fetchrow("""
                            SELECT close_price, date
                            FROM daily_price_data
                            WHERE symbol = $1
                            AND date >= $2
                            AND date <= $3
                            ORDER BY date
                            LIMIT 1
                        """, symbol, 
                            test_date.date() + timedelta(days=85),
                            test_date.date() + timedelta(days=95)
                        )
                        
                        if exit_result:
                            exit_price = float(exit_result['close_price'])
                            actual_return = (exit_price - market_price) / market_price
                            
                            # Filter out corporate actions
                            if abs(actual_return) < 2.0:  # Less than 200%
                                all_trades.append({
                                    'symbol': symbol,
                                    'sector': result['sector'],
                                    'signal_date': test_date.date(),
                                    'entry_price': market_price,
                                    'exit_price': exit_price,
                                    'fair_value': result['fair_value'],
                                    'predicted_return': result['implied_return'],
                                    'actual_return': actual_return,
                                    'confidence': result['confidence'],
                                    'wacc': result['wacc_used'],
                                    'success': actual_return > 0
                                })
                
                except Exception as e:
                    # Ignore individual errors
                    pass
            
            print(f"   Generated {signals_generated} buy signals")
        
        # Analyze results
        if all_trades:
            df = pd.DataFrame(all_trades)
            
            print(f"\n📊 DCF v2.0 BACKTEST RESULTS")
            print("=" * 60)
            print(f"Total Trades: {len(df)}")
            
            # Overall metrics
            win_rate = df['success'].mean()
            avg_predicted = df['predicted_return'].mean()
            avg_actual = df['actual_return'].mean()
            
            print(f"Win Rate: {win_rate:.1%}")
            print(f"Average Predicted Return: {avg_predicted:+.1%}")
            print(f"Average Actual Return: {avg_actual:+.1%}")
            
            if len(df) > 1:
                correlation = df['predicted_return'].corr(df['actual_return'])
                print(f"Prediction Correlation: {correlation:.3f}")
            
            # By confidence level
            print(f"\n📈 RESULTS BY CONFIDENCE:")
            for conf_level in [0.3, 0.5, 0.7]:
                high_conf = df[df['confidence'] >= conf_level]
                if len(high_conf) > 0:
                    print(f"   Confidence ≥{conf_level:.0%}: {len(high_conf)} trades, "
                          f"{high_conf['success'].mean():.0%} win rate, "
                          f"{high_conf['actual_return'].mean():+.1%} avg return")
            
            # By sector
            print(f"\n🏭 RESULTS BY SECTOR:")
            sector_stats = df.groupby('sector').agg({
                'success': ['count', 'mean'],
                'actual_return': 'mean'
            }).round(3)
            
            for sector in sector_stats.index[:5]:  # Top 5 sectors
                count = int(sector_stats.loc[sector, ('success', 'count')])
                if count >= 2:
                    win_rate = sector_stats.loc[sector, ('success', 'mean')]
                    avg_return = sector_stats.loc[sector, ('actual_return', 'mean')]
                    print(f"   {sector[:20]:20}: {count:3} trades, {win_rate:4.0%} win, {avg_return:+5.1%} return")
            
            # Show sample trades
            print(f"\n📋 SAMPLE TRADES:")
            print(f"{'Symbol':<8} {'Sector':<15} {'Entry':<7} {'Fair':<7} {'Pred':<6} {'Actual':<7} {'Success'}")
            print("-" * 70)
            
            for _, trade in df.head(10).iterrows():
                success_mark = "✅" if trade['success'] else "❌"
                print(f"{trade['symbol']:<8} {trade['sector'][:14]:<15} "
                      f"{trade['entry_price']:<7.2f} {trade['fair_value']:<7.0f} "
                      f"{trade['predicted_return']:+5.0%} {trade['actual_return']:+6.0%} {success_mark}")
            
            # Save results
            df.to_csv('dcf_v2_backtest_results.csv', index=False)
            print(f"\n💾 Results saved to dcf_v2_backtest_results.csv")
            
            # Compare with v1.0
            print(f"\n🆚 DCF v2.0 vs v1.0:")
            print(f"   v1.0: 33% win rate, -12.8% avg return")
            print(f"   v2.0: {win_rate:.0%} win rate, {avg_actual:+.1%} avg return")
            
            if avg_actual > -0.128:
                print(f"   ✅ v2.0 is {abs(avg_actual - (-0.128)):.1%} better!")
            else:
                print(f"   ❌ v2.0 is {abs(avg_actual - (-0.128)):.1%} worse")
            
        else:
            print("❌ No valid trades found")
    
    finally:
        await conn.close()
        await dcf.cleanup()

if __name__ == "__main__":
    asyncio.run(backtest_dcf_v2())