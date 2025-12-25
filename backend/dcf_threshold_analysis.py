#!/usr/bin/env python3
"""
DCF Threshold Analysis

Test different confidence and upside thresholds to find the sweet spot.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, date, timedelta

async def test_dcf_thresholds():
    """Test different thresholds for DCF signals."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🎯 DCF THRESHOLD ANALYSIS")
        print("=" * 60)
        
        # Test different combinations
        confidence_thresholds = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7]
        upside_thresholds = [0.1, 0.2, 0.3, 0.5, 1.0]  # 10%, 20%, 30%, 50%, 100%
        
        results = []
        
        for conf_thresh in confidence_thresholds:
            for upside_thresh in upside_thresholds:
                
                print(f"🔍 Testing: Confidence ≥{conf_thresh:.0%}, Upside ≥{upside_thresh:.0%}")
                
                # Get DCF signals with these thresholds
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
                AND v.valuation_confidence >= $1
                ORDER BY v.publish_date, v.symbol
                """
                
                dcf_rows = await conn.fetch(dcf_query, conf_thresh)
                
                clean_trades = []
                corporate_actions_filtered = 0
                insufficient_upside = 0
                no_price_data = 0
                
                for row in dcf_rows:
                    symbol = row['symbol']
                    signal_date = row['publish_date']
                    fair_value = float(row['fair_value'])
                    
                    # Get entry price
                    entry_result = await conn.fetchrow("""
                        SELECT close_price, date
                        FROM daily_price_data
                        WHERE symbol = $1
                        AND date >= $2
                        AND date <= $3
                        ORDER BY date LIMIT 1
                    """, symbol, signal_date + timedelta(days=1), signal_date + timedelta(days=7))
                    
                    if not entry_result:
                        no_price_data += 1
                        continue
                    
                    entry_price = float(entry_result['close_price'])
                    entry_date = entry_result['date']
                    
                    # Check upside threshold
                    predicted_return = (fair_value - entry_price) / entry_price
                    if predicted_return < upside_thresh:
                        insufficient_upside += 1
                        continue
                    
                    # Get exit price
                    exit_result = await conn.fetchrow("""
                        SELECT close_price, date
                        FROM daily_price_data
                        WHERE symbol = $1
                        AND date >= $2
                        AND date <= $3
                        ORDER BY date LIMIT 1
                    """, symbol, entry_date + timedelta(days=80), entry_date + timedelta(days=100))
                    
                    if not exit_result:
                        no_price_data += 1
                        continue
                        
                    exit_price = float(exit_result['close_price'])
                    exit_date = exit_result['date']
                    actual_return = (exit_price - entry_price) / entry_price
                    
                    # Filter suspicious returns (corporate actions)
                    if abs(actual_return) > 2.0:
                        corporate_actions_filtered += 1
                        continue
                    
                    # Check for price discontinuities
                    price_history = await conn.fetch("""
                        SELECT date, close_price
                        FROM daily_price_data
                        WHERE symbol = $1
                        AND date >= $2
                        AND date <= $3
                        ORDER BY date
                    """, symbol, entry_date, exit_date)
                    
                    # Look for jumps >100%
                    has_discontinuity = False
                    for i in range(1, len(price_history)):
                        prev_price = float(price_history[i-1]['close_price'])
                        curr_price = float(price_history[i]['close_price'])
                        
                        if prev_price > 0:
                            jump = abs((curr_price - prev_price) / prev_price)
                            if jump > 1.0:
                                has_discontinuity = True
                                break
                    
                    if has_discontinuity:
                        corporate_actions_filtered += 1
                        continue
                    
                    # Clean trade
                    clean_trades.append({
                        'symbol': symbol,
                        'actual_return': actual_return,
                        'predicted_return': predicted_return,
                        'success': actual_return > 0
                    })
                
                # Calculate metrics for this threshold combination
                if clean_trades:
                    df = pd.DataFrame(clean_trades)
                    
                    total_trades = len(df)
                    win_rate = df['success'].mean()
                    avg_actual = df['actual_return'].mean()
                    avg_predicted = df['predicted_return'].mean()
                    
                    if len(df) > 1:
                        correlation = df['predicted_return'].corr(df['actual_return'])
                        volatility = df['actual_return'].std()
                        sharpe = avg_actual / volatility if volatility > 0 else 0
                    else:
                        correlation = 0
                        volatility = 0
                        sharpe = 0
                    
                    results.append({
                        'confidence_thresh': conf_thresh,
                        'upside_thresh': upside_thresh,
                        'total_signals': len(dcf_rows),
                        'clean_trades': total_trades,
                        'win_rate': win_rate,
                        'avg_actual_return': avg_actual,
                        'avg_predicted_return': avg_predicted,
                        'correlation': correlation,
                        'volatility': volatility,
                        'sharpe_ratio': sharpe,
                        'filtered_corp_actions': corporate_actions_filtered,
                        'filtered_insufficient_upside': insufficient_upside,
                        'filtered_no_price': no_price_data
                    })
                    
                    print(f"   📊 {total_trades} trades: {win_rate:.1%} win rate, {avg_actual:+.1%} avg return, {correlation:.2f} correlation")
                else:
                    print(f"   ❌ No clean trades found")
        
        # Convert results to DataFrame and analyze
        results_df = pd.DataFrame(results)
        
        print(f"\n📊 COMPREHENSIVE THRESHOLD ANALYSIS")
        print("=" * 80)
        print(f"{'Conf':<5} {'Upside':<6} {'Signals':<7} {'Trades':<6} {'Win%':<6} {'Avg Ret':<8} {'Corr':<6} {'Sharpe':<6}")
        print("-" * 80)
        
        for _, row in results_df.iterrows():
            print(f"{row['confidence_thresh']:4.0%} {row['upside_thresh']:5.0%} "
                  f"{int(row['total_signals']):6} {int(row['clean_trades']):5} "
                  f"{row['win_rate']:5.0%} {row['avg_actual_return']:+7.1%} "
                  f"{row['correlation']:5.2f} {row['sharpe_ratio']:5.2f}")
        
        # Find best combinations
        print(f"\n🏆 BEST PERFORMING COMBINATIONS:")
        
        # Best by win rate (min 5 trades)
        good_sample = results_df[results_df['clean_trades'] >= 5]
        if len(good_sample) > 0:
            best_win_rate = good_sample.loc[good_sample['win_rate'].idxmax()]
            print(f"   Best Win Rate: Confidence ≥{best_win_rate['confidence_thresh']:.0%}, "
                  f"Upside ≥{best_win_rate['upside_thresh']:.0%} → "
                  f"{best_win_rate['win_rate']:.0%} win rate, {best_win_rate['avg_actual_return']:+.1%} avg return")
        
        # Best by average return
        if len(good_sample) > 0:
            best_return = good_sample.loc[good_sample['avg_actual_return'].idxmax()]
            print(f"   Best Avg Return: Confidence ≥{best_return['confidence_thresh']:.0%}, "
                  f"Upside ≥{best_return['upside_thresh']:.0%} → "
                  f"{best_return['win_rate']:.0%} win rate, {best_return['avg_actual_return']:+.1%} avg return")
        
        # Best by Sharpe ratio
        if len(good_sample) > 0:
            best_sharpe = good_sample.loc[good_sample['sharpe_ratio'].idxmax()]
            print(f"   Best Sharpe: Confidence ≥{best_sharpe['confidence_thresh']:.0%}, "
                  f"Upside ≥{best_sharpe['upside_thresh']:.0%} → "
                  f"{best_sharpe['sharpe_ratio']:.2f} Sharpe, {best_sharpe['avg_actual_return']:+.1%} avg return")
        
        # Save detailed results
        results_df.to_csv('dcf_threshold_analysis.csv', index=False)
        print(f"\n💾 Detailed results saved to dcf_threshold_analysis.csv")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_dcf_thresholds())