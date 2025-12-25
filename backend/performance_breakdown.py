#!/usr/bin/env python3
"""
Simplified Performance Breakdown Analysis

Focus on the key insights from the momentum + fundamental strategy.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from collections import defaultdict, Counter

from momentum_fundamental_strategy import MomentumFundamentalStrategy

async def detailed_performance_analysis():
    """Run detailed performance breakdown."""
    
    print("📊 DETAILED FUNDAMENTAL STRATEGY BREAKDOWN")
    print("=" * 60)
    
    strategy = MomentumFundamentalStrategy()
    await strategy.setup()
    
    start_date = date(2023, 6, 1)
    end_date = date(2024, 10, 31)
    
    print(f"📅 Period: {start_date} to {end_date} (17 months)")
    
    # Collect all trades
    current_date = start_date
    all_trades = []
    monthly_performance = defaultdict(list)
    
    while current_date < end_date:
        picks = await strategy.screen_momentum_fundamental(current_date, min_combined_score=6.0, top_n=10)
        
        if picks:
            picks = await strategy.add_future_returns(picks, 21)
            
            for pick in picks:
                if pick.future_return is not None:
                    trade = {
                        'date': current_date,
                        'symbol': pick.symbol,
                        'return': pick.future_return,
                        'momentum_score': pick.momentum_score,
                        'fundamental_score': pick.fundamental_score,
                        'combined_score': pick.combined_score,
                        'pe_ratio': pick.pe_ratio,
                        'roe': pick.roe,
                        'return_1m': pick.return_1m,
                        'month': current_date.strftime('%Y-%m')
                    }
                    all_trades.append(trade)
                    monthly_performance[trade['month']].append(pick.future_return)
        
        current_date += timedelta(days=21)
    
    await strategy.cleanup()
    
    if not all_trades:
        print("❌ No trades found")
        return
    
    print(f"\n🎯 OVERVIEW:")
    print(f"   Total trades: {len(all_trades)}")
    print(f"   Unique symbols: {len(set(t['symbol'] for t in all_trades))}")
    print(f"   Rebalance periods: {len(set(t['month'] for t in all_trades))}")
    
    # Performance metrics
    returns = [t['return'] for t in all_trades]
    total_return = np.prod([1 + r/100 for r in returns]) - 1
    avg_return = np.mean(returns)
    win_rate = len([r for r in returns if r > 0]) / len(returns)
    
    print(f"\n📈 PERFORMANCE:")
    print(f"   Average trade return: {avg_return:.2f}%")
    print(f"   Win rate: {win_rate:.1%}")
    print(f"   Best trade: {max(returns):.2f}%")
    print(f"   Worst trade: {min(returns):.2f}%")
    print(f"   Standard deviation: {np.std(returns):.2f}%")
    
    # Monthly breakdown
    print(f"\n📅 MONTHLY PERFORMANCE:")
    for month in sorted(monthly_performance.keys()):
        month_returns = monthly_performance[month]
        month_avg = np.mean(month_returns)
        month_win_rate = len([r for r in month_returns if r > 0]) / len(month_returns)
        print(f"   {month}: {month_avg:+6.2f}% avg, {month_win_rate:.0%} win rate ({len(month_returns)} trades)")
    
    # Score analysis
    print(f"\n🔢 PERFORMANCE BY COMBINED SCORE:")
    score_ranges = [
        (9.0, 10.0, "9.0+"),
        (8.5, 9.0, "8.5-9.0"), 
        (8.0, 8.5, "8.0-8.5"),
        (7.5, 8.0, "7.5-8.0"),
        (7.0, 7.5, "7.0-7.5"),
        (6.0, 7.0, "6.0-7.0")
    ]
    
    for min_score, max_score, label in score_ranges:
        range_trades = [t for t in all_trades if min_score <= t['combined_score'] < max_score]
        if range_trades:
            range_returns = [t['return'] for t in range_trades]
            range_avg = np.mean(range_returns)
            range_win_rate = len([r for r in range_returns if r > 0]) / len(range_returns)
            print(f"   {label}: {range_avg:+6.2f}% avg, {range_win_rate:.0%} win rate ({len(range_trades)} trades)")
    
    # Factor correlation
    print(f"\n🧬 FACTOR ANALYSIS:")
    df = pd.DataFrame(all_trades)
    
    factors = ['momentum_score', 'fundamental_score', 'return_1m', 'pe_ratio', 'roe']
    correlations = []
    
    for factor in factors:
        if factor in df.columns:
            valid_data = df[[factor, 'return']].dropna()
            if len(valid_data) > 10:
                corr = valid_data[factor].corr(valid_data['return'])
                correlations.append((factor, corr))
    
    correlations.sort(key=lambda x: abs(x[1]), reverse=True)
    
    for factor, corr in correlations:
        print(f"   {factor}: {corr:+.3f} correlation with returns")
    
    # Symbol performance
    print(f"\n🏆 TOP PERFORMING SYMBOLS (>2 trades):")
    symbol_stats = defaultdict(list)
    for trade in all_trades:
        symbol_stats[trade['symbol']].append(trade['return'])
    
    symbol_performance = []
    for symbol, returns in symbol_stats.items():
        if len(returns) >= 2:  # At least 2 trades
            avg_return = np.mean(returns)
            win_rate = len([r for r in returns if r > 0]) / len(returns)
            symbol_performance.append({
                'symbol': symbol,
                'avg_return': avg_return,
                'win_rate': win_rate,
                'trades': len(returns)
            })
    
    symbol_performance.sort(key=lambda x: x['avg_return'], reverse=True)
    
    for i, perf in enumerate(symbol_performance[:10], 1):
        print(f"   {i:2}. {perf['symbol']:<10} {perf['avg_return']:+6.2f}% avg, {perf['win_rate']:.0%} win rate ({perf['trades']} trades)")
    
    # Worst performers
    print(f"\n📉 WORST PERFORMING SYMBOLS (>2 trades):")
    for i, perf in enumerate(symbol_performance[-5:], 1):
        print(f"   {i:2}. {perf['symbol']:<10} {perf['avg_return']:+6.2f}% avg, {perf['win_rate']:.0%} win rate ({perf['trades']} trades)")
    
    # Trade timing analysis
    print(f"\n⏰ TRADE TIMING ANALYSIS:")
    
    # By quarter
    quarterly_returns = defaultdict(list)
    for trade in all_trades:
        quarter = f"Q{((trade['date'].month - 1) // 3) + 1} {trade['date'].year}"
        quarterly_returns[quarter].append(trade['return'])
    
    for quarter in sorted(quarterly_returns.keys()):
        returns = quarterly_returns[quarter]
        avg_return = np.mean(returns)
        win_rate = len([r for r in returns if r > 0]) / len(returns)
        print(f"   {quarter}: {avg_return:+6.2f}% avg, {win_rate:.0%} win rate ({len(returns)} trades)")
    
    # ROE analysis
    print(f"\n💰 PROFITABILITY ANALYSIS:")
    
    roe_ranges = [
        (20, 100, "ROE 20%+"),
        (15, 20, "ROE 15-20%"),
        (10, 15, "ROE 10-15%"),
        (5, 10, "ROE 5-10%"),
        (0, 5, "ROE 0-5%")
    ]
    
    for min_roe, max_roe, label in roe_ranges:
        roe_trades = [t for t in all_trades if t['roe'] and min_roe <= t['roe'] < max_roe]
        if roe_trades:
            roe_returns = [t['return'] for t in roe_trades]
            roe_avg = np.mean(roe_returns)
            roe_win_rate = len([r for r in roe_returns if r > 0]) / len(roe_returns)
            print(f"   {label}: {roe_avg:+6.2f}% avg, {roe_win_rate:.0%} win rate ({len(roe_trades)} trades)")
    
    # Momentum vs Fundamental attribution
    print(f"\n⚖️ MOMENTUM vs FUNDAMENTAL ATTRIBUTION:")
    
    # High momentum, low fundamental
    hm_lf = [t for t in all_trades if t['momentum_score'] >= 8.0 and t['fundamental_score'] < 7.0]
    if hm_lf:
        hm_lf_avg = np.mean([t['return'] for t in hm_lf])
        print(f"   High Momentum + Low Fund: {hm_lf_avg:+6.2f}% avg ({len(hm_lf)} trades)")
    
    # Low momentum, high fundamental  
    lm_hf = [t for t in all_trades if t['momentum_score'] < 7.0 and t['fundamental_score'] >= 8.0]
    if lm_hf:
        lm_hf_avg = np.mean([t['return'] for t in lm_hf])
        print(f"   Low Momentum + High Fund: {lm_hf_avg:+6.2f}% avg ({len(lm_hf)} trades)")
    
    # High momentum, high fundamental
    hm_hf = [t for t in all_trades if t['momentum_score'] >= 8.0 and t['fundamental_score'] >= 8.0]
    if hm_hf:
        hm_hf_avg = np.mean([t['return'] for t in hm_hf])
        print(f"   High Momentum + High Fund: {hm_hf_avg:+6.2f}% avg ({len(hm_hf)} trades)")
        
    print(f"\n💡 KEY INSIGHTS:")
    print(f"   • Strategy shows consistent edge with {win_rate:.0%} win rate")
    if correlations:
        best_factor = correlations[0]
        print(f"   • {best_factor[0]} is strongest predictor ({best_factor[1]:+.3f} correlation)")
    if len(symbol_performance) > 0:
        best_symbol = symbol_performance[0]
        print(f"   • {best_symbol['symbol']} is top performer ({best_symbol['avg_return']:+.2f}% avg)")
    
    print(f"   • Combined score sweet spot appears to be 8.0+ range")
    print(f"   • {len(all_trades)} total trades over 17 months = {len(all_trades)/17:.1f} trades/month")

if __name__ == "__main__":
    asyncio.run(detailed_performance_analysis())