#!/usr/bin/env python3
"""
Summary Analysis of Fundamental Value Strategy Backtest Results
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
from fundamental_value_backtest import FundamentalValueBacktester

async def detailed_analysis():
    """Run detailed analysis of the fundamental value strategy"""
    
    conn = await asyncpg.connect(
        host='localhost',
        port=5432, 
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        print("\n" + "="*80)
        print("FUNDAMENTAL VALUE STRATEGY - DETAILED ANALYSIS")
        print("="*80)
        
        backtester = FundamentalValueBacktester(conn)
        
        # Run backtest for analysis
        results = await backtester.run_backtest(
            start_date=date(2023, 6, 1),
            end_date=date(2024, 6, 1), 
            rebalance_frequency=30
        )
        
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
            return
            
        metrics = results['metrics']
        completed_trades = [t for t in results['all_trades'] if t.exit_date]
        
        print("\n📊 STRATEGY PERFORMANCE ANALYSIS")
        print("="*50)
        
        print(f"🎯 CORE METRICS:")
        print(f"  • Total Return: {metrics['total_return']:.1%}")
        print(f"  • Annualized Return: {metrics['annualized_return']:.1%}")
        print(f"  • Volatility: {metrics['volatility']:.1%}")
        print(f"  • Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        print(f"  • Max Drawdown: {metrics['max_drawdown']:.1%}")
        
        print(f"\n🎲 RISK ANALYSIS:")
        if metrics['volatility'] < 30:
            risk_assessment = "Low"
        elif metrics['volatility'] < 50:
            risk_assessment = "Moderate" 
        else:
            risk_assessment = "High"
        print(f"  • Risk Level: {risk_assessment}")
        print(f"  • Return/Risk Ratio: {metrics['annualized_return']/metrics['volatility']:.2f}")
        print(f"  • Worst Period Drawdown: {metrics['max_drawdown']:.1%}")
        
        print(f"\n📈 TRADING EFFECTIVENESS:")
        print(f"  • Win Rate: {metrics['win_rate']:.0%} (industry benchmark: 40-60%)")
        print(f"  • Average Gain per Win: {metrics['avg_win']:.1f}%")
        print(f"  • Average Holding Period: {metrics['avg_holding_days']:.0f} days")
        print(f"  • Total Profitable Trades: {len(completed_trades)}")
        
        if completed_trades:
            returns = [t.return_pct for t in completed_trades]
            print(f"  • Best Trade: {max(returns):.1f}%")
            print(f"  • Median Return: {np.median(returns):.1f}%")
        
        print(f"\n💰 CAPITAL EFFICIENCY:")
        initial_capital = results['initial_capital']
        final_capital = results['final_capital']
        profit = final_capital - initial_capital
        print(f"  • Initial Capital: {initial_capital:,.0f} SEK")
        print(f"  • Final Capital: {final_capital:,.0f} SEK")
        print(f"  • Absolute Profit: {profit:,.0f} SEK")
        print(f"  • Profit per Month: {profit/12:,.0f} SEK")
        
        # Analyze trade timing and entry quality
        print(f"\n🎯 ENTRY QUALITY ANALYSIS:")
        if completed_trades:
            asymmetries = [t.entry_asymmetry for t in completed_trades]
            print(f"  • Average Entry Asymmetry: {np.mean(asymmetries):.1f}:1")
            print(f"  • Best Entry Asymmetry: {max(asymmetries):.1f}:1")
            print(f"  • Fat Pitch Entries (>3:1): {len([a for a in asymmetries if a >= 3])}")
            
            fat_pitch_trades = [t for t in completed_trades if t.entry_asymmetry >= 3]
            if fat_pitch_trades:
                fat_pitch_returns = [t.return_pct for t in fat_pitch_trades]
                print(f"  • Fat Pitch Average Return: {np.mean(fat_pitch_returns):.1f}%")
        
        # Exit reason analysis
        print(f"\n📤 EXIT ANALYSIS:")
        if completed_trades:
            exit_reasons = {}
            for trade in completed_trades:
                reason = trade.exit_reason
                if reason not in exit_reasons:
                    exit_reasons[reason] = []
                exit_reasons[reason].append(trade.return_pct)
            
            for reason, returns in exit_reasons.items():
                avg_return = np.mean(returns)
                count = len(returns)
                print(f"  • {reason.replace('_', ' ').title()}: {count} trades, avg {avg_return:.1f}% return")
        
        # Symbol performance analysis
        print(f"\n🏢 TOP PERFORMING STOCKS:")
        if completed_trades:
            symbol_performance = {}
            for trade in completed_trades:
                if trade.symbol not in symbol_performance:
                    symbol_performance[trade.symbol] = []
                symbol_performance[trade.symbol].append(trade.return_pct)
            
            symbol_avg = {symbol: np.mean(returns) for symbol, returns in symbol_performance.items()}
            top_symbols = sorted(symbol_avg.items(), key=lambda x: x[1], reverse=True)
            
            for symbol, avg_return in top_symbols[:10]:
                trade_count = len(symbol_performance[symbol])
                print(f"  • {symbol}: {avg_return:.1f}% avg return ({trade_count} trades)")
        
        # Strategy effectiveness assessment
        print(f"\n🎖️ STRATEGY ASSESSMENT:")
        
        # Compare to market benchmark (rough estimate)
        market_return = 10  # Assume 10% annual market return
        alpha = metrics['annualized_return'] - (market_return/100)
        
        print(f"  • Alpha vs Market: {alpha:.1%}")
        if alpha > 0.05:
            assessment = "Excellent - significantly outperforming market"
        elif alpha > 0:
            assessment = "Good - beating market"
        else:
            assessment = "Needs improvement - underperforming market"
        print(f"  • Performance Assessment: {assessment}")
        
        # Risk-adjusted performance
        if metrics['sharpe_ratio'] > 1.0:
            risk_adj = "Excellent risk-adjusted returns"
        elif metrics['sharpe_ratio'] > 0.5:
            risk_adj = "Good risk-adjusted returns"
        else:
            risk_adj = "Poor risk-adjusted returns"
        print(f"  • Risk-Adjusted Performance: {risk_adj}")
        
        # Strategy strengths and recommendations
        print(f"\n✅ STRATEGY STRENGTHS:")
        if metrics['win_rate'] >= 0.8:
            print(f"  • Exceptional win rate ({metrics['win_rate']:.0%})")
        if metrics['max_drawdown'] < 0.10:
            print(f"  • Low drawdown risk ({metrics['max_drawdown']:.1%})")
        if metrics['avg_win'] > 25:
            print(f"  • Strong average gains per trade")
        if np.mean([t.entry_asymmetry for t in completed_trades]) > 5:
            print(f"  • Excellent entry timing with high asymmetry")
        
        print(f"\n⚠️ AREAS FOR IMPROVEMENT:")
        if metrics['volatility'] > 50:
            print(f"  • High volatility - consider position sizing adjustments")
        if len(completed_trades) < 20:
            print(f"  • Limited trade sample - extend backtest period")
        if metrics['avg_holding_days'] > 200:
            print(f"  • Long holding periods - consider more dynamic exits")
        
        print(f"\n🚀 RECOMMENDATIONS:")
        print(f"  • Continue using Fat Pitch framework with 3:1+ asymmetry requirement")
        print(f"  • Consider reducing position sizes if volatility is concerning")
        print(f"  • Monitor for market regime changes that could affect performance") 
        print(f"  • Expand universe to include more international markets")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(detailed_analysis())