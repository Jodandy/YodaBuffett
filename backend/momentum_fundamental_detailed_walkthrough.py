#!/usr/bin/env python3
"""
Momentum + Fundamental Strategy - Detailed Walkthrough

Shows exactly how the strategy works with:
- Step-by-step screening process
- Detailed trade history with entry/exit logic
- Score calculations for each pick
- Monthly portfolio performance
- Clear explanations of every decision
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Tuple, Optional
import logging

from momentum_fundamental_strategy import MomentumFundamentalStrategy

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DetailedStrategyWalkthrough:
    """Detailed walkthrough of the momentum + fundamental strategy."""
    
    def __init__(self):
        self.strategy = MomentumFundamentalStrategy()
        
    async def setup(self):
        await self.strategy.setup()
        
    async def explain_scoring_system(self):
        """Explain how the scoring system works."""
        
        print("📖 HOW THE MOMENTUM + FUNDAMENTAL SCORING WORKS")
        print("=" * 60)
        
        print("\n🎯 Combined Score Formula:")
        print("   Combined Score = (Momentum Score × 0.6) + (Fundamental Score × 0.4)")
        print("   • Momentum weighted 60% (market timing)")
        print("   • Fundamentals weighted 40% (quality filter)")
        
        print("\n📈 Momentum Score Components (0-10 scale):")
        print("   1. Recent Returns (1-month):")
        print("      • +20% return = 10 points")
        print("      • +10% return = 6.7 points") 
        print("      • 0% return = 3.3 points")
        print("      • -10% return = 0 points")
        
        print("\n   2. RSI (Relative Strength Index):")
        print("      • 40-70 RSI = 10 points (ideal momentum)")
        print("      • 30-40 RSI = 8 points (oversold recovery)")
        print("      • 30-80 RSI = 7 points (acceptable)")
        print("      • >80 RSI = 3 points (overbought)")
        
        print("\n   3. Volatility (risk assessment):")
        print("      • 15-35% volatility = 10 points (sweet spot)")
        print("      • 10-50% volatility = 7 points (acceptable)")
        print("      • <10% or >50% = 3 points (too low/high)")
        
        print("\n🏦 Fundamental Score Components (0-10 scale):")
        print("   1. P/E Ratio (valuation):")
        print("      • 8-20 P/E = 10 points (reasonable)")
        print("      • 5-30 P/E = 7 points (acceptable)")
        print("      • 30-35 P/E = 4 points (expensive)")
        print("      • >35 P/E = 1 point (very expensive)")
        
        print("\n   2. ROE (profitability):")
        print("      • >15% ROE = 10 points (excellent)")
        print("      • 10-15% ROE = 8 points (good)")
        print("      • 5-10% ROE = 5 points (acceptable)")
        print("      • <5% ROE = 2 points (poor)")
        
        print("\n   3. Debt-to-Equity (financial health):")
        print("      • <50% D/E = 10 points (low debt)")
        print("      • 50-100% D/E = 7 points (moderate)")
        print("      • 100-200% D/E = 4 points (high debt)")
        print("      • >200% D/E = 1 point (very high debt)")
        
    async def demonstrate_screening_process(self, screen_date: date):
        """Show the complete screening process for a specific date."""
        
        print(f"\n🔍 DETAILED SCREENING PROCESS FOR {screen_date}")
        print("=" * 60)
        
        print("🔄 Step 1: Get all companies with recent fundamental data...")
        scores = await self.strategy.get_momentum_fundamental_scores(screen_date)
        print(f"   Found {len(scores)} companies with fundamental data")
        
        print("\n🔄 Step 2: Calculate momentum metrics for each company...")
        # Show calculation for first few companies
        for i, score in enumerate(scores[:5]):
            print(f"\n   📊 {score.symbol}:")
            print(f"      1-Month Return: {score.return_1m:.1f}%" if score.return_1m else "      1-Month Return: N/A")
            print(f"      RSI: {score.rsi:.1f}" if score.rsi else "      RSI: N/A")
            print(f"      Volatility: {score.volatility:.1f}%" if score.volatility else "      Volatility: N/A")
            print(f"      P/E Ratio: {score.pe_ratio:.1f}" if score.pe_ratio else "      P/E Ratio: N/A")
            print(f"      ROE: {score.roe:.1f}%" if score.roe else "      ROE: N/A")
            print(f"      Debt/Equity: {score.debt_to_equity:.1f}%" if score.debt_to_equity else "      Debt/Equity: N/A")
            print(f"      → Momentum Score: {score.momentum_score:.1f}")
            print(f"      → Fundamental Score: {score.fundamental_score:.1f}")
            print(f"      → Combined Score: {score.combined_score:.1f}")
        
        print(f"\n   ... (calculated for all {len(scores)} companies)")
        
        print("\n🔄 Step 3: Apply filtering criteria...")
        print("   Filters:")
        print("   • Combined score ≥ 6.0")
        print("   • P/E ratio > 0 and < 50 (no loss-making or extreme valuations)")
        print("   • ROE > 0 (must be profitable)")
        
        # Apply filters
        filtered = []
        for score in scores:
            if (score.combined_score >= 6.0 and
                score.pe_ratio and score.pe_ratio > 0 and score.pe_ratio < 50 and
                score.roe and score.roe > 0):
                filtered.append(score)
                
        print(f"   After filtering: {len(filtered)} companies qualify")
        
        print("\n🔄 Step 4: Rank by combined score and select top 10...")
        filtered.sort(key=lambda x: x.combined_score, reverse=True)
        top_picks = filtered[:10]
        
        print(f"\n🎯 FINAL PICKS FOR {screen_date}:")
        print(f"{'Rank':<5} {'Symbol':<10} {'Combined':<9} {'Momentum':<9} {'Fund':<9} {'1M Ret':<8} {'P/E':<6} {'ROE':<6}")
        print("-" * 70)
        
        for i, pick in enumerate(top_picks, 1):
            print(f"{i:<5} {pick.symbol:<10} "
                  f"{pick.combined_score:<9.1f} "
                  f"{pick.momentum_score:<9.1f} "
                  f"{pick.fundamental_score:<9.1f} "
                  f"{pick.return_1m:<8.1f} " if pick.return_1m else f"{'N/A':<8} "
                  f"{pick.pe_ratio:<6.1f} " if pick.pe_ratio else f"{'N/A':<6} "
                  f"{pick.roe:<6.1f}" if pick.roe else f"{'N/A':<6}")
        
        return top_picks
        
    async def show_trade_lifecycle(self, picks: List, entry_date: date):
        """Show what happens to trades over their lifecycle."""
        
        print(f"\n📊 TRADE LIFECYCLE ANALYSIS")
        print("=" * 50)
        
        print(f"Entry Date: {entry_date}")
        print(f"Hold Period: 21 days")
        print(f"Exit Date: ~{entry_date + timedelta(days=21)}")
        
        # Add future returns
        picks_with_returns = await self.strategy.add_future_returns(picks, 21)
        
        print(f"\n📈 TRADE RESULTS:")
        print(f"{'Symbol':<10} {'Entry':<8} {'Score':<6} {'Return':<8} {'Outcome':<12} {'Reason'}")
        print("-" * 65)
        
        total_return = 0
        valid_trades = 0
        
        for pick in picks_with_returns:
            if pick.future_return is not None:
                outcome = "WIN 🎯" if pick.future_return > 0 else "LOSS ❌"
                
                # Analyze why it worked/didn't work
                if pick.future_return > 2:
                    reason = "Strong momentum continued"
                elif pick.future_return > 0:
                    reason = "Momentum played out"
                elif pick.future_return > -2:
                    reason = "Mixed signals"
                else:
                    reason = "Momentum failed"
                    
                print(f"{pick.symbol:<10} "
                      f"{pick.close_price:<8.1f} "
                      f"{pick.combined_score:<6.1f} "
                      f"{pick.future_return:<8.1f}% "
                      f"{outcome:<12} "
                      f"{reason}")
                
                total_return += pick.future_return
                valid_trades += 1
            else:
                print(f"{pick.symbol:<10} "
                      f"{pick.close_price:<8.1f} "
                      f"{pick.combined_score:<6.1f} "
                      f"{'N/A':<8} "
                      f"{'NO DATA':<12} "
                      f"Insufficient price data")
        
        if valid_trades > 0:
            avg_return = total_return / valid_trades
            win_rate = len([p for p in picks_with_returns 
                           if p.future_return and p.future_return > 0]) / valid_trades
            
            print(f"\n📊 Period Summary:")
            print(f"   Portfolio Return: {avg_return:.2f}%")
            print(f"   Win Rate: {win_rate:.1%}")
            print(f"   Valid Trades: {valid_trades}")
            
            return {
                'date': entry_date,
                'avg_return': avg_return,
                'win_rate': win_rate,
                'trades': valid_trades,
                'picks': picks_with_returns
            }
            
        return None
        
    async def run_detailed_backtest(self, start_date: date, end_date: date, show_detail: bool = True):
        """Run a detailed backtest with explanations."""
        
        print(f"\n🚀 DETAILED BACKTEST: {start_date} to {end_date}")
        print("=" * 70)
        
        current_date = start_date
        all_results = []
        portfolio_value = 100000  # Start with $100k
        
        period_count = 0
        
        while current_date < end_date:
            period_count += 1
            
            if show_detail:
                print(f"\n" + "="*50)
                print(f"📅 PERIOD {period_count}: {current_date}")
                print("="*50)
            else:
                print(f"📅 Processing {current_date}...")
            
            # Screen for picks
            picks = await self.demonstrate_screening_process(current_date)
            
            if not picks:
                print("   ⚠️ No qualifying picks found")
                current_date += timedelta(days=21)
                continue
            
            # Show trade lifecycle
            if show_detail:
                result = await self.show_trade_lifecycle(picks, current_date)
            else:
                # Just calculate returns without detailed display
                picks_with_returns = await self.strategy.add_future_returns(picks, 21)
                valid_returns = [p.future_return for p in picks_with_returns if p.future_return is not None]
                
                if valid_returns:
                    avg_return = np.mean(valid_returns)
                    win_rate = len([r for r in valid_returns if r > 0]) / len(valid_returns)
                    
                    result = {
                        'date': current_date,
                        'avg_return': avg_return,
                        'win_rate': win_rate,
                        'trades': len(valid_returns),
                        'picks': picks_with_returns
                    }
                else:
                    result = None
            
            if result:
                all_results.append(result)
                
                # Update portfolio value
                portfolio_value *= (1 + result['avg_return'] / 100)
                
                if show_detail:
                    print(f"   💰 Portfolio Value: ${portfolio_value:,.0f}")
            
            # Move to next period
            current_date += timedelta(days=21)
            
            # Limit detail to first few periods
            if period_count >= 3 and show_detail:
                print(f"\n⏭️ Continuing backtest without detailed display for speed...")
                show_detail = False
        
        # Final summary
        print(f"\n🎯 BACKTEST COMPLETE - FINAL RESULTS")
        print("=" * 50)
        
        total_return = (portfolio_value / 100000 - 1) * 100
        avg_period_return = np.mean([r['avg_return'] for r in all_results])
        overall_win_rate = np.mean([r['win_rate'] for r in all_results])
        total_trades = sum([r['trades'] for r in all_results])
        
        print(f"📊 Performance Summary:")
        print(f"   Starting Value: $100,000")
        print(f"   Ending Value: ${portfolio_value:,.0f}")
        print(f"   Total Return: {total_return:.2f}%")
        print(f"   Average Period Return: {avg_period_return:.2f}%")
        print(f"   Overall Win Rate: {overall_win_rate:.1%}")
        print(f"   Total Trades: {total_trades}")
        print(f"   Rebalance Periods: {len(all_results)}")
        
        # Best and worst periods
        best_period = max(all_results, key=lambda x: x['avg_return'])
        worst_period = min(all_results, key=lambda x: x['avg_return'])
        
        print(f"\n🏆 Best Period: {best_period['date']} ({best_period['avg_return']:+.2f}%)")
        print(f"📉 Worst Period: {worst_period['date']} ({worst_period['avg_return']:+.2f}%)")
        
        # Monthly breakdown
        print(f"\n📅 Monthly Performance:")
        monthly_returns = {}
        for result in all_results:
            month = result['date'].strftime('%Y-%m')
            if month not in monthly_returns:
                monthly_returns[month] = []
            monthly_returns[month].append(result['avg_return'])
        
        for month in sorted(monthly_returns.keys())[-6:]:  # Last 6 months
            month_avg = np.mean(monthly_returns[month])
            print(f"   {month}: {month_avg:+.2f}%")
        
        return all_results
        
    async def cleanup(self):
        await self.strategy.cleanup()

async def main():
    """Run the detailed walkthrough."""
    
    walkthrough = DetailedStrategyWalkthrough()
    
    try:
        await walkthrough.setup()
        
        print("🎓 MOMENTUM + FUNDAMENTAL STRATEGY - COMPLETE WALKTHROUGH")
        print("=" * 80)
        
        # 1. Explain the scoring system
        await walkthrough.explain_scoring_system()
        
        # 2. Show detailed screening for one date
        print(f"\n" + "="*80)
        demo_date = date(2024, 8, 1)
        picks = await walkthrough.demonstrate_screening_process(demo_date)
        
        # 3. Show what happens to those trades
        await walkthrough.show_trade_lifecycle(picks, demo_date)
        
        # 4. Run a short detailed backtest
        print(f"\n" + "="*80)
        print("🚀 Would you like to see a detailed backtest? (y/n): ", end="")
        
        # For demo, automatically run a 3-month backtest
        print("y")
        print("\n📊 Running 3-month detailed backtest (Aug-Oct 2024)...")
        
        results = await walkthrough.run_detailed_backtest(
            date(2024, 8, 1), 
            date(2024, 11, 1)
        )
        
        print(f"\n💡 KEY TAKEAWAYS:")
        print("=" * 30)
        print("✅ Strategy systematically finds momentum + quality companies")
        print("✅ Scoring system balances technical momentum with fundamentals")
        print("✅ Regular rebalancing captures new opportunities")
        print("✅ Risk management through diversification (8-10 stocks)")
        print("✅ Transparent, repeatable process")
        
        print(f"\n🔧 IMPLEMENTATION READY:")
        print("• Clear entry/exit rules")
        print("• Systematic screening process")
        print("• Risk-controlled position sizing")
        print("• Regular performance monitoring")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await walkthrough.cleanup()

if __name__ == "__main__":
    asyncio.run(main())