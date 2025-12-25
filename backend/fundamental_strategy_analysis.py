#!/usr/bin/env python3
"""
Comprehensive Fundamental Strategy Performance Analysis

Deep dive into fundamental strategy performance with detailed breakdowns:
- Monthly/quarterly performance tracking
- Individual stock contribution analysis
- Risk-adjusted metrics and drawdown analysis
- Sector/market cap performance attribution
- Factor analysis (momentum vs fundamental contributions)
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass
# import matplotlib.pyplot as plt  # Removed - not needed for analysis
from collections import defaultdict

# Import our strategies
from momentum_fundamental_strategy import MomentumFundamentalStrategy
from fundamental_value_strategy import FundamentalValueStrategy

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DetailedPerformance:
    """Detailed performance metrics for a strategy."""
    strategy_name: str
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    monthly_returns: List[Dict]
    top_performers: List[Dict]
    worst_performers: List[Dict]
    period_analysis: Dict

class FundamentalStrategyAnalyzer:
    """Comprehensive analyzer for fundamental strategies."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def analyze_momentum_fundamental_detailed(self, start_date: date, end_date: date) -> DetailedPerformance:
        """Detailed analysis of momentum + fundamental strategy."""
        
        logger.info("🔍 Analyzing Momentum + Fundamental Strategy in Detail")
        
        strategy = MomentumFundamentalStrategy()
        await strategy.setup()
        
        # Run strategy with more frequent rebalancing for detailed analysis
        current_date = start_date
        all_trades = []
        daily_portfolio_values = []
        portfolio_value = 100000  # Start with $100k
        
        while current_date < end_date:
            # Get picks
            picks = await strategy.screen_momentum_fundamental(current_date, min_combined_score=6.0, top_n=10)
            
            if picks:
                # Calculate returns for 21-day hold period
                picks = await strategy.add_future_returns(picks, 21)
                
                # Track each trade
                for pick in picks:
                    if pick.future_return is not None:
                        trade = {
                            'entry_date': current_date,
                            'symbol': pick.symbol,
                            'entry_price': pick.close_price,
                            'return_pct': pick.future_return,
                            'return_dollars': (portfolio_value / 10) * (pick.future_return / 100),  # 10% position
                            'momentum_score': pick.momentum_score,
                            'fundamental_score': pick.fundamental_score,
                            'combined_score': pick.combined_score,
                            'pe_ratio': pick.pe_ratio,
                            'roe': pick.roe,
                            'return_1m': pick.return_1m,
                            'hold_period': 21
                        }
                        all_trades.append(trade)
                
                # Update portfolio value
                if picks:
                    period_return = np.mean([p.future_return for p in picks if p.future_return is not None])
                    portfolio_value *= (1 + period_return / 100)
                    
                    daily_portfolio_values.append({
                        'date': current_date,
                        'portfolio_value': portfolio_value,
                        'period_return': period_return,
                        'num_stocks': len([p for p in picks if p.future_return is not None])
                    })
            
            current_date += timedelta(days=21)
            
        await strategy.cleanup()
        
        # Calculate detailed metrics
        returns = [t['return_pct'] for t in all_trades]
        
        if not returns:
            return DetailedPerformance(
                strategy_name="Momentum + Fundamental",
                total_return=0, annualized_return=0, volatility=0, sharpe_ratio=0,
                max_drawdown=0, win_rate=0, avg_win=0, avg_loss=0, profit_factor=0,
                monthly_returns=[], top_performers=[], worst_performers=[], period_analysis={}
            )
        
        # Basic metrics
        total_return = (portfolio_value / 100000 - 1) * 100
        days_total = (end_date - start_date).days
        annualized_return = ((portfolio_value / 100000) ** (365 / days_total) - 1) * 100
        volatility = np.std(returns) * np.sqrt(252 / 21)  # Annualized
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Win/loss analysis
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        win_rate = len(wins) / len(returns)
        avg_win = np.mean(wins) if wins else 0
        avg_loss = np.mean(losses) if losses else 0
        profit_factor = abs(sum(wins) / sum(losses)) if losses else float('inf')
        
        # Maximum drawdown
        portfolio_values = [d['portfolio_value'] for d in daily_portfolio_values]
        peak = portfolio_values[0]
        max_drawdown = 0
        for value in portfolio_values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
        max_drawdown *= 100
        
        # Monthly analysis
        monthly_returns = self.calculate_monthly_returns(daily_portfolio_values)
        
        # Top/worst performers
        top_performers = sorted(all_trades, key=lambda x: x['return_pct'], reverse=True)[:10]
        worst_performers = sorted(all_trades, key=lambda x: x['return_pct'])[:10]
        
        # Period analysis
        period_analysis = self.analyze_by_periods(all_trades)
        
        return DetailedPerformance(
            strategy_name="Momentum + Fundamental",
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            monthly_returns=monthly_returns,
            top_performers=top_performers,
            worst_performers=worst_performers,
            period_analysis=period_analysis
        )
        
    def calculate_monthly_returns(self, daily_values: List[Dict]) -> List[Dict]:
        """Calculate monthly returns from daily portfolio values."""
        
        if not daily_values:
            return []
            
        df = pd.DataFrame(daily_values)
        df['date'] = pd.to_datetime(df['date'])
        df['year_month'] = df['date'].dt.to_period('M')
        
        monthly_returns = []
        for period in df['year_month'].unique():
            period_data = df[df['year_month'] == period]
            if len(period_data) > 0:
                start_value = period_data.iloc[0]['portfolio_value']
                end_value = period_data.iloc[-1]['portfolio_value']
                monthly_return = (end_value / start_value - 1) * 100
                
                monthly_returns.append({
                    'month': str(period),
                    'return': monthly_return,
                    'trades': len(period_data)
                })
                
        return monthly_returns
        
    def analyze_by_periods(self, trades: List[Dict]) -> Dict:
        """Analyze performance by different time periods."""
        
        analysis = {
            'by_quarter': defaultdict(list),
            'by_month': defaultdict(list),
            'by_score_range': defaultdict(list)
        }
        
        for trade in trades:
            # By quarter
            quarter = f"Q{((trade['entry_date'].month - 1) // 3) + 1} {trade['entry_date'].year}"
            analysis['by_quarter'][quarter].append(trade['return_pct'])
            
            # By month
            month = f"{trade['entry_date'].strftime('%Y-%m')}"
            analysis['by_month'][month].append(trade['return_pct'])
            
            # By score range
            if trade['combined_score'] >= 8.0:
                score_range = '8.0+'
            elif trade['combined_score'] >= 7.0:
                score_range = '7.0-8.0'
            else:
                score_range = '6.0-7.0'
            analysis['by_score_range'][score_range].append(trade['return_pct'])
            
        # Convert to summary statistics
        summary = {}
        for category, data in analysis.items():
            summary[category] = {}
            for period, returns in data.items():
                summary[category][period] = {
                    'avg_return': np.mean(returns),
                    'count': len(returns),
                    'win_rate': len([r for r in returns if r > 0]) / len(returns)
                }
                
        return summary
        
    async def factor_attribution_analysis(self, start_date: date, end_date: date) -> Dict:
        """Analyze which factors contribute most to returns."""
        
        logger.info("🧪 Running Factor Attribution Analysis")
        
        strategy = MomentumFundamentalStrategy()
        await strategy.setup()
        
        current_date = start_date
        factor_data = []
        
        while current_date < end_date:
            picks = await strategy.screen_momentum_fundamental(current_date, min_combined_score=5.0, top_n=20)
            
            if picks:
                picks = await strategy.add_future_returns(picks, 21)
                
                for pick in picks:
                    if pick.future_return is not None:
                        factor_data.append({
                            'return': pick.future_return,
                            'momentum_score': pick.momentum_score,
                            'fundamental_score': pick.fundamental_score,
                            'pe_ratio': pick.pe_ratio,
                            'roe': pick.roe,
                            'return_1m': pick.return_1m,
                            'rsi': pick.rsi
                        })
            
            current_date += timedelta(days=21)
            
        await strategy.cleanup()
        
        # Correlation analysis
        df = pd.DataFrame(factor_data)
        correlations = df.corr()['return'].sort_values(ascending=False)
        
        # Quintile analysis
        quintile_analysis = {}
        for factor in ['momentum_score', 'fundamental_score', 'pe_ratio', 'roe', 'return_1m']:
            if factor in df.columns and not df[factor].isna().all():
                try:
                    df[f'{factor}_quintile'] = pd.qcut(df[factor], 5, labels=['Q1', 'Q2', 'Q3', 'Q4', 'Q5'], duplicates='drop')
                    quintile_returns = df.groupby(f'{factor}_quintile')['return'].agg(['mean', 'count']).to_dict()
                    quintile_analysis[factor] = quintile_returns
                except ValueError:
                    # Skip if not enough unique values for quintiles
                    continue
        
        return {
            'correlations': correlations.to_dict(),
            'quintile_analysis': quintile_analysis,
            'sample_size': len(factor_data)
        }
        
    async def compare_strategies_detailed(self, start_date: date, end_date: date) -> Dict:
        """Compare all fundamental strategies side by side."""
        
        logger.info("⚖️ Comparing All Fundamental Strategies")
        
        results = {}
        
        # 1. Momentum + Fundamental Strategy
        momentum_perf = await self.analyze_momentum_fundamental_detailed(start_date, end_date)
        results['momentum_fundamental'] = momentum_perf
        
        # 2. Value Strategy Analysis
        value_strategy = FundamentalValueStrategy()
        await value_strategy.setup()
        
        value_results = await value_strategy.backtest_value_strategy(
            start_date, end_date, rebalance_days=60, portfolio_size=10
        )
        await value_strategy.cleanup()
        
        if 'error' not in value_results:
            results['value_strategy'] = {
                'total_return': value_results['total_return'],
                'win_rate': value_results['win_rate'],
                'sharpe_ratio': value_results['sharpe_ratio'],
                'periods': value_results['rebalance_periods']
            }
        
        # 3. Factor Attribution
        factor_analysis = await self.factor_attribution_analysis(start_date, end_date)
        results['factor_attribution'] = factor_analysis
        
        return results
        
    def print_detailed_performance(self, perf: DetailedPerformance):
        """Print detailed performance analysis."""
        
        print(f"\n📊 DETAILED PERFORMANCE: {perf.strategy_name}")
        print("=" * 70)
        
        # Core metrics
        print(f"🎯 Core Performance:")
        print(f"   Total Return: {perf.total_return:.2f}%")
        print(f"   Annualized Return: {perf.annualized_return:.2f}%")
        print(f"   Volatility: {perf.volatility:.2f}%")
        print(f"   Sharpe Ratio: {perf.sharpe_ratio:.2f}")
        print(f"   Maximum Drawdown: {perf.max_drawdown:.2f}%")
        
        # Win/Loss analysis
        print(f"\n📈 Win/Loss Analysis:")
        print(f"   Win Rate: {perf.win_rate:.1%}")
        print(f"   Average Win: {perf.avg_win:.2f}%")
        print(f"   Average Loss: {perf.avg_loss:.2f}%")
        print(f"   Profit Factor: {perf.profit_factor:.2f}")
        
        # Monthly performance
        if perf.monthly_returns:
            print(f"\n📅 Monthly Performance (Last 12 months):")
            for monthly in perf.monthly_returns[-12:]:
                print(f"   {monthly['month']}: {monthly['return']:+6.2f}% ({monthly['trades']} trades)")
        
        # Top performers
        print(f"\n🏆 Top 5 Performers:")
        for i, trade in enumerate(perf.top_performers[:5], 1):
            print(f"   {i}. {trade['symbol']} ({trade['entry_date']}): {trade['return_pct']:+.2f}% "
                  f"(Score: {trade['combined_score']:.1f})")
        
        # Worst performers
        print(f"\n📉 Worst 5 Performers:")
        for i, trade in enumerate(perf.worst_performers[:5], 1):
            print(f"   {i}. {trade['symbol']} ({trade['entry_date']}): {trade['return_pct']:+.2f}% "
                  f"(Score: {trade['combined_score']:.1f})")
        
        # Period analysis
        if 'by_score_range' in perf.period_analysis:
            print(f"\n🔢 Performance by Score Range:")
            for score_range, stats in perf.period_analysis['by_score_range'].items():
                print(f"   {score_range}: {stats['avg_return']:+.2f}% avg, {stats['win_rate']:.1%} win rate "
                      f"({stats['count']} trades)")
                      
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run comprehensive fundamental strategy analysis."""
    
    analyzer = FundamentalStrategyAnalyzer()
    
    try:
        await analyzer.setup()
        
        print("🔬 COMPREHENSIVE FUNDAMENTAL STRATEGY ANALYSIS")
        print("=" * 80)
        
        # Analysis period
        start_date = date(2023, 6, 1)
        end_date = date(2024, 10, 31)
        
        print(f"📅 Analysis Period: {start_date} to {end_date}")
        
        # 1. Detailed Momentum + Fundamental Analysis
        momentum_perf = await analyzer.analyze_momentum_fundamental_detailed(start_date, end_date)
        analyzer.print_detailed_performance(momentum_perf)
        
        # 2. Factor Attribution
        print(f"\n🧬 FACTOR ATTRIBUTION ANALYSIS")
        print("=" * 50)
        
        factor_analysis = await analyzer.factor_attribution_analysis(start_date, end_date)
        
        print(f"📊 Factor Correlations with Returns:")
        for factor, corr in sorted(factor_analysis['correlations'].items(), key=lambda x: abs(x[1]), reverse=True):
            if factor != 'return':
                print(f"   {factor}: {corr:+.3f}")
                
        print(f"\n🎯 Quintile Analysis (Q5 = highest factor value):")
        for factor, quintiles in factor_analysis['quintile_analysis'].items():
            if 'mean' in quintiles:
                print(f"\n   {factor}:")
                for quintile in ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']:
                    if quintile in quintiles['mean']:
                        avg_return = quintiles['mean'][quintile]
                        count = quintiles['count'][quintile]
                        print(f"     {quintile}: {avg_return:+6.2f}% ({count} obs)")
        
        # 3. Strategy Comparison
        print(f"\n⚖️ STRATEGY COMPARISON")
        print("=" * 40)
        
        comparison = await analyzer.compare_strategies_detailed(start_date, end_date)
        
        if 'momentum_fundamental' in comparison:
            mf = comparison['momentum_fundamental']
            print(f"🚀 Momentum + Fundamental:")
            print(f"   Total Return: {mf.total_return:+.2f}%")
            print(f"   Sharpe Ratio: {mf.sharpe_ratio:.2f}")
            print(f"   Win Rate: {mf.win_rate:.1%}")
            print(f"   Max Drawdown: {mf.max_drawdown:.2f}%")
            
        if 'value_strategy' in comparison:
            vs = comparison['value_strategy']
            print(f"\n🏦 Pure Value Strategy:")
            print(f"   Total Return: {vs['total_return']:+.2f}%")
            print(f"   Win Rate: {vs['win_rate']:.1%}")
            print(f"   Sharpe Ratio: {vs['sharpe_ratio']:.2f}")
        
        # 4. Key Insights
        print(f"\n💡 KEY INSIGHTS:")
        print("=" * 30)
        
        # Best factor correlations
        best_factors = sorted(factor_analysis['correlations'].items(), 
                            key=lambda x: abs(x[1]), reverse=True)[:3]
        print(f"🎯 Strongest Predictive Factors:")
        for factor, corr in best_factors:
            if factor != 'return':
                print(f"   {factor}: {corr:+.3f} correlation")
        
        # Performance summary
        if momentum_perf.sharpe_ratio > 0.15:
            print(f"✅ Momentum + Fundamental shows strong risk-adjusted returns")
        if momentum_perf.win_rate > 0.6:
            print(f"✅ High win rate indicates consistent edge")
        if momentum_perf.max_drawdown < 15:
            print(f"✅ Controlled drawdowns suggest good risk management")
            
        print(f"\n📈 Sample Size: {factor_analysis['sample_size']} observations")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())