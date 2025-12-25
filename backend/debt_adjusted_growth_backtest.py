#!/usr/bin/env python3
"""
Comprehensive Backtest for Debt-Adjusted Growth Strategy

Provides detailed performance analysis and comparison with benchmarks.
"""

import asyncio
import asyncpg
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import logging
from debt_adjusted_growth_strategy import DebtAdjustedGrowthStrategy
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrategyBacktester:
    """Enhanced backtester with detailed analytics."""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        self.strategy = DebtAdjustedGrowthStrategy(conn)
        
    async def get_benchmark_return(self, start_date: date, end_date: date) -> float:
        """Calculate buy-and-hold return for a market index or basket of stocks."""
        
        # Use a simple equal-weight portfolio of all stocks as benchmark
        query = """
        WITH first_last_prices AS (
            SELECT 
                symbol,
                MIN(CASE WHEN date >= $1 THEN date END) as first_date,
                MAX(CASE WHEN date <= $2 THEN date END) as last_date
            FROM daily_price_data
            WHERE date >= $1 AND date <= $2
            GROUP BY symbol
        ),
        returns AS (
            SELECT 
                flp.symbol,
                md1.close as first_price,
                md2.close as last_price,
                (md2.close / md1.close - 1) as return
            FROM first_last_prices flp
            JOIN daily_price_data md1 ON md1.symbol = flp.symbol AND md1.date = flp.first_date
            JOIN daily_price_data md2 ON md2.symbol = flp.symbol AND md2.date = flp.last_date
            WHERE md1.close > 0 AND md2.close > 0
        )
        SELECT AVG(return) * 100 as avg_return
        FROM returns
        """
        
        result = await self.conn.fetchval(query, start_date, end_date)
        return result if result else 0
        
    async def enhanced_backtest(self, start_date: date, end_date: date, 
                               symbols: Optional[List[str]] = None) -> Dict:
        """
        Enhanced backtest with more detailed tracking and metrics.
        """
        
        logger.info(f"Running enhanced backtest from {start_date} to {end_date}")
        
        # Get all trading days
        query = """
        SELECT DISTINCT date 
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2
        ORDER BY date
        """
        
        date_rows = await self.conn.fetch(query, start_date, end_date)
        trading_days = [row['date'] for row in date_rows]
        
        if not trading_days:
            logger.error("No trading days found in the specified period")
            return {}
        
        # Initialize tracking
        initial_capital = 100000
        cash = initial_capital
        positions = {}  # symbol -> (entry_price, entry_date, shares, signal_data)
        trades = []
        daily_portfolio_values = []
        daily_cash = []
        daily_positions = []
        
        # Transaction costs
        commission_rate = 0.001  # 0.1% per trade
        
        for i, current_date in enumerate(trading_days):
            # First, check for exits (3-month holding period)
            for symbol in list(positions.keys()):
                entry_price, entry_date, shares, signal_data = positions[symbol]
                holding_days = (current_date - entry_date).days
                
                if holding_days >= 90:  # 3 months
                    # Exit position
                    exit_price_row = await self.conn.fetchrow(
                        "SELECT close FROM daily_price_data WHERE symbol = $1 AND date = $2",
                        symbol, current_date
                    )
                    
                    if exit_price_row:
                        exit_price = float(exit_price_row['close'])
                        gross_proceeds = shares * exit_price
                        commission = gross_proceeds * commission_rate
                        net_proceeds = gross_proceeds - commission
                        cash += net_proceeds
                        
                        # Calculate returns
                        gross_return = (exit_price - entry_price) / entry_price
                        trade_cost = 2 * commission_rate  # Entry + exit
                        net_return = gross_return - trade_cost
                        
                        trades.append({
                            'symbol': symbol,
                            'entry_date': entry_date,
                            'exit_date': current_date,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'shares': shares,
                            'gross_return': gross_return * 100,
                            'net_return': net_return * 100,
                            'holding_days': holding_days,
                            'entry_growth': signal_data['revenue_growth'],
                            'entry_ps': signal_data['ps_ratio'],
                            'entry_debt_ratio': signal_data['debt_ratio']
                        })
                        
                        del positions[symbol]
                        logger.info(f"EXIT {symbol}: {net_return*100:.1f}% net return")
            
            # Scan for new opportunities (weekly scans on Mondays)
            if current_date.weekday() == 0 and len(positions) < 10:  # Max 10 positions
                signals = await self.strategy.scan_market(current_date, symbols)
                
                # Filter to strong buy signals
                buy_signals = [s for s in signals if s.is_buy_signal]
                
                for signal in buy_signals:
                    if signal.symbol not in positions and cash > initial_capital * 0.05:  # Min 5% cash
                        # Position sizing: Equal weight up to 10% per position
                        max_position_size = initial_capital * 0.1
                        available_for_position = min(cash * 0.9, max_position_size)  # Keep some cash
                        
                        # Get entry price (next day's open to be realistic)
                        next_day_idx = i + 1 if i + 1 < len(trading_days) else i
                        entry_date = trading_days[next_day_idx]
                        
                        price_row = await self.conn.fetchrow(
                            "SELECT open FROM daily_price_data WHERE symbol = $1 AND date = $2",
                            signal.symbol, entry_date
                        )
                        
                        if price_row and price_row['open']:
                            entry_price = float(price_row['open'])
                            position_value = available_for_position
                            commission = position_value * commission_rate
                            shares = (position_value - commission) / entry_price
                            
                            if shares > 0:
                                cash -= position_value
                                positions[signal.symbol] = (
                                    entry_price, 
                                    entry_date, 
                                    shares,
                                    {
                                        'revenue_growth': (signal.revenue_1y_growth + signal.revenue_3y_cagr) / 2,
                                        'ps_ratio': signal.current_ps_ratio,
                                        'debt_ratio': signal.debt_to_revenue
                                    }
                                )
                                
                                logger.info(f"BUY {signal.symbol} on {entry_date}: "
                                          f"Growth={signal.revenue_1y_growth:.1f}%, "
                                          f"P/S={signal.current_ps_ratio:.2f} vs fair {signal.growth_adjusted_ps:.2f}")
                                
                                if len(positions) >= 10:
                                    break
            
            # Calculate daily portfolio value
            position_value = 0
            for symbol, (entry_price, entry_date, shares, _) in positions.items():
                price_row = await self.conn.fetchrow(
                    "SELECT close FROM daily_price_data WHERE symbol = $1 AND date = $2",
                    symbol, current_date
                )
                if price_row:
                    position_value += shares * float(price_row['close'])
                    
            total_value = cash + position_value
            daily_portfolio_values.append({
                'date': current_date,
                'total_value': total_value,
                'cash': cash,
                'position_value': position_value,
                'num_positions': len(positions)
            })
        
        # Calculate performance metrics
        if not daily_portfolio_values:
            logger.error("No portfolio values calculated")
            return {}
            
        # Convert to DataFrame for easier analysis
        portfolio_df = pd.DataFrame(daily_portfolio_values)
        portfolio_df['returns'] = portfolio_df['total_value'].pct_change()
        
        # Calculate metrics
        total_return = (portfolio_df['total_value'].iloc[-1] / initial_capital - 1) * 100
        
        # Annualized metrics
        years = (end_date - start_date).days / 365.25
        annualized_return = ((portfolio_df['total_value'].iloc[-1] / initial_capital) ** (1/years) - 1) * 100
        
        # Risk metrics
        daily_returns = portfolio_df['returns'].dropna()
        volatility = daily_returns.std() * np.sqrt(252) * 100  # Annualized
        
        # Sharpe ratio (assuming 2% risk-free rate)
        risk_free_rate = 0.02
        excess_returns = daily_returns - risk_free_rate/252
        sharpe_ratio = np.sqrt(252) * excess_returns.mean() / daily_returns.std() if len(daily_returns) > 1 else 0
        
        # Maximum drawdown
        cumulative = (1 + daily_returns).cumprod()
        running_max = cumulative.expanding().max()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min() * 100
        
        # Trade statistics
        if trades:
            trade_df = pd.DataFrame(trades)
            winning_trades = trade_df[trade_df['net_return'] > 0]
            losing_trades = trade_df[trade_df['net_return'] <= 0]
            
            avg_win = winning_trades['net_return'].mean() if len(winning_trades) > 0 else 0
            avg_loss = losing_trades['net_return'].mean() if len(losing_trades) > 0 else 0
            
            # Group performance by entry characteristics
            high_growth_trades = trade_df[trade_df['entry_growth'] > 20]
            low_debt_trades = trade_df[trade_df['entry_debt_ratio'] < 0.5]
            
            trade_stats = {
                'total_trades': len(trades),
                'winning_trades': len(winning_trades),
                'losing_trades': len(losing_trades),
                'win_rate': len(winning_trades) / len(trades) * 100,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'profit_factor': abs(avg_win / avg_loss) if avg_loss != 0 else 0,
                'avg_holding_days': trade_df['holding_days'].mean(),
                'high_growth_win_rate': len(high_growth_trades[high_growth_trades['net_return'] > 0]) / len(high_growth_trades) * 100 if len(high_growth_trades) > 0 else 0,
                'low_debt_win_rate': len(low_debt_trades[low_debt_trades['net_return'] > 0]) / len(low_debt_trades) * 100 if len(low_debt_trades) > 0 else 0
            }
        else:
            trade_stats = {
                'total_trades': 0,
                'win_rate': 0
            }
        
        # Get benchmark return
        benchmark_return = await self.get_benchmark_return(start_date, end_date)
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'benchmark_return': benchmark_return,
            'alpha': total_return - benchmark_return,
            'trade_stats': trade_stats,
            'portfolio_history': portfolio_df,
            'trades': trades
        }
        
    async def analyze_factor_performance(self, trades: List[Dict]) -> Dict:
        """Analyze which factors contributed most to performance."""
        
        if not trades:
            return {}
            
        trade_df = pd.DataFrame(trades)
        
        # Analyze by growth buckets
        trade_df['growth_bucket'] = pd.cut(trade_df['entry_growth'], 
                                          bins=[-100, 0, 10, 20, 50, 100], 
                                          labels=['Negative', '0-10%', '10-20%', '20-50%', '50%+'])
        
        # Analyze by debt levels
        trade_df['debt_bucket'] = pd.cut(trade_df['entry_debt_ratio'], 
                                        bins=[0, 0.3, 0.6, 1.0, 100], 
                                        labels=['Low', 'Medium', 'High', 'Very High'])
        
        # Calculate performance by factor
        growth_performance = trade_df.groupby('growth_bucket').agg({
            'net_return': ['mean', 'count'],
            'symbol': 'count'
        })
        
        debt_performance = trade_df.groupby('debt_bucket').agg({
            'net_return': ['mean', 'count'],
            'symbol': 'count'
        })
        
        return {
            'growth_performance': growth_performance,
            'debt_performance': debt_performance,
            'best_growth_range': trade_df.groupby('growth_bucket')['net_return'].mean().idxmax(),
            'best_debt_range': trade_df.groupby('debt_bucket')['net_return'].mean().idxmax()
        }


async def main():
    """Run comprehensive backtest and analysis."""
    
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        backtester = StrategyBacktester(conn)
        
        # Test multiple time periods
        test_periods = [
            (date(2024, 1, 1), date(2024, 12, 31), "2024 Full Year"),
            (date(2024, 7, 1), date(2024, 12, 31), "2024 H2"),
            (date(2023, 1, 1), date(2024, 12, 31), "2023-2024 (2 Years)")
        ]
        
        for start_date, end_date, period_name in test_periods:
            print("\n" + "="*80)
            print(f"BACKTEST RESULTS: {period_name}")
            print("="*80)
            
            # Get Nordic symbols
            nordic_symbols = await conn.fetch("""
                SELECT DISTINCT cm.primary_ticker as symbol
                FROM company_master cm
                JOIN daily_fundamentals df ON df.symbol = cm.primary_ticker
                WHERE cm.primary_exchange IN ('STO', 'HEL', 'CPH', 'OSL')
                    AND df.total_revenue IS NOT NULL
                GROUP BY cm.primary_ticker
                HAVING COUNT(*) > 50
            """)
            
            symbols = [row['symbol'] for row in nordic_symbols]
            logger.info(f"Testing with {len(symbols)} Nordic companies")
            
            # Run enhanced backtest
            results = await backtester.enhanced_backtest(start_date, end_date, symbols)
            
            if results:
                print(f"\nPeriod: {start_date} to {end_date}")
                print(f"Strategy: Debt-Adjusted Topline Growth\n")
                
                # Performance summary
                print("PERFORMANCE SUMMARY:")
                print(f"  Total Return: {results['total_return']:+.2f}%")
                print(f"  Annualized Return: {results['annualized_return']:+.2f}%")
                print(f"  Benchmark Return: {results['benchmark_return']:+.2f}%")
                print(f"  Alpha: {results['alpha']:+.2f}%")
                print(f"  Volatility: {results['volatility']:.2f}%")
                print(f"  Sharpe Ratio: {results['sharpe_ratio']:.2f}")
                print(f"  Max Drawdown: {results['max_drawdown']:.2f}%")
                
                # Trade statistics
                trade_stats = results['trade_stats']
                print(f"\nTRADE STATISTICS:")
                print(f"  Total Trades: {trade_stats['total_trades']}")
                print(f"  Win Rate: {trade_stats.get('win_rate', 0):.1f}%")
                
                if trade_stats['total_trades'] > 0:
                    print(f"  Average Win: {trade_stats.get('avg_win', 0):+.2f}%")
                    print(f"  Average Loss: {trade_stats.get('avg_loss', 0):+.2f}%")
                    print(f"  Profit Factor: {trade_stats.get('profit_factor', 0):.2f}")
                    print(f"  Avg Holding Period: {trade_stats.get('avg_holding_days', 0):.0f} days")
                    
                    # Factor performance
                    if trade_stats.get('high_growth_win_rate') is not None:
                        print(f"\nFACTOR ANALYSIS:")
                        print(f"  High Growth (>20%) Win Rate: {trade_stats['high_growth_win_rate']:.1f}%")
                        print(f"  Low Debt (<0.5x) Win Rate: {trade_stats['low_debt_win_rate']:.1f}%")
                
                # Show sample trades
                if results['trades']:
                    print(f"\nTOP 5 WINNING TRADES:")
                    trade_df = pd.DataFrame(results['trades'])
                    top_trades = trade_df.nlargest(5, 'net_return')
                    
                    print(f"{'Symbol':<8} {'Entry':<12} {'Exit':<12} {'Growth':<10} {'P/S':<8} {'Return':<10}")
                    print("-" * 68)
                    
                    for _, trade in top_trades.iterrows():
                        print(f"{trade['symbol']:<8} "
                              f"{trade['entry_date'].strftime('%Y-%m-%d'):<12} "
                              f"{trade['exit_date'].strftime('%Y-%m-%d'):<12} "
                              f"{trade['entry_growth']:>9.1f}% "
                              f"{trade['entry_ps']:>7.2f} "
                              f"{trade['net_return']:>9.1f}%")
                    
                    print(f"\nBOTTOM 5 LOSING TRADES:")
                    bottom_trades = trade_df.nsmallest(5, 'net_return')
                    
                    print(f"{'Symbol':<8} {'Entry':<12} {'Exit':<12} {'Growth':<10} {'P/S':<8} {'Return':<10}")
                    print("-" * 68)
                    
                    for _, trade in bottom_trades.iterrows():
                        print(f"{trade['symbol']:<8} "
                              f"{trade['entry_date'].strftime('%Y-%m-%d'):<12} "
                              f"{trade['exit_date'].strftime('%Y-%m-%d'):<12} "
                              f"{trade['entry_growth']:>9.1f}% "
                              f"{trade['entry_ps']:>7.2f} "
                              f"{trade['net_return']:>9.1f}%")
                
                # Analyze factor performance
                if results['trades'] and len(results['trades']) > 10:
                    factor_analysis = await backtester.analyze_factor_performance(results['trades'])
                    
                    if factor_analysis:
                        print(f"\nFACTOR PERFORMANCE ANALYSIS:")
                        print(f"  Best Growth Range: {factor_analysis['best_growth_range']}")
                        print(f"  Best Debt Range: {factor_analysis['best_debt_range']}")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())