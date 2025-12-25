#!/usr/bin/env python3
"""
Simple Debt-Adjusted Growth Strategy

Uses available historical fundamentals data to create a simple growth-based valuation model:
1. Calculate revenue growth from revenue_per_share
2. Adjust for debt using debt_to_equity ratio
3. Compare current P/S to growth-adjusted fair value
4. Generate signals when undervalued relative to growth potential

Much simpler than DCF - just uses current metrics available in the database.
"""

import asyncio
import asyncpg
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SimpleGrowthSignal:
    """Growth signal data"""
    symbol: str
    date: date
    revenue_growth: float
    debt_to_equity: float
    current_ps: float
    fair_ps: float
    signal_strength: float
    is_buy_signal: bool
    price: float


class SimpleGrowthStrategy:
    """
    Simple growth strategy using available historical data.
    
    Key insight: Companies with high revenue growth should trade at higher P/S ratios,
    but high debt reduces the premium we're willing to pay.
    """
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        
        # Strategy parameters (tunable)
        self.base_ps_multiple = 1.5  # Base P/S for zero-growth company
        self.growth_multiplier = 0.15  # P/S increase per 1% revenue growth
        self.debt_penalty_factor = 0.3  # Penalty for debt/equity ratio
        self.buy_threshold = 0.8  # Buy when trading at 80% of fair value
        
    async def calculate_revenue_growth(self, symbol: str, current_date: date, 
                                     lookback_days: int = 365) -> Optional[float]:
        """Calculate revenue growth rate over specified period."""
        
        query = """
        SELECT date, revenue_per_share
        FROM historical_fundamentals_daily
        WHERE symbol = $1 
            AND revenue_per_share IS NOT NULL
            AND revenue_per_share > 0
            AND date <= $2
        ORDER BY date DESC
        LIMIT 500
        """
        
        rows = await self.conn.fetch(query, symbol, current_date)
        
        if len(rows) < 2:
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(rows)
        df = df.sort_values('date')  # dates are already date objects from asyncpg
        
        # Get current and historical revenue per share
        current_revenue = df.iloc[-1]['revenue_per_share']
        target_date = current_date - timedelta(days=lookback_days)
        
        # Find closest historical point
        historical_data = df[df['date'] <= target_date]
        
        if historical_data.empty:
            return None
            
        historical_revenue = historical_data.iloc[-1]['revenue_per_share']
        
        if historical_revenue <= 0:
            return None
            
        # Calculate annualized growth rate
        actual_days = (df.iloc[-1]['date'] - historical_data.iloc[-1]['date']).days
        if actual_days < 90:  # Need at least 3 months of data
            return None
            
        years = actual_days / 365.25
        growth_rate = ((current_revenue / historical_revenue) ** (1/years) - 1) * 100
        
        return growth_rate
        
    async def get_latest_metrics(self, symbol: str, current_date: date) -> Optional[Dict]:
        """Get latest financial metrics for a symbol."""
        
        query = """
        SELECT 
            ps_ratio,
            debt_to_equity,
            revenue_per_share,
            close_price,
            market_cap,
            date
        FROM historical_fundamentals_daily
        WHERE symbol = $1 
            AND date <= $2
            AND ps_ratio IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
        """
        
        row = await self.conn.fetchrow(query, symbol, current_date)
        
        if not row:
            return None
            
        return {
            'ps_ratio': float(row['ps_ratio']),
            'debt_to_equity': float(row['debt_to_equity']) if row['debt_to_equity'] else 0,
            'revenue_per_share': float(row['revenue_per_share']) if row['revenue_per_share'] else None,
            'price': float(row['close_price']) if row['close_price'] else None,
            'market_cap': float(row['market_cap']) if row['market_cap'] else None,
            'date': row['date']
        }
        
    def calculate_fair_ps(self, revenue_growth: float, debt_to_equity: float) -> float:
        """
        Calculate fair P/S ratio based on growth and debt.
        
        Formula:
        Fair P/S = Base + (Growth * Multiplier) * (1 - Debt Penalty)
        
        Higher growth = higher multiple
        Higher debt = lower multiple (risk adjustment)
        """
        
        # Growth component (can be negative for declining companies)
        growth_component = self.base_ps_multiple + (revenue_growth * self.growth_multiplier)
        
        # Debt penalty - higher debt/equity reduces the multiple
        debt_penalty = min(0.8, self.debt_penalty_factor * debt_to_equity)  # Cap penalty at 80%
        debt_adjustment = max(0.2, 1 - debt_penalty)  # Never go below 20% of base multiple
        
        fair_ps = growth_component * debt_adjustment
        
        # Reasonable bounds
        return max(0.3, min(fair_ps, 8.0))
        
    async def analyze_symbol(self, symbol: str, analysis_date: date) -> Optional[SimpleGrowthSignal]:
        """Analyze a single symbol and generate signal."""
        
        try:
            # Get revenue growth
            revenue_growth = await self.calculate_revenue_growth(symbol, analysis_date)
            
            if revenue_growth is None:
                return None
                
            # Get current metrics
            metrics = await self.get_latest_metrics(symbol, analysis_date)
            
            if not metrics or metrics['ps_ratio'] <= 0:
                return None
                
            # Calculate fair value
            fair_ps = self.calculate_fair_ps(revenue_growth, metrics['debt_to_equity'])
            
            # Signal strength (how undervalued)
            signal_strength = (fair_ps - metrics['ps_ratio']) / fair_ps
            
            # Buy signal if significantly undervalued and growing
            is_buy = (metrics['ps_ratio'] < fair_ps * self.buy_threshold and 
                     revenue_growth > 0)  # Only buy growing companies
                     
            return SimpleGrowthSignal(
                symbol=symbol,
                date=analysis_date,
                revenue_growth=revenue_growth,
                debt_to_equity=metrics['debt_to_equity'],
                current_ps=metrics['ps_ratio'],
                fair_ps=fair_ps,
                signal_strength=signal_strength,
                is_buy_signal=is_buy,
                price=metrics['price'] or 0
            )
            
        except Exception as e:
            logger.warning(f"Error analyzing {symbol}: {e}")
            return None
            
    async def scan_universe(self, scan_date: date, 
                           min_data_points: int = 100) -> List[SimpleGrowthSignal]:
        """Scan universe of stocks for opportunities."""
        
        # Get symbols with sufficient data
        query = """
        SELECT symbol, COUNT(*) as data_points, MAX(date) as latest_date
        FROM historical_fundamentals_daily
        WHERE ps_ratio IS NOT NULL 
            AND revenue_per_share IS NOT NULL
            AND revenue_per_share > 0
            AND date <= $1
        GROUP BY symbol
        HAVING COUNT(*) >= $2
            AND MAX(date) >= $1::date - INTERVAL '30 days'
        ORDER BY data_points DESC
        """
        
        symbols_data = await self.conn.fetch(query, scan_date, min_data_points)
        symbols = [row['symbol'] for row in symbols_data]
        
        logger.info(f"Scanning {len(symbols)} symbols with sufficient data")
        
        # Analyze all symbols
        signals = []
        for symbol in symbols:
            signal = await self.analyze_symbol(symbol, scan_date)
            if signal:
                signals.append(signal)
                
        # Sort by signal strength (most undervalued first)
        signals.sort(key=lambda x: x.signal_strength, reverse=True)
        
        return signals
        
    async def backtest(self, start_date: date, end_date: date, 
                      holding_days: int = 90) -> Dict:
        """
        Simple backtest with fixed holding periods.
        """
        
        logger.info(f"Backtesting from {start_date} to {end_date}")
        
        # Get all trading days
        query = """
        SELECT DISTINCT date 
        FROM historical_fundamentals_daily 
        WHERE date >= $1 AND date <= $2
        ORDER BY date
        """
        
        date_rows = await self.conn.fetch(query, start_date, end_date)
        trading_days = [row['date'] for row in date_rows]
        
        # Track portfolio
        initial_cash = 100000
        cash = initial_cash
        positions = {}  # symbol -> (entry_price, entry_date, shares, signal)
        trades = []
        
        # Track daily values for performance calculation
        daily_values = []
        
        for current_date in trading_days[::5]:  # Check every 5 days to reduce compute
            
            # Process exits first
            for symbol in list(positions.keys()):
                entry_price, entry_date, shares, entry_signal = positions[symbol]
                holding_period = (current_date - entry_date).days
                
                if holding_period >= holding_days:
                    # Exit position
                    exit_metrics = await self.get_latest_metrics(symbol, current_date)
                    
                    if exit_metrics and exit_metrics['price']:
                        exit_price = exit_metrics['price']
                        exit_value = shares * exit_price
                        cash += exit_value
                        
                        # Record trade
                        return_pct = (exit_price - entry_price) / entry_price * 100
                        
                        trades.append({
                            'symbol': symbol,
                            'entry_date': entry_date,
                            'exit_date': current_date,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'return_pct': return_pct,
                            'holding_days': holding_period,
                            'entry_growth': entry_signal.revenue_growth,
                            'entry_ps': entry_signal.current_ps,
                            'fair_ps': entry_signal.fair_ps
                        })
                        
                        del positions[symbol]
                        
            # Look for new opportunities (max 5 positions)
            if len(positions) < 5:
                signals = await self.scan_universe(current_date)
                buy_signals = [s for s in signals if s.is_buy_signal]
                
                for signal in buy_signals[:3]:  # Max 3 new positions per period
                    if signal.symbol not in positions and signal.price > 0:
                        
                        # Position size: 15% of cash
                        position_size = cash * 0.15
                        
                        if position_size > 1000:  # Minimum position
                            shares = position_size / signal.price
                            cash -= position_size
                            
                            positions[signal.symbol] = (signal.price, current_date, shares, signal)
                            
                            logger.info(f"BUY {signal.symbol}: Growth={signal.revenue_growth:.1f}%, "
                                      f"P/S={signal.current_ps:.2f} vs fair {signal.fair_ps:.2f}")
                                      
                            if len(positions) >= 5:
                                break
            
            # Calculate portfolio value
            position_value = 0
            for symbol, (entry_price, entry_date, shares, _) in positions.items():
                current_metrics = await self.get_latest_metrics(symbol, current_date)
                if current_metrics and current_metrics['price']:
                    position_value += shares * current_metrics['price']
                    
            total_value = cash + position_value
            daily_values.append(total_value)
        
        # Calculate performance metrics
        if not trades:
            return {
                'total_return': 0,
                'num_trades': 0,
                'win_rate': 0,
                'message': 'No trades executed'
            }
            
        # Performance calculations
        returns = [t['return_pct'] for t in trades]
        winning_trades = [r for r in returns if r > 0]
        
        total_return = (daily_values[-1] / initial_cash - 1) * 100 if daily_values else 0
        
        results = {
            'total_return': total_return,
            'num_trades': len(trades),
            'win_rate': len(winning_trades) / len(trades) * 100,
            'avg_return': np.mean(returns),
            'median_return': np.median(returns),
            'best_trade': max(returns),
            'worst_trade': min(returns),
            'trades': trades[:10]  # Sample trades
        }
        
        return results


async def main():
    """Run the simple growth strategy."""
    
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        strategy = SimpleGrowthStrategy(conn)
        
        # Current market scan
        print("\n" + "="*70)
        print("CURRENT MARKET OPPORTUNITIES")
        print("="*70)
        
        # Get most recent data date
        latest_date_row = await conn.fetchrow("""
            SELECT MAX(date) as max_date 
            FROM historical_fundamentals_daily 
            WHERE ps_ratio IS NOT NULL
        """)
        
        if latest_date_row:
            latest_date = latest_date_row['max_date']
            print(f"Scanning date: {latest_date}")
            
            signals = await strategy.scan_universe(latest_date)
            buy_signals = [s for s in signals if s.is_buy_signal]
            
            print(f"Found {len(buy_signals)} buy signals out of {len(signals)} analyzed\n")
            
            if buy_signals:
                print(f"{'Symbol':<8} {'Growth':<8} {'Debt/Eq':<8} {'P/S':<6} {'Fair P/S':<8} {'Discount':<8} {'Price':<8}")
                print("-" * 62)
                
                for signal in buy_signals[:15]:
                    discount = (1 - signal.current_ps / signal.fair_ps) * 100
                    print(f"{signal.symbol:<8} {signal.revenue_growth:>7.1f}% "
                          f"{signal.debt_to_equity:>7.2f} {signal.current_ps:>5.2f} "
                          f"{signal.fair_ps:>7.2f} {discount:>7.1f}% "
                          f"{signal.price:>7.2f}")
                          
        # Backtest
        print("\n" + "="*70)
        print("BACKTEST RESULTS")
        print("="*70)
        
        # Test different periods
        test_periods = [
            (date(2024, 1, 1), date(2024, 11, 30), "2024 YTD"),
            (date(2023, 1, 1), date(2023, 12, 31), "2023 Full Year"),
            (date(2022, 1, 1), date(2024, 11, 30), "2022-2024 (3 Years)")
        ]
        
        for start_date, end_date, period_name in test_periods:
            print(f"\n{period_name}: {start_date} to {end_date}")
            
            results = await strategy.backtest(start_date, end_date)
            
            print(f"  Total Return: {results['total_return']:+.2f}%")
            print(f"  Number of Trades: {results['num_trades']}")
            
            if results['num_trades'] > 0:
                print(f"  Win Rate: {results['win_rate']:.1f}%")
                print(f"  Average Return: {results['avg_return']:+.2f}%")
                print(f"  Median Return: {results['median_return']:+.2f}%")
                print(f"  Best Trade: {results['best_trade']:+.2f}%")
                print(f"  Worst Trade: {results['worst_trade']:+.2f}%")
                
        # Show sample trades from most recent backtest
        if test_periods and results.get('trades'):
            print(f"\nSample Trades from {test_periods[-1][2]}:")
            print(f"{'Symbol':<8} {'Entry':<12} {'Exit':<12} {'Growth':<8} {'P/S':<6} {'Return':<8}")
            print("-" * 60)
            
            for trade in results['trades'][:8]:
                print(f"{trade['symbol']:<8} "
                      f"{trade['entry_date'].strftime('%Y-%m-%d'):<12} "
                      f"{trade['exit_date'].strftime('%Y-%m-%d'):<12} "
                      f"{trade['entry_growth']:>7.1f}% "
                      f"{trade['entry_ps']:>5.2f} "
                      f"{trade['return_pct']:>7.1f}%")
                      
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())