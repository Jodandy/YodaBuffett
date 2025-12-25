#!/usr/bin/env python3
"""
Debt-Adjusted Topline Growth Strategy

A simple valuation model that:
1. Calculates YoY and 3-year revenue growth rates
2. Adjusts for debt levels (high debt requires higher growth)
3. Compares current P/S ratio to growth-adjusted fair value
4. Generates buy signals when growth is high but valuation is low

This is much simpler than DCF - no complex projections, just current metrics.
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
class GrowthMetrics:
    """Holds growth and valuation metrics for a company"""
    symbol: str
    date: date
    revenue_1y_growth: float
    revenue_3y_cagr: float
    debt_to_revenue: float
    current_ps_ratio: float
    growth_adjusted_ps: float
    signal_strength: float
    is_buy_signal: bool


class DebtAdjustedGrowthStrategy:
    """
    Simple topline growth strategy with debt adjustment.
    
    Core principle: Fast-growing companies deserve higher valuations,
    but debt reduces the margin of safety required.
    """
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        
        # Strategy parameters
        self.base_ps_multiple = 2.0  # Base P/S for no-growth company
        self.growth_multiplier = 20.0  # How much 1% growth adds to P/S
        self.debt_penalty = 0.5  # Debt/Revenue ratio penalty factor
        self.buy_discount = 0.7  # Buy when trading at 70% of fair value
        
    async def calculate_revenue_growth(self, symbol: str, current_date: date) -> Tuple[Optional[float], Optional[float]]:
        """Calculate 1-year and 3-year revenue growth rates."""
        
        # Get historical revenue data
        query = """
        SELECT 
            date,
            total_revenue,
            quarterly_revenue_growth
        FROM daily_fundamentals
        WHERE symbol = $1 
            AND total_revenue IS NOT NULL
            AND date <= $2
        ORDER BY date DESC
        LIMIT 20
        """
        
        rows = await self.conn.fetch(query, symbol, current_date)
        
        if len(rows) < 2:
            return None, None
            
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Get current revenue
        current_revenue = float(rows[0]['total_revenue'])
        current_dt = pd.to_datetime(rows[0]['date'])
        
        # Calculate 1-year growth
        one_year_ago = current_dt - pd.Timedelta(days=365)
        one_year_data = df[df['date'] <= one_year_ago].tail(1)
        
        revenue_1y_growth = None
        if not one_year_data.empty:
            old_revenue = float(one_year_data.iloc[0]['total_revenue'])
            if old_revenue > 0:
                revenue_1y_growth = ((current_revenue / old_revenue) - 1) * 100
        
        # Calculate 3-year CAGR
        three_years_ago = current_dt - pd.Timedelta(days=365*3)
        three_year_data = df[df['date'] <= three_years_ago].tail(1)
        
        revenue_3y_cagr = None
        if not three_year_data.empty:
            old_revenue = float(three_year_data.iloc[0]['total_revenue'])
            if old_revenue > 0:
                years = (current_dt - pd.to_datetime(three_year_data.iloc[0]['date'])).days / 365.25
                revenue_3y_cagr = ((current_revenue / old_revenue) ** (1/years) - 1) * 100
                
        return revenue_1y_growth, revenue_3y_cagr
        
    async def calculate_debt_metrics(self, symbol: str, current_date: date) -> Optional[float]:
        """Calculate debt to revenue ratio."""
        
        query = """
        SELECT 
            total_revenue,
            total_debt,
            total_debt_to_equity
        FROM daily_fundamentals
        WHERE symbol = $1 
            AND date <= $2
            AND total_revenue IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
        """
        
        row = await self.conn.fetchrow(query, symbol, current_date)
        
        if not row or not row['total_revenue']:
            return None
            
        total_revenue = float(row['total_revenue'])
        total_debt = float(row['total_debt']) if row['total_debt'] else 0
        
        if total_revenue > 0:
            return total_debt / total_revenue
        return None
        
    async def get_current_valuation(self, symbol: str, current_date: date) -> Tuple[Optional[float], Optional[float]]:
        """Get current P/S ratio and stock price."""
        
        # Get market cap and revenue for P/S calculation
        query = """
        SELECT 
            df.market_cap,
            df.total_revenue,
            df.price_to_sales,
            md.close_price as price
        FROM daily_fundamentals df
        LEFT JOIN daily_price_data md ON md.symbol = df.symbol AND md.date = df.date
        WHERE df.symbol = $1 
            AND df.date <= $2
        ORDER BY df.date DESC
        LIMIT 1
        """
        
        row = await self.conn.fetchrow(query, symbol, current_date)
        
        if not row:
            return None, None
            
        # Try to get P/S ratio directly first
        if row['price_to_sales']:
            ps_ratio = float(row['price_to_sales'])
        elif row['market_cap'] and row['total_revenue'] and row['total_revenue'] > 0:
            # Calculate P/S if not provided
            ps_ratio = float(row['market_cap']) / float(row['total_revenue'])
        else:
            ps_ratio = None
            
        price = float(row['price']) if row['price'] else None
        
        return ps_ratio, price
        
    def calculate_fair_value_ps(self, revenue_growth: float, debt_to_revenue: float) -> float:
        """
        Calculate fair value P/S ratio based on growth and debt.
        
        Formula:
        Fair P/S = Base P/S + (Growth Rate * Growth Multiplier) * (1 - Debt Penalty * Debt/Revenue)
        
        High debt reduces the valuation multiple for the same growth rate.
        """
        
        # Use average of 1y and 3y growth if both available
        growth_component = self.base_ps_multiple + (revenue_growth * self.growth_multiplier / 100)
        
        # Apply debt penalty (high debt reduces multiple)
        debt_adjustment = max(0.3, 1 - self.debt_penalty * debt_to_revenue)
        
        fair_ps = growth_component * debt_adjustment
        
        # Cap at reasonable levels
        return min(max(fair_ps, 0.5), 10.0)
        
    async def analyze_company(self, symbol: str, analysis_date: date) -> Optional[GrowthMetrics]:
        """Analyze a single company and generate signals."""
        
        try:
            # Get growth rates
            growth_1y, growth_3y = await self.calculate_revenue_growth(symbol, analysis_date)
            
            if growth_1y is None and growth_3y is None:
                return None
                
            # Use average of available growth rates
            if growth_1y is not None and growth_3y is not None:
                avg_growth = (growth_1y + growth_3y) / 2
            else:
                avg_growth = growth_1y if growth_1y is not None else growth_3y
                
            # Get debt metrics
            debt_to_revenue = await self.calculate_debt_metrics(symbol, analysis_date)
            if debt_to_revenue is None:
                debt_to_revenue = 0  # Assume no debt if not available
                
            # Get current valuation
            current_ps, price = await self.get_current_valuation(symbol, analysis_date)
            if current_ps is None:
                return None
                
            # Calculate fair value P/S
            fair_ps = self.calculate_fair_value_ps(avg_growth, debt_to_revenue)
            
            # Calculate signal strength (how undervalued)
            signal_strength = (fair_ps - current_ps) / fair_ps
            
            # Generate buy signal if significantly undervalued
            is_buy = current_ps < (fair_ps * self.buy_discount) and avg_growth > 5  # At least 5% growth
            
            return GrowthMetrics(
                symbol=symbol,
                date=analysis_date,
                revenue_1y_growth=growth_1y if growth_1y is not None else 0,
                revenue_3y_cagr=growth_3y if growth_3y is not None else 0,
                debt_to_revenue=debt_to_revenue,
                current_ps_ratio=current_ps,
                growth_adjusted_ps=fair_ps,
                signal_strength=signal_strength,
                is_buy_signal=is_buy
            )
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {e}")
            return None
            
    async def scan_market(self, scan_date: date, symbols: Optional[List[str]] = None) -> List[GrowthMetrics]:
        """Scan the market for opportunities."""
        
        # Get symbols if not provided
        if symbols is None:
            query = """
            SELECT DISTINCT symbol 
            FROM daily_fundamentals 
            WHERE date = $1
                AND total_revenue IS NOT NULL
            """
            rows = await self.conn.fetch(query, scan_date)
            symbols = [row['symbol'] for row in rows]
            
        logger.info(f"Scanning {len(symbols)} symbols for {scan_date}")
        
        # Analyze all symbols
        results = []
        for symbol in symbols:
            metrics = await self.analyze_company(symbol, scan_date)
            if metrics:
                results.append(metrics)
                
        # Sort by signal strength
        results.sort(key=lambda x: x.signal_strength, reverse=True)
        
        return results
        
    async def backtest(self, start_date: date, end_date: date, 
                      symbols: Optional[List[str]] = None) -> Dict:
        """
        Backtest the strategy over a date range.
        
        Simple approach: Buy on signal, hold for 3 months, track returns.
        """
        
        logger.info(f"Backtesting from {start_date} to {end_date}")
        
        # Get all trading days
        query = """
        SELECT DISTINCT date 
        FROM daily_price_data 
        WHERE date >= $1 AND date <= $2
        ORDER BY date
        """
        
        date_rows = await self.conn.fetch(query, start_date, end_date)
        trading_days = [row['date'] for row in date_rows]
        
        # Track trades
        trades = []
        portfolio_value = 10000  # Starting cash
        cash = portfolio_value
        positions = {}  # symbol -> (entry_price, entry_date, shares)
        
        # Also track daily portfolio value for metrics
        daily_values = []
        
        for current_date in trading_days:
            # Check for exit signals (3 months holding period)
            for symbol in list(positions.keys()):
                entry_price, entry_date, shares = positions[symbol]
                holding_days = (current_date - entry_date).days
                
                if holding_days >= 90:  # 3 months
                    # Exit position
                    exit_price_row = await self.conn.fetchrow(
                        "SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date = $2",
                        symbol, current_date
                    )
                    
                    if exit_price_row:
                        exit_price = float(exit_price_row['close_price'])
                        exit_value = shares * exit_price
                        cash += exit_value
                        
                        return_pct = (exit_price - entry_price) / entry_price * 100
                        
                        trades.append({
                            'symbol': symbol,
                            'entry_date': entry_date,
                            'exit_date': current_date,
                            'entry_price': entry_price,
                            'exit_price': exit_price,
                            'return_pct': return_pct,
                            'holding_days': holding_days
                        })
                        
                        del positions[symbol]
            
            # Scan for new opportunities (only on Mondays to reduce noise)
            if current_date.weekday() == 0 and len(positions) < 5:  # Max 5 positions
                signals = await self.scan_market(current_date, symbols)
                
                for signal in signals:
                    if signal.is_buy_signal and signal.symbol not in positions:
                        # Check if we have cash
                        position_size = cash * 0.2  # 20% per position
                        
                        if position_size > 1000:  # Minimum position size
                            # Get entry price
                            price_row = await self.conn.fetchrow(
                                "SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date = $2",
                                signal.symbol, current_date
                            )
                            
                            if price_row:
                                entry_price = float(price_row['close_price'])
                                shares = position_size / entry_price
                                cash -= position_size
                                
                                positions[signal.symbol] = (entry_price, current_date, shares)
                                
                                logger.info(f"BUY {signal.symbol} on {current_date}: "
                                          f"Growth={signal.revenue_1y_growth:.1f}%, "
                                          f"P/S={signal.current_ps_ratio:.2f} vs {signal.growth_adjusted_ps:.2f}")
                                
                                if len(positions) >= 5:
                                    break
                                    
            # Calculate daily portfolio value
            position_value = 0
            for symbol, (entry_price, entry_date, shares) in positions.items():
                price_row = await self.conn.fetchrow(
                    "SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date = $2",
                    symbol, current_date
                )
                if price_row:
                    position_value += shares * float(price_row['close_price'])
                    
            total_value = cash + position_value
            daily_values.append(total_value)
            
        # Calculate performance metrics
        if not trades:
            logger.warning("No trades executed during backtest period")
            return {
                'total_return': 0,
                'num_trades': 0,
                'win_rate': 0,
                'avg_return': 0,
                'sharpe_ratio': 0
            }
            
        # Calculate metrics
        returns = [t['return_pct'] for t in trades]
        winning_trades = [r for r in returns if r > 0]
        
        # Calculate Sharpe ratio from daily returns
        daily_returns = pd.Series(daily_values).pct_change().dropna()
        sharpe = np.sqrt(252) * daily_returns.mean() / daily_returns.std() if len(daily_returns) > 1 else 0
        
        total_return = (daily_values[-1] / portfolio_value - 1) * 100 if daily_values else 0
        
        results = {
            'total_return': total_return,
            'num_trades': len(trades),
            'win_rate': len(winning_trades) / len(trades) * 100 if trades else 0,
            'avg_return': np.mean(returns) if returns else 0,
            'sharpe_ratio': sharpe,
            'trades': trades[:10]  # First 10 trades as examples
        }
        
        return results


async def main():
    """Run analysis and backtest."""
    
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        strategy = DebtAdjustedGrowthStrategy(conn)
        
        # First, scan for current opportunities
        print("\n" + "="*60)
        print("CURRENT MARKET SCAN - TOP GROWTH OPPORTUNITIES")
        print("="*60)
        
        # Get the most recent date with data
        latest_date_row = await conn.fetchrow(
            "SELECT MAX(date) as max_date FROM daily_fundamentals"
        )
        
        if latest_date_row and latest_date_row['max_date']:
            latest_date = latest_date_row['max_date']
            
            signals = await strategy.scan_market(latest_date)
            
            print(f"\nScanning date: {latest_date}")
            print(f"Found {len([s for s in signals if s.is_buy_signal])} buy signals out of {len(signals)} companies\n")
            
            # Show top 10 opportunities
            print(f"{'Symbol':<8} {'Growth 1Y':<10} {'Growth 3Y':<10} {'Debt/Rev':<10} "
                  f"{'P/S':<8} {'Fair P/S':<10} {'Discount':<10}")
            print("-" * 76)
            
            for signal in signals[:10]:
                if signal.is_buy_signal:
                    discount = (1 - signal.current_ps_ratio / signal.growth_adjusted_ps) * 100
                    print(f"{signal.symbol:<8} {signal.revenue_1y_growth:>9.1f}% "
                          f"{signal.revenue_3y_cagr:>9.1f}% {signal.debt_to_revenue:>9.2f} "
                          f"{signal.current_ps_ratio:>7.2f} {signal.growth_adjusted_ps:>9.2f} "
                          f"{discount:>9.1f}%")
                          
        # Run backtest for 2024
        print("\n" + "="*60)
        print("2024 BACKTEST RESULTS")
        print("="*60)
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 12, 31)
        
        # Focus on Nordic companies
        nordic_symbols = await conn.fetch("""
            SELECT DISTINCT cm.primary_ticker as symbol
            FROM company_master cm
            JOIN daily_fundamentals df ON df.symbol = cm.primary_ticker
            WHERE cm.primary_exchange IN ('STO', 'HEL', 'CPH', 'OSL')
                AND df.total_revenue IS NOT NULL
            GROUP BY cm.primary_ticker
            HAVING COUNT(*) > 100
        """)
        
        symbols = [row['symbol'] for row in nordic_symbols]
        logger.info(f"Testing with {len(symbols)} Nordic companies")
        
        results = await strategy.backtest(start_date, end_date, symbols)
        
        print(f"\nBacktest Period: {start_date} to {end_date}")
        print(f"Strategy: Debt-Adjusted Topline Growth\n")
        
        print(f"Total Return: {results['total_return']:.2f}%")
        print(f"Number of Trades: {results['num_trades']}")
        print(f"Win Rate: {results['win_rate']:.1f}%")
        print(f"Average Return per Trade: {results['avg_return']:.2f}%")
        print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
        
        if results['trades']:
            print("\nSample Trades:")
            print(f"{'Symbol':<8} {'Entry':<12} {'Exit':<12} {'Return':<10}")
            print("-" * 42)
            for trade in results['trades'][:5]:
                print(f"{trade['symbol']:<8} {trade['entry_date'].strftime('%Y-%m-%d'):<12} "
                      f"{trade['exit_date'].strftime('%Y-%m-%d'):<12} {trade['return_pct']:>9.1f}%")
                      
    finally:
        await conn.close_price()


if __name__ == "__main__":
    asyncio.run(main())