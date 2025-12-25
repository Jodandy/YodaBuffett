#!/usr/bin/env python3
"""
Fixed Simple Debt-Adjusted Growth Strategy

Handles date types properly and includes error handling.
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
        
        try:
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
            
            # Get current and target dates
            current_revenue = float(rows[0]['revenue_per_share'])
            current_dt = rows[0]['date']
            target_date = current_date - timedelta(days=lookback_days)
            
            # Find closest historical point
            historical_revenue = None
            historical_dt = None
            
            for row in rows:
                if row['date'] <= target_date:
                    historical_revenue = float(row['revenue_per_share'])
                    historical_dt = row['date']
                    break
                    
            if historical_revenue is None or historical_revenue <= 0:
                return None
                
            # Calculate annualized growth rate
            if isinstance(current_dt, date) and isinstance(historical_dt, date):
                actual_days = (current_dt - historical_dt).days
            else:
                # Handle datetime objects if needed
                actual_days = (current_dt - historical_dt).days
                
            if actual_days < 90:  # Need at least 3 months of data
                return None
                
            years = actual_days / 365.25
            growth_rate = ((current_revenue / historical_revenue) ** (1/years) - 1) * 100
            
            # Sanity check: cap extreme growth rates
            return max(-50, min(growth_rate, 200))  # -50% to +200%
            
        except Exception as e:
            logger.debug(f"Error calculating growth for {symbol}: {e}")
            return None
            
    async def get_latest_metrics(self, symbol: str, current_date: date) -> Optional[Dict]:
        """Get latest financial metrics for a symbol."""
        
        try:
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
                AND ps_ratio > 0
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
            
        except Exception as e:
            logger.debug(f"Error getting metrics for {symbol}: {e}")
            return None
        
    def calculate_fair_ps(self, revenue_growth: float, debt_to_equity: float) -> float:
        """Calculate fair P/S ratio based on growth and debt."""
        
        try:
            # Growth component (can be negative for declining companies)
            growth_component = self.base_ps_multiple + (revenue_growth * self.growth_multiplier)
            
            # Debt penalty - higher debt/equity reduces the multiple
            debt_penalty = min(0.8, self.debt_penalty_factor * debt_to_equity)
            debt_adjustment = max(0.2, 1 - debt_penalty)
            
            fair_ps = growth_component * debt_adjustment
            
            # Reasonable bounds
            return max(0.3, min(fair_ps, 8.0))
            
        except Exception as e:
            logger.debug(f"Error calculating fair P/S: {e}")
            return 1.5  # Default reasonable P/S
        
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
                     revenue_growth > 0 and  # Only buy growing companies
                     signal_strength > 0.15)  # At least 15% undervalued
                     
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
            logger.debug(f"Error analyzing {symbol}: {e}")
            return None
            
    async def scan_universe(self, scan_date: date, 
                           min_data_points: int = 100) -> List[SimpleGrowthSignal]:
        """Scan universe of stocks for opportunities."""
        
        # Get symbols with sufficient data
        query = """
        SELECT symbol, COUNT(*) as data_points
        FROM historical_fundamentals_daily
        WHERE ps_ratio IS NOT NULL 
            AND revenue_per_share IS NOT NULL
            AND revenue_per_share > 0
            AND date <= $1
        GROUP BY symbol
        HAVING COUNT(*) >= $2
        ORDER BY data_points DESC
        LIMIT 100
        """
        
        symbols_data = await self.conn.fetch(query, scan_date, min_data_points)
        symbols = [row['symbol'] for row in symbols_data]
        
        logger.info(f"Analyzing top {len(symbols)} symbols with sufficient data")
        
        # Analyze symbols
        signals = []
        for symbol in symbols:
            signal = await self.analyze_symbol(symbol, scan_date)
            if signal:
                signals.append(signal)
                
        # Sort by signal strength (most undervalued first)
        signals.sort(key=lambda x: x.signal_strength, reverse=True)
        
        return signals


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
        print("DEBT-ADJUSTED GROWTH STRATEGY - TOP OPPORTUNITIES")
        print("="*70)
        
        # Get most recent data date
        latest_date_row = await conn.fetchrow("""
            SELECT MAX(date) as max_date 
            FROM historical_fundamentals_daily 
            WHERE ps_ratio IS NOT NULL
        """)
        
        if latest_date_row:
            latest_date = latest_date_row['max_date']
            print(f"Analysis date: {latest_date}")
            
            signals = await strategy.scan_universe(latest_date)
            buy_signals = [s for s in signals if s.is_buy_signal]
            
            print(f"Total analyzed: {len(signals)} companies")
            print(f"Buy signals: {len(buy_signals)}\n")
            
            if buy_signals:
                print("TOP OPPORTUNITIES:")
                print(f"{'Symbol':<10} {'Growth':<8} {'Debt/Eq':<8} {'P/S':<6} {'Fair':<6} {'Disc%':<7} {'Price':<8}")
                print("-" * 65)
                
                for signal in buy_signals[:15]:
                    discount = (1 - signal.current_ps / signal.fair_ps) * 100
                    print(f"{signal.symbol:<10} {signal.revenue_growth:>7.1f}% "
                          f"{signal.debt_to_equity:>7.2f} {signal.current_ps:>5.2f} "
                          f"{signal.fair_ps:>5.2f} {discount:>6.1f}% "
                          f"{signal.price:>7.2f}")
            else:
                print("No buy signals found with current criteria.")
                
            # Show some analysis stats
            if signals:
                growth_rates = [s.revenue_growth for s in signals]
                ps_ratios = [s.current_ps for s in signals]
                
                print(f"\nMARKET STATISTICS:")
                print(f"Average growth rate: {np.mean(growth_rates):+.1f}%")
                print(f"Median P/S ratio: {np.median(ps_ratios):.2f}")
                print(f"Companies with positive growth: {len([g for g in growth_rates if g > 0])}")
                
        # Simple backtest for demonstration
        print("\n" + "="*70)
        print("SIMPLE BACKTEST - LAST 6 MONTHS")
        print("="*70)
        
        if latest_date_row:
            backtest_start = latest_date_row['max_date'] - timedelta(days=180)
            backtest_end = latest_date_row['max_date']
            
            print(f"Period: {backtest_start} to {backtest_end}")
            
            # Get signals from 6 months ago
            old_signals = await strategy.scan_universe(backtest_start)
            old_buys = [s for s in old_signals if s.is_buy_signal]
            
            if old_buys:
                print(f"Generated {len(old_buys)} buy signals 6 months ago")
                
                # Check performance of those signals
                total_return = 0
                successful_checks = 0
                
                for signal in old_buys[:10]:  # Check top 10
                    # Get current metrics
                    current_metrics = await strategy.get_latest_metrics(signal.symbol, backtest_end)
                    
                    if current_metrics and current_metrics['price'] and signal.price > 0:
                        return_pct = (current_metrics['price'] / signal.price - 1) * 100
                        total_return += return_pct
                        successful_checks += 1
                        
                        print(f"  {signal.symbol:<10}: {signal.price:>6.2f} → {current_metrics['price']:>6.2f} "
                              f"({return_pct:+.1f}%)")
                
                if successful_checks > 0:
                    avg_return = total_return / successful_checks
                    print(f"\nAverage return from signals: {avg_return:+.1f}%")
                    print(f"Sample size: {successful_checks} companies")
            else:
                print("No buy signals were generated 6 months ago.")
                
        print(f"\nSTRATEGY SUMMARY:")
        print(f"- Base P/S multiple: {strategy.base_ps_multiple:.1f}x")
        print(f"- Growth multiplier: {strategy.growth_multiplier:.2f}x per 1% growth")
        print(f"- Debt penalty factor: {strategy.debt_penalty_factor:.1f}x debt/equity")
        print(f"- Buy threshold: {strategy.buy_threshold:.0%} of fair value")
        print(f"- Focuses on: Growing companies with reasonable debt levels")
        print(f"- Simple valuation: P/S based on revenue growth, adjusted for leverage")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())