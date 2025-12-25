#!/usr/bin/env python3
"""
Fundamental Value Strategy Backtester

Comprehensive backtesting system for the Fat Pitch valuation strategy
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Any
import pandas as pd
import numpy as np
from dataclasses import dataclass
import json
import logging
from fundamental_value_strategy_enhanced import FundamentalValueStrategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class Trade:
    """Individual trade record"""
    symbol: str
    entry_date: date
    exit_date: Optional[date]
    entry_price: float
    exit_price: Optional[float]
    shares: int
    entry_reason: str  # 'fat_pitch_buy', 'undervalued_buy'
    exit_reason: Optional[str]  # 'target_hit', 'overvalued', 'stop_loss', 'time_limit'
    
    # Valuation at entry
    fat_pitch_price: float
    fair_value: float
    upside_target: float
    entry_asymmetry: float
    
    # Performance
    holding_days: Optional[int] = None
    return_pct: Optional[float] = None
    position_size: float = 0.0
    
    # Commission and costs
    entry_commission: float = 0.0
    exit_commission: float = 0.0
    total_cost: float = 0.0


@dataclass
class PortfolioSnapshot:
    """Portfolio state at a point in time"""
    date: date
    cash: float
    total_value: float
    positions: int
    returns: float  # Daily return
    
    # Performance metrics
    total_return: float
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None


class FundamentalValueBacktester:
    """Backtesting engine for fundamental value strategy"""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        self.strategy = FundamentalValueStrategy(conn)
        
        # Backtesting parameters
        self.initial_capital = 1000000  # 1M SEK
        self.commission_rate = 0.002  # 0.2% total commission
        self.position_size = 0.20  # 20% per position
        self.max_positions = 5
        self.stop_loss_pct = -0.50  # 50% stop loss
        self.max_holding_days = 365 * 2  # 2 years max hold
        
        # Strategy parameters
        self.min_asymmetry = 3.0  # Minimum 3:1 asymmetry for Fat Pitch
        self.min_market_cap = 1_000_000_000  # 1B SEK minimum
        
    async def get_rebalance_dates(self, start_date: date, end_date: date, 
                                 frequency_days: int = 30) -> List[date]:
        """Generate rebalancing dates"""
        dates = []
        current = start_date
        
        while current <= end_date:
            # Check if we have price data for this date
            price_check = await self.conn.fetchval("""
                SELECT COUNT(*) FROM daily_price_data 
                WHERE date = $1
            """, current)
            
            if price_check > 0:
                dates.append(current)
            
            current += timedelta(days=frequency_days)
        
        return dates
    
    async def get_universe_on_date(self, target_date: date) -> List[str]:
        """Get investable universe on a specific date"""
        query = """
            WITH recent_fundamentals AS (
                SELECT DISTINCT symbol
                FROM historical_fundamentals_daily
                WHERE date <= $1
                    AND date >= $1 - INTERVAL '90 days'
                    AND market_cap >= $2
            ),
            recent_prices AS (
                SELECT DISTINCT symbol
                FROM daily_price_data
                WHERE date <= $1
                    AND date >= $1 - INTERVAL '30 days'
                    AND close_price > 10  -- Minimum price filter
            )
            SELECT f.symbol
            FROM recent_fundamentals f
            INNER JOIN recent_prices p ON f.symbol = p.symbol
            ORDER BY f.symbol
        """
        
        rows = await self.conn.fetch(query, target_date, self.min_market_cap)
        return [row['symbol'] for row in rows]
    
    async def screen_opportunities(self, target_date: date, 
                                 universe: List[str]) -> List[Dict]:
        """Screen for value opportunities on a specific date"""
        opportunities = []
        
        for symbol in universe:
            try:
                # Get current price
                price_query = """
                    SELECT close_price FROM daily_price_data
                    WHERE symbol = $1 AND date <= $2
                    ORDER BY date DESC LIMIT 1
                """
                price_row = await self.conn.fetchrow(price_query, symbol, target_date)
                
                if not price_row:
                    continue
                
                current_price = float(price_row['close_price'])
                
                # Evaluate opportunity
                composite = await self.strategy.evaluate_opportunity(
                    symbol, datetime.combine(target_date, datetime.min.time()), current_price
                )
                
                if (composite.fat_pitch_price and composite.current_asymmetry and 
                    composite.method_count >= 2):
                    
                    opportunities.append({
                        'symbol': symbol,
                        'date': target_date,
                        'current_price': current_price,
                        'fat_pitch_price': composite.fat_pitch_price,
                        'fair_value': composite.fair_value,
                        'upside_target': composite.upside_target,
                        'downside_target': composite.downside_target,
                        'current_asymmetry': composite.current_asymmetry,
                        'method_count': composite.method_count,
                        'signal_type': self._classify_signal(current_price, composite)
                    })
                    
            except Exception as e:
                logger.debug(f"Error evaluating {symbol} on {target_date}: {e}")
                continue
        
        return opportunities
    
    def _classify_signal(self, current_price: float, composite) -> str:
        """Classify the type of signal"""
        if current_price < composite.fat_pitch_price and composite.current_asymmetry >= self.min_asymmetry:
            return 'fat_pitch_buy'
        elif current_price < composite.fair_value and composite.current_asymmetry >= 2.0:
            return 'undervalued_buy'
        elif composite.upside_target and current_price >= composite.upside_target:
            return 'target_hit'
        elif composite.overvalued_price and current_price > composite.overvalued_price:
            return 'overvalued'
        else:
            return 'neutral'
    
    async def execute_trades(self, opportunities: List[Dict], 
                           current_portfolio: Dict, target_date: date) -> Tuple[List[Trade], Dict]:
        """Execute trades based on opportunities"""
        new_trades = []
        portfolio = current_portfolio.copy()
        
        # Check exit conditions for existing positions
        exits_executed = await self._check_exit_conditions(portfolio, target_date)
        new_trades.extend(exits_executed)
        
        # Look for new entry opportunities
        buy_opportunities = [opp for opp in opportunities 
                           if opp['signal_type'] in ['fat_pitch_buy', 'undervalued_buy']]
        
        # Sort by asymmetry ratio (best opportunities first)
        buy_opportunities.sort(key=lambda x: x['current_asymmetry'], reverse=True)
        
        # Execute new positions if we have capital and room
        for opp in buy_opportunities:
            if len(portfolio['positions']) >= self.max_positions:
                break
            
            if opp['symbol'] in portfolio['positions']:
                continue  # Already holding
            
            # Calculate position size
            position_value = portfolio['cash'] * self.position_size
            shares = int(position_value / opp['current_price'])
            entry_cost = shares * opp['current_price']
            commission = entry_cost * self.commission_rate
            total_cost = entry_cost + commission
            
            if total_cost <= portfolio['cash'] and shares > 0:
                # Execute trade
                trade = Trade(
                    symbol=opp['symbol'],
                    entry_date=target_date,
                    exit_date=None,
                    entry_price=opp['current_price'],
                    exit_price=None,
                    shares=shares,
                    entry_reason=opp['signal_type'],
                    exit_reason=None,
                    fat_pitch_price=opp['fat_pitch_price'],
                    fair_value=opp['fair_value'],
                    upside_target=opp['upside_target'],
                    entry_asymmetry=opp['current_asymmetry'],
                    position_size=total_cost,
                    entry_commission=commission
                )
                
                # Update portfolio
                portfolio['cash'] -= total_cost
                portfolio['positions'][opp['symbol']] = trade
                new_trades.append(trade)
                
                logger.info(f"BUY: {opp['symbol']} @ {opp['current_price']:.2f} "
                          f"(asymmetry: {opp['current_asymmetry']:.1f}:1)")
        
        return new_trades, portfolio
    
    async def _check_exit_conditions(self, portfolio: Dict, target_date: date) -> List[Trade]:
        """Check exit conditions for existing positions"""
        exits = []
        
        for symbol, trade in list(portfolio['positions'].items()):
            # Get current price
            price_query = """
                SELECT close_price FROM daily_price_data
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 1
            """
            price_row = await self.conn.fetchrow(price_query, symbol, target_date)
            
            if not price_row:
                continue
            
            current_price = float(price_row['close_price'])
            holding_days = (target_date - trade.entry_date).days
            current_return = (current_price / trade.entry_price - 1)
            
            exit_reason = None
            
            # Check exit conditions
            if current_price >= trade.upside_target:
                exit_reason = 'target_hit'
            elif current_return <= self.stop_loss_pct:
                exit_reason = 'stop_loss'
            elif holding_days >= self.max_holding_days:
                exit_reason = 'time_limit'
            else:
                # Check if overvalued based on current fundamentals
                try:
                    composite = await self.strategy.evaluate_opportunity(
                        symbol, datetime.combine(target_date, datetime.min.time()), current_price
                    )
                    if (composite.overvalued_price and 
                        current_price > composite.overvalued_price):
                        exit_reason = 'overvalued'
                except:
                    pass
            
            if exit_reason:
                # Execute exit
                exit_value = trade.shares * current_price
                exit_commission = exit_value * self.commission_rate
                net_proceeds = exit_value - exit_commission
                
                # Update trade record
                trade.exit_date = target_date
                trade.exit_price = current_price
                trade.exit_reason = exit_reason
                trade.holding_days = holding_days
                trade.return_pct = current_return * 100
                trade.exit_commission = exit_commission
                trade.total_cost = trade.entry_commission + exit_commission
                
                # Update portfolio
                portfolio['cash'] += net_proceeds
                del portfolio['positions'][symbol]
                exits.append(trade)
                
                logger.info(f"SELL: {symbol} @ {current_price:.2f} "
                          f"(return: {current_return:.1%}, reason: {exit_reason})")
        
        return exits
    
    async def calculate_portfolio_value(self, portfolio: Dict, target_date: date) -> float:
        """Calculate total portfolio value"""
        total_value = portfolio['cash']
        
        for symbol, trade in portfolio['positions'].items():
            price_query = """
                SELECT close_price FROM daily_price_data
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 1
            """
            price_row = await self.conn.fetchrow(price_query, symbol, target_date)
            
            if price_row:
                current_price = float(price_row['close_price'])
                position_value = trade.shares * current_price
                total_value += position_value
        
        return total_value
    
    async def run_backtest(self, start_date: date, end_date: date,
                          rebalance_frequency: int = 30) -> Dict[str, Any]:
        """Run complete backtest"""
        
        logger.info(f"🧪 Starting Fundamental Value Backtest")
        logger.info(f"   Period: {start_date} to {end_date}")
        logger.info(f"   Initial Capital: {self.initial_capital:,.0f} SEK")
        logger.info(f"   Rebalance Frequency: {rebalance_frequency} days")
        logger.info(f"   Min Asymmetry: {self.min_asymmetry}:1")
        
        # Get rebalance dates
        rebalance_dates = await self.get_rebalance_dates(
            start_date, end_date, rebalance_frequency
        )
        
        if not rebalance_dates:
            return {'error': 'No valid rebalance dates found'}
        
        logger.info(f"   Rebalance dates: {len(rebalance_dates)}")
        
        # Initialize portfolio
        portfolio = {
            'cash': self.initial_capital,
            'positions': {}  # symbol -> Trade object
        }
        
        # Track results
        all_trades = []
        portfolio_history = []
        
        for i, target_date in enumerate(rebalance_dates):
            logger.info(f"📅 Rebalancing {i+1}/{len(rebalance_dates)}: {target_date}")
            
            # Get investable universe
            universe = await self.get_universe_on_date(target_date)
            logger.info(f"   Universe: {len(universe)} stocks")
            
            if not universe:
                continue
            
            # Screen for opportunities
            opportunities = await self.screen_opportunities(target_date, universe)
            logger.info(f"   Opportunities: {len(opportunities)}")
            
            # Execute trades
            new_trades, portfolio = await self.execute_trades(opportunities, portfolio, target_date)
            all_trades.extend(new_trades)
            
            # Calculate portfolio value
            total_value = await self.calculate_portfolio_value(portfolio, target_date)
            
            # Calculate daily return
            daily_return = 0.0
            if portfolio_history:
                prev_value = portfolio_history[-1].total_value
                daily_return = (total_value / prev_value - 1) if prev_value > 0 else 0.0
            
            # Record portfolio snapshot
            snapshot = PortfolioSnapshot(
                date=target_date,
                cash=portfolio['cash'],
                total_value=total_value,
                positions=len(portfolio['positions']),
                returns=daily_return,
                total_return=(total_value / self.initial_capital - 1)
            )
            
            portfolio_history.append(snapshot)
            
            logger.info(f"   Portfolio Value: {total_value:,.0f} SEK "
                       f"(Return: {snapshot.total_return:.1%})")
        
        # Calculate final metrics
        results = self._calculate_performance_metrics(portfolio_history, all_trades)
        
        return {
            'start_date': start_date,
            'end_date': end_date,
            'initial_capital': self.initial_capital,
            'final_capital': portfolio_history[-1].total_value if portfolio_history else self.initial_capital,
            'portfolio_history': portfolio_history,
            'all_trades': all_trades,
            'metrics': results
        }
    
    def _calculate_performance_metrics(self, portfolio_history: List[PortfolioSnapshot], 
                                     trades: List[Trade]) -> Dict[str, Any]:
        """Calculate comprehensive performance metrics"""
        
        if not portfolio_history:
            return {}
        
        # Returns analysis
        returns = [p.returns for p in portfolio_history[1:]]  # Skip first (no return)
        total_return = portfolio_history[-1].total_return
        
        # Annualized return
        days = (portfolio_history[-1].date - portfolio_history[0].date).days
        annualized_return = (1 + total_return) ** (365.25 / days) - 1 if days > 0 else 0
        
        # Volatility and Sharpe
        if len(returns) > 1:
            volatility = np.std(returns) * np.sqrt(252)  # Annualized
            sharpe_ratio = (annualized_return) / volatility if volatility > 0 else 0
        else:
            volatility = 0
            sharpe_ratio = 0
        
        # Drawdown analysis
        values = [p.total_value for p in portfolio_history]
        peak = values[0]
        max_drawdown = 0
        
        for value in values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
        
        # Trade analysis
        completed_trades = [t for t in trades if t.exit_date is not None]
        
        if completed_trades:
            trade_returns = [t.return_pct for t in completed_trades]
            win_rate = len([r for r in trade_returns if r > 0]) / len(trade_returns)
            avg_win = np.mean([r for r in trade_returns if r > 0]) if any(r > 0 for r in trade_returns) else 0
            avg_loss = np.mean([r for r in trade_returns if r <= 0]) if any(r <= 0 for r in trade_returns) else 0
            avg_holding_days = np.mean([t.holding_days for t in completed_trades])
        else:
            win_rate = 0
            avg_win = 0
            avg_loss = 0
            avg_holding_days = 0
        
        return {
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'total_trades': len(completed_trades),
            'win_rate': win_rate,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'avg_holding_days': avg_holding_days,
            'profit_factor': abs(avg_win * win_rate / (avg_loss * (1 - win_rate))) if avg_loss != 0 else float('inf')
        }


async def main():
    """Run backtest example"""
    
    # Connect to database
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        backtester = FundamentalValueBacktester(conn)
        
        # Define backtest period with available historical data
        end_date = date(2025, 6, 1)
        start_date = date(2022, 6, 1)  # 1 year backtest with historical fundamentals
        
        print("\n" + "="*80)
        print("FUNDAMENTAL VALUE STRATEGY BACKTEST")
        print("="*80)
        
        # Run backtest
        results = await backtester.run_backtest(
            start_date=start_date,
            end_date=end_date,
            rebalance_frequency=30  # Monthly rebalancing
        )
        
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
            return
        
        # Display results
        metrics = results['metrics']
        
        print(f"\n📊 BACKTEST RESULTS")
        print(f"{'='*50}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: {results['initial_capital']:,.0f} SEK")
        print(f"Final Capital: {results['final_capital']:,.0f} SEK")
        print()
        print(f"📈 PERFORMANCE METRICS")
        print(f"Total Return: {metrics['total_return']:.2%}")
        print(f"Annualized Return: {metrics['annualized_return']:.2%}")
        print(f"Volatility: {metrics['volatility']:.2%}")
        print(f"Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {metrics['max_drawdown']:.2%}")
        print()
        print(f"🎯 TRADING METRICS")
        print(f"Total Trades: {metrics['total_trades']}")
        print(f"Win Rate: {metrics['win_rate']:.1%}")
        print(f"Avg Win: {metrics['avg_win']:.1f}%")
        print(f"Avg Loss: {metrics['avg_loss']:.1f}%")
        print(f"Avg Holding: {metrics['avg_holding_days']:.0f} days")
        print(f"Profit Factor: {metrics['profit_factor']:.2f}")
        
        # Show some sample trades
        completed_trades = [t for t in results['all_trades'] if t.exit_date]
        
        if completed_trades:
            print(f"\n🔄 SAMPLE TRADES:")
            print(f"{'Symbol':<10} {'Entry':<12} {'Exit':<12} {'Days':<6} {'Return':<8} {'Reason':<12}")
            print("-" * 70)
            
            # Show best and worst trades
            best_trades = sorted(completed_trades, key=lambda x: x.return_pct, reverse=True)[:5]
            worst_trades = sorted(completed_trades, key=lambda x: x.return_pct)[:5]
            
            for trade in best_trades + worst_trades:
                print(f"{trade.symbol:<10} {trade.entry_date} {trade.exit_date} "
                      f"{trade.holding_days:<6} {trade.return_pct:>7.1f}% {trade.exit_reason:<12}")
        
        # Portfolio evolution
        if len(results['portfolio_history']) > 1:
            print(f"\n📈 PORTFOLIO EVOLUTION:")
            print(f"{'Date':<12} {'Value':<15} {'Return':<10} {'Positions':<10}")
            print("-" * 50)
            
            # Show quarterly snapshots
            history = results['portfolio_history']
            step = max(1, len(history) // 8)  # Show ~8 snapshots
            
            for i in range(0, len(history), step):
                p = history[i]
                print(f"{p.date} {p.total_value:>14,.0f} {p.total_return:>9.1%} {p.positions:>9}")
        
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())