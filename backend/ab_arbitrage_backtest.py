#!/usr/bin/env python3
"""
A/B Share Arbitrage Backtesting System

Backtests statistical arbitrage strategies on Nordic A/B share pairs
using mean reversion signals based on historical spread analysis.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import logging
from dataclasses import dataclass
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradeAction(Enum):
    BUY_A_SELL_B = "buy_a_sell_b"
    BUY_B_SELL_A = "buy_b_sell_a" 
    HOLD = "hold"
    CLOSE = "close"

@dataclass
class Trade:
    entry_date: datetime
    exit_date: Optional[datetime]
    company: str
    action: TradeAction
    entry_spread: float
    exit_spread: Optional[float]
    entry_a_price: float
    entry_b_price: float
    exit_a_price: Optional[float]
    exit_b_price: Optional[float]
    z_score_entry: float
    z_score_exit: Optional[float]
    shares_a: int
    shares_b: int
    investment_amount: float
    pnl: Optional[float] = None
    transaction_costs: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True

@dataclass
class BacktestConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0  # $100k
    max_positions: int = 5
    position_size: float = 0.15  # 15% per position
    transaction_cost_rate: float = 0.002  # 0.2% total (buy + sell)
    z_score_entry: float = 2.0  # Enter when |z-score| > 2
    z_score_exit: float = 0.5   # Exit when |z-score| < 0.5
    max_holding_days: int = 30  # Force exit after 30 days
    lookback_window: int = 90   # Days for spread calculation

class ABArbitrageBacktester:
    
    def __init__(self, config: BacktestConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[Trade] = []
        self.closed_trades: List[Trade] = []
        self.cash = config.initial_capital
        self.daily_portfolio_values = []
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_ab_pairs(self) -> List[str]:
        """Get list of companies with complete A/B pairs."""
        
        query = """
        SELECT 
            CASE 
                WHEN cm.primary_ticker ~ '-A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-A$')
                WHEN cm.primary_ticker ~ '-B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+)-B$')
                WHEN cm.primary_ticker ~ ' A$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) A$')
                WHEN cm.primary_ticker ~ ' B$' THEN SUBSTRING(cm.primary_ticker FROM '^(.+) B$')
            END as company_base,
            COUNT(DISTINCT cm.primary_ticker) as total_classes,
            COUNT(DISTINCT pd.symbol) as classes_with_data
        FROM company_master cm
        LEFT JOIN daily_price_data pd ON cm.primary_ticker = pd.symbol
        WHERE cm.primary_ticker ~ '-[AB]$' OR cm.primary_ticker ~ ' [AB]$'
        GROUP BY 1
        HAVING COUNT(DISTINCT cm.primary_ticker) = 2 
        AND COUNT(DISTINCT pd.symbol) = 2
        ORDER BY company_base
        """
        
        records = await self.db_conn.fetch(query)
        companies = [record['company_base'] for record in records]
        
        logger.info(f"📊 Found {len(companies)} complete A/B pairs for backtesting")
        return companies
        
    async def get_historical_prices(self, company: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get historical price data for A/B shares of a company."""
        
        # Build symbol patterns for this company - check what actually exists first
        symbols_to_check = [
            f"{company}-A", f"{company}-B",  # Dash format
            f"{company} A", f"{company} B"   # Space format  
        ]
        
        # Check which symbols actually exist in company_master
        symbol_check_query = """
        SELECT primary_ticker FROM company_master 
        WHERE primary_ticker = ANY($1)
        """
        
        existing_symbols = await self.db_conn.fetch(symbol_check_query, symbols_to_check)
        symbols = [record['primary_ticker'] for record in existing_symbols]
        
        if len(symbols) != 2:
            return pd.DataFrame()  # Need exactly 2 symbols (A and B)
        
        query = """
        SELECT 
            cm.primary_ticker as symbol,
            pd.date,
            pd.close_price,
            pd.volume
        FROM company_master cm
        JOIN daily_price_data pd ON cm.primary_ticker = pd.symbol
        WHERE cm.primary_ticker = ANY($1)
        AND pd.date BETWEEN $2 AND $3
        ORDER BY pd.date, cm.primary_ticker
        """
        
        records = await self.db_conn.fetch(query, symbols, start_date.date(), end_date.date())
        
        if not records:
            return pd.DataFrame()
            
        # Convert asyncpg.Record objects to dict first
        df = pd.DataFrame([dict(record) for record in records])
        df['date'] = pd.to_datetime(df['date'])
        df['close_price'] = df['close_price'].astype(float)
        
        # Pivot to get A and B prices in columns
        pivot_df = df.pivot_table(
            index='date', 
            columns='symbol', 
            values=['close_price', 'volume'],
            aggfunc='first'
        ).fillna(method='ffill')
        
        # Flatten column names
        pivot_df.columns = [f"{col[1]}_{col[0]}" for col in pivot_df.columns]
        
        # Find A and B columns
        a_price_col = None
        b_price_col = None
        a_volume_col = None 
        b_volume_col = None
        
        for col in pivot_df.columns:
            if 'close_price' in col and ('-A' in col or ' A' in col):
                a_price_col = col
                a_volume_col = col.replace('close_price', 'volume')
            elif 'close_price' in col and ('-B' in col or ' B' in col):
                b_price_col = col
                b_volume_col = col.replace('close_price', 'volume')
                
        if a_price_col is None or b_price_col is None:
            return pd.DataFrame()
            
        # Clean DataFrame
        result_df = pd.DataFrame({
            'date': pivot_df.index,
            'a_price': pivot_df[a_price_col],
            'b_price': pivot_df[b_price_col],
            'a_volume': pivot_df.get(a_volume_col, 0),
            'b_volume': pivot_df.get(b_volume_col, 0)
        }).dropna()
        
        if len(result_df) == 0:
            return pd.DataFrame()
            
        # Calculate spread and rolling statistics
        result_df['spread'] = (result_df['a_price'] - result_df['b_price']) / result_df['b_price'] * 100
        result_df['spread_mean'] = result_df['spread'].rolling(
            window=self.config.lookback_window, min_periods=30
        ).mean()
        result_df['spread_std'] = result_df['spread'].rolling(
            window=self.config.lookback_window, min_periods=30
        ).std()
        result_df['z_score'] = (result_df['spread'] - result_df['spread_mean']) / result_df['spread_std']
        
        return result_df.dropna()
        
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate trading signals based on z-score thresholds."""
        
        signals = []
        
        for _, row in data.iterrows():
            z_score = row['z_score']
            
            if abs(z_score) >= self.config.z_score_entry:
                if z_score > 0:
                    # Spread too high, buy B sell A
                    action = TradeAction.BUY_B_SELL_A
                else:
                    # Spread too low, buy A sell B  
                    action = TradeAction.BUY_A_SELL_B
            else:
                action = TradeAction.HOLD
                
            signals.append(action)
            
        data['signal'] = signals
        return data
        
    def calculate_position_size(self, a_price: float, b_price: float) -> Tuple[int, int, float]:
        """Calculate position sizes for pair trade."""
        
        available_capital = self.cash * self.config.position_size
        
        # For dollar-neutral strategy, invest equally in both sides
        investment_per_side = available_capital / 2
        
        # Calculate shares (rounded down to avoid over-investment)
        shares_a = int(investment_per_side / a_price)
        shares_b = int(investment_per_side / b_price)
        
        # Actual investment amount
        actual_investment = shares_a * a_price + shares_b * b_price
        
        return shares_a, shares_b, actual_investment
        
    def calculate_transaction_costs(self, investment_amount: float) -> float:
        """Calculate transaction costs."""
        return investment_amount * self.config.transaction_cost_rate
        
    def open_trade(self, company: str, date: datetime, action: TradeAction, 
                  a_price: float, b_price: float, z_score: float) -> Trade:
        """Open a new arbitrage trade."""
        
        shares_a, shares_b, investment = self.calculate_position_size(a_price, b_price)
        transaction_costs = self.calculate_transaction_costs(investment)
        
        trade = Trade(
            entry_date=date,
            exit_date=None,
            company=company,
            action=action,
            entry_spread=(a_price - b_price) / b_price * 100,
            exit_spread=None,
            entry_a_price=a_price,
            entry_b_price=b_price,
            exit_a_price=None,
            exit_b_price=None,
            z_score_entry=z_score,
            z_score_exit=None,
            shares_a=shares_a,
            shares_b=shares_b,
            investment_amount=investment,
            transaction_costs=transaction_costs
        )
        
        # Update cash
        self.cash -= investment + transaction_costs
        
        return trade
        
    def close_trade(self, trade: Trade, date: datetime, a_price: float, 
                   b_price: float, z_score: float, reason: str = "signal") -> float:
        """Close an existing trade and calculate P&L."""
        
        trade.exit_date = date
        trade.exit_a_price = a_price
        trade.exit_b_price = b_price
        trade.z_score_exit = z_score
        trade.exit_spread = (a_price - b_price) / b_price * 100
        trade.holding_days = (date - trade.entry_date).days
        trade.is_open = False
        
        # Calculate P&L based on trade direction
        if trade.action == TradeAction.BUY_A_SELL_B:
            # We bought A and sold B, so we gain when A outperforms B
            a_return = (a_price - trade.entry_a_price) / trade.entry_a_price
            b_return = (b_price - trade.entry_b_price) / trade.entry_b_price
            # P&L = long A performance - short B performance  
            pnl_gross = (a_return * trade.shares_a * trade.entry_a_price) - \
                       (b_return * trade.shares_b * trade.entry_b_price)
        else:  # BUY_B_SELL_A
            # We bought B and sold A, so we gain when B outperforms A
            a_return = (a_price - trade.entry_a_price) / trade.entry_a_price
            b_return = (b_price - trade.entry_b_price) / trade.entry_b_price
            # P&L = long B performance - short A performance
            pnl_gross = (b_return * trade.shares_b * trade.entry_b_price) - \
                       (a_return * trade.shares_a * trade.entry_a_price)
        
        # Exit transaction costs
        exit_costs = self.calculate_transaction_costs(trade.investment_amount)
        trade.pnl = pnl_gross - exit_costs
        
        # Return capital plus P&L
        self.cash += trade.investment_amount + trade.pnl
        
        logger.info(f"   📤 Closed {trade.company} {reason}: {trade.pnl:+.2f} ({trade.holding_days}d)")
        
        return trade.pnl
        
    def should_close_trade(self, trade: Trade, z_score: float, days_held: int) -> Tuple[bool, str]:
        """Determine if a trade should be closed."""
        
        # Exit if z-score has normalized
        if abs(z_score) <= self.config.z_score_exit:
            return True, "z_score_normalized"
            
        # Exit if maximum holding period reached
        if days_held >= self.config.max_holding_days:
            return True, "max_holding_period"
            
        return False, ""
        
    async def run_backtest(self) -> Dict:
        """Run the complete backtest."""
        
        logger.info(f"🚀 Starting A/B Arbitrage Backtest")
        logger.info(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        logger.info(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        logger.info(f"   📊 Max positions: {self.config.max_positions}")
        
        # Get A/B pairs
        companies = await self.get_ab_pairs()
        
        if not companies:
            logger.error("❌ No complete A/B pairs found")
            return {}
            
        # Load historical data for all companies
        all_data = {}
        for company in companies:
            data = await self.get_historical_prices(
                company, 
                self.config.start_date - timedelta(days=self.config.lookback_window), 
                self.config.end_date
            )
            
            if len(data) > 0:
                all_data[company] = self.generate_signals(data)
                logger.info(f"   📈 {company}: {len(data)} trading days")
        
        if not all_data:
            logger.error("❌ No price data found for any companies")
            return {}
            
        logger.info(f"📊 Backtesting {len(all_data)} A/B pairs...")
        
        # Get all unique dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation loop
        for i, current_date in enumerate(all_dates):
            
            if i % 50 == 0:
                logger.info(f"   📅 Processing {current_date.date()} ({i+1}/{len(all_dates)})")
            
            # Check exit conditions for open trades
            trades_to_close = []
            for trade in self.open_trades:
                # Find current prices for this company
                if trade.company not in all_data:
                    continue
                    
                company_data = all_data[trade.company]
                current_row = company_data[company_data['date'] == current_date]
                
                if current_row.empty:
                    continue
                    
                current_row = current_row.iloc[0]
                days_held = (current_date - trade.entry_date).days
                
                should_close, reason = self.should_close_trade(
                    trade, current_row['z_score'], days_held
                )
                
                if should_close:
                    self.close_trade(
                        trade, current_date, 
                        current_row['a_price'], current_row['b_price'],
                        current_row['z_score'], reason
                    )
                    trades_to_close.append(trade)
            
            # Move closed trades
            for trade in trades_to_close:
                self.open_trades.remove(trade)
                self.closed_trades.append(trade)
            
            # Look for new entry opportunities
            if len(self.open_trades) < self.config.max_positions:
                for company, data in all_data.items():
                    # Skip if already have position in this company
                    if any(t.company == company for t in self.open_trades):
                        continue
                        
                    current_row = data[data['date'] == current_date]
                    if current_row.empty:
                        continue
                        
                    current_row = current_row.iloc[0]
                    signal = current_row['signal']
                    
                    if signal != TradeAction.HOLD:
                        # Check if we have enough cash
                        shares_a, shares_b, investment = self.calculate_position_size(
                            current_row['a_price'], current_row['b_price']
                        )
                        
                        total_cost = investment + self.calculate_transaction_costs(investment)
                        
                        if self.cash >= total_cost and len(self.open_trades) < self.config.max_positions:
                            trade = self.open_trade(
                                company, current_date, signal,
                                current_row['a_price'], current_row['b_price'],
                                current_row['z_score']
                            )
                            self.open_trades.append(trade)
                            logger.info(f"   📥 Opened {company} {signal.value}: Z={current_row['z_score']:.2f}")
            
            # Record daily portfolio value
            portfolio_value = self.cash
            for trade in self.open_trades:
                if trade.company in all_data:
                    company_data = all_data[trade.company]
                    current_row = company_data[company_data['date'] == current_date]
                    if not current_row.empty:
                        current_row = current_row.iloc[0]
                        portfolio_value += trade.shares_a * current_row['a_price'] + \
                                         trade.shares_b * current_row['b_price']
                        
            self.daily_portfolio_values.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'open_positions': len(self.open_trades)
            })
        
        # Close any remaining open trades
        final_date = all_dates[-1] if all_dates else self.config.end_date
        for trade in self.open_trades[:]:
            if trade.company in all_data:
                company_data = all_data[trade.company]
                final_row = company_data[company_data['date'] == final_date]
                if not final_row.empty:
                    final_row = final_row.iloc[0]
                    self.close_trade(
                        trade, final_date,
                        final_row['a_price'], final_row['b_price'],
                        final_row['z_score'], "backtest_end"
                    )
                    self.closed_trades.append(trade)
        
        self.open_trades = []
        
        return self.analyze_results()
        
    def analyze_results(self) -> Dict:
        """Analyze backtest results."""
        
        if not self.closed_trades:
            return {
                'error': 'No completed trades found',
                'total_trades': 0
            }
        
        # Calculate metrics
        total_trades = len(self.closed_trades)
        winning_trades = [t for t in self.closed_trades if t.pnl > 0]
        losing_trades = [t for t in self.closed_trades if t.pnl <= 0]
        
        total_pnl = sum(t.pnl for t in self.closed_trades)
        win_rate = len(winning_trades) / total_trades * 100
        
        avg_win = np.mean([t.pnl for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t.pnl for t in losing_trades]) if losing_trades else 0
        
        avg_holding_days = np.mean([t.holding_days for t in self.closed_trades])
        
        # Portfolio performance
        if self.daily_portfolio_values:
            portfolio_df = pd.DataFrame(self.daily_portfolio_values)
            initial_value = portfolio_df['portfolio_value'].iloc[0]
            final_value = portfolio_df['portfolio_value'].iloc[-1]
            total_return = (final_value - initial_value) / initial_value * 100
            
            portfolio_df['daily_return'] = portfolio_df['portfolio_value'].pct_change()
            volatility = portfolio_df['daily_return'].std() * np.sqrt(252) * 100
            sharpe_ratio = (total_return / 100) / (volatility / 100) if volatility > 0 else 0
            
            max_drawdown = 0
            peak = portfolio_df['portfolio_value'].iloc[0]
            for value in portfolio_df['portfolio_value']:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak * 100
                max_drawdown = max(max_drawdown, drawdown)
        else:
            total_return = volatility = sharpe_ratio = max_drawdown = 0
            final_value = self.config.initial_capital
        
        return {
            'total_trades': total_trades,
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'avg_holding_days': avg_holding_days,
            'total_return': total_return,
            'final_portfolio_value': final_value,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'profit_factor': abs(avg_win * len(winning_trades) / (avg_loss * len(losing_trades))) if losing_trades and avg_loss != 0 else float('inf')
        }
        
    def print_results(self, results: Dict):
        """Print formatted backtest results."""
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
            
        print(f"\n" + "="*80)
        print(f"🎯 A/B SHARE ARBITRAGE BACKTEST RESULTS")
        print(f"="*80)
        
        print(f"\n📊 TRADING STATISTICS:")
        print(f"   Total trades:              {results['total_trades']:,}")
        print(f"   Winning trades:            {results['winning_trades']:,} ({results['win_rate']:.1f}%)")
        print(f"   Losing trades:             {results['losing_trades']:,}")
        print(f"   Average holding period:    {results['avg_holding_days']:.1f} days")
        
        print(f"\n💰 P&L ANALYSIS:")
        print(f"   Total P&L:                 ${results['total_pnl']:,.2f}")
        print(f"   Average winning trade:     ${results['avg_win']:,.2f}")
        print(f"   Average losing trade:      ${results['avg_loss']:,.2f}")
        print(f"   Profit factor:             {results['profit_factor']:.2f}")
        
        print(f"\n📈 PORTFOLIO PERFORMANCE:")
        print(f"   Initial capital:           ${self.config.initial_capital:,.2f}")
        print(f"   Final portfolio value:     ${results['final_portfolio_value']:,.2f}")
        print(f"   Total return:              {results['total_return']:+.2f}%")
        print(f"   Annualized volatility:     {results['volatility']:.2f}%")
        print(f"   Sharpe ratio:              {results['sharpe_ratio']:.2f}")
        print(f"   Maximum drawdown:          {results['max_drawdown']:.2f}%")
        
        print(f"\n🎯 TRADE BREAKDOWN BY COMPANY:")
        company_stats = {}
        for trade in self.closed_trades:
            if trade.company not in company_stats:
                company_stats[trade.company] = {'trades': 0, 'pnl': 0, 'wins': 0}
            company_stats[trade.company]['trades'] += 1
            company_stats[trade.company]['pnl'] += trade.pnl
            if trade.pnl > 0:
                company_stats[trade.company]['wins'] += 1
        
        for company, stats in sorted(company_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
            win_rate = stats['wins'] / stats['trades'] * 100 if stats['trades'] > 0 else 0
            print(f"   {company:<12}: {stats['trades']:2d} trades, ${stats['pnl']:+7.0f} P&L, {win_rate:4.0f}% wins")
            
        print(f"\n" + "="*80)
        
        # Add detailed trade list
        self.print_detailed_trade_list()
        
    def print_detailed_trade_list(self):
        """Print detailed list of all trades for analysis."""
        
        if not self.closed_trades:
            return
            
        print(f"\n" + "="*120)
        print(f"📋 DETAILED TRADE LIST ({len(self.closed_trades)} trades)")
        print(f"="*120)
        
        # Sort trades by entry date
        sorted_trades = sorted(self.closed_trades, key=lambda x: x.entry_date)
        
        print(f"{'#':<3} {'Company':<8} {'Entry':<10} {'Exit':<10} {'Days':<4} {'Action':<12} {'Entry Z':<8} {'Exit Z':<8} {'P&L':<10} {'Return':<7} {'Reason':<12}")
        print(f"{'-'*120}")
        
        for i, trade in enumerate(sorted_trades, 1):
            # Calculate return percentage
            return_pct = (trade.pnl / trade.investment_amount) * 100 if trade.investment_amount > 0 else 0
            
            # Format action
            action_str = "Buy A" if trade.action == TradeAction.BUY_A_SELL_B else "Buy B"
            
            # Determine exit reason
            exit_reason = "timeout" if trade.holding_days >= self.config.max_holding_days else "z_norm"
            
            # Color coding for P&L
            pnl_str = f"${trade.pnl:+,.0f}"
            if trade.pnl > 0:
                pnl_color = pnl_str  # Green in terminal
            else:
                pnl_color = pnl_str  # Red in terminal
                
            print(f"{i:<3} {trade.company:<8} {trade.entry_date.strftime('%Y-%m-%d'):<10} "
                  f"{trade.exit_date.strftime('%Y-%m-%d'):<10} {trade.holding_days:<4} {action_str:<12} "
                  f"{trade.z_score_entry:+.2f}{'':1} {trade.z_score_exit:+.2f}{'':1} "
                  f"{pnl_color:<10} {return_pct:+.1f}%{'':1} {exit_reason:<12}")
        
        # Summary statistics by action type
        print(f"\n📊 TRADE ANALYSIS BY ACTION:")
        buy_a_trades = [t for t in self.closed_trades if t.action == TradeAction.BUY_A_SELL_B]
        buy_b_trades = [t for t in self.closed_trades if t.action == TradeAction.BUY_B_SELL_A]
        
        if buy_a_trades:
            buy_a_pnl = sum(t.pnl for t in buy_a_trades)
            buy_a_wins = len([t for t in buy_a_trades if t.pnl > 0])
            buy_a_win_rate = buy_a_wins / len(buy_a_trades) * 100
            print(f"   Buy A (sell B): {len(buy_a_trades):3d} trades, ${buy_a_pnl:+8,.0f} P&L, {buy_a_win_rate:5.1f}% wins")
        
        if buy_b_trades:
            buy_b_pnl = sum(t.pnl for t in buy_b_trades)
            buy_b_wins = len([t for t in buy_b_trades if t.pnl > 0])
            buy_b_win_rate = buy_b_wins / len(buy_b_trades) * 100
            print(f"   Buy B (sell A): {len(buy_b_trades):3d} trades, ${buy_b_pnl:+8,.0f} P&L, {buy_b_win_rate:5.1f}% wins")
        
        # Summary by holding period
        print(f"\n📊 TRADE ANALYSIS BY HOLDING PERIOD:")
        quick_trades = [t for t in self.closed_trades if t.holding_days <= 7]
        medium_trades = [t for t in self.closed_trades if 7 < t.holding_days <= 21]
        long_trades = [t for t in self.closed_trades if t.holding_days > 21]
        
        for trades, label in [(quick_trades, "≤7 days"), (medium_trades, "8-21 days"), (long_trades, ">21 days")]:
            if trades:
                pnl = sum(t.pnl for t in trades)
                wins = len([t for t in trades if t.pnl > 0])
                win_rate = wins / len(trades) * 100
                avg_return = np.mean([(t.pnl / t.investment_amount) * 100 for t in trades])
                print(f"   {label:<10}: {len(trades):3d} trades, ${pnl:+8,.0f} P&L, {win_rate:5.1f}% wins, {avg_return:+5.1f}% avg return")
        
        # Best and worst trades
        print(f"\n🏆 BEST TRADES (Top 5):")
        best_trades = sorted(self.closed_trades, key=lambda x: x.pnl, reverse=True)[:5]
        for trade in best_trades:
            return_pct = (trade.pnl / trade.investment_amount) * 100
            action_str = "Buy A" if trade.action == TradeAction.BUY_A_SELL_B else "Buy B"
            print(f"   {trade.company:<8} {trade.entry_date.strftime('%Y-%m-%d')} {action_str:<8} "
                  f"{trade.holding_days:2d}d ${trade.pnl:+8,.0f} ({return_pct:+5.1f}%)")
        
        print(f"\n📉 WORST TRADES (Bottom 5):")
        worst_trades = sorted(self.closed_trades, key=lambda x: x.pnl)[:5]
        for trade in worst_trades:
            return_pct = (trade.pnl / trade.investment_amount) * 100
            action_str = "Buy A" if trade.action == TradeAction.BUY_A_SELL_B else "Buy B"
            print(f"   {trade.company:<8} {trade.entry_date.strftime('%Y-%m-%d')} {action_str:<8} "
                  f"{trade.holding_days:2d}d ${trade.pnl:+8,.0f} ({return_pct:+5.1f}%)")
        
        print(f"\n" + "="*120)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run A/B arbitrage backtest with different configurations."""
    
    # Default configuration
    config = BacktestConfig(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2024, 12, 1),
        initial_capital=100000.0,
        max_positions=5,
        position_size=0.15,  # 15% per position
        transaction_cost_rate=0.002,  # 0.2% total
        z_score_entry=2.0,
        z_score_exit=0.5,
        max_holding_days=30,
        lookback_window=90
    )
    
    backtester = ABArbitrageBacktester(config)
    
    try:
        await backtester.setup()
        results = await backtester.run_backtest()
        backtester.print_results(results)
        
        # Test different configurations
        print(f"\n🧪 TESTING DIFFERENT PARAMETERS:")
        
        test_configs = [
            ("Conservative (Z=2.5)", {'z_score_entry': 2.5, 'z_score_exit': 0.3}),
            ("Aggressive (Z=1.5)", {'z_score_entry': 1.5, 'z_score_exit': 0.7}),
            ("Quick Exit (15 days)", {'max_holding_days': 15}),
            ("Patient (60 days)", {'max_holding_days': 60}),
            ("Lower Costs (0.1%)", {'transaction_cost_rate': 0.001}),
        ]
        
        for name, params in test_configs:
            print(f"\n📊 {name}:")
            test_config = BacktestConfig(
                start_date=datetime(2023, 1, 1),
                end_date=datetime(2024, 12, 1),
                initial_capital=100000.0,
                **params
            )
            
            test_backtester = ABArbitrageBacktester(test_config)
            await test_backtester.setup()
            test_results = await test_backtester.run_backtest()
            
            if 'total_return' in test_results:
                print(f"   Return: {test_results['total_return']:+.1f}% | "
                      f"Sharpe: {test_results['sharpe_ratio']:.2f} | "
                      f"Trades: {test_results['total_trades']} | "
                      f"Win Rate: {test_results['win_rate']:.1f}%")
                
            await test_backtester.cleanup()
        
    except Exception as e:
        logger.error(f"Error during backtest: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backtester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())