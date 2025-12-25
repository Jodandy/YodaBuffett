#!/usr/bin/env python3
"""
A/B Share Pair Trading Arbitrage Backtesting System

True pair trading version with long/short positions and all the realistic execution
features we've developed: momentum-based signals, signal-driven stop losses,
OHLC-based execution, proper portfolio accounting, etc.
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
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PairAction(Enum):
    BUY_A_SELL_B = "buy_a_sell_b"
    BUY_B_SELL_A = "buy_b_sell_a" 
    HOLD = "hold"
    CLOSE = "close"

@dataclass
class PairTrade:
    signal_date: datetime  # When signal was generated
    entry_date: Optional[datetime]  # When trade actually executed
    exit_date: Optional[datetime]
    company: str
    action: PairAction
    entry_spread: float
    exit_spread: Optional[float]
    # A share details
    entry_a_price: float
    exit_a_price: Optional[float]
    shares_a: int
    # B share details  
    entry_b_price: float
    exit_b_price: Optional[float]
    shares_b: int
    # Trade metrics
    z_score_entry: float
    z_score_exit: Optional[float]
    investment_amount: float
    pnl: Optional[float] = None
    transaction_costs: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True
    # Exit order management
    highest_spread: Optional[float] = None  # For trailing stops on spread
    trailing_stop_spread: Optional[float] = None
    hard_stop_spread: Optional[float] = None
    limit_close_spread: Optional[float] = None
    exit_reason: Optional[str] = None

@dataclass
class PairTradingConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    max_positions: int = 5  # Fewer since each pair uses 2x capital
    position_size: float = 0.15  # 15% per side of pair
    transaction_cost_rate: float = 0.002  # 0.2% per transaction
    z_score_entry: float = 2.0  # Enter when |z-score| > 2
    z_score_exit: float = 0.5   # Exit when |z-score| < 0.5
    max_holding_days: int = 30  # Force exit after 30 days
    lookback_window: int = 90   # Days for spread calculation
    # Exit order settings (for spread convergence)
    trailing_stop_pct: float = 0.03  # 3% trailing stop on spread
    stop_loss_pct: float = 0.10  # 10% hard stop loss on spread
    limit_close_buffer: float = 0.01  # 1% buffer for limit close

class ABPairTradingBacktester:
    
    def __init__(self, config: PairTradingConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[PairTrade] = []
        self.closed_trades: List[PairTrade] = []
        self.pending_orders: Dict[str, PairTrade] = {}
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
                WHEN symbol ~ ' A$' THEN SUBSTRING(symbol FROM '^(.+) A$')
                WHEN symbol ~ ' B$' THEN SUBSTRING(symbol FROM '^(.+) B$')
            END as company_base,
            COUNT(DISTINCT symbol) as classes_with_data
        FROM daily_price_data 
        WHERE symbol ~ ' [AB]$'
        AND date >= $1
        GROUP BY 1
        HAVING COUNT(DISTINCT symbol) = 2
        ORDER BY company_base
        """
        
        min_date = self.config.start_date - timedelta(days=30)
        records = await self.db_conn.fetch(query, min_date.date())
        companies = [record['company_base'] for record in records]
        
        print(f"📊 Found {len(companies)} complete A/B pairs for pair trading")
        return companies
        
    async def get_historical_prices(self, company: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get historical price data for A/B shares of a company."""
        
        symbols = [f"{company} A", f"{company} B"]
        
        query = """
        SELECT 
            symbol,
            date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM daily_price_data
        WHERE symbol = ANY($1)
        AND date BETWEEN $2 AND $3
        ORDER BY date, symbol
        """
        
        records = await self.db_conn.fetch(query, symbols, start_date.date(), end_date.date())
        
        if not records:
            return pd.DataFrame()
            
        # Convert to DataFrame and pivot
        df = pd.DataFrame([dict(record) for record in records])
        df['date'] = pd.to_datetime(df['date'])
        
        # Convert price columns to float
        price_cols = ['open_price', 'high_price', 'low_price', 'close_price']
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        # Pivot to get A and B prices in columns
        pivot_df = df.pivot_table(
            index='date', 
            columns='symbol', 
            values=['open_price', 'high_price', 'low_price', 'close_price', 'volume'],
            aggfunc='first'
        ).ffill()
        
        # Flatten column names
        pivot_df.columns = [f"{col[1]}_{col[0]}" for col in pivot_df.columns]
        
        # Find A and B columns
        a_cols = {}
        b_cols = {}
        
        for col in pivot_df.columns:
            if ' A_' in col:
                price_type = '_'.join(col.split('_')[1:])
                a_cols[price_type] = col
            elif ' B_' in col:
                price_type = '_'.join(col.split('_')[1:])
                b_cols[price_type] = col
                
        if 'close_price' not in a_cols or 'close_price' not in b_cols:
            return pd.DataFrame()
            
        # Clean DataFrame with OHLC data
        result_df = pd.DataFrame({
            'date': pivot_df.index,
            'a_open': pivot_df.get(a_cols.get('open_price'), pivot_df[a_cols['close_price']]),
            'a_high': pivot_df.get(a_cols.get('high_price'), pivot_df[a_cols['close_price']]),
            'a_low': pivot_df.get(a_cols.get('low_price'), pivot_df[a_cols['close_price']]),
            'a_price': pivot_df[a_cols['close_price']],
            'b_open': pivot_df.get(b_cols.get('open_price'), pivot_df[b_cols['close_price']]),
            'b_high': pivot_df.get(b_cols.get('high_price'), pivot_df[b_cols['close_price']]),
            'b_low': pivot_df.get(b_cols.get('low_price'), pivot_df[b_cols['close_price']]),
            'b_price': pivot_df[b_cols['close_price']]
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
        
        # Add momentum indicators for signal filtering
        result_df['a_return_5d'] = result_df['a_price'].pct_change(5)
        result_df['b_return_5d'] = result_df['b_price'].pct_change(5)
        result_df['a_momentum_positive'] = result_df['a_return_5d'] > 0
        result_df['b_momentum_positive'] = result_df['b_return_5d'] > 0
        
        return result_df.dropna()
        
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate pair trading signals based on momentum-filtered z-score."""
        
        signals = []
        
        for _, row in data.iterrows():
            z_score = row['z_score']
            a_momentum_positive = row.get('a_momentum_positive', True)
            b_momentum_positive = row.get('b_momentum_positive', True)
            
            # MOMENTUM-BASED PAIR TRADING: Only trade when momentum supports mean reversion
            if z_score >= self.config.z_score_entry:
                # A expensive relative to B - check WHY
                if a_momentum_positive and not b_momentum_positive:
                    # A up, B down - risky divergence
                    action = PairAction.HOLD
                elif a_momentum_positive:
                    # A outperformed - expect reversion: SELL A, BUY B
                    action = PairAction.BUY_B_SELL_A
                else:
                    action = PairAction.HOLD
                    
            elif z_score <= -self.config.z_score_entry:
                # B expensive relative to A - check WHY
                if b_momentum_positive and not a_momentum_positive:
                    # B up, A down - risky divergence
                    action = PairAction.HOLD
                elif b_momentum_positive:
                    # B outperformed - expect reversion: BUY A, SELL B
                    action = PairAction.BUY_A_SELL_B
                else:
                    action = PairAction.HOLD
            else:
                action = PairAction.HOLD
                
            signals.append(action)
            
        data['signal'] = signals
        return data
        
    def calculate_pair_position_sizes(self, a_price: float, b_price: float) -> Tuple[int, int, float]:
        """Calculate position sizes for dollar-neutral pair trade."""
        
        available_capital = self.cash * self.config.position_size
        investment_per_side = available_capital / 2  # Split between long and short sides
        
        # Calculate shares (rounded down)
        shares_a = int(investment_per_side / a_price)
        shares_b = int(investment_per_side / b_price)
        
        # Actual investment (we need capital for the long side only in practice)
        actual_investment = shares_a * a_price + shares_b * b_price
        
        return shares_a, shares_b, actual_investment
        
    def calculate_transaction_costs(self, investment_amount: float) -> float:
        """Calculate transaction costs for pair trade (4 transactions total)."""
        return investment_amount * self.config.transaction_cost_rate
        
    def open_trade(self, company: str, signal_date: datetime, action: PairAction,
                  a_price: float, b_price: float, z_score: float) -> PairTrade:
        """Create a pending pair trade."""
        
        shares_a, shares_b, investment = self.calculate_pair_position_sizes(a_price, b_price)
        transaction_costs = self.calculate_transaction_costs(investment)
        
        trade = PairTrade(
            signal_date=signal_date,
            entry_date=None,
            exit_date=None,
            company=company,
            action=action,
            entry_spread=(a_price - b_price) / b_price * 100,
            exit_spread=None,
            entry_a_price=a_price,
            exit_a_price=None,
            shares_a=shares_a,
            entry_b_price=b_price,
            exit_b_price=None,
            shares_b=shares_b,
            z_score_entry=z_score,
            z_score_exit=None,
            investment_amount=investment,
            transaction_costs=transaction_costs
        )
        
        # Reserve capital
        self.cash -= investment + transaction_costs
        
        return trade
        
    def initialize_stop_losses(self, trade: PairTrade) -> None:
        """Initialize spread-based stop losses (not activated until exit signal)."""
        trade.highest_spread = abs(trade.entry_spread)
        trade.hard_stop_spread = None
        trade.trailing_stop_spread = None
        
    def activate_stop_losses(self, trade: PairTrade, current_spread: float) -> None:
        """Activate stop losses when we get an exit signal."""
        if trade.hard_stop_spread is None:
            spread_range = abs(current_spread)
            trade.hard_stop_spread = spread_range * (1 - self.config.stop_loss_pct)
            trade.trailing_stop_spread = spread_range * (1 - self.config.trailing_stop_pct)
            trade.highest_spread = spread_range
            
    def update_trailing_stop(self, trade: PairTrade, current_spread: float) -> None:
        """Update trailing stop based on favorable spread movement."""
        spread_range = abs(current_spread)
        if spread_range > trade.highest_spread:
            trade.highest_spread = spread_range
            new_trailing = spread_range * (1 - self.config.trailing_stop_pct)
            if new_trailing > trade.trailing_stop_spread:
                trade.trailing_stop_spread = new_trailing
    
    def can_execute_limit_order(self, trade: PairTrade, current_data: pd.Series) -> bool:
        """Check if pair trade limit order would execute."""
        z_score = current_data.get('z_score', 0)
        
        if trade.action == PairAction.BUY_A_SELL_B:
            return z_score <= -self.config.z_score_entry
        elif trade.action == PairAction.BUY_B_SELL_A:
            return z_score >= self.config.z_score_entry
        return False
        
    def check_stop_loss_execution(self, trade: PairTrade, current_data: pd.Series) -> Tuple[bool, str, Optional[float], Optional[float]]:
        """Check if spread-based stop losses should execute."""
        if trade.hard_stop_spread is None:
            return False, "", None, None
            
        current_spread = current_data.get('spread', 0)
        current_spread_range = abs(current_spread)
        
        # Update trailing stop
        self.update_trailing_stop(trade, current_spread)
        
        # Check stops (spread not converging as expected)
        if current_spread_range <= trade.hard_stop_spread:
            return True, "hard_stop_loss", current_data['a_price'], current_data['b_price']
        
        if current_spread_range <= trade.trailing_stop_spread:
            return True, "trailing_stop", current_data['a_price'], current_data['b_price']
        
        return False, "", None, None
        
    def check_limit_close_execution(self, trade: PairTrade, current_data: pd.Series) -> Tuple[bool, Optional[float], Optional[float]]:
        """Check if limit close order executes based on z-score normalization."""
        z_score = current_data.get('z_score', 0)
        
        # Exit signal: z-score has normalized
        if abs(z_score) <= self.config.z_score_exit:
            # Activate stop losses for downside protection
            current_spread = current_data.get('spread', 0)
            self.activate_stop_losses(trade, current_spread)
            
            # Execute close at current prices
            return True, current_data['a_price'], current_data['b_price']
        
        return False, None, None
        
    def should_close_trade(self, trade: PairTrade, current_data: pd.Series, days_held: int) -> Tuple[bool, str, Optional[float], Optional[float]]:
        """Determine if a pair trade should be closed."""
        
        # Check stop losses first
        stop_triggered, stop_reason, a_price, b_price = self.check_stop_loss_execution(trade, current_data)
        if stop_triggered:
            return True, stop_reason, a_price, b_price
        
        # Check z-score normalization
        close_triggered, a_price, b_price = self.check_limit_close_execution(trade, current_data)
        if close_triggered:
            return True, "z_score_normalized", a_price, b_price
        
        # Force exit on max holding period
        if days_held >= self.config.max_holding_days:
            return True, "max_holding_period", current_data['a_price'], current_data['b_price']
        
        return False, "", None, None
        
    def close_trade(self, trade: PairTrade, date: datetime, a_price: float, b_price: float, 
                   z_score: float, reason: str = "signal", 
                   exit_a_price: Optional[float] = None, exit_b_price: Optional[float] = None) -> float:
        """Close a pair trade and calculate P&L."""
        
        trade.exit_date = date
        trade.z_score_exit = z_score
        trade.exit_spread = (a_price - b_price) / b_price * 100
        trade.holding_days = (date - trade.entry_date).days if trade.entry_date else 0
        trade.is_open = False
        trade.exit_reason = reason
        
        # Use provided exit prices or fall back to current prices
        trade.exit_a_price = exit_a_price if exit_a_price is not None else a_price
        trade.exit_b_price = exit_b_price if exit_b_price is not None else b_price
        
        # Calculate P&L based on pair trade direction
        if trade.action == PairAction.BUY_A_SELL_B:
            # We bought A and sold B, so gain when A outperforms B
            a_return = (trade.exit_a_price - trade.entry_a_price) / trade.entry_a_price
            b_return = (trade.exit_b_price - trade.entry_b_price) / trade.entry_b_price
            gross_pnl = (a_return * trade.shares_a * trade.entry_a_price) - \
                       (b_return * trade.shares_b * trade.entry_b_price)
        else:  # BUY_B_SELL_A
            # We bought B and sold A, so gain when B outperforms A
            a_return = (trade.exit_a_price - trade.entry_a_price) / trade.entry_a_price
            b_return = (trade.exit_b_price - trade.entry_b_price) / trade.entry_b_price
            gross_pnl = (b_return * trade.shares_b * trade.entry_b_price) - \
                       (a_return * trade.shares_a * trade.entry_a_price)
        
        # Exit transaction costs
        exit_costs = self.calculate_transaction_costs(trade.investment_amount)
        trade.pnl = gross_pnl - exit_costs
        
        # Return invested capital plus P&L
        self.cash += trade.investment_amount + trade.pnl
        
        return trade.pnl
        
    async def run_backtest(self) -> Dict:
        """Run the complete pair trading backtest."""
        
        print(f"🚀 Starting A/B Pair Trading Arbitrage Backtest")
        print(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        print(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        print(f"   📊 Max positions: {self.config.max_positions}")
        
        # Get A/B pairs
        companies = await self.get_ab_pairs()
        
        if not companies:
            logger.error("❌ No complete A/B pairs found")
            return {}
            
        # Load historical data
        all_data = {}
        for company in companies:
            data = await self.get_historical_prices(
                company, 
                self.config.start_date - timedelta(days=self.config.lookback_window), 
                self.config.end_date
            )
            
            if len(data) > 0:
                all_data[company] = self.generate_signals(data)
        
        if not all_data:
            logger.error("❌ No price data found")
            return {}
            
        print(f"📊 Pair trading {len(all_data)} A/B pairs...")
        
        # Get all dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation loop
        for i, current_date in enumerate(all_dates):
            
            if i % 200 == 0:
                print(f"   📅 Processing {current_date.date()} ({i+1}/{len(all_dates)})")
            
            # Execute pending orders
            executed_orders = []
            for company, pending_trade in self.pending_orders.items():
                if company in all_data:
                    current_data = all_data[company][all_data[company]['date'] == current_date]
                    if not current_data.empty:
                        current_row = current_data.iloc[0]
                        
                        if self.can_execute_limit_order(pending_trade, current_row):
                            pending_trade.entry_date = current_date
                            self.initialize_stop_losses(pending_trade)
                            self.open_trades.append(pending_trade)
                            executed_orders.append(company)
                        elif (current_date - pending_trade.signal_date).days > 3:
                            # Cancel expired orders
                            self.cash += pending_trade.investment_amount + pending_trade.transaction_costs
                            executed_orders.append(company)
            
            # Remove executed orders
            for company in executed_orders:
                del self.pending_orders[company]
            
            # Check exit conditions
            trades_to_close = []
            for trade in self.open_trades:
                if trade.company not in all_data:
                    continue
                    
                company_data = all_data[trade.company]
                current_row = company_data[company_data['date'] == current_date]
                
                if current_row.empty:
                    continue
                    
                current_row = current_row.iloc[0]
                days_held = (current_date - trade.entry_date).days
                
                should_close, reason, exit_a_price, exit_b_price = self.should_close_trade(
                    trade, current_row, days_held
                )
                
                if should_close:
                    self.close_trade(
                        trade, current_date,
                        current_row['a_price'], current_row['b_price'],
                        current_row['z_score'], reason, exit_a_price, exit_b_price
                    )
                    trades_to_close.append(trade)
            
            # Move closed trades
            for trade in trades_to_close:
                self.open_trades.remove(trade)
                self.closed_trades.append(trade)
            
            # Look for new entry opportunities
            if len(self.open_trades) + len(self.pending_orders) < self.config.max_positions:
                for company, data in all_data.items():
                    if (any(t.company == company for t in self.open_trades) or 
                        company in self.pending_orders):
                        continue
                        
                    current_row = data[data['date'] == current_date]
                    if current_row.empty:
                        continue
                        
                    current_row = current_row.iloc[0]
                    signal = current_row['signal']
                    
                    if signal != PairAction.HOLD:
                        trade = self.open_trade(
                            company, current_date, signal,
                            current_row['a_price'], current_row['b_price'],
                            current_row['z_score']
                        )
                        
                        total_cost = trade.investment_amount + trade.transaction_costs
                        
                        if self.cash >= total_cost:
                            self.pending_orders[company] = trade
            
            # Record portfolio value
            portfolio_value = self.cash
            for trade in self.open_trades:
                if trade.company in all_data:
                    company_data = all_data[trade.company]
                    current_row = company_data[company_data['date'] == current_date]
                    if not current_row.empty:
                        current_row = current_row.iloc[0]
                        # Value pair position
                        if trade.action == PairAction.BUY_A_SELL_B:
                            portfolio_value += (trade.shares_a * current_row['a_price'] - 
                                              trade.shares_b * current_row['b_price'])
                        else:  # BUY_B_SELL_A
                            portfolio_value += (trade.shares_b * current_row['b_price'] - 
                                              trade.shares_a * current_row['a_price'])
                        
            self.daily_portfolio_values.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'open_positions': len(self.open_trades)
            })
        
        # Close remaining trades
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
        
        # Return cash for pending orders
        for company, pending_trade in self.pending_orders.items():
            self.cash += pending_trade.investment_amount + pending_trade.transaction_costs
        self.pending_orders.clear()
        
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
        final_value = self.cash
        initial_value = self.config.initial_capital
        total_return = (final_value - initial_value) / initial_value * 100
        
        if self.daily_portfolio_values:
            portfolio_df = pd.DataFrame(self.daily_portfolio_values)
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
            volatility = sharpe_ratio = max_drawdown = 0
        
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
        print(f"🎯 A/B SHARE PAIR TRADING BACKTEST RESULTS")
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
        
        # Action breakdown
        print(f"\n📊 TRADE ANALYSIS BY PAIR ACTION:")
        buy_a_sell_b_trades = [t for t in self.closed_trades if t.action == PairAction.BUY_A_SELL_B]
        buy_b_sell_a_trades = [t for t in self.closed_trades if t.action == PairAction.BUY_B_SELL_A]
        
        if buy_a_sell_b_trades:
            pnl = sum(t.pnl for t in buy_a_sell_b_trades)
            wins = len([t for t in buy_a_sell_b_trades if t.pnl > 0])
            win_rate = wins / len(buy_a_sell_b_trades) * 100
            print(f"   Buy A, Sell B: {len(buy_a_sell_b_trades):3d} trades, ${pnl:+8,.0f} P&L, {win_rate:5.1f}% wins")
        
        if buy_b_sell_a_trades:
            pnl = sum(t.pnl for t in buy_b_sell_a_trades)
            wins = len([t for t in buy_b_sell_a_trades if t.pnl > 0])
            win_rate = wins / len(buy_b_sell_a_trades) * 100
            print(f"   Buy B, Sell A: {len(buy_b_sell_a_trades):3d} trades, ${pnl:+8,.0f} P&L, {win_rate:5.1f}% wins")
        
        # Company breakdown
        print(f"\n🎯 TRADE BREAKDOWN BY COMPANY:")
        company_stats = {}
        for trade in self.closed_trades:
            if trade.company not in company_stats:
                company_stats[trade.company] = {'trades': 0, 'pnl': 0, 'wins': 0}
            company_stats[trade.company]['trades'] += 1
            company_stats[trade.company]['pnl'] += trade.pnl
            if trade.pnl > 0:
                company_stats[trade.company]['wins'] += 1
        
        for company, stats in sorted(company_stats.items(), key=lambda x: x[1]['pnl'], reverse=True)[:15]:
            win_rate = stats['wins'] / stats['trades'] * 100 if stats['trades'] > 0 else 0
            print(f"   {company:<12}: {stats['trades']:2d} trades, ${stats['pnl']:+7.0f} P&L, {win_rate:4.0f}% wins")
        
        print(f"\n" + "="*80)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run A/B pair trading backtest."""
    
    config = PairTradingConfig(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2025, 10, 1),
        initial_capital=100000.0,
        max_positions=5,
        position_size=0.15,  # 15% per side
        transaction_cost_rate=0.002,
        z_score_entry=2.0,
        z_score_exit=0.5,
        max_holding_days=30,
        lookback_window=90
    )
    
    backtester = ABPairTradingBacktester(config)
    
    try:
        await backtester.setup()
        results = await backtester.run_backtest()
        backtester.print_results(results)
        
    except Exception as e:
        logger.error(f"Error during backtest: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backtester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())