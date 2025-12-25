#!/usr/bin/env python3
"""
A/B Share Long-Only Arbitrage Backtesting System

Modified version that only buys the undervalued share instead of pair trading.
No short selling required - suitable for retail investors.
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
    level=logging.WARNING,  # Changed from INFO to WARNING to reduce output
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LongAction(Enum):
    BUY_A = "buy_a_only"
    BUY_B = "buy_b_only" 
    HOLD = "hold"
    SELL = "sell"

@dataclass
class LongTrade:
    signal_date: datetime  # When signal was generated
    entry_date: Optional[datetime]  # When trade actually executed
    exit_date: Optional[datetime]
    company: str
    action: LongAction
    entry_spread: float
    exit_spread: Optional[float]
    entry_price: float
    exit_price: Optional[float]
    z_score_entry: float
    z_score_exit: Optional[float]
    shares: int
    investment_amount: float
    pnl: Optional[float] = None
    transaction_costs: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True
    target_price: Optional[float] = None  # For limit order validation
    # Exit order management
    highest_price: Optional[float] = None  # For trailing stop
    trailing_stop_price: Optional[float] = None  # Current trailing stop level
    hard_stop_price: Optional[float] = None  # Fixed stop loss from entry
    limit_sell_price: Optional[float] = None  # Pending limit sell order
    exit_reason: Optional[str] = None  # Why the trade was closed

@dataclass
class LongOnlyConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    max_positions: int = 10  # More positions since we're not shorting
    position_size: float = 0.15  # 10% per position
    transaction_cost_rate: float = 0.002  # 0.2% per transaction (buy + sell)
    z_score_entry: float = 2  # Enter when |z-score| > 2
    z_score_exit: float = 0.5   # Exit when |z-score| < 0.5
    max_holding_days: int = 60  # Force exit after 30 days
    lookback_window: int = 90   # Days for spread calculation
    # Exit order settings
    trailing_stop_pct: float = 0.02  # 8% trailing stop loss (more suitable for mean reversion)
    stop_loss_pct: float = 0.6  # 15% hard stop loss from entry (catastrophic risk only)
    limit_sell_buffer: float = 0.01  # 0.5% above current for limit sell

class ABLongOnlyBacktester:
    
    def __init__(self, config: LongOnlyConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[LongTrade] = []
        self.closed_trades: List[LongTrade] = []
        self.pending_orders: Dict[str, LongTrade] = {}  # Company -> pending limit order
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
        
        # Use a recent date to ensure we have current data
        min_date = self.config.start_date - timedelta(days=30)
        records = await self.db_conn.fetch(query, min_date.date())
        companies = [record['company_base'] for record in records]
        
        print(f"📊 Found {len(companies)} complete A/B pairs for long-only backtesting")
        return companies
        
    async def get_historical_prices(self, company: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get historical price data for A/B shares of a company."""
        
        # Use space format symbols for this company
        symbols = [f"{company} A", f"{company} B"]
        
        if len(symbols) != 2:
            return pd.DataFrame()  # Need exactly 2 symbols (A and B)
        
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
            
        # Convert asyncpg.Record objects to dict first
        df = pd.DataFrame([dict(record) for record in records])
        df['date'] = pd.to_datetime(df['date'])
        
        # Convert all price columns to float
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
        
        # Find A and B columns for all price types
        a_cols = {}
        b_cols = {}
        
        for col in pivot_df.columns:
            if ' A_' in col:
                price_type = '_'.join(col.split('_')[1:])  # get 'open_price', 'close_price', etc.
                a_cols[price_type] = col
            elif ' B_' in col:
                price_type = '_'.join(col.split('_')[1:])
                b_cols[price_type] = col
                
        if 'close_price' not in a_cols or 'close_price' not in b_cols:
            return pd.DataFrame()
            
        # Clean DataFrame with all OHLC data
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
        
        # Add momentum indicators to identify WHY the spread changed
        result_df['a_return_5d'] = result_df['a_price'].pct_change(5)
        result_df['b_return_5d'] = result_df['b_price'].pct_change(5)
        result_df['a_momentum_positive'] = result_df['a_return_5d'] > 0
        result_df['b_momentum_positive'] = result_df['b_return_5d'] > 0
        
        return result_df.dropna()
        
    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate long-only trading signals based on z-score thresholds."""
        
        signals = []
        
        for _, row in data.iterrows():
            z_score = row['z_score']
            a_momentum_positive = row.get('a_momentum_positive', True)
            b_momentum_positive = row.get('b_momentum_positive', True)
            
            # LONG-ONLY MOMENTUM-BASED LOGIC: Only buy after positive momentum
            if z_score >= self.config.z_score_entry:
                # Spread has widened - check WHY
                if a_momentum_positive and not b_momentum_positive:
                    # A went up, B went down - risky, A might get dragged down
                    action = LongAction.HOLD  
                elif a_momentum_positive:
                    # A outperformed (went up faster), buy B for catch-up
                    action = LongAction.BUY_B
                else:
                    action = LongAction.HOLD
                    
            elif z_score <= -self.config.z_score_entry:
                # Spread has compressed - check WHY  
                if b_momentum_positive and not a_momentum_positive:
                    # B went up, A went down - risky, B might get dragged down
                    action = LongAction.HOLD
                elif b_momentum_positive:
                    # B outperformed (went up faster), buy A for catch-up
                    action = LongAction.BUY_A
                else:
                    action = LongAction.HOLD
            else:
                action = LongAction.HOLD
                
            signals.append(action)
            
        data['signal'] = signals
        return data
        
    def calculate_position_size(self, price: float) -> Tuple[int, float]:
        """Calculate position size for long-only trade."""
        
        available_capital = self.cash * self.config.position_size
        shares = int(available_capital / price) if price > 0 else 0
        actual_investment = shares * price
        
        return shares, actual_investment
        
    def calculate_transaction_costs(self, investment_amount: float) -> float:
        """Calculate transaction costs for realistic trading (0.09% courtage each way = 0.18% total)."""
        return investment_amount * self.config.transaction_cost_rate
        
    def calculate_limit_order_price(self, current_spread_mean: float, current_spread_std: float, 
                                   a_price: float, b_price: float, buy_a: bool) -> float:
        """Calculate the limit order price that would trigger our z-score entry signal."""
        # We want to buy when |z-score| >= entry_threshold
        # z_score = (spread - mean) / std
        # For BUY_A: z_score <= -threshold (A is cheap relative to B)
        # For BUY_B: z_score >= +threshold (B is cheap relative to A)
        
        if buy_a:
            # We want: (spread - mean) / std <= -threshold
            # spread <= mean - threshold * std
            target_spread = current_spread_mean - self.config.z_score_entry * current_spread_std
            # spread = (a_price - b_price) / b_price * 100
            # Solve for a_price: a_price = b_price * (1 + target_spread/100)
            return b_price * (1 + target_spread / 100)
        else:
            # We want: (spread - mean) / std >= threshold  
            # spread >= mean + threshold * std
            target_spread = current_spread_mean + self.config.z_score_entry * current_spread_std
            # For buying B when spread is high, we want to buy B at current price
            # The limit price for B doesn't change based on spread
            return b_price
    
    def open_trade(self, company: str, signal_date: datetime, action: LongAction, 
                  a_price: float, b_price: float, z_score: float, spread_mean: float, 
                  spread_std: float) -> LongTrade:
        """Create a pending long-only arbitrage trade with calculated limit order."""
        
        # Calculate the exact limit price that would trigger our signal
        if action == LongAction.BUY_A:
            target_price = self.calculate_limit_order_price(spread_mean, spread_std, a_price, b_price, True)
            entry_price = target_price  # We'll execute at this price if market allows
        else:  # BUY_B
            target_price = b_price  # For buying B, we can use current price as limit
            entry_price = target_price
            
        shares, investment = self.calculate_position_size(entry_price)
        transaction_costs = self.calculate_transaction_costs(investment)
        
        trade = LongTrade(
            signal_date=signal_date,  # Signal generated today
            entry_date=None,  # Will be filled when executed
            exit_date=None,
            company=company,
            action=action,
            entry_spread=(a_price - b_price) / b_price * 100,
            exit_spread=None,
            entry_price=entry_price,
            exit_price=None,
            z_score_entry=z_score,
            z_score_exit=None,
            shares=shares,
            investment_amount=investment,
            transaction_costs=transaction_costs
        )
        
        # Store target price for limit order validation
        trade.target_price = target_price
        
        # Update cash
        self.cash -= investment + transaction_costs
        
        return trade
        
    def initialize_stop_losses(self, trade: LongTrade) -> None:
        """Initialize tracking for potential stop losses (not activated until exit signal)."""
        trade.highest_price = trade.entry_price
        # Don't set stop prices yet - only activate after exit signal
        trade.hard_stop_price = None
        trade.trailing_stop_price = None
    
    def update_trailing_stop(self, trade: LongTrade, current_price: float) -> None:
        """Update trailing stop loss based on new high prices."""
        if current_price > trade.highest_price:
            trade.highest_price = current_price
            new_trailing_stop = current_price * (1 - self.config.trailing_stop_pct)
            if new_trailing_stop > trade.trailing_stop_price:
                trade.trailing_stop_price = new_trailing_stop
    
    def can_execute_limit_order(self, trade: LongTrade, current_data: pd.Series) -> bool:
        """Check if limit order would have executed based on daily high/low."""
        if trade.action == LongAction.BUY_A:
            # For buying A shares, check if daily low was <= our limit price
            daily_low = current_data.get('a_low', current_data['a_price'])
            # Also need to check that the z-score condition is actually met
            z_score = current_data.get('z_score', 0)
            return daily_low <= trade.target_price and z_score <= -self.config.z_score_entry
        else:  # BUY_B
            # For buying B shares, check if daily low was <= our limit price
            daily_low = current_data.get('b_low', current_data['b_price'])
            # Also need to check that the z-score condition is actually met
            z_score = current_data.get('z_score', 0)
            return daily_low <= trade.target_price and z_score >= self.config.z_score_entry
        
    def close_trade(self, trade: LongTrade, date: datetime, a_price: float, 
                   b_price: float, z_score: float, reason: str = "signal", 
                   exit_price: Optional[float] = None) -> float:
        """Close an existing trade and calculate P&L with realistic exit execution."""
        
        trade.exit_date = date
        trade.z_score_exit = z_score
        trade.exit_spread = (a_price - b_price) / b_price * 100
        trade.holding_days = (date - trade.entry_date).days if trade.entry_date else 0
        trade.is_open = False
        trade.exit_reason = reason
        
        # Use provided exit price (from stop loss or limit order) or fall back to close price
        if exit_price is not None:
            trade.exit_price = exit_price
        else:
            # Fallback to close price for simple exits
            if trade.action == LongAction.BUY_A:
                trade.exit_price = a_price
            else:  # BUY_B
                trade.exit_price = b_price
            
        # Calculate P&L: (exit_price - entry_price) * shares - exit_costs
        price_return = (trade.exit_price - trade.entry_price) / trade.entry_price
        gross_pnl = price_return * trade.investment_amount
        
        # Exit transaction costs
        exit_costs = self.calculate_transaction_costs(trade.shares * trade.exit_price)
        trade.pnl = gross_pnl - exit_costs
        
        # Return original investment plus net P&L
        self.cash += trade.investment_amount + trade.pnl
        
        # No logging during backtest execution to keep output clean
        pass
        
        return trade.pnl
        
    def can_execute_limit_order(self, trade: LongTrade, current_data: pd.Series) -> bool:
        """Check if limit order would have executed based on daily high/low."""
        if trade.action == LongAction.BUY_A:
            daily_low = current_data.get('a_low', current_data['a_price'])
            return daily_low <= trade.target_price
        elif trade.action == LongAction.BUY_B:
            daily_low = current_data.get('b_low', current_data['b_price'])
            return daily_low <= trade.target_price
        return False
        
    def activate_stop_losses(self, trade: LongTrade, current_price: float) -> None:
        """Activate stop losses when we get an exit signal."""
        if trade.hard_stop_price is None:  # First time activating
            trade.hard_stop_price = current_price * (1 - self.config.stop_loss_pct)
            trade.trailing_stop_price = current_price * (1 - self.config.trailing_stop_pct)
            trade.highest_price = current_price
    
    def check_stop_loss_execution(self, trade: LongTrade, current_data: pd.Series) -> Tuple[bool, str, Optional[float]]:
        """Check if any stop losses should execute (only if activated)."""
        # Only check stops if they've been activated
        if trade.hard_stop_price is None:
            return False, "", None
            
        if trade.action == LongAction.BUY_A:
            current_price = current_data.get('a_price', 0)
            daily_low = current_data.get('a_low', current_price)
            daily_high = current_data.get('a_high', current_price)
        else:  # BUY_B
            current_price = current_data.get('b_price', 0)
            daily_low = current_data.get('b_low', current_price)
            daily_high = current_data.get('b_high', current_price)
        
        # Update trailing stop with daily high
        self.update_trailing_stop(trade, daily_high)
        
        # Check hard stop loss (gaps down)
        if daily_low <= trade.hard_stop_price:
            # Execute at the lower of stop price or daily low (gap protection)
            exit_price = max(trade.hard_stop_price, daily_low)
            return True, "hard_stop_loss", exit_price
        
        # Check trailing stop loss
        if daily_low <= trade.trailing_stop_price:
            # Execute at the lower of trailing stop or daily low
            exit_price = max(trade.trailing_stop_price, daily_low)
            return True, "trailing_stop", exit_price
        
        return False, "", None
    
    def check_limit_sell_execution(self, trade: LongTrade, current_data: pd.Series) -> Tuple[bool, Optional[float]]:
        """Check if we get an exit signal and handle limit sell execution."""
        z_score = current_data.get('z_score', 0)
        
        # Check if z-score has normalized (mean reversion signal)
        if abs(z_score) <= self.config.z_score_exit:
            if trade.action == LongAction.BUY_A:
                current_price = current_data.get('a_price', 0)
                daily_high = current_data.get('a_high', current_price)
            else:  # BUY_B
                current_price = current_data.get('b_price', 0)
                daily_high = current_data.get('b_high', current_price)
            
            # ACTIVATE stop losses now that we have exit signal
            self.activate_stop_losses(trade, current_price)
            
            # Set limit sell price above current price
            limit_price = current_price * (1 + self.config.limit_sell_buffer)
            
            # Check if limit sell would execute (daily high >= limit price)
            if daily_high >= limit_price:
                return True, limit_price
            else:
                # If limit sell fails, use daily low as market sell
                daily_low = current_data.get('a_low' if trade.action == LongAction.BUY_A else 'b_low', current_price)
                return True, daily_low
        
        return False, None
    
    def should_close_trade(self, trade: LongTrade, current_data: pd.Series, days_held: int) -> Tuple[bool, str, Optional[float]]:
        """Determine if a trade should be closed using realistic order execution."""
        
        # Check stop losses first (highest priority)
        stop_triggered, stop_reason, stop_price = self.check_stop_loss_execution(trade, current_data)
        if stop_triggered:
            return True, stop_reason, stop_price
        
        # Check limit sell based on z-score normalization
        limit_triggered, limit_price = self.check_limit_sell_execution(trade, current_data)
        if limit_triggered:
            return True, "z_score_normalized", limit_price
        
        # Force exit if maximum holding period reached (market sell at close)
        if days_held >= self.config.max_holding_days:
            if trade.action == LongAction.BUY_A:
                market_price = current_data.get('a_price', 0)
            else:
                market_price = current_data.get('b_price', 0)
            return True, "max_holding_period", market_price
        
        return False, "", None
        
    async def run_backtest(self) -> Dict:
        """Run the complete long-only backtest."""
        
        print(f"🚀 Starting A/B Long-Only Arbitrage Backtest")
        print(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        print(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        print(f"   📊 Max positions: {self.config.max_positions}")
        
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
                # Silent loading to reduce output clutter
        
        if not all_data:
            logger.error("❌ No price data found for any companies")
            return {}
            
        print(f"📊 Long-only backtesting {len(all_data)} A/B pairs...")
        
        # Get all unique dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation loop
        for i, current_date in enumerate(all_dates):
            
            if i % 200 == 0:  # Very reduced frequency of progress updates
                print(f"   📅 Processing {current_date.date()} ({i+1}/{len(all_dates)})")
            
            # First, check if any pending limit orders can be executed today
            executed_orders = []
            for company, pending_trade in self.pending_orders.items():
                if company in all_data:
                    current_data = all_data[company][all_data[company]['date'] == current_date]
                    if not current_data.empty:
                        current_row = current_data.iloc[0]
                        
                        # Check if our limit order would have executed
                        if self.can_execute_limit_order(pending_trade, current_row):
                            # Execute the trade
                            pending_trade.entry_date = current_date  # Actual execution date
                            self.initialize_stop_losses(pending_trade)
                            self.open_trades.append(pending_trade)
                            executed_orders.append(company)
                        elif (current_date - pending_trade.signal_date).days > 3:
                            # Cancel order if not filled within 3 days
                            self.cash += pending_trade.investment_amount + pending_trade.transaction_costs
                            executed_orders.append(company)  # Remove expired order
            
            # Remove executed or cancelled orders
            for company in executed_orders:
                del self.pending_orders[company]
            
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
                
                should_close, reason, exit_price = self.should_close_trade(
                    trade, current_row, days_held
                )
                
                if should_close:
                    self.close_trade(
                        trade, current_date, 
                        current_row['a_price'], current_row['b_price'],
                        current_row['z_score'], reason, exit_price
                    )
                    trades_to_close.append(trade)
            
            # Move closed trades
            for trade in trades_to_close:
                self.open_trades.remove(trade)
                self.closed_trades.append(trade)
            
            # Look for new entry opportunities to place limit orders
            if len(self.open_trades) + len(self.pending_orders) < self.config.max_positions:
                for company, data in all_data.items():
                    # Skip if already have position or pending order in this company
                    if (any(t.company == company for t in self.open_trades) or 
                        company in self.pending_orders):
                        continue
                        
                    current_row = data[data['date'] == current_date]
                    if current_row.empty:
                        continue
                        
                    current_row = current_row.iloc[0]
                    
                    # Check if we're close to a signal (within 80% of threshold)
                    z_score = abs(current_row['z_score'])
                    if z_score >= self.config.z_score_entry:  # Close to signal
                        # Determine which type of trade we'd make
                        if current_row['z_score'] >= self.config.z_score_entry:
                            signal = LongAction.BUY_B
                        elif current_row['z_score'] <= -self.config.z_score_entry:
                            signal = LongAction.BUY_A
                        else:
                            continue
                            
                        # Calculate limit order details
                        trade = self.open_trade(
                            company, current_date, signal,  # Signal date is today
                            current_row['a_price'], current_row['b_price'],
                            current_row['z_score'], current_row['spread_mean'],
                            current_row['spread_std']
                        )
                        
                        # Check if we have enough cash for this order
                        total_cost = trade.investment_amount + trade.transaction_costs
                        
                        if self.cash >= total_cost:
                            # Place pending limit order (reserve cash)
                            self.pending_orders[company] = trade
                            # Cash is already deducted in open_trade()
            
            # Record daily portfolio value
            portfolio_value = self.cash
            for trade in self.open_trades:
                if trade.company in all_data:
                    company_data = all_data[trade.company]
                    current_row = company_data[company_data['date'] == current_date]
                    if not current_row.empty:
                        current_row = current_row.iloc[0]
                        # Value position at current market price
                        if trade.action == LongAction.BUY_A:
                            current_price = current_row['a_price']
                        else:  # BUY_B
                            current_price = current_row['b_price']
                        portfolio_value += trade.shares * current_price
                        
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
        
        # Return cash for any pending orders that never executed
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
        
        # Portfolio performance - use actual cash since all positions are closed
        final_value = self.cash
        initial_value = self.config.initial_capital
        total_return = (final_value - initial_value) / initial_value * 100
        
        # Portfolio time series analysis
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
        print(f"🎯 A/B SHARE LONG-ONLY ARBITRAGE BACKTEST RESULTS")
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
        
        for company, stats in sorted(company_stats.items(), key=lambda x: x[1]['pnl'], reverse=True)[:15]:
            win_rate = stats['wins'] / stats['trades'] * 100 if stats['trades'] > 0 else 0
            print(f"   {company:<12}: {stats['trades']:2d} trades, ${stats['pnl']:+7.0f} P&L, {win_rate:4.0f}% wins")
        
        # Action breakdown
        print(f"\n📊 TRADE ANALYSIS BY ACTION:")
        buy_a_trades = [t for t in self.closed_trades if t.action == LongAction.BUY_A]
        buy_b_trades = [t for t in self.closed_trades if t.action == LongAction.BUY_B]
        
        if buy_a_trades:
            buy_a_pnl = sum(t.pnl for t in buy_a_trades)
            buy_a_wins = len([t for t in buy_a_trades if t.pnl > 0])
            buy_a_win_rate = buy_a_wins / len(buy_a_trades) * 100
            print(f"   Buy A shares: {len(buy_a_trades):3d} trades, ${buy_a_pnl:+8,.0f} P&L, {buy_a_win_rate:5.1f}% wins")
        
        if buy_b_trades:
            buy_b_pnl = sum(t.pnl for t in buy_b_trades)
            buy_b_wins = len([t for t in buy_b_trades if t.pnl > 0])
            buy_b_win_rate = buy_b_wins / len(buy_b_trades) * 100
            print(f"   Buy B shares: {len(buy_b_trades):3d} trades, ${buy_b_pnl:+8,.0f} P&L, {buy_b_win_rate:5.1f}% wins")
        
        # Add detailed trade list
        self.print_detailed_trade_list()
            
        print(f"\n" + "="*80)
        
    def print_detailed_trade_list(self):
        """Print detailed list of all long-only trades with signal date, entry/exit info."""
        
        if not self.closed_trades:
            return
            
        print(f"\n" + "="*140)
        print(f"📋 DETAILED LONG-ONLY TRADE LIST ({len(self.closed_trades)} trades)")
        print(f"="*140)
        
        # Sort trades by entry date
        sorted_trades = sorted(self.closed_trades, key=lambda x: x.signal_date if hasattr(x, 'signal_date') else x.entry_date)
        
        # Header
        print(f"{'#':<4} {'Company':<8} {'Action':<10} {'Signal Date':<12} {'Entry Date':<12} {'Exit Date':<12} "
              f"{'Days':<4} {'Entry $':<8} {'Exit $':<8} {'Shares':<6} {'P&L':<10} {'Return':<7} {'Reason':<10}")
        print(f"{'-'*140}")
        
        for i, trade in enumerate(sorted_trades, 1):
            # Calculate return percentage
            return_pct = (trade.pnl / trade.investment_amount) * 100 if trade.investment_amount > 0 else 0
            
            # Format action
            action_str = "Buy A" if trade.action == LongAction.BUY_A else "Buy B"
            
            # Get signal date
            signal_date = trade.signal_date if hasattr(trade, 'signal_date') else trade.entry_date
            
            # Determine exit reason
            exit_reason = "timeout" if trade.holding_days >= self.config.max_holding_days else "z_norm"
            
            # Format P&L with color indication
            pnl_str = f"${trade.pnl:+,.0f}"
            
            print(f"{i:<4} {trade.company:<8} {action_str:<10} {signal_date.strftime('%Y-%m-%d'):<12} "
                  f"{trade.entry_date.strftime('%Y-%m-%d') if trade.entry_date else 'Not Exec':<12} {trade.exit_date.strftime('%Y-%m-%d'):<12} "
                  f"{trade.holding_days:<4} ${trade.entry_price:<7.2f} ${trade.exit_price:<7.2f} "
                  f"{trade.shares:<6} {pnl_str:<10} {return_pct:+6.1f}% {exit_reason:<10}")
        
        # Summary statistics by different categories
        print(f"\n📊 TRADE PERFORMANCE ANALYSIS:")
        
        # By action type
        buy_a_trades = [t for t in self.closed_trades if t.action == LongAction.BUY_A]
        buy_b_trades = [t for t in self.closed_trades if t.action == LongAction.BUY_B]
        
        if buy_a_trades:
            buy_a_returns = [(t.pnl / t.investment_amount) * 100 for t in buy_a_trades if t.investment_amount > 0]
            buy_a_avg_return = np.mean(buy_a_returns) if buy_a_returns else 0
            buy_a_avg_days = np.mean([t.holding_days for t in buy_a_trades])
            print(f"   Buy A strategy: {len(buy_a_trades):3d} trades, {buy_a_avg_return:+5.1f}% avg return, {buy_a_avg_days:4.1f} avg days")
        
        if buy_b_trades:
            buy_b_returns = [(t.pnl / t.investment_amount) * 100 for t in buy_b_trades if t.investment_amount > 0]
            buy_b_avg_return = np.mean(buy_b_returns) if buy_b_returns else 0
            buy_b_avg_days = np.mean([t.holding_days for t in buy_b_trades])
            print(f"   Buy B strategy: {len(buy_b_trades):3d} trades, {buy_b_avg_return:+5.1f}% avg return, {buy_b_avg_days:4.1f} avg days")
        
        # By holding period
        quick_trades = [t for t in self.closed_trades if t.holding_days <= 7]
        medium_trades = [t for t in self.closed_trades if 7 < t.holding_days <= 21]
        long_trades = [t for t in self.closed_trades if t.holding_days > 21]
        
        for trades, label in [(quick_trades, "≤7 days"), (medium_trades, "8-21 days"), (long_trades, ">21 days")]:
            if trades:
                returns = [(t.pnl / t.investment_amount) * 100 for t in trades if t.investment_amount > 0]
                avg_return = np.mean(returns) if returns else 0
                win_rate = len([t for t in trades if t.pnl > 0]) / len(trades) * 100
                print(f"   {label:<10}: {len(trades):3d} trades, {avg_return:+5.1f}% avg return, {win_rate:5.1f}% win rate")
        
        # Best and worst trades
        print(f"\n🏆 TOP 10 BEST TRADES:")
        best_trades = sorted(self.closed_trades, key=lambda x: x.pnl, reverse=True)[:10]
        for i, trade in enumerate(best_trades, 1):
            return_pct = (trade.pnl / trade.investment_amount) * 100 if trade.investment_amount > 0 else 0
            action_str = "Buy A" if trade.action == LongAction.BUY_A else "Buy B"
            print(f"   {i:2d}. {trade.company:<8} {action_str:<6} {trade.entry_date.strftime('%Y-%m-%d')} "
                  f"{trade.holding_days:2d}d ${trade.pnl:+8,.0f} ({return_pct:+5.1f}%)")
        
        print(f"\n📉 BOTTOM 5 WORST TRADES:")
        worst_trades = sorted(self.closed_trades, key=lambda x: x.pnl)[:5]
        for i, trade in enumerate(worst_trades, 1):
            return_pct = (trade.pnl / trade.investment_amount) * 100 if trade.investment_amount > 0 else 0
            action_str = "Buy A" if trade.action == LongAction.BUY_A else "Buy B"
            print(f"   {i:2d}. {trade.company:<8} {action_str:<6} {trade.entry_date.strftime('%Y-%m-%d')} "
                  f"{trade.holding_days:2d}d ${trade.pnl:+8,.0f} ({return_pct:+5.1f}%)")
        
        print(f"\n" + "="*140)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run A/B long-only arbitrage backtest with different configurations."""
    
    # Default configuration
    config = LongOnlyConfig(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2025, 10, 1),
        initial_capital=100000.0,
        max_positions=10,  # More positions since we're long-only
        position_size=0.2,  # 10% per position
        transaction_cost_rate=0.001,  # 0.1% per trade
        z_score_entry=2.4,
        z_score_exit=0.8,
        max_holding_days=30,
        lookback_window=90
    )
    
    backtester = ABLongOnlyBacktester(config)
    
    try:
        await backtester.setup()
        results = await backtester.run_backtest()
        backtester.print_results(results)
        
        # Test different configurations
        # print(f"\n🧪 TESTING DIFFERENT LONG-ONLY PARAMETERS:")
        
        # test_configs = [
        #     ("Conservative (Z=2.5)", {'z_score_entry': 2.5, 'z_score_exit': 0.3}),
        #     ("Aggressive (Z=1.5)", {'z_score_entry': 1.5, 'z_score_exit': 0.7}),
        #     ("Quick Exit (15 days)", {'max_holding_days': 15}),
        #     ("Patient (60 days)", {'max_holding_days': 60}),
        #     ("More Positions (15)", {'max_positions': 15, 'position_size': 0.067}),
        # ]
        
        # for name, params in test_configs:
            # print(f"\n📊 {name}:")
            # test_config = LongOnlyConfig(
            #     start_date=datetime(2023, 1, 1),
            #     end_date=datetime(2025, 10, 1),
            #     initial_capital=100000.0,
            #     **params
            # )
            
            # test_backtester = ABLongOnlyBacktester(test_config)
            # await test_backtester.setup()
            # test_results = await test_backtester.run_backtest()
            
            # if 'total_return' in test_results:
            #     print(f"   Return: {test_results['total_return']:+.1f}% | "
            #           f"Sharpe: {test_results['sharpe_ratio']:.2f} | "
            #           f"Trades: {test_results['total_trades']} | "
            #           f"Win Rate: {test_results['win_rate']:.1f}%")
                
            #await test_backtester.cleanup()
        
    except Exception as e:
        logger.error(f"Error during backtest: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await backtester.cleanup()

if __name__ == "__main__":
    asyncio.run(main())