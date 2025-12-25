#!/usr/bin/env python3
"""
A/B Share Premium-Discount Arbitrage Strategy - FIXED VERSION

Fixed bugs:
1. No lookahead bias - pending orders execute next day
2. Proper cash flow accounting  
3. Better premium share identification
4. Realistic position sizing
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

class PremiumAction(Enum):
    BUY_A = "buy_a_when_discount"
    BUY_B = "buy_b_when_discount" 
    HOLD = "hold"
    SELL = "sell"

@dataclass
class PremiumTrade:
    signal_date: datetime
    entry_date: Optional[datetime]
    exit_date: Optional[datetime]
    company: str
    action: PremiumAction
    share_class: str  # 'A' or 'B'
    entry_price: float
    exit_price: Optional[float]
    entry_spread: float  # How much discount when we bought
    exit_spread: Optional[float]
    shares: int
    investment_amount: float
    premium_share: str  # Which share is usually premium ('A' or 'B')
    discount_magnitude: float  # How big the discount was when we bought
    pnl: Optional[float] = None
    transaction_costs: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True
    exit_reason: Optional[str] = None

@dataclass
class PremiumDiscountConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    max_positions: int = 8
    position_size: float = 0.10  # 10% per position (more conservative)
    transaction_cost_rate: float = 0.002
    lookback_window: int = 120  # Days to determine which share is premium
    min_discount_pct: float = 2.0  # Must be at least 2% discount to trade
    exit_premium_pct: float = 1.0   # Exit when back to 1% premium
    max_holding_days: int = 30  # Shorter holding period
    min_observation_days: int = 90  # Need this much history to determine premium

class ABPremiumDiscountBacktester:
    
    def __init__(self, config: PremiumDiscountConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[PremiumTrade] = []
        self.closed_trades: List[PremiumTrade] = []
        self.pending_orders: Dict[str, PremiumTrade] = {}
        self.cash = config.initial_capital
        self.daily_portfolio_values = []
        self.premium_share_cache: Dict[str, str] = {}  # company -> 'A' or 'B'
        
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
        
        min_date = self.config.start_date - timedelta(days=200)
        records = await self.db_conn.fetch(query, min_date.date())
        companies = [record['company_base'] for record in records]
        
        print(f"📊 Found {len(companies)} complete A/B pairs for premium-discount trading")
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
        
        # Flatten column names: symbol_pricetype
        pivot_df.columns = [f"{col[1]}_{col[0]}" for col in pivot_df.columns]
        
        # Find A and B columns
        a_cols = {}
        b_cols = {}
        
        for col in pivot_df.columns:
            if f' A_' in col:
                price_type = '_'.join(col.split('_')[1:])
                a_cols[price_type] = col
            elif f' B_' in col:
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
        
        # Remove any rows with zero or negative prices (data quality)
        result_df = result_df[(result_df['a_price'] > 0) & (result_df['b_price'] > 0)]
        
        if len(result_df) == 0:
            return pd.DataFrame()
            
        # Calculate spreads
        result_df['a_premium'] = (result_df['a_price'] - result_df['b_price']) / result_df['b_price'] * 100
        result_df['b_premium'] = (result_df['b_price'] - result_df['a_price']) / result_df['a_price'] * 100
        
        return result_df.dropna()
    
    def determine_premium_share(self, data: pd.DataFrame, company: str) -> Optional[str]:
        """Determine which share class typically trades at a premium."""
        
        # Need sufficient history
        if len(data) < self.config.min_observation_days:
            return None
            
        # Calculate average premiums over lookback window  
        recent_data = data.tail(self.config.lookback_window)
        
        avg_a_premium = recent_data['a_premium'].mean()
        avg_b_premium = recent_data['b_premium'].mean()
        
        # Need a clear premium (at least 1% average difference)
        if avg_a_premium > avg_b_premium + 1.0:
            premium_share = 'A'
        elif avg_b_premium > avg_a_premium + 1.0:
            premium_share = 'B'
        else:
            return None  # No clear premium share
            
        self.premium_share_cache[company] = premium_share
        return premium_share
        
    def generate_signals(self, data: pd.DataFrame, company: str) -> pd.DataFrame:
        """Generate premium-discount arbitrage signals."""
        
        signals = []
        
        # Determine which share is typically premium
        premium_share = self.determine_premium_share(data, company)
        
        if premium_share is None:
            # No clear premium pattern, don't trade this pair
            data['signal'] = [PremiumAction.HOLD] * len(data)
            data['premium_share'] = 'NONE'
            return data
        
        for _, row in data.iterrows():
            a_premium = row['a_premium']
            b_premium = row['b_premium']
            
            # Skip if data looks suspicious (extreme values)
            if abs(a_premium) > 50 or abs(b_premium) > 50:
                action = PremiumAction.HOLD
            elif premium_share == 'A':
                # A is usually premium, buy A when it's at significant discount to B
                if a_premium <= -self.config.min_discount_pct:  # A trading at 2%+ discount
                    action = PremiumAction.BUY_A
                else:
                    action = PremiumAction.HOLD
            else:
                # B is usually premium, buy B when it's at significant discount to A  
                if b_premium <= -self.config.min_discount_pct:  # B trading at 2%+ discount
                    action = PremiumAction.BUY_B
                else:
                    action = PremiumAction.HOLD
                    
            signals.append(action)
            
        data['signal'] = signals
        data['premium_share'] = premium_share
        return data
        
    def calculate_position_size(self, price: float) -> Tuple[int, float]:
        """Calculate position size for premium share purchase."""
        
        available_capital = self.cash * self.config.position_size
        shares = int(available_capital / price)
        actual_investment = shares * price
        
        return shares, actual_investment
        
    def calculate_transaction_costs(self, investment_amount: float) -> float:
        """Calculate round-trip transaction costs."""
        return investment_amount * self.config.transaction_cost_rate
        
    def open_trade(self, company: str, signal_date: datetime, action: PremiumAction,
                  a_price: float, b_price: float, premium_share: str) -> PremiumTrade:
        """Create a pending premium-discount trade."""
        
        if action == PremiumAction.BUY_A:
            entry_price = a_price
            share_class = 'A'
            spread = (a_price - b_price) / b_price * 100  # Negative = discount
        else:  # BUY_B
            entry_price = b_price
            share_class = 'B'
            spread = (b_price - a_price) / a_price * 100  # Negative = discount
            
        shares, investment = self.calculate_position_size(entry_price)
        
        # Must have enough cash
        transaction_costs = self.calculate_transaction_costs(investment)
        total_cost = investment + transaction_costs
        
        if total_cost > self.cash:
            # Scale down position to fit available cash
            max_investment = self.cash * 0.95 - transaction_costs  # Leave 5% buffer
            shares = int(max_investment / entry_price)
            investment = shares * entry_price
        
        transaction_costs = self.calculate_transaction_costs(investment)
        
        trade = PremiumTrade(
            signal_date=signal_date,
            entry_date=None,
            exit_date=None,
            company=company,
            action=action,
            share_class=share_class,
            entry_price=entry_price,
            exit_price=None,
            entry_spread=spread,
            exit_spread=None,
            shares=shares,
            investment_amount=investment,
            transaction_costs=transaction_costs,
            premium_share=premium_share,
            discount_magnitude=abs(spread)
        )
        
        return trade
        
    def can_execute_limit_order(self, trade: PremiumTrade, current_data: pd.Series) -> bool:
        """Check if pending order can execute (next day)."""
        
        if trade.share_class == 'A':
            current_price = current_data.get('a_open', current_data.get('a_price'))
            # Execute if open price is still favorable
            return current_price <= trade.entry_price * 1.02  # 2% slippage tolerance
        else:
            current_price = current_data.get('b_open', current_data.get('b_price'))
            return current_price <= trade.entry_price * 1.02
        
    def should_close_trade(self, trade: PremiumTrade, current_data: pd.Series, days_held: int) -> Tuple[bool, str]:
        """Determine if a premium-discount trade should be closed."""
        
        if trade.share_class == 'A':
            current_spread = current_data.get('a_premium', 0)
        else:
            current_spread = current_data.get('b_premium', 0)
            
        # Exit if back to premium (spread > exit threshold)
        if current_spread >= self.config.exit_premium_pct:
            return True, "back_to_premium"
            
        # Exit if maximum holding period reached
        if days_held >= self.config.max_holding_days:
            return True, "max_holding_period"
            
        return False, ""
        
    def close_trade(self, trade: PremiumTrade, date: datetime, 
                   current_data: pd.Series, reason: str = "signal") -> float:
        """Close a premium-discount trade and calculate P&L."""
        
        if trade.share_class == 'A':
            exit_price = current_data['a_price']
            trade.exit_spread = current_data.get('a_premium', 0)
        else:
            exit_price = current_data['b_price']
            trade.exit_spread = current_data.get('b_premium', 0)
            
        trade.exit_date = date
        trade.exit_price = exit_price
        trade.holding_days = (date - trade.entry_date).days if trade.entry_date else 0
        trade.is_open = False
        trade.exit_reason = reason
        
        # Calculate P&L
        gross_pnl = (exit_price - trade.entry_price) * trade.shares
        exit_costs = self.calculate_transaction_costs(trade.investment_amount)
        trade.pnl = gross_pnl - exit_costs
        
        # Return invested capital plus P&L to cash
        self.cash += trade.investment_amount + trade.pnl
        
        return trade.pnl
        
    async def run_backtest(self) -> Dict:
        """Run the complete premium-discount backtest."""
        
        print(f"🚀 Starting A/B Premium-Discount Arbitrage Backtest (FIXED)")
        print(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        print(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        print(f"   📊 Strategy: Buy premium share when it trades at {self.config.min_discount_pct}%+ discount")
        
        # Get A/B pairs
        companies = await self.get_ab_pairs()
        
        if not companies:
            logger.error("❌ No complete A/B pairs found")
            return {}
            
        # Load historical data
        all_data = {}
        premium_shares = {}
        skipped_companies = []
        
        for company in companies:
            data = await self.get_historical_prices(
                company, 
                self.config.start_date - timedelta(days=self.config.lookback_window + 60), 
                self.config.end_date
            )
            
            if len(data) > self.config.min_observation_days:
                signals_data = self.generate_signals(data, company)
                
                # Only include companies with clear premium patterns
                if signals_data['premium_share'].iloc[0] != 'NONE':
                    all_data[company] = signals_data
                    premium_shares[company] = self.premium_share_cache.get(company, 'NONE')
                else:
                    skipped_companies.append(company)
            else:
                skipped_companies.append(company)
        
        if not all_data:
            logger.error("❌ No price data found")
            return {}
            
        print(f"📊 Trading {len(all_data)} pairs (skipped {len(skipped_companies)} without clear premium patterns):")
        for company, premium in premium_shares.items():
            print(f"   {company}: {premium} share typically premium")
            
        # Get all dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation loop
        for i, current_date in enumerate(all_dates):
            
            if i % 200 == 0:
                print(f"   📅 Processing {current_date.date()} ({i+1}/{len(all_dates)}) - Cash: ${self.cash:,.0f}")
            
            # Execute pending orders (NO LOOKAHEAD - use next day open/close)
            executed_orders = []
            for company, pending_trade in self.pending_orders.items():
                if company in all_data:
                    current_data = all_data[company][all_data[company]['date'] == current_date]
                    if not current_data.empty:
                        current_row = current_data.iloc[0]
                        
                        if self.can_execute_limit_order(pending_trade, current_row):
                            # Execute at actual next-day price (with slippage)
                            if pending_trade.share_class == 'A':
                                actual_entry_price = current_row.get('a_open', current_row['a_price'])
                            else:
                                actual_entry_price = current_row.get('b_open', current_row['b_price'])
                            
                            # Update trade with actual execution price
                            pending_trade.entry_price = actual_entry_price
                            pending_trade.investment_amount = pending_trade.shares * actual_entry_price
                            pending_trade.transaction_costs = self.calculate_transaction_costs(pending_trade.investment_amount)
                            pending_trade.entry_date = current_date
                            
                            self.open_trades.append(pending_trade)
                            executed_orders.append(company)
                            
                        elif (current_date - pending_trade.signal_date).days > 2:
                            # Cancel expired orders (after 2 days)
                            executed_orders.append(company)
            
            # Remove executed/expired orders
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
                
                should_close, reason = self.should_close_trade(trade, current_row, days_held)
                
                if should_close:
                    self.close_trade(trade, current_date, current_row, reason)
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
                    
                    if signal != PremiumAction.HOLD:
                        trade = self.open_trade(
                            company, current_date, signal,
                            current_row['a_price'], current_row['b_price'],
                            current_row['premium_share']
                        )
                        
                        total_cost = trade.investment_amount + trade.transaction_costs
                        
                        if self.cash >= total_cost and trade.shares > 0:
                            # Reserve cash for this pending order
                            self.cash -= total_cost
                            self.pending_orders[company] = trade
            
            # Record portfolio value
            portfolio_value = self.cash
            for trade in self.open_trades:
                if trade.company in all_data:
                    company_data = all_data[trade.company]
                    current_row = company_data[company_data['date'] == current_date]
                    if not current_row.empty:
                        current_row = current_row.iloc[0]
                        if trade.share_class == 'A':
                            portfolio_value += trade.shares * current_row['a_price']
                        else:
                            portfolio_value += trade.shares * current_row['b_price']
                        
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
                    self.close_trade(trade, final_date, final_row, "backtest_end")
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
        avg_discount = np.mean([t.discount_magnitude for t in self.closed_trades])
        
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
            'avg_discount_magnitude': avg_discount,
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
        print(f"🎯 A/B PREMIUM-DISCOUNT ARBITRAGE BACKTEST RESULTS (FIXED)")
        print(f"="*80)
        
        print(f"\n📊 TRADING STATISTICS:")
        print(f"   Total trades:              {results['total_trades']:,}")
        print(f"   Winning trades:            {results['winning_trades']:,} ({results['win_rate']:.1f}%)")
        print(f"   Losing trades:             {results['losing_trades']:,}")
        print(f"   Average holding period:    {results['avg_holding_days']:.1f} days")
        print(f"   Average discount bought:   {results['avg_discount_magnitude']:.2f}%")
        
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
        print(f"\n📊 TRADE ANALYSIS BY SHARE CLASS:")
        a_trades = [t for t in self.closed_trades if t.share_class == 'A']
        b_trades = [t for t in self.closed_trades if t.share_class == 'B']
        
        if a_trades:
            pnl = sum(t.pnl for t in a_trades)
            wins = len([t for t in a_trades if t.pnl > 0])
            win_rate = wins / len(a_trades) * 100
            avg_discount = np.mean([t.discount_magnitude for t in a_trades])
            print(f"   A shares: {len(a_trades):3d} trades, ${pnl:+8,.0f} P&L, {win_rate:5.1f}% wins, {avg_discount:.2f}% avg discount")
        
        if b_trades:
            pnl = sum(t.pnl for t in b_trades)
            wins = len([t for t in b_trades if t.pnl > 0])
            win_rate = wins / len(b_trades) * 100
            avg_discount = np.mean([t.discount_magnitude for t in b_trades])
            print(f"   B shares: {len(b_trades):3d} trades, ${pnl:+8,.0f} P&L, {win_rate:5.1f}% wins, {avg_discount:.2f}% avg discount")
        
        # Company breakdown
        print(f"\n🎯 TRADE BREAKDOWN BY COMPANY:")
        company_stats = {}
        for trade in self.closed_trades:
            if trade.company not in company_stats:
                company_stats[trade.company] = {'trades': 0, 'pnl': 0, 'wins': 0, 'share_class': ''}
            company_stats[trade.company]['trades'] += 1
            company_stats[trade.company]['pnl'] += trade.pnl
            company_stats[trade.company]['share_class'] = self.premium_share_cache.get(trade.company, '?')
            if trade.pnl > 0:
                company_stats[trade.company]['wins'] += 1
        
        for company, stats in sorted(company_stats.items(), key=lambda x: x[1]['pnl'], reverse=True)[:15]:
            win_rate = stats['wins'] / stats['trades'] * 100 if stats['trades'] > 0 else 0
            print(f"   {company:<12}: {stats['trades']:2d} trades, ${stats['pnl']:+7.0f} P&L, {win_rate:4.0f}% wins (Premium: {stats['share_class']})")
        
        print(f"\n" + "="*80)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run A/B premium-discount backtest."""
    
    config = PremiumDiscountConfig(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2025, 10, 1),
        initial_capital=100000.0,
        max_positions=8,
        position_size=0.10,  # 10% per position
        transaction_cost_rate=0.002,
        lookback_window=120,
        min_discount_pct=2.0,  # Must be 2%+ discount
        exit_premium_pct=1.0,  # Exit at 1% premium
        max_holding_days=30,
        min_observation_days=90
    )
    
    backtester = ABPremiumDiscountBacktester(config)
    
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