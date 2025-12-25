#!/usr/bin/env python3
"""
A/B Share Premium-Discount Arbitrage Strategy - FULLY RECONCILED VERSION

Fixed all cash flow issues with detailed transaction tracking.
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
class CashTransaction:
    """Track every cash movement for reconciliation"""
    date: datetime
    type: str  # 'order_reserve', 'order_release', 'trade_open', 'trade_close'
    amount: float
    description: str
    balance_after: float

@dataclass
class PremiumTrade:
    signal_date: datetime
    entry_date: Optional[datetime]
    exit_date: Optional[datetime]
    company: str
    action: PremiumAction
    share_class: str
    entry_price: float
    exit_price: Optional[float]
    entry_spread: float
    exit_spread: Optional[float]
    shares: int
    investment_amount: float
    premium_share: str
    discount_magnitude: float
    entry_transaction_cost: float
    exit_transaction_cost: float
    pnl: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True
    exit_reason: Optional[str] = None

@dataclass
class PremiumDiscountConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    max_positions: int = 4  # Even more conservative
    position_size: float = 0.20  # 20% per position 
    transaction_cost_rate: float = 0.002
    lookback_window: int = 120
    min_discount_pct: float = 4.0  # Higher threshold
    exit_premium_pct: float = 0.5
    max_holding_days: int = 15  # Shorter holding
    min_observation_days: int = 90

class ABPremiumDiscountBacktester:
    
    def __init__(self, config: PremiumDiscountConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[PremiumTrade] = []
        self.closed_trades: List[PremiumTrade] = []
        self.pending_orders: Dict[str, PremiumTrade] = {}
        self.cash = config.initial_capital
        self.daily_portfolio_values = []
        self.premium_share_cache: Dict[str, str] = {}
        self.cash_transactions: List[CashTransaction] = []
        
        # Record initial cash
        self.record_cash_transaction(
            self.config.start_date, 
            'initial_capital', 
            config.initial_capital, 
            'Starting capital'
        )
        
    def record_cash_transaction(self, date: datetime, tx_type: str, amount: float, description: str):
        """Record every cash movement for reconciliation."""
        self.cash_transactions.append(CashTransaction(
            date=date,
            type=tx_type,
            amount=amount,
            description=description,
            balance_after=self.cash
        ))
        
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
        
        # Flatten column names
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
        
        # Remove any rows with zero or negative prices
        result_df = result_df[(result_df['a_price'] > 0) & (result_df['b_price'] > 0)]
        
        if len(result_df) == 0:
            return pd.DataFrame()
            
        # Calculate spreads
        result_df['a_premium'] = (result_df['a_price'] - result_df['b_price']) / result_df['b_price'] * 100
        result_df['b_premium'] = (result_df['b_price'] - result_df['a_price']) / result_df['a_price'] * 100
        
        return result_df.dropna()
    
    def determine_premium_share(self, data: pd.DataFrame, company: str) -> Optional[str]:
        """Determine which share class typically trades at a premium."""
        
        if len(data) < self.config.min_observation_days:
            return None
            
        recent_data = data.tail(self.config.lookback_window)
        
        avg_a_premium = recent_data['a_premium'].mean()
        avg_b_premium = recent_data['b_premium'].mean()
        
        # Need a clear premium (at least 3% average difference)
        if avg_a_premium > avg_b_premium + 3.0:
            premium_share = 'A'
        elif avg_b_premium > avg_a_premium + 3.0:
            premium_share = 'B'
        else:
            return None
            
        self.premium_share_cache[company] = premium_share
        return premium_share
        
    def generate_signals(self, data: pd.DataFrame, company: str) -> pd.DataFrame:
        """Generate premium-discount arbitrage signals."""
        
        signals = []
        premium_share = self.determine_premium_share(data, company)
        
        if premium_share is None:
            data['signal'] = [PremiumAction.HOLD] * len(data)
            data['premium_share'] = 'NONE'
            return data
        
        for _, row in data.iterrows():
            a_premium = row['a_premium']
            b_premium = row['b_premium']
            
            # Skip if data looks suspicious
            if abs(a_premium) > 50 or abs(b_premium) > 50:
                action = PremiumAction.HOLD
            elif premium_share == 'A':
                if a_premium <= -self.config.min_discount_pct:
                    action = PremiumAction.BUY_A
                else:
                    action = PremiumAction.HOLD
            else:
                if b_premium <= -self.config.min_discount_pct:
                    action = PremiumAction.BUY_B
                else:
                    action = PremiumAction.HOLD
                    
            signals.append(action)
            
        data['signal'] = signals
        data['premium_share'] = premium_share
        return data
        
    def calculate_position_size(self, price: float) -> Tuple[int, float, float]:
        """Calculate position size and return (shares, investment, entry_cost)."""
        
        # Use simpler calculation
        max_investment = self.cash * self.config.position_size
        shares = max(1, int(max_investment / price))
        investment = shares * price
        entry_cost = investment * self.config.transaction_cost_rate
        
        # Ensure we don't exceed available cash
        total_needed = investment + entry_cost
        if total_needed > self.cash:
            # Scale down
            max_total = self.cash * 0.95
            investment = max_total / (1 + self.config.transaction_cost_rate)
            shares = max(1, int(investment / price))
            investment = shares * price
            entry_cost = investment * self.config.transaction_cost_rate
        
        return shares, investment, entry_cost
        
    def open_trade(self, company: str, signal_date: datetime, action: PremiumAction,
                  a_price: float, b_price: float, premium_share: str) -> Optional[PremiumTrade]:
        """Execute trade immediately (no pending orders to avoid cash flow issues)."""
        
        if action == PremiumAction.BUY_A:
            entry_price = a_price
            share_class = 'A'
            spread = (a_price - b_price) / b_price * 100
        else:
            entry_price = b_price
            share_class = 'B'
            spread = (b_price - a_price) / a_price * 100
            
        shares, investment, entry_cost = self.calculate_position_size(entry_price)
        
        # Check if we have enough cash
        total_cost = investment + entry_cost
        if total_cost > self.cash:
            return None
        
        # Deduct cash immediately
        self.cash -= total_cost
        self.record_cash_transaction(
            signal_date, 'trade_open', -total_cost, 
            f"Open {company} {share_class}: {shares} shares @ ${entry_price:.3f}"
        )
        
        trade = PremiumTrade(
            signal_date=signal_date,
            entry_date=signal_date,  # Execute immediately
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
            premium_share=premium_share,
            discount_magnitude=abs(spread),
            entry_transaction_cost=entry_cost,
            exit_transaction_cost=0.0  # Will be calculated at exit
        )
        
        return trade
        
    def close_trade(self, trade: PremiumTrade, date: datetime, 
                   current_data: pd.Series, reason: str = "signal") -> float:
        """Close a trade and calculate P&L."""
        
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
        
        # Calculate exit transaction cost
        exit_cost = trade.investment_amount * self.config.transaction_cost_rate
        trade.exit_transaction_cost = exit_cost
        
        # Calculate P&L
        gross_proceeds = trade.shares * exit_price
        net_proceeds = gross_proceeds - exit_cost
        trade.pnl = net_proceeds - trade.investment_amount
        
        # Add proceeds back to cash
        self.cash += net_proceeds
        self.record_cash_transaction(
            date, 'trade_close', net_proceeds,
            f"Close {trade.company} {trade.share_class}: {trade.shares} shares @ ${exit_price:.3f}, P&L: ${trade.pnl:.2f}"
        )
        
        return trade.pnl
        
    def should_close_trade(self, trade: PremiumTrade, current_data: pd.Series, days_held: int) -> Tuple[bool, str]:
        """Determine if a trade should be closed."""
        
        if trade.share_class == 'A':
            current_spread = current_data.get('a_premium', 0)
        else:
            current_spread = current_data.get('b_premium', 0)
            
        if current_spread >= self.config.exit_premium_pct:
            return True, "back_to_premium"
            
        if days_held >= self.config.max_holding_days:
            return True, "max_holding_period"
            
        return False, ""
        
    async def run_backtest(self) -> Dict:
        """Run the complete premium-discount backtest."""
        
        print(f"🚀 Starting A/B Premium-Discount Arbitrage Backtest (RECONCILED)")
        print(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        print(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        print(f"   📊 Min discount: {self.config.min_discount_pct}%, Max positions: {self.config.max_positions}")
        
        # Get A/B pairs
        companies = await self.get_ab_pairs()
        
        if not companies:
            return {}
            
        # Load historical data
        all_data = {}
        premium_shares = {}
        
        for company in companies:
            data = await self.get_historical_prices(
                company, 
                self.config.start_date - timedelta(days=self.config.lookback_window + 60), 
                self.config.end_date
            )
            
            if len(data) > self.config.min_observation_days:
                signals_data = self.generate_signals(data, company)
                
                if signals_data['premium_share'].iloc[0] != 'NONE':
                    all_data[company] = signals_data
                    premium_shares[company] = self.premium_share_cache.get(company, 'NONE')
        
        if not all_data:
            return {}
            
        print(f"📊 Trading {len(all_data)} pairs with clear premium patterns")
        
        # Get all dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation loop
        for i, current_date in enumerate(all_dates):
            
            if i % 200 == 0:
                print(f"   📅 {current_date.date()} - Cash: ${self.cash:,.0f}, Trades: {len(self.open_trades)}")
            
            # Check exit conditions first
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
            if len(self.open_trades) < self.config.max_positions:
                for company, data in all_data.items():
                    if any(t.company == company for t in self.open_trades):
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
                        
                        if trade:
                            self.open_trades.append(trade)
                            break  # Only one trade per day
            
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
        
        self.open_trades = []
        
        return self.analyze_results()
        
    def analyze_results(self) -> Dict:
        """Analyze backtest results with full reconciliation."""
        
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
        
        # Calculate total transaction costs
        total_transaction_costs = sum(t.entry_transaction_cost + t.exit_transaction_cost for t in self.closed_trades)
        
        # Portfolio performance
        final_value = self.cash
        initial_value = self.config.initial_capital
        total_return = (final_value - initial_value) / initial_value * 100
        
        # Detailed cash flow reconciliation
        cash_from_transactions = sum(tx.amount for tx in self.cash_transactions)
        expected_final_cash = self.config.initial_capital + cash_from_transactions
        cash_difference = abs(self.cash - expected_final_cash)
        
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
            'total_transaction_costs': total_transaction_costs,
            'total_return': total_return,
            'final_portfolio_value': final_value,
            'cash_from_transactions': cash_from_transactions,
            'expected_final_cash': expected_final_cash,
            'cash_difference': cash_difference,
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
        print(f"🎯 A/B PREMIUM-DISCOUNT ARBITRAGE BACKTEST RESULTS (RECONCILED)")
        print(f"="*80)
        
        print(f"\n📊 TRADING STATISTICS:")
        print(f"   Total trades:              {results['total_trades']:,}")
        print(f"   Winning trades:            {results['winning_trades']:,} ({results['win_rate']:.1f}%)")
        print(f"   Losing trades:             {results['losing_trades']:,}")
        print(f"   Average holding period:    {results['avg_holding_days']:.1f} days")
        print(f"   Average discount bought:   {results['avg_discount_magnitude']:.2f}%")
        
        print(f"\n💰 P&L ANALYSIS:")
        print(f"   Total P&L:                 ${results['total_pnl']:,.2f}")
        print(f"   Total transaction costs:   ${results['total_transaction_costs']:,.2f}")
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
        
        print(f"\n🔍 CASH FLOW RECONCILIATION:")
        print(f"   Sum of all transactions:   ${results['cash_from_transactions']:,.2f}")
        print(f"   Expected final cash:       ${results['expected_final_cash']:,.2f}")
        print(f"   Actual final cash:         ${results['final_portfolio_value']:,.2f}")
        print(f"   Difference:                ${results['cash_difference']:,.2f}")
        if results['cash_difference'] < 1.0:
            print(f"   ✅ Perfect cash flow reconciliation!")
        else:
            print(f"   ❌ Cash flow mismatch!")
        
        print(f"\n" + "="*80)
        
        # Print detailed trade analysis
        self.print_detailed_trades()
        
        # Print some sample transactions
        print(f"\n📋 SAMPLE CASH TRANSACTIONS:")
        for tx in self.cash_transactions[:5]:
            print(f"   {tx.date.date()} {tx.type:15} ${tx.amount:+8,.0f} - {tx.description}")
        if len(self.cash_transactions) > 5:
            print(f"   ... ({len(self.cash_transactions)} total transactions)")
            
    def print_detailed_trades(self):
        """Print detailed analysis of all trades sorted by P&L."""
        
        if not self.closed_trades:
            return
            
        print(f"\n" + "="*120)
        print(f"📋 DETAILED TRADE ANALYSIS ({len(self.closed_trades)} trades)")
        print(f"="*120)
        
        # Sort trades by P&L (biggest winners first)
        sorted_trades = sorted(self.closed_trades, key=lambda x: x.pnl, reverse=True)
        
        print(f"{'#':<3} {'Company':<8} {'Type':<6} {'Entry':<10} {'Exit':<10} {'Days':<4} {'Entry $':<8} {'Exit $':<8} {'Discount':<8} {'P&L':<10} {'Return':<7} {'Reason':<12}")
        print(f"{'-'*120}")
        
        for i, trade in enumerate(sorted_trades, 1):
            # Calculate return percentage
            return_pct = (trade.pnl / trade.investment_amount) * 100 if trade.investment_amount > 0 else 0
            
            # Format P&L with color coding (simplified for text)
            pnl_str = f"${trade.pnl:+,.0f}"
            if trade.pnl > 5000:
                pnl_str = f"🟢{pnl_str}"  # Big winner
            elif trade.pnl > 1000:
                pnl_str = f"🔵{pnl_str}"  # Good winner
            elif trade.pnl < -1000:
                pnl_str = f"🔴{pnl_str}"  # Big loser
            elif trade.pnl < 0:
                pnl_str = f"🟡{pnl_str}"  # Small loser
                
            # Truncate exit reason
            reason = (trade.exit_reason[:10] + '..') if len(trade.exit_reason) > 10 else trade.exit_reason
            
            print(f"{i:<3} {trade.company:<8} {trade.share_class:<6} {trade.entry_date.strftime('%Y-%m-%d'):<10} "
                  f"{trade.exit_date.strftime('%Y-%m-%d'):<10} {trade.holding_days:<4} "
                  f"${trade.entry_price:<7.3f} ${trade.exit_price:<7.3f} {trade.discount_magnitude:<7.2f}% "
                  f"{pnl_str:<10} {return_pct:+6.1f}% {reason:<12}")
        
        # Summary statistics
        print(f"\n📊 TRADE STATISTICS BY P&L RANGES:")
        
        huge_winners = [t for t in self.closed_trades if t.pnl > 10000]
        big_winners = [t for t in self.closed_trades if 5000 < t.pnl <= 10000]
        good_winners = [t for t in self.closed_trades if 1000 < t.pnl <= 5000]
        small_winners = [t for t in self.closed_trades if 0 < t.pnl <= 1000]
        small_losers = [t for t in self.closed_trades if -1000 <= t.pnl < 0]
        big_losers = [t for t in self.closed_trades if t.pnl < -1000]
        
        print(f"   🟢 Huge winners (>$10K):   {len(huge_winners):3d} trades, ${sum(t.pnl for t in huge_winners):8,.0f} total")
        print(f"   🔵 Big winners ($5-10K):   {len(big_winners):3d} trades, ${sum(t.pnl for t in big_winners):8,.0f} total")
        print(f"   🟦 Good winners ($1-5K):   {len(good_winners):3d} trades, ${sum(t.pnl for t in good_winners):8,.0f} total")
        print(f"   ⚪ Small winners (<$1K):   {len(small_winners):3d} trades, ${sum(t.pnl for t in small_winners):8,.0f} total")
        print(f"   🟡 Small losers (>-$1K):   {len(small_losers):3d} trades, ${sum(t.pnl for t in small_losers):8,.0f} total")
        print(f"   🔴 Big losers (<-$1K):     {len(big_losers):3d} trades, ${sum(t.pnl for t in big_losers):8,.0f} total")
        
        # Top 10 biggest winners analysis
        print(f"\n🏆 TOP 10 BIGGEST WINNERS:")
        for i, trade in enumerate(sorted_trades[:10], 1):
            return_pct = (trade.pnl / trade.investment_amount) * 100
            price_change = ((trade.exit_price - trade.entry_price) / trade.entry_price) * 100
            print(f"   #{i:2d}: {trade.company} {trade.share_class} - ${trade.pnl:8,.0f} ({return_pct:+5.1f}%) "
                  f"Entry: {trade.entry_date.strftime('%Y-%m-%d')} @ ${trade.entry_price:.3f}, "
                  f"Exit: {trade.exit_date.strftime('%Y-%m-%d')} @ ${trade.exit_price:.3f} ({price_change:+5.1f}%)")
        
        # Top 5 biggest losers analysis  
        print(f"\n📉 TOP 5 BIGGEST LOSERS:")
        worst_trades = sorted(self.closed_trades, key=lambda x: x.pnl)[:5]
        for i, trade in enumerate(worst_trades, 1):
            return_pct = (trade.pnl / trade.investment_amount) * 100
            price_change = ((trade.exit_price - trade.entry_price) / trade.entry_price) * 100
            print(f"   #{i:2d}: {trade.company} {trade.share_class} - ${trade.pnl:8,.0f} ({return_pct:+5.1f}%) "
                  f"Entry: {trade.entry_date.strftime('%Y-%m-%d')} @ ${trade.entry_price:.3f}, "
                  f"Exit: {trade.exit_date.strftime('%Y-%m-%d')} @ ${trade.exit_price:.3f} ({price_change:+5.1f}%)")
                  
        # Company performance breakdown
        print(f"\n🏢 PERFORMANCE BY COMPANY:")
        company_stats = {}
        for trade in self.closed_trades:
            if trade.company not in company_stats:
                company_stats[trade.company] = {'trades': 0, 'total_pnl': 0, 'wins': 0}
            company_stats[trade.company]['trades'] += 1
            company_stats[trade.company]['total_pnl'] += trade.pnl
            if trade.pnl > 0:
                company_stats[trade.company]['wins'] += 1
        
        # Sort by total P&L
        sorted_companies = sorted(company_stats.items(), key=lambda x: x[1]['total_pnl'], reverse=True)
        
        for company, stats in sorted_companies[:15]:
            win_rate = (stats['wins'] / stats['trades']) * 100 if stats['trades'] > 0 else 0
            avg_pnl = stats['total_pnl'] / stats['trades'] if stats['trades'] > 0 else 0
            premium_share = self.premium_share_cache.get(company, '?')
            print(f"   {company:<8}: {stats['trades']:2d} trades, ${stats['total_pnl']:+8,.0f} total, "
                  f"{win_rate:4.0f}% wins, ${avg_pnl:+6,.0f} avg, Premium: {premium_share}")
                  
        # Discount analysis
        print(f"\n📊 DISCOUNT ANALYSIS:")
        discount_ranges = {
            'Extreme (>15%)': [t for t in self.closed_trades if t.discount_magnitude > 15],
            'Very High (10-15%)': [t for t in self.closed_trades if 10 < t.discount_magnitude <= 15],
            'High (7-10%)': [t for t in self.closed_trades if 7 < t.discount_magnitude <= 10],
            'Moderate (4-7%)': [t for t in self.closed_trades if 4 < t.discount_magnitude <= 7]
        }
        
        for range_name, trades in discount_ranges.items():
            if trades:
                total_pnl = sum(t.pnl for t in trades)
                win_rate = (len([t for t in trades if t.pnl > 0]) / len(trades)) * 100
                avg_return = np.mean([(t.pnl / t.investment_amount) * 100 for t in trades])
                print(f"   {range_name:<18}: {len(trades):3d} trades, ${total_pnl:8,.0f} P&L, "
                      f"{win_rate:4.0f}% wins, {avg_return:+5.1f}% avg return")
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run A/B premium-discount backtest."""
    
    config = PremiumDiscountConfig(
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 10, 1),
        initial_capital=100000.0,
        max_positions=4,
        position_size=0.20,
        transaction_cost_rate=0.002,
        lookback_window=120,
        min_discount_pct=4.0,  # 4% minimum discount
        exit_premium_pct=0.5,
        max_holding_days=15,
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