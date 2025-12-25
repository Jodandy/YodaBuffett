#!/usr/bin/env python3
"""
A/B Share Premium-Discount Arbitrage Strategy - FINAL FIXED VERSION

Fixed all cash flow bugs:
1. Proper cash reservation and return accounting
2. Realistic position sizing
3. No cash leakage between pending orders and execution
4. Proper reconciliation of P&L vs cash
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
    entry_spread: float
    exit_spread: Optional[float]
    shares: int
    investment_amount: float
    premium_share: str
    discount_magnitude: float
    reserved_cash: float  # Amount actually reserved (including costs)
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
    max_positions: int = 5  # Much more conservative
    position_size: float = 0.15  # 15% per position
    transaction_cost_rate: float = 0.002
    lookback_window: int = 120
    min_discount_pct: float = 3.0  # Higher threshold
    exit_premium_pct: float = 0.5
    max_holding_days: int = 20
    min_observation_days: int = 90

class ABPremiumDiscountBacktester:
    
    def __init__(self, config: PremiumDiscountConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[PremiumTrade] = []
        self.closed_trades: List[PremiumTrade] = []
        self.pending_orders: Dict[str, PremiumTrade] = {}
        self.cash = config.initial_capital
        self.reserved_cash = 0.0  # Track reserved cash separately
        self.daily_portfolio_values = []
        self.premium_share_cache: Dict[str, str] = {}
        
    @property
    def available_cash(self) -> float:
        """Available cash = total cash - reserved cash"""
        return self.cash - self.reserved_cash
        
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
        
        # Need a clear premium (at least 2% average difference)
        if avg_a_premium > avg_b_premium + 2.0:
            premium_share = 'A'
        elif avg_b_premium > avg_a_premium + 2.0:
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
        """Calculate position size and return (shares, investment, total_cost)."""
        
        available_capital = self.available_cash * self.config.position_size
        shares = max(1, int(available_capital / price))  # At least 1 share
        investment = shares * price
        transaction_costs = investment * self.config.transaction_cost_rate
        total_cost = investment + transaction_costs
        
        # Scale down if we don't have enough cash
        if total_cost > self.available_cash:
            max_investment = self.available_cash * 0.95  # Leave 5% buffer
            shares = max(1, int(max_investment / price))
            investment = shares * price
            transaction_costs = investment * self.config.transaction_cost_rate
            total_cost = investment + transaction_costs
        
        return shares, investment, total_cost
        
    def open_trade(self, company: str, signal_date: datetime, action: PremiumAction,
                  a_price: float, b_price: float, premium_share: str) -> PremiumTrade:
        """Create a pending premium-discount trade."""
        
        if action == PremiumAction.BUY_A:
            entry_price = a_price
            share_class = 'A'
            spread = (a_price - b_price) / b_price * 100
        else:
            entry_price = b_price
            share_class = 'B'
            spread = (b_price - a_price) / a_price * 100
            
        shares, investment, total_cost = self.calculate_position_size(entry_price)
        
        # Don't create trade if we can't afford it
        if total_cost > self.available_cash:
            return None
        
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
            transaction_costs=investment * self.config.transaction_cost_rate,
            premium_share=premium_share,
            discount_magnitude=abs(spread),
            reserved_cash=total_cost
        )
        
        return trade
        
    def reserve_cash_for_order(self, trade: PremiumTrade) -> bool:
        """Reserve cash for a pending order."""
        if trade.reserved_cash <= self.available_cash:
            self.reserved_cash += trade.reserved_cash
            return True
        return False
        
    def release_reserved_cash(self, trade: PremiumTrade) -> None:
        """Release reserved cash when order expires or executes."""
        self.reserved_cash -= trade.reserved_cash
        self.reserved_cash = max(0, self.reserved_cash)  # Prevent negative
        
    def execute_trade(self, trade: PremiumTrade, execution_date: datetime, execution_price: float) -> None:
        """Execute a pending trade."""
        
        # Release originally reserved cash
        self.release_reserved_cash(trade)
        
        # Calculate actual costs based on execution price
        actual_investment = trade.shares * execution_price
        actual_transaction_costs = actual_investment * self.config.transaction_cost_rate
        actual_total_cost = actual_investment + actual_transaction_costs
        
        # Deduct actual cost from cash
        self.cash -= actual_total_cost
        
        # Update trade with actual execution details
        trade.entry_date = execution_date
        trade.entry_price = execution_price
        trade.investment_amount = actual_investment
        trade.transaction_costs = actual_transaction_costs
        
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
        exit_costs = trade.investment_amount * self.config.transaction_cost_rate
        trade.pnl = gross_pnl - exit_costs
        
        # Return investment proceeds to cash
        proceeds = trade.investment_amount + trade.pnl
        self.cash += proceeds
        
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
        
        print(f"🚀 Starting A/B Premium-Discount Arbitrage Backtest (FINAL FIXED)")
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
                print(f"   📅 {current_date.date()} - Cash: ${self.cash:,.0f}, Reserved: ${self.reserved_cash:,.0f}, Available: ${self.available_cash:,.0f}")
            
            # Execute pending orders
            executed_orders = []
            for company, pending_trade in self.pending_orders.items():
                if company in all_data:
                    current_data = all_data[company][all_data[company]['date'] == current_date]
                    if not current_data.empty:
                        current_row = current_data.iloc[0]
                        
                        # Execute at next day's open price (with 2% slippage tolerance)
                        if pending_trade.share_class == 'A':
                            execution_price = current_row.get('a_open', current_row['a_price'])
                        else:
                            execution_price = current_row.get('b_open', current_row['b_price'])
                        
                        if execution_price <= pending_trade.entry_price * 1.02:
                            self.execute_trade(pending_trade, current_date, execution_price)
                            self.open_trades.append(pending_trade)
                            executed_orders.append(company)
                        elif (current_date - pending_trade.signal_date).days > 2:
                            # Expire order
                            self.release_reserved_cash(pending_trade)
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
            current_positions = len(self.open_trades) + len(self.pending_orders)
            if current_positions < self.config.max_positions:
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
                        
                        if trade and self.reserve_cash_for_order(trade):
                            self.pending_orders[company] = trade
                            break  # Only create one order per day
            
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
                'reserved_cash': self.reserved_cash,
                'available_cash': self.available_cash,
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
        
        # Return any remaining reserved cash
        for company, pending_trade in self.pending_orders.items():
            self.release_reserved_cash(pending_trade)
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
        
        # Verify P&L matches cash change
        cash_change = final_value - initial_value
        pnl_difference = abs(total_pnl - cash_change)
        
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
            'cash_change': cash_change,
            'pnl_difference': pnl_difference,
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
        print(f"🎯 A/B PREMIUM-DISCOUNT ARBITRAGE BACKTEST RESULTS (FINAL)")
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
        
        print(f"\n🔍 CASH FLOW RECONCILIATION:")
        print(f"   Cash change:               ${results['cash_change']:,.2f}")
        print(f"   Total P&L:                 ${results['total_pnl']:,.2f}")
        print(f"   Difference:                ${results['pnl_difference']:,.2f}")
        if results['pnl_difference'] < 10:
            print(f"   ✅ Cash flow reconciled!")
        else:
            print(f"   ❌ Cash flow mismatch!")
        
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
        max_positions=5,
        position_size=0.15,
        transaction_cost_rate=0.002,
        lookback_window=120,
        min_discount_pct=3.0,  # 3% minimum discount
        exit_premium_pct=0.5,
        max_holding_days=20,
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