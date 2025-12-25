#!/usr/bin/env python3
"""
Fundamental Value Strategy - IMPROVED VERSION

A more robust fundamental strategy that:
1. Uses normalized scoring to prevent calculation errors
2. Focuses on clear value metrics (P/B, P/E, revenue growth)
3. Uses proper statistical methods for anomaly detection
4. Implements better exit strategies

Key improvements:
- Fixed composite scoring with proper normalization
- Better exit strategy to reduce holding period losses
- Focus on proven value metrics
- More conservative position sizing
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

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class ValueSignal(Enum):
    STRONG_BUY = "strong_value_opportunity"
    BUY = "value_opportunity"
    HOLD = "hold"
    SELL = "overvalued"

@dataclass
class ValueScore:
    """Individual value metric scores (0-5 scale)"""
    pb_score: float = 0.0          # Lower P/B is better (0-5)
    revenue_growth_score: float = 0.0  # Higher revenue growth (0-5)
    cash_score: float = 0.0        # Higher cash position (0-5)
    momentum_score: float = 0.0    # Recent price momentum (0-5)
    
    @property
    def composite_score(self) -> float:
        """Simple average of all scores"""
        return (self.pb_score + self.revenue_growth_score + self.cash_score + self.momentum_score) / 4

@dataclass
class ValueTrade:
    signal_date: datetime
    entry_date: Optional[datetime]
    exit_date: Optional[datetime]
    symbol: str
    entry_price: float
    exit_price: Optional[float]
    shares: int
    investment_amount: float
    entry_scores: ValueScore
    entry_pb: float
    entry_revenue_ps: float
    trailing_stop_price: Optional[float] = None
    pnl: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True
    exit_reason: Optional[str] = None

@dataclass
class ValueConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    max_positions: int = 8
    position_size: float = 0.12  # 12% per position
    transaction_cost_rate: float = 0.002
    
    # Lookback periods
    short_lookback: int = 60   # 2 months
    long_lookback: int = 120   # 4 months
    min_history_days: int = 150
    
    # Signal thresholds (0-5 scale)
    strong_signal_threshold: float = 3.5
    buy_signal_threshold: float = 2.5
    exit_threshold: float = 1.5
    
    # Risk management
    max_holding_days: int = 60  # Shorter holding
    trailing_stop_pct: float = 0.15  # 15% trailing stop
    profit_target_pct: float = 0.25   # 25% profit target

class FundamentalValueStrategy:
    
    def __init__(self, config: ValueConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[ValueTrade] = []
        self.closed_trades: List[ValueTrade] = []
        self.pending_orders: Dict[str, ValueTrade] = {}
        self.cash = config.initial_capital
        self.daily_portfolio_values = []
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_quality_companies(self) -> List[str]:
        """Get companies with high-quality fundamental data."""
        query = """
        SELECT 
            symbol,
            COUNT(*) as total_records,
            COUNT(pb_ratio) as pb_records,
            COUNT(revenue_per_share) as revenue_records,
            COUNT(cash_per_share) as cash_records,
            COUNT(close_price) as price_records,
            AVG(pb_ratio) as avg_pb,
            AVG(revenue_per_share) as avg_revenue
        FROM historical_fundamentals_daily
        WHERE date >= $1
        AND pb_ratio > 0 AND pb_ratio < 50  -- Reasonable P/B range
        AND revenue_per_share > 0           -- Must have revenue
        AND cash_per_share >= 0             -- Non-negative cash
        AND close_price > 5                 -- Avoid penny stocks
        GROUP BY symbol
        HAVING COUNT(*) >= $2
        AND COUNT(pb_ratio) >= $3
        AND COUNT(revenue_per_share) >= $3
        AND AVG(revenue_per_share) > 0
        ORDER BY COUNT(*) DESC, AVG(revenue_per_share) DESC
        """
        
        min_date = self.config.start_date - timedelta(days=self.config.min_history_days)
        min_records = self.config.min_history_days
        min_coverage = int(min_records * 0.8)
        
        records = await self.db_conn.fetch(query, min_date.date(), min_records, min_coverage)
        
        companies = [record['symbol'] for record in records]
        print(f"📊 Found {len(companies)} quality companies for value investing")
        return companies[:30]  # Top 30 quality companies
        
    async def get_company_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get company data with price information."""
        
        # Get fundamental data
        fundamental_query = """
        SELECT 
            date,
            pb_ratio,
            revenue_per_share,
            cash_per_share,
            book_value_per_share,
            close_price
        FROM historical_fundamentals_daily
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        # Get price data for momentum
        price_query = """
        SELECT 
            date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        fundamental_records = await self.db_conn.fetch(fundamental_query, symbol, start_date.date(), end_date.date())
        price_records = await self.db_conn.fetch(price_query, symbol, start_date.date(), end_date.date())
        
        if not fundamental_records or not price_records:
            return pd.DataFrame()
            
        # Convert to DataFrames
        fund_df = pd.DataFrame([dict(r) for r in fundamental_records])
        price_df = pd.DataFrame([dict(r) for r in price_records])
        
        # Convert dates
        fund_df['date'] = pd.to_datetime(fund_df['date'])
        price_df['date'] = pd.to_datetime(price_df['date'])
        
        # Merge on date
        df = pd.merge(fund_df, price_df[['date', 'open_price', 'high_price', 'low_price', 'volume']], 
                     on='date', how='inner', suffixes=('_fund', '_price'))
        
        if df.empty:
            return pd.DataFrame()
            
        # Clean data
        numeric_cols = ['pb_ratio', 'revenue_per_share', 'cash_per_share', 'book_value_per_share', 
                       'close_price', 'open_price', 'high_price', 'low_price', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # Filter reasonable data
        df = df[
            (df['pb_ratio'] > 0) & (df['pb_ratio'] < 50) &
            (df['revenue_per_share'] > 0) &
            (df['close_price'] > 0)
        ].dropna()
        
        return df
        
    def calculate_value_scores(self, data: pd.DataFrame, current_idx: int) -> ValueScore:
        """Calculate normalized value scores (0-5 scale)."""
        
        if current_idx < self.config.long_lookback:
            return ValueScore()
            
        current_row = data.iloc[current_idx]
        lookback_window = data.iloc[max(0, current_idx - self.config.long_lookback):current_idx]
        
        scores = ValueScore()
        
        # 1. P/B Score (lower is better, 0-5 scale)
        if not pd.isna(current_row['pb_ratio']) and len(lookback_window) > 0:
            pb_percentile = (lookback_window['pb_ratio'] <= current_row['pb_ratio']).mean()
            scores.pb_score = (1 - pb_percentile) * 5  # Invert so lower P/B = higher score
            
        # 2. Revenue Growth Score (0-5 scale)
        if not pd.isna(current_row['revenue_per_share']) and len(lookback_window) >= 30:
            recent_revenue = data.iloc[max(0, current_idx - 30):current_idx]['revenue_per_share'].mean()
            old_revenue = lookback_window.iloc[:30]['revenue_per_share'].mean()
            
            if old_revenue > 0:
                revenue_growth = (recent_revenue - old_revenue) / old_revenue
                
                # Compare to historical growth patterns
                historical_growths = []
                for i in range(30, len(lookback_window), 15):
                    if i >= 60:
                        recent = lookback_window.iloc[i-30:i]['revenue_per_share'].mean()
                        past = lookback_window.iloc[i-60:i-30]['revenue_per_share'].mean()
                        if past > 0:
                            historical_growths.append((recent - past) / past)
                
                if historical_growths:
                    growth_percentile = sum(1 for g in historical_growths if g <= revenue_growth) / len(historical_growths)
                    scores.revenue_growth_score = growth_percentile * 5
                    
        # 3. Cash Position Score (0-5 scale)
        if not pd.isna(current_row['cash_per_share']) and len(lookback_window) > 0:
            cash_percentile = (lookback_window['cash_per_share'] <= current_row['cash_per_share']).mean()
            scores.cash_score = cash_percentile * 5
            
        # 4. Price Momentum Score (0-5 scale)
        if len(lookback_window) >= 20:
            # 20-day price momentum
            price_20_days_ago = data.iloc[current_idx - 20]['close_price']
            current_price = current_row['close_price']
            
            if price_20_days_ago > 0:
                momentum = (current_price - price_20_days_ago) / price_20_days_ago
                
                # Compare to historical 20-day momentum
                historical_momentum = []
                for i in range(20, len(lookback_window)):
                    past_price = lookback_window.iloc[i - 20]['close_price']
                    current_past_price = lookback_window.iloc[i]['close_price']
                    if past_price > 0:
                        historical_momentum.append((current_past_price - past_price) / past_price)
                
                if historical_momentum:
                    momentum_percentile = sum(1 for m in historical_momentum if m <= momentum) / len(historical_momentum)
                    scores.momentum_score = momentum_percentile * 5
                    
        return scores
        
    def generate_signals(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Generate value investing signals."""
        
        signals = []
        scores_list = []
        
        for idx in range(len(data)):
            scores = self.calculate_value_scores(data, idx)
            composite = scores.composite_score
            
            scores_list.append(scores)
            
            # Signal logic
            if composite >= self.config.strong_signal_threshold:
                signal = ValueSignal.STRONG_BUY
            elif composite >= self.config.buy_signal_threshold:
                signal = ValueSignal.BUY
            elif composite <= self.config.exit_threshold:
                signal = ValueSignal.SELL
            else:
                signal = ValueSignal.HOLD
                
            signals.append(signal)
            
        data['signal'] = signals
        data['composite_score'] = [s.composite_score for s in scores_list]
        data['pb_score'] = [s.pb_score for s in scores_list]
        data['revenue_score'] = [s.revenue_growth_score for s in scores_list]
        data['cash_score'] = [s.cash_score for s in scores_list]
        data['momentum_score'] = [s.momentum_score for s in scores_list]
        
        return data
        
    def calculate_position_size(self, price: float) -> Tuple[int, float]:
        """Calculate conservative position size."""
        
        available_capital = self.cash * self.config.position_size
        shares = max(1, int(available_capital / price))
        investment = shares * price
        
        # Check we have enough cash
        total_cost = investment * (1 + self.config.transaction_cost_rate)
        
        if total_cost > self.cash:
            max_investment = self.cash * 0.95
            shares = max(1, int(max_investment / price))
            investment = shares * price
            
        return shares, investment
        
    def open_trade(self, symbol: str, signal_date: datetime, signal: ValueSignal,
                  current_data: pd.Series, scores: ValueScore) -> ValueTrade:
        """Create a new value trade."""
        
        entry_price = current_data['close_price']
        shares, investment = self.calculate_position_size(entry_price)
        
        trade = ValueTrade(
            signal_date=signal_date,
            entry_date=None,
            exit_date=None,
            symbol=symbol,
            entry_price=entry_price,
            exit_price=None,
            shares=shares,
            investment_amount=investment,
            entry_scores=scores,
            entry_pb=current_data.get('pb_ratio', 0),
            entry_revenue_ps=current_data.get('revenue_per_share', 0)
        )
        
        return trade
        
    def update_trailing_stop(self, trade: ValueTrade, current_price: float) -> None:
        """Update trailing stop price."""
        if trade.trailing_stop_price is None:
            trade.trailing_stop_price = current_price * (1 - self.config.trailing_stop_pct)
        else:
            new_stop = current_price * (1 - self.config.trailing_stop_pct)
            trade.trailing_stop_price = max(trade.trailing_stop_price, new_stop)
            
    def should_close_trade(self, trade: ValueTrade, current_data: pd.Series, 
                          current_scores: ValueScore, days_held: int) -> Tuple[bool, str]:
        """Determine if trade should be closed."""
        
        current_price = current_data['close_price']
        
        # Profit target
        if current_price >= trade.entry_price * (1 + self.config.profit_target_pct):
            return True, "profit_target"
            
        # Trailing stop
        if trade.trailing_stop_price and current_price <= trade.trailing_stop_price:
            return True, "trailing_stop"
            
        # Value deterioration
        if current_scores.composite_score <= self.config.exit_threshold:
            return True, "value_deterioration"
            
        # Max holding period
        if days_held >= self.config.max_holding_days:
            return True, "max_holding_period"
            
        return False, ""
        
    def close_trade(self, trade: ValueTrade, date: datetime, current_price: float, reason: str) -> float:
        """Close a trade and calculate P&L."""
        
        trade.exit_date = date
        trade.exit_price = current_price
        trade.holding_days = (date - trade.entry_date).days if trade.entry_date else 0
        trade.is_open = False
        trade.exit_reason = reason
        
        # Calculate P&L
        gross_pnl = (current_price - trade.entry_price) * trade.shares
        transaction_costs = trade.investment_amount * self.config.transaction_cost_rate * 2
        trade.pnl = gross_pnl - transaction_costs
        
        # Return cash
        proceeds = trade.investment_amount + trade.pnl
        self.cash += proceeds
        
        return trade.pnl
        
    async def run_backtest(self) -> Dict:
        """Run the value strategy backtest."""
        
        print(f"🚀 Starting Fundamental Value Strategy")
        print(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        print(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        print(f"   📊 Strategy: Buy undervalued companies with improving fundamentals")
        
        # Get quality companies
        companies = await self.get_quality_companies()
        
        if not companies:
            return {'error': 'No quality companies found'}
            
        # Load data for all companies
        all_data = {}
        
        for i, symbol in enumerate(companies):
            if i % 5 == 0:
                print(f"   📈 Loading data... {i+1}/{len(companies)}")
                
            data = await self.get_company_data(
                symbol,
                self.config.start_date - timedelta(days=self.config.min_history_days),
                self.config.end_date
            )
            
            if len(data) >= self.config.min_history_days:
                signals_data = self.generate_signals(data, symbol)
                all_data[symbol] = signals_data
                
        if not all_data:
            return {'error': 'No usable company data found'}
            
        print(f"📊 Trading {len(all_data)} quality companies")
        
        # Get all dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation
        for i, current_date in enumerate(all_dates):
            
            if i % 50 == 0:
                print(f"   📅 {current_date.date()} - Cash: ${self.cash:,.0f}, Positions: {len(self.open_trades)}")
            
            # Execute pending orders
            executed_orders = []
            for symbol, pending_trade in self.pending_orders.items():
                if symbol in all_data:
                    current_data = all_data[symbol][all_data[symbol]['date'] == current_date]
                    if not current_data.empty:
                        current_row = current_data.iloc[0]
                        
                        pending_trade.entry_date = current_date
                        pending_trade.entry_price = current_row['close_price']
                        pending_trade.investment_amount = pending_trade.shares * pending_trade.entry_price
                        
                        total_cost = pending_trade.investment_amount * (1 + self.config.transaction_cost_rate)
                        self.cash -= total_cost
                        
                        # Set initial trailing stop
                        self.update_trailing_stop(pending_trade, pending_trade.entry_price)
                        
                        self.open_trades.append(pending_trade)
                        executed_orders.append(symbol)
                        
            for symbol in executed_orders:
                del self.pending_orders[symbol]
                
            # Check exits and update trailing stops
            trades_to_close = []
            for trade in self.open_trades:
                if trade.symbol not in all_data:
                    continue
                    
                current_data = all_data[trade.symbol][all_data[trade.symbol]['date'] == current_date]
                if current_data.empty:
                    continue
                    
                current_row = current_data.iloc[0]
                current_price = current_row['close_price']
                days_held = (current_date - trade.entry_date).days
                
                # Update trailing stop
                self.update_trailing_stop(trade, current_price)
                
                # Get current scores
                data_idx = len(all_data[trade.symbol][all_data[trade.symbol]['date'] <= current_date]) - 1
                current_scores = self.calculate_value_scores(all_data[trade.symbol], data_idx)
                
                should_close, reason = self.should_close_trade(trade, current_row, current_scores, days_held)
                
                if should_close:
                    self.close_trade(trade, current_date, current_price, reason)
                    trades_to_close.append(trade)
                    
            for trade in trades_to_close:
                self.open_trades.remove(trade)
                self.closed_trades.append(trade)
                
            # Look for new opportunities
            if len(self.open_trades) + len(self.pending_orders) < self.config.max_positions:
                
                # Get today's best signals
                opportunities = []
                for symbol, data in all_data.items():
                    if (any(t.symbol == symbol for t in self.open_trades) or 
                        symbol in self.pending_orders):
                        continue
                        
                    current_data = data[data['date'] == current_date]
                    if current_data.empty:
                        continue
                        
                    current_row = current_data.iloc[0]
                    signal = current_row['signal']
                    
                    if signal in [ValueSignal.STRONG_BUY, ValueSignal.BUY]:
                        data_idx = len(data[data['date'] <= current_date]) - 1
                        scores = self.calculate_value_scores(data, data_idx)
                        
                        opportunities.append({
                            'symbol': symbol,
                            'signal': signal,
                            'scores': scores,
                            'data': current_row,
                            'composite_score': scores.composite_score
                        })
                
                # Sort by composite score
                opportunities.sort(key=lambda x: x['composite_score'], reverse=True)
                
                # Take best opportunities
                for opp in opportunities[:1]:  # One new position per day
                    if len(self.open_trades) + len(self.pending_orders) >= self.config.max_positions:
                        break
                        
                    trade = self.open_trade(
                        opp['symbol'], current_date, opp['signal'], opp['data'], opp['scores']
                    )
                    
                    total_cost = trade.investment_amount * (1 + self.config.transaction_cost_rate)
                    if self.cash >= total_cost:
                        self.pending_orders[opp['symbol']] = trade
                        
            # Record portfolio value
            portfolio_value = self.cash
            for trade in self.open_trades:
                if trade.symbol in all_data:
                    current_data = all_data[trade.symbol][all_data[trade.symbol]['date'] == current_date]
                    if not current_data.empty:
                        current_price = current_data.iloc[0]['close_price']
                        portfolio_value += trade.shares * current_price
                        
            self.daily_portfolio_values.append({
                'date': current_date,
                'portfolio_value': portfolio_value,
                'cash': self.cash,
                'open_positions': len(self.open_trades)
            })
            
        # Close remaining trades
        final_date = all_dates[-1] if all_dates else self.config.end_date
        for trade in self.open_trades[:]:
            if trade.symbol in all_data:
                final_data = all_data[trade.symbol][all_data[trade.symbol]['date'] == final_date]
                if not final_data.empty:
                    final_price = final_data.iloc[0]['close_price']
                    self.close_trade(trade, final_date, final_price, "backtest_end")
                    self.closed_trades.append(trade)
                    
        self.open_trades = []
        self.pending_orders.clear()
        
        return self.analyze_results()
        
    def analyze_results(self) -> Dict:
        """Analyze backtest results."""
        
        if not self.closed_trades:
            return {'error': 'No completed trades found'}
            
        total_trades = len(self.closed_trades)
        winning_trades = [t for t in self.closed_trades if t.pnl > 0]
        losing_trades = [t for t in self.closed_trades if t.pnl <= 0]
        
        total_pnl = sum(t.pnl for t in self.closed_trades)
        win_rate = len(winning_trades) / total_trades * 100
        
        avg_win = np.mean([t.pnl for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t.pnl for t in losing_trades]) if losing_trades else 0
        
        avg_holding_days = np.mean([t.holding_days for t in self.closed_trades])
        avg_composite_score = np.mean([t.entry_scores.composite_score for t in self.closed_trades])
        
        final_value = self.cash
        total_return = (final_value - self.config.initial_capital) / self.config.initial_capital * 100
        
        return {
            'total_trades': total_trades,
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'avg_holding_days': avg_holding_days,
            'avg_composite_score': avg_composite_score,
            'total_return': total_return,
            'final_portfolio_value': final_value,
            'profit_factor': abs(avg_win * len(winning_trades) / (avg_loss * len(losing_trades))) if losing_trades and avg_loss != 0 else float('inf')
        }
        
    def print_detailed_results(self, results: Dict):
        """Print comprehensive results analysis."""
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
            
        print(f"\n" + "="*80)
        print(f"🎯 FUNDAMENTAL VALUE STRATEGY RESULTS")
        print(f"="*80)
        
        print(f"\n📊 TRADING STATISTICS:")
        print(f"   Total trades:              {results['total_trades']:,}")
        print(f"   Winning trades:            {results['winning_trades']:,} ({results['win_rate']:.1f}%)")
        print(f"   Losing trades:             {results['losing_trades']:,}")
        print(f"   Average holding period:    {results['avg_holding_days']:.1f} days")
        print(f"   Average entry score:       {results['avg_composite_score']:.2f}/5.0")
        
        print(f"\n💰 P&L ANALYSIS:")
        print(f"   Total P&L:                 ${results['total_pnl']:,.2f}")
        print(f"   Average winning trade:     ${results['avg_win']:,.2f}")
        print(f"   Average losing trade:      ${results['avg_loss']:,.2f}")
        print(f"   Profit factor:             {results['profit_factor']:.2f}")
        
        print(f"\n📈 PORTFOLIO PERFORMANCE:")
        print(f"   Initial capital:           ${self.config.initial_capital:,.2f}")
        print(f"   Final portfolio value:     ${results['final_portfolio_value']:,.2f}")
        print(f"   Total return:              {results['total_return']:+.2f}%")
        
        # Exit reason analysis
        if self.closed_trades:
            print(f"\n📊 EXIT REASON BREAKDOWN:")
            exit_stats = {}
            for trade in self.closed_trades:
                reason = trade.exit_reason
                if reason not in exit_stats:
                    exit_stats[reason] = {'count': 0, 'pnl': 0}
                exit_stats[reason]['count'] += 1
                exit_stats[reason]['pnl'] += trade.pnl
                
            for reason, stats in sorted(exit_stats.items(), key=lambda x: x[1]['pnl'], reverse=True):
                print(f"   {reason:<20}: {stats['count']:3d} trades, ${stats['pnl']:+8,.0f} P&L")
                
        # Best trades analysis
        if len(self.closed_trades) >= 5:
            print(f"\n🏆 TOP 5 WINNING TRADES:")
            best_trades = sorted(self.closed_trades, key=lambda x: x.pnl, reverse=True)[:5]
            for i, trade in enumerate(best_trades):
                return_pct = (trade.exit_price - trade.entry_price) / trade.entry_price * 100
                print(f"   {i+1}. {trade.symbol:<8}: ${trade.pnl:+7,.0f} ({return_pct:+5.1f}%) - Score: {trade.entry_scores.composite_score:.1f}")
                
        print(f"\n" + "="*80)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run fundamental value strategy backtest."""
    
    config = ValueConfig(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2025, 11, 1),
        initial_capital=100000.0,
        max_positions=8,
        position_size=0.12,
        transaction_cost_rate=0.002,
        short_lookback=60,
        long_lookback=120,
        min_history_days=150,
        strong_signal_threshold=3.5,
        buy_signal_threshold=2.5,
        exit_threshold=1.5,
        max_holding_days=60,
        trailing_stop_pct=0.15,
        profit_target_pct=0.25
    )
    
    strategy = FundamentalValueStrategy(config)
    
    try:
        await strategy.setup()
        results = await strategy.run_backtest()
        strategy.print_detailed_results(results)
        
    except Exception as e:
        logger.error(f"Error during backtest: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await strategy.cleanup()

if __name__ == "__main__":
    asyncio.run(main())