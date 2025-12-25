#!/usr/bin/env python3
"""
Fundamental Anomaly Detection Strategy

Detects when companies show significant improvement in key fundamental metrics
relative to their historical patterns, then enters long positions expecting
the market to eventually reflect these improved fundamentals.

Key Metrics:
- P/B ratio improvement (lower is better)  
- Revenue per share growth
- Cash per share growth
- Debt to equity improvement (lower is better)
- Book value per share growth

Strategy:
1. Calculate rolling historical averages for each metric (90-day, 180-day)
2. Detect significant positive deviations from historical norms
3. Score fundamental strength using composite anomaly score
4. Enter positions when multiple metrics improve simultaneously
5. Use realistic position sizing and risk management
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

class FundamentalSignal(Enum):
    STRONG_BUY = "strong_fundamental_improvement"
    MODERATE_BUY = "moderate_improvement"
    HOLD = "hold"
    SELL = "deteriorating_fundamentals"

@dataclass
class FundamentalScore:
    """Individual metric anomaly scores"""
    pb_ratio_score: float = 0.0
    revenue_growth_score: float = 0.0
    cash_growth_score: float = 0.0
    debt_improvement_score: float = 0.0
    book_value_growth_score: float = 0.0
    
    @property
    def composite_score(self) -> float:
        """Weighted composite of all scores"""
        weights = {
            'pb_ratio': 0.25,       # Value metric
            'revenue_growth': 0.25, # Growth metric
            'cash_growth': 0.20,    # Financial strength
            'debt_improvement': 0.15,# Financial health
            'book_value': 0.15      # Asset growth
        }
        
        return (
            self.pb_ratio_score * weights['pb_ratio'] +
            self.revenue_growth_score * weights['revenue_growth'] +
            self.cash_growth_score * weights['cash_growth'] +
            self.debt_improvement_score * weights['debt_improvement'] +
            self.book_value_growth_score * weights['book_value']
        )

@dataclass
class FundamentalTrade:
    signal_date: datetime
    entry_date: Optional[datetime]
    exit_date: Optional[datetime]
    symbol: str
    entry_price: float
    exit_price: Optional[float]
    shares: int
    investment_amount: float
    fundamental_scores: FundamentalScore
    entry_pb_ratio: float
    entry_revenue_ps: float
    entry_cash_ps: float
    pnl: Optional[float] = None
    holding_days: Optional[int] = None
    is_open: bool = True
    exit_reason: Optional[str] = None

@dataclass
class FundamentalConfig:
    start_date: datetime
    end_date: datetime
    initial_capital: float = 100000.0
    max_positions: int = 6
    position_size: float = 0.15  # 15% per position
    transaction_cost_rate: float = 0.002
    
    # Lookback periods for averages
    short_lookback: int = 90   # 3 months
    long_lookback: int = 180   # 6 months
    
    # Minimum data requirements
    min_history_days: int = 200
    
    # Signal thresholds
    strong_signal_threshold: float = 1.5  # Composite score
    moderate_signal_threshold: float = 1.0
    exit_threshold: float = 0.2           # Exit when improvement fades
    
    # Maximum holding
    max_holding_days: int = 90  # Quarterly cycle
    
    # Minimum improvement thresholds (Z-scores)
    min_pb_improvement: float = 1.0      # P/B must be 1 std dev better
    min_revenue_growth: float = 1.5      # Revenue growth 1.5 std devs
    min_cash_growth: float = 1.0         # Cash growth 1 std dev

class FundamentalAnomalyStrategy:
    
    def __init__(self, config: FundamentalConfig):
        self.config = config
        self.db_conn = None
        self.open_trades: List[FundamentalTrade] = []
        self.closed_trades: List[FundamentalTrade] = []
        self.pending_orders: Dict[str, FundamentalTrade] = {}
        self.cash = config.initial_capital
        self.daily_portfolio_values = []
        self.company_stats_cache: Dict[str, Dict] = {}
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_companies_with_fundamentals(self) -> List[str]:
        """Get companies with sufficient fundamental data."""
        query = """
        SELECT 
            symbol,
            COUNT(*) as records,
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(pb_ratio) as pb_records,
            COUNT(revenue_per_share) as revenue_records,
            COUNT(cash_per_share) as cash_records
        FROM historical_fundamentals_daily
        WHERE date >= $1
        GROUP BY symbol
        HAVING COUNT(*) >= $2
        AND COUNT(pb_ratio) >= $3
        AND COUNT(revenue_per_share) >= $3
        AND COUNT(cash_per_share) >= $3
        ORDER BY records DESC
        """
        
        min_date = self.config.start_date - timedelta(days=self.config.min_history_days)
        min_records = self.config.min_history_days
        min_metric_coverage = int(min_records * 0.8)  # 80% coverage required
        
        records = await self.db_conn.fetch(
            query, min_date.date(), min_records, min_metric_coverage
        )
        
        companies = [record['symbol'] for record in records]
        print(f"📊 Found {len(companies)} companies with sufficient fundamental data")
        return companies
        
    async def get_fundamental_data(self, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get fundamental data for a company."""
        query = """
        SELECT 
            date,
            pb_ratio,
            revenue_per_share,
            cash_per_share,
            debt_to_equity,
            book_value_per_share,
            pe_ratio,
            close_price
        FROM historical_fundamentals_daily
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        records = await self.db_conn.fetch(query, symbol, start_date.date(), end_date.date())
        
        if not records:
            return pd.DataFrame()
            
        df = pd.DataFrame([dict(record) for record in records])
        df['date'] = pd.to_datetime(df['date'])
        
        # Convert to numeric, handling None values
        numeric_cols = ['pb_ratio', 'revenue_per_share', 'cash_per_share', 
                       'debt_to_equity', 'book_value_per_share', 'pe_ratio', 'close_price']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df.dropna(subset=['pb_ratio', 'revenue_per_share', 'cash_per_share', 'close_price'])
        
    def calculate_fundamental_scores(self, data: pd.DataFrame, current_idx: int) -> FundamentalScore:
        """Calculate anomaly scores for fundamental metrics."""
        
        if current_idx < self.config.long_lookback:
            return FundamentalScore()  # Not enough history
            
        current_data = data.iloc[current_idx]
        
        # Get historical windows
        long_window = data.iloc[current_idx - self.config.long_lookback:current_idx]
        short_window = data.iloc[current_idx - self.config.short_lookback:current_idx]
        
        scores = FundamentalScore()
        
        # 1. P/B Ratio Score (lower is better)
        if not pd.isna(current_data['pb_ratio']) and len(long_window) > 0:
            long_pb_mean = long_window['pb_ratio'].mean()
            long_pb_std = long_window['pb_ratio'].std()
            
            if long_pb_std > 0:
                # Negative Z-score is good (lower P/B)
                pb_zscore = -(current_data['pb_ratio'] - long_pb_mean) / long_pb_std
                scores.pb_ratio_score = max(0, pb_zscore)
                
        # 2. Revenue Growth Score
        if not pd.isna(current_data['revenue_per_share']) and len(short_window) > 0:
            recent_revenue = short_window['revenue_per_share'].tail(30).mean()
            historical_revenue = long_window['revenue_per_share'].head(60).mean()
            
            if historical_revenue > 0:
                revenue_growth = (recent_revenue - historical_revenue) / historical_revenue
                
                # Z-score vs historical growth rates
                historical_growth_window = []
                for i in range(60, len(long_window), 30):
                    if i > 30:
                        recent = long_window.iloc[i-30:i]['revenue_per_share'].mean()
                        past = long_window.iloc[i-60:i-30]['revenue_per_share'].mean()
                        if past > 0:
                            historical_growth_window.append((recent - past) / past)
                
                if historical_growth_window:
                    growth_mean = np.mean(historical_growth_window)
                    growth_std = np.std(historical_growth_window)
                    
                    if growth_std > 0:
                        growth_zscore = (revenue_growth - growth_mean) / growth_std
                        scores.revenue_growth_score = max(0, growth_zscore)
                        
        # 3. Cash Growth Score
        if not pd.isna(current_data['cash_per_share']) and len(short_window) > 0:
            recent_cash = short_window['cash_per_share'].tail(30).mean()
            historical_cash = long_window['cash_per_share'].head(60).mean()
            
            if historical_cash > 0:
                cash_growth = (recent_cash - historical_cash) / historical_cash
                
                # Compare to historical volatility
                cash_changes = long_window['cash_per_share'].pct_change().dropna()
                if len(cash_changes) > 0 and cash_changes.std() > 0:
                    cash_zscore = cash_growth / cash_changes.std()
                    scores.cash_growth_score = max(0, cash_zscore)
                    
        # 4. Debt Improvement Score (lower debt is better)
        if not pd.isna(current_data['debt_to_equity']) and len(long_window) > 0:
            long_debt_mean = long_window['debt_to_equity'].mean()
            long_debt_std = long_window['debt_to_equity'].std()
            
            if long_debt_std > 0:
                # Negative Z-score is good (lower debt)
                debt_zscore = -(current_data['debt_to_equity'] - long_debt_mean) / long_debt_std
                scores.debt_improvement_score = max(0, debt_zscore)
                
        # 5. Book Value Growth Score
        if not pd.isna(current_data['book_value_per_share']) and len(short_window) > 0:
            recent_bv = short_window['book_value_per_share'].tail(30).mean()
            historical_bv = long_window['book_value_per_share'].head(60).mean()
            
            if historical_bv > 0:
                bv_growth = (recent_bv - historical_bv) / historical_bv
                
                # Compare to historical book value growth volatility
                bv_changes = long_window['book_value_per_share'].pct_change().dropna()
                if len(bv_changes) > 0 and bv_changes.std() > 0:
                    bv_zscore = bv_growth / bv_changes.std()
                    scores.book_value_growth_score = max(0, bv_zscore)
                    
        return scores
        
    def generate_signals(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Generate fundamental anomaly signals."""
        
        signals = []
        scores_list = []
        
        for idx in range(len(data)):
            scores = self.calculate_fundamental_scores(data, idx)
            composite = scores.composite_score
            
            scores_list.append(scores)
            
            # Apply minimum thresholds for individual metrics
            meets_minimums = (
                scores.pb_ratio_score >= self.config.min_pb_improvement or
                scores.revenue_growth_score >= self.config.min_revenue_growth or
                scores.cash_growth_score >= self.config.min_cash_growth
            )
            
            if composite >= self.config.strong_signal_threshold and meets_minimums:
                signal = FundamentalSignal.STRONG_BUY
            elif composite >= self.config.moderate_signal_threshold and meets_minimums:
                signal = FundamentalSignal.MODERATE_BUY
            elif composite <= -self.config.exit_threshold:
                signal = FundamentalSignal.SELL
            else:
                signal = FundamentalSignal.HOLD
                
            signals.append(signal)
            
        data['signal'] = signals
        data['composite_score'] = [s.composite_score for s in scores_list]
        data['pb_score'] = [s.pb_ratio_score for s in scores_list]
        data['revenue_score'] = [s.revenue_growth_score for s in scores_list]
        data['cash_score'] = [s.cash_growth_score for s in scores_list]
        
        return data
        
    def calculate_position_size(self, price: float) -> Tuple[int, float]:
        """Calculate position size based on available cash."""
        
        available_capital = self.cash * self.config.position_size
        shares = max(1, int(available_capital / price))
        investment = shares * price
        
        # Ensure we don't exceed available cash
        transaction_costs = investment * self.config.transaction_cost_rate
        total_cost = investment + transaction_costs
        
        if total_cost > self.cash:
            max_investment = self.cash * 0.95  # Leave 5% buffer
            shares = max(1, int(max_investment / price))
            investment = shares * price
            
        return shares, investment
        
    def open_trade(self, symbol: str, signal_date: datetime, signal: FundamentalSignal,
                  current_data: pd.Series, scores: FundamentalScore) -> FundamentalTrade:
        """Create a pending fundamental trade."""
        
        entry_price = current_data['close_price']
        shares, investment = self.calculate_position_size(entry_price)
        
        trade = FundamentalTrade(
            signal_date=signal_date,
            entry_date=None,
            exit_date=None,
            symbol=symbol,
            entry_price=entry_price,
            exit_price=None,
            shares=shares,
            investment_amount=investment,
            fundamental_scores=scores,
            entry_pb_ratio=current_data.get('pb_ratio', 0),
            entry_revenue_ps=current_data.get('revenue_per_share', 0),
            entry_cash_ps=current_data.get('cash_per_share', 0)
        )
        
        return trade
        
    def should_close_trade(self, trade: FundamentalTrade, current_data: pd.Series, 
                          current_scores: FundamentalScore, days_held: int) -> Tuple[bool, str]:
        """Determine if a fundamental trade should be closed."""
        
        # Exit if fundamentals deteriorated
        if current_scores.composite_score <= -self.config.exit_threshold:
            return True, "fundamentals_deteriorated"
            
        # Exit if maximum holding period reached
        if days_held >= self.config.max_holding_days:
            return True, "max_holding_period"
            
        # Exit if strong gains achieved (20%+)
        if not pd.isna(current_data['close_price']):
            return_pct = (current_data['close_price'] - trade.entry_price) / trade.entry_price
            if return_pct >= 0.20:
                return True, "profit_taking"
                
        return False, ""
        
    def close_trade(self, trade: FundamentalTrade, date: datetime, 
                   current_data: pd.Series, reason: str = "signal") -> float:
        """Close a fundamental trade and calculate P&L."""
        
        exit_price = current_data['close_price']
        trade.exit_date = date
        trade.exit_price = exit_price
        trade.holding_days = (date - trade.entry_date).days if trade.entry_date else 0
        trade.is_open = False
        trade.exit_reason = reason
        
        # Calculate P&L
        gross_pnl = (exit_price - trade.entry_price) * trade.shares
        transaction_costs = trade.investment_amount * self.config.transaction_cost_rate * 2  # Entry + exit
        trade.pnl = gross_pnl - transaction_costs
        
        # Return cash
        proceeds = trade.investment_amount + trade.pnl
        self.cash += proceeds
        
        return trade.pnl
        
    async def run_backtest(self) -> Dict:
        """Run the complete fundamental anomaly backtest."""
        
        print(f"🚀 Starting Fundamental Anomaly Detection Strategy")
        print(f"   📅 Period: {self.config.start_date.date()} to {self.config.end_date.date()}")
        print(f"   💰 Capital: ${self.config.initial_capital:,.0f}")
        print(f"   📊 Strategy: Buy when fundamentals improve significantly vs historical norms")
        
        # Get companies with sufficient data
        companies = await self.get_companies_with_fundamentals()
        
        if not companies:
            return {'error': 'No companies with sufficient fundamental data found'}
            
        # Limit to top companies to manageable set
        companies = companies[:50]  # Top 50 by data availability
        
        # Load fundamental data for all companies
        all_data = {}
        
        for i, symbol in enumerate(companies):
            if i % 10 == 0:
                print(f"   📈 Loading data... {i+1}/{len(companies)}")
                
            data = await self.get_fundamental_data(
                symbol,
                self.config.start_date - timedelta(days=self.config.min_history_days),
                self.config.end_date
            )
            
            if len(data) >= self.config.min_history_days:
                signals_data = self.generate_signals(data, symbol)
                all_data[symbol] = signals_data
                
        if not all_data:
            return {'error': 'No usable fundamental data found'}
            
        print(f"📊 Analyzing {len(all_data)} companies with fundamental signals")
        
        # Get all dates
        all_dates = set()
        for data in all_data.values():
            all_dates.update(data['date'])
        all_dates = sorted([d for d in all_dates if d >= self.config.start_date])
        
        # Daily simulation
        for i, current_date in enumerate(all_dates):
            
            if i % 100 == 0:
                print(f"   📅 {current_date.date()} - Cash: ${self.cash:,.0f}, Positions: {len(self.open_trades)}")
            
            # Execute pending orders (next day execution)
            executed_orders = []
            for symbol, pending_trade in self.pending_orders.items():
                if symbol in all_data:
                    current_data = all_data[symbol][all_data[symbol]['date'] == current_date]
                    if not current_data.empty:
                        current_row = current_data.iloc[0]
                        
                        # Execute at current day's price
                        if not pd.isna(current_row['close_price']):
                            pending_trade.entry_date = current_date
                            pending_trade.entry_price = current_row['close_price']
                            pending_trade.investment_amount = pending_trade.shares * pending_trade.entry_price
                            
                            # Deduct cost from cash
                            total_cost = pending_trade.investment_amount * (1 + self.config.transaction_cost_rate)
                            self.cash -= total_cost
                            
                            self.open_trades.append(pending_trade)
                            executed_orders.append(symbol)
                        elif (current_date - pending_trade.signal_date).days > 3:
                            executed_orders.append(symbol)  # Expire order
                            
            # Remove executed/expired orders
            for symbol in executed_orders:
                del self.pending_orders[symbol]
                
            # Check exit conditions
            trades_to_close = []
            for trade in self.open_trades:
                if trade.symbol not in all_data:
                    continue
                    
                current_data = all_data[trade.symbol][all_data[trade.symbol]['date'] == current_date]
                if current_data.empty:
                    continue
                    
                current_row = current_data.iloc[0]
                days_held = (current_date - trade.entry_date).days
                
                # Get current fundamental scores
                data_idx = len(all_data[trade.symbol][all_data[trade.symbol]['date'] <= current_date]) - 1
                current_scores = self.calculate_fundamental_scores(all_data[trade.symbol], data_idx)
                
                should_close, reason = self.should_close_trade(trade, current_row, current_scores, days_held)
                
                if should_close:
                    self.close_trade(trade, current_date, current_row, reason)
                    trades_to_close.append(trade)
                    
            # Move closed trades
            for trade in trades_to_close:
                self.open_trades.remove(trade)
                self.closed_trades.append(trade)
                
            # Look for new opportunities
            if len(self.open_trades) + len(self.pending_orders) < self.config.max_positions:
                
                # Get today's best signals
                today_signals = []
                for symbol, data in all_data.items():
                    if (any(t.symbol == symbol for t in self.open_trades) or 
                        symbol in self.pending_orders):
                        continue
                        
                    current_data = data[data['date'] == current_date]
                    if current_data.empty:
                        continue
                        
                    current_row = current_data.iloc[0]
                    signal = current_row['signal']
                    
                    if signal in [FundamentalSignal.STRONG_BUY, FundamentalSignal.MODERATE_BUY]:
                        data_idx = len(data[data['date'] <= current_date]) - 1
                        scores = self.calculate_fundamental_scores(data, data_idx)
                        
                        today_signals.append({
                            'symbol': symbol,
                            'signal': signal,
                            'scores': scores,
                            'data': current_row,
                            'composite_score': scores.composite_score
                        })
                
                # Sort by composite score and take best signals
                today_signals.sort(key=lambda x: x['composite_score'], reverse=True)
                
                for signal_data in today_signals[:2]:  # Max 2 new positions per day
                    if len(self.open_trades) + len(self.pending_orders) >= self.config.max_positions:
                        break
                        
                    trade = self.open_trade(
                        signal_data['symbol'],
                        current_date,
                        signal_data['signal'],
                        signal_data['data'],
                        signal_data['scores']
                    )
                    
                    total_cost = trade.investment_amount * (1 + self.config.transaction_cost_rate)
                    if self.cash >= total_cost and trade.shares > 0:
                        self.pending_orders[signal_data['symbol']] = trade
                        
            # Record portfolio value
            portfolio_value = self.cash
            for trade in self.open_trades:
                if trade.symbol in all_data:
                    current_data = all_data[trade.symbol][all_data[trade.symbol]['date'] == current_date]
                    if not current_data.empty:
                        current_price = current_data.iloc[0]['close_price']
                        if not pd.isna(current_price):
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
                    final_row = final_data.iloc[0]
                    self.close_trade(trade, final_date, final_row, "backtest_end")
                    self.closed_trades.append(trade)
                    
        self.open_trades = []
        self.pending_orders.clear()
        
        return self.analyze_results()
        
    def analyze_results(self) -> Dict:
        """Analyze fundamental strategy results."""
        
        if not self.closed_trades:
            return {'error': 'No completed trades found', 'total_trades': 0}
            
        total_trades = len(self.closed_trades)
        winning_trades = [t for t in self.closed_trades if t.pnl > 0]
        losing_trades = [t for t in self.closed_trades if t.pnl <= 0]
        
        total_pnl = sum(t.pnl for t in self.closed_trades)
        win_rate = len(winning_trades) / total_trades * 100
        
        avg_win = np.mean([t.pnl for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t.pnl for t in losing_trades]) if losing_trades else 0
        
        avg_holding_days = np.mean([t.holding_days for t in self.closed_trades])
        avg_composite_score = np.mean([t.fundamental_scores.composite_score for t in self.closed_trades])
        
        # Portfolio performance
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
        
    def print_results(self, results: Dict):
        """Print formatted backtest results."""
        
        if 'error' in results:
            print(f"❌ {results['error']}")
            return
            
        print(f"\n" + "="*80)
        print(f"🎯 FUNDAMENTAL ANOMALY DETECTION STRATEGY RESULTS")
        print(f"="*80)
        
        print(f"\n📊 TRADING STATISTICS:")
        print(f"   Total trades:              {results['total_trades']:,}")
        print(f"   Winning trades:            {results['winning_trades']:,} ({results['win_rate']:.1f}%)")
        print(f"   Losing trades:             {results['losing_trades']:,}")
        print(f"   Average holding period:    {results['avg_holding_days']:.1f} days")
        print(f"   Average composite score:   {results['avg_composite_score']:.2f}")
        
        print(f"\n💰 P&L ANALYSIS:")
        print(f"   Total P&L:                 ${results['total_pnl']:,.2f}")
        print(f"   Average winning trade:     ${results['avg_win']:,.2f}")
        print(f"   Average losing trade:      ${results['avg_loss']:,.2f}")
        print(f"   Profit factor:             {results['profit_factor']:.2f}")
        
        print(f"\n📈 PORTFOLIO PERFORMANCE:")
        print(f"   Initial capital:           ${self.config.initial_capital:,.2f}")
        print(f"   Final portfolio value:     ${results['final_portfolio_value']:,.2f}")
        print(f"   Total return:              {results['total_return']:+.2f}%")
        
        # Trade breakdown by exit reason
        if self.closed_trades:
            print(f"\n📊 EXIT REASON BREAKDOWN:")
            exit_reasons = {}
            for trade in self.closed_trades:
                reason = trade.exit_reason
                if reason not in exit_reasons:
                    exit_reasons[reason] = {'count': 0, 'pnl': 0}
                exit_reasons[reason]['count'] += 1
                exit_reasons[reason]['pnl'] += trade.pnl
                
            for reason, stats in sorted(exit_reasons.items(), key=lambda x: x[1]['pnl'], reverse=True):
                print(f"   {reason:<25}: {stats['count']:3d} trades, ${stats['pnl']:+8,.0f} P&L")
        
        print(f"\n" + "="*80)
        
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run fundamental anomaly detection strategy backtest."""
    
    config = FundamentalConfig(
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2025, 11, 1),
        initial_capital=100000.0,
        max_positions=6,
        position_size=0.15,
        transaction_cost_rate=0.002,
        short_lookback=90,
        long_lookback=180,
        min_history_days=200,
        strong_signal_threshold=1.5,
        moderate_signal_threshold=1.0,
        max_holding_days=90
    )
    
    strategy = FundamentalAnomalyStrategy(config)
    
    try:
        await strategy.setup()
        results = await strategy.run_backtest()
        strategy.print_results(results)
        
    except Exception as e:
        logger.error(f"Error during backtest: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await strategy.cleanup()

if __name__ == "__main__":
    asyncio.run(main())