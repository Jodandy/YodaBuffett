#!/usr/bin/env python3
"""
Qullamaggie Momentum Breakout Optimizer

Runs multivariate parameter optimization and exports results to Excel.

Features:
- Extended test period (2020-present)
- Grid search over key parameters
- Excel export with all trades, summaries, and parameter analysis
- Parallel parameter testing

Usage:
    python momentum_breakout_optimizer.py
    python momentum_breakout_optimizer.py --start 2020-01-01 --end 2025-12-31
    python momentum_breakout_optimizer.py --quick  # Fast test with fewer parameters
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from collections import defaultdict
import argparse
import itertools
from pathlib import Path

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


@dataclass
class BreakoutSignal:
    """A detected breakout setup."""
    symbol: str
    signal_date: date
    close_price: float
    consolidation_high: float
    consolidation_low: float
    consolidation_days: int
    adr_20: float
    volume_ratio: float
    ema_20: float
    momentum_1m: float
    momentum_3m: float
    stop_price: float
    stop_pct: float


@dataclass
class Position:
    """An open position."""
    symbol: str
    entry_date: date
    entry_price: float
    shares: int
    initial_stop: float
    current_stop: float
    risk_per_share: float
    adr_20: float
    trailing_ema: int
    partial_taken: bool = False
    partial_shares_sold: int = 0
    highest_close: float = 0.0


@dataclass
class Trade:
    """A completed trade."""
    symbol: str
    entry_date: date
    exit_date: date
    entry_price: float
    exit_price: float
    shares: int
    gross_pnl: float
    r_multiple: float
    exit_reason: str
    days_held: int
    adr_at_entry: float
    initial_stop: float

    # Additional metadata for analysis
    momentum_1m: float = 0.0
    momentum_3m: float = 0.0
    volume_ratio: float = 0.0
    consol_range_pct: float = 0.0


@dataclass
class BacktestResult:
    """Summary of a backtest run."""
    params: Dict[str, Any]
    trades: List[Trade]
    total_trades: int
    win_rate: float
    avg_r: float
    avg_winner_r: float
    avg_loser_r: float
    expectancy: float
    total_return_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    final_equity: float
    equity_curve: List[Tuple[date, float]]


class MomentumBreakoutBacktester:
    """
    Backtester for Qullamaggie-style momentum breakout system.
    """

    def __init__(
        self,
        initial_capital: float = 100000,
        risk_per_trade_pct: float = 0.01,
        max_positions: int = 8,
        min_adr: float = 2.5,
        min_price: float = 5.0,
        consolidation_days: int = 10,
        volume_breakout_ratio: float = 1.2,
        max_stop_adr_ratio: float = 0.5,
        consolidation_tightness: float = 3.0,
        trailing_ema_fast: int = 9,
        trailing_ema_slow: int = 23,
        adr_threshold_for_fast_ema: float = 8.0,
        partial_profit_adr_multiple: float = 3.0,
        partial_sell_pct: float = 0.1,
    ):
        self.initial_capital = initial_capital
        self.risk_per_trade_pct = risk_per_trade_pct
        self.max_positions = max_positions
        self.min_adr = min_adr
        self.min_price = min_price
        self.consolidation_days = consolidation_days
        self.volume_breakout_ratio = volume_breakout_ratio
        self.max_stop_adr_ratio = max_stop_adr_ratio
        self.consolidation_tightness = consolidation_tightness
        self.trailing_ema_fast = trailing_ema_fast
        self.trailing_ema_slow = trailing_ema_slow
        self.adr_threshold_for_fast_ema = adr_threshold_for_fast_ema
        self.partial_profit_adr_multiple = partial_profit_adr_multiple
        self.partial_sell_pct = partial_sell_pct

        # State
        self.cash = initial_capital
        self.positions: List[Position] = []
        self.completed_trades: List[Trade] = []
        self.equity_curve: List[Tuple[date, float]] = []
        self.conn: Optional[asyncpg.Connection] = None

    def reset(self):
        """Reset state for a new backtest run."""
        self.cash = self.initial_capital
        self.positions = []
        self.completed_trades = []
        self.equity_curve = []

    def get_params_dict(self) -> Dict[str, Any]:
        """Get current parameters as dictionary."""
        return {
            'min_adr': self.min_adr,
            'volume_breakout_ratio': self.volume_breakout_ratio,
            'consolidation_tightness': self.consolidation_tightness,
            'max_stop_adr_ratio': self.max_stop_adr_ratio,
            'consolidation_days': self.consolidation_days,
            'trailing_ema_fast': self.trailing_ema_fast,
            'trailing_ema_slow': self.trailing_ema_slow,
            'adr_threshold_for_fast_ema': self.adr_threshold_for_fast_ema,
            'partial_profit_adr_multiple': self.partial_profit_adr_multiple,
            'risk_per_trade_pct': self.risk_per_trade_pct,
            'max_positions': self.max_positions,
        }

    async def setup(self, conn: asyncpg.Connection = None):
        """Connect to database."""
        if conn:
            self.conn = conn
        else:
            self.conn = await asyncpg.connect(DATABASE_URL)

    async def cleanup(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def get_universe(self, as_of_date: date, lookback_days: int = 90) -> List[str]:
        """Get universe of stocks to scan."""
        start_date = as_of_date - timedelta(days=lookback_days + 60)

        query = """
        SELECT symbol,
               COUNT(*) as trading_days,
               AVG(close_price::NUMERIC) as avg_price,
               AVG(volume::NUMERIC) as avg_volume,
               MIN(close_price::NUMERIC) as min_price
        FROM daily_price_data
        WHERE date BETWEEN $1 AND $2
          AND close_price > 0
          AND volume > 0
        GROUP BY symbol
        HAVING COUNT(*) >= $3
           AND MIN(close_price::NUMERIC) >= $4
           AND AVG(volume::NUMERIC) >= 50000
        ORDER BY AVG(volume::NUMERIC) DESC
        LIMIT 200
        """

        rows = await self.conn.fetch(
            query, start_date, as_of_date, lookback_days, self.min_price
        )

        return [row['symbol'] for row in rows]

    async def get_market_data(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """Get OHLCV data for a symbol."""
        buffer_start = start_date - timedelta(days=100)

        query = """
        SELECT date,
               open_price::NUMERIC as open,
               high_price::NUMERIC as high,
               low_price::NUMERIC as low,
               close_price::NUMERIC as close,
               volume::BIGINT as volume
        FROM daily_price_data
        WHERE symbol = $1
          AND date BETWEEN $2 AND $3
        ORDER BY date
        """

        rows = await self.conn.fetch(query, symbol, buffer_start, end_date)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

        return df.dropna()

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate all required indicators on the dataframe."""
        if len(df) < 60:
            return df

        # ADR(20): Average Daily Range
        daily_range_pct = (df['high'] / df['low'] - 1) * 100
        df['adr_20'] = daily_range_pct.rolling(window=20, min_periods=20).mean()

        # EMAs for trend and trailing stops
        df['ema_9'] = df['close'].ewm(span=self.trailing_ema_fast, adjust=False).mean()
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_23'] = df['close'].ewm(span=self.trailing_ema_slow, adjust=False).mean()

        # Volume SMA and ratio
        df['volume_sma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']

        # Consolidation range
        df['consol_high'] = df['high'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).max()
        df['consol_low'] = df['low'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).min()
        df['consol_range_pct'] = (df['consol_high'] - df['consol_low']) / df['consol_low'] * 100

        # Momentum
        df['return_1m'] = df['close'].pct_change(periods=20) * 100
        df['return_3m'] = df['close'].pct_change(periods=60) * 100

        # Prior day high
        df['prior_high'] = df['high'].shift(1)

        return df

    def detect_breakout(self, df: pd.DataFrame, check_date: date) -> Optional[BreakoutSignal]:
        """Check if a breakout occurred on check_date."""
        try:
            check_ts = pd.Timestamp(check_date)
            if check_ts not in df.index:
                return None

            row = df.loc[check_ts]

            required = ['close', 'adr_20', 'ema_20', 'consol_high', 'consol_low',
                       'consol_range_pct', 'volume_ratio', 'return_1m', 'return_3m', 'low']
            for col in required:
                if pd.isna(row.get(col)):
                    return None

            close = float(row['close'])
            adr_20 = float(row['adr_20'])
            ema_20 = float(row['ema_20'])
            consol_high = float(row['consol_high'])
            consol_low = float(row['consol_low'])
            consol_range_pct = float(row['consol_range_pct'])
            volume_ratio = float(row['volume_ratio'])
            return_1m = float(row['return_1m'])
            return_3m = float(row['return_3m'])
            day_low = float(row['low'])

            # Conditions
            if close < self.min_price:
                return None
            if adr_20 < self.min_adr:
                return None
            if close <= ema_20:
                return None
            if consol_range_pct > adr_20 * self.consolidation_tightness:
                return None

            # Breakout check
            prior_idx = df.index.get_loc(check_ts) - 1
            if prior_idx < 0:
                return None
            prior_consol_high = float(df.iloc[prior_idx]['consol_high'])
            if pd.isna(prior_consol_high):
                return None
            if close <= prior_consol_high:
                return None

            # Volume surge
            if volume_ratio < self.volume_breakout_ratio:
                return None

            # Stop price
            stop_price = min(day_low, consol_low)
            stop_pct = (close - stop_price) / close * 100

            max_stop_pct = adr_20 * self.max_stop_adr_ratio
            if stop_pct > max_stop_pct:
                stop_price = close * (1 - max_stop_pct / 100)
                stop_pct = max_stop_pct

            if stop_pct < 0.5:
                return None

            return BreakoutSignal(
                symbol="",
                signal_date=check_date,
                close_price=close,
                consolidation_high=prior_consol_high,
                consolidation_low=consol_low,
                consolidation_days=self.consolidation_days,
                adr_20=adr_20,
                volume_ratio=volume_ratio,
                ema_20=ema_20,
                momentum_1m=return_1m,
                momentum_3m=return_3m,
                stop_price=stop_price,
                stop_pct=stop_pct
            )

        except Exception:
            return None

    async def get_open_price(self, symbol: str, target_date: date) -> Optional[float]:
        """Get opening price for a specific date."""
        query = """
        SELECT open_price::NUMERIC as open
        FROM daily_price_data
        WHERE symbol = $1 AND date = $2
        """
        row = await self.conn.fetchrow(query, symbol, target_date)
        if row and row['open']:
            return float(row['open'])

        for i in range(1, 5):
            check_date = target_date + timedelta(days=i)
            row = await self.conn.fetchrow(query, symbol, check_date)
            if row and row['open']:
                return float(row['open'])

        return None

    def calculate_position_size(self, entry_price: float, stop_price: float) -> Tuple[int, float]:
        """Calculate position size based on risk."""
        risk_per_share = entry_price - stop_price
        if risk_per_share <= 0:
            return 0, 0

        account_value = self.cash + sum(
            pos.shares * pos.entry_price for pos in self.positions
        )
        risk_amount = account_value * self.risk_per_trade_pct

        shares = int(risk_amount / risk_per_share)

        position_cost = shares * entry_price
        if position_cost > self.cash:
            shares = int(self.cash / entry_price)
            risk_amount = shares * risk_per_share

        return shares, risk_amount

    def rank_signals(self, signals: List[BreakoutSignal]) -> List[BreakoutSignal]:
        """Rank signals by momentum and quality."""
        return sorted(
            signals,
            key=lambda s: (s.momentum_1m + s.momentum_3m, s.volume_ratio),
            reverse=True
        )

    async def manage_positions(self, current_date: date, market_data_cache: Dict[str, pd.DataFrame]):
        """Manage existing positions."""
        positions_to_close = []

        for i, pos in enumerate(self.positions):
            symbol = pos.symbol

            if symbol not in market_data_cache:
                continue

            df = market_data_cache[symbol]
            check_ts = pd.Timestamp(current_date)

            if check_ts not in df.index:
                continue

            row = df.loc[check_ts]
            current_low = float(row['low'])
            current_close = float(row['close'])

            pos.highest_close = max(pos.highest_close, current_close)

            ema_col = f'ema_{pos.trailing_ema}'
            if ema_col not in df.columns or pd.isna(row.get(ema_col)):
                continue
            ema_value = float(row[ema_col])

            # Stop loss check
            if current_low <= pos.current_stop:
                exit_price = pos.current_stop
                positions_to_close.append((i, exit_price, 'stop_loss', current_date))
                continue

            # Trailing stop check
            if current_close < ema_value:
                positions_to_close.append((i, None, 'trailing_stop', current_date))
                continue

            # Partial profit check
            if not pos.partial_taken:
                gain_pct = (current_close - pos.entry_price) / pos.entry_price * 100
                threshold = pos.adr_20 * self.partial_profit_adr_multiple

                if gain_pct >= threshold:
                    partial_shares = int(pos.shares * self.partial_sell_pct)
                    if partial_shares > 0:
                        pos.partial_taken = True
                        pos.partial_shares_sold = partial_shares
                        pos.shares -= partial_shares

                        r_multiple = (current_close - pos.entry_price) / pos.risk_per_share
                        partial_trade = Trade(
                            symbol=symbol,
                            entry_date=pos.entry_date,
                            exit_date=current_date,
                            entry_price=pos.entry_price,
                            exit_price=current_close,
                            shares=partial_shares,
                            gross_pnl=partial_shares * (current_close - pos.entry_price),
                            r_multiple=r_multiple,
                            exit_reason='partial',
                            days_held=(current_date - pos.entry_date).days,
                            adr_at_entry=pos.adr_20,
                            initial_stop=pos.initial_stop
                        )
                        self.completed_trades.append(partial_trade)
                        self.cash += partial_shares * current_close

        # Process exits
        for i, exit_price, reason, signal_date in sorted(positions_to_close, reverse=True):
            pos = self.positions[i]

            if exit_price is None:
                next_date = signal_date + timedelta(days=1)
                exit_price = await self.get_open_price(pos.symbol, next_date)
                if exit_price is None:
                    exit_price = pos.entry_price

            r_multiple = (exit_price - pos.entry_price) / pos.risk_per_share
            trade = Trade(
                symbol=pos.symbol,
                entry_date=pos.entry_date,
                exit_date=signal_date if reason == 'stop_loss' else signal_date + timedelta(days=1),
                entry_price=pos.entry_price,
                exit_price=exit_price,
                shares=pos.shares,
                gross_pnl=pos.shares * (exit_price - pos.entry_price),
                r_multiple=r_multiple,
                exit_reason=reason,
                days_held=(signal_date - pos.entry_date).days,
                adr_at_entry=pos.adr_20,
                initial_stop=pos.initial_stop
            )
            self.completed_trades.append(trade)
            self.cash += pos.shares * exit_price
            self.positions.pop(i)

    async def run_backtest(self, start_date: date, end_date: date, verbose: bool = True) -> BacktestResult:
        """Run the full backtest."""
        self.reset()

        if verbose:
            print(f"  Running: ADR>{self.min_adr}%, Vol>{self.volume_breakout_ratio}x, "
                  f"Tight<{self.consolidation_tightness}x")

        # Get universe
        universe = await self.get_universe(start_date)

        # Pre-load market data
        market_data_cache: Dict[str, pd.DataFrame] = {}
        for symbol in universe:
            df = await self.get_market_data(symbol, start_date - timedelta(days=100), end_date)
            if not df.empty and len(df) >= 60:
                df = self.calculate_indicators(df)
                market_data_cache[symbol] = df

        # Iterate through each trading day
        current_date = start_date
        trading_days = 0

        while current_date <= end_date:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            trading_days += 1

            # Manage positions
            await self.manage_positions(current_date, market_data_cache)

            # Scan for new signals
            if len(self.positions) < self.max_positions:
                signals = []
                for symbol, df in market_data_cache.items():
                    if any(pos.symbol == symbol for pos in self.positions):
                        continue

                    signal = self.detect_breakout(df, current_date)
                    if signal:
                        signal.symbol = symbol
                        signals.append(signal)

                if signals:
                    ranked_signals = self.rank_signals(signals)
                    available_slots = self.max_positions - len(self.positions)

                    for signal in ranked_signals[:available_slots]:
                        entry_date = current_date + timedelta(days=1)
                        entry_price = await self.get_open_price(signal.symbol, entry_date)

                        if entry_price is None:
                            continue

                        stop_price = signal.stop_price
                        shares, risk_amount = self.calculate_position_size(entry_price, stop_price)

                        if shares <= 0:
                            continue

                        trailing_ema = self.trailing_ema_fast if signal.adr_20 > self.adr_threshold_for_fast_ema else self.trailing_ema_slow

                        position = Position(
                            symbol=signal.symbol,
                            entry_date=entry_date,
                            entry_price=entry_price,
                            shares=shares,
                            initial_stop=stop_price,
                            current_stop=stop_price,
                            risk_per_share=entry_price - stop_price,
                            adr_20=signal.adr_20,
                            trailing_ema=trailing_ema,
                            highest_close=entry_price
                        )

                        self.cash -= shares * entry_price
                        self.positions.append(position)

            # Record equity
            position_value = 0
            for pos in self.positions:
                if pos.symbol in market_data_cache:
                    df = market_data_cache[pos.symbol]
                    check_ts = pd.Timestamp(current_date)
                    if check_ts in df.index:
                        position_value += pos.shares * float(df.loc[check_ts, 'close'])
                    else:
                        position_value += pos.shares * pos.entry_price
                else:
                    position_value += pos.shares * pos.entry_price

            total_equity = self.cash + position_value
            self.equity_curve.append((current_date, total_equity))

            current_date += timedelta(days=1)

        # Close remaining positions
        for pos in self.positions[:]:
            exit_price = await self.get_open_price(pos.symbol, end_date)
            if exit_price is None:
                exit_price = pos.entry_price

            r_multiple = (exit_price - pos.entry_price) / pos.risk_per_share
            trade = Trade(
                symbol=pos.symbol,
                entry_date=pos.entry_date,
                exit_date=end_date,
                entry_price=pos.entry_price,
                exit_price=exit_price,
                shares=pos.shares,
                gross_pnl=pos.shares * (exit_price - pos.entry_price),
                r_multiple=r_multiple,
                exit_reason='end_of_test',
                days_held=(end_date - pos.entry_date).days,
                adr_at_entry=pos.adr_20,
                initial_stop=pos.initial_stop
            )
            self.completed_trades.append(trade)
            self.cash += pos.shares * exit_price

        self.positions.clear()

        # Calculate results
        return self._calculate_results()

    def _calculate_results(self) -> BacktestResult:
        """Calculate backtest result metrics."""
        trades = self.completed_trades
        params = self.get_params_dict()

        if not trades:
            return BacktestResult(
                params=params,
                trades=[],
                total_trades=0,
                win_rate=0,
                avg_r=0,
                avg_winner_r=0,
                avg_loser_r=0,
                expectancy=0,
                total_return_pct=0,
                max_drawdown_pct=0,
                sharpe_ratio=0,
                final_equity=self.initial_capital,
                equity_curve=self.equity_curve
            )

        total_trades = len(trades)
        winners = [t for t in trades if t.r_multiple > 0]
        losers = [t for t in trades if t.r_multiple <= 0]
        win_rate = len(winners) / total_trades if total_trades > 0 else 0

        r_multiples = [t.r_multiple for t in trades]
        avg_r = np.mean(r_multiples)
        avg_winner_r = np.mean([t.r_multiple for t in winners]) if winners else 0
        avg_loser_r = np.mean([t.r_multiple for t in losers]) if losers else 0
        expectancy = win_rate * avg_winner_r + (1 - win_rate) * avg_loser_r

        final_equity = self.cash
        total_return_pct = (final_equity / self.initial_capital - 1) * 100

        # Max drawdown
        max_dd = 0
        if self.equity_curve:
            equities = [e[1] for e in self.equity_curve]
            peak = equities[0]
            for eq in equities:
                if eq > peak:
                    peak = eq
                dd = (peak - eq) / peak
                if dd > max_dd:
                    max_dd = dd

        # Sharpe ratio (simplified - annualized)
        if len(self.equity_curve) > 1:
            returns = []
            for i in range(1, len(self.equity_curve)):
                r = (self.equity_curve[i][1] / self.equity_curve[i-1][1]) - 1
                returns.append(r)
            if returns and np.std(returns) > 0:
                sharpe = (np.mean(returns) / np.std(returns)) * np.sqrt(252)
            else:
                sharpe = 0
        else:
            sharpe = 0

        return BacktestResult(
            params=params,
            trades=trades,
            total_trades=total_trades,
            win_rate=win_rate,
            avg_r=avg_r,
            avg_winner_r=avg_winner_r,
            avg_loser_r=avg_loser_r,
            expectancy=expectancy,
            total_return_pct=total_return_pct,
            max_drawdown_pct=max_dd * 100,
            sharpe_ratio=sharpe,
            final_equity=final_equity,
            equity_curve=self.equity_curve
        )


async def run_parameter_optimization(
    start_date: date,
    end_date: date,
    param_grid: Dict[str, List[Any]],
    conn: asyncpg.Connection
) -> List[BacktestResult]:
    """Run backtest for each parameter combination."""
    results = []

    # Generate all combinations
    param_names = list(param_grid.keys())
    param_values = list(param_grid.values())
    combinations = list(itertools.product(*param_values))

    print(f"\nRunning {len(combinations)} parameter combinations...")
    print(f"Period: {start_date} to {end_date}")
    print("-" * 60)

    for i, combo in enumerate(combinations):
        params = dict(zip(param_names, combo))

        backtester = MomentumBreakoutBacktester(**params)
        await backtester.setup(conn)

        try:
            result = await backtester.run_backtest(start_date, end_date, verbose=True)
            results.append(result)

            print(f"  [{i+1}/{len(combinations)}] Trades: {result.total_trades}, "
                  f"Win: {result.win_rate:.1%}, Return: {result.total_return_pct:+.1f}%, "
                  f"Expectancy: {result.expectancy:.2f}R")
        except Exception as e:
            print(f"  [{i+1}/{len(combinations)}] Error: {e}")
            continue

    return results


def export_to_excel(
    results: List[BacktestResult],
    start_date: date,
    end_date: date,
    output_path: str
):
    """Export all results to Excel."""
    print(f"\nExporting results to {output_path}...")

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 1. Summary sheet - all parameter combinations
        summary_data = []
        for i, r in enumerate(results):
            row = {
                'Run': i + 1,
                'Total Trades': r.total_trades,
                'Win Rate': r.win_rate,
                'Avg R': r.avg_r,
                'Avg Winner R': r.avg_winner_r,
                'Avg Loser R': r.avg_loser_r,
                'Expectancy': r.expectancy,
                'Total Return %': r.total_return_pct,
                'Max Drawdown %': r.max_drawdown_pct,
                'Sharpe Ratio': r.sharpe_ratio,
                'Final Equity': r.final_equity,
            }
            # Add parameters
            for k, v in r.params.items():
                row[f'Param_{k}'] = v
            summary_data.append(row)

        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values('Expectancy', ascending=False)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

        # 2. All trades from best run
        if results:
            best_result = max(results, key=lambda r: r.expectancy)
            trades_data = []
            for t in best_result.trades:
                trades_data.append({
                    'Symbol': t.symbol,
                    'Entry Date': t.entry_date,
                    'Exit Date': t.exit_date,
                    'Entry Price': t.entry_price,
                    'Exit Price': t.exit_price,
                    'Shares': t.shares,
                    'Gross PnL': t.gross_pnl,
                    'R Multiple': t.r_multiple,
                    'Exit Reason': t.exit_reason,
                    'Days Held': t.days_held,
                    'ADR at Entry': t.adr_at_entry,
                    'Initial Stop': t.initial_stop,
                })

            if trades_data:
                trades_df = pd.DataFrame(trades_data)
                trades_df = trades_df.sort_values('Entry Date')
                trades_df.to_excel(writer, sheet_name='Best_Run_Trades', index=False)

            # 3. Equity curve from best run
            if best_result.equity_curve:
                equity_data = [{'Date': d, 'Equity': e} for d, e in best_result.equity_curve]
                equity_df = pd.DataFrame(equity_data)
                equity_df.to_excel(writer, sheet_name='Best_Run_Equity', index=False)

        # 4. All trades combined (for deeper analysis)
        all_trades_data = []
        for run_idx, r in enumerate(results):
            for t in r.trades:
                row = {
                    'Run': run_idx + 1,
                    'Symbol': t.symbol,
                    'Entry Date': t.entry_date,
                    'Exit Date': t.exit_date,
                    'Entry Price': t.entry_price,
                    'Exit Price': t.exit_price,
                    'Shares': t.shares,
                    'Gross PnL': t.gross_pnl,
                    'R Multiple': t.r_multiple,
                    'Exit Reason': t.exit_reason,
                    'Days Held': t.days_held,
                    'ADR at Entry': t.adr_at_entry,
                    'Initial Stop': t.initial_stop,
                }
                # Add params for this run
                for k, v in r.params.items():
                    row[f'Param_{k}'] = v
                all_trades_data.append(row)

        if all_trades_data:
            all_trades_df = pd.DataFrame(all_trades_data)
            all_trades_df.to_excel(writer, sheet_name='All_Trades', index=False)

        # 5. Parameter analysis
        param_analysis = []
        for param_name in results[0].params.keys() if results else []:
            unique_values = set(r.params[param_name] for r in results)
            for val in sorted(unique_values):
                matching = [r for r in results if r.params[param_name] == val]
                if matching:
                    avg_return = np.mean([r.total_return_pct for r in matching])
                    avg_expectancy = np.mean([r.expectancy for r in matching])
                    avg_win_rate = np.mean([r.win_rate for r in matching])
                    avg_trades = np.mean([r.total_trades for r in matching])
                    param_analysis.append({
                        'Parameter': param_name,
                        'Value': val,
                        'Avg Return %': avg_return,
                        'Avg Expectancy': avg_expectancy,
                        'Avg Win Rate': avg_win_rate,
                        'Avg Trades': avg_trades,
                        'Runs': len(matching)
                    })

        if param_analysis:
            param_df = pd.DataFrame(param_analysis)
            param_df.to_excel(writer, sheet_name='Parameter_Analysis', index=False)

        # 6. Metadata
        metadata = {
            'Start Date': [str(start_date)],
            'End Date': [str(end_date)],
            'Total Runs': [len(results)],
            'Generated At': [datetime.now().isoformat()],
        }
        meta_df = pd.DataFrame(metadata)
        meta_df.to_excel(writer, sheet_name='Metadata', index=False)

    print(f"  Exported {len(results)} runs with {sum(r.total_trades for r in results)} total trades")


async def main():
    parser = argparse.ArgumentParser(description='Momentum Breakout Parameter Optimizer')
    parser.add_argument('--start', type=str, default='2020-01-01', help='Start date')
    parser.add_argument('--end', type=str, default='2025-12-31', help='End date')
    parser.add_argument('--output', type=str, default='momentum_backtest_results.xlsx', help='Output Excel file')
    parser.add_argument('--quick', action='store_true', help='Quick test with fewer parameters')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    # Parameter grid
    if args.quick:
        param_grid = {
            'min_adr': [2.5, 4.0],
            'volume_breakout_ratio': [1.2, 1.5],
            'consolidation_tightness': [2.5, 3.5],
        }
    else:
        # Optimized grid based on initial findings
        # ~36 combinations instead of 300
        param_grid = {
            'min_adr': [2.0, 2.5, 3.5],
            'volume_breakout_ratio': [1.0, 1.2, 1.5],
            'consolidation_tightness': [2.0, 2.5, 3.0, 3.5],
        }

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        results = await run_parameter_optimization(
            start_date, end_date, param_grid, conn
        )

        if results:
            export_to_excel(results, start_date, end_date, args.output)

            # Print best results
            print("\n" + "=" * 60)
            print("TOP 5 PARAMETER COMBINATIONS BY EXPECTANCY")
            print("=" * 60)

            sorted_results = sorted(results, key=lambda r: r.expectancy, reverse=True)
            for i, r in enumerate(sorted_results[:5]):
                print(f"\n#{i+1}: Expectancy {r.expectancy:.2f}R")
                print(f"    Return: {r.total_return_pct:+.1f}%, Max DD: {r.max_drawdown_pct:.1f}%")
                print(f"    Trades: {r.total_trades}, Win Rate: {r.win_rate:.1%}")
                print(f"    Params: ADR>{r.params['min_adr']}%, "
                      f"Vol>{r.params['volume_breakout_ratio']}x, "
                      f"Tight<{r.params['consolidation_tightness']}x")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
