#!/usr/bin/env python3
"""
Qullamaggie Momentum Breakout Backtester

Implements a momentum breakout trading system:
- Find strongest momentum stocks (top 1mo/3mo gainers)
- Buy tight consolidation breakouts with tight stops
- Trail with 9/23 EMA until trend breaks
- ~25% win rate, relies on fat-tail winners (10R-50R+)

NO look-ahead bias:
- Entry at OPEN of day AFTER signal
- Exit at OPEN of day AFTER exit signal
- Stop checked intraday (exit at stop price if low < stop)

Usage:
    python momentum_breakout_backtest.py
    python momentum_breakout_backtest.py --start 2023-01-01 --end 2024-12-31
    python momentum_breakout_backtest.py --risk-pct 0.01 --max-positions 8
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
import argparse

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
    trailing_ema: int  # 9 or 23
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
    exit_reason: str  # 'trailing_stop', 'stop_loss', 'partial', 'end_of_test'
    days_held: int
    adr_at_entry: float
    initial_stop: float


class MomentumBreakoutBacktester:
    """
    Backtester for Qullamaggie-style momentum breakout system.
    """

    def __init__(
        self,
        initial_capital: float = 100000,
        risk_per_trade_pct: float = 0.01,  # 1% account risk per trade
        max_positions: int = 8,
        min_adr: float = 2.5,  # Minimum ADR% (Nordic stocks have lower volatility than US)
        min_price: float = 5.0,  # Minimum stock price (SEK)
        consolidation_days: int = 10,
        volume_breakout_ratio: float = 1.2,  # Lower for Nordic markets
        max_stop_adr_ratio: float = 0.5,  # Max stop = 50% of ADR
        consolidation_tightness: float = 3.0,  # Max range as multiple of ADR (looser for Nordic)
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

        # State
        self.cash = initial_capital
        self.positions: List[Position] = []
        self.completed_trades: List[Trade] = []
        self.equity_curve: List[Tuple[date, float]] = []

        # Database
        self.conn: Optional[asyncpg.Connection] = None

    async def setup(self):
        """Connect to database."""
        self.conn = await asyncpg.connect(DATABASE_URL)
        print(f"Connected to database")

    async def cleanup(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()

    async def get_universe(self, as_of_date: date, lookback_days: int = 90) -> List[str]:
        """
        Get universe of stocks to scan.
        Filter: price > min_price, sufficient volume, sufficient data.
        """
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
        # Add buffer for indicator calculation
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
        df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_23'] = df['close'].ewm(span=23, adjust=False).mean()

        # Volume SMA and ratio
        df['volume_sma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']

        # Consolidation range (10-day)
        df['consol_high'] = df['high'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).max()
        df['consol_low'] = df['low'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).min()
        df['consol_range_pct'] = (df['consol_high'] - df['consol_low']) / df['consol_low'] * 100

        # Momentum (1mo and 3mo returns)
        df['return_1m'] = df['close'].pct_change(periods=20) * 100
        df['return_3m'] = df['close'].pct_change(periods=60) * 100

        # Prior day high (breakout level)
        df['prior_high'] = df['high'].shift(1)

        return df

    def detect_breakout(self, df: pd.DataFrame, check_date: date) -> Optional[BreakoutSignal]:
        """
        Check if a breakout occurred on check_date.

        Conditions:
        1. Price > min_price
        2. ADR(20) >= min_adr
        3. Close > 20 EMA (uptrend)
        4. Consolidation is tight (range < 1.5 * ADR)
        5. Close > consolidation high (breakout)
        6. Volume >= 1.5x average
        """
        try:
            # Get the row for check_date
            check_ts = pd.Timestamp(check_date)
            if check_ts not in df.index:
                return None

            row = df.loc[check_ts]

            # Check required indicators exist
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

            # Condition 1: Price > min_price
            if close < self.min_price:
                return None

            # Condition 2: ADR >= min_adr
            if adr_20 < self.min_adr:
                return None

            # Condition 3: Close > 20 EMA (uptrend filter)
            if close <= ema_20:
                return None

            # Condition 4: Tight consolidation (range < tightness_multiplier * ADR)
            if consol_range_pct > adr_20 * self.consolidation_tightness:
                return None

            # Condition 5: Breakout - close above consolidation high
            # Use prior consolidation high (shifted by 1 to avoid look-ahead)
            prior_idx = df.index.get_loc(check_ts) - 1
            if prior_idx < 0:
                return None
            prior_consol_high = float(df.iloc[prior_idx]['consol_high'])
            if pd.isna(prior_consol_high):
                return None

            if close <= prior_consol_high:
                return None

            # Condition 6: Volume surge
            if volume_ratio < self.volume_breakout_ratio:
                return None

            # Calculate stop price
            # Stop = low of breakout day or consolidation low, whichever is lower
            stop_price = min(day_low, consol_low)
            stop_pct = (close - stop_price) / close * 100

            # Max stop = 50% of ADR
            max_stop_pct = adr_20 * self.max_stop_adr_ratio
            if stop_pct > max_stop_pct:
                # Adjust stop to max allowed
                stop_price = close * (1 - max_stop_pct / 100)
                stop_pct = max_stop_pct

            # Skip if stop would be too tight (< 0.5%)
            if stop_pct < 0.5:
                return None

            return BreakoutSignal(
                symbol="",  # Will be filled by caller
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

        except Exception as e:
            return None

    async def get_open_price(self, symbol: str, target_date: date) -> Optional[float]:
        """Get opening price for a specific date (entry/exit execution)."""
        query = """
        SELECT open_price::NUMERIC as open
        FROM daily_price_data
        WHERE symbol = $1 AND date = $2
        """
        row = await self.conn.fetchrow(query, symbol, target_date)
        if row and row['open']:
            return float(row['open'])

        # Try next trading days if exact date not found (weekend/holiday)
        for i in range(1, 5):
            check_date = target_date + timedelta(days=i)
            row = await self.conn.fetchrow(query, symbol, check_date)
            if row and row['open']:
                return float(row['open'])

        return None

    def calculate_position_size(self, entry_price: float, stop_price: float) -> Tuple[int, float]:
        """
        Calculate position size based on risk.

        Returns: (shares, risk_amount)
        """
        risk_per_share = entry_price - stop_price
        if risk_per_share <= 0:
            return 0, 0

        # Risk amount = account * risk_pct
        account_value = self.cash + sum(
            pos.shares * pos.entry_price for pos in self.positions
        )
        risk_amount = account_value * self.risk_per_trade_pct

        # Shares = risk_amount / risk_per_share
        shares = int(risk_amount / risk_per_share)

        # Check if we have enough cash
        position_cost = shares * entry_price
        if position_cost > self.cash:
            shares = int(self.cash / entry_price)
            risk_amount = shares * risk_per_share

        return shares, risk_amount

    def should_exit_trailing_stop(
        self, position: Position, current_close: float, ema_value: float
    ) -> bool:
        """Check if position should exit based on trailing EMA stop."""
        return current_close < ema_value

    def should_take_partial(
        self, position: Position, current_close: float
    ) -> bool:
        """Check if we should take partial profit (10% when gain > 3x ADR)."""
        if position.partial_taken:
            return False

        gain_pct = (current_close - position.entry_price) / position.entry_price * 100
        threshold = position.adr_20 * 3  # 3x ADR from entry

        return gain_pct >= threshold

    async def scan_for_signals(
        self, symbols: List[str], scan_date: date, end_date: date
    ) -> List[BreakoutSignal]:
        """Scan all symbols for breakout signals on a given date."""
        signals = []

        for symbol in symbols:
            try:
                df = await self.get_market_data(symbol, scan_date - timedelta(days=100), end_date)
                if df.empty or len(df) < 60:
                    continue

                df = self.calculate_indicators(df)
                signal = self.detect_breakout(df, scan_date)

                if signal:
                    signal.symbol = symbol
                    signals.append(signal)

            except Exception as e:
                continue

        return signals

    def rank_signals(self, signals: List[BreakoutSignal]) -> List[BreakoutSignal]:
        """Rank signals by momentum and quality."""
        # Sort by combined momentum score (1m + 3m), volume ratio
        return sorted(
            signals,
            key=lambda s: (s.momentum_1m + s.momentum_3m, s.volume_ratio),
            reverse=True
        )

    async def manage_positions(self, current_date: date, market_data_cache: Dict[str, pd.DataFrame]):
        """
        Manage existing positions:
        - Check stop losses (intraday)
        - Check trailing EMA stops (EOD)
        - Take partials if applicable
        """
        positions_to_close = []

        for i, pos in enumerate(self.positions):
            symbol = pos.symbol

            # Get today's data
            if symbol not in market_data_cache:
                continue

            df = market_data_cache[symbol]
            check_ts = pd.Timestamp(current_date)

            if check_ts not in df.index:
                continue

            row = df.loc[check_ts]
            current_low = float(row['low'])
            current_close = float(row['close'])
            current_high = float(row['high'])

            # Update highest close for trailing
            pos.highest_close = max(pos.highest_close, current_close)

            # Get trailing EMA value
            ema_col = f'ema_{pos.trailing_ema}'
            if ema_col not in df.columns or pd.isna(row.get(ema_col)):
                continue
            ema_value = float(row[ema_col])

            # Check stop loss (intraday) - exit at stop price
            if current_low <= pos.current_stop:
                exit_price = pos.current_stop  # Assume stopped out at stop
                positions_to_close.append((i, exit_price, 'stop_loss', current_date))
                continue

            # Check trailing EMA stop (EOD) - will exit at next open
            if self.should_exit_trailing_stop(pos, current_close, ema_value):
                # Mark for exit at next day's open
                positions_to_close.append((i, None, 'trailing_stop', current_date))
                continue

            # Check partial profit
            if self.should_take_partial(pos, current_close):
                partial_shares = int(pos.shares * 0.1)  # Sell 10%
                if partial_shares > 0:
                    pos.partial_taken = True
                    pos.partial_shares_sold = partial_shares
                    pos.shares -= partial_shares

                    # Record partial as a trade
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

        # Process exits (in reverse order to maintain indices)
        for i, exit_price, reason, signal_date in sorted(positions_to_close, reverse=True):
            pos = self.positions[i]

            # If trailing stop, exit at next day's open
            if exit_price is None:
                next_date = signal_date + timedelta(days=1)
                exit_price = await self.get_open_price(pos.symbol, next_date)
                if exit_price is None:
                    exit_price = pos.entry_price  # Fallback

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

    async def run_backtest(self, start_date: date, end_date: date):
        """Run the full backtest."""
        print(f"\n{'='*80}")
        print(f"QULLAMAGGIE MOMENTUM BREAKOUT BACKTEST")
        print(f"{'='*80}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.0f}")
        print(f"Risk per Trade: {self.risk_per_trade_pct:.1%}")
        print(f"Max Positions: {self.max_positions}")
        print(f"Min ADR: {self.min_adr}%")
        print(f"Min Price: {self.min_price} SEK")
        print(f"{'='*80}\n")

        # Get initial universe
        print("Scanning universe...")
        universe = await self.get_universe(start_date)
        print(f"Found {len(universe)} stocks in universe")

        # Pre-load market data for all symbols
        print("Loading market data...")
        market_data_cache: Dict[str, pd.DataFrame] = {}
        for symbol in universe:
            df = await self.get_market_data(symbol, start_date - timedelta(days=100), end_date)
            if not df.empty and len(df) >= 60:
                df = self.calculate_indicators(df)
                market_data_cache[symbol] = df
        print(f"Loaded data for {len(market_data_cache)} stocks")

        # Iterate through each trading day
        current_date = start_date
        trading_days = 0
        signals_found = 0

        while current_date <= end_date:
            # Skip weekends (rough check)
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            trading_days += 1

            # 1. Manage existing positions (check stops, trailing EMAs)
            await self.manage_positions(current_date, market_data_cache)

            # 2. Scan for new signals (if we have capacity)
            if len(self.positions) < self.max_positions:
                signals = []
                for symbol, df in market_data_cache.items():
                    # Skip if already in position
                    if any(pos.symbol == symbol for pos in self.positions):
                        continue

                    signal = self.detect_breakout(df, current_date)
                    if signal:
                        signal.symbol = symbol
                        signals.append(signal)

                if signals:
                    signals_found += len(signals)
                    ranked_signals = self.rank_signals(signals)

                    # Take top signals up to capacity
                    available_slots = self.max_positions - len(self.positions)
                    for signal in ranked_signals[:available_slots]:
                        # Get entry price (next day's open)
                        entry_date = current_date + timedelta(days=1)
                        entry_price = await self.get_open_price(signal.symbol, entry_date)

                        if entry_price is None:
                            continue

                        # Recalculate stop based on entry price
                        # Use signal's stop price but adjust if entry is different
                        stop_price = signal.stop_price

                        # Calculate position size
                        shares, risk_amount = self.calculate_position_size(entry_price, stop_price)

                        if shares <= 0:
                            continue

                        # Determine trailing EMA (9 for fast movers, 23 for slower)
                        trailing_ema = 9 if signal.adr_20 > 8 else 23

                        # Create position
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

                        print(f"  {entry_date}: BUY {signal.symbol} @ {entry_price:.2f}, "
                              f"Stop: {stop_price:.2f} ({signal.stop_pct:.1f}%), "
                              f"Shares: {shares}, ADR: {signal.adr_20:.1f}%")

            # Record equity using current day's close price (not end-of-period)
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

            # Progress update
            if trading_days % 50 == 0:
                print(f"  Day {trading_days}: Equity ${total_equity:,.0f}, "
                      f"Positions: {len(self.positions)}, Trades: {len(self.completed_trades)}")

            current_date += timedelta(days=1)

        # Close remaining positions at end
        print(f"\nClosing {len(self.positions)} remaining positions...")
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

        print(f"\nBacktest complete!")
        print(f"Trading days: {trading_days}")
        print(f"Signals detected: {signals_found}")

    def analyze_results(self):
        """Analyze and display backtest results."""
        if not self.completed_trades:
            print("No trades completed!")
            return

        print(f"\n{'='*80}")
        print(f"BACKTEST RESULTS")
        print(f"{'='*80}\n")

        # Basic stats
        total_trades = len(self.completed_trades)
        winners = [t for t in self.completed_trades if t.r_multiple > 0]
        losers = [t for t in self.completed_trades if t.r_multiple <= 0]
        win_rate = len(winners) / total_trades if total_trades > 0 else 0

        print(f"TRADE STATISTICS:")
        print(f"  Total Trades: {total_trades}")
        print(f"  Winners: {len(winners)} ({win_rate:.1%})")
        print(f"  Losers: {len(losers)} ({1-win_rate:.1%})")

        # R-multiple analysis
        r_multiples = [t.r_multiple for t in self.completed_trades]
        avg_r = np.mean(r_multiples)
        avg_winner_r = np.mean([t.r_multiple for t in winners]) if winners else 0
        avg_loser_r = np.mean([t.r_multiple for t in losers]) if losers else 0

        print(f"\nR-MULTIPLE ANALYSIS:")
        print(f"  Average R: {avg_r:.2f}")
        print(f"  Average Winner R: {avg_winner_r:.2f}")
        print(f"  Average Loser R: {avg_loser_r:.2f}")
        print(f"  Expectancy: {win_rate * avg_winner_r + (1-win_rate) * avg_loser_r:.2f}R")

        # Best and worst trades
        best_trade = max(self.completed_trades, key=lambda t: t.r_multiple)
        worst_trade = min(self.completed_trades, key=lambda t: t.r_multiple)

        print(f"\nBEST TRADES:")
        sorted_by_r = sorted(self.completed_trades, key=lambda t: t.r_multiple, reverse=True)
        for t in sorted_by_r[:5]:
            print(f"  {t.symbol}: {t.r_multiple:+.1f}R ({t.entry_date} -> {t.exit_date}, {t.exit_reason})")

        print(f"\nWORST TRADES:")
        for t in sorted_by_r[-5:]:
            print(f"  {t.symbol}: {t.r_multiple:+.1f}R ({t.entry_date} -> {t.exit_date}, {t.exit_reason})")

        # R distribution
        print(f"\nR-MULTIPLE DISTRIBUTION:")
        r_ranges = [(-float('inf'), -1), (-1, 0), (0, 1), (1, 3), (3, 5), (5, 10), (10, float('inf'))]
        for low, high in r_ranges:
            count = len([r for r in r_multiples if low < r <= high])
            if count > 0:
                label = f"{low:.0f}R to {high:.0f}R" if high != float('inf') else f">{low:.0f}R"
                if low == -float('inf'):
                    label = f"<{high:.0f}R"
                print(f"  {label}: {count} trades ({count/total_trades:.1%})")

        # Portfolio performance
        final_equity = self.cash
        total_return = (final_equity / self.initial_capital - 1) * 100

        print(f"\nPORTFOLIO PERFORMANCE:")
        print(f"  Initial Capital: ${self.initial_capital:,.0f}")
        print(f"  Final Equity: ${final_equity:,.0f}")
        print(f"  Total Return: {total_return:+.1f}%")

        # Max drawdown
        if self.equity_curve:
            equities = [e[1] for e in self.equity_curve]
            peak = equities[0]
            max_dd = 0
            for eq in equities:
                if eq > peak:
                    peak = eq
                dd = (peak - eq) / peak
                if dd > max_dd:
                    max_dd = dd
            print(f"  Max Drawdown: {max_dd*100:.1f}%")

        # Exit reason breakdown
        print(f"\nEXIT REASONS:")
        exit_reasons = defaultdict(list)
        for t in self.completed_trades:
            exit_reasons[t.exit_reason].append(t)

        for reason, trades in sorted(exit_reasons.items()):
            avg_r = np.mean([t.r_multiple for t in trades])
            print(f"  {reason}: {len(trades)} trades, avg {avg_r:+.2f}R")

        # Days held analysis
        avg_days = np.mean([t.days_held for t in self.completed_trades])
        print(f"\nHOLDING PERIOD:")
        print(f"  Average Days Held: {avg_days:.1f}")
        print(f"  Median Days Held: {np.median([t.days_held for t in self.completed_trades]):.0f}")


async def main():
    parser = argparse.ArgumentParser(description='Qullamaggie Momentum Breakout Backtester')
    parser.add_argument('--start', type=str, default='2023-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default='2024-11-30', help='End date (YYYY-MM-DD)')
    parser.add_argument('--capital', type=float, default=100000, help='Initial capital')
    parser.add_argument('--risk-pct', type=float, default=0.01, help='Risk per trade (0.01 = 1%)')
    parser.add_argument('--max-positions', type=int, default=8, help='Max concurrent positions')
    parser.add_argument('--min-adr', type=float, default=5.0, help='Minimum ADR%')
    parser.add_argument('--min-price', type=float, default=5.0, help='Minimum stock price')
    parser.add_argument('--volume-ratio', type=float, default=1.2, help='Min volume breakout ratio')
    parser.add_argument('--consol-tight', type=float, default=3.0, help='Consolidation tightness (x ADR)')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    backtester = MomentumBreakoutBacktester(
        initial_capital=args.capital,
        risk_per_trade_pct=args.risk_pct,
        max_positions=args.max_positions,
        min_adr=args.min_adr,
        min_price=args.min_price,
        volume_breakout_ratio=args.volume_ratio,
        consolidation_tightness=args.consol_tight
    )

    try:
        await backtester.setup()
        await backtester.run_backtest(start_date, end_date)
        backtester.analyze_results()
    finally:
        await backtester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
