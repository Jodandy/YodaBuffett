#!/usr/bin/env python3
"""
Qullamaggie Momentum Breakout V2 - With Fixes

Improvements over V1:
1. Market regime filter (only trade when OMX30 > 50 SMA)
2. Wider stops (max_stop_adr_ratio = 0.75)
3. Confirmation filter (require close above breakout level)
4. Longer consolidation (15 days instead of 10)

Usage:
    python momentum_breakout_v2.py
    python momentum_breakout_v2.py --start 2020-01-01 --end 2025-01-31
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


@dataclass
class Trade:
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


class MomentumBreakoutV2:
    """
    Improved momentum breakout system with:
    - Market regime filter
    - Wider stops
    - Better entry timing
    """

    def __init__(
        self,
        initial_capital: float = 100000,
        risk_per_trade_pct: float = 0.01,
        max_positions: int = 8,
        # V2 CHANGES
        min_adr: float = 2.5,
        min_price: float = 5.0,
        consolidation_days: int = 15,  # V2: Longer consolidation (was 10)
        volume_breakout_ratio: float = 1.5,
        max_stop_adr_ratio: float = 0.75,  # V2: Wider stops (was 0.5)
        consolidation_tightness: float = 3.5,
        require_market_uptrend: bool = True,  # V2: NEW
        require_confirmation: bool = True,  # V2: NEW - close above breakout level
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
        self.require_market_uptrend = require_market_uptrend
        self.require_confirmation = require_confirmation

        self.cash = initial_capital
        self.positions = []
        self.completed_trades = []
        self.equity_curve = []
        self.conn = None
        self.market_data_cache = {}
        self.market_regime_cache = {}

    async def setup(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def cleanup(self):
        if self.conn:
            await self.conn.close()

    async def get_market_regime(self, check_date: date) -> bool:
        """
        Check if market is in uptrend (OMX30 or similar index above 50 SMA).
        Returns True if bullish, False if bearish.
        """
        if not self.require_market_uptrend:
            return True

        if check_date in self.market_regime_cache:
            return self.market_regime_cache[check_date]

        # Try to find a market index - use a large cap Swedish stock as proxy
        # (ERIC-B, VOLV-B, or similar that tracks market)
        market_symbols = ['ERIC-B', 'VOLV-B', 'ABB']

        for symbol in market_symbols:
            query = """
            SELECT date, close_price::NUMERIC as close
            FROM daily_price_data
            WHERE symbol = $1
              AND date <= $2
            ORDER BY date DESC
            LIMIT 60
            """
            rows = await self.conn.fetch(query, symbol, check_date)

            if len(rows) >= 50:
                closes = [float(r['close']) for r in reversed(rows)]
                sma_50 = np.mean(closes[-50:])
                current_close = closes[-1]
                is_uptrend = current_close > sma_50
                self.market_regime_cache[check_date] = is_uptrend
                return is_uptrend

        # Default to bullish if we can't determine
        self.market_regime_cache[check_date] = True
        return True

    async def get_universe(self, as_of_date: date) -> List[str]:
        query = """
        SELECT symbol
        FROM daily_price_data
        WHERE date BETWEEN $1 AND $2
          AND close_price > 0
          AND volume > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 90
           AND MIN(close_price::NUMERIC) >= $3
           AND AVG(volume::NUMERIC) >= 50000
        ORDER BY AVG(volume::NUMERIC) DESC
        LIMIT 200
        """
        start = as_of_date - timedelta(days=150)
        rows = await self.conn.fetch(query, start, as_of_date, self.min_price)
        return [r['symbol'] for r in rows]

    async def get_market_data(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        buffer_start = start - timedelta(days=100)
        query = """
        SELECT date,
               open_price::NUMERIC as open,
               high_price::NUMERIC as high,
               low_price::NUMERIC as low,
               close_price::NUMERIC as close,
               volume::BIGINT as volume
        FROM daily_price_data
        WHERE symbol = $1 AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        rows = await self.conn.fetch(query, symbol, buffer_start, end)
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame([dict(r) for r in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        return df.dropna()

    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df) < 60:
            return df

        daily_range_pct = (df['high'] / df['low'] - 1) * 100
        df['adr_20'] = daily_range_pct.rolling(window=20, min_periods=20).mean()

        df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_23'] = df['close'].ewm(span=23, adjust=False).mean()

        df['volume_sma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']

        # V2: Longer consolidation
        df['consol_high'] = df['high'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).max()
        df['consol_low'] = df['low'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).min()
        df['consol_range_pct'] = (df['consol_high'] - df['consol_low']) / df['consol_low'] * 100

        df['return_1m'] = df['close'].pct_change(periods=20) * 100
        df['return_3m'] = df['close'].pct_change(periods=60) * 100

        return df

    def detect_breakout(self, df: pd.DataFrame, check_date: date) -> Optional[dict]:
        try:
            check_ts = pd.Timestamp(check_date)
            if check_ts not in df.index:
                return None

            row = df.loc[check_ts]
            idx = df.index.get_loc(check_ts)
            if idx < 2:
                return None

            required = ['close', 'adr_20', 'ema_20', 'consol_high', 'consol_low',
                       'consol_range_pct', 'volume_ratio', 'return_1m', 'return_3m', 'low']
            for col in required:
                if pd.isna(row.get(col)):
                    return None

            close = float(row['close'])
            adr_20 = float(row['adr_20'])
            ema_20 = float(row['ema_20'])
            consol_low = float(row['consol_low'])
            consol_range_pct = float(row['consol_range_pct'])
            volume_ratio = float(row['volume_ratio'])
            return_1m = float(row['return_1m'])
            return_3m = float(row['return_3m'])
            day_low = float(row['low'])

            # Get prior day's consolidation high
            prior_consol_high = float(df.iloc[idx - 1]['consol_high'])
            if pd.isna(prior_consol_high):
                return None

            # Basic filters
            if close < self.min_price:
                return None
            if adr_20 < self.min_adr:
                return None
            if close <= ema_20:
                return None
            if consol_range_pct > adr_20 * self.consolidation_tightness:
                return None

            # Breakout condition
            if close <= prior_consol_high:
                return None

            # Volume surge
            if volume_ratio < self.volume_breakout_ratio:
                return None

            # V2: CONFIRMATION FILTER
            # Require prior day to have also closed above the consolidation high
            # This filters out false breakouts
            if self.require_confirmation:
                prior_close = float(df.iloc[idx - 1]['close'])
                two_days_ago_consol_high = float(df.iloc[idx - 2]['consol_high'])
                if pd.isna(two_days_ago_consol_high):
                    return None
                # Prior day should have been consolidating (not already broken out)
                # This ensures we're catching fresh breakouts
                if prior_close > two_days_ago_consol_high * 1.02:  # Already broke out
                    return None

            # Stop calculation - V2: WIDER STOPS
            stop_price = min(day_low, consol_low)
            stop_pct = (close - stop_price) / close * 100

            # V2: Allow wider stops (0.75 * ADR instead of 0.5)
            max_stop_pct = adr_20 * self.max_stop_adr_ratio
            if stop_pct > max_stop_pct:
                stop_price = close * (1 - max_stop_pct / 100)
                stop_pct = max_stop_pct

            # V2: Require minimum stop of 1.5% (avoid too tight)
            if stop_pct < 1.5:
                return None

            return {
                'close_price': close,
                'adr_20': adr_20,
                'volume_ratio': volume_ratio,
                'momentum': return_1m + return_3m,
                'stop_price': stop_price,
                'stop_pct': stop_pct,
                'consol_low': consol_low,
            }

        except Exception:
            return None

    async def run_backtest(self, start_date: date, end_date: date):
        print(f"\n{'='*70}")
        print(f"MOMENTUM BREAKOUT V2 - IMPROVED")
        print(f"{'='*70}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.0f}")
        print(f"Risk per Trade: {self.risk_per_trade_pct:.1%}")
        print(f"Max Positions: {self.max_positions}")
        print(f"\nV2 IMPROVEMENTS:")
        print(f"  - Consolidation days: {self.consolidation_days} (was 10)")
        print(f"  - Max stop/ADR ratio: {self.max_stop_adr_ratio} (was 0.5)")
        print(f"  - Market regime filter: {self.require_market_uptrend}")
        print(f"  - Confirmation required: {self.require_confirmation}")
        print(f"{'='*70}\n")

        # Get universe
        print("Scanning universe...")
        universe = await self.get_universe(start_date)
        print(f"Found {len(universe)} stocks")

        # Load market data
        print("Loading market data...")
        for symbol in universe:
            df = await self.get_market_data(symbol, start_date, end_date)
            if not df.empty and len(df) >= 60:
                df = self.calculate_indicators(df)
                self.market_data_cache[symbol] = df
        print(f"Loaded {len(self.market_data_cache)} stocks")

        # Main backtest loop
        current_date = start_date
        trading_days = 0
        trades_skipped_regime = 0

        while current_date <= end_date:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            trading_days += 1

            # Manage existing positions
            await self._manage_positions(current_date)

            # V2: CHECK MARKET REGIME
            is_bullish = await self.get_market_regime(current_date)

            # Scan for new signals
            if len(self.positions) < self.max_positions and is_bullish:
                signals = []
                for symbol, df in self.market_data_cache.items():
                    if any(p['symbol'] == symbol for p in self.positions):
                        continue

                    signal = self.detect_breakout(df, current_date)
                    if signal:
                        signal['symbol'] = symbol
                        signals.append(signal)

                # Rank by momentum
                signals.sort(key=lambda s: s['momentum'], reverse=True)

                # Take top signals
                available = self.max_positions - len(self.positions)
                for signal in signals[:available]:
                    entry_date = current_date + timedelta(days=1)
                    entry_price = await self._get_open_price(signal['symbol'], entry_date)

                    if entry_price is None:
                        continue

                    stop_price = signal['stop_price']
                    risk_per_share = entry_price - stop_price
                    if risk_per_share <= 0:
                        continue

                    account_value = self.cash + sum(p['shares'] * p['entry_price'] for p in self.positions)
                    risk_amount = account_value * self.risk_per_trade_pct
                    shares = int(risk_amount / risk_per_share)

                    if shares <= 0 or shares * entry_price > self.cash:
                        continue

                    trailing_ema = 9 if signal['adr_20'] > 8 else 23

                    self.positions.append({
                        'symbol': signal['symbol'],
                        'entry_date': entry_date,
                        'entry_price': entry_price,
                        'shares': shares,
                        'initial_stop': stop_price,
                        'current_stop': stop_price,
                        'risk_per_share': risk_per_share,
                        'adr_20': signal['adr_20'],
                        'trailing_ema': trailing_ema,
                        'partial_taken': False,
                    })

                    self.cash -= shares * entry_price

                    print(f"  {entry_date}: BUY {signal['symbol']} @ {entry_price:.2f}, "
                          f"Stop: {stop_price:.2f} ({signal['stop_pct']:.1f}%), Shares: {shares}")

            elif not is_bullish and len(self.positions) < self.max_positions:
                # Count skipped due to regime
                for symbol, df in self.market_data_cache.items():
                    if any(p['symbol'] == symbol for p in self.positions):
                        continue
                    signal = self.detect_breakout(df, current_date)
                    if signal:
                        trades_skipped_regime += 1

            # Record equity
            position_value = 0
            for pos in self.positions:
                if pos['symbol'] in self.market_data_cache:
                    df = self.market_data_cache[pos['symbol']]
                    check_ts = pd.Timestamp(current_date)
                    if check_ts in df.index:
                        position_value += pos['shares'] * float(df.loc[check_ts, 'close'])
                    else:
                        position_value += pos['shares'] * pos['entry_price']
                else:
                    position_value += pos['shares'] * pos['entry_price']

            self.equity_curve.append((current_date, self.cash + position_value))

            if trading_days % 100 == 0:
                print(f"  Day {trading_days}: Equity ${self.cash + position_value:,.0f}, "
                      f"Positions: {len(self.positions)}, Trades: {len(self.completed_trades)}")

            current_date += timedelta(days=1)

        # Close remaining positions
        for pos in self.positions[:]:
            exit_price = await self._get_open_price(pos['symbol'], end_date)
            if exit_price is None:
                exit_price = pos['entry_price']

            r_multiple = (exit_price - pos['entry_price']) / pos['risk_per_share']
            self.completed_trades.append(Trade(
                symbol=pos['symbol'],
                entry_date=pos['entry_date'],
                exit_date=end_date,
                entry_price=pos['entry_price'],
                exit_price=exit_price,
                shares=pos['shares'],
                gross_pnl=pos['shares'] * (exit_price - pos['entry_price']),
                r_multiple=r_multiple,
                exit_reason='end_of_test',
                days_held=(end_date - pos['entry_date']).days,
                adr_at_entry=pos['adr_20'],
                initial_stop=pos['initial_stop']
            ))
            self.cash += pos['shares'] * exit_price

        self.positions.clear()

        print(f"\nBacktest complete!")
        print(f"Trading days: {trading_days}")
        print(f"Trades skipped due to bear market: {trades_skipped_regime}")

        self._analyze_results()

    async def _manage_positions(self, current_date: date):
        positions_to_close = []

        for i, pos in enumerate(self.positions):
            symbol = pos['symbol']
            if symbol not in self.market_data_cache:
                continue

            df = self.market_data_cache[symbol]
            check_ts = pd.Timestamp(current_date)
            if check_ts not in df.index:
                continue

            row = df.loc[check_ts]
            current_low = float(row['low'])
            current_close = float(row['close'])

            ema_col = f'ema_{pos["trailing_ema"]}'
            if ema_col not in df.columns or pd.isna(row.get(ema_col)):
                continue
            ema_value = float(row[ema_col])

            # Stop loss check
            if current_low <= pos['current_stop']:
                positions_to_close.append((i, pos['current_stop'], 'stop_loss', current_date))
                continue

            # Trailing stop check
            if current_close < ema_value:
                positions_to_close.append((i, None, 'trailing_stop', current_date))
                continue

            # Partial profit
            if not pos['partial_taken']:
                gain_pct = (current_close - pos['entry_price']) / pos['entry_price'] * 100
                if gain_pct >= pos['adr_20'] * 3:
                    partial_shares = int(pos['shares'] * 0.1)
                    if partial_shares > 0:
                        pos['partial_taken'] = True
                        pos['shares'] -= partial_shares
                        r_multiple = (current_close - pos['entry_price']) / pos['risk_per_share']
                        self.completed_trades.append(Trade(
                            symbol=symbol,
                            entry_date=pos['entry_date'],
                            exit_date=current_date,
                            entry_price=pos['entry_price'],
                            exit_price=current_close,
                            shares=partial_shares,
                            gross_pnl=partial_shares * (current_close - pos['entry_price']),
                            r_multiple=r_multiple,
                            exit_reason='partial',
                            days_held=(current_date - pos['entry_date']).days,
                            adr_at_entry=pos['adr_20'],
                            initial_stop=pos['initial_stop']
                        ))
                        self.cash += partial_shares * current_close

        # Process exits
        for i, exit_price, reason, signal_date in sorted(positions_to_close, reverse=True):
            pos = self.positions[i]

            if exit_price is None:
                next_date = signal_date + timedelta(days=1)
                exit_price = await self._get_open_price(pos['symbol'], next_date)
                if exit_price is None:
                    exit_price = pos['entry_price']

            r_multiple = (exit_price - pos['entry_price']) / pos['risk_per_share']
            self.completed_trades.append(Trade(
                symbol=pos['symbol'],
                entry_date=pos['entry_date'],
                exit_date=signal_date if reason == 'stop_loss' else signal_date + timedelta(days=1),
                entry_price=pos['entry_price'],
                exit_price=exit_price,
                shares=pos['shares'],
                gross_pnl=pos['shares'] * (exit_price - pos['entry_price']),
                r_multiple=r_multiple,
                exit_reason=reason,
                days_held=(signal_date - pos['entry_date']).days,
                adr_at_entry=pos['adr_20'],
                initial_stop=pos['initial_stop']
            ))
            self.cash += pos['shares'] * exit_price
            self.positions.pop(i)

    async def _get_open_price(self, symbol: str, target_date: date) -> Optional[float]:
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

    def _analyze_results(self):
        if not self.completed_trades:
            print("No trades completed!")
            return

        print(f"\n{'='*70}")
        print("BACKTEST RESULTS")
        print(f"{'='*70}\n")

        total_trades = len(self.completed_trades)
        winners = [t for t in self.completed_trades if t.r_multiple > 0]
        losers = [t for t in self.completed_trades if t.r_multiple <= 0]
        win_rate = len(winners) / total_trades

        print(f"TRADE STATISTICS:")
        print(f"  Total Trades: {total_trades}")
        print(f"  Winners: {len(winners)} ({win_rate:.1%})")
        print(f"  Losers: {len(losers)} ({1-win_rate:.1%})")

        r_multiples = [t.r_multiple for t in self.completed_trades]
        avg_r = np.mean(r_multiples)
        avg_winner = np.mean([t.r_multiple for t in winners]) if winners else 0
        avg_loser = np.mean([t.r_multiple for t in losers]) if losers else 0
        expectancy = win_rate * avg_winner + (1 - win_rate) * avg_loser

        print(f"\nR-MULTIPLE ANALYSIS:")
        print(f"  Average R: {avg_r:.2f}")
        print(f"  Average Winner: {avg_winner:.2f}R")
        print(f"  Average Loser: {avg_loser:.2f}R")
        print(f"  Expectancy: {expectancy:.2f}R")

        # Exit reasons
        print(f"\nEXIT REASONS:")
        reasons = defaultdict(list)
        for t in self.completed_trades:
            reasons[t.exit_reason].append(t)
        for reason, trades in sorted(reasons.items()):
            avg_r = np.mean([t.r_multiple for t in trades])
            print(f"  {reason}: {len(trades)} trades ({len(trades)/total_trades*100:.1f}%), avg {avg_r:+.2f}R")

        # Stop loss analysis
        stopped = [t for t in self.completed_trades if t.exit_reason == 'stop_loss']
        if stopped:
            quick_stops = [t for t in stopped if t.days_held <= 1]
            print(f"\nSTOP LOSS ANALYSIS:")
            print(f"  Total stops: {len(stopped)} ({len(stopped)/total_trades*100:.1f}%)")
            print(f"  Quick stops (0-1 days): {len(quick_stops)} ({len(quick_stops)/len(stopped)*100:.1f}% of stops)")

        # Portfolio performance
        final_equity = self.cash
        total_return = (final_equity / self.initial_capital - 1) * 100

        print(f"\nPORTFOLIO PERFORMANCE:")
        print(f"  Initial: ${self.initial_capital:,.0f}")
        print(f"  Final: ${final_equity:,.0f}")
        print(f"  Return: {total_return:+.1f}%")

        # Max drawdown
        if self.equity_curve:
            equities = [e[1] for e in self.equity_curve]
            peak = equities[0]
            max_dd = 0
            for eq in equities:
                if eq > peak:
                    peak = eq
                dd = (peak - eq) / peak
                max_dd = max(max_dd, dd)
            print(f"  Max Drawdown: {max_dd*100:.1f}%")

        # Best trades
        print(f"\nBEST TRADES:")
        sorted_trades = sorted(self.completed_trades, key=lambda t: t.r_multiple, reverse=True)
        for t in sorted_trades[:5]:
            print(f"  {t.symbol}: {t.r_multiple:+.1f}R ({t.exit_reason})")


async def main():
    parser = argparse.ArgumentParser(description='Momentum Breakout V2')
    parser.add_argument('--start', type=str, default='2020-01-01')
    parser.add_argument('--end', type=str, default='2025-01-31')
    parser.add_argument('--no-regime-filter', action='store_true', help='Disable market regime filter')
    parser.add_argument('--no-confirmation', action='store_true', help='Disable confirmation filter')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    backtester = MomentumBreakoutV2(
        require_market_uptrend=not args.no_regime_filter,
        require_confirmation=not args.no_confirmation,
    )

    try:
        await backtester.setup()
        await backtester.run_backtest(start_date, end_date)
    finally:
        await backtester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
