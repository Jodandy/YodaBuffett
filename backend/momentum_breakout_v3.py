#!/usr/bin/env python3
"""
Qullamaggie Momentum Breakout V3 - AGGRESSIVE

Key changes from V2:
1. 2% risk per trade (was 1%)
2. Pyramiding: Add 50% more at +3R
3. 20 EMA trailing stop (was 9 EMA for volatile)
4. No early partials - let winners run
5. Focus on TOP 30 momentum stocks only
6. Require breakout on HIGH volume day

Usage:
    python momentum_breakout_v3.py --start 2020-01-01 --end 2021-12-31
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
    is_pyramid: bool = False


class MomentumBreakoutV3:
    """
    Aggressive momentum breakout system with pyramiding.
    """

    def __init__(
        self,
        initial_capital: float = 100000,
        risk_per_trade_pct: float = 0.02,  # V3: 2% risk (was 1%)
        max_positions: int = 6,  # V3: Fewer but bigger positions
        min_adr: float = 2.5,  # V3: Back to 2.5%
        min_price: float = 5.0,
        consolidation_days: int = 10,
        volume_breakout_ratio: float = 1.2,  # V3: Lowered to catch more
        max_stop_adr_ratio: float = 0.75,
        consolidation_tightness: float = 3.5,  # V3: Loosened
        trailing_ema: int = 20,  # V3: 20 EMA for all (was 9 for volatile)
        enable_pyramiding: bool = True,  # V3: NEW
        pyramid_at_r: float = 3.0,  # V3: Add at +3R
        pyramid_size_pct: float = 0.5,  # V3: Add 50% of original position
        top_n_momentum: int = 100,  # V3: Wider universe
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
        self.trailing_ema = trailing_ema
        self.enable_pyramiding = enable_pyramiding
        self.pyramid_at_r = pyramid_at_r
        self.pyramid_size_pct = pyramid_size_pct
        self.top_n_momentum = top_n_momentum

        self.cash = initial_capital
        self.positions = []
        self.completed_trades = []
        self.equity_curve = []
        self.conn = None
        self.market_data_cache = {}
        self.momentum_rankings = {}  # symbol -> momentum score

    async def setup(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def cleanup(self):
        if self.conn:
            await self.conn.close()

    async def calculate_momentum_rankings(self, as_of_date: date) -> Dict[str, float]:
        """
        Rank all stocks by momentum (3-month return).
        Only trade the top N momentum leaders.
        """
        query = """
        WITH recent_prices AS (
            SELECT symbol, date, close_price::NUMERIC as close,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
            FROM daily_price_data
            WHERE date <= $1 AND date >= $2
        ),
        momentum AS (
            SELECT
                symbol,
                MAX(CASE WHEN rn = 1 THEN close END) as current_price,
                MAX(CASE WHEN rn >= 60 THEN close END) as price_3m_ago
            FROM recent_prices
            WHERE rn <= 65
            GROUP BY symbol
            HAVING COUNT(*) >= 60
        )
        SELECT symbol,
               (current_price / NULLIF(price_3m_ago, 0) - 1) * 100 as momentum_3m
        FROM momentum
        WHERE price_3m_ago > 0 AND current_price > $3
        ORDER BY momentum_3m DESC
        LIMIT $4
        """
        start = as_of_date - timedelta(days=100)
        rows = await self.conn.fetch(query, as_of_date, start, self.min_price, self.top_n_momentum * 2)

        rankings = {}
        for row in rows:
            if row['momentum_3m'] is not None:
                rankings[row['symbol']] = float(row['momentum_3m'])

        return rankings

    async def get_universe(self, as_of_date: date) -> List[str]:
        """Get only the top momentum stocks."""
        self.momentum_rankings = await self.calculate_momentum_rankings(as_of_date)

        # Get top N by momentum
        sorted_symbols = sorted(self.momentum_rankings.keys(),
                               key=lambda s: self.momentum_rankings.get(s, 0),
                               reverse=True)

        return sorted_symbols[:self.top_n_momentum]

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

        # V3: Use 20 EMA for trailing stop
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['ema_10'] = df['close'].ewm(span=10, adjust=False).mean()

        df['volume_sma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']

        df['consol_high'] = df['high'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).max()
        df['consol_low'] = df['low'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).min()
        df['consol_range_pct'] = (df['consol_high'] - df['consol_low']) / df['consol_low'] * 100

        df['return_1m'] = df['close'].pct_change(periods=20) * 100
        df['return_3m'] = df['close'].pct_change(periods=60) * 100

        return df

    def detect_breakout(self, df: pd.DataFrame, check_date: date, symbol: str) -> Optional[dict]:
        try:
            check_ts = pd.Timestamp(check_date)
            if check_ts not in df.index:
                return None

            row = df.loc[check_ts]
            idx = df.index.get_loc(check_ts)
            if idx < 2:
                return None

            required = ['close', 'adr_20', 'ema_20', 'consol_high', 'consol_low',
                       'consol_range_pct', 'volume_ratio', 'low']
            for col in required:
                if pd.isna(row.get(col)):
                    return None

            close = float(row['close'])
            adr_20 = float(row['adr_20'])
            ema_20 = float(row['ema_20'])
            consol_low = float(row['consol_low'])
            consol_range_pct = float(row['consol_range_pct'])
            volume_ratio = float(row['volume_ratio'])
            day_low = float(row['low'])

            prior_consol_high = float(df.iloc[idx - 1]['consol_high'])
            if pd.isna(prior_consol_high):
                return None

            # V3: Higher ADR requirement
            if close < self.min_price:
                return None
            if adr_20 < self.min_adr:
                return None
            if close <= ema_20:
                return None
            if consol_range_pct > adr_20 * self.consolidation_tightness:
                return None
            if close <= prior_consol_high:
                return None
            if volume_ratio < self.volume_breakout_ratio:
                return None

            # V3: Prefer top momentum stocks (but don't exclude entirely)
            momentum_rank = 999
            if symbol in self.momentum_rankings:
                sorted_symbols = sorted(self.momentum_rankings.keys(),
                                       key=lambda s: self.momentum_rankings.get(s, 0),
                                       reverse=True)
                if symbol in sorted_symbols:
                    momentum_rank = sorted_symbols.index(symbol)

            # Stop calculation
            stop_price = min(day_low, consol_low)
            stop_pct = (close - stop_price) / close * 100

            max_stop_pct = adr_20 * self.max_stop_adr_ratio
            if stop_pct > max_stop_pct:
                stop_price = close * (1 - max_stop_pct / 100)
                stop_pct = max_stop_pct

            if stop_pct < 1.5:
                return None

            return {
                'close_price': close,
                'adr_20': adr_20,
                'volume_ratio': volume_ratio,
                'momentum_rank': momentum_rank,
                'momentum_score': self.momentum_rankings.get(symbol, 0),
                'stop_price': stop_price,
                'stop_pct': stop_pct,
                'consol_low': consol_low,
            }

        except Exception:
            return None

    async def run_backtest(self, start_date: date, end_date: date):
        print(f"\n{'='*70}")
        print(f"MOMENTUM BREAKOUT V3 - AGGRESSIVE")
        print(f"{'='*70}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.0f}")
        print(f"\nV3 AGGRESSIVE SETTINGS:")
        print(f"  - Risk per trade: {self.risk_per_trade_pct:.0%} (was 1%)")
        print(f"  - Max positions: {self.max_positions}")
        print(f"  - Trailing EMA: {self.trailing_ema} (slower, let winners run)")
        print(f"  - Pyramiding: {'ON' if self.enable_pyramiding else 'OFF'} (add at +{self.pyramid_at_r}R)")
        print(f"  - Top N momentum only: {self.top_n_momentum}")
        print(f"  - Min ADR: {self.min_adr}%")
        print(f"{'='*70}\n")

        # Get broad universe first, then filter by momentum
        print("Loading broad universe...")
        query = """
        SELECT symbol
        FROM daily_price_data
        WHERE date BETWEEN $1 AND $2
          AND close_price > 0 AND volume > 0
        GROUP BY symbol
        HAVING COUNT(*) >= 90
           AND MIN(close_price::NUMERIC) >= $3
           AND AVG(volume::NUMERIC) >= 50000
        ORDER BY AVG(volume::NUMERIC) DESC
        LIMIT 200
        """
        rows = await self.conn.fetch(query, start_date - timedelta(days=150), end_date, self.min_price)
        all_symbols = [r['symbol'] for r in rows]
        print(f"Broad universe: {len(all_symbols)} stocks")

        # Load market data for all
        print("Loading market data...")
        for symbol in all_symbols:
            df = await self.get_market_data(symbol, start_date, end_date)
            if not df.empty and len(df) >= 60:
                df = self.calculate_indicators(df)
                self.market_data_cache[symbol] = df
        print(f"Loaded {len(self.market_data_cache)} stocks")

        # Main backtest loop
        current_date = start_date
        trading_days = 0
        pyramids_added = 0

        while current_date <= end_date:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            trading_days += 1

            # Refresh momentum rankings monthly
            if trading_days % 20 == 0:
                self.momentum_rankings = await self.calculate_momentum_rankings(current_date)

            # Manage existing positions (including pyramiding)
            await self._manage_positions(current_date)

            # Check for pyramid opportunities
            if self.enable_pyramiding:
                pyramids = await self._check_pyramid_opportunities(current_date)
                pyramids_added += pyramids

            # Scan for new signals
            if len(self.positions) < self.max_positions:
                signals = []
                for symbol, df in self.market_data_cache.items():
                    if any(p['symbol'] == symbol for p in self.positions):
                        continue

                    signal = self.detect_breakout(df, current_date, symbol)
                    if signal:
                        signal['symbol'] = symbol
                        signals.append(signal)

                # V3: Rank by momentum score (strongest first)
                signals.sort(key=lambda s: s['momentum_score'], reverse=True)

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

                    # V3: 2% risk sizing
                    account_value = self.cash + sum(p['shares'] * p['entry_price'] for p in self.positions)
                    risk_amount = account_value * self.risk_per_trade_pct
                    shares = int(risk_amount / risk_per_share)

                    position_cost = shares * entry_price
                    if shares <= 0 or position_cost > self.cash:
                        continue

                    self.positions.append({
                        'symbol': signal['symbol'],
                        'entry_date': entry_date,
                        'entry_price': entry_price,
                        'shares': shares,
                        'initial_stop': stop_price,
                        'current_stop': stop_price,
                        'risk_per_share': risk_per_share,
                        'adr_20': signal['adr_20'],
                        'pyramid_count': 0,
                        'highest_close': entry_price,
                    })

                    self.cash -= position_cost

                    print(f"  {entry_date}: BUY {signal['symbol']} @ {entry_price:.2f}, "
                          f"Stop: {stop_price:.2f} ({signal['stop_pct']:.1f}%), "
                          f"Shares: {shares}, Momentum Rank: #{signal['momentum_rank']+1}")

            # Record equity
            position_value = 0
            for p in self.positions:
                price = self._get_current_price(p['symbol'], current_date)
                if price:
                    position_value += p['shares'] * price
                else:
                    position_value += p['shares'] * p['entry_price']
            self.equity_curve.append((current_date, self.cash + position_value))

            if trading_days % 50 == 0:
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
        print(f"Pyramids added: {pyramids_added}")

        self._analyze_results()

    async def _check_pyramid_opportunities(self, current_date: date) -> int:
        """Add to winning positions at +3R."""
        pyramids_added = 0

        for pos in self.positions:
            if pos['pyramid_count'] >= 1:  # Only pyramid once
                continue

            symbol = pos['symbol']
            current_price = self._get_current_price(symbol, current_date)

            if current_price is None:
                continue

            # Calculate current R
            current_r = (current_price - pos['entry_price']) / pos['risk_per_share']

            # Pyramid at +3R
            if current_r >= self.pyramid_at_r:
                # Add 50% of original position
                additional_shares = int(pos['shares'] * self.pyramid_size_pct)
                cost = additional_shares * current_price

                if additional_shares > 0 and cost <= self.cash:
                    # Update position
                    old_shares = pos['shares']
                    pos['shares'] += additional_shares
                    pos['pyramid_count'] += 1

                    # Move stop to breakeven on original
                    pos['current_stop'] = pos['entry_price']

                    self.cash -= cost
                    pyramids_added += 1

                    print(f"  {current_date}: PYRAMID {symbol} +{additional_shares} shares @ {current_price:.2f} "
                          f"(now {pos['shares']} total, stop moved to breakeven)")

        return pyramids_added

    def _get_current_price(self, symbol: str, current_date: date) -> float:
        """Get current price for a symbol."""
        if symbol not in self.market_data_cache:
            return None
        df = self.market_data_cache[symbol]
        check_ts = pd.Timestamp(current_date)
        if check_ts in df.index:
            return float(df.loc[check_ts, 'close'])
        return None

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

            # Update highest close
            pos['highest_close'] = max(pos['highest_close'], current_close)

            # V3: Use 20 EMA trailing stop
            ema_value = float(row['ema_20']) if 'ema_20' in df.columns else pos['entry_price']

            # Stop loss check
            if current_low <= pos['current_stop']:
                positions_to_close.append((i, pos['current_stop'], 'stop_loss', current_date))
                continue

            # V3: Only exit on close below 20 EMA (not 9 EMA)
            if current_close < ema_value:
                positions_to_close.append((i, None, 'trailing_stop', current_date))
                continue

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
                initial_stop=pos['initial_stop'],
                is_pyramid=pos['pyramid_count'] > 0
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

        # Big winners
        print(f"\nBIG WINNERS (>5R):")
        big_winners = sorted([t for t in winners if t.r_multiple > 5],
                            key=lambda t: t.r_multiple, reverse=True)
        for t in big_winners[:10]:
            pyramid_flag = " [PYRAMIDED]" if t.is_pyramid else ""
            print(f"  {t.symbol}: {t.r_multiple:+.1f}R, ${t.gross_pnl:+,.0f}, "
                  f"held {t.days_held}d{pyramid_flag}")

        # Exit reasons
        print(f"\nEXIT REASONS:")
        reasons = defaultdict(list)
        for t in self.completed_trades:
            reasons[t.exit_reason].append(t)
        for reason, trades in sorted(reasons.items()):
            avg_r = np.mean([t.r_multiple for t in trades])
            total_pnl = sum(t.gross_pnl for t in trades)
            print(f"  {reason}: {len(trades)} trades, avg {avg_r:+.2f}R, ${total_pnl:+,.0f}")

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

        # Pyramided trades
        pyramided = [t for t in self.completed_trades if t.is_pyramid]
        if pyramided:
            print(f"\nPYRAMIDED TRADES:")
            print(f"  Count: {len(pyramided)}")
            print(f"  Avg R: {np.mean([t.r_multiple for t in pyramided]):+.2f}")
            print(f"  Total PnL: ${sum(t.gross_pnl for t in pyramided):+,.0f}")


async def main():
    parser = argparse.ArgumentParser(description='Momentum Breakout V3 - Aggressive')
    parser.add_argument('--start', type=str, default='2020-01-01')
    parser.add_argument('--end', type=str, default='2021-12-31')
    parser.add_argument('--no-pyramid', action='store_true', help='Disable pyramiding')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    backtester = MomentumBreakoutV3(
        enable_pyramiding=not args.no_pyramid,
    )

    try:
        await backtester.setup()
        await backtester.run_backtest(start_date, end_date)
    finally:
        await backtester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
