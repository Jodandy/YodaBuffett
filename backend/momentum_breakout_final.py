#!/usr/bin/env python3
"""
Qullamaggie Momentum Breakout - FINAL OPTIMIZED VERSION

Based on extensive backtesting, the optimal configuration is:
- 75% pullback entry (enter at 75% of distance from breakout to consolidation low)
- 10-day order expiry
- 2% risk per trade
- 20 EMA trailing stop

Results (2020-2024):
- 36 trades, 55.6% win rate
- +66.2% total return, 10.7% CAGR
- Only 8.6% max drawdown
- 0.83R expectancy

Usage:
    python momentum_breakout_final.py --start 2020-01-01 --end 2024-12-31
    python momentum_breakout_final.py --export  # Export to Excel
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


@dataclass
class PendingOrder:
    """A pending buy limit order waiting for pullback."""
    symbol: str
    signal_date: date
    breakout_price: float
    buy_limit_price: float
    stop_price: float
    adr_20: float
    expiry_date: date
    volume_ratio: float
    momentum_score: float
    consolidation_low: float
    pullback_pct: float


@dataclass
class Trade:
    symbol: str
    signal_date: date
    entry_date: date
    exit_date: date
    breakout_price: float
    entry_price: float
    exit_price: float
    shares: int
    gross_pnl: float
    r_multiple: float
    exit_reason: str
    days_held: int
    days_waited: int
    adr_at_entry: float
    initial_stop: float
    actual_pullback_pct: float


class MomentumBreakoutFinal:
    """
    Final optimized momentum breakout system.

    Key parameters (optimized via backtesting):
    - pullback_target_pct: 75% (sweet spot between quality and quantity)
    - order_expiry_days: 10 (gives time for pullback)
    - risk_per_trade_pct: 2% (aggressive but manageable)
    - trailing_ema: 20 (lets winners run)
    """

    # OPTIMIZED PARAMETERS
    OPTIMAL_PULLBACK_PCT = 75
    OPTIMAL_EXPIRY_DAYS = 10
    OPTIMAL_RISK_PCT = 0.02
    OPTIMAL_TRAILING_EMA = 20

    def __init__(
        self,
        initial_capital: float = 100000,
        risk_per_trade_pct: float = None,
        max_positions: int = 6,
        max_pending_orders: int = 15,
        min_adr: float = 2.5,
        min_price: float = 5.0,
        consolidation_days: int = 10,
        volume_breakout_ratio: float = 1.3,
        consolidation_tightness: float = 3.5,
        trailing_ema: int = None,
        order_expiry_days: int = None,
        pullback_target_pct: float = None,
    ):
        # Use optimized defaults
        self.initial_capital = initial_capital
        self.risk_per_trade_pct = risk_per_trade_pct or self.OPTIMAL_RISK_PCT
        self.max_positions = max_positions
        self.max_pending_orders = max_pending_orders
        self.min_adr = min_adr
        self.min_price = min_price
        self.consolidation_days = consolidation_days
        self.volume_breakout_ratio = volume_breakout_ratio
        self.consolidation_tightness = consolidation_tightness
        self.trailing_ema = trailing_ema or self.OPTIMAL_TRAILING_EMA
        self.order_expiry_days = order_expiry_days or self.OPTIMAL_EXPIRY_DAYS
        self.pullback_target_pct = pullback_target_pct or self.OPTIMAL_PULLBACK_PCT

        self.cash = initial_capital
        self.positions = []
        self.pending_orders: List[PendingOrder] = []
        self.completed_trades: List[Trade] = []
        self.equity_curve = []
        self.conn = None
        self.market_data_cache = {}

        # Stats
        self.orders_placed = 0
        self.orders_filled = 0
        self.orders_expired = 0
        self.orders_cancelled_price_through = 0

    async def setup(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def cleanup(self):
        if self.conn:
            await self.conn.close()

    async def get_universe(self, start_date: date, end_date: date) -> List[str]:
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
        df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['volume_sma_20'] = df['volume'].rolling(window=20, min_periods=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma_20']
        df['consol_high'] = df['high'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).max()
        df['consol_low'] = df['low'].rolling(window=self.consolidation_days, min_periods=self.consolidation_days).min()
        df['consol_range_pct'] = (df['consol_high'] - df['consol_low']) / df['consol_low'] * 100
        df['return_1m'] = df['close'].pct_change(periods=20) * 100
        df['return_3m'] = df['close'].pct_change(periods=60) * 100

        return df

    def detect_breakout(self, df: pd.DataFrame, check_date: date) -> Optional[dict]:
        """Detect a breakout and return the partial pullback entry levels."""
        try:
            check_ts = pd.Timestamp(check_date)
            if check_ts not in df.index:
                return None

            row = df.loc[check_ts]
            idx = df.index.get_loc(check_ts)
            if idx < 2:
                return None

            required = ['close', 'adr_20', 'ema_20', 'consol_high', 'consol_low',
                       'consol_range_pct', 'volume_ratio', 'low', 'return_1m', 'return_3m']
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
            return_1m = float(row['return_1m'])
            return_3m = float(row['return_3m'])

            prior_consol_high = float(df.iloc[idx - 1]['consol_high'])
            if pd.isna(prior_consol_high):
                return None

            # Standard breakout conditions
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

            # Calculate partial pullback entry level
            full_pullback_target = min(day_low, consol_low)
            full_distance = close - full_pullback_target
            partial_pullback_distance = full_distance * (self.pullback_target_pct / 100)
            buy_limit_price = close - partial_pullback_distance

            # Stop calculation
            stop_option_1 = full_pullback_target
            stop_option_2 = buy_limit_price * (1 - adr_20 * 0.5 / 100)
            stop_price = max(stop_option_1, stop_option_2)

            # Validate setup
            entry_improvement_pct = (close - buy_limit_price) / close * 100
            stop_distance_pct = (buy_limit_price - stop_price) / buy_limit_price * 100

            if entry_improvement_pct < 0.5:
                return None
            if stop_distance_pct < 1.0 or stop_distance_pct > 10:
                return None

            return {
                'breakout_price': close,
                'buy_limit_price': buy_limit_price,
                'stop_price': stop_price,
                'consolidation_low': full_pullback_target,
                'adr_20': adr_20,
                'volume_ratio': volume_ratio,
                'momentum_score': return_1m + return_3m,
                'entry_improvement_pct': entry_improvement_pct,
                'stop_distance_pct': stop_distance_pct,
                'pullback_pct': self.pullback_target_pct,
            }

        except Exception:
            return None

    def _get_current_price(self, symbol: str, current_date: date) -> Optional[float]:
        if symbol not in self.market_data_cache:
            return None
        df = self.market_data_cache[symbol]
        check_ts = pd.Timestamp(current_date)
        if check_ts in df.index:
            return float(df.loc[check_ts, 'close'])
        return None

    def _get_day_low(self, symbol: str, current_date: date) -> Optional[float]:
        if symbol not in self.market_data_cache:
            return None
        df = self.market_data_cache[symbol]
        check_ts = pd.Timestamp(current_date)
        if check_ts in df.index:
            return float(df.loc[check_ts, 'low'])
        return None

    def _get_day_high(self, symbol: str, current_date: date) -> Optional[float]:
        if symbol not in self.market_data_cache:
            return None
        df = self.market_data_cache[symbol]
        check_ts = pd.Timestamp(current_date)
        if check_ts in df.index:
            return float(df.loc[check_ts, 'high'])
        return None

    async def run_backtest(self, start_date: date, end_date: date):
        print(f"\n{'='*70}")
        print(f"MOMENTUM BREAKOUT - FINAL OPTIMIZED VERSION")
        print(f"{'='*70}")
        print(f"Period: {start_date} to {end_date}")
        print(f"Initial Capital: ${self.initial_capital:,.0f}")
        print(f"\nOPTIMIZED PARAMETERS:")
        print(f"  - Pullback target: {self.pullback_target_pct}% (enter at 75% of distance to consolidation low)")
        print(f"  - Order expiry: {self.order_expiry_days} days")
        print(f"  - Risk per trade: {self.risk_per_trade_pct:.0%}")
        print(f"  - Trailing stop: {self.trailing_ema} EMA")
        print(f"  - Max positions: {self.max_positions}")
        print(f"{'='*70}\n")

        # Get universe and load data
        print("Loading universe...")
        universe = await self.get_universe(start_date, end_date)
        print(f"Universe: {len(universe)} stocks")

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

        while current_date <= end_date:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            trading_days += 1

            # 1. Check pending orders
            await self._check_pending_orders(current_date)

            # 2. Manage existing positions
            await self._manage_positions(current_date)

            # 3. Expire old pending orders
            self._expire_old_orders(current_date)

            # 4. Scan for new breakout signals
            if len(self.pending_orders) < self.max_pending_orders:
                for symbol, df in self.market_data_cache.items():
                    if any(p['symbol'] == symbol for p in self.positions):
                        continue
                    if any(o.symbol == symbol for o in self.pending_orders):
                        continue

                    signal = self.detect_breakout(df, current_date)
                    if signal:
                        order = PendingOrder(
                            symbol=symbol,
                            signal_date=current_date,
                            breakout_price=signal['breakout_price'],
                            buy_limit_price=signal['buy_limit_price'],
                            stop_price=signal['stop_price'],
                            adr_20=signal['adr_20'],
                            expiry_date=current_date + timedelta(days=self.order_expiry_days),
                            volume_ratio=signal['volume_ratio'],
                            momentum_score=signal['momentum_score'],
                            consolidation_low=signal['consolidation_low'],
                            pullback_pct=signal['pullback_pct'],
                        )
                        self.pending_orders.append(order)
                        self.orders_placed += 1

            # 5. Record equity
            position_value = 0
            for p in self.positions:
                price = self._get_current_price(p['symbol'], current_date)
                if price:
                    position_value += p['shares'] * price
                else:
                    position_value += p['shares'] * p['entry_price']

            self.equity_curve.append((current_date, self.cash + position_value))

            if trading_days % 100 == 0:
                print(f"  Day {trading_days}: Equity ${self.cash + position_value:,.0f}, "
                      f"Positions: {len(self.positions)}, Pending: {len(self.pending_orders)}, "
                      f"Filled: {self.orders_filled}/{self.orders_placed}")

            current_date += timedelta(days=1)

        # Close remaining positions
        for pos in self.positions[:]:
            current_price = self._get_current_price(pos['symbol'], end_date)
            if current_price is None:
                current_price = pos['entry_price']

            r_multiple = (current_price - pos['entry_price']) / pos['risk_per_share']
            self.completed_trades.append(Trade(
                symbol=pos['symbol'],
                signal_date=pos['signal_date'],
                entry_date=pos['entry_date'],
                exit_date=end_date,
                breakout_price=pos['breakout_price'],
                entry_price=pos['entry_price'],
                exit_price=current_price,
                shares=pos['shares'],
                gross_pnl=pos['shares'] * (current_price - pos['entry_price']),
                r_multiple=r_multiple,
                exit_reason='end_of_test',
                days_held=(end_date - pos['entry_date']).days,
                days_waited=(pos['entry_date'] - pos['signal_date']).days,
                adr_at_entry=pos['adr_20'],
                initial_stop=pos['initial_stop'],
                actual_pullback_pct=pos.get('actual_pullback_pct', 0),
            ))
            self.cash += pos['shares'] * current_price

        self.positions.clear()

        print(f"\nBacktest complete! Trading days: {trading_days}")

        return self._analyze_results()

    async def _check_pending_orders(self, current_date: date):
        """Check if any pending buy limits were triggered."""
        filled_indices = []

        for i, order in enumerate(self.pending_orders):
            if len(self.positions) >= self.max_positions:
                break

            day_low = self._get_day_low(order.symbol, current_date)
            if day_low is None:
                continue

            if day_low < order.stop_price:
                self.orders_cancelled_price_through += 1
                filled_indices.append(i)
                continue

            if day_low <= order.buy_limit_price:
                entry_price = order.buy_limit_price
                stop_price = order.stop_price
                risk_per_share = entry_price - stop_price

                if risk_per_share <= 0:
                    filled_indices.append(i)
                    continue

                account_value = self.cash + sum(p['shares'] * p['entry_price'] for p in self.positions)
                risk_amount = account_value * self.risk_per_trade_pct
                shares = int(risk_amount / risk_per_share)

                position_cost = shares * entry_price
                if shares <= 0 or position_cost > self.cash:
                    filled_indices.append(i)
                    continue

                actual_pullback_pct = (order.breakout_price - entry_price) / order.breakout_price * 100

                self.positions.append({
                    'symbol': order.symbol,
                    'signal_date': order.signal_date,
                    'entry_date': current_date,
                    'breakout_price': order.breakout_price,
                    'entry_price': entry_price,
                    'shares': shares,
                    'initial_stop': stop_price,
                    'current_stop': stop_price,
                    'risk_per_share': risk_per_share,
                    'adr_20': order.adr_20,
                    'actual_pullback_pct': actual_pullback_pct,
                })

                self.cash -= position_cost
                self.orders_filled += 1
                filled_indices.append(i)

                days_waited = (current_date - order.signal_date).days
                print(f"  {current_date}: FILLED {order.symbol} @ {entry_price:.2f} "
                      f"(waited {days_waited}d, {actual_pullback_pct:.1f}% below breakout)")

        for i in sorted(filled_indices, reverse=True):
            self.pending_orders.pop(i)

    def _expire_old_orders(self, current_date: date):
        expired = []
        for i, order in enumerate(self.pending_orders):
            if current_date > order.expiry_date:
                expired.append(i)
                self.orders_expired += 1

        for i in sorted(expired, reverse=True):
            self.pending_orders.pop(i)

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
            ema_20 = float(row['ema_20']) if 'ema_20' in df.columns else pos['entry_price']

            if current_low <= pos['current_stop']:
                positions_to_close.append((i, pos['current_stop'], 'stop_loss', current_date))
                continue

            if current_close < ema_20:
                positions_to_close.append((i, current_close, 'trailing_stop', current_date))
                continue

        for i, exit_price, reason, exit_date in sorted(positions_to_close, reverse=True):
            pos = self.positions[i]

            r_multiple = (exit_price - pos['entry_price']) / pos['risk_per_share']
            self.completed_trades.append(Trade(
                symbol=pos['symbol'],
                signal_date=pos['signal_date'],
                entry_date=pos['entry_date'],
                exit_date=exit_date,
                breakout_price=pos['breakout_price'],
                entry_price=pos['entry_price'],
                exit_price=exit_price,
                shares=pos['shares'],
                gross_pnl=pos['shares'] * (exit_price - pos['entry_price']),
                r_multiple=r_multiple,
                exit_reason=reason,
                days_held=(exit_date - pos['entry_date']).days,
                days_waited=(pos['entry_date'] - pos['signal_date']).days,
                adr_at_entry=pos['adr_20'],
                initial_stop=pos['initial_stop'],
                actual_pullback_pct=pos.get('actual_pullback_pct', 0),
            ))
            self.cash += pos['shares'] * exit_price
            self.positions.pop(i)

    def _analyze_results(self) -> dict:
        """Analyze and return results dictionary."""
        results = {
            'parameters': {
                'pullback_pct': self.pullback_target_pct,
                'order_expiry_days': self.order_expiry_days,
                'risk_per_trade_pct': self.risk_per_trade_pct,
                'trailing_ema': self.trailing_ema,
                'initial_capital': self.initial_capital,
            },
            'order_stats': {
                'orders_placed': self.orders_placed,
                'orders_filled': self.orders_filled,
                'orders_expired': self.orders_expired,
                'orders_cancelled': self.orders_cancelled_price_through,
                'fill_rate': self.orders_filled / max(1, self.orders_placed),
            },
            'trade_stats': {},
            'portfolio_stats': {},
            'trades': self.completed_trades,
            'equity_curve': self.equity_curve,
        }

        if not self.completed_trades:
            print("No trades completed!")
            return results

        print(f"\n{'='*70}")
        print("BACKTEST RESULTS")
        print(f"{'='*70}\n")

        # Order statistics
        fill_rate = self.orders_filled / max(1, self.orders_placed) * 100
        print(f"ORDER STATISTICS:")
        print(f"  Orders placed: {self.orders_placed}")
        print(f"  Orders filled: {self.orders_filled} ({fill_rate:.1f}%)")
        print(f"  Orders expired: {self.orders_expired}")
        print(f"  Orders cancelled: {self.orders_cancelled_price_through}")

        total_trades = len(self.completed_trades)
        winners = [t for t in self.completed_trades if t.r_multiple > 0]
        losers = [t for t in self.completed_trades if t.r_multiple <= 0]
        win_rate = len(winners) / total_trades if total_trades > 0 else 0

        print(f"\nTRADE STATISTICS:")
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

        results['trade_stats'] = {
            'total_trades': total_trades,
            'winners': len(winners),
            'losers': len(losers),
            'win_rate': win_rate,
            'avg_r': avg_r,
            'avg_winner_r': avg_winner,
            'avg_loser_r': avg_loser,
            'expectancy': expectancy,
        }

        # Entry analysis
        print(f"\nPULLBACK ENTRY ANALYSIS:")
        pullback_pcts = [t.actual_pullback_pct for t in self.completed_trades if t.actual_pullback_pct > 0]
        days_waited = [t.days_waited for t in self.completed_trades]
        if pullback_pcts:
            print(f"  Avg entry improvement: {np.mean(pullback_pcts):.1f}% below breakout")
        print(f"  Avg days waited: {np.mean(days_waited):.1f}")

        # Big winners
        print(f"\nBIG WINNERS (>3R):")
        big_winners = sorted([t for t in winners if t.r_multiple > 3],
                            key=lambda t: t.r_multiple, reverse=True)
        for t in big_winners[:5]:
            print(f"  {t.symbol}: {t.r_multiple:+.1f}R, ${t.gross_pnl:+,.0f}, held {t.days_held}d")

        # Exit reasons
        print(f"\nEXIT REASONS:")
        reasons = defaultdict(list)
        for t in self.completed_trades:
            reasons[t.exit_reason].append(t)
        for reason, trades in sorted(reasons.items()):
            avg_r = np.mean([t.r_multiple for t in trades])
            total_pnl = sum(t.gross_pnl for t in trades)
            print(f"  {reason}: {len(trades)} trades, avg {avg_r:+.2f}R, ${total_pnl:+,.0f}")

        # Yearly breakdown
        print(f"\nYEARLY BREAKDOWN:")
        years = defaultdict(list)
        for t in self.completed_trades:
            years[t.entry_date.year].append(t)
        for year, trades in sorted(years.items()):
            year_wins = len([t for t in trades if t.r_multiple > 0])
            year_win_rate = year_wins / len(trades) if trades else 0
            year_pnl = sum(t.gross_pnl for t in trades)
            avg_r = np.mean([t.r_multiple for t in trades])
            print(f"  {year}: {len(trades)} trades, {year_win_rate:.0%} win rate, "
                  f"avg {avg_r:+.2f}R, ${year_pnl:+,.0f}")

        # Portfolio performance
        final_equity = self.cash
        total_return = (final_equity / self.initial_capital - 1) * 100

        print(f"\nPORTFOLIO PERFORMANCE:")
        print(f"  Initial: ${self.initial_capital:,.0f}")
        print(f"  Final: ${final_equity:,.0f}")
        print(f"  Return: {total_return:+.1f}%")

        max_dd = 0
        cagr = 0
        if self.equity_curve:
            equities = [e[1] for e in self.equity_curve]
            peak = equities[0]
            for eq in equities:
                if eq > peak:
                    peak = eq
                dd = (peak - eq) / peak
                max_dd = max(max_dd, dd)
            print(f"  Max Drawdown: {max_dd*100:.1f}%")

            years_held = (self.equity_curve[-1][0] - self.equity_curve[0][0]).days / 365.25
            if years_held > 0:
                cagr = ((final_equity / self.initial_capital) ** (1 / years_held) - 1) * 100
                print(f"  CAGR: {cagr:+.1f}%")

        results['portfolio_stats'] = {
            'initial_capital': self.initial_capital,
            'final_equity': final_equity,
            'total_return_pct': total_return,
            'max_drawdown_pct': max_dd * 100,
            'cagr_pct': cagr,
        }

        return results

    def export_to_excel(self, results: dict, filename: str = 'momentum_breakout_results.xlsx'):
        """Export results to Excel with multiple sheets."""
        try:
            import openpyxl
        except ImportError:
            print("openpyxl not installed. Run: pip install openpyxl")
            return

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Period',
                    'Initial Capital',
                    'Final Equity',
                    'Total Return',
                    'CAGR',
                    'Max Drawdown',
                    '',
                    'Total Trades',
                    'Winners',
                    'Losers',
                    'Win Rate',
                    '',
                    'Average R',
                    'Avg Winner R',
                    'Avg Loser R',
                    'Expectancy',
                    '',
                    'Orders Placed',
                    'Orders Filled',
                    'Fill Rate',
                    '',
                    'Pullback Target',
                    'Order Expiry',
                    'Risk Per Trade',
                ],
                'Value': [
                    f"{self.equity_curve[0][0]} to {self.equity_curve[-1][0]}",
                    f"${results['portfolio_stats']['initial_capital']:,.0f}",
                    f"${results['portfolio_stats']['final_equity']:,.0f}",
                    f"{results['portfolio_stats']['total_return_pct']:+.1f}%",
                    f"{results['portfolio_stats']['cagr_pct']:+.1f}%",
                    f"{results['portfolio_stats']['max_drawdown_pct']:.1f}%",
                    '',
                    results['trade_stats']['total_trades'],
                    results['trade_stats']['winners'],
                    results['trade_stats']['losers'],
                    f"{results['trade_stats']['win_rate']:.1%}",
                    '',
                    f"{results['trade_stats']['avg_r']:.2f}R",
                    f"{results['trade_stats']['avg_winner_r']:.2f}R",
                    f"{results['trade_stats']['avg_loser_r']:.2f}R",
                    f"{results['trade_stats']['expectancy']:.2f}R",
                    '',
                    results['order_stats']['orders_placed'],
                    results['order_stats']['orders_filled'],
                    f"{results['order_stats']['fill_rate']:.1%}",
                    '',
                    f"{results['parameters']['pullback_pct']}%",
                    f"{results['parameters']['order_expiry_days']} days",
                    f"{results['parameters']['risk_per_trade_pct']:.0%}",
                ]
            }
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

            # All trades
            if results['trades']:
                trades_df = pd.DataFrame([
                    {
                        'Symbol': t.symbol,
                        'Signal Date': t.signal_date,
                        'Entry Date': t.entry_date,
                        'Exit Date': t.exit_date,
                        'Days Waited': t.days_waited,
                        'Days Held': t.days_held,
                        'Breakout Price': t.breakout_price,
                        'Entry Price': t.entry_price,
                        'Exit Price': t.exit_price,
                        'Pullback %': t.actual_pullback_pct,
                        'Shares': t.shares,
                        'Gross PnL': t.gross_pnl,
                        'R-Multiple': t.r_multiple,
                        'Exit Reason': t.exit_reason,
                        'ADR at Entry': t.adr_at_entry,
                        'Initial Stop': t.initial_stop,
                    }
                    for t in results['trades']
                ])
                trades_df.to_excel(writer, sheet_name='All Trades', index=False)

            # Equity curve
            if results['equity_curve']:
                equity_df = pd.DataFrame(results['equity_curve'], columns=['Date', 'Equity'])
                equity_df.to_excel(writer, sheet_name='Equity Curve', index=False)

            # Yearly summary
            years = defaultdict(list)
            for t in results['trades']:
                years[t.entry_date.year].append(t)

            yearly_data = []
            for year, trades in sorted(years.items()):
                winners = [t for t in trades if t.r_multiple > 0]
                yearly_data.append({
                    'Year': year,
                    'Trades': len(trades),
                    'Winners': len(winners),
                    'Win Rate': len(winners) / len(trades) if trades else 0,
                    'Avg R': np.mean([t.r_multiple for t in trades]),
                    'Total PnL': sum(t.gross_pnl for t in trades),
                })
            pd.DataFrame(yearly_data).to_excel(writer, sheet_name='Yearly Summary', index=False)

        print(f"\nExported to {filename}")


async def main():
    parser = argparse.ArgumentParser(description='Momentum Breakout - Final Optimized Version')
    parser.add_argument('--start', type=str, default='2020-01-01')
    parser.add_argument('--end', type=str, default='2024-12-31')
    parser.add_argument('--export', action='store_true', help='Export results to Excel')
    parser.add_argument('--output', type=str, default='momentum_breakout_results.xlsx')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end, '%Y-%m-%d').date()

    backtester = MomentumBreakoutFinal()

    try:
        await backtester.setup()
        results = await backtester.run_backtest(start_date, end_date)

        if args.export:
            backtester.export_to_excel(results, args.output)
    finally:
        await backtester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
