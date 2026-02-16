#!/usr/bin/env python3
"""
Cash Cow + Technical Overlay Backtest

Tests adding simple technical rules to the Cash Cow (high OCF/NI) strategy:
1. Baseline: Buy and hold
2. 200 SMA entry filter: Only buy if price > 200 SMA
3. 20% trailing stop: Exit if price drops 20% from peak
4. 200 SMA exit: Exit when price closes below 200 SMA
5. Combination: Entry filter + exit rule

Goal: Keep winners, cut the big losers before they become -40% to -60%.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from decimal import Decimal
from collections import defaultdict
from typing import Optional, Tuple, List
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def to_float(val):
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    return float(val)


async def get_cash_cows_for_date(conn, score_date: date, top_pct: float = 0.40) -> List[str]:
    """
    Get top cash cow companies (by OCF/NI) as of score_date.
    Returns tickers for top X% of companies by OCF/NI.
    """
    cutoff = score_date - timedelta(days=3 * 365)

    companies = await conn.fetch("""
        SELECT DISTINCT cm.primary_ticker, cm.company_name
        FROM company_master cm
        WHERE cm.primary_ticker IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM financial_statements fs
            WHERE (fs.symbol = cm.primary_ticker OR fs.symbol = REPLACE(cm.primary_ticker, '-', ' '))
            AND fs.statement_type = 'annual'
        )
    """)

    ocf_ni_scores = []

    for comp in companies:
        ticker = comp['primary_ticker']
        ticker_space = ticker.replace('-', ' ')

        # Get latest income statement
        income = await conn.fetchrow("""
            SELECT net_income, period_date
            FROM financial_statements
            WHERE (symbol = $1 OR symbol = $2)
              AND statement_type = 'annual'
              AND (
                  (publish_date IS NOT NULL AND publish_date <= $3)
                  OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $3)
              )
            ORDER BY period_date DESC
            LIMIT 1
        """, ticker, ticker_space, score_date)

        if not income or not income['net_income']:
            continue

        ni = to_float(income['net_income'])
        if ni <= 0:
            continue

        # Get matching cash flow
        period_date = income['period_date']
        cashflow = await conn.fetchrow("""
            SELECT operating_cash_flow
            FROM cash_flow_data
            WHERE (symbol = $1 OR symbol = $2)
              AND (
                  (publish_date IS NOT NULL AND publish_date <= $3)
                  OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $3)
              )
              AND period_date >= ($4::date - INTERVAL '90 days')
              AND period_date <= ($4::date + INTERVAL '90 days')
            ORDER BY ABS(period_date - $4::date)
            LIMIT 1
        """, ticker, ticker_space, score_date, period_date)

        if not cashflow or not cashflow['operating_cash_flow']:
            continue

        ocf = to_float(cashflow['operating_cash_flow'])
        ocf_ni = ocf / ni

        ocf_ni_scores.append((ticker, ocf_ni))

    # Sort by OCF/NI descending, take top X%
    ocf_ni_scores.sort(key=lambda x: x[1], reverse=True)
    n_top = int(len(ocf_ni_scores) * top_pct)

    return [t[0] for t in ocf_ni_scores[:n_top]]


async def get_price_series(conn, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
    """Get daily price data for a ticker."""

    rows = await conn.fetch("""
        SELECT date, open_price, high_price, low_price, close_price, volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date >= $2
        AND date <= $3
        ORDER BY date
    """, ticker, start_date, end_date)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)

    for col in ['open_price', 'high_price', 'low_price', 'close_price']:
        df[col] = df[col].apply(to_float)

    return df


def calculate_sma(prices: pd.Series, period: int) -> pd.Series:
    """Calculate simple moving average."""
    return prices.rolling(window=period, min_periods=period).mean()


def simulate_strategy(
    df: pd.DataFrame,
    entry_date: date,
    holding_days: int,
    strategy: str,
    sma_period: int = 200,
    trailing_stop_pct: float = 0.20,
) -> Tuple[Optional[float], str, Optional[date]]:
    """
    Simulate a strategy and return (return%, exit_reason, exit_date).

    Strategies:
    - 'baseline': Buy and hold
    - 'sma_entry': Only enter if price > SMA
    - 'trailing_stop': Exit if price drops X% from peak
    - 'sma_exit': Exit when price closes below SMA
    - 'combo': SMA entry + SMA exit
    - 'combo_trailing': SMA entry + trailing stop
    """

    if df.empty or len(df) < sma_period:
        return None, 'insufficient_data', None

    # Calculate SMA
    df = df.copy()
    df['sma'] = calculate_sma(df['close_price'], sma_period)

    # Find entry point
    entry_idx = df.index.searchsorted(pd.Timestamp(entry_date))
    if entry_idx >= len(df):
        return None, 'no_entry_data', None

    # Get data from entry onwards
    df_trade = df.iloc[entry_idx:].copy()

    if len(df_trade) < 5:
        return None, 'insufficient_trade_data', None

    entry_price = to_float(df_trade.iloc[0]['close_price'])
    entry_sma = df_trade.iloc[0]['sma']

    if entry_price is None or entry_price <= 0:
        return None, 'invalid_entry_price', None

    # Check entry filter for SMA strategies
    if strategy in ['sma_entry', 'combo', 'combo_trailing']:
        if pd.isna(entry_sma) or entry_price < entry_sma:
            return None, 'sma_entry_filter', None

    # Simulate holding period
    max_price = entry_price
    exit_price = None
    exit_reason = 'held_to_end'
    exit_date = None

    target_exit_date = pd.Timestamp(entry_date) + pd.Timedelta(days=holding_days)

    for i, (idx, row) in enumerate(df_trade.iterrows()):
        current_price = to_float(row['close_price'])
        current_sma = row['sma']

        if current_price is None:
            continue

        # Update max price for trailing stop
        max_price = max(max_price, current_price)

        # Check trailing stop
        if strategy in ['trailing_stop', 'combo_trailing']:
            drawdown = (max_price - current_price) / max_price
            if drawdown >= trailing_stop_pct:
                exit_price = current_price
                exit_reason = f'trailing_stop_{int(trailing_stop_pct*100)}pct'
                exit_date = idx.date() if hasattr(idx, 'date') else idx
                break

        # Check SMA exit
        if strategy in ['sma_exit', 'combo']:
            if not pd.isna(current_sma) and current_price < current_sma:
                exit_price = current_price
                exit_reason = 'sma_exit'
                exit_date = idx.date() if hasattr(idx, 'date') else idx
                break

        # Check if we've reached holding period
        if idx >= target_exit_date:
            exit_price = current_price
            exit_reason = 'held_to_end'
            exit_date = idx.date() if hasattr(idx, 'date') else idx
            break

    # If no exit yet, use last available price
    if exit_price is None:
        exit_price = to_float(df_trade.iloc[-1]['close_price'])
        exit_date = df_trade.index[-1].date() if hasattr(df_trade.index[-1], 'date') else df_trade.index[-1]

    if exit_price is None or exit_price <= 0:
        return None, 'invalid_exit_price', None

    return_pct = ((exit_price / entry_price) - 1) * 100

    return return_pct, exit_reason, exit_date


async def run_backtest(
    start_year: int = 2022,
    end_year: int = 2024,
    holding_days: int = 252,
    top_pct: float = 0.40,
):
    """Run the backtest comparing all strategies."""

    conn = await asyncpg.connect(DATABASE_URL)

    # Generate test dates (quarterly)
    test_dates = []
    for year in range(start_year, end_year + 1):
        for month in [3, 6, 9, 12]:
            d = date(year, month, 28)
            if d <= date.today() - timedelta(days=holding_days + 30):
                test_dates.append(d)

    strategies = ['baseline', 'sma_entry', 'trailing_stop', 'sma_exit', 'combo', 'combo_trailing']

    print(f"Testing {len(test_dates)} periods from {test_dates[0]} to {test_dates[-1]}")
    print(f"Holding period: {holding_days} days (~{holding_days//21} months)")
    print(f"Universe: Top {int(top_pct*100)}% by OCF/NI (Cash Cows)")
    print(f"Strategies: {', '.join(strategies)}")
    print()

    # Results storage
    all_results = []
    strategy_returns = {s: [] for s in strategies}
    strategy_exit_reasons = {s: defaultdict(int) for s in strategies}

    for test_date in test_dates:
        print(f"Processing {test_date}...", end=" ")

        # Get Cash Cow tickers
        cash_cows = await get_cash_cows_for_date(conn, test_date, top_pct)

        if len(cash_cows) < 20:
            print(f"skipped (only {len(cash_cows)} cash cows)")
            continue

        # Need price data from before entry (for SMA calculation)
        price_start = test_date - timedelta(days=250)  # Extra buffer for 200 SMA
        price_end = test_date + timedelta(days=holding_days + 30)

        period_count = 0

        for ticker in cash_cows:
            # Get price data
            df = await get_price_series(conn, ticker, price_start, price_end)

            if df.empty or len(df) < 220:  # Need enough for SMA
                continue

            # Run each strategy
            for strategy in strategies:
                ret, reason, exit_dt = simulate_strategy(
                    df, test_date, holding_days, strategy,
                    sma_period=200, trailing_stop_pct=0.20
                )

                if ret is not None and abs(ret) < 500:  # Filter extreme outliers
                    strategy_returns[strategy].append(ret)
                    strategy_exit_reasons[strategy][reason] += 1

                    if strategy == 'baseline':
                        period_count += 1

                    all_results.append({
                        'date': test_date,
                        'ticker': ticker,
                        'strategy': strategy,
                        'return': ret,
                        'exit_reason': reason,
                        'exit_date': exit_dt,
                    })

        print(f"{period_count} stocks tested")

    await conn.close()

    # Analyze results
    print("\n" + "="*80)
    print("RESULTS: Cash Cow Strategy + Technical Overlays")
    print("="*80)

    print(f"\n{'Strategy':<20} {'N':<8} {'Median':<10} {'Mean':<10} {'Win%':<8} {'Worst':<10} {'Best':<10}")
    print("-" * 76)

    baseline_median = np.median(strategy_returns['baseline']) if strategy_returns['baseline'] else 0

    for strategy in strategies:
        rets = strategy_returns[strategy]
        if rets:
            median = np.median(rets)
            mean = np.mean(rets)
            win_rate = sum(1 for r in rets if r > 0) / len(rets) * 100
            worst = np.percentile(rets, 5)  # 5th percentile
            best = np.percentile(rets, 95)  # 95th percentile

            diff = median - baseline_median
            diff_str = f"({diff:+.1f}pp)" if strategy != 'baseline' else ""

            print(f"{strategy:<20} {len(rets):<8} {median:>+6.1f}% {diff_str:<4} {mean:>+6.1f}%    {win_rate:>5.1f}%   {worst:>+6.1f}%    {best:>+6.1f}%")

    # Exit reason analysis
    print("\n" + "-"*80)
    print("EXIT REASONS:")
    print("-"*80)

    for strategy in strategies:
        if strategy == 'baseline':
            continue
        reasons = strategy_exit_reasons[strategy]
        total = sum(reasons.values())
        if total > 0:
            print(f"\n{strategy}:")
            for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
                pct = count / total * 100
                print(f"  {reason}: {count} ({pct:.1f}%)")

    # Analyze tail risk improvement
    print("\n" + "-"*80)
    print("TAIL RISK ANALYSIS (worst outcomes):")
    print("-"*80)

    for strategy in strategies:
        rets = strategy_returns[strategy]
        if rets:
            pct_below_minus20 = sum(1 for r in rets if r < -20) / len(rets) * 100
            pct_below_minus30 = sum(1 for r in rets if r < -30) / len(rets) * 100
            pct_below_minus50 = sum(1 for r in rets if r < -50) / len(rets) * 100

            print(f"{strategy:<20} <-20%: {pct_below_minus20:>5.1f}%   <-30%: {pct_below_minus30:>5.1f}%   <-50%: {pct_below_minus50:>5.1f}%")

    # Risk-adjusted returns
    print("\n" + "-"*80)
    print("RISK-ADJUSTED METRICS:")
    print("-"*80)

    for strategy in strategies:
        rets = strategy_returns[strategy]
        if rets and len(rets) > 10:
            mean = np.mean(rets)
            std = np.std(rets)
            sharpe_approx = mean / std if std > 0 else 0  # Simplified, not annualized
            sortino_denom = np.std([r for r in rets if r < 0]) if any(r < 0 for r in rets) else 1
            sortino_approx = mean / sortino_denom if sortino_denom > 0 else 0

            print(f"{strategy:<20} Sharpe-like: {sharpe_approx:>6.3f}   Sortino-like: {sortino_approx:>6.3f}")

    # Export to Excel
    df = pd.DataFrame(all_results)

    if not df.empty:
        output_file = 'cashcow_technical_overlay_backtest.xlsx'

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='All Trades', index=False)

            # Summary by strategy
            summary = []
            for strategy in strategies:
                rets = strategy_returns[strategy]
                if rets:
                    summary.append({
                        'Strategy': strategy,
                        'N': len(rets),
                        'Median': np.median(rets),
                        'Mean': np.mean(rets),
                        'Std': np.std(rets),
                        'Win Rate': sum(1 for r in rets if r > 0) / len(rets) * 100,
                        'P5 (Worst)': np.percentile(rets, 5),
                        'P95 (Best)': np.percentile(rets, 95),
                        'Below -20%': sum(1 for r in rets if r < -20) / len(rets) * 100,
                        'Below -30%': sum(1 for r in rets if r < -30) / len(rets) * 100,
                    })

            pd.DataFrame(summary).to_excel(writer, sheet_name='Strategy Summary', index=False)

        print(f"\n📊 Results exported to {output_file}")

    return all_results, strategy_returns


async def main():
    parser = argparse.ArgumentParser(description='Backtest Cash Cow + Technical Overlay')
    parser.add_argument('--start-year', type=int, default=2022)
    parser.add_argument('--end-year', type=int, default=2024)
    parser.add_argument('--holding-days', type=int, default=252)
    parser.add_argument('--top-pct', type=float, default=0.40, help='Top X% by OCF/NI')

    args = parser.parse_args()

    await run_backtest(
        start_year=args.start_year,
        end_year=args.end_year,
        holding_days=args.holding_days,
        top_pct=args.top_pct,
    )


if __name__ == '__main__':
    asyncio.run(main())
