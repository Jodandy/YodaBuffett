#!/usr/bin/env python3
"""
Cash Cow-ness Backtest

Tests OCF/NI (cash conversion) as a continuous ranking:
- Rank ALL companies by OCF/NI ratio
- Split into quintiles (top 20%, next 20%, etc.)
- Measure forward returns for each quintile
- Test across multiple years

This reveals if "cash cow-yness" predicts returns systematically.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from decimal import Decimal
from collections import defaultdict
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def to_float(val):
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    return float(val)


async def get_ocf_ni_for_date(conn, score_date: date) -> dict:
    """
    Calculate OCF/NI for all companies as of score_date.
    Point-in-time safe: only uses data published before score_date.

    Returns: {ticker: {'ocf_ni': float, 'company_name': str}}
    """
    cutoff = score_date - timedelta(days=3 * 365)  # Look back 3 years max

    # Get all companies with financial data
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

    results = {}

    for comp in companies:
        ticker = comp['primary_ticker']
        ticker_space = ticker.replace('-', ' ')

        # Get latest income statement (point-in-time safe)
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
        if ni <= 0:  # Skip loss-making companies
            continue

        # Get matching cash flow (same or close period)
        period_date = income['period_date']
        cashflow = await conn.fetchrow("""
            SELECT operating_cash_flow, period_date
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

        results[ticker] = {
            'ocf_ni': ocf_ni,
            'company_name': comp['company_name'],
        }

    return results


async def get_forward_return(conn, ticker: str, start_date: date, days: int) -> float:
    """Get forward return for a ticker. Returns None if insufficient data."""

    prices = await conn.fetch("""
        SELECT date, close_price
        FROM daily_price_data
        WHERE symbol = $1
        AND date >= $2
        AND date <= $3
        ORDER BY date
    """, ticker, start_date, start_date + timedelta(days=days + 30))

    if len(prices) < 10:
        return None

    entry = to_float(prices[0]['close_price'])
    if entry <= 0:
        return None

    # Find exit price at target date
    target = start_date + timedelta(days=days)
    exit_price = None
    for p in prices:
        if p['date'] >= target:
            exit_price = to_float(p['close_price'])
            break

    if not exit_price:
        exit_price = to_float(prices[-1]['close_price'])

    return ((exit_price / entry) - 1) * 100


async def run_backtest(
    start_year: int = 2021,
    end_year: int = 2024,
    holding_days: int = 252,  # ~12 months
    frequency: str = 'quarterly'
):
    """Run the backtest across multiple periods."""

    conn = await asyncpg.connect(DATABASE_URL)

    # Generate test dates
    test_dates = []
    for year in range(start_year, end_year + 1):
        if frequency == 'quarterly':
            for month in [3, 6, 9, 12]:
                d = date(year, month, 28)
                if d <= date.today() - timedelta(days=holding_days):
                    test_dates.append(d)
        else:  # annual
            d = date(year, 6, 30)
            if d <= date.today() - timedelta(days=holding_days):
                test_dates.append(d)

    print(f"Testing {len(test_dates)} periods from {test_dates[0]} to {test_dates[-1]}")
    print(f"Forward return period: {holding_days} days (~{holding_days//21} months)")
    print()

    all_results = []
    quintile_returns = defaultdict(list)  # {quintile: [returns]}

    for test_date in test_dates:
        print(f"Processing {test_date}...", end=" ")

        # Get OCF/NI for all companies
        ocf_ni_data = await get_ocf_ni_for_date(conn, test_date)

        if len(ocf_ni_data) < 50:
            print(f"skipped (only {len(ocf_ni_data)} companies)")
            continue

        # Rank by OCF/NI and assign quintiles
        sorted_tickers = sorted(ocf_ni_data.keys(), key=lambda t: ocf_ni_data[t]['ocf_ni'], reverse=True)
        n = len(sorted_tickers)
        quintile_size = n // 5

        period_results = []

        for i, ticker in enumerate(sorted_tickers):
            quintile = min(i // quintile_size + 1, 5)  # 1 = top 20% (highest OCF/NI)

            fwd_return = await get_forward_return(conn, ticker, test_date, holding_days)

            if fwd_return is not None and abs(fwd_return) < 500:  # Filter outliers
                period_results.append({
                    'date': test_date,
                    'ticker': ticker,
                    'company': ocf_ni_data[ticker]['company_name'],
                    'ocf_ni': ocf_ni_data[ticker]['ocf_ni'],
                    'quintile': quintile,
                    'forward_return': fwd_return,
                })
                quintile_returns[quintile].append(fwd_return)

        all_results.extend(period_results)
        print(f"{len(period_results)} companies with returns")

    await conn.close()

    # Analyze results
    print("\n" + "="*70)
    print("RESULTS: OCF/NI Quintile Performance")
    print("="*70)
    print(f"\nQuintile 1 = Highest OCF/NI (best cash conversion)")
    print(f"Quintile 5 = Lowest OCF/NI (worst cash conversion)\n")

    print(f"{'Quintile':<12} {'N':<8} {'Median':<12} {'Mean':<12} {'Win Rate':<12}")
    print("-" * 56)

    for q in range(1, 6):
        rets = quintile_returns[q]
        if rets:
            median = np.median(rets)
            mean = np.mean(rets)
            win_rate = sum(1 for r in rets if r > 0) / len(rets) * 100
            print(f"Q{q:<11} {len(rets):<8} {median:>+8.1f}%    {mean:>+8.1f}%    {win_rate:>6.1f}%")

    # Calculate spread
    if quintile_returns[1] and quintile_returns[5]:
        q1_median = np.median(quintile_returns[1])
        q5_median = np.median(quintile_returns[5])
        spread = q1_median - q5_median
        print("-" * 56)
        print(f"{'Q1-Q5 Spread':<12} {'':<8} {spread:>+8.1f}pp")

    # Statistical significance
    from scipy import stats
    if quintile_returns[1] and quintile_returns[5]:
        t_stat, p_value = stats.ttest_ind(quintile_returns[1], quintile_returns[5])
        print(f"\nt-test Q1 vs Q5: t={t_stat:.2f}, p={p_value:.4f}")
        if p_value < 0.05:
            print("✅ Statistically significant difference")
        else:
            print("❌ Not statistically significant")

    # Export to Excel
    df = pd.DataFrame(all_results)

    # Summary by quintile and year
    if not df.empty:
        df['year'] = df['date'].apply(lambda d: d.year)

        summary = df.groupby(['year', 'quintile']).agg({
            'forward_return': ['median', 'mean', 'count']
        }).round(2)

        output_file = 'cash_cowness_backtest.xlsx'
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='All Data', index=False)
            summary.to_excel(writer, sheet_name='By Year & Quintile')

            # Quintile summary
            q_summary = pd.DataFrame({
                'Quintile': range(1, 6),
                'N': [len(quintile_returns[q]) for q in range(1, 6)],
                'Median': [np.median(quintile_returns[q]) if quintile_returns[q] else None for q in range(1, 6)],
                'Mean': [np.mean(quintile_returns[q]) if quintile_returns[q] else None for q in range(1, 6)],
                'Std': [np.std(quintile_returns[q]) if quintile_returns[q] else None for q in range(1, 6)],
                'Win Rate': [sum(1 for r in quintile_returns[q] if r > 0) / len(quintile_returns[q]) * 100
                            if quintile_returns[q] else None for q in range(1, 6)],
            })
            q_summary.to_excel(writer, sheet_name='Quintile Summary', index=False)

        print(f"\n📊 Results exported to {output_file}")

    return all_results, quintile_returns


async def main():
    parser = argparse.ArgumentParser(description='Backtest OCF/NI (cash cow-ness) as predictor')
    parser.add_argument('--start-year', type=int, default=2021, help='Start year')
    parser.add_argument('--end-year', type=int, default=2024, help='End year')
    parser.add_argument('--holding-days', type=int, default=252, help='Forward return period in days')
    parser.add_argument('--frequency', choices=['quarterly', 'annual'], default='quarterly')

    args = parser.parse_args()

    await run_backtest(
        start_year=args.start_year,
        end_year=args.end_year,
        holding_days=args.holding_days,
        frequency=args.frequency
    )


if __name__ == '__main__':
    asyncio.run(main())
