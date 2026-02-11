#!/usr/bin/env python3
"""
Dimension Skewness Backtest

Tests each dimension in isolation:
- Rank all companies by dimension score
- Split into quintiles (top 20%, next 20%, etc.)
- Measure forward returns for each quintile
- Export to Excel for analysis

This reveals which dimensions have predictive power on their own.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# All dimensions to test
DIMENSIONS = [
    'profitability',
    'returns',
    'growth',
    'financial_health',
    'value',
    'risk',
    'momentum',
    'quality',
    'earnings_quality',
    'capital_allocation',
    'valuation_percentile',
    'beneish_mscore',
    'working_capital',
]

async def get_dimension_scores_for_date(conn, score_date: date) -> Dict[str, Dict[str, float]]:
    """Get all dimension scores for all companies on a specific date."""

    rows = await conn.fetch("""
        SELECT
            dds.company_id::text,
            dds.dimension_code,
            dds.score,
            cm.primary_ticker
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1
        AND dds.score IS NOT NULL
    """, score_date)

    # Structure: {dimension: {company_id: score}}
    scores = defaultdict(dict)
    tickers = {}

    for row in rows:
        company_id = row['company_id']
        dimension = row['dimension_code']
        scores[dimension][company_id] = row['score']
        tickers[company_id] = row['primary_ticker']

    return dict(scores), tickers


async def get_forward_returns(conn, tickers: Dict[str, str], start_date: date,
                              holding_days: int = 21) -> Dict[str, float]:
    """Get forward returns for each company."""

    end_date = start_date + timedelta(days=holding_days + 10)  # Buffer for weekends

    returns = {}

    for company_id, ticker in tickers.items():
        rows = await conn.fetch("""
            SELECT date, close_price
            FROM daily_price_data
            WHERE symbol = $1
            AND date >= $2
            AND date <= $3
            ORDER BY date
        """, ticker, start_date, end_date)

        if len(rows) >= 2:
            # Find entry price (first available) and exit price (after holding period)
            entry_price = rows[0]['close_price']

            # Find price closest to holding_days out
            target_date = start_date + timedelta(days=holding_days)
            exit_price = None

            for row in rows:
                if row['date'] >= target_date:
                    exit_price = row['close_price']
                    break

            # If no price after target, use last available
            if exit_price is None and len(rows) > 1:
                exit_price = rows[-1]['close_price']

            if entry_price and exit_price and entry_price > 0:
                returns[company_id] = (exit_price - entry_price) / entry_price * 100

    return returns


def assign_quintiles(scores: Dict[str, float]) -> Dict[str, int]:
    """Assign each company to a quintile (1=lowest score, 5=highest score)."""

    if not scores:
        return {}

    # Sort by score
    sorted_items = sorted(scores.items(), key=lambda x: x[1])
    n = len(sorted_items)

    quintiles = {}
    for i, (company_id, score) in enumerate(sorted_items):
        # Quintile 1 = lowest 20%, Quintile 5 = highest 20%
        quintile = min(5, int(i / n * 5) + 1)
        quintiles[company_id] = quintile

    return quintiles


async def analyze_dimension(conn, dimension: str, score_dates: List[date],
                           holding_days: int = 21) -> pd.DataFrame:
    """Analyze a single dimension across multiple dates."""

    all_results = []

    for score_date in score_dates:
        # Get scores for this date
        scores_by_dim, tickers = await get_dimension_scores_for_date(conn, score_date)

        if dimension not in scores_by_dim:
            continue

        dim_scores = scores_by_dim[dimension]

        if len(dim_scores) < 20:  # Need enough companies for meaningful quintiles
            continue

        # Assign quintiles
        quintiles = assign_quintiles(dim_scores)

        # Get forward returns
        returns = await get_forward_returns(conn, tickers, score_date, holding_days)

        # Record results
        for company_id, quintile in quintiles.items():
            if company_id in returns:
                all_results.append({
                    'date': score_date,
                    'company_id': company_id,
                    'ticker': tickers.get(company_id, 'N/A'),
                    'dimension': dimension,
                    'score': dim_scores[company_id],
                    'quintile': quintile,
                    'forward_return': returns[company_id],
                })

    return pd.DataFrame(all_results)


async def get_available_score_dates(conn) -> List[date]:
    """Get all dates where we have dimension scores."""

    rows = await conn.fetch("""
        SELECT DISTINCT score_date
        FROM daily_dimension_scores
        WHERE score_date <= CURRENT_DATE - INTERVAL '30 days'  -- Need forward return window
        ORDER BY score_date
    """)

    return [row['score_date'] for row in rows]


async def main():
    parser = argparse.ArgumentParser(description='Dimension Skewness Backtest')
    parser.add_argument('--holding-days', type=int, default=21,
                        help='Holding period in days (default: 21)')
    parser.add_argument('--output', default='dimension_skewness.xlsx',
                        help='Output Excel file')
    parser.add_argument('--dimension', help='Test single dimension only')
    parser.add_argument('--limit-dates', type=int, help='Limit number of dates to test')
    args = parser.parse_args()

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("Fetching available score dates...")
        score_dates = await get_available_score_dates(conn)
        print(f"Found {len(score_dates)} dates with dimension scores")

        if args.limit_dates:
            # Sample evenly across the date range
            step = max(1, len(score_dates) // args.limit_dates)
            score_dates = score_dates[::step][:args.limit_dates]
            print(f"Limiting to {len(score_dates)} dates")

        if not score_dates:
            print("No score dates found!")
            return

        # Determine which dimensions to test
        dimensions = [args.dimension] if args.dimension else DIMENSIONS

        all_data = []
        summary_data = []

        for dim in dimensions:
            print(f"\nAnalyzing {dim}...")

            df = await analyze_dimension(conn, dim, score_dates, args.holding_days)

            if df.empty:
                print(f"  No data for {dim}")
                continue

            all_data.append(df)

            # Calculate quintile statistics
            quintile_stats = df.groupby('quintile').agg({
                'forward_return': ['mean', 'median', 'std', 'count'],
            }).round(2)

            quintile_stats.columns = ['mean_return', 'median_return', 'std_return', 'n_trades']
            quintile_stats = quintile_stats.reset_index()
            quintile_stats['dimension'] = dim

            # Calculate spread (Q5 - Q1)
            q1_return = quintile_stats[quintile_stats['quintile'] == 1]['mean_return'].values[0]
            q5_return = quintile_stats[quintile_stats['quintile'] == 5]['mean_return'].values[0]
            spread = q5_return - q1_return

            # Win rate by quintile
            for q in range(1, 6):
                q_data = df[df['quintile'] == q]
                win_rate = (q_data['forward_return'] > 0).mean() * 100
                quintile_stats.loc[quintile_stats['quintile'] == q, 'win_rate'] = round(win_rate, 1)

            print(f"  Q5-Q1 Spread: {spread:+.2f}%")
            print(f"  Q1 (lowest score): {q1_return:+.2f}% avg return")
            print(f"  Q5 (highest score): {q5_return:+.2f}% avg return")

            # Summary row
            summary_data.append({
                'dimension': dim,
                'q1_return': q1_return,
                'q2_return': quintile_stats[quintile_stats['quintile'] == 2]['mean_return'].values[0],
                'q3_return': quintile_stats[quintile_stats['quintile'] == 3]['mean_return'].values[0],
                'q4_return': quintile_stats[quintile_stats['quintile'] == 4]['mean_return'].values[0],
                'q5_return': q5_return,
                'spread_q5_q1': spread,
                'monotonic': 'Yes' if _is_monotonic(quintile_stats['mean_return'].tolist()) else 'No',
                'total_trades': int(quintile_stats['n_trades'].sum()),
            })

        if not all_data:
            print("\nNo data collected!")
            return

        # Combine all raw data
        full_df = pd.concat(all_data, ignore_index=True)

        # Create summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        summary_df = summary_df.sort_values('spread_q5_q1', ascending=False)

        # Create quintile breakdown for each dimension
        quintile_pivot = full_df.groupby(['dimension', 'quintile']).agg({
            'forward_return': ['mean', 'median', 'std', 'count']
        }).round(2)
        quintile_pivot.columns = ['mean', 'median', 'std', 'count']
        quintile_pivot = quintile_pivot.reset_index()

        # Export to Excel with multiple sheets
        print(f"\nExporting to {args.output}...")

        with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
            # Summary sheet
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Quintile details
            quintile_pivot.to_excel(writer, sheet_name='Quintile Details', index=False)

            # Raw trade data (limited to avoid huge files)
            if len(full_df) > 50000:
                full_df.sample(50000).to_excel(writer, sheet_name='Raw Trades (Sample)', index=False)
            else:
                full_df.to_excel(writer, sheet_name='Raw Trades', index=False)

            # Per-dimension sheets with quintile breakdown
            for dim in dimensions:
                dim_data = full_df[full_df['dimension'] == dim]
                if not dim_data.empty:
                    # Create quintile summary for this dimension
                    dim_summary = dim_data.groupby('quintile').agg({
                        'forward_return': ['mean', 'median', 'std', 'count'],
                        'score': ['mean', 'min', 'max'],
                    }).round(2)
                    dim_summary.columns = ['ret_mean', 'ret_median', 'ret_std', 'n_trades',
                                          'score_mean', 'score_min', 'score_max']
                    dim_summary['win_rate'] = dim_data.groupby('quintile').apply(
                        lambda x: (x['forward_return'] > 0).mean() * 100
                    ).round(1)

                    # Truncate sheet name to 31 chars (Excel limit)
                    sheet_name = dim[:31]
                    dim_summary.to_excel(writer, sheet_name=sheet_name)

        print(f"\nDone! Results saved to {args.output}")

        # Print summary table
        print("\n" + "="*80)
        print("DIMENSION SKEWNESS SUMMARY")
        print("="*80)
        print(f"{'Dimension':<25} {'Q1':>8} {'Q2':>8} {'Q3':>8} {'Q4':>8} {'Q5':>8} {'Spread':>8} {'Mono':>6}")
        print("-"*80)

        for _, row in summary_df.iterrows():
            print(f"{row['dimension']:<25} "
                  f"{row['q1_return']:>+7.2f}% "
                  f"{row['q2_return']:>+7.2f}% "
                  f"{row['q3_return']:>+7.2f}% "
                  f"{row['q4_return']:>+7.2f}% "
                  f"{row['q5_return']:>+7.2f}% "
                  f"{row['spread_q5_q1']:>+7.2f}% "
                  f"{row['monotonic']:>6}")

        print("="*80)
        print("\nInterpretation:")
        print("- Positive spread: Higher scores → better returns (dimension works as expected)")
        print("- Negative spread: Higher scores → worse returns (contrarian signal)")
        print("- Monotonic: Returns increase smoothly from Q1 to Q5")
        print(f"\nHolding period: {args.holding_days} days")
        print(f"Total score dates analyzed: {len(score_dates)}")

    finally:
        await conn.close()


def _is_monotonic(values: List[float]) -> bool:
    """Check if values are monotonically increasing."""
    if len(values) < 2:
        return True

    # Allow for small deviations (within 0.5%)
    increasing = all(values[i] <= values[i+1] + 0.5 for i in range(len(values)-1))
    return increasing


if __name__ == "__main__":
    asyncio.run(main())
