#!/usr/bin/env python3
"""
Score Momentum Analysis

Test hypothesis: Winners have RISING scores before their run,
not necessarily high absolute scores.

The "fat pitch" signal might be: low scores that are improving.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from collections import defaultdict
from typing import Dict, List, Optional

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

DIMENSIONS = [
    'profitability', 'returns', 'growth', 'financial_health',
    'value', 'risk', 'momentum', 'quality', 'earnings_quality',
    'capital_allocation', 'valuation_percentile', 'beneish_mscore',
    'working_capital',
]


async def get_score_changes(conn, company_id: str, score_date: date,
                            lookback_days: int = 90) -> Dict[str, float]:
    """Get score changes over lookback period."""

    start_date = score_date - timedelta(days=lookback_days)

    rows = await conn.fetch("""
        SELECT dimension_code, score_date, score
        FROM daily_dimension_scores
        WHERE company_id = $1
          AND score_date >= $2 AND score_date <= $3
          AND score IS NOT NULL
        ORDER BY dimension_code, score_date
    """, company_id, start_date, score_date)

    if not rows:
        return {}

    # Group by dimension
    by_dim = defaultdict(list)
    for r in rows:
        by_dim[r['dimension_code']].append((r['score_date'], float(r['score'])))

    changes = {}
    for dim, scores in by_dim.items():
        if len(scores) >= 2:
            # Get first and last score
            first_score = scores[0][1]
            last_score = scores[-1][1]
            changes[f"{dim}_change"] = last_score - first_score
            changes[f"{dim}_current"] = last_score
            changes[f"{dim}_start"] = first_score

    return changes


async def get_forward_return(conn, ticker: str, start_date: date,
                             holding_days: int) -> Optional[float]:
    """Get forward return for a single company."""

    end_date = start_date + timedelta(days=holding_days + 10)

    rows = await conn.fetch("""
        SELECT date, close_price
        FROM daily_price_data
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        ORDER BY date
    """, ticker, start_date, end_date)

    if len(rows) < 2:
        return None

    entry = float(rows[0]['close_price'])
    target_date = start_date + timedelta(days=holding_days)

    exit_price = None
    for r in rows:
        if r['date'] >= target_date:
            exit_price = float(r['close_price'])
            break

    if exit_price is None:
        exit_price = float(rows[-1]['close_price'])

    if entry > 0:
        ret = (exit_price - entry) / entry * 100
        if -90 < ret < 500:
            return ret

    return None


async def analyze_score_momentum(conn, holding_days: int = 180,
                                  lookback_days: int = 90):
    """Analyze relationship between score changes and forward returns."""

    # Get companies with enough history
    # Need: lookback_days of history before score_date, and holding_days of forward returns after
    cutoff = date.today() - timedelta(days=holding_days + 30)
    # Start from first date that has enough lookback history
    start_cutoff = date(2021, 9, 1)  # After we have 90 days of dimension history

    dates = await conn.fetch("""
        SELECT DISTINCT score_date FROM daily_dimension_scores
        WHERE score_date <= $1 AND score_date >= $2
        ORDER BY score_date
    """, cutoff, start_cutoff)
    score_dates = [r['score_date'] for r in dates]

    print(f"Analyzing {len(score_dates)} score dates")
    print(f"Lookback: {lookback_days} days, Forward return: {holding_days} days\n")

    all_data = []

    for i, score_date in enumerate(score_dates):
        if i % 5 == 0:
            print(f"Processing {i+1}/{len(score_dates)}: {score_date}")

        # Get all companies with scores on this date
        rows = await conn.fetch("""
            SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
            FROM daily_dimension_scores dds
            JOIN company_master cm ON cm.id = dds.company_id
            WHERE dds.score_date = $1
        """, score_date)

        for row in rows:
            company_id = row['company_id']
            ticker = row['primary_ticker']

            # Get score changes
            changes = await get_score_changes(conn, company_id, score_date, lookback_days)
            if not changes:
                continue

            # Get forward return
            fwd_return = await get_forward_return(conn, ticker, score_date, holding_days)
            if fwd_return is None:
                continue

            record = {
                'company_id': company_id,
                'ticker': ticker,
                'company_name': row['company_name'],
                'score_date': score_date,
                'forward_return': fwd_return,
                **changes
            }
            all_data.append(record)

    if not all_data:
        print("No data found!")
        return

    df = pd.DataFrame(all_data)
    print(f"\nTotal observations: {len(df):,}")

    # Calculate summary stats for each dimension's change
    print("\n" + "="*80)
    print("SCORE MOMENTUM vs FORWARD RETURNS")
    print("="*80)

    results = []
    for dim in DIMENSIONS:
        change_col = f"{dim}_change"
        current_col = f"{dim}_current"

        if change_col not in df.columns:
            continue

        # Split into quintiles by score change
        df_valid = df[df[change_col].notna()].copy()
        if len(df_valid) < 100:
            continue

        try:
            df_valid['change_quintile'] = pd.qcut(df_valid[change_col], 5, labels=False, duplicates='drop') + 1
        except ValueError:
            continue

        quintile_returns = df_valid.groupby('change_quintile')['forward_return'].mean()

        if len(quintile_returns) >= 2:
            q1_ret = quintile_returns.get(1, 0)
            q5_ret = quintile_returns.get(5, 0)
            spread = q5_ret - q1_ret

            results.append({
                'dimension': dim,
                'q1_return': q1_ret,
                'q5_return': q5_ret,
                'spread': spread,
                'avg_change_q1': df_valid[df_valid['change_quintile']==1][change_col].mean(),
                'avg_change_q5': df_valid[df_valid['change_quintile']==5][change_col].mean(),
            })

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('spread', ascending=False)

    print(f"\n{'Dimension':<22} {'Q1 (falling)':>12} {'Q5 (rising)':>12} {'Spread':>10}")
    print("-" * 60)
    for _, row in results_df.iterrows():
        print(f"{row['dimension']:<22} {row['q1_return']:>+11.1f}% {row['q5_return']:>+11.1f}% {row['spread']:>+9.1f}%")

    # Now look at COMBO: low current score + rising
    print("\n" + "="*80)
    print("COMBO ANALYSIS: Low Current Score + Rising")
    print("="*80)

    for dim in ['quality', 'profitability', 'returns', 'capital_allocation']:
        change_col = f"{dim}_change"
        current_col = f"{dim}_current"

        if change_col not in df.columns or current_col not in df.columns:
            continue

        df_valid = df[(df[change_col].notna()) & (df[current_col].notna())].copy()

        # Low and rising: current < 40 AND change > 5
        low_rising = df_valid[(df_valid[current_col] < 40) & (df_valid[change_col] > 5)]
        # Low and falling: current < 40 AND change < -5
        low_falling = df_valid[(df_valid[current_col] < 40) & (df_valid[change_col] < -5)]
        # High and rising
        high_rising = df_valid[(df_valid[current_col] >= 60) & (df_valid[change_col] > 5)]
        # High and falling
        high_falling = df_valid[(df_valid[current_col] >= 60) & (df_valid[change_col] < -5)]

        print(f"\n{dim.upper()}")
        print(f"  Low + Rising (turnaround):   {len(low_rising):>5} obs, avg return: {low_rising['forward_return'].mean():>+.1f}%")
        print(f"  Low + Falling (deteriorating):{len(low_falling):>5} obs, avg return: {low_falling['forward_return'].mean():>+.1f}%")
        print(f"  High + Rising (improving):   {len(high_rising):>5} obs, avg return: {high_rising['forward_return'].mean():>+.1f}%")
        print(f"  High + Falling (weakening):  {len(high_falling):>5} obs, avg return: {high_falling['forward_return'].mean():>+.1f}%")

    # Find example turnarounds
    print("\n" + "="*80)
    print("EXAMPLE TURNAROUNDS: Low Quality + Rising Scores + Big Returns")
    print("="*80)

    # Composite rising score
    change_cols = [f"{d}_change" for d in ['quality', 'profitability', 'returns'] if f"{d}_change" in df.columns]
    current_cols = [f"{d}_current" for d in ['quality', 'profitability', 'returns'] if f"{d}_current" in df.columns]

    df['avg_change'] = df[change_cols].mean(axis=1)
    df['avg_current'] = df[current_cols].mean(axis=1)

    # Turnarounds: low current (<40) + rising (>5 avg change) + good returns (>30%)
    turnarounds = df[
        (df['avg_current'] < 40) &
        (df['avg_change'] > 5) &
        (df['forward_return'] > 30)
    ].sort_values('forward_return', ascending=False)

    print(f"\nFound {len(turnarounds)} turnaround cases")
    print("\nTop 20 Turnarounds:")
    for _, row in turnarounds.head(20).iterrows():
        print(f"\n{row['ticker']} ({row['company_name'][:25]}) - {row['score_date']}")
        print(f"  Forward Return: {row['forward_return']:+.1f}%")
        print(f"  Avg Quality Current: {row['avg_current']:.1f}, Change: {row['avg_change']:+.1f}")
        for dim in ['quality', 'profitability', 'returns']:
            curr = row.get(f"{dim}_current")
            chg = row.get(f"{dim}_change")
            if pd.notna(curr) and pd.notna(chg):
                print(f"    {dim}: {curr:.0f} ({chg:+.1f})")

    # Failed turnarounds for comparison
    print("\n" + "="*80)
    print("FAILED TURNAROUNDS: Low Quality + Rising Scores + Bad Returns")
    print("="*80)

    failed = df[
        (df['avg_current'] < 40) &
        (df['avg_change'] > 5) &
        (df['forward_return'] < -30)
    ].sort_values('forward_return', ascending=True)

    print(f"\nFound {len(failed)} failed turnaround cases")
    print("\nWorst 20 Failed Turnarounds:")
    for _, row in failed.head(20).iterrows():
        print(f"\n{row['ticker']} ({row['company_name'][:25]}) - {row['score_date']}")
        print(f"  Forward Return: {row['forward_return']:+.1f}%")
        print(f"  Avg Quality Current: {row['avg_current']:.1f}, Change: {row['avg_change']:+.1f}")

    # Export
    output_file = 'score_momentum_analysis.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='Momentum vs Returns', index=False)
        turnarounds.head(100).to_excel(writer, sheet_name='Successful Turnarounds', index=False)
        failed.head(100).to_excel(writer, sheet_name='Failed Turnarounds', index=False)

    print(f"\n\nResults saved to {output_file}")

    return df, results_df


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--holding-days', type=int, default=180)
    parser.add_argument('--lookback-days', type=int, default=90)
    args = parser.parse_args()

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await analyze_score_momentum(conn, args.holding_days, args.lookback_days)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
