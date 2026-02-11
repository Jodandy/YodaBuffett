#!/usr/bin/env python3
"""
Reverse Engineer Winners

Instead of factor analysis, find what the BIGGEST WINNERS had in common
before they became winners.

Approach:
1. Find top 5-10% returns over 6-12 months
2. Look at their dimension scores BEFORE the return period
3. Compare to average and to biggest losers
4. Find patterns that could identify fat pitches
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

DIMENSIONS = [
    'profitability', 'returns', 'growth', 'financial_health',
    'value', 'risk', 'momentum', 'quality', 'earnings_quality',
    'capital_allocation', 'valuation_percentile', 'beneish_mscore',
    'working_capital',
]


async def get_dimension_scores(conn, score_date: date) -> pd.DataFrame:
    """Get all dimension scores for a date."""
    rows = await conn.fetch("""
        SELECT dds.company_id::text, cm.primary_ticker, cm.company_name,
               dds.dimension_code, dds.score
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1 AND dds.score IS NOT NULL
    """, score_date)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    pivoted = df.pivot_table(
        index=['company_id', 'primary_ticker', 'company_name'],
        columns='dimension_code',
        values='score'
    ).reset_index()

    return pivoted


async def get_forward_returns(conn, company_ids: List[str], start_date: date,
                              holding_days: int) -> Dict[str, Tuple[float, str]]:
    """Get forward returns for companies. Returns dict of company_id -> (return%, ticker)."""

    # Get tickers for company IDs
    rows = await conn.fetch("""
        SELECT id::text as company_id, primary_ticker
        FROM company_master
        WHERE id = ANY($1::uuid[])
    """, company_ids)

    id_to_ticker = {r['company_id']: r['primary_ticker'] for r in rows}
    tickers = list(id_to_ticker.values())

    end_date = start_date + timedelta(days=holding_days + 10)

    # Get prices
    price_rows = await conn.fetch("""
        SELECT symbol, date, close_price
        FROM daily_price_data
        WHERE symbol = ANY($1) AND date >= $2 AND date <= $3
        ORDER BY symbol, date
    """, tickers, start_date, end_date)

    by_symbol = defaultdict(list)
    for row in price_rows:
        by_symbol[row['symbol']].append(row)

    returns = {}
    target_date = start_date + timedelta(days=holding_days)

    ticker_to_id = {v: k for k, v in id_to_ticker.items()}

    for symbol, prices in by_symbol.items():
        if len(prices) >= 2:
            entry = float(prices[0]['close_price'])
            exit_price = None
            for p in prices:
                if p['date'] >= target_date:
                    exit_price = float(p['close_price'])
                    break
            if exit_price is None:
                exit_price = float(prices[-1]['close_price'])

            if entry > 0 and symbol in ticker_to_id:
                ret = (exit_price - entry) / entry * 100
                # Filter extreme outliers (likely stock splits)
                if -90 < ret < 500:
                    returns[ticker_to_id[symbol]] = (ret, symbol)

    return returns


async def analyze_winners(conn, holding_days: int = 180,
                          top_pct: float = 0.10, bottom_pct: float = 0.10):
    """Find winners and analyze their pre-win characteristics."""

    # Get all score dates with enough history for forward returns
    cutoff = date.today() - timedelta(days=holding_days + 30)
    dates = await conn.fetch("""
        SELECT DISTINCT score_date FROM daily_dimension_scores
        WHERE score_date <= $1
        ORDER BY score_date
    """, cutoff)
    score_dates = [r['score_date'] for r in dates]

    print(f"Analyzing {len(score_dates)} score dates with {holding_days}-day forward returns")
    print(f"Looking for top {top_pct*100:.0f}% winners and bottom {bottom_pct*100:.0f}% losers\n")

    all_data = []

    for i, score_date in enumerate(score_dates):
        if i % 10 == 0:
            print(f"Processing {i+1}/{len(score_dates)}: {score_date}")

        # Get scores
        scores_df = await get_dimension_scores(conn, score_date)
        if scores_df.empty or len(scores_df) < 50:
            continue

        # Get forward returns
        company_ids = scores_df['company_id'].tolist()
        returns = await get_forward_returns(conn, company_ids, score_date, holding_days)

        # Merge returns with scores
        scores_df['forward_return'] = scores_df['company_id'].map(
            lambda x: returns.get(x, (None, None))[0]
        )
        scores_df['score_date'] = score_date

        # Drop rows without returns
        scores_df = scores_df.dropna(subset=['forward_return'])

        if len(scores_df) >= 50:
            all_data.append(scores_df)

    if not all_data:
        print("No data found!")
        return

    # Combine all data
    df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal observations: {len(df):,}")
    print(f"Unique companies: {df['company_id'].nunique()}")
    print(f"Date range: {df['score_date'].min()} to {df['score_date'].max()}")

    # Define winners and losers
    top_threshold = df['forward_return'].quantile(1 - top_pct)
    bottom_threshold = df['forward_return'].quantile(bottom_pct)

    winners = df[df['forward_return'] >= top_threshold].copy()
    losers = df[df['forward_return'] <= bottom_threshold].copy()
    middle = df[(df['forward_return'] > bottom_threshold) &
                (df['forward_return'] < top_threshold)].copy()

    print(f"\nWinners (top {top_pct*100:.0f}%): {len(winners):,} observations")
    print(f"  Return threshold: >= {top_threshold:.1f}%")
    print(f"  Avg return: {winners['forward_return'].mean():.1f}%")
    print(f"  Median return: {winners['forward_return'].median():.1f}%")

    print(f"\nLosers (bottom {bottom_pct*100:.0f}%): {len(losers):,} observations")
    print(f"  Return threshold: <= {bottom_threshold:.1f}%")
    print(f"  Avg return: {losers['forward_return'].mean():.1f}%")
    print(f"  Median return: {losers['forward_return'].median():.1f}%")

    print(f"\nMiddle: {len(middle):,} observations")
    print(f"  Avg return: {middle['forward_return'].mean():.1f}%")

    # Analyze dimension profiles
    print("\n" + "="*80)
    print("DIMENSION PROFILES: WINNERS vs LOSERS vs AVERAGE")
    print("="*80)

    results = []
    for dim in DIMENSIONS:
        if dim not in df.columns:
            continue

        winner_avg = winners[dim].mean()
        loser_avg = losers[dim].mean()
        middle_avg = middle[dim].mean()
        overall_avg = df[dim].mean()

        # How different are winners from average?
        winner_diff = winner_avg - overall_avg
        loser_diff = loser_avg - overall_avg

        results.append({
            'dimension': dim,
            'winners': winner_avg,
            'losers': loser_avg,
            'middle': middle_avg,
            'overall': overall_avg,
            'winner_vs_avg': winner_diff,
            'loser_vs_avg': loser_diff,
            'winner_vs_loser': winner_avg - loser_avg,
        })

    results_df = pd.DataFrame(results)
    results_df = results_df.sort_values('winner_vs_loser', ascending=False)

    print(f"\n{'Dimension':<22} {'Winners':>10} {'Losers':>10} {'Middle':>10} {'W-L Diff':>10}")
    print("-" * 65)
    for _, row in results_df.iterrows():
        print(f"{row['dimension']:<22} {row['winners']:>10.1f} {row['losers']:>10.1f} "
              f"{row['middle']:>10.1f} {row['winner_vs_loser']:>+10.1f}")

    # Find the biggest differentiators
    print("\n" + "="*80)
    print("KEY DIFFERENTIATORS (sorted by Winner-Loser gap)")
    print("="*80)

    for _, row in results_df.head(5).iterrows():
        print(f"\n{row['dimension'].upper()}")
        print(f"  Winners avg: {row['winners']:.1f} (vs overall {row['overall']:.1f})")
        print(f"  Losers avg:  {row['losers']:.1f}")
        print(f"  Gap: {row['winner_vs_loser']:+.1f} points")

    # Show example winners
    print("\n" + "="*80)
    print("BIGGEST WINNERS (sample)")
    print("="*80)

    top_winners = winners.nlargest(20, 'forward_return')
    for _, row in top_winners.iterrows():
        dims_above_70 = []
        dims_below_30 = []
        for dim in DIMENSIONS:
            if dim in row and pd.notna(row[dim]):
                if row[dim] >= 70:
                    dims_above_70.append(f"{dim}:{row[dim]:.0f}")
                elif row[dim] <= 30:
                    dims_below_30.append(f"{dim}:{row[dim]:.0f}")

        print(f"\n{row['primary_ticker']} ({row['company_name'][:30]}) - {row['score_date']}")
        print(f"  Return: {row['forward_return']:+.1f}%")
        if dims_above_70:
            print(f"  High (>=70): {', '.join(dims_above_70[:5])}")
        if dims_below_30:
            print(f"  Low (<=30): {', '.join(dims_below_30[:5])}")

    # Show example losers
    print("\n" + "="*80)
    print("BIGGEST LOSERS (sample)")
    print("="*80)

    top_losers = losers.nsmallest(20, 'forward_return')
    for _, row in top_losers.iterrows():
        dims_above_70 = []
        dims_below_30 = []
        for dim in DIMENSIONS:
            if dim in row and pd.notna(row[dim]):
                if row[dim] >= 70:
                    dims_above_70.append(f"{dim}:{row[dim]:.0f}")
                elif row[dim] <= 30:
                    dims_below_30.append(f"{dim}:{row[dim]:.0f}")

        print(f"\n{row['primary_ticker']} ({row['company_name'][:30]}) - {row['score_date']}")
        print(f"  Return: {row['forward_return']:+.1f}%")
        if dims_above_70:
            print(f"  High (>=70): {', '.join(dims_above_70[:5])}")
        if dims_below_30:
            print(f"  Low (<=30): {', '.join(dims_below_30[:5])}")

    # Look for common patterns in winners
    print("\n" + "="*80)
    print("PATTERN ANALYSIS: What % of winners had...")
    print("="*80)

    patterns = {}
    for dim in DIMENSIONS:
        if dim not in winners.columns:
            continue

        # What % of winners had this dimension >= 70?
        pct_high = (winners[dim] >= 70).mean() * 100
        pct_low = (winners[dim] <= 30).mean() * 100

        # Compare to overall
        overall_high = (df[dim] >= 70).mean() * 100
        overall_low = (df[dim] <= 30).mean() * 100

        patterns[dim] = {
            'winner_high_pct': pct_high,
            'winner_low_pct': pct_low,
            'overall_high_pct': overall_high,
            'overall_low_pct': overall_low,
            'high_lift': pct_high - overall_high,
            'low_lift': pct_low - overall_low,
        }

    print(f"\n{'Dimension':<22} {'Winners >=70':>12} {'Overall >=70':>12} {'Lift':>8}")
    print("-" * 58)
    for dim, p in sorted(patterns.items(), key=lambda x: -x[1]['high_lift']):
        print(f"{dim:<22} {p['winner_high_pct']:>11.1f}% {p['overall_high_pct']:>11.1f}% {p['high_lift']:>+7.1f}%")

    print(f"\n{'Dimension':<22} {'Winners <=30':>12} {'Overall <=30':>12} {'Lift':>8}")
    print("-" * 58)
    for dim, p in sorted(patterns.items(), key=lambda x: -x[1]['low_lift']):
        print(f"{dim:<22} {p['winner_low_pct']:>11.1f}% {p['overall_low_pct']:>11.1f}% {p['low_lift']:>+7.1f}%")

    # Export to Excel
    output_file = 'winner_analysis.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        results_df.to_excel(writer, sheet_name='Dimension Profiles', index=False)

        # Top winners detail
        winner_cols = ['primary_ticker', 'company_name', 'score_date', 'forward_return'] + DIMENSIONS
        available_cols = [c for c in winner_cols if c in winners.columns]
        winners[available_cols].nlargest(100, 'forward_return').to_excel(
            writer, sheet_name='Top 100 Winners', index=False
        )

        # Top losers detail
        losers[available_cols].nsmallest(100, 'forward_return').to_excel(
            writer, sheet_name='Top 100 Losers', index=False
        )

        # Pattern analysis
        pattern_df = pd.DataFrame(patterns).T
        pattern_df.to_excel(writer, sheet_name='Pattern Analysis')

    print(f"\n\nResults saved to {output_file}")

    return results_df, winners, losers


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--holding-days', type=int, default=180,
                        help='Forward return period in days')
    parser.add_argument('--top-pct', type=float, default=0.10,
                        help='Top percentile to consider winners (0.10 = top 10%)')
    parser.add_argument('--bottom-pct', type=float, default=0.10,
                        help='Bottom percentile to consider losers')
    args = parser.parse_args()

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await analyze_winners(
            conn,
            holding_days=args.holding_days,
            top_pct=args.top_pct,
            bottom_pct=args.bottom_pct
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
