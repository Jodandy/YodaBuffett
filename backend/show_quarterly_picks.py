#!/usr/bin/env python3
"""
Show Quarterly Picks

Display the top picks for each quarter with their actual returns.
Great for reviewing what the strategy would have picked historically.

Usage:
    python show_quarterly_picks.py                    # Default: ML weights, top 20
    python show_quarterly_picks.py --top 10           # Top 10 per quarter
    python show_quarterly_picks.py --weights equal    # Equal weights
    python show_quarterly_picks.py --lag 60           # No look-ahead bias
    python show_quarterly_picks.py --quarter 2024-06-30  # Single quarter
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Weight profiles (same as fat_pitch_backtest.py)
WEIGHT_PROFILES = {
    'ml': {
        'beneish_mscore': 15, 'capital_allocation': 15, 'profitability': 12,
        'quality': 12, 'returns': 10, 'earnings_quality': 10, 'growth': 8,
        'working_capital': 6, 'risk': 5, 'valuation_percentile': 4, 'value': 3,
        'momentum': 0, 'sentiment': 0, 'financial_health': 0,
    },
    'original': {
        'profitability': 10, 'returns': 10, 'growth': 10, 'financial_health': 10,
        'earnings_quality': 10, 'capital_allocation': 10, 'working_capital': 5,
        'beneish_mscore': 5, 'value': 10, 'risk': 5, 'momentum': 5, 'quality': 10,
        'valuation_percentile': 0, 'sentiment': 0,
    },
    'equal': {dim: 1 for dim in [
        'profitability', 'returns', 'growth', 'financial_health', 'earnings_quality',
        'capital_allocation', 'working_capital', 'beneish_mscore', 'value', 'risk',
        'momentum', 'quality', 'valuation_percentile', 'sentiment'
    ]},
    'value': {
        'value': 25, 'valuation_percentile': 20, 'earnings_quality': 15,
        'profitability': 15, 'financial_health': 10, 'beneish_mscore': 10, 'quality': 5,
        'returns': 0, 'growth': 0, 'capital_allocation': 0, 'working_capital': 0,
        'risk': 0, 'momentum': 0, 'sentiment': 0,
    },
    'quality': {
        'quality': 20, 'profitability': 15, 'returns': 15, 'earnings_quality': 15,
        'capital_allocation': 10, 'beneish_mscore': 10, 'financial_health': 10, 'growth': 5,
        'working_capital': 0, 'value': 0, 'risk': 0, 'momentum': 0,
        'valuation_percentile': 0, 'sentiment': 0,
    },
    'contrarian': {
        'capital_allocation': 15, 'beneish_mscore': 14, 'quality': 11, 'profitability': 11,
        'returns': 10, 'earnings_quality': 7, 'growth': 4, 'working_capital': 4, 'value': 3,
        'momentum': -10, 'risk': 0, 'valuation_percentile': 0, 'sentiment': 0, 'financial_health': 0,
    },
    'anti': {
        'capital_allocation': -15, 'beneish_mscore': -14, 'quality': -11, 'profitability': -11,
        'returns': -10, 'earnings_quality': -7, 'growth': -4, 'working_capital': -4, 'value': -3,
        'momentum': 0, 'risk': 0, 'valuation_percentile': 0, 'sentiment': 0, 'financial_health': 0,
    },
}

DIMENSIONS = list(WEIGHT_PROFILES['ml'].keys())


async def get_quarterly_dates(conn, lag_days: int = 0) -> List[date]:
    """Get all quarterly dates with data."""
    rows = await conn.fetch('''
        SELECT score_date, COUNT(DISTINCT company_id) as companies
        FROM daily_dimension_scores
        GROUP BY score_date
        HAVING COUNT(DISTINCT company_id) >= 500
        ORDER BY score_date
    ''')

    # Filter based on lag (need prior quarter data)
    if lag_days > 0:
        dates = []
        all_dates = [r['score_date'] for r in rows]
        for d in all_dates:
            lagged = d - timedelta(days=lag_days)
            # Check if there's a score_date <= lagged
            if any(sd <= lagged for sd in all_dates):
                dates.append(d)
        return dates

    return [r['score_date'] for r in rows]


async def get_top_picks(conn, score_date: date, weights: Dict, top_n: int, lag_days: int) -> pd.DataFrame:
    """Get top N picks for a quarter."""

    # Apply lag
    if lag_days > 0:
        lagged_date = score_date - timedelta(days=lag_days)
        actual_score_date = await conn.fetchval("""
            SELECT MAX(score_date) FROM daily_dimension_scores
            WHERE score_date <= $1
        """, lagged_date)
        if not actual_score_date:
            return pd.DataFrame()
    else:
        actual_score_date = score_date

    pivot_cases = ",\n        ".join([
        f"MAX(CASE WHEN dimension_code = '{dim}' THEN score END) as {dim}"
        for dim in DIMENSIONS
    ])

    query = f"""
    SELECT
        dds.company_id,
        cm.company_name,
        cm.sector,
        cm.yahoo_symbol,
        {pivot_cases}
    FROM daily_dimension_scores dds
    JOIN company_master cm ON dds.company_id = cm.id
    WHERE dds.score_date = $1
    GROUP BY dds.company_id, cm.company_name, cm.sector, cm.yahoo_symbol
    HAVING COUNT(DISTINCT dimension_code) >= 8
    """

    rows = await conn.fetch(query, actual_score_date)
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty:
        return df

    # Convert dimensions to float
    for dim in DIMENSIONS:
        if dim in df.columns:
            df[dim] = pd.to_numeric(df[dim], errors='coerce').fillna(50)

    # Calculate weighted score (use absolute weights for normalization)
    total_weight = sum(abs(w) for w in weights.values())
    df['score'] = sum(
        df[dim] * weight / total_weight
        for dim, weight in weights.items()
        if dim in df.columns and weight != 0
    )

    # Get top N
    top = df.nlargest(top_n, 'score')

    return top


async def get_returns(conn, picks_df: pd.DataFrame, entry_date: date) -> Dict:
    """Get forward returns for companies using symbol matching (not company_id)."""

    returns = {}

    for _, row in picks_df.iterrows():
        cid = row['company_id']
        yahoo_symbol = row.get('yahoo_symbol')

        if not yahoo_symbol:
            returns[cid] = {'3M': None, '6M': None, '12M': None}
            continue

        # Strip exchange suffix and normalize (ATCO-B.ST -> ATCO B, MAERSK-B.CO -> MAERSK B)
        symbol = yahoo_symbol.split('.')[0] if '.' in yahoo_symbol else yahoo_symbol
        symbol = symbol.replace('-', ' ')  # daily_price_data uses spaces not hyphens

        # Get entry price by symbol
        entry_row = await conn.fetchrow("""
            SELECT date as entry_date, close_price as entry_price
            FROM daily_price_data
            WHERE symbol = $1 AND date >= $2 AND date <= $2 + INTERVAL '7 days'
            AND close_price > 0
            ORDER BY date LIMIT 1
        """, symbol, entry_date)

        if not entry_row:
            returns[cid] = {'3M': None, '6M': None, '12M': None}
            continue

        entry_dt = entry_row['entry_date']
        entry_price = float(entry_row['entry_price'])
        returns[cid] = {'entry_price': entry_price}

        for name, days in [('3M', 63), ('6M', 126), ('12M', 252)]:
            target_start = entry_dt + timedelta(days=days)
            target_end = entry_dt + timedelta(days=days + 14)

            exit_row = await conn.fetchrow("""
                SELECT close_price FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close_price > 0
                ORDER BY date LIMIT 1
            """, symbol, target_start, target_end)

            if exit_row and entry_price > 0:
                ret = ((float(exit_row['close_price']) - entry_price) / entry_price) * 100
                returns[cid][name] = ret
            else:
                returns[cid][name] = None

    return returns


def format_return(val):
    """Format return value."""
    if val is None:
        return "   N/A"
    return f"{val:+6.1f}%"


def color_return(val):
    """Add color indicator for return."""
    if val is None:
        return "  "
    if val > 20:
        return "🟢"
    elif val > 0:
        return "🔵"
    elif val > -20:
        return "🟡"
    else:
        return "🔴"


async def show_picks(weights_name: str, top_n: int, lag_days: int, single_quarter: Optional[str] = None):
    """Show top picks for each quarter."""

    conn = await asyncpg.connect(DATABASE_URL)
    weights = WEIGHT_PROFILES[weights_name]

    try:
        if single_quarter:
            quarters = [date.fromisoformat(single_quarter)]
        else:
            quarters = await get_quarterly_dates(conn, lag_days)
            # Exclude recent quarters without 12M returns
            cutoff = date.today() - timedelta(days=365)
            quarters = [q for q in quarters if q < cutoff]

        print("\n" + "=" * 120)
        print(f"QUARTERLY TOP {top_n} PICKS")
        print(f"Weights: {weights_name.upper()}, Lag: {lag_days} days {'(NO LOOK-AHEAD BIAS)' if lag_days >= 60 else ''}")
        print("=" * 120)

        all_picks = []

        for q_date in quarters:
            print(f"\n{'─' * 120}")
            print(f"📅 {q_date} (Q{(q_date.month-1)//3 + 1} {q_date.year})")
            print(f"{'─' * 120}")
            print(f"{'#':>2} {'Company':<35} {'Sector':<20} {'Score':>6} │ {'3M':>7} {'6M':>7} {'12M':>7} │")
            print(f"{'─' * 120}")

            # Get picks
            picks = await get_top_picks(conn, q_date, weights, top_n, lag_days)

            if picks.empty:
                print("   No data available for this quarter")
                continue

            # Get returns (using symbol matching for better coverage)
            returns = await get_returns(conn, picks, q_date)

            # Display
            for i, (_, row) in enumerate(picks.iterrows(), 1):
                cid = row['company_id']
                ret = returns.get(cid, {})

                r3m = ret.get('3M')
                r6m = ret.get('6M')
                r12m = ret.get('12M')

                company = row['company_name'][:33] if row['company_name'] else 'Unknown'
                sector = (row['sector'] or '')[:18]

                indicator = color_return(r12m)

                print(f"{i:>2} {company:<35} {sector:<20} {row['score']:>5.1f} │ "
                      f"{format_return(r3m)} {format_return(r6m)} {format_return(r12m)} {indicator}")

                all_picks.append({
                    'quarter': q_date,
                    'rank': i,
                    'company': row['company_name'],
                    'sector': row['sector'],
                    'score': row['score'],
                    'return_3M': r3m,
                    'return_6M': r6m,
                    'return_12M': r12m,
                })

            # Quarter summary
            valid_12m = [p['return_12M'] for p in all_picks if p['quarter'] == q_date and p['return_12M'] is not None]
            if valid_12m:
                avg = np.mean(valid_12m)
                winners = sum(1 for r in valid_12m if r > 0)
                print(f"{'─' * 120}")
                print(f"   Quarter Avg 12M: {avg:+.1f}% | Winners: {winners}/{len(valid_12m)} ({100*winners/len(valid_12m):.0f}%)")

        # Overall summary
        if all_picks:
            df = pd.DataFrame(all_picks)
            print("\n" + "=" * 120)
            print("OVERALL SUMMARY")
            print("=" * 120)

            for horizon in ['3M', '6M', '12M']:
                col = f'return_{horizon}'
                valid = df[col].dropna()
                if len(valid) > 0:
                    avg = valid.mean()
                    winners = (valid > 0).sum()
                    big_winners = (valid > 50).sum()
                    big_losers = (valid < -50).sum()
                    print(f"{horizon}: Avg {avg:+.1f}% | Winners {winners}/{len(valid)} ({100*winners/len(valid):.0f}%) | "
                          f">50%: {big_winners} | <-50%: {big_losers}")

            # Most picked companies
            print(f"\n{'─' * 60}")
            print("MOST FREQUENTLY PICKED COMPANIES")
            print(f"{'─' * 60}")
            freq = df['company'].value_counts().head(10)
            for company, count in freq.items():
                company_returns = df[df['company'] == company]['return_12M'].dropna()
                avg_ret = company_returns.mean() if len(company_returns) > 0 else None
                ret_str = f"{avg_ret:+.1f}%" if avg_ret is not None else "N/A"
                print(f"  {company:<40} picked {count}x, avg 12M: {ret_str}")

    finally:
        await conn.close()


async def main():
    parser = argparse.ArgumentParser(description='Show quarterly top picks')
    parser.add_argument('--top', type=int, default=20, help='Number of top picks per quarter')
    parser.add_argument('--weights', type=str, default='ml',
                        choices=['ml', 'original', 'equal', 'value', 'quality', 'contrarian', 'anti'],
                        help='Weight profile')
    parser.add_argument('--lag', type=int, default=0, help='Days to lag (60 = no look-ahead)')
    parser.add_argument('--quarter', type=str, default=None, help='Single quarter (YYYY-MM-DD)')

    args = parser.parse_args()
    await show_picks(args.weights, args.top, args.lag, args.quarter)


if __name__ == '__main__':
    asyncio.run(main())
