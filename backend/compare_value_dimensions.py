#!/usr/bin/env python3
"""
Compare Value Dimensions

Tests different value dimension choices in GARP-style weights:
1. value (fixed) - P/E, P/B vs peers
2. valuation_percentile - P/E vs own history
3. Both combined

Run after value dimension backfill completes.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from typing import Dict, List
from collections import defaultdict

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# GARP base weights (without value component)
GARP_BASE = {
    'growth': 0.30,
    'profitability': 0.15,
    'earnings_quality': 0.10,
    'returns': 0.10,
    'quality': 0.05,
    'momentum': 0.05,
}

# Three versions to test
WEIGHT_PROFILES = {
    'garp_value': {
        **GARP_BASE,
        'value': 0.25,
        'valuation_percentile': 0,
    },
    'garp_val_pct': {
        **GARP_BASE,
        'value': 0,
        'valuation_percentile': 0.25,
    },
    'garp_both': {
        **GARP_BASE,
        'value': 0.125,
        'valuation_percentile': 0.125,
    },
}


async def get_scores_for_date(conn, score_date: date) -> pd.DataFrame:
    """Get all dimension scores for a date."""
    rows = await conn.fetch("""
        SELECT dds.company_id::text, cm.primary_ticker, dds.dimension_code, dds.score
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1 AND dds.score IS NOT NULL
    """, score_date)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    return df.pivot_table(
        index=['company_id', 'primary_ticker'],
        columns='dimension_code',
        values='score'
    ).reset_index()


def calculate_weighted_score(row: pd.Series, weights: Dict[str, float]) -> float:
    """Calculate weighted composite score."""
    total_weight = 0
    weighted_sum = 0

    for dim, weight in weights.items():
        if weight > 0 and dim in row and pd.notna(row[dim]):
            weighted_sum += float(row[dim]) * weight
            total_weight += weight

    return weighted_sum / total_weight if total_weight > 0.5 else None


async def get_forward_returns(conn, tickers: List[str], start_date: date,
                              holding_days: int) -> Dict[str, float]:
    """Get forward returns."""
    end_date = start_date + timedelta(days=holding_days + 10)

    rows = await conn.fetch("""
        SELECT symbol, date, close_price
        FROM daily_price_data
        WHERE symbol = ANY($1) AND date >= $2 AND date <= $3
        ORDER BY symbol, date
    """, tickers, start_date, end_date)

    by_symbol = defaultdict(list)
    for row in rows:
        by_symbol[row['symbol']].append(row)

    returns = {}
    target_date = start_date + timedelta(days=holding_days)

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
            if entry > 0:
                returns[symbol] = (exit_price - entry) / entry * 100

    return returns


async def run_backtest(conn, weights: Dict[str, float], holding_days: int = 63) -> Dict:
    """Run backtest for a weight profile."""

    # Get score dates
    dates = await conn.fetch("""
        SELECT DISTINCT score_date FROM daily_dimension_scores
        WHERE score_date <= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY score_date
    """)
    score_dates = [r['score_date'] for r in dates]

    all_returns = []

    for score_date in score_dates:
        df = await get_scores_for_date(conn, score_date)
        if df.empty:
            continue

        # Calculate weighted scores
        df['weighted_score'] = df.apply(lambda row: calculate_weighted_score(row, weights), axis=1)
        df = df.dropna(subset=['weighted_score'])

        if len(df) < 20:
            continue

        # Get top 20 by score
        top20 = df.nlargest(20, 'weighted_score')
        tickers = top20['primary_ticker'].tolist()

        # Get forward returns
        returns = await get_forward_returns(conn, tickers, score_date, holding_days)

        for ticker in tickers:
            if ticker in returns:
                all_returns.append(returns[ticker])

    if not all_returns:
        return {'avg': 0, 'median': 0, 'win_rate': 0, 'n': 0}

    return {
        'avg': sum(all_returns) / len(all_returns),
        'median': sorted(all_returns)[len(all_returns)//2],
        'win_rate': sum(1 for r in all_returns if r > 0) / len(all_returns) * 100,
        'n': len(all_returns),
    }


async def main():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Check value dimension status
        value_count = await conn.fetchval("""
            SELECT COUNT(*) FROM daily_dimension_scores WHERE dimension_code = 'value'
        """)
        val_pct_count = await conn.fetchval("""
            SELECT COUNT(*) FROM daily_dimension_scores WHERE dimension_code = 'valuation_percentile'
        """)

        print("=" * 70)
        print("COMPARING VALUE DIMENSIONS IN GARP WEIGHTS")
        print("=" * 70)
        print(f"\nDimension counts:")
        print(f"  value: {value_count:,}")
        print(f"  valuation_percentile: {val_pct_count:,}")

        print(f"\n{'Profile':<20} {'Avg Ret':>10} {'Median':>10} {'Win Rate':>10} {'Trades':>8}")
        print("-" * 60)

        for profile_name, weights in WEIGHT_PROFILES.items():
            results = await run_backtest(conn, weights, holding_days=63)
            print(f"{profile_name:<20} {results['avg']:>+9.2f}% {results['median']:>+9.2f}% "
                  f"{results['win_rate']:>9.1f}% {results['n']:>8}")

        print("-" * 60)
        print("\nInterpretation:")
        print("  garp_value: Uses fixed value dimension (P/E vs peers)")
        print("  garp_val_pct: Uses valuation_percentile (P/E vs own history)")
        print("  garp_both: Uses 50/50 blend of both")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
