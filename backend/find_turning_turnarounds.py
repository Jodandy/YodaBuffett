#!/usr/bin/env python3
"""
Find Turning Turnarounds

The thesis: Buy when a beaten-down stock shows SIGNS OF LIFE
- Cheap (low P/S or high valuation_percentile)
- Momentum turning UP (from low base)
- Maybe profitability improving

Not just cheap + compressed - need the CATALYST signal.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from typing import Dict, Optional
from uuid import UUID

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_score_with_change(conn, company_id: str, score_date: date,
                                 lookback_days: int = 60) -> Optional[Dict]:
    """Get current scores and recent changes."""

    company_uuid = UUID(company_id) if isinstance(company_id, str) else company_id
    start_date = score_date - timedelta(days=lookback_days)

    # Get current scores
    current = await conn.fetch("""
        SELECT dimension_code, score
        FROM daily_dimension_scores
        WHERE company_id = $1 AND score_date = $2
    """, company_uuid, score_date)

    if not current:
        return None

    current_scores = {r['dimension_code']: float(r['score']) if r['score'] else 0 for r in current}

    # Get historical scores for change calculation
    historical = await conn.fetch("""
        SELECT dimension_code, score, score_date
        FROM daily_dimension_scores
        WHERE company_id = $1
          AND score_date >= $2 AND score_date < $3
        ORDER BY score_date
    """, company_uuid, start_date, score_date)

    # Calculate changes
    changes = {}
    for dim, curr_score in current_scores.items():
        hist_scores = [float(r['score']) for r in historical
                       if r['dimension_code'] == dim and r['score']]
        if hist_scores:
            old_score = hist_scores[0]  # Earliest in lookback
            changes[f"{dim}_change"] = curr_score - old_score
        else:
            changes[f"{dim}_change"] = 0

    return {**{f"score_{k}": v for k, v in current_scores.items()}, **changes}


async def backtest_turning_turnarounds(conn, holding_days: int = 180):
    """Backtest different turnaround signals."""

    cutoff = date.today() - timedelta(days=holding_days + 30)
    dates = await conn.fetch("""
        SELECT DISTINCT score_date FROM daily_dimension_scores
        WHERE score_date <= $1 AND score_date >= '2022-01-01'
        ORDER BY score_date
    """, cutoff)
    score_dates = [r['score_date'] for r in dates]

    print(f"Testing {len(score_dates)} dates with {holding_days}-day forward returns\n")

    # Define different strategies
    strategies = {
        'cheap_momentum_turn': {
            'desc': 'Cheap + Momentum turning up from low',
            'filter': lambda p: (
                p.get('score_valuation_percentile', 0) >= 70 and  # Cheap vs history
                p.get('score_momentum', 100) <= 40 and  # Currently low momentum
                p.get('momentum_change', 0) >= 10  # But momentum improving
            )
        },
        'cheap_quality_momentum': {
            'desc': 'Cheap + OK quality + Rising momentum',
            'filter': lambda p: (
                p.get('score_valuation_percentile', 0) >= 60 and
                p.get('score_quality', 0) >= 40 and
                p.get('momentum_change', 0) >= 5
            )
        },
        'deep_value_turn': {
            'desc': 'Very cheap + Any positive momentum change',
            'filter': lambda p: (
                p.get('score_valuation_percentile', 0) >= 80 and
                p.get('score_value', 0) >= 60 and
                p.get('momentum_change', 0) > 0
            )
        },
        'quality_recovery': {
            'desc': 'Quality improving + Cheap',
            'filter': lambda p: (
                p.get('score_valuation_percentile', 0) >= 60 and
                p.get('quality_change', 0) >= 5 and
                p.get('profitability_change', 0) >= 0
            )
        },
        'growth_reacceleration': {
            'desc': 'Growth accelerating from low base',
            'filter': lambda p: (
                p.get('score_growth', 0) >= 30 and
                p.get('growth_change', 0) >= 10 and
                p.get('score_valuation_percentile', 0) >= 50
            )
        },
    }

    results = {name: [] for name in strategies}

    for i, score_date in enumerate(score_dates):
        if i % 10 == 0:
            print(f"Processing {i+1}/{len(score_dates)}: {score_date}")

        # Get all companies
        companies = await conn.fetch("""
            SELECT DISTINCT dds.company_id::text, cm.primary_ticker
            FROM daily_dimension_scores dds
            JOIN company_master cm ON cm.id = dds.company_id
            WHERE dds.score_date = $1
        """, score_date)

        for comp in companies:
            profile = await get_score_with_change(conn, comp['company_id'], score_date)
            if not profile:
                continue

            # Get forward return
            ticker = comp['primary_ticker']
            end_date = score_date + timedelta(days=holding_days + 10)

            prices = await conn.fetch("""
                SELECT date, close_price FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $3
                ORDER BY date
            """, ticker, score_date, end_date)

            if len(prices) < 2:
                continue

            entry = float(prices[0]['close_price'])
            target_date = score_date + timedelta(days=holding_days)

            exit_price = None
            for p in prices:
                if p['date'] >= target_date:
                    exit_price = float(p['close_price'])
                    break
            if exit_price is None:
                exit_price = float(prices[-1]['close_price'])

            if entry <= 0:
                continue

            ret = (exit_price - entry) / entry * 100
            if ret < -90 or ret > 500:
                continue

            # Check each strategy
            for name, strat in strategies.items():
                if strat['filter'](profile):
                    results[name].append({
                        'ticker': ticker,
                        'score_date': score_date,
                        'forward_return': ret,
                        **profile
                    })

    # Print results
    print("\n" + "="*80)
    print("STRATEGY COMPARISON")
    print("="*80)

    summary = []
    for name, trades in results.items():
        if not trades:
            continue
        df = pd.DataFrame(trades)
        avg_ret = df['forward_return'].mean()
        med_ret = df['forward_return'].median()
        win_rate = (df['forward_return'] > 0).mean() * 100

        summary.append({
            'strategy': name,
            'desc': strategies[name]['desc'],
            'trades': len(df),
            'unique_cos': df['ticker'].nunique(),
            'avg_return': avg_ret,
            'median_return': med_ret,
            'win_rate': win_rate,
        })

        print(f"\n{name}: {strategies[name]['desc']}")
        print(f"  Trades: {len(df)}, Unique companies: {df['ticker'].nunique()}")
        print(f"  Avg return: {avg_ret:+.1f}%, Median: {med_ret:+.1f}%, Win rate: {win_rate:.0f}%")

        if len(df) > 0:
            print(f"  Top 3: ", end="")
            for _, row in df.nlargest(3, 'forward_return').iterrows():
                print(f"{row['ticker']} +{row['forward_return']:.0f}%  ", end="")
            print()

    # Summary table
    print("\n" + "="*80)
    print("SUMMARY TABLE (sorted by avg return)")
    print("="*80)

    summary_df = pd.DataFrame(summary).sort_values('avg_return', ascending=False)
    print(f"\n{'Strategy':<25} {'Trades':>8} {'Avg Ret':>10} {'Win Rate':>10}")
    print("-" * 60)
    for _, row in summary_df.iterrows():
        print(f"{row['strategy']:<25} {row['trades']:>8} {row['avg_return']:>+9.1f}% {row['win_rate']:>9.0f}%")

    return summary_df


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await backtest_turning_turnarounds(conn, holding_days=180)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
