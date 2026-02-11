#!/usr/bin/env python3
"""
Find Growth Reacceleration Candidates

The winning strategy: Growth accelerating from low base
- Growth score >= 30
- Growth change >= 10 (accelerating!)
- Valuation percentile >= 50 (not expensive)
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from uuid import UUID

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_current_candidates(conn, lookback_days: int = 60):
    """Find current growth reacceleration candidates."""

    # Get latest score date with good coverage
    score_date = await conn.fetchval("""
        SELECT score_date FROM daily_dimension_scores
        GROUP BY score_date
        HAVING COUNT(DISTINCT company_id) > 1000
        ORDER BY score_date DESC
        LIMIT 1
    """)

    print(f"Looking for Growth Reacceleration as of {score_date}")
    print(f"Criteria: growth >= 30, growth_change >= 10, valuation_pct >= 50\n")

    start_date = score_date - timedelta(days=lookback_days)

    # Get all companies with current scores
    companies = await conn.fetch("""
        SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1
    """, score_date)

    candidates = []

    for i, comp in enumerate(companies):
        if i % 200 == 0:
            print(f"Processing {i}/{len(companies)}...")

        company_uuid = UUID(comp['company_id'])

        # Get current scores
        current = await conn.fetch("""
            SELECT dimension_code, score
            FROM daily_dimension_scores
            WHERE company_id = $1 AND score_date = $2
        """, company_uuid, score_date)

        scores = {r['dimension_code']: float(r['score']) if r['score'] else 0 for r in current}

        growth = scores.get('growth', 0)
        val_pct = scores.get('valuation_percentile', 0)

        # Get growth change
        hist = await conn.fetch("""
            SELECT score, score_date
            FROM daily_dimension_scores
            WHERE company_id = $1
              AND dimension_code = 'growth'
              AND score_date >= $2 AND score_date < $3
            ORDER BY score_date
            LIMIT 1
        """, company_uuid, start_date, score_date)

        growth_change = 0
        if hist and hist[0]['score']:
            growth_change = growth - float(hist[0]['score'])

        # Check criteria
        if growth >= 30 and growth_change >= 10 and val_pct >= 50:
            candidates.append({
                'ticker': comp['primary_ticker'],
                'company': comp['company_name'],
                'growth': growth,
                'growth_change': growth_change,
                'valuation_pct': val_pct,
                'momentum': scores.get('momentum', 0),
                'quality': scores.get('quality', 0),
                'profitability': scores.get('profitability', 0),
                'value': scores.get('value', 0),
            })

    df = pd.DataFrame(candidates)

    if df.empty:
        print("No candidates found!")
        return df

    df = df.sort_values('growth_change', ascending=False)

    print(f"\nFound {len(df)} candidates:\n")
    print(f"{'Ticker':<12} {'Company':<30} {'Growth':>8} {'Δ Growth':>10} {'Val%':>8} {'Mom':>6} {'Qual':>6}")
    print("-" * 90)

    for _, row in df.iterrows():
        print(f"{row['ticker']:<12} {row['company'][:29]:<30} {row['growth']:>8.0f} "
              f"{row['growth_change']:>+9.0f} {row['valuation_pct']:>8.0f} "
              f"{row['momentum']:>6.0f} {row['quality']:>6.0f}")

    # Save to Excel
    df.to_excel('growth_reacceleration_candidates.xlsx', index=False)
    print(f"\nSaved to growth_reacceleration_candidates.xlsx")

    return df


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await get_current_candidates(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
