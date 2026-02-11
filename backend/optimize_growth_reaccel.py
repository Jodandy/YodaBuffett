#!/usr/bin/env python3
"""
Optimize Growth Reacceleration Strategy

Test different parameter combinations to find the best version.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from uuid import UUID
from itertools import product

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def run_backtest(conn, params: dict, holding_days: int = 180) -> dict:
    """Run backtest with given parameters."""

    cutoff = date.today() - timedelta(days=holding_days + 30)
    dates = await conn.fetch("""
        SELECT DISTINCT score_date FROM daily_dimension_scores
        WHERE score_date <= $1 AND score_date >= '2022-01-01'
        ORDER BY score_date
    """, cutoff)
    score_dates = [r['score_date'] for r in dates]

    trades = []

    for score_date in score_dates:
        start_date = score_date - timedelta(days=60)

        companies = await conn.fetch("""
            SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
            FROM daily_dimension_scores dds
            JOIN company_master cm ON cm.id = dds.company_id
            WHERE dds.score_date = $1
        """, score_date)

        for comp in companies:
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
            quality = scores.get('quality', 0)
            momentum = scores.get('momentum', 0)
            profitability = scores.get('profitability', 0)

            # Get growth change
            hist = await conn.fetch("""
                SELECT score FROM daily_dimension_scores
                WHERE company_id = $1 AND dimension_code = 'growth'
                  AND score_date >= $2 AND score_date < $3
                ORDER BY score_date LIMIT 1
            """, company_uuid, start_date, score_date)

            growth_change = 0
            if hist and hist[0]['score']:
                growth_change = growth - float(hist[0]['score'])

            # Get momentum change
            hist_mom = await conn.fetch("""
                SELECT score FROM daily_dimension_scores
                WHERE company_id = $1 AND dimension_code = 'momentum'
                  AND score_date >= $2 AND score_date < $3
                ORDER BY score_date LIMIT 1
            """, company_uuid, start_date, score_date)

            momentum_change = 0
            if hist_mom and hist_mom[0]['score']:
                momentum_change = momentum - float(hist_mom[0]['score'])

            # Apply filters
            if growth < params['min_growth']:
                continue
            if growth_change < params['min_growth_change']:
                continue
            if val_pct < params['min_val_pct']:
                continue
            if params.get('min_quality') and quality < params['min_quality']:
                continue
            if params.get('min_momentum_change') and momentum_change < params['min_momentum_change']:
                continue
            if params.get('max_momentum') and momentum > params['max_momentum']:
                continue
            if params.get('min_profitability') and profitability < params['min_profitability']:
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

            trades.append({
                'ticker': ticker,
                'company': comp['company_name'],
                'date': score_date,
                'growth': growth,
                'growth_change': growth_change,
                'val_pct': val_pct,
                'quality': quality,
                'momentum': momentum,
                'momentum_change': momentum_change,
                'profitability': profitability,
                'return': ret,
            })

    if not trades:
        return {'params': params, 'trades': 0, 'avg_return': 0, 'win_rate': 0}

    df = pd.DataFrame(trades)

    return {
        'params': params,
        'trades': len(df),
        'unique_cos': df['ticker'].nunique(),
        'avg_return': df['return'].mean(),
        'median_return': df['return'].median(),
        'win_rate': (df['return'] > 0).mean() * 100,
        'best': df['return'].max(),
        'worst': df['return'].min(),
        'trades_df': df,
    }


async def optimize():
    """Test different parameter combinations."""

    conn = await asyncpg.connect(DATABASE_URL)

    # Parameter grid
    param_grid = [
        # Base variations
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50},  # Original
        {'min_growth': 40, 'min_growth_change': 10, 'min_val_pct': 50},  # Higher growth
        {'min_growth': 30, 'min_growth_change': 15, 'min_val_pct': 50},  # Higher change
        {'min_growth': 30, 'min_growth_change': 20, 'min_val_pct': 50},  # Much higher change
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 60},  # Cheaper
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 70},  # Much cheaper

        # Add quality filter
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'min_quality': 40},
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'min_quality': 50},
        {'min_growth': 30, 'min_growth_change': 15, 'min_val_pct': 50, 'min_quality': 40},

        # Add momentum filter (buy when momentum also turning)
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'min_momentum_change': 5},
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'min_momentum_change': 10},

        # Contrarian momentum (buy when momentum still low)
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'max_momentum': 40},
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'max_momentum': 50},

        # Add profitability filter
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'min_profitability': 30},
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 50, 'min_profitability': 40},

        # Combined filters
        {'min_growth': 30, 'min_growth_change': 15, 'min_val_pct': 60, 'min_quality': 40},
        {'min_growth': 40, 'min_growth_change': 15, 'min_val_pct': 60, 'min_quality': 40},
        {'min_growth': 30, 'min_growth_change': 10, 'min_val_pct': 60, 'min_quality': 40, 'min_momentum_change': 5},
    ]

    results = []
    best_result = None
    best_score = -999

    print("Testing parameter combinations...\n")

    for i, params in enumerate(param_grid):
        print(f"[{i+1}/{len(param_grid)}] {params}")
        result = await run_backtest(conn, params)
        results.append(result)

        # Score: prioritize avg return but penalize too few trades
        if result['trades'] >= 30:
            score = result['avg_return'] * (result['win_rate'] / 50)  # Bonus for higher win rate
            if score > best_score:
                best_score = score
                best_result = result

        print(f"    Trades: {result['trades']}, Avg: {result['avg_return']:+.1f}%, Win: {result['win_rate']:.0f}%")

    await conn.close()

    # Summary
    print("\n" + "="*100)
    print("OPTIMIZATION RESULTS (sorted by avg return, min 30 trades)")
    print("="*100)

    results_summary = []
    for r in results:
        if r['trades'] >= 30:
            results_summary.append({
                'params': str(r['params']),
                'trades': r['trades'],
                'avg_return': r['avg_return'],
                'median_return': r['median_return'],
                'win_rate': r['win_rate'],
            })

    summary_df = pd.DataFrame(results_summary).sort_values('avg_return', ascending=False)

    print(f"\n{'Params':<70} {'Trades':>7} {'Avg':>8} {'Med':>8} {'Win%':>6}")
    print("-"*105)
    for _, row in summary_df.iterrows():
        params_short = row['params'][:68]
        print(f"{params_short:<70} {row['trades']:>7} {row['avg_return']:>+7.1f}% {row['median_return']:>+7.1f}% {row['win_rate']:>5.0f}%")

    # Export best result
    if best_result and 'trades_df' in best_result:
        print(f"\n\nBEST STRATEGY: {best_result['params']}")
        print(f"Trades: {best_result['trades']}, Avg: {best_result['avg_return']:+.1f}%, Win: {best_result['win_rate']:.0f}%")

        df = best_result['trades_df'].sort_values('return', ascending=False)

        # Export to Excel
        output_file = 'growth_reaccel_optimized.xlsx'
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # All trades
            df.to_excel(writer, sheet_name='All Trades', index=False)

            # Summary by company
            company_summary = df.groupby('ticker').agg({
                'company': 'first',
                'return': ['mean', 'count', 'max', 'min'],
                'growth': 'mean',
                'quality': 'mean',
            }).reset_index()
            company_summary.columns = ['ticker', 'company', 'avg_return', 'trade_count', 'best_return', 'worst_return', 'avg_growth', 'avg_quality']
            company_summary = company_summary.sort_values('best_return', ascending=False)
            company_summary.to_excel(writer, sheet_name='By Company', index=False)

            # Winners only
            winners = df[df['return'] > 0].sort_values('return', ascending=False)
            winners.to_excel(writer, sheet_name='Winners', index=False)

            # Losers only
            losers = df[df['return'] <= 0].sort_values('return', ascending=True)
            losers.to_excel(writer, sheet_name='Losers', index=False)

            # Parameters
            params_df = pd.DataFrame([best_result['params']])
            params_df['trades'] = best_result['trades']
            params_df['avg_return'] = best_result['avg_return']
            params_df['win_rate'] = best_result['win_rate']
            params_df.to_excel(writer, sheet_name='Parameters', index=False)

            # Optimization summary
            summary_df.to_excel(writer, sheet_name='All Tested Params', index=False)

        print(f"\nExported to {output_file}")

        # Print top trades
        print("\nTOP 20 TRADES:")
        print(f"{'Ticker':<12} {'Company':<30} {'Date':<12} {'Return':>10}")
        print("-"*70)
        for _, row in df.head(20).iterrows():
            print(f"{row['ticker']:<12} {row['company'][:29]:<30} {str(row['date']):<12} {row['return']:>+9.1f}%")

    return results, best_result


if __name__ == "__main__":
    asyncio.run(optimize())
