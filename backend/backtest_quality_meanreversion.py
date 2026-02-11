#!/usr/bin/env python3
"""
Quality + Mean Reversion Backtest

Strategy: Buy quality companies when they're cheap relative to their own history.

1. Filter for quality stocks (top N% on quality dimensions)
2. Among those, buy the ones with LOW valuation_percentile (cheap vs own 5-year history)
3. Measure forward returns

This avoids value traps (quality filter) while capturing mean reversion (buy the dip).
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
import argparse

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Quality dimensions to use as filters
QUALITY_DIMENSIONS = [
    'profitability',
    'returns',
    'earnings_quality',
    'financial_health',
]

# Alternative: use composite quality score
QUALITY_COMPOSITE_WEIGHTS = {
    'profitability': 0.30,
    'returns': 0.25,
    'earnings_quality': 0.25,
    'financial_health': 0.20,
}


async def get_scores_for_date(conn, score_date: date) -> pd.DataFrame:
    """Get all dimension scores for all companies on a date."""

    rows = await conn.fetch("""
        SELECT
            dds.company_id::text,
            cm.primary_ticker,
            cm.company_name,
            dds.dimension_code,
            dds.score
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1
        AND dds.score IS NOT NULL
    """, score_date)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])

    # Pivot to get one row per company with dimension columns
    pivot = df.pivot_table(
        index=['company_id', 'primary_ticker', 'company_name'],
        columns='dimension_code',
        values='score'
    ).reset_index()

    return pivot


async def get_forward_returns(conn, tickers: List[str], start_date: date,
                              holding_days: int) -> Dict[str, float]:
    """Get forward returns for tickers."""

    end_date = start_date + timedelta(days=holding_days + 10)

    rows = await conn.fetch("""
        SELECT symbol, date, close_price
        FROM daily_price_data
        WHERE symbol = ANY($1)
        AND date >= $2
        AND date <= $3
        ORDER BY symbol, date
    """, tickers, start_date, end_date)

    # Group by symbol
    by_symbol = defaultdict(list)
    for row in rows:
        by_symbol[row['symbol']].append(row)

    returns = {}
    target_date = start_date + timedelta(days=holding_days)

    for symbol, prices in by_symbol.items():
        if len(prices) >= 2:
            entry_price = prices[0]['close_price']

            # Find exit price
            exit_price = None
            for p in prices:
                if p['date'] >= target_date:
                    exit_price = p['close_price']
                    break

            if exit_price is None:
                exit_price = prices[-1]['close_price']

            if entry_price and exit_price and entry_price > 0:
                returns[symbol] = float((exit_price - entry_price) / entry_price * 100)

    return returns


async def get_score_dates(conn) -> List[date]:
    """Get available score dates."""
    rows = await conn.fetch("""
        SELECT DISTINCT score_date
        FROM daily_dimension_scores
        WHERE score_date <= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY score_date
    """)
    return [row['score_date'] for row in rows]


def calculate_quality_score(row: pd.Series, weights: Dict[str, float]) -> Optional[float]:
    """Calculate composite quality score."""
    total_weight = 0
    weighted_sum = 0

    for dim, weight in weights.items():
        if dim in row and pd.notna(row[dim]):
            weighted_sum += row[dim] * weight
            total_weight += weight

    if total_weight < 0.5:  # Need at least half the dimensions
        return None

    return weighted_sum / total_weight


async def run_backtest(
    conn,
    holding_days: int = 63,
    quality_threshold: float = 60,  # Min quality score to pass filter
    valuation_threshold: float = 30,  # Max valuation percentile (cheap)
    min_companies: int = 5,
) -> pd.DataFrame:
    """Run the quality + mean reversion backtest."""

    score_dates = await get_score_dates(conn)
    print(f"Testing {len(score_dates)} dates")

    all_trades = []

    for score_date in score_dates:
        # Get scores
        df = await get_scores_for_date(conn, score_date)

        if df.empty or 'valuation_percentile' not in df.columns:
            continue

        # Calculate composite quality score
        df['quality_composite'] = df.apply(
            lambda row: calculate_quality_score(row, QUALITY_COMPOSITE_WEIGHTS),
            axis=1
        )

        # Filter for quality
        quality_stocks = df[df['quality_composite'] >= quality_threshold].copy()

        if len(quality_stocks) < min_companies:
            continue

        # Among quality stocks, find cheap ones (low valuation_percentile)
        cheap_quality = quality_stocks[
            quality_stocks['valuation_percentile'] <= valuation_threshold
        ].copy()

        # Also get expensive quality for comparison
        expensive_quality = quality_stocks[
            quality_stocks['valuation_percentile'] >= (100 - valuation_threshold)
        ].copy()

        # Get all quality stocks for baseline
        all_quality_tickers = quality_stocks['primary_ticker'].tolist()

        # Get forward returns
        returns = await get_forward_returns(conn, all_quality_tickers, score_date, holding_days)

        # Record trades
        for _, row in cheap_quality.iterrows():
            ticker = row['primary_ticker']
            if ticker in returns:
                all_trades.append({
                    'date': score_date,
                    'ticker': ticker,
                    'company': row['company_name'],
                    'strategy': 'cheap_quality',
                    'quality_score': row['quality_composite'],
                    'valuation_pct': row['valuation_percentile'],
                    'forward_return': returns[ticker],
                })

        for _, row in expensive_quality.iterrows():
            ticker = row['primary_ticker']
            if ticker in returns:
                all_trades.append({
                    'date': score_date,
                    'ticker': ticker,
                    'company': row['company_name'],
                    'strategy': 'expensive_quality',
                    'quality_score': row['quality_composite'],
                    'valuation_pct': row['valuation_percentile'],
                    'forward_return': returns[ticker],
                })

        # All quality baseline
        for _, row in quality_stocks.iterrows():
            ticker = row['primary_ticker']
            if ticker in returns:
                all_trades.append({
                    'date': score_date,
                    'ticker': ticker,
                    'company': row['company_name'],
                    'strategy': 'all_quality',
                    'quality_score': row['quality_composite'],
                    'valuation_pct': row['valuation_percentile'],
                    'forward_return': returns[ticker],
                })

    return pd.DataFrame(all_trades)


async def main():
    parser = argparse.ArgumentParser(description='Quality + Mean Reversion Backtest')
    parser.add_argument('--holding-days', type=int, default=63,
                        help='Holding period in days (default: 63 = 3 months)')
    parser.add_argument('--quality-threshold', type=float, default=60,
                        help='Minimum quality score (default: 60)')
    parser.add_argument('--valuation-threshold', type=float, default=30,
                        help='Max valuation percentile for "cheap" (default: 30)')
    parser.add_argument('--output', default='quality_meanreversion.xlsx',
                        help='Output Excel file')
    args = parser.parse_args()

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print(f"\nQuality + Mean Reversion Backtest")
        print(f"="*60)
        print(f"Holding period: {args.holding_days} days")
        print(f"Quality threshold: >= {args.quality_threshold}")
        print(f"Valuation threshold: <= {args.valuation_threshold} (cheap)")
        print(f"="*60)

        df = await run_backtest(
            conn,
            holding_days=args.holding_days,
            quality_threshold=args.quality_threshold,
            valuation_threshold=args.valuation_threshold,
        )

        if df.empty:
            print("No trades generated!")
            return

        # Summary stats by strategy
        print(f"\n{'Strategy':<20} {'Trades':>8} {'Avg Ret':>10} {'Median':>10} {'Win Rate':>10} {'Std':>10}")
        print("-"*70)

        summary_data = []
        for strategy in ['cheap_quality', 'expensive_quality', 'all_quality']:
            strat_df = df[df['strategy'] == strategy]
            if len(strat_df) > 0:
                avg_ret = strat_df['forward_return'].mean()
                median_ret = strat_df['forward_return'].median()
                win_rate = (strat_df['forward_return'] > 0).mean() * 100
                std_ret = strat_df['forward_return'].std()

                print(f"{strategy:<20} {len(strat_df):>8} {avg_ret:>+9.2f}% {median_ret:>+9.2f}% {win_rate:>9.1f}% {std_ret:>9.2f}%")

                summary_data.append({
                    'strategy': strategy,
                    'trades': len(strat_df),
                    'avg_return': avg_ret,
                    'median_return': median_ret,
                    'win_rate': win_rate,
                    'std': std_ret,
                })

        print("-"*70)

        # Calculate alpha
        cheap = df[df['strategy'] == 'cheap_quality']['forward_return'].mean()
        expensive = df[df['strategy'] == 'expensive_quality']['forward_return'].mean()
        baseline = df[df['strategy'] == 'all_quality']['forward_return'].mean()

        print(f"\nAlpha Analysis:")
        print(f"  Cheap Quality vs All Quality:     {cheap - baseline:+.2f}%")
        print(f"  Cheap Quality vs Expensive:       {cheap - expensive:+.2f}%")
        print(f"  Mean Reversion Spread:            {cheap - expensive:+.2f}%")

        # Time series analysis
        print(f"\n\nMonthly Breakdown (Cheap Quality - Expensive Quality spread):")
        print("-"*60)

        cheap_by_date = df[df['strategy'] == 'cheap_quality'].groupby('date')['forward_return'].mean()
        expensive_by_date = df[df['strategy'] == 'expensive_quality'].groupby('date')['forward_return'].mean()

        # Align dates
        common_dates = cheap_by_date.index.intersection(expensive_by_date.index)

        positive_months = 0
        negative_months = 0

        for d in sorted(common_dates):
            spread = cheap_by_date[d] - expensive_by_date[d]
            bar = '+' * min(15, int(abs(spread) / 2)) if spread > 0 else '-' * min(15, int(abs(spread) / 2))
            print(f"{d}: {spread:+6.1f}% {bar}")
            if spread > 0:
                positive_months += 1
            else:
                negative_months += 1

        if positive_months + negative_months > 0:
            print(f"\nMonthly win rate: {positive_months}/{positive_months+negative_months} = {positive_months/(positive_months+negative_months)*100:.0f}%")

        # Export to Excel
        print(f"\nExporting to {args.output}...")

        with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
            pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

            # Trades by strategy
            for strategy in ['cheap_quality', 'expensive_quality', 'all_quality']:
                strat_df = df[df['strategy'] == strategy].copy()
                if not strat_df.empty:
                    strat_df.to_excel(writer, sheet_name=strategy[:31], index=False)

            # Top performers
            top = df[df['strategy'] == 'cheap_quality'].nlargest(50, 'forward_return')
            top.to_excel(writer, sheet_name='Top Cheap Quality', index=False)

            # Worst performers (value traps that got through)
            worst = df[df['strategy'] == 'cheap_quality'].nsmallest(50, 'forward_return')
            worst.to_excel(writer, sheet_name='Worst Cheap Quality', index=False)

        print(f"Done! Results saved to {args.output}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
