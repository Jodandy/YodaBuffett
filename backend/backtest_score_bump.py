#!/usr/bin/env python3
"""
Score Bump Backtest with GARP Weighting

Strategy:
- BUY when Fat Pitch score increases by >= min_bump points from previous period
- Uses GARP weighting (growth at reasonable price)
- Analyzes by score level breakpoints

Usage:
    python backtest_score_bump.py                    # Default score bump analysis
    python backtest_score_bump.py --min-bump 5       # Require +5 point bump
    python backtest_score_bump.py --min-bump 10      # Require +10 point bump
    python backtest_score_bump.py --breakpoints      # Full breakpoint analysis
    python backtest_score_bump.py --export           # Export to Excel
"""

import asyncio
import asyncpg
from datetime import date, timedelta
from typing import Optional, Dict, List
from dataclasses import dataclass
from collections import defaultdict

# Database connection
DATABASE_URL = "postgresql://yodabuffett:password@localhost:5432/yodabuffett"

# GARP Weights (Peter Lynch style - growth at reasonable price)
# NOTE: Using valuation_percentile instead of value because value calculator
# was broken (used price at financial period date, not current price)
GARP_WEIGHTS = {
    'growth': 0.30,              # Strong growth
    'valuation_percentile': 0.25, # Current price vs historical (FIXED - was 'value')
    'profitability': 0.15,       # Profitable
    'earnings_quality': 0.10,    # Real earnings
    'returns': 0.10,             # Good returns on capital
    'quality': 0.05,             # Quality business
    'momentum': 0.05,            # Some positive momentum
    # Zero weights (not included in score)
    'financial_health': 0,
    'capital_allocation': 0,
    'working_capital': 0,
    'beneish_mscore': 0,
    'value': 0,                  # BROKEN - don't use
    'risk': 0,
    'sentiment': 0,
}


@dataclass
class Trade:
    """Represents a completed trade"""
    company_id: str
    symbol: str
    company_name: str
    entry_date: date
    entry_price: float
    entry_score: float
    prev_score: float
    score_bump: float
    exit_date: date
    exit_price: float
    exit_score: float
    hold_days: int
    return_pct: float

    @property
    def return_per_day(self) -> float:
        """Return percentage per day held"""
        return self.return_pct / self.hold_days if self.hold_days > 0 else 0.0


def build_garp_score_sql() -> tuple:
    """Build SQL CASE statements for GARP weighted score."""
    weight_cases = []
    weight_sum_cases = []

    for dim, weight in GARP_WEIGHTS.items():
        if weight > 0:
            weight_cases.append(f"WHEN dimension_code = '{dim}' THEN score * {weight}")
            weight_sum_cases.append(f"WHEN dimension_code = '{dim}' THEN {weight}")

    weight_sql = " ".join(weight_cases)
    weight_sum_sql = " ".join(weight_sum_cases)

    return weight_sql, weight_sum_sql


async def get_historical_scores(conn: asyncpg.Connection) -> dict:
    """
    Get all historical fat pitch scores with company info using GARP weights.
    Returns dict: company_id -> [(score_date, score, prev_score, bump, symbol, name), ...]
    """
    weight_sql, weight_sum_sql = build_garp_score_sql()

    query = f"""
    WITH company_scores AS (
        SELECT
            dds.company_id,
            dds.score_date,
            cm.primary_ticker as symbol,
            cm.company_name,
            -- Calculate weighted fat pitch score using GARP weights with proper normalization
            SUM(CASE {weight_sql} ELSE 0 END) /
            NULLIF(SUM(CASE {weight_sum_sql} ELSE 0 END), 0) as fat_pitch_score
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE cm.primary_ticker IS NOT NULL
        GROUP BY dds.company_id, dds.score_date, cm.primary_ticker, cm.company_name
        HAVING COUNT(DISTINCT dimension_code) >= 5  -- Need enough dimensions
    ),
    with_prev AS (
        SELECT
            *,
            LAG(fat_pitch_score) OVER (PARTITION BY company_id ORDER BY score_date) as prev_score,
            fat_pitch_score - LAG(fat_pitch_score) OVER (PARTITION BY company_id ORDER BY score_date) as score_bump
        FROM company_scores
    )
    SELECT company_id, score_date, symbol, company_name, fat_pitch_score, prev_score, score_bump
    FROM with_prev
    WHERE fat_pitch_score > 0 AND prev_score IS NOT NULL
    ORDER BY company_id, score_date
    """

    rows = await conn.fetch(query)

    scores_by_company = defaultdict(list)
    for row in rows:
        scores_by_company[row['company_id']].append({
            'date': row['score_date'],
            'score': float(row['fat_pitch_score']),
            'prev_score': float(row['prev_score']) if row['prev_score'] else None,
            'bump': float(row['score_bump']) if row['score_bump'] else 0,
            'symbol': row['symbol'],
            'name': row['company_name']
        })

    return scores_by_company


async def get_price_on_date(conn: asyncpg.Connection, symbol: str, target_date: date) -> Optional[float]:
    """Get the closing price on or shortly after a given date"""
    query = """
    SELECT close_price, date
    FROM daily_price_data
    WHERE symbol = $1 AND date >= $2
    ORDER BY date ASC
    LIMIT 1
    """
    row = await conn.fetchrow(query, symbol, target_date)
    if row:
        return float(row['close_price'])
    return None


async def run_score_bump_backtest(
    min_bump: float = 5.0,
    hold_days: int = 63,  # 3 months default
    min_score: float = 0,  # Minimum score to consider
    contrarian: bool = False,  # Buy on score DROPS instead of bumps
):
    """Run backtest on score bump signals with GARP weights"""

    mode = "CONTRARIAN (buy on drops)" if contrarian else "MOMENTUM (buy on bumps)"

    print(f"\n{'='*60}")
    print(f"SCORE BUMP BACKTEST - GARP WEIGHTED - {mode}")
    print(f"{'='*60}")
    print(f"Min score change: >= {min_bump} points")
    print(f"Direction: {'DROPS (contrarian)' if contrarian else 'BUMPS (momentum)'}")
    print(f"Min entry score: >= {min_score}")
    print(f"Hold period: {hold_days} days (~{hold_days//21} months)")
    print(f"{'='*60}")
    print("\nGARP Weights:")
    for dim, weight in sorted(GARP_WEIGHTS.items(), key=lambda x: -x[1]):
        if weight > 0:
            print(f"  {dim}: {weight*100:.0f}%")
    print(f"{'='*60}\n")

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Get all historical scores
        print("Loading historical scores with GARP weighting...")
        scores_by_company = await get_historical_scores(conn)
        print(f"Found {len(scores_by_company)} companies with score history\n")

        # Track trades
        completed_trades: list[Trade] = []
        signals_found = 0

        for company_id, score_history in scores_by_company.items():
            if len(score_history) < 2:
                continue

            symbol = score_history[0]['symbol']
            name = score_history[0]['name']

            for i, data in enumerate(score_history):
                # Check for score bump signal
                bump = data.get('bump', 0)
                current_score = data['score']
                prev_score = data.get('prev_score', 0) or 0
                current_date = data['date']

                # For contrarian mode, we want negative bumps (drops)
                # For regular mode, we want positive bumps
                if abs(bump) >= min_bump and current_score >= min_score:
                    # Skip if bump direction doesn't match mode
                    if not contrarian and bump < 0:
                        continue
                    if contrarian and bump > 0:
                        continue
                    signals_found += 1

                    # Get entry price
                    entry_price = await get_price_on_date(conn, symbol, current_date)
                    if not entry_price:
                        continue

                    # Get exit price (after hold period)
                    exit_date = current_date + timedelta(days=hold_days)
                    exit_price = await get_price_on_date(conn, symbol, exit_date)
                    if not exit_price:
                        continue

                    # Calculate return
                    return_pct = ((exit_price - entry_price) / entry_price) * 100

                    # Filter outliers (likely splits)
                    if return_pct > 200 or return_pct < -90:
                        continue

                    # Find exit score (closest to exit date)
                    exit_score = current_score  # Default
                    for future_data in score_history[i:]:
                        if future_data['date'] >= exit_date:
                            exit_score = future_data['score']
                            break

                    trade = Trade(
                        company_id=company_id,
                        symbol=symbol,
                        company_name=name,
                        entry_date=current_date,
                        entry_price=entry_price,
                        entry_score=current_score,
                        prev_score=prev_score,
                        score_bump=bump,
                        exit_date=exit_date,
                        exit_price=exit_price,
                        exit_score=exit_score,
                        hold_days=hold_days,
                        return_pct=return_pct
                    )
                    completed_trades.append(trade)

        # Print results
        print(f"\n{'='*60}")
        print("BACKTEST RESULTS")
        print(f"{'='*60}\n")

        print(f"Signals found: {signals_found}")
        print(f"Trades completed: {len(completed_trades)}")

        if completed_trades:
            returns = [t.return_pct for t in completed_trades]
            avg_return = sum(returns) / len(returns)
            median_return = sorted(returns)[len(returns)//2]
            win_rate = len([r for r in returns if r > 0]) / len(returns) * 100
            avg_bump = sum(t.score_bump for t in completed_trades) / len(completed_trades)
            avg_entry_score = sum(t.entry_score for t in completed_trades) / len(completed_trades)

            print(f"\nWin Rate: {win_rate:.1f}%")
            print(f"Average Return: {avg_return:.2f}%")
            print(f"Median Return: {median_return:.2f}%")
            print(f"Total Return (sum): {sum(returns):.2f}%")
            print(f"Best Trade: {max(returns):.2f}%")
            print(f"Worst Trade: {min(returns):.2f}%")
            print(f"\nAvg Score Bump: {avg_bump:.1f}")
            print(f"Avg Entry Score: {avg_entry_score:.1f}")

            # Top trades
            completed_trades.sort(key=lambda t: t.return_pct, reverse=True)

            print(f"\n{'='*60}")
            print("TOP 10 TRADES")
            print(f"{'='*60}")
            print(f"{'Symbol':<12} {'Entry':<12} {'Score':<8} {'Bump':<8} {'Return':<10}")
            print("-" * 60)
            for trade in completed_trades[:10]:
                print(f"{trade.symbol:<12} {str(trade.entry_date):<12} "
                      f"{trade.entry_score:>5.1f}   {trade.score_bump:>+5.1f}   {trade.return_pct:>+7.2f}%")

            # Worst trades
            print(f"\n{'='*60}")
            print("WORST 10 TRADES")
            print(f"{'='*60}")
            print(f"{'Symbol':<12} {'Entry':<12} {'Score':<8} {'Bump':<8} {'Return':<10}")
            print("-" * 60)
            for trade in completed_trades[-10:]:
                print(f"{trade.symbol:<12} {str(trade.entry_date):<12} "
                      f"{trade.entry_score:>5.1f}   {trade.score_bump:>+5.1f}   {trade.return_pct:>+7.2f}%")

            return completed_trades
        else:
            print("No completed trades found.")
            return []

    finally:
        await conn.close()


async def run_breakpoint_analysis():
    """Analyze returns by Fat Pitch score level and bump size"""

    print(f"\n{'='*80}")
    print("FAT PITCH SCORE BREAKPOINT ANALYSIS - GARP WEIGHTED")
    print(f"{'='*80}\n")

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Get all historical scores
        print("Loading historical scores...")
        scores_by_company = await get_historical_scores(conn)
        print(f"Found {len(scores_by_company)} companies\n")

        # Collect all trades with various bumps
        all_trades = []

        for company_id, score_history in scores_by_company.items():
            symbol = score_history[0]['symbol'] if score_history else None
            name = score_history[0]['name'] if score_history else None

            for i, data in enumerate(score_history):
                bump = data.get('bump', 0)
                if bump <= 0:  # Only positive bumps
                    continue

                current_score = data['score']
                current_date = data['date']

                # Get entry price
                entry_price = await get_price_on_date(conn, symbol, current_date)
                if not entry_price:
                    continue

                # Get exit prices at different horizons
                for horizon_days, horizon_name in [(63, '3M'), (126, '6M'), (252, '12M')]:
                    exit_date = current_date + timedelta(days=horizon_days)
                    exit_price = await get_price_on_date(conn, symbol, exit_date)
                    if not exit_price:
                        continue

                    return_pct = ((exit_price - entry_price) / entry_price) * 100

                    # Filter outliers
                    if return_pct > 200 or return_pct < -90:
                        continue

                    all_trades.append({
                        'symbol': symbol,
                        'entry_date': current_date,
                        'entry_score': current_score,
                        'score_bump': bump,
                        'horizon': horizon_name,
                        'return_pct': return_pct,
                    })

        print(f"Collected {len(all_trades)} trade observations\n")

        # Analysis 1: By Score Level
        print(f"{'='*80}")
        print("RETURNS BY ENTRY SCORE LEVEL")
        print(f"{'='*80}\n")

        score_levels = [(0, 40), (40, 50), (50, 60), (60, 70), (70, 80), (80, 100)]

        print(f"{'Score Range':<15} {'3M Avg':<12} {'3M Win%':<10} {'6M Avg':<12} {'6M Win%':<10} {'12M Avg':<12} {'12M Win%':<10} {'N':<8}")
        print("-" * 95)

        for low, high in score_levels:
            for horizon in ['3M', '6M', '12M']:
                trades = [t for t in all_trades
                         if t['entry_score'] >= low
                         and t['entry_score'] < high
                         and t['horizon'] == horizon]
                if trades:
                    returns = [t['return_pct'] for t in trades]
                    avg = sum(returns) / len(returns)
                    win_rate = len([r for r in returns if r > 0]) / len(returns) * 100
                    if horizon == '3M':
                        print(f"{low}-{high:<11}", end="")
                    print(f" {avg:>+8.1f}%   {win_rate:>5.1f}%  ", end="")
                    if horizon == '12M':
                        print(f"  {len(trades)}")
                else:
                    if horizon == '3M':
                        print(f"{low}-{high:<11}", end="")
                    print(f" {'N/A':>10} {'N/A':>10}", end="")
                    if horizon == '12M':
                        print(f"  0")

        # Analysis 2: By Bump Size
        print(f"\n{'='*80}")
        print("RETURNS BY SCORE BUMP SIZE")
        print(f"{'='*80}\n")

        bump_levels = [(0, 3), (3, 5), (5, 10), (10, 15), (15, 20), (20, 100)]

        print(f"{'Bump Range':<15} {'3M Avg':<12} {'3M Win%':<10} {'6M Avg':<12} {'6M Win%':<10} {'12M Avg':<12} {'12M Win%':<10} {'N':<8}")
        print("-" * 95)

        for low, high in bump_levels:
            for horizon in ['3M', '6M', '12M']:
                trades = [t for t in all_trades
                         if t['score_bump'] >= low
                         and t['score_bump'] < high
                         and t['horizon'] == horizon]
                if trades:
                    returns = [t['return_pct'] for t in trades]
                    avg = sum(returns) / len(returns)
                    win_rate = len([r for r in returns if r > 0]) / len(returns) * 100
                    if horizon == '3M':
                        print(f"+{low}-{high:<11}", end="")
                    print(f" {avg:>+8.1f}%   {win_rate:>5.1f}%  ", end="")
                    if horizon == '12M':
                        print(f"  {len(trades)}")
                else:
                    if horizon == '3M':
                        print(f"+{low}-{high:<11}", end="")
                    print(f" {'N/A':>10} {'N/A':>10}", end="")
                    if horizon == '12M':
                        print(f"  0")

        # Analysis 3: Combination (High Score + Big Bump)
        print(f"\n{'='*80}")
        print("COMBINED: HIGH SCORE (>60) + BIG BUMP (>5)")
        print(f"{'='*80}\n")

        combos = [
            ("Score 60+ Bump 5+", 60, 100, 5, 100),
            ("Score 70+ Bump 5+", 70, 100, 5, 100),
            ("Score 60+ Bump 10+", 60, 100, 10, 100),
            ("Score 70+ Bump 10+", 70, 100, 10, 100),
            ("Score 80+ Bump 5+", 80, 100, 5, 100),
        ]

        print(f"{'Criteria':<25} {'3M Avg':<12} {'3M Win%':<10} {'6M Avg':<12} {'6M Win%':<10} {'12M Avg':<12} {'12M Win%':<10} {'N':<8}")
        print("-" * 110)

        for label, score_low, score_high, bump_low, bump_high in combos:
            for horizon in ['3M', '6M', '12M']:
                trades = [t for t in all_trades
                         if t['entry_score'] >= score_low
                         and t['entry_score'] < score_high
                         and t['score_bump'] >= bump_low
                         and t['score_bump'] < bump_high
                         and t['horizon'] == horizon]
                if trades:
                    returns = [t['return_pct'] for t in trades]
                    avg = sum(returns) / len(returns)
                    win_rate = len([r for r in returns if r > 0]) / len(returns) * 100
                    if horizon == '3M':
                        print(f"{label:<25}", end="")
                    print(f" {avg:>+8.1f}%   {win_rate:>5.1f}%  ", end="")
                    if horizon == '12M':
                        print(f"  {len(trades)}")
                else:
                    if horizon == '3M':
                        print(f"{label:<25}", end="")
                    print(f" {'N/A':>10} {'N/A':>10}", end="")
                    if horizon == '12M':
                        print(f"  0")

        # Summary
        print(f"\n{'='*80}")
        print("KEY INSIGHTS")
        print(f"{'='*80}\n")

        # Find best combination
        best_combo = None
        best_avg = -999

        for label, score_low, score_high, bump_low, bump_high in combos:
            trades = [t for t in all_trades
                     if t['entry_score'] >= score_low
                     and t['entry_score'] < score_high
                     and t['score_bump'] >= bump_low
                     and t['score_bump'] < bump_high
                     and t['horizon'] == '12M']
            if len(trades) >= 10:
                returns = [t['return_pct'] for t in trades]
                avg = sum(returns) / len(returns)
                if avg > best_avg:
                    best_avg = avg
                    best_combo = (label, avg, len(trades))

        if best_combo:
            print(f"Best 12M return (min 10 trades): {best_combo[0]}")
            print(f"  Average return: {best_combo[1]:+.1f}%")
            print(f"  Number of trades: {best_combo[2]}")

        return all_trades

    finally:
        await conn.close()


async def export_to_excel(trades: List[Dict], filename: str = "score_bump_backtest.xlsx"):
    """Export backtest results to Excel"""
    try:
        import pandas as pd
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
    except ImportError:
        print("Please install pandas and openpyxl: pip install pandas openpyxl")
        return

    df = pd.DataFrame(trades)

    # Create summary by score level
    score_summary = df.groupby([pd.cut(df['entry_score'], bins=[0, 40, 50, 60, 70, 80, 100]), 'horizon']).agg({
        'return_pct': ['mean', 'median', 'count', lambda x: (x > 0).mean() * 100]
    }).reset_index()
    score_summary.columns = ['Score Range', 'Horizon', 'Avg Return', 'Median Return', 'Count', 'Win Rate']

    # Create summary by bump size
    bump_summary = df.groupby([pd.cut(df['score_bump'], bins=[0, 3, 5, 10, 15, 20, 100]), 'horizon']).agg({
        'return_pct': ['mean', 'median', 'count', lambda x: (x > 0).mean() * 100]
    }).reset_index()
    bump_summary.columns = ['Bump Range', 'Horizon', 'Avg Return', 'Median Return', 'Count', 'Win Rate']

    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='All Trades', index=False)
        score_summary.to_excel(writer, sheet_name='By Score Level', index=False)
        bump_summary.to_excel(writer, sheet_name='By Bump Size', index=False)

    print(f"\n✅ Exported to {filename}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Score Bump Backtest with GARP Weighting")
    parser.add_argument("--min-bump", type=float, default=5, help="Minimum score change to trigger entry")
    parser.add_argument("--min-score", type=float, default=0, help="Minimum score level to consider")
    parser.add_argument("--hold-days", type=int, default=63, help="Hold period in days")
    parser.add_argument("--contrarian", action="store_true", help="Buy on score DROPS instead of bumps")
    parser.add_argument("--breakpoints", action="store_true", help="Run full breakpoint analysis")
    parser.add_argument("--export", action="store_true", help="Export results to Excel")
    parser.add_argument("--output", type=str, default="score_bump_backtest.xlsx", help="Excel output filename")

    args = parser.parse_args()

    if args.breakpoints:
        trades = asyncio.run(run_breakpoint_analysis())
        if args.export and trades:
            asyncio.run(export_to_excel(trades, args.output))
    else:
        trades = asyncio.run(run_score_bump_backtest(
            min_bump=args.min_bump,
            hold_days=args.hold_days,
            min_score=args.min_score,
            contrarian=args.contrarian
        ))
