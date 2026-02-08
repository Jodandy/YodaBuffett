#!/usr/bin/env python3
"""
Fat Pitch Strategy Backtest

For each quarter:
1. Score all companies using weighted dimensions
2. Select top 20
3. Track their actual forward returns (3M, 6M, 12M)
4. Compare to market average

Usage:
    python fat_pitch_backtest.py                    # Full backtest
    python fat_pitch_backtest.py --top 10           # Top 10 instead of 20
    python fat_pitch_backtest.py --weights ml       # Use ML-optimized weights
    python fat_pitch_backtest.py --weights equal    # Equal weights baseline
    python fat_pitch_backtest.py --export           # Export full data to Excel
    python fat_pitch_backtest.py --export --weights equal --top 50  # Custom export
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Tuple
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Weight profiles
WEIGHT_PROFILES = {
    # ML-optimized weights based on 12M quintile analysis (2026-02-08)
    # Derived from top 20% vs bottom 20% return differences
    'ml': {
        'capital_allocation': 15,   # +14.3 diff - strongest predictor
        'beneish_mscore': 14,       # +13.6 diff - no manipulation
        'quality': 11,              # +11.0 diff
        'profitability': 11,        # +10.9 diff
        'returns': 10,              # +9.9 diff - ROE/ROIC matters
        'earnings_quality': 7,      # +6.8 diff
        'growth': 4,                # +4.2 diff
        'working_capital': 4,       # +4.2 diff
        'value': 3,                 # +2.6 diff - surprisingly weak
        'risk': 0,                  # +3.4 but counterintuitive
        'momentum': 0,              # contrarian signal - ignore or negative
        'valuation_percentile': 0,  # +1.5 diff - noise
        'sentiment': 0,             # -0.7 diff - no signal
        'financial_health': 0,      # -0.5 diff - doesn't predict
    },
    # Original Fat Pitch weights (for comparison)
    'original': {
        'profitability': 10,
        'returns': 10,
        'growth': 10,
        'financial_health': 10,
        'earnings_quality': 10,
        'capital_allocation': 10,
        'working_capital': 5,
        'beneish_mscore': 5,
        'value': 10,
        'risk': 5,
        'momentum': 5,
        'quality': 10,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Equal weights baseline
    'equal': {
        'profitability': 1,
        'returns': 1,
        'growth': 1,
        'financial_health': 1,
        'earnings_quality': 1,
        'capital_allocation': 1,
        'working_capital': 1,
        'beneish_mscore': 1,
        'value': 1,
        'risk': 1,
        'momentum': 1,
        'quality': 1,
        'valuation_percentile': 1,
        'sentiment': 1,
    },
    # Value-focused
    'value': {
        'value': 25,
        'valuation_percentile': 20,
        'earnings_quality': 15,
        'profitability': 15,
        'financial_health': 10,
        'beneish_mscore': 10,
        'quality': 5,
        'returns': 0,
        'growth': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'risk': 0,
        'momentum': 0,
        'sentiment': 0,
    },
    # Quality-focused
    'quality': {
        'quality': 20,
        'profitability': 15,
        'returns': 15,
        'earnings_quality': 15,
        'capital_allocation': 10,
        'beneish_mscore': 10,
        'financial_health': 10,
        'growth': 5,
        'working_capital': 0,
        'value': 0,
        'risk': 0,
        'momentum': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Contrarian - negative momentum (mean reversion bet)
    'contrarian': {
        'capital_allocation': 15,
        'beneish_mscore': 14,
        'quality': 11,
        'profitability': 11,
        'returns': 10,
        'earnings_quality': 7,
        'growth': 4,
        'working_capital': 4,
        'value': 3,
        'momentum': -10,            # NEGATIVE - bet against recent winners
        'risk': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
        'financial_health': 0,
    },
    # Deep contrarian - all signals reversed
    'anti': {
        'capital_allocation': -15,
        'beneish_mscore': -14,
        'quality': -11,
        'profitability': -11,
        'returns': -10,
        'earnings_quality': -7,
        'growth': -4,
        'working_capital': -4,
        'value': -3,
        'momentum': 0,
        'risk': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
        'financial_health': 0,
    },
}

DIMENSIONS = list(WEIGHT_PROFILES['ml'].keys())


class FatPitchBacktester:
    """Backtest Fat Pitch strategy across quarters."""

    def __init__(self, top_n: int = 20, weight_profile: str = 'ml', lag_days: int = 0, select_bottom: bool = False):
        self.top_n = top_n
        self.weight_profile = weight_profile
        self.weights = WEIGHT_PROFILES[weight_profile]
        self.lag_days = lag_days  # Conservative lag to avoid look-ahead bias
        self.select_bottom = select_bottom  # Pick worst instead of best
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def get_quarterly_dates(self) -> List[date]:
        """Get all quarterly dates with sufficient data."""
        rows = await self.conn.fetch('''
            SELECT score_date, COUNT(DISTINCT company_id) as companies
            FROM daily_dimension_scores
            GROUP BY score_date
            HAVING COUNT(DISTINCT company_id) >= 500
            ORDER BY score_date
        ''')
        # Exclude last 12 months (need forward returns)
        cutoff = date.today() - timedelta(days=365)
        dates = [r['score_date'] for r in rows if r['score_date'] < cutoff]
        return dates

    async def get_scored_companies(self, score_date: date) -> pd.DataFrame:
        """Get all companies with dimensions for a date, apply weighted scoring."""

        # Apply lag to avoid look-ahead bias
        # Use the most recent score_date that is at least lag_days before our target
        if self.lag_days > 0:
            lagged_date = score_date - timedelta(days=self.lag_days)
            # Find the most recent available score_date <= lagged_date
            actual_score_date = await self.conn.fetchval("""
                SELECT MAX(score_date) FROM daily_dimension_scores
                WHERE score_date <= $1
            """, lagged_date)
            if not actual_score_date:
                return pd.DataFrame()
        else:
            actual_score_date = score_date

        pivot_cases = ",\n            ".join([
            f"MAX(CASE WHEN dimension_code = '{dim}' THEN score END) as {dim}"
            for dim in DIMENSIONS
        ])

        query = f"""
        SELECT
            dds.company_id,
            cm.company_name,
            cm.yahoo_symbol,
            {pivot_cases}
        FROM daily_dimension_scores dds
        JOIN company_master cm ON dds.company_id = cm.id
        WHERE dds.score_date = $1
        GROUP BY dds.company_id, cm.company_name, cm.yahoo_symbol
        HAVING COUNT(DISTINCT dimension_code) >= 8
        """

        rows = await self.conn.fetch(query, actual_score_date)
        df = pd.DataFrame([dict(r) for r in rows])

        if df.empty:
            return df

        # Convert to float
        for dim in DIMENSIONS:
            if dim in df.columns:
                df[dim] = pd.to_numeric(df[dim], errors='coerce')

        # Calculate weighted score
        df['weighted_score'] = 0.0
        total_weight = sum(abs(w) for w in self.weights.values())  # Use absolute for normalization

        for dim, weight in self.weights.items():
            if dim in df.columns and weight != 0:
                # Normalize dimension to 0-100, handle NaN
                df[dim] = df[dim].fillna(50)  # Neutral for missing
                # Negative weights invert the contribution (high score → low contribution)
                df['weighted_score'] += (df[dim] * weight / total_weight)

        return df

    async def get_forward_returns(self, top_df: pd.DataFrame, entry_date: date) -> Dict:
        """Get forward returns for companies using symbol matching."""

        returns = {}

        for _, row in top_df.iterrows():
            company_id = row['company_id']
            yahoo_symbol = row.get('yahoo_symbol')

            if not yahoo_symbol:
                returns[company_id] = {'3M': None, '6M': None, '12M': None}
                continue

            # Strip exchange suffix and normalize (ATCO-B.ST -> ATCO B, MAERSK-B.CO -> MAERSK B)
            symbol = yahoo_symbol.split('.')[0] if '.' in yahoo_symbol else yahoo_symbol
            symbol = symbol.replace('-', ' ')  # daily_price_data uses spaces not hyphens

            # Get entry price by symbol
            entry_row = await self.conn.fetchrow("""
                SELECT date as entry_date, close_price as entry_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $2 + INTERVAL '7 days'
                AND close_price > 0
                ORDER BY date LIMIT 1
            """, symbol, entry_date)

            if not entry_row:
                returns[company_id] = {'3M': None, '6M': None, '12M': None}
                continue

            entry_dt = entry_row['entry_date']
            entry_price = float(entry_row['entry_price'])
            returns[company_id] = {'entry_price': entry_price}

            for horizon_name, days in [('3M', 63), ('6M', 126), ('12M', 252)]:
                target_start = entry_dt + timedelta(days=days)
                target_end = entry_dt + timedelta(days=days + 14)

                exit_row = await self.conn.fetchrow("""
                    SELECT close_price FROM daily_price_data
                    WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close_price > 0
                    ORDER BY date LIMIT 1
                """, symbol, target_start, target_end)

                if exit_row and entry_price > 0:
                    exit_price = float(exit_row['close_price'])
                    ret = ((exit_price - entry_price) / entry_price) * 100
                    # Filter likely stock splits (>500% or <-90% are suspicious)
                    if -90 <= ret <= 500:
                        returns[company_id][horizon_name] = ret
                    else:
                        returns[company_id][horizon_name] = None  # Exclude split-affected
                else:
                    returns[company_id][horizon_name] = None

        return returns

    async def get_detailed_returns(self, df: pd.DataFrame, entry_date: date) -> pd.DataFrame:
        """Get detailed returns with entry/exit dates and prices for all companies."""

        results = []

        for _, row in df.iterrows():
            company_id = row['company_id']
            yahoo_symbol = row.get('yahoo_symbol')

            result = {
                'company_id': company_id,
                'entry_date': None,
                'entry_price': None,
            }

            if not yahoo_symbol:
                for horizon in ['3M', '6M', '12M']:
                    result[f'{horizon}_exit_date'] = None
                    result[f'{horizon}_exit_price'] = None
                    result[f'{horizon}_return'] = None
                results.append(result)
                continue

            # Strip exchange suffix and normalize
            symbol = yahoo_symbol.split('.')[0] if '.' in yahoo_symbol else yahoo_symbol
            symbol = symbol.replace('-', ' ')

            # Get entry price
            entry_row = await self.conn.fetchrow("""
                SELECT date as entry_date, close_price as entry_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $2 + INTERVAL '7 days'
                AND close_price > 0
                ORDER BY date LIMIT 1
            """, symbol, entry_date)

            if not entry_row:
                for horizon in ['3M', '6M', '12M']:
                    result[f'{horizon}_exit_date'] = None
                    result[f'{horizon}_exit_price'] = None
                    result[f'{horizon}_return'] = None
                results.append(result)
                continue

            entry_dt = entry_row['entry_date']
            entry_price = float(entry_row['entry_price'])
            result['entry_date'] = entry_dt
            result['entry_price'] = entry_price

            for horizon_name, days in [('3M', 63), ('6M', 126), ('12M', 252)]:
                target_start = entry_dt + timedelta(days=days)
                target_end = entry_dt + timedelta(days=days + 14)

                exit_row = await self.conn.fetchrow("""
                    SELECT date as exit_date, close_price as exit_price
                    FROM daily_price_data
                    WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close_price > 0
                    ORDER BY date LIMIT 1
                """, symbol, target_start, target_end)

                if exit_row and entry_price > 0:
                    exit_dt = exit_row['exit_date']
                    exit_price = float(exit_row['exit_price'])
                    ret = ((exit_price - entry_price) / entry_price) * 100

                    # Flag likely stock splits (returns > 500% or < -90% are suspicious)
                    if ret > 500 or ret < -90:
                        # Mark as None - likely corporate action (split, reverse split)
                        result[f'{horizon_name}_exit_date'] = exit_dt
                        result[f'{horizon_name}_exit_price'] = exit_price
                        result[f'{horizon_name}_return'] = None  # Exclude from analysis
                        result[f'{horizon_name}_flag'] = 'SPLIT_SUSPECTED'
                    else:
                        result[f'{horizon_name}_exit_date'] = exit_dt
                        result[f'{horizon_name}_exit_price'] = exit_price
                        result[f'{horizon_name}_return'] = ret
                else:
                    result[f'{horizon_name}_exit_date'] = None
                    result[f'{horizon_name}_exit_price'] = None
                    result[f'{horizon_name}_return'] = None

            results.append(result)

        return pd.DataFrame(results)

    async def run_backtest_with_export(self) -> Dict:
        """Run backtest and collect full data for export."""
        await self.connect()

        try:
            quarters = await self.get_quarterly_dates()
            logger.info(f"Backtesting {len(quarters)} quarters for export")

            all_company_data = []

            for q_date in quarters:
                logger.info(f"Processing {q_date} for export...")

                # Get ALL scored companies (not just top N)
                df = await self.get_scored_companies(q_date)
                if df.empty:
                    continue

                # Get detailed returns for all companies
                returns_df = await self.get_detailed_returns(df, q_date)

                # Merge dimensions with returns
                merged = df.merge(returns_df, on='company_id', how='left')

                # Add quarter info and rank
                merged['quarter'] = q_date
                merged['rank'] = merged['weighted_score'].rank(ascending=False, method='min').astype(int)

                all_company_data.append(merged)

            # Combine all quarters
            if all_company_data:
                full_df = pd.concat(all_company_data, ignore_index=True)

                # Reorder columns for clarity
                dim_cols = [d for d in DIMENSIONS if d in full_df.columns]
                ordered_cols = [
                    'quarter', 'rank', 'company_name', 'yahoo_symbol', 'weighted_score',
                ] + dim_cols + [
                    'entry_date', 'entry_price',
                    '3M_exit_date', '3M_exit_price', '3M_return',
                    '6M_exit_date', '6M_exit_price', '6M_return',
                    '12M_exit_date', '12M_exit_price', '12M_return',
                ]
                # Only include columns that exist
                ordered_cols = [c for c in ordered_cols if c in full_df.columns]
                full_df = full_df[ordered_cols]

                return {
                    'data': full_df,
                    'weight_profile': self.weight_profile,
                    'weights': self.weights,
                    'top_n': self.top_n,
                    'lag_days': self.lag_days,
                    'quarters': len(quarters),
                }

            return {'data': pd.DataFrame()}

        finally:
            await self.close()

    async def run_backtest(self) -> Dict:
        """Run full backtest across all quarters."""
        await self.connect()

        try:
            quarters = await self.get_quarterly_dates()
            logger.info(f"Backtesting {len(quarters)} quarters with {self.weight_profile} weights, top {self.top_n}")

            all_results = []
            quarterly_summaries = []

            for q_date in quarters:
                logger.info(f"Processing {q_date}...")

                # Get scored companies
                df = await self.get_scored_companies(q_date)
                if df.empty:
                    continue

                # Select top N (or bottom N if testing reverse strategy)
                if self.select_bottom:
                    top_n = df.nsmallest(self.top_n, 'weighted_score')
                else:
                    top_n = df.nlargest(self.top_n, 'weighted_score')

                # Get forward returns (using symbol matching)
                returns = await self.get_forward_returns(top_n, q_date)

                # Also get market average (all companies)
                all_returns = await self.get_forward_returns(df, q_date)

                # Build results
                quarter_picks = []
                for _, row in top_n.iterrows():
                    cid = row['company_id']
                    if cid in returns:
                        pick = {
                            'quarter': q_date,
                            'company_name': row['company_name'],
                            'weighted_score': row['weighted_score'],
                            'return_3M': returns[cid].get('3M'),
                            'return_6M': returns[cid].get('6M'),
                            'return_12M': returns[cid].get('12M'),
                        }
                        quarter_picks.append(pick)
                        all_results.append(pick)

                # Calculate quarter summary
                if quarter_picks:
                    picks_df = pd.DataFrame(quarter_picks)

                    # Market median (more robust than mean - excludes outlier moonshots)
                    def robust_avg(returns_list):
                        """Use median and clip extremes for fair comparison."""
                        valid = [r for r in returns_list if r is not None and -90 < r < 200]
                        return np.median(valid) if valid else None

                    market_3m = robust_avg([r.get('3M') for r in all_returns.values()])
                    market_6m = robust_avg([r.get('6M') for r in all_returns.values()])
                    market_12m = robust_avg([r.get('12M') for r in all_returns.values()])

                    summary = {
                        'quarter': q_date,
                        'n_picks': len(picks_df),
                        'avg_score': picks_df['weighted_score'].mean(),
                        'top20_3M': picks_df['return_3M'].mean(),
                        'top20_6M': picks_df['return_6M'].mean(),
                        'top20_12M': picks_df['return_12M'].mean(),
                        'market_3M': market_3m,
                        'market_6M': market_6m,
                        'market_12M': market_12m,
                        'alpha_3M': picks_df['return_3M'].mean() - market_3m if not pd.isna(picks_df['return_3M'].mean()) else None,
                        'alpha_6M': picks_df['return_6M'].mean() - market_6m if not pd.isna(picks_df['return_6M'].mean()) else None,
                        'alpha_12M': picks_df['return_12M'].mean() - market_12m if not pd.isna(picks_df['return_12M'].mean()) else None,
                    }
                    quarterly_summaries.append(summary)

            return {
                'weight_profile': self.weight_profile,
                'weights': self.weights,
                'top_n': self.top_n,
                'lag_days': self.lag_days,
                'quarters': len(quarterly_summaries),
                'all_picks': all_results,
                'quarterly_summaries': quarterly_summaries,
            }

        finally:
            await self.close()


def print_results(results: Dict):
    """Print formatted backtest results."""

    print("\n" + "=" * 100)
    print(f"FAT PITCH BACKTEST RESULTS")
    print(f"Weight Profile: {results['weight_profile'].upper()}")
    print(f"Top N: {results['top_n']}")
    print(f"Lag Days: {results.get('lag_days', 0)} {'(NO LOOK-AHEAD BIAS)' if results.get('lag_days', 0) >= 60 else ''}")
    print(f"Quarters Tested: {results['quarters']}")
    print("=" * 100)

    # Print weights
    print("\nWeight Configuration:")
    for dim, weight in sorted(results['weights'].items(), key=lambda x: -x[1]):
        if weight > 0:
            bar = '█' * int(weight / 2)
            print(f"  {dim:25s} {weight:3d}% {bar}")

    # Quarterly breakdown
    print("\n" + "-" * 100)
    print(f"{'Quarter':<12} {'Top20 3M':>10} {'Mkt 3M':>10} {'Alpha':>8} │ {'Top20 6M':>10} {'Mkt 6M':>10} {'Alpha':>8} │ {'Top20 12M':>10} {'Mkt 12M':>10} {'Alpha':>8}")
    print("-" * 100)

    for q in results['quarterly_summaries']:
        def fmt(val):
            return f"{val:+.1f}%" if val is not None else "N/A"

        def fmt_alpha(val):
            if val is None:
                return "N/A"
            color = "" if val >= 0 else ""
            return f"{val:+.1f}%"

        print(f"{str(q['quarter']):<12} "
              f"{fmt(q['top20_3M']):>10} {fmt(q['market_3M']):>10} {fmt_alpha(q['alpha_3M']):>8} │ "
              f"{fmt(q['top20_6M']):>10} {fmt(q['market_6M']):>10} {fmt_alpha(q['alpha_6M']):>8} │ "
              f"{fmt(q['top20_12M']):>10} {fmt(q['market_12M']):>10} {fmt_alpha(q['alpha_12M']):>8}")

    # Overall summary
    print("-" * 100)

    summaries = pd.DataFrame(results['quarterly_summaries'])

    avg_alpha_3m = summaries['alpha_3M'].mean()
    avg_alpha_6m = summaries['alpha_6M'].mean()
    avg_alpha_12m = summaries['alpha_12M'].mean()

    win_rate_3m = (summaries['alpha_3M'] > 0).mean() * 100
    win_rate_6m = (summaries['alpha_6M'] > 0).mean() * 100
    win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

    print(f"\n{'SUMMARY':^100}")
    print("-" * 100)
    print(f"{'Metric':<30} {'3 Month':>20} {'6 Month':>20} {'12 Month':>20}")
    print("-" * 100)
    print(f"{'Avg Top 20 Return':<30} {summaries['top20_3M'].mean():>19.1f}% {summaries['top20_6M'].mean():>19.1f}% {summaries['top20_12M'].mean():>19.1f}%")
    print(f"{'Avg Market Return':<30} {summaries['market_3M'].mean():>19.1f}% {summaries['market_6M'].mean():>19.1f}% {summaries['market_12M'].mean():>19.1f}%")
    print(f"{'Avg Alpha (Top20 - Market)':<30} {avg_alpha_3m:>+18.1f}% {avg_alpha_6m:>+18.1f}% {avg_alpha_12m:>+18.1f}%")
    print(f"{'Win Rate (Alpha > 0)':<30} {win_rate_3m:>19.0f}% {win_rate_6m:>19.0f}% {win_rate_12m:>19.0f}%")
    print("-" * 100)

    # Top picks across all quarters
    print(f"\n{'TOP 10 BEST PICKS (12M Return)':^100}")
    print("-" * 100)

    picks_df = pd.DataFrame(results['all_picks'])
    if not picks_df.empty and 'return_12M' in picks_df.columns:
        top_picks = picks_df.dropna(subset=['return_12M']).nlargest(10, 'return_12M')
        print(f"{'Quarter':<12} {'Company':<40} {'Score':>8} {'12M Return':>12}")
        print("-" * 100)
        for _, pick in top_picks.iterrows():
            print(f"{str(pick['quarter']):<12} {pick['company_name'][:38]:<40} {pick['weighted_score']:>7.1f} {pick['return_12M']:>+11.1f}%")

    # Worst picks
    print(f"\n{'TOP 10 WORST PICKS (12M Return)':^100}")
    print("-" * 100)

    if not picks_df.empty and 'return_12M' in picks_df.columns:
        worst_picks = picks_df.dropna(subset=['return_12M']).nsmallest(10, 'return_12M')
        print(f"{'Quarter':<12} {'Company':<40} {'Score':>8} {'12M Return':>12}")
        print("-" * 100)
        for _, pick in worst_picks.iterrows():
            print(f"{str(pick['quarter']):<12} {pick['company_name'][:38]:<40} {pick['weighted_score']:>7.1f} {pick['return_12M']:>+11.1f}%")


def generate_charts(results: Dict, output_prefix: str):
    """Generate visualization charts for backtest results."""
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    df = results['data']
    if df.empty:
        return []

    charts = []
    top_n = results.get('top_n', 20)
    profile = results['weight_profile']

    # Set style
    plt.style.use('seaborn-v0_8-whitegrid')

    # --- Chart 1: Alpha by Quarter ---
    fig, ax = plt.subplots(figsize=(14, 6))

    # Calculate alpha per quarter
    quarters = sorted(df['quarter'].unique())
    alphas_3m, alphas_6m, alphas_12m = [], [], []

    for q in quarters:
        q_df = df[df['quarter'] == q]
        top = q_df[q_df['rank'] <= top_n]

        # Top N average vs market median
        top_12m = top['12M_return'].mean()
        mkt_12m = q_df['12M_return'].median()
        top_6m = top['6M_return'].mean()
        mkt_6m = q_df['6M_return'].median()
        top_3m = top['3M_return'].mean()
        mkt_3m = q_df['3M_return'].median()

        alphas_12m.append(top_12m - mkt_12m if pd.notna(top_12m) and pd.notna(mkt_12m) else 0)
        alphas_6m.append(top_6m - mkt_6m if pd.notna(top_6m) and pd.notna(mkt_6m) else 0)
        alphas_3m.append(top_3m - mkt_3m if pd.notna(top_3m) and pd.notna(mkt_3m) else 0)

    x = np.arange(len(quarters))
    width = 0.25

    bars1 = ax.bar(x - width, alphas_3m, width, label='3M Alpha', color='#3498db', alpha=0.8)
    bars2 = ax.bar(x, alphas_6m, width, label='6M Alpha', color='#9b59b6', alpha=0.8)
    bars3 = ax.bar(x + width, alphas_12m, width, label='12M Alpha', color='#2ecc71', alpha=0.8)

    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax.set_xlabel('Quarter')
    ax.set_ylabel('Alpha (%)')
    ax.set_title(f'Top {top_n} Alpha vs Market by Quarter ({profile.upper()} weights)')
    ax.set_xticks(x)
    ax.set_xticklabels([str(q)[:10] for q in quarters], rotation=45, ha='right')
    ax.legend()

    # Add average line
    avg_12m = np.nanmean(alphas_12m)
    ax.axhline(y=avg_12m, color='#2ecc71', linestyle='--', linewidth=2, label=f'Avg 12M: {avg_12m:+.1f}%')

    plt.tight_layout()
    chart1 = f"{output_prefix}_alpha_by_quarter.png"
    plt.savefig(chart1, dpi=150)
    plt.close()
    charts.append(chart1)

    # --- Chart 2: Cumulative Returns ---
    fig, ax = plt.subplots(figsize=(14, 6))

    # Calculate cumulative returns for top picks vs market
    top_cumulative = [100]
    mkt_cumulative = [100]

    for q in quarters:
        q_df = df[df['quarter'] == q]
        top = q_df[q_df['rank'] <= top_n]

        # Use 3M returns for more data points
        top_ret = top['3M_return'].mean()
        mkt_ret = q_df['3M_return'].median()

        if pd.notna(top_ret):
            top_cumulative.append(top_cumulative[-1] * (1 + top_ret/100))
        else:
            top_cumulative.append(top_cumulative[-1])

        if pd.notna(mkt_ret):
            mkt_cumulative.append(mkt_cumulative[-1] * (1 + mkt_ret/100))
        else:
            mkt_cumulative.append(mkt_cumulative[-1])

    quarter_labels = ['Start'] + [str(q)[:10] for q in quarters]

    ax.plot(quarter_labels, top_cumulative, 'o-', linewidth=2, markersize=6,
            color='#2ecc71', label=f'Top {top_n} Picks')
    ax.plot(quarter_labels, mkt_cumulative, 's-', linewidth=2, markersize=6,
            color='#95a5a6', label='Market Median')

    ax.fill_between(quarter_labels, top_cumulative, mkt_cumulative,
                    alpha=0.2, color='#2ecc71')

    ax.set_xlabel('Quarter')
    ax.set_ylabel('Cumulative Value (Starting $100)')
    ax.set_title(f'Cumulative Returns: Top {top_n} vs Market ({profile.upper()} weights)')
    ax.legend()
    plt.xticks(rotation=45, ha='right')

    # Add final values
    ax.annotate(f'${top_cumulative[-1]:.0f}',
                xy=(len(quarters), top_cumulative[-1]),
                xytext=(5, 5), textcoords='offset points', fontsize=10, color='#2ecc71')
    ax.annotate(f'${mkt_cumulative[-1]:.0f}',
                xy=(len(quarters), mkt_cumulative[-1]),
                xytext=(5, -10), textcoords='offset points', fontsize=10, color='#95a5a6')

    plt.tight_layout()
    chart2 = f"{output_prefix}_cumulative_returns.png"
    plt.savefig(chart2, dpi=150)
    plt.close()
    charts.append(chart2)

    # --- Chart 3: Dimension Scores - Top vs Bottom Performers ---
    fig, ax = plt.subplots(figsize=(14, 7))

    dimensions = [d for d in DIMENSIONS if d in df.columns]

    # Get top and bottom performers by 12M return
    valid_returns = df.dropna(subset=['12M_return'])
    if len(valid_returns) > 100:
        top_performers = valid_returns.nlargest(100, '12M_return')
        bottom_performers = valid_returns.nsmallest(100, '12M_return')

        top_scores = [top_performers[d].mean() for d in dimensions]
        bottom_scores = [bottom_performers[d].mean() for d in dimensions]

        x = np.arange(len(dimensions))
        width = 0.35

        ax.bar(x - width/2, top_scores, width, label='Top 100 (by 12M return)', color='#2ecc71', alpha=0.8)
        ax.bar(x + width/2, bottom_scores, width, label='Bottom 100 (by 12M return)', color='#e74c3c', alpha=0.8)

        ax.set_xlabel('Dimension')
        ax.set_ylabel('Average Score (0-100)')
        ax.set_title('Dimension Scores: Best vs Worst Performers')
        ax.set_xticks(x)
        ax.set_xticklabels([d.replace('_', '\n') for d in dimensions], rotation=45, ha='right', fontsize=9)
        ax.legend()
        ax.axhline(y=50, color='black', linestyle='--', linewidth=0.5, alpha=0.5)

        plt.tight_layout()
        chart3 = f"{output_prefix}_dimension_comparison.png"
        plt.savefig(chart3, dpi=150)
        plt.close()
        charts.append(chart3)

    # --- Chart 4: Rank vs Return Scatter ---
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    for idx, (horizon, color) in enumerate([('3M', '#3498db'), ('6M', '#9b59b6'), ('12M', '#2ecc71')]):
        ax = axes[idx]
        col = f'{horizon}_return'

        # Get data with valid returns
        plot_df = df[['rank', col]].dropna()

        # Scatter plot
        ax.scatter(plot_df['rank'], plot_df[col], alpha=0.3, s=10, color=color)

        # Add trend line (rolling average by rank buckets)
        plot_df = plot_df.sort_values('rank')
        bucket_size = max(1, len(plot_df) // 50)
        rolling_avg = plot_df.groupby(plot_df['rank'] // bucket_size * bucket_size)[col].mean()
        ax.plot(rolling_avg.index, rolling_avg.values, color='red', linewidth=2, label='Trend')

        # Add reference lines
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax.axhline(y=plot_df[col].median(), color='gray', linestyle='--', linewidth=1,
                   label=f'Median: {plot_df[col].median():.1f}%')

        ax.set_xlabel('Rank (1 = Best Score)')
        ax.set_ylabel(f'{horizon} Return (%)')
        ax.set_title(f'Rank vs {horizon} Return')
        ax.legend(fontsize=9)

        # Limit y-axis for readability
        ax.set_ylim(-100, 200)

    plt.suptitle(f'Does Ranking Predict Returns? ({profile.upper()} weights)', y=1.02)
    plt.tight_layout()
    chart_scatter = f"{output_prefix}_rank_vs_return.png"
    plt.savefig(chart_scatter, dpi=150, bbox_inches='tight')
    plt.close()
    charts.append(chart_scatter)

    # --- Chart 5: Return Distribution (histogram) ---
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    for idx, (horizon, color) in enumerate([('3M', '#3498db'), ('6M', '#9b59b6'), ('12M', '#2ecc71')]):
        ax = axes[idx]
        col = f'{horizon}_return'

        # Top N picks
        top_returns = df[df['rank'] <= top_n][col].dropna()
        # All market
        all_returns = df[col].dropna()

        # Clip for visualization
        top_clipped = top_returns.clip(-100, 200)
        all_clipped = all_returns.clip(-100, 200)

        ax.hist(all_clipped, bins=50, alpha=0.5, label='All Companies', color='#95a5a6', density=True)
        ax.hist(top_clipped, bins=30, alpha=0.7, label=f'Top {top_n}', color=color, density=True)

        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
        ax.axvline(x=top_returns.median(), color=color, linestyle='--', linewidth=2,
                   label=f'Top {top_n} Median: {top_returns.median():.1f}%')
        ax.axvline(x=all_returns.median(), color='#95a5a6', linestyle='--', linewidth=2,
                   label=f'Market Median: {all_returns.median():.1f}%')

        ax.set_xlabel(f'{horizon} Return (%)')
        ax.set_ylabel('Density')
        ax.set_title(f'{horizon} Return Distribution')
        ax.legend(fontsize=8)

    plt.suptitle(f'Return Distributions: Top {top_n} vs Market ({profile.upper()} weights)', y=1.02)
    plt.tight_layout()
    chart4 = f"{output_prefix}_return_distributions.png"
    plt.savefig(chart4, dpi=150, bbox_inches='tight')
    plt.close()
    charts.append(chart4)

    return charts


def export_to_excel(results: Dict, filename: str = None):
    """Export full backtest data to Excel file."""
    from datetime import datetime

    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        profile = results['weight_profile']
        filename = f"fat_pitch_export_{profile}_{timestamp}.xlsx"

    df = results['data']
    if df.empty:
        logger.error("No data to export")
        return None

    # Get unique quarters sorted
    quarters = sorted(df['quarter'].unique())
    top_n = results.get('top_n', 20)

    # Create Excel writer with multiple sheets
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Sheet 1: Full ranked data (all companies, all quarters)
        df.to_excel(writer, sheet_name='All Companies', index=False)

        # Sheet 2: Summary by quarter
        summary = df.groupby('quarter').agg({
            'company_name': 'count',
            'weighted_score': 'mean',
            '3M_return': 'mean',
            '6M_return': 'mean',
            '12M_return': 'mean',
        }).rename(columns={'company_name': 'num_companies'})
        summary.to_excel(writer, sheet_name='Summary')

        # Sheet 3: Top picks only (rank <= top_n)
        top_picks = df[df['rank'] <= top_n].copy()
        top_picks.to_excel(writer, sheet_name=f'Top {top_n} Picks', index=False)

        # Sheets 4+: Individual quarter tabs
        for q in quarters:
            q_df = df[df['quarter'] == q].copy()
            # Format quarter as sheet name (Excel limits to 31 chars)
            q_str = str(q)[:10]  # e.g., "2024-06-30"
            sheet_name = f"Q {q_str}"
            q_df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Weight configuration
        weights_df = pd.DataFrame([
            {'dimension': dim, 'weight': w}
            for dim, w in sorted(results['weights'].items(), key=lambda x: -abs(x[1]))
        ])
        weights_df.to_excel(writer, sheet_name='Weights', index=False)

        # Metadata
        meta = pd.DataFrame([{
            'weight_profile': results['weight_profile'],
            'top_n': results['top_n'],
            'lag_days': results['lag_days'],
            'quarters_tested': results['quarters'],
            'total_company_quarters': len(df),
            'export_date': datetime.now().isoformat(),
        }])
        meta.to_excel(writer, sheet_name='Metadata', index=False)

    logger.info(f"Exported to {filename}")

    # Generate charts
    output_prefix = filename.rsplit('.', 1)[0]  # Remove .xlsx extension
    charts = generate_charts(results, output_prefix)

    print(f"\n✅ Exported to: {filename}")
    print(f"   - {len(df):,} company-quarter records")
    print(f"   - {results['quarters']} quarters")
    print(f"   - {len(df['company_name'].unique()):,} unique companies")
    print(f"\nSheets:")
    print(f"   1. All Companies - Full ranked list (all quarters combined)")
    print(f"   2. Summary - Aggregated stats per quarter")
    print(f"   3. Top {top_n} Picks - Only top ranked companies")
    for i, q in enumerate(quarters, 4):
        print(f"   {i}. Q {str(q)[:10]} - All companies for that quarter")
    print(f"   {len(quarters)+4}. Weights - Dimension weights used")
    print(f"   {len(quarters)+5}. Metadata - Export configuration")

    if charts:
        print(f"\n📊 Charts generated:")
        for chart in charts:
            print(f"   - {chart}")

    return filename


async def compare_strategies(lag_days: int = 0, select_bottom: bool = False):
    """Compare different weight profiles."""
    mode = "BOTTOM" if select_bottom else "TOP"
    print("\n" + "=" * 100)
    print(f"STRATEGY COMPARISON - {mode} 20 (lag={lag_days} days)")
    print("=" * 100)

    results = {}
    profiles = ['ml', 'original', 'equal', 'value', 'quality', 'contrarian', 'anti']
    for profile in profiles:
        logger.info(f"Running {profile} strategy ({mode})...")
        backtester = FatPitchBacktester(
            top_n=20,
            weight_profile=profile,
            lag_days=lag_days,
            select_bottom=select_bottom
        )
        results[profile] = await backtester.run_backtest()

    # Comparison table - Mean (sensitive to outliers)
    print(f"\n{'Profile':<12} {'Mean Alpha 3M':>15} {'Mean Alpha 6M':>15} {'Mean Alpha 12M':>15} {'Win Rate 12M':>15}")
    print("-" * 75)

    for profile, res in results.items():
        summaries = pd.DataFrame(res['quarterly_summaries'])
        avg_alpha_3m = summaries['alpha_3M'].mean()
        avg_alpha_6m = summaries['alpha_6M'].mean()
        avg_alpha_12m = summaries['alpha_12M'].mean()
        win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

        print(f"{profile:<12} {avg_alpha_3m:>+14.1f}% {avg_alpha_6m:>+14.1f}% {avg_alpha_12m:>+14.1f}% {win_rate_12m:>14.0f}%")

    # Comparison table - Median (robust to outliers)
    print(f"\n{'Profile':<12} {'Med Alpha 3M':>15} {'Med Alpha 6M':>15} {'Med Alpha 12M':>15} {'Win Rate 12M':>15}")
    print("-" * 75)

    for profile, res in results.items():
        summaries = pd.DataFrame(res['quarterly_summaries'])
        med_alpha_3m = summaries['alpha_3M'].median()
        med_alpha_6m = summaries['alpha_6M'].median()
        med_alpha_12m = summaries['alpha_12M'].median()
        win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

        print(f"{profile:<12} {med_alpha_3m:>+14.1f}% {med_alpha_6m:>+14.1f}% {med_alpha_12m:>+14.1f}% {win_rate_12m:>14.0f}%")


async def main():
    parser = argparse.ArgumentParser(description='Fat Pitch Strategy Backtest')
    parser.add_argument('--top', type=int, default=20, help='Number of top picks per quarter')
    parser.add_argument('--weights', type=str, default='ml',
                        choices=['ml', 'original', 'equal', 'value', 'quality', 'contrarian', 'anti'],
                        help='Weight profile to use')
    parser.add_argument('--compare', action='store_true', help='Compare all weight profiles')
    parser.add_argument('--bottom', action='store_true',
                        help='Select BOTTOM N instead of top N (test if signal works both ways)')
    parser.add_argument('--lag', type=int, default=0,
                        help='Days to lag dimension scores (60 = no look-ahead bias)')
    parser.add_argument('--export', action='store_true',
                        help='Export full data to Excel (all companies, all dimensions, all returns)')
    parser.add_argument('--output', type=str, default=None,
                        help='Output filename for export (default: auto-generated)')

    args = parser.parse_args()

    if args.compare:
        await compare_strategies(lag_days=args.lag, select_bottom=args.bottom)
    elif args.export:
        backtester = FatPitchBacktester(
            top_n=args.top,
            weight_profile=args.weights,
            lag_days=args.lag,
            select_bottom=args.bottom
        )
        results = await backtester.run_backtest_with_export()
        export_to_excel(results, args.output)
    else:
        backtester = FatPitchBacktester(
            top_n=args.top,
            weight_profile=args.weights,
            lag_days=args.lag,
            select_bottom=args.bottom
        )
        results = await backtester.run_backtest()
        print_results(results)


if __name__ == '__main__':
    asyncio.run(main())
