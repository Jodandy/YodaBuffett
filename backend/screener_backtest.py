#!/usr/bin/env python3
"""
Screener Backtest - Compare custom screeners to Fat Pitch dimension approach

Usage:
    python screener_backtest.py --screener quality_growth    # Run specific screener
    python screener_backtest.py --list                       # List available screeners
    python screener_backtest.py --compare                    # Compare all screeners
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# ============================================================
# SCREENER DEFINITIONS
# ============================================================
# Add your screeners here. Each screener is a dict with:
#   - name: Display name
#   - description: What it looks for
#   - criteria: Function that takes a DataFrame and returns filtered DataFrame

SCREENERS = {
    'quality_growth': {
        'name': 'Quality Growth (Original)',
        'description': 'ROE>12, CR>1.8, EPS/Rev Growth>0, NM>9%, 3Y Rev Growth>9%, Vol>5M SEK',
        'criteria': {
            'roe': ('>', 12),
            'current_ratio': ('>', 1.8),
            'earnings_growth_yoy': ('>', 0),
            'revenue_growth_yoy': ('>', 0),
            'net_margin': ('>', 9),
            'revenue_growth_3y': ('>', 9),
            'avg_volume_sek_100d': ('>', 5_000_000),
        }
    },
    'quality_growth_relaxed': {
        'name': 'Quality Growth (Relaxed)',
        'description': 'ROE>10, CR>1.5, EPS/Rev Growth>0, NM>5%',
        'criteria': {
            'roe': ('>', 10),
            'current_ratio': ('>', 1.5),
            'earnings_growth_yoy': ('>', 0),
            'revenue_growth_yoy': ('>', 0),
            'net_margin': ('>', 5),
        }
    },
    'quality_simple': {
        'name': 'Quality Simple',
        'description': 'ROE>12, NM>8% (no current ratio - too many NULLs)',
        'criteria': {
            'roe': ('>', 12),
            'net_margin': ('>', 8),
            'revenue_growth_yoy': ('>', 0),
        }
    },
    'high_roe': {
        'name': 'High ROE',
        'description': 'ROE>15 only',
        'criteria': {
            'roe': ('>', 15),
        }
    },
    # Add more screeners here as needed
}


class ScreenerBacktester:
    """Backtest screener strategies with forward returns."""

    def __init__(self, screener_id: str, lag_days: int = 60):
        self.screener_id = screener_id
        self.screener = SCREENERS[screener_id]
        self.lag_days = lag_days
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def get_quarterly_dates(self) -> List[date]:
        """Get quarterly dates where we have sufficient data."""
        rows = await self.conn.fetch('''
            SELECT DISTINCT period_date
            FROM financial_statements
            WHERE period_date >= '2021-06-01'
            ORDER BY period_date
        ''')

        # Filter to quarter ends
        dates = []
        for r in rows:
            d = r['period_date']
            if d.month in [3, 6, 9, 12] and d.day >= 28:
                dates.append(d)

        return dates

    async def calculate_metrics(self, as_of_date: date) -> pd.DataFrame:
        """Calculate all screener metrics for companies as of a date."""

        # Apply lag to avoid look-ahead bias
        if self.lag_days > 0:
            data_cutoff = as_of_date - timedelta(days=self.lag_days)
        else:
            data_cutoff = as_of_date

        # Get latest financial statements per company (with lag)
        query = """
        WITH latest_financials AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                period_date,
                total_revenue,
                net_income,
                fiscal_year
            FROM financial_statements
            WHERE period_date <= $1
              AND statement_type = 'annual'
              AND total_revenue > 0
            ORDER BY symbol, period_date DESC
        ),
        prior_year_financials AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                total_revenue as prior_revenue,
                net_income as prior_net_income,
                fiscal_year as prior_year
            FROM financial_statements
            WHERE period_date <= $1 - INTERVAL '1 year'
              AND statement_type = 'annual'
            ORDER BY symbol, period_date DESC
        ),
        three_year_ago_financials AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                total_revenue as revenue_3y_ago
            FROM financial_statements
            WHERE period_date <= $1 - INTERVAL '3 years'
              AND statement_type = 'annual'
            ORDER BY symbol, period_date DESC
        ),
        latest_balance AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                current_assets,
                current_liabilities,
                total_equity
            FROM balance_sheet_data
            WHERE period_date <= $1
              AND statement_type = 'annual'
            ORDER BY symbol, period_date DESC
        ),
        volume_data AS (
            SELECT
                symbol,
                AVG(close_price * volume) as avg_volume_sek_100d
            FROM daily_price_data
            WHERE date BETWEEN $1 - INTERVAL '100 days' AND $1
              AND volume > 0
            GROUP BY symbol
        )
        SELECT
            lf.symbol,
            cm.company_name,
            cm.sector,
            cm.yahoo_symbol,
            cm.id as company_id,
            -- ROE (cast to float to avoid integer division)
            CASE WHEN lb.total_equity > 0
                 THEN (lf.net_income::float / lb.total_equity::float) * 100
                 ELSE NULL END as roe,
            -- Current Ratio
            CASE WHEN lb.current_liabilities > 0
                 THEN lb.current_assets::float / lb.current_liabilities::float
                 ELSE NULL END as current_ratio,
            -- Net Margin
            CASE WHEN lf.total_revenue > 0
                 THEN (lf.net_income::float / lf.total_revenue::float) * 100
                 ELSE NULL END as net_margin,
            -- YoY Revenue Growth
            CASE WHEN pf.prior_revenue > 0
                 THEN ((lf.total_revenue::float - pf.prior_revenue::float) / pf.prior_revenue::float) * 100
                 ELSE NULL END as revenue_growth_yoy,
            -- YoY Earnings Growth
            CASE WHEN pf.prior_net_income > 0
                 THEN ((lf.net_income::float - pf.prior_net_income::float) / ABS(pf.prior_net_income::float)) * 100
                 ELSE NULL END as earnings_growth_yoy,
            -- 3Y Revenue CAGR
            CASE WHEN tf.revenue_3y_ago > 0 AND lf.total_revenue > 0
                 THEN (POWER(lf.total_revenue::float / tf.revenue_3y_ago::float, 1.0/3.0) - 1) * 100
                 ELSE NULL END as revenue_growth_3y,
            -- Volume
            vd.avg_volume_sek_100d
        FROM latest_financials lf
        LEFT JOIN prior_year_financials pf ON lf.symbol = pf.symbol
        LEFT JOIN three_year_ago_financials tf ON lf.symbol = tf.symbol
        LEFT JOIN latest_balance lb ON lf.symbol = lb.symbol
        LEFT JOIN volume_data vd ON lf.symbol = vd.symbol
        LEFT JOIN company_master cm ON lf.symbol = cm.yahoo_symbol
                                     OR lf.symbol || '.ST' = cm.yahoo_symbol
                                     OR lf.symbol || '.CO' = cm.yahoo_symbol
                                     OR lf.symbol || '.OL' = cm.yahoo_symbol
                                     OR lf.symbol || '.HE' = cm.yahoo_symbol
                                     OR REPLACE(lf.symbol, ' ', '-') || '.ST' = cm.yahoo_symbol
                                     OR REPLACE(lf.symbol, ' ', '-') || '.CO' = cm.yahoo_symbol
                                     OR REPLACE(lf.symbol, ' ', '-') || '.OL' = cm.yahoo_symbol
                                     OR REPLACE(lf.symbol, ' ', '-') || '.HE' = cm.yahoo_symbol
        WHERE cm.id IS NOT NULL
        """

        rows = await self.conn.fetch(query, data_cutoff)
        df = pd.DataFrame([dict(r) for r in rows])

        return df

    def apply_screener(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply screener criteria and return matching companies."""
        if df.empty:
            return df

        mask = pd.Series([True] * len(df), index=df.index)

        for metric, (operator, value) in self.screener['criteria'].items():
            if metric not in df.columns:
                logger.warning(f"Metric {metric} not found in data")
                continue

            if operator == '>':
                mask &= df[metric] > value
            elif operator == '>=':
                mask &= df[metric] >= value
            elif operator == '<':
                mask &= df[metric] < value
            elif operator == '<=':
                mask &= df[metric] <= value
            elif operator == '=':
                mask &= df[metric] == value

            # Handle NaN - exclude rows with missing data for this metric
            mask &= df[metric].notna()

        return df[mask].copy()

    async def get_forward_returns(self, symbols: List[str], entry_date: date) -> Dict[str, Dict]:
        """Get forward returns for symbols."""
        returns = {}

        for symbol in symbols:
            # Normalize symbol for price lookup
            price_symbol = symbol.split('.')[0] if '.' in symbol else symbol
            price_symbol = price_symbol.replace('-', ' ')

            # Get entry price
            entry_row = await self.conn.fetchrow("""
                SELECT date, close_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $2 + INTERVAL '7 days'
                AND close_price > 0
                ORDER BY date LIMIT 1
            """, price_symbol, entry_date)

            if not entry_row:
                returns[symbol] = {'3M': None, '6M': None, '12M': None}
                continue

            entry_price = float(entry_row['close_price'])
            entry_dt = entry_row['date']
            returns[symbol] = {}

            for horizon_name, days in [('3M', 63), ('6M', 126), ('12M', 252)]:
                target_start = entry_dt + timedelta(days=days)
                target_end = entry_dt + timedelta(days=days + 14)

                exit_row = await self.conn.fetchrow("""
                    SELECT close_price FROM daily_price_data
                    WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close_price > 0
                    ORDER BY date LIMIT 1
                """, price_symbol, target_start, target_end)

                if exit_row and entry_price > 0:
                    exit_price = float(exit_row['close_price'])
                    ret = ((exit_price - entry_price) / entry_price) * 100
                    returns[symbol][horizon_name] = ret
                else:
                    returns[symbol][horizon_name] = None

        return returns

    async def get_market_median_returns(self, entry_date: date) -> Dict[str, float]:
        """Get market median returns for comparison."""
        # Get all companies with price data
        query = """
            SELECT DISTINCT symbol FROM daily_price_data
            WHERE date >= $1 AND date <= $1 + INTERVAL '7 days'
        """
        rows = await self.conn.fetch(query, entry_date)
        all_symbols = [r['symbol'] for r in rows]

        # Sample for efficiency (use ~200 random symbols)
        if len(all_symbols) > 200:
            import random
            all_symbols = random.sample(all_symbols, 200)

        all_returns = await self.get_forward_returns(all_symbols, entry_date)

        market_medians = {}
        for horizon in ['3M', '6M', '12M']:
            valid_returns = [r[horizon] for r in all_returns.values()
                          if r.get(horizon) is not None]
            market_medians[horizon] = np.median(valid_returns) if valid_returns else 0

        return market_medians

    async def run_backtest(self) -> Dict:
        """Run full backtest."""
        await self.connect()

        try:
            quarters = await self.get_quarterly_dates()
            logger.info(f"Backtesting {self.screener['name']} on {len(quarters)} quarters")

            results = {
                'screener': self.screener_id,
                'name': self.screener['name'],
                'lag_days': self.lag_days,
                'quarterly_results': [],
                'all_picks': []
            }

            for q_date in quarters:
                logger.info(f"Processing {q_date}...")

                # Calculate metrics
                df = await self.calculate_metrics(q_date)
                if df.empty:
                    continue

                # Apply screener
                matches = self.apply_screener(df)
                if matches.empty:
                    continue

                # Get forward returns for matches
                symbols = matches['symbol'].tolist()
                forward_returns = await self.get_forward_returns(symbols, q_date)

                # Get market median for alpha calculation
                market_median = await self.get_market_median_returns(q_date)

                # Calculate stats
                returns_3m = [forward_returns[s]['3M'] for s in symbols if forward_returns[s]['3M'] is not None]
                returns_6m = [forward_returns[s]['6M'] for s in symbols if forward_returns[s]['6M'] is not None]
                returns_12m = [forward_returns[s]['12M'] for s in symbols if forward_returns[s]['12M'] is not None]

                quarter_result = {
                    'quarter': str(q_date),
                    'matches': len(matches),
                    'avg_return_3M': np.mean(returns_3m) if returns_3m else None,
                    'avg_return_6M': np.mean(returns_6m) if returns_6m else None,
                    'avg_return_12M': np.mean(returns_12m) if returns_12m else None,
                    'median_return_3M': np.median(returns_3m) if returns_3m else None,
                    'median_return_6M': np.median(returns_6m) if returns_6m else None,
                    'median_return_12M': np.median(returns_12m) if returns_12m else None,
                    'market_median_3M': market_median['3M'],
                    'market_median_6M': market_median['6M'],
                    'market_median_12M': market_median['12M'],
                    'alpha_3M': (np.mean(returns_3m) - market_median['3M']) if returns_3m else None,
                    'alpha_6M': (np.mean(returns_6m) - market_median['6M']) if returns_6m else None,
                    'alpha_12M': (np.mean(returns_12m) - market_median['12M']) if returns_12m else None,
                    'win_rate_12M': (np.mean([r > 0 for r in returns_12m]) * 100) if returns_12m else None,
                }
                results['quarterly_results'].append(quarter_result)

                # Store individual picks
                for _, row in matches.iterrows():
                    symbol = row['symbol']
                    pick = {
                        'quarter': str(q_date),
                        'symbol': symbol,
                        'company_name': row.get('company_name', symbol),
                        'sector': row.get('sector', ''),
                        'roe': row.get('roe'),
                        'current_ratio': row.get('current_ratio'),
                        'net_margin': row.get('net_margin'),
                        'return_3M': forward_returns[symbol]['3M'],
                        'return_6M': forward_returns[symbol]['6M'],
                        'return_12M': forward_returns[symbol]['12M'],
                    }
                    results['all_picks'].append(pick)

            return results

        finally:
            await self.close()


def print_results(results: Dict):
    """Print backtest results."""
    print("\n" + "=" * 100)
    print(f"SCREENER BACKTEST: {results['name']}")
    print(f"Lag: {results['lag_days']} days (NO LOOK-AHEAD BIAS)")
    print("=" * 100)

    df = pd.DataFrame(results['quarterly_results'])

    if df.empty:
        print("No results - screener found no matches")
        return

    # Quarterly breakdown
    print(f"\n{'Quarter':<12} {'Matches':>8} {'Avg 12M':>10} {'Mkt Med':>10} {'Alpha':>10} {'Win Rate':>10}")
    print("-" * 70)

    for _, row in df.iterrows():
        avg_12m = f"{row['avg_return_12M']:+.1f}%" if row['avg_return_12M'] is not None else "N/A"
        mkt_12m = f"{row['market_median_12M']:+.1f}%" if row['market_median_12M'] is not None else "N/A"
        alpha = f"{row['alpha_12M']:+.1f}%" if row['alpha_12M'] is not None else "N/A"
        win_rate = f"{row['win_rate_12M']:.0f}%" if row['win_rate_12M'] is not None else "N/A"

        print(f"{row['quarter']:<12} {row['matches']:>8} {avg_12m:>10} {mkt_12m:>10} {alpha:>10} {win_rate:>10}")

    # Summary
    print("\n" + "-" * 70)
    print("SUMMARY (Mean)")
    print("-" * 70)

    valid_alpha_3m = df['alpha_3M'].dropna()
    valid_alpha_6m = df['alpha_6M'].dropna()
    valid_alpha_12m = df['alpha_12M'].dropna()

    print(f"  Avg Alpha 3M:  {valid_alpha_3m.mean():+.1f}%" if len(valid_alpha_3m) > 0 else "  Avg Alpha 3M:  N/A")
    print(f"  Avg Alpha 6M:  {valid_alpha_6m.mean():+.1f}%" if len(valid_alpha_6m) > 0 else "  Avg Alpha 6M:  N/A")
    print(f"  Avg Alpha 12M: {valid_alpha_12m.mean():+.1f}%" if len(valid_alpha_12m) > 0 else "  Avg Alpha 12M: N/A")
    print(f"  Win Rate 12M:  {(valid_alpha_12m > 0).mean() * 100:.0f}%" if len(valid_alpha_12m) > 0 else "  Win Rate 12M:  N/A")
    print(f"  Avg Matches:   {df['matches'].mean():.1f} companies per quarter")

    print("\n" + "-" * 70)
    print("SUMMARY (Median - robust to outliers)")
    print("-" * 70)

    print(f"  Med Alpha 3M:  {valid_alpha_3m.median():+.1f}%" if len(valid_alpha_3m) > 0 else "  Med Alpha 3M:  N/A")
    print(f"  Med Alpha 6M:  {valid_alpha_6m.median():+.1f}%" if len(valid_alpha_6m) > 0 else "  Med Alpha 6M:  N/A")
    print(f"  Med Alpha 12M: {valid_alpha_12m.median():+.1f}%" if len(valid_alpha_12m) > 0 else "  Med Alpha 12M: N/A")

    # Top/Bottom picks
    picks_df = pd.DataFrame(results['all_picks'])
    if not picks_df.empty and 'return_12M' in picks_df.columns:
        valid_picks = picks_df.dropna(subset=['return_12M'])

        if len(valid_picks) > 0:
            print("\n" + "-" * 70)
            print("TOP 10 PICKS (12M Return)")
            print("-" * 70)
            top_picks = valid_picks.nlargest(10, 'return_12M')
            for _, p in top_picks.iterrows():
                print(f"  {p['quarter']} {p['company_name'][:30]:<32} {p['return_12M']:+.1f}%")

            print("\n" + "-" * 70)
            print("BOTTOM 10 PICKS (12M Return)")
            print("-" * 70)
            bottom_picks = valid_picks.nsmallest(10, 'return_12M')
            for _, p in bottom_picks.iterrows():
                print(f"  {p['quarter']} {p['company_name'][:30]:<32} {p['return_12M']:+.1f}%")


async def compare_to_fat_pitch(screener_results: Dict):
    """Compare screener to Fat Pitch equal weights."""
    print("\n" + "=" * 100)
    print("COMPARISON: Screener vs Fat Pitch (Equal Weights)")
    print("=" * 100)

    screener_df = pd.DataFrame(screener_results['quarterly_results'])

    if screener_df.empty or 'alpha_12M' not in screener_df.columns:
        print("\nNo screener results to compare")
        return

    valid_alpha = screener_df['alpha_12M'].dropna()
    if len(valid_alpha) == 0:
        print("\nNo valid alpha data to compare")
        return

    # Fat Pitch results (from earlier runs)
    # Equal weights: +7.9% median alpha, 83% win rate
    print(f"\n{'Strategy':<25} {'Med Alpha 12M':>15} {'Win Rate 12M':>15}")
    print("-" * 60)

    screener_alpha = valid_alpha.median()
    screener_win = (valid_alpha > 0).mean() * 100

    print(f"{screener_results['name']:<25} {screener_alpha:>+14.1f}% {screener_win:>14.0f}%")
    print(f"{'Fat Pitch (Equal)':<25} {'+7.9%':>15} {'83%':>15}")

    diff = screener_alpha - 7.9
    print(f"\n{'Difference':<25} {diff:>+14.1f}%")

    if diff > 0:
        print(f"\n>>> {screener_results['name']} BEATS Fat Pitch by {diff:.1f}%")
    else:
        print(f"\n>>> Fat Pitch BEATS {screener_results['name']} by {-diff:.1f}%")


async def main():
    parser = argparse.ArgumentParser(description='Screener Backtest')
    parser.add_argument('--screener', type=str, default='quality_simple',
                        choices=list(SCREENERS.keys()),
                        help='Screener to backtest')
    parser.add_argument('--list', action='store_true', help='List available screeners')
    parser.add_argument('--lag', type=int, default=60,
                        help='Days to lag data (60 = no look-ahead bias)')
    parser.add_argument('--compare', action='store_true',
                        help='Compare to Fat Pitch')

    args = parser.parse_args()

    if args.list:
        print("\nAvailable Screeners:")
        print("-" * 60)
        for sid, s in SCREENERS.items():
            print(f"\n  {sid}:")
            print(f"    {s['name']}")
            print(f"    {s['description']}")
        return

    backtester = ScreenerBacktester(args.screener, lag_days=args.lag)
    results = await backtester.run_backtest()
    print_results(results)

    if args.compare:
        await compare_to_fat_pitch(results)


if __name__ == '__main__':
    asyncio.run(main())
