#!/usr/bin/env python3
"""
Monte Carlo Ranking Backtest

Replicates the exact Monte Carlo ranking approach from monte_carlo_rankings_v11:
1. Get historical revenue growth & margin distributions
2. Run Monte Carlo simulation to project EPS
3. Calculate yield percentiles (eps/price)
4. Apply log scaling and quality adjustments
5. Measure forward returns to validate the ranking

NO LOOK-AHEAD BIAS - all data constrained to valuation date
"""

import asyncio
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Monte Carlo parameters (same as original)
NUM_SIMULATIONS = 1000
PROJECTION_YEARS = 5


@dataclass
class MCValuation:
    """Single Monte Carlo valuation result"""
    ticker: str
    company: str
    valuation_date: datetime

    # Price & Market
    price: float
    market_cap_b: float

    # Input distributions (historical)
    rev_growth_mean: float
    rev_growth_std: float
    net_margin_mean: float
    net_margin_std: float
    data_points: int

    # Monte Carlo outputs
    eps_p10: float
    eps_p50: float
    eps_p90: float
    yield_p10: float
    yield_p50: float
    yield_p90: float

    # Adjusted (log-scaled)
    yield_p50_log: float

    # Quality
    data_quality: str
    quality_factor: float

    # Final rank score
    mc_score: float

    # Forward returns (filled later)
    return_1m: Optional[float] = None
    return_3m: Optional[float] = None
    return_6m: Optional[float] = None
    return_12m: Optional[float] = None

    # Estimate accuracy
    actual_eps_1y: Optional[float] = None
    eps_prediction_error: Optional[float] = None


class MCRankingBacktest:

    def __init__(self):
        self.conn = None

    async def setup(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def get_companies_at_date(self, valuation_date: datetime) -> List[Dict]:
        """Get companies with financial data available at valuation_date"""

        query = """
        WITH fs_data AS (
            SELECT
                fs.symbol,
                cm.company_name,
                cm.yahoo_symbol,
                fs.fiscal_year,
                fs.total_revenue,
                fs.net_income,
                fs.operating_income / NULLIF(fs.total_revenue, 0) as op_margin,
                fs.net_income / NULLIF(fs.total_revenue, 0) as net_margin,
                cm.stock_currency,
                COALESCE(cm.market_cap_usd, 0) / 1e9 as market_cap_b
            FROM financial_statements fs
            INNER JOIN company_master cm ON cm.primary_ticker = fs.symbol
            WHERE fs.period_date <= $1
            AND fs.total_revenue > 0
        ),
        growth_calc AS (
            SELECT
                symbol,
                company_name,
                yahoo_symbol,
                stock_currency,
                market_cap_b,
                fiscal_year,
                total_revenue,
                net_income,
                net_margin,
                LAG(total_revenue) OVER (PARTITION BY symbol ORDER BY fiscal_year) as prev_revenue,
                LAG(net_income) OVER (PARTITION BY symbol ORDER BY fiscal_year) as prev_net_income
            FROM fs_data
        ),
        company_stats AS (
            SELECT
                symbol,
                company_name,
                yahoo_symbol,
                stock_currency,
                MAX(market_cap_b) as market_cap_b,
                AVG((total_revenue / NULLIF(prev_revenue, 0) - 1) * 100) as rev_growth_mean,
                STDDEV((total_revenue / NULLIF(prev_revenue, 0) - 1) * 100) as rev_growth_std,
                AVG(net_margin * 100) as net_margin_mean,
                STDDEV(net_margin * 100) as net_margin_std,
                MAX(total_revenue) as latest_revenue,
                MAX(net_income) as latest_net_income,
                COUNT(*) as data_points
            FROM growth_calc
            WHERE prev_revenue IS NOT NULL
            GROUP BY symbol, company_name, yahoo_symbol, stock_currency
            HAVING COUNT(*) >= 2
        )
        SELECT
            cs.*,
            dp.close_price as price,
            dp.volume
        FROM company_stats cs
        INNER JOIN (
            SELECT symbol, close_price, volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
            FROM daily_price_data
            WHERE date <= $1
        ) dp ON dp.symbol = cs.symbol AND dp.rn = 1
        WHERE dp.close_price > 0
        """

        rows = await self.conn.fetch(query, valuation_date.date())
        return [dict(r) for r in rows]

    def run_monte_carlo(self, rev_growth_mean: float, rev_growth_std: float,
                        net_margin_mean: float, net_margin_std: float,
                        latest_revenue: float, shares: float = 1e8) -> Dict:
        """
        Run Monte Carlo simulation to project EPS

        Same logic as original:
        1. Sample revenue growth from normal distribution
        2. Sample net margin from normal distribution
        3. Project revenue forward
        4. Calculate net income = revenue × margin
        5. Calculate EPS = net income / shares
        """

        eps_results = []

        # Handle edge cases
        if rev_growth_std is None or np.isnan(rev_growth_std):
            rev_growth_std = abs(rev_growth_mean) * 0.5 + 5  # Default std
        if net_margin_std is None or np.isnan(net_margin_std):
            net_margin_std = abs(net_margin_mean) * 0.3 + 2  # Default std

        # Ensure minimum std to avoid zero variance
        rev_growth_std = max(rev_growth_std, 3)
        net_margin_std = max(net_margin_std, 1)

        for _ in range(NUM_SIMULATIONS):
            # Sample growth rate (% per year)
            growth_rate = np.random.normal(rev_growth_mean, rev_growth_std) / 100
            growth_rate = np.clip(growth_rate, -0.30, 0.50)  # Cap at -30% to +50%

            # Sample net margin (%)
            margin = np.random.normal(net_margin_mean, net_margin_std) / 100
            margin = np.clip(margin, -0.50, 0.50)  # Cap at -50% to +50%

            # Project revenue forward (compound growth)
            projected_revenue = latest_revenue
            for year in range(PROJECTION_YEARS):
                # Decay growth towards 2% terminal rate
                year_growth = growth_rate * (0.8 ** year) + 0.02 * (1 - 0.8 ** year)
                projected_revenue *= (1 + year_growth)

            # Calculate projected net income
            projected_net_income = projected_revenue * margin

            # Calculate EPS
            eps = projected_net_income / shares
            eps_results.append(eps)

        eps_results = np.array(eps_results)

        return {
            'eps_p10': np.percentile(eps_results, 10),
            'eps_p50': np.percentile(eps_results, 50),
            'eps_p90': np.percentile(eps_results, 90)
        }

    def calculate_quality_factor(self, price: float, market_cap_b: float,
                                  data_points: int) -> tuple:
        """
        Calculate quality factor (same as v11 logic)

        Returns (data_quality_string, quality_factor)
        """
        issues = []
        factor = 1.0

        if price < 1.0:
            issues.append('penny_stock')
            factor *= 0.5

        if market_cap_b < 0.01:  # < 10M
            issues.append('tiny_mcap')
            factor *= 0.3

        if data_points < 3:
            issues.append('few_datapoints')
            factor *= 0.7

        quality_str = '|'.join(issues) if issues else 'OK'
        return quality_str, factor

    async def get_forward_return(self, symbol: str, start_date: datetime,
                                  months: int) -> Optional[float]:
        """Get forward return from start_date (no look-ahead)"""

        start_price = await self.conn.fetchval("""
            SELECT close_price FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC LIMIT 1
        """, symbol, start_date.date())

        if not start_price:
            return None

        end_date = start_date + timedelta(days=months * 30)
        end_price = await self.conn.fetchval("""
            SELECT close_price FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC LIMIT 1
        """, symbol, end_date.date())

        if not end_price:
            return None

        return (float(end_price) / float(start_price) - 1) * 100

    async def value_company(self, company: Dict, valuation_date: datetime) -> Optional[MCValuation]:
        """Run full Monte Carlo valuation for a company"""

        ticker = company['symbol']
        price = float(company['price'])

        # Handle nulls
        rev_growth_mean = float(company['rev_growth_mean'] or 0)
        rev_growth_std = float(company['rev_growth_std']) if company['rev_growth_std'] else None
        net_margin_mean = float(company['net_margin_mean'] or 0)
        net_margin_std = float(company['net_margin_std']) if company['net_margin_std'] else None
        latest_revenue = float(company['latest_revenue'] or 0)
        market_cap_b = float(company['market_cap_b'] or 0)

        if latest_revenue <= 0 or price <= 0:
            return None

        # Estimate shares from market cap / price (or use default)
        if market_cap_b > 0:
            shares = (market_cap_b * 1e9) / price
        else:
            shares = 1e8  # Default 100M shares

        # Run Monte Carlo
        mc_result = self.run_monte_carlo(
            rev_growth_mean, rev_growth_std,
            net_margin_mean, net_margin_std,
            latest_revenue, shares
        )

        # Calculate yields (eps / price * 100)
        yield_p10 = (mc_result['eps_p10'] / price) * 100 if price > 0 else 0
        yield_p50 = (mc_result['eps_p50'] / price) * 100 if price > 0 else 0
        yield_p90 = (mc_result['eps_p90'] / price) * 100 if price > 0 else 0

        # Handle negative/zero yields for log
        yield_for_log = max(yield_p50, 0.01)
        yield_p50_log = np.log(1 + yield_for_log)

        # Quality factor
        data_quality, quality_factor = self.calculate_quality_factor(
            price, market_cap_b, company['data_points']
        )

        # Final MC score (same as v11 ranking)
        mc_score = yield_p50_log * quality_factor

        # Get forward returns
        return_1m = await self.get_forward_return(ticker, valuation_date, 1)
        return_3m = await self.get_forward_return(ticker, valuation_date, 3)
        return_6m = await self.get_forward_return(ticker, valuation_date, 6)
        return_12m = await self.get_forward_return(ticker, valuation_date, 12)

        return MCValuation(
            ticker=ticker,
            company=company['company_name'],
            valuation_date=valuation_date,
            price=price,
            market_cap_b=market_cap_b,
            rev_growth_mean=rev_growth_mean,
            rev_growth_std=rev_growth_std or 0,
            net_margin_mean=net_margin_mean,
            net_margin_std=net_margin_std or 0,
            data_points=company['data_points'],
            eps_p10=mc_result['eps_p10'],
            eps_p50=mc_result['eps_p50'],
            eps_p90=mc_result['eps_p90'],
            yield_p10=yield_p10,
            yield_p50=yield_p50,
            yield_p90=yield_p90,
            yield_p50_log=yield_p50_log,
            data_quality=data_quality,
            quality_factor=quality_factor,
            mc_score=mc_score,
            return_1m=return_1m,
            return_3m=return_3m,
            return_6m=return_6m,
            return_12m=return_12m
        )

    async def run_backtest(self, start_year: int = 2022, end_year: int = 2024,
                           frequency: str = 'quarterly') -> pd.DataFrame:
        """Run backtest for multiple historical dates"""

        # Generate valuation dates
        dates = []
        for year in range(start_year, end_year + 1):
            if frequency == 'quarterly':
                for month in [3, 6, 9, 12]:
                    dates.append(datetime(year, month, 1))
            else:
                dates.append(datetime(year, 6, 1))  # Annual mid-year

        # Filter to dates with enough forward data
        cutoff = datetime.now() - timedelta(days=365)
        dates = [d for d in dates if d < cutoff]

        print(f"\n{'='*70}")
        print(f"📊 MONTE CARLO RANKING BACKTEST")
        print(f"{'='*70}")
        print(f"Period: {dates[0].date()} to {dates[-1].date()}")
        print(f"Valuation dates: {len(dates)}")
        print(f"Using same logic as monte_carlo_rankings_v11")

        all_results = []

        for val_date in dates:
            print(f"\n🔄 {val_date.date()}...", end=" ", flush=True)

            companies = await self.get_companies_at_date(val_date)

            period_results = []
            for company in companies:
                result = await self.value_company(company, val_date)
                if result:
                    period_results.append(result)

            # Assign ranks within this period
            period_results.sort(key=lambda x: x.mc_score, reverse=True)
            for i, r in enumerate(period_results):
                r.mc_rank = i + 1

            all_results.extend(period_results)
            print(f"{len(period_results)} companies")

        # Convert to DataFrame
        df = pd.DataFrame([asdict(r) for r in all_results])

        return df

    def analyze_results(self, df: pd.DataFrame):
        """Analyze backtest results"""

        print(f"\n{'='*70}")
        print(f"📈 BACKTEST RESULTS")
        print(f"{'='*70}")
        print(f"Total valuations: {len(df)}")

        # Filter extreme yields for cleaner analysis
        df_clean = df[(df['yield_p50'] > 0) & (df['yield_p50'] < 1000)].copy()
        print(f"After filtering extremes: {len(df_clean)}")

        # 1. Quintile analysis by mc_score
        print(f"\n1️⃣  QUINTILE ANALYSIS (by mc_score)")
        print("-" * 50)

        for horizon in ['return_1m', 'return_3m', 'return_6m', 'return_12m']:
            valid = df_clean[df_clean[horizon].notna()].copy()
            if len(valid) < 100:
                continue

            valid['quintile'] = pd.qcut(
                valid['mc_score'].rank(method='first'), 5,
                labels=['Q1(Low)', 'Q2', 'Q3', 'Q4', 'Q5(High)']
            )

            qr = valid.groupby('quintile', observed=True)[horizon].agg(['mean', 'median', 'count'])

            print(f"\n{horizon}:")
            print(qr.round(2).to_string())

            q5 = valid[valid['quintile'] == 'Q5(High)'][horizon].mean()
            q1 = valid[valid['quintile'] == 'Q1(Low)'][horizon].mean()
            print(f"  Spread (Q5-Q1): {q5-q1:.1f}%")

        # 2. Correlation
        print(f"\n2️⃣  CORRELATION (mc_score vs forward returns)")
        print("-" * 50)
        for h in ['return_1m', 'return_3m', 'return_6m', 'return_12m']:
            valid = df_clean[df_clean[h].notna()]
            if len(valid) > 100:
                corr = valid['mc_score'].corr(valid[h])
                print(f"  {h}: {corr:.3f}")

        # 3. Top 20 picks alpha
        print(f"\n3️⃣  TOP 20 PICKS ALPHA (each period)")
        print("-" * 50)

        df_clean['valuation_date'] = pd.to_datetime(df_clean['valuation_date'])

        def calc_alpha(g):
            top20 = g.nlargest(20, 'mc_score')
            return pd.Series({
                'top20_12m': top20['return_12m'].mean(),
                'all_12m': g['return_12m'].mean(),
                'alpha': top20['return_12m'].mean() - g['return_12m'].mean(),
                'n': len(g)
            })

        by_period = df_clean.groupby('valuation_date').apply(calc_alpha, include_groups=False)
        print(by_period.round(2).to_string())

        avg_alpha = by_period['alpha'].mean()
        win_rate = (by_period['alpha'] > 0).mean() * 100
        print(f"\nAverage Alpha: {avg_alpha:.1f}%")
        print(f"Win Rate: {win_rate:.0f}%")

        # 4. Data quality impact
        print(f"\n4️⃣  DATA QUALITY IMPACT")
        print("-" * 50)
        quality_stats = df_clean.groupby('data_quality').agg({
            'return_12m': ['mean', 'count'],
            'mc_score': 'mean'
        }).round(2)
        print(quality_stats)

    async def cleanup(self):
        if self.conn:
            await self.conn.close()


async def main():
    bt = MCRankingBacktest()

    try:
        await bt.setup()

        df = await bt.run_backtest(start_year=2022, end_year=2024)

        # Save results
        df.to_csv('mc_ranking_backtest.csv', index=False)
        df.to_excel('mc_ranking_backtest.xlsx', index=False)
        print(f"\n💾 Saved {len(df)} results")

        # Analyze
        bt.analyze_results(df)

    finally:
        await bt.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
