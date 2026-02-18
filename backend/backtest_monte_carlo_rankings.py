#!/usr/bin/env python3
"""
Monte Carlo DCF Ranking Backtest

Point-in-time backtest from 2022-2025:
1. Calculate rankings using only data available at each date
2. Measure forward returns (1m, 3m, 6m, 12m)
3. Compare predicted vs actual financials
4. Analyze if top-ranked stocks outperformed

NO LOOK-AHEAD BIAS - all data constrained to valuation date
"""

import asyncio
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters, CompanyFinancials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


@dataclass
class BacktestResult:
    """Results for a single company at a single valuation date"""
    symbol: str
    valuation_date: datetime

    # Predictions (from Monte Carlo)
    predicted_fair_value_p10: float
    predicted_fair_value_p50: float
    predicted_fair_value_p90: float
    predicted_yield_p50: float  # (fv_p50 / price - 1)

    # Inputs used
    revenue_at_valuation: float
    margin_at_valuation: float
    price_at_valuation: float

    # Actual forward returns
    return_1m: Optional[float] = None
    return_3m: Optional[float] = None
    return_6m: Optional[float] = None
    return_12m: Optional[float] = None

    # Actual vs predicted (for accuracy check)
    actual_revenue_1y: Optional[float] = None
    actual_margin_1y: Optional[float] = None
    predicted_revenue_1y: Optional[float] = None
    predicted_margin_1y: Optional[float] = None

    # Ranking
    mc_rank: int = 0
    cash_cow_rank: int = 0
    data_quality: str = "OK"


class MonteCarloBacktester:

    def __init__(self):
        self.conn = None
        self.dcf_engine = None

    async def setup(self):
        """Initialize connections"""
        self.conn = await asyncpg.connect(DATABASE_URL)

        params = DCFParameters(
            num_simulations=1000,  # Fewer for speed in backtest
            projection_years=10,
            terminal_growth_rate=0.025,
            risk_free_rate=0.03,
            market_premium=0.07
        )
        self.dcf_engine = DCFMonteCarloFixed(params)
        self.dcf_engine.db_conn = self.conn

    async def get_companies_with_data(self, valuation_date: datetime) -> List[Dict]:
        """Get companies with sufficient data at valuation date"""

        query = """
        WITH company_data AS (
            SELECT
                cm.id as company_id,
                cm.company_name,
                cm.primary_ticker,
                cm.yahoo_symbol,
                cm.stock_currency,
                fs.symbol as fs_symbol,
                COUNT(DISTINCT fs.period_date) as statement_count,
                MAX(fs.period_date) as latest_statement
            FROM company_master cm
            INNER JOIN financial_statements fs ON (
                fs.symbol = cm.primary_ticker
                OR fs.symbol = REPLACE(cm.primary_ticker, '-', ' ')
            )
            WHERE fs.period_date <= $1
            AND fs.total_revenue > 0
            GROUP BY cm.id, cm.company_name, cm.primary_ticker, cm.yahoo_symbol, cm.stock_currency, fs.symbol
            HAVING COUNT(DISTINCT fs.period_date) >= 4
        )
        SELECT
            cd.*,
            dp.close_price as price_at_date,
            dp.volume
        FROM company_data cd
        INNER JOIN daily_price_data dp ON dp.symbol = cd.primary_ticker
        WHERE dp.date <= $1
        AND dp.date >= $1 - INTERVAL '7 days'
        AND dp.close_price > 0
        ORDER BY dp.date DESC
        """

        rows = await self.conn.fetch(query, valuation_date.date())

        # Deduplicate by primary_ticker (take most recent price)
        seen = set()
        companies = []
        for row in rows:
            ticker = row['primary_ticker']
            if ticker not in seen:
                seen.add(ticker)
                companies.append(dict(row))

        return companies

    async def get_price_at_date(self, symbol: str, target_date: datetime) -> Optional[float]:
        """Get price at or before target date (no look-ahead)"""

        query = """
        SELECT close_price
        FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
        """

        result = await self.conn.fetchval(query, symbol, target_date.date())
        return float(result) if result else None

    async def get_forward_returns(self, symbol: str, start_date: datetime) -> Dict[str, Optional[float]]:
        """Calculate forward returns from start_date"""

        start_price = await self.get_price_at_date(symbol, start_date)
        if not start_price:
            return {'1m': None, '3m': None, '6m': None, '12m': None}

        returns = {}

        for label, months in [('1m', 1), ('3m', 3), ('6m', 6), ('12m', 12)]:
            future_date = start_date + timedelta(days=months * 30)
            future_price = await self.get_price_at_date(symbol, future_date)

            if future_price:
                returns[label] = (future_price / start_price - 1) * 100
            else:
                returns[label] = None

        return returns

    async def get_actual_financials_1y_later(self, symbol: str, valuation_date: datetime) -> Dict:
        """Get actual financials 1 year after valuation (for accuracy check)"""

        future_date = valuation_date + timedelta(days=365)

        # Get revenue and margin from financial statements published after 1 year
        query = """
        SELECT
            total_revenue,
            operating_income,
            operating_income / NULLIF(total_revenue, 0) as operating_margin,
            period_date
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
        AND period_date <= $3
        AND period_date >= $3 - INTERVAL '6 months'
        ORDER BY period_date DESC
        LIMIT 1
        """

        symbol_space = symbol.replace('-', ' ')
        result = await self.conn.fetchrow(query, symbol, symbol_space, future_date.date())

        if result:
            return {
                'revenue': float(result['total_revenue']) if result['total_revenue'] else None,
                'margin': float(result['operating_margin']) if result['operating_margin'] else None
            }
        return {'revenue': None, 'margin': None}

    async def run_valuation_at_date(self, company: Dict, valuation_date: datetime) -> Optional[BacktestResult]:
        """Run DCF valuation for a company at a specific date"""

        symbol = company['fs_symbol']
        price = company['price_at_date']

        try:
            # Get financials available at valuation date
            financials = await self.dcf_engine.get_real_financials(symbol, valuation_date)

            if not financials:
                return None

            # Calculate distributions
            growth_dist = self.dcf_engine.calculate_growth_distribution(financials.revenues_ttm)
            margin_dist = self.dcf_engine.calculate_margin_distribution(financials.operating_margins)
            interest_dist = self.dcf_engine.calculate_interest_distribution(
                financials.interest_expenses, financials.revenues_ttm
            )

            # Calculate WACC
            market_cap = price * financials.shares_outstanding
            wacc = self.dcf_engine.calculate_wacc(financials.beta, financials.net_debt, market_cap)

            # Run Monte Carlo
            fair_values = []
            for _ in range(self.dcf_engine.params.num_simulations):
                fv = self.dcf_engine.run_single_simulation(
                    financials, growth_dist, margin_dist, interest_dist, wacc
                )
                fair_values.append(fv)

            fair_values = np.array(fair_values)

            # Calculate predicted revenue 1 year out (for accuracy check)
            # Using mean growth rate from distribution
            predicted_revenue_1y = financials.revenues_ttm[0] * (1 + growth_dist['mean'])

            # Get forward returns
            forward_returns = await self.get_forward_returns(company['primary_ticker'], valuation_date)

            # Get actual financials 1 year later (for accuracy check)
            actual_1y = await self.get_actual_financials_1y_later(symbol, valuation_date)

            # Data quality flags
            quality_issues = []
            if price < 1.0:
                quality_issues.append('penny_stock')
            if market_cap < 10_000_000:  # < 10M
                quality_issues.append('tiny_mcap')
            if len(financials.revenues_ttm) < 4:
                quality_issues.append('few_datapoints')

            return BacktestResult(
                symbol=company['primary_ticker'],
                valuation_date=valuation_date,
                predicted_fair_value_p10=np.percentile(fair_values, 10),
                predicted_fair_value_p50=np.percentile(fair_values, 50),
                predicted_fair_value_p90=np.percentile(fair_values, 90),
                predicted_yield_p50=(np.percentile(fair_values, 50) / price - 1) * 100,
                revenue_at_valuation=financials.revenues_ttm[0],
                margin_at_valuation=margin_dist['mean'],
                price_at_valuation=price,
                return_1m=forward_returns['1m'],
                return_3m=forward_returns['3m'],
                return_6m=forward_returns['6m'],
                return_12m=forward_returns['12m'],
                actual_revenue_1y=actual_1y['revenue'],
                actual_margin_1y=actual_1y['margin'],
                predicted_revenue_1y=predicted_revenue_1y,
                predicted_margin_1y=margin_dist['mean'],  # Assume margin stays same
                data_quality='|'.join(quality_issues) if quality_issues else 'OK'
            )

        except Exception as e:
            logger.warning(f"Error valuing {symbol} at {valuation_date}: {e}")
            return None

    def calculate_rankings(self, results: List[BacktestResult]) -> List[BacktestResult]:
        """Calculate MC rank and cash cow rank with quality adjustments"""

        # MC rank: log(yield) * quality_factor
        def mc_score(r: BacktestResult) -> float:
            if r.predicted_yield_p50 <= 0:
                return -999

            base_score = np.log(1 + r.predicted_yield_p50)

            # Quality penalty
            if 'tiny_mcap' in r.data_quality:
                factor = 0.3
            elif 'penny_stock' in r.data_quality:
                factor = 0.5
            elif 'few_datapoints' in r.data_quality:
                factor = 0.7
            else:
                factor = 1.0

            return base_score * factor

        # Cash cow rank: log(OCF/NI) * margin_factor
        # We don't have OCF/NI in backtest, so use margin as proxy
        def cash_cow_score(r: BacktestResult) -> float:
            margin = r.margin_at_valuation

            if margin <= 0:
                return margin  # Negative = bad
            else:
                return np.log(1 + margin * 10)  # Scale margin for ranking

        # Calculate scores
        for r in results:
            r._mc_score = mc_score(r)
            r._cash_score = cash_cow_score(r)

        # Rank by MC score (higher = better = rank 1)
        sorted_by_mc = sorted(results, key=lambda x: x._mc_score, reverse=True)
        for i, r in enumerate(sorted_by_mc):
            r.mc_rank = i + 1

        # Rank by cash cow score
        sorted_by_cash = sorted(results, key=lambda x: x._cash_score, reverse=True)
        for i, r in enumerate(sorted_by_cash):
            r.cash_cow_rank = i + 1

        return results

    async def run_backtest(self, start_date: datetime, end_date: datetime,
                          frequency: str = 'quarterly') -> pd.DataFrame:
        """Run full backtest from start_date to end_date"""

        # Generate valuation dates
        dates = []
        current = start_date

        while current <= end_date:
            dates.append(current)
            if frequency == 'quarterly':
                current = current + timedelta(days=91)
            elif frequency == 'monthly':
                current = current + timedelta(days=30)
            else:
                current = current + timedelta(days=365)

        print(f"\n📊 Running Monte Carlo Backtest")
        print(f"   Period: {start_date.date()} to {end_date.date()}")
        print(f"   Valuation dates: {len(dates)}")

        all_results = []

        for val_date in dates:
            print(f"\n🔄 Processing {val_date.date()}...")

            # Get companies with data at this date
            companies = await self.get_companies_with_data(val_date)
            print(f"   Found {len(companies)} companies with data")

            # Run valuations
            period_results = []
            for i, company in enumerate(companies[:200]):  # Limit for speed
                result = await self.run_valuation_at_date(company, val_date)
                if result:
                    period_results.append(result)

                if (i + 1) % 50 == 0:
                    print(f"   Processed {i+1}/{min(len(companies), 200)} companies")

            # Calculate rankings for this period
            period_results = self.calculate_rankings(period_results)
            all_results.extend(period_results)

            print(f"   ✓ {len(period_results)} valuations completed")

        # Convert to DataFrame
        df = pd.DataFrame([vars(r) for r in all_results])

        # Clean up internal score columns
        if '_mc_score' in df.columns:
            df = df.drop(columns=['_mc_score', '_cash_score'])

        return df

    def analyze_results(self, df: pd.DataFrame) -> Dict:
        """Analyze backtest results"""

        analysis = {}

        # 1. Does ranking predict forward returns?
        print("\n" + "="*60)
        print("📈 RANKING VS FORWARD RETURNS ANALYSIS")
        print("="*60)

        for horizon in ['return_1m', 'return_3m', 'return_6m', 'return_12m']:
            valid = df[df[horizon].notna()]
            if len(valid) < 50:
                continue

            # Correlation between rank and return
            corr = valid['mc_rank'].corr(valid[horizon])

            # Top decile vs bottom decile
            n = len(valid)
            top_decile = valid.nsmallest(n // 10, 'mc_rank')[horizon].mean()
            bottom_decile = valid.nlargest(n // 10, 'mc_rank')[horizon].mean()

            print(f"\n{horizon}:")
            print(f"  Rank-Return Correlation: {corr:.3f} (negative = good)")
            print(f"  Top 10% avg return: {top_decile:.1f}%")
            print(f"  Bottom 10% avg return: {bottom_decile:.1f}%")
            print(f"  Spread: {top_decile - bottom_decile:.1f}%")

            analysis[horizon] = {
                'correlation': corr,
                'top_decile_return': top_decile,
                'bottom_decile_return': bottom_decile,
                'spread': top_decile - bottom_decile
            }

        # 2. Estimate accuracy
        print("\n" + "="*60)
        print("🎯 ESTIMATE ACCURACY (Predicted vs Actual)")
        print("="*60)

        valid_revenue = df[(df['actual_revenue_1y'].notna()) & (df['predicted_revenue_1y'].notna())]
        if len(valid_revenue) > 0:
            # Calculate prediction error
            valid_revenue = valid_revenue.copy()
            valid_revenue['revenue_error'] = (
                (valid_revenue['actual_revenue_1y'] - valid_revenue['predicted_revenue_1y'])
                / valid_revenue['predicted_revenue_1y'] * 100
            )

            # Remove extreme outliers
            valid_revenue = valid_revenue[abs(valid_revenue['revenue_error']) < 200]

            print(f"\nRevenue Predictions (n={len(valid_revenue)}):")
            print(f"  Mean Absolute Error: {valid_revenue['revenue_error'].abs().mean():.1f}%")
            print(f"  Median Error: {valid_revenue['revenue_error'].median():.1f}%")
            print(f"  % within ±20%: {(abs(valid_revenue['revenue_error']) <= 20).mean()*100:.1f}%")
            print(f"  % within ±50%: {(abs(valid_revenue['revenue_error']) <= 50).mean()*100:.1f}%")

            analysis['revenue_accuracy'] = {
                'mae': valid_revenue['revenue_error'].abs().mean(),
                'median_error': valid_revenue['revenue_error'].median(),
                'within_20pct': (abs(valid_revenue['revenue_error']) <= 20).mean() * 100,
                'within_50pct': (abs(valid_revenue['revenue_error']) <= 50).mean() * 100
            }

        # 3. By valuation date
        print("\n" + "="*60)
        print("📅 PERFORMANCE BY VALUATION DATE")
        print("="*60)

        by_date = df.groupby('valuation_date').agg({
            'return_12m': ['mean', 'median', 'count'],
            'mc_rank': 'count'
        }).round(2)
        print(by_date)

        # 4. Data quality impact
        print("\n" + "="*60)
        print("🔍 DATA QUALITY IMPACT")
        print("="*60)

        quality_groups = df.groupby('data_quality').agg({
            'return_12m': ['mean', 'median', 'count'],
            'predicted_yield_p50': 'mean'
        }).round(2)
        print(quality_groups)

        return analysis

    async def cleanup(self):
        """Close connections"""
        if self.conn:
            await self.conn.close()


async def main():
    """Run the Monte Carlo ranking backtest"""

    backtester = MonteCarloBacktester()

    try:
        await backtester.setup()

        # Run backtest from Q1 2022 to Q4 2024
        # (Q4 2024 needs 12 months forward, so cutoff is end of 2024)
        start_date = datetime(2022, 1, 1)
        end_date = datetime(2024, 10, 1)  # Last date with 12m forward data

        df = await backtester.run_backtest(start_date, end_date, frequency='quarterly')

        # Save raw results
        df.to_csv('monte_carlo_backtest_results.csv', index=False)
        print(f"\n💾 Saved {len(df)} results to monte_carlo_backtest_results.csv")

        # Analyze
        analysis = backtester.analyze_results(df)

        # Save to Excel with multiple sheets
        with pd.ExcelWriter('monte_carlo_backtest_analysis.xlsx', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Raw Results', index=False)

            # Summary by date
            summary = df.groupby('valuation_date').agg({
                'symbol': 'count',
                'return_1m': 'mean',
                'return_3m': 'mean',
                'return_6m': 'mean',
                'return_12m': 'mean',
                'predicted_yield_p50': 'mean'
            }).round(2)
            summary.columns = ['Companies', 'Avg 1M Return', 'Avg 3M Return',
                             'Avg 6M Return', 'Avg 12M Return', 'Avg Predicted Yield']
            summary.to_excel(writer, sheet_name='Summary by Date')

            # Top performers analysis
            top_decile = df[df['mc_rank'] <= df.groupby('valuation_date')['mc_rank'].transform('max') * 0.1]
            top_decile.to_excel(writer, sheet_name='Top Decile', index=False)

        print(f"\n📊 Saved analysis to monte_carlo_backtest_analysis.xlsx")

    finally:
        await backtester.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
