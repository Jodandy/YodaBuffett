#!/usr/bin/env python3
"""
Recalibrated Monte Carlo Ranking Backtest

Key changes from original:
1. Wider uncertainty bands calibrated to empirical data
2. Log-normal multipliers for realistic tails
3. Company-specific shock factor
4. Target: 80% of actuals within P10-P90 range
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

# Calibrated from empirical data analysis:
# Actual revenue outcomes: P10=0.25x, P50=1.00x, P90=1.37x
# Log-space std: 0.696
CALIBRATED_LOG_STD = 0.70  # Empirical log-space std deviation
CALIBRATED_LOG_BIAS = -0.10  # Slight upward bias in predictions


@dataclass
class CompanyValuation:
    symbol: str
    company_name: str
    valuation_date: datetime

    # Price & Market
    price: float
    market_cap: Optional[float]

    # Financials at valuation date
    revenue: float
    operating_margin: float
    revenue_growth_hist: float

    # Monte Carlo outputs
    predicted_yield: float
    fair_value_p10: float
    fair_value_p50: float
    fair_value_p90: float

    # Revenue predictions (for calibration check)
    yield_p10: float = 0
    yield_p50: float = 0
    yield_p90: float = 0

    # Forward returns
    return_1m: Optional[float] = None
    return_3m: Optional[float] = None
    return_6m: Optional[float] = None
    return_12m: Optional[float] = None

    # Estimate accuracy
    actual_revenue_1y: Optional[float] = None
    predicted_revenue_1y: Optional[float] = None
    revenue_prediction_error: Optional[float] = None

    # Quality
    data_quality: str = "OK"
    years_of_data: int = 0


class CalibratedBacktester:

    def __init__(self):
        self.conn = None

    async def setup(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def get_companies_at_date(self, valuation_date: datetime, min_years: int = 2) -> List[Dict]:
        """Get companies with sufficient financial data at valuation date"""

        query = """
        WITH fs_data AS (
            SELECT
                symbol,
                COUNT(DISTINCT fiscal_year) as years_of_data,
                MAX(period_date) as latest_period
            FROM financial_statements
            WHERE period_date <= $1
            AND total_revenue > 0
            GROUP BY symbol
            HAVING COUNT(DISTINCT fiscal_year) >= $2
        )
        SELECT
            fd.symbol,
            cm.company_name,
            cm.yahoo_symbol,
            fd.years_of_data,
            fd.latest_period,
            dp.close_price as price,
            dp.date as price_date
        FROM fs_data fd
        INNER JOIN company_master cm ON cm.primary_ticker = fd.symbol
        INNER JOIN (
            SELECT symbol, close_price, date,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
            FROM daily_price_data
            WHERE date <= $1
        ) dp ON dp.symbol = fd.symbol AND dp.rn = 1
        WHERE dp.close_price > 0.5
        ORDER BY fd.years_of_data DESC
        """

        rows = await self.conn.fetch(query, valuation_date.date(), min_years)
        return [dict(r) for r in rows]

    async def get_financials_at_date(self, symbol: str, valuation_date: datetime) -> Optional[Dict]:
        """Get revenue, margins, and historical growth at valuation date"""

        query = """
        SELECT
            fiscal_year,
            total_revenue,
            operating_income,
            operating_income / NULLIF(total_revenue, 0) as operating_margin
        FROM financial_statements
        WHERE symbol = $1
        AND period_date <= $2
        AND total_revenue > 0
        ORDER BY fiscal_year DESC
        LIMIT 5
        """

        rows = await self.conn.fetch(query, symbol, valuation_date.date())

        if not rows:
            return None

        revenues = [float(r['total_revenue']) for r in rows if r['total_revenue']]
        margins = [float(r['operating_margin']) for r in rows if r['operating_margin']]

        if not revenues:
            return None

        # Calculate historical growth rate with more history
        if len(revenues) >= 2:
            growth_rates = []
            for i in range(len(revenues) - 1):
                if revenues[i+1] > 0:
                    growth = (revenues[i] / revenues[i+1]) - 1
                    growth_rates.append(growth)
            avg_growth = np.mean(growth_rates) if growth_rates else 0
            growth_std = np.std(growth_rates) if len(growth_rates) >= 2 else abs(avg_growth) * 0.5
        else:
            avg_growth = 0
            growth_std = 0.15

        # Margin volatility
        margin_std = np.std(margins) if len(margins) >= 2 else abs(margins[0]) * 0.3 if margins else 0.05

        return {
            'revenue': revenues[0],
            'operating_margin': margins[0] if margins else 0,
            'revenue_growth_hist': avg_growth,
            'growth_std': growth_std,
            'margin_std': margin_std,
            'years_of_data': len(revenues)
        }

    async def get_forward_return(self, symbol: str, start_date: datetime, months: int) -> Optional[float]:
        """Get forward return from start_date"""

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

    async def get_actual_revenue_1y(self, symbol: str, valuation_date: datetime) -> Optional[float]:
        """Get actual revenue 1 year after valuation date"""

        future_date = valuation_date + timedelta(days=400)

        revenue = await self.conn.fetchval("""
            SELECT total_revenue FROM financial_statements
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date > $3
            AND total_revenue > 0
            ORDER BY period_date DESC LIMIT 1
        """, symbol, future_date.date(), valuation_date.date())

        return float(revenue) if revenue else None

    def run_calibrated_dcf(self, revenue: float, margin: float, growth: float,
                           growth_std: float, margin_std: float,
                           price: float, shares: float = 1e6) -> Dict:
        """
        Calibrated Monte Carlo DCF with wider uncertainty bands

        Key insight: The final fair value uncertainty should match empirical
        forecast error distribution (log-space std ~0.70)
        """

        fair_values = []
        projected_revenues_1y = []

        for _ in range(1000):
            # --- CALIBRATED UNCERTAINTY PARAMETERS ---

            # 1. Growth uncertainty - use actual historical std, with minimum floor
            sim_growth_std = max(growth_std, 0.15)  # At least 15% std
            sim_growth = np.random.normal(growth, sim_growth_std)

            # Wider bounds - real outcomes range from -50% to +60%
            sim_growth = np.clip(sim_growth, -0.50, 0.60)

            # 2. Margin uncertainty - wider range
            sim_margin_std = max(margin_std, 0.05)  # At least 5pp std
            sim_margin = np.random.normal(margin, sim_margin_std)
            sim_margin = np.clip(sim_margin, -0.30, 0.50)

            # 3. Company-specific shock factor (regime changes, acquisitions, etc.)
            # This is the key calibration factor
            # Log-normal shock: median=1.0, but can range from 0.3x to 3x
            shock_log = np.random.normal(CALIBRATED_LOG_BIAS, CALIBRATED_LOG_STD * 0.5)
            company_shock = np.exp(shock_log)
            company_shock = np.clip(company_shock, 0.25, 4.0)

            # 5-year projection
            projected_revenue = revenue
            total_earnings = 0
            discount_rate = 0.10

            for year in range(1, 6):
                # Decay growth towards 2.5%
                year_growth = sim_growth * (0.8 ** (year-1)) + 0.025 * (1 - 0.8 ** (year-1))
                projected_revenue *= (1 + year_growth)
                earnings = projected_revenue * sim_margin * 0.78
                total_earnings += earnings / (1 + discount_rate) ** year

                # Track 1-year revenue for calibration
                if year == 1:
                    projected_revenues_1y.append(projected_revenue * company_shock)

            # Terminal value
            terminal_earnings = projected_revenue * sim_margin * 0.78 * 1.025
            terminal_value = terminal_earnings / (discount_rate - 0.025)
            total_value = total_earnings + terminal_value / (1 + discount_rate) ** 5

            # Apply company shock to fair value
            fair_value_per_share = (total_value * company_shock) / shares
            fair_values.append(fair_value_per_share)

        fair_values = np.array(fair_values)
        projected_revenues_1y = np.array(projected_revenues_1y)

        # Calculate yield percentiles
        p10_fv = np.percentile(fair_values, 10)
        p50_fv = np.percentile(fair_values, 50)
        p90_fv = np.percentile(fair_values, 90)

        return {
            'fair_value_p10': p10_fv,
            'fair_value_p50': p50_fv,
            'fair_value_p90': p90_fv,
            'predicted_yield': (p50_fv / price - 1) * 100,
            'yield_p10': (p10_fv / price - 1) * 100,
            'yield_p50': (p50_fv / price - 1) * 100,
            'yield_p90': (p90_fv / price - 1) * 100,
            'predicted_rev_1y_p10': np.percentile(projected_revenues_1y, 10),
            'predicted_rev_1y_p50': np.percentile(projected_revenues_1y, 50),
            'predicted_rev_1y_p90': np.percentile(projected_revenues_1y, 90),
        }

    async def value_company(self, company: Dict, valuation_date: datetime) -> Optional[CompanyValuation]:
        """Run full valuation for a company"""

        symbol = company['symbol']
        price = float(company['price'])

        financials = await self.get_financials_at_date(symbol, valuation_date)
        if not financials:
            return None

        shares = 1e8

        dcf_result = self.run_calibrated_dcf(
            revenue=financials['revenue'],
            margin=financials['operating_margin'],
            growth=financials['revenue_growth_hist'],
            growth_std=financials['growth_std'],
            margin_std=financials['margin_std'],
            price=price,
            shares=shares
        )

        # Get forward returns
        return_1m = await self.get_forward_return(symbol, valuation_date, 1)
        return_3m = await self.get_forward_return(symbol, valuation_date, 3)
        return_6m = await self.get_forward_return(symbol, valuation_date, 6)
        return_12m = await self.get_forward_return(symbol, valuation_date, 12)

        # Get actual revenue 1 year later
        actual_rev_1y = await self.get_actual_revenue_1y(symbol, valuation_date)
        predicted_rev_1y = dcf_result['predicted_rev_1y_p50']

        rev_error = None
        if actual_rev_1y and predicted_rev_1y:
            rev_error = (actual_rev_1y - predicted_rev_1y) / predicted_rev_1y * 100

        # Data quality
        quality = "OK"
        if price < 1:
            quality = "penny_stock"
        elif financials['years_of_data'] < 3:
            quality = "limited_history"

        return CompanyValuation(
            symbol=symbol,
            company_name=company['company_name'],
            valuation_date=valuation_date,
            price=price,
            market_cap=None,
            revenue=financials['revenue'],
            operating_margin=financials['operating_margin'],
            revenue_growth_hist=financials['revenue_growth_hist'],
            predicted_yield=dcf_result['predicted_yield'],
            fair_value_p10=dcf_result['fair_value_p10'],
            fair_value_p50=dcf_result['fair_value_p50'],
            fair_value_p90=dcf_result['fair_value_p90'],
            yield_p10=dcf_result['yield_p10'],
            yield_p50=dcf_result['yield_p50'],
            yield_p90=dcf_result['yield_p90'],
            return_1m=return_1m,
            return_3m=return_3m,
            return_6m=return_6m,
            return_12m=return_12m,
            actual_revenue_1y=actual_rev_1y,
            predicted_revenue_1y=predicted_rev_1y,
            revenue_prediction_error=rev_error,
            data_quality=quality,
            years_of_data=financials['years_of_data']
        )

    async def run_backtest(self, start_year: int = 2022, end_year: int = 2024) -> pd.DataFrame:
        """Run backtest for multiple years"""

        dates = []
        for year in range(start_year, end_year + 1):
            for month in [3, 6, 9, 12]:
                dates.append(datetime(year, month, 1))

        cutoff = datetime.now() - timedelta(days=365)
        dates = [d for d in dates if d < cutoff]

        print(f"\n📊 Calibrated Monte Carlo Backtest")
        print(f"   Dates: {dates[0].date()} to {dates[-1].date()} ({len(dates)} periods)")
        print(f"   Calibrated log-std: {CALIBRATED_LOG_STD}")

        all_results = []

        for val_date in dates:
            print(f"\n🔄 {val_date.date()}...", end=" ")

            companies = await self.get_companies_at_date(val_date, min_years=2)

            period_results = []
            for company in companies[:150]:
                result = await self.value_company(company, val_date)
                if result:
                    period_results.append(result)

            print(f"{len(period_results)} companies valued")
            all_results.extend(period_results)

        df = pd.DataFrame([asdict(r) for r in all_results])

        return df

    def analyze_calibration(self, df: pd.DataFrame):
        """Check if calibration achieved target capture rate"""

        print("\n" + "="*70)
        print("🎯 CALIBRATION CHECK")
        print("="*70)

        # Check revenue prediction accuracy
        valid = df[df['actual_revenue_1y'].notna() & df['predicted_revenue_1y'].notna()].copy()
        valid = valid[valid['predicted_revenue_1y'] > 0]

        if len(valid) < 100:
            print(f"Not enough data for calibration check ({len(valid)} samples)")
            return

        # Calculate ratio
        valid['ratio'] = valid['actual_revenue_1y'] / valid['predicted_revenue_1y']
        valid = valid[(valid['ratio'] > 0.05) & (valid['ratio'] < 20)]

        print(f"\n📊 Revenue Prediction Accuracy (n={len(valid)}):")
        print(f"   Actual/Predicted Ratio:")
        print(f"   P10: {valid['ratio'].quantile(0.10):.2f}x")
        print(f"   P50: {valid['ratio'].quantile(0.50):.2f}x")
        print(f"   P90: {valid['ratio'].quantile(0.90):.2f}x")

        # Check if actual falls within our predicted P10-P90 range
        # For yield-based check, we need the revenue P10/P90 bounds
        # We'll proxy this by checking prediction error bounds

        # Simple check: what % of actuals fall within predicted ±X%
        for pct in [20, 50, 80, 100]:
            within = (abs(valid['revenue_prediction_error']) <= pct).mean() * 100
            print(f"   Within ±{pct}%: {within:.1f}%")

        # Log-space analysis
        log_ratios = np.log(valid['ratio'])
        print(f"\n   Log-space std: {log_ratios.std():.3f} (target: ~{CALIBRATED_LOG_STD})")

    def analyze_results(self, df: pd.DataFrame):
        """Analyze backtest results"""

        print("\n" + "="*70)
        print("📈 MONTE CARLO RANKING BACKTEST RESULTS")
        print("="*70)

        # 1. Quintile analysis
        print("\n1️⃣  QUINTILE ANALYSIS (by predicted_yield)")
        print("-" * 50)

        for horizon in ['return_1m', 'return_3m', 'return_6m', 'return_12m']:
            valid = df[df[horizon].notna()].copy()
            if len(valid) < 100:
                continue

            valid['quintile'] = pd.qcut(valid['predicted_yield'], 5, labels=['Q1(Low)', 'Q2', 'Q3', 'Q4', 'Q5(High)'])

            quintile_returns = valid.groupby('quintile')[horizon].agg(['mean', 'median', 'count'])

            print(f"\n{horizon}:")
            print(quintile_returns.round(2).to_string())

            q5_ret = valid[valid['quintile'] == 'Q5(High)'][horizon].mean()
            q1_ret = valid[valid['quintile'] == 'Q1(Low)'][horizon].mean()
            print(f"  Q5-Q1 Spread: {q5_ret - q1_ret:.1f}%")

        # 2. Correlation
        print("\n2️⃣  RANK CORRELATION")
        print("-" * 50)

        for horizon in ['return_1m', 'return_3m', 'return_6m', 'return_12m']:
            valid = df[df[horizon].notna()]
            if len(valid) < 100:
                continue

            corr = valid['predicted_yield'].corr(valid[horizon])
            print(f"  {horizon} correlation: {corr:.3f}")

        # 3. Revenue accuracy
        print("\n3️⃣  REVENUE ESTIMATE ACCURACY")
        print("-" * 50)

        valid = df[df['revenue_prediction_error'].notna()].copy()
        valid = valid[abs(valid['revenue_prediction_error']) < 200]

        if len(valid) > 50:
            print(f"  Sample size: {len(valid)}")
            print(f"  Mean Absolute Error: {valid['revenue_prediction_error'].abs().mean():.1f}%")
            print(f"  Median Error: {valid['revenue_prediction_error'].median():.1f}%")
            print(f"  % within ±20%: {(abs(valid['revenue_prediction_error']) <= 20).mean()*100:.1f}%")
            print(f"  % within ±50%: {(abs(valid['revenue_prediction_error']) <= 50).mean()*100:.1f}%")

        # 4. Top picks
        print("\n4️⃣  TOP 20 PICKS PERFORMANCE")
        print("-" * 50)

        top_picks = df.groupby('valuation_date').apply(
            lambda x: x.nlargest(20, 'predicted_yield')
        ).reset_index(drop=True)

        avg_top_return = top_picks['return_12m'].mean()
        avg_all_return = df['return_12m'].mean()

        print(f"  Top 20 avg 12M return: {avg_top_return:.1f}%")
        print(f"  All companies avg 12M return: {avg_all_return:.1f}%")
        print(f"  Alpha: {avg_top_return - avg_all_return:.1f}%")

    async def cleanup(self):
        if self.conn:
            await self.conn.close()


async def main():
    bt = CalibratedBacktester()

    try:
        await bt.setup()

        df = await bt.run_backtest(start_year=2022, end_year=2024)

        # Save results
        df.to_csv('mc_backtest_calibrated.csv', index=False)
        print(f"\n💾 Saved {len(df)} results")

        # Check calibration
        bt.analyze_calibration(df)

        # Analyze results
        bt.analyze_results(df)

    finally:
        await bt.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
