#!/usr/bin/env python3
"""
Monte Carlo Ranking Backtest v3 - Using Established Codebase Patterns

Follows patterns from valuation_percentile_calculator.py:
- company_master as source of truth
- Currency conversion via currency_utils
- statement_type = 'annual' filter
- Proper publish_date checks
- Symbol format resolution
"""

import asyncio
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import logging

from domains.dimensions.calculators.currency_utils import get_exchange_rate

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Calibrated P10/P90 multipliers
EMPIRICAL_P10_MULTIPLIER = 0.16
EMPIRICAL_P90_MULTIPLIER = 1.74


def get_stock_currency(yahoo_symbol: str) -> str:
    """Determine stock trading currency from exchange suffix."""
    if not yahoo_symbol:
        return 'SEK'
    if yahoo_symbol.endswith('.ST'):
        return 'SEK'
    elif yahoo_symbol.endswith('.OL'):
        return 'NOK'
    elif yahoo_symbol.endswith('.CO'):
        return 'DKK'
    elif yahoo_symbol.endswith('.HE'):
        return 'EUR'
    return 'SEK'


@dataclass
class CompanyValuation:
    symbol: str
    company_name: str
    valuation_date: datetime
    price: float
    market_cap: Optional[float]
    revenue: float
    operating_margin: float
    revenue_growth_hist: float
    predicted_yield: float
    fair_value_p10: float
    fair_value_p50: float
    fair_value_p90: float
    yield_p10: float = 0
    yield_p50: float = 0
    yield_p90: float = 0
    return_1m: Optional[float] = None
    return_3m: Optional[float] = None
    return_6m: Optional[float] = None
    return_12m: Optional[float] = None
    actual_revenue_1y: Optional[float] = None
    predicted_revenue_1y: Optional[float] = None
    predicted_rev_p10: Optional[float] = None
    predicted_rev_p90: Optional[float] = None
    revenue_prediction_error: Optional[float] = None
    within_p10_p90: Optional[bool] = None
    data_quality: str = "OK"
    years_of_data: int = 0


class BacktesterV3:

    def __init__(self):
        self.conn = None

    async def setup(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def get_companies_at_date(self, valuation_date: datetime, min_years: int = 2) -> List[Dict]:
        """Get companies from company_master with sufficient data.

        Filter to .ST (Swedish) stocks only to avoid currency mixing.
        """
        query = """
        WITH fs_data AS (
            SELECT
                cm.id as company_id,
                cm.primary_ticker,
                cm.company_name,
                cm.yahoo_symbol,
                cm.report_currency,
                COUNT(DISTINCT fs.fiscal_year) as years_of_data
            FROM company_master cm
            INNER JOIN financial_statements fs
                ON fs.symbol = cm.primary_ticker
                OR fs.symbol = REPLACE(cm.primary_ticker, '-', ' ')
            WHERE fs.period_date <= $1
            AND fs.total_revenue > 0
            AND fs.statement_type = 'annual'
            AND cm.yahoo_symbol LIKE '%%.ST'  -- Swedish stocks only
            GROUP BY cm.id, cm.primary_ticker, cm.company_name, cm.yahoo_symbol, cm.report_currency
            HAVING COUNT(DISTINCT fs.fiscal_year) >= $2
        )
        SELECT
            fd.*,
            dp.close_price as price,
            dp.date as price_date
        FROM fs_data fd
        INNER JOIN (
            SELECT symbol, close_price, date,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) as rn
            FROM daily_price_data
            WHERE date <= $1
        ) dp ON dp.symbol = fd.primary_ticker AND dp.rn = 1
        WHERE dp.close_price > 0.5
        ORDER BY fd.years_of_data DESC
        """
        rows = await self.conn.fetch(query, valuation_date.date(), min_years)
        return [dict(r) for r in rows]

    async def get_financials(self, symbol: str, valuation_date: date) -> Optional[Dict]:
        """Get annual financials with publish_date check.

        Uses both symbol formats (hyphen and space).
        """
        symbol_space = symbol.replace('-', ' ')

        query = """
        SELECT
            fiscal_year,
            period_date,
            total_revenue,
            operating_income,
            net_income,
            currency,
            operating_income::float / NULLIF(total_revenue::float, 0) as operating_margin
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
        AND statement_type = 'annual'
        AND (
            (publish_date IS NOT NULL AND publish_date <= $3)
            OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $3)
        )
        AND total_revenue > 0
        ORDER BY period_date DESC
        LIMIT 3
        """
        rows = await self.conn.fetch(query, symbol, symbol_space, valuation_date)

        if not rows:
            return None

        revenues = [float(r['total_revenue']) for r in rows if r['total_revenue']]
        margins = [float(r['operating_margin']) for r in rows if r['operating_margin'] is not None]

        if not revenues:
            return None

        # Calculate historical growth rate
        if len(revenues) >= 2:
            growth_rates = []
            for i in range(len(revenues) - 1):
                if revenues[i+1] > 0:
                    growth = (revenues[i] / revenues[i+1]) - 1
                    growth_rates.append(growth)
            avg_growth = np.mean(growth_rates) if growth_rates else 0
        else:
            avg_growth = 0

        return {
            'revenue': revenues[0],
            'operating_margin': margins[0] if margins else 0,
            'revenue_growth_hist': avg_growth,
            'years_of_data': len(revenues),
            'currency': rows[0]['currency']
        }

    async def get_shares(self, symbol: str, valuation_date: date) -> Optional[float]:
        """Get shares outstanding from balance_sheet_data."""
        symbol_space = symbol.replace('-', ' ')

        shares = await self.conn.fetchval("""
            SELECT shares_outstanding
            FROM balance_sheet_data
            WHERE (symbol = $1 OR symbol = $2)
            AND statement_type = 'annual'
            AND (
                (publish_date IS NOT NULL AND publish_date <= $3)
                OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $3)
            )
            AND shares_outstanding > 0
            ORDER BY period_date DESC
            LIMIT 1
        """, symbol, symbol_space, valuation_date)

        return float(shares) if shares else None

    async def get_forward_return(self, symbol: str, start_date: datetime, months: int) -> Optional[float]:
        """Get forward return."""
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
        """Get actual revenue 1 year later."""
        symbol_space = symbol.replace('-', ' ')
        future_date = valuation_date + timedelta(days=400)

        revenue = await self.conn.fetchval("""
            SELECT total_revenue FROM financial_statements
            WHERE (symbol = $1 OR symbol = $2)
            AND statement_type = 'annual'
            AND period_date <= $3
            AND period_date > $4
            AND total_revenue > 0
            ORDER BY period_date DESC LIMIT 1
        """, symbol, symbol_space, future_date.date(), valuation_date.date())

        return float(revenue) if revenue else None

    def run_dcf(self, revenue: float, margin: float, growth: float,
                price: float, shares: float, fx_rate: float = 1.0) -> Dict:
        """Monte Carlo DCF with currency-adjusted financials."""

        # Apply fx_rate to revenue (convert to stock currency)
        revenue_adj = revenue * fx_rate

        fair_values = []

        for _ in range(1000):
            sim_growth = np.random.normal(growth, abs(growth) * 0.5 + 0.05)
            sim_growth = np.clip(sim_growth, -0.25, 0.40)

            sim_margin = np.random.normal(margin, abs(margin) * 0.2 + 0.02)
            sim_margin = np.clip(sim_margin, 0, 0.40)

            projected_revenue = revenue_adj
            total_earnings = 0
            discount_rate = 0.10

            for year in range(1, 6):
                year_growth = sim_growth * (0.8 ** (year-1)) + 0.025 * (1 - 0.8 ** (year-1))
                projected_revenue *= (1 + year_growth)
                earnings = projected_revenue * sim_margin * 0.78
                total_earnings += earnings / (1 + discount_rate) ** year

            terminal_earnings = projected_revenue * sim_margin * 0.78 * 1.025
            terminal_value = terminal_earnings / (discount_rate - 0.025)
            total_value = total_earnings + terminal_value / (1 + discount_rate) ** 5

            fair_value_per_share = total_value / shares
            fair_values.append(fair_value_per_share)

        fair_values = np.array(fair_values)
        fv_p50 = np.percentile(fair_values, 50)

        # Calibrated bounds
        fv_p10 = fv_p50 * EMPIRICAL_P10_MULTIPLIER
        fv_p90 = fv_p50 * EMPIRICAL_P90_MULTIPLIER

        # Revenue prediction with calibrated bounds
        predicted_rev_1y = revenue_adj * (1 + growth)
        pred_rev_p10 = predicted_rev_1y * EMPIRICAL_P10_MULTIPLIER
        pred_rev_p90 = predicted_rev_1y * EMPIRICAL_P90_MULTIPLIER

        return {
            'fair_value_p10': fv_p10,
            'fair_value_p50': fv_p50,
            'fair_value_p90': fv_p90,
            'predicted_yield': (fv_p50 / price - 1) * 100,
            'yield_p10': (fv_p10 / price - 1) * 100,
            'yield_p50': (fv_p50 / price - 1) * 100,
            'yield_p90': (fv_p90 / price - 1) * 100,
            'predicted_rev_1y': predicted_rev_1y,
            'pred_rev_p10': pred_rev_p10,
            'pred_rev_p90': pred_rev_p90,
        }

    async def value_company(self, company: Dict, valuation_date: datetime) -> Optional[CompanyValuation]:
        """Value a single company with proper currency handling."""

        symbol = company['primary_ticker']
        yahoo_symbol = company['yahoo_symbol']
        price = float(company['price'])

        # Get financials
        financials = await self.get_financials(symbol, valuation_date.date())
        if not financials:
            return None

        # Filter out extreme cases that break the DCF model
        margin = financials['operating_margin']
        growth = financials['revenue_growth_hist']

        # Skip companies with extreme margins (>100% or <-100%)
        if abs(margin) > 1.0:
            return None

        # Skip companies with extreme growth (>100% or <-50%)
        if growth > 1.0 or growth < -0.5:
            return None

        # Get shares
        shares = await self.get_shares(symbol, valuation_date.date())
        if not shares or shares <= 0:
            return None

        # Currency conversion
        stock_currency = get_stock_currency(yahoo_symbol)
        report_currency = financials.get('currency') or company.get('report_currency')

        fx_rate = 1.0
        if report_currency and stock_currency and report_currency != stock_currency:
            fx_rate = get_exchange_rate(report_currency, stock_currency) or 1.0

        # Sanity check: Price/Sales ratio
        # Catches stock split timing issues, bad data, etc.
        market_cap = price * shares
        revenue_adj = financials['revenue'] * fx_rate
        price_to_sales = market_cap / revenue_adj if revenue_adj > 0 else 999

        # Skip unrealistic P/S ratios (likely data quality issues)
        # P/S < 0.2 = suspiciously cheap (split timing issue)
        # P/S > 100 = company with near-zero revenue
        if price_to_sales < 0.2 or price_to_sales > 100:
            return None

        # Skip preferred shares (DCF doesn't apply)
        if 'PREF' in symbol.upper():
            return None

        # Run DCF
        dcf_result = self.run_dcf(
            revenue=financials['revenue'],
            margin=financials['operating_margin'],
            growth=financials['revenue_growth_hist'],
            price=price,
            shares=shares,
            fx_rate=fx_rate
        )

        # Get forward returns
        return_1m = await self.get_forward_return(symbol, valuation_date, 1)
        return_3m = await self.get_forward_return(symbol, valuation_date, 3)
        return_6m = await self.get_forward_return(symbol, valuation_date, 6)
        return_12m = await self.get_forward_return(symbol, valuation_date, 12)

        # Get actual revenue (also currency adjusted)
        actual_rev_1y = await self.get_actual_revenue_1y(symbol, valuation_date)
        if actual_rev_1y:
            actual_rev_1y = actual_rev_1y * fx_rate

        predicted_rev_1y = dcf_result['predicted_rev_1y']
        pred_rev_p10 = dcf_result['pred_rev_p10']
        pred_rev_p90 = dcf_result['pred_rev_p90']

        rev_error = None
        within_range = None
        if actual_rev_1y and predicted_rev_1y:
            rev_error = (actual_rev_1y - predicted_rev_1y) / predicted_rev_1y * 100
            within_range = pred_rev_p10 <= actual_rev_1y <= pred_rev_p90

        # Data quality
        quality = "OK"
        if price < 1:
            quality = "penny_stock"
        elif financials['years_of_data'] < 3:
            quality = "limited_history"

        market_cap = price * shares

        return CompanyValuation(
            symbol=symbol,
            company_name=company['company_name'],
            valuation_date=valuation_date,
            price=price,
            market_cap=market_cap,
            revenue=financials['revenue'] * fx_rate,
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
            predicted_rev_p10=pred_rev_p10,
            predicted_rev_p90=pred_rev_p90,
            revenue_prediction_error=rev_error,
            within_p10_p90=within_range,
            data_quality=quality,
            years_of_data=financials['years_of_data']
        )

    async def run_backtest(self, start_year: int = 2022, end_year: int = 2024) -> pd.DataFrame:
        """Run backtest."""
        dates = []
        for year in range(start_year, end_year + 1):
            for month in [3, 6, 9, 12]:
                dates.append(datetime(year, month, 1))

        cutoff = datetime.now() - timedelta(days=365)
        dates = [d for d in dates if d < cutoff]

        print(f"\n📊 Monte Carlo Backtest v3 (Swedish stocks only)")
        print(f"   Dates: {dates[0].date()} to {dates[-1].date()} ({len(dates)} periods)")

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

    def analyze_results(self, df: pd.DataFrame):
        """Analyze backtest results."""
        print("\n" + "="*70)
        print("📈 BACKTEST RESULTS")
        print("="*70)

        # Calibration check
        valid = df[df['within_p10_p90'].notna()].copy()
        if 'within_p10_p90' in valid.columns and len(valid) > 0:
            # Handle both bool and object types from CSV
            if valid['within_p10_p90'].dtype == 'object':
                valid['within_p10_p90'] = valid['within_p10_p90'].astype(str).str.lower() == 'true'
            capture_rate = valid['within_p10_p90'].mean() * 100
            print(f"\n🎯 P10-P90 Capture Rate: {capture_rate:.1f}% (target: 80%)")

        # Quintile analysis
        print("\n📊 QUINTILE ANALYSIS (12M returns)")
        valid = df[df['return_12m'].notna()].copy()
        if len(valid) >= 100:
            try:
                valid['quintile'] = pd.qcut(valid['predicted_yield'].rank(method='first'), 5,
                                            labels=['Q1(Low)', 'Q2', 'Q3', 'Q4', 'Q5(High)'])
                quintile_returns = valid.groupby('quintile', observed=False)['return_12m'].agg(['mean', 'count'])
                print(quintile_returns.round(2).to_string())

                q5_ret = valid[valid['quintile'] == 'Q5(High)']['return_12m'].mean()
                q1_ret = valid[valid['quintile'] == 'Q1(Low)']['return_12m'].mean()
                print(f"\nQ5-Q1 Spread: {q5_ret - q1_ret:.1f}%")
            except Exception as e:
                print(f"Quintile analysis failed: {e}")

        # Correlation
        print("\n📈 CORRELATIONS")
        for horizon in ['return_1m', 'return_3m', 'return_6m', 'return_12m']:
            valid = df[df[horizon].notna()]
            if len(valid) >= 50:
                corr = valid['predicted_yield'].corr(valid[horizon])
                print(f"  {horizon}: {corr:.3f}")

        # Top 20 alpha
        print("\n🏆 TOP 20 ALPHA")
        top20 = df.groupby('valuation_date', group_keys=False).apply(
            lambda x: x.nlargest(20, 'predicted_yield')
        ).reset_index(drop=True)

        avg_top = top20['return_12m'].mean()
        avg_all = df['return_12m'].mean()
        print(f"  Top 20 avg 12M return: {avg_top:.1f}%")
        print(f"  All companies avg: {avg_all:.1f}%")
        print(f"  Alpha: {avg_top - avg_all:.1f}%")

    async def cleanup(self):
        if self.conn:
            await self.conn.close()


async def main():
    bt = BacktesterV3()
    try:
        await bt.setup()
        df = await bt.run_backtest(start_year=2022, end_year=2024)

        df.to_csv('mc_backtest_v3.csv', index=False)
        print(f"\n💾 Saved {len(df)} results")

        bt.analyze_results(df)

    finally:
        await bt.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
