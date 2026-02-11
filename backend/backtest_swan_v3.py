#!/usr/bin/env python3
"""
Backtest SWAN v3 Strategy

Test if the SWAN ranking (Quality + Low Debt + Owner Value) predicts forward returns.

Methodology:
1. At each historical date, calculate SWAN scores for all companies
2. Rank into quintiles
3. Measure forward returns (63 days / ~3 months)
4. Check if high SWAN score = better returns (positive skew)
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from uuid import UUID
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

FORWARD_DAYS = 63  # ~3 months


async def get_historical_dates(conn) -> List[date]:
    """Get dates with sufficient dimension score coverage."""
    dates = await conn.fetch("""
        SELECT score_date, COUNT(DISTINCT company_id) as companies
        FROM daily_dimension_scores
        GROUP BY score_date
        HAVING COUNT(DISTINCT company_id) > 500
        ORDER BY score_date
    """)
    return [r['score_date'] for r in dates]


async def get_comprehensive_metrics(conn, ticker: str, score_date: date) -> Optional[Dict]:
    """Get comprehensive metrics for SWAN scoring - point in time safe."""
    ticker_space = ticker.replace('-', ' ')
    cutoff = score_date - timedelta(days=4 * 365)

    # Get financials available at score_date (with publish lag)
    financials = await conn.fetch("""
        SELECT period_date, net_income, total_revenue, operating_income,
               ebitda, interest_expense
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND statement_type = 'annual'
          AND (
              (publish_date IS NOT NULL AND publish_date <= $4)
              OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $4)
          )
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    if len(financials) < 2:
        return None

    # Get balance sheets
    balance_sheets = await conn.fetch("""
        SELECT period_date, total_equity, total_debt, long_term_debt,
               cash_and_equivalents, shares_outstanding, total_assets
        FROM balance_sheet_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND (
              (publish_date IS NOT NULL AND publish_date <= $4)
              OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $4)
          )
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    if len(balance_sheets) < 2:
        return None

    # Get cash flows
    cash_flows = await conn.fetch("""
        SELECT period_date, operating_cash_flow, capital_expenditure, free_cash_flow
        FROM cash_flow_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND (
              (publish_date IS NOT NULL AND publish_date <= $4)
              OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $4)
          )
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    # Get price at score_date
    price = await conn.fetchrow("""
        SELECT close_price FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC LIMIT 1
    """, ticker, score_date)

    if not price:
        return None

    latest_fin = financials[-1]
    latest_bal = balance_sheets[-1]

    price_val = float(price['close_price'])
    shares = float(latest_bal['shares_outstanding']) if latest_bal['shares_outstanding'] else None

    if not shares or shares <= 0:
        return None

    market_cap = price_val * shares

    # Extract values
    revenue = float(latest_fin['total_revenue']) if latest_fin['total_revenue'] else 0
    net_income = float(latest_fin['net_income']) if latest_fin['net_income'] else 0
    operating_income = float(latest_fin['operating_income']) if latest_fin['operating_income'] else 0
    ebitda = float(latest_fin['ebitda']) if latest_fin['ebitda'] else None
    interest_expense = float(latest_fin['interest_expense']) if latest_fin['interest_expense'] else 0

    equity = float(latest_bal['total_equity']) if latest_bal['total_equity'] else 0
    total_debt = float(latest_bal['total_debt']) if latest_bal['total_debt'] else 0
    cash = float(latest_bal['cash_and_equivalents']) if latest_bal['cash_and_equivalents'] else 0

    if not ebitda and operating_income:
        ebitda = operating_income * 1.15

    net_debt = total_debt - cash
    ev = market_cap + net_debt

    # Debt metrics
    net_debt_to_ebitda = net_debt / ebitda if ebitda and ebitda > 0 else None
    interest_coverage = operating_income / interest_expense if interest_expense and interest_expense > 0 else None
    debt_to_equity = total_debt / equity if equity > 0 else None

    # Debt growth
    debt_growth = None
    if len(balance_sheets) >= 2:
        old_debt = float(balance_sheets[0]['total_debt']) if balance_sheets[0]['total_debt'] else 0
        if old_debt > 0:
            years = (balance_sheets[-1]['period_date'] - balance_sheets[0]['period_date']).days / 365
            if years > 0:
                debt_growth = (total_debt / old_debt) ** (1/years) - 1

    # Valuation
    pe_ratio = market_cap / net_income if net_income > 0 else None
    ev_to_ebitda = ev / ebitda if ebitda and ebitda > 0 else None

    # FCF
    fcf = None
    if cash_flows:
        latest_cf = cash_flows[-1]
        if latest_cf['free_cash_flow']:
            fcf = float(latest_cf['free_cash_flow'])
        elif latest_cf['operating_cash_flow'] and latest_cf['capital_expenditure']:
            fcf = float(latest_cf['operating_cash_flow']) - abs(float(latest_cf['capital_expenditure']))

    fcf_yield_ev = fcf / ev if fcf and ev > 0 else None

    # Normalized P/E
    net_margins = []
    for f in financials:
        if f['total_revenue'] and f['total_revenue'] > 0 and f['net_income']:
            net_margins.append(float(f['net_income']) / float(f['total_revenue']))

    avg_net_margin = np.mean(net_margins) if net_margins else None
    normalized_pe = None
    if avg_net_margin and revenue > 0 and market_cap > 0:
        normalized_earnings = revenue * avg_net_margin
        if normalized_earnings > 0:
            normalized_pe = market_cap / normalized_earnings

    return {
        'net_debt_to_ebitda': net_debt_to_ebitda,
        'interest_coverage': interest_coverage,
        'debt_to_equity': debt_to_equity,
        'debt_growth': debt_growth,
        'pe_ratio': pe_ratio,
        'ev_to_ebitda': ev_to_ebitda,
        'fcf_yield_ev': fcf_yield_ev,
        'normalized_pe': normalized_pe,
    }


def calculate_debt_score(metrics: Dict) -> float:
    """Calculate debt burden score (0-100, higher = less debt = better)."""
    score = 100

    nd_ebitda = metrics.get('net_debt_to_ebitda')
    if nd_ebitda is not None:
        if nd_ebitda < 0:
            score += 10
        elif nd_ebitda < 1:
            pass
        elif nd_ebitda < 2:
            score -= 10
        elif nd_ebitda < 3:
            score -= 25
        elif nd_ebitda < 4:
            score -= 40
        elif nd_ebitda < 5:
            score -= 55
        else:
            score -= 70

    int_cov = metrics.get('interest_coverage')
    if int_cov is not None:
        if int_cov > 10:
            pass
        elif int_cov > 5:
            score -= 5
        elif int_cov > 3:
            score -= 15
        elif int_cov > 2:
            score -= 30
        elif int_cov > 1:
            score -= 50
        else:
            score -= 70

    debt_growth = metrics.get('debt_growth')
    if debt_growth is not None:
        if debt_growth < 0:
            score += 5
        elif debt_growth < 0.05:
            pass
        elif debt_growth < 0.10:
            score -= 10
        elif debt_growth < 0.20:
            score -= 20
        else:
            score -= 35

    d_e = metrics.get('debt_to_equity')
    if d_e is not None:
        if d_e < 0.3:
            pass
        elif d_e < 0.5:
            score -= 5
        elif d_e < 1.0:
            score -= 15
        elif d_e < 2.0:
            score -= 30
        else:
            score -= 45

    return max(0, min(100, score))


def calculate_owner_value_score(metrics: Dict) -> float:
    """Calculate owner value score."""
    score = 0

    fcf_yield_ev = metrics.get('fcf_yield_ev')
    if fcf_yield_ev is not None:
        if fcf_yield_ev > 0.10:
            score += 35
        elif fcf_yield_ev > 0.07:
            score += 28
        elif fcf_yield_ev > 0.05:
            score += 20
        elif fcf_yield_ev > 0.03:
            score += 12
        elif fcf_yield_ev > 0:
            score += 5
        else:
            score -= 10

    ev_ebitda = metrics.get('ev_to_ebitda')
    if ev_ebitda is not None:
        if ev_ebitda < 6:
            score += 25
        elif ev_ebitda < 8:
            score += 20
        elif ev_ebitda < 10:
            score += 15
        elif ev_ebitda < 12:
            score += 10
        elif ev_ebitda < 15:
            score += 5
        else:
            score -= 5

    norm_pe = metrics.get('normalized_pe')
    if norm_pe is not None:
        if norm_pe < 10:
            score += 15
        elif norm_pe < 15:
            score += 10
        elif norm_pe < 20:
            score += 5

    debt_score = calculate_debt_score(metrics)
    score += (debt_score / 100) * 25

    return min(100, score)


async def calculate_swan_score(conn, company_id: UUID, ticker: str, score_date: date) -> Optional[float]:
    """Calculate SWAN score for a company at a point in time."""

    # Get dimension scores
    scores = await conn.fetch("""
        SELECT dimension_code, score
        FROM daily_dimension_scores
        WHERE company_id = $1 AND score_date = $2
    """, company_id, score_date)

    dim_scores = {r['dimension_code']: float(r['score']) if r['score'] else 0 for r in scores}

    financial_health = dim_scores.get('financial_health', 0)
    earnings_quality = dim_scores.get('earnings_quality', 0)
    profitability = dim_scores.get('profitability', 0)

    # Quality filters
    if financial_health < 40 or earnings_quality < 35 or profitability < 30:
        return None

    # Get comprehensive metrics
    metrics = await get_comprehensive_metrics(conn, ticker, score_date)
    if not metrics:
        return None

    if not metrics['pe_ratio'] or metrics['pe_ratio'] <= 0:
        return None

    # Calculate scores
    debt_score = calculate_debt_score(metrics)
    owner_value_score = calculate_owner_value_score(metrics)

    # Combined SWAN score
    quality_component = (financial_health + earnings_quality + profitability) / 3
    swan_score = (quality_component * 0.4) + (debt_score * 0.3) + (owner_value_score * 0.3)

    return swan_score


async def get_forward_return(conn, ticker: str, start_date: date, days: int) -> Optional[float]:
    """Get forward return from start_date."""
    end_date = start_date + timedelta(days=days + 10)  # Buffer for weekends

    prices = await conn.fetch("""
        SELECT date, close_price FROM daily_price_data
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        ORDER BY date
    """, ticker, start_date, end_date)

    if len(prices) < 2:
        return None

    start_price = float(prices[0]['close_price'])

    # Find price closest to target days
    target_date = start_date + timedelta(days=days)
    end_price = None
    for p in prices:
        if p['date'] >= target_date:
            end_price = float(p['close_price'])
            break

    if end_price is None and len(prices) > 1:
        end_price = float(prices[-1]['close_price'])

    if start_price <= 0 or end_price is None:
        return None

    return (end_price - start_price) / start_price


async def backtest_swan(conn):
    """Run SWAN backtest."""

    print("SWAN v3 Backtest")
    print("="*80)
    print(f"Forward period: {FORWARD_DAYS} days (~3 months)")
    print()

    # Get historical dates
    all_dates = await get_historical_dates(conn)

    # Filter to dates with enough forward data
    max_date = date.today() - timedelta(days=FORWARD_DAYS + 30)
    test_dates = [d for d in all_dates if d <= max_date]

    # Sample quarterly to speed up
    test_dates = [d for i, d in enumerate(test_dates) if i % 3 == 0]

    print(f"Testing {len(test_dates)} dates from {test_dates[0]} to {test_dates[-1]}")
    print()

    all_results = []

    for idx, score_date in enumerate(test_dates):
        print(f"[{idx+1}/{len(test_dates)}] {score_date}...", end=" ", flush=True)

        # Get all companies with dimension scores on this date
        companies = await conn.fetch("""
            SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
            FROM daily_dimension_scores dds
            JOIN company_master cm ON cm.id = dds.company_id
            WHERE dds.score_date = $1
        """, score_date)

        date_scores = []

        for comp in companies:
            ticker = comp['primary_ticker']
            company_uuid = UUID(comp['company_id'])

            swan_score = await calculate_swan_score(conn, company_uuid, ticker, score_date)
            if swan_score is None:
                continue

            forward_return = await get_forward_return(conn, ticker, score_date, FORWARD_DAYS)
            if forward_return is None:
                continue

            # Filter extreme returns (likely data errors)
            if forward_return > 3.0 or forward_return < -0.9:
                continue

            date_scores.append({
                'date': score_date,
                'ticker': ticker,
                'company': comp['company_name'],
                'swan_score': swan_score,
                'forward_return': forward_return,
            })

        if len(date_scores) >= 20:
            # Rank into quintiles
            df_date = pd.DataFrame(date_scores)
            df_date['quintile'] = pd.qcut(df_date['swan_score'], 5, labels=[1, 2, 3, 4, 5], duplicates='drop')
            all_results.extend(df_date.to_dict('records'))
            print(f"{len(date_scores)} companies scored")
        else:
            print(f"only {len(date_scores)} companies (skipped)")

    if not all_results:
        print("No results!")
        return

    df = pd.DataFrame(all_results)

    print()
    print("="*80)
    print("RESULTS BY QUINTILE")
    print("="*80)
    print()
    print("Quintile 5 = Highest SWAN scores (best quality + lowest debt + best value)")
    print("Quintile 1 = Lowest SWAN scores")
    print()

    # Analyze by quintile
    quintile_stats = df.groupby('quintile').agg({
        'forward_return': ['mean', 'median', 'std', 'count'],
        'swan_score': ['mean', 'min', 'max']
    }).round(4)

    print(f"{'Quintile':<10} {'Avg Return':>12} {'Median':>10} {'Std':>10} {'Count':>8} {'SWAN Range':>15}")
    print("-"*70)

    for q in [1, 2, 3, 4, 5]:
        if q in quintile_stats.index:
            stats = quintile_stats.loc[q]
            avg_ret = stats[('forward_return', 'mean')] * 100
            med_ret = stats[('forward_return', 'median')] * 100
            std_ret = stats[('forward_return', 'std')] * 100
            count = int(stats[('forward_return', 'count')])
            swan_min = stats[('swan_score', 'min')]
            swan_max = stats[('swan_score', 'max')]

            print(f"Q{q:<9} {avg_ret:>11.2f}% {med_ret:>9.2f}% {std_ret:>9.2f}% {count:>8} {swan_min:.0f}-{swan_max:.0f}")

    # Calculate spread
    q5_return = df[df['quintile'] == 5]['forward_return'].mean() * 100
    q1_return = df[df['quintile'] == 1]['forward_return'].mean() * 100
    spread = q5_return - q1_return

    print()
    print(f"SPREAD (Q5 - Q1): {spread:+.2f}%")
    print()

    if spread > 0:
        print("✅ POSITIVE SKEW: High SWAN scores outperform low SWAN scores")
    else:
        print("❌ NEGATIVE SKEW: High SWAN scores underperform")

    # Component analysis
    print()
    print("="*80)
    print("COMPONENT ANALYSIS")
    print("="*80)

    # Test debt score alone
    print("\nDoes DEBT SCORE alone predict returns?")
    # We'd need to recalculate, but we can approximate from swan_score distribution

    # Correlation analysis
    correlation = df['swan_score'].corr(df['forward_return'])
    print(f"\nCorrelation (SWAN score vs forward return): {correlation:.4f}")

    # Win rate by quintile
    print()
    print("WIN RATE BY QUINTILE (% with positive return):")
    for q in [1, 2, 3, 4, 5]:
        q_df = df[df['quintile'] == q]
        if len(q_df) > 0:
            win_rate = (q_df['forward_return'] > 0).mean() * 100
            print(f"  Q{q}: {win_rate:.1f}%")

    # Best and worst performers
    print()
    print("="*80)
    print("TOP PERFORMERS FROM Q5 (High SWAN)")
    print("="*80)

    q5_df = df[df['quintile'] == 5].sort_values('forward_return', ascending=False)
    print(f"\n{'Ticker':<12} {'Company':<25} {'SWAN':>6} {'Return':>10} {'Date'}")
    print("-"*70)
    for _, row in q5_df.head(15).iterrows():
        print(f"{row['ticker']:<12} {row['company'][:24]:<25} {row['swan_score']:>6.1f} "
              f"{row['forward_return']*100:>9.1f}% {row['date']}")

    print()
    print("WORST PERFORMERS FROM Q5 (High SWAN that failed)")
    print("-"*70)
    for _, row in q5_df.tail(10).iterrows():
        print(f"{row['ticker']:<12} {row['company'][:24]:<25} {row['swan_score']:>6.1f} "
              f"{row['forward_return']*100:>9.1f}% {row['date']}")

    # Export
    output_file = 'swan_v3_backtest.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='All Trades', index=False)

        # Summary by quintile
        summary = df.groupby('quintile').agg({
            'forward_return': ['mean', 'median', 'std', 'count'],
        }).round(4)
        summary.to_excel(writer, sheet_name='Quintile Summary')

        # Q5 trades
        q5_df.to_excel(writer, sheet_name='Q5 High SWAN', index=False)

        # Q1 trades
        q1_df = df[df['quintile'] == 1].sort_values('forward_return', ascending=False)
        q1_df.to_excel(writer, sheet_name='Q1 Low SWAN', index=False)

    print(f"\nExported to {output_file}")

    return df


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await backtest_swan(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
