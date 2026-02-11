#!/usr/bin/env python3
"""
Find SWAN Stocks (Sleep Well At Night)

Philosophy:
- Companies with stable, predictable cash flows
- Trading at reasonable valuations vs their earning power
- Downside protection through quality, not hoping for re-rating
- You can hold through drawdowns with confidence

Not looking for: lottery tickets, turnarounds, momentum plays
Looking for: steady compounders at fair-to-cheap prices
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from uuid import UUID
from typing import Dict, List, Optional

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_earnings_stability(conn, ticker: str, years: int = 4) -> Optional[Dict]:
    """
    Calculate earnings stability metrics.
    Stable earnings = low coefficient of variation, consistent positive.
    """
    ticker_space = ticker.replace('-', ' ')

    cutoff = date.today() - timedelta(days=years * 365)

    rows = await conn.fetch("""
        SELECT period_date, net_income, total_revenue, operating_income, gross_profit
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND statement_type = 'annual'
        ORDER BY period_date
    """, ticker, ticker_space, cutoff)

    if len(rows) < 3:
        return None

    revenues = [float(r['total_revenue']) for r in rows if r['total_revenue']]
    net_incomes = [float(r['net_income']) for r in rows if r['net_income']]
    operating_incomes = [float(r['operating_income']) for r in rows if r['operating_income']]

    if len(revenues) < 3 or len(net_incomes) < 3:
        return None

    # Calculate margins
    net_margins = []
    op_margins = []
    for r in rows:
        if r['total_revenue'] and r['total_revenue'] > 0:
            if r['net_income']:
                net_margins.append(float(r['net_income']) / float(r['total_revenue']))
            if r['operating_income']:
                op_margins.append(float(r['operating_income']) / float(r['total_revenue']))

    # Stability metrics
    def coefficient_of_variation(values):
        if len(values) < 2:
            return None
        mean = np.mean(values)
        if mean == 0:
            return None
        return np.std(values) / abs(mean)

    def pct_positive(values):
        if not values:
            return 0
        return sum(1 for v in values if v > 0) / len(values)

    revenue_cv = coefficient_of_variation(revenues)
    earnings_cv = coefficient_of_variation(net_incomes)
    margin_cv = coefficient_of_variation(net_margins) if net_margins else None

    # Revenue growth (CAGR)
    if revenues[0] > 0 and revenues[-1] > 0:
        years_span = len(revenues) - 1
        revenue_cagr = (revenues[-1] / revenues[0]) ** (1/years_span) - 1 if years_span > 0 else 0
    else:
        revenue_cagr = None

    return {
        'years_of_data': len(rows),
        'revenue_cv': revenue_cv,  # Lower = more stable
        'earnings_cv': earnings_cv,  # Lower = more stable
        'margin_cv': margin_cv,  # Lower = more stable
        'pct_profitable': pct_positive(net_incomes),
        'avg_net_margin': np.mean(net_margins) if net_margins else None,
        'avg_op_margin': np.mean(op_margins) if op_margins else None,
        'latest_revenue': revenues[-1] if revenues else None,
        'latest_net_income': net_incomes[-1] if net_incomes else None,
        'revenue_cagr': revenue_cagr,
    }


async def get_valuation_metrics(conn, ticker: str, score_date: date) -> Optional[Dict]:
    """Get current valuation metrics."""
    ticker_space = ticker.replace('-', ' ')

    # Get latest financials
    financials = await conn.fetchrow("""
        SELECT net_income, total_revenue, operating_income
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
        ORDER BY period_date DESC
        LIMIT 1
    """, ticker, ticker_space, score_date)

    if not financials or not financials['net_income']:
        return None

    # Get balance sheet
    balance = await conn.fetchrow("""
        SELECT total_equity, total_debt, cash_and_equivalents, shares_outstanding
        FROM balance_sheet_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
        ORDER BY period_date DESC
        LIMIT 1
    """, ticker, ticker_space, score_date)

    if not balance or not balance['shares_outstanding']:
        return None

    # Get price
    price = await conn.fetchrow("""
        SELECT close_price FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
    """, ticker, score_date)

    if not price:
        return None

    price_val = float(price['close_price'])
    shares = float(balance['shares_outstanding'])
    market_cap = price_val * shares

    net_income = float(financials['net_income'])
    equity = float(balance['total_equity']) if balance['total_equity'] else 0
    debt = float(balance['total_debt']) if balance['total_debt'] else 0
    cash = float(balance['cash_and_equivalents']) if balance['cash_and_equivalents'] else 0

    # Valuation ratios
    pe_ratio = market_cap / net_income if net_income > 0 else None
    earnings_yield = net_income / market_cap if market_cap > 0 else None
    pb_ratio = market_cap / equity if equity > 0 else None

    # Enterprise value
    ev = market_cap + debt - cash

    return {
        'market_cap': market_cap,
        'pe_ratio': pe_ratio,
        'earnings_yield': earnings_yield,
        'pb_ratio': pb_ratio,
        'ev': ev,
        'net_debt': debt - cash,
        'debt_to_equity': debt / equity if equity > 0 else None,
    }


async def find_swan_stocks(conn, score_date: date = None):
    """Find Sleep Well At Night stocks."""

    if score_date is None:
        score_date = await conn.fetchval("""
            SELECT score_date FROM daily_dimension_scores
            GROUP BY score_date
            HAVING COUNT(DISTINCT company_id) > 1000
            ORDER BY score_date DESC
            LIMIT 1
        """)

    print(f"Finding SWAN stocks as of {score_date}")
    print("="*80)
    print("\nCriteria:")
    print("  1. Consistently profitable (4+ years)")
    print("  2. Stable earnings (low volatility)")
    print("  3. Strong balance sheet")
    print("  4. Reasonable valuation")
    print("  5. High quality scores")
    print()

    # Get all companies with dimension scores
    companies = await conn.fetch("""
        SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1
    """, score_date)

    print(f"Analyzing {len(companies)} companies...\n")

    candidates = []

    for i, comp in enumerate(companies):
        if i % 200 == 0:
            print(f"  {i}/{len(companies)}")

        ticker = comp['primary_ticker']
        company_uuid = UUID(comp['company_id'])

        # Get dimension scores
        scores = await conn.fetch("""
            SELECT dimension_code, score
            FROM daily_dimension_scores
            WHERE company_id = $1 AND score_date = $2
        """, company_uuid, score_date)

        dim_scores = {r['dimension_code']: float(r['score']) if r['score'] else 0 for r in scores}

        # First filter: dimension scores
        financial_health = dim_scores.get('financial_health', 0)
        earnings_quality = dim_scores.get('earnings_quality', 0)
        profitability = dim_scores.get('profitability', 0)
        quality = dim_scores.get('quality', 0)
        valuation_pct = dim_scores.get('valuation_percentile', 0)
        value = dim_scores.get('value', 0)

        # Minimum quality bar
        if financial_health < 50:
            continue
        if earnings_quality < 40:
            continue
        if profitability < 40:
            continue

        # Get earnings stability
        stability = await get_earnings_stability(conn, ticker)
        if not stability:
            continue

        # Must be consistently profitable
        if stability['pct_profitable'] < 0.75:  # 75% of years profitable
            continue

        # Must have stable earnings (CV < 0.5 means std dev < 50% of mean)
        if stability['earnings_cv'] and stability['earnings_cv'] > 0.6:
            continue

        # Get valuation
        valuation = await get_valuation_metrics(conn, ticker, score_date)
        if not valuation:
            continue

        # Must have positive earnings
        if not valuation['pe_ratio'] or valuation['pe_ratio'] <= 0:
            continue

        # Calculate a "SWAN score"
        # Higher = better SWAN candidate
        swan_score = 0

        # Quality components (40 points max)
        swan_score += min(15, financial_health / 100 * 15)
        swan_score += min(15, earnings_quality / 100 * 15)
        swan_score += min(10, quality / 100 * 10)

        # Stability components (30 points max)
        if stability['earnings_cv']:
            stability_score = max(0, 1 - stability['earnings_cv']) * 15
            swan_score += stability_score
        swan_score += stability['pct_profitable'] * 15

        # Valuation component (30 points max)
        # Earnings yield > 5% is good, > 10% is great
        if valuation['earnings_yield']:
            ey_score = min(15, valuation['earnings_yield'] * 100)  # 10% yield = 10 points
            swan_score += ey_score
        swan_score += min(15, valuation_pct / 100 * 15)

        candidates.append({
            'ticker': ticker,
            'company': comp['company_name'],
            'swan_score': round(swan_score, 1),
            # Quality
            'financial_health': round(financial_health, 0),
            'earnings_quality': round(earnings_quality, 0),
            'profitability': round(profitability, 0),
            'quality': round(quality, 0),
            # Stability
            'pct_profitable': round(stability['pct_profitable'] * 100, 0),
            'earnings_cv': round(stability['earnings_cv'], 2) if stability['earnings_cv'] else None,
            'avg_net_margin': round(stability['avg_net_margin'] * 100, 1) if stability['avg_net_margin'] else None,
            'revenue_cagr': round(stability['revenue_cagr'] * 100, 1) if stability['revenue_cagr'] else None,
            # Valuation
            'pe_ratio': round(valuation['pe_ratio'], 1) if valuation['pe_ratio'] else None,
            'earnings_yield': round(valuation['earnings_yield'] * 100, 1) if valuation['earnings_yield'] else None,
            'pb_ratio': round(valuation['pb_ratio'], 2) if valuation['pb_ratio'] else None,
            'valuation_pct': round(valuation_pct, 0),
            'value': round(value, 0),
            'market_cap_b': round(valuation['market_cap'] / 1e9, 2),
            'debt_to_equity': round(valuation['debt_to_equity'], 2) if valuation['debt_to_equity'] else None,
        })

    if not candidates:
        print("No SWAN candidates found!")
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values('swan_score', ascending=False)

    print(f"\nFound {len(df)} SWAN candidates\n")
    print("="*100)
    print("TOP SWAN STOCKS")
    print("="*100)

    print(f"\n{'Ticker':<10} {'Company':<25} {'SWAN':>6} {'EY%':>6} {'PE':>6} {'Val%':>5} {'Margin':>7} {'Stable':>6}")
    print("-"*85)

    for _, row in df.head(30).iterrows():
        ey = f"{row['earnings_yield']:.1f}" if row['earnings_yield'] else "N/A"
        pe = f"{row['pe_ratio']:.0f}" if row['pe_ratio'] else "N/A"
        margin = f"{row['avg_net_margin']:.1f}%" if row['avg_net_margin'] else "N/A"
        cv = f"{row['earnings_cv']:.2f}" if row['earnings_cv'] else "N/A"

        print(f"{row['ticker']:<10} {row['company'][:24]:<25} {row['swan_score']:>6.1f} "
              f"{ey:>6} {pe:>6} {row['valuation_pct']:>5.0f} {margin:>7} {cv:>6}")

    # Show some detail on top 10
    print("\n" + "="*100)
    print("TOP 10 DETAILED")
    print("="*100)

    for _, row in df.head(10).iterrows():
        print(f"\n{row['ticker']} - {row['company']}")
        print(f"  SWAN Score: {row['swan_score']:.1f}")
        print(f"  Market Cap: ${row['market_cap_b']:.2f}B")
        print(f"  Quality: fin_health={row['financial_health']:.0f}, earn_qual={row['earnings_quality']:.0f}, "
              f"profit={row['profitability']:.0f}, qual={row['quality']:.0f}")
        print(f"  Stability: {row['pct_profitable']:.0f}% profitable years, CV={row['earnings_cv']}, "
              f"margin={row['avg_net_margin']}%")
        print(f"  Valuation: P/E={row['pe_ratio']}, EY={row['earnings_yield']}%, "
              f"cheap vs history={row['valuation_pct']:.0f}%")
        if row['revenue_cagr']:
            print(f"  Growth: {row['revenue_cagr']:.1f}% CAGR")

    # Export
    output_file = 'swan_stocks.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='SWAN Candidates', index=False)

        # High conviction (top scores + cheap)
        high_conviction = df[(df['swan_score'] >= 50) & (df['valuation_pct'] >= 60)]
        high_conviction.to_excel(writer, sheet_name='High Conviction', index=False)

        # Cheapest quality
        cheap_quality = df[df['valuation_pct'] >= 70].sort_values('swan_score', ascending=False)
        cheap_quality.to_excel(writer, sheet_name='Cheapest Quality', index=False)

    print(f"\n\nExported to {output_file}")

    return df


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await find_swan_stocks(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
