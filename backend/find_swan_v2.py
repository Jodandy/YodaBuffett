#!/usr/bin/env python3
"""
SWAN Stocks v2 - Enhanced Valuation

Improvements:
- Multiple valuation metrics (P/E, P/S, EV/Sales)
- Historical margin comparison (spot temporary compression)
- Normalized earnings (using historical margins)
- Mean reversion candidates (current margin < historical)
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from uuid import UUID
from typing import Dict, Optional

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_comprehensive_metrics(conn, ticker: str, score_date: date) -> Optional[Dict]:
    """Get comprehensive financial and valuation metrics."""

    ticker_space = ticker.replace('-', ' ')

    # Get multiple years of financials
    financials = await conn.fetch("""
        SELECT period_date, net_income, total_revenue, operating_income, gross_profit
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
          AND statement_type = 'annual'
        ORDER BY period_date DESC
        LIMIT 5
    """, ticker, ticker_space, score_date)

    if len(financials) < 2:
        return None

    # Current year (most recent)
    current = financials[0]
    if not current['total_revenue'] or not current['net_income']:
        return None

    current_revenue = float(current['total_revenue'])
    current_net_income = float(current['net_income'])
    current_op_income = float(current['operating_income']) if current['operating_income'] else 0

    # Calculate current margins
    current_net_margin = current_net_income / current_revenue if current_revenue > 0 else 0
    current_op_margin = current_op_income / current_revenue if current_revenue > 0 else 0

    # Historical margins (excluding current year)
    hist_net_margins = []
    hist_op_margins = []
    hist_revenues = []
    hist_net_incomes = []

    for f in financials[1:]:  # Skip current
        if f['total_revenue'] and float(f['total_revenue']) > 0:
            rev = float(f['total_revenue'])
            hist_revenues.append(rev)
            if f['net_income']:
                ni = float(f['net_income'])
                hist_net_incomes.append(ni)
                hist_net_margins.append(ni / rev)
            if f['operating_income']:
                hist_op_margins.append(float(f['operating_income']) / rev)

    if not hist_net_margins:
        return None

    avg_hist_net_margin = np.mean(hist_net_margins)
    avg_hist_op_margin = np.mean(hist_op_margins) if hist_op_margins else None

    # Normalized earnings (current revenue * historical avg margin)
    normalized_earnings = current_revenue * avg_hist_net_margin

    # Is margin compressed?
    margin_vs_history = current_net_margin - avg_hist_net_margin
    margin_compressed = margin_vs_history < -0.02  # More than 2% below historical

    # Revenue growth
    if len(hist_revenues) >= 2:
        oldest_revenue = hist_revenues[-1]
        years = len(hist_revenues)
        if oldest_revenue > 0:
            revenue_cagr = (current_revenue / oldest_revenue) ** (1/years) - 1
        else:
            revenue_cagr = 0
    else:
        revenue_cagr = 0

    # Profitability consistency
    all_net_incomes = [current_net_income] + hist_net_incomes
    pct_profitable = sum(1 for ni in all_net_incomes if ni > 0) / len(all_net_incomes)

    # Earnings stability
    if len(all_net_incomes) >= 3:
        earnings_cv = np.std(all_net_incomes) / abs(np.mean(all_net_incomes)) if np.mean(all_net_incomes) != 0 else None
    else:
        earnings_cv = None

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

    shares = float(balance['shares_outstanding'])
    equity = float(balance['total_equity']) if balance['total_equity'] else 0
    debt = float(balance['total_debt']) if balance['total_debt'] else 0
    cash = float(balance['cash_and_equivalents']) if balance['cash_and_equivalents'] else 0

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
    market_cap = price_val * shares
    ev = market_cap + debt - cash

    # Valuation metrics
    pe_ratio = market_cap / current_net_income if current_net_income > 0 else None
    pe_normalized = market_cap / normalized_earnings if normalized_earnings > 0 else None
    ps_ratio = market_cap / current_revenue if current_revenue > 0 else None
    ev_sales = ev / current_revenue if current_revenue > 0 else None
    pb_ratio = market_cap / equity if equity > 0 else None

    earnings_yield = current_net_income / market_cap if market_cap > 0 else None
    earnings_yield_normalized = normalized_earnings / market_cap if market_cap > 0 else None

    # PEG (using revenue CAGR as growth proxy since earnings can be lumpy)
    peg = pe_ratio / (revenue_cagr * 100) if pe_ratio and revenue_cagr > 0.01 else None

    return {
        # Fundamentals
        'revenue': current_revenue,
        'net_income': current_net_income,
        'current_net_margin': current_net_margin,
        'current_op_margin': current_op_margin,
        'avg_hist_net_margin': avg_hist_net_margin,
        'avg_hist_op_margin': avg_hist_op_margin,
        'margin_vs_history': margin_vs_history,
        'margin_compressed': margin_compressed,
        'normalized_earnings': normalized_earnings,
        'revenue_cagr': revenue_cagr,
        'pct_profitable': pct_profitable,
        'earnings_cv': earnings_cv,
        'years_of_data': len(financials),

        # Balance sheet
        'equity': equity,
        'net_debt': debt - cash,
        'debt_to_equity': debt / equity if equity > 0 else None,

        # Valuation
        'market_cap': market_cap,
        'ev': ev,
        'pe_ratio': pe_ratio,
        'pe_normalized': pe_normalized,
        'ps_ratio': ps_ratio,
        'ev_sales': ev_sales,
        'pb_ratio': pb_ratio,
        'earnings_yield': earnings_yield,
        'earnings_yield_normalized': earnings_yield_normalized,
        'peg': peg,
    }


async def find_swan_v2(conn, score_date: date = None):
    """Find SWAN stocks with enhanced valuation analysis."""

    if score_date is None:
        score_date = await conn.fetchval("""
            SELECT score_date FROM daily_dimension_scores
            GROUP BY score_date
            HAVING COUNT(DISTINCT company_id) > 1000
            ORDER BY score_date DESC
            LIMIT 1
        """)

    print(f"Finding SWAN stocks v2 as of {score_date}")
    print("="*100)

    # Get all companies
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

        financial_health = dim_scores.get('financial_health', 0)
        earnings_quality = dim_scores.get('earnings_quality', 0)
        profitability = dim_scores.get('profitability', 0)
        quality = dim_scores.get('quality', 0)
        valuation_pct = dim_scores.get('valuation_percentile', 0)

        # Minimum quality bar
        if financial_health < 45:
            continue
        if earnings_quality < 35:
            continue

        # Get comprehensive metrics
        metrics = await get_comprehensive_metrics(conn, ticker, score_date)
        if not metrics:
            continue

        # Must be mostly profitable
        if metrics['pct_profitable'] < 0.6:
            continue

        # Must have reasonable earnings stability
        if metrics['earnings_cv'] and metrics['earnings_cv'] > 0.8:
            continue

        # Calculate composite valuation score
        # Multiple ways to be "cheap"
        valuation_points = 0

        # P/E based (using normalized to avoid one-year distortions)
        if metrics['pe_normalized']:
            if metrics['pe_normalized'] < 12:
                valuation_points += 3
            elif metrics['pe_normalized'] < 18:
                valuation_points += 2
            elif metrics['pe_normalized'] < 25:
                valuation_points += 1

        # P/S based
        if metrics['ps_ratio']:
            if metrics['ps_ratio'] < 1.0:
                valuation_points += 3
            elif metrics['ps_ratio'] < 2.0:
                valuation_points += 2
            elif metrics['ps_ratio'] < 3.0:
                valuation_points += 1

        # EV/Sales
        if metrics['ev_sales']:
            if metrics['ev_sales'] < 1.5:
                valuation_points += 2
            elif metrics['ev_sales'] < 3.0:
                valuation_points += 1

        # Earnings yield
        if metrics['earnings_yield_normalized']:
            if metrics['earnings_yield_normalized'] > 0.08:  # >8% yield
                valuation_points += 3
            elif metrics['earnings_yield_normalized'] > 0.05:  # >5% yield
                valuation_points += 2
            elif metrics['earnings_yield_normalized'] > 0.03:  # >3% yield
                valuation_points += 1

        # Cheap vs history
        if valuation_pct >= 80:
            valuation_points += 2
        elif valuation_pct >= 60:
            valuation_points += 1

        # Margin compression bonus (mean reversion opportunity)
        margin_reversion_opportunity = False
        if metrics['margin_compressed'] and metrics['margin_vs_history'] < -0.03:
            valuation_points += 2
            margin_reversion_opportunity = True

        # Calculate SWAN score
        swan_score = 0

        # Quality (35 points)
        swan_score += min(12, financial_health / 100 * 12)
        swan_score += min(12, earnings_quality / 100 * 12)
        swan_score += min(11, quality / 100 * 11)

        # Stability (25 points)
        swan_score += metrics['pct_profitable'] * 15
        if metrics['earnings_cv']:
            stability_bonus = max(0, 1 - metrics['earnings_cv']) * 10
            swan_score += stability_bonus

        # Valuation (40 points) - now based on composite
        swan_score += min(40, valuation_points * 3)

        candidates.append({
            'ticker': ticker,
            'company': comp['company_name'],
            'swan_score': round(swan_score, 1),
            'valuation_points': valuation_points,

            # Quality scores
            'financial_health': round(financial_health, 0),
            'earnings_quality': round(earnings_quality, 0),
            'profitability': round(profitability, 0),
            'quality': round(quality, 0),

            # Stability
            'pct_profitable': round(metrics['pct_profitable'] * 100, 0),
            'earnings_cv': round(metrics['earnings_cv'], 2) if metrics['earnings_cv'] else None,

            # Margins
            'current_margin': round(metrics['current_net_margin'] * 100, 1),
            'hist_margin': round(metrics['avg_hist_net_margin'] * 100, 1),
            'margin_vs_hist': round(metrics['margin_vs_history'] * 100, 1),
            'margin_compressed': margin_reversion_opportunity,

            # Valuation - multiple metrics
            'pe': round(metrics['pe_ratio'], 1) if metrics['pe_ratio'] and metrics['pe_ratio'] < 500 else None,
            'pe_normalized': round(metrics['pe_normalized'], 1) if metrics['pe_normalized'] and metrics['pe_normalized'] < 500 else None,
            'ps': round(metrics['ps_ratio'], 2) if metrics['ps_ratio'] else None,
            'ev_sales': round(metrics['ev_sales'], 2) if metrics['ev_sales'] else None,
            'ey_pct': round(metrics['earnings_yield'] * 100, 1) if metrics['earnings_yield'] else None,
            'ey_norm_pct': round(metrics['earnings_yield_normalized'] * 100, 1) if metrics['earnings_yield_normalized'] else None,
            'peg': round(metrics['peg'], 2) if metrics['peg'] and metrics['peg'] < 10 else None,
            'valuation_pct': round(valuation_pct, 0),

            # Size & leverage
            'market_cap_b': round(metrics['market_cap'] / 1e9, 2),
            'debt_equity': round(metrics['debt_to_equity'], 2) if metrics['debt_to_equity'] else None,

            # Growth
            'revenue_cagr': round(metrics['revenue_cagr'] * 100, 1),
        })

    if not candidates:
        print("No candidates found!")
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values('swan_score', ascending=False)

    # Print results
    print(f"\nFound {len(df)} SWAN candidates\n")

    # Top candidates
    print("="*120)
    print("TOP SWAN STOCKS (Quality + Stability + Reasonable Valuation)")
    print("="*120)

    print(f"\n{'Ticker':<10} {'Company':<25} {'SWAN':>5} {'VP':>3} {'PE':>5} {'PE*':>5} {'P/S':>5} {'EY%':>5} {'Margin':>7} {'ΔMarg':>6} {'Val%':>5}")
    print("-"*105)

    for _, row in df.head(35).iterrows():
        pe = f"{row['pe']:.0f}" if row['pe'] else "-"
        pe_n = f"{row['pe_normalized']:.0f}" if row['pe_normalized'] else "-"
        ps = f"{row['ps']:.1f}" if row['ps'] else "-"
        ey = f"{row['ey_pct']:.1f}" if row['ey_pct'] else "-"
        margin = f"{row['current_margin']:.0f}%"
        d_margin = f"{row['margin_vs_hist']:+.0f}%" if row['margin_vs_hist'] else "-"

        marker = " *" if row['margin_compressed'] else ""

        print(f"{row['ticker']:<10} {row['company'][:24]:<25} {row['swan_score']:>5.0f} {row['valuation_points']:>3} "
              f"{pe:>5} {pe_n:>5} {ps:>5} {ey:>5} {margin:>7} {d_margin:>6} {row['valuation_pct']:>5.0f}{marker}")

    print("\n* = Margin compressed vs history (mean reversion candidate)")
    print("PE* = Normalized P/E (using historical avg margin)")
    print("VP = Valuation Points (composite score)")

    # Margin compression candidates
    margin_compressed = df[df['margin_compressed'] == True].sort_values('swan_score', ascending=False)

    if len(margin_compressed) > 0:
        print("\n" + "="*100)
        print("MARGIN COMPRESSION CANDIDATES (Current margin below historical)")
        print("="*100)

        print(f"\n{'Ticker':<10} {'Company':<25} {'Current':>8} {'Historical':>10} {'Delta':>8} {'PE':>6} {'P/S':>5}")
        print("-"*80)

        for _, row in margin_compressed.head(20).iterrows():
            pe = f"{row['pe']:.0f}" if row['pe'] else "-"
            ps = f"{row['ps']:.1f}" if row['ps'] else "-"
            print(f"{row['ticker']:<10} {row['company'][:24]:<25} {row['current_margin']:>7.1f}% "
                  f"{row['hist_margin']:>9.1f}% {row['margin_vs_hist']:>+7.1f}% {pe:>6} {ps:>5}")

    # Best absolute value
    cheap_absolute = df[
        ((df['pe_normalized'].notna()) & (df['pe_normalized'] < 20)) |
        ((df['ps'].notna()) & (df['ps'] < 2)) |
        ((df['ey_pct'].notna()) & (df['ey_pct'] > 5))
    ].sort_values('swan_score', ascending=False)

    print("\n" + "="*100)
    print("BEST ABSOLUTE VALUE (P/E norm < 20 OR P/S < 2 OR EY > 5%)")
    print("="*100)

    print(f"\n{'Ticker':<10} {'Company':<25} {'SWAN':>5} {'PE*':>5} {'P/S':>5} {'EY%':>5} {'Margin':>7} {'CAGR':>6}")
    print("-"*80)

    for _, row in cheap_absolute.head(25).iterrows():
        pe_n = f"{row['pe_normalized']:.0f}" if row['pe_normalized'] else "-"
        ps = f"{row['ps']:.1f}" if row['ps'] else "-"
        ey = f"{row['ey_pct']:.1f}" if row['ey_pct'] else "-"
        margin = f"{row['current_margin']:.0f}%"
        cagr = f"{row['revenue_cagr']:.0f}%"

        print(f"{row['ticker']:<10} {row['company'][:24]:<25} {row['swan_score']:>5.0f} "
              f"{pe_n:>5} {ps:>5} {ey:>5} {margin:>7} {cagr:>6}")

    # Export
    output_file = 'swan_v2.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='All SWAN', index=False)
        margin_compressed.to_excel(writer, sheet_name='Margin Compressed', index=False)
        cheap_absolute.to_excel(writer, sheet_name='Cheap Absolute', index=False)

    print(f"\n\nExported to {output_file}")

    return df


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await find_swan_v2(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
