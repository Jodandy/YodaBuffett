#!/usr/bin/env python3
"""
Find SWAN Stocks v3 - With Debt Analysis

Key insight: "Would I buy this whole company including its debt?"

Added metrics:
- Net Debt / EBITDA - How many years to pay off debt?
- Interest Coverage - Can they afford debt payments?
- Debt trend - Is debt growing or shrinking?
- FCF Yield on EV - Real yield after accounting for debt
- Debt-adjusted valuation scoring
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from uuid import UUID
from typing import Dict, List, Optional

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_comprehensive_metrics(conn, ticker: str, score_date: date) -> Optional[Dict]:
    """Get comprehensive metrics including debt analysis."""
    ticker_space = ticker.replace('-', ' ')

    # Get historical financials (4 years)
    cutoff = score_date - timedelta(days=4 * 365)

    financials = await conn.fetch("""
        SELECT period_date, net_income, total_revenue, operating_income,
               gross_profit, ebitda, interest_expense
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND period_date <= $4
          AND statement_type = 'annual'
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    if len(financials) < 2:
        return None

    # Get historical balance sheets
    balance_sheets = await conn.fetch("""
        SELECT period_date, total_equity, total_debt, long_term_debt,
               cash_and_equivalents, shares_outstanding, total_assets
        FROM balance_sheet_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND period_date <= $4
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    if len(balance_sheets) < 2:
        return None

    # Get cash flow for FCF
    cash_flows = await conn.fetch("""
        SELECT period_date, operating_cash_flow, capital_expenditure, free_cash_flow
        FROM cash_flow_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND period_date <= $4
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    # Get current price
    price = await conn.fetchrow("""
        SELECT close_price FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
    """, ticker, score_date)

    if not price:
        return None

    # Latest data
    latest_fin = financials[-1]
    latest_bal = balance_sheets[-1]

    price_val = float(price['close_price'])
    shares = float(latest_bal['shares_outstanding']) if latest_bal['shares_outstanding'] else None

    if not shares or shares <= 0:
        return None

    market_cap = price_val * shares

    # Extract values with safe defaults
    revenue = float(latest_fin['total_revenue']) if latest_fin['total_revenue'] else 0
    net_income = float(latest_fin['net_income']) if latest_fin['net_income'] else 0
    operating_income = float(latest_fin['operating_income']) if latest_fin['operating_income'] else 0
    ebitda = float(latest_fin['ebitda']) if latest_fin['ebitda'] else None
    interest_expense = float(latest_fin['interest_expense']) if latest_fin['interest_expense'] else 0

    equity = float(latest_bal['total_equity']) if latest_bal['total_equity'] else 0
    total_debt = float(latest_bal['total_debt']) if latest_bal['total_debt'] else 0
    long_term_debt = float(latest_bal['long_term_debt']) if latest_bal['long_term_debt'] else 0
    cash = float(latest_bal['cash_and_equivalents']) if latest_bal['cash_and_equivalents'] else 0
    total_assets = float(latest_bal['total_assets']) if latest_bal['total_assets'] else 0

    # Calculate EBITDA if not available
    if not ebitda and operating_income:
        # Rough approximation: EBITDA ≈ Operating Income * 1.15 (adds back D&A)
        ebitda = operating_income * 1.15

    # Net debt
    net_debt = total_debt - cash

    # Enterprise Value
    ev = market_cap + net_debt

    # ===== DEBT METRICS =====

    # Net Debt / EBITDA (years to pay off debt)
    net_debt_to_ebitda = None
    if ebitda and ebitda > 0:
        net_debt_to_ebitda = net_debt / ebitda

    # Interest Coverage (how easily can they pay interest)
    interest_coverage = None
    if interest_expense and interest_expense > 0 and operating_income:
        interest_coverage = operating_income / interest_expense

    # Debt to Equity
    debt_to_equity = total_debt / equity if equity > 0 else None

    # Debt to Assets
    debt_to_assets = total_debt / total_assets if total_assets > 0 else None

    # Debt trend (is debt growing?)
    debt_growth = None
    if len(balance_sheets) >= 2:
        old_debt = float(balance_sheets[0]['total_debt']) if balance_sheets[0]['total_debt'] else 0
        if old_debt > 0:
            years = (balance_sheets[-1]['period_date'] - balance_sheets[0]['period_date']).days / 365
            if years > 0:
                debt_growth = ((total_debt / old_debt) ** (1/years) - 1) if old_debt > 0 else None

    # ===== VALUATION METRICS =====

    # P/E
    pe_ratio = market_cap / net_income if net_income > 0 else None

    # P/S
    ps_ratio = market_cap / revenue if revenue > 0 else None

    # EV/EBITDA (the RIGHT way to value leveraged companies)
    ev_to_ebitda = ev / ebitda if ebitda and ebitda > 0 else None

    # EV/Sales
    ev_to_sales = ev / revenue if revenue > 0 else None

    # Earnings yield on market cap
    earnings_yield = net_income / market_cap if market_cap > 0 else None

    # ===== FREE CASH FLOW METRICS =====

    fcf = None
    if cash_flows:
        latest_cf = cash_flows[-1]
        if latest_cf['free_cash_flow']:
            fcf = float(latest_cf['free_cash_flow'])
        elif latest_cf['operating_cash_flow'] and latest_cf['capital_expenditure']:
            fcf = float(latest_cf['operating_cash_flow']) - abs(float(latest_cf['capital_expenditure']))

    # FCF yield on market cap
    fcf_yield_mkt = fcf / market_cap if fcf and market_cap > 0 else None

    # FCF yield on EV (the REAL yield - what you'd get as owner of whole business)
    fcf_yield_ev = fcf / ev if fcf and ev > 0 else None

    # ===== HISTORICAL MARGINS =====

    net_margins = []
    for f in financials:
        if f['total_revenue'] and f['total_revenue'] > 0 and f['net_income']:
            net_margins.append(float(f['net_income']) / float(f['total_revenue']))

    avg_net_margin = np.mean(net_margins) if net_margins else None
    current_net_margin = net_income / revenue if revenue > 0 else None

    # Margin compression?
    margin_vs_history = None
    if avg_net_margin and current_net_margin:
        margin_vs_history = current_net_margin - avg_net_margin

    # Normalized P/E (if margins revert to historical)
    normalized_pe = None
    if avg_net_margin and revenue > 0 and market_cap > 0:
        normalized_earnings = revenue * avg_net_margin
        if normalized_earnings > 0:
            normalized_pe = market_cap / normalized_earnings

    # Revenue growth
    revenue_cagr = None
    if len(financials) >= 2:
        first_rev = float(financials[0]['total_revenue']) if financials[0]['total_revenue'] else 0
        if first_rev > 0 and revenue > 0:
            years = (financials[-1]['period_date'] - financials[0]['period_date']).days / 365
            if years > 0:
                revenue_cagr = (revenue / first_rev) ** (1/years) - 1

    return {
        'market_cap': market_cap,
        'enterprise_value': ev,
        'net_debt': net_debt,

        # Debt metrics
        'net_debt_to_ebitda': net_debt_to_ebitda,
        'interest_coverage': interest_coverage,
        'debt_to_equity': debt_to_equity,
        'debt_to_assets': debt_to_assets,
        'debt_growth': debt_growth,

        # Valuation
        'pe_ratio': pe_ratio,
        'ps_ratio': ps_ratio,
        'ev_to_ebitda': ev_to_ebitda,
        'ev_to_sales': ev_to_sales,
        'earnings_yield': earnings_yield,
        'normalized_pe': normalized_pe,

        # Cash flow
        'fcf': fcf,
        'fcf_yield_mkt': fcf_yield_mkt,
        'fcf_yield_ev': fcf_yield_ev,

        # Margins
        'current_net_margin': current_net_margin,
        'avg_net_margin': avg_net_margin,
        'margin_vs_history': margin_vs_history,

        # Growth
        'revenue_cagr': revenue_cagr,
    }


async def get_dimension_scores(conn, company_id: UUID, score_date: date) -> Dict:
    """Get dimension scores for a company."""
    scores = await conn.fetch("""
        SELECT dimension_code, score
        FROM daily_dimension_scores
        WHERE company_id = $1 AND score_date = $2
    """, company_id, score_date)

    return {r['dimension_code']: float(r['score']) if r['score'] else 0 for r in scores}


def calculate_debt_burden_score(metrics: Dict) -> float:
    """
    Calculate a debt burden score (0-100, higher = LESS debt burden = better).

    Penalizes:
    - High Net Debt / EBITDA
    - Low interest coverage
    - Growing debt
    - High debt to equity
    """
    score = 100  # Start perfect, deduct for problems

    # Net Debt / EBITDA penalties
    nd_ebitda = metrics.get('net_debt_to_ebitda')
    if nd_ebitda is not None:
        if nd_ebitda < 0:  # Net cash position
            score += 10  # Bonus!
        elif nd_ebitda < 1:
            pass  # Fine
        elif nd_ebitda < 2:
            score -= 10
        elif nd_ebitda < 3:
            score -= 25
        elif nd_ebitda < 4:
            score -= 40
        elif nd_ebitda < 5:
            score -= 55
        else:
            score -= 70  # Very leveraged

    # Interest coverage penalties
    int_cov = metrics.get('interest_coverage')
    if int_cov is not None:
        if int_cov > 10:
            pass  # Excellent
        elif int_cov > 5:
            score -= 5
        elif int_cov > 3:
            score -= 15
        elif int_cov > 2:
            score -= 30
        elif int_cov > 1:
            score -= 50
        else:
            score -= 70  # Can't cover interest!

    # Debt growth penalties
    debt_growth = metrics.get('debt_growth')
    if debt_growth is not None:
        if debt_growth < 0:
            score += 5  # Paying down debt, bonus
        elif debt_growth < 0.05:
            pass  # Modest
        elif debt_growth < 0.10:
            score -= 10
        elif debt_growth < 0.20:
            score -= 20
        else:
            score -= 35  # Debt growing fast

    # Debt to equity penalties
    d_e = metrics.get('debt_to_equity')
    if d_e is not None:
        if d_e < 0.3:
            pass  # Conservative
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
    """
    Would I buy this whole business?

    Considers:
    - FCF yield on EV (real owner earnings)
    - EV/EBITDA (reasonable price for whole business)
    - Debt burden
    - Quality of earnings
    """
    score = 0

    # FCF yield on EV (most important - real cash you'd get as owner)
    fcf_yield_ev = metrics.get('fcf_yield_ev')
    if fcf_yield_ev is not None:
        if fcf_yield_ev > 0.10:  # >10% FCF yield on EV = excellent
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
            score -= 10  # Negative FCF

    # EV/EBITDA (reasonable acquisition price?)
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
            score -= 5  # Expensive

    # Normalized P/E (margin reversion potential)
    norm_pe = metrics.get('normalized_pe')
    if norm_pe is not None:
        if norm_pe < 10:
            score += 15
        elif norm_pe < 15:
            score += 10
        elif norm_pe < 20:
            score += 5

    # Debt burden score contribution
    debt_score = calculate_debt_burden_score(metrics)
    score += (debt_score / 100) * 25  # Max 25 points from debt quality

    return min(100, score)


async def find_swan_stocks_v3(conn, score_date: date = None):
    """Find SWAN stocks with proper debt analysis."""

    if score_date is None:
        score_date = await conn.fetchval("""
            SELECT score_date FROM daily_dimension_scores
            GROUP BY score_date
            HAVING COUNT(DISTINCT company_id) > 1000
            ORDER BY score_date DESC
            LIMIT 1
        """)

    print(f"Finding SWAN stocks v3 as of {score_date}")
    print("="*100)
    print("\nKey question: Would I buy this WHOLE company, including its debt?")
    print("\nFilters:")
    print("  - Quality dimensions (financial health, earnings quality, profitability)")
    print("  - Debt analysis (Net Debt/EBITDA, interest coverage, debt trend)")
    print("  - Owner economics (FCF yield on EV, EV/EBITDA)")
    print()

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
        dim_scores = await get_dimension_scores(conn, company_uuid, score_date)

        # Quality filters
        financial_health = dim_scores.get('financial_health', 0)
        earnings_quality = dim_scores.get('earnings_quality', 0)
        profitability = dim_scores.get('profitability', 0)

        if financial_health < 40:
            continue
        if earnings_quality < 35:
            continue
        if profitability < 30:
            continue

        # Get comprehensive metrics
        metrics = await get_comprehensive_metrics(conn, ticker, score_date)
        if not metrics:
            continue

        # Must have positive earnings
        if not metrics['pe_ratio'] or metrics['pe_ratio'] <= 0:
            continue

        # Calculate scores
        debt_score = calculate_debt_burden_score(metrics)
        owner_value_score = calculate_owner_value_score(metrics)

        # Combined SWAN score
        # Quality (40%) + Debt Safety (30%) + Owner Value (30%)
        quality_component = (financial_health + earnings_quality + profitability) / 3
        swan_score = (quality_component * 0.4) + (debt_score * 0.3) + (owner_value_score * 0.3)

        # Flags
        margin_compressed = (metrics['margin_vs_history'] or 0) < -0.02
        high_leverage = (metrics['net_debt_to_ebitda'] or 0) > 3
        debt_growing = (metrics['debt_growth'] or 0) > 0.10

        candidates.append({
            'ticker': ticker,
            'company': comp['company_name'],
            'swan_score': round(swan_score, 1),
            'debt_score': round(debt_score, 0),
            'owner_value': round(owner_value_score, 0),

            # Debt metrics
            'nd_ebitda': round(metrics['net_debt_to_ebitda'], 1) if metrics['net_debt_to_ebitda'] else None,
            'int_coverage': round(metrics['interest_coverage'], 1) if metrics['interest_coverage'] else None,
            'd_e': round(metrics['debt_to_equity'], 2) if metrics['debt_to_equity'] else None,
            'debt_growth': round(metrics['debt_growth'] * 100, 1) if metrics['debt_growth'] else None,

            # Valuation
            'pe': round(metrics['pe_ratio'], 1) if metrics['pe_ratio'] else None,
            'pe_norm': round(metrics['normalized_pe'], 1) if metrics['normalized_pe'] else None,
            'ev_ebitda': round(metrics['ev_to_ebitda'], 1) if metrics['ev_to_ebitda'] else None,
            'ev_sales': round(metrics['ev_to_sales'], 2) if metrics['ev_to_sales'] else None,
            'ps': round(metrics['ps_ratio'], 2) if metrics['ps_ratio'] else None,

            # Cash flow
            'fcf_yield_ev': round(metrics['fcf_yield_ev'] * 100, 1) if metrics['fcf_yield_ev'] else None,
            'fcf_yield_mkt': round(metrics['fcf_yield_mkt'] * 100, 1) if metrics['fcf_yield_mkt'] else None,

            # Other
            'margin': round(metrics['current_net_margin'] * 100, 1) if metrics['current_net_margin'] else None,
            'margin_delta': round(metrics['margin_vs_history'] * 100, 1) if metrics['margin_vs_history'] else None,
            'cagr': round(metrics['revenue_cagr'] * 100, 1) if metrics['revenue_cagr'] else None,
            'mkt_cap_b': round(metrics['market_cap'] / 1e9, 2),
            'ev_b': round(metrics['enterprise_value'] / 1e9, 2),
            'net_debt_b': round(metrics['net_debt'] / 1e9, 2),

            # Quality dims
            'fin_health': round(financial_health, 0),
            'earn_qual': round(earnings_quality, 0),
            'profit': round(profitability, 0),

            # Flags
            'margin_compressed': margin_compressed,
            'high_leverage': high_leverage,
            'debt_growing': debt_growing,
        })

    if not candidates:
        print("No SWAN candidates found!")
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values('swan_score', ascending=False)

    print(f"\nFound {len(df)} SWAN candidates")

    # ===== MAIN TABLE =====
    print("\n" + "="*120)
    print("TOP SWAN STOCKS (Quality + Low Debt + Owner Value)")
    print("="*120)

    print(f"\n{'Ticker':<10} {'Company':<22} {'SWAN':>5} {'Debt':>5} {'OwnV':>5} {'ND/EB':>6} {'IntCov':>6} {'D/E':>5} {'EV/EB':>6} {'FCF%EV':>7} Flags")
    print("-"*115)

    for _, row in df.head(35).iterrows():
        nd_eb = f"{row['nd_ebitda']:.1f}" if row['nd_ebitda'] else "N/A"
        int_cov = f"{row['int_coverage']:.1f}" if row['int_coverage'] else "N/A"
        d_e = f"{row['d_e']:.2f}" if row['d_e'] else "N/A"
        ev_eb = f"{row['ev_ebitda']:.1f}" if row['ev_ebitda'] else "N/A"
        fcf_ev = f"{row['fcf_yield_ev']:.1f}%" if row['fcf_yield_ev'] else "N/A"

        flags = []
        if row['margin_compressed']:
            flags.append("MARG↓")
        if row['high_leverage']:
            flags.append("LEV!")
        if row['debt_growing']:
            flags.append("DEBT↑")
        if row['nd_ebitda'] and row['nd_ebitda'] < 0:
            flags.append("CASH+")

        flag_str = " ".join(flags)

        print(f"{row['ticker']:<10} {row['company'][:21]:<22} {row['swan_score']:>5.1f} "
              f"{row['debt_score']:>5.0f} {row['owner_value']:>5.0f} "
              f"{nd_eb:>6} {int_cov:>6} {d_e:>5} {ev_eb:>6} {fcf_ev:>7} {flag_str}")

    # ===== BEST OWNER ECONOMICS =====
    print("\n" + "="*120)
    print("BEST OWNER ECONOMICS (High FCF Yield on EV + Low Leverage)")
    print("="*120)

    owner_df = df[
        (df['fcf_yield_ev'].notna()) &
        (df['fcf_yield_ev'] > 5) &  # >5% FCF yield on EV
        ((df['nd_ebitda'].isna()) | (df['nd_ebitda'] < 3))  # Reasonable leverage
    ].sort_values('fcf_yield_ev', ascending=False)

    print(f"\n{'Ticker':<10} {'Company':<22} {'FCF%EV':>7} {'EV/EB':>6} {'ND/EB':>6} {'PE':>5} {'Margin':>7} {'CAGR':>6}")
    print("-"*90)

    for _, row in owner_df.head(20).iterrows():
        fcf_ev = f"{row['fcf_yield_ev']:.1f}%" if row['fcf_yield_ev'] else "N/A"
        ev_eb = f"{row['ev_ebitda']:.1f}" if row['ev_ebitda'] else "N/A"
        nd_eb = f"{row['nd_ebitda']:.1f}" if row['nd_ebitda'] else "N/A"
        pe = f"{row['pe']:.0f}" if row['pe'] else "N/A"
        margin = f"{row['margin']:.1f}%" if row['margin'] else "N/A"
        cagr = f"{row['cagr']:.1f}%" if row['cagr'] else "N/A"

        print(f"{row['ticker']:<10} {row['company'][:21]:<22} {fcf_ev:>7} {ev_eb:>6} "
              f"{nd_eb:>6} {pe:>5} {margin:>7} {cagr:>6}")

    # ===== NET CASH COMPANIES =====
    print("\n" + "="*120)
    print("NET CASH COMPANIES (Negative Net Debt = Cash > Debt)")
    print("="*120)

    cash_df = df[df['nd_ebitda'].notna() & (df['nd_ebitda'] < 0)].sort_values('swan_score', ascending=False)

    if len(cash_df) > 0:
        print(f"\n{'Ticker':<10} {'Company':<22} {'SWAN':>5} {'Net Cash':>10} {'PE':>5} {'FCF%EV':>7} {'Margin':>7}")
        print("-"*85)

        for _, row in cash_df.head(15).iterrows():
            net_cash = f"${abs(row['net_debt_b']):.2f}B"
            pe = f"{row['pe']:.0f}" if row['pe'] else "N/A"
            fcf_ev = f"{row['fcf_yield_ev']:.1f}%" if row['fcf_yield_ev'] else "N/A"
            margin = f"{row['margin']:.1f}%" if row['margin'] else "N/A"

            print(f"{row['ticker']:<10} {row['company'][:21]:<22} {row['swan_score']:>5.1f} "
                  f"{net_cash:>10} {pe:>5} {fcf_ev:>7} {margin:>7}")
    else:
        print("\nNo net cash companies found in filtered list.")

    # ===== AVOID LIST (High Leverage) =====
    print("\n" + "="*120)
    print("⚠️  AVOID: High Leverage (ND/EBITDA > 3 or Debt Growing > 10%/yr)")
    print("="*120)

    avoid_df = df[df['high_leverage'] | df['debt_growing']].sort_values('nd_ebitda', ascending=False)

    if len(avoid_df) > 0:
        print(f"\n{'Ticker':<10} {'Company':<22} {'ND/EB':>6} {'D/E':>6} {'Debt↑':>7} {'PE':>5} {'P/S':>5} Why Avoid")
        print("-"*95)

        for _, row in avoid_df.head(15).iterrows():
            nd_eb = f"{row['nd_ebitda']:.1f}" if row['nd_ebitda'] else "N/A"
            d_e = f"{row['d_e']:.2f}" if row['d_e'] else "N/A"
            debt_gr = f"{row['debt_growth']:.1f}%" if row['debt_growth'] else "N/A"
            pe = f"{row['pe']:.0f}" if row['pe'] else "N/A"
            ps = f"{row['ps']:.2f}" if row['ps'] else "N/A"

            reasons = []
            if row['high_leverage']:
                reasons.append(f"Leverage {nd_eb}x")
            if row['debt_growing']:
                reasons.append(f"Debt growing {debt_gr}")

            print(f"{row['ticker']:<10} {row['company'][:21]:<22} {nd_eb:>6} {d_e:>6} "
                  f"{debt_gr:>7} {pe:>5} {ps:>5} {', '.join(reasons)}")

    # Export
    output_file = 'swan_v3.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='All SWAN Candidates', index=False)

        # Best owner economics
        owner_df.to_excel(writer, sheet_name='Best Owner Economics', index=False)

        # Net cash
        if len(cash_df) > 0:
            cash_df.to_excel(writer, sheet_name='Net Cash', index=False)

        # Avoid list
        if len(avoid_df) > 0:
            avoid_df.to_excel(writer, sheet_name='Avoid - High Leverage', index=False)

        # Clean (low leverage + good FCF)
        clean_df = df[
            (df['debt_score'] >= 70) &
            (df['fcf_yield_ev'].notna()) &
            (df['fcf_yield_ev'] > 3)
        ].sort_values('swan_score', ascending=False)
        clean_df.to_excel(writer, sheet_name='Clean Balance Sheets', index=False)

    print(f"\n\nExported to {output_file}")

    return df


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await find_swan_stocks_v3(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
