#!/usr/bin/env python3
"""
Backtest Golden Combos - Focused Quality Screening

Test the best-performing Tier + Cash Quality + Business Model combinations
over many historical dates and multiple timeframes.

Target Combos (identified from quality_backtest_full.xlsx):
    Tier 3, Good, Cash Cow
    Tier 1, Good, Cash Cow
    Tier 2, Moderate, Compounder
    Tier 3, Excellent, Cash Cow
    Tier 2, Good, Compounder
    Tier 2, Excellent, Compounder
    Tier 1, Moderate, Compounder
    Tier 2, Excellent, Cash Cow
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, Optional, List, Tuple
from find_quality_candidates import (
    get_business_characteristics,
    assess_quality_tier,
    classify_business_model,
    get_size_category,
    get_cash_quality,
)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# The golden combos to test
GOLDEN_COMBOS = [
    (3, 'Good', 'Cash Cow'),
    (1, 'Good', 'Cash Cow'),
    (2, 'Moderate', 'Compounder'),
    (3, 'Excellent', 'Cash Cow'),
    (2, 'Good', 'Compounder'),
    (2, 'Excellent', 'Compounder'),
    (1, 'Moderate', 'Compounder'),
    (2, 'Excellent', 'Cash Cow'),
]

# Return measurement timeframes in months
TIMEFRAMES = [3, 6, 12, 18, 24]


async def get_forward_return(conn, ticker: str, start_date: date, months: int) -> Optional[float]:
    """
    Get forward return over specified months.
    Uses first available price within 10 days of start, and end date.
    """
    end_date = start_date + relativedelta(months=months)

    # Get price at start (allow up to 10 days forward for trading day)
    start_price = await conn.fetchrow("""
        SELECT close_price, date FROM daily_price_data
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        ORDER BY date ASC LIMIT 1
    """, ticker, start_date, start_date + timedelta(days=10))

    if not start_price:
        return None

    # Get price at end
    end_price = await conn.fetchrow("""
        SELECT close_price FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC LIMIT 1
    """, ticker, end_date)

    if not end_price:
        return None

    start_p = float(start_price['close_price'])
    end_p = float(end_price['close_price'])

    if start_p <= 0:
        return None

    return (end_p - start_p) / start_p


async def get_market_return(conn, start_date: date, months: int) -> Optional[float]:
    """
    Get market return (using OMXS30 or similar proxy).
    For now, we'll use a simple average of all large caps.
    """
    # This is a simplification - ideally use an index
    # For Nordic, could use ^OMX (but we don't have that in daily_price_data)
    # Return None for now, caller will handle
    return None


def matches_combo(tier: int, cash_quality: str, biz_model: str, combo: Tuple) -> bool:
    """Check if company matches a golden combo."""
    target_tier, target_cash, target_model = combo
    return tier == target_tier and cash_quality == target_cash and biz_model == target_model


def get_combo_name(tier: int, cash_quality: str, biz_model: str) -> str:
    """Create readable combo name."""
    return f"T{tier} {cash_quality} {biz_model}"


async def analyze_date(conn, pick_date: date, today: date) -> List[Dict]:
    """Analyze all companies for a single pick date."""

    results = []

    # Get all companies with price data around that date
    companies = await conn.fetch("""
        SELECT DISTINCT cm.id::text, cm.primary_ticker, cm.company_name
        FROM company_master cm
        JOIN daily_price_data dpd ON dpd.symbol = cm.primary_ticker
        WHERE dpd.date >= ($1::date - INTERVAL '7 days')
          AND dpd.date <= ($1::date + INTERVAL '7 days')
    """, pick_date)

    for comp in companies:
        ticker = comp['primary_ticker']

        # Get characteristics at that point in time
        chars = await get_business_characteristics(conn, ticker, pick_date)
        if not chars:
            continue

        # Skip micro caps and unprofitable
        if chars['market_cap'] < 25e6:
            continue
        if not chars['net_margin'] or chars['net_margin'] <= 0:
            continue

        # Assess quality
        try:
            tier, tier_desc, reasons, concerns, val_notes, score = assess_quality_tier(chars)
            biz_model, biz_model_reason = classify_business_model(chars)
        except Exception:
            continue

        # Get cash quality
        ocf_ni_val = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
        cash_quality = get_cash_quality(ocf_ni_val)

        # Check if matches any golden combo
        combo_match = None
        for combo in GOLDEN_COMBOS:
            if matches_combo(tier, cash_quality, biz_model, combo):
                combo_match = combo
                break

        # Get returns for all timeframes
        returns = {}
        for months in TIMEFRAMES:
            end_date = pick_date + relativedelta(months=months)
            if end_date <= today:
                ret = await get_forward_return(conn, ticker, pick_date, months)
                if ret is not None and -0.95 < ret < 5.0:  # Filter extremes
                    returns[f'return_{months}m'] = ret
                else:
                    returns[f'return_{months}m'] = None
            else:
                returns[f'return_{months}m'] = None

        # Skip if no valid returns
        if all(v is None for v in returns.values()):
            continue

        result = {
            'pick_date': pick_date,
            'ticker': ticker,
            'company': comp['company_name'],
            'tier': tier,
            'tier_desc': tier_desc,
            'cash_quality': cash_quality,
            'biz_model': biz_model,
            'combo_name': get_combo_name(tier, cash_quality, biz_model),
            'is_golden_combo': combo_match is not None,
            'quality_score': score,
            'size_category': get_size_category(chars['market_cap']),
            'market_cap': chars['market_cap'],
            'roic': chars['roic'],
            'ocf_ni': ocf_ni_val,
            'fcf_yield': chars.get('fcf_yield'),
            'pe': chars['pe'],
            **returns
        }

        results.append(result)

    return results


async def main():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        today = date.today()

        # Generate quarterly test dates going back to 2019
        # First day of each quarter
        test_dates = []
        for year in range(2019, 2025):
            for month in [1, 4, 7, 10]:
                d = date(year, month, 2)  # 2nd to avoid Jan 1 holiday
                # Only include if we can measure at least 6 months forward
                if d + relativedelta(months=6) <= today:
                    test_dates.append(d)

        print("=" * 80)
        print("GOLDEN COMBO BACKTEST")
        print("=" * 80)
        print(f"\nTest dates: {len(test_dates)} quarterly periods")
        print(f"From: {min(test_dates)} to {max(test_dates)}")
        print(f"Timeframes: {', '.join(f'{m}m' for m in TIMEFRAMES)}")
        print()
        print("Target combos:")
        for combo in GOLDEN_COMBOS:
            print(f"  Tier {combo[0]}, {combo[1]}, {combo[2]}")
        print()

        all_results = []

        for i, pick_date in enumerate(test_dates):
            print(f"\rAnalyzing {pick_date} ({i+1}/{len(test_dates)})...", end='', flush=True)
            results = await analyze_date(conn, pick_date, today)
            all_results.extend(results)

        print(f"\rProcessed {len(test_dates)} dates, {len(all_results)} total observations")

        df = pd.DataFrame(all_results)

        if len(df) == 0:
            print("No results!")
            return

        # Filter to golden combos
        df_golden = df[df['is_golden_combo']].copy()

        print(f"\nGolden combo observations: {len(df_golden)}")
        print()

        # === ANALYSIS BY COMBO ===
        print("=" * 80)
        print("RESULTS BY GOLDEN COMBO")
        print("=" * 80)
        print()

        combo_summary = []

        for combo in GOLDEN_COMBOS:
            tier, cash, model = combo
            combo_name = get_combo_name(tier, cash, model)

            mask = (df['tier'] == tier) & (df['cash_quality'] == cash) & (df['biz_model'] == model)
            combo_df = df[mask]

            if len(combo_df) == 0:
                continue

            print(f"\n{combo_name}")
            print(f"  Observations: {len(combo_df)}")
            print(f"  Unique companies: {combo_df['ticker'].nunique()}")
            print()

            row = {
                'Combo': combo_name,
                'Tier': tier,
                'Cash Quality': cash,
                'Business Model': model,
                'Observations': len(combo_df),
                'Unique Companies': combo_df['ticker'].nunique(),
            }

            print(f"  {'Timeframe':<10} {'N':>5} {'Mean':>8} {'Median':>8} {'Win%':>8} {'Std':>8}")
            print(f"  {'-'*50}")

            for months in TIMEFRAMES:
                col = f'return_{months}m'
                valid = combo_df[col].dropna()
                if len(valid) > 0:
                    mean_ret = valid.mean() * 100
                    median_ret = valid.median() * 100
                    win_rate = (valid > 0).mean() * 100
                    std_ret = valid.std() * 100
                    print(f"  {months:>2}m       {len(valid):>5} {mean_ret:>+7.1f}% {median_ret:>+7.1f}% {win_rate:>7.1f}% {std_ret:>7.1f}%")

                    row[f'{months}m_n'] = len(valid)
                    row[f'{months}m_mean'] = mean_ret / 100
                    row[f'{months}m_median'] = median_ret / 100
                    row[f'{months}m_win_rate'] = win_rate / 100
                    row[f'{months}m_std'] = std_ret / 100

            combo_summary.append(row)

        combo_summary_df = pd.DataFrame(combo_summary)

        # === COMPARISON TABLE ===
        print("\n" + "=" * 80)
        print("12-MONTH RETURN COMPARISON (SORTED BY MEDIAN)")
        print("=" * 80)
        print()

        if '12m_median' in combo_summary_df.columns:
            ranked = combo_summary_df.sort_values('12m_median', ascending=False)
            print(f"{'Combo':<30} {'N':>6} {'Mean':>8} {'Median':>8} {'Win%':>8}")
            print("-" * 65)
            for _, row in ranked.iterrows():
                if pd.notna(row.get('12m_mean')):
                    print(f"{row['Combo']:<30} {row['12m_n']:>6} {row['12m_mean']*100:>+7.1f}% {row['12m_median']*100:>+7.1f}% {row['12m_win_rate']*100:>7.1f}%")

        # === BASELINE COMPARISON ===
        print("\n" + "=" * 80)
        print("BASELINE COMPARISON (ALL PROFITABLE > 25M)")
        print("=" * 80)
        print()

        # All companies meeting basic criteria
        df_all = df[df['market_cap'] >= 25e6].copy()

        print(f"  {'Group':<30} {'N':>6} {'12m Mean':>10} {'12m Median':>12} {'Win%':>8}")
        print("-" * 70)

        # All
        valid_all = df_all['return_12m'].dropna()
        if len(valid_all) > 0:
            print(f"  {'All Companies':<30} {len(valid_all):>6} {valid_all.mean()*100:>+9.1f}% {valid_all.median()*100:>+11.1f}% {(valid_all>0).mean()*100:>7.1f}%")

        # Golden combos combined
        valid_golden = df_golden['return_12m'].dropna()
        if len(valid_golden) > 0:
            print(f"  {'All Golden Combos':<30} {len(valid_golden):>6} {valid_golden.mean()*100:>+9.1f}% {valid_golden.median()*100:>+11.1f}% {(valid_golden>0).mean()*100:>7.1f}%")

        # Non-golden
        df_other = df[~df['is_golden_combo']]
        valid_other = df_other['return_12m'].dropna()
        if len(valid_other) > 0:
            print(f"  {'Non-Golden Combos':<30} {len(valid_other):>6} {valid_other.mean()*100:>+9.1f}% {valid_other.median()*100:>+11.1f}% {(valid_other>0).mean()*100:>7.1f}%")

        # Spread
        if len(valid_golden) > 0 and len(valid_other) > 0:
            spread = (valid_golden.median() - valid_other.median()) * 100
            print(f"\n  ALPHA (Golden median - Other median): {spread:+.1f}%")

        # === TIME STABILITY ===
        print("\n" + "=" * 80)
        print("TIME STABILITY (12M RETURNS BY PICK YEAR)")
        print("=" * 80)
        print()

        df_golden['pick_year'] = pd.to_datetime(df_golden['pick_date']).dt.year

        year_summary = []
        print(f"  {'Year':<6} {'N':>6} {'Mean':>10} {'Median':>10} {'Win%':>8}")
        print("-" * 45)

        for year in sorted(df_golden['pick_year'].unique()):
            year_df = df_golden[df_golden['pick_year'] == year]
            valid = year_df['return_12m'].dropna()
            if len(valid) >= 5:  # Minimum sample
                print(f"  {year:<6} {len(valid):>6} {valid.mean()*100:>+9.1f}% {valid.median()*100:>+9.1f}% {(valid>0).mean()*100:>7.1f}%")
                year_summary.append({
                    'Year': year,
                    'N': len(valid),
                    'Mean': valid.mean(),
                    'Median': valid.median(),
                    'Win Rate': (valid > 0).mean()
                })

        # === TOP PERFORMERS ===
        print("\n" + "=" * 80)
        print("TOP 30 GOLDEN COMBO PICKS (BY 12M RETURN)")
        print("=" * 80)
        print()

        top = df_golden.dropna(subset=['return_12m']).sort_values('return_12m', ascending=False).head(30)
        print(f"{'Ticker':<10} {'Company':<25} {'Pick Date':<12} {'Combo':<25} {'12m':>8}")
        print("-" * 85)
        for _, row in top.iterrows():
            print(f"{row['ticker']:<10} {row['company'][:24]:<25} {row['pick_date']} {row['combo_name']:<25} {row['return_12m']*100:>+7.1f}%")

        # === WORST PERFORMERS ===
        print("\n" + "=" * 80)
        print("BOTTOM 20 GOLDEN COMBO PICKS (WHAT WENT WRONG?)")
        print("=" * 80)
        print()

        bottom = df_golden.dropna(subset=['return_12m']).sort_values('return_12m', ascending=True).head(20)
        print(f"{'Ticker':<10} {'Company':<25} {'Pick Date':<12} {'Combo':<25} {'12m':>8}")
        print("-" * 85)
        for _, row in bottom.iterrows():
            print(f"{row['ticker']:<10} {row['company'][:24]:<25} {row['pick_date']} {row['combo_name']:<25} {row['return_12m']*100:>+7.1f}%")

        # === EXPORT TO EXCEL ===
        output_file = 'golden_combos_backtest.xlsx'

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # All golden combo picks
            df_golden.sort_values(['pick_date', 'return_12m'], ascending=[True, False]).to_excel(
                writer, sheet_name='Golden Combo Picks', index=False)

            # All data for reference
            df.sort_values(['pick_date', 'return_12m'], ascending=[True, False]).to_excel(
                writer, sheet_name='All Companies', index=False)

            # Combo summary
            combo_summary_df.to_excel(writer, sheet_name='Combo Summary', index=False)

            # Year summary
            if year_summary:
                pd.DataFrame(year_summary).to_excel(writer, sheet_name='By Year', index=False)

            # Individual combo sheets
            for combo in GOLDEN_COMBOS:
                tier, cash, model = combo
                combo_name = get_combo_name(tier, cash, model)
                mask = (df['tier'] == tier) & (df['cash_quality'] == cash) & (df['biz_model'] == model)
                combo_df = df[mask]
                if len(combo_df) > 0:
                    sheet_name = combo_name[:31]  # Excel limit
                    combo_df.sort_values('return_12m', ascending=False).to_excel(
                        writer, sheet_name=sheet_name, index=False)

            # Unique companies from golden combos
            unique_tickers = df_golden.groupby('ticker').agg({
                'company': 'first',
                'tier': 'first',
                'cash_quality': 'first',
                'biz_model': 'first',
                'combo_name': 'first',
                'return_12m': ['count', 'mean', 'median'],
                'return_3m': 'mean',
                'return_6m': 'mean',
                'return_24m': 'mean',
            }).reset_index()
            unique_tickers.columns = ['ticker', 'company', 'tier', 'cash_quality', 'biz_model',
                                       'combo_name', 'times_picked', 'avg_12m', 'median_12m',
                                       'avg_3m', 'avg_6m', 'avg_24m']
            unique_tickers.sort_values('times_picked', ascending=False).to_excel(
                writer, sheet_name='Unique Companies', index=False)

        print(f"\n\nExported to {output_file}")
        print(f"  Golden combo observations: {len(df_golden)}")
        print(f"  Unique companies: {df_golden['ticker'].nunique()}")
        print(f"  Test periods: {len(test_dates)}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
