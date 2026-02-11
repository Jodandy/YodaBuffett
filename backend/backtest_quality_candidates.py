#!/usr/bin/env python3
"""
Backtest Quality Candidate Screener

Test if the quality screening formula identifies good long-term investments.
- Pick stocks at start of 2022, 2023, 2024
- Measure total returns (price + dividends) to present
- Compare by tier and business model
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, Optional
from find_quality_candidates import (
    get_business_characteristics,
    assess_quality_tier,
    classify_business_model,
    get_size_category,
    get_cash_quality,
)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_total_return(conn, ticker: str, start_date: date, end_date: date) -> Optional[Dict]:
    """
    Calculate total return including dividends.
    """
    # Get price at start
    start_price = await conn.fetchrow("""
        SELECT close_price, date FROM daily_price_data
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        ORDER BY date ASC
        LIMIT 1
    """, ticker, start_date, start_date + timedelta(days=10))

    if not start_price:
        return None

    # Get price at end
    end_price = await conn.fetchrow("""
        SELECT close_price, date FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
    """, ticker, end_date)

    if not end_price:
        return None

    start_p = float(start_price['close_price'])
    end_p = float(end_price['close_price'])

    if start_p <= 0:
        return None

    # Price return
    price_return = (end_p - start_p) / start_p

    # Get dividends paid during period
    ticker_space = ticker.replace('-', ' ')
    dividends = await conn.fetch("""
        SELECT period_date, dividends_paid
        FROM cash_flow_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date > $3
          AND period_date <= $4
    """, ticker, ticker_space, start_price['date'], end_price['date'])

    # Get shares outstanding to calculate dividend per share
    shares = await conn.fetchrow("""
        SELECT shares_outstanding
        FROM balance_sheet_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
        ORDER BY period_date DESC
        LIMIT 1
    """, ticker, ticker_space, start_date)

    total_dividends = 0
    if dividends and shares and shares['shares_outstanding']:
        shares_out = float(shares['shares_outstanding'])
        for d in dividends:
            if d['dividends_paid']:
                # dividends_paid is negative in cash flow (cash out), so we take abs
                div_total = abs(float(d['dividends_paid']))
                div_per_share = div_total / shares_out
                total_dividends += div_per_share

    dividend_return = total_dividends / start_p

    total_return = price_return + dividend_return

    holding_days = (end_price['date'] - start_price['date']).days
    years = holding_days / 365

    # Annualized return
    if years > 0 and total_return > -1:
        annualized = (1 + total_return) ** (1/years) - 1
    else:
        annualized = None

    return {
        'start_date': start_price['date'],
        'end_date': end_price['date'],
        'start_price': start_p,
        'end_price': end_p,
        'price_return': price_return,
        'dividend_return': dividend_return,
        'total_return': total_return,
        'annualized_return': annualized,
        'holding_years': years,
    }


async def run_backtest(conn, test_dates: list, end_date: date, include_all: bool = True):
    """Run backtest for given start dates.

    Args:
        include_all: If True, include ALL companies with flags. If False, apply filters.
    """

    all_results = []

    for test_date in test_dates:
        print(f"\n{'='*80}")
        print(f"TESTING: Stocks picked on {test_date}")
        print(f"{'='*80}")

        # Get all companies with price data around that date
        companies = await conn.fetch("""
            SELECT DISTINCT cm.id::text, cm.primary_ticker, cm.company_name
            FROM company_master cm
            JOIN daily_price_data dpd ON dpd.symbol = cm.primary_ticker
            WHERE dpd.date >= ($1::date - INTERVAL '7 days')
              AND dpd.date <= ($1::date + INTERVAL '7 days')
        """, test_date)

        print(f"Analyzing {len(companies)} companies...")

        date_results = []

        for i, comp in enumerate(companies):
            if i % 200 == 0 and i > 0:
                print(f"  {i}/{len(companies)}")

            ticker = comp['primary_ticker']

            # Get characteristics at that point in time
            chars = await get_business_characteristics(conn, ticker, test_date)
            if not chars:
                continue

            # Flag columns instead of filtering
            is_small_cap = chars['market_cap'] < 100e6
            is_micro_cap = chars['market_cap'] < 25e6
            is_unprofitable = not chars['net_margin'] or chars['net_margin'] <= 0
            is_negative_gross = not chars['gross_margin'] or chars['gross_margin'] <= 0

            # Categories for combination analysis
            size_category = get_size_category(chars['market_cap'])
            ocf_ni_val = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
            cash_quality = get_cash_quality(ocf_ni_val)

            # Assess quality (may fail for unprofitable companies)
            try:
                tier, tier_desc, reasons, concerns, val_notes, score = assess_quality_tier(chars)
                biz_model, biz_model_reason = classify_business_model(chars)
            except Exception:
                tier, tier_desc, score = 5, "No Data", 0
                biz_model, biz_model_reason = "Unknown", "No data available"

            # Get total return
            returns = await get_total_return(conn, ticker, test_date, end_date)
            if not returns:
                continue

            # Flag extreme returns instead of filtering
            is_extreme_positive = returns['total_return'] > 5.0
            is_extreme_negative = returns['total_return'] < -0.95

            date_results.append({
                'pick_date': test_date,
                'ticker': ticker,
                'company': comp['company_name'],
                'tier': tier,
                'tier_desc': tier_desc,
                'biz_model': biz_model,
                'biz_model_reason': biz_model_reason,
                'quality_score': score,
                'size_category': size_category,
                'cash_quality': cash_quality,

                # Flag columns for filtering
                'is_small_cap': is_small_cap,
                'is_micro_cap': is_micro_cap,
                'is_unprofitable': is_unprofitable,
                'is_negative_gross': is_negative_gross,
                'is_extreme_positive': is_extreme_positive,
                'is_extreme_negative': is_extreme_negative,
                'in_original_filter': not (is_small_cap or is_unprofitable or is_extreme_positive or is_extreme_negative),

                # Characteristics at pick time
                'market_cap': chars['market_cap'],
                'gross_margin': chars['gross_margin'],
                'operating_margin': chars['operating_margin'],
                'net_margin': chars['net_margin'],
                'roic': chars['roic'],
                'roe': chars.get('roe'),
                'ocf_ni': chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni'),
                'fcf_yield': chars.get('fcf_yield'),
                'capex_rev': chars['capex_to_revenue'],
                'pe': chars['pe'],
                'ps': chars.get('ps'),
                'pb': chars.get('pb'),
                'revenue_growth': chars.get('revenue_growth'),
                'earnings_growth': chars.get('earnings_growth'),

                # Returns
                'start_price': returns['start_price'],
                'end_price': returns['end_price'],
                'price_return': returns['price_return'],
                'dividend_return': returns['dividend_return'],
                'total_return': returns['total_return'],
                'annualized_return': returns['annualized_return'],
                'holding_years': returns['holding_years'],
            })

        all_results.extend(date_results)

        # Summary for this date (using original filter for comparison)
        if date_results:
            df_date = pd.DataFrame(date_results)
            df_filtered = df_date[df_date['in_original_filter']]

            print(f"\nResults for {test_date}:")
            print(f"  Total: {len(df_date)} stocks | Filtered (original criteria): {len(df_filtered)}")
            print(f"  Holding period: ~{df_date['holding_years'].mean():.1f} years")
            print()

            # By tier (filtered only for display)
            print("  BY TIER (filtered):")
            for tier in [1, 2, 3, 4, 5]:
                tier_df = df_filtered[df_filtered['tier'] == tier]
                if len(tier_df) > 0:
                    avg_ret = tier_df['total_return'].mean() * 100
                    med_ret = tier_df['total_return'].median() * 100
                    ann_ret = tier_df['annualized_return'].mean() * 100 if tier_df['annualized_return'].notna().any() else 0
                    print(f"    Tier {tier}: {len(tier_df):>3} stocks | Total: {avg_ret:>+6.1f}% | Median: {med_ret:>+6.1f}% | Ann: {ann_ret:>+5.1f}%")

            print()
            print("  BY BUSINESS MODEL (filtered):")
            for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear']:
                model_df = df_filtered[df_filtered['biz_model'] == model]
                if len(model_df) > 0:
                    avg_ret = model_df['total_return'].mean() * 100
                    med_ret = model_df['total_return'].median() * 100
                    print(f"    {model:<12}: {len(model_df):>3} stocks | Total: {avg_ret:>+6.1f}% | Median: {med_ret:>+6.1f}%")

    return pd.DataFrame(all_results)


async def main():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Test dates: start of each year
        test_dates = [
            date(2022, 1, 3),  # First trading day 2022
            date(2023, 1, 2),  # First trading day 2023
            date(2024, 1, 2),  # First trading day 2024
        ]

        end_date = date.today()

        print("="*80)
        print("QUALITY CANDIDATE BACKTEST")
        print("="*80)
        print(f"Testing: {', '.join(str(d) for d in test_dates)}")
        print(f"Measuring returns to: {end_date}")
        print(f"Including: Price returns + Dividends")
        print()

        df = await run_backtest(conn, test_dates, end_date, include_all=True)

        if len(df) == 0:
            print("No results!")
            return

        # Use original filter for summaries
        df_filtered = df[df['in_original_filter']]

        # Overall summary
        print("\n" + "="*80)
        print("OVERALL SUMMARY (ALL PERIODS COMBINED)")
        print("="*80)
        print(f"Total companies: {len(df)} | Filtered (original criteria): {len(df_filtered)}")

        print("\nBY TIER (filtered):")
        print(f"{'Tier':<8} {'Count':>6} {'Avg Total':>12} {'Median':>10} {'Avg Ann':>10} {'Win Rate':>10}")
        print("-"*60)

        for tier in [1, 2, 3, 4, 5]:
            tier_df = df_filtered[df_filtered['tier'] == tier]
            if len(tier_df) > 0:
                avg_ret = tier_df['total_return'].mean() * 100
                med_ret = tier_df['total_return'].median() * 100
                ann_ret = tier_df['annualized_return'].mean() * 100
                win_rate = (tier_df['total_return'] > 0).mean() * 100
                print(f"Tier {tier:<3} {len(tier_df):>6} {avg_ret:>+11.1f}% {med_ret:>+9.1f}% {ann_ret:>+9.1f}% {win_rate:>9.1f}%")

        # Spread
        t1_ret = df_filtered[df_filtered['tier'] == 1]['total_return'].mean() * 100
        t5_ret = df_filtered[df_filtered['tier'] == 5]['total_return'].mean() * 100
        spread = t1_ret - t5_ret
        print(f"\nSPREAD (Tier 1 - Tier 5): {spread:+.1f}%")

        print("\nBY BUSINESS MODEL (filtered):")
        print(f"{'Model':<12} {'Count':>6} {'Avg Total':>12} {'Median':>10} {'Win Rate':>10}")
        print("-"*55)

        for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear', 'Unknown']:
            model_df = df_filtered[df_filtered['biz_model'] == model]
            if len(model_df) > 0:
                avg_ret = model_df['total_return'].mean() * 100
                med_ret = model_df['total_return'].median() * 100
                win_rate = (model_df['total_return'] > 0).mean() * 100
                print(f"{model:<12} {len(model_df):>6} {avg_ret:>+11.1f}% {med_ret:>+9.1f}% {win_rate:>9.1f}%")

        # Cash Cow vs Red Flag spread
        cc_df = df_filtered[df_filtered['biz_model'] == 'Cash Cow']
        rf_df = df_filtered[df_filtered['biz_model'] == 'Red Flag']
        if len(cc_df) > 0 and len(rf_df) > 0:
            cc_ret = cc_df['total_return'].mean() * 100
            rf_ret = rf_df['total_return'].mean() * 100
            print(f"\nSPREAD (Cash Cow - Red Flag): {cc_ret - rf_ret:+.1f}%")

        # Best combinations
        print("\n" + "="*80)
        print("BEST COMBINATIONS (filtered)")
        print("="*80)

        # Tier 1 + Cash Cow
        combo1 = df_filtered[(df_filtered['tier'] == 1) & (df_filtered['biz_model'] == 'Cash Cow')]
        if len(combo1) > 0:
            print(f"\nTier 1 + Cash Cow ({len(combo1)} stocks):")
            print(f"  Avg Total Return: {combo1['total_return'].mean()*100:+.1f}%")
            print(f"  Win Rate: {(combo1['total_return'] > 0).mean()*100:.1f}%")

        # Tier 1 + Compounder
        combo2 = df_filtered[(df_filtered['tier'] == 1) & (df_filtered['biz_model'] == 'Compounder')]
        if len(combo2) > 0:
            print(f"\nTier 1 + Compounder ({len(combo2)} stocks):")
            print(f"  Avg Total Return: {combo2['total_return'].mean()*100:+.1f}%")
            print(f"  Win Rate: {(combo2['total_return'] > 0).mean()*100:.1f}%")

        # Tier 1-2 + (Cash Cow or Compounder)
        combo3 = df_filtered[(df_filtered['tier'] <= 2) & (df_filtered['biz_model'].isin(['Cash Cow', 'Compounder']))]
        if len(combo3) > 0:
            print(f"\nTier 1-2 + (Cash Cow or Compounder) ({len(combo3)} stocks):")
            print(f"  Avg Total Return: {combo3['total_return'].mean()*100:+.1f}%")
            print(f"  Median Total Return: {combo3['total_return'].median()*100:+.1f}%")
            print(f"  Win Rate: {(combo3['total_return'] > 0).mean()*100:.1f}%")

        # Avoid: Tier 4-5 or Red Flag
        avoid = df_filtered[(df_filtered['tier'] >= 4) | (df_filtered['biz_model'] == 'Red Flag')]
        if len(avoid) > 0:
            print(f"\nAVOID: Tier 4-5 or Red Flag ({len(avoid)} stocks):")
            print(f"  Avg Total Return: {avoid['total_return'].mean()*100:+.1f}%")
            print(f"  Win Rate: {(avoid['total_return'] > 0).mean()*100:.1f}%")

        # Top performers
        print("\n" + "="*80)
        print("TOP 20 PERFORMERS (Tier 1-2)")
        print("="*80)

        top_df = df_filtered[df_filtered['tier'] <= 2].sort_values('total_return', ascending=False).head(20)
        print(f"\n{'Ticker':<10} {'Company':<22} {'Tier':>4} {'Model':<12} {'Total':>8} {'Div':>6} {'Pick Date'}")
        print("-"*85)

        for _, row in top_df.iterrows():
            total = f"{row['total_return']*100:+.0f}%"
            div = f"{row['dividend_return']*100:.1f}%" if row['dividend_return'] else "-"
            print(f"{row['ticker']:<10} {row['company'][:21]:<22} {row['tier']:>4} {row['biz_model']:<12} {total:>8} {div:>6} {row['pick_date']}")

        # Worst performers from Tier 1-2 (what went wrong?)
        print("\n" + "="*80)
        print("WORST 10 PERFORMERS (Tier 1-2) - What went wrong?")
        print("="*80)

        worst_df = df_filtered[df_filtered['tier'] <= 2].sort_values('total_return', ascending=True).head(10)
        print(f"\n{'Ticker':<10} {'Company':<22} {'Tier':>4} {'Model':<12} {'Total':>8} {'Pick Date'}")
        print("-"*75)

        for _, row in worst_df.iterrows():
            total = f"{row['total_return']*100:+.0f}%"
            print(f"{row['ticker']:<10} {row['company'][:21]:<22} {row['tier']:>4} {row['biz_model']:<12} {total:>8} {row['pick_date']}")

        # Export ALL data to Excel with multiple analysis sheets
        output_file = 'quality_backtest_full.xlsx'
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: ALL raw data (for your charts)
            df.sort_values(['pick_date', 'total_return'], ascending=[True, False]).to_excel(
                writer, sheet_name='All Companies', index=False)

            # Sheet 2: Filtered only (original criteria)
            df_filtered.sort_values(['pick_date', 'total_return'], ascending=[True, False]).to_excel(
                writer, sheet_name='Filtered Only', index=False)

            # Sheet 3: Summary by Tier
            tier_summary = []
            for tier in [1, 2, 3, 4, 5]:
                tier_df = df_filtered[df_filtered['tier'] == tier]
                if len(tier_df) > 0:
                    tier_summary.append({
                        'Tier': tier,
                        'Count': len(tier_df),
                        'Avg Total Return': tier_df['total_return'].mean(),
                        'Median Return': tier_df['total_return'].median(),
                        'Annualized Return': tier_df['annualized_return'].mean(),
                        'Win Rate': (tier_df['total_return'] > 0).mean(),
                        'Std Dev': tier_df['total_return'].std(),
                    })
            pd.DataFrame(tier_summary).to_excel(writer, sheet_name='Summary by Tier', index=False)

            # Sheet 4: Summary by Business Model
            model_summary = []
            for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear', 'Unknown']:
                model_df = df_filtered[df_filtered['biz_model'] == model]
                if len(model_df) > 0:
                    model_summary.append({
                        'Business Model': model,
                        'Count': len(model_df),
                        'Avg Total Return': model_df['total_return'].mean(),
                        'Median Return': model_df['total_return'].median(),
                        'Win Rate': (model_df['total_return'] > 0).mean(),
                        'Std Dev': model_df['total_return'].std(),
                    })
            pd.DataFrame(model_summary).to_excel(writer, sheet_name='Summary by Model', index=False)

            # Sheet 5: Summary by Pick Date
            date_summary = []
            for pick_date in df_filtered['pick_date'].unique():
                date_df = df_filtered[df_filtered['pick_date'] == pick_date]
                date_summary.append({
                    'Pick Date': pick_date,
                    'Count': len(date_df),
                    'Avg Total Return': date_df['total_return'].mean(),
                    'Median Return': date_df['total_return'].median(),
                    'Holding Years': date_df['holding_years'].mean(),
                    'Win Rate': (date_df['total_return'] > 0).mean(),
                })
            pd.DataFrame(date_summary).to_excel(writer, sheet_name='Summary by Date', index=False)

            # Sheet 6-8: By pick date (all companies)
            for test_date in df['pick_date'].unique():
                date_df = df[df['pick_date'] == test_date]
                sheet_name = f"All {test_date}"[:31]
                date_df.sort_values('total_return', ascending=False).to_excel(
                    writer, sheet_name=sheet_name, index=False)

            # Sheet 9: By Tier (for histogram charts)
            for tier in [1, 2, 3, 4, 5]:
                tier_df = df[df['tier'] == tier]
                if len(tier_df) > 0:
                    tier_df.sort_values('total_return', ascending=False).to_excel(
                        writer, sheet_name=f'Tier {tier}', index=False)

            # Sheet 10: By Business Model (for histogram charts)
            for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag']:
                model_df = df[df['biz_model'] == model]
                if len(model_df) > 0:
                    model_df.sort_values('total_return', ascending=False).to_excel(
                        writer, sheet_name=model[:31], index=False)

            # Sheet 11: Profitable only (for profitability analysis)
            profitable_df = df[~df['is_unprofitable']]
            profitable_df.sort_values('total_return', ascending=False).to_excel(
                writer, sheet_name='Profitable Only', index=False)

            # Sheet 12: Unprofitable (losers analysis)
            unprofitable_df = df[df['is_unprofitable']]
            if len(unprofitable_df) > 0:
                unprofitable_df.sort_values('total_return', ascending=False).to_excel(
                    writer, sheet_name='Unprofitable', index=False)

            # Sheet 13: Small caps vs Large caps
            small_cap_df = df[df['is_small_cap'] & ~df['is_micro_cap']]
            if len(small_cap_df) > 0:
                small_cap_df.sort_values('total_return', ascending=False).to_excel(
                    writer, sheet_name='Small Cap 25-100M', index=False)

            large_cap_df = df[~df['is_small_cap']]
            if len(large_cap_df) > 0:
                large_cap_df.sort_values('total_return', ascending=False).to_excel(
                    writer, sheet_name='Large Cap 100M+', index=False)

            # Sheet 14: Best combo (Tier 1-2 + Cash Cow/Compounder)
            if len(combo3) > 0:
                combo3.sort_values('total_return', ascending=False).to_excel(
                    writer, sheet_name='T1-2 CashCow+Compndr', index=False)

            # === COMBINATION ANALYSIS SHEETS ===

            # Sheet: Summary by Size Category
            size_summary = []
            for size in ['Micro', 'Small', 'Mid', 'Large', 'Mega']:
                size_df = df[df['size_category'] == size]
                if len(size_df) > 0:
                    size_summary.append({
                        'Size': size,
                        'Count': len(size_df),
                        'Avg Return': size_df['total_return'].mean(),
                        'Median Return': size_df['total_return'].median(),
                        'Win Rate': (size_df['total_return'] > 0).mean(),
                        'Std Dev': size_df['total_return'].std(),
                    })
            pd.DataFrame(size_summary).to_excel(writer, sheet_name='Summary by Size', index=False)

            # Sheet: Summary by Cash Quality
            cash_summary = []
            for cash in ['Excellent', 'Good', 'Moderate', 'Weak', 'Poor', 'Unknown']:
                cash_df = df[df['cash_quality'] == cash]
                if len(cash_df) > 0:
                    cash_summary.append({
                        'Cash Quality': cash,
                        'Count': len(cash_df),
                        'Avg Return': cash_df['total_return'].mean(),
                        'Median Return': cash_df['total_return'].median(),
                        'Win Rate': (cash_df['total_return'] > 0).mean(),
                        'Std Dev': cash_df['total_return'].std(),
                    })
            pd.DataFrame(cash_summary).to_excel(writer, sheet_name='Summary by Cash', index=False)

            # Sheet: Tier x Business Model combinations
            tier_model_combos = []
            for tier in [1, 2, 3, 4, 5]:
                for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear', 'Unknown']:
                    combo_df = df[(df['tier'] == tier) & (df['biz_model'] == model)]
                    if len(combo_df) >= 3:  # Only include if enough samples
                        tier_model_combos.append({
                            'Tier': tier,
                            'Business Model': model,
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })
            tier_model_df = pd.DataFrame(tier_model_combos)
            if len(tier_model_df) > 0:
                tier_model_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Tier x Model', index=False)

            # Sheet: Tier x Size combinations
            tier_size_combos = []
            for tier in [1, 2, 3, 4, 5]:
                for size in ['Micro', 'Small', 'Mid', 'Large', 'Mega']:
                    combo_df = df[(df['tier'] == tier) & (df['size_category'] == size)]
                    if len(combo_df) >= 3:
                        tier_size_combos.append({
                            'Tier': tier,
                            'Size': size,
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })
            tier_size_df = pd.DataFrame(tier_size_combos)
            if len(tier_size_df) > 0:
                tier_size_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Tier x Size', index=False)

            # Sheet: Business Model x Size combinations
            model_size_combos = []
            for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear']:
                for size in ['Micro', 'Small', 'Mid', 'Large', 'Mega']:
                    combo_df = df[(df['biz_model'] == model) & (df['size_category'] == size)]
                    if len(combo_df) >= 3:
                        model_size_combos.append({
                            'Business Model': model,
                            'Size': size,
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })
            model_size_df = pd.DataFrame(model_size_combos)
            if len(model_size_df) > 0:
                model_size_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Model x Size', index=False)

            # Sheet: Tier x Cash Quality combinations
            tier_cash_combos = []
            for tier in [1, 2, 3, 4, 5]:
                for cash in ['Excellent', 'Good', 'Moderate', 'Weak', 'Poor']:
                    combo_df = df[(df['tier'] == tier) & (df['cash_quality'] == cash)]
                    if len(combo_df) >= 3:
                        tier_cash_combos.append({
                            'Tier': tier,
                            'Cash Quality': cash,
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })
            tier_cash_df = pd.DataFrame(tier_cash_combos)
            if len(tier_cash_df) > 0:
                tier_cash_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Tier x Cash', index=False)

            # Sheet: Business Model x Cash Quality combinations
            model_cash_combos = []
            for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear']:
                for cash in ['Excellent', 'Good', 'Moderate', 'Weak', 'Poor']:
                    combo_df = df[(df['biz_model'] == model) & (df['cash_quality'] == cash)]
                    if len(combo_df) >= 3:
                        model_cash_combos.append({
                            'Business Model': model,
                            'Cash Quality': cash,
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })
            model_cash_df = pd.DataFrame(model_cash_combos)
            if len(model_cash_df) > 0:
                model_cash_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Model x Cash', index=False)

            # Sheet: Triple combination - Tier x Model x Size (top combos only)
            triple_combos = []
            for tier in [1, 2, 3]:  # Only best tiers
                for model in ['Cash Cow', 'Compounder']:  # Only best models
                    for size in ['Small', 'Mid', 'Large']:  # Exclude micro/mega
                        combo_df = df[
                            (df['tier'] == tier) &
                            (df['biz_model'] == model) &
                            (df['size_category'] == size)
                        ]
                        if len(combo_df) >= 2:  # Lower threshold for triple
                            triple_combos.append({
                                'Tier': tier,
                                'Business Model': model,
                                'Size': size,
                                'Count': len(combo_df),
                                'Avg Return': combo_df['total_return'].mean(),
                                'Median Return': combo_df['total_return'].median(),
                                'Win Rate': (combo_df['total_return'] > 0).mean(),
                            })
            triple_df = pd.DataFrame(triple_combos)
            if len(triple_df) > 0:
                triple_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Tier x Model x Size', index=False)

            # Sheet: THE GOLDEN COMBO - Tier x Cash Quality x Business Model (Cash Cow/Compounder)
            golden_combos = []
            for tier in [1, 2, 3, 4, 5]:
                for cash in ['Excellent', 'Good', 'Moderate', 'Weak', 'Poor']:
                    for model in ['Cash Cow', 'Compounder']:
                        combo_df = df[
                            (df['tier'] == tier) &
                            (df['cash_quality'] == cash) &
                            (df['biz_model'] == model)
                        ]
                        if len(combo_df) >= 2:
                            golden_combos.append({
                                'Tier': tier,
                                'Cash Quality': cash,
                                'Business Model': model,
                                'Count': len(combo_df),
                                'Avg Return': combo_df['total_return'].mean(),
                                'Median Return': combo_df['total_return'].median(),
                                'Win Rate': (combo_df['total_return'] > 0).mean(),
                                'Std Dev': combo_df['total_return'].std(),
                            })
            golden_df = pd.DataFrame(golden_combos)
            if len(golden_df) > 0:
                golden_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='Tier x Cash x Model', index=False)

            # Sheet: All combinations ranked (master list)
            all_combos = []

            # Add all Tier x Model combos
            for tier in [1, 2, 3, 4, 5]:
                for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear']:
                    combo_df = df[(df['tier'] == tier) & (df['biz_model'] == model)]
                    if len(combo_df) >= 3:
                        all_combos.append({
                            'Combination': f'Tier {tier} + {model}',
                            'Type': 'Tier x Model',
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })

            # Add all Tier x Size combos
            for tier in [1, 2, 3, 4, 5]:
                for size in ['Micro', 'Small', 'Mid', 'Large', 'Mega']:
                    combo_df = df[(df['tier'] == tier) & (df['size_category'] == size)]
                    if len(combo_df) >= 3:
                        all_combos.append({
                            'Combination': f'Tier {tier} + {size}',
                            'Type': 'Tier x Size',
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })

            # Add all Model x Size combos
            for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag']:
                for size in ['Micro', 'Small', 'Mid', 'Large', 'Mega']:
                    combo_df = df[(df['biz_model'] == model) & (df['size_category'] == size)]
                    if len(combo_df) >= 3:
                        all_combos.append({
                            'Combination': f'{model} + {size}',
                            'Type': 'Model x Size',
                            'Count': len(combo_df),
                            'Avg Return': combo_df['total_return'].mean(),
                            'Median Return': combo_df['total_return'].median(),
                            'Win Rate': (combo_df['total_return'] > 0).mean(),
                            'Std Dev': combo_df['total_return'].std(),
                        })

            all_combos_df = pd.DataFrame(all_combos)
            if len(all_combos_df) > 0:
                all_combos_df.sort_values('Avg Return', ascending=False).to_excel(
                    writer, sheet_name='All Combos Ranked', index=False)

        print(f"\n\nExported to {output_file}")
        print(f"  Total companies: {len(df)}")
        print(f"  Filtered (original): {len(df_filtered)}")
        print(f"  Sheets: All Companies, Filtered Only, Summaries, By Tier, By Model, and more")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
