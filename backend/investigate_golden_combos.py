#!/usr/bin/env python3
"""
Investigate companies in each golden combo bucket.
Look for outliers, bad data, or patterns that explain the results.
"""

import pandas as pd
import numpy as np

# Load the backtest data
df = pd.read_excel('quality_backtest_full.xlsx', sheet_name='All Companies')

print("="*100)
print("INVESTIGATING GOLDEN COMBO BUCKETS")
print("="*100)

# Filter to Cash Cow and Compounder only
df_golden = df[df['biz_model'].isin(['Cash Cow', 'Compounder'])].copy()

print(f"\nTotal companies in golden combos: {len(df_golden)}")
print(f"Unique companies: {df_golden['ticker'].nunique()}")

# Define the buckets to investigate (sorted by avg return from the user's data)
buckets_to_check = [
    (2, 'Good', 'Compounder'),      # 111% - TOP PERFORMER
    (3, 'Good', 'Cash Cow'),        # 78%
    (2, 'Moderate', 'Compounder'),  # 61%
    (3, 'Excellent', 'Cash Cow'),   # 56%
    (2, 'Excellent', 'Cash Cow'),   # 55%
    (2, 'Good', 'Cash Cow'),        # 46%
    (1, 'Excellent', 'Compounder'), # 7.7% - UNDERPERFORMER (best quality!)
    (1, 'Excellent', 'Cash Cow'),   # 19.6%
]

for tier, cash, model in buckets_to_check:
    bucket_df = df_golden[
        (df_golden['tier'] == tier) &
        (df_golden['cash_quality'] == cash) &
        (df_golden['biz_model'] == model)
    ].sort_values('total_return', ascending=False)

    if len(bucket_df) == 0:
        continue

    avg_ret = bucket_df['total_return'].mean() * 100
    med_ret = bucket_df['total_return'].median() * 100
    std_ret = bucket_df['total_return'].std() * 100
    win_rate = (bucket_df['total_return'] > 0).mean() * 100

    print(f"\n{'='*100}")
    print(f"TIER {tier} + {cash.upper()} CASH + {model.upper()}")
    print(f"{'='*100}")
    print(f"Count: {len(bucket_df)} | Avg: {avg_ret:+.1f}% | Median: {med_ret:+.1f}% | Std: {std_ret:.1f}% | Win: {win_rate:.0f}%")
    print()

    # Show all companies in this bucket
    print(f"{'Ticker':<12} {'Company':<25} {'Pick Date':<12} {'Return':>10} {'PE':>8} {'ROE':>8} {'Margin':>8} {'Growth':>8} {'MCap(M)':>10}")
    print("-"*100)

    for _, row in bucket_df.iterrows():
        ret_str = f"{row['total_return']*100:+.0f}%"
        pe_str = f"{row['pe']:.1f}" if pd.notna(row['pe']) else "-"
        roe_str = f"{row['roe']*100:.0f}%" if pd.notna(row['roe']) else "-"
        margin_str = f"{row['net_margin']*100:.0f}%" if pd.notna(row['net_margin']) else "-"
        growth_str = f"{row['revenue_growth']*100:.0f}%" if pd.notna(row['revenue_growth']) else "-"
        mcap_str = f"{row['market_cap']/1e6:,.0f}" if pd.notna(row['market_cap']) else "-"

        company_name = str(row['company'])[:24] if pd.notna(row['company']) else "-"

        print(f"{row['ticker']:<12} {company_name:<25} {str(row['pick_date']):<12} {ret_str:>10} {pe_str:>8} {roe_str:>8} {margin_str:>8} {growth_str:>8} {mcap_str:>10}")

# Summary comparison
print("\n" + "="*100)
print("SUMMARY: KEY DIFFERENCES BETWEEN TIER 1 AND TIER 2")
print("="*100)

# Tier 1 Excellent Compounder vs Tier 2 Good Compounder
t1_ec = df_golden[(df_golden['tier'] == 1) & (df_golden['cash_quality'] == 'Excellent') & (df_golden['biz_model'] == 'Compounder')]
t2_gc = df_golden[(df_golden['tier'] == 2) & (df_golden['cash_quality'] == 'Good') & (df_golden['biz_model'] == 'Compounder')]

print(f"\n{'Metric':<20} {'T1 Excellent Comp (n={})'.format(len(t1_ec)):>25} {'T2 Good Comp (n={})'.format(len(t2_gc)):>25}")
print("-"*70)

for col, label in [
    ('total_return', 'Avg Return'),
    ('pe', 'Avg P/E'),
    ('roe', 'Avg ROE'),
    ('net_margin', 'Avg Net Margin'),
    ('roic', 'Avg ROIC'),
    ('revenue_growth', 'Avg Rev Growth'),
    ('market_cap', 'Avg Market Cap'),
    ('quality_score', 'Avg Quality Score'),
]:
    t1_val = t1_ec[col].mean()
    t2_val = t2_gc[col].mean()

    if col == 'market_cap':
        t1_str = f"${t1_val/1e9:.1f}B" if pd.notna(t1_val) else "-"
        t2_str = f"${t2_val/1e9:.1f}B" if pd.notna(t2_val) else "-"
    elif col in ['total_return', 'roe', 'net_margin', 'roic', 'revenue_growth']:
        t1_str = f"{t1_val*100:.1f}%" if pd.notna(t1_val) else "-"
        t2_str = f"{t2_val*100:.1f}%" if pd.notna(t2_val) else "-"
    else:
        t1_str = f"{t1_val:.1f}" if pd.notna(t1_val) else "-"
        t2_str = f"{t2_val:.1f}" if pd.notna(t2_val) else "-"

    print(f"{label:<20} {t1_str:>25} {t2_str:>25}")

# Check for outliers
print("\n" + "="*100)
print("OUTLIER CHECK: Companies with >200% or <-50% returns")
print("="*100)

outliers = df_golden[(df_golden['total_return'] > 2.0) | (df_golden['total_return'] < -0.5)]
outliers = outliers.sort_values('total_return', ascending=False)

print(f"\nFound {len(outliers)} extreme returns:")
print(f"\n{'Ticker':<12} {'Company':<25} {'Tier':>5} {'Cash':>10} {'Model':<12} {'Return':>10} {'Pick Date'}")
print("-"*90)

for _, row in outliers.iterrows():
    ret_str = f"{row['total_return']*100:+.0f}%"
    company_name = str(row['company'])[:24] if pd.notna(row['company']) else "-"
    print(f"{row['ticker']:<12} {company_name:<25} {row['tier']:>5} {row['cash_quality']:>10} {row['biz_model']:<12} {ret_str:>10} {row['pick_date']}")

# Check for data quality issues - companies appearing multiple times with different characteristics
print("\n" + "="*100)
print("DATA QUALITY CHECK: Companies appearing in different categories over time")
print("="*100)

ticker_changes = df_golden.groupby('ticker').agg({
    'tier': lambda x: list(x.unique()),
    'cash_quality': lambda x: list(x.unique()),
    'total_return': ['mean', 'count']
}).reset_index()

ticker_changes.columns = ['ticker', 'tiers', 'cash_qualities', 'avg_return', 'appearances']
ticker_changes = ticker_changes[ticker_changes['appearances'] > 1]

# Find ones that moved between tiers
tier_movers = ticker_changes[ticker_changes['tiers'].apply(len) > 1]
print(f"\nCompanies that changed tiers across pick dates: {len(tier_movers)}")
if len(tier_movers) > 0:
    for _, row in tier_movers.head(10).iterrows():
        print(f"  {row['ticker']}: Tiers {row['tiers']}, Avg Return: {row['avg_return']*100:.0f}%")

# Market cap distribution
print("\n" + "="*100)
print("MARKET CAP DISTRIBUTION BY BUCKET")
print("="*100)

for tier, cash, model in [(2, 'Good', 'Compounder'), (1, 'Excellent', 'Compounder')]:
    bucket = df_golden[
        (df_golden['tier'] == tier) &
        (df_golden['cash_quality'] == cash) &
        (df_golden['biz_model'] == model)
    ]
    if len(bucket) > 0:
        mcap = bucket['market_cap'] / 1e9
        print(f"\nTier {tier} + {cash} + {model}:")
        print(f"  Min: ${mcap.min():.2f}B | Median: ${mcap.median():.2f}B | Max: ${mcap.max():.2f}B")
        print(f"  <$1B: {(mcap < 1).sum()} | $1-10B: {((mcap >= 1) & (mcap < 10)).sum()} | >$10B: {(mcap >= 10).sum()}")
