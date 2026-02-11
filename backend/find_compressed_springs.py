#!/usr/bin/env python3
"""
Find Compressed Springs

Quality turnarounds, not speculative lottery tickets.
Companies that:
1. Have a real revenue base (not pre-revenue)
2. Were profitable historically (proven they can make money)
3. Currently beaten down (low P/S, cheap vs own history)
4. Margin compression (current margins below historical)
5. Multiple expansion potential

Think: Nelly, Cheffelo - decent businesses, overly beaten down.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from typing import Dict, List, Optional

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_company_profile(conn, company_id: str, ticker: str,
                               score_date: date) -> Optional[Dict]:
    """Build a comprehensive profile for a company."""

    # Get current dimension scores
    from uuid import UUID
    company_uuid = UUID(company_id) if isinstance(company_id, str) else company_id

    scores = await conn.fetch("""
        SELECT dimension_code, score
        FROM daily_dimension_scores
        WHERE company_id = $1 AND score_date = $2
    """, company_uuid, score_date)

    if not scores:
        return None

    current_scores = {r['dimension_code']: float(r['score']) if r['score'] else 0 for r in scores}

    # Get financial data - need revenue and margins
    # Try both ticker formats
    ticker_space = ticker.replace('-', ' ')

    financials = await conn.fetchrow("""
        SELECT
            total_revenue,
            gross_profit,
            operating_income,
            net_income,
            period_date
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
        ORDER BY period_date DESC
        LIMIT 1
    """, ticker, ticker_space, score_date)

    if not financials or not financials['total_revenue']:
        return None

    revenue = float(financials['total_revenue'])

    # Filter out tiny companies (< 100M revenue)
    if revenue < 100_000_000:
        return None

    # Calculate current margins
    gross_margin = None
    operating_margin = None
    net_margin = None

    if financials['gross_profit']:
        gross_margin = float(financials['gross_profit']) / revenue * 100
    if financials['operating_income']:
        operating_margin = float(financials['operating_income']) / revenue * 100
    if financials['net_income']:
        net_margin = float(financials['net_income']) / revenue * 100

    # Get historical margins (2-3 years ago)
    historical_date = score_date - timedelta(days=2*365)

    hist_financials = await conn.fetchrow("""
        SELECT
            total_revenue,
            gross_profit,
            operating_income,
            net_income
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
          AND period_date >= $4
        ORDER BY period_date DESC
        LIMIT 1
    """, ticker, ticker_space, historical_date, historical_date - timedelta(days=365))

    hist_operating_margin = None
    hist_net_margin = None
    was_profitable = False

    if hist_financials and hist_financials['total_revenue']:
        hist_revenue = float(hist_financials['total_revenue'])
        if hist_financials['operating_income']:
            hist_operating_margin = float(hist_financials['operating_income']) / hist_revenue * 100
        if hist_financials['net_income']:
            hist_net_margin = float(hist_financials['net_income']) / hist_revenue * 100
            was_profitable = hist_net_margin > 0

    # Get current price and market cap for P/S
    price_data = await conn.fetchrow("""
        SELECT close_price, date
        FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
    """, ticker, score_date)

    # Get shares outstanding from balance sheet
    balance = await conn.fetchrow("""
        SELECT shares_outstanding
        FROM balance_sheet_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date <= $3
        ORDER BY period_date DESC
        LIMIT 1
    """, ticker, ticker_space, score_date)

    ps_ratio = None
    market_cap = None
    if price_data and balance and balance['shares_outstanding']:
        shares = float(balance['shares_outstanding'])
        price = float(price_data['close_price'])
        market_cap = price * shares
        if revenue > 0:
            ps_ratio = market_cap / revenue

    # Calculate margin compression
    margin_compression = None
    if operating_margin is not None and hist_operating_margin is not None:
        margin_compression = hist_operating_margin - operating_margin

    return {
        'company_id': company_id,
        'ticker': ticker,
        'score_date': score_date,
        'revenue': revenue,
        'market_cap': market_cap,
        'ps_ratio': ps_ratio,
        'operating_margin': operating_margin,
        'net_margin': net_margin,
        'hist_operating_margin': hist_operating_margin,
        'hist_net_margin': hist_net_margin,
        'margin_compression': margin_compression,
        'was_profitable': was_profitable,
        **{f"score_{k}": v for k, v in current_scores.items()}
    }


async def find_compressed_springs(conn, score_date: date = None,
                                   min_revenue: float = 100_000_000,
                                   max_ps: float = 1.5,
                                   min_margin_compression: float = 3.0):
    """Find quality turnarounds - compressed springs."""

    if score_date is None:
        # Get latest score date with good coverage (>1000 companies)
        score_date = await conn.fetchval("""
            SELECT score_date FROM daily_dimension_scores
            GROUP BY score_date
            HAVING COUNT(DISTINCT company_id) > 1000
            ORDER BY score_date DESC
            LIMIT 1
        """)

    print(f"Looking for compressed springs as of {score_date}")
    print(f"Criteria:")
    print(f"  Min revenue: ${min_revenue/1e6:.0f}M")
    print(f"  Max P/S: {max_ps}")
    print(f"  Min margin compression: {min_margin_compression}%")
    print()

    # Get all companies with scores
    companies = await conn.fetch("""
        SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
        FROM daily_dimension_scores dds
        JOIN company_master cm ON cm.id = dds.company_id
        WHERE dds.score_date = $1
    """, score_date)

    print(f"Analyzing {len(companies)} companies...")

    profiles = []
    for i, comp in enumerate(companies):
        if i % 100 == 0:
            print(f"  {i}/{len(companies)}")

        profile = await get_company_profile(
            conn,
            comp['company_id'],
            comp['primary_ticker'],
            score_date
        )

        if profile:
            profile['company_name'] = comp['company_name']
            profiles.append(profile)

    df = pd.DataFrame(profiles)
    print(f"\nCompanies with complete data: {len(df)}")

    if df.empty:
        print("No data found!")
        return

    # Apply filters for compressed springs
    springs = df[
        (df['ps_ratio'].notna()) &
        (df['ps_ratio'] <= max_ps) &
        (df['ps_ratio'] > 0) &
        (df['was_profitable'] == True) &  # Was profitable historically
        (df['margin_compression'].notna()) &
        (df['margin_compression'] >= min_margin_compression)  # Margins compressed
    ].copy()

    print(f"\nCompressed springs found: {len(springs)}")

    if springs.empty:
        # Show why we're filtering out
        print("\nDiagnostics:")
        print(f"  With P/S data: {df['ps_ratio'].notna().sum()}")
        print(f"  P/S <= {max_ps}: {(df['ps_ratio'] <= max_ps).sum()}")
        print(f"  Was profitable: {(df['was_profitable'] == True).sum()}")
        print(f"  Has margin compression data: {df['margin_compression'].notna().sum()}")
        if df['margin_compression'].notna().sum() > 0:
            print(f"  Margin compression >= {min_margin_compression}: {(df['margin_compression'] >= min_margin_compression).sum()}")
        return df

    # Sort by margin compression (most compressed = most potential)
    springs = springs.sort_values('margin_compression', ascending=False)

    print("\n" + "="*100)
    print("COMPRESSED SPRINGS - Quality Turnarounds")
    print("="*100)

    print(f"\n{'Ticker':<12} {'Company':<25} {'Revenue':>12} {'P/S':>6} {'Op Margin':>10} {'Hist Margin':>12} {'Compression':>12}")
    print("-" * 100)

    for _, row in springs.head(30).iterrows():
        print(f"{row['ticker']:<12} {row['company_name'][:24]:<25} "
              f"${row['revenue']/1e9:.2f}B {row['ps_ratio']:>6.2f} "
              f"{row['operating_margin']:>+9.1f}% {row['hist_operating_margin']:>+11.1f}% "
              f"{row['margin_compression']:>+11.1f}%")

    # Show dimension scores for top candidates
    print("\n" + "="*100)
    print("TOP CANDIDATES - Dimension Scores")
    print("="*100)

    score_cols = [c for c in springs.columns if c.startswith('score_')]

    for _, row in springs.head(10).iterrows():
        print(f"\n{row['ticker']} - {row['company_name']}")
        print(f"  Revenue: ${row['revenue']/1e9:.2f}B | P/S: {row['ps_ratio']:.2f} | Margin compression: {row['margin_compression']:+.1f}%")
        print(f"  Current op margin: {row['operating_margin']:.1f}% vs historical: {row['hist_operating_margin']:.1f}%")

        # Show key scores
        key_dims = ['value', 'valuation_percentile', 'profitability', 'quality', 'momentum', 'growth']
        scores_str = []
        for dim in key_dims:
            col = f'score_{dim}'
            if col in row and pd.notna(row[col]):
                scores_str.append(f"{dim}:{row[col]:.0f}")
        print(f"  Scores: {', '.join(scores_str)}")

    # Look for the sweet spot: cheap + quality wasn't terrible + margins compressed
    print("\n" + "="*100)
    print("SWEET SPOT: Cheap + Decent Quality + Compressed Margins")
    print("="*100)

    sweet_spot = springs[
        (springs.get('score_valuation_percentile', 0) >= 60) &  # Cheap vs own history
        (springs.get('score_quality', 0) >= 35)  # Not total garbage quality
    ].copy()

    if not sweet_spot.empty:
        print(f"\nFound {len(sweet_spot)} sweet spot candidates:")
        for _, row in sweet_spot.iterrows():
            print(f"\n{row['ticker']} - {row['company_name']}")
            print(f"  P/S: {row['ps_ratio']:.2f} | Margin compression: {row['margin_compression']:+.1f}%")
            print(f"  Valuation %ile: {row.get('score_valuation_percentile', 'N/A'):.0f} | Quality: {row.get('score_quality', 'N/A'):.0f}")
    else:
        print("\nNo sweet spot candidates found with current criteria.")
        print("Try relaxing filters...")

    # Export
    output_file = 'compressed_springs.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        springs.to_excel(writer, sheet_name='Compressed Springs', index=False)
        df.to_excel(writer, sheet_name='All Companies', index=False)

    print(f"\n\nResults saved to {output_file}")

    return springs


async def backtest_compressed_springs(conn, holding_days: int = 180):
    """Backtest the compressed springs strategy historically."""

    print("="*80)
    print("BACKTEST: Compressed Springs Strategy")
    print("="*80)

    # Get historical score dates
    cutoff = date.today() - timedelta(days=holding_days + 30)
    dates = await conn.fetch("""
        SELECT DISTINCT score_date FROM daily_dimension_scores
        WHERE score_date <= $1 AND score_date >= '2022-01-01'
        ORDER BY score_date
    """, cutoff)
    score_dates = [r['score_date'] for r in dates]

    print(f"Testing {len(score_dates)} dates with {holding_days}-day forward returns\n")

    all_trades = []

    for i, score_date in enumerate(score_dates):
        if i % 10 == 0:
            print(f"Processing {i+1}/{len(score_dates)}: {score_date}")

        # Get all companies with profiles
        companies = await conn.fetch("""
            SELECT DISTINCT dds.company_id::text, cm.primary_ticker, cm.company_name
            FROM daily_dimension_scores dds
            JOIN company_master cm ON cm.id = dds.company_id
            WHERE dds.score_date = $1
        """, score_date)

        for comp in companies:
            profile = await get_company_profile(
                conn, comp['company_id'], comp['primary_ticker'], score_date
            )

            if not profile:
                continue

            # Check if it's a compressed spring
            ps = profile.get('ps_ratio')
            was_prof = profile.get('was_profitable')
            compression = profile.get('margin_compression')
            val_pct = profile.get('score_valuation_percentile')
            quality = profile.get('score_quality')

            # TIGHTER FILTER: Sweet spot
            if (ps and ps <= 1.5 and ps > 0 and
                was_prof and
                compression and compression >= 3.0 and
                val_pct and val_pct >= 60 and  # Cheap vs own history
                quality and quality >= 35):  # Not total garbage

                # Get forward return
                end_date = score_date + timedelta(days=holding_days + 10)
                prices = await conn.fetch("""
                    SELECT date, close_price FROM daily_price_data
                    WHERE symbol = $1 AND date >= $2 AND date <= $3
                    ORDER BY date
                """, comp['primary_ticker'], score_date, end_date)

                if len(prices) >= 2:
                    entry = float(prices[0]['close_price'])
                    target_date = score_date + timedelta(days=holding_days)

                    exit_price = None
                    for p in prices:
                        if p['date'] >= target_date:
                            exit_price = float(p['close_price'])
                            break
                    if exit_price is None:
                        exit_price = float(prices[-1]['close_price'])

                    if entry > 0:
                        ret = (exit_price - entry) / entry * 100
                        if -90 < ret < 500:
                            all_trades.append({
                                'ticker': comp['primary_ticker'],
                                'company': comp['company_name'],
                                'score_date': score_date,
                                'ps_ratio': ps,
                                'margin_compression': compression,
                                'forward_return': ret,
                                'valuation_pct': profile.get('score_valuation_percentile'),
                                'quality': profile.get('score_quality'),
                            })

    if not all_trades:
        print("No trades found!")
        return

    trades_df = pd.DataFrame(all_trades)

    print(f"\n{'='*60}")
    print("RESULTS")
    print(f"{'='*60}")
    print(f"Total trades: {len(trades_df)}")
    print(f"Unique companies: {trades_df['ticker'].nunique()}")
    print(f"Avg return: {trades_df['forward_return'].mean():+.1f}%")
    print(f"Median return: {trades_df['forward_return'].median():+.1f}%")
    print(f"Win rate: {(trades_df['forward_return'] > 0).mean()*100:.1f}%")
    print(f"Best: {trades_df['forward_return'].max():+.1f}%")
    print(f"Worst: {trades_df['forward_return'].min():+.1f}%")

    # Top performers
    print(f"\nTop 10 trades:")
    for _, row in trades_df.nlargest(10, 'forward_return').iterrows():
        print(f"  {row['ticker']:<10} {row['score_date']} P/S:{row['ps_ratio']:.2f} → {row['forward_return']:+.1f}%")

    # Worst performers
    print(f"\nWorst 10 trades:")
    for _, row in trades_df.nsmallest(10, 'forward_return').iterrows():
        print(f"  {row['ticker']:<10} {row['score_date']} P/S:{row['ps_ratio']:.2f} → {row['forward_return']:+.1f}%")

    return trades_df


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--backtest', action='store_true', help='Run historical backtest')
    parser.add_argument('--holding-days', type=int, default=180)
    parser.add_argument('--min-revenue', type=float, default=100, help='Min revenue in millions')
    parser.add_argument('--max-ps', type=float, default=1.5, help='Max P/S ratio')
    parser.add_argument('--min-compression', type=float, default=3.0, help='Min margin compression %')
    args = parser.parse_args()

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        if args.backtest:
            await backtest_compressed_springs(conn, args.holding_days)
        else:
            await find_compressed_springs(
                conn,
                min_revenue=args.min_revenue * 1_000_000,
                max_ps=args.max_ps,
                min_margin_compression=args.min_compression
            )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
