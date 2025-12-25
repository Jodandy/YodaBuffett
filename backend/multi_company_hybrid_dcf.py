#!/usr/bin/env python3
"""
Multi-Company Hybrid R12 DCF Analysis

Run comprehensive historical DCF on multiple companies using hybrid R12 approach
to establish baseline valuation patterns across different companies and sectors.
"""

import asyncio
from hybrid_r12_dcf import HybridR12DCF
from dcf_monte_carlo_fixed import DCFParameters
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def run_multi_company_hybrid_dcf():
    """Run hybrid R12 DCF analysis for ALL companies with sufficient data"""
    
    # Get ALL companies with sufficient historical data for DCF analysis
    print("📊 Discovering companies with sufficient historical data...")
    companies_query = """
    SELECT 
        fs.symbol,
        COUNT(*) as report_count,
        COUNT(CASE WHEN h.market_cap > 0 AND h.close_price > 0 THEN 1 END) as price_data_count
    FROM financial_statements fs
    LEFT JOIN historical_fundamentals_daily h ON fs.symbol = h.symbol 
        AND h.date BETWEEN fs.period_date - INTERVAL '30 days' AND fs.period_date + INTERVAL '30 days'
    WHERE fs.total_revenue > 0 
    AND fs.operating_income IS NOT NULL
    AND fs.period_date >= '2020-01-01'
    GROUP BY fs.symbol
    HAVING COUNT(*) >= 4 
    AND COUNT(CASE WHEN h.market_cap > 0 AND h.close_price > 0 THEN 1 END) >= 100
    ORDER BY report_count DESC, price_data_count DESC
    """
    
    # Initialize connection to get companies list
    import asyncpg
    temp_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    company_records = await temp_conn.fetch(companies_query)
    companies = [record['symbol'] for record in company_records]
    await temp_conn.close()
    
    print(f"✅ Found {len(companies)} companies with sufficient data for DCF analysis")
    
    # Initialize hybrid R12 DCF engine
    params = DCFParameters(
        num_simulations=1000,  # Reduced for speed across many companies
        projection_years=10,
        terminal_growth_rate=0.025,
        risk_free_rate=0.03,
        market_premium=0.07
    )
    
    engine = HybridR12DCF(params)
    await engine.setup()
    
    print(f"🎯 COMPREHENSIVE MULTI-COMPANY HYBRID R12 DCF ANALYSIS")
    print(f"📊 Processing {len(companies)} companies with hybrid R12 methodology")
    print(f"   Annual data for pre-2024, Quarterly R12 for 2024+")
    print(f"   Sampling every 30 days for manageable processing time")
    print("=" * 80)
    
    all_company_results = {}
    total_valuations = 0
    
    for i, symbol in enumerate(companies):
        print(f"\n📈 {i+1}/{len(companies)} - Processing {symbol}")
        
        try:
            # Get historical dates for this company
            historical_dates_query = """
            SELECT date, close_price
            FROM historical_fundamentals_daily
            WHERE symbol = $1
            AND date >= '2022-01-01'
            AND close_price > 0
            AND market_cap > 0
            ORDER BY date DESC
            """
            
            historical_dates = await engine.db_conn.fetch(historical_dates_query, symbol)
            
            if not historical_dates:
                print(f"❌ {symbol}: No historical data available")
                continue
            
            earliest = historical_dates[-1]['date']
            latest = historical_dates[0]['date']
            span_years = (latest - earliest).days / 365.25
            
            print(f"   Data range: {earliest} to {latest} ({span_years:.1f} years)")
            
            # Sample every 30 days for good coverage but manageable processing across 321 companies
            sample_dates = historical_dates[::30]
            
            successful_valuations = 0
            pre_2024_count = 0
            post_2024_count = 0
            company_results = []
            
            for j, date_record in enumerate(sample_dates):
                valuation_date = datetime.combine(date_record['date'], datetime.min.time())
                market_price = float(date_record['close_price'])
                
                try:
                    result = await engine.value_company(symbol, valuation_date, market_price)
                    
                    if result:
                        # Save to database
                        dcf_id = await engine.save_valuation(result)
                        successful_valuations += 1
                        total_valuations += 1
                        
                        # Track eras
                        if valuation_date.year < 2024:
                            pre_2024_count += 1
                        else:
                            post_2024_count += 1
                        
                        # Store for analysis
                        company_results.append({
                            'date': valuation_date,
                            'market_price': market_price,
                            'fair_value': result['fair_value_median'],
                            'implied_return': result['implied_return'],
                            'signal': result['valuation_signal'],
                            'rate_sensitivity': result['rate_sensitivity']['rate_sensitivity_score'],
                            'margin': result['actual_margin'],
                        })
                        
                        # Progress update every 10 valuations for large-scale processing
                        if successful_valuations % 10 == 0:
                            return_pct = result['implied_return'] * 100
                            rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                            print(f"   ✓ {successful_valuations:>2} | {date_record['date']} | "
                                  f"${result['fair_value_median']:.0f} vs ${market_price:.0f} ({return_pct:+4.0f}%) | "
                                  f"Rate: {rate_sens:.1f}/10")
                            
                except Exception as e:
                    # Skip errors silently
                    pass
            
            if successful_valuations > 0:
                # Calculate company baseline patterns
                fair_values = [r['fair_value'] for r in company_results]
                market_prices = [r['market_price'] for r in company_results]
                returns = [r['implied_return'] for r in company_results]
                rate_sensitivities = [r['rate_sensitivity'] for r in company_results]
                
                avg_fair_value = sum(fair_values) / len(fair_values)
                avg_market_price = sum(market_prices) / len(market_prices)
                avg_return = sum(returns) / len(returns)
                avg_rate_sens = sum(rate_sensitivities) / len(rate_sensitivities)
                
                # Valuation signal breakdown
                signals = {}
                for result in company_results:
                    signal = result['signal']
                    signals[signal] = signals.get(signal, 0) + 1
                
                # Era comparison
                pre_2024_results = [r for r in company_results if r['date'].year < 2024]
                post_2024_results = [r for r in company_results if r['date'].year >= 2024]
                
                pre_avg_return = (sum(r['implied_return'] for r in pre_2024_results) / len(pre_2024_results) * 100) if pre_2024_results else 0
                post_avg_return = (sum(r['implied_return'] for r in post_2024_results) / len(post_2024_results) * 100) if post_2024_results else 0
                
                print(f"\n✅ {symbol}: {successful_valuations} DCF valuations created")
                print(f"   Baseline: Avg fair value ${avg_fair_value:.0f} vs market ${avg_market_price:.0f} ({avg_return*100:+.0f}%)")
                print(f"   Rate sensitivity: {avg_rate_sens:.1f}/10 average")
                print(f"   Signals: {dict(signals)}")
                print(f"   Era comparison: Pre-2024 {pre_avg_return:+.0f}% vs 2024+ {post_avg_return:+.0f}%")
                
                # Store company summary
                all_company_results[symbol] = {
                    'total_valuations': successful_valuations,
                    'pre_2024_count': pre_2024_count,
                    'post_2024_count': post_2024_count,
                    'avg_fair_value': avg_fair_value,
                    'avg_market_price': avg_market_price,
                    'avg_return_pct': avg_return * 100,
                    'avg_rate_sensitivity': avg_rate_sens,
                    'signals': signals,
                    'pre_2024_return': pre_avg_return,
                    'post_2024_return': post_avg_return,
                    'data_span_years': span_years
                }
                
            else:
                print(f"❌ {symbol}: No successful valuations created")
                
        except Exception as e:
            print(f"❌ {symbol}: Fatal error - {str(e)[:60]}...")
            continue
    
    await engine.cleanup()
    
    # Summary analysis across all companies
    print("\n" + "=" * 80)
    print(f"📈 MULTI-COMPANY DCF ANALYSIS COMPLETE")
    print("=" * 80)
    
    if all_company_results:
        print(f"\n✅ Successfully analyzed {len(all_company_results)} companies")
        print(f"📊 Total DCF valuations created: {total_valuations:,}")
        
        print(f"\n📋 COMPANY BASELINE PATTERNS:")
        for symbol, data in all_company_results.items():
            print(f"\n   {symbol}:")
            print(f"     Valuations: {data['total_valuations']} ({data['pre_2024_count']} pre-2024, {data['post_2024_count']} 2024+)")
            print(f"     Baseline valuation: Fair ${data['avg_fair_value']:.0f} vs Market ${data['avg_market_price']:.0f} ({data['avg_return_pct']:+.0f}%)")
            print(f"     Rate sensitivity: {data['avg_rate_sensitivity']:.1f}/10")
            print(f"     Era evolution: {data['pre_2024_return']:+.0f}% → {data['post_2024_return']:+.0f}%")
            
            # Most common signal
            most_common_signal = max(data['signals'].items(), key=lambda x: x[1])
            signal_pct = (most_common_signal[1] / data['total_valuations']) * 100
            print(f"     Primary signal: {most_common_signal[0]} ({signal_pct:.0f}% of periods)")
        
        # Cross-company insights
        print(f"\n🎯 CROSS-COMPANY INSIGHTS:")
        
        # Average metrics across all companies
        all_returns = [data['avg_return_pct'] for data in all_company_results.values()]
        all_rate_sens = [data['avg_rate_sensitivity'] for data in all_company_results.values()]
        
        avg_return_all = sum(all_returns) / len(all_returns)
        avg_rate_sens_all = sum(all_rate_sens) / len(all_rate_sens)
        
        print(f"   Average implied return across companies: {avg_return_all:+.0f}%")
        print(f"   Average rate sensitivity: {avg_rate_sens_all:.1f}/10")
        
        # Companies ranked by attractiveness (highest implied return)
        ranked_companies = sorted(all_company_results.items(), key=lambda x: x[1]['avg_return_pct'], reverse=True)
        
        print(f"\n🏆 COMPANIES RANKED BY BASELINE ATTRACTIVENESS:")
        for i, (symbol, data) in enumerate(ranked_companies):
            print(f"   {i+1}. {symbol}: {data['avg_return_pct']:+.0f}% avg return (Rate sens: {data['avg_rate_sensitivity']:.1f}/10)")
        
        # Rate sensitivity ranking
        rate_sens_ranked = sorted(all_company_results.items(), key=lambda x: x[1]['avg_rate_sensitivity'], reverse=True)
        
        print(f"\n⚡ COMPANIES BY RATE SENSITIVITY:")
        for i, (symbol, data) in enumerate(rate_sens_ranked):
            print(f"   {i+1}. {symbol}: {data['avg_rate_sensitivity']:.1f}/10 (Avg return: {data['avg_return_pct']:+.0f}%)")
        
        print(f"\n💾 All {total_valuations:,} valuations saved to dcf_valuations table")
        print(f"🔍 Baseline patterns established for {len(all_company_results)} companies using hybrid R12")
        
    else:
        print(f"❌ No companies successfully analyzed")

if __name__ == "__main__":
    asyncio.run(run_multi_company_hybrid_dcf())