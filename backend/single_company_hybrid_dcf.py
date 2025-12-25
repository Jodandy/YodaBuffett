#!/usr/bin/env python3
"""
Single Company Hybrid R12 DCF Analysis

Run comprehensive historical DCF on one company using the hybrid R12 approach
to validate the methodology and show baseline valuation patterns.
"""

import asyncio
from hybrid_r12_dcf import HybridR12DCF
from dcf_monte_carlo_fixed import DCFParameters
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def run_single_company_hybrid_dcf():
    """Run comprehensive historical DCF for AAK using hybrid R12"""
    
    symbol = 'AAK'  # Good historical data, interesting business
    
    # Initialize hybrid R12 DCF engine
    params = DCFParameters(
        num_simulations=2000,
        projection_years=10,
        terminal_growth_rate=0.025,
        risk_free_rate=0.03,
        market_premium=0.07
    )
    
    engine = HybridR12DCF(params)
    await engine.setup()
    
    print(f"🎯 COMPREHENSIVE HISTORICAL DCF: {symbol}")
    print(f"📊 Using HYBRID R12 approach (Annual pre-2024, Quarterly R12 for 2024+)")
    print("=" * 80)
    
    # Get all historical dates
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
        print(f"❌ No historical data for {symbol}")
        await engine.cleanup()
        return
    
    earliest = historical_dates[-1]['date']
    latest = historical_dates[0]['date']
    span_years = (latest - earliest).days / 365.25
    
    print(f"📈 Processing {len(historical_dates):,} dates from {earliest} to {latest} ({span_years:.1f} years)")
    
    # Sample every 10 days for detailed but manageable coverage
    sample_dates = historical_dates[::10]
    
    successful_valuations = 0
    pre_2024_count = 0
    post_2024_count = 0
    all_results = []
    
    print(f"\n🚀 Processing {len(sample_dates):,} sample dates (every 10 days)")
    print(f"   Progress updates every 25 successful valuations\n")
    
    for j, date_record in enumerate(sample_dates):
        valuation_date = datetime.combine(date_record['date'], datetime.min.time())
        market_price = float(date_record['close_price'])
        
        try:
            result = await engine.value_company(symbol, valuation_date, market_price)
            
            if result:
                # Save to database
                dcf_id = await engine.save_valuation(result)
                successful_valuations += 1
                
                # Track eras
                if valuation_date.year < 2024:
                    pre_2024_count += 1
                else:
                    post_2024_count += 1
                
                # Store result for analysis
                all_results.append({
                    'date': valuation_date,
                    'market_price': market_price,
                    'fair_value': result['fair_value_median'],
                    'implied_return': result['implied_return'],
                    'signal': result['valuation_signal'],
                    'rate_sensitivity': result['rate_sensitivity']['rate_sensitivity_score'],
                    'margin': result['actual_margin'],
                    'revenue': result['actual_revenue']
                })
                
                # Progress updates
                if successful_valuations % 25 == 0:
                    rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                    return_pct = result['implied_return'] * 100
                    print(f"   ✓ {successful_valuations:>3} | {date_record['date']} | "
                          f"${result['fair_value_median']:.0f} vs ${market_price:.0f} ({return_pct:+5.0f}%) | "
                          f"Rate: {rate_sens:.1f}/10 | {result['valuation_signal']}")
                        
        except Exception as e:
            # Silently skip errors to avoid spam
            pass
    
    await engine.cleanup()
    
    # Analysis and summary
    print(f"\n" + "=" * 80)
    print(f"📈 {symbol} HISTORICAL DCF ANALYSIS COMPLETE")
    print("=" * 80)
    
    if successful_valuations > 0:
        print(f"\n✅ Successfully created {successful_valuations:,} DCF valuations")
        print(f"   Pre-2024 era (Annual R12): {pre_2024_count:,}")
        print(f"   2024+ era (Hybrid R12): {post_2024_count:,}")
        
        # Calculate baseline valuation patterns
        if all_results:
            
            # Sort by date for time series analysis
            all_results.sort(key=lambda x: x['date'])
            
            # Overall statistics
            fair_values = [r['fair_value'] for r in all_results]
            market_prices = [r['market_price'] for r in all_results]
            returns = [r['implied_return'] for r in all_results]
            
            avg_fair_value = sum(fair_values) / len(fair_values)
            avg_market_price = sum(market_prices) / len(market_prices)
            avg_return = sum(returns) / len(returns)
            
            print(f"\n📊 BASELINE VALUATION PATTERNS:")
            print(f"   Average fair value: ${avg_fair_value:.0f}")
            print(f"   Average market price: ${avg_market_price:.0f}")
            print(f"   Average implied return: {avg_return*100:+.1f}%")
            
            # Valuation signal breakdown
            signals = {}
            for result in all_results:
                signal = result['signal']
                if signal not in signals:
                    signals[signal] = []
                signals[signal].append(result)
            
            print(f"\n🎯 VALUATION SIGNALS OVER TIME:")
            for signal, results in signals.items():
                count = len(results)
                pct = (count / len(all_results)) * 100
                avg_return_signal = sum(r['implied_return'] for r in results) / len(results) * 100
                print(f"   {signal}: {count:,} periods ({pct:.0f}%) | Avg return: {avg_return_signal:+.1f}%")
            
            # Era comparison
            pre_2024_results = [r for r in all_results if r['date'].year < 2024]
            post_2024_results = [r for r in all_results if r['date'].year >= 2024]
            
            if pre_2024_results and post_2024_results:
                pre_avg_return = sum(r['implied_return'] for r in pre_2024_results) / len(pre_2024_results) * 100
                post_avg_return = sum(r['implied_return'] for r in post_2024_results) / len(post_2024_results) * 100
                
                print(f"\n🕰️  ERA COMPARISON:")
                print(f"   Pre-2024 (Annual R12): {pre_avg_return:+.1f}% avg implied return")
                print(f"   2024+ (Hybrid R12): {post_avg_return:+.1f}% avg implied return")
            
            # Recent vs historical
            recent_results = [r for r in all_results if r['date'].year >= 2024]
            historical_results = [r for r in all_results if r['date'].year < 2024]
            
            if recent_results and historical_results:
                recent_avg_fv = sum(r['fair_value'] for r in recent_results) / len(recent_results)
                historical_avg_fv = sum(r['fair_value'] for r in historical_results) / len(historical_results)
                
                print(f"\n💡 BASELINE INSIGHTS:")
                print(f"   Historical fair value: ${historical_avg_fv:.0f}")
                print(f"   Recent fair value: ${recent_avg_fv:.0f}")
                print(f"   Fair value evolution: {((recent_avg_fv/historical_avg_fv)-1)*100:+.0f}%")
            
            # Show sample of extreme valuations
            all_results.sort(key=lambda x: x['implied_return'], reverse=True)
            
            print(f"\n🔥 MOST UNDERVALUED PERIODS:")
            for result in all_results[:3]:
                date_str = result['date'].strftime('%Y-%m-%d')
                return_pct = result['implied_return'] * 100
                print(f"   {date_str}: ${result['fair_value']:.0f} vs ${result['market_price']:.0f} ({return_pct:+.0f}%)")
                
            print(f"\n🧊 MOST OVERVALUED PERIODS:")
            for result in all_results[-3:]:
                date_str = result['date'].strftime('%Y-%m-%d')
                return_pct = result['implied_return'] * 100
                print(f"   {date_str}: ${result['fair_value']:.0f} vs ${result['market_price']:.0f} ({return_pct:+.0f}%)")
        
        print(f"\n💾 All {successful_valuations:,} valuations saved to dcf_valuations table")
        print(f"🔍 {symbol} baseline patterns established using PROPER R12 methodology")
        
    else:
        print(f"❌ No successful valuations created")

if __name__ == "__main__":
    asyncio.run(run_single_company_hybrid_dcf())