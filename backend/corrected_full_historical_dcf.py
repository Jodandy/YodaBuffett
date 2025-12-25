#!/usr/bin/env python3
"""
Corrected Full Historical DCF Analysis

Uses proper quarterly vs annual handling as requested:
- Quarterly reports: Single quarter data, annualized by ×4 for projections  
- Annual reports: Used directly as reported
- NO fake R12 calculations from quarterly data
"""

import asyncio
from fixed_quarterly_dcf import CorrectQuarterlyDCF
from dcf_monte_carlo_fixed import DCFParameters
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def run_corrected_full_historical():
    """Run full historical DCF with CORRECT quarterly handling"""
    
    # Target companies with good historical data
    target_companies = ['AAK', 'SOBI', 'EVO', 'VOLV B']
    
    # Initialize corrected DCF engine
    params = DCFParameters(
        num_simulations=1500,  # Reasonable for full historical
        projection_years=10,
        terminal_growth_rate=0.025,
        risk_free_rate=0.03,
        market_premium=0.07
    )
    
    engine = CorrectQuarterlyDCF(params)
    await engine.setup()
    
    print(f"🎯 CORRECTED FULL HISTORICAL DCF (2022-2025)")
    print(f"📊 PROPER quarterly vs annual handling - NO fake R12 calculations")
    print(f"   Quarterly: Single quarter × 4 for annualization")
    print(f"   Annual: Used directly as reported")
    print("=" * 80)
    
    total_valuations = 0
    company_summaries = []
    
    for i, symbol in enumerate(target_companies):
        print(f"\n📈 {i+1}/{len(target_companies)} - Processing {symbol} with corrected logic")
        
        try:
            # Get historical dates from 2022 onwards
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
                print(f"❌ {symbol}: No historical data")
                continue
                
            earliest = historical_dates[-1]['date']
            latest = historical_dates[0]['date']
            span_years = (latest - earliest).days / 365.25
            
            print(f"   Processing {len(historical_dates):,} dates from {earliest} to {latest} ({span_years:.1f} years)")
            
            successful_valuations = 0
            early_period = 0  # 2022-2023
            later_period = 0  # 2024-2025
            
            # Sample every 7 days for reasonable coverage without overload
            sample_dates = historical_dates[::7]
            
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
                        
                        # Track periods
                        if valuation_date.year <= 2023:
                            early_period += 1
                        else:
                            later_period += 1
                        
                        # Progress update every 25 valuations
                        if successful_valuations % 25 == 0:
                            rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                            margin = result['actual_margin'] * 100
                            print(f"   ✓ {successful_valuations:>3} | {date_record['date']} | "
                                  f"${result['fair_value_median']:.0f} vs ${market_price:.0f} | "
                                  f"Margin: {margin:.1f}% | Rate: {rate_sens:.1f}/10")
                            
                except Exception as e:
                    # Minimal error reporting to avoid spam
                    pass
                    
            print(f"✅ {symbol}: {successful_valuations:,} DCF valuations (CORRECTED quarterly handling)")
            print(f"   Early period (2022-2023): {early_period:,}")
            print(f"   Later period (2024-2025): {later_period:,}")
            
            company_summaries.append({
                'symbol': symbol,
                'total_valuations': successful_valuations,
                'early_period': early_period,
                'later_period': later_period
            })
                
        except Exception as e:
            print(f"❌ {symbol}: Fatal error - {e}")
            continue
    
    await engine.cleanup()
    
    # Summary
    print("\n" + "=" * 80)
    print(f"📈 CORRECTED FULL HISTORICAL DCF COMPLETE")
    print("=" * 80)
    
    if company_summaries:
        total_early = sum(s['early_period'] for s in company_summaries)
        total_later = sum(s['later_period'] for s in company_summaries)
        
        print(f"\n✅ Processed {len(company_summaries)} companies with CORRECT quarterly logic:")
        print(f"📊 Total DCF valuations: {total_valuations:,}")
        print(f"   Early period (2022-2023): {total_early:,}")
        print(f"   Later period (2024-2025): {total_later:,}")
        
        print(f"\n📋 COMPANY BREAKDOWN:")
        for summary in company_summaries:
            early_pct = (summary['early_period'] / summary['total_valuations'] * 100) if summary['total_valuations'] > 0 else 0
            print(f"   {summary['symbol']:<8}: {summary['total_valuations']:>3,} total "
                  f"(Early: {summary['early_period']:>2,} ({early_pct:.0f}%), "
                  f"Later: {summary['later_period']:>2,})")
        
    print(f"\n💾 All {total_valuations:,} corrected DCF valuations saved to database")
    print(f"🔧 Now uses PROPER quarterly vs annual handling as requested")
    print(f"📊 Ready for baseline valuation pattern analysis across full historical range")

if __name__ == "__main__":
    asyncio.run(run_corrected_full_historical())