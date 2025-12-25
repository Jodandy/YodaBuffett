#!/usr/bin/env python3
"""
Comprehensive Historical DCF Analysis

Run DCF analysis on ALL available historical data for companies with sufficient data quality.
Addresses user request: "To clarify, I wanted to run on all availale historical data for the companies"
"""

import asyncio
import asyncpg
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def run_comprehensive_historical_dcf():
    """Run comprehensive DCF analysis on ALL historical data for good companies"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    # First, identify companies with sufficient historical data quality
    data_quality_query = """
    WITH company_data_quality AS (
        SELECT 
            fs.symbol,
            COUNT(DISTINCT fs.period_date) as financial_statements_count,
            COUNT(DISTINCT hfd.date) as price_data_count,
            MIN(fs.period_date) as first_financial_date,
            MAX(fs.period_date) as last_financial_date,
            MIN(hfd.date) as first_price_date,
            MAX(hfd.date) as last_price_date
        FROM financial_statements fs
        INNER JOIN historical_fundamentals_daily hfd ON fs.symbol = hfd.symbol
        WHERE fs.total_revenue > 0
        AND fs.operating_income IS NOT NULL
        AND hfd.close_price > 0
        AND hfd.market_cap > 0
        GROUP BY fs.symbol
        HAVING COUNT(DISTINCT fs.period_date) >= 8  -- At least 8 financial statements
        AND COUNT(DISTINCT hfd.date) >= 200        -- At least 200 days of price data
    )
    SELECT 
        symbol,
        financial_statements_count,
        price_data_count,
        first_financial_date,
        last_financial_date,
        (last_financial_date - first_financial_date) as data_span_days
    FROM company_data_quality
    WHERE first_financial_date <= CURRENT_DATE - INTERVAL '2 years'  -- At least 2 years history
    ORDER BY financial_statements_count DESC, price_data_count DESC
    LIMIT 10  -- Top 10 companies with best data quality
    """
    
    companies_with_quality_data = await conn.fetch(data_quality_query)
    
    print(f"🎯 Comprehensive Historical DCF Analysis")
    print(f"📊 Found {len(companies_with_quality_data)} companies with high-quality historical data")
    print("=" * 80)
    
    # Show data quality summary
    for company in companies_with_quality_data:
        span_years = company['data_span_days'] / 365.25 if company['data_span_days'] else 0
        print(f"✓ {company['symbol']:<8}: {company['financial_statements_count']:>2} statements, "
              f"{company['price_data_count']:>4} price points, "
              f"{span_years:.1f} years span")
    
    await conn.close()
    
    # Initialize DCF engine
    params = DCFParameters(
        num_simulations=2000,  # Reasonable for bulk processing
        projection_years=10,
        terminal_growth_rate=0.025,
        risk_free_rate=0.03,
        market_premium=0.07
    )
    
    engine = DCFMonteCarloFixed(params)
    await engine.setup()
    
    print(f"\n🚀 Processing ALL historical data for these {len(companies_with_quality_data)} companies")
    print(f"   Using 2,000 Monte Carlo simulations per valuation")
    print("=" * 80)
    
    total_valuations = 0
    company_summaries = []
    
    for i, company_info in enumerate(companies_with_quality_data):
        symbol = company_info['symbol']
        
        print(f"\n📈 {i+1}/{len(companies_with_quality_data)} - Processing {symbol}")
        print(f"   Data span: {company_info['first_financial_date']} to {company_info['last_financial_date']}")
        
        try:
            # Get ALL historical dates with both price and adequate financial data
            historical_dates_query = """
            WITH financial_coverage AS (
                SELECT 
                    period_date,
                    period_date + INTERVAL '3 months' as coverage_end
                FROM financial_statements
                WHERE symbol = $1
                AND total_revenue > 0
                AND operating_income IS NOT NULL
                ORDER BY period_date
            ),
            covered_price_dates AS (
                SELECT DISTINCT hfd.date, hfd.close_price
                FROM historical_fundamentals_daily hfd
                CROSS JOIN financial_coverage fc
                WHERE hfd.symbol = $1
                AND hfd.date >= fc.period_date
                AND hfd.date <= fc.coverage_end
                AND hfd.close_price > 0
                AND hfd.market_cap > 0
                ORDER BY hfd.date DESC
            )
            SELECT date, close_price
            FROM covered_price_dates
            ORDER BY date DESC
            """
            
            historical_dates = await engine.db_conn.fetch(historical_dates_query, symbol)
            
            if not historical_dates:
                print(f"❌ {symbol}: No adequate historical data overlap")
                continue
                
            print(f"   Found {len(historical_dates)} historical valuations to process")
            
            company_valuations = 0
            successful_valuations = 0
            
            # Process historical dates in batches to avoid overwhelming output
            for j, date_record in enumerate(historical_dates):
                valuation_date = datetime.combine(date_record['date'], datetime.min.time())
                market_price = float(date_record['close_price'])
                
                try:
                    # Run DCF valuation for this specific date
                    result = await engine.value_company(
                        symbol, 
                        valuation_date, 
                        market_price
                    )
                    
                    if result:
                        # Save to database
                        dcf_id = await engine.save_valuation(result)
                        successful_valuations += 1
                        total_valuations += 1
                        
                        # Show progress every 100 successful valuations
                        if successful_valuations % 100 == 0:
                            rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                            print(f"   ✓ {successful_valuations:>3} valuations | Latest: {date_record['date']} | "
                                  f"Fair value: ${result['fair_value_median']:.0f} vs ${market_price:.0f} | "
                                  f"Rate sens: {rate_sens:.1f}/10")
                            
                except Exception as e:
                    if company_valuations < 5:  # Only show first few errors
                        logger.warning(f"{symbol} {date_record['date']}: {str(e)[:50]}...")
                        
                company_valuations += 1
                    
            print(f"✅ {symbol}: {successful_valuations:,} DCF valuations created from {len(historical_dates):,} historical dates")
            company_summaries.append({
                'symbol': symbol,
                'total_dates_processed': len(historical_dates),
                'successful_valuations': successful_valuations,
                'success_rate': (successful_valuations / len(historical_dates)) * 100 if historical_dates else 0
            })
                
        except Exception as e:
            print(f"❌ {symbol}: Fatal error - {e}")
            continue
    
    await engine.cleanup()
    
    # Final summary
    print("\n" + "=" * 80)
    print(f"📈 COMPREHENSIVE HISTORICAL DCF ANALYSIS COMPLETE")
    print("=" * 80)
    
    if company_summaries:
        print(f"\n✅ Successfully processed {len(company_summaries)} companies:")
        print(f"📊 Total DCF valuations created: {total_valuations:,}")
        
        # Show detailed per-company breakdown
        company_summaries.sort(key=lambda x: x['successful_valuations'], reverse=True)
        
        print(f"\n📋 DETAILED RESULTS:")
        for summary in company_summaries:
            print(f"   {summary['symbol']:<8}: {summary['successful_valuations']:>6,} valuations "
                  f"({summary['success_rate']:>5.1f}% success rate from {summary['total_dates_processed']:,} dates)")
        
        # Calculate totals
        total_success = sum(s['successful_valuations'] for s in company_summaries)
        total_processed = sum(s['total_dates_processed'] for s in company_summaries)
        overall_success_rate = (total_success / total_processed) * 100 if total_processed else 0
        
        print(f"\n📈 OVERALL STATISTICS:")
        print(f"   Total historical dates processed: {total_processed:,}")
        print(f"   Total successful DCF valuations: {total_success:,}")
        print(f"   Overall success rate: {overall_success_rate:.1f}%")
        print(f"   Average valuations per company: {total_success / len(company_summaries):,.0f}")
        
    print(f"\n💾 All {total_valuations:,} DCF valuations saved to dcf_valuations table")
    print(f"🔍 Historical valuation patterns and trends are now available for analysis")
    print(f"📊 This addresses your request to run DCF on 'all available historical data'")

if __name__ == "__main__":
    asyncio.run(run_comprehensive_historical_dcf())