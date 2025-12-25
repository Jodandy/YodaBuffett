#!/usr/bin/env python3
"""
Generate Historical DCF Valuations for Backtesting

Specifically targets financial reports from 2022-2024 to build
a comprehensive historical dataset for proper backtesting.
"""

import asyncio
import asyncpg
import time
from datetime import datetime, date
from dcf_report_processor import DCFReportProcessor

async def generate_historical_dcf_valuations():
    """Generate DCF valuations specifically for historical backtesting periods."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🕰️  GENERATING HISTORICAL DCF VALUATIONS FOR BACKTESTING")
        print("=" * 65)
        
        # 1. Check current historical coverage
        current_historical = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_valuations,
                COUNT(DISTINCT symbol) as companies_covered,
                MIN(publish_date) as earliest,
                MAX(publish_date) as latest
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            AND publish_date < '2025-01-01'  -- Historical only
        """)
        
        print(f"📊 CURRENT HISTORICAL COVERAGE:")
        print(f"   Existing Historical Valuations: {current_historical['total_valuations']:,}")
        print(f"   Companies Covered: {current_historical['companies_covered'] or 0}")
        if current_historical['earliest']:
            print(f"   Date Range: {current_historical['earliest']} to {current_historical['latest']}")
        else:
            print(f"   Date Range: None")
        
        # 2. Target historical periods for backtesting
        target_periods = [
            (date(2022, 1, 1), date(2022, 12, 31), '2022'),
            (date(2023, 1, 1), date(2023, 12, 31), '2023'), 
            (date(2024, 1, 1), date(2024, 12, 31), '2024')
        ]
        
        processor = DCFReportProcessor(model_version="clean_dcf_v1.0")
        await processor.setup()
        
        try:
            total_generated = 0
            
            for start_date, end_date, year_label in target_periods:
                print(f"\n🎯 PROCESSING {year_label} FINANCIAL REPORTS")
                print("-" * 50)
                
                # Get reports for this period that haven't been processed
                period_query = """
                SELECT DISTINCT
                    fs.symbol,
                    fs.period_date as report_date,
                    fs.publish_date,
                    CASE 
                        WHEN (EXTRACT(MONTH FROM fs.period_date) = 12 AND EXTRACT(DAY FROM fs.period_date) >= 28)
                             OR (EXTRACT(MONTH FROM fs.period_date) = 1 AND EXTRACT(DAY FROM fs.period_date) <= 3)
                        THEN 'annual'
                        ELSE 'quarterly'
                    END as report_type,
                    cm.stock_currency,
                    cm.report_currency
                FROM financial_statements fs
                JOIN company_master cm ON fs.symbol = cm.primary_ticker
                LEFT JOIN dcf_valuations dv ON (
                    dv.symbol = fs.symbol 
                    AND dv.report_date = fs.period_date 
                    AND dv.model_version = 'clean_dcf_v1.0'
                )
                WHERE fs.publish_date BETWEEN $1 AND $2
                AND fs.total_revenue > 0
                AND dv.id IS NULL  -- Not already processed
                ORDER BY fs.publish_date, fs.symbol
                """
                
                period_reports = await conn.fetch(period_query, start_date, end_date)
                period_reports = [dict(row) for row in period_reports]
                
                print(f"   Found {len(period_reports)} unprocessed reports from {year_label}")
                
                if not period_reports:
                    print(f"   ✅ All {year_label} reports already processed")
                    continue
                
                # Process in batches for this year
                batch_size = 50
                successful_this_year = 0
                
                for i in range(0, len(period_reports), batch_size):
                    batch = period_reports[i:i+batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    print(f"\n   📦 {year_label} Batch {batch_num}: Processing {len(batch)} reports")
                    
                    batch_success = 0
                    for j, report in enumerate(batch, 1):
                        success = await processor.process_report(report)
                        if success:
                            batch_success += 1
                            successful_this_year += 1
                            total_generated += 1
                        
                        # Progress within batch
                        if j % 10 == 0:
                            print(f"      Progress: {j}/{len(batch)}, {batch_success} successful")
                    
                    print(f"   ✅ Batch {batch_num} complete: {batch_success}/{len(batch)} successful")
                
                print(f"\n📊 {year_label} Summary: {successful_this_year} successful DCF valuations generated")
                
                # Show year statistics
                year_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as count,
                        COUNT(DISTINCT symbol) as companies,
                        AVG(valuation_confidence) as avg_confidence
                    FROM dcf_valuations
                    WHERE model_version = 'clean_dcf_v1.0'
                    AND publish_date BETWEEN $1 AND $2
                """, start_date, end_date)
                
                if year_stats['count']:
                    print(f"   Total {year_label} valuations: {year_stats['count']} across {year_stats['companies']} companies")
                    print(f"   Average confidence: {year_stats['avg_confidence']:.0%}")
        
        finally:
            await processor.cleanup()
        
        # 3. Final historical statistics
        print(f"\n📊 FINAL HISTORICAL DCF STATISTICS")
        print("=" * 55)
        
        final_historical = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_valuations,
                COUNT(DISTINCT symbol) as companies_covered,
                AVG(valuation_confidence) as avg_confidence,
                AVG(data_quality_score) as avg_data_quality,
                MIN(publish_date) as earliest_report,
                MAX(publish_date) as latest_report
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            AND publish_date < '2025-01-01'  -- Historical only
        """)
        
        print(f"Total Historical Valuations: {final_historical['total_valuations']:,}")
        print(f"Companies Covered: {final_historical['companies_covered']}")
        print(f"Date Range: {final_historical['earliest_report']} to {final_historical['latest_report']}")
        print(f"Average Confidence: {final_historical['avg_confidence']:.1%}")
        print(f"Average Data Quality: {final_historical['avg_data_quality']:.1%}")
        
        # 4. Year-by-year breakdown
        print(f"\n📅 HISTORICAL COVERAGE BY YEAR:")
        
        yearly_breakdown = await conn.fetch("""
            SELECT 
                DATE_TRUNC('year', publish_date) as year,
                COUNT(*) as valuations,
                COUNT(DISTINCT symbol) as companies,
                AVG(valuation_confidence) as avg_confidence
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            AND publish_date < '2025-01-01'
            GROUP BY DATE_TRUNC('year', publish_date)
            ORDER BY year
        """)
        
        for row in yearly_breakdown:
            year = row['year'].year
            print(f"   {year}: {row['valuations']:,} valuations across {row['companies']} companies "
                  f"(confidence: {row['avg_confidence']:.0%})")
        
        # 5. Backtesting readiness check
        print(f"\n🎯 BACKTESTING READINESS:")
        
        backtest_potential = await conn.fetch("""
            SELECT 
                DATE_TRUNC('year', v.publish_date) as year,
                COUNT(*) as valuations_available,
                COUNT(DISTINCT v.symbol) as companies_available,
                COUNT(DISTINCT CASE WHEN p.symbol IS NOT NULL THEN v.symbol END) as companies_with_future_prices
            FROM dcf_valuations v
            LEFT JOIN daily_price_data p ON v.symbol = p.symbol 
                AND p.date > v.publish_date 
                AND p.date <= v.publish_date + INTERVAL '90 days'
            WHERE v.model_version = 'clean_dcf_v1.0'
            AND v.publish_date < '2025-01-01'
            GROUP BY DATE_TRUNC('year', v.publish_date)
            ORDER BY year
        """)
        
        total_backtest_signals = 0
        for row in backtest_potential:
            year = row['year'].year
            valuations = row['valuations_available']
            companies_with_prices = row['companies_with_future_prices'] or 0
            coverage = companies_with_prices / row['companies_available'] * 100 if row['companies_available'] > 0 else 0
            
            print(f"   {year}: {valuations} DCF signals, {companies_with_prices}/{row['companies_available']} "
                  f"companies have subsequent price data ({coverage:.0f}% testable)")
            
            total_backtest_signals += companies_with_prices
        
        print(f"\n✅ HISTORICAL DCF GENERATION COMPLETE!")
        print(f"🎯 {total_backtest_signals} DCF signals ready for historical backtesting!")
        print(f"📈 Comprehensive backtesting now possible across 2022-2024!")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(generate_historical_dcf_valuations())