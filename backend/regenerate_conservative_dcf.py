#!/usr/bin/env python3
"""
Regenerate DCF Valuations with Conservative Assumptions

Clears existing valuations and regenerates them with the new conservative model:
- Growth rates capped at ±15%
- Faster growth decay (0.8 vs 0.9)
- Higher WACC (13% vs 10%)
- More conservative FCF conversion (65% vs 85%)
"""

import asyncio
import asyncpg
from dcf_report_processor import DCFReportProcessor

async def regenerate_conservative_valuations():
    """Clear and regenerate all DCF valuations with conservative model."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🔄 REGENERATING DCF VALUATIONS WITH CONSERVATIVE MODEL")
        print("=" * 65)
        
        # 1. Show current valuations count
        current_count = await conn.fetchval("""
            SELECT COUNT(*) FROM dcf_valuations 
            WHERE model_version = 'clean_dcf_v1.0'
        """)
        
        print(f"\n📊 Current valuations: {current_count}")
        
        # 2. Clear existing valuations for fresh start
        print(f"\n🗑️  Clearing existing valuations...")
        deleted_count = await conn.fetchval("""
            DELETE FROM dcf_valuations 
            WHERE model_version = 'clean_dcf_v1.0'
            RETURNING 1
        """)
        
        if deleted_count:
            print(f"✅ Cleared {current_count} existing valuations")
        else:
            print(f"✅ No existing valuations to clear")
        
        # 3. Create new processor with conservative model
        print(f"\n🏗️  Initializing conservative DCF processor...")
        processor = DCFReportProcessor(model_version="clean_dcf_v1.0")
        await processor.setup()
        
        try:
            # 4. Get reports for reliable companies first
            print(f"\n🎯 Processing reliable companies first...")
            
            reliable_companies = ['VOLV-B', 'ABB', 'AAK', 'ERIC-B', 'SEB-A', 'SWED-A']
            
            reliable_reports_query = """
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
            WHERE fs.publish_date IS NOT NULL
            AND fs.total_revenue > 0
            AND fs.symbol = ANY($1)
            AND fs.publish_date >= '2023-01-01'
            ORDER BY fs.publish_date DESC, fs.symbol
            """
            
            rows = await conn.fetch(reliable_reports_query, reliable_companies)
            reliable_reports = [dict(row) for row in rows]
            
            print(f"📋 Found {len(reliable_reports)} reports from reliable companies")
            
            # Process reliable companies
            reliable_success = 0
            for i, report in enumerate(reliable_reports, 1):
                print(f"\n🔄 [{i}/{len(reliable_reports)}] Processing {report['symbol']} - {report['report_date']}")
                
                success = await processor.process_report(report)
                if success:
                    reliable_success += 1
                    print(f"   ✅ Success!")
                else:
                    print(f"   ❌ Failed")
                
                # Progress update every 5 reports
                if i % 5 == 0:
                    print(f"\n📊 Progress: {i}/{len(reliable_reports)} processed, {reliable_success} successful")
            
            print(f"\n🎯 Reliable companies: {reliable_success}/{len(reliable_reports)} successful")
            
            # 5. Process remaining companies in smaller batches
            print(f"\n🔄 Processing additional companies...")
            await processor.process_batch(limit=30)  # Process 30 more companies
            
            # 6. Show final statistics
            await processor.get_processing_stats()
            
            # 7. Show sample of new valuations
            print(f"\n📊 SAMPLE OF NEW CONSERVATIVE VALUATIONS:")
            
            sample_query = """
            SELECT 
                symbol,
                report_date,
                fair_value_stock_median,
                valuation_confidence,
                computation_time_ms
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            ORDER BY computation_date DESC
            LIMIT 10
            """
            
            samples = await conn.fetch(sample_query)
            
            if samples:
                print(f"{'Symbol':<10} {'Report Date':<12} {'Fair Value':<12} {'Confidence':<11} {'Time':<8}")
                print("-" * 65)
                
                for sample in samples:
                    print(f"{sample['symbol']:<10} {sample['report_date']:<12} "
                          f"{sample['fair_value_stock_median']:>10.0f} SEK "
                          f"{sample['valuation_confidence']:>9.0%} "
                          f"{sample['computation_time_ms']:>6}ms")
            
            # 8. Compare with market prices for validation
            print(f"\n🎯 VALIDATION - COMPARISON WITH MARKET PRICES:")
            
            validation_query = """
            SELECT 
                v.symbol,
                v.fair_value_stock_median as fair_value,
                p.close_price as market_price,
                (v.fair_value_stock_median - p.close_price) / p.close_price as implied_return
            FROM dcf_valuations v
            JOIN daily_price_data p ON v.symbol = p.symbol
            WHERE v.model_version = 'clean_dcf_v1.0'
            AND p.date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY ABS((v.fair_value_stock_median - p.close_price) / p.close_price) DESC
            LIMIT 8
            """
            
            validations = await conn.fetch(validation_query)
            
            if validations:
                print(f"{'Symbol':<10} {'Fair Value':<12} {'Market Price':<12} {'Implied Return':<15}")
                print("-" * 60)
                
                for val in validations:
                    implied_return = val['implied_return']
                    print(f"{val['symbol']:<10} {val['fair_value']:>10.0f} SEK "
                          f"{val['market_price']:>10.0f} SEK "
                          f"{implied_return:>13.0%}")
            
            print(f"\n✅ CONSERVATIVE DCF MODEL REGENERATION COMPLETE!")
            print(f"🎯 Valuations should now be much more realistic and actionable")
        
        finally:
            await processor.cleanup()
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(regenerate_conservative_valuations())