#!/usr/bin/env python3
"""
Generate ALL DCF Valuations with Conservative Model

Processes all available financial reports across all companies to build
a comprehensive DCF valuation database for historical backtesting.
"""

import asyncio
import asyncpg
import time
from datetime import datetime
from dcf_report_processor import DCFReportProcessor

async def generate_comprehensive_dcf_valuations():
    """Generate DCF valuations for all available financial reports."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🏗️  GENERATING COMPREHENSIVE DCF VALUATIONS")
        print("=" * 60)
        
        # 1. Check current state
        current_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_valuations,
                COUNT(DISTINCT symbol) as companies_covered,
                MIN(publish_date) as earliest,
                MAX(publish_date) as latest
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
        """)
        
        print(f"\n📊 CURRENT STATE:")
        print(f"   Existing Valuations: {current_stats['total_valuations']:,}")
        print(f"   Companies Covered: {current_stats['companies_covered']}")
        if current_stats['earliest']:
            print(f"   Date Range: {current_stats['earliest']} to {current_stats['latest']}")
        
        # 2. Get total available reports
        total_reports = await conn.fetchval("""
            SELECT COUNT(*)
            FROM financial_statements fs
            JOIN company_master cm ON fs.symbol = cm.primary_ticker
            WHERE fs.publish_date IS NOT NULL
            AND fs.total_revenue > 0
            AND fs.publish_date >= '2020-01-01'
        """)
        
        print(f"   Total Available Reports: {total_reports:,}")
        
        # 3. Get companies with sufficient data
        companies_with_data = await conn.fetch("""
            SELECT 
                fs.symbol,
                COUNT(*) as report_count,
                MIN(fs.publish_date) as earliest_report,
                MAX(fs.publish_date) as latest_report,
                cm.stock_currency,
                cm.report_currency
            FROM financial_statements fs
            JOIN company_master cm ON fs.symbol = cm.primary_ticker
            WHERE fs.publish_date IS NOT NULL
            AND fs.total_revenue > 0
            AND fs.publish_date >= '2020-01-01'
            GROUP BY fs.symbol, cm.stock_currency, cm.report_currency
            HAVING COUNT(*) >= 3  -- Need at least 3 reports for DCF
            ORDER BY COUNT(*) DESC, fs.symbol
        """)
        
        print(f"   Companies with Sufficient Data: {len(companies_with_data):,}")
        
        # 4. Show breakdown by report count
        report_count_breakdown = await conn.fetch("""
            SELECT 
                CASE 
                    WHEN COUNT(*) >= 20 THEN '20+ reports'
                    WHEN COUNT(*) >= 10 THEN '10-19 reports'
                    WHEN COUNT(*) >= 5 THEN '5-9 reports'
                    ELSE '3-4 reports'
                END as category,
                COUNT(DISTINCT fs.symbol) as companies
            FROM financial_statements fs
            JOIN company_master cm ON fs.symbol = cm.primary_ticker
            WHERE fs.publish_date IS NOT NULL
            AND fs.total_revenue > 0
            AND fs.publish_date >= '2020-01-01'
            GROUP BY fs.symbol
            HAVING COUNT(*) >= 3
        """)
        
        print(f"\n📈 BREAKDOWN BY DATA RICHNESS:")
        for row in sorted(report_count_breakdown, key=lambda x: x['companies'], reverse=True):
            print(f"   {row['category']}: {row['companies']:,} companies")
        
        # 5. Start comprehensive processing
        print(f"\n🚀 STARTING COMPREHENSIVE DCF GENERATION")
        processor = DCFReportProcessor(model_version="clean_dcf_v1.0")
        await processor.setup()
        
        start_time = time.time()
        batch_size = 100
        processed_batches = 0
        
        try:
            while True:
                print(f"\n🔄 Processing batch {processed_batches + 1} ({batch_size} reports)...")
                
                # Get current processing stats before batch
                pre_batch_stats = await processor.get_processing_stats()
                
                # Process batch
                await processor.process_batch(limit=batch_size)
                processed_batches += 1
                
                # Get post-batch stats
                post_batch_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM dcf_valuations 
                    WHERE model_version = 'clean_dcf_v1.0'
                """)
                
                # Check if we processed anything new
                new_valuations = post_batch_count - current_stats['total_valuations']
                
                if new_valuations == 0:
                    print("✅ No new reports to process - all done!")
                    break
                
                # Update current count for next iteration
                current_stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_valuations,
                        COUNT(DISTINCT symbol) as companies_covered
                    FROM dcf_valuations
                    WHERE model_version = 'clean_dcf_v1.0'
                """)
                
                elapsed = time.time() - start_time
                rate = (processed_batches * batch_size) / elapsed * 60  # per minute
                
                print(f"📊 Progress Update:")
                print(f"   Batches Processed: {processed_batches}")
                print(f"   Total Valuations: {current_stats['total_valuations']:,}")
                print(f"   Companies Covered: {current_stats['companies_covered']}")
                print(f"   Processing Rate: {rate:.1f} attempts/min")
                print(f"   Elapsed Time: {elapsed/60:.1f} minutes")
                
                # Prevent infinite processing
                if processed_batches >= 50:  # Safety limit
                    print("⚠️ Reached batch limit, pausing for review")
                    break
        
        finally:
            await processor.cleanup()
        
        # 6. Final comprehensive statistics
        print(f"\n📊 FINAL COMPREHENSIVE STATISTICS")
        print("=" * 50)
        
        final_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_valuations,
                COUNT(DISTINCT symbol) as companies_covered,
                AVG(valuation_confidence) as avg_confidence,
                AVG(data_quality_score) as avg_data_quality,
                AVG(computation_time_ms) as avg_computation_time,
                MIN(publish_date) as earliest_report,
                MAX(publish_date) as latest_report
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
        """)
        
        print(f"Total DCF Valuations: {final_stats['total_valuations']:,}")
        print(f"Companies Covered: {final_stats['companies_covered']}")
        print(f"Date Range: {final_stats['earliest_report']} to {final_stats['latest_report']}")
        print(f"Average Confidence: {final_stats['avg_confidence']:.1%}")
        print(f"Average Data Quality: {final_stats['avg_data_quality']:.1%}")
        print(f"Average Computation: {final_stats['avg_computation_time']:.0f}ms")
        
        # 7. Coverage by time period
        print(f"\n📅 COVERAGE BY TIME PERIOD:")
        
        time_coverage = await conn.fetch("""
            SELECT 
                DATE_TRUNC('year', publish_date) as year,
                COUNT(*) as valuations,
                COUNT(DISTINCT symbol) as companies
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            GROUP BY DATE_TRUNC('year', publish_date)
            ORDER BY year DESC
        """)
        
        for row in time_coverage:
            year = row['year'].year
            print(f"   {year}: {row['valuations']:,} valuations across {row['companies']} companies")
        
        # 8. Top companies by valuation count
        print(f"\n🏢 TOP COMPANIES BY VALUATION COUNT:")
        
        top_companies = await conn.fetch("""
            SELECT 
                symbol,
                COUNT(*) as valuations,
                MIN(publish_date) as earliest,
                MAX(publish_date) as latest,
                AVG(valuation_confidence) as avg_confidence
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            GROUP BY symbol
            ORDER BY COUNT(*) DESC, symbol
            LIMIT 15
        """)
        
        for comp in top_companies:
            print(f"   {comp['symbol']}: {comp['valuations']} valuations "
                  f"({comp['earliest']} to {comp['latest']}, "
                  f"confidence: {comp['avg_confidence']:.0%})")
        
        # 9. Data quality distribution
        print(f"\n📈 DATA QUALITY DISTRIBUTION:")
        
        quality_dist = await conn.fetch("""
            SELECT 
                CASE 
                    WHEN valuation_confidence >= 0.8 THEN 'Excellent (80%+)'
                    WHEN valuation_confidence >= 0.6 THEN 'Good (60-79%)'
                    WHEN valuation_confidence >= 0.4 THEN 'Fair (40-59%)'
                    WHEN valuation_confidence >= 0.2 THEN 'Poor (20-39%)'
                    ELSE 'Very Poor (<20%)'
                END as quality_tier,
                COUNT(*) as count
            FROM dcf_valuations
            WHERE model_version = 'clean_dcf_v1.0'
            GROUP BY 
                CASE 
                    WHEN valuation_confidence >= 0.8 THEN 'Excellent (80%+)'
                    WHEN valuation_confidence >= 0.6 THEN 'Good (60-79%)'
                    WHEN valuation_confidence >= 0.4 THEN 'Fair (40-59%)'
                    WHEN valuation_confidence >= 0.2 THEN 'Poor (20-39%)'
                    ELSE 'Very Poor (<20%)'
                END
            ORDER BY COUNT(*) DESC
        """)
        
        for quality in quality_dist:
            percentage = quality['count'] / final_stats['total_valuations'] * 100
            print(f"   {quality['quality_tier']}: {quality['count']:,} ({percentage:.1f}%)")
        
        print(f"\n✅ COMPREHENSIVE DCF GENERATION COMPLETE!")
        print(f"🎯 Ready for extensive historical backtesting!")
        
        total_elapsed = time.time() - start_time
        print(f"⏱️  Total Processing Time: {total_elapsed/60:.1f} minutes")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(generate_comprehensive_dcf_valuations())