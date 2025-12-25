#!/usr/bin/env python3
"""
Analyze Document Publish Dates

Check nordic_documents for publish date coverage, classification issues,
and patterns for extracting year/quarter information.
"""

import asyncio
import asyncpg

async def analyze_document_publish_dates():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('📅 ANALYZING NORDIC DOCUMENTS PUBLISH DATES')
    print('=' * 70)
    
    # Check document types and publish date availability
    doc_types_query = """
    SELECT 
        document_type,
        COUNT(*) as total_docs,
        COUNT(publish_date) as with_publish_date,
        COUNT(CASE WHEN publish_date IS NOT NULL THEN 1 END)::float / COUNT(*) * 100 as publish_date_coverage,
        MIN(publish_date) as earliest_publish,
        MAX(publish_date) as latest_publish
    FROM nordic_documents
    GROUP BY document_type
    ORDER BY total_docs DESC
    """
    
    doc_types = await conn.fetch(doc_types_query)
    
    print('📊 DOCUMENT TYPES & PUBLISH DATE COVERAGE:')
    print()
    print(f"{'Type':<20} {'Total':<8} {'W/Date':<8} {'Coverage':<10} {'Date Range':<25}")
    print('-' * 75)
    
    for row in doc_types:
        coverage = row['publish_date_coverage'] or 0
        date_range = f'{row["earliest_publish"]} to {row["latest_publish"]}' if row['earliest_publish'] else 'No dates'
        print(f'{row["document_type"]:<20} {row["total_docs"]:<8} {row["with_publish_date"]:<8} {coverage:<9.1f}% {date_range}')
    
    # Focus on annual and quarterly reports
    print(f'\n🎯 ANNUAL & QUARTERLY REPORTS ANALYSIS:')
    
    key_reports_query = """
    SELECT 
        document_type,
        COUNT(*) as total,
        COUNT(publish_date) as with_dates,
        COUNT(CASE WHEN title ~* 'q[1-4]|kvartal|delår' THEN 1 END) as likely_quarterly,
        COUNT(CASE WHEN title ~* 'år|annual|year' THEN 1 END) as likely_annual
    FROM nordic_documents
    WHERE document_type IN ('annual_report', 'quarterly_report', 'interim_report', 'financial_report')
    GROUP BY document_type
    ORDER BY total DESC
    """
    
    key_reports = await conn.fetch(key_reports_query)
    
    for row in key_reports:
        print(f'\n{row["document_type"]}:')
        print(f'   Total documents: {row["total"]:,}')
        print(f'   With publish dates: {row["with_dates"]:,} ({row["with_dates"]/row["total"]*100:.1f}%)')
        print(f'   Likely quarterly (by title): {row["likely_quarterly"]:,}')
        print(f'   Likely annual (by title): {row["likely_annual"]:,}')
    
    # Check for potential misclassifications
    print(f'\n🔍 POTENTIAL CLASSIFICATION ISSUES:')
    
    misclassified_query = """
    SELECT 
        document_type,
        title,
        publish_date,
        CASE 
            WHEN title ~* 'q[1-4]|kvartal|delår|interim' THEN 'likely_quarterly'
            WHEN title ~* 'år|annual|year' AND title !~* 'q[1-4]|kvartal|delår' THEN 'likely_annual'
            ELSE 'unclear'
        END as suggested_type
    FROM nordic_documents
    WHERE document_type IN ('annual_report', 'quarterly_report')
    AND (
        (document_type = 'annual_report' AND title ~* 'q[1-4]|kvartal|delår') OR
        (document_type = 'quarterly_report' AND title ~* 'år|annual|year')
    )
    ORDER BY publish_date DESC
    LIMIT 15
    """
    
    misclassified = await conn.fetch(misclassified_query)
    
    if misclassified:
        print(f'🚨 Found {len(misclassified)} potentially misclassified documents:')
        for row in misclassified:
            print(f'   {row["document_type"]:16} → {row["suggested_type"]:16} | {row["title"][:50]}')
            print(f'     📅 Published: {row["publish_date"]}')
    else:
        print('✅ No obvious misclassifications found')
    
    # Check year/quarter extraction patterns
    print(f'\n📊 YEAR/QUARTER EXTRACTION PATTERNS:')
    
    patterns_query = """
    SELECT 
        document_type,
        EXTRACT(YEAR FROM publish_date) as publish_year,
        COUNT(*) as doc_count,
        COUNT(CASE WHEN title ~* '2024|2023|2022|2021|2020' THEN 1 END) as with_year_in_title,
        COUNT(CASE WHEN title ~* 'q[1-4]|kvartal' THEN 1 END) as with_quarter_in_title
    FROM nordic_documents
    WHERE document_type IN ('annual_report', 'quarterly_report')
    AND publish_date >= '2020-01-01'
    GROUP BY document_type, EXTRACT(YEAR FROM publish_date)
    ORDER BY document_type, publish_year DESC
    LIMIT 20
    """
    
    patterns = await conn.fetch(patterns_query)
    
    print(f'Year | Type             | Total | Year in Title | Quarter in Title')
    print('-' * 70)
    
    for row in patterns:
        year_pct = (row['with_year_in_title'] / row['doc_count'] * 100) if row['doc_count'] > 0 else 0
        quarter_pct = (row['with_quarter_in_title'] / row['doc_count'] * 100) if row['doc_count'] > 0 else 0
        print(f'{int(row["publish_year"])} | {row["document_type"]:16} | {row["doc_count"]:5} | {row["with_year_in_title"]:5} ({year_pct:4.1f}%) | {row["with_quarter_in_title"]:5} ({quarter_pct:4.1f}%)')
    
    # Sample titles for manual review
    print(f'\n📋 SAMPLE RECENT TITLES FOR PATTERN ANALYSIS:')
    
    sample_query = """
    SELECT document_type, title, publish_date
    FROM nordic_documents
    WHERE document_type IN ('annual_report', 'quarterly_report')
    AND publish_date >= '2024-01-01'
    ORDER BY document_type, publish_date DESC
    LIMIT 20
    """
    
    samples = await conn.fetch(sample_query)
    
    current_type = None
    for row in samples:
        if row['document_type'] != current_type:
            print(f'\n{row["document_type"].upper()}:')
            current_type = row['document_type']
        print(f'   {row["publish_date"]} | {row["title"][:60]}...')
    
    # Check financial statements publish date coverage
    print(f'\n💰 FINANCIAL STATEMENTS PUBLISH DATE STATUS:')
    
    financial_query = """
    SELECT 
        'financial_statements' as table_name,
        COUNT(*) as total_rows,
        COUNT(publish_date) as with_publish_date,
        COUNT(CASE WHEN publish_date IS NOT NULL THEN 1 END)::float / COUNT(*) * 100 as coverage,
        MIN(publish_date) as earliest,
        MAX(publish_date) as latest
    FROM financial_statements
    UNION ALL
    SELECT 
        'balance_sheet_data' as table_name,
        COUNT(*) as total_rows,
        COUNT(publish_date) as with_publish_date,
        COUNT(CASE WHEN publish_date IS NOT NULL THEN 1 END)::float / COUNT(*) * 100 as coverage,
        MIN(publish_date) as earliest,
        MAX(publish_date) as latest
    FROM balance_sheet_data
    UNION ALL
    SELECT 
        'cash_flow_data' as table_name,
        COUNT(*) as total_rows,
        COUNT(publish_date) as with_publish_date,
        COUNT(CASE WHEN publish_date IS NOT NULL THEN 1 END)::float / COUNT(*) * 100 as coverage,
        MIN(publish_date) as earliest,
        MAX(publish_date) as latest
    FROM cash_flow_data
    ORDER BY table_name
    """
    
    financial_stats = await conn.fetch(financial_query)
    
    for row in financial_stats:
        coverage = row['coverage'] or 0
        print(f'{row["table_name"]:20}: {row["total_rows"]:6,} rows, {row["with_publish_date"]:6,} with dates ({coverage:5.1f}%)')
        if row['earliest']:
            print(f'                      Date range: {row["earliest"]} to {row["latest"]}')
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_document_publish_dates())