#!/usr/bin/env python3
"""
Smart Publish Date Matcher

Match documents to financial statements using a smarter approach:
1. Filter documents to actual financial reports only
2. Use publish_date quarter to infer financial period
3. Match by proximity rather than title parsing
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta

async def get_financial_report_documents():
    """Get only documents that are actual financial reports (not AGM notices, etc.)"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    # Filter for actual financial reports by title patterns
    financial_docs_query = """
    SELECT 
        nd.id as document_id,
        nd.company_id,
        nd.title,
        nd.publish_date,
        nd.document_type,
        cm.company_name,
        cm.primary_ticker,
        EXTRACT(YEAR FROM nd.publish_date) as publish_year,
        EXTRACT(QUARTER FROM nd.publish_date) as publish_quarter
    FROM nordic_documents nd
    JOIN company_master cm ON nd.company_id = cm.id
    WHERE nd.document_type IN ('annual_report', 'quarterly_report')
    AND nd.publish_date >= '2020-01-01'
    AND (
        -- Actual financial reports (not AGM/nomination notices)
        nd.title ~* 'delårsrapport|årsredovisning|interim.*report|annual.*report|quarterly.*report|financial.*report|kvartalsrapport|q[1-4].*report' OR
        -- Or contains clear financial indicators
        nd.title ~* 'resultat|omsättning|vinst|förlust|balans|kassaflöde|revenue|profit|loss|earnings|ebit|financial'
    )
    AND NOT (
        -- Exclude AGM and governance documents
        nd.title ~* 'årsstämma|annual.*general.*meeting|agm|kallelse|notice.*meeting|valberedning|nomination|kommuniké.*stämma|bulletin.*meeting|stämma.*kommuniké'
    )
    ORDER BY nd.company_id, nd.publish_date DESC
    """
    
    docs = await conn.fetch(financial_docs_query)
    await conn.close()
    
    return docs

async def match_by_proximity():
    """Match documents to financial statements by date proximity and period logic"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('🎯 SMART PUBLISH DATE MATCHING')
    print('=' * 70)
    
    # Get filtered financial documents
    docs = await get_financial_report_documents()
    
    print(f'📄 Found {len(docs):,} actual financial report documents')
    
    # Statistics
    stats = {
        'documents_processed': 0,
        'annual_reports_matched': 0,
        'quarterly_reports_matched': 0,
        'financial_statements_updated': 0,
        'balance_sheet_updated': 0,
        'cash_flow_updated': 0,
        'total_updates': 0
    }
    
    # Process each document
    for i, doc in enumerate(docs):
        stats['documents_processed'] += 1
        
        company_id = doc['company_id']
        publish_date = doc['publish_date']
        doc_type = doc['document_type'] 
        publish_year = int(doc['publish_year'])
        publish_quarter = int(doc['publish_quarter'])
        title = doc['title']
        
        # Determine target fiscal period based on publish timing
        if doc_type == 'annual_report':
            # Annual reports usually published Q1-Q2 of following year
            if publish_quarter in [1, 2]:
                target_year = publish_year - 1  # Report for previous year
            else:
                target_year = publish_year  # Report for same year
            
            target_quarter = None  # Annual reports
            stats['annual_reports_matched'] += 1
            
        else:  # quarterly_report
            # Quarterly reports usually published 1-2 months after quarter end
            if publish_quarter == 1:
                # Published Q1 -> likely Q4 of previous year
                target_year = publish_year - 1
                target_quarter = 4
            elif publish_quarter == 2:
                # Published Q2 -> likely Q1 of same year
                target_year = publish_year
                target_quarter = 1
            elif publish_quarter == 3:
                # Published Q3 -> likely Q2 of same year
                target_year = publish_year
                target_quarter = 2
            else:  # publish_quarter == 4
                # Published Q4 -> likely Q3 of same year
                target_year = publish_year
                target_quarter = 3
            
            stats['quarterly_reports_matched'] += 1
        
        # Match and update financial_statements
        try:
            if target_quarter is None:
                # Annual report
                fs_update = await conn.execute("""
                    UPDATE financial_statements 
                    SET publish_date = $1, updated_at = NOW()
                    WHERE company_id = $2 
                    AND fiscal_year = $3
                    AND (fiscal_quarter IS NULL OR fiscal_quarter = 4)
                    AND publish_date IS NULL
                """, publish_date, company_id, target_year)
            else:
                # Quarterly report
                fs_update = await conn.execute("""
                    UPDATE financial_statements 
                    SET publish_date = $1, updated_at = NOW()
                    WHERE company_id = $2 
                    AND fiscal_year = $3
                    AND fiscal_quarter = $4
                    AND publish_date IS NULL
                """, publish_date, company_id, target_year, target_quarter)
            
            if fs_update:
                fs_rows = int(fs_update.split()[-1])
                stats['financial_statements_updated'] += fs_rows
                stats['total_updates'] += fs_rows
            
            # Match and update balance_sheet_data
            if target_quarter is None:
                bs_update = await conn.execute("""
                    UPDATE balance_sheet_data 
                    SET publish_date = $1, updated_at = NOW()
                    WHERE company_id = $2 
                    AND fiscal_year = $3
                    AND (fiscal_quarter IS NULL OR fiscal_quarter = 4)
                    AND publish_date IS NULL
                """, publish_date, company_id, target_year)
            else:
                bs_update = await conn.execute("""
                    UPDATE balance_sheet_data 
                    SET publish_date = $1, updated_at = NOW()
                    WHERE company_id = $2 
                    AND fiscal_year = $3
                    AND fiscal_quarter = $4
                    AND publish_date IS NULL
                """, publish_date, company_id, target_year, target_quarter)
            
            if bs_update:
                bs_rows = int(bs_update.split()[-1])
                stats['balance_sheet_updated'] += bs_rows
                stats['total_updates'] += bs_rows
            
            # Match and update cash_flow_data
            if target_quarter is None:
                cf_update = await conn.execute("""
                    UPDATE cash_flow_data 
                    SET publish_date = $1, updated_at = NOW()
                    WHERE company_id = $2 
                    AND fiscal_year = $3
                    AND (fiscal_quarter IS NULL OR fiscal_quarter = 4)
                    AND publish_date IS NULL
                """, publish_date, company_id, target_year)
            else:
                cf_update = await conn.execute("""
                    UPDATE cash_flow_data 
                    SET publish_date = $1, updated_at = NOW()
                    WHERE company_id = $2 
                    AND fiscal_year = $3
                    AND fiscal_quarter = $4
                    AND publish_date IS NULL
                """, publish_date, company_id, target_year, target_quarter)
            
            if cf_update:
                cf_rows = int(cf_update.split()[-1])
                stats['cash_flow_updated'] += cf_rows
                stats['total_updates'] += cf_rows
        
        except Exception as e:
            print(f'   ⚠️ Error processing {doc["primary_ticker"]} ({target_year}): {str(e)[:50]}...')
            continue
        
        # Progress updates
        if i > 0 and i % 1000 == 0:
            progress = i / len(docs) * 100
            print(f'   📊 Progress: {progress:.1f}% | Updates: FS={stats["financial_statements_updated"]}, BS={stats["balance_sheet_updated"]}, CF={stats["cash_flow_updated"]}')
    
    # Final statistics
    print(f'\n📈 MATCHING COMPLETE:')
    print(f'   Documents processed: {stats["documents_processed"]:,}')
    print(f'   Annual reports matched: {stats["annual_reports_matched"]:,}')
    print(f'   Quarterly reports matched: {stats["quarterly_reports_matched"]:,}')
    print(f'   Financial statements updated: {stats["financial_statements_updated"]:,}')
    print(f'   Balance sheet data updated: {stats["balance_sheet_updated"]:,}')
    print(f'   Cash flow data updated: {stats["cash_flow_updated"]:,}')
    print(f'   Total records updated: {stats["total_updates"]:,}')
    
    # Check final coverage
    print(f'\n✅ FINAL PUBLISH DATE COVERAGE:')
    
    coverage_queries = [
        ('financial_statements', 'SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM financial_statements'),
        ('balance_sheet_data', 'SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM balance_sheet_data'),
        ('cash_flow_data', 'SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM cash_flow_data')
    ]
    
    for table_name, query in coverage_queries:
        result = await conn.fetchrow(query)
        total = result['total']
        with_dates = result['with_dates']
        coverage = (with_dates / total * 100) if total > 0 else 0
        improvement = "📈" if with_dates > 0 else "📊"
        print(f'   {table_name:20}: {with_dates:,}/{total:,} ({coverage:.1f}%) {improvement}')
    
    await conn.close()
    return stats

async def validate_sample_matches():
    """Validate a sample of matches to ensure accuracy"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print(f'\n🔍 VALIDATION SAMPLE:')
    
    # Get sample matches
    sample_query = """
    SELECT 
        cm.primary_ticker,
        fs.fiscal_year,
        fs.fiscal_quarter,
        fs.publish_date,
        fs.total_revenue / 1e6 as revenue_millions
    FROM financial_statements fs
    JOIN company_master cm ON fs.company_id = cm.id
    WHERE fs.publish_date IS NOT NULL
    ORDER BY fs.publish_date DESC
    LIMIT 20
    """
    
    samples = await conn.fetch(sample_query)
    
    print(f'Sample of matched financial statements with publish dates:')
    for row in samples:
        quarter_str = f"Q{row['fiscal_quarter']}" if row['fiscal_quarter'] else "Annual"
        revenue_str = f"${row['revenue_millions']:.0f}M" if row['revenue_millions'] else "N/A"
        print(f'   {row["primary_ticker"]:8} | {row["fiscal_year"]} {quarter_str:7} | {row["publish_date"]} | {revenue_str:>8}')
    
    await conn.close()

async def main():
    """Main function to run smart publish date matching"""
    
    print('📅 SMART PUBLISH DATE MATCHING SYSTEM')
    print('=' * 70)
    
    # Run the smart matching
    stats = await match_by_proximity()
    
    # Validate sample results
    await validate_sample_matches()
    
    print(f'\n🎯 PUBLISH DATE MATCHING COMPLETE')
    print(f'   Successfully set publish dates on {stats["total_updates"]:,} financial records')
    print(f'   Using proximity-based matching with financial report filtering')

if __name__ == "__main__":
    asyncio.run(main())