#!/usr/bin/env python3
"""
Simple Publish Date Setter

Simple approach: match documents to financial statements by symbol and
set publish dates using quarter-based logic.
"""

import asyncio
import asyncpg
from datetime import datetime

async def set_publish_dates():
    """Set publish dates on financial statements from documents"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    print('📅 SIMPLE PUBLISH DATE SETTING')
    print('=' * 70)
    
    # Get documents with their company symbols
    docs_query = """
    SELECT 
        nd.id,
        cm.primary_ticker as symbol,
        nd.title,
        nd.publish_date,
        nd.document_type,
        EXTRACT(YEAR FROM nd.publish_date) as pub_year,
        EXTRACT(QUARTER FROM nd.publish_date) as pub_quarter
    FROM nordic_documents nd
    JOIN company_master cm ON nd.company_id = cm.id
    WHERE nd.document_type IN ('annual_report', 'quarterly_report')
    AND nd.publish_date >= '2020-01-01'
    AND cm.primary_ticker IS NOT NULL
    AND (
        nd.title ~* 'delårsrapport|årsredovisning|interim.*report|annual.*report|quarterly.*report|kvartalsrapport'
        AND NOT nd.title ~* 'årsstämma|annual.*general.*meeting|kallelse|notice.*meeting|valberedning|nomination'
    )
    ORDER BY nd.publish_date DESC
    """
    
    docs = await conn.fetch(docs_query)
    
    print(f'📄 Found {len(docs):,} relevant documents to process')
    
    stats = {
        'docs_processed': 0,
        'financial_statements_updated': 0,
        'balance_sheet_updated': 0,  
        'cash_flow_updated': 0
    }
    
    # Process documents
    for i, doc in enumerate(docs):
        stats['docs_processed'] += 1
        
        symbol = doc['symbol']
        publish_date = doc['publish_date']
        doc_type = doc['document_type']
        pub_year = int(doc['pub_year'])
        pub_quarter = int(doc['pub_quarter'])
        
        # Determine target fiscal period using simple quarter logic
        if doc_type == 'annual_report':
            # Annual reports published Q1-Q2 usually for previous year
            if pub_quarter in [1, 2]:
                target_year = pub_year - 1
            else:
                target_year = pub_year
            target_quarter = None  # Annual
        else:
            # Quarterly reports: assume 1-quarter lag
            if pub_quarter == 1:
                target_year = pub_year - 1
                target_quarter = 4
            else:
                target_year = pub_year  
                target_quarter = pub_quarter - 1
        
        # Update financial_statements
        try:
            if target_quarter is None:
                # Annual
                fs_result = await conn.execute("""
                    UPDATE financial_statements 
                    SET publish_date = $1
                    WHERE symbol = $2 
                    AND fiscal_year = $3
                    AND (fiscal_quarter IS NULL OR fiscal_quarter = 4)
                    AND publish_date IS NULL
                """, publish_date, symbol, target_year)
            else:
                # Quarterly
                fs_result = await conn.execute("""
                    UPDATE financial_statements 
                    SET publish_date = $1
                    WHERE symbol = $2 
                    AND fiscal_year = $3
                    AND fiscal_quarter = $4
                    AND publish_date IS NULL
                """, publish_date, symbol, target_year, target_quarter)
            
            if fs_result and fs_result.startswith('UPDATE'):
                updated_count = int(fs_result.split()[-1])
                stats['financial_statements_updated'] += updated_count
            
            # Update balance_sheet_data (if exists - check schema first)
            try:
                if target_quarter is None:
                    bs_result = await conn.execute("""
                        UPDATE balance_sheet_data 
                        SET publish_date = $1
                        WHERE symbol = $2 
                        AND fiscal_year = $3
                        AND (fiscal_quarter IS NULL OR fiscal_quarter = 4)
                        AND publish_date IS NULL
                    """, publish_date, symbol, target_year)
                else:
                    bs_result = await conn.execute("""
                        UPDATE balance_sheet_data 
                        SET publish_date = $1
                        WHERE symbol = $2 
                        AND fiscal_year = $3
                        AND fiscal_quarter = $4
                        AND publish_date IS NULL
                    """, publish_date, symbol, target_year, target_quarter)
                
                if bs_result and bs_result.startswith('UPDATE'):
                    updated_count = int(bs_result.split()[-1])
                    stats['balance_sheet_updated'] += updated_count
            except:
                pass  # Table might not have these columns
            
            # Update cash_flow_data (if exists)
            try:
                if target_quarter is None:
                    cf_result = await conn.execute("""
                        UPDATE cash_flow_data 
                        SET publish_date = $1
                        WHERE symbol = $2 
                        AND fiscal_year = $3
                        AND (fiscal_quarter IS NULL OR fiscal_quarter = 4)
                        AND publish_date IS NULL
                    """, publish_date, symbol, target_year)
                else:
                    cf_result = await conn.execute("""
                        UPDATE cash_flow_data 
                        SET publish_date = $1
                        WHERE symbol = $2 
                        AND fiscal_year = $3
                        AND fiscal_quarter = $4
                        AND publish_date IS NULL
                    """, publish_date, symbol, target_year, target_quarter)
                
                if cf_result and cf_result.startswith('UPDATE'):
                    updated_count = int(cf_result.split()[-1])
                    stats['cash_flow_updated'] += updated_count
            except:
                pass  # Table might not have these columns
        
        except Exception as e:
            continue  # Skip errors
        
        # Progress updates
        if i > 0 and i % 1000 == 0:
            progress = i / len(docs) * 100
            print(f'   📊 {progress:.1f}% | FS: {stats["financial_statements_updated"]}, BS: {stats["balance_sheet_updated"]}, CF: {stats["cash_flow_updated"]}')
    
    # Final stats
    print(f'\n📈 RESULTS:')
    print(f'   Documents processed: {stats["docs_processed"]:,}')
    print(f'   Financial statements updated: {stats["financial_statements_updated"]:,}')
    print(f'   Balance sheet data updated: {stats["balance_sheet_updated"]:,}')
    print(f'   Cash flow data updated: {stats["cash_flow_updated"]:,}')
    
    # Check final coverage
    fs_coverage = await conn.fetchrow('SELECT COUNT(*) as total, COUNT(publish_date) as with_dates FROM financial_statements')
    fs_pct = (fs_coverage['with_dates'] / fs_coverage['total'] * 100) if fs_coverage['total'] > 0 else 0
    
    print(f'\n✅ FINAL COVERAGE:')
    print(f'   Financial statements: {fs_coverage["with_dates"]:,}/{fs_coverage["total"]:,} ({fs_pct:.1f}%)')
    
    # Show sample results
    sample_query = """
    SELECT symbol, fiscal_year, fiscal_quarter, publish_date, total_revenue/1e6 as revenue_m
    FROM financial_statements
    WHERE publish_date IS NOT NULL
    ORDER BY publish_date DESC
    LIMIT 10
    """
    
    samples = await conn.fetch(sample_query)
    
    print(f'\n📋 SAMPLE RESULTS:')
    for row in samples:
        quarter = f"Q{row['fiscal_quarter']}" if row['fiscal_quarter'] else "Annual"
        revenue = f"${row['revenue_m']:.0f}M" if row['revenue_m'] else "N/A"
        print(f'   {row["symbol"]:8} | {row["fiscal_year"]} {quarter:7} | {row["publish_date"]} | {revenue:>8}')
    
    await conn.close()
    return stats

if __name__ == "__main__":
    asyncio.run(set_publish_dates())