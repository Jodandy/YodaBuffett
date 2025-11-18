#!/usr/bin/env python3
"""Find documents with successful text extraction for testing"""

import asyncio
import asyncpg
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def find_good_docs():
    conn = await asyncpg.connect(get_database_url())
    
    # Find documents with substantial extracted text
    good_docs = await conn.fetch("""
        SELECT company_name, form_type, year, 
               LENGTH(extracted_text) as text_length
        FROM extracted_documents 
        WHERE LENGTH(extracted_text) > 50000  -- At least 50k chars
        AND (form_type = 'annual_report' OR form_type = 'quarterly_report')
        ORDER BY text_length DESC
        LIMIT 20
    """)
    
    print("📄 Documents with Good Text Extraction (>50k chars):")
    print("=" * 60)
    
    for doc in good_docs:
        print(f"{doc['company_name']:30} {doc['form_type']:20} {doc['year']} - {doc['text_length']:,} chars")
    
    # Also check Swedish companies specifically
    print("\n\n🇸🇪 Swedish Companies with Good Extraction:")
    print("=" * 60)
    
    swedish_docs = await conn.fetch("""
        SELECT DISTINCT company_name, COUNT(*) as doc_count,
               AVG(LENGTH(extracted_text)) as avg_length
        FROM extracted_documents 
        WHERE LENGTH(extracted_text) > 10000
        GROUP BY company_name
        HAVING AVG(LENGTH(extracted_text)) > 50000
        ORDER BY avg_length DESC
        LIMIT 10
    """)
    
    for doc in swedish_docs:
        print(f"{doc['company_name']:30} {doc['doc_count']} docs, avg {int(doc['avg_length']):,} chars")
        
    await conn.close()

if __name__ == "__main__":
    asyncio.run(find_good_docs())