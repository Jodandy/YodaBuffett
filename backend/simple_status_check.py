#!/usr/bin/env python3
"""
Simple status check for document processing pipeline.
"""

import asyncio
import asyncpg
import os
import sys
from pathlib import Path

# Add to path
sys.path.append(str(Path(__file__).parent))

from domains.document_intelligence.factory import get_database_url


async def main():
    print("🔍 DOCUMENT PROCESSING PIPELINE STATUS")
    print("="*60)
    
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # Create missing tables if needed
        print("\n🔧 Ensuring database tables exist...")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS batch_processing_sessions (
                batch_id VARCHAR(50) PRIMARY KEY,
                started_at TIMESTAMP DEFAULT NOW(),
                ended_at TIMESTAMP,
                max_documents INTEGER,
                status VARCHAR(20) DEFAULT 'running',
                documents_discovered INTEGER DEFAULT 0,
                documents_processed INTEGER DEFAULT 0,
                documents_failed INTEGER DEFAULT 0
            );
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS document_processing_state (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                file_path TEXT UNIQUE NOT NULL,
                file_size BIGINT,
                discovered_at TIMESTAMP DEFAULT NOW(),
                processing_status VARCHAR(20) DEFAULT 'discovered',
                priority INTEGER DEFAULT 3,
                document_type VARCHAR(100),
                last_attempt_at TIMESTAMP,
                attempt_count INTEGER DEFAULT 0,
                last_error TEXT,
                processed_at TIMESTAMP
            );
        """)
        print("✅ Database tables ready")
        
        # Check status
        print("\n📊 Current Status:")
        
        # 1. Extracted documents
        doc_count = await conn.fetchval("""
            SELECT COUNT(*) FROM extracted_documents
            WHERE extracted_text IS NOT NULL
            AND LENGTH(extracted_text) > 1000
        """)
        print(f"   Documents extracted: {doc_count:,}")
        
        # 2. Document sections
        try:
            section_count = await conn.fetchval("SELECT COUNT(*) FROM document_sections")
            print(f"   Document sections: {section_count:,}")
        except:
            section_count = 0
            print(f"   Document sections: Table not found")
        
        # 3. Section embeddings
        try:
            embedding_count = await conn.fetchval("SELECT COUNT(*) FROM section_embeddings")
            print(f"   Section embeddings: {embedding_count:,}")
            
            if embedding_count > 0:
                models = await conn.fetch("""
                    SELECT embedding_model, COUNT(*) as count
                    FROM section_embeddings
                    GROUP BY embedding_model
                """)
                for model in models:
                    print(f"      {model['embedding_model']}: {model['count']:,}")
        except:
            embedding_count = 0
            print(f"   Section embeddings: Table not found")
        
        # 4. Document embeddings
        try:
            doc_embedding_count = await conn.fetchval("SELECT COUNT(*) FROM document_embeddings")
            print(f"   Document embeddings: {doc_embedding_count:,}")
        except:
            doc_embedding_count = 0
            print(f"   Document embeddings: Table not found")
        
        # 5. PDF files available
        data_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE"
        if os.path.exists(data_path):
            pdf_count = 0
            for root, dirs, files in os.walk(data_path):
                pdf_count += len([f for f in files if f.lower().endswith('.pdf')])
            print(f"   PDF files available: {pdf_count:,}")
        else:
            print(f"   PDF files available: Directory not found")
        
        # Next steps
        print("\n" + "="*60)
        print("🎯 NEXT STEPS:")
        print("="*60)
        
        if doc_count == 0:
            print("\n1️⃣ Extract text from PDFs first:")
            print("   PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py discover")
            print("   PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py process 100")
            
        elif section_count == 0:
            print("\n2️⃣ Create document sections:")
            print("   python domains/document_intelligence/cli_section_chunking.py setup")
            print("   python domains/document_intelligence/cli_section_chunking.py process 100")
            
        elif embedding_count == 0:
            print("\n3️⃣ Generate section embeddings:")
            print("   python domains/document_intelligence/cli_multi_embeddings.py local setup")
            print("   python domains/document_intelligence/cli_multi_embeddings.py local process 1000")
            
        elif doc_embedding_count == 0:
            print("\n4️⃣ Generate document embeddings:")
            print("   python test_document_embedding_cli.py  # Test setup first")
            print("   python domains/document_intelligence/cli_document_embeddings.py local setup")
            print("   python domains/document_intelligence/cli_document_embeddings.py local process --count 500 --method hierarchical")
            
        else:
            print("\n5️⃣ Run analysis:")
            print("   python test_temporal_patterns.py")
            print("   python test_document_temporal_patterns.py")
            print("   python test_unified_embedding_search.py")
        
        print(f"\n💡 TIP: Run 'python simple_status_check.py' anytime to see your progress")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())