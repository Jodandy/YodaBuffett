#!/usr/bin/env python3
"""
Complete extraction and embedding pipeline runner.
Guides through the entire process with proper error handling.
"""

import asyncio
import asyncpg
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url


async def run_complete_pipeline():
    """Run the complete extraction and embedding pipeline."""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🚀 COMPLETE DOCUMENT EXTRACTION & EMBEDDING PIPELINE")
        print("="*70)
        
        # Step 1: Create all necessary tables
        print("\n📊 Step 1: Setting up database tables...")
        
        # Create batch processing tables
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
            
            CREATE INDEX IF NOT EXISTS idx_dps_status 
            ON document_processing_state(processing_status);
        """)
        
        print("✅ Database tables created")
        
        # Step 2: Check current status
        print("\n📊 Step 2: Checking current status...")
        
        # Check extracted documents
        doc_count = await conn.fetchval("""
            SELECT COUNT(*) FROM extracted_documents
            WHERE extracted_text IS NOT NULL
            AND LENGTH(extracted_text) > 1000
        """)
        print(f"   Extracted documents: {doc_count:,}")
        
        # Check sections
        section_count = await conn.fetchval("""
            SELECT COUNT(*) FROM document_sections
        """)
        print(f"   Document sections: {section_count:,}")
        
        # Check embeddings
        embeddings = await conn.fetch("""
            SELECT embedding_model, COUNT(*) as count
            FROM section_embeddings
            GROUP BY embedding_model
        """)
        
        print(f"   Section embeddings:")
        if embeddings:
            for row in embeddings:
                print(f"      {row['embedding_model']}: {row['count']:,}")
        else:
            print(f"      None")
        
        # Step 3: Provide specific commands based on status
        print("\n" + "="*70)
        print("🎯 NEXT STEPS BASED ON YOUR CURRENT STATUS:")
        print("="*70)
        
        if doc_count == 0:
            print("\n1️⃣  You need to extract text from PDFs first!")
            print("\nOption A: If you have PDFs in data/companies/SE/")
            print("   cd backend/")
            print("   PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py discover")
            print("   PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py process 100")
            
            print("\nOption B: If you need to download PDFs first:")
            print("   python3 pdf_download_batch.py --year 2025 --delay 10")
            
        elif section_count == 0:
            print("\n2️⃣  You have documents but need to create sections!")
            print("\nRun these commands:")
            print("   cd backend/")
            print("   python domains/document_intelligence/cli_section_chunking.py setup")
            print("   python domains/document_intelligence/cli_section_chunking.py process 100")
            
            # Check how many docs need sectioning
            docs_needing_sections = await conn.fetchval("""
                SELECT COUNT(*)
                FROM extracted_documents ed
                LEFT JOIN document_sections ds ON ed.id = ds.extracted_document_id
                WHERE ds.id IS NULL
                AND ed.extracted_text IS NOT NULL
                AND LENGTH(ed.extracted_text) > 1000
            """)
            print(f"\n   📊 {docs_needing_sections:,} documents need section chunking")
            
        else:
            # Check for sections without embeddings
            sections_without_embeddings = await conn.fetchval("""
                SELECT COUNT(*)
                FROM document_sections ds
                LEFT JOIN section_embeddings se ON ds.id = se.document_section_id
                    AND se.embedding_model LIKE 'local/%'
                WHERE se.id IS NULL
            """)
            
            if sections_without_embeddings > 0:
                print(f"\n3️⃣  You have {sections_without_embeddings:,} sections that need embeddings!")
                print("\nRun these commands:")
                print("   cd backend/")
                print("   python domains/document_intelligence/cli_multi_embeddings.py local setup")
                print(f"   python domains/document_intelligence/cli_multi_embeddings.py local process {min(1000, sections_without_embeddings)}")
            else:
                print("\n✅ Your pipeline is ready! You can now:")
                print("\n4️⃣  Generate document-level embeddings:")
                print("   python domains/document_intelligence/cli_document_embeddings.py local setup")
                print("   python domains/document_intelligence/cli_document_embeddings.py local process 500 --method hierarchical")
                
                print("\n5️⃣  Run temporal anomaly detection:")
                print("   python test_temporal_patterns.py")
                print("   python test_document_temporal_patterns.py")
                print("   python test_unified_embedding_search.py")
        
        # Additional diagnostics
        print("\n" + "="*70)
        print("📊 DETAILED STATISTICS:")
        print("="*70)
        
        # Check PDF files
        data_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE"
        if os.path.exists(data_path):
            pdf_count = sum(1 for root, dirs, files in os.walk(data_path) 
                           for file in files if file.lower().endswith('.pdf'))
            print(f"\n📁 PDF files available: {pdf_count:,}")
            
            # Sample PDFs
            pdf_files = []
            for root, dirs, files in os.walk(data_path):
                for file in files:
                    if file.lower().endswith('.pdf'):
                        pdf_files.append(os.path.join(root, file))
                        if len(pdf_files) >= 5:
                            break
                if len(pdf_files) >= 5:
                    break
            
            if pdf_files:
                print("\nSample PDFs:")
                for pdf in pdf_files[:5]:
                    rel_path = pdf.replace(data_path + '/', '')
                    print(f"   - {rel_path}")
        
        # Check company distribution
        if doc_count > 0:
            companies = await conn.fetch("""
                SELECT company_name, COUNT(*) as doc_count
                FROM extracted_documents
                GROUP BY company_name
                ORDER BY doc_count DESC
                LIMIT 10
            """)
            
            print(f"\n🏢 Top companies by document count:")
            for company in companies:
                print(f"   {company['company_name']}: {company['doc_count']} docs")
        
        # Check section type distribution
        if section_count > 0:
            section_types = await conn.fetch("""
                SELECT section_type, COUNT(*) as count
                FROM document_sections
                GROUP BY section_type
                ORDER BY count DESC
                LIMIT 10
            """)
            
            print(f"\n📊 Section types distribution:")
            for st in section_types:
                print(f"   {st['section_type']}: {st['count']:,}")
        
        print("\n" + "="*70)
        print("💡 TIP: Run this script again after each step to see your progress!")
        print("="*70)
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_complete_pipeline())