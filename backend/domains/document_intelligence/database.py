"""
Document Intelligence - Database Initialization
Sets up database tables for the document intelligence domain
"""

import asyncpg
import logging

from .factory import get_database_url

logger = logging.getLogger(__name__)


async def init_document_intelligence_tables():
    """Initialize all database tables for document intelligence domain"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # Create filings table (main documents table)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS filings (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                company_symbol VARCHAR(50),
                company_name VARCHAR(200),
                country VARCHAR(10),
                form_type VARCHAR(50),
                filing_date DATE,
                year INTEGER,
                raw_text TEXT,
                extracted_text TEXT,
                processing_status VARCHAR(20) DEFAULT 'processed',
                file_path TEXT UNIQUE,  -- Prevent duplicate processing
                file_name TEXT,
                total_pages INTEGER,
                language VARCHAR(10),
                text_length INTEGER,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create document chunks table for vector processing
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS document_chunks (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
                chunk_index INTEGER,
                chunk_text TEXT,
                page_numbers INTEGER[],
                char_start INTEGER,
                char_end INTEGER,
                chunk_metadata JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create extraction results table for financial metrics
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS extraction_results (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                document_id UUID REFERENCES filings(id) ON DELETE CASCADE,
                extraction_confidence DECIMAL(5,4),
                extracted_metrics JSONB,
                extraction_method VARCHAR(100),
                processing_timestamp TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create processing log table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processing_log (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                file_path TEXT,
                company_name VARCHAR(200),
                document_type VARCHAR(50),
                status VARCHAR(20),
                error_message TEXT,
                processing_stats JSONB,
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create performance indexes
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_company_name ON filings(company_name)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_year ON filings(year)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_country ON filings(country)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_file_path ON filings(file_path)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_filing_id ON document_chunks(filing_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_extraction_document_id ON extraction_results(document_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_log_status ON processing_log(status)")
        
        logger.info("✅ Document Intelligence database tables initialized successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize database tables: {e}")
        raise
        
    finally:
        await conn.close()