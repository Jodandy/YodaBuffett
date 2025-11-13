"""
Processing State Management - Add robust pause/resume tracking
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))
from domains.document_intelligence.factory import get_database_url

async def create_processing_state_table():
    """Create table to track document processing state independently"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Creating document_processing_state table...")
        
        # Create processing state tracking table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS document_processing_state (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                file_path TEXT UNIQUE NOT NULL,
                file_name TEXT,
                company_name TEXT,
                country VARCHAR(10),
                region VARCHAR(20),
                document_type VARCHAR(50),
                year INTEGER,
                
                -- File metadata
                file_size_bytes BIGINT,
                pdf_pages INTEGER,
                file_modified_at TIMESTAMP,
                
                -- Processing state
                processing_status VARCHAR(30) DEFAULT 'discovered',
                processing_priority INTEGER DEFAULT 5, -- 1=highest, 10=lowest
                
                -- Processing attempts and results
                attempt_count INTEGER DEFAULT 0,
                last_attempt_at TIMESTAMP,
                last_error TEXT,
                
                -- Success tracking
                filing_id UUID, -- References filings.id when successful
                processed_at TIMESTAMP,
                text_length INTEGER,
                
                -- Discovery and management
                discovered_at TIMESTAMP DEFAULT NOW(),
                batch_id VARCHAR(50), -- Track which batch discovered this
                
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create indexes for efficient querying
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_state_status ON document_processing_state(processing_status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_state_priority ON document_processing_state(processing_priority)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_state_region ON document_processing_state(region)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_state_company ON document_processing_state(company_name)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_state_year ON document_processing_state(year)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_processing_state_batch ON document_processing_state(batch_id)")
        
        # Create processing status constraint (PostgreSQL doesn't support IF NOT EXISTS for constraints)
        try:
            await conn.execute("""
                ALTER TABLE document_processing_state 
                ADD CONSTRAINT check_processing_status 
                CHECK (processing_status IN (
                    'discovered',      -- File found, not yet processed
                    'queued',         -- Ready for processing
                    'processing',     -- Currently being processed  
                    'completed',      -- Successfully processed
                    'failed',         -- Processing failed
                    'skipped',        -- Intentionally skipped (e.g., not priority)
                    'retry_needed'    -- Failed but should retry
                ))
            """)
        except asyncpg.exceptions.DuplicateObjectError:
            pass  # Constraint already exists
        
        print("✅ Processing state table created successfully!")
        
        # Show the schema
        print("\n📊 Document Processing State Schema:")
        columns = await conn.fetch("""
            SELECT column_name, data_type, column_default 
            FROM information_schema.columns 
            WHERE table_name = 'document_processing_state'
            ORDER BY ordinal_position
        """)
        
        for col in columns:
            print(f"  {col['column_name']}: {col['data_type']}")
        
    except Exception as e:
        print(f"❌ Failed to create processing state table: {e}")
        raise
        
    finally:
        await conn.close()

async def create_batch_tracking_table():
    """Create table to track processing batches and sessions"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("\n🔧 Creating batch_processing_sessions table...")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS batch_processing_sessions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                batch_id VARCHAR(50) UNIQUE NOT NULL,
                session_name VARCHAR(100),
                
                -- Session configuration
                max_documents INTEGER,
                priority_filter INTEGER, -- Only process docs with priority <= this
                region_filter VARCHAR(20),
                document_type_filter VARCHAR(50),
                company_filter VARCHAR(200),
                
                -- Session state
                session_status VARCHAR(20) DEFAULT 'active',
                started_at TIMESTAMP DEFAULT NOW(),
                paused_at TIMESTAMP,
                completed_at TIMESTAMP,
                
                -- Progress tracking  
                total_discovered INTEGER DEFAULT 0,
                total_processed INTEGER DEFAULT 0,
                total_failed INTEGER DEFAULT 0,
                total_skipped INTEGER DEFAULT 0,
                
                -- Performance metrics
                avg_processing_time_seconds DECIMAL(10,3),
                documents_per_hour DECIMAL(10,2),
                
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_sessions_status ON batch_processing_sessions(session_status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_sessions_started ON batch_processing_sessions(started_at)")
        
        print("✅ Batch tracking table created successfully!")
        
    except Exception as e:
        print(f"❌ Failed to create batch tracking table: {e}")
        raise
        
    finally:
        await conn.close()

if __name__ == "__main__":
    async def main():
        await create_processing_state_table()
        await create_batch_tracking_table()
        print("\n🎯 Processing state tracking ready!")
        print("Now you can:")
        print("  1. Discover all documents without processing")
        print("  2. Process in controllable batches")
        print("  3. Pause/resume at any time")
        print("  4. Track progress and performance")
        
    asyncio.run(main())