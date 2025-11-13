#!/usr/bin/env python3
"""
Add Extraction Tracking Columns to nordic_documents Table

This migration adds comprehensive tracking for text extraction lifecycle
to the existing nordic_documents table, eliminating the need for a 
separate document_processing_state table.
"""

import asyncio
import asyncpg
import sys
from pathlib import Path
from datetime import datetime

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from domains.document_intelligence.factory import get_database_url

async def add_extraction_tracking_columns():
    """Add extraction tracking columns to nordic_documents table"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Adding extraction tracking columns to nordic_documents table...")
        
        # Add all extraction tracking columns
        await conn.execute("""
            -- Extraction lifecycle
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_status VARCHAR(30) DEFAULT 'pending';
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_priority INTEGER DEFAULT 5;
        """)
        
        # Attempt tracking
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_attempts INTEGER DEFAULT 0;
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS last_extraction_attempt TIMESTAMP;
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extracted_at TIMESTAMP;
        """)
        
        # Error handling
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_error TEXT;
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_warnings TEXT[];
        """)
        
        # Success tracking
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS filing_id UUID;
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS text_length INTEGER;
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_confidence DECIMAL(3,2);
        """)
        
        # Version control for re-extraction
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_version VARCHAR(20) DEFAULT 'v1.0';
        """)
        
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extractor_model VARCHAR(50);
        """)
        
        # Performance tracking
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS extraction_duration_seconds INTEGER;
        """)
        
        # Content analysis metadata
        await conn.execute("""
            ALTER TABLE nordic_documents 
            ADD COLUMN IF NOT EXISTS content_analysis JSONB DEFAULT '{}';
        """)
        
        print("✅ All extraction tracking columns added successfully!")
        
    except Exception as e:
        print(f"❌ Error adding columns: {e}")
        raise
    
    finally:
        await conn.close()

async def add_extraction_constraints():
    """Add constraints for data integrity"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Adding extraction status constraint...")
        
        # Add constraint for valid extraction statuses
        try:
            await conn.execute("""
                ALTER TABLE nordic_documents 
                ADD CONSTRAINT check_extraction_status 
                CHECK (extraction_status IN (
                    'pending',           -- Not yet processed
                    'processing',        -- Currently being extracted
                    'completed',         -- Successfully extracted
                    'failed_extraction', -- Extraction failed (retryable)
                    'failed_permanent',  -- Permanent failure (bad PDF, etc.)
                    'skipped'            -- Intentionally skipped
                ))
            """)
            print("✅ Extraction status constraint added!")
        except asyncpg.exceptions.DuplicateObjectError:
            print("ℹ️  Extraction status constraint already exists")
        
        # Add constraint for extraction confidence (0.0 to 1.0)
        try:
            await conn.execute("""
                ALTER TABLE nordic_documents 
                ADD CONSTRAINT check_extraction_confidence 
                CHECK (extraction_confidence IS NULL OR (extraction_confidence >= 0.0 AND extraction_confidence <= 1.0))
            """)
            print("✅ Extraction confidence constraint added!")
        except asyncpg.exceptions.DuplicateObjectError:
            print("ℹ️  Extraction confidence constraint already exists")
        
        # Add constraint for extraction priority (1-10)
        try:
            await conn.execute("""
                ALTER TABLE nordic_documents 
                ADD CONSTRAINT check_extraction_priority 
                CHECK (extraction_priority >= 1 AND extraction_priority <= 10)
            """)
            print("✅ Extraction priority constraint added!")
        except asyncpg.exceptions.DuplicateObjectError:
            print("ℹ️  Extraction priority constraint already exists")
            
    except Exception as e:
        print(f"❌ Error adding constraints: {e}")
        raise
    
    finally:
        await conn.close()

async def create_extraction_indexes():
    """Create indexes for efficient extraction queries"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Creating extraction indexes...")
        
        # Index for finding documents ready for extraction
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_nordic_documents_extraction_status 
            ON nordic_documents(extraction_status)
        """)
        
        # Index for priority-based processing
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_nordic_documents_extraction_priority 
            ON nordic_documents(extraction_priority)
        """)
        
        # Composite index for efficient extraction queue queries
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_nordic_documents_extraction_queue 
            ON nordic_documents(processing_status, extraction_status, extraction_priority)
        """)
        
        # Index for version-based reprocessing
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_nordic_documents_extraction_version 
            ON nordic_documents(extraction_version)
        """)
        
        # Index for performance monitoring
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_nordic_documents_extracted_at 
            ON nordic_documents(extracted_at)
        """)
        
        print("✅ All extraction indexes created successfully!")
        
    except Exception as e:
        print(f"❌ Error creating indexes: {e}")
        raise
    
    finally:
        await conn.close()

async def set_initial_extraction_priorities():
    """Set initial extraction priorities based on document types"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Setting initial extraction priorities...")
        
        # Priority 1: Annual reports (highest priority)
        result1 = await conn.execute("""
            UPDATE nordic_documents 
            SET extraction_priority = 1 
            WHERE document_type LIKE '%annual%' OR document_type LIKE '%årsredovisning%'
        """)
        
        # Priority 2: Quarterly reports  
        result2 = await conn.execute("""
            UPDATE nordic_documents 
            SET extraction_priority = 2 
            WHERE document_type LIKE '%quarterly%' OR document_type LIKE '%kvartalsrapport%'
            OR document_type LIKE '%q1%' OR document_type LIKE '%q2%' 
            OR document_type LIKE '%q3%' OR document_type LIKE '%q4%'
        """)
        
        # Priority 3: Interim reports
        result3 = await conn.execute("""
            UPDATE nordic_documents 
            SET extraction_priority = 3 
            WHERE document_type LIKE '%interim%' OR document_type LIKE '%delårsrapport%'
            OR document_type LIKE '%halvårsrapport%'
        """)
        
        # Priority 7: Press releases, governance (lower priority)
        result4 = await conn.execute("""
            UPDATE nordic_documents 
            SET extraction_priority = 7 
            WHERE document_type LIKE '%press%' OR document_type LIKE '%pressmeddelande%'
            OR document_type LIKE '%governance%' OR document_type LIKE '%bolagsstyrning%'
        """)
        
        print(f"✅ Priorities set: P1={result1}, P2={result2}, P3={result3}, P7={result4}")
        
    except Exception as e:
        print(f"❌ Error setting priorities: {e}")
        raise
    
    finally:
        await conn.close()

async def show_extraction_schema():
    """Show the updated schema with extraction columns"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("\n📊 Updated nordic_documents Schema (Extraction Columns):")
        print("=" * 60)
        
        # Get extraction-related columns
        columns = await conn.fetch("""
            SELECT column_name, data_type, column_default, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'nordic_documents'
            AND column_name LIKE '%extraction%'
            ORDER BY ordinal_position
        """)
        
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
            print(f"  {col['column_name']}: {col['data_type']} {nullable}{default}")
        
        # Show related columns
        other_cols = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'nordic_documents'
            AND column_name IN ('filing_id', 'text_length', 'content_analysis')
            ORDER BY column_name
        """)
        
        if other_cols:
            print("\n📋 Related Columns:")
            for col in other_cols:
                print(f"  {col['column_name']}: {col['data_type']}")
        
    except Exception as e:
        print(f"❌ Error showing schema: {e}")
    
    finally:
        await conn.close()

async def show_extraction_stats():
    """Show current extraction status statistics"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("\n📈 Current Extraction Status:")
        print("=" * 40)
        
        # Overall counts
        stats = await conn.fetch("""
            SELECT 
                processing_status,
                extraction_status,
                COUNT(*) as count
            FROM nordic_documents 
            GROUP BY processing_status, extraction_status
            ORDER BY processing_status, extraction_status
        """)
        
        for stat in stats:
            print(f"  {stat['processing_status']} + {stat['extraction_status']}: {stat['count']:,}")
        
        # Priority distribution for downloaded documents
        print("\n📋 Extraction Priorities (Downloaded Documents):")
        priorities = await conn.fetch("""
            SELECT 
                extraction_priority,
                COUNT(*) as count
            FROM nordic_documents 
            WHERE processing_status = 'downloaded'
            GROUP BY extraction_priority
            ORDER BY extraction_priority
        """)
        
        for priority in priorities:
            print(f"  Priority {priority['extraction_priority']}: {priority['count']:,} documents")
        
    except Exception as e:
        print(f"❌ Error showing stats: {e}")
    
    finally:
        await conn.close()

async def main():
    """Run the complete migration"""
    print("🏗️  Nordic Documents Extraction Tracking Migration")
    print("=" * 60)
    
    try:
        # Step 1: Add columns
        await add_extraction_tracking_columns()
        
        # Step 2: Add constraints
        await add_extraction_constraints()
        
        # Step 3: Create indexes
        await create_extraction_indexes()
        
        # Step 4: Set initial priorities
        await set_initial_extraction_priorities()
        
        # Step 5: Show results
        await show_extraction_schema()
        await show_extraction_stats()
        
        print("\n🎯 Migration Complete!")
        print("\nNext steps:")
        print("1. Update DocumentDiscoveryService to use nordic_documents table")
        print("2. Modify StatefulProcessingController to work with extraction_status")
        print("3. Remove document_processing_state table (no longer needed)")
        print("4. Test extraction pipeline with new schema")
        
        print("\n📖 Example Queries:")
        print("# Find documents ready for extraction:")
        print("SELECT * FROM nordic_documents")
        print("WHERE processing_status = 'downloaded' AND extraction_status = 'pending'")
        print("ORDER BY extraction_priority, year DESC;")
        
        print("\n# Check extraction progress:")
        print("SELECT extraction_status, COUNT(*), ")
        print("       COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage")
        print("FROM nordic_documents WHERE processing_status = 'downloaded'")
        print("GROUP BY extraction_status;")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())