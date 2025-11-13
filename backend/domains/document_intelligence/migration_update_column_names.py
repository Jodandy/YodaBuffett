#!/usr/bin/env python3
"""
Update column names from filing_id to extracted_document_id

This migration updates the remaining column references to use
the clearer "extracted_document_id" naming.
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from domains.document_intelligence.factory import get_database_url

async def update_column_names():
    """Update filing_id columns to extracted_document_id"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Updating column names from filing_id to extracted_document_id...")
        
        # Step 1: Update nordic_documents.filing_id → extracted_document_id
        await conn.execute("""
            ALTER TABLE nordic_documents 
            RENAME COLUMN filing_id TO extracted_document_id;
        """)
        print("✅ Updated nordic_documents.filing_id → extracted_document_id")
        
        # Step 2: Update document_chunks.filing_id → extracted_document_id
        await conn.execute("""
            ALTER TABLE document_chunks 
            RENAME COLUMN filing_id TO extracted_document_id;
        """)
        print("✅ Updated document_chunks.filing_id → extracted_document_id")
        
        # Step 3: Rename document_chunks table to extracted_document_chunks for consistency
        await conn.execute("""
            ALTER TABLE document_chunks RENAME TO extracted_document_chunks;
        """)
        print("✅ Renamed document_chunks → extracted_document_chunks")
        
        print("\n🎉 Successfully updated all column names!")
        
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        raise
    
    finally:
        await conn.close()

async def verify_updates():
    """Verify the updates were successful"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("\n🔍 Verifying column updates...")
        
        # Check nordic_documents column
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'nordic_documents' 
                AND column_name = 'extracted_document_id'
            );
        """)
        
        if column_exists:
            print("✅ nordic_documents.extracted_document_id exists")
        else:
            print("❌ nordic_documents.extracted_document_id not found")
        
        # Check chunks table exists  
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'extracted_document_chunks'
            );
        """)
        
        if table_exists:
            print("✅ extracted_document_chunks table exists")
            
            # Check column in chunks table
            chunk_column = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns 
                    WHERE table_name = 'extracted_document_chunks' 
                    AND column_name = 'extracted_document_id'
                );
            """)
            
            if chunk_column:
                print("✅ extracted_document_chunks.extracted_document_id exists")
            else:
                print("❌ extracted_document_chunks.extracted_document_id not found")
                
        else:
            print("❌ extracted_document_chunks table not found")
        
        # Check that old filing_id columns don't exist
        old_nordic = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'nordic_documents' 
                AND column_name = 'filing_id'
            );
        """)
        
        if not old_nordic:
            print("✅ Old nordic_documents.filing_id column removed")
        else:
            print("⚠️  Old nordic_documents.filing_id column still exists")
        
        print("\n📋 Updated Schema:")
        print("  📄 extracted_documents (text content)")
        print("  📝 extracted_document_chunks (text chunks)")
        print("  🔗 nordic_documents.extracted_document_id → extracted_documents.id")
        print("  🔗 extracted_document_chunks.extracted_document_id → extracted_documents.id")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
    
    finally:
        await conn.close()

async def main():
    """Run the complete migration"""
    print("🔄 UPDATING COLUMN NAMES TO EXTRACTED_DOCUMENT_ID")
    print("=" * 55)
    
    try:
        # Step 1: Update column names
        await update_column_names()
        
        # Step 2: Verify updates
        await verify_updates()
        
        print("\n🎯 Migration Complete!")
        print("\nNext steps:")
        print("1. Update code references from 'filing_id' to 'extracted_document_id'")
        print("2. Update repository classes to use new table/column names")
        print("3. Update any queries in services")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())