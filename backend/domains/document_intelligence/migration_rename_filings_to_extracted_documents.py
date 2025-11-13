#!/usr/bin/env python3
"""
Rename filings table to extracted_documents for better clarity

This migration renames the filings table and all related references
to use the clearer name "extracted_documents".
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from domains.document_intelligence.factory import get_database_url

async def rename_filings_to_extracted_documents():
    """Rename filings table and all related references"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Renaming filings table to extracted_documents...")
        
        # Step 1: Rename the main table
        await conn.execute("""
            ALTER TABLE filings RENAME TO extracted_documents;
        """)
        print("✅ Renamed filings → extracted_documents")
        
        # Step 2: Rename filing_chunks table
        await conn.execute("""
            ALTER TABLE filing_chunks RENAME TO extracted_document_chunks;
        """)
        print("✅ Renamed filing_chunks → extracted_document_chunks")
        
        # Step 3: Rename filing_id column in chunks table
        await conn.execute("""
            ALTER TABLE extracted_document_chunks 
            RENAME COLUMN filing_id TO extracted_document_id;
        """)
        print("✅ Renamed filing_id → extracted_document_id in chunks table")
        
        # Step 4: Rename filing_id column in nordic_documents
        await conn.execute("""
            ALTER TABLE nordic_documents 
            RENAME COLUMN filing_id TO extracted_document_id;
        """)
        print("✅ Renamed filing_id → extracted_document_id in nordic_documents")
        
        # Step 5: Update any indexes that reference the old names
        try:
            await conn.execute("""
                ALTER INDEX IF EXISTS idx_filings_region 
                RENAME TO idx_extracted_documents_region;
            """)
            print("✅ Renamed index: idx_filings_region → idx_extracted_documents_region")
        except:
            print("ℹ️  Index idx_filings_region not found (ok)")
        
        try:
            await conn.execute("""
                ALTER INDEX IF EXISTS idx_filing_chunks_filing_id 
                RENAME TO idx_extracted_document_chunks_document_id;
            """)
            print("✅ Renamed index: idx_filing_chunks_filing_id → idx_extracted_document_chunks_document_id")
        except:
            print("ℹ️  Index idx_filing_chunks_filing_id not found (ok)")
        
        # Step 6: Update any foreign key constraint names
        try:
            # Get current constraint names
            constraints = await conn.fetch("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name IN ('extracted_documents', 'extracted_document_chunks', 'nordic_documents')
                AND constraint_type = 'FOREIGN KEY'
                AND constraint_name LIKE '%filing%'
            """)
            
            for constraint in constraints:
                old_name = constraint['constraint_name']
                new_name = old_name.replace('filing', 'extracted_document')
                print(f"ℹ️  Found constraint to rename: {old_name} → {new_name}")
                
        except Exception as e:
            print(f"ℹ️  Could not check constraints: {e}")
        
        print("\n🎉 Successfully renamed filings to extracted_documents!")
        
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        raise
    
    finally:
        await conn.close()

async def verify_rename():
    """Verify the rename was successful"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("\n🔍 Verifying table rename...")
        
        # Check that extracted_documents exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'extracted_documents'
            );
        """)
        
        if exists:
            print("✅ extracted_documents table exists")
            
            # Count records
            count = await conn.fetchval("SELECT COUNT(*) FROM extracted_documents")
            print(f"📊 extracted_documents contains {count:,} records")
            
        else:
            print("❌ extracted_documents table not found")
        
        # Check that chunks table exists
        chunks_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'extracted_document_chunks'
            );
        """)
        
        if chunks_exists:
            print("✅ extracted_document_chunks table exists")
            
            # Count chunks
            chunk_count = await conn.fetchval("SELECT COUNT(*) FROM extracted_document_chunks")
            print(f"📊 extracted_document_chunks contains {chunk_count:,} records")
            
        else:
            print("❌ extracted_document_chunks table not found")
        
        # Check that old tables don't exist
        old_filings = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'filings'
            );
        """)
        
        if not old_filings:
            print("✅ Old 'filings' table successfully removed")
        else:
            print("⚠️  Old 'filings' table still exists")
        
        # Check nordic_documents column
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns 
                WHERE table_name = 'nordic_documents' 
                AND column_name = 'extracted_document_id'
            );
        """)
        
        if column_exists:
            print("✅ nordic_documents.extracted_document_id column exists")
        else:
            print("❌ nordic_documents.extracted_document_id column not found")
        
        print("\n📋 Updated Schema Summary:")
        print("  📄 extracted_documents (main text storage)")
        print("  📝 extracted_document_chunks (text chunks)")  
        print("  🔗 nordic_documents.extracted_document_id (foreign key)")
        
    except Exception as e:
        print(f"❌ Error during verification: {e}")
    
    finally:
        await conn.close()

async def main():
    """Run the complete migration"""
    print("🔄 RENAMING FILINGS TO EXTRACTED_DOCUMENTS")
    print("=" * 50)
    
    try:
        # Step 1: Rename tables
        await rename_filings_to_extracted_documents()
        
        # Step 2: Verify rename
        await verify_rename()
        
        print("\n🎯 Migration Complete!")
        print("\nNext steps:")
        print("1. Update code references from 'filings' to 'extracted_documents'")
        print("2. Update documentation")
        print("3. Update any application code that references the old table names")
        
        print(f"\n📖 Updated Data Flow:")
        print("nordic_documents.extracted_document_id → extracted_documents.id")
        print("extracted_documents.id ← extracted_document_chunks.extracted_document_id")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())