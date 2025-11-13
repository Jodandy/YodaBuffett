#!/usr/bin/env python3
"""
Setup pgvector extension and vector embedding tables

This migration sets up:
1. pgvector extension
2. document_embeddings table for storing vector embeddings
3. embedding_batches table for tracking processing
4. Required indexes for efficient vector search
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Direct database URL
import os
def get_database_url():
    return os.getenv('DATABASE_URL') or 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

async def setup_pgvector():
    """Setup pgvector extension and related tables"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Setting up pgvector extension and vector tables...")
        
        # Step 1: Install pgvector extension
        print("📦 Installing pgvector extension...")
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            print("✅ pgvector extension installed successfully")
        except Exception as e:
            if "could not open extension control file" in str(e):
                print("❌ pgvector extension not found on system")
                print("Please install pgvector with: sudo apt-get install postgresql-15-pgvector")
                print("Or for macOS: brew install pgvector")
                raise
            else:
                raise
        
        # Step 2: Create document_embeddings table
        print("📄 Creating document_embeddings table...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS document_embeddings (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                extracted_document_id UUID NOT NULL,
                chunk_index INTEGER NOT NULL,
                embedding vector(1536) NOT NULL,
                chunk_text TEXT NOT NULL,
                chunk_metadata JSONB,
                embedding_model VARCHAR(100) DEFAULT 'text-embedding-3-small',
                embedding_version VARCHAR(50) DEFAULT 'v1.0',
                created_at TIMESTAMP DEFAULT NOW(),
                
                -- Foreign key to extracted_documents
                CONSTRAINT fk_document_embeddings_extracted_document
                    FOREIGN KEY (extracted_document_id) 
                    REFERENCES extracted_documents(id) 
                    ON DELETE CASCADE,
                    
                -- Ensure unique embeddings per chunk
                UNIQUE(extracted_document_id, chunk_index, embedding_version)
            );
        """)
        print("✅ document_embeddings table created")
        
        # Step 3: Create embedding_batches table for tracking processing
        print("📊 Creating embedding_batches table...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS embedding_batches (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                batch_name VARCHAR(200) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed
                documents_processed INTEGER DEFAULT 0,
                total_documents INTEGER DEFAULT 0,
                total_chunks INTEGER DEFAULT 0,
                embedding_model VARCHAR(100) NOT NULL,
                embedding_version VARCHAR(50) NOT NULL,
                cost_usd DECIMAL(10,4) DEFAULT 0.00,
                processing_start TIMESTAMP,
                processing_end TIMESTAMP,
                error_message TEXT,
                metadata JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        print("✅ embedding_batches table created")
        
        # Step 4: Create indexes for efficient vector search
        print("🔍 Creating vector search indexes...")
        
        # Index for exact document lookups
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_document_embeddings_document_id 
            ON document_embeddings(extracted_document_id);
        """)
        
        # Index for chunk lookups
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_document_embeddings_chunk 
            ON document_embeddings(extracted_document_id, chunk_index);
        """)
        
        # Index for embedding version filtering
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_document_embeddings_version 
            ON document_embeddings(embedding_version);
        """)
        
        # HNSW index for vector similarity search (most important!)
        print("🚀 Creating HNSW vector similarity index...")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_document_embeddings_vector_hnsw 
            ON document_embeddings 
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64);
        """)
        
        # IVFFlat index as alternative for large datasets
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_document_embeddings_vector_ivf 
            ON document_embeddings 
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
        """)
        
        print("✅ Vector search indexes created")
        
        # Step 5: Create helper views for analytics
        print("📈 Creating analytics views...")
        await conn.execute("""
            CREATE OR REPLACE VIEW embedding_statistics AS
            SELECT 
                eb.batch_name,
                eb.status,
                eb.documents_processed,
                eb.total_documents,
                eb.total_chunks,
                eb.cost_usd,
                eb.embedding_model,
                eb.embedding_version,
                EXTRACT(EPOCH FROM (eb.processing_end - eb.processing_start)) / 60 as processing_minutes,
                eb.created_at
            FROM embedding_batches eb
            ORDER BY eb.created_at DESC;
        """)
        
        await conn.execute("""
            CREATE OR REPLACE VIEW document_embedding_coverage AS
            SELECT 
                ed.company_name,
                ed.country,
                ed.form_type,
                ed.year,
                COUNT(DISTINCT de.extracted_document_id) as documents_with_embeddings,
                COUNT(de.id) as total_chunks,
                AVG(LENGTH(de.chunk_text)) as avg_chunk_length,
                MAX(de.created_at) as last_embedded
            FROM extracted_documents ed
            LEFT JOIN document_embeddings de ON ed.id = de.extracted_document_id
            GROUP BY ed.company_name, ed.country, ed.form_type, ed.year
            ORDER BY ed.company_name, ed.year DESC;
        """)
        
        print("✅ Analytics views created")
        
        print("\n🎉 pgvector setup completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during pgvector setup: {e}")
        raise
    
    finally:
        await conn.close()

async def verify_setup():
    """Verify pgvector setup is working"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("\n🔍 Verifying pgvector setup...")
        
        # Check extension
        ext_exists = await conn.fetchval("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        if ext_exists:
            print("✅ pgvector extension installed")
        else:
            print("❌ pgvector extension not found")
            return False
        
        # Check tables
        tables = ['document_embeddings', 'embedding_batches']
        for table in tables:
            exists = await conn.fetchval(f"""
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = '{table}'
            """)
            if exists:
                print(f"✅ {table} table exists")
            else:
                print(f"❌ {table} table missing")
                return False
        
        # Test vector operations
        print("🧪 Testing vector operations...")
        try:
            # Create a test embedding
            test_vector = [0.1] * 1536  # OpenAI embedding dimension
            result = await conn.fetchval("""
                SELECT '[0.1,0.2,0.3]'::vector <-> '[0.1,0.2,0.4]'::vector
            """)
            print(f"✅ Vector distance calculation works: {result}")
            
            # Test vector similarity
            similarity = await conn.fetchval("""
                SELECT 1 - ('[0.1,0.2,0.3]'::vector <=> '[0.1,0.2,0.3]'::vector)
            """)
            print(f"✅ Vector similarity calculation works: {similarity}")
            
        except Exception as e:
            print(f"❌ Vector operations failed: {e}")
            return False
        
        # Show table info
        print("\n📊 Setup Summary:")
        embedding_count = await conn.fetchval("SELECT COUNT(*) FROM document_embeddings")
        batch_count = await conn.fetchval("SELECT COUNT(*) FROM embedding_batches")
        
        print(f"📄 Document embeddings: {embedding_count}")
        print(f"📦 Embedding batches: {batch_count}")
        
        print("\n🚀 Ready to generate embeddings for 109,237 Nordic documents!")
        return True
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False
    
    finally:
        await conn.close()

async def main():
    """Run the complete pgvector setup"""
    print("🔄 PGVECTOR SETUP FOR YODABUFFETT")
    print("=" * 50)
    
    try:
        # Step 1: Setup pgvector
        await setup_pgvector()
        
        # Step 2: Verify setup
        success = await verify_setup()
        
        if success:
            print("\n🎯 Setup Complete!")
            print("\nNext steps:")
            print("1. Generate embeddings for extracted documents")
            print("2. Implement semantic search APIs")
            print("3. Build cross-document analytics")
        else:
            print("\n❌ Setup verification failed")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())