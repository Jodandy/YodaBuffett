#!/usr/bin/env python3
"""
Section-Based Embedding Service

Generates vector embeddings for intelligently parsed financial sections
rather than mechanical chunks. Uses the FinancialSectionParser to identify 
complete financial statements and creates meaningful sectional embeddings.
"""

import asyncio
import logging
import json
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime

import asyncpg
from ..factory import get_database_url
from .financial_section_parser import FinancialSectionParser, FinancialSection, SectionType
from .embedding_service import EmbeddingService

logger = logging.getLogger(__name__)


class SectionEmbeddingService:
    """Service for generating section-based embeddings from parsed financial sections"""
    
    def __init__(self, api_key: str = None, model: str = "text-embedding-3-small"):
        self.embedding_service = EmbeddingService(api_key=api_key, model=model)
        self.section_parser = FinancialSectionParser()
        self.provider = "openai"
        self.model_full_name = f"{self.provider}/{model}"
        
    async def setup_section_embeddings_table(self):
        """Create the section_embeddings table if it doesn't exist"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS section_embeddings (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    extracted_document_id UUID NOT NULL REFERENCES extracted_documents(id) ON DELETE CASCADE,
                    section_type VARCHAR(50) NOT NULL,
                    section_title TEXT,
                    section_content TEXT NOT NULL,
                    section_start_pos INTEGER,
                    section_end_pos INTEGER,
                    section_confidence FLOAT,
                    embedding VECTOR(1536),  -- OpenAI text-embedding-3-small dimension
                    embedding_model VARCHAR(100),
                    embedding_version VARCHAR(20),
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(extracted_document_id, section_type, section_start_pos)
                );
                
                -- Indexes for efficient queries
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_document ON section_embeddings(extracted_document_id);
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_type ON section_embeddings(section_type);
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_confidence ON section_embeddings(section_confidence);
                
                -- Vector similarity index
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_vector 
                ON section_embeddings USING ivfflat (embedding vector_cosine_ops) 
                WITH (lists = 100);
            """)
            
            logger.info("✅ Section embeddings table and indexes created/verified")
            
        finally:
            await conn.close()
    
    async def get_documents_for_section_embedding(
        self, 
        limit: int = 10,
        company_filter: str = None
    ) -> List[Dict]:
        """Get documents that need section-based embedding"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [limit]
            
            if company_filter:
                where_clause = "WHERE ed.company_name ILIKE $2"
                params.append(f"%{company_filter}%")
            
            # Get documents that haven't been section-embedded yet
            query = f"""
                SELECT ed.id, ed.company_name, ed.form_type, ed.year, ed.extracted_text,
                       LENGTH(ed.extracted_text) as text_length
                FROM extracted_documents ed
                LEFT JOIN section_embeddings se ON ed.id = se.extracted_document_id
                {where_clause}
                GROUP BY ed.id, ed.company_name, ed.form_type, ed.year, ed.extracted_text
                HAVING COUNT(se.id) = 0  -- No section embeddings yet
                ORDER BY LENGTH(ed.extracted_text) ASC  -- Start with smaller documents
                LIMIT $1
            """
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def parse_and_embed_document(self, document_id: str, document_text: str, metadata: Dict) -> Dict:
        """Parse document into sections and generate embeddings for each section"""
        start_time = time.time()
        
        logger.info(f"🔄 Parsing and embedding document: {document_id}")
        logger.info(f"📄 {metadata.get('company_name')} - {metadata.get('form_type')} ({len(document_text):,} chars)")
        
        # Parse document into financial sections
        parsing_result = self.section_parser.parse_document(document_text, document_id)
        
        if not parsing_result['parsing_success']:
            logger.warning(f"⚠️ Failed to parse document {document_id} into sections")
            return {
                "success": False,
                "reason": "parsing_failed",
                "sections_found": 0
            }
        
        sections = parsing_result['sections']
        logger.info(f"📋 Found {len(sections)} meaningful sections")
        
        # Log section summary
        for section in sections:
            logger.info(f"   {section.section_type.value:20} {len(section.content):6,} chars (confidence: {section.confidence:.2f})")
        
        # Generate embeddings for each section
        section_embeddings = []
        total_cost_estimate = 0.0
        
        for i, section in enumerate(sections):
            logger.info(f"🔄 Generating embedding for section {i+1}/{len(sections)}: {section.section_type.value}")
            
            try:
                # Generate embedding for the complete section
                embeddings = await self.embedding_service.generate_embeddings_batch([section.content])
                embedding = embeddings[0]
                
                section_embeddings.append({
                    'section': section,
                    'embedding': embedding
                })
                
                # Estimate cost (rough: $0.00002 per 1K tokens)
                section_tokens = len(section.content) // 4  # Rough token estimate
                section_cost = (section_tokens / 1000) * 0.00002
                total_cost_estimate += section_cost
                
                # Small rate limiting delay
                if i + 1 < len(sections):
                    await asyncio.sleep(0.2)
                    
            except Exception as e:
                logger.error(f"❌ Failed to generate embedding for section {section.section_type.value}: {e}")
                continue
        
        # Store section embeddings in database
        stored_count = await self.store_section_embeddings(document_id, section_embeddings)
        
        processing_time = time.time() - start_time
        
        # Extract financial statements for summary
        financial_statements = self.section_parser.extract_financial_statements(sections)
        
        result = {
            "success": True,
            "document_id": document_id,
            "sections_parsed": len(sections),
            "sections_embedded": stored_count,
            "financial_statements_found": list(financial_statements.keys()),
            "processing_time_seconds": round(processing_time, 2),
            "estimated_cost_usd": round(total_cost_estimate, 6),
            "parsing_details": parsing_result['section_summary']
        }
        
        logger.info(f"✅ Completed section embedding for {document_id}: {stored_count} sections in {processing_time:.1f}s (${total_cost_estimate:.4f})")
        
        return result
    
    async def store_section_embeddings(
        self,
        document_id: str,
        section_embeddings: List[Dict]
    ) -> int:
        """Store section embeddings in database"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            stored_count = 0
            
            for item in section_embeddings:
                section = item['section']
                embedding = item['embedding']
                
                # Convert embedding to string format for pgvector
                embedding_str = str(embedding)
                
                await conn.execute("""
                    INSERT INTO section_embeddings (
                        extracted_document_id, section_type, section_title, section_content,
                        section_start_pos, section_end_pos, section_confidence,
                        embedding, embedding_model, embedding_version, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (extracted_document_id, section_type, section_start_pos) 
                    DO NOTHING
                """, 
                    document_id,
                    section.section_type.value,
                    section.title,
                    section.content,
                    section.start_pos,
                    section.end_pos,
                    section.confidence,
                    embedding_str,
                    self.model_full_name,
                    "v1.0",
                    datetime.now()
                )
                stored_count += 1
            
            logger.info(f"💾 Stored {stored_count} section embeddings for document {document_id}")
            return stored_count
            
        finally:
            await conn.close()
    
    async def process_documents_batch(
        self,
        max_documents: int = 5,
        company_filter: str = None
    ) -> Dict:
        """Process a batch of documents for section-based embedding"""
        batch_start = time.time()
        
        logger.info(f"🚀 Starting section embedding batch: max {max_documents} documents")
        
        # Ensure database table exists
        await self.setup_section_embeddings_table()
        
        # Get documents to process
        documents = await self.get_documents_for_section_embedding(
            limit=max_documents,
            company_filter=company_filter
        )
        
        if not documents:
            logger.info("📭 No documents need section embedding")
            return {"success": True, "message": "No documents to process"}
        
        logger.info(f"📋 Processing {len(documents)} documents for section embedding")
        
        results = []
        total_cost = 0.0
        total_sections = 0
        
        for i, doc in enumerate(documents, 1):
            logger.info(f"📄 Document {i}/{len(documents)}: {doc['company_name']} - {doc['form_type']}")
            
            try:
                metadata = {
                    'company_name': doc['company_name'],
                    'form_type': doc['form_type'],
                    'year': doc['year']
                }
                
                result = await self.parse_and_embed_document(
                    doc['id'], 
                    doc['extracted_text'], 
                    metadata
                )
                results.append(result)
                
                if result['success']:
                    total_cost += result['estimated_cost_usd']
                    total_sections += result['sections_embedded']
                
            except Exception as e:
                logger.error(f"❌ Failed to process {doc['id']}: {e}")
                results.append({
                    "success": False,
                    "document_id": doc['id'],
                    "error": str(e)
                })
        
        batch_time = time.time() - batch_start
        
        summary = {
            "success": True,
            "batch_summary": {
                "documents_attempted": len(documents),
                "documents_successful": sum(1 for r in results if r['success']),
                "total_sections_embedded": total_sections,
                "total_cost_usd": round(total_cost, 4),
                "batch_time_seconds": round(batch_time, 2),
                "avg_time_per_document": round(batch_time / len(documents), 2)
            },
            "detailed_results": results
        }
        
        logger.info(f"🎉 Section embedding batch complete: {summary['batch_summary']['documents_successful']}/{len(documents)} documents, {total_sections:,} sections, ${total_cost:.4f}")
        
        return summary
    
    async def get_section_embedding_statistics(self) -> Dict:
        """Get current section embedding status and statistics"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Overall section embedding status
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(DISTINCT ed.id) as total_documents,
                    COUNT(DISTINCT se.extracted_document_id) as documents_with_section_embeddings,
                    COUNT(se.id) as total_section_embeddings,
                    AVG(se.section_confidence) as avg_section_confidence,
                    AVG(LENGTH(se.section_content)) as avg_section_length,
                    MIN(se.created_at) as first_section_embedding,
                    MAX(se.created_at) as last_section_embedding
                FROM extracted_documents ed
                LEFT JOIN section_embeddings se ON ed.id = se.extracted_document_id
            """)
            
            # Section embeddings by type
            by_section_type = await conn.fetch("""
                SELECT section_type, COUNT(*) as count,
                       AVG(section_confidence) as avg_confidence,
                       AVG(LENGTH(section_content)) as avg_length
                FROM section_embeddings
                GROUP BY section_type
                ORDER BY count DESC
            """)
            
            # Financial statement coverage
            financial_statements = await conn.fetch("""
                SELECT 
                    COUNT(DISTINCT extracted_document_id) as documents_with_balance_sheet
                FROM section_embeddings 
                WHERE section_type = 'balance_sheet'
                UNION ALL
                SELECT COUNT(DISTINCT extracted_document_id) FROM section_embeddings WHERE section_type = 'income_statement'
                UNION ALL
                SELECT COUNT(DISTINCT extracted_document_id) FROM section_embeddings WHERE section_type = 'cash_flow'
                UNION ALL
                SELECT COUNT(DISTINCT extracted_document_id) FROM section_embeddings WHERE section_type = 'equity_statement'
            """)
            
            # Documents needing section embedding
            pending_section = await conn.fetch("""
                SELECT ed.company_name, ed.form_type, ed.year,
                       LENGTH(ed.extracted_text) as text_length
                FROM extracted_documents ed
                LEFT JOIN section_embeddings se ON ed.id = se.extracted_document_id
                GROUP BY ed.id, ed.company_name, ed.form_type, ed.year, ed.extracted_text
                HAVING COUNT(se.id) = 0
                ORDER BY LENGTH(ed.extracted_text) ASC
                LIMIT 10
            """)
            
            return {
                "overall": dict(stats) if stats else {},
                "by_section_type": [dict(row) for row in by_section_type],
                "financial_statement_counts": [dict(row) for row in financial_statements],
                "pending_documents": [dict(row) for row in pending_section]
            }
            
        finally:
            await conn.close()
    
    async def find_similar_sections(
        self, 
        query_text: str, 
        section_type: str = None,
        limit: int = 5,
        similarity_threshold: float = 0.7
    ) -> List[Dict]:
        """Find similar financial sections using vector similarity"""
        
        # Generate embedding for query
        query_embeddings = await self.embedding_service.generate_embeddings_batch([query_text])
        query_embedding = query_embeddings[0]
        query_embedding_str = str(query_embedding)
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [query_embedding_str, limit]
            
            if section_type:
                where_clause = "WHERE se.section_type = $3"
                params.append(section_type)
            
            query = f"""
                SELECT 
                    se.section_type, se.section_title, se.section_confidence,
                    se.section_content,
                    ed.company_name, ed.form_type, ed.year,
                    1 - (se.embedding <=> $1::vector) as similarity_score
                FROM section_embeddings se
                JOIN extracted_documents ed ON se.extracted_document_id = ed.id
                {where_clause}
                ORDER BY se.embedding <=> $1::vector
                LIMIT $2
            """
            
            # Adjust parameter indices if we have a where clause
            if section_type:
                # We have 3 params: embedding, limit, section_type
                pass
            
            similar_sections = await conn.fetch(query, *params)
            
            # Filter by similarity threshold
            results = []
            for row in similar_sections:
                row_dict = dict(row)
                if row_dict['similarity_score'] >= similarity_threshold:
                    # Truncate content for preview
                    row_dict['content_preview'] = row_dict['section_content'][:300] + "..."
                    results.append(row_dict)
            
            return results
            
        finally:
            await conn.close()


if __name__ == "__main__":
    # For testing
    async def test_section_embedding_service():
        import os
        
        service = SectionEmbeddingService()
        
        print("🔧 Setting up section embeddings database...")
        try:
            await service.setup_section_embeddings_table()
            print("✅ Database setup complete")
        except Exception as e:
            print(f"❌ Database setup failed: {e}")
            return
        
        # Get status
        try:
            stats = await service.get_section_embedding_statistics()
            print("📊 Section Embedding Status:")
            
            overall = stats.get('overall', {})
            total_docs = overall.get('total_documents', 0)
            section_docs = overall.get('documents_with_section_embeddings', 0)
            total_sections = overall.get('total_section_embeddings', 0)
            
            print(f"   📄 Total documents: {total_docs:,}")
            print(f"   🧩 Documents with section embeddings: {section_docs:,}")
            print(f"   📋 Total section embeddings: {total_sections:,}")
            
            if total_docs > 0:
                completion_rate = (section_docs / total_docs) * 100
                print(f"   📊 Completion rate: {completion_rate:.1f}%")
            
            # Show section types
            section_types = stats.get('by_section_type', [])
            if section_types:
                print("\n📋 Section Types:")
                for section in section_types[:5]:  # Top 5
                    print(f"   {section['section_type']:20} {section['count']:5,} sections")
            
            # Show pending documents
            pending = stats.get('pending_documents', [])
            if pending:
                print(f"\n📝 Documents ready for section embedding: {len(pending)}")
                for doc in pending[:3]:  # Show first 3
                    print(f"   {doc['company_name']:25} {doc['form_type']:15} ({doc['text_length']:,} chars)")
            
        except Exception as e:
            print(f"❌ Failed to get status: {e}")
        
        print("\n🎉 Section embedding service test complete!")
        print("💡 Use the CLI tool to process documents: python cli_section_embeddings.py")
    
    asyncio.run(test_section_embedding_service())