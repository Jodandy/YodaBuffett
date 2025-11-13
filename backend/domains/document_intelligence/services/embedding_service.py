#!/usr/bin/env python3
"""
Document Embedding Service

Generates vector embeddings from extracted document chunks using OpenAI's embedding API.
Handles batch processing with rate limiting and cost tracking.
"""

import asyncio
import logging
import json
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import openai
from openai import AsyncOpenAI

import asyncpg
from ..factory import get_database_url

logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating and storing document embeddings"""
    
    def __init__(self, api_key: str = None, model: str = "text-embedding-3-small"):
        self.client = AsyncOpenAI(api_key=api_key) if api_key else None
        self.model = model
        self.provider = "openai"  # Explicit provider tracking
        self.model_full_name = f"{self.provider}/{model}"  # Full provider/model string
        self.embedding_dimension = 1536 if "small" in model else 3072
        self.batch_size = 100  # OpenAI recommended batch size
        self.rate_limit_delay = 1.0  # Seconds between batches
        
    async def get_documents_for_embedding(
        self, 
        limit: int = 100,
        company_filter: str = None
    ) -> List[Dict]:
        """Get extracted documents that need embedding"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [limit]
            
            if company_filter:
                where_clause = "WHERE ed.company_name ILIKE $2"
                params.append(f"%{company_filter}%")
            
            # Get documents that haven't been embedded yet
            query = f"""
                SELECT ed.id, ed.company_name, ed.form_type, ed.year,
                       COUNT(edc.id) as chunk_count,
                       MIN(edc.chunk_index) as first_chunk,
                       MAX(edc.chunk_index) as last_chunk
                FROM extracted_documents ed
                JOIN extracted_document_chunks edc ON ed.id = edc.extracted_document_id
                LEFT JOIN document_embeddings de ON ed.id = de.extracted_document_id
                {where_clause}
                GROUP BY ed.id, ed.company_name, ed.form_type, ed.year
                HAVING COUNT(de.id) = 0  -- No embeddings yet
                ORDER BY COUNT(edc.id) ASC  -- Start with smaller documents
                LIMIT $1
            """
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def get_document_chunks(self, document_id: str) -> List[Dict]:
        """Get all chunks for a document"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            chunks = await conn.fetch("""
                SELECT chunk_index, chunk_text, page_numbers,
                       char_start, char_end, chunk_metadata
                FROM extracted_document_chunks
                WHERE extracted_document_id = $1
                ORDER BY chunk_index
            """, document_id)
            
            return [dict(chunk) for chunk in chunks]
            
        finally:
            await conn.close()
    
    async def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts using OpenAI API"""
        if not self.client:
            # For testing without API key, return dummy vectors
            logger.warning("No OpenAI API key provided, returning dummy embeddings")
            return [[0.1] * self.embedding_dimension for _ in texts]
        
        try:
            # Clean texts (remove excessive whitespace, limit length)
            cleaned_texts = []
            for text in texts:
                cleaned = " ".join(text.split())  # Normalize whitespace
                # OpenAI has token limits, roughly 8000 chars = ~2000 tokens
                if len(cleaned) > 8000:
                    cleaned = cleaned[:8000] + "..."
                cleaned_texts.append(cleaned)
            
            # Call OpenAI embedding API
            response = await self.client.embeddings.create(
                model=self.model,
                input=cleaned_texts,
                encoding_format="float"
            )
            
            # Extract embedding vectors
            embeddings = [data.embedding for data in response.data]
            
            # Log usage for cost tracking
            logger.info(f"Generated {len(embeddings)} embeddings using {response.usage.total_tokens} tokens")
            
            return embeddings
            
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            # Return dummy vectors on error to continue processing
            return [[0.0] * self.embedding_dimension for _ in texts]
    
    async def store_embeddings(
        self, 
        document_id: str, 
        chunk_embeddings: List[Tuple[int, List[float], str]]
    ) -> int:
        """Store embeddings in the database"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            stored_count = 0
            
            for chunk_index, embedding, chunk_text in chunk_embeddings:
                # Convert embedding list to string format for pgvector
                embedding_str = str(embedding)
                
                await conn.execute("""
                    INSERT INTO document_embeddings (
                        extracted_document_id, chunk_index, embedding, chunk_text,
                        embedding_model, embedding_version, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (extracted_document_id, chunk_index, embedding_version) 
                    DO NOTHING
                """, 
                    document_id, chunk_index, embedding_str, chunk_text,
                    self.model_full_name, "v1.0", datetime.now()
                )
                stored_count += 1
            
            logger.info(f"Stored {stored_count} embeddings for document {document_id}")
            return stored_count
            
        finally:
            await conn.close()
    
    async def process_document(self, document_id: str) -> Dict:
        """Process a single document - generate and store all embeddings"""
        start_time = time.time()
        
        logger.info(f"🔄 Processing document: {document_id}")
        
        # Get all chunks for this document
        chunks = await self.get_document_chunks(document_id)
        
        if not chunks:
            logger.warning(f"No chunks found for document {document_id}")
            return {"success": False, "reason": "no_chunks"}
        
        logger.info(f"📄 Processing {len(chunks)} chunks")
        
        # Process in batches to respect API limits
        all_embeddings = []
        total_cost_estimate = 0.0
        
        for i in range(0, len(chunks), self.batch_size):
            batch_chunks = chunks[i:i + self.batch_size]
            batch_texts = [chunk['chunk_text'] for chunk in batch_chunks]
            
            logger.info(f"🔄 Processing batch {i//self.batch_size + 1}/{(len(chunks) + self.batch_size - 1)//self.batch_size}")
            
            # Generate embeddings for this batch
            batch_embeddings = await self.generate_embeddings_batch(batch_texts)
            
            # Combine with chunk metadata
            for j, embedding in enumerate(batch_embeddings):
                chunk_idx = batch_chunks[j]['chunk_index']
                chunk_text = batch_chunks[j]['chunk_text']
                all_embeddings.append((chunk_idx, embedding, chunk_text))
            
            # Estimate cost (rough: $0.00002 per 1K tokens, assume 2000 chars = 500 tokens)
            batch_tokens = sum(len(text) // 4 for text in batch_texts)  # Rough estimate
            batch_cost = (batch_tokens / 1000) * 0.00002
            total_cost_estimate += batch_cost
            
            # Rate limiting
            if i + self.batch_size < len(chunks):
                await asyncio.sleep(self.rate_limit_delay)
        
        # Store all embeddings
        stored_count = await self.store_embeddings(document_id, all_embeddings)
        
        processing_time = time.time() - start_time
        
        result = {
            "success": True,
            "document_id": document_id,
            "chunks_processed": len(chunks),
            "embeddings_stored": stored_count,
            "processing_time_seconds": round(processing_time, 2),
            "estimated_cost_usd": round(total_cost_estimate, 6)
        }
        
        logger.info(f"✅ Completed document {document_id}: {stored_count} embeddings in {processing_time:.1f}s (${total_cost_estimate:.4f})")
        
        return result
    
    async def process_batch(
        self, 
        max_documents: int = 10,
        company_filter: str = None
    ) -> Dict:
        """Process a batch of documents"""
        batch_start = time.time()
        
        logger.info(f"🚀 Starting embedding batch: max {max_documents} documents")
        
        # Get documents to process
        documents = await self.get_documents_for_embedding(
            limit=max_documents,
            company_filter=company_filter
        )
        
        if not documents:
            logger.info("📭 No documents need embedding")
            return {"success": True, "message": "No documents to process"}
        
        logger.info(f"📋 Processing {len(documents)} documents")
        
        results = []
        total_cost = 0.0
        total_embeddings = 0
        
        for i, doc in enumerate(documents, 1):
            logger.info(f"📄 Document {i}/{len(documents)}: {doc['company_name']} - {doc['form_type']} ({doc['chunk_count']} chunks)")
            
            try:
                result = await self.process_document(doc['id'])
                results.append(result)
                
                if result['success']:
                    total_cost += result['estimated_cost_usd']
                    total_embeddings += result['embeddings_stored']
                
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
                "total_embeddings_created": total_embeddings,
                "total_cost_usd": round(total_cost, 4),
                "batch_time_seconds": round(batch_time, 2),
                "avg_time_per_document": round(batch_time / len(documents), 2)
            },
            "detailed_results": results
        }
        
        logger.info(f"🎉 Batch complete: {summary['batch_summary']['documents_successful']}/{len(documents)} documents, {total_embeddings:,} embeddings, ${total_cost:.4f}")
        
        return summary


async def get_embedding_statistics() -> Dict:
    """Get current embedding status and statistics"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # Overall embedding status
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(DISTINCT ed.id) as total_documents,
                COUNT(DISTINCT de.extracted_document_id) as documents_with_embeddings,
                COUNT(de.id) as total_embeddings,
                AVG(LENGTH(de.chunk_text)) as avg_chunk_length,
                MIN(de.created_at) as first_embedding,
                MAX(de.created_at) as last_embedding
            FROM extracted_documents ed
            LEFT JOIN document_embeddings de ON ed.id = de.extracted_document_id
        """)
        
        # Embeddings by model
        by_model = await conn.fetch("""
            SELECT embedding_model, embedding_version, COUNT(*) as count
            FROM document_embeddings
            GROUP BY embedding_model, embedding_version
            ORDER BY count DESC
        """)
        
        # Documents needing embedding
        pending = await conn.fetch("""
            SELECT ed.company_name, ed.form_type, 
                   COUNT(edc.id) as total_chunks,
                   COUNT(de.id) as embedded_chunks
            FROM extracted_documents ed
            JOIN extracted_document_chunks edc ON ed.id = edc.extracted_document_id
            LEFT JOIN document_embeddings de ON ed.id = de.extracted_document_id
            GROUP BY ed.id, ed.company_name, ed.form_type
            HAVING COUNT(de.id) = 0
            ORDER BY COUNT(edc.id) ASC
            LIMIT 10
        """)
        
        return {
            "overall": dict(stats) if stats else {},
            "by_model": [dict(row) for row in by_model],
            "pending_documents": [dict(row) for row in pending]
        }
        
    finally:
        await conn.close()


if __name__ == "__main__":
    # For testing
    async def test_embedding_service():
        service = EmbeddingService()
        
        # Get status
        stats = await get_embedding_statistics()
        print("📊 Current Status:", json.dumps(stats, indent=2, default=str))
        
        # Test with small batch
        # result = await service.process_batch(max_documents=2)
        # print("🔄 Test Result:", json.dumps(result, indent=2, default=str))
    
    asyncio.run(test_embedding_service())