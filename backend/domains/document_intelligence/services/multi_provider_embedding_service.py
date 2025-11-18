#!/usr/bin/env python3
"""
Multi-Provider Embedding Service

Generates vector embeddings from stored document sections using different providers.
Can use OpenAI, Cohere, local models, etc. Works with sections created by 
SectionChunkingService.
"""

import asyncio
import logging
import json
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from abc import ABC, abstractmethod

import asyncpg
from ..factory import get_database_url

logger = logging.getLogger(__name__)


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers"""
    
    @abstractmethod
    async def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts"""
        pass
    
    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return the model identifier"""
        pass
    
    @property
    @abstractmethod
    def embedding_dimension(self) -> int:
        """Return the embedding dimension"""
        pass


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """OpenAI embedding provider"""
    
    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        from openai import AsyncOpenAI
        self.client = AsyncOpenAI(api_key=api_key)
        self.model = model
        self._embedding_dimension = 1536 if "small" in model else 3072
    
    async def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        try:
            # Clean texts
            cleaned_texts = []
            for text in texts:
                cleaned = " ".join(text.split())  # Normalize whitespace
                if len(cleaned) > 8000:
                    cleaned = cleaned[:8000] + "..."
                cleaned_texts.append(cleaned)
            
            # Call OpenAI API
            response = await self.client.embeddings.create(
                model=self.model,
                input=cleaned_texts,
                encoding_format="float"
            )
            
            embeddings = [data.embedding for data in response.data]
            logger.info(f"Generated {len(embeddings)} OpenAI embeddings using {response.usage.total_tokens} tokens")
            
            return embeddings
            
        except Exception as e:
            logger.error(f"OpenAI embedding error: {e}")
            # Return dummy vectors on error
            return [[0.0] * self.embedding_dimension for _ in texts]
    
    @property
    def model_name(self) -> str:
        return f"openai/{self.model}"
    
    @property
    def embedding_dimension(self) -> int:
        return self._embedding_dimension


class CohereEmbeddingProvider(EmbeddingProvider):
    """Cohere embedding provider (example)"""
    
    def __init__(self, api_key: str, model: str = "embed-english-v3.0"):
        # This would import cohere library
        # import cohere
        # self.client = cohere.AsyncClient(api_key)
        self.model = model
        self._embedding_dimension = 1024  # Cohere dimension
        logger.warning("Cohere provider not implemented - returning dummy embeddings")
    
    async def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        # Placeholder implementation
        logger.info(f"Would generate {len(texts)} Cohere embeddings")
        return [[0.1] * self.embedding_dimension for _ in texts]
    
    @property
    def model_name(self) -> str:
        return f"cohere/{self.model}"
    
    @property
    def embedding_dimension(self) -> int:
        return self._embedding_dimension


class LocalEmbeddingProvider(EmbeddingProvider):
    """Local embedding provider (example for sentence-transformers)"""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        # This would use sentence-transformers
        # from sentence_transformers import SentenceTransformer
        # self.model = SentenceTransformer(model_name)
        self.model_name_str = model_name
        self._embedding_dimension = 384  # MiniLM dimension
        logger.warning("Local provider not implemented - returning dummy embeddings")
    
    async def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        # Placeholder implementation
        logger.info(f"Would generate {len(texts)} local embeddings")
        return [[0.2] * self.embedding_dimension for _ in texts]
    
    @property
    def model_name(self) -> str:
        return f"local/{self.model_name_str}"
    
    @property
    def embedding_dimension(self) -> int:
        return self._embedding_dimension


class MultiProviderEmbeddingService:
    """Service for generating embeddings from stored sections using multiple providers"""
    
    def __init__(self, provider: EmbeddingProvider):
        self.provider = provider
        self.batch_size = 100  # Process sections in batches
        self.rate_limit_delay = 1.0  # Seconds between batches
    
    async def setup_embeddings_table(self):
        """Create the section_embeddings table if it doesn't exist"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Create table with flexible dimension support
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS section_embeddings (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    document_section_id UUID NOT NULL REFERENCES document_sections(id) ON DELETE CASCADE,
                    extracted_document_id UUID NOT NULL REFERENCES extracted_documents(id) ON DELETE CASCADE,
                    section_index INTEGER NOT NULL,
                    section_type VARCHAR(50) NOT NULL,
                    embedding VECTOR({self.provider.embedding_dimension}),
                    embedding_model VARCHAR(100) NOT NULL,
                    embedding_version VARCHAR(20) DEFAULT 'v1.0',
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(document_section_id, embedding_model)
                );
                
                -- Indexes for efficient queries
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_document ON section_embeddings(extracted_document_id);
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_section ON section_embeddings(document_section_id);
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_type ON section_embeddings(section_type);
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_model ON section_embeddings(embedding_model);
                
                -- Vector similarity index
                CREATE INDEX IF NOT EXISTS idx_section_embeddings_vector 
                ON section_embeddings USING ivfflat (embedding vector_cosine_ops) 
                WITH (lists = 100);
            """)
            
            logger.info(f"✅ Section embeddings table created for {self.provider.model_name} ({self.provider.embedding_dimension}D)")
            
        finally:
            await conn.close()
    
    async def get_sections_for_embedding(
        self, 
        limit: int = 50,
        company_filter: str = None
    ) -> List[Dict]:
        """Get document sections that need embeddings for this provider"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [self.provider.model_name, limit]
            
            if company_filter:
                where_clause = "AND ed.company_name ILIKE $3"
                params.append(f"%{company_filter}%")
            
            # Get sections that don't have embeddings for this provider yet
            query = f"""
                SELECT ds.id as section_id, ds.extracted_document_id, ds.section_index,
                       ds.section_type, ds.section_title, ds.section_content, ds.section_confidence,
                       ed.company_name, ed.form_type, ed.year,
                       LENGTH(ds.section_content) as content_length
                FROM document_sections ds
                JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
                LEFT JOIN section_embeddings se ON ds.id = se.document_section_id AND se.embedding_model = $1
                WHERE se.id IS NULL {where_clause}
                ORDER BY ed.company_name, ds.section_index
                LIMIT $2
            """
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def generate_section_embeddings(
        self,
        sections: List[Dict]
    ) -> List[Tuple[Dict, List[float]]]:
        """Generate embeddings for a list of sections"""
        
        if not sections:
            return []
        
        # Extract section content
        section_texts = [section['section_content'] for section in sections]
        
        logger.info(f"🔄 Generating embeddings for {len(sections)} sections using {self.provider.model_name}")
        
        # Generate embeddings
        embeddings = await self.provider.generate_embeddings(section_texts)
        
        # Pair sections with their embeddings
        section_embeddings = []
        for section, embedding in zip(sections, embeddings):
            section_embeddings.append((section, embedding))
        
        return section_embeddings
    
    async def store_section_embeddings(
        self,
        section_embeddings: List[Tuple[Dict, List[float]]]
    ) -> int:
        """Store section embeddings in database"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            stored_count = 0
            
            for section, embedding in section_embeddings:
                # Convert embedding to string format for pgvector
                embedding_str = str(embedding)
                
                await conn.execute("""
                    INSERT INTO section_embeddings (
                        document_section_id, extracted_document_id, section_index, section_type,
                        embedding, embedding_model, embedding_version, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (document_section_id, embedding_model) 
                    DO NOTHING
                """, 
                    section['section_id'],
                    section['extracted_document_id'],
                    section['section_index'],
                    section['section_type'],
                    embedding_str,
                    self.provider.model_name,
                    "v1.0",
                    datetime.now()
                )
                stored_count += 1
            
            logger.info(f"💾 Stored {stored_count} embeddings using {self.provider.model_name}")
            return stored_count
            
        finally:
            await conn.close()
    
    async def process_sections_batch(
        self,
        max_sections: int = 50,
        company_filter: str = None
    ) -> Dict:
        """Process a batch of sections for embedding"""
        batch_start = time.time()
        
        logger.info(f"🚀 Starting embedding batch with {self.provider.model_name}: max {max_sections} sections")
        
        # Ensure database table exists
        await self.setup_embeddings_table()
        
        # Get sections to process
        sections = await self.get_sections_for_embedding(
            limit=max_sections,
            company_filter=company_filter
        )
        
        if not sections:
            logger.info("📭 No sections need embeddings for this provider")
            return {"success": True, "message": "No sections to process"}
        
        logger.info(f"📋 Processing {len(sections)} sections for embedding")
        
        # Process in batches to respect API limits
        all_section_embeddings = []
        total_cost_estimate = 0.0
        
        for i in range(0, len(sections), self.batch_size):
            batch_sections = sections[i:i + self.batch_size]
            
            logger.info(f"🔄 Processing batch {i//self.batch_size + 1}/{(len(sections) + self.batch_size - 1)//self.batch_size}")
            
            # Generate embeddings for this batch
            batch_embeddings = await self.generate_section_embeddings(batch_sections)
            all_section_embeddings.extend(batch_embeddings)
            
            # Estimate cost (rough: OpenAI ~$0.00002 per 1K tokens)
            if "openai" in self.provider.model_name:
                batch_tokens = sum(len(section['section_content']) // 4 for section in batch_sections)
                batch_cost = (batch_tokens / 1000) * 0.00002
                total_cost_estimate += batch_cost
            
            # Rate limiting
            if i + self.batch_size < len(sections):
                await asyncio.sleep(self.rate_limit_delay)
        
        # Store all embeddings
        stored_count = await self.store_section_embeddings(all_section_embeddings)
        
        batch_time = time.time() - batch_start
        
        # Group results by document
        documents_processed = {}
        for section, _ in all_section_embeddings:
            doc_id = section['extracted_document_id']
            if doc_id not in documents_processed:
                documents_processed[doc_id] = {
                    'company_name': section['company_name'],
                    'form_type': section['form_type'],
                    'sections_count': 0
                }
            documents_processed[doc_id]['sections_count'] += 1
        
        summary = {
            "success": True,
            "provider": self.provider.model_name,
            "batch_summary": {
                "sections_processed": len(sections),
                "embeddings_stored": stored_count,
                "documents_affected": len(documents_processed),
                "estimated_cost_usd": round(total_cost_estimate, 6),
                "batch_time_seconds": round(batch_time, 2),
                "avg_time_per_section": round(batch_time / len(sections), 3)
            },
            "documents_processed": documents_processed
        }
        
        logger.info(f"🎉 Embedding batch complete: {stored_count}/{len(sections)} sections, {len(documents_processed)} documents, ${total_cost_estimate:.6f}")
        
        return summary
    
    async def get_embedding_statistics(self) -> Dict:
        """Get embedding statistics for this provider"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Overall stats for this provider
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_embeddings,
                    COUNT(DISTINCT extracted_document_id) as documents_with_embeddings,
                    COUNT(DISTINCT section_type) as section_types_covered,
                    MIN(created_at) as first_embedding,
                    MAX(created_at) as last_embedding
                FROM section_embeddings
                WHERE embedding_model = $1
            """, self.provider.model_name)
            
            # By section type for this provider
            by_section_type = await conn.fetch("""
                SELECT section_type, COUNT(*) as count
                FROM section_embeddings
                WHERE embedding_model = $1
                GROUP BY section_type
                ORDER BY count DESC
            """, self.provider.model_name)
            
            # Pending sections for this provider
            pending = await conn.fetch("""
                SELECT ed.company_name, ed.form_type, COUNT(ds.id) as pending_sections
                FROM document_sections ds
                JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
                LEFT JOIN section_embeddings se ON ds.id = se.document_section_id AND se.embedding_model = $1
                WHERE se.id IS NULL
                GROUP BY ed.id, ed.company_name, ed.form_type
                ORDER BY COUNT(ds.id) DESC
                LIMIT 10
            """, self.provider.model_name)
            
            return {
                "provider": self.provider.model_name,
                "overall": dict(stats) if stats else {},
                "by_section_type": [dict(row) for row in by_section_type],
                "pending_documents": [dict(row) for row in pending]
            }
            
        finally:
            await conn.close()


# Provider factory function
def create_embedding_service(provider_type: str, **kwargs) -> MultiProviderEmbeddingService:
    """Create embedding service with specified provider"""
    
    if provider_type == "openai":
        api_key = kwargs.get('api_key')
        model = kwargs.get('model', 'text-embedding-3-small')
        provider = OpenAIEmbeddingProvider(api_key, model)
    
    elif provider_type == "cohere":
        api_key = kwargs.get('api_key')
        model = kwargs.get('model', 'embed-english-v3.0')
        provider = CohereEmbeddingProvider(api_key, model)
    
    elif provider_type == "local":
        model = kwargs.get('model', 'all-MiniLM-L6-v2')
        provider = LocalEmbeddingProvider(model)
    
    else:
        raise ValueError(f"Unknown provider type: {provider_type}")
    
    return MultiProviderEmbeddingService(provider)


if __name__ == "__main__":
    # For testing
    async def test_multi_provider_embedding():
        import os
        
        # Test OpenAI provider
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            service = create_embedding_service('openai', api_key=api_key)
            
            print(f"🔧 Testing {service.provider.model_name}")
            await service.setup_embeddings_table()
            
            stats = await service.get_embedding_statistics()
            print("📊 Statistics:", json.dumps(stats, indent=2, default=str))
        else:
            print("⚠️ No OpenAI API key found")
    
    asyncio.run(test_multi_provider_embedding())