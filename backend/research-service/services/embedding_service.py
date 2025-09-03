"""
Embedding Service
Generates and manages document embeddings for semantic search
"""
import asyncio
import json
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import logging
import tiktoken
import numpy as np

from openai import AsyncOpenAI
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from ..config import settings
from ..processors.pdf_processor import PDFChunk

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingResult:
    """Result of embedding generation"""
    chunk_id: str
    embedding: List[float]
    tokens: int
    cost: float


@dataclass
class SearchResult:
    """Result from semantic search"""
    document_id: str
    chunk_id: str
    chunk_text: str
    similarity_score: float
    metadata: Dict


class EmbeddingService:
    """Handles embedding generation and semantic search"""
    
    def __init__(self):
        self.client = AsyncOpenAI(api_key=settings.openai_api_key)
        self.model = settings.embedding_model
        self.tokenizer = tiktoken.encoding_for_model("gpt-4")
        
        # Pricing per 1M tokens
        self.embedding_costs = {
            "text-embedding-3-small": 0.02,
            "text-embedding-3-large": 0.13,
            "text-embedding-ada-002": 0.10
        }
    
    async def generate_embeddings(self, chunks: List[PDFChunk]) -> List[EmbeddingResult]:
        """Generate embeddings for document chunks"""
        results = []
        
        # Process in batches for efficiency
        batch_size = 20
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i + batch_size]
            batch_results = await self._process_embedding_batch(batch)
            results.extend(batch_results)
        
        return results
    
    async def _process_embedding_batch(self, chunks: List[PDFChunk]) -> List[EmbeddingResult]:
        """Process a batch of chunks"""
        texts = [chunk.text for chunk in chunks]
        
        try:
            # Generate embeddings
            response = await self.client.embeddings.create(
                model=self.model,
                input=texts
            )
            
            results = []
            for idx, (chunk, embedding_data) in enumerate(zip(chunks, response.data)):
                # Count tokens
                tokens = len(self.tokenizer.encode(chunk.text))
                
                # Calculate cost
                cost = (tokens / 1_000_000) * self.embedding_costs.get(self.model, 0.02)
                
                result = EmbeddingResult(
                    chunk_id=f"chunk_{chunk.chunk_index}",
                    embedding=embedding_data.embedding,
                    tokens=tokens,
                    cost=cost
                )
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Error generating embeddings: {e}")
            return []
    
    async def semantic_search(
        self, 
        query: str, 
        company_id: Optional[str] = None,
        limit: int = 10,
        threshold: float = 0.7
    ) -> List[SearchResult]:
        """Perform semantic search across document embeddings"""
        
        # Generate query embedding
        query_embedding = await self._generate_query_embedding(query)
        if not query_embedding:
            return []
        
        # Search in vector database
        results = await self._search_vectors(
            query_embedding, 
            company_id=company_id,
            limit=limit,
            threshold=threshold
        )
        
        return results
    
    async def _generate_query_embedding(self, query: str) -> Optional[List[float]]:
        """Generate embedding for search query"""
        try:
            response = await self.client.embeddings.create(
                model=self.model,
                input=query
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error generating query embedding: {e}")
            return None
    
    async def _search_vectors(
        self, 
        query_embedding: List[float],
        company_id: Optional[str] = None,
        limit: int = 10,
        threshold: float = 0.7
    ) -> List[SearchResult]:
        """Search for similar vectors in the database"""
        
        # This is a placeholder for pgvector search
        # In production, you'd use pgvector's similarity operators
        
        query_str = """
        SELECT 
            de.id,
            de.document_id,
            de.chunk_index,
            de.chunk_text,
            de.metadata,
            1 - (de.embedding <=> :query_embedding) as similarity
        FROM document_embeddings de
        JOIN nordic_documents nd ON de.document_id = nd.id
        WHERE 1 - (de.embedding <=> :query_embedding) > :threshold
        """
        
        if company_id:
            query_str += " AND nd.company_id = :company_id"
        
        query_str += " ORDER BY similarity DESC LIMIT :limit"
        
        # Note: This is pseudo-code for pgvector
        # Actual implementation would need proper pgvector setup
        results = []
        
        # For now, return mock results
        return results
    
    async def find_similar_chunks(
        self, 
        chunk_id: str, 
        limit: int = 5
    ) -> List[SearchResult]:
        """Find chunks similar to a given chunk"""
        
        # Get the chunk's embedding
        # Then search for similar chunks
        # This is useful for finding related content
        
        return []
    
    def calculate_embedding_cost(self, text: str) -> float:
        """Calculate the cost of embedding a text"""
        tokens = len(self.tokenizer.encode(text))
        cost_per_million = self.embedding_costs.get(self.model, 0.02)
        return (tokens / 1_000_000) * cost_per_million
    
    async def update_document_embeddings(
        self,
        document_id: str,
        chunks: List[PDFChunk]
    ) -> int:
        """Update embeddings for a document"""
        
        # Generate embeddings
        embeddings = await self.generate_embeddings(chunks)
        
        # Store in database
        stored_count = 0
        for chunk, embedding_result in zip(chunks, embeddings):
            # Store embedding (pseudo-code)
            # In production, this would insert into document_embeddings table
            stored_count += 1
        
        return stored_count


class VectorSearchOptimizer:
    """Optimizes vector search queries"""
    
    @staticmethod
    def rerank_results(
        results: List[SearchResult],
        query: str,
        rerank_model: Optional[str] = None
    ) -> List[SearchResult]:
        """Rerank search results for better relevance"""
        
        # Simple reranking based on keyword matches
        # In production, you might use a cross-encoder model
        
        query_words = set(query.lower().split())
        
        for result in results:
            text_words = set(result.chunk_text.lower().split())
            keyword_overlap = len(query_words & text_words) / len(query_words)
            
            # Adjust score based on keyword overlap
            result.similarity_score *= (1 + keyword_overlap * 0.2)
        
        # Resort by adjusted scores
        results.sort(key=lambda x: x.similarity_score, reverse=True)
        
        return results
    
    @staticmethod
    def expand_query(query: str, language: str = 'en') -> List[str]:
        """Expand query with synonyms and related terms"""
        
        expansions = [query]
        
        # Simple expansion for financial terms
        financial_expansions = {
            'revenue': ['sales', 'turnover', 'income'],
            'profit': ['earnings', 'income', 'gain'],
            'growth': ['increase', 'expansion', 'rise'],
            'risk': ['threat', 'challenge', 'concern'],
        }
        
        query_lower = query.lower()
        for term, synonyms in financial_expansions.items():
            if term in query_lower:
                for synonym in synonyms:
                    expansions.append(query_lower.replace(term, synonym))
        
        return expansions[:3]  # Limit expansions