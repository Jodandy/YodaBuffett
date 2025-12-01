#!/usr/bin/env python3
"""
Document-Level Embedding Service for YodaBuffett.

Complements section-based embeddings by providing high-level document representations
for classification, retrieval, and macro pattern detection.
"""

import asyncio
import logging
import json
import time
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import numpy as np

import asyncpg
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from domains.document_intelligence.factory import get_database_url

logger = logging.getLogger(__name__)


class DocumentEmbeddingService:
    """
    Service for generating and managing document-level embeddings.
    
    Uses a hierarchical approach:
    1. Section embeddings (detailed, already implemented)
    2. Document embeddings (high-level, this service)
    """
    
    def __init__(self, embedding_provider):
        self.embedding_provider = embedding_provider
        self.max_document_length = 30000  # Reasonable limit for full documents
    
    async def setup_document_embeddings_table(self):
        """Create the document_embeddings table if it doesn't exist"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS document_embeddings (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    extracted_document_id UUID NOT NULL REFERENCES extracted_documents(id) ON DELETE CASCADE,
                    embedding VECTOR({self.embedding_provider.embedding_dimension}),
                    embedding_model VARCHAR(100) NOT NULL,
                    
                    -- Document metadata captured in embedding
                    document_length INTEGER,
                    section_count INTEGER,
                    avg_section_confidence FLOAT,
                    
                    -- Hierarchical relationship
                    derived_from_sections BOOLEAN DEFAULT FALSE,
                    
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(extracted_document_id, embedding_model)
                );
                
                -- Indexes for efficient queries
                CREATE INDEX IF NOT EXISTS idx_document_embeddings_doc ON document_embeddings(extracted_document_id);
                CREATE INDEX IF NOT EXISTS idx_document_embeddings_model ON document_embeddings(embedding_model);
                
                -- Vector similarity index
                CREATE INDEX IF NOT EXISTS idx_document_embeddings_vector 
                ON document_embeddings USING ivfflat (embedding vector_cosine_ops) 
                WITH (lists = 100);
            """)
            
            logger.info(f"✅ Document embeddings table created")
            
        finally:
            await conn.close()
    
    async def generate_document_embedding(
        self,
        document_id: str,
        method: str = "full_text"
    ) -> Optional[Dict]:
        """
        Generate embedding for an entire document.
        
        Methods:
        - full_text: Embed the complete document text
        - section_summary: Summarize key sections and embed summary
        - hierarchical: Average/pool section embeddings
        """
        conn = await asyncpg.connect(get_database_url())
        
        try:
            if method == "full_text":
                return await self._embed_full_text(conn, document_id)
            elif method == "section_summary":
                return await self._embed_section_summary(conn, document_id)
            elif method == "hierarchical":
                return await self._embed_hierarchical(conn, document_id)
            else:
                raise ValueError(f"Unknown method: {method}")
                
        finally:
            await conn.close()
    
    async def _embed_full_text(self, conn: asyncpg.Connection, document_id: str) -> Optional[Dict]:
        """Embed the complete document text"""
        
        # Get document text
        doc_data = await conn.fetchrow("""
            SELECT 
                ed.id,
                ed.extracted_text,
                ed.company_name,
                ed.form_type,
                ed.year,
                LENGTH(ed.extracted_text) as text_length
            FROM extracted_documents ed
            WHERE ed.id = $1
        """, document_id)
        
        if not doc_data or not doc_data['extracted_text']:
            logger.warning(f"No text found for document {document_id}")
            return None
        
        # Prepare text (truncate if needed)
        full_text = doc_data['extracted_text']
        if len(full_text) > self.max_document_length:
            # Smart truncation: try to keep beginning and end
            beginning = full_text[:self.max_document_length // 2]
            ending = full_text[-(self.max_document_length // 2):]
            full_text = beginning + "\n...[truncated]...\n" + ending
            logger.info(f"Truncated document {document_id} from {doc_data['text_length']} to {self.max_document_length} chars")
        
        # Generate embedding
        embeddings = await self.embedding_provider.generate_embeddings([full_text])
        if not embeddings:
            return None
        
        embedding = embeddings[0]
        
        # Get section count for metadata
        section_count = await conn.fetchval("""
            SELECT COUNT(*) FROM document_sections WHERE extracted_document_id = $1
        """, document_id)
        
        # Store embedding
        await conn.execute("""
            INSERT INTO document_embeddings (
                extracted_document_id,
                embedding,
                embedding_model,
                document_length,
                section_count,
                derived_from_sections
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (extracted_document_id, embedding_model) 
            DO UPDATE SET 
                embedding = EXCLUDED.embedding,
                document_length = EXCLUDED.document_length,
                section_count = EXCLUDED.section_count,
                created_at = NOW()
        """, 
            document_id,
            str(embedding),
            self.embedding_provider.model_name,
            doc_data['text_length'],
            section_count,
            False  # Not derived from sections
        )
        
        return {
            'document_id': document_id,
            'method': 'full_text',
            'text_length': doc_data['text_length'],
            'embedding_dimension': len(embedding),
            'model': self.embedding_provider.model_name
        }
    
    async def _embed_hierarchical(self, conn: asyncpg.Connection, document_id: str) -> Optional[Dict]:
        """Create document embedding by intelligently combining section embeddings"""
        
        # Get all section embeddings for this document
        section_embeddings = await conn.fetch("""
            SELECT 
                se.embedding,
                ds.section_type,
                ds.section_confidence,
                LENGTH(ds.section_content) as section_length
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            WHERE ds.extracted_document_id = $1
            AND se.embedding_model = $2
            ORDER BY ds.section_index
        """, document_id, self.embedding_provider.model_name)
        
        if not section_embeddings:
            logger.warning(f"No section embeddings found for document {document_id}")
            return None
        
        # Parse embeddings and compute weighted average
        embeddings = []
        weights = []
        
        # Define section importance weights
        section_weights = {
            'executive_summary': 2.0,
            'business_overview': 1.5,
            'risk_factors': 1.5,
            'management_discussion': 1.5,
            'financial_statements': 1.0,
            'income_statement': 1.0,
            'balance_sheet': 1.0,
            'cash_flow_statement': 1.0,
            'notes': 0.5,
            'other': 0.3
        }
        
        for row in section_embeddings:
            embedding = eval(row['embedding'])  # Convert string to list
            embeddings.append(embedding)
            
            # Calculate weight based on section type and confidence
            base_weight = section_weights.get(row['section_type'], 0.5)
            confidence_factor = row['section_confidence'] or 0.5
            weights.append(base_weight * confidence_factor)
        
        # Normalize weights
        weights = np.array(weights)
        weights = weights / weights.sum()
        
        # Compute weighted average
        embeddings_array = np.array(embeddings)
        document_embedding = np.average(embeddings_array, axis=0, weights=weights)
        
        # Store the hierarchical embedding
        avg_confidence = sum(row['section_confidence'] or 0.5 for row in section_embeddings) / len(section_embeddings)
        
        await conn.execute("""
            INSERT INTO document_embeddings (
                extracted_document_id,
                embedding,
                embedding_model,
                document_length,
                section_count,
                avg_section_confidence,
                derived_from_sections
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (extracted_document_id, embedding_model) 
            DO UPDATE SET 
                embedding = EXCLUDED.embedding,
                section_count = EXCLUDED.section_count,
                avg_section_confidence = EXCLUDED.avg_section_confidence,
                created_at = NOW()
        """,
            document_id,
            str(document_embedding.tolist()),
            self.embedding_provider.model_name,
            sum(row['section_length'] for row in section_embeddings),
            len(section_embeddings),
            avg_confidence,
            True  # Derived from sections
        )
        
        return {
            'document_id': document_id,
            'method': 'hierarchical',
            'sections_used': len(section_embeddings),
            'avg_confidence': avg_confidence,
            'model': self.embedding_provider.model_name
        }
    
    async def _embed_section_summary(self, conn: asyncpg.Connection, document_id: str) -> Optional[Dict]:
        """Create a summary of key sections and embed that"""
        
        # Get key sections
        key_sections = await conn.fetch("""
            SELECT 
                ds.section_type,
                ds.section_title,
                ds.section_content,
                ds.section_confidence
            FROM document_sections ds
            WHERE ds.extracted_document_id = $1
            AND ds.section_type IN (
                'executive_summary', 'business_overview', 
                'risk_factors', 'management_discussion'
            )
            AND ds.section_confidence > 0.7
            ORDER BY ds.section_index
            LIMIT 5
        """, document_id)
        
        if not key_sections:
            # Fallback to any high-confidence sections
            key_sections = await conn.fetch("""
                SELECT 
                    ds.section_type,
                    ds.section_title,
                    ds.section_content,
                    ds.section_confidence
                FROM document_sections ds
                WHERE ds.extracted_document_id = $1
                AND ds.section_confidence > 0.6
                ORDER BY ds.section_confidence DESC
                LIMIT 5
            """, document_id)
        
        if not key_sections:
            logger.warning(f"No suitable sections found for summary in document {document_id}")
            return None
        
        # Create a structured summary
        summary_parts = []
        for section in key_sections:
            # Take first 500 chars of each key section
            content_preview = section['section_content'][:500]
            summary_parts.append(f"{section['section_type'].upper()}: {content_preview}")
        
        summary_text = "\n\n".join(summary_parts)
        
        # Generate embedding for summary
        embeddings = await self.embedding_provider.generate_embeddings([summary_text])
        if not embeddings:
            return None
        
        embedding = embeddings[0]
        
        # Store
        await conn.execute("""
            INSERT INTO document_embeddings (
                extracted_document_id,
                embedding,
                embedding_model,
                section_count,
                derived_from_sections
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (extracted_document_id, embedding_model) 
            DO UPDATE SET 
                embedding = EXCLUDED.embedding,
                section_count = EXCLUDED.section_count,
                created_at = NOW()
        """,
            document_id,
            str(embedding),
            self.embedding_provider.model_name,
            len(key_sections),
            True
        )
        
        return {
            'document_id': document_id,
            'method': 'section_summary',
            'sections_summarized': len(key_sections),
            'model': self.embedding_provider.model_name
        }
    
    async def find_similar_documents(
        self,
        query_embedding: List[float],
        limit: int = 10,
        company_filter: Optional[str] = None,
        year_filter: Optional[int] = None
    ) -> List[Dict]:
        """Find documents similar to a query embedding"""
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Build query with filters
            query_parts = ["""
                SELECT 
                    de.extracted_document_id,
                    ed.company_name,
                    ed.form_type,
                    ed.year,
                    ed.filing_date,
                    de.embedding <=> $1::vector as distance,
                    1 - (de.embedding <=> $1::vector) as similarity
                FROM document_embeddings de
                JOIN extracted_documents ed ON de.extracted_document_id = ed.id
                WHERE de.embedding_model = $2
            """]
            
            params = [str(query_embedding), self.embedding_provider.model_name]
            param_count = 2
            
            if company_filter:
                param_count += 1
                query_parts.append(f"AND ed.company_name ILIKE ${param_count}")
                params.append(f"%{company_filter}%")
            
            if year_filter:
                param_count += 1
                query_parts.append(f"AND ed.year = ${param_count}")
                params.append(year_filter)
            
            query_parts.append(f"ORDER BY distance LIMIT ${param_count + 1}")
            params.append(limit)
            
            query = " ".join(query_parts)
            results = await conn.fetch(query, *params)
            
            return [dict(row) for row in results]
            
        finally:
            await conn.close()
    
    async def cluster_documents(
        self,
        n_clusters: int = 10,
        method: str = "kmeans"
    ) -> Dict[int, List[str]]:
        """Cluster documents based on their embeddings"""
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Get all document embeddings
            embeddings_data = await conn.fetch("""
                SELECT 
                    de.extracted_document_id,
                    de.embedding,
                    ed.company_name,
                    ed.form_type,
                    ed.year
                FROM document_embeddings de
                JOIN extracted_documents ed ON de.extracted_document_id = ed.id
                WHERE de.embedding_model = $1
                ORDER BY ed.company_name, ed.year
            """, self.embedding_provider.model_name)
            
            if len(embeddings_data) < n_clusters:
                logger.warning(f"Not enough documents ({len(embeddings_data)}) for {n_clusters} clusters")
                return {}
            
            # Convert to numpy array
            embeddings = []
            doc_ids = []
            
            for row in embeddings_data:
                embeddings.append(eval(row['embedding']))
                doc_ids.append({
                    'id': row['extracted_document_id'],
                    'company': row['company_name'],
                    'type': row['form_type'],
                    'year': row['year']
                })
            
            embeddings_array = np.array(embeddings)
            
            # Perform clustering
            if method == "kmeans":
                from sklearn.cluster import KMeans
                
                kmeans = KMeans(n_clusters=n_clusters, random_state=42)
                cluster_labels = kmeans.fit_predict(embeddings_array)
                
                # Organize results
                clusters = {}
                for i, label in enumerate(cluster_labels):
                    if label not in clusters:
                        clusters[label] = []
                    clusters[label].append(doc_ids[i])
                
                # Add cluster statistics
                for cluster_id, docs in clusters.items():
                    cluster_embeddings = embeddings_array[[i for i, l in enumerate(cluster_labels) if l == cluster_id]]
                    centroid = cluster_embeddings.mean(axis=0)
                    
                    clusters[cluster_id] = {
                        'documents': docs,
                        'size': len(docs),
                        'companies': list(set(d['company'] for d in docs)),
                        'centroid': centroid.tolist()[:5]  # First 5 dims for inspection
                    }
                
                return clusters
            
        finally:
            await conn.close()
    
    async def detect_document_outliers(
        self,
        company_name: str,
        threshold_percentile: float = 5.0
    ) -> List[Dict]:
        """Find documents that are outliers compared to company's typical communication"""
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Get all embeddings for this company
            company_embeddings = await conn.fetch("""
                SELECT 
                    de.extracted_document_id,
                    de.embedding,
                    ed.form_type,
                    ed.year,
                    ed.filing_date
                FROM document_embeddings de
                JOIN extracted_documents ed ON de.extracted_document_id = ed.id
                WHERE ed.company_name = $1
                AND de.embedding_model = $2
                ORDER BY ed.year, ed.filing_date
            """, company_name, self.embedding_provider.model_name)
            
            if len(company_embeddings) < 3:
                logger.warning(f"Not enough documents for {company_name} to detect outliers")
                return []
            
            # Convert to numpy array
            embeddings = np.array([eval(row['embedding']) for row in company_embeddings])
            
            # Calculate pairwise distances
            from sklearn.metrics.pairwise import cosine_distances
            distances = cosine_distances(embeddings)
            
            # For each document, calculate its average distance to all others
            avg_distances = distances.mean(axis=1)
            
            # Find outliers (documents with high average distance)
            threshold = np.percentile(avg_distances, 100 - threshold_percentile)
            outlier_indices = np.where(avg_distances > threshold)[0]
            
            outliers = []
            for idx in outlier_indices:
                doc = company_embeddings[idx]
                outliers.append({
                    'document_id': doc['extracted_document_id'],
                    'form_type': doc['form_type'],
                    'year': doc['year'],
                    'filing_date': doc['filing_date'],
                    'avg_distance': float(avg_distances[idx]),
                    'distance_percentile': float(np.percentile(avg_distances, avg_distances[idx]))
                })
            
            return sorted(outliers, key=lambda x: x['avg_distance'], reverse=True)
            
        finally:
            await conn.close()


if __name__ == "__main__":
    async def test_document_embeddings():
        """Test document embedding functionality"""
        from domains.document_intelligence.services.multi_provider_embedding_service import LocalEmbeddingProvider
        
        print("🧪 Testing Document Embedding Service...")
        
        # Create local provider
        provider = LocalEmbeddingProvider()
        service = DocumentEmbeddingService(provider)
        
        # Setup table
        await service.setup_document_embeddings_table()
        print("✅ Document embeddings table created")
        
        # Test embedding methods
        print("\n📊 Testing different embedding methods:")
        print("1. full_text: Embed complete document")
        print("2. hierarchical: Combine section embeddings")
        print("3. section_summary: Embed key sections summary")
        
        print("\n💡 Use cases enabled:")
        print("- Document classification and clustering")
        print("- Finding similar companies by communication style")
        print("- Detecting major outliers in company history")
        print("- Fast document retrieval before detailed analysis")
        print("- Cross-company pattern detection")
    
    asyncio.run(test_document_embeddings())