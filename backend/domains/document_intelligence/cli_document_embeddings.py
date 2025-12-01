#!/usr/bin/env python3
"""
CLI for document-level embeddings generation and management.

Parallel to cli_multi_embeddings.py but for entire documents.
"""

import asyncio
import argparse
import logging
import json
import os
from datetime import datetime
from typing import Dict, List, Optional

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.document_embedding_service import DocumentEmbeddingService
from domains.document_intelligence.services.multi_provider_embedding_service import (
    create_embedding_service, LocalEmbeddingProvider,
    OpenAIEmbeddingProvider, CohereEmbeddingProvider
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DocumentEmbeddingsCLI:
    """CLI for managing document-level embeddings"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.CLI")
    
    async def setup_embeddings(self, provider_type: str):
        """Setup embedding tables and configuration"""
        self.logger.info(f"🔧 Setting up document embeddings for {provider_type} provider...")
        
        provider = self._create_provider(provider_type)
        service = DocumentEmbeddingService(provider)
        
        await service.setup_document_embeddings_table()
        self.logger.info("✅ Document embeddings table created successfully!")
        
        return {"success": True, "provider": provider_type}
    
    async def check_status(self, provider_type: str):
        """Check document embedding status"""
        provider = self._create_provider(provider_type)
        conn = await self._get_db_connection()
        
        try:
            # Overall statistics
            total_docs = await conn.fetchval("""
                SELECT COUNT(*) FROM extracted_documents
            """)
            
            # Document embeddings by provider
            doc_embeddings = await conn.fetchval("""
                SELECT COUNT(*) 
                FROM document_embeddings
                WHERE embedding_model = $1
            """, provider.model_name)
            
            # Method breakdown
            method_stats = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN derived_from_sections THEN 'hierarchical/summary'
                        ELSE 'full_text'
                    END as method,
                    COUNT(*) as count
                FROM document_embeddings
                WHERE embedding_model = $1
                GROUP BY derived_from_sections
            """, provider.model_name)
            
            # Recent embeddings
            recent = await conn.fetch("""
                SELECT 
                    ed.company_name,
                    ed.form_type,
                    ed.year,
                    de.created_at
                FROM document_embeddings de
                JOIN extracted_documents ed ON de.extracted_document_id = ed.id
                WHERE de.embedding_model = $1
                ORDER BY de.created_at DESC
                LIMIT 5
            """, provider.model_name)
            
            # Print status
            print(f"\n📊 DOCUMENT EMBEDDING STATUS - {provider_type.upper()}")
            print("=" * 60)
            print(f"Total documents in database: {total_docs:,}")
            print(f"Documents with embeddings: {doc_embeddings:,}")
            print(f"Coverage: {(doc_embeddings/total_docs*100) if total_docs > 0 else 0:.1f}%")
            print(f"Remaining: {total_docs - doc_embeddings:,}")
            
            if method_stats:
                print(f"\n📈 Embedding Methods:")
                for stat in method_stats:
                    print(f"   {stat['method']}: {stat['count']:,}")
            
            if recent:
                print(f"\n🕐 Recent Document Embeddings:")
                for r in recent:
                    print(f"   {r['company_name']} - {r['form_type']} {r['year']} "
                          f"({r['created_at'].strftime('%Y-%m-%d %H:%M')})")
            else:
                print(f"\n⚠️  No document embeddings found for {provider_type}")
            
            # Check for documents that need embeddings
            need_embeddings = await conn.fetchval("""
                SELECT COUNT(*)
                FROM extracted_documents ed
                LEFT JOIN document_embeddings de ON ed.id = de.extracted_document_id 
                    AND de.embedding_model = $1
                WHERE de.id IS NULL
                AND ed.extracted_text IS NOT NULL
                AND LENGTH(ed.extracted_text) > 1000
            """, provider.model_name)
            
            if need_embeddings > 0:
                print(f"\n💡 {need_embeddings:,} documents ready for embedding")
                print(f"   Run: python {__file__} {provider_type} process [count]")
            
        finally:
            await conn.close()
    
    async def process_documents(
        self,
        provider_type: str,
        count: int = 50,
        method: str = "hierarchical",
        company_filter: Optional[str] = None
    ):
        """Process documents to generate embeddings"""
        self.logger.info(f"🚀 Processing {count} documents with {provider_type} provider...")
        self.logger.info(f"📋 Method: {method}")
        
        provider = self._create_provider(provider_type)
        service = DocumentEmbeddingService(provider)
        conn = await self._get_db_connection()
        
        try:
            # Setup table if needed
            await service.setup_document_embeddings_table()
            
            # Build query to find documents needing embeddings
            query_parts = ["""
                SELECT ed.id
                FROM extracted_documents ed
                LEFT JOIN document_embeddings de ON ed.id = de.extracted_document_id 
                    AND de.embedding_model = $1
                WHERE de.id IS NULL
                AND ed.extracted_text IS NOT NULL
                AND LENGTH(ed.extracted_text) > 1000
            """]
            params = [provider.model_name]
            param_count = 1
            
            if company_filter:
                param_count += 1
                query_parts.append(f"AND ed.company_name ILIKE ${param_count}")
                params.append(f"%{company_filter}%")
            
            # For hierarchical method, ensure sections exist
            if method == "hierarchical":
                query_parts.append("""
                    AND EXISTS (
                        SELECT 1 FROM document_sections ds
                        WHERE ds.extracted_document_id = ed.id
                    )
                """)
            
            query_parts.append(f"ORDER BY ed.year DESC, ed.company_name LIMIT ${param_count + 1}")
            params.append(count)
            
            query = " ".join(query_parts)
            documents = await conn.fetch(query, *params)
            
            if not documents:
                print("📭 No documents need embeddings")
                return
            
            print(f"📋 Found {len(documents)} documents to process")
            
            # Process each document
            success_count = 0
            failed_count = 0
            
            for i, doc in enumerate(documents, 1):
                doc_id = str(doc['id'])
                
                try:
                    # Get document info for display
                    doc_info = await conn.fetchrow("""
                        SELECT company_name, form_type, year 
                        FROM extracted_documents 
                        WHERE id = $1
                    """, doc['id'])
                    
                    print(f"\n[{i}/{len(documents)}] Processing: {doc_info['company_name']} "
                          f"- {doc_info['form_type']} {doc_info['year']}")
                    
                    # Generate embedding
                    result = await service.generate_document_embedding(doc_id, method)
                    
                    if result:
                        success_count += 1
                        print(f"   ✅ Embedded using {result['method']} method")
                        if method == "hierarchical":
                            print(f"   📊 Used {result.get('sections_used', 'N/A')} sections")
                    else:
                        failed_count += 1
                        print(f"   ❌ Failed to generate embedding")
                    
                except Exception as e:
                    failed_count += 1
                    print(f"   ❌ Error: {str(e)}")
                    self.logger.error(f"Error processing {doc_id}: {e}")
                
                # Add small delay to avoid overwhelming the system
                if i % 10 == 0:
                    await asyncio.sleep(0.5)
            
            # Summary
            print(f"\n📊 PROCESSING SUMMARY")
            print("=" * 40)
            print(f"✅ Successful: {success_count}")
            print(f"❌ Failed: {failed_count}")
            print(f"📈 Success rate: {(success_count/(success_count+failed_count)*100):.1f}%")
            
        finally:
            await conn.close()
    
    async def find_similar(
        self,
        provider_type: str,
        company_name: str,
        year: int,
        limit: int = 10
    ):
        """Find documents similar to a given document"""
        provider = self._create_provider(provider_type)
        service = DocumentEmbeddingService(provider)
        conn = await self._get_db_connection()
        
        try:
            # Get the reference document
            ref_doc = await conn.fetchrow("""
                SELECT 
                    de.extracted_document_id,
                    de.embedding,
                    ed.company_name,
                    ed.form_type,
                    ed.year
                FROM document_embeddings de
                JOIN extracted_documents ed ON de.extracted_document_id = ed.id
                WHERE ed.company_name ILIKE $1
                AND ed.year = $2
                AND de.embedding_model = $3
                LIMIT 1
            """, f"%{company_name}%", year, provider.model_name)
            
            if not ref_doc:
                print(f"❌ No document embedding found for {company_name} {year}")
                return
            
            print(f"\n🔍 Finding documents similar to: {ref_doc['company_name']} "
                  f"{ref_doc['form_type']} {ref_doc['year']}")
            
            # Find similar documents
            ref_embedding = eval(ref_doc['embedding'])
            similar_docs = await service.find_similar_documents(
                ref_embedding, 
                limit=limit + 1  # +1 to exclude self
            )
            
            # Filter out the reference document
            similar_docs = [d for d in similar_docs if d['extracted_document_id'] != ref_doc['extracted_document_id']]
            
            print(f"\n📊 TOP {min(limit, len(similar_docs))} SIMILAR DOCUMENTS:")
            print("=" * 60)
            
            for i, doc in enumerate(similar_docs[:limit], 1):
                print(f"\n{i}. {doc['company_name']} - {doc['form_type']} {doc['year']}")
                print(f"   Similarity: {doc['similarity']:.3f}")
                print(f"   Distance: {doc['distance']:.3f}")
            
        finally:
            await conn.close()
    
    async def cluster_documents(
        self,
        provider_type: str,
        n_clusters: int = 10,
        year_filter: Optional[int] = None
    ):
        """Cluster documents based on embeddings"""
        provider = self._create_provider(provider_type)
        service = DocumentEmbeddingService(provider)
        
        print(f"\n🗂️ Clustering documents into {n_clusters} groups...")
        if year_filter:
            print(f"📅 Filtering to year: {year_filter}")
        
        clusters = await service.cluster_documents(n_clusters)
        
        if not clusters:
            print("❌ Not enough documents for clustering")
            return
        
        print(f"\n📊 DOCUMENT CLUSTERS")
        print("=" * 60)
        
        for cluster_id, cluster_info in sorted(clusters.items()):
            print(f"\n🏷️ Cluster {cluster_id + 1}: {cluster_info['size']} documents")
            print(f"   Companies: {', '.join(cluster_info['companies'][:5])}"
                  f"{'...' if len(cluster_info['companies']) > 5 else ''}")
            
            # Show sample documents
            print("   Sample documents:")
            for doc in cluster_info['documents'][:3]:
                print(f"     - {doc['company']} {doc['type']} {doc['year']}")
    
    async def detect_outliers(
        self,
        provider_type: str,
        company_name: str,
        threshold_percentile: float = 5.0
    ):
        """Detect outlier documents for a company"""
        provider = self._create_provider(provider_type)
        service = DocumentEmbeddingService(provider)
        
        print(f"\n🔍 Detecting outlier documents for: {company_name}")
        print(f"📊 Threshold: Top {threshold_percentile}% most unusual")
        
        outliers = await service.detect_document_outliers(company_name, threshold_percentile)
        
        if not outliers:
            print(f"❌ No outliers found (need at least 3 documents)")
            return
        
        print(f"\n🚨 OUTLIER DOCUMENTS")
        print("=" * 60)
        
        for i, outlier in enumerate(outliers, 1):
            print(f"\n{i}. {outlier['form_type']} {outlier['year']} "
                  f"(filed: {outlier['filing_date'].strftime('%Y-%m-%d') if outlier['filing_date'] else 'N/A'})")
            print(f"   Average distance: {outlier['avg_distance']:.3f}")
            print(f"   Percentile: {outlier['distance_percentile']:.1f}% "
                  f"(more unusual than {outlier['distance_percentile']:.0f}% of documents)")
    
    def _create_provider(self, provider_type: str):
        """Create embedding provider based on type"""
        if provider_type == "local":
            return LocalEmbeddingProvider()
        elif provider_type == "openai":
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set")
            return OpenAIEmbeddingProvider(api_key)
        elif provider_type == "cohere":
            api_key = os.getenv('COHERE_API_KEY')
            if not api_key:
                raise ValueError("COHERE_API_KEY environment variable not set")
            return CohereEmbeddingProvider(api_key)
        else:
            raise ValueError(f"Unknown provider type: {provider_type}")
    
    async def _get_db_connection(self):
        """Get database connection"""
        import asyncpg
        from domains.document_intelligence.factory import get_database_url
        return await asyncpg.connect(get_database_url())


async def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Document-level embeddings management")
    
    # Provider selection
    parser.add_argument('provider', choices=['local', 'openai', 'cohere'],
                       help='Embedding provider to use')
    
    # Commands
    parser.add_argument('command', 
                       choices=['setup', 'status', 'process', 'similar', 'cluster', 'outliers'],
                       help='Command to execute')
    
    # Command-specific arguments
    parser.add_argument('--count', type=int, default=50,
                       help='Number of documents to process (for process command)')
    parser.add_argument('--method', choices=['full_text', 'hierarchical', 'section_summary'],
                       default='hierarchical',
                       help='Embedding method to use')
    parser.add_argument('--company', type=str,
                       help='Company name filter')
    parser.add_argument('--year', type=int,
                       help='Year filter or reference')
    parser.add_argument('--limit', type=int, default=10,
                       help='Result limit for queries')
    parser.add_argument('--clusters', type=int, default=10,
                       help='Number of clusters for clustering')
    parser.add_argument('--threshold', type=float, default=5.0,
                       help='Percentile threshold for outlier detection')
    
    args = parser.parse_args()
    
    # Create CLI instance
    cli = DocumentEmbeddingsCLI()
    
    # Execute command
    if args.command == 'setup':
        await cli.setup_embeddings(args.provider)
    
    elif args.command == 'status':
        await cli.check_status(args.provider)
    
    elif args.command == 'process':
        await cli.process_documents(
            args.provider,
            args.count,
            args.method,
            args.company
        )
    
    elif args.command == 'similar':
        if not args.company or not args.year:
            print("❌ --company and --year required for similar command")
            return
        await cli.find_similar(args.provider, args.company, args.year, args.limit)
    
    elif args.command == 'cluster':
        await cli.cluster_documents(args.provider, args.clusters, args.year)
    
    elif args.command == 'outliers':
        if not args.company:
            print("❌ --company required for outliers command")
            return
        await cli.detect_outliers(args.provider, args.company, args.threshold)


if __name__ == "__main__":
    asyncio.run(main())