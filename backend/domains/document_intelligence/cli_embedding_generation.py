#!/usr/bin/env python3
"""
Embedding Generation CLI

Generate vector embeddings for extracted Nordic documents.
Supports batch processing with cost tracking and progress monitoring.
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.embedding_service import EmbeddingService, get_embedding_statistics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EmbeddingController:
    """Controller for document embedding generation"""
    
    def __init__(self):
        # Get OpenAI API key from environment
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("⚠️  No OPENAI_API_KEY found - will generate dummy embeddings for testing")
        
        self.embedding_service = EmbeddingService(api_key=api_key)
    
    async def show_status(self):
        """Display current embedding status"""
        logger.info("📊 EMBEDDING STATUS")
        logger.info("=" * 40)
        
        stats = await get_embedding_statistics()
        
        overall = stats['overall']
        if overall:
            total_docs = overall.get('total_documents', 0)
            embedded_docs = overall.get('documents_with_embeddings', 0)
            total_embeddings = overall.get('total_embeddings', 0)
            
            logger.info(f"📄 Total documents: {total_docs:,}")
            logger.info(f"✅ Documents with embeddings: {embedded_docs:,}")
            logger.info(f"📝 Documents pending: {total_docs - embedded_docs:,}")
            logger.info(f"🔢 Total embeddings: {total_embeddings:,}")
            
            if total_docs > 0:
                completion_rate = (embedded_docs / total_docs) * 100
                logger.info(f"📊 Completion rate: {completion_rate:.1f}%")
            
            if overall.get('avg_chunk_length'):
                logger.info(f"📏 Avg chunk length: {overall['avg_chunk_length']:.0f} chars")
            
            if overall.get('first_embedding'):
                logger.info(f"🕐 First embedding: {overall['first_embedding']}")
            if overall.get('last_embedding'):
                logger.info(f"🕐 Last embedding: {overall['last_embedding']}")
        
        # Show embedding models used
        if stats['by_model']:
            logger.info(f"\n🤖 Embedding Models:")
            for model_info in stats['by_model']:
                logger.info(f"   {model_info['embedding_model']} v{model_info['embedding_version']}: {model_info['count']:,}")
        
        # Show next documents to process
        if stats['pending_documents']:
            logger.info(f"\n📋 Next Documents to Process:")
            for doc in stats['pending_documents'][:5]:
                logger.info(f"   {doc['company_name'][:25]:25} {doc['form_type']:15} {doc['total_chunks']:3} chunks")
            
            if len(stats['pending_documents']) > 5:
                logger.info(f"   ... and {len(stats['pending_documents']) - 5} more")
    
    async def preview_batch(self, limit: int = 10, company_filter: str = None):
        """Preview next documents for embedding"""
        logger.info("🔍 EMBEDDING BATCH PREVIEW")
        logger.info("=" * 40)
        
        documents = await self.embedding_service.get_documents_for_embedding(
            limit=limit,
            company_filter=company_filter
        )
        
        if not documents:
            logger.info("📭 No documents need embedding")
            return
        
        logger.info(f"📄 Next {len(documents)} documents to embed:")
        logger.info("")
        
        total_chunks = 0
        estimated_cost = 0.0
        
        for i, doc in enumerate(documents, 1):
            chunk_count = doc['chunk_count']
            total_chunks += chunk_count
            
            # Rough cost estimate: assume 2000 chars/chunk = 500 tokens, $0.00002/1K tokens
            doc_cost = (chunk_count * 500 / 1000) * 0.00002
            estimated_cost += doc_cost
            
            logger.info(f"  {i:2d}. {doc['company_name'][:25]:25} {doc['form_type']:15} {chunk_count:3} chunks (${doc_cost:.4f})")
        
        logger.info("")
        logger.info(f"📊 Total: {total_chunks:,} chunks")
        logger.info(f"💰 Estimated cost: ${estimated_cost:.4f}")
        
        # Estimate processing time (roughly 1 second per chunk for API + processing)
        estimated_minutes = (total_chunks * 1.2) / 60  # 1.2 seconds per chunk
        logger.info(f"⏱️  Estimated time: {estimated_minutes:.1f} minutes")
    
    async def generate_embeddings(
        self,
        max_documents: int = 5,
        company_filter: str = None,
        dry_run: bool = False
    ):
        """Generate embeddings for a batch of documents"""
        logger.info("🚀 EMBEDDING GENERATION")
        logger.info("=" * 40)
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No actual embedding generation")
        
        # Preview what will be processed
        await self.preview_batch(max_documents, company_filter)
        
        if dry_run:
            logger.info("✅ Dry run complete - no embeddings generated")
            return
        
        # Confirm before proceeding (unless very small batch)
        if max_documents > 3:
            response = input(f"\n⚠️  Proceed with embedding {max_documents} documents? [y/N]: ")
            if response.lower() != 'y':
                logger.info("❌ Cancelled by user")
                return
        
        # Start processing
        logger.info(f"\n🔄 Starting embedding generation...")
        start_time = datetime.now()
        
        try:
            result = await self.embedding_service.process_batch(
                max_documents=max_documents,
                company_filter=company_filter
            )
            
            if result['success']:
                summary = result['batch_summary']
                logger.info(f"\n✅ Batch Complete!")
                logger.info(f"   📄 Documents processed: {summary['documents_successful']}/{summary['documents_attempted']}")
                logger.info(f"   🔢 Embeddings created: {summary['total_embeddings_created']:,}")
                logger.info(f"   💰 Total cost: ${summary['total_cost_usd']:.4f}")
                logger.info(f"   ⏱️  Total time: {summary['batch_time_seconds']:.1f} seconds")
                logger.info(f"   📊 Avg per document: {summary['avg_time_per_document']:.1f} seconds")
                
                # Show any failures
                failures = [r for r in result['detailed_results'] if not r['success']]
                if failures:
                    logger.warning(f"\n⚠️  {len(failures)} documents failed:")
                    for failure in failures:
                        logger.warning(f"   - {failure['document_id']}: {failure.get('error', 'Unknown error')}")
            else:
                logger.error(f"❌ Batch failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Embedding generation failed: {e}")
        
        # Show updated status
        logger.info(f"\n📊 Updated Status:")
        await self.show_status()
    
    async def test_single_document(self, company_filter: str = None):
        """Test embedding generation with a single small document"""
        logger.info("🧪 SINGLE DOCUMENT TEST")
        logger.info("=" * 40)
        
        # Find a small document for testing
        documents = await self.embedding_service.get_documents_for_embedding(
            limit=10,
            company_filter=company_filter
        )
        
        if not documents:
            logger.info("📭 No documents available for testing")
            return
        
        # Pick the smallest document
        test_doc = min(documents, key=lambda x: x['chunk_count'])
        
        logger.info(f"🎯 Testing with: {test_doc['company_name']} - {test_doc['form_type']}")
        logger.info(f"📊 Chunks: {test_doc['chunk_count']}")
        
        # Estimate cost
        estimated_cost = (test_doc['chunk_count'] * 500 / 1000) * 0.00002
        logger.info(f"💰 Estimated cost: ${estimated_cost:.6f}")
        
        response = input(f"\n⚠️  Proceed with test? [y/N]: ")
        if response.lower() != 'y':
            logger.info("❌ Test cancelled")
            return
        
        try:
            result = await self.embedding_service.process_document(test_doc['id'])
            
            if result['success']:
                logger.info(f"\n✅ Test Successful!")
                logger.info(f"   📄 Document: {test_doc['id']}")
                logger.info(f"   🔢 Embeddings: {result['embeddings_stored']}")
                logger.info(f"   💰 Cost: ${result['estimated_cost_usd']:.6f}")
                logger.info(f"   ⏱️  Time: {result['processing_time_seconds']} seconds")
            else:
                logger.error(f"❌ Test failed: {result.get('reason', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Test failed with exception: {e}")


async def main():
    """CLI entry point"""
    controller = EmbeddingController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_embedding_generation.py status                    # Show embedding status")
        print("  python cli_embedding_generation.py preview [limit]           # Preview next documents")
        print("  python cli_embedding_generation.py test [company_filter]     # Test with single document")
        print("  python cli_embedding_generation.py generate [count] [--company=X] [--dry-run]")
        print("")
        print("Examples:")
        print("  python cli_embedding_generation.py generate 3               # Generate 3 documents")
        print("  python cli_embedding_generation.py generate 5 --company=Volvo --dry-run")
        print("  python cli_embedding_generation.py preview 10")
        print("  python cli_embedding_generation.py test Ericsson")
        return
    
    command = sys.argv[1]
    
    if command == "status":
        await controller.show_status()
        
    elif command == "preview":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        await controller.preview_batch(limit)
        
    elif command == "test":
        company_filter = sys.argv[2] if len(sys.argv) > 2 else None
        await controller.test_single_document(company_filter)
        
    elif command == "generate":
        max_documents = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        
        # Parse additional arguments
        company_filter = None
        dry_run = False
        
        for arg in sys.argv[3:]:
            if arg.startswith('--company='):
                company_filter = arg.split('=')[1]
            elif arg == '--dry-run':
                dry_run = True
        
        await controller.generate_embeddings(
            max_documents=max_documents,
            company_filter=company_filter,
            dry_run=dry_run
        )
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())