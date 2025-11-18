#!/usr/bin/env python3
"""
Multi-Provider Embeddings CLI

Generate embeddings from stored document sections using different providers.
Works with sections created by the section chunking service.
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.multi_provider_embedding_service import create_embedding_service

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MultiEmbeddingController:
    """Controller for multi-provider embedding operations"""
    
    def __init__(self, provider_type: str, **provider_kwargs):
        try:
            self.service = create_embedding_service(provider_type, **provider_kwargs)
            self.provider_type = provider_type
        except Exception as e:
            logger.error(f"❌ Failed to create {provider_type} provider: {e}")
            self.service = None
    
    async def setup_database(self):
        """Setup database schema for embeddings"""
        if not self.service:
            return False
            
        logger.info(f"🔧 SETTING UP EMBEDDINGS DATABASE FOR {self.service.provider.model_name}")
        logger.info("=" * 60)
        
        try:
            await self.service.setup_embeddings_table()
            logger.info(f"✅ Embeddings database setup complete for {self.service.provider.model_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Database setup failed: {e}")
            return False
    
    async def show_status(self):
        """Display current embedding status for this provider"""
        if not self.service:
            return
            
        logger.info(f"📊 EMBEDDINGS STATUS FOR {self.service.provider.model_name}")
        logger.info("=" * 60)
        
        try:
            stats = await self.service.get_embedding_statistics()
            
            overall = stats['overall']
            if overall:
                total_embeddings = overall.get('total_embeddings', 0)
                docs_with_embeddings = overall.get('documents_with_embeddings', 0)
                section_types = overall.get('section_types_covered', 0)
                first_embedding = overall.get('first_embedding')
                last_embedding = overall.get('last_embedding')
                
                logger.info(f"🧩 Total embeddings: {total_embeddings:,}")
                logger.info(f"📄 Documents with embeddings: {docs_with_embeddings:,}")
                logger.info(f"📋 Section types covered: {section_types}")
                logger.info(f"🔧 Provider: {stats['provider']}")
                
                if first_embedding:
                    logger.info(f"📅 First embedding: {first_embedding}")
                if last_embedding:
                    logger.info(f"📅 Last embedding: {last_embedding}")
            
            # Show section type distribution
            if stats['by_section_type']:
                logger.info(f"\n📋 Section Type Distribution:")
                for section_info in stats['by_section_type']:
                    section_type = section_info['section_type']
                    count = section_info['count']
                    logger.info(f"   {section_type:20} {count:5,} embeddings")
            
            # Show pending documents
            if stats['pending_documents']:
                logger.info(f"\n📝 Documents with Pending Sections (showing first 5):")
                for i, doc in enumerate(stats['pending_documents'][:5]):
                    logger.info(f"   {doc['company_name']:25} {doc['form_type']:15} ({doc['pending_sections']} sections)")
                
                remaining = len(stats['pending_documents']) - 5
                if remaining > 0:
                    logger.info(f"   ... and {remaining} more documents")
            
        except Exception as e:
            logger.error(f"❌ Failed to get status: {e}")
    
    async def process_batch(self, max_sections: int = 20, company_filter: str = None):
        """Process a batch of sections for embedding"""
        if not self.service:
            return
            
        logger.info(f"🚀 PROCESSING EMBEDDINGS BATCH WITH {self.service.provider.model_name}")
        logger.info("=" * 60)
        
        try:
            result = await self.service.process_sections_batch(
                max_sections=max_sections,
                company_filter=company_filter
            )
            
            if result['success']:
                summary = result['batch_summary']
                logger.info(f"\n✅ Batch Processing Complete!")
                logger.info(f"   🔧 Provider: {result['provider']}")
                logger.info(f"   📋 Sections processed: {summary['sections_processed']}")
                logger.info(f"   💾 Embeddings stored: {summary['embeddings_stored']}")
                logger.info(f"   📄 Documents affected: {summary['documents_affected']}")
                logger.info(f"   💵 Estimated cost: ${summary['estimated_cost_usd']:.6f}")
                logger.info(f"   ⏱️  Total time: {summary['batch_time_seconds']} seconds")
                logger.info(f"   ⚡ Avg time per section: {summary['avg_time_per_section']} seconds")
                
                # Show affected documents
                if result['documents_processed']:
                    logger.info(f"\n📄 Documents Processed:")
                    for doc_id, doc_info in list(result['documents_processed'].items())[:5]:
                        company = doc_info['company_name']
                        form_type = doc_info['form_type']
                        sections = doc_info['sections_count']
                        logger.info(f"   {company:25} {form_type:15} ({sections} sections)")
                    
                    remaining = len(result['documents_processed']) - 5
                    if remaining > 0:
                        logger.info(f"   ... and {remaining} more documents")
            else:
                logger.error(f"❌ Batch processing failed: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
    
    async def compare_providers(self):
        """Compare embedding statistics across all providers"""
        logger.info("📊 COMPARING ALL EMBEDDING PROVIDERS")
        logger.info("=" * 60)
        
        try:
            from domains.document_intelligence.factory import get_database_url
            import asyncpg
            
            conn = await asyncpg.connect(get_database_url())
            
            # Get stats for all providers
            provider_stats = await conn.fetch("""
                SELECT embedding_model, 
                       COUNT(*) as total_embeddings,
                       COUNT(DISTINCT extracted_document_id) as documents_with_embeddings,
                       COUNT(DISTINCT section_type) as section_types_covered,
                       MIN(created_at) as first_embedding,
                       MAX(created_at) as last_embedding
                FROM section_embeddings
                GROUP BY embedding_model
                ORDER BY total_embeddings DESC
            """)
            
            if provider_stats:
                logger.info("📋 Provider Comparison:")
                logger.info(f"{'Provider':25} {'Embeddings':>12} {'Documents':>10} {'Types':>6} {'Latest':>20}")
                logger.info("-" * 80)
                
                for stats in provider_stats:
                    provider = stats['embedding_model']
                    embeddings = stats['total_embeddings']
                    documents = stats['documents_with_embeddings']
                    types = stats['section_types_covered']
                    latest = stats['last_embedding'].strftime('%Y-%m-%d %H:%M') if stats['last_embedding'] else 'N/A'
                    
                    logger.info(f"{provider:25} {embeddings:>12,} {documents:>10,} {types:>6} {latest:>20}")
                
                # Show section type coverage by provider
                logger.info(f"\n📋 Section Type Coverage by Provider:")
                section_coverage = await conn.fetch("""
                    SELECT embedding_model, section_type, COUNT(*) as count
                    FROM section_embeddings
                    GROUP BY embedding_model, section_type
                    ORDER BY embedding_model, count DESC
                """)
                
                current_provider = None
                for row in section_coverage:
                    if row['embedding_model'] != current_provider:
                        current_provider = row['embedding_model']
                        logger.info(f"\n   {current_provider}:")
                    
                    logger.info(f"     {row['section_type']:20} {row['count']:,} embeddings")
            else:
                logger.info("📭 No embeddings found in database")
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Provider comparison failed: {e}")


async def main():
    """CLI entry point"""
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_multi_embeddings.py <provider> <command> [options]")
        print("")
        print("Providers:")
        print("  openai              # OpenAI text-embedding-3-small")
        print("  cohere              # Cohere embeddings (placeholder)")
        print("  local               # Local sentence-transformers (placeholder)")
        print("")
        print("Commands:")
        print("  setup                         # Setup database for provider")
        print("  status                        # Show provider status")
        print("  process [max_sections] [company]  # Process sections batch")
        print("  compare                       # Compare all providers")
        print("")
        print("Examples:")
        print("  python cli_multi_embeddings.py openai setup")
        print("  python cli_multi_embeddings.py openai status")
        print("  python cli_multi_embeddings.py openai process 20 Volvo")
        print("  python cli_multi_embeddings.py openai compare")
        print("")
        print("Environment variables:")
        print("  OPENAI_API_KEY                # Required for OpenAI provider")
        print("  COHERE_API_KEY                # Required for Cohere provider")
        return
    
    provider_type = sys.argv[1]
    command = sys.argv[2] if len(sys.argv) > 2 else 'status'
    
    # Prepare provider kwargs
    provider_kwargs = {}
    if provider_type == "openai":
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.error("❌ OPENAI_API_KEY environment variable required")
            return
        provider_kwargs['api_key'] = api_key
    
    elif provider_type == "cohere":
        api_key = os.getenv('COHERE_API_KEY')
        if not api_key:
            logger.error("❌ COHERE_API_KEY environment variable required")
            return
        provider_kwargs['api_key'] = api_key
    
    # Create controller
    controller = MultiEmbeddingController(provider_type, **provider_kwargs)
    
    if not controller.service:
        return
    
    if command == "setup":
        success = await controller.setup_database()
        if success:
            logger.info("🎉 Database setup completed successfully!")
    
    elif command == "status":
        await controller.show_status()
    
    elif command == "process":
        max_sections = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        company_filter = sys.argv[4] if len(sys.argv) > 4 else None
        await controller.process_batch(max_sections, company_filter)
    
    elif command == "compare":
        await controller.compare_providers()
    
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())