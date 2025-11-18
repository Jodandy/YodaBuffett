#!/usr/bin/env python3
"""
Section-Based Embeddings CLI

Test and run the intelligent section-based embedding generation system.
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.section_embedding_service import SectionEmbeddingService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SectionEmbeddingController:
    """Controller for section-based embedding operations"""
    
    def __init__(self):
        # Get OpenAI API key from environment
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("⚠️  No OPENAI_API_KEY found - section embeddings will not work")
        
        self.service = SectionEmbeddingService(api_key=api_key)
    
    async def setup_database(self):
        """Setup database schema for section embeddings"""
        logger.info("🔧 SETTING UP SECTION EMBEDDINGS DATABASE")
        logger.info("=" * 50)
        
        try:
            await self.service.setup_section_embeddings_table()
            logger.info("✅ Section embeddings database setup complete")
            return True
        except Exception as e:
            logger.error(f"❌ Database setup failed: {e}")
            return False
    
    async def show_status(self):
        """Display current section embedding status"""
        logger.info("📊 SECTION EMBEDDINGS STATUS")
        logger.info("=" * 50)
        
        try:
            stats = await self.service.get_section_embedding_statistics()
            
            overall = stats['overall']
            if overall:
                total_docs = overall.get('total_documents', 0)
                section_embedded_docs = overall.get('documents_with_section_embeddings', 0)
                total_sections = overall.get('total_section_embeddings', 0)
                avg_confidence = overall.get('avg_section_confidence', 0)
                avg_length = overall.get('avg_section_length', 0)
                
                logger.info(f"📄 Total documents: {total_docs:,}")
                logger.info(f"🧩 Documents with section embeddings: {section_embedded_docs:,}")
                logger.info(f"📋 Total section embeddings: {total_sections:,}")
                
                if total_docs > 0:
                    completion_rate = (section_embedded_docs / total_docs) * 100
                    logger.info(f"📊 Section embedding completion rate: {completion_rate:.1f}%")
                
                if avg_confidence:
                    logger.info(f"🎯 Average section confidence: {avg_confidence:.2f}")
                
                if avg_length:
                    logger.info(f"📏 Average section length: {avg_length:,.0f} characters")
            
            # Show section type distribution
            if stats['by_section_type']:
                logger.info(f"\n📋 Section Type Distribution:")
                for section_info in stats['by_section_type']:
                    section_type = section_info['section_type']
                    count = section_info['count']
                    confidence = section_info['avg_confidence']
                    length = section_info['avg_length']
                    logger.info(f"   {section_type:20} {count:5,} sections (confidence: {confidence:.2f}, avg length: {length:,.0f})")
            
            # Show pending documents
            if stats['pending_documents']:
                logger.info(f"\n📝 Documents Pending Section Embedding (showing first 5):")
                for i, doc in enumerate(stats['pending_documents'][:5]):
                    logger.info(f"   {doc['company_name']:25} {doc['form_type']:15} {doc['year']} ({doc['text_length']:,} chars)")
                
                remaining = len(stats['pending_documents']) - 5
                if remaining > 0:
                    logger.info(f"   ... and {remaining} more documents")
            
        except Exception as e:
            logger.error(f"❌ Failed to get status: {e}")
    
    async def test_single_document(self, company_filter: str = None):
        """Test section embedding on a single document"""
        logger.info("🧪 TESTING SECTION EMBEDDINGS ON SINGLE DOCUMENT")
        logger.info("=" * 50)
        
        try:
            # Get one document to test
            documents = await self.service.get_documents_for_section_embedding(
                limit=1, 
                company_filter=company_filter
            )
            
            if not documents:
                logger.info("📭 No documents found for testing")
                return
            
            doc = documents[0]
            logger.info(f"🎯 Testing: {doc['company_name']} - {doc['form_type']} ({doc['year']})")
            logger.info(f"📏 Document length: {doc['text_length']:,} characters")
            
            # Process the document
            metadata = {
                'company_name': doc['company_name'],
                'form_type': doc['form_type'],
                'year': doc['year']
            }
            
            result = await self.service.parse_and_embed_document(
                doc['id'], 
                doc['extracted_text'], 
                metadata
            )
            
            if result['success']:
                logger.info(f"\n✅ Section Embedding Test Complete!")
                logger.info(f"   📋 Sections parsed: {result['sections_parsed']}")
                logger.info(f"   🧩 Sections embedded: {result['sections_embedded']}")
                logger.info(f"   💰 Financial statements found: {', '.join(result['financial_statements_found'])}")
                logger.info(f"   ⏱️  Processing time: {result['processing_time_seconds']} seconds")
                logger.info(f"   💵 Estimated cost: ${result['estimated_cost_usd']:.6f}")
                
                # Show section details
                if result['parsing_details']:
                    logger.info(f"\n📋 Section Breakdown:")
                    for section_type, sections in result['parsing_details'].items():
                        for section in sections:
                            logger.info(f"   {section_type:20} '{section['title'][:40]}...' ({section['length']:,} chars, confidence: {section['confidence']:.2f})")
            else:
                logger.error(f"❌ Test failed: {result.get('reason', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
    
    async def process_batch(self, max_documents: int = 3, company_filter: str = None, dry_run: bool = True):
        """Process a batch of documents for section embedding"""
        logger.info("🚀 PROCESSING SECTION EMBEDDINGS BATCH")
        logger.info("=" * 50)
        
        if dry_run:
            logger.info("🧪 DRY RUN MODE - No database updates will be made")
        
        try:
            result = await self.service.process_documents_batch(
                max_documents=max_documents,
                company_filter=company_filter
            )
            
            if result['success']:
                summary = result['batch_summary']
                logger.info(f"\n✅ Batch Processing Complete!")
                logger.info(f"   📄 Documents attempted: {summary['documents_attempted']}")
                logger.info(f"   ✅ Documents successful: {summary['documents_successful']}")
                logger.info(f"   🧩 Total sections embedded: {summary['total_sections_embedded']}")
                logger.info(f"   💵 Total cost: ${summary['total_cost_usd']:.6f}")
                logger.info(f"   ⏱️  Total time: {summary['batch_time_seconds']} seconds")
                logger.info(f"   ⚡ Avg time per document: {summary['avg_time_per_document']} seconds")
                
                # Show individual document results
                logger.info(f"\n📋 Individual Document Results:")
                for doc_result in result['detailed_results']:
                    if doc_result['success']:
                        logger.info(f"   ✅ {doc_result['document_id']}: {doc_result['sections_embedded']} sections (${doc_result['estimated_cost_usd']:.6f})")
                    else:
                        logger.info(f"   ❌ {doc_result['document_id']}: {doc_result.get('error', 'Failed')}")
            else:
                logger.error(f"❌ Batch processing failed: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
    
    async def search_sections(self, query: str, section_type: str = None, limit: int = 3):
        """Search for similar financial sections using vector similarity"""
        logger.info("🔍 SEARCHING FINANCIAL SECTIONS")
        logger.info("=" * 50)
        
        try:
            logger.info(f"🎯 Query: '{query}'")
            if section_type:
                logger.info(f"📋 Section type filter: {section_type}")
            
            results = await self.service.find_similar_sections(
                query_text=query,
                section_type=section_type,
                limit=limit
            )
            
            if results:
                logger.info(f"\n📋 Found {len(results)} similar sections:")
                for i, result in enumerate(results, 1):
                    similarity = result['similarity_score']
                    company = result['company_name']
                    form_type = result['form_type']
                    year = result['year']
                    sec_type = result['section_type']
                    confidence = result['section_confidence']
                    
                    logger.info(f"\n   {i}. {company} - {form_type} ({year})")
                    logger.info(f"      Section: {sec_type} (confidence: {confidence:.2f})")
                    logger.info(f"      Similarity: {similarity:.3f}")
                    logger.info(f"      Preview: {result['content_preview']}")
            else:
                logger.info("📭 No similar sections found")
                
        except Exception as e:
            logger.error(f"❌ Search failed: {e}")


async def main():
    """CLI entry point"""
    controller = SectionEmbeddingController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_section_embeddings.py setup                          # Setup database schema")
        print("  python cli_section_embeddings.py status                         # Show current status")
        print("  python cli_section_embeddings.py test [company]                 # Test on single document")
        print("  python cli_section_embeddings.py process [max_docs] [company]   # Process batch")
        print("  python cli_section_embeddings.py search 'query' [section_type]  # Search sections")
        print("")
        print("Examples:")
        print("  python cli_section_embeddings.py test Volvo")
        print("  python cli_section_embeddings.py process 3 Ericsson")
        print("  python cli_section_embeddings.py search 'revenue growth' income_statement")
        return
    
    command = sys.argv[1]
    
    if command == "setup":
        success = await controller.setup_database()
        if success:
            logger.info("🎉 Database setup completed successfully!")
        
    elif command == "status":
        await controller.show_status()
        
    elif command == "test":
        company_filter = sys.argv[2] if len(sys.argv) > 2 else None
        await controller.test_single_document(company_filter)
        
    elif command == "process":
        max_docs = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        company_filter = sys.argv[3] if len(sys.argv) > 3 else None
        dry_run = '--dry-run' in sys.argv or True  # Default to dry run for safety
        
        if '--run' in sys.argv:
            dry_run = False
            
        await controller.process_batch(max_docs, company_filter, dry_run)
        
    elif command == "search":
        if len(sys.argv) < 3:
            print("Error: Search query required")
            return
            
        query = sys.argv[2]
        section_type = sys.argv[3] if len(sys.argv) > 3 else None
        await controller.search_sections(query, section_type)
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())