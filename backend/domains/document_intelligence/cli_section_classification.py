#!/usr/bin/env python3
"""
Financial Report Section Classification CLI

Test and run OpenAI-based section classification on existing document chunks.
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.section_classifier import FinancialSectionClassifier, add_classification_columns

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SectionClassificationController:
    """Controller for section classification operations"""
    
    def __init__(self):
        # Get OpenAI API key from environment
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            logger.warning("⚠️  No OPENAI_API_KEY found - classification will not work")
        
        self.classifier = FinancialSectionClassifier(api_key=api_key)
    
    async def setup_database(self):
        """Setup database schema for section classification"""
        logger.info("🔧 SETTING UP DATABASE SCHEMA")
        logger.info("=" * 40)
        
        try:
            await add_classification_columns()
            logger.info("✅ Database schema setup complete")
            return True
        except Exception as e:
            logger.error(f"❌ Database setup failed: {e}")
            return False
    
    async def show_status(self):
        """Display current classification status"""
        logger.info("📊 SECTION CLASSIFICATION STATUS")
        logger.info("=" * 40)
        
        try:
            stats = await self.classifier.get_classification_statistics()
            
            overall = stats['overall']
            if overall:
                total = overall.get('total_chunks', 0)
                classified = overall.get('classified_chunks', 0)
                unclassified = overall.get('unclassified_chunks', 0)
                avg_confidence = overall.get('avg_confidence', 0)
                
                logger.info(f"📄 Total chunks: {total:,}")
                logger.info(f"✅ Classified chunks: {classified:,}")
                logger.info(f"📝 Unclassified chunks: {unclassified:,}")
                
                if total > 0:
                    completion_rate = (classified / total) * 100
                    logger.info(f"📊 Completion rate: {completion_rate:.1f}%")
                
                if avg_confidence:
                    logger.info(f"🎯 Average confidence: {avg_confidence:.2f}")
            
            # Show section distribution
            if stats['section_distribution']:
                logger.info(f"\n📋 Section Distribution:")
                for section_info in stats['section_distribution'][:10]:  # Top 10
                    section_type = section_info['section_type']
                    count = section_info['count']
                    confidence = section_info['avg_confidence']
                    logger.info(f"   {section_type:20} {count:5,} chunks (avg confidence: {confidence:.2f})")
                    
                if len(stats['section_distribution']) > 10:
                    remaining = len(stats['section_distribution']) - 10
                    logger.info(f"   ... and {remaining} more section types")
            
        except Exception as e:
            logger.error(f"❌ Failed to get status: {e}")
    
    async def test_classification(self, limit: int = 3, company_filter: str = None):
        """Test classification on a few unclassified chunks"""
        logger.info("🧪 TESTING SECTION CLASSIFICATION")
        logger.info("=" * 40)
        
        try:
            chunks = await self.classifier.get_unclassified_chunks(
                limit=limit, 
                document_filter=company_filter
            )
            
            if not chunks:
                logger.info("📭 No unclassified chunks found for testing")
                return
            
            logger.info(f"🎯 Testing classification on {len(chunks)} chunks")
            
            total_cost = 0.0
            
            for i, chunk in enumerate(chunks, 1):
                logger.info(f"\n🔍 Chunk {i}/{len(chunks)}:")
                logger.info(f"   📄 Document: {chunk['company_name']} - {chunk['form_type']}")
                logger.info(f"   📍 Chunk {chunk['chunk_index']} (pages: {chunk.get('page_numbers', [])})")
                logger.info(f"   📝 Text preview: {chunk['chunk_text'][:200]}...")
                
                # Build context
                context = {
                    "company": chunk['company_name'],
                    "document_type": chunk['form_type'],
                    "year": chunk['year'],
                    "chunk_index": chunk['chunk_index']
                }
                
                # Classify
                section_type, confidence, reasoning = await self.classifier.classify_chunk(
                    chunk['chunk_text'], 
                    context
                )
                
                logger.info(f"   🎯 Classification: {section_type}")
                logger.info(f"   📊 Confidence: {confidence:.2f}")
                logger.info(f"   💭 Reasoning: {reasoning}")
                
                # Estimate cost
                estimated_cost = 200 * 0.00000015  # ~$0.00003 per classification
                total_cost += estimated_cost
                
                # Small delay for rate limiting
                if i < len(chunks):
                    await asyncio.sleep(0.5)
            
            logger.info(f"\n💰 Total estimated cost: ${total_cost:.6f}")
            
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
    
    async def classify_document(self, document_id: str = None, company_filter: str = None, dry_run: bool = True):
        """Classify all chunks in a specific document or find one to classify"""
        logger.info("🚀 DOCUMENT CLASSIFICATION")
        logger.info("=" * 40)
        
        try:
            if not document_id:
                # Find a document to classify
                chunks = await self.classifier.get_unclassified_chunks(
                    limit=1, 
                    document_filter=company_filter
                )
                
                if not chunks:
                    logger.info("📭 No unclassified documents found")
                    return
                
                document_id = chunks[0]['extracted_document_id']
                logger.info(f"🎯 Found document to classify: {chunks[0]['company_name']} - {chunks[0]['form_type']}")
            
            if dry_run:
                logger.info("🧪 DRY RUN MODE - No database updates will be made")
            
            # Classify all chunks in the document
            result = await self.classifier.classify_document_chunks(
                document_id, 
                dry_run=dry_run
            )
            
            if result['success']:
                logger.info(f"\n✅ Classification Complete!")
                logger.info(f"   📄 Document: {document_id}")
                logger.info(f"   🔢 Chunks classified: {result['chunks_classified']}")
                logger.info(f"   💰 Estimated cost: ${result['estimated_cost']:.6f}")
                
                # Show section breakdown
                section_counts = {}
                total_confidence = 0.0
                
                for chunk_result in result['results']:
                    section_type = chunk_result['section_type']
                    confidence = chunk_result['confidence']
                    
                    section_counts[section_type] = section_counts.get(section_type, 0) + 1
                    total_confidence += confidence
                
                avg_confidence = total_confidence / len(result['results'])
                
                logger.info(f"\n📋 Section Breakdown:")
                for section_type, count in sorted(section_counts.items()):
                    logger.info(f"   {section_type:20} {count:3} chunks")
                
                logger.info(f"\n📊 Average confidence: {avg_confidence:.2f}")
                
                # Show a few examples
                logger.info(f"\n🔍 Sample Classifications:")
                for chunk_result in result['results'][:3]:
                    logger.info(f"   Chunk {chunk_result['chunk_index']:2}: {chunk_result['section_type']:20} (confidence: {chunk_result['confidence']:.2f})")
                
            else:
                logger.error(f"❌ Classification failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Document classification failed: {e}")


async def main():
    """CLI entry point"""
    controller = SectionClassificationController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_section_classification.py setup                    # Setup database schema")
        print("  python cli_section_classification.py status                   # Show classification status")
        print("  python cli_section_classification.py test [limit] [company]   # Test classification")
        print("  python cli_section_classification.py classify [company] [--dry-run]  # Classify document")
        print("")
        print("Examples:")
        print("  python cli_section_classification.py test 5 Volvo")
        print("  python cli_section_classification.py classify Ericsson --dry-run")
        print("  python cli_section_classification.py classify Volvo")
        return
    
    command = sys.argv[1]
    
    if command == "setup":
        success = await controller.setup_database()
        if success:
            logger.info("🎉 Database setup completed successfully!")
        
    elif command == "status":
        await controller.show_status()
        
    elif command == "test":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 3
        company_filter = sys.argv[3] if len(sys.argv) > 3 else None
        await controller.test_classification(limit, company_filter)
        
    elif command == "classify":
        company_filter = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else None
        dry_run = '--dry-run' in sys.argv or True  # Default to dry run for safety
        
        # Override dry run if user explicitly wants to run for real
        if '--run' in sys.argv:
            dry_run = False
            
        await controller.classify_document(
            company_filter=company_filter, 
            dry_run=dry_run
        )
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())