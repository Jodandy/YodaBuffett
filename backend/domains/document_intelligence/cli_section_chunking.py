#!/usr/bin/env python3
"""
Section Chunking CLI

Test and run the intelligent financial section chunking system.
Creates meaningful document sections that can then be embedded by any provider.
"""

import asyncio
import logging
import sys
import os
import json
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.section_chunking_service import SectionChunkingService
import asyncpg
from domains.document_intelligence.factory import get_database_url

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SectionChunkingController:
    """Controller for section chunking operations"""
    
    def __init__(self):
        self.service = SectionChunkingService()
    
    async def setup_database(self):
        """Setup database schema for document sections"""
        logger.info("🔧 SETTING UP DOCUMENT SECTIONS DATABASE")
        logger.info("=" * 50)
        
        try:
            await self.service.setup_sections_table()
            logger.info("✅ Document sections database setup complete")
            return True
        except Exception as e:
            logger.error(f"❌ Database setup failed: {e}")
            return False
    
    async def show_status(self):
        """Display current section chunking status"""
        logger.info("📊 SECTION CHUNKING STATUS")
        logger.info("=" * 50)
        
        try:
            stats = await self.service.get_sectioning_statistics()
            
            overall = stats['overall']
            if overall:
                total_docs = overall.get('total_documents', 0)
                sectioned_docs = overall.get('documents_with_sections', 0)
                total_sections = overall.get('total_sections', 0)
                avg_confidence = overall.get('avg_section_confidence', 0)
                avg_length = overall.get('avg_section_length', 0)
                
                logger.info(f"📄 Total documents: {total_docs:,}")
                logger.info(f"🧩 Documents with sections: {sectioned_docs:,}")
                logger.info(f"📋 Total sections: {total_sections:,}")
                
                if total_docs > 0:
                    completion_rate = (sectioned_docs / total_docs) * 100
                    logger.info(f"📊 Section chunking completion rate: {completion_rate:.1f}%")
                
                if sectioned_docs > 0:
                    avg_sections_per_doc = total_sections / sectioned_docs
                    logger.info(f"📈 Average sections per document: {avg_sections_per_doc:.1f}")
                
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
                logger.info(f"\n📝 Documents Pending Section Chunking (showing first 5):")
                for i, doc in enumerate(stats['pending_documents'][:5]):
                    logger.info(f"   {doc['company_name']:25} {doc['form_type']:15} {doc['year']} ({doc['text_length']:,} chars)")
                
                remaining = len(stats['pending_documents']) - 5
                if remaining > 0:
                    logger.info(f"   ... and {remaining} more documents")
            
        except Exception as e:
            logger.error(f"❌ Failed to get status: {e}")
    
    async def test_single_document(self, company_filter: str = None):
        """Test section chunking on a single document"""
        logger.info("🧪 TESTING SECTION CHUNKING ON SINGLE DOCUMENT")
        logger.info("=" * 50)
        
        try:
            # Get one document to test
            documents = await self.service.get_documents_for_sectioning(
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
            
            result = await self.service.chunk_document_into_sections(
                doc['id'], 
                doc['extracted_text'], 
                metadata
            )
            
            if result['success']:
                logger.info(f"\n✅ Section Chunking Test Complete!")
                logger.info(f"   📋 Sections parsed: {result['sections_parsed']}")
                logger.info(f"   💾 Sections stored: {result['sections_stored']}")
                logger.info(f"   💰 Financial statements found: {', '.join(result['financial_statements_found'])}")
                logger.info(f"   ⏱️  Processing time: {result['processing_time_seconds']} seconds")
                
                # Show section details
                if result['parsing_details']:
                    logger.info(f"\n📋 Section Breakdown:")
                    for section_type, sections in result['parsing_details'].items():
                        for section in sections:
                            logger.info(f"   {section_type:20} '{section['title'][:40]}...' ({section['length']:,} chars, confidence: {section['confidence']:.2f})")
                
                # Get and display the stored sections
                sections = await self.service.get_document_sections(doc['id'])
                logger.info(f"\n📄 Stored Sections for {doc['company_name']}:")
                for section in sections:
                    content_preview = section['section_content'][:100].replace('\n', ' ')
                    logger.info(f"   {section['section_index']:2}. {section['section_type']:20} ({section['content_length']:,} chars)")
                    logger.info(f"      Title: '{section['section_title']}'")
                    logger.info(f"      Preview: {content_preview}...")
                    
            else:
                logger.error(f"❌ Test failed: {result.get('reason', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Test failed: {e}")
    
    async def process_batch(self, max_documents: int = 3, company_filter: str = None):
        """Process a batch of documents for section chunking"""
        logger.info("🚀 PROCESSING SECTION CHUNKING BATCH")
        logger.info("=" * 50)
        
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
                logger.info(f"   🧩 Total sections created: {summary['total_sections_created']}")
                logger.info(f"   ⏱️  Total time: {summary['batch_time_seconds']} seconds")
                logger.info(f"   ⚡ Avg time per document: {summary['avg_time_per_document']} seconds")
                
                # Show individual document results
                logger.info(f"\n📋 Individual Document Results:")
                for doc_result in result['detailed_results']:
                    if doc_result['success']:
                        statements = ', '.join(doc_result['financial_statements_found']) if doc_result['financial_statements_found'] else 'None'
                        logger.info(f"   ✅ {doc_result['document_id'][:8]}...: {doc_result['sections_stored']} sections")
                        logger.info(f"      Financial statements: {statements}")
                    else:
                        logger.info(f"   ❌ {doc_result['document_id'][:8]}...: {doc_result.get('error', 'Failed')}")
            else:
                logger.error(f"❌ Batch processing failed: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"❌ Batch processing failed: {e}")
    
    async def inspect_document_sections(self, company_filter: str = None):
        """Inspect sections of a specific document"""
        logger.info("🔍 INSPECTING DOCUMENT SECTIONS")
        logger.info("=" * 50)
        
        try:
            # Find a document with sections
            stats = await self.service.get_sectioning_statistics()
            
            # For now, we'll need to modify this to get a specific document ID
            # This is a simplified version - in practice you'd want to search by company
            conn = await asyncpg.connect(get_database_url())
            
            where_clause = ""
            params = []
            if company_filter:
                where_clause = "AND ed.company_name ILIKE $1"
                params.append(f"%{company_filter}%")
            
            doc = await conn.fetchrow(f"""
                SELECT ed.id, ed.company_name, ed.form_type, ed.year,
                       COUNT(ds.id) as section_count
                FROM extracted_documents ed
                JOIN document_sections ds ON ed.id = ds.extracted_document_id
                {where_clause}
                GROUP BY ed.id, ed.company_name, ed.form_type, ed.year
                ORDER BY COUNT(ds.id) DESC
                LIMIT 1
            """, *params)
            
            await conn.close()
            
            if not doc:
                logger.info("📭 No documents with sections found")
                return
            
            logger.info(f"📄 Document: {doc['company_name']} - {doc['form_type']} ({doc['year']})")
            logger.info(f"🧩 Total sections: {doc['section_count']}")
            
            # Get detailed sections
            sections = await self.service.get_document_sections(doc['id'])
            
            logger.info(f"\n📋 Detailed Section Breakdown:")
            for section in sections:
                logger.info(f"\n   {section['section_index']:2}. {section['section_type']:20} (confidence: {section['section_confidence']:.2f})")
                logger.info(f"       Title: '{section['section_title']}'")
                logger.info(f"       Length: {section['content_length']:,} characters")
                logger.info(f"       Position: {section['section_start_pos']:,} - {section['section_end_pos']:,}")
                
                # Show content preview
                content_lines = section['section_content'][:300].split('\n')[:3]
                for line in content_lines:
                    if line.strip():
                        logger.info(f"       Preview: {line.strip()}")
                        break
                
        except Exception as e:
            logger.error(f"❌ Inspection failed: {e}")


async def main():
    """CLI entry point"""
    controller = SectionChunkingController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_section_chunking.py setup                          # Setup database schema")
        print("  python cli_section_chunking.py status                         # Show current status")
        print("  python cli_section_chunking.py test [company]                 # Test on single document")
        print("  python cli_section_chunking.py process [max_docs] [company]   # Process batch")
        print("  python cli_section_chunking.py inspect [company]              # Inspect document sections")
        print("")
        print("Examples:")
        print("  python cli_section_chunking.py test Volvo")
        print("  python cli_section_chunking.py process 3 Ericsson") 
        print("  python cli_section_chunking.py inspect Volvo")
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
        await controller.process_batch(max_docs, company_filter)
        
    elif command == "inspect":
        company_filter = sys.argv[2] if len(sys.argv) > 2 else None
        await controller.inspect_document_sections(company_filter)
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())