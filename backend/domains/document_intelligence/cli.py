#!/usr/bin/env python3
"""
Document Intelligence - CLI for testing the domain
Demonstrates the properly structured domain architecture
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add backend to path
backend_path = str(Path(__file__).parent.parent.parent)
sys.path.append(backend_path)

# Import from the domain modules directly
from domains.document_intelligence.factory import create_document_processing_service
from domains.document_intelligence.database import init_document_intelligence_tables

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def test_document_processing(max_documents: int = 3):
    """Test the document processing pipeline"""
    logger.info("🚀 Starting Document Intelligence Domain Test")
    
    try:
        # Initialize database tables
        await init_document_intelligence_tables()
        
        # Create service with all dependencies injected
        doc_service = create_document_processing_service()
        
        # Discover documents to process
        data_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE"
        file_paths = await doc_service.discover_unprocessed_documents(data_path, max_documents)
        
        if not file_paths:
            logger.error("❌ No PDF files found to process")
            return
        
        logger.info(f"📄 Found {len(file_paths)} documents to process")
        
        # Process documents
        results = await doc_service.process_batch_documents(file_paths, max_concurrent=3)
        
        # Show results
        logger.info(f"📊 Processing Results:")
        logger.info(f"  Total files: {results['total_files']}")
        logger.info(f"  Processed: {results['processed_count']}")
        logger.info(f"  Errors: {results['error_count']}")
        logger.info(f"  Success rate: {results['success_rate']:.1f}%")
        
        # Get overall statistics
        stats = await doc_service.get_processing_statistics()
        logger.info(f"📈 Overall Statistics:")
        logger.info(f"  Total documents: {stats['total_documents']}")
        logger.info(f"  Success rate: {stats['success_rate']:.1f}%")
        
        # Test document retrieval
        if results['document_ids']:
            doc_id = results['document_ids'][0]
            retrieved_doc = await doc_service.get_document_by_id(doc_id)
            if retrieved_doc:
                logger.info(f"📄 Sample retrieved document:")
                logger.info(f"  Company: {retrieved_doc.document_info.company_name}")
                logger.info(f"  Type: {retrieved_doc.document_info.document_type}")
                logger.info(f"  Pages: {retrieved_doc.total_pages}")
                logger.info(f"  Language: {retrieved_doc.language}")
                logger.info(f"  Text length: {retrieved_doc.text_length}")
                logger.info(f"  Chunks: {len(retrieved_doc.chunks)}")
        
        logger.info("✅ Document Intelligence Domain Test Complete!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise


async def main():
    """Main CLI entry point"""
    if len(sys.argv) > 1:
        try:
            max_docs = int(sys.argv[1])
        except ValueError:
            max_docs = 3
    else:
        max_docs = 3
    
    await test_document_processing(max_docs)


if __name__ == "__main__":
    asyncio.run(main())