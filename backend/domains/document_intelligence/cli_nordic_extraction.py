#!/usr/bin/env python3
"""
Nordic Document Extraction CLI - Updated for nordic_documents table

Robust PDF text extraction with pause/resume capabilities
using the existing nordic_documents table for state management.
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime
from time import time

sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.nordic_extraction_service import NordicExtractionService, get_nordic_extraction_statistics
from domains.document_intelligence.factory import create_document_processing_service

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class NordicExtractionController:
    """Controller for Nordic document extraction with pause/resume"""
    
    def __init__(self):
        self.extraction_service = NordicExtractionService()
        self.processing_service = create_document_processing_service()
    
    async def show_extraction_status(self):
        """Display current extraction status"""
        logger.info("📊 NORDIC DOCUMENT EXTRACTION STATUS")
        logger.info("=" * 50)
        
        stats = await get_nordic_extraction_statistics()
        
        overall = stats['overall']
        logger.info(f"📄 Total downloaded documents: {overall['total_downloaded']:,}")
        logger.info(f"⏳ Pending extraction: {overall['pending']:,}")
        logger.info(f"✅ Completed: {overall['completed']:,}")
        logger.info(f"❌ Failed: {overall['failed']:,}")
        logger.info(f"⚙️ Processing: {overall['processing']:,}")
        logger.info(f"⏭️ Skipped: {overall['skipped']:,}")
        
        if overall['total_downloaded'] > 0:
            completion_rate = (overall['completed'] / overall['total_downloaded'] * 100)
            logger.info(f"🎯 Completion Rate: {completion_rate:.1f}%")
        
        if overall['pending'] > 0:
            logger.info(f"\n📋 Next to Extract (by priority):")
            for item in stats['by_priority'][:5]:  # Top 5 priorities
                logger.info(f"   Priority {item['extraction_priority']}: {item['count']:,} documents")
            
            logger.info(f"\n📊 By Document Type (top 5):")
            for item in stats['by_document_type'][:5]:
                logger.info(f"   {item['document_type']}: {item['count']:,}")
            
            logger.info(f"\n📅 By Year (priority 1-2 only):")
            for item in stats['by_year']:
                logger.info(f"   {item['year']}: {item['count']:,}")
        
        # Performance stats
        perf = stats.get('performance', {})
        if perf.get('avg_duration_seconds'):
            logger.info(f"\n⚡ Performance (completed extractions):")
            logger.info(f"   Avg duration: {perf['avg_duration_seconds']:.1f} seconds")
            logger.info(f"   Avg text length: {perf['avg_text_length']:,.0f} characters")
            if perf.get('avg_confidence'):
                logger.info(f"   Avg confidence: {perf['avg_confidence']:.2f}")
            if perf.get('last_extraction'):
                logger.info(f"   Last extraction: {perf['last_extraction']}")
    
    async def extract_batch(
        self, 
        batch_size: int = 100,
        priority_filter: int = None,
        document_type_filter: str = None,
        year_filter: int = None
    ):
        """Extract a batch of documents with pause/resume capabilities"""
        logger.info("⚙️ NORDIC DOCUMENT EXTRACTION")
        logger.info("=" * 50)
        
        # Get documents to process
        documents_to_process = await self.extraction_service.get_next_extraction_batch(
            batch_size=batch_size,
            priority_filter=priority_filter,
            document_type_filter=document_type_filter,
            year_filter=year_filter
        )
        
        if not documents_to_process:
            logger.info("🎉 No more documents to extract!")
            return
        
        logger.info(f"📋 Extracting text from {len(documents_to_process)} documents...")
        
        # Mark as processing
        document_ids = [doc['id'] for doc in documents_to_process]
        await self.extraction_service.mark_extraction_processing(document_ids)
        
        processed_count = 0
        failed_count = 0
        
        try:
            for i, doc in enumerate(documents_to_process, 1):
                logger.info(f"⚙️ Processing {i}/{len(documents_to_process)}: {doc['company_name']} - {doc['document_type']} ({doc['report_period']})")
                
                start_time = time()
                
                try:
                    # Process single document using storage_path
                    filing_id = await self.processing_service.process_single_document(
                        doc['storage_path']
                    )
                    
                    extraction_duration = int(time() - start_time)
                    
                    if filing_id:
                        # Mark as completed with performance metrics
                        await self.extraction_service.mark_extraction_completed(
                            document_id=doc['id'],
                            filing_id=filing_id,
                            text_length=0,  # Will be updated by processing service
                            extraction_duration=extraction_duration,
                            confidence=0.95  # Default confidence for successful extraction
                        )
                        processed_count += 1
                        logger.info(f"   ✅ Success ({extraction_duration}s)")
                    else:
                        # Mark as failed
                        await self.extraction_service.mark_extraction_failed(
                            document_id=doc['id'],
                            error_message="Processing returned None"
                        )
                        failed_count += 1
                        logger.info(f"   ❌ Failed (no result)")
                
                except FileNotFoundError as e:
                    # Mark as permanently failed for missing files
                    await self.extraction_service.mark_extraction_failed(
                        document_id=doc['id'],
                        error_message=f"File not found: {str(e)}",
                        is_permanent=True
                    )
                    failed_count += 1
                    logger.error(f"   ❌ File not found: {e}")
                    
                except Exception as e:
                    # Mark as failed with error
                    await self.extraction_service.mark_extraction_failed(
                        document_id=doc['id'],
                        error_message=str(e)
                    )
                    failed_count += 1
                    logger.error(f"   ❌ Error: {e}")
        
        except KeyboardInterrupt:
            logger.info("⏸️ Extraction interrupted by user")
            # Documents still marked as 'processing' can be reset later
        
        logger.info(f"📊 Batch Results:")
        logger.info(f"   ✅ Extracted: {processed_count}")
        logger.info(f"   ❌ Failed: {failed_count}")
        
        # Show updated status
        await self.show_extraction_status()
    
    async def preview_extraction_queue(self, limit: int = 20):
        """Preview next documents in the extraction queue"""
        logger.info("🔍 EXTRACTION QUEUE PREVIEW")
        logger.info("=" * 50)
        
        queue = await self.extraction_service.get_extraction_queue_preview(limit)
        
        if not queue:
            logger.info("📭 No documents in extraction queue")
            return
        
        logger.info(f"📋 Next {len(queue)} documents to extract:")
        logger.info("")
        
        for i, doc in enumerate(queue, 1):
            attempts_info = f" (attempt {doc['extraction_attempts']+1})" if doc['extraction_attempts'] > 0 else ""
            logger.info(f"  {i:2d}. P{doc['extraction_priority']} | {doc['year']} | {doc['company_name']} | {doc['document_type']}{attempts_info}")
            if i <= 5:  # Show paths for first 5
                logger.info(f"      📁 {doc['storage_path_short']}")
    
    async def reset_failed_extractions(self, max_attempts: int = 2):
        """Reset failed extractions for retry"""
        logger.info("🔄 RESET FAILED EXTRACTIONS")
        logger.info("=" * 50)
        
        reset_count = await self.extraction_service.reset_failed_extractions(
            max_attempts=max_attempts
        )
        
        logger.info(f"✅ Reset {reset_count} failed extractions for retry")
        
        if reset_count > 0:
            logger.info("These documents will be retried in the next extraction batch")
    
    async def update_version(self, new_version: str, reprocess: bool = False, priority_filter: int = None):
        """Update extraction version and optionally reprocess documents"""
        logger.info("🔄 UPDATE EXTRACTION VERSION")
        logger.info("=" * 50)
        
        updated_count = await self.extraction_service.update_extraction_version(
            new_version=new_version,
            reprocess_completed=reprocess,
            priority_filter=priority_filter
        )
        
        action = "updated and marked for reprocessing" if reprocess else "updated"
        filter_info = f" (priority <= {priority_filter})" if priority_filter else ""
        
        logger.info(f"✅ {updated_count:,} documents {action} to version {new_version}{filter_info}")


async def main():
    """CLI entry point"""
    controller = NordicExtractionController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_nordic_extraction.py status                    # Show extraction status")
        print("  python cli_nordic_extraction.py extract [batch_size]      # Extract next batch")
        print("  python cli_nordic_extraction.py preview [limit]           # Preview extraction queue")
        print("  python cli_nordic_extraction.py reset-failed [max_attempts] # Reset failed extractions")
        print("  python cli_nordic_extraction.py version <version> [--reprocess] [--priority=N] # Update version")
        print("")
        print("Examples:")
        print("  python cli_nordic_extraction.py extract 50               # Extract 50 documents")
        print("  python cli_nordic_extraction.py extract 100 --priority=2 # Extract 100 priority 1-2 documents")
        print("  python cli_nordic_extraction.py version v1.1 --reprocess # Update to v1.1 and reprocess all")
        return
    
    command = sys.argv[1]
    
    if command == "status":
        await controller.show_extraction_status()
        
    elif command == "extract":
        batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        
        # Parse additional arguments
        priority_filter = None
        for arg in sys.argv[3:]:
            if arg.startswith('--priority='):
                priority_filter = int(arg.split('=')[1])
        
        await controller.extract_batch(
            batch_size=batch_size,
            priority_filter=priority_filter
        )
        
    elif command == "preview":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        await controller.preview_extraction_queue(limit)
        
    elif command == "reset-failed":
        max_attempts = int(sys.argv[2]) if len(sys.argv) > 2 else 2
        await controller.reset_failed_extractions(max_attempts)
        
    elif command == "version":
        if len(sys.argv) < 3:
            print("Error: version command requires a version string")
            return
        
        new_version = sys.argv[2]
        reprocess = '--reprocess' in sys.argv
        
        priority_filter = None
        for arg in sys.argv[3:]:
            if arg.startswith('--priority='):
                priority_filter = int(arg.split('=')[1])
        
        await controller.update_version(
            new_version=new_version,
            reprocess=reprocess,
            priority_filter=priority_filter
        )
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())