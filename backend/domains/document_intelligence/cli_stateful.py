#!/usr/bin/env python3
"""
Stateful Document Processing CLI - Discover, Process, Pause, Resume
"""

import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent.parent))

from domains.document_intelligence.services.document_discovery import DocumentDiscoveryService, get_processing_statistics
from domains.document_intelligence.factory import create_document_processing_service
from domains.document_intelligence.services.document_processor import DocumentProcessingService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StatefulProcessingController:
    """Controller for stateful document processing with pause/resume"""
    
    def __init__(self):
        self.discovery_service = DocumentDiscoveryService()
        self.processing_service = create_document_processing_service()
    
    async def discover_documents(self, data_path: str, max_documents: int = None):
        """Phase 1: Discover and catalog all documents"""
        batch_id = f"discovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info("🔍 PHASE 1: DOCUMENT DISCOVERY")
        logger.info("=" * 50)
        
        result = await self.discovery_service.discover_all_documents(
            data_path=data_path,
            batch_id=batch_id,
            max_documents=max_documents
        )
        
        logger.info(f"✅ Discovery Results:")
        logger.info(f"   📄 Total files found: {result['total_found']}")
        logger.info(f"   ➕ New documents cataloged: {result['discovered']}")
        logger.info(f"   ⏭️  Already cataloged: {result['skipped']}")
        logger.info(f"   ❌ Errors: {result['errors']}")
        logger.info(f"   🏷️  Batch ID: {result['batch_id']}")
        
        # Show processing queue
        await self._show_processing_queue()
        
        return result
    
    async def process_batch(
        self, 
        batch_size: int = 10,
        priority_filter: int = 5,  # Only process priority <= 5
        region_filter: str = None,
        document_type_filter: str = None
    ):
        """Phase 2: Process a batch of discovered documents"""
        logger.info("⚙️ PHASE 2: BATCH PROCESSING")
        logger.info("=" * 50)
        
        # Get documents to process
        documents_to_process = await self._get_next_batch(
            batch_size=batch_size,
            priority_filter=priority_filter,
            region_filter=region_filter,
            document_type_filter=document_type_filter
        )
        
        if not documents_to_process:
            logger.info("🎉 No more documents to process!")
            return
        
        logger.info(f"📋 Processing {len(documents_to_process)} documents...")
        
        # Mark as processing
        await self._mark_documents_processing(documents_to_process)
        
        processed_count = 0
        failed_count = 0
        
        try:
            for i, doc_state in enumerate(documents_to_process, 1):
                logger.info(f"⚙️ Processing {i}/{len(documents_to_process)}: {doc_state['company_name']} - {doc_state['document_type']}")
                
                try:
                    # Process single document
                    filing_id = await self.processing_service.process_single_document(
                        doc_state['file_path']
                    )
                    
                    if filing_id:
                        # Mark as completed
                        await self._mark_document_completed(doc_state['id'], filing_id)
                        processed_count += 1
                        logger.info(f"   ✅ Success")
                    else:
                        # Mark as failed
                        await self._mark_document_failed(doc_state['id'], "Processing returned None")
                        failed_count += 1
                        logger.info(f"   ❌ Failed")
                
                except Exception as e:
                    # Mark as failed with error
                    await self._mark_document_failed(doc_state['id'], str(e))
                    failed_count += 1
                    logger.error(f"   ❌ Error: {e}")
        
        except KeyboardInterrupt:
            logger.info("⏸️ Processing interrupted by user")
            # Mark remaining documents back to 'discovered'
            await self._mark_documents_discovered([
                doc for doc in documents_to_process 
                if doc not in documents_to_process[:processed_count + failed_count]
            ])
        
        logger.info(f"📊 Batch Results:")
        logger.info(f"   ✅ Processed: {processed_count}")
        logger.info(f"   ❌ Failed: {failed_count}")
        
        # Show updated queue
        await self._show_processing_queue()
    
    async def _get_next_batch(
        self,
        batch_size: int,
        priority_filter: int,
        region_filter: str,
        document_type_filter: str
    ) -> list:
        """Get next batch of documents to process"""
        import asyncpg
        from domains.document_intelligence.factory import get_database_url
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Build query with filters
            where_conditions = ["processing_status = 'discovered'"]
            params = []
            
            if priority_filter:
                where_conditions.append(f"processing_priority <= ${len(params) + 1}")
                params.append(priority_filter)
            
            if region_filter:
                where_conditions.append(f"region = ${len(params) + 1}")
                params.append(region_filter)
            
            if document_type_filter:
                where_conditions.append(f"document_type = ${len(params) + 1}")
                params.append(document_type_filter)
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
                SELECT id, file_path, company_name, document_type, processing_priority
                FROM document_processing_state 
                WHERE {where_clause}
                ORDER BY processing_priority ASC, year DESC
                LIMIT ${len(params) + 1}
            """
            params.append(batch_size)
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def _mark_documents_processing(self, documents: list):
        """Mark documents as currently processing"""
        import asyncpg
        from domains.document_intelligence.factory import get_database_url
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            doc_ids = [doc['id'] for doc in documents]
            await conn.execute("""
                UPDATE document_processing_state 
                SET processing_status = 'processing',
                    last_attempt_at = NOW(),
                    attempt_count = attempt_count + 1
                WHERE id = ANY($1)
            """, doc_ids)
            
        finally:
            await conn.close()
    
    async def _mark_document_completed(self, doc_id: str, filing_id: str):
        """Mark document as successfully completed"""
        import asyncpg
        from domains.document_intelligence.factory import get_database_url
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                UPDATE document_processing_state 
                SET processing_status = 'completed',
                    processed_at = NOW(),
                    filing_id = $2,
                    last_error = NULL
                WHERE id = $1
            """, doc_id, filing_id)
            
        finally:
            await conn.close()
    
    async def _mark_document_failed(self, doc_id: str, error_message: str):
        """Mark document as failed"""
        import asyncpg
        from domains.document_intelligence.factory import get_database_url
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                UPDATE document_processing_state 
                SET processing_status = 'failed',
                    last_error = $2
                WHERE id = $1
            """, doc_id, error_message)
            
        finally:
            await conn.close()
    
    async def _mark_documents_discovered(self, documents: list):
        """Mark documents back to discovered state (for pause/resume)"""
        import asyncpg
        from domains.document_intelligence.factory import get_database_url
        
        if not documents:
            return
        
        conn = await asyncpg.connect(get_database_url())
        
        try:
            doc_ids = [doc['id'] for doc in documents]
            await conn.execute("""
                UPDATE document_processing_state 
                SET processing_status = 'discovered'
                WHERE id = ANY($1)
            """, doc_ids)
            
        finally:
            await conn.close()
    
    async def _show_processing_queue(self):
        """Display current processing queue status"""
        stats = await get_processing_statistics()
        
        logger.info("\n📊 PROCESSING QUEUE STATUS:")
        logger.info("=" * 30)
        
        overall = stats['overall']
        logger.info(f"📄 Total documents: {overall['total_documents']}")
        logger.info(f"🔍 Discovered: {overall['discovered']}")
        logger.info(f"✅ Completed: {overall['completed']}")
        logger.info(f"❌ Failed: {overall['failed']}")
        logger.info(f"⚙️ Processing: {overall['processing']}")
        
        if overall['discovered'] > 0:
            logger.info(f"\n📋 Next to Process (by priority):")
            for item in stats['by_priority'][:5]:  # Top 5 priorities
                logger.info(f"   Priority {item['processing_priority']}: {item['count']} documents")
        
        completion_rate = (overall['completed'] / overall['total_documents'] * 100) if overall['total_documents'] > 0 else 0
        logger.info(f"\n🎯 Completion Rate: {completion_rate:.1f}%")


async def main():
    """CLI entry point"""
    controller = StatefulProcessingController()
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli_stateful.py discover [max_docs]     # Discover documents")
        print("  python cli_stateful.py process [batch_size]    # Process next batch")
        print("  python cli_stateful.py status                  # Show processing status")
        return
    
    command = sys.argv[1]
    
    if command == "discover":
        max_docs = int(sys.argv[2]) if len(sys.argv) > 2 else None
        data_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE"
        await controller.discover_documents(data_path, max_docs)
        
    elif command == "process":
        batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        await controller.process_batch(batch_size=batch_size)
        
    elif command == "status":
        await controller._show_processing_queue()
        
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())