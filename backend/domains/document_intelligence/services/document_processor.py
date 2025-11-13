"""
Document Intelligence - Main Document Processing Service
Orchestrates the entire document processing pipeline with explicit dependencies
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Optional
from uuid import UUID

from ..models.document import ProcessedDocument, DocumentInfo
from ..repositories.ports import DocumentRepository, ProcessingLogRepository
from .pdf_processor import PDFProcessor


logger = logging.getLogger(__name__)


class DocumentProcessingService:
    """
    Main service for processing documents with explicit dependencies
    Follows HARD principle #5: Explicit Dependencies
    """
    
    def __init__(
        self,
        pdf_processor: PDFProcessor,
        document_repository: DocumentRepository,
        processing_log_repository: ProcessingLogRepository
    ):
        """Initialize with explicitly injected dependencies"""
        self.pdf_processor = pdf_processor
        self.document_repository = document_repository
        self.processing_log_repository = processing_log_repository
    
    async def process_single_document(self, file_path: str) -> Optional[UUID]:
        """
        Process a single document through the complete pipeline
        
        Returns:
            UUID of saved document if successful, None if failed
        """
        try:
            logger.info(f"Processing document: {file_path}")
            
            # Process PDF (pure business logic)
            processed_doc = await self.pdf_processor.process_pdf(file_path)
            
            # Validate processing result
            if not processed_doc.full_text.strip():
                await self.processing_log_repository.log_processing_attempt(
                    processed_doc.document_info, 
                    "failed", 
                    "No text extracted from PDF"
                )
                return None
            
            # Save to repository
            document_id = await self.document_repository.save_processed_document(processed_doc)
            
            # Log success
            await self.processing_log_repository.log_processing_attempt(
                processed_doc.document_info, 
                "success"
            )
            
            logger.info(f"✅ Successfully processed: {processed_doc.document_info.company_name} - {processed_doc.document_info.document_type}")
            return document_id
            
        except Exception as e:
            logger.error(f"❌ Error processing {file_path}: {e}")
            
            # Try to extract document info for logging
            try:
                doc_info = self.pdf_processor._extract_document_info_from_path(file_path)
                if doc_info:
                    await self.processing_log_repository.log_processing_attempt(
                        doc_info, 
                        "failed", 
                        str(e)
                    )
            except:
                pass  # Don't fail the error logging
            
            return None
    
    async def process_batch_documents(self, file_paths: List[str], max_concurrent: int = 5) -> dict:
        """
        Process multiple documents concurrently with structured concurrency
        Follows HARD principle #9: Structured Concurrency
        
        Returns:
            Dictionary with processing statistics
        """
        logger.info(f"Processing batch of {len(file_paths)} documents")
        
        processed_count = 0
        error_count = 0
        results = []
        
        # Process in batches to control concurrency
        for i in range(0, len(file_paths), max_concurrent):
            batch = file_paths[i:i + max_concurrent]
            
            # Use structured concurrency
            async with asyncio.TaskGroup() as tg:
                tasks = [
                    tg.create_task(self.process_single_document(file_path))
                    for file_path in batch
                ]
            
            # Collect results
            for task in tasks:
                result = task.result()
                if result:
                    processed_count += 1
                    results.append(result)
                else:
                    error_count += 1
        
        logger.info(f"Batch processing complete: {processed_count} successful, {error_count} errors")
        
        return {
            "total_files": len(file_paths),
            "processed_count": processed_count,
            "error_count": error_count,
            "success_rate": (processed_count / len(file_paths)) * 100 if file_paths else 0,
            "document_ids": results
        }
    
    async def discover_unprocessed_documents(self, data_path: str, max_documents: Optional[int] = None) -> List[str]:
        """
        Discover PDF files that haven't been processed yet
        
        This is a pure function that doesn't modify state
        """
        data_dir = Path(data_path)
        
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_path}")
            return []
        
        # Find all PDFs
        pdf_files = []
        for pdf_file in data_dir.rglob("*.pdf"):
            pdf_files.append(str(pdf_file))
            
            if max_documents and len(pdf_files) >= max_documents:
                break
        
        logger.info(f"Discovered {len(pdf_files)} PDF files")
        return sorted(pdf_files)
    
    async def get_processing_statistics(self) -> dict:
        """Get overall processing statistics"""
        return await self.processing_log_repository.get_processing_stats()
    
    async def find_documents_by_company(self, company_name: str) -> List[ProcessedDocument]:
        """Find all processed documents for a company"""
        return await self.document_repository.find_documents_by_company(company_name)
    
    async def find_documents_by_type(self, document_type: str) -> List[ProcessedDocument]:
        """Find all processed documents of a specific type"""
        return await self.document_repository.find_documents_by_type(document_type)
    
    async def get_document_by_id(self, document_id: UUID) -> Optional[ProcessedDocument]:
        """Retrieve a specific document by ID"""
        return await self.document_repository.get_document_by_id(document_id)