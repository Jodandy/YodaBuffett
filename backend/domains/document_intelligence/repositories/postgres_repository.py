"""
Document Intelligence - PostgreSQL Repository Implementation
Adapter that implements the repository ports using PostgreSQL
"""

import json
from typing import List, Optional
from uuid import UUID
from datetime import datetime

import asyncpg

from ..models.document import ProcessedDocument, ExtractionResult, DocumentInfo, TextChunk
from .ports import DocumentRepository, ExtractionRepository, ProcessingLogRepository


class PostgresDocumentRepository(DocumentRepository):
    """PostgreSQL implementation of DocumentRepository port"""
    
    def __init__(self, db_url: str):
        self.db_url = db_url
    
    async def save_processed_document(self, document: ProcessedDocument) -> UUID:
        """Save a processed document and return its ID"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            # Insert filing record
            filing_id = await conn.fetchval("""
                INSERT INTO filings (
                    company_name, country, form_type, filing_date, year,
                    raw_text, extracted_text, file_path, file_name,
                    total_pages, language, text_length, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                RETURNING id
            """, 
                document.document_info.company_name,
                document.document_info.country,
                document.document_info.document_type,
                document.document_info.date_published,
                document.document_info.year,
                document.full_text,
                document.full_text,
                document.document_info.file_path,
                document.document_info.file_name,
                document.total_pages,
                document.language,
                document.text_length,
                json.dumps({
                    "processing_errors": document.processing_errors,
                    "original_file_path": document.document_info.file_path
                })
            )
            
            # Insert chunks
            for chunk in document.chunks:
                await conn.execute("""
                    INSERT INTO document_chunks (
                        filing_id, chunk_index, chunk_text, page_numbers,
                        char_start, char_end, chunk_metadata
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                    filing_id,
                    chunk.chunk_index,
                    chunk.text,
                    chunk.page_numbers,
                    chunk.char_start,
                    chunk.char_end,
                    json.dumps(chunk.metadata)
                )
            
            return filing_id
            
        finally:
            await conn.close()
    
    async def get_document_by_id(self, document_id: UUID) -> Optional[ProcessedDocument]:
        """Retrieve a document by its ID"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            # Get filing record
            filing = await conn.fetchrow("""
                SELECT * FROM filings WHERE id = $1
            """, document_id)
            
            if not filing:
                return None
            
            # Get chunks
            chunk_rows = await conn.fetch("""
                SELECT * FROM document_chunks 
                WHERE filing_id = $1 
                ORDER BY chunk_index
            """, document_id)
            
            # Build document info
            doc_info = DocumentInfo(
                company_name=filing['company_name'],
                country=filing['country'],
                year=filing['year'],
                document_type=filing['form_type'],
                date_published=filing['filing_date'],
                file_path=filing['file_path'],
                file_name=filing['file_name']
            )
            
            # Build chunks
            chunks = [
                TextChunk(
                    text=chunk['chunk_text'],
                    page_numbers=chunk['page_numbers'],
                    chunk_index=chunk['chunk_index'],
                    char_start=chunk['char_start'],
                    char_end=chunk['char_end'],
                    metadata=json.loads(chunk['chunk_metadata']) if chunk['chunk_metadata'] else {}
                )
                for chunk in chunk_rows
            ]
            
            # Build processed document
            metadata = json.loads(filing['metadata']) if filing['metadata'] else {}
            
            return ProcessedDocument(
                document_info=doc_info,
                full_text=filing['extracted_text'],
                chunks=chunks,
                total_pages=filing['total_pages'],
                language=filing['language'],
                text_length=filing['text_length'],
                processing_errors=metadata.get('processing_errors', [])
            )
            
        finally:
            await conn.close()
    
    async def find_documents_by_company(self, company_name: str) -> List[ProcessedDocument]:
        """Find all documents for a specific company"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            filing_ids = await conn.fetch("""
                SELECT id FROM filings WHERE company_name = $1
                ORDER BY year DESC, filing_date DESC
            """, company_name)
            
            documents = []
            for row in filing_ids:
                doc = await self.get_document_by_id(row['id'])
                if doc:
                    documents.append(doc)
            
            return documents
            
        finally:
            await conn.close()
    
    async def find_documents_by_type(self, document_type: str) -> List[ProcessedDocument]:
        """Find all documents of a specific type"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            filing_ids = await conn.fetch("""
                SELECT id FROM filings WHERE form_type = $1
                ORDER BY year DESC, filing_date DESC
                LIMIT 100
            """, document_type)
            
            documents = []
            for row in filing_ids:
                doc = await self.get_document_by_id(row['id'])
                if doc:
                    documents.append(doc)
            
            return documents
            
        finally:
            await conn.close()


class PostgresExtractionRepository(ExtractionRepository):
    """PostgreSQL implementation of ExtractionRepository port"""
    
    def __init__(self, db_url: str):
        self.db_url = db_url
    
    async def save_extraction_result(self, result: ExtractionResult) -> UUID:
        """Save extraction results"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            extraction_id = await conn.fetchval("""
                INSERT INTO extraction_results (
                    document_id, extraction_confidence, extracted_metrics,
                    extraction_method, processing_timestamp
                ) VALUES ($1, $2, $3, $4, $5)
                RETURNING id
            """,
                result.document_id,
                result.extraction_confidence,
                json.dumps(result.extracted_metrics),
                result.extraction_method,
                result.processing_timestamp
            )
            
            return extraction_id
            
        finally:
            await conn.close()
    
    async def get_extraction_by_document_id(self, document_id: UUID) -> Optional[ExtractionResult]:
        """Get extraction results for a document"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            row = await conn.fetchrow("""
                SELECT * FROM extraction_results WHERE document_id = $1
                ORDER BY processing_timestamp DESC
                LIMIT 1
            """, document_id)
            
            if not row:
                return None
            
            return ExtractionResult(
                document_id=row['document_id'],
                extraction_confidence=row['extraction_confidence'],
                extracted_metrics=json.loads(row['extracted_metrics']),
                extraction_method=row['extraction_method'],
                processing_timestamp=row['processing_timestamp']
            )
            
        finally:
            await conn.close()


class PostgresProcessingLogRepository(ProcessingLogRepository):
    """PostgreSQL implementation of ProcessingLogRepository port"""
    
    def __init__(self, db_url: str):
        self.db_url = db_url
    
    async def log_processing_attempt(self, document_info: DocumentInfo, status: str, error_message: Optional[str] = None) -> None:
        """Log a processing attempt"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            await conn.execute("""
                INSERT INTO processing_log (
                    file_path, company_name, document_type, status,
                    error_message, processing_stats
                ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
                document_info.file_path,
                document_info.company_name,
                document_info.document_type,
                status,
                error_message,
                json.dumps({
                    "year": document_info.year,
                    "country": document_info.country
                })
            )
            
        finally:
            await conn.close()
    
    async def get_processing_stats(self) -> dict:
        """Get overall processing statistics"""
        conn = await asyncpg.connect(self.db_url)
        
        try:
            # Overall counts
            total_docs = await conn.fetchval("SELECT COUNT(*) FROM filings")
            total_errors = await conn.fetchval("SELECT COUNT(*) FROM processing_log WHERE status = 'error'")
            total_success = await conn.fetchval("SELECT COUNT(*) FROM processing_log WHERE status = 'success'")
            
            # By document type
            by_type = await conn.fetch("""
                SELECT form_type, COUNT(*) as count
                FROM filings
                GROUP BY form_type
                ORDER BY count DESC
            """)
            
            # By language
            by_language = await conn.fetch("""
                SELECT language, COUNT(*) as count
                FROM filings
                GROUP BY language
                ORDER BY count DESC
            """)
            
            return {
                "total_documents": total_docs,
                "total_errors": total_errors,
                "total_success": total_success,
                "success_rate": (total_success / (total_success + total_errors)) * 100 if (total_success + total_errors) > 0 else 0,
                "by_document_type": [dict(row) for row in by_type],
                "by_language": [dict(row) for row in by_language]
            }
            
        finally:
            await conn.close()