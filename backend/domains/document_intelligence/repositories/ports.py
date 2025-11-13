"""
Document Intelligence - Repository Ports (Interfaces)
Pure interfaces with no implementation details per HARD principle #3
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import UUID

from ..models.document import ProcessedDocument, ExtractionResult, DocumentInfo


class DocumentRepository(ABC):
    """Port for document data access"""
    
    @abstractmethod
    async def save_processed_document(self, document: ProcessedDocument) -> UUID:
        """Save a processed document and return its ID"""
        pass
    
    @abstractmethod
    async def get_document_by_id(self, document_id: UUID) -> Optional[ProcessedDocument]:
        """Retrieve a document by its ID"""
        pass
    
    @abstractmethod
    async def find_documents_by_company(self, company_name: str) -> List[ProcessedDocument]:
        """Find all documents for a specific company"""
        pass
    
    @abstractmethod
    async def find_documents_by_type(self, document_type: str) -> List[ProcessedDocument]:
        """Find all documents of a specific type"""
        pass


class ExtractionRepository(ABC):
    """Port for financial extraction data access"""
    
    @abstractmethod
    async def save_extraction_result(self, result: ExtractionResult) -> UUID:
        """Save extraction results"""
        pass
    
    @abstractmethod
    async def get_extraction_by_document_id(self, document_id: UUID) -> Optional[ExtractionResult]:
        """Get extraction results for a document"""
        pass


class ProcessingLogRepository(ABC):
    """Port for processing log data access"""
    
    @abstractmethod
    async def log_processing_attempt(self, document_info: DocumentInfo, status: str, error_message: Optional[str] = None) -> None:
        """Log a processing attempt"""
        pass
    
    @abstractmethod
    async def get_processing_stats(self) -> dict:
        """Get overall processing statistics"""
        pass