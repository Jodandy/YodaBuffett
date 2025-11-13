"""
Document Intelligence - Dependency Injection Factory
Wires up all dependencies following HARD architecture principles
"""

import sys
import os
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))
from .repositories.postgres_repository import (
    PostgresDocumentRepository,
    PostgresExtractionRepository, 
    PostgresProcessingLogRepository
)
from .services.pdf_processor import PDFProcessor
from .services.document_processor import DocumentProcessingService


def get_database_url() -> str:
    """Get PostgreSQL database URL for asyncpg"""
    # Try environment variable first, then fallback to default
    db_url = os.getenv('DATABASE_URL') or 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    
    # Convert from SQLAlchemy format if needed
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
    
    return db_url


def create_document_processing_service() -> DocumentProcessingService:
    """
    Factory function that creates a fully configured DocumentProcessingService
    with all dependencies injected (HARD principle #5)
    """
    db_url = get_database_url()
    
    # Create repositories (adapters)
    document_repository = PostgresDocumentRepository(db_url)
    extraction_repository = PostgresExtractionRepository(db_url)
    processing_log_repository = PostgresProcessingLogRepository(db_url)
    
    # Create services (pure business logic)
    pdf_processor = PDFProcessor(chunk_size=8000, chunk_overlap=200)
    
    # Wire everything together
    document_service = DocumentProcessingService(
        pdf_processor=pdf_processor,
        document_repository=document_repository,
        processing_log_repository=processing_log_repository
    )
    
    return document_service


def create_pdf_processor() -> PDFProcessor:
    """Factory for creating standalone PDF processor"""
    return PDFProcessor(chunk_size=8000, chunk_overlap=200)


def create_repositories(db_url: str = None):
    """Factory for creating repository instances"""
    if not db_url:
        db_url = get_database_url()
    
    return {
        "document": PostgresDocumentRepository(db_url),
        "extraction": PostgresExtractionRepository(db_url),
        "processing_log": PostgresProcessingLogRepository(db_url)
    }