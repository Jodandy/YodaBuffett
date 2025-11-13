"""
Document Intelligence - Core Domain Models
Pure data structures with no external dependencies
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Literal
from datetime import date
from uuid import UUID


@dataclass(frozen=True)  # Immutable per HARD principle #4
class DocumentInfo:
    """Document metadata extracted from file structure"""
    company_name: str
    country: str
    year: int
    document_type: str
    date_published: Optional[date]
    file_path: str
    file_name: str


@dataclass(frozen=True)
class TextChunk:
    """A chunk of text with metadata for processing"""
    text: str
    page_numbers: List[int]
    chunk_index: int
    char_start: int
    char_end: int
    metadata: Dict[str, any]


@dataclass(frozen=True)
class ProcessedDocument:
    """Result of document text extraction and processing"""
    document_info: DocumentInfo
    full_text: str
    chunks: List[TextChunk]
    total_pages: int
    language: str
    text_length: int
    processing_errors: List[str]


@dataclass(frozen=True)
class ExtractionResult:
    """Result of financial data extraction from document"""
    document_id: UUID
    extraction_confidence: float
    extracted_metrics: Dict[str, any]
    extraction_method: str
    processing_timestamp: date


# Type aliases for clarity
Language = Literal["sv", "no", "da", "fi", "en", "unknown"]
DocumentType = Literal["annual_report", "quarterly_report", "press_release", "governance", "other"]
ProcessingStatus = Literal["pending", "processing", "completed", "failed"]