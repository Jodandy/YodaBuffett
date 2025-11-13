"""
Document Intelligence - PDF Processing Service
Pure business logic with explicit dependencies per HARD principles
"""

import re
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import date
import logging

import PyPDF2
import pdfplumber

from ..models.document import DocumentInfo, TextChunk, ProcessedDocument, Language


logger = logging.getLogger(__name__)


class PDFProcessor:
    """Handles PDF text extraction with fallback mechanisms"""
    
    def __init__(self, chunk_size: int = 8000, chunk_overlap: int = 200):
        """
        Initialize PDF processor with chunking configuration
        
        Args:
            chunk_size: Maximum characters per chunk
            chunk_overlap: Character overlap between chunks
        """
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    async def process_pdf(self, file_path: str) -> ProcessedDocument:
        """
        Process a PDF file and extract structured document data
        
        This is the main entry point that orchestrates the entire processing pipeline
        """
        logger.debug(f"Processing PDF: {file_path}")
        
        # Extract document info from file path (pure function)
        document_info = self._extract_document_info_from_path(file_path)
        if not document_info:
            raise ValueError(f"Could not extract document metadata from path: {file_path}")
        
        # Extract text from PDF
        full_text, total_pages, errors = await self._extract_text_from_pdf(file_path)
        
        if not full_text.strip():
            errors.append("No text could be extracted from PDF")
        
        # Clean and normalize text
        cleaned_text = self._clean_text(full_text)
        
        # Detect language
        language = self._detect_language(cleaned_text[:1000])
        
        # Create text chunks
        chunks = self._create_text_chunks(cleaned_text)
        
        return ProcessedDocument(
            document_info=document_info,
            full_text=cleaned_text,
            chunks=chunks,
            total_pages=total_pages,
            language=language,
            text_length=len(cleaned_text),
            processing_errors=errors
        )
    
    def _extract_document_info_from_path(self, file_path: str) -> Optional[DocumentInfo]:
        """
        Extract document metadata from file path structure (pure function)
        
        Expected structure: .../companies/SE/{Letter}/{Company}/{Year}/{Type}/{filename.pdf}
        """
        try:
            path_parts = Path(file_path).parts
            
            if len(path_parts) < 6:
                return None
            
            country = path_parts[-5]  # SE
            company_name = path_parts[-4].replace('_', ' ')  # AAK_AB -> AAK AB
            year = int(path_parts[-3])  # 2025
            document_type = path_parts[-2]  # annual_report, quarterly_report, etc.
            file_name = path_parts[-1]
            
            # Extract date from filename (format: YYYY-MM-DD-...)
            date_published = None
            if file_name.startswith(('2024-', '2025-')):
                date_part = file_name[:10]
                if len(date_part) == 10 and date_part.count('-') == 2:
                    try:
                        year_part, month_part, day_part = date_part.split('-')
                        date_published = date(int(year_part), int(month_part), int(day_part))
                    except ValueError:
                        pass  # Invalid date, leave as None
            
            return DocumentInfo(
                company_name=company_name,
                country=country,
                year=year,
                document_type=document_type,
                date_published=date_published,
                file_path=file_path,
                file_name=file_name
            )
            
        except (IndexError, ValueError) as e:
            logger.warning(f"Failed to parse document path {file_path}: {e}")
            return None
    
    async def _extract_text_from_pdf(self, file_path: str) -> Tuple[str, int, List[str]]:
        """
        Extract text from PDF using multiple strategies
        
        Returns: (full_text, total_pages, errors)
        """
        errors = []
        full_text = ""
        total_pages = 0
        
        # Strategy 1: Try pdfplumber (better for complex layouts)
        try:
            with pdfplumber.open(file_path) as pdf:
                total_pages = len(pdf.pages)
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    full_text += page_text + "\\n\\n"
                
                if full_text.strip():
                    return full_text, total_pages, errors
                    
        except Exception as e:
            logger.warning(f"pdfplumber extraction failed for {file_path}: {e}")
            errors.append(f"pdfplumber error: {str(e)}")
        
        # Strategy 2: Fallback to PyPDF2
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                total_pages = len(pdf_reader.pages)
                
                for page in pdf_reader.pages:
                    page_text = page.extract_text()
                    full_text += page_text + "\\n\\n"
                    
        except Exception as e:
            logger.error(f"PyPDF2 extraction also failed for {file_path}: {e}")
            errors.append(f"PyPDF2 error: {str(e)}")
        
        return full_text, total_pages, errors
    
    def _clean_text(self, text: str) -> str:
        """
        Clean extracted text (pure function)
        
        Removes artifacts and normalizes formatting
        """
        if not text:
            return ""
        
        # Remove excessive whitespace
        text = re.sub(r'\\s+', ' ', text)
        
        # Fix common PDF extraction artifacts
        text = text.replace('ﬁ', 'fi')
        text = text.replace('ﬂ', 'fl')
        text = text.replace('™', "'")
        text = text.replace('œ', '"')
        text = text.replace('"', '"')
        
        # Remove page numbers (common patterns)
        text = re.sub(r'\\n\\s*\\d+\\s*\\n', '\\n', text)
        
        # Fix broken words (hyphenation)
        text = re.sub(r'(\\w+)-\\s*\\n\\s*(\\w+)', r'\\1\\2', text)
        
        return text.strip()
    
    def _detect_language(self, text_sample: str) -> Language:
        """
        Detect document language (pure function)
        
        Simple heuristic-based detection for Nordic languages
        """
        if not text_sample:
            return "unknown"
        
        text_lower = text_sample.lower()
        
        # Check for Swedish characters
        if any(char in text_lower for char in 'åäöÅÄÖ'):
            return "sv"
        # Check for Norwegian/Danish characters  
        elif any(char in text_lower for char in 'æøÆØ'):
            # More sophisticated check could distinguish Norwegian from Danish
            return "no"
        else:
            return "en"  # Default to English
    
    def _create_text_chunks(self, full_text: str) -> List[TextChunk]:
        """
        Split text into overlapping chunks (pure function)
        
        Creates chunks suitable for vector embedding and LLM processing
        """
        if not full_text:
            return []
        
        chunks = []
        text_length = len(full_text)
        chunk_index = 0
        
        for start in range(0, text_length, self.chunk_size - self.chunk_overlap):
            end = min(start + self.chunk_size, text_length)
            
            # Try to end at a sentence boundary
            chunk_text = full_text[start:end]
            last_period = chunk_text.rfind('.')
            if last_period > self.chunk_size * 0.8:  # If period is in last 20%
                chunk_text = chunk_text[:last_period + 1]
                end = start + len(chunk_text)
            
            chunk = TextChunk(
                text=chunk_text.strip(),
                page_numbers=[1],  # Simplified - would need page mapping for accurate numbers
                chunk_index=chunk_index,
                char_start=start,
                char_end=end,
                metadata={
                    "chunk_size": len(chunk_text),
                    "is_sentence_boundary": last_period > self.chunk_size * 0.8
                }
            )
            
            chunks.append(chunk)
            chunk_index += 1
            
            # Avoid infinite loops
            if end >= text_length:
                break
        
        return chunks