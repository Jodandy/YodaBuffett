"""
PDF Processing Module
Extracts text and metadata from PDF documents
"""
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

import PyPDF2
import pdfplumber
from langdetect import detect, LangDetectException

logger = logging.getLogger(__name__)


@dataclass
class PDFChunk:
    """Represents a chunk of text from a PDF"""
    text: str
    page_numbers: List[int]
    chunk_index: int
    metadata: Dict[str, any]


@dataclass
class ProcessedPDF:
    """Represents a fully processed PDF document"""
    file_path: str
    total_pages: int
    language: str
    chunks: List[PDFChunk]
    full_text: str
    metadata: Dict[str, any]
    processing_errors: List[str]


class PDFProcessor:
    """Handles PDF text extraction and chunking"""
    
    def __init__(self, chunk_size: int = 8000, chunk_overlap: int = 200):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
    
    async def process_pdf(self, file_path: str) -> ProcessedPDF:
        """Process a PDF file and extract text with metadata"""
        errors = []
        chunks = []
        full_text = ""
        total_pages = 0
        language = "unknown"
        
        try:
            # Try pdfplumber first (better for tables and complex layouts)
            full_text, page_texts, total_pages = await self._extract_with_pdfplumber(file_path)
            
            if not full_text.strip():
                # Fallback to PyPDF2
                full_text, page_texts, total_pages = await self._extract_with_pypdf2(file_path)
            
            # Detect language
            language = self._detect_language(full_text[:1000])
            
            # Create chunks
            chunks = self._create_chunks(full_text, page_texts)
            
            # Extract metadata
            metadata = await self._extract_metadata(file_path)
            
        except Exception as e:
            logger.error(f"Error processing PDF {file_path}: {str(e)}")
            errors.append(f"Processing error: {str(e)}")
        
        return ProcessedPDF(
            file_path=file_path,
            total_pages=total_pages,
            language=language,
            chunks=chunks,
            full_text=full_text,
            metadata=metadata,
            processing_errors=errors
        )
    
    async def _extract_with_pdfplumber(self, file_path: str) -> Tuple[str, List[str], int]:
        """Extract text using pdfplumber (better for tables)"""
        full_text = ""
        page_texts = []
        
        try:
            with pdfplumber.open(file_path) as pdf:
                total_pages = len(pdf.pages)
                
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    page_texts.append(page_text)
                    full_text += page_text + "\n\n"
                
                return full_text, page_texts, total_pages
        except Exception as e:
            logger.warning(f"pdfplumber extraction failed: {e}")
            return "", [], 0
    
    async def _extract_with_pypdf2(self, file_path: str) -> Tuple[str, List[str], int]:
        """Extract text using PyPDF2 (fallback)"""
        full_text = ""
        page_texts = []
        
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                total_pages = len(pdf_reader.pages)
                
                for page_num, page in enumerate(pdf_reader.pages):
                    page_text = page.extract_text()
                    page_texts.append(page_text)
                    full_text += page_text + "\n\n"
                
                return full_text, page_texts, total_pages
        except Exception as e:
            logger.error(f"PyPDF2 extraction failed: {e}")
            return "", [], 0
    
    def _detect_language(self, text_sample: str) -> str:
        """Detect document language"""
        try:
            lang = detect(text_sample)
            return lang
        except LangDetectException:
            # Check for Swedish characters as fallback
            if any(char in text_sample for char in 'åäöÅÄÖ'):
                return 'sv'
            return 'en'
    
    def _create_chunks(self, full_text: str, page_texts: List[str]) -> List[PDFChunk]:
        """Split text into chunks with metadata"""
        chunks = []
        
        # Simple chunking by character count (can be improved with token counting)
        text_length = len(full_text)
        chunk_index = 0
        
        for start in range(0, text_length, self.chunk_size - self.chunk_overlap):
            end = min(start + self.chunk_size, text_length)
            
            # Try to end at a sentence boundary
            chunk_text = full_text[start:end]
            last_period = chunk_text.rfind('.')
            if last_period > self.chunk_size * 0.8:  # If period is in last 20%
                chunk_text = chunk_text[:last_period + 1]
            
            # Determine which pages this chunk spans
            pages = self._find_chunk_pages(chunk_text, page_texts)
            
            chunk = PDFChunk(
                text=chunk_text.strip(),
                page_numbers=pages,
                chunk_index=chunk_index,
                metadata={
                    "char_start": start,
                    "char_end": start + len(chunk_text),
                    "chunk_size": len(chunk_text)
                }
            )
            
            chunks.append(chunk)
            chunk_index += 1
        
        return chunks
    
    def _find_chunk_pages(self, chunk_text: str, page_texts: List[str]) -> List[int]:
        """Determine which pages a chunk spans"""
        pages = []
        
        # Simple approach: check which pages contain parts of this chunk
        for i, page_text in enumerate(page_texts):
            # Check if any significant portion of chunk appears in this page
            if page_text and len(page_text) > 100:
                # Look for overlapping text
                overlap = self._text_overlap(chunk_text[:200], page_text)
                if overlap > 50:  # At least 50 char overlap
                    pages.append(i + 1)  # 1-indexed
        
        return pages or [1]  # Default to page 1 if no match
    
    def _text_overlap(self, text1: str, text2: str) -> int:
        """Find longest common substring length"""
        # Simple implementation - can be optimized
        max_len = 0
        for i in range(len(text1)):
            for j in range(i + 1, min(i + 200, len(text1) + 1)):
                if text1[i:j] in text2:
                    max_len = max(max_len, j - i)
        return max_len
    
    async def _extract_metadata(self, file_path: str) -> Dict[str, any]:
        """Extract PDF metadata"""
        metadata = {
            "file_name": os.path.basename(file_path),
            "file_size_mb": os.path.getsize(file_path) / (1024 * 1024),
            "file_path": file_path
        }
        
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                if pdf_reader.metadata:
                    metadata.update({
                        "title": pdf_reader.metadata.get('/Title', ''),
                        "author": pdf_reader.metadata.get('/Author', ''),
                        "subject": pdf_reader.metadata.get('/Subject', ''),
                        "creator": pdf_reader.metadata.get('/Creator', ''),
                        "creation_date": str(pdf_reader.metadata.get('/CreationDate', ''))
                    })
        except Exception as e:
            logger.warning(f"Could not extract PDF metadata: {e}")
        
        return metadata


class DocumentCleaner:
    """Cleans and normalizes extracted text"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean extracted text"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Fix common PDF extraction artifacts
        text = text.replace('ﬁ', 'fi')
        text = text.replace('ﬂ', 'fl')
        text = text.replace('™', "'")
        text = text.replace('œ', '"')
        text = text.replace('"', '"')
        
        # Remove page numbers (common patterns)
        text = re.sub(r'\n\s*\d+\s*\n', '\n', text)
        
        # Fix broken words (hy-phenation)
        text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)
        
        return text.strip()