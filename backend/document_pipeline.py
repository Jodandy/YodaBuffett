#!/usr/bin/env python3
"""
Document Processing Pipeline
Processes the 47K+ Nordic PDFs into structured database format
"""

import os
import asyncio
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
import traceback

import asyncpg
import sys
sys.path.append('.')  # Add current directory to path
from research_service.processors.pdf_processor import PDFProcessor, ProcessedPDF
from shared.config import settings

def get_database_url() -> str:
    """Get PostgreSQL database URL for asyncpg"""
    # Convert SQLAlchemy URL to asyncpg format
    db_url = settings.database_url
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
    return db_url

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class DocumentInfo:
    """Document information extracted from file path"""
    company_name: str
    country: str
    year: int
    document_type: str
    date_published: Optional[str]
    file_path: str
    file_name: str


class DocumentPipeline:
    """Processes Nordic PDFs into structured database format"""
    
    def __init__(self, batch_size: int = 10):
        self.batch_size = batch_size
        self.pdf_processor = PDFProcessor()
        self.processed_count = 0
        self.error_count = 0
        self.processing_log = []
    
    async def run_full_pipeline(self, data_path: str = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies"):
        """Run the complete document processing pipeline"""
        logger.info("Starting full document processing pipeline...")
        
        # Initialize database
        await self._init_database()
        
        # Find all PDFs
        pdf_files = self._discover_pdfs(data_path)
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        # Process in batches
        for i in range(0, len(pdf_files), self.batch_size):
            batch = pdf_files[i:i + self.batch_size]
            logger.info(f"Processing batch {i // self.batch_size + 1} ({len(batch)} files)")
            
            await self._process_batch(batch)
            
            # Save progress periodically
            if i % (self.batch_size * 10) == 0:
                await self._save_progress()
        
        logger.info(f"Pipeline complete. Processed: {self.processed_count}, Errors: {self.error_count}")
        await self._save_final_report()
    
    def _discover_pdfs(self, data_path: str) -> List[str]:
        """Discover all PDF files in the data directory"""
        pdf_files = []
        data_dir = Path(data_path)
        
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_path}")
            return []
        
        # Find all PDFs recursively
        for pdf_file in data_dir.rglob("*.pdf"):
            pdf_files.append(str(pdf_file))
        
        return sorted(pdf_files)
    
    async def _process_batch(self, pdf_files: List[str]):
        """Process a batch of PDF files"""
        tasks = []
        
        for pdf_file in pdf_files:
            task = self._process_single_document(pdf_file)
            tasks.append(task)
        
        # Process batch concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to process {pdf_files[i]}: {result}")
                self.error_count += 1
            else:
                self.processed_count += 1
    
    async def _process_single_document(self, pdf_path: str) -> bool:
        """Process a single PDF document"""
        try:
            # Extract document info from path
            doc_info = self._extract_document_info(pdf_path)
            if not doc_info:
                logger.warning(f"Could not extract document info from: {pdf_path}")
                return False
            
            # Process PDF
            processed_pdf = await self.pdf_processor.process_pdf(pdf_path)
            
            if not processed_pdf.full_text.strip():
                logger.warning(f"No text extracted from: {pdf_path}")
                return False
            
            # Save to database
            await self._save_to_database(doc_info, processed_pdf)
            
            self.processing_log.append({
                "file_path": pdf_path,
                "company": doc_info.company_name,
                "type": doc_info.document_type,
                "pages": processed_pdf.total_pages,
                "chunks": len(processed_pdf.chunks),
                "language": processed_pdf.language,
                "processed_at": datetime.now().isoformat(),
                "status": "success"
            })
            
            logger.debug(f"Successfully processed: {doc_info.company_name} - {doc_info.document_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing {pdf_path}: {e}")
            self.processing_log.append({
                "file_path": pdf_path,
                "error": str(e),
                "processed_at": datetime.now().isoformat(),
                "status": "error"
            })
            return False
    
    def _extract_document_info(self, pdf_path: str) -> Optional[DocumentInfo]:
        """Extract document metadata from file path structure"""
        try:
            path_parts = Path(pdf_path).parts
            
            # Expected structure: .../companies/SE/{Letter}/{Company}/{Year}/{Type}/{filename.pdf}
            if len(path_parts) < 6:
                return None
            
            country = path_parts[-5]  # SE
            company_name = path_parts[-4].replace('_', ' ')  # AAK_AB -> AAK AB
            year = int(path_parts[-3])  # 2025
            document_type = path_parts[-2]  # annual_report, quarterly_report, etc.
            file_name = path_parts[-1]
            
            # Try to extract date from filename (format: YYYY-MM-DD-...)
            date_published = None
            if file_name.startswith(('2024-', '2025-')):
                date_part = file_name[:10]
                if len(date_part) == 10 and date_part.count('-') == 2:
                    date_published = date_part
            
            return DocumentInfo(
                company_name=company_name,
                country=country,
                year=year,
                document_type=document_type,
                date_published=date_published,
                file_path=pdf_path,
                file_name=file_name
            )
            
        except Exception as e:
            logger.warning(f"Failed to parse path {pdf_path}: {e}")
            return None
    
    async def _init_database(self):
        """Initialize database tables"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Create filings table (from data-architecture.md)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS filings (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    company_symbol VARCHAR(50),
                    company_name VARCHAR(200),
                    country VARCHAR(10),
                    form_type VARCHAR(50),
                    filing_date DATE,
                    year INTEGER,
                    raw_text TEXT,
                    extracted_text TEXT,
                    processing_status VARCHAR(20) DEFAULT 'processed',
                    file_path TEXT,
                    file_name TEXT,
                    total_pages INTEGER,
                    language VARCHAR(10),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create document chunks table for vector processing later
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS document_chunks (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    filing_id UUID REFERENCES filings(id) ON DELETE CASCADE,
                    chunk_index INTEGER,
                    chunk_text TEXT,
                    page_numbers INTEGER[],
                    char_start INTEGER,
                    char_end INTEGER,
                    chunk_metadata JSONB,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create processing log table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    file_path TEXT,
                    company_name VARCHAR(200),
                    document_type VARCHAR(50),
                    status VARCHAR(20),
                    error_message TEXT,
                    processing_stats JSONB,
                    processed_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Create indexes for performance
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_company_name ON filings(company_name)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_year ON filings(year)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_form_type ON filings(form_type)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_filings_country ON filings(country)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_filing_id ON document_chunks(filing_id)")
            
            logger.info("Database tables initialized successfully")
            
        finally:
            await conn.close()
    
    async def _save_to_database(self, doc_info: DocumentInfo, processed_pdf: ProcessedPDF):
        """Save processed document to database"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Prepare metadata
            metadata = {
                **processed_pdf.metadata,
                "processing_errors": processed_pdf.processing_errors,
                "original_file_path": doc_info.file_path
            }
            
            # Insert filing record
            filing_id = await conn.fetchval("""
                INSERT INTO filings (
                    company_name, country, form_type, filing_date, year,
                    raw_text, extracted_text, file_path, file_name,
                    total_pages, language, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING id
            """, 
                doc_info.company_name,
                doc_info.country,
                doc_info.document_type,
                doc_info.date_published,
                doc_info.year,
                processed_pdf.full_text,  # raw_text
                processed_pdf.full_text,  # extracted_text (same for now)
                doc_info.file_path,
                doc_info.file_name,
                processed_pdf.total_pages,
                processed_pdf.language,
                json.dumps(metadata)
            )
            
            # Insert chunks
            for chunk in processed_pdf.chunks:
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
                    chunk.metadata.get("char_start", 0),
                    chunk.metadata.get("char_end", 0),
                    json.dumps(chunk.metadata)
                )
            
            # Log processing
            await conn.execute("""
                INSERT INTO processing_log (
                    file_path, company_name, document_type, status,
                    processing_stats
                ) VALUES ($1, $2, $3, $4, $5)
            """,
                doc_info.file_path,
                doc_info.company_name,
                doc_info.document_type,
                "success",
                json.dumps({
                    "pages": processed_pdf.total_pages,
                    "chunks": len(processed_pdf.chunks),
                    "language": processed_pdf.language,
                    "text_length": len(processed_pdf.full_text)
                })
            )
            
        except Exception as e:
            logger.error(f"Database save failed: {e}")
            raise
        finally:
            await conn.close()
    
    async def _save_progress(self):
        """Save processing progress to file"""
        progress_file = f"document_pipeline_progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        progress_data = {
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "last_update": datetime.now().isoformat(),
            "recent_log": self.processing_log[-100:]  # Last 100 entries
        }
        
        with open(progress_file, 'w') as f:
            json.dump(progress_data, f, indent=2)
        
        logger.info(f"Progress saved to {progress_file}")
    
    async def _save_final_report(self):
        """Save final processing report"""
        report_file = f"document_pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report_data = {
            "pipeline_completed": True,
            "total_processed": self.processed_count,
            "total_errors": self.error_count,
            "success_rate": f"{(self.processed_count / (self.processed_count + self.error_count) * 100):.1f}%" if (self.processed_count + self.error_count) > 0 else "0%",
            "completed_at": datetime.now().isoformat(),
            "processing_log": self.processing_log
        }
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"Final report saved to {report_file}")


async def main():
    """Main pipeline execution"""
    pipeline = DocumentPipeline(batch_size=5)  # Start with small batches
    await pipeline.run_full_pipeline()


if __name__ == "__main__":
    asyncio.run(main())