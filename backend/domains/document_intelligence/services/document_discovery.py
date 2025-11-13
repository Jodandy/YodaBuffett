"""
Document Discovery Service - Find and catalog all documents for processing
"""

import os
import asyncio
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timezone
import logging

import asyncpg

from ..models.document import DocumentInfo
from ..factory import get_database_url

logger = logging.getLogger(__name__)


class DocumentDiscoveryService:
    """Service to discover and catalog documents without processing them"""
    
    def __init__(self):
        self.region_mapping = {
            'SE': 'nordic',    'NO': 'nordic',    'DK': 'nordic',    'FI': 'nordic',
            'US': 'north_america',    'CA': 'north_america',
            'DE': 'europe',    'FR': 'europe',    'GB': 'europe',    'NL': 'europe',
            'JP': 'asia',      'CN': 'asia',      'SG': 'asia',      'HK': 'asia'
        }
    
    async def discover_all_documents(
        self, 
        data_path: str,
        batch_id: str,
        max_documents: Optional[int] = None
    ) -> dict:
        """
        Discover all PDF documents and catalog them for processing
        
        This is separate from processing - just builds the inventory
        """
        logger.info(f"🔍 Discovering documents in: {data_path}")
        
        # Create batch session
        await self._create_batch_session(batch_id, max_documents)
        
        # Find all PDF files
        pdf_files = self._find_all_pdfs(data_path, max_documents)
        logger.info(f"📄 Found {len(pdf_files)} PDF files")
        
        # Process files in chunks to avoid memory issues
        discovered_count = 0
        skipped_count = 0
        error_count = 0
        
        chunk_size = 100
        for i in range(0, len(pdf_files), chunk_size):
            chunk = pdf_files[i:i + chunk_size]
            
            logger.info(f"📋 Processing discovery chunk {i//chunk_size + 1}/{(len(pdf_files) + chunk_size - 1)//chunk_size}")
            
            chunk_results = await self._catalog_file_chunk(chunk, batch_id)
            discovered_count += chunk_results['discovered']
            skipped_count += chunk_results['skipped']
            error_count += chunk_results['errors']
        
        # Update batch session
        await self._update_batch_session(batch_id, {
            'total_discovered': discovered_count,
            'total_skipped': skipped_count,
            'total_failed': error_count
        })
        
        logger.info(f"✅ Discovery complete: {discovered_count} discovered, {skipped_count} skipped, {error_count} errors")
        
        return {
            'total_found': len(pdf_files),
            'discovered': discovered_count,
            'skipped': skipped_count,
            'errors': error_count,
            'batch_id': batch_id
        }
    
    def _find_all_pdfs(self, data_path: str, max_documents: Optional[int]) -> List[str]:
        """Find all PDF files recursively"""
        pdf_files = []
        data_dir = Path(data_path)
        
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_path}")
            return []
        
        for pdf_file in data_dir.rglob("*.pdf"):
            pdf_files.append(str(pdf_file))
            
            if max_documents and len(pdf_files) >= max_documents:
                break
        
        return sorted(pdf_files)
    
    async def _catalog_file_chunk(self, file_paths: List[str], batch_id: str) -> dict:
        """Catalog a chunk of files into the processing state table"""
        conn = await asyncpg.connect(get_database_url())
        
        discovered = 0
        skipped = 0
        errors = 0
        
        try:
            for file_path in file_paths:
                try:
                    # Check if already cataloged
                    exists = await conn.fetchval("""
                        SELECT COUNT(*) FROM document_processing_state 
                        WHERE file_path = $1
                    """, file_path)
                    
                    if exists > 0:
                        skipped += 1
                        continue
                    
                    # Extract document info
                    doc_info = self._extract_document_info_from_path(file_path)
                    if not doc_info:
                        logger.warning(f"Could not parse document path: {file_path}")
                        errors += 1
                        continue
                    
                    # Get file metadata
                    file_stat = os.stat(file_path)
                    file_size = file_stat.st_size
                    file_modified = datetime.fromtimestamp(file_stat.st_mtime).replace(tzinfo=None)  # Remove timezone for PostgreSQL
                    
                    # Determine processing priority
                    priority = self._calculate_priority(doc_info)
                    
                    # Insert into processing state table
                    await conn.execute("""
                        INSERT INTO document_processing_state (
                            file_path, file_name, company_name, country, region,
                            document_type, year, file_size_bytes, file_modified_at,
                            processing_priority, batch_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """,
                        file_path,
                        doc_info.file_name,
                        doc_info.company_name,
                        doc_info.country,
                        self.region_mapping.get(doc_info.country, 'unknown'),
                        doc_info.document_type,
                        doc_info.year,
                        file_size,
                        file_modified,
                        priority,
                        batch_id
                    )
                    
                    discovered += 1
                    
                except Exception as e:
                    logger.error(f"Error cataloging {file_path}: {e}")
                    errors += 1
        
        finally:
            await conn.close()
        
        return {
            'discovered': discovered,
            'skipped': skipped,
            'errors': errors
        }
    
    def _extract_document_info_from_path(self, file_path: str) -> Optional[DocumentInfo]:
        """Extract document info from file path (same logic as PDFProcessor)"""
        try:
            path_parts = Path(file_path).parts
            
            for i, part in enumerate(path_parts):
                if part == "companies" and i + 1 < len(path_parts):
                    country = path_parts[i + 1]  # SE
                    company_name = path_parts[i + 3].replace('_', ' ')  # AAK_AB -> AAK AB  
                    year = int(path_parts[i + 4])  # 2025
                    document_type = path_parts[i + 5]  # annual_report, quarterly_report, etc.
                    file_name = path_parts[-1]
                    
                    # Extract date from filename
                    date_published = None
                    if file_name.startswith(('2024-', '2025-')):
                        date_part = file_name[:10]
                        if len(date_part) == 10 and date_part.count('-') == 2:
                            try:
                                from datetime import date
                                year_part, month_part, day_part = date_part.split('-')
                                date_published = date(int(year_part), int(month_part), int(day_part))
                            except ValueError:
                                pass
                    
                    return DocumentInfo(
                        company_name=company_name,
                        country=country,
                        year=year,
                        document_type=document_type,
                        date_published=date_published,
                        file_path=file_path,
                        file_name=file_name
                    )
                    
        except Exception as e:
            logger.debug(f"Failed to parse document path {file_path}: {e}")
            
        return None
    
    def _calculate_priority(self, doc_info: DocumentInfo) -> int:
        """Calculate processing priority (1=highest, 10=lowest)"""
        priority = 5  # Default
        
        # Prioritize by document type
        if doc_info.document_type == "annual_report":
            priority = 1
        elif doc_info.document_type == "quarterly_report":
            priority = 2
        elif doc_info.document_type in ["governance", "press_release"]:
            priority = 7
        
        # Prioritize recent documents
        current_year = datetime.now().year
        if doc_info.year == current_year:
            priority -= 1
        elif doc_info.year == current_year - 1:
            priority -= 0  # No change
        else:
            priority += 1
        
        # Ensure within bounds
        return max(1, min(10, priority))
    
    async def _create_batch_session(self, batch_id: str, max_documents: Optional[int]):
        """Create a new batch processing session"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                INSERT INTO batch_processing_sessions (
                    batch_id, session_name, max_documents, session_status
                ) VALUES ($1, $2, $3, 'active')
                ON CONFLICT (batch_id) DO UPDATE SET
                    started_at = NOW(),
                    session_status = 'active',
                    max_documents = EXCLUDED.max_documents
            """, batch_id, f"Discovery {batch_id}", max_documents)
            
        finally:
            await conn.close()
    
    async def _update_batch_session(self, batch_id: str, stats: dict):
        """Update batch session with statistics"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                UPDATE batch_processing_sessions 
                SET total_discovered = $2,
                    total_failed = $3,
                    total_skipped = $4,
                    updated_at = NOW()
                WHERE batch_id = $1
            """, batch_id, stats['total_discovered'], stats['total_failed'], stats['total_skipped'])
            
        finally:
            await conn.close()


async def get_processing_statistics() -> dict:
    """Get current processing state statistics"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        # Overall stats
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_documents,
                COUNT(*) FILTER (WHERE processing_status = 'discovered') as discovered,
                COUNT(*) FILTER (WHERE processing_status = 'completed') as completed,
                COUNT(*) FILTER (WHERE processing_status = 'failed') as failed,
                COUNT(*) FILTER (WHERE processing_status = 'processing') as processing
            FROM document_processing_state
        """)
        
        # By priority
        by_priority = await conn.fetch("""
            SELECT processing_priority, COUNT(*) as count
            FROM document_processing_state 
            WHERE processing_status = 'discovered'
            GROUP BY processing_priority 
            ORDER BY processing_priority
        """)
        
        # By document type
        by_type = await conn.fetch("""
            SELECT document_type, COUNT(*) as count
            FROM document_processing_state 
            WHERE processing_status = 'discovered'
            GROUP BY document_type 
            ORDER BY count DESC
        """)
        
        # By region
        by_region = await conn.fetch("""
            SELECT region, COUNT(*) as count
            FROM document_processing_state 
            GROUP BY region 
            ORDER BY count DESC
        """)
        
        return {
            'overall': dict(stats),
            'by_priority': [dict(row) for row in by_priority],
            'by_document_type': [dict(row) for row in by_type],
            'by_region': [dict(row) for row in by_region]
        }
        
    finally:
        await conn.close()