"""
Nordic Document Extraction Service - Updated to use nordic_documents table

This service replaces DocumentDiscoveryService and works directly with the
existing nordic_documents table for extraction state management.
"""

import os
import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

import asyncpg

from ..models.document import DocumentInfo
from ..factory import get_database_url

logger = logging.getLogger(__name__)


class NordicExtractionService:
    """Service for managing text extraction from downloaded Nordic documents"""
    
    def __init__(self):
        self.region_mapping = {
            'SE': 'nordic',    'NO': 'nordic',    'DK': 'nordic',    'FI': 'nordic',
            'US': 'north_america',    'CA': 'north_america',
            'DE': 'europe',    'FR': 'europe',    'GB': 'europe',    'NL': 'europe',
            'JP': 'asia',      'CN': 'asia',      'SG': 'asia',      'HK': 'asia'
        }
    
    async def get_extraction_statistics(self) -> Dict[str, Any]:
        """Get current extraction status statistics"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Overall stats
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_downloaded,
                    COUNT(*) FILTER (WHERE extraction_status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE extraction_status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE extraction_status = 'failed_extraction') as failed,
                    COUNT(*) FILTER (WHERE extraction_status = 'processing') as processing,
                    COUNT(*) FILTER (WHERE extraction_status = 'skipped') as skipped
                FROM nordic_documents 
                WHERE processing_status = 'downloaded'
            """)
            
            # By priority
            by_priority = await conn.fetch("""
                SELECT extraction_priority, COUNT(*) as count
                FROM nordic_documents 
                WHERE processing_status = 'downloaded' 
                AND extraction_status = 'pending'
                GROUP BY extraction_priority 
                ORDER BY extraction_priority
            """)
            
            # By document type
            by_type = await conn.fetch("""
                SELECT document_type, COUNT(*) as count
                FROM nordic_documents 
                WHERE processing_status = 'downloaded' 
                AND extraction_status = 'pending'
                GROUP BY document_type 
                ORDER BY count DESC
                LIMIT 10
            """)
            
            # By year for priority documents (extract from report_period)
            by_year = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN report_period ~ '^[0-9]{4}$' THEN report_period::integer
                        WHEN report_period ~ '^Q[1-4]_[0-9]{4}$' THEN RIGHT(report_period, 4)::integer
                        WHEN report_period ~ '^FY_[0-9]{4}$' THEN RIGHT(report_period, 4)::integer
                        ELSE EXTRACT(year FROM publish_date)::integer
                    END as year,
                    COUNT(*) as count
                FROM nordic_documents 
                WHERE processing_status = 'downloaded' 
                AND extraction_status = 'pending'
                AND extraction_priority <= 2
                GROUP BY 1
                ORDER BY 1 DESC
                LIMIT 5
            """)
            
            # Performance stats for completed extractions
            performance = await conn.fetchrow("""
                SELECT 
                    AVG(extraction_duration_seconds) as avg_duration_seconds,
                    AVG(text_length) as avg_text_length,
                    AVG(extraction_confidence) as avg_confidence,
                    MAX(extracted_at) as last_extraction
                FROM nordic_documents 
                WHERE extraction_status = 'completed'
            """)
            
            return {
                'overall': dict(stats),
                'by_priority': [dict(row) for row in by_priority],
                'by_document_type': [dict(row) for row in by_type],
                'by_year': [dict(row) for row in by_year],
                'performance': dict(performance) if performance else {}
            }
            
        finally:
            await conn.close()
    
    async def get_next_extraction_batch(
        self,
        batch_size: int = 100,
        priority_filter: Optional[int] = None,
        document_type_filter: Optional[str] = None,
        year_filter: Optional[int] = None,
        max_attempts: int = 3
    ) -> List[Dict[str, Any]]:
        """Get next batch of documents for extraction"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Build query with filters
            where_conditions = [
                "processing_status = 'downloaded'",
                "extraction_status = 'pending'",
                f"extraction_attempts < {max_attempts}"
            ]
            params = []
            
            if priority_filter:
                where_conditions.append(f"extraction_priority <= ${len(params) + 1}")
                params.append(priority_filter)
            
            if document_type_filter:
                where_conditions.append(f"document_type ILIKE ${len(params) + 1}")
                params.append(f"%{document_type_filter}%")
            
            if year_filter:
                where_conditions.append(f"""(
                    CASE 
                        WHEN report_period ~ '^[0-9]{{4}}$' THEN report_period::integer
                        WHEN report_period ~ '^Q[1-4]_[0-9]{{4}}$' THEN RIGHT(report_period, 4)::integer
                        WHEN report_period ~ '^FY_[0-9]{{4}}$' THEN RIGHT(report_period, 4)::integer
                        ELSE EXTRACT(year FROM publish_date)::integer
                    END = ${len(params) + 1}
                )""")
                params.append(year_filter)
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
                SELECT 
                    id, storage_path, title as company_name, document_type, report_period,
                    extraction_priority, extraction_attempts, extraction_version
                FROM nordic_documents 
                WHERE {where_clause}
                ORDER BY extraction_priority ASC, 
                    CASE 
                        WHEN report_period ~ '^[0-9]{{4}}$' THEN report_period::integer
                        WHEN report_period ~ '^Q[1-4]_[0-9]{{4}}$' THEN RIGHT(report_period, 4)::integer
                        WHEN report_period ~ '^FY_[0-9]{{4}}$' THEN RIGHT(report_period, 4)::integer
                        ELSE EXTRACT(year FROM publish_date)::integer
                    END DESC, 
                    created_at ASC
                LIMIT ${len(params) + 1}
            """
            params.append(batch_size)
            
            result = await conn.fetch(query, *params)
            return [dict(row) for row in result]
            
        finally:
            await conn.close()
    
    async def mark_extraction_processing(self, document_ids: List[str]) -> None:
        """Mark documents as currently processing"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                UPDATE nordic_documents 
                SET extraction_status = 'processing',
                    last_extraction_attempt = NOW(),
                    extraction_attempts = extraction_attempts + 1
                WHERE id = ANY($1)
            """, document_ids)
            
        finally:
            await conn.close()
    
    async def mark_extraction_completed(
        self, 
        document_id: str, 
        filing_id: str,
        text_length: int,
        extraction_duration: int,
        confidence: Optional[float] = None,
        content_analysis: Optional[Dict] = None,
        warnings: Optional[List[str]] = None
    ) -> None:
        """Mark document as successfully extracted"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                UPDATE nordic_documents 
                SET extraction_status = 'completed',
                    extracted_at = NOW(),
                    filing_id = $2,
                    text_length = $3,
                    extraction_duration_seconds = $4,
                    extraction_confidence = $5,
                    content_analysis = $6,
                    extraction_warnings = $7,
                    extraction_error = NULL
                WHERE id = $1
            """, 
                document_id, filing_id, text_length, extraction_duration,
                confidence, content_analysis, warnings
            )
            
        finally:
            await conn.close()
    
    async def mark_extraction_failed(
        self, 
        document_id: str, 
        error_message: str,
        is_permanent: bool = False
    ) -> None:
        """Mark document extraction as failed"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            status = 'failed_permanent' if is_permanent else 'failed_extraction'
            
            await conn.execute("""
                UPDATE nordic_documents 
                SET extraction_status = $2,
                    extraction_error = $3
                WHERE id = $1
            """, document_id, status, error_message)
            
        finally:
            await conn.close()
    
    async def reset_failed_extractions(
        self, 
        max_attempts: Optional[int] = None,
        document_type_filter: Optional[str] = None
    ) -> int:
        """Reset failed extractions for retry"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_conditions = ["extraction_status = 'failed_extraction'"]
            params = []
            
            if max_attempts:
                where_conditions.append(f"extraction_attempts <= ${len(params) + 1}")
                params.append(max_attempts)
            
            if document_type_filter:
                where_conditions.append(f"document_type ILIKE ${len(params) + 1}")
                params.append(f"%{document_type_filter}%")
            
            where_clause = " AND ".join(where_conditions)
            
            result = await conn.execute(f"""
                UPDATE nordic_documents 
                SET extraction_status = 'pending',
                    extraction_error = NULL
                WHERE {where_clause}
            """, *params)
            
            # Extract number of updated rows from result
            return int(result.split()[-1])
            
        finally:
            await conn.close()
    
    async def update_extraction_version(
        self, 
        new_version: str,
        reprocess_completed: bool = True,
        priority_filter: Optional[int] = None
    ) -> int:
        """Update extraction version and optionally reset for reprocessing"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_conditions = ["processing_status = 'downloaded'"]
            params = [new_version]
            
            if priority_filter:
                where_conditions.append(f"extraction_priority <= ${len(params) + 1}")
                params.append(priority_filter)
            
            if reprocess_completed:
                # Reset completed extractions to pending for reprocessing
                set_clause = """
                    extraction_version = $1,
                    extraction_status = CASE 
                        WHEN extraction_status = 'completed' THEN 'pending'
                        ELSE extraction_status 
                    END,
                    extraction_attempts = 0,
                    extraction_error = NULL
                """
            else:
                # Just update version without changing status
                set_clause = "extraction_version = $1"
            
            where_clause = " AND ".join(where_conditions)
            
            result = await conn.execute(f"""
                UPDATE nordic_documents 
                SET {set_clause}
                WHERE {where_clause}
            """, *params)
            
            return int(result.split()[-1])
            
        finally:
            await conn.close()
    
    async def get_extraction_queue_preview(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get preview of next documents in extraction queue"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            result = await conn.fetch("""
                SELECT 
                    title as company_name,
                    document_type,
                    CASE 
                        WHEN report_period ~ '^[0-9]{4}$' THEN report_period::integer
                        WHEN report_period ~ '^Q[1-4]_[0-9]{4}$' THEN RIGHT(report_period, 4)::integer
                        WHEN report_period ~ '^FY_[0-9]{4}$' THEN RIGHT(report_period, 4)::integer
                        ELSE EXTRACT(year FROM publish_date)::integer
                    END as year,
                    extraction_priority,
                    extraction_attempts,
                    CASE 
                        WHEN LENGTH(storage_path) > 60 
                        THEN '...' || RIGHT(storage_path, 57)
                        ELSE storage_path 
                    END as storage_path_short
                FROM nordic_documents 
                WHERE processing_status = 'downloaded' 
                AND extraction_status = 'pending'
                ORDER BY extraction_priority ASC, 
                    CASE 
                        WHEN report_period ~ '^[0-9]{4}$' THEN report_period::integer
                        WHEN report_period ~ '^Q[1-4]_[0-9]{4}$' THEN RIGHT(report_period, 4)::integer
                        WHEN report_period ~ '^FY_[0-9]{4}$' THEN RIGHT(report_period, 4)::integer
                        ELSE EXTRACT(year FROM publish_date)::integer
                    END DESC
                LIMIT $1
            """, limit)
            
            return [dict(row) for row in result]
            
        finally:
            await conn.close()


async def get_nordic_extraction_statistics() -> Dict[str, Any]:
    """Convenience function for getting extraction statistics"""
    service = NordicExtractionService()
    return await service.get_extraction_statistics()