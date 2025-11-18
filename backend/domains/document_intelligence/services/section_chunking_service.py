#!/usr/bin/env python3
"""
Financial Section Chunking Service

Intelligently chunks Nordic financial documents into meaningful sections
using rule-based parsing. Creates sections that can then be embedded
by any embedding provider.
"""

import asyncio
import logging
import json
import time
from typing import List, Dict, Optional
from datetime import datetime

import asyncpg
from ..factory import get_database_url
from .financial_section_parser import FinancialSectionParser, FinancialSection, SectionType

logger = logging.getLogger(__name__)


class SectionChunkingService:
    """Service for chunking documents into intelligent financial sections"""
    
    def __init__(self):
        self.section_parser = FinancialSectionParser()
    
    async def setup_sections_table(self):
        """Create the document_sections table if it doesn't exist"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS document_sections (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    extracted_document_id UUID NOT NULL REFERENCES extracted_documents(id) ON DELETE CASCADE,
                    section_index INTEGER NOT NULL,
                    section_type VARCHAR(50) NOT NULL,
                    section_title TEXT,
                    section_content TEXT NOT NULL,
                    section_start_pos INTEGER,
                    section_end_pos INTEGER,
                    section_confidence FLOAT,
                    parser_version VARCHAR(20) DEFAULT 'v1.0',
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(extracted_document_id, section_index)
                );
                
                -- Indexes for efficient queries
                CREATE INDEX IF NOT EXISTS idx_document_sections_document ON document_sections(extracted_document_id);
                CREATE INDEX IF NOT EXISTS idx_document_sections_type ON document_sections(section_type);
                CREATE INDEX IF NOT EXISTS idx_document_sections_confidence ON document_sections(section_confidence);
            """)
            
            logger.info("✅ Document sections table and indexes created/verified")
            
        finally:
            await conn.close()
    
    async def get_documents_for_sectioning(
        self, 
        limit: int = 10,
        company_filter: str = None
    ) -> List[Dict]:
        """Get documents that need section chunking"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            where_clause = ""
            params = [limit]
            
            if company_filter:
                where_clause = "WHERE ed.company_name ILIKE $2"
                params.append(f"%{company_filter}%")
            
            # Get documents that haven't been sectioned yet
            query = f"""
                SELECT ed.id, ed.company_name, ed.form_type, ed.year, ed.extracted_text,
                       LENGTH(ed.extracted_text) as text_length
                FROM extracted_documents ed
                LEFT JOIN document_sections ds ON ed.id = ds.extracted_document_id
                {where_clause}
                GROUP BY ed.id, ed.company_name, ed.form_type, ed.year, ed.extracted_text
                HAVING COUNT(ds.id) = 0  -- No sections yet
                AND LENGTH(ed.extracted_text) > 10000  -- Skip documents with minimal text (at least 10k chars)
                ORDER BY LENGTH(ed.extracted_text) DESC  -- Start with larger documents that have more content
                LIMIT $1
            """
            
            result = await conn.fetch(query, *params)
            
            # Filter out documents with excessive CID artifacts
            filtered_documents = []
            for row in result:
                doc_dict = dict(row)
                if not self._has_excessive_cid_artifacts(doc_dict['extracted_text']):
                    filtered_documents.append(doc_dict)
                else:
                    logger.warning(f"🚫 Skipping document with excessive CID artifacts: {doc_dict['company_name']} - {doc_dict['form_type']} ({doc_dict['year']})")
            
            return filtered_documents
            
        finally:
            await conn.close()
    
    async def chunk_document_into_sections(self, document_id: str, document_text: str, metadata: Dict) -> Dict:
        """Parse document into financial sections and store them"""
        start_time = time.time()
        
        logger.info(f"🔄 Chunking document into sections: {document_id}")
        logger.info(f"📄 {metadata.get('company_name')} - {metadata.get('form_type')} ({len(document_text):,} chars)")
        
        # Parse document into financial sections
        parsing_result = self.section_parser.parse_document(document_text, document_id)
        
        if not parsing_result['parsing_success']:
            logger.warning(f"⚠️ Failed to parse document {document_id} into sections")
            return {
                "success": False,
                "reason": "parsing_failed",
                "sections_found": 0
            }
        
        sections = parsing_result['sections']
        logger.info(f"📋 Found {len(sections)} meaningful sections")
        
        # Log section summary
        for i, section in enumerate(sections):
            logger.info(f"   {i+1:2}. {section.section_type.value:20} {len(section.content):6,} chars (confidence: {section.confidence:.2f})")
            logger.info(f"       Title: '{section.title[:50]}...'")
        
        # Store sections in database
        stored_count = await self.store_document_sections(document_id, sections)
        
        processing_time = time.time() - start_time
        
        # Extract financial statements for summary
        financial_statements = self.section_parser.extract_financial_statements(sections)
        
        result = {
            "success": True,
            "document_id": document_id,
            "sections_parsed": len(sections),
            "sections_stored": stored_count,
            "financial_statements_found": list(financial_statements.keys()),
            "processing_time_seconds": round(processing_time, 2),
            "parsing_details": parsing_result['section_summary']
        }
        
        logger.info(f"✅ Completed section chunking for {document_id}: {stored_count} sections in {processing_time:.1f}s")
        
        return result
    
    async def store_document_sections(
        self,
        document_id: str,
        sections: List[FinancialSection]
    ) -> int:
        """Store document sections in database"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            stored_count = 0
            
            for i, section in enumerate(sections):
                await conn.execute("""
                    INSERT INTO document_sections (
                        extracted_document_id, section_index, section_type, section_title, 
                        section_content, section_start_pos, section_end_pos, section_confidence,
                        parser_version, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (extracted_document_id, section_index) 
                    DO NOTHING
                """, 
                    document_id,
                    i,  # section_index
                    section.section_type.value,
                    section.title,
                    section.content,
                    section.start_pos,
                    section.end_pos,
                    section.confidence,
                    "v1.0",  # parser_version
                    datetime.now()
                )
                stored_count += 1
            
            logger.info(f"💾 Stored {stored_count} sections for document {document_id}")
            return stored_count
            
        finally:
            await conn.close()
    
    async def process_documents_batch(
        self,
        max_documents: int = 5,
        company_filter: str = None
    ) -> Dict:
        """Process a batch of documents for section chunking"""
        batch_start = time.time()
        
        logger.info(f"🚀 Starting section chunking batch: max {max_documents} documents")
        
        # Ensure database table exists
        await self.setup_sections_table()
        
        # Get documents to process
        documents = await self.get_documents_for_sectioning(
            limit=max_documents,
            company_filter=company_filter
        )
        
        if not documents:
            logger.info("📭 No documents need section chunking")
            return {"success": True, "message": "No documents to process"}
        
        logger.info(f"📋 Processing {len(documents)} documents for section chunking")
        
        results = []
        total_sections = 0
        
        for i, doc in enumerate(documents, 1):
            logger.info(f"📄 Document {i}/{len(documents)}: {doc['company_name']} - {doc['form_type']}")
            
            try:
                metadata = {
                    'company_name': doc['company_name'],
                    'form_type': doc['form_type'],
                    'year': doc['year']
                }
                
                result = await self.chunk_document_into_sections(
                    doc['id'], 
                    doc['extracted_text'], 
                    metadata
                )
                results.append(result)
                
                if result['success']:
                    total_sections += result['sections_stored']
                
            except Exception as e:
                logger.error(f"❌ Failed to process {doc['id']}: {e}")
                results.append({
                    "success": False,
                    "document_id": doc['id'],
                    "error": str(e)
                })
        
        batch_time = time.time() - batch_start
        
        summary = {
            "success": True,
            "batch_summary": {
                "documents_attempted": len(documents),
                "documents_successful": sum(1 for r in results if r['success']),
                "total_sections_created": total_sections,
                "batch_time_seconds": round(batch_time, 2),
                "avg_time_per_document": round(batch_time / len(documents), 2)
            },
            "detailed_results": results
        }
        
        logger.info(f"🎉 Section chunking batch complete: {summary['batch_summary']['documents_successful']}/{len(documents)} documents, {total_sections:,} sections")
        
        return summary
    
    async def get_sectioning_statistics(self) -> Dict:
        """Get current section chunking status and statistics"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            # Overall sectioning status
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(DISTINCT ed.id) as total_documents,
                    COUNT(DISTINCT ds.extracted_document_id) as documents_with_sections,
                    COUNT(ds.id) as total_sections,
                    AVG(ds.section_confidence) as avg_section_confidence,
                    AVG(LENGTH(ds.section_content)) as avg_section_length,
                    MIN(ds.created_at) as first_section,
                    MAX(ds.created_at) as last_section
                FROM extracted_documents ed
                LEFT JOIN document_sections ds ON ed.id = ds.extracted_document_id
            """)
            
            # Sections by type
            by_section_type = await conn.fetch("""
                SELECT section_type, COUNT(*) as count,
                       AVG(section_confidence) as avg_confidence,
                       AVG(LENGTH(section_content)) as avg_length
                FROM document_sections
                GROUP BY section_type
                ORDER BY count DESC
            """)
            
            # Documents needing sectioning
            pending_sectioning = await conn.fetch("""
                SELECT ed.company_name, ed.form_type, ed.year,
                       LENGTH(ed.extracted_text) as text_length
                FROM extracted_documents ed
                LEFT JOIN document_sections ds ON ed.id = ds.extracted_document_id
                GROUP BY ed.id, ed.company_name, ed.form_type, ed.year, ed.extracted_text
                HAVING COUNT(ds.id) = 0
                ORDER BY LENGTH(ed.extracted_text) ASC
                LIMIT 10
            """)
            
            return {
                "overall": dict(stats) if stats else {},
                "by_section_type": [dict(row) for row in by_section_type],
                "pending_documents": [dict(row) for row in pending_sectioning]
            }
            
        finally:
            await conn.close()
    
    async def get_document_sections(self, document_id: str) -> List[Dict]:
        """Get all sections for a specific document"""
        conn = await asyncpg.connect(get_database_url())
        
        try:
            sections = await conn.fetch("""
                SELECT section_index, section_type, section_title, section_content,
                       section_start_pos, section_end_pos, section_confidence,
                       LENGTH(section_content) as content_length
                FROM document_sections
                WHERE extracted_document_id = $1
                ORDER BY section_index
            """, document_id)
            
            return [dict(section) for section in sections]
            
        finally:
            await conn.close()
    
    def _has_excessive_cid_artifacts(self, text: str) -> bool:
        """
        Check if document has too many CID artifacts (PDF extraction issues)
        
        CID artifacts appear as (cid:XX) patterns when PDF fonts can't be properly decoded.
        Documents with >1% CID content are usually problematic for analysis.
        """
        if not text or len(text) < 1000:
            return False
        
        cid_count = text.count('(cid:')
        text_length = len(text)
        cid_ratio = cid_count / text_length
        
        # Log CID statistics for monitoring
        if cid_count > 0:
            logger.debug(f"CID artifacts found: {cid_count} in {text_length:,} chars ({cid_ratio:.4f} ratio)")
        
        # Skip documents with more than 1% CID artifacts
        return cid_ratio > 0.01


if __name__ == "__main__":
    # For testing
    async def test_section_chunking_service():
        service = SectionChunkingService()
        
        print("🔧 Setting up sections database...")
        try:
            await service.setup_sections_table()
            print("✅ Database setup complete")
        except Exception as e:
            print(f"❌ Database setup failed: {e}")
            return
        
        # Get status
        try:
            stats = await service.get_sectioning_statistics()
            print("📊 Section Chunking Status:")
            
            overall = stats.get('overall', {})
            total_docs = overall.get('total_documents', 0)
            section_docs = overall.get('documents_with_sections', 0)
            total_sections = overall.get('total_sections', 0)
            
            print(f"   📄 Total documents: {total_docs:,}")
            print(f"   🧩 Documents with sections: {section_docs:,}")
            print(f"   📋 Total sections: {total_sections:,}")
            
            if total_docs > 0:
                completion_rate = (section_docs / total_docs) * 100
                print(f"   📊 Completion rate: {completion_rate:.1f}%")
            
            # Show section types
            section_types = stats.get('by_section_type', [])
            if section_types:
                print("\n📋 Section Types:")
                for section in section_types[:5]:  # Top 5
                    print(f"   {section['section_type']:20} {section['count']:5,} sections (confidence: {section['avg_confidence']:.2f})")
            
            # Show pending documents
            pending = stats.get('pending_documents', [])
            if pending:
                print(f"\n📝 Documents ready for sectioning: {len(pending)}")
                for doc in pending[:3]:  # Show first 3
                    print(f"   {doc['company_name']:25} {doc['form_type']:15} ({doc['text_length']:,} chars)")
            
        except Exception as e:
            print(f"❌ Failed to get status: {e}")
        
        print("\n🎉 Section chunking service test complete!")
        print("💡 Use CLI: python cli_section_chunking.py")
    
    asyncio.run(test_section_chunking_service())