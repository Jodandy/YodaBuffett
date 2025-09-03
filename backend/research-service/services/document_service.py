"""
Document Service
Manages document retrieval and processing for research
"""
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, desc

from ..processors.pdf_processor import PDFProcessor, ProcessedPDF
from ..processors.financial_parser import FinancialParser
from ..processors.language_detector import LanguageDetector
from .embedding_service import EmbeddingService

# Import from main backend models
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from nordic_ingestion.models import NordicDocument, NordicCompany

logger = logging.getLogger(__name__)


class DocumentService:
    """Handles document retrieval and processing"""
    
    def __init__(self):
        self.pdf_processor = PDFProcessor()
        self.financial_parser = FinancialParser()
        self.language_detector = LanguageDetector()
        self.embedding_service = EmbeddingService()
    
    async def get_company_documents(
        self,
        db: AsyncSession,
        company_id: str,
        document_types: Optional[List[str]] = None,
        years: Optional[List[int]] = None,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Retrieve documents for a company"""
        
        # Build query
        query = select(NordicDocument).where(
            NordicDocument.company_id == company_id
        )
        
        # Filter by document type
        if document_types:
            query = query.where(
                NordicDocument.document_type.in_(document_types)
            )
        
        # Filter by year
        if years:
            year_conditions = []
            for year in years:
                year_conditions.append(
                    NordicDocument.report_period.like(f"%{year}%")
                )
            query = query.where(or_(*year_conditions))
        
        # Order by date and limit
        query = query.order_by(desc(NordicDocument.publish_date))
        if limit:
            query = query.limit(limit)
        
        # Execute query
        result = await db.execute(query)
        documents = result.scalars().all()
        
        # Convert to dict format
        doc_list = []
        for doc in documents:
            doc_dict = {
                "id": str(doc.id),
                "title": doc.title,
                "document_type": doc.document_type,
                "report_period": doc.report_period,
                "publish_date": doc.publish_date.isoformat() if doc.publish_date else None,
                "file_path": doc.storage_path,
                "source_url": doc.source_url,
                "language": doc.language,
                "metadata": doc.metadata_ or {}
            }
            doc_list.append(doc_dict)
        
        return doc_list
    
    async def process_documents(
        self,
        documents: List[Dict],
        extract_financials: bool = True,
        generate_embeddings: bool = True
    ) -> List[ProcessedPDF]:
        """Process multiple documents"""
        
        processed_docs = []
        
        for doc in documents:
            if doc.get('file_path') and os.path.exists(doc['file_path']):
                # Process PDF
                processed = await self.pdf_processor.process_pdf(doc['file_path'])
                
                # Extract financial data if requested
                if extract_financials:
                    financial_data = self.financial_parser.parse_document(
                        processed.full_text,
                        metadata=doc
                    )
                    processed.metadata['financial_data'] = financial_data.__dict__
                
                # Generate embeddings if requested
                if generate_embeddings:
                    embeddings = await self.embedding_service.generate_embeddings(
                        processed.chunks
                    )
                    processed.metadata['embeddings_generated'] = len(embeddings)
                
                processed_docs.append(processed)
            else:
                logger.warning(f"Document file not found: {doc.get('file_path')}")
        
        return processed_docs
    
    async def search_documents(
        self,
        db: AsyncSession,
        query: str,
        company_id: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict]:
        """Search documents semantically"""
        
        # Use embedding service for semantic search
        search_results = await self.embedding_service.semantic_search(
            query=query,
            company_id=company_id,
            limit=limit
        )
        
        # If no semantic results, fall back to keyword search
        if not search_results:
            return await self._keyword_search(db, query, company_id, limit)
        
        # Enrich results with document metadata
        enriched_results = []
        for result in search_results:
            doc = await self._get_document_by_id(db, result.document_id)
            if doc:
                enriched_results.append({
                    "document": doc,
                    "chunk": result.chunk_text,
                    "score": result.similarity_score,
                    "metadata": result.metadata
                })
        
        return enriched_results
    
    async def _keyword_search(
        self,
        db: AsyncSession,
        query: str,
        company_id: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict]:
        """Fallback keyword search"""
        
        # Simple title/content search
        search_query = select(NordicDocument).where(
            or_(
                NordicDocument.title.ilike(f"%{query}%"),
                NordicDocument.metadata_['llm_filter_context']['content'].astext.ilike(f"%{query}%")
            )
        )
        
        if company_id:
            search_query = search_query.where(
                NordicDocument.company_id == company_id
            )
        
        search_query = search_query.limit(limit)
        
        result = await db.execute(search_query)
        documents = result.scalars().all()
        
        return [self._document_to_dict(doc) for doc in documents]
    
    async def _get_document_by_id(
        self,
        db: AsyncSession,
        document_id: str
    ) -> Optional[Dict]:
        """Get document by ID"""
        
        result = await db.execute(
            select(NordicDocument).where(NordicDocument.id == document_id)
        )
        doc = result.scalar_one_or_none()
        
        return self._document_to_dict(doc) if doc else None
    
    def _document_to_dict(self, doc: NordicDocument) -> Dict:
        """Convert document model to dict"""
        return {
            "id": str(doc.id),
            "company_id": str(doc.company_id),
            "title": doc.title,
            "document_type": doc.document_type,
            "report_period": doc.report_period,
            "publish_date": doc.publish_date.isoformat() if doc.publish_date else None,
            "file_path": doc.storage_path,
            "source_url": doc.source_url,
            "language": doc.language,
            "metadata": doc.metadata_ or {}
        }
    
    async def get_document_timeline(
        self,
        db: AsyncSession,
        company_id: str,
        metric: Optional[str] = None
    ) -> List[Dict]:
        """Get timeline of documents/metrics for a company"""
        
        documents = await self.get_company_documents(
            db,
            company_id,
            document_types=['quarterly_report', 'annual_report']
        )
        
        timeline = []
        for doc in documents:
            entry = {
                "period": doc['report_period'],
                "date": doc['publish_date'],
                "document_type": doc['document_type'],
                "title": doc['title']
            }
            
            # Add specific metric if requested
            if metric and 'financial_data' in doc.get('metadata', {}):
                metrics = doc['metadata']['financial_data'].get('metrics', [])
                for m in metrics:
                    if m['name'] == metric:
                        entry['metric_value'] = m['value']
                        entry['metric_unit'] = m['unit']
                        break
            
            timeline.append(entry)
        
        return timeline