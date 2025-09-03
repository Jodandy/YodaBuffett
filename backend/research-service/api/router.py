"""
API Router for Research Service
FastAPI endpoints for company research
"""
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import logging
import json

from ..config import settings
from ..services.document_service import DocumentService
from ..services.analysis_service import AnalysisService, AnalysisRequest
from ..services.embedding_service import EmbeddingService
from ..services.insight_service import InsightService
from .schemas import (
    CompanyResearchRequest, DocumentSearchRequest, ComparisonRequest,
    QuestionRequest, AnalysisResponse, DocumentResponse, SearchResultResponse,
    CompanyOverviewResponse, MetricTimelineResponse, QuestionAnswerResponse,
    ComparisonResponse, HealthCheckResponse, ProcessingStatusResponse,
    ErrorResponse
)

# Import database dependency
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.database import get_db
from nordic_ingestion.models import NordicCompany

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix=settings.api_prefix, tags=["research"])

# Initialize services
document_service = DocumentService()
analysis_service = AnalysisService()
embedding_service = EmbeddingService()
insight_service = InsightService()


@router.get("/health", response_model=HealthCheckResponse)
async def health_check(db: AsyncSession = Depends(get_db)):
    """Check service health"""
    
    # Check database
    try:
        await db.execute("SELECT 1")
        db_connected = True
    except:
        db_connected = False
    
    # Check services
    embeddings_available = settings.openai_api_key is not None
    llm_available = settings.openai_api_key is not None
    
    return HealthCheckResponse(
        status="healthy" if all([db_connected, embeddings_available, llm_available]) else "degraded",
        version=settings.service_version,
        database_connected=db_connected,
        embeddings_available=embeddings_available,
        llm_available=llm_available,
        timestamp=datetime.now()
    )


@router.get("/companies", response_model=List[CompanyOverviewResponse])
async def list_companies(
    db: AsyncSession = Depends(get_db),
    country: Optional[str] = Query(None, description="Filter by country code"),
    limit: int = Query(50, le=100)
):
    """List available companies with overview"""
    
    # Get companies
    query = select(NordicCompany)
    if country:
        query = query.where(NordicCompany.country == country)
    query = query.limit(limit)
    
    result = await db.execute(query)
    companies = result.scalars().all()
    
    # Build overview for each company
    overviews = []
    for company in companies:
        # Get document stats
        docs = await document_service.get_company_documents(
            db, str(company.id), limit=10
        )
        
        # Build overview
        overview = CompanyOverviewResponse(
            company_id=str(company.id),
            company_name=company.name,
            document_count=len(docs),
            latest_report=DocumentResponse(**docs[0]) if docs else None,
            document_types_available=list(set(d['document_type'] for d in docs)),
            years_available=sorted(set(
                int(d['report_period'][:4]) 
                for d in docs 
                if d['report_period'] and d['report_period'][:4].isdigit()
            )),
            languages=list(set(d['language'] for d in docs)),
            key_metrics_summary={},  # Would populate from latest analysis
            recent_insights=[]  # Would populate from cached insights
        )
        overviews.append(overview)
    
    return overviews


@router.get("/company/{company_id}", response_model=CompanyOverviewResponse)
async def get_company_overview(
    company_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get detailed company overview"""
    
    # Get company
    result = await db.execute(
        select(NordicCompany).where(NordicCompany.id == company_id)
    )
    company = result.scalar_one_or_none()
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Get documents
    docs = await document_service.get_company_documents(db, company_id)
    
    # Build detailed overview
    overview = CompanyOverviewResponse(
        company_id=str(company.id),
        company_name=company.name,
        document_count=len(docs),
        latest_report=DocumentResponse(**docs[0]) if docs else None,
        document_types_available=list(set(d['document_type'] for d in docs)),
        years_available=sorted(set(
            int(d['report_period'][:4]) 
            for d in docs 
            if d['report_period'] and d['report_period'][:4].isdigit()
        )),
        languages=list(set(d['language'] for d in docs)),
        key_metrics_summary={},  # TODO: Populate from analysis
        recent_insights=[]  # TODO: Populate from cache
    )
    
    return overview


@router.post("/company/{company_id}/analyze", response_model=AnalysisResponse)
async def analyze_company(
    company_id: str,
    request: CompanyResearchRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db)
):
    """Perform deep analysis of a company"""
    
    # Get company
    result = await db.execute(
        select(NordicCompany).where(NordicCompany.id == company_id)
    )
    company = result.scalar_one_or_none()
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Get documents
    documents = await document_service.get_company_documents(
        db,
        company_id,
        document_types=request.document_types,
        years=request.years
    )
    
    if not documents:
        raise HTTPException(status_code=404, detail="No documents found for analysis")
    
    # Process documents
    processed_docs = await document_service.process_documents(
        documents,
        extract_financials=True,
        generate_embeddings=request.include_embeddings
    )
    
    # Prepare chunks for analysis
    all_chunks = []
    for doc in processed_docs:
        for chunk in doc.chunks:
            all_chunks.append({
                'document_id': documents[0]['id'],  # Map back to original
                'text': chunk.text,
                'pages': chunk.page_numbers
            })
    
    # Create analysis request
    analysis_request = AnalysisRequest(
        company_id=company_id,
        company_name=company.name,
        analysis_type=request.analysis_type,
        documents=documents,
        time_range=request.time_range,
        focus_areas=request.focus_areas,
        language=request.language,
        streaming=request.streaming
    )
    
    # Run analysis
    if request.streaming:
        # Return streaming response
        return StreamingResponse(
            analysis_service.analyze_company(analysis_request, all_chunks),
            media_type="text/event-stream"
        )
    else:
        # Run complete analysis
        result = await analysis_service.analyze_company(analysis_request, all_chunks)
        
        # Convert to response model
        return AnalysisResponse(
            request_id=result.request_id,
            company_id=company_id,
            company_name=result.company_name,
            analysis_type=result.analysis_type,
            executive_summary=result.executive_summary,
            insights=[
                {
                    "category": i.category,
                    "insight": i.insight,
                    "confidence": i.confidence,
                    "supporting_evidence": i.supporting_evidence,
                    "source_documents": i.source_documents,
                    "metrics": i.metrics
                }
                for i in result.insights
            ],
            key_metrics=result.key_metrics,
            risk_assessment=result.risk_assessment,
            recommendations=result.recommendations,
            processing_time=0.0,  # TODO: Track actual time
            model_used=result.model_used,
            tokens_used=result.tokens_used,
            cost=result.cost,
            timestamp=result.timestamp
        )


@router.post("/search", response_model=List[SearchResultResponse])
async def search_documents(
    request: DocumentSearchRequest,
    db: AsyncSession = Depends(get_db)
):
    """Search across documents"""
    
    results = await document_service.search_documents(
        db,
        query=request.query,
        company_id=request.company_id,
        limit=request.limit
    )
    
    # Convert to response format
    search_results = []
    for result in results:
        search_results.append(SearchResultResponse(
            document=DocumentResponse(**result['document']),
            relevance_score=result.get('score', 0.0),
            matching_chunks=[{
                'text': result.get('chunk', ''),
                'metadata': result.get('metadata', {})
            }],
            highlight=result.get('chunk', '')[:200] + '...'
        ))
    
    return search_results


@router.get("/company/{company_id}/timeline", response_model=MetricTimelineResponse)
async def get_metric_timeline(
    company_id: str,
    metric: str = Query(..., description="Metric to track over time"),
    db: AsyncSession = Depends(get_db)
):
    """Get timeline of a specific metric"""
    
    # Get company
    result = await db.execute(
        select(NordicCompany).where(NordicCompany.id == company_id)
    )
    company = result.scalar_one_or_none()
    
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    
    # Get timeline
    timeline_data = await document_service.get_document_timeline(
        db, company_id, metric=metric
    )
    
    # Generate trend insights
    trend_insights = insight_service.generate_trend_insights(timeline_data)
    
    return MetricTimelineResponse(
        company_id=company_id,
        company_name=company.name,
        metric_name=metric,
        timeline=[TimelineEntry(**entry) for entry in timeline_data],
        trend_analysis=trend_insights[0] if trend_insights else None
    )


@router.post("/company/{company_id}/ask", response_model=QuestionAnswerResponse)
async def ask_question(
    company_id: str,
    request: QuestionRequest,
    db: AsyncSession = Depends(get_db)
):
    """Ask a question about a company"""
    
    # This would implement Q&A functionality
    # For now, return a placeholder
    
    return QuestionAnswerResponse(
        question=request.question,
        answer="Q&A functionality coming soon",
        confidence=0.0,
        sources=[],
        related_insights=[],
        follow_up_questions=[]
    )


@router.post("/compare", response_model=ComparisonResponse)
async def compare_companies(
    request: ComparisonRequest,
    db: AsyncSession = Depends(get_db)
):
    """Compare multiple companies"""
    
    # This would implement comparison functionality
    # For now, return a placeholder
    
    return ComparisonResponse(
        company_names={},
        time_period=request.time_period,
        results=[],
        summary="Comparison functionality coming soon",
        recommendations={}
    )


# Add more endpoints as needed...