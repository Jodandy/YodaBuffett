"""
API Schemas for Research Service
Pydantic models for request/response validation
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class AnalysisType(str, Enum):
    """Types of analysis available"""
    comprehensive = "comprehensive"
    financial = "financial"
    risk = "risk"
    competitive = "competitive"
    growth = "growth"


class DocumentType(str, Enum):
    """Types of documents"""
    quarterly_report = "quarterly_report"
    annual_report = "annual_report"
    press_release = "press_release"
    presentation = "presentation"
    other = "other"


# Request schemas
class CompanyResearchRequest(BaseModel):
    """Request for company research"""
    company_id: str
    analysis_type: AnalysisType = AnalysisType.comprehensive
    document_types: Optional[List[DocumentType]] = None
    years: Optional[List[int]] = None
    time_range: Optional[str] = None  # e.g., "2024-2025", "last_4_quarters"
    focus_areas: Optional[List[str]] = None
    language: str = "en"
    include_embeddings: bool = True
    streaming: bool = False


class DocumentSearchRequest(BaseModel):
    """Request for document search"""
    query: str
    company_id: Optional[str] = None
    document_types: Optional[List[DocumentType]] = None
    limit: int = Field(default=10, le=50)
    semantic_search: bool = True


class ComparisonRequest(BaseModel):
    """Request for company comparison"""
    company_ids: List[str] = Field(min_length=2, max_length=5)
    metrics: List[str]
    time_period: str  # e.g., "Q3 2024", "2024"
    analysis_type: str = "comparative"


class QuestionRequest(BaseModel):
    """Request for Q&A"""
    company_id: str
    question: str
    search_scope: str = "all"  # all, recent, specific_docs
    document_ids: Optional[List[str]] = None
    include_sources: bool = True


# Response schemas
class InsightResponse(BaseModel):
    """Single insight response"""
    category: str
    insight: str
    confidence: float
    supporting_evidence: List[str]
    source_documents: List[str]
    metrics: Optional[Dict[str, Any]] = None


class AnalysisResponse(BaseModel):
    """Complete analysis response"""
    request_id: str
    company_id: str
    company_name: str
    analysis_type: str
    executive_summary: str
    insights: List[InsightResponse]
    key_metrics: Dict[str, Any]
    risk_assessment: Dict[str, Any]
    recommendations: List[str]
    processing_time: float
    model_used: str
    tokens_used: int
    cost: float
    timestamp: datetime
    
    model_config = ConfigDict(from_attributes=True)


class DocumentResponse(BaseModel):
    """Document information response"""
    id: str
    company_id: str
    company_name: str
    title: str
    document_type: str
    report_period: str
    publish_date: Optional[datetime]
    file_path: Optional[str]
    source_url: Optional[str]
    language: str
    page_count: Optional[int]
    file_size_mb: Optional[float]
    has_embeddings: bool
    metadata: Dict[str, Any]


class SearchResultResponse(BaseModel):
    """Search result response"""
    document: DocumentResponse
    relevance_score: float
    matching_chunks: List[Dict[str, Any]]
    highlight: Optional[str]


class CompanyOverviewResponse(BaseModel):
    """Company overview response"""
    company_id: str
    company_name: str
    document_count: int
    latest_report: Optional[DocumentResponse]
    document_types_available: List[str]
    years_available: List[int]
    languages: List[str]
    key_metrics_summary: Dict[str, Any]
    recent_insights: List[InsightResponse]


class TimelineEntry(BaseModel):
    """Single timeline entry"""
    period: str
    date: Optional[datetime]
    document_type: str
    title: str
    metric_value: Optional[float] = None
    metric_unit: Optional[str] = None


class MetricTimelineResponse(BaseModel):
    """Metric timeline response"""
    company_id: str
    company_name: str
    metric_name: str
    timeline: List[TimelineEntry]
    trend_analysis: Optional[Dict[str, Any]]


class QuestionAnswerResponse(BaseModel):
    """Q&A response"""
    question: str
    answer: str
    confidence: float
    sources: List[Dict[str, Any]]
    related_insights: List[InsightResponse]
    follow_up_questions: List[str]


class ComparisonResult(BaseModel):
    """Single comparison result"""
    metric: str
    companies: Dict[str, Any]  # company_id -> value
    analysis: str
    winner: Optional[str] = None


class ComparisonResponse(BaseModel):
    """Company comparison response"""
    company_names: Dict[str, str]  # id -> name mapping
    time_period: str
    results: List[ComparisonResult]
    summary: str
    recommendations: Dict[str, List[str]]  # company_id -> recommendations


# Status responses
class HealthCheckResponse(BaseModel):
    """Health check response"""
    status: str
    version: str
    database_connected: bool
    embeddings_available: bool
    llm_available: bool
    timestamp: datetime


class ProcessingStatusResponse(BaseModel):
    """Processing status response"""
    request_id: str
    status: str  # pending, processing, completed, failed
    progress: float  # 0.0 to 1.0
    message: Optional[str]
    result: Optional[AnalysisResponse] = None


# Error responses
class ErrorResponse(BaseModel):
    """Error response"""
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)