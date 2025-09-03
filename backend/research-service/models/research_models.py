"""
Research Service Database Models
"""
from datetime import datetime
from typing import Optional, Dict, List
from sqlalchemy import String, Text, Integer, DateTime, JSON, ForeignKey, DECIMAL, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID, ARRAY
import uuid

# Import base from shared
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.database import Base


class ResearchSession(Base):
    """Track research sessions for audit and caching"""
    __tablename__ = "research_sessions"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"))
    session_type: Mapped[str] = mapped_column(String(50))  # 'analysis', 'search', 'qa', 'comparison'
    request_data: Mapped[Dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), default='pending')  # pending, processing, completed, failed
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    analyses: Mapped[List["AnalysisResult"]] = relationship("AnalysisResult", back_populates="session")
    
    def __repr__(self):
        return f"<ResearchSession(id={self.id}, type={self.session_type}, status={self.status})>"


class AnalysisResult(Base):
    """Store analysis results for caching and history"""
    __tablename__ = "analysis_results"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("research_sessions.id"))
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"))
    analysis_type: Mapped[str] = mapped_column(String(50))
    
    # Analysis content
    executive_summary: Mapped[str] = mapped_column(Text)
    insights: Mapped[List[Dict]] = mapped_column(JSON)  # List of insight objects
    key_metrics: Mapped[Dict] = mapped_column(JSON)
    risk_assessment: Mapped[Dict] = mapped_column(JSON)
    recommendations: Mapped[List[str]] = mapped_column(JSON)
    
    # Metadata
    model_used: Mapped[str] = mapped_column(String(50))
    tokens_used: Mapped[int] = mapped_column(Integer)
    cost: Mapped[float] = mapped_column(DECIMAL(10, 4))
    confidence_score: Mapped[float] = mapped_column(Float)
    processing_time_seconds: Mapped[float] = mapped_column(Float)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)  # For cache expiration
    
    # Relationships
    session: Mapped["ResearchSession"] = relationship("ResearchSession", back_populates="analyses")
    
    def __repr__(self):
        return f"<AnalysisResult(id={self.id}, type={self.analysis_type}, company={self.company_id})>"


class DocumentEmbedding(Base):
    """Store document embeddings for semantic search"""
    __tablename__ = "document_embeddings"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_documents.id"))
    chunk_index: Mapped[int] = mapped_column(Integer)
    chunk_text: Mapped[str] = mapped_column(Text)
    
    # Embedding vector - using ARRAY for now, can switch to pgvector
    embedding: Mapped[List[float]] = mapped_column(ARRAY(Float))
    embedding_model: Mapped[str] = mapped_column(String(50))
    
    # Metadata
    page_numbers: Mapped[List[int]] = mapped_column(ARRAY(Integer))
    char_start: Mapped[int] = mapped_column(Integer)
    char_end: Mapped[int] = mapped_column(Integer)
    metadata: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<DocumentEmbedding(doc={self.document_id}, chunk={self.chunk_index})>"


class ResearchInsight(Base):
    """Store individual insights for tracking and learning"""
    __tablename__ = "research_insights"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"))
    analysis_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("analysis_results.id"), nullable=True)
    
    # Insight content
    category: Mapped[str] = mapped_column(String(50))  # financial, strategic, risk, competitive
    insight_text: Mapped[str] = mapped_column(Text)
    confidence: Mapped[float] = mapped_column(Float)
    
    # Supporting data
    supporting_evidence: Mapped[List[str]] = mapped_column(JSON)
    source_documents: Mapped[List[str]] = mapped_column(JSON)
    metrics: Mapped[Optional[Dict]] = mapped_column(JSON, nullable=True)
    
    # Tracking
    importance_score: Mapped[float] = mapped_column(Float, default=0.5)
    verified: Mapped[bool] = mapped_column(default=False)
    tags: Mapped[List[str]] = mapped_column(JSON, default=list)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    valid_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<ResearchInsight(id={self.id}, category={self.category}, company={self.company_id})>"


class CompanyMetricHistory(Base):
    """Track company metrics over time"""
    __tablename__ = "company_metric_history"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_companies.id"))
    document_id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("nordic_documents.id"))
    
    # Metric data
    metric_name: Mapped[str] = mapped_column(String(100))
    metric_value: Mapped[float] = mapped_column(Float)
    metric_unit: Mapped[str] = mapped_column(String(50))
    period: Mapped[str] = mapped_column(String(50))  # Q1 2024, FY 2023, etc.
    period_date: Mapped[datetime] = mapped_column(DateTime)
    
    # Context
    confidence: Mapped[float] = mapped_column(Float, default=1.0)
    extraction_method: Mapped[str] = mapped_column(String(50))  # 'llm', 'regex', 'manual'
    context_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<CompanyMetric(company={self.company_id}, metric={self.metric_name}, value={self.metric_value})>"


class SearchQuery(Base):
    """Track search queries for analytics and improvement"""
    __tablename__ = "search_queries"
    
    id: Mapped[UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    session_id: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("research_sessions.id"), nullable=True)
    
    # Query data
    query_text: Mapped[str] = mapped_column(Text)
    query_type: Mapped[str] = mapped_column(String(50))  # 'semantic', 'keyword', 'qa'
    company_filter: Mapped[Optional[UUID]] = mapped_column(UUID(as_uuid=True), nullable=True)
    
    # Results
    result_count: Mapped[int] = mapped_column(Integer, default=0)
    top_results: Mapped[List[Dict]] = mapped_column(JSON)  # Top 5 results with scores
    user_clicked: Mapped[Optional[List[str]]] = mapped_column(JSON, nullable=True)  # Document IDs clicked
    
    # Performance
    search_time_ms: Mapped[int] = mapped_column(Integer)
    embedding_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<SearchQuery(query='{self.query_text[:50]}...', results={self.result_count})>"