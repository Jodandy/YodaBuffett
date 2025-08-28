"""
Nordic Ingestion API Schemas
Pydantic models for request/response validation
"""
from datetime import datetime, date, time
from typing import Optional, Dict, Any, List
from decimal import Decimal
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID


class CompanyResponse(BaseModel):
    """Nordic company response schema"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    name: str
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    country: str
    market_cap_category: Optional[str] = None
    sector: Optional[str] = None
    ir_email: Optional[str] = None
    ir_website: Optional[str] = None
    website: Optional[str] = None
    reporting_language: str = "sv"
    created_at: datetime
    updated_at: datetime


class DocumentResponse(BaseModel):
    """Nordic document response schema"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    company_id: UUID
    document_type: str
    report_period: str
    title: str
    source_url: Optional[str] = None
    language: str = "sv"
    ingestion_date: datetime
    processing_status: str = "pending"
    page_count: Optional[int] = None
    file_size_mb: Optional[Decimal] = None
    created_at: datetime
    
    # Related company info (if needed)
    company: Optional[CompanyResponse] = None


class CalendarEventResponse(BaseModel):
    """Nordic calendar event response schema"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    company_id: UUID
    event_type: str
    event_date: date
    event_time: Optional[time] = None
    report_period: Optional[str] = None
    title: str
    description: Optional[str] = None
    location: Optional[str] = None
    webcast_url: Optional[str] = None
    source_url: Optional[str] = None
    confirmed: bool = False
    created_date: datetime
    
    # Computed properties
    is_upcoming: bool = Field(default=False, computed=True)
    days_until_event: int = Field(default=0, computed=True)
    
    # Related company info (if needed)
    company: Optional[CompanyResponse] = None


class ManualTaskResponse(BaseModel):
    """Manual collection task response schema"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    company_id: UUID
    document_type: str
    report_period: str
    priority: str = "medium"
    status: str = "pending"
    failure_reason: str
    error_details: Optional[str] = None
    expected_url: Optional[str] = None
    manual_steps: Optional[str] = None
    deadline: Optional[datetime] = None
    github_issue_number: Optional[int] = None
    github_issue_url: Optional[str] = None
    assigned_to: Optional[str] = None
    completed_by: Optional[str] = None
    completion_notes: Optional[str] = None
    created_at: datetime
    completed_at: Optional[datetime] = None
    
    # Computed properties
    is_overdue: bool = Field(default=False, computed=True)
    
    # Related company info
    company: Optional[CompanyResponse] = None


class HealthResponse(BaseModel):
    """Health check response schema"""
    status: str = "healthy"
    database_connected: bool = True
    companies_registered: int = 0
    documents_last_24h: int = 0
    timestamp: datetime
    
    
class CollectionStatsResponse(BaseModel):
    """Collection statistics response schema"""
    companies_by_country: Dict[str, int] = Field(default_factory=dict)
    documents_by_type: Dict[str, int] = Field(default_factory=dict)
    recent_documents_7d: int = 0
    pending_manual_tasks: int = 0
    upcoming_events_30d: int = 0
    last_updated: datetime


class DocumentUploadRequest(BaseModel):
    """Manual document upload request schema"""
    company_id: UUID
    document_type: str = Field(..., regex="^(Q1|Q2|Q3|annual|press_release|other)$")
    report_period: str
    title: str
    source_url: Optional[str] = None
    notes: Optional[str] = None


class CompanyCreateRequest(BaseModel):
    """Create new Nordic company request schema"""
    name: str = Field(..., min_length=1, max_length=255)
    ticker: Optional[str] = Field(None, max_length=50)
    exchange: Optional[str] = Field(None, max_length=50)
    country: str = Field(..., regex="^(SE|NO|DK|FI)$")
    market_cap_category: Optional[str] = Field(None, regex="^(Large|Mid|Small)$")
    sector: Optional[str] = Field(None, max_length=100)
    ir_email: Optional[str] = Field(None, max_length=255)
    ir_website: Optional[str] = None
    website: Optional[str] = None
    reporting_language: str = Field(default="sv", regex="^(sv|no|da|fi|en)$")


class CalendarEventCreateRequest(BaseModel):
    """Create calendar event request schema"""
    company_id: UUID
    event_type: str = Field(..., regex="^(Q1_report|Q2_report|Q3_report|annual_report|agm|earnings_call|other)$")
    event_date: date
    event_time: Optional[time] = None
    report_period: Optional[str] = None
    title: str = Field(..., min_length=1)
    description: Optional[str] = None
    location: Optional[str] = None
    webcast_url: Optional[str] = None
    source_url: Optional[str] = None
    confirmed: bool = False


class DataSourceResponse(BaseModel):
    """Data source response schema"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    company_id: UUID
    source_type: str
    priority: int
    config: Optional[Dict[str, Any]] = None
    status: str = "active"
    last_success: Optional[datetime] = None
    failure_count: int = 0
    notes: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class IngestionLogResponse(BaseModel):
    """Ingestion log response schema"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    source_id: UUID
    timestamp: datetime
    collection_type: str
    status: str
    error_message: Optional[str] = None
    reports_found: int = 0
    reports_downloaded: int = 0
    news_items_found: int = 0
    processing_time_seconds: Optional[int] = None


# Error response schemas
class ErrorResponse(BaseModel):
    """Standard error response"""
    detail: str
    error_code: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ValidationErrorResponse(BaseModel):
    """Validation error response"""
    detail: str
    errors: List[Dict[str, Any]]
    timestamp: datetime = Field(default_factory=datetime.utcnow)