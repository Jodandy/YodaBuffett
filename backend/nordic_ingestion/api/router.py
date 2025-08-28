"""
Nordic Ingestion API Router
Production-ready endpoints for Swedish financial data ingestion
"""
import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from shared.database import get_db_session
from shared.monitoring import record_document_processed
from ..models import NordicCompany, NordicDocument, NordicCalendarEvent, ManualCollectionTask, NordicIngestionLog
from .schemas import (
    CompanyResponse, 
    CompanyCreateRequest,
    DocumentResponse, 
    CalendarEventResponse,
    CalendarEventCreateRequest,
    ManualTaskResponse,
    HealthResponse,
    CollectionStatsResponse
)

# Create router
router = APIRouter()

logger = logging.getLogger(__name__)


@router.get("/health", response_model=HealthResponse)
async def health_check(db: AsyncSession = Depends(get_db_session)):
    """Nordic ingestion service health check"""
    try:
        # Check database connectivity
        result = await db.execute(select(func.count(NordicCompany.id)))
        company_count = result.scalar_one()
        
        # Check recent ingestion activity
        recent_docs = await db.execute(
            select(func.count(NordicDocument.id)).where(
                NordicDocument.ingestion_date >= datetime.utcnow() - timedelta(days=1)
            )
        )
        recent_count = recent_docs.scalar_one()
        
        return HealthResponse(
            status="healthy",
            database_connected=True,
            companies_registered=company_count,
            documents_last_24h=recent_count,
            timestamp=datetime.now(timezone.utc)
        )
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Nordic ingestion service unhealthy"
        )


@router.get("/companies", response_model=List[CompanyResponse])
async def list_companies(
    country: Optional[str] = Query(None, description="Filter by country code (SE, NO, DK, FI)"),
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db_session)
):
    """List Nordic companies"""
    query = select(NordicCompany).order_by(NordicCompany.name)
    
    if country:
        query = query.where(NordicCompany.country == country.upper())
    if exchange:
        query = query.where(NordicCompany.exchange == exchange)
    
    query = query.offset(offset).limit(limit)
    
    result = await db.execute(query)
    companies = result.scalars().all()
    
    return [CompanyResponse.model_validate(company) for company in companies]


@router.get("/companies/{company_id}", response_model=CompanyResponse)
async def get_company(
    company_id: str,
    db: AsyncSession = Depends(get_db_session)
):
    """Get company details"""
    result = await db.execute(
        select(NordicCompany).where(NordicCompany.id == company_id)
    )
    company = result.scalar_one_or_none()
    
    if not company:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Company not found"
        )
    
    return CompanyResponse.model_validate(company)


@router.post("/companies", response_model=CompanyResponse, status_code=status.HTTP_201_CREATED)
async def create_company(
    company_data: CompanyCreateRequest,
    db: AsyncSession = Depends(get_db_session)
):
    """Create new Nordic company"""
    
    # Check if company already exists
    existing_query = select(NordicCompany).where(
        NordicCompany.name == company_data.name,
        NordicCompany.country == company_data.country
    )
    result = await db.execute(existing_query)
    
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Company already exists"
        )
    
    # Create new company
    new_company = NordicCompany(
        name=company_data.name,
        ticker=company_data.ticker,
        exchange=company_data.exchange,
        country=company_data.country,
        market_cap_category=company_data.market_cap_category,
        sector=company_data.sector,
        ir_email=company_data.ir_email,
        ir_website=company_data.ir_website,
        website=company_data.website,
        reporting_language=company_data.reporting_language
    )
    
    db.add(new_company)
    await db.commit()
    await db.refresh(new_company)
    
    logger.info(f"Created new company: {new_company.name} ({new_company.country})")
    
    return CompanyResponse.model_validate(new_company)


@router.get("/documents", response_model=List[DocumentResponse])
async def list_documents(
    company_id: Optional[str] = Query(None, description="Filter by company ID"),
    document_type: Optional[str] = Query(None, description="Filter by document type (Q1, Q2, Q3, annual, press_release)"),
    language: Optional[str] = Query(None, description="Filter by language (sv, en, no, da, fi)"),
    since: Optional[date] = Query(None, description="Documents ingested since this date"),
    limit: int = Query(50, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db_session)
):
    """List Nordic documents"""
    query = select(NordicDocument).order_by(NordicDocument.ingestion_date.desc())
    
    if company_id:
        query = query.where(NordicDocument.company_id == company_id)
    if document_type:
        query = query.where(NordicDocument.document_type == document_type)
    if language:
        query = query.where(NordicDocument.language == language)
    if since:
        query = query.where(NordicDocument.ingestion_date >= since)
    
    query = query.offset(offset).limit(limit)
    
    result = await db.execute(query)
    documents = result.scalars().all()
    
    return [DocumentResponse.model_validate(doc) for doc in documents]


@router.get("/calendar", response_model=List[CalendarEventResponse])
async def list_calendar_events(
    company_id: Optional[str] = Query(None, description="Filter by company ID"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    upcoming_days: Optional[int] = Query(None, description="Only events in next N days"),
    confirmed_only: bool = Query(False, description="Only confirmed events"),
    limit: int = Query(100, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db_session)
):
    """List Nordic calendar events"""
    query = select(NordicCalendarEvent).order_by(NordicCalendarEvent.event_date)
    
    if company_id:
        query = query.where(NordicCalendarEvent.company_id == company_id)
    if event_type:
        query = query.where(NordicCalendarEvent.event_type == event_type)
    if upcoming_days:
        cutoff_date = date.today() + timedelta(days=upcoming_days)
        query = query.where(
            NordicCalendarEvent.event_date.between(date.today(), cutoff_date)
        )
    if confirmed_only:
        query = query.where(NordicCalendarEvent.confirmed == True)
    
    query = query.offset(offset).limit(limit)
    
    result = await db.execute(query)
    events = result.scalars().all()
    
    return [CalendarEventResponse.model_validate(event) for event in events]


@router.get("/calendar/this-week", response_model=List[CalendarEventResponse])
async def this_week_calendar(db: AsyncSession = Depends(get_db_session)):
    """Get all Nordic events for this week"""
    today = date.today()
    week_end = today + timedelta(days=7)
    
    result = await db.execute(
        select(NordicCalendarEvent)
        .where(NordicCalendarEvent.event_date.between(today, week_end))
        .order_by(NordicCalendarEvent.event_date, NordicCalendarEvent.event_time)
    )
    events = result.scalars().all()
    
    return [CalendarEventResponse.model_validate(event) for event in events]


@router.post("/calendar", response_model=CalendarEventResponse, status_code=status.HTTP_201_CREATED)
async def create_calendar_event(
    event_data: CalendarEventCreateRequest,
    db: AsyncSession = Depends(get_db_session)
):
    """Create new calendar event"""
    
    # Check if event already exists
    existing_query = select(NordicCalendarEvent).where(
        NordicCalendarEvent.company_id == event_data.company_id,
        NordicCalendarEvent.event_date == event_data.event_date,
        NordicCalendarEvent.title == event_data.title
    )
    result = await db.execute(existing_query)
    
    if result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Calendar event already exists"
        )
    
    # Verify company exists
    company_query = select(NordicCompany).where(NordicCompany.id == event_data.company_id)
    company_result = await db.execute(company_query)
    
    if not company_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Company not found"
        )
    
    # Create new calendar event
    new_event = NordicCalendarEvent(
        company_id=event_data.company_id,
        event_type=event_data.event_type,
        event_date=event_data.event_date,
        event_time=event_data.event_time,
        report_period=event_data.report_period,
        title=event_data.title,
        description=event_data.description,
        location=event_data.location,
        webcast_url=event_data.webcast_url,
        source_url=event_data.source_url,
        confirmed=event_data.confirmed
    )
    
    db.add(new_event)
    await db.commit()
    await db.refresh(new_event)
    
    logger.info(f"Created calendar event: {new_event.title} for {new_event.event_date}")
    
    return CalendarEventResponse.model_validate(new_event)


@router.put("/calendar/{event_id}", response_model=CalendarEventResponse)
async def update_calendar_event(
    event_id: str,
    event_data: CalendarEventCreateRequest,
    db: AsyncSession = Depends(get_db_session)
):
    """Update calendar event"""
    
    # Find existing event
    result = await db.execute(
        select(NordicCalendarEvent).where(NordicCalendarEvent.id == event_id)
    )
    existing_event = result.scalar_one_or_none()
    
    if not existing_event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Calendar event not found"
        )
    
    # Update event fields
    existing_event.event_type = event_data.event_type
    existing_event.event_date = event_data.event_date
    existing_event.event_time = event_data.event_time
    existing_event.report_period = event_data.report_period
    existing_event.title = event_data.title
    existing_event.description = event_data.description
    existing_event.location = event_data.location
    existing_event.webcast_url = event_data.webcast_url
    existing_event.source_url = event_data.source_url
    existing_event.confirmed = event_data.confirmed
    existing_event.updated_date = datetime.utcnow()
    
    await db.commit()
    await db.refresh(existing_event)
    
    logger.info(f"Updated calendar event: {existing_event.title}")
    
    return CalendarEventResponse.model_validate(existing_event)


@router.get("/manual-tasks", response_model=List[ManualTaskResponse])
async def list_manual_tasks(
    status: Optional[str] = Query(None, description="Filter by status (pending, in_progress, completed)"),
    priority: Optional[str] = Query(None, description="Filter by priority"),
    company_id: Optional[str] = Query(None, description="Filter by company ID"),
    overdue_only: bool = Query(False, description="Only overdue tasks"),
    limit: int = Query(50, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db_session)
):
    """List manual collection tasks"""
    query = select(ManualCollectionTask).order_by(ManualCollectionTask.created_at.desc())
    
    if status:
        query = query.where(ManualCollectionTask.status == status)
    if priority:
        query = query.where(ManualCollectionTask.priority == priority)
    if company_id:
        query = query.where(ManualCollectionTask.company_id == company_id)
    if overdue_only:
        query = query.where(
            ManualCollectionTask.deadline < datetime.utcnow(),
            ManualCollectionTask.status.in_(['pending', 'in_progress'])
        )
    
    query = query.offset(offset).limit(limit)
    
    result = await db.execute(query)
    tasks = result.scalars().all()
    
    return [ManualTaskResponse.model_validate(task) for task in tasks]


@router.get("/stats", response_model=CollectionStatsResponse)
async def collection_stats(db: AsyncSession = Depends(get_db_session)):
    """Get Nordic ingestion statistics"""
    
    # Company counts by country
    company_stats = await db.execute(
        select(NordicCompany.country, func.count(NordicCompany.id))
        .group_by(NordicCompany.country)
    )
    companies_by_country = dict(company_stats.all())
    
    # Document counts by type
    doc_stats = await db.execute(
        select(NordicDocument.document_type, func.count(NordicDocument.id))
        .group_by(NordicDocument.document_type)
    )
    documents_by_type = dict(doc_stats.all())
    
    # Recent activity (last 7 days)
    recent_docs = await db.execute(
        select(func.count(NordicDocument.id))
        .where(NordicDocument.ingestion_date >= datetime.utcnow() - timedelta(days=7))
    )
    recent_document_count = recent_docs.scalar_one()
    
    # Pending manual tasks
    pending_tasks = await db.execute(
        select(func.count(ManualCollectionTask.id))
        .where(ManualCollectionTask.status.in_(['pending', 'in_progress']))
    )
    pending_manual_tasks = pending_tasks.scalar_one()
    
    # Upcoming events (next 30 days)
    upcoming_events = await db.execute(
        select(func.count(NordicCalendarEvent.id))
        .where(NordicCalendarEvent.event_date.between(
            date.today(),
            date.today() + timedelta(days=30)
        ))
    )
    upcoming_event_count = upcoming_events.scalar_one()
    
    return CollectionStatsResponse(
        companies_by_country=companies_by_country,
        documents_by_type=documents_by_type,
        recent_documents_7d=recent_document_count,
        pending_manual_tasks=pending_manual_tasks,
        upcoming_events_30d=upcoming_event_count,
        last_updated=datetime.now(timezone.utc)
    )


@router.post("/collect/run", status_code=status.HTTP_202_ACCEPTED)
async def trigger_collection():
    """Manually trigger data collection (for testing)"""
    try:
        # Import here to avoid circular imports
        from ..orchestrator.daily_collector import run_collection_now
        
        # Run collection in background
        asyncio.create_task(run_collection_now())
        
        return {
            "message": "Collection started",
            "status": "running",
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger collection: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to start collection"
        )


@router.get("/collect/status")
async def collection_status(db: AsyncSession = Depends(get_db_session)):
    """Get current collection status"""
    try:
        # Get recent collection activity
        recent_logs = await db.execute(
            select(NordicIngestionLog)
            .order_by(NordicIngestionLog.timestamp.desc())
            .limit(5)
        )
        recent_activity = recent_logs.scalars().all()
        
        return {
            "status": "ready",
            "recent_collections": [
                {
                    "timestamp": log.timestamp,
                    "status": log.status,
                    "reports_found": log.reports_found,
                    "processing_time": log.processing_time_seconds
                }
                for log in recent_activity
            ],
            "last_updated": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error(f"Failed to get collection status: {e}")
        return {
            "status": "unknown",
            "error": str(e),
            "last_updated": datetime.now(timezone.utc)
        }