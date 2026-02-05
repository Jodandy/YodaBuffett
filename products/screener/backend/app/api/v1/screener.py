"""
Screener API endpoints - core screening functionality
"""
import logging
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db, get_db_manager, DatabaseManager
from app.schemas.screener import (
    ScreenerQuery, ScreenerResponse, SavedQuery, SaveQueryRequest,
    ApiResponse, ApiError, ExportRequest, ExportResponse
)
from app.services.screener_service import ScreenerService
from app.services.metrics_service import MetricsService
from app.services.query_builder import QueryBuilder

logger = logging.getLogger(__name__)

router = APIRouter()


async def get_screener_service(
    db_manager: DatabaseManager = Depends(get_db_manager)
) -> ScreenerService:
    """Get screener service dependency"""
    metrics_service = MetricsService(db_manager)
    return ScreenerService(db_manager, metrics_service)


@router.post("/run", response_model=ScreenerResponse)
async def run_screen(
    query: ScreenerQuery,
    screener_service: ScreenerService = Depends(get_screener_service),
    db: AsyncSession = Depends(get_db)
):
    """
    Execute a screening query with optional point-in-time data
    
    Features:
    - Complex AND/OR logic combinations
    - Relative metric comparisons (P/E < Industry P/E)
    - Point-in-time historical screening
    - Forward return calculation for backtesting
    """
    try:
        # Validate query
        query_builder = QueryBuilder()
        validation_errors = query_builder.validate_query(query)
        
        if validation_errors:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Query validation failed",
                    "errors": validation_errors
                }
            )
        
        # Execute screening
        response = await screener_service.execute_screen(query, db)
        
        logger.info(f"Screen executed: {response.totalMatches} matches")
        return response
        
    except ValidationError as e:
        logger.error(f"Validation error in run_screen: {e}")
        raise HTTPException(
            status_code=400, 
            detail={
                "message": "Query validation failed",
                "errors": [str(err) for err in e.errors()]
            }
        )
    
    except ValueError as e:
        logger.error(f"Business logic error in run_screen: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except Exception as e:
        logger.error(f"Error executing screen: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/validate")
async def validate_query(
    query: ScreenerQuery,
    screener_service: ScreenerService = Depends(get_screener_service)
):
    """
    Validate a screening query without executing it
    
    Returns validation errors and estimated execution complexity
    """
    try:
        query_builder = QueryBuilder()
        validation_errors = query_builder.validate_query(query)
        
        # Calculate complexity score
        complexity_score = len(query.groups) * 10 + sum(len(g.conditions) for g in query.groups)
        
        return ApiResponse(
            data={
                "valid": len(validation_errors) == 0,
                "errors": validation_errors,
                "complexity_score": complexity_score,
                "estimated_execution_time": complexity_score * 0.1  # Rough estimate
            }
        )
        
    except Exception as e:
        logger.error(f"Error validating query: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/save", response_model=SavedQuery)
async def save_query(
    request: SaveQueryRequest,
    db: AsyncSession = Depends(get_db),
    # user_id would come from authentication middleware
    current_user_id: Optional[str] = None  
):
    """
    Save a screening query for future use
    
    Allows users to build a library of proven screening strategies
    """
    try:
        # TODO: Implement actual save logic with user authentication
        # For now, return a mock response
        
        from uuid import uuid4
        from datetime import datetime
        
        saved_query = SavedQuery(
            id=uuid4(),
            name=request.name,
            description=request.description,
            query=request.query,
            createdAt=datetime.now(),
            updatedAt=datetime.now(),
            isPublic=request.isPublic,
            tags=request.tags
        )
        
        # TODO: Save to database
        
        logger.info(f"Query saved: {request.name}")
        return saved_query
        
    except Exception as e:
        logger.error(f"Error saving query: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/saved", response_model=List[SavedQuery])
async def get_saved_queries(
    user_id: Optional[str] = None,  # From auth
    include_public: bool = Query(True, description="Include public queries"),
    tags: Optional[List[str]] = Query(None, description="Filter by tags"),
    limit: int = Query(50, ge=1, le=100, description="Number of queries to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get saved screening queries
    
    Returns user's private queries and optionally public community queries
    """
    try:
        # TODO: Implement actual database query
        # For now, return empty list
        
        logger.info(f"Retrieved saved queries for user: {user_id}")
        return []
        
    except Exception as e:
        logger.error(f"Error retrieving saved queries: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/saved/{query_id}", response_model=SavedQuery)
async def get_saved_query(
    query_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific saved query by ID"""
    try:
        # TODO: Implement actual database lookup
        raise HTTPException(status_code=404, detail="Query not found")
        
    except Exception as e:
        logger.error(f"Error retrieving query {query_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.put("/saved/{query_id}", response_model=SavedQuery)
async def update_saved_query(
    query_id: UUID,
    request: SaveQueryRequest,
    db: AsyncSession = Depends(get_db)
):
    """Update a saved screening query"""
    try:
        # TODO: Implement actual update logic
        raise HTTPException(status_code=404, detail="Query not found")
        
    except Exception as e:
        logger.error(f"Error updating query {query_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/saved/{query_id}")
async def delete_saved_query(
    query_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Delete a saved screening query"""
    try:
        # TODO: Implement actual deletion
        
        return ApiResponse(
            message="Query deleted successfully"
        )
        
    except Exception as e:
        logger.error(f"Error deleting query {query_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/export", response_model=ExportResponse)
async def export_results(
    query: ScreenerQuery,
    export_request: ExportRequest,
    screener_service: ScreenerService = Depends(get_screener_service),
    db: AsyncSession = Depends(get_db)
):
    """
    Export screening results to various formats
    
    Supports CSV, Excel, and JSON exports with optional metadata
    """
    try:
        # Execute the screening query
        response = await screener_service.execute_screen(query, db)
        
        # TODO: Implement actual export logic
        # For now, return mock response
        
        from datetime import datetime, timedelta
        
        export_response = ExportResponse(
            downloadUrl=f"/api/v1/exports/download/mock-export-id",
            filename=f"screener_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{export_request.format}",
            expiresAt=datetime.now() + timedelta(hours=24)
        )
        
        logger.info(f"Export created: {export_response.filename}")
        return export_response
        
    except Exception as e:
        logger.error(f"Error creating export: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")