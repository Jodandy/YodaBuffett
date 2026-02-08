"""
Screener API endpoints - metrics, screening, and backtesting
"""
import logging
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import ValidationError

from .database import get_db_manager, DatabaseManager
from .schemas import (
    ScreenerQuery, ScreenerResponse, SavedQuery, SaveQueryRequest,
    BacktestRequest, BacktestResponse, MetricDefinition, ApiResponse
)
from .services.screener_service import ScreenerService
from .services.metrics_service import MetricsService
from .services.backtest_service import BacktestService
from .services.query_builder import QueryBuilder

logger = logging.getLogger(__name__)

router = APIRouter()


# ===== Dependency Injection =====

async def get_metrics_service(
    db_manager: DatabaseManager = Depends(get_db_manager)
) -> MetricsService:
    """Get metrics service dependency"""
    return MetricsService(db_manager)


async def get_screener_service(
    db_manager: DatabaseManager = Depends(get_db_manager)
) -> ScreenerService:
    """Get screener service dependency"""
    metrics_service = MetricsService(db_manager)
    return ScreenerService(db_manager, metrics_service)


async def get_backtest_service(
    db_manager: DatabaseManager = Depends(get_db_manager)
) -> BacktestService:
    """Get backtest service dependency"""
    metrics_service = MetricsService(db_manager)
    screener_service = ScreenerService(db_manager, metrics_service)
    return BacktestService(db_manager, screener_service)


# ===== Metrics Endpoints =====

@router.get("/metrics/available", response_model=List[MetricDefinition])
async def get_available_metrics(
    category: Optional[str] = Query(None, description="Filter by category"),
    data_type: Optional[str] = Query(None, description="Filter by data type"),
    relative_only: bool = Query(False, description="Only return relative metrics"),
    metrics_service: MetricsService = Depends(get_metrics_service)
):
    """Get list of available screening metrics"""
    try:
        metrics = await metrics_service.get_available_metrics(
            category=category,
            data_type=data_type,
            relative_only=relative_only
        )
        return metrics
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics/categories", response_model=List[str])
async def get_metric_categories(
    metrics_service: MetricsService = Depends(get_metrics_service)
):
    """Get list of available metric categories"""
    try:
        return await metrics_service.get_metric_categories()
    except Exception as e:
        logger.error(f"Error retrieving categories: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics/{metric_id}", response_model=MetricDefinition)
async def get_metric_details(
    metric_id: str,
    metrics_service: MetricsService = Depends(get_metrics_service)
):
    """Get detailed information about a specific metric"""
    try:
        metric = await metrics_service.get_metric_definition(metric_id)
        if not metric:
            raise HTTPException(status_code=404, detail=f"Metric {metric_id} not found")
        return metric
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving metric {metric_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== Screener Endpoints =====

@router.post("/screener/run", response_model=ScreenerResponse)
async def run_screen(
    query: ScreenerQuery,
    screener_service: ScreenerService = Depends(get_screener_service)
):
    """Execute a screening query with optional point-in-time data"""
    try:
        query_builder = QueryBuilder()
        validation_errors = query_builder.validate_query(query)

        if validation_errors:
            raise HTTPException(
                status_code=400,
                detail={"message": "Query validation failed", "errors": validation_errors}
            )

        response = await screener_service.execute_screen(query)
        logger.info(f"Screen executed: {response.total_matches} matches")
        return response

    except ValidationError as e:
        logger.error(f"Validation error in run_screen: {e}")
        raise HTTPException(
            status_code=400,
            detail={"message": "Query validation failed", "errors": [str(err) for err in e.errors()]}
        )
    except ValueError as e:
        logger.error(f"Business logic error in run_screen: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise  # Re-raise HTTP exceptions as-is
    except Exception as e:
        logger.error(f"Error executing screen: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/screener/validate")
async def validate_query(
    query: ScreenerQuery,
    screener_service: ScreenerService = Depends(get_screener_service)
):
    """Validate a screening query without executing it"""
    try:
        query_builder = QueryBuilder()
        validation_errors = query_builder.validate_query(query)

        complexity_score = len(query.groups) * 10 + sum(len(g.conditions) for g in query.groups)

        return ApiResponse(
            data={
                "valid": len(validation_errors) == 0,
                "errors": validation_errors,
                "complexity_score": complexity_score,
                "estimated_execution_time": complexity_score * 0.1
            }
        )
    except Exception as e:
        logger.error(f"Error validating query: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/screener/save", response_model=SavedQuery)
async def save_query(request: SaveQueryRequest):
    """Save a screening query for future use"""
    try:
        saved_query = SavedQuery(
            id=uuid4(),
            name=request.name,
            description=request.description,
            query=request.query,
            createdAt=datetime.now(),
            updatedAt=datetime.now(),
            isPublic=request.is_public,
            tags=request.tags
        )
        logger.info(f"Query saved: {request.name}")
        return saved_query
    except Exception as e:
        logger.error(f"Error saving query: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/screener/saved", response_model=List[SavedQuery])
async def get_saved_queries(
    include_public: bool = Query(True, description="Include public queries"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """Get saved screening queries"""
    try:
        # TODO: Implement actual database query
        return []
    except Exception as e:
        logger.error(f"Error retrieving saved queries: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ===== Backtest Endpoints =====

@router.post("/backtest/run", response_model=BacktestResponse)
async def run_backtest(
    request: BacktestRequest,
    backtest_service: BacktestService = Depends(get_backtest_service)
):
    """Execute a comprehensive backtest of a screening strategy"""
    try:
        validation_errors = backtest_service.validate_backtest_request(request)

        if validation_errors:
            raise HTTPException(
                status_code=400,
                detail={"message": "Backtest validation failed", "errors": validation_errors}
            )

        response = await backtest_service.execute_backtest(request)

        logger.info(
            f"Backtest completed: {len(response.results)} periods, "
            f"avg signals: {response.summary.total_signals / len(response.results) if response.results else 0:.1f}"
        )
        return response

    except ValueError as e:
        logger.error(f"Validation error in run_backtest: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error executing backtest: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/backtest/quick")
async def quick_backtest(
    request: BacktestRequest,
    max_periods: int = Query(12, description="Maximum periods to test"),
    backtest_service: BacktestService = Depends(get_backtest_service)
):
    """Execute a quick backtest with limited periods for rapid feedback"""
    try:
        import pandas as pd
        from datetime import datetime

        limited_request = request.model_copy(deep=True)

        start_date = datetime.fromisoformat(request.start_date).date()
        end_date = datetime.fromisoformat(request.end_date).date()

        if request.frequency == "monthly":
            date_range = pd.date_range(start_date, end_date, freq='MS')[:max_periods]
        elif request.frequency == "weekly":
            date_range = pd.date_range(start_date, end_date, freq='W')[:max_periods]
        else:
            date_range = pd.date_range(start_date, end_date, freq='D')[:max_periods]

        if len(date_range) > 0:
            limited_request.end_date = date_range[-1].strftime('%Y-%m-%d')

        response = await backtest_service.execute_backtest(limited_request)

        logger.info(f"Quick backtest completed: {len(response.results)} periods")
        return response

    except Exception as e:
        logger.error(f"Error executing quick backtest: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/backtest/saved", response_model=List[dict])
async def get_saved_backtests(
    query_id: Optional[UUID] = Query(None, description="Filter by specific query"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0)
):
    """Get previously executed backtests"""
    try:
        # TODO: Implement actual database query
        return []
    except Exception as e:
        logger.error(f"Error retrieving saved backtests: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/backtest/benchmarks")
async def get_performance_benchmarks():
    """Get benchmark performance data for strategy comparison"""
    try:
        benchmarks = {
            "market_indices": {
                "OMX_Stockholm_30": {
                    "symbol": "OMXS30",
                    "description": "Swedish large-cap index",
                    "available_periods": ["1M", "3M", "1Y", "2Y"]
                },
                "OMX_Stockholm_All": {
                    "symbol": "OMXSPI",
                    "description": "All Swedish stocks",
                    "available_periods": ["1M", "3M", "1Y", "2Y"]
                }
            },
            "risk_free_rate": {
                "description": "Swedish Government Bond 10Y",
                "current_rate": 2.1,
                "historical_available": True
            }
        }
        return ApiResponse(data=benchmarks)
    except Exception as e:
        logger.error(f"Error retrieving benchmarks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
