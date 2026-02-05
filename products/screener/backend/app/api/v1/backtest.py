"""
Backtesting API endpoints - historical strategy validation
"""
import logging
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db, get_db_manager, DatabaseManager
from app.schemas.screener import (
    BacktestRequest, BacktestResponse, BacktestResult, BacktestSummary,
    ApiResponse
)
from app.services.backtest_service import BacktestService
from app.services.screener_service import ScreenerService
from app.services.metrics_service import MetricsService

logger = logging.getLogger(__name__)

router = APIRouter()


async def get_backtest_service(
    db_manager: DatabaseManager = Depends(get_db_manager)
) -> BacktestService:
    """Get backtest service dependency"""
    metrics_service = MetricsService(db_manager)
    screener_service = ScreenerService(db_manager, metrics_service)
    return BacktestService(db_manager, screener_service)


@router.post("/run", response_model=BacktestResponse)
async def run_backtest(
    request: BacktestRequest,
    backtest_service: BacktestService = Depends(get_backtest_service),
    db: AsyncSession = Depends(get_db)
):
    """
    Execute a comprehensive backtest of a screening strategy
    
    Features:
    - Point-in-time screening at multiple historical dates
    - Forward return calculation for each screening date
    - Performance analytics and risk metrics
    - Strategy comparison capabilities
    """
    try:
        # Validate backtest parameters
        validation_errors = backtest_service.validate_backtest_request(request)
        
        if validation_errors:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Backtest validation failed",
                    "errors": validation_errors
                }
            )
        
        # Execute backtest
        response = await backtest_service.execute_backtest(request, db)
        
        logger.info(
            f"Backtest completed: {len(response.results)} periods, "
            f"avg signals: {response.summary.totalSignals / len(response.results) if response.results else 0:.1f}"
        )
        return response
        
    except ValueError as e:
        logger.error(f"Validation error in run_backtest: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    
    except Exception as e:
        logger.error(f"Error executing backtest: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/quick")
async def quick_backtest(
    request: BacktestRequest,
    max_periods: int = Query(12, description="Maximum periods to test (for speed)"),
    backtest_service: BacktestService = Depends(get_backtest_service),
    db: AsyncSession = Depends(get_db)
):
    """
    Execute a quick backtest with limited periods for rapid feedback
    
    Useful for strategy development and iteration
    """
    try:
        # Limit the backtest for speed
        limited_request = request.copy(deep=True)
        
        # Calculate limited date range
        import pandas as pd
        from datetime import datetime
        
        start_date = datetime.fromisoformat(request.startDate).date()
        end_date = datetime.fromisoformat(request.endDate).date()
        
        # Create date range based on frequency
        if request.frequency == "monthly":
            date_range = pd.date_range(start_date, end_date, freq='MS')[:max_periods]
        elif request.frequency == "weekly":
            date_range = pd.date_range(start_date, end_date, freq='W')[:max_periods]
        else:  # daily
            date_range = pd.date_range(start_date, end_date, freq='D')[:max_periods]
        
        if len(date_range) > 0:
            limited_request.endDate = date_range[-1].strftime('%Y-%m-%d')
        
        # Execute limited backtest
        response = await backtest_service.execute_backtest(limited_request, db)
        
        # Add metadata about limitation
        response.summary.totalSignals = response.summary.totalSignals  # Keep original
        
        logger.info(f"Quick backtest completed: {len(response.results)} periods (limited)")
        return response
        
    except Exception as e:
        logger.error(f"Error executing quick backtest: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/saved", response_model=List[dict])
async def get_saved_backtests(
    user_id: Optional[str] = None,  # From auth
    query_id: Optional[UUID] = Query(None, description="Filter by specific query"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    """
    Get previously executed backtests
    
    Allows users to compare different strategies and track performance over time
    """
    try:
        # TODO: Implement actual database query
        # For now, return empty list
        
        logger.info(f"Retrieved saved backtests for user: {user_id}")
        return []
        
    except Exception as e:
        logger.error(f"Error retrieving saved backtests: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{backtest_id}", response_model=BacktestResponse)
async def get_backtest_result(
    backtest_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Get a specific backtest result by ID"""
    try:
        # TODO: Implement actual database lookup
        raise HTTPException(status_code=404, detail="Backtest not found")
        
    except Exception as e:
        logger.error(f"Error retrieving backtest {backtest_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/compare")
async def compare_strategies(
    strategy_1: BacktestRequest,
    strategy_2: BacktestRequest,
    backtest_service: BacktestService = Depends(get_backtest_service),
    db: AsyncSession = Depends(get_db)
):
    """
    Compare two screening strategies side-by-side
    
    Returns comparative performance metrics and statistical significance
    """
    try:
        # Ensure both strategies use same time period
        if strategy_1.startDate != strategy_2.startDate or strategy_1.endDate != strategy_2.endDate:
            raise HTTPException(
                status_code=400,
                detail="Both strategies must use the same time period for comparison"
            )
        
        # Execute both backtests
        result_1 = await backtest_service.execute_backtest(strategy_1, db)
        result_2 = await backtest_service.execute_backtest(strategy_2, db)
        
        # Calculate comparative metrics
        comparison = backtest_service.compare_backtests(result_1, result_2)
        
        return ApiResponse(
            data={
                "strategy_1": result_1,
                "strategy_2": result_2,
                "comparison": comparison
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing strategies: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/performance/benchmarks")
async def get_performance_benchmarks():
    """
    Get benchmark performance data for strategy comparison
    
    Returns market performance, sector averages, and risk-free rates
    """
    try:
        # TODO: Implement actual benchmark data retrieval
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
            },
            "volatility_measures": {
                "vix_equivalent": "Not available for Swedish market",
                "market_volatility": "Calculated from OMXS30"
            }
        }
        
        return ApiResponse(data=benchmarks)
        
    except Exception as e:
        logger.error(f"Error retrieving benchmarks: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/optimize")
async def optimize_strategy(
    base_query: BacktestRequest,
    optimization_params: dict,
    backtest_service: BacktestService = Depends(get_backtest_service),
    db: AsyncSession = Depends(get_db)
):
    """
    Optimize a screening strategy by testing parameter variations
    
    Tests different threshold values to find optimal parameters
    """
    try:
        # TODO: Implement strategy optimization
        # This would test multiple variations of parameters
        
        return ApiResponse(
            data={
                "message": "Strategy optimization not yet implemented",
                "suggested_approach": "Manually test different parameter values using the compare endpoint"
            }
        )
        
    except Exception as e:
        logger.error(f"Error optimizing strategy: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/analytics/periods")
async def get_period_analytics(
    backtest_id: UUID,
    period_type: str = Query("monthly", description="Grouping: monthly, quarterly, yearly"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get detailed analytics for specific time periods within a backtest
    
    Useful for understanding when strategies work best
    """
    try:
        # TODO: Implement period-specific analytics
        
        return ApiResponse(
            data={
                "message": "Period analytics not yet implemented"
            }
        )
        
    except Exception as e:
        logger.error(f"Error retrieving period analytics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")