"""
Metrics API endpoints - available metrics and definitions
"""
import logging
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db, get_db_manager, DatabaseManager
from app.schemas.screener import MetricDefinition, ApiResponse
from app.services.metrics_service import MetricsService

logger = logging.getLogger(__name__)

router = APIRouter()


async def get_metrics_service(
    db_manager: DatabaseManager = Depends(get_db_manager)
) -> MetricsService:
    """Get metrics service dependency"""
    return MetricsService(db_manager)


@router.get("/available", response_model=List[MetricDefinition])
async def get_available_metrics(
    category: Optional[str] = Query(None, description="Filter by category: fundamental, technical, derived, market"),
    data_type: Optional[str] = Query(None, description="Filter by data type: number, percentage, ratio, currency"),
    relative_only: bool = Query(False, description="Only return metrics that support relative comparisons"),
    metrics_service: MetricsService = Depends(get_metrics_service)
):
    """
    Get all available metrics for screening
    
    Returns comprehensive list of fundamental, technical, and derived metrics
    with metadata about types and usage
    """
    try:
        metrics = await metrics_service.get_available_metrics(
            category=category,
            data_type=data_type,
            relative_only=relative_only
        )
        
        logger.info(f"Retrieved {len(metrics)} available metrics")
        return metrics
        
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/categories")
async def get_metric_categories(
    metrics_service: MetricsService = Depends(get_metrics_service)
):
    """
    Get available metric categories and their descriptions
    
    Useful for organizing UI and understanding metric types
    """
    try:
        categories = await metrics_service.get_metric_categories()
        
        return ApiResponse(
            data={
                "categories": categories,
                "descriptions": {
                    "fundamental": "Company financials and ratios (P/E, ROE, Debt/Equity)",
                    "technical": "Price and volume-based indicators (RSI, Moving Averages)",
                    "market": "Market-related metrics (Market Cap, Price, Volume)",
                    "derived": "Calculated metrics (Price changes, Distance from highs/lows)"
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Error retrieving categories: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/operators")
async def get_available_operators():
    """
    Get available comparison operators for screening conditions
    
    Returns operators with descriptions and compatible data types
    """
    try:
        operators = {
            ">": {
                "description": "Greater than",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "P/E > 15"
            },
            ">=": {
                "description": "Greater than or equal to",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "ROE >= 15"
            },
            "<": {
                "description": "Less than",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "P/E < 20"
            },
            "<=": {
                "description": "Less than or equal to",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "Debt/Equity <= 0.5"
            },
            "=": {
                "description": "Equal to",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "Sector = 'Technology'"
            },
            "!=": {
                "description": "Not equal to",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "Market Cap != 0"
            },
            "between": {
                "description": "Between two values (inclusive)",
                "compatible_types": ["number", "percentage", "ratio", "currency"],
                "example": "P/E between 10 and 20"
            }
        }
        
        return ApiResponse(data=operators)
        
    except Exception as e:
        logger.error(f"Error retrieving operators: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/{metric_id}", response_model=MetricDefinition)
async def get_metric_definition(
    metric_id: str,
    metrics_service: MetricsService = Depends(get_metrics_service)
):
    """
    Get detailed definition for a specific metric
    
    Includes calculation method, data source, and usage examples
    """
    try:
        metric = await metrics_service.get_metric_definition(metric_id)
        
        if not metric:
            raise HTTPException(status_code=404, detail=f"Metric '{metric_id}' not found")
        
        return metric
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving metric {metric_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/examples/queries")
async def get_example_queries():
    """
    Get example screening queries for common strategies
    
    Provides templates for popular screening approaches
    """
    try:
        examples = {
            "value_stocks": {
                "name": "Classic Value Stocks",
                "description": "Low P/E, P/B stocks with profitable operations",
                "query": {
                    "groups": [
                        {
                            "id": "value_criteria",
                            "conditions": [
                                {
                                    "id": "low_pe",
                                    "leftOperand": "pe_ratio",
                                    "operator": "<",
                                    "rightOperand": 15,
                                    "isRelative": False
                                },
                                {
                                    "id": "low_pb",
                                    "leftOperand": "pb_ratio", 
                                    "operator": "<",
                                    "rightOperand": 2,
                                    "isRelative": False
                                },
                                {
                                    "id": "profitable",
                                    "leftOperand": "roe",
                                    "operator": ">",
                                    "rightOperand": 10,
                                    "isRelative": False
                                }
                            ],
                            "logicalOperator": "AND"
                        }
                    ],
                    "groupLogic": "AND",
                    "columns": ["pe_ratio", "pb_ratio", "roe", "market_cap"]
                }
            },
            "growth_stocks": {
                "name": "High Growth Stocks",
                "description": "Fast growing companies with strong momentum",
                "query": {
                    "groups": [
                        {
                            "id": "growth_criteria",
                            "conditions": [
                                {
                                    "id": "revenue_growth",
                                    "leftOperand": "revenue_growth_yoy",
                                    "operator": ">",
                                    "rightOperand": 20,
                                    "isRelative": False
                                },
                                {
                                    "id": "strong_rsi",
                                    "leftOperand": "rsi_14",
                                    "operator": ">",
                                    "rightOperand": 50,
                                    "isRelative": False
                                }
                            ],
                            "logicalOperator": "AND"
                        }
                    ],
                    "groupLogic": "AND",
                    "columns": ["revenue_growth_yoy", "rsi_14", "market_cap", "pe_ratio"]
                }
            },
            "relative_value": {
                "name": "Relative Value Opportunities", 
                "description": "Stocks trading below their historical averages",
                "query": {
                    "groups": [
                        {
                            "id": "relative_criteria",
                            "conditions": [
                                {
                                    "id": "pe_vs_historical",
                                    "leftOperand": "pe_ratio",
                                    "operator": "<",
                                    "rightOperand": "historical_avg_pe",
                                    "isRelative": True
                                }
                            ],
                            "logicalOperator": "AND"
                        }
                    ],
                    "groupLogic": "AND", 
                    "columns": ["pe_ratio", "pb_ratio", "market_cap"]
                }
            }
        }
        
        return ApiResponse(data=examples)
        
    except Exception as e:
        logger.error(f"Error retrieving example queries: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/validation/rules")
async def get_validation_rules():
    """
    Get validation rules for building queries
    
    Returns limits and constraints for query complexity
    """
    try:
        rules = {
            "limits": {
                "max_groups": 5,
                "max_conditions_per_group": 10,
                "max_columns": 20,
                "max_forward_return_periods": 6
            },
            "supported_periods": ["1W", "1M", "3M", "6M", "1Y", "2Y"],
            "supported_frequencies": ["daily", "weekly", "monthly"],
            "data_availability": {
                "earliest_date": "2021-01-01",
                "latest_date": "present",
                "update_frequency": "daily"
            },
            "performance_guidelines": {
                "recommended_max_conditions": 15,
                "typical_execution_time": "2-5 seconds",
                "complex_query_threshold": 20
            }
        }
        
        return ApiResponse(data=rules)
        
    except Exception as e:
        logger.error(f"Error retrieving validation rules: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")