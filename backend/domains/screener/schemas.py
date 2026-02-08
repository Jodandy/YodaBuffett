"""
Pydantic schemas for screener API requests and responses
"""
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Company(BaseModel):
    """Company information"""
    id: str
    symbol: str
    name: str
    market: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    market_cap: Optional[float] = None
    currency: str = "SEK"


class MetricDefinition(BaseModel):
    """Available metric definition"""
    id: str
    name: str
    description: str
    category: str  # fundamental, technical, derived, market
    data_type: str  # number, percentage, ratio, currency
    unit: Optional[str] = None
    is_relative: bool = False
    source_type: str  # database, calculated


class QueryCondition(BaseModel):
    """Individual screening condition"""
    id: str
    left_operand: str = Field(..., alias="leftOperand")  # metric ID
    operator: str  # >, <, >=, <=, =, !=, between
    right_operand: Union[str, float, int, List[Union[str, float, int]]] = Field(..., alias="rightOperand")
    is_relative: bool = Field(False, alias="isRelative")  # true for metric vs metric

    @field_validator('operator')
    @classmethod
    def validate_operator(cls, v):
        valid_ops = ['>', '>=', '<', '<=', '=', '!=', 'between', 'in', 'not_in']
        if v not in valid_ops:
            raise ValueError(f'Operator must be one of: {valid_ops}')
        return v

    model_config = ConfigDict(populate_by_name=True)


class QueryGroup(BaseModel):
    """Group of conditions with logical operator"""
    id: str
    conditions: List[QueryCondition]
    logical_operator: str = Field("AND", alias="logicalOperator")

    @field_validator('logical_operator')
    @classmethod
    def validate_logical_operator(cls, v):
        if v not in ['AND', 'OR']:
            raise ValueError('Logical operator must be AND or OR')
        return v

    @field_validator('conditions')
    @classmethod
    def validate_conditions(cls, v):
        if not v:
            raise ValueError('Group must have at least one condition')
        if len(v) > 10:
            raise ValueError('Group cannot have more than 10 conditions')
        return v

    model_config = ConfigDict(populate_by_name=True)


class ScreenerQuery(BaseModel):
    """Complete screening query"""
    id: Optional[UUID] = None
    name: Optional[str] = None
    description: Optional[str] = None
    groups: List[QueryGroup]
    group_logic: str = Field("AND", alias="groupLogic")  # How to combine groups
    as_of_date: Optional[str] = Field(None, alias="asOfDate")  # ISO date string
    columns: List[str] = []  # Metric IDs to display
    include_forward_returns: Optional[List[str]] = Field(None, alias="includeForwardReturns")

    @field_validator('group_logic')
    @classmethod
    def validate_group_logic(cls, v):
        if v not in ['AND', 'OR']:
            raise ValueError('Group logic must be AND or OR')
        return v

    @field_validator('groups')
    @classmethod
    def validate_groups(cls, v):
        if not v:
            raise ValueError('Query must have at least one group')
        if len(v) > 5:
            raise ValueError('Query cannot have more than 5 groups')
        return v

    @field_validator('columns')
    @classmethod
    def validate_columns(cls, v):
        if len(v) > 20:
            raise ValueError('Cannot display more than 20 columns')
        return v

    @field_validator('include_forward_returns')
    @classmethod
    def validate_forward_returns(cls, v):
        if v is not None:
            valid_periods = ['1W', '1M', '3M', '6M', '1Y', '2Y']
            for period in v:
                if period not in valid_periods:
                    raise ValueError(f'Invalid forward return period: {period}')
        return v

    model_config = ConfigDict(populate_by_name=True)


class ScreenerResult(BaseModel):
    """Individual company result from screening"""
    company: Company
    values: Dict[str, Union[float, int, str, None]]
    forward_returns: Optional[Dict[str, float]] = Field(None, alias="forwardReturns")
    rank: Optional[int] = None


class ResultSummary(BaseModel):
    """Summary statistics for screening results"""
    count: int
    averages: Dict[str, float]
    medians: Dict[str, float]
    win_rates: Optional[Dict[str, float]] = Field(None, alias="winRates")
    sharpe_ratios: Optional[Dict[str, float]] = Field(None, alias="sharpeRatios")

    model_config = ConfigDict(populate_by_name=True)


class ScreenerResponse(BaseModel):
    """Complete response from screening operation"""
    query: ScreenerQuery
    results: List[ScreenerResult]
    summary: ResultSummary
    execution_time: float = Field(..., alias="executionTime")
    as_of_date: str = Field(..., alias="asOfDate")  # ISO date string
    total_matches: int = Field(..., alias="totalMatches")

    model_config = ConfigDict(populate_by_name=True)


# Backtest schemas
class BacktestRequest(BaseModel):
    """Request for backtesting a screening strategy"""
    query: ScreenerQuery
    start_date: str = Field(..., alias="startDate")  # ISO date string
    end_date: str = Field(..., alias="endDate")  # ISO date string
    frequency: str = "monthly"  # daily, weekly, monthly
    forward_periods: List[str] = Field(["1M", "3M", "1Y"], alias="forwardPeriods")

    @field_validator('frequency')
    @classmethod
    def validate_frequency(cls, v):
        if v not in ['daily', 'weekly', 'monthly']:
            raise ValueError('Frequency must be daily, weekly, or monthly')
        return v

    @field_validator('start_date', 'end_date')
    @classmethod
    def validate_date_format(cls, v):
        try:
            datetime.fromisoformat(v)
            return v
        except ValueError:
            raise ValueError('Date must be in ISO format (YYYY-MM-DD)')

    model_config = ConfigDict(populate_by_name=True)


class BacktestResult(BaseModel):
    """Results for one point in backtest"""
    date: str
    matches: int
    avg_return: Dict[str, float] = Field(..., alias="avgReturn")
    win_rate: Dict[str, float] = Field(..., alias="winRate")
    sharpe_ratio: Dict[str, float] = Field(..., alias="sharpeRatio")
    top_performers: List[ScreenerResult] = Field(..., alias="topPerformers")

    model_config = ConfigDict(populate_by_name=True)


class BacktestSummary(BaseModel):
    """Aggregate backtest performance summary"""
    total_signals: int = Field(..., alias="totalSignals")
    avg_returns: Dict[str, float] = Field(..., alias="avgReturns")
    win_rates: Dict[str, float] = Field(..., alias="winRates")
    sharpe_ratios: Dict[str, float] = Field(..., alias="sharpeRatios")
    best_month: Dict[str, Union[str, float]] = Field(..., alias="bestMonth")
    worst_month: Dict[str, Union[str, float]] = Field(..., alias="worstMonth")
    max_drawdown: float = Field(..., alias="maxDrawdown")

    model_config = ConfigDict(populate_by_name=True)


class BacktestResponse(BaseModel):
    """Complete backtest response"""
    query: ScreenerQuery
    results: List[BacktestResult]
    summary: BacktestSummary
    total_execution_time: float = Field(..., alias="totalExecutionTime")

    model_config = ConfigDict(populate_by_name=True)


# Saved query schemas
class SavedQuery(BaseModel):
    """Saved screening query"""
    id: UUID
    name: str
    description: Optional[str] = None
    query: ScreenerQuery
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")
    is_public: bool = Field(False, alias="isPublic")
    tags: List[str] = []

    model_config = ConfigDict(populate_by_name=True)


class SaveQueryRequest(BaseModel):
    """Request to save a screening query"""
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    query: ScreenerQuery
    is_public: bool = Field(False, alias="isPublic")
    tags: List[str] = Field([], max_length=10)

    @field_validator('tags')
    @classmethod
    def validate_tags(cls, v):
        for tag in v:
            if len(tag) > 50:
                raise ValueError('Tag length cannot exceed 50 characters')
        return v

    model_config = ConfigDict(populate_by_name=True)


# API Response wrappers
class ApiResponse(BaseModel):
    """Generic API response wrapper"""
    success: bool = True
    message: Optional[str] = None
    data: Optional[Any] = None


class ApiError(BaseModel):
    """API error response"""
    success: bool = False
    message: str
    errors: List[str] = []
    code: Optional[str] = None


# Export schemas
class ExportRequest(BaseModel):
    """Request for exporting screening results"""
    format: str = "csv"  # csv, xlsx, json
    include_metadata: bool = Field(True, alias="includeMetadata")

    @field_validator('format')
    @classmethod
    def validate_format(cls, v):
        if v not in ['csv', 'xlsx', 'json']:
            raise ValueError('Format must be csv, xlsx, or json')
        return v

    model_config = ConfigDict(populate_by_name=True)


class ExportResponse(BaseModel):
    """Response with export download information"""
    download_url: str = Field(..., alias="downloadUrl")
    filename: str
    expires_at: datetime = Field(..., alias="expiresAt")

    model_config = ConfigDict(populate_by_name=True)
