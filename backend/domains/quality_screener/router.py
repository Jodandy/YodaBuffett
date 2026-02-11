"""
Quality Screener Router

REST API endpoints for the quality screener.
Standalone - no Fat Pitch dependencies.
"""

from fastapi import APIRouter, Query, Depends
from typing import List, Optional
from datetime import date
from pydantic import BaseModel
import asyncpg

from .service import QualityScreenerService
from .models import ScreenerFilters

router = APIRouter(prefix="/quality-screener", tags=["Quality Screener"])

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


# === Pydantic Response Models ===

class QualityCandidateResponse(BaseModel):
    ticker: str
    company_name: str
    tier: int
    tier_description: str
    business_model: str
    business_model_reason: str
    size_category: str
    cash_quality: str
    is_profitable: bool = False
    quality_score: int
    market_cap: float
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    net_margin: Optional[float] = None
    roic: Optional[float] = None
    roe: Optional[float] = None
    revenue_cagr: Optional[float] = None
    ocf_to_ni: Optional[float] = None
    fcf_to_ni: Optional[float] = None
    fcf_yield: Optional[float] = None
    capex_to_revenue: Optional[float] = None
    capex_to_da: Optional[float] = None
    negative_working_capital: bool = False
    receivables_vs_revenue: Optional[float] = None
    gross_margin_trend: Optional[float] = None
    share_dilution: Optional[float] = None
    net_cash: bool = False
    net_debt_to_ebitda: Optional[float] = None
    pe_ratio: Optional[float] = None
    ev_to_ebitda: Optional[float] = None
    reasons: List[str] = []
    concerns: List[str] = []

    class Config:
        from_attributes = True


class ScreenerSummaryResponse(BaseModel):
    total_companies: int
    by_tier: dict
    by_business_model: dict
    by_size: dict
    by_cash_quality: dict
    profitable_count: int
    unprofitable_count: int


class CategoryOptionResponse(BaseModel):
    value: str
    label: str
    description: str


class CategoryOptionsResponse(BaseModel):
    tiers: List[dict]
    business_models: List[dict]
    size_categories: List[dict]
    cash_qualities: List[dict]


class ScreenerResponse(BaseModel):
    candidates: List[QualityCandidateResponse]
    summary: ScreenerSummaryResponse
    score_date: date
    filters_applied: dict


# === Dependency ===

async def get_db_connection():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


# === Endpoints ===

@router.get("/categories", response_model=CategoryOptionsResponse)
async def get_categories(conn: asyncpg.Connection = Depends(get_db_connection)):
    """Get all available filter categories."""
    service = QualityScreenerService(conn)
    options = service.get_category_options()
    return CategoryOptionsResponse(
        tiers=options.tiers,
        business_models=options.business_models,
        size_categories=options.size_categories,
        cash_qualities=options.cash_qualities,
    )


@router.get("/companies", response_model=ScreenerResponse)
async def get_companies(
    score_date: Optional[date] = Query(None, description="Date to evaluate (default: latest)"),
    tier: Optional[List[int]] = Query(None, description="Filter by tier(s): 1,2,3,4,5"),
    model: Optional[List[str]] = Query(None, description="Filter by business model(s)"),
    size: Optional[List[str]] = Query(None, description="Filter by size category(s)"),
    cash: Optional[List[str]] = Query(None, description="Filter by cash quality(s)"),
    profitable: bool = Query(False, description="Only profitable companies"),
    min_cap: Optional[float] = Query(None, description="Minimum market cap in millions"),
    max_cap: Optional[float] = Query(None, description="Maximum market cap in millions"),
    limit: Optional[int] = Query(None, description="Limit number of results"),
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """
    Get quality screened companies with filtering.

    Examples:
    - /companies?tier=1&tier=2 - Tier 1-2 only
    - /companies?model=Cash%20Cow - Cash Cows only
    - /companies?model=Cash%20Cow&tier=1&tier=2 - Tier 1-2 Cash Cows
    - /companies?size=Mid&size=Large&profitable=true - Mid/Large profitable
    - /companies?cash=Excellent&cash=Good - Good cash conversion
    - /companies?min_cap=100&max_cap=1000 - $100M-$1B market cap
    """
    service = QualityScreenerService(conn)

    # Build filters
    filters = ScreenerFilters(
        tiers=tier,
        business_models=model,
        size_categories=size,
        cash_qualities=cash,
        profitable_only=profitable,
        min_market_cap=(min_cap * 1e6) if min_cap else 0,
        max_market_cap=(max_cap * 1e6) if max_cap else float('inf'),
    )

    # Get candidates
    candidates = await service.get_candidates(score_date, filters)

    # Apply limit
    if limit and limit > 0:
        candidates = candidates[:limit]

    # Get summary
    summary = await service.get_summary(candidates)

    # Get actual score date used
    if score_date is None:
        score_date = await conn.fetchval("SELECT MAX(date) FROM daily_price_data")

    return ScreenerResponse(
        candidates=[QualityCandidateResponse(
            ticker=c.ticker,
            company_name=c.company_name,
            tier=c.tier,
            tier_description=c.tier_description,
            business_model=c.business_model,
            business_model_reason=c.business_model_reason,
            size_category=c.size_category,
            cash_quality=c.cash_quality,
            is_profitable=bool(c.is_profitable),
            quality_score=c.quality_score,
            market_cap=c.market_cap,
            gross_margin=c.gross_margin,
            operating_margin=c.operating_margin,
            net_margin=c.net_margin,
            roic=c.roic,
            roe=c.roe,
            revenue_cagr=c.revenue_cagr,
            ocf_to_ni=c.ocf_to_ni,
            fcf_to_ni=c.fcf_to_ni,
            fcf_yield=c.fcf_yield,
            capex_to_revenue=c.capex_to_revenue,
            capex_to_da=c.capex_to_da,
            negative_working_capital=bool(c.negative_working_capital),
            receivables_vs_revenue=c.receivables_vs_revenue,
            gross_margin_trend=c.gross_margin_trend,
            share_dilution=c.share_dilution,
            net_cash=bool(c.net_cash),
            net_debt_to_ebitda=c.net_debt_to_ebitda,
            pe_ratio=c.pe_ratio,
            ev_to_ebitda=c.ev_to_ebitda,
            reasons=c.reasons or [],
            concerns=c.concerns or [],
        ) for c in candidates],
        summary=ScreenerSummaryResponse(
            total_companies=summary.total_companies,
            by_tier=summary.by_tier,
            by_business_model=summary.by_business_model,
            by_size=summary.by_size,
            by_cash_quality=summary.by_cash_quality,
            profitable_count=summary.profitable_count,
            unprofitable_count=summary.unprofitable_count,
        ),
        score_date=score_date,
        filters_applied={
            "tiers": tier,
            "business_models": model,
            "size_categories": size,
            "cash_qualities": cash,
            "profitable_only": profitable,
            "min_market_cap": min_cap,
            "max_market_cap": max_cap,
        },
    )


@router.get("/companies/{ticker}", response_model=QualityCandidateResponse)
async def get_company(
    ticker: str,
    score_date: Optional[date] = Query(None, description="Date to evaluate"),
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Get quality analysis for a specific company."""
    service = QualityScreenerService(conn)

    # Get all candidates and find the one we want
    candidates = await service.get_candidates(score_date)

    for c in candidates:
        if c.ticker.upper() == ticker.upper():
            return QualityCandidateResponse(
                ticker=c.ticker,
                company_name=c.company_name,
                tier=c.tier,
                tier_description=c.tier_description,
                business_model=c.business_model,
                business_model_reason=c.business_model_reason,
                size_category=c.size_category,
                cash_quality=c.cash_quality,
                is_profitable=bool(c.is_profitable),
                quality_score=c.quality_score,
                market_cap=c.market_cap,
                gross_margin=c.gross_margin,
                operating_margin=c.operating_margin,
                net_margin=c.net_margin,
                roic=c.roic,
                roe=c.roe,
                revenue_cagr=c.revenue_cagr,
                ocf_to_ni=c.ocf_to_ni,
                fcf_to_ni=c.fcf_to_ni,
                fcf_yield=c.fcf_yield,
                capex_to_revenue=c.capex_to_revenue,
                capex_to_da=c.capex_to_da,
                negative_working_capital=bool(c.negative_working_capital),
                receivables_vs_revenue=c.receivables_vs_revenue,
                gross_margin_trend=c.gross_margin_trend,
                share_dilution=c.share_dilution,
                net_cash=bool(c.net_cash),
                net_debt_to_ebitda=c.net_debt_to_ebitda,
                pe_ratio=c.pe_ratio,
                ev_to_ebitda=c.ev_to_ebitda,
                reasons=c.reasons or [],
                concerns=c.concerns or [],
            )

    from fastapi import HTTPException
    raise HTTPException(status_code=404, detail=f"Company {ticker} not found")


@router.get("/summary", response_model=ScreenerSummaryResponse)
async def get_summary(
    score_date: Optional[date] = Query(None, description="Date to evaluate"),
    conn: asyncpg.Connection = Depends(get_db_connection),
):
    """Get summary statistics for all companies (no filters)."""
    service = QualityScreenerService(conn)
    candidates = await service.get_candidates(score_date)
    summary = await service.get_summary(candidates)

    return ScreenerSummaryResponse(
        total_companies=summary.total_companies,
        by_tier=summary.by_tier,
        by_business_model=summary.by_business_model,
        by_size=summary.by_size,
        by_cash_quality=summary.by_cash_quality,
        profitable_count=summary.profitable_count,
        unprofitable_count=summary.unprofitable_count,
    )
