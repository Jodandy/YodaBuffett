"""
Fat Pitch API Router

API endpoints for the fat pitch pitching machine.
Provides ranked lists of investment opportunities by stage.
"""

import logging
from datetime import date
from typing import Dict, List, Optional
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg

from .models import BusinessStage, FatPitch as FatPitchModel, PitchRanking as PitchRankingModel
from .service import FatPitchService

logger = logging.getLogger(__name__)

router = APIRouter()

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


# ============================================================================
# Pydantic Schemas
# ============================================================================

class StageEnum(str, Enum):
    """API-friendly stage enum."""
    early_stage = "early_stage"
    growth_stage = "growth_stage"
    mature_yield = "mature_yield"
    compounder = "compounder"
    established = "established"


class FatPitchResponse(BaseModel):
    """Single fat pitch response."""
    company_id: str
    symbol: str
    company_name: str
    stage: str
    stage_confidence: float
    quality_score: float
    cheapness_score: float
    fat_pitch_score: float
    quality_tier: int
    dimension_scores: Dict[str, float]
    dimension_contributions: Dict[str, float]
    flags: List[str]
    warnings: List[str]
    is_actionable: bool
    pitch_summary: str

    class Config:
        from_attributes = True


class PitchRankingResponse(BaseModel):
    """Stage ranking response."""
    stage: str
    score_date: str
    pitches: List[FatPitchResponse]
    total_companies: int
    companies_with_data: int
    actionable_pitches: int


class StageSummaryResponse(BaseModel):
    """Stage summary statistics."""
    total_companies: int
    companies_scored: int
    score_date: str
    stages: Dict[str, dict]


class StageProfileResponse(BaseModel):
    """Stage profile information."""
    stage: str
    display_name: str
    description: str
    dimension_weights: Dict[str, float]
    min_quality_score: float
    cheapness_weight: float
    tier_thresholds: Dict[int, float]


class DimensionDetailResponse(BaseModel):
    """Full dimension details including metadata."""
    dimension_code: str
    score: Optional[float]
    confidence: Optional[float]
    data_quality: Optional[float]
    score_low: Optional[float]
    score_high: Optional[float]
    metadata: Dict


# ============================================================================
# Dependency
# ============================================================================

async def get_fat_pitch_service():
    """Get fat pitch service with database connection."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield FatPitchService(conn)
    finally:
        await conn.close()


def _pitch_to_response(pitch: FatPitchModel) -> FatPitchResponse:
    """Convert FatPitch model to API response."""
    return FatPitchResponse(
        company_id=pitch.company_id,
        symbol=pitch.symbol,
        company_name=pitch.company_name,
        stage=pitch.stage.value,
        stage_confidence=pitch.stage_confidence,
        quality_score=pitch.quality_score,
        cheapness_score=pitch.cheapness_score,
        fat_pitch_score=pitch.fat_pitch_score,
        quality_tier=pitch.quality_tier,
        dimension_scores=pitch.dimension_scores,
        dimension_contributions=pitch.dimension_contributions,
        flags=pitch.flags,
        warnings=pitch.warnings,
        is_actionable=pitch.is_actionable,
        pitch_summary=pitch.pitch_summary,
    )


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/pitches", response_model=List[FatPitchResponse])
async def get_all_pitches(
    min_quality_score: float = Query(0.0, ge=0, le=100, description="Minimum quality score filter"),
    limit: int = Query(100, ge=1, le=500, description="Maximum pitches to return"),
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD), defaults to today"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get all fat pitches across all stages, ranked by score.

    Returns companies with highest fat_pitch_score combining quality and cheapness.
    """
    try:
        parsed_date = date.fromisoformat(score_date) if score_date else None
        pitches = await service.get_all_pitches(
            score_date=parsed_date,
            min_quality_score=min_quality_score,
            limit=limit
        )
        return [_pitch_to_response(p) for p in pitches]
    except Exception as e:
        logger.error(f"Error getting all pitches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pitches/actionable", response_model=List[FatPitchResponse])
async def get_actionable_pitches(
    limit: int = Query(20, ge=1, le=100, description="Maximum pitches to return"),
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get top actionable pitches worth investigating now.

    Actionable = Tier 1-3 quality + cheapness score >= 60.
    These are the fat pitches worth swinging at.
    """
    try:
        parsed_date = date.fromisoformat(score_date) if score_date else None
        pitches = await service.get_top_actionable(
            limit=limit,
            score_date=parsed_date
        )
        return [_pitch_to_response(p) for p in pitches]
    except Exception as e:
        logger.error(f"Error getting actionable pitches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pitches/stage/{stage}", response_model=PitchRankingResponse)
async def get_stage_pitches(
    stage: StageEnum,
    limit: int = Query(50, ge=1, le=200, description="Maximum pitches to return"),
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get ranked pitches for a specific business stage.

    Stages:
    - early_stage: High growth, pre-profit (Revenue < $50M OR Growth > 40%)
    - growth_stage: Profitable growth (Profitable & Growth > 20%)
    - mature_yield: Dividend focus (Dividend > 3% AND Growth < 15%)
    - compounder: Quality compounders (Profitable & Stable growth)
    - established: General quality assessment
    """
    try:
        business_stage = BusinessStage(stage.value)
        parsed_date = date.fromisoformat(score_date) if score_date else None

        ranking = await service.get_stage_pitches(
            stage=business_stage,
            score_date=parsed_date,
            limit=limit
        )

        return PitchRankingResponse(
            stage=ranking.stage.value,
            score_date=str(ranking.score_date),
            pitches=[_pitch_to_response(p) for p in ranking.pitches],
            total_companies=ranking.total_companies,
            companies_with_data=ranking.companies_with_data,
            actionable_pitches=ranking.actionable_pitches,
        )
    except Exception as e:
        logger.error(f"Error getting stage pitches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pitches/all-stages", response_model=Dict[str, PitchRankingResponse])
async def get_all_stage_rankings(
    pitches_per_stage: int = Query(20, ge=1, le=100, description="Pitches per stage"),
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get rankings for all stages at once.

    Returns a dict with each stage as a key.
    Useful for dashboard views showing all opportunities.
    """
    try:
        parsed_date = date.fromisoformat(score_date) if score_date else None

        rankings = await service.get_all_stage_rankings(
            score_date=parsed_date,
            pitches_per_stage=pitches_per_stage
        )

        return {
            stage.value: PitchRankingResponse(
                stage=ranking.stage.value,
                score_date=str(ranking.score_date),
                pitches=[_pitch_to_response(p) for p in ranking.pitches],
                total_companies=ranking.total_companies,
                companies_with_data=ranking.companies_with_data,
                actionable_pitches=ranking.actionable_pitches,
            )
            for stage, ranking in rankings.items()
        }
    except Exception as e:
        logger.error(f"Error getting all stage rankings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pitches/company/{company_id}", response_model=FatPitchResponse)
async def analyze_company(
    company_id: str,
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Analyze a specific company as a fat pitch candidate.

    Returns detailed scoring, stage assignment, and dimension breakdown.
    """
    try:
        parsed_date = date.fromisoformat(score_date) if score_date else None

        pitch = await service.analyze_company(
            company_id=company_id,
            score_date=parsed_date
        )

        if not pitch:
            raise HTTPException(
                status_code=404,
                detail=f"Company {company_id} not found or has insufficient data"
            )

        return _pitch_to_response(pitch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing company: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dimensions/{company_id}", response_model=List[DimensionDetailResponse])
async def get_dimension_details(
    company_id: str,
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get full dimension details including metadata for a company.

    Returns all dimension scores with their underlying metrics breakdown.
    Useful for understanding why a company has a specific dimension score.
    """
    try:
        parsed_date = date.fromisoformat(score_date) if score_date else None

        details = await service.get_dimension_details(
            company_id=company_id,
            score_date=parsed_date
        )

        if not details:
            raise HTTPException(
                status_code=404,
                detail=f"No dimension data found for company {company_id}"
            )

        return [DimensionDetailResponse(**d) for d in details]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dimension details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary", response_model=StageSummaryResponse)
async def get_stage_summary(
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get summary statistics for all stages.

    Returns counts, average scores, and actionable pitch counts per stage.
    """
    try:
        parsed_date = date.fromisoformat(score_date) if score_date else None
        summary = await service.get_stage_summary(score_date=parsed_date)
        return StageSummaryResponse(**summary)
    except Exception as e:
        logger.error(f"Error getting stage summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profiles/{stage}", response_model=StageProfileResponse)
async def get_stage_profile(
    stage: StageEnum,
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get scoring profile information for a stage.

    Shows which dimensions matter most and their weights.
    """
    try:
        business_stage = BusinessStage(stage.value)
        profile = await service.get_profile_info(business_stage)

        if not profile:
            raise HTTPException(
                status_code=404,
                detail=f"Profile not found for stage {stage.value}"
            )

        return StageProfileResponse(**profile)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stage profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/profiles", response_model=Dict[str, StageProfileResponse])
async def get_all_stage_profiles(
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get all stage profiles.

    Useful for understanding how each stage is scored differently.
    """
    try:
        profiles = {}
        for stage in BusinessStage:
            if stage == BusinessStage.UNKNOWN:
                continue
            profile = await service.get_profile_info(stage)
            if profile:
                profiles[stage.value] = StageProfileResponse(**profile)

        return profiles
    except Exception as e:
        logger.error(f"Error getting all profiles: {e}")
        raise HTTPException(status_code=500, detail=str(e))
