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
from .scorer import WEIGHT_PROFILES, DEFAULT_WEIGHT_PROFILE

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
    score_momentum: Optional[float] = None  # Score change from prior period
    prior_score: Optional[float] = None  # Previous fat pitch score
    prior_score_date: Optional[str] = None  # Date of prior score

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


class HistoricalScorePoint(BaseModel):
    """A single historical score data point."""
    score_date: str
    score: float
    dimension_code: Optional[str] = None


class HistoricalScoresResponse(BaseModel):
    """Historical scores for a company."""
    company_id: str
    symbol: str
    company_name: str
    fat_pitch_scores: List[HistoricalScorePoint]
    dimension_scores: Dict[str, List[HistoricalScorePoint]]  # dimension_code -> scores over time


class AnomalyPoint(BaseModel):
    """A single anomaly data point."""
    date: str
    anomaly_score: float  # 0-100, higher = more anomalous
    section_type: Optional[str] = None
    similarity_to_prior: Optional[float] = None
    year: Optional[int] = None


class AnomalyResponse(BaseModel):
    """Temporal anomaly data for a company."""
    company_id: str
    symbol: str
    company_name: str
    anomalies: List[AnomalyPoint]
    avg_anomaly_score: float
    max_anomaly_score: float
    anomaly_count: int


class WeightProfileResponse(BaseModel):
    """Weight profile information."""
    name: str
    description: str
    weights: Dict[str, float]
    is_default: bool


class WeightProfileListResponse(BaseModel):
    """List of available weight profiles."""
    profiles: List[WeightProfileResponse]
    default_profile: str


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


def _pitch_to_response(
    pitch: FatPitchModel,
    score_momentum: Optional[float] = None,
    prior_score: Optional[float] = None,
    prior_score_date: Optional[str] = None
) -> FatPitchResponse:
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
        score_momentum=score_momentum,
        prior_score=prior_score,
        prior_score_date=prior_score_date,
    )


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/pitches", response_model=List[FatPitchResponse])
async def get_all_pitches(
    min_quality_score: float = Query(0.0, ge=0, le=100, description="Minimum quality score filter"),
    limit: int = Query(100, ge=1, le=2000, description="Maximum pitches to return"),
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD), defaults to today"),
    weight_profile: Optional[str] = Query(None, description="Weight profile (optimal, garp, buffett, quality, value, equal)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get all fat pitches across all stages, ranked by score.

    Returns companies with highest fat_pitch_score combining quality and cheapness.
    Now includes score_momentum showing change from prior period.

    Weight profiles (backtested on Nordic markets 2021-2024):
    - optimal: Best predictor from backtesting (growth + quality focus)
    - garp: Growth at Reasonable Price (Peter Lynch style)
    - buffett: Quality compounder (Buffett style)
    - quality: Pure quality focus
    - value: Deep value focus
    - equal: Equal weights baseline
    """
    try:
        # Validate weight profile if provided
        if weight_profile and weight_profile not in WEIGHT_PROFILES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid weight profile '{weight_profile}'. Valid options: {list(WEIGHT_PROFILES.keys())}"
            )

        parsed_date = date.fromisoformat(score_date) if score_date else None
        target_date = parsed_date or date.today()

        pitches = await service.get_all_pitches(
            score_date=parsed_date,
            min_quality_score=min_quality_score,
            limit=limit,
            weight_profile=weight_profile
        )

        # Calculate score momentum for each company
        # Get weight profile for calculating prior scores
        profile_name = weight_profile or DEFAULT_WEIGHT_PROFILE
        weights = WEIGHT_PROFILES.get(profile_name, WEIGHT_PROFILES[DEFAULT_WEIGHT_PROFILE])

        # Build weight case statements for SQL (both score*weight and weight tracking)
        weight_cases = []
        weight_sum_cases = []
        for dim, weight in weights.items():
            if weight > 0:
                weight_cases.append(f"WHEN dimension_code = '{dim}' THEN score * {weight}")
                weight_sum_cases.append(f"WHEN dimension_code = '{dim}' THEN {weight}")
        weight_sql = " ".join(weight_cases)
        weight_sum_sql = " ".join(weight_sum_cases)

        # Get prior scores for all companies in one query
        company_ids = [p.company_id for p in pitches]
        if company_ids:
            momentum_query = f"""
            WITH company_scores AS (
                SELECT
                    dds.company_id::text as company_id,
                    dds.score_date,
                    SUM(CASE {weight_sql} ELSE 0 END) / NULLIF(SUM(CASE {weight_sum_sql} ELSE 0 END), 0) as fat_pitch_score
                FROM daily_dimension_scores dds
                WHERE dds.company_id = ANY($1::uuid[])
                  AND dds.score_date <= $2
                GROUP BY dds.company_id, dds.score_date
                HAVING COUNT(DISTINCT dimension_code) >= 5
            ),
            ranked_scores AS (
                SELECT
                    company_id, score_date, fat_pitch_score,
                    ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY score_date DESC) as rn
                FROM company_scores
            )
            SELECT
                c.company_id,
                c.score_date as current_date,
                c.fat_pitch_score as current_score,
                p.score_date as prior_date,
                p.fat_pitch_score as prior_score,
                c.fat_pitch_score - COALESCE(p.fat_pitch_score, c.fat_pitch_score) as score_change
            FROM ranked_scores c
            LEFT JOIN ranked_scores p ON c.company_id = p.company_id AND p.rn = 2
            WHERE c.rn = 1
            """

            momentum_rows = await service.db_conn.fetch(momentum_query, company_ids, target_date)

            # Build lookup dict
            momentum_lookup = {}
            for row in momentum_rows:
                momentum_lookup[row['company_id']] = {
                    'score_momentum': float(row['score_change']) if row['score_change'] is not None else None,
                    'prior_score': float(row['prior_score']) if row['prior_score'] is not None else None,
                    'prior_score_date': str(row['prior_date']) if row['prior_date'] else None,
                }
        else:
            momentum_lookup = {}

        # Build responses with momentum data
        results = []
        for p in pitches:
            momentum_data = momentum_lookup.get(p.company_id, {})
            results.append(_pitch_to_response(
                p,
                score_momentum=momentum_data.get('score_momentum'),
                prior_score=momentum_data.get('prior_score'),
                prior_score_date=momentum_data.get('prior_score_date'),
            ))

        return results
    except HTTPException:
        raise
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


@router.get("/pitches/momentum", response_model=List[FatPitchResponse])
async def get_momentum_pitches(
    min_score_change: float = Query(5.0, description="Minimum score increase from prior period"),
    limit: int = Query(50, ge=1, le=500, description="Maximum pitches to return"),
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    weight_profile: Optional[str] = Query(None, description="Weight profile (garp, optimal, etc.)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get pitches with significant score momentum (improving companies).

    Score momentum = current fat pitch score - prior period score.
    Backtesting shows companies with big score increases (+5 to +15 points)
    outperform by 3-4% over the following 3 months.

    This endpoint finds companies whose fundamentals are IMPROVING,
    regardless of absolute score level.
    """
    from datetime import timedelta

    try:
        # Validate weight profile
        if weight_profile and weight_profile not in WEIGHT_PROFILES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid weight profile '{weight_profile}'. Valid options: {list(WEIGHT_PROFILES.keys())}"
            )

        parsed_date = date.fromisoformat(score_date) if score_date else None
        target_date = parsed_date or date.today()

        # Get weight profile
        profile_name = weight_profile or DEFAULT_WEIGHT_PROFILE
        weights = WEIGHT_PROFILES.get(profile_name, WEIGHT_PROFILES[DEFAULT_WEIGHT_PROFILE])

        # Build weight case statements for SQL (both score*weight and weight tracking for normalization)
        weight_cases = []
        weight_sum_cases = []
        for dim, weight in weights.items():
            if weight > 0:
                weight_cases.append(f"WHEN dimension_code = '{dim}' THEN score * {weight}")
                weight_sum_cases.append(f"WHEN dimension_code = '{dim}' THEN {weight}")
        weight_sql = " ".join(weight_cases)
        weight_sum_sql = " ".join(weight_sum_cases)

        # Find the two most recent score dates for each company
        query = f"""
        WITH company_scores AS (
            SELECT
                dds.company_id,
                cm.primary_ticker as symbol,
                cm.company_name,
                dds.score_date,
                SUM(CASE {weight_sql} ELSE 0 END) / NULLIF(SUM(CASE {weight_sum_sql} ELSE 0 END), 0) as fat_pitch_score
            FROM daily_dimension_scores dds
            JOIN company_master cm ON cm.id = dds.company_id
            WHERE cm.primary_ticker IS NOT NULL
              AND dds.score_date <= $1
            GROUP BY dds.company_id, cm.primary_ticker, cm.company_name, dds.score_date
            HAVING COUNT(DISTINCT dimension_code) >= 5
        ),
        ranked_scores AS (
            SELECT
                company_id, symbol, company_name, score_date, fat_pitch_score,
                ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY score_date DESC) as rn
            FROM company_scores
        ),
        current_and_prior AS (
            SELECT
                c.company_id, c.symbol, c.company_name,
                c.score_date as current_date,
                c.fat_pitch_score as current_score,
                p.score_date as prior_date,
                p.fat_pitch_score as prior_score,
                c.fat_pitch_score - p.fat_pitch_score as score_change
            FROM ranked_scores c
            LEFT JOIN ranked_scores p ON c.company_id = p.company_id AND p.rn = 2
            WHERE c.rn = 1 AND p.fat_pitch_score IS NOT NULL
        )
        SELECT * FROM current_and_prior
        WHERE score_change >= $2
        ORDER BY score_change DESC
        LIMIT $3
        """

        rows = await service.db_conn.fetch(query, target_date, min_score_change, limit)

        # Get full pitch details for each company
        results = []
        for row in rows:
            pitch = await service.analyze_company(
                company_id=str(row['company_id']),
                score_date=target_date,
                weight_profile=profile_name
            )
            if pitch:
                results.append(_pitch_to_response(
                    pitch,
                    score_momentum=float(row['score_change']),
                    prior_score=float(row['prior_score']),
                    prior_score_date=str(row['prior_date'])
                ))

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting momentum pitches: {e}")
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


@router.get("/pitches/symbol/{symbol}", response_model=FatPitchResponse)
async def analyze_company_by_symbol(
    symbol: str,
    score_date: Optional[str] = Query(None, description="Score date (YYYY-MM-DD)"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Analyze a specific company by symbol as a fat pitch candidate.

    Looks up the company by primary_ticker or yahoo_symbol, then returns
    detailed scoring, stage assignment, and dimension breakdown.
    """
    try:
        # Look up company_id from symbol
        company_row = await service.db_conn.fetchrow("""
            SELECT id::text as company_id
            FROM company_master
            WHERE primary_ticker = $1 OR yahoo_symbol = $1
            LIMIT 1
        """, symbol)

        if not company_row:
            raise HTTPException(
                status_code=404,
                detail=f"Company with symbol '{symbol}' not found"
            )

        company_id = company_row['company_id']
        parsed_date = date.fromisoformat(score_date) if score_date else None

        pitch = await service.analyze_company(
            company_id=company_id,
            score_date=parsed_date
        )

        if not pitch:
            raise HTTPException(
                status_code=404,
                detail=f"Company '{symbol}' found but has insufficient data for scoring"
            )

        return _pitch_to_response(pitch)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error analyzing company by symbol: {e}")
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


# ============================================================================
# Weight Profile Endpoints
# ============================================================================

WEIGHT_PROFILE_DESCRIPTIONS = {
    'optimal': 'Best predictor from backtesting (growth + quality focus)',
    'garp': 'Growth at Reasonable Price (Peter Lynch style)',
    'buffett': 'Quality compounder (Buffett style)',
    'quality': 'Pure quality focus',
    'value': 'Deep value focus',
    'equal': 'Equal weights baseline',
}


@router.get("/weight-profiles", response_model=WeightProfileListResponse)
async def get_weight_profiles():
    """
    Get all available weight profiles.

    Weight profiles were backtested on Nordic markets 2021-2024.
    Ranked by predictive power (slope of rank vs 12M return):
    1. optimal (-0.0350) - BEST
    2. garp (-0.0349)
    3. quality (-0.0326)
    4. buffett (-0.0322)
    5. equal (-0.0311)
    6. value (-0.0201)
    """
    profiles = []
    for name, weights in WEIGHT_PROFILES.items():
        profiles.append(WeightProfileResponse(
            name=name,
            description=WEIGHT_PROFILE_DESCRIPTIONS.get(name, ''),
            weights=weights,
            is_default=(name == DEFAULT_WEIGHT_PROFILE)
        ))

    return WeightProfileListResponse(
        profiles=profiles,
        default_profile=DEFAULT_WEIGHT_PROFILE
    )


@router.get("/weight-profiles/{profile_name}", response_model=WeightProfileResponse)
async def get_weight_profile(profile_name: str):
    """
    Get a specific weight profile.

    Shows dimension weights used for scoring.
    """
    if profile_name not in WEIGHT_PROFILES:
        raise HTTPException(
            status_code=404,
            detail=f"Weight profile '{profile_name}' not found. Valid options: {list(WEIGHT_PROFILES.keys())}"
        )

    return WeightProfileResponse(
        name=profile_name,
        description=WEIGHT_PROFILE_DESCRIPTIONS.get(profile_name, ''),
        weights=WEIGHT_PROFILES[profile_name],
        is_default=(profile_name == DEFAULT_WEIGHT_PROFILE)
    )


# ============================================================================
# Historical Scores Endpoint
# ============================================================================

@router.get("/history/{symbol}", response_model=HistoricalScoresResponse)
async def get_historical_scores(
    symbol: str,
    weight_profile: Optional[str] = Query(None, description="Weight profile for fat pitch score calculation"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get historical dimension scores and calculated fat pitch scores for a company.

    Returns all available historical scores, useful for:
    - Seeing dimension trends (improving/declining)
    - Overlaying score on price chart
    - Understanding if score changes predict price movements
    """
    try:
        # Look up company
        company_row = await service.db_conn.fetchrow("""
            SELECT id::text as company_id, company_name, primary_ticker as symbol
            FROM company_master
            WHERE primary_ticker = $1 OR yahoo_symbol = $1
            LIMIT 1
        """, symbol)

        if not company_row:
            raise HTTPException(
                status_code=404,
                detail=f"Company with symbol '{symbol}' not found"
            )

        company_id = company_row['company_id']
        company_name = company_row['company_name']

        # Get all historical dimension scores
        rows = await service.db_conn.fetch("""
            SELECT score_date, dimension_code, score
            FROM daily_dimension_scores
            WHERE company_id = $1::uuid
            ORDER BY score_date ASC, dimension_code
        """, company_id)

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No historical scores found for {symbol}"
            )

        # Organize by date and dimension
        scores_by_date: Dict[str, Dict[str, float]] = {}
        for row in rows:
            date_str = str(row['score_date'])
            if date_str not in scores_by_date:
                scores_by_date[date_str] = {}
            scores_by_date[date_str][row['dimension_code']] = float(row['score'])

        # Get weight profile
        profile_name = weight_profile or DEFAULT_WEIGHT_PROFILE
        weights = WEIGHT_PROFILES.get(profile_name, WEIGHT_PROFILES[DEFAULT_WEIGHT_PROFILE])

        # Calculate fat pitch scores for each date
        fat_pitch_scores = []
        dimension_scores: Dict[str, List[HistoricalScorePoint]] = {}

        for date_str, dims in sorted(scores_by_date.items()):
            # Calculate weighted fat pitch score
            weighted_sum = 0.0
            total_weight = 0.0
            for dim, weight in weights.items():
                if weight > 0 and dim in dims:
                    weighted_sum += dims[dim] * weight
                    total_weight += weight

            if total_weight > 0:
                fat_pitch_score = weighted_sum / total_weight
                fat_pitch_scores.append(HistoricalScorePoint(
                    score_date=date_str,
                    score=round(fat_pitch_score, 1)
                ))

            # Collect dimension scores
            for dim, score in dims.items():
                if dim not in dimension_scores:
                    dimension_scores[dim] = []
                dimension_scores[dim].append(HistoricalScorePoint(
                    score_date=date_str,
                    score=round(score, 1),
                    dimension_code=dim
                ))

        return HistoricalScoresResponse(
            company_id=company_id,
            symbol=symbol,
            company_name=company_name,
            fat_pitch_scores=fat_pitch_scores,
            dimension_scores=dimension_scores
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting historical scores for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Temporal Anomaly Detection Endpoint
# ============================================================================

@router.get("/anomalies/{symbol}", response_model=AnomalyResponse)
async def get_temporal_anomalies(
    symbol: str,
    min_year: int = Query(2018, description="Minimum year for anomaly detection"),
    service: FatPitchService = Depends(get_fat_pitch_service)
):
    """
    Get temporal anomaly scores for a company.

    Compares each year's document embeddings to the prior year.
    Higher anomaly score = more different from historical pattern.

    Based on research showing:
    - High anomalies (score >= 40) correlate with -3.63% avg 60d return
    - Low anomalies (score < 40) correlate with +2.26% avg 60d return
    - Use as a risk indicator / veto signal
    """
    import numpy as np
    import json
    import ast

    try:
        # Look up company
        company_row = await service.db_conn.fetchrow("""
            SELECT id::text as company_id, company_name, primary_ticker as symbol
            FROM company_master
            WHERE primary_ticker = $1 OR yahoo_symbol = $1
            LIMIT 1
        """, symbol)

        if not company_row:
            raise HTTPException(
                status_code=404,
                detail=f"Company with symbol '{symbol}' not found"
            )

        company_id = company_row['company_id']
        company_name = company_row['company_name']

        # Get section embeddings by year
        rows = await service.db_conn.fetch("""
            SELECT
                ed.year,
                ed.company_name,
                se.section_type,
                se.embedding,
                ed.filing_date
            FROM section_embeddings se
            JOIN extracted_documents ed ON ed.id = se.extracted_document_id
            WHERE ed.company_name ILIKE $1
            AND ed.year >= $2
            AND se.section_type IN (
                'balance_sheet', 'income_statement', 'cash_flow',
                'risk_factors', 'management_discussion'
            )
            ORDER BY ed.year, se.section_type
        """, f"%{company_name}%", min_year - 1)

        if not rows:
            # Return empty response if no embeddings
            return AnomalyResponse(
                company_id=company_id,
                symbol=symbol,
                company_name=company_name,
                anomalies=[],
                avg_anomaly_score=0.0,
                max_anomaly_score=0.0,
                anomaly_count=0
            )

        # Group by year and section type
        by_year_section: Dict[tuple, List] = {}
        for row in rows:
            key = (row['year'], row['section_type'])
            if key not in by_year_section:
                by_year_section[key] = []

            # Parse embedding string to list
            emb_str = row['embedding']
            if isinstance(emb_str, str):
                try:
                    embedding = json.loads(emb_str)
                except json.JSONDecodeError:
                    try:
                        embedding = ast.literal_eval(emb_str)
                    except:
                        continue
            else:
                embedding = list(emb_str) if emb_str else None

            if embedding:
                by_year_section[key].append({
                    'embedding': embedding,
                    'filing_date': row['filing_date'],
                })

        anomalies = []
        years = sorted(set(k[0] for k in by_year_section.keys()))

        for year in years:
            if year < min_year:
                continue

            year_anomalies = []
            for section_type in ['balance_sheet', 'income_statement', 'cash_flow', 'risk_factors', 'management_discussion']:
                current_key = (year, section_type)
                prior_key = (year - 1, section_type)

                if current_key not in by_year_section or prior_key not in by_year_section:
                    continue

                current_data = by_year_section[current_key]
                prior_data = by_year_section[prior_key]

                if not current_data or not prior_data:
                    continue

                try:
                    # Average embedding for each year
                    current_avg = np.mean([d['embedding'] for d in current_data], axis=0)
                    prior_avg = np.mean([d['embedding'] for d in prior_data], axis=0)

                    # Calculate cosine similarity
                    norm_current = np.linalg.norm(current_avg)
                    norm_prior = np.linalg.norm(prior_avg)

                    if norm_current > 0 and norm_prior > 0:
                        similarity = np.dot(current_avg, prior_avg) / (norm_current * norm_prior)
                        # Anomaly score = 1 - similarity, scaled to 0-100
                        anomaly_score = (1 - similarity) * 100

                        # Get filing date
                        filing_date = current_data[0].get('filing_date')
                        if filing_date:
                            date_str = str(filing_date) if not hasattr(filing_date, 'date') else str(filing_date.date() if hasattr(filing_date, 'date') else filing_date)
                        else:
                            # Approximate: Q1 of next year
                            date_str = f"{year + 1}-03-01"

                        year_anomalies.append({
                            'date': date_str,
                            'anomaly_score': round(float(anomaly_score), 1),
                            'section_type': section_type,
                            'similarity_to_prior': round(float(similarity), 3),
                            'year': year
                        })
                except Exception as e:
                    logger.warning(f"Error computing anomaly for {company_name} {year} {section_type}: {e}")
                    continue

            # Average anomaly across sections for this year
            if year_anomalies:
                avg_year_score = np.mean([a['anomaly_score'] for a in year_anomalies])
                # Create a single aggregated anomaly point per year for chart overlay
                anomalies.append(AnomalyPoint(
                    date=year_anomalies[0]['date'],
                    anomaly_score=round(avg_year_score, 1),
                    section_type='aggregate',
                    similarity_to_prior=round(1 - avg_year_score/100, 3),
                    year=year
                ))

        # Calculate summary stats
        if anomalies:
            scores = [a.anomaly_score for a in anomalies]
            avg_score = float(np.mean(scores))
            max_score = float(max(scores))
        else:
            avg_score = 0.0
            max_score = 0.0

        return AnomalyResponse(
            company_id=company_id,
            symbol=symbol,
            company_name=company_name,
            anomalies=anomalies,
            avg_anomaly_score=round(avg_score, 1),
            max_anomaly_score=round(max_score, 1),
            anomaly_count=len(anomalies)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting anomalies for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
