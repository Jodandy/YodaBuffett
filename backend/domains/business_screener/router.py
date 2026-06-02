"""
Business Screener API Router

API endpoints for the Business Screener Deluxe - 15 investment screens.
"""

import logging
from datetime import date
from typing import List, Dict, Any, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg

from .models.screen_definition import SCREEN_DEFINITIONS, ScreenDefinition
from .models.screen_result import ScreenResult
from .repositories.screen_repository import ScreenRepository

logger = logging.getLogger(__name__)

router = APIRouter()

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


# ============================================================================
# Pydantic Schemas
# ============================================================================

class ScreenDefinitionResponse(BaseModel):
    """Screen definition response."""
    screen_type: int
    name: str
    short_name: str
    description: str
    tier_a_enabled: bool
    tier_b_enabled: bool
    tier_c_enabled: bool
    tiers: str
    run_frequency: str
    is_active: bool


class ScreenResultResponse(BaseModel):
    """Screen result response."""
    id: Optional[int] = None
    company_id: str
    company_name: Optional[str] = None
    primary_ticker: Optional[str] = None
    screen_type: int
    tier: str
    passed: bool = True
    score: float
    metrics: Dict[str, Any]
    flags: List[str]
    requires_tier_b: bool = False
    requires_tier_c: bool = False
    is_active: bool = True
    triggered_at: str
    expires_at: Optional[str] = None


class MultiHitResponse(BaseModel):
    """Multi-hit company response."""
    company_id: str
    company_name: str
    primary_ticker: str
    screens: List[Dict[str, Any]]
    screen_count: int
    avg_score: float
    best_screen: int


class DashboardResponse(BaseModel):
    """Dashboard summary response."""
    total_results: int
    active_results: int
    unique_companies: int
    screens_with_results: int
    last_updated: Optional[str] = None
    by_screen: List[Dict[str, Any]]


class TriggerRunResponse(BaseModel):
    """Response for triggering a screen run."""
    status: str
    message: str
    count: int = 0


# ============================================================================
# Dependency
# ============================================================================

async def get_db_connection():
    """Get database connection."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        await conn.close()


def _result_to_response(result: ScreenResult) -> ScreenResultResponse:
    """Convert ScreenResult to API response."""
    return ScreenResultResponse(
        id=result.id,
        company_id=str(result.company_id),
        company_name=result.company_name,
        primary_ticker=result.primary_ticker,
        screen_type=result.screen_type,
        tier=result.tier,
        passed=result.passed,
        score=result.score,
        metrics=result.metrics,
        flags=result.flags,
        requires_tier_b=result.requires_tier_b,
        requires_tier_c=result.requires_tier_c,
        is_active=result.is_active,
        triggered_at=result.triggered_at.isoformat() if result.triggered_at else '',
        expires_at=result.expires_at.isoformat() if result.expires_at else None,
    )


def _definition_to_response(definition: ScreenDefinition) -> ScreenDefinitionResponse:
    """Convert ScreenDefinition to API response."""
    return ScreenDefinitionResponse(
        screen_type=definition.screen_type,
        name=definition.name,
        short_name=definition.short_name,
        description=definition.description,
        tier_a_enabled=definition.tier_a_enabled,
        tier_b_enabled=definition.tier_b_enabled,
        tier_c_enabled=definition.tier_c_enabled,
        tiers=definition.tiers,
        run_frequency=definition.run_frequency,
        is_active=definition.is_active,
    )


# ============================================================================
# Screen Definition Endpoints
# ============================================================================

@router.get("/screens", response_model=List[ScreenDefinitionResponse])
async def get_all_screen_definitions(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get all screen definitions.

    Returns the 15 screen types with their metadata.
    """
    try:
        repo = ScreenRepository(conn)
        definitions = await repo.get_all_definitions()

        # If table is empty, return the predefined definitions
        if not definitions:
            return [_definition_to_response(d) for d in SCREEN_DEFINITIONS.values()]

        return [_definition_to_response(d) for d in definitions]
    except Exception as e:
        logger.error(f"Error getting screen definitions: {e}")
        # Fallback to predefined definitions
        return [_definition_to_response(d) for d in SCREEN_DEFINITIONS.values()]


@router.get("/screens/{screen_type}", response_model=ScreenDefinitionResponse)
async def get_screen_definition(
    screen_type: int,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get a specific screen definition.
    """
    if screen_type < 1 or screen_type > 20:
        raise HTTPException(status_code=400, detail="Screen type must be 1-20")

    try:
        repo = ScreenRepository(conn)
        definition = await repo.get_definition(screen_type)

        if not definition:
            # Fallback to predefined
            definition = SCREEN_DEFINITIONS.get(screen_type)

        if not definition:
            raise HTTPException(status_code=404, detail=f"Screen {screen_type} not found")

        return _definition_to_response(definition)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting screen definition: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Screen Results Endpoints
# ============================================================================

@router.get("/screens/{screen_type}/results", response_model=List[ScreenResultResponse])
async def get_screen_results(
    screen_type: int,
    active: bool = Query(True, description="Only show active results"),
    limit: int = Query(100, ge=1, le=500, description="Maximum results to return"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get results for a specific screen type.
    """
    if screen_type < 1 or screen_type > 20:
        raise HTTPException(status_code=400, detail="Screen type must be 1-20")

    try:
        repo = ScreenRepository(conn)
        results = await repo.get_results_by_screen(screen_type, active_only=active, limit=limit)
        return [_result_to_response(r) for r in results]
    except Exception as e:
        logger.error(f"Error getting screen results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/results", response_model=List[ScreenResultResponse])
async def get_all_results(
    active: bool = Query(True, description="Only show active results"),
    limit: int = Query(2000, ge=1, le=5000, description="Maximum results to return"),
    score_date: Optional[str] = Query(None, description="Point-in-time date (YYYY-MM-DD) for historical results"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get all screen results across all screens.

    Point-in-time support: Use score_date to get results as they were on a specific date.
    This returns results that were triggered on or before the given date.
    """
    try:
        repo = ScreenRepository(conn)

        if score_date:
            # Point-in-time query: get results valid as of the given date
            parsed_date = date.fromisoformat(score_date)
            results = await repo.get_results_as_of_date(parsed_date, limit)
        else:
            # Current results
            results = await repo.get_all_active_results(limit) if active else []

        return [_result_to_response(r) for r in results]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Error getting all results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/companies/{company_id}/screens", response_model=List[ScreenResultResponse])
async def get_company_screens(
    company_id: str,
    active: bool = Query(True, description="Only show active results"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get all screen results for a specific company.
    """
    try:
        repo = ScreenRepository(conn)
        results = await repo.get_results_by_company(UUID(company_id), active_only=active)
        return [_result_to_response(r) for r in results]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company_id format")
    except Exception as e:
        logger.error(f"Error getting company screens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Dashboard Endpoints
# ============================================================================

@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get dashboard summary statistics.
    """
    try:
        repo = ScreenRepository(conn)
        stats = await repo.get_stats()
        return DashboardResponse(
            total_results=stats['total_results'],
            active_results=stats['active_results'],
            unique_companies=stats['unique_companies'],
            screens_with_results=stats['screens_with_results'],
            last_updated=stats.get('last_run'),
            by_screen=stats['by_screen'],
        )
    except Exception as e:
        logger.error(f"Error getting dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/multi-hits", response_model=List[MultiHitResponse])
async def get_multi_hits(
    min_screens: int = Query(2, ge=2, le=15, description="Minimum screens triggered"),
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get companies triggering multiple screens.

    Multi-hits are potentially more interesting as they pass multiple
    independent filters simultaneously.
    """
    try:
        repo = ScreenRepository(conn)
        hits = await repo.get_multi_screen_hits(min_screens)

        results = []
        for hit in hits:
            # Build screens list
            screen_types = hit.get('screen_types', [])
            screens = []
            for st in screen_types:
                definition = SCREEN_DEFINITIONS.get(st)
                if definition:
                    screens.append({
                        'screenType': st,
                        'screenName': definition.short_name,
                        'score': 0,  # Would need additional query to get per-screen scores
                        'flags': [],
                    })

            results.append(MultiHitResponse(
                company_id=str(hit['company_id']),
                company_name=hit.get('company_name', 'Unknown'),
                primary_ticker=hit.get('primary_ticker', 'N/A'),
                screens=screens,
                screen_count=hit['screens_triggered'],
                avg_score=float(hit.get('avg_score', 0) or 0),
                best_screen=screen_types[0] if screen_types else 1,
            ))

        return results
    except Exception as e:
        logger.error(f"Error getting multi-hits: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/warnings", response_model=List[ScreenResultResponse])
async def get_warnings(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get all results with warning flags.
    """
    try:
        repo = ScreenRepository(conn)
        results = await repo.get_warnings()
        return [_result_to_response(r) for r in results]
    except Exception as e:
        logger.error(f"Error getting warnings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard/cyclical-warnings")
async def get_cyclical_warnings(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    CRITICAL: Get cyclicals appearing on value screens at peak earnings.

    This is the most dangerous oversight - a cyclical at peak earnings
    will show a low P/E that looks like value, but it's actually a trap.
    """
    try:
        repo = ScreenRepository(conn)
        warnings = await repo.get_cyclical_warnings()
        return {
            "count": len(warnings),
            "message": "Cyclicals at peak earnings on value screens - POTENTIAL VALUE TRAPS",
            "warnings": warnings
        }
    except Exception as e:
        logger.error(f"Error getting cyclical warnings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dashboard/flag-cyclicals")
async def flag_cyclicals_at_peak(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Add WARNING flags to value screen results for cyclicals at peak earnings.

    Call this after running value screens (3, 4, 5) to flag dangerous results.
    Requires bsd_company_classifications to be populated.
    """
    try:
        repo = ScreenRepository(conn)
        flagged_count = await repo.flag_cyclicals_at_peak()
        return {
            "status": "success",
            "flagged_count": flagged_count,
            "message": f"Flagged {flagged_count} cyclicals at peak on value screens"
        }
    except Exception as e:
        logger.error(f"Error flagging cyclicals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Screen Run Endpoints
# ============================================================================

@router.post("/screens/{screen_type}/run", response_model=TriggerRunResponse)
async def trigger_screen_run(
    screen_type: int,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Trigger a screen run.

    Note: For production use, this should be a background task.
    Currently returns immediately with a message to use CLI.
    """
    if screen_type < 1 or screen_type > 20:
        raise HTTPException(status_code=400, detail="Screen type must be 1-20")

    definition = SCREEN_DEFINITIONS.get(screen_type)
    if not definition:
        raise HTTPException(status_code=404, detail=f"Screen {screen_type} not found")

    # For now, return instruction to use CLI
    # In production, this could trigger a background job
    return TriggerRunResponse(
        status="pending",
        message=f"Use CLI to run screen: python -m domains.business_screener.cli run --screen {screen_type}",
        count=0,
    )


# ============================================================================
# Analysis Endpoints (Tier B/C)
# ============================================================================

@router.post("/analysis/tier-b/{company_id}", response_model=TriggerRunResponse)
async def trigger_tier_b_analysis(
    company_id: str,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Trigger Tier B (local LLM) analysis for a company.

    Not yet implemented - returns placeholder response.
    """
    return TriggerRunResponse(
        status="not_implemented",
        message="Tier B analysis not yet implemented. Use CLI for manual analysis.",
        count=0,
    )


@router.post("/analysis/tier-c/{company_id}", response_model=TriggerRunResponse)
async def trigger_tier_c_analysis(
    company_id: str,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Trigger Tier C (Claude API) deep analysis for a company.

    Not yet implemented - returns placeholder response.
    """
    return TriggerRunResponse(
        status="not_implemented",
        message="Tier C analysis not yet implemented. Use CLI for manual analysis.",
        count=0,
    )


@router.get("/analysis/{company_id}")
async def get_company_analysis(
    company_id: str,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get all LLM analysis results for a company.

    Returns empty list if no analysis has been run.
    """
    try:
        rows = await conn.fetch("""
            SELECT * FROM bsd_llm_analysis_results
            WHERE company_id = $1::uuid
            ORDER BY created_at DESC
        """, UUID(company_id))
        return [dict(r) for r in rows]
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company_id format")
    except Exception as e:
        logger.error(f"Error getting company analysis: {e}")
        return []


# ============================================================================
# Classifications Endpoints
# ============================================================================

@router.get("/classifications")
async def get_classifications(
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Get all company cyclical classifications.
    """
    try:
        rows = await conn.fetch("""
            SELECT
                cc.*,
                cm.company_name,
                cm.primary_ticker
            FROM bsd_company_classifications cc
            JOIN company_master cm ON cc.company_id = cm.id
            ORDER BY cm.company_name
        """)
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Error getting classifications: {e}")
        return []


@router.put("/classifications/{company_id}")
async def update_classification(
    company_id: str,
    classification: str,
    cycle_position: Optional[str] = None,
    cycle_driver: Optional[str] = None,
    mid_cycle_ebitda: Optional[float] = None,
    conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Update or create a company classification.

    Classifications: STABLE, CYCLICAL, GROWTH, TURNAROUND, EARLY_STAGE
    Cycle positions: TROUGH, EARLY_RECOVERY, MID_CYCLE, LATE_CYCLE, PEAK
    """
    valid_classifications = ['STABLE', 'CYCLICAL', 'GROWTH', 'TURNAROUND', 'EARLY_STAGE']
    if classification not in valid_classifications:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid classification. Must be one of: {valid_classifications}"
        )

    try:
        await conn.execute("""
            INSERT INTO bsd_company_classifications
                (company_id, classification, cycle_driver, cycle_position, mid_cycle_ebitda, classification_source)
            VALUES ($1::uuid, $2, $3, $4, $5, 'MANUAL')
            ON CONFLICT (company_id) DO UPDATE SET
                classification = $2,
                cycle_driver = COALESCE($3, bsd_company_classifications.cycle_driver),
                cycle_position = COALESCE($4, bsd_company_classifications.cycle_position),
                mid_cycle_ebitda = COALESCE($5, bsd_company_classifications.mid_cycle_ebitda),
                classification_source = 'MANUAL',
                classified_at = NOW()
        """, company_id, classification, cycle_driver, cycle_position, mid_cycle_ebitda)

        return {"status": "success", "company_id": company_id, "classification": classification}
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid company_id format")
    except Exception as e:
        logger.error(f"Error updating classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))
