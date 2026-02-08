"""
Fat Pitch Domain

Implements the fat pitch pitching machine - a system that routes companies
through lifecycle stages and scores them as investment opportunities.

Stages:
- EARLY_STAGE: High growth, pre-profit (Revenue < $50M OR Growth > 40%)
- GROWTH_STAGE: Profitable growth (Profitable & Growth > 20%)
- MATURE_YIELD: Dividend focus (Dividend > 3% AND Growth < 15%)
- COMPOUNDER: Quality compounders (Profitable & Stable growth)
- ESTABLISHED: General quality assessment

Each stage has different scoring weights based on what matters most.
"""

from .models import BusinessStage, FatPitch, StageProfile, PitchRanking, CompanyFinancials
from .business_router import BusinessRouter
from .scorer import FatPitchScorer, STAGE_PROFILES
from .service import FatPitchService
from .router import router as fat_pitch_router

__all__ = [
    "BusinessStage",
    "FatPitch",
    "StageProfile",
    "PitchRanking",
    "CompanyFinancials",
    "BusinessRouter",
    "FatPitchScorer",
    "STAGE_PROFILES",
    "FatPitchService",
    "fat_pitch_router",
]
