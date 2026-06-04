"""
NAV-Quality Evaluator

Two-layer system for evaluating asset-backed companies:
- Layer 1: Deterministic math (discount, soft assets, runway, dilution)
- Layer 2: LLM judgment (NAV reality, catalysts, value traps)
"""
from .models import (
    CompanyAssetInput,
    Layer1Result,
    Layer2Judgment,
    NAVEvaluationResult,
    NAVRealityStatus,
    CatalystStatus,
    JudgmentFlag
)
from .layer1_math import Layer1MathEngine
from .layer2_judgment import Layer2Evaluator
from .data_fetcher import NAVDataFetcher
from .document_fetcher import DocumentFetcher
from .service import NAVEvaluatorService

__all__ = [
    # Models
    'CompanyAssetInput',
    'Layer1Result',
    'Layer2Judgment',
    'NAVEvaluationResult',
    'NAVRealityStatus',
    'CatalystStatus',
    'JudgmentFlag',

    # Engines
    'Layer1MathEngine',
    'Layer2Evaluator',

    # Data fetchers
    'NAVDataFetcher',
    'DocumentFetcher',

    # Orchestrator service
    'NAVEvaluatorService',
]
