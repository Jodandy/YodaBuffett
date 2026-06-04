"""
AI Analyst Domain

Generates LLM-powered investment analysis using raw financial data.
Clean architecture with modular prompts and composable data sources.
"""

from .service import AIAnalystService
from .models import AnalysisRequest, AnalysisResult, PromptType

__all__ = [
    'AIAnalystService',
    'AnalysisRequest',
    'AnalysisResult',
    'PromptType',
]
