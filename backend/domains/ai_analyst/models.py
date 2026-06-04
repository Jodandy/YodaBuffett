"""
Core models for AI Analyst domain
"""
from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any, List
from enum import Enum


class PromptType(str, Enum):
    """Available analysis prompt types"""
    INVESTMENT_MEMO = "investment_memo"
    # Future: RISK_ANALYSIS = "risk_analysis"
    # Future: GROWTH_ANALYSIS = "growth_analysis"


class LLMProvider(str, Enum):
    """Supported LLM providers"""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    LOCAL = "local"


@dataclass
class AnalysisRequest:
    """Request to generate an AI analysis"""
    company_id: str
    prompt_type: PromptType
    analysis_date: Optional[date] = None  # None = use latest data

    # Data source options
    include_financials: bool = True
    include_prices: bool = True
    include_documents: bool = False
    years_back: int = 3  # How many years of historical data

    # LLM options
    llm_provider: LLMProvider = LLMProvider.OPENAI
    model: Optional[str] = None  # None = use provider default
    temperature: float = 0.3  # Lower = more conservative
    max_tokens: int = 4000


@dataclass
class AnalysisResult:
    """Result from AI analysis"""
    company_id: str
    company_name: str
    prompt_type: PromptType
    analysis_date: date

    # LLM response
    raw_response: str
    structured_data: Optional[Dict[str, Any]] = None  # If using structured output

    # Metadata
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float = 0.0
    processing_time_seconds: float = 0.0
    llm_provider: str = "unknown"
    model_used: str = "unknown"

    # Data included
    data_sources_used: List[str] = None

    def __post_init__(self):
        if self.data_sources_used is None:
            self.data_sources_used = []


@dataclass
class DataSourceResult:
    """Result from a data source query"""
    source_name: str
    data: Dict[str, Any]
    success: bool = True
    error: Optional[str] = None
    query_time_seconds: float = 0.0
