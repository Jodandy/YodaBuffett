"""
LLM Infrastructure for Business Screener Deluxe

Provides Tier B (local LLM) and Tier C (Claude API) analysis capabilities.
"""

from .base import BaseLLM, LLMResponse, LLMConfig, LLMTier
from .local_provider import OllamaLLM, MockLocalLLM
from .claude_provider import ClaudeLLM, MockClaudeLLM
from .parser import ResponseParser, ValidationLevel, ValidationResult
from .service import LLMService, AnalysisResult, create_llm_service
from .document_context import DocumentContextFetcher, DocumentContext, DocumentSection

__all__ = [
    # Base
    'BaseLLM',
    'LLMResponse',
    'LLMConfig',
    'LLMTier',
    # Providers
    'OllamaLLM',
    'MockLocalLLM',
    'ClaudeLLM',
    'MockClaudeLLM',
    # Parser
    'ResponseParser',
    'ValidationLevel',
    'ValidationResult',
    # Service
    'LLMService',
    'AnalysisResult',
    'create_llm_service',
    # Document Context
    'DocumentContextFetcher',
    'DocumentContext',
    'DocumentSection',
]
