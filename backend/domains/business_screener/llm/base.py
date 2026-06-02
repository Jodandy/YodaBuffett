"""
Base LLM Interface

Abstract base class that all LLM providers must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum


class LLMTier(Enum):
    """LLM tier classification."""
    TIER_B = "B"  # Local LLM (Ollama, etc.)
    TIER_C = "C"  # API LLM (Claude, etc.)


@dataclass
class LLMResponse:
    """
    Standardized response from any LLM provider.
    """
    # Core response
    raw_text: str
    parsed_json: Optional[Dict[str, Any]] = None

    # Metadata
    model: str = ""
    tier: LLMTier = LLMTier.TIER_B
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    # Timing
    latency_ms: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)

    # Status
    success: bool = True
    error: Optional[str] = None
    parse_error: Optional[str] = None

    @property
    def has_valid_json(self) -> bool:
        """Check if response has valid parsed JSON."""
        return self.parsed_json is not None and self.parse_error is None

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from parsed JSON."""
        if self.parsed_json:
            return self.parsed_json.get(key, default)
        return default


@dataclass
class LLMConfig:
    """
    Configuration for LLM providers.
    """
    # Model settings
    model: str = "mistral:7b"
    temperature: float = 0.1  # Low for structured output
    max_tokens: int = 4096

    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: float = 1.0

    # Timeout
    timeout_seconds: float = 120.0

    # JSON mode
    require_json: bool = True

    # Provider-specific
    base_url: Optional[str] = None
    api_key: Optional[str] = None


class BaseLLM(ABC):
    """
    Abstract base class for LLM providers.

    All providers (Ollama, Claude, etc.) must implement this interface.
    """

    def __init__(self, config: LLMConfig = None):
        self.config = config or LLMConfig()
        self._tier: LLMTier = LLMTier.TIER_B

    @property
    def tier(self) -> LLMTier:
        """Get the tier of this LLM provider."""
        return self._tier

    @property
    def model(self) -> str:
        """Get the model name."""
        return self.config.model

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None
    ) -> LLMResponse:
        """
        Generate a response from the LLM.

        Args:
            prompt: The user prompt (typically from get_tier_b_prompt())
            system: Optional system prompt for context
            context: Optional document context (annual report text, etc.)

        Returns:
            LLMResponse with raw text and metadata
        """
        pass

    @abstractmethod
    async def generate_json(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> LLMResponse:
        """
        Generate a JSON response from the LLM.

        Args:
            prompt: The user prompt (should ask for JSON output)
            system: Optional system prompt
            context: Optional document context
            schema: Optional JSON schema for validation

        Returns:
            LLMResponse with parsed_json populated
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Check if the LLM provider is available and responsive.

        Returns:
            True if healthy, False otherwise
        """
        pass

    def _build_full_prompt(
        self,
        prompt: str,
        context: Optional[str] = None
    ) -> str:
        """
        Build the full prompt with optional context.

        Args:
            prompt: The analysis prompt
            context: Document context (annual report sections, etc.)

        Returns:
            Combined prompt string
        """
        if context:
            return f"""Here is the relevant document content for your analysis:

<document>
{context}
</document>

Based on the document above, please complete the following analysis:

{prompt}"""
        return prompt

    def _log(self, message: str):
        """Log a message (override for custom logging)."""
        print(f"[{self.__class__.__name__}] {message}")
