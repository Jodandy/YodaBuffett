"""
Base LLM provider interface
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class LLMResponse:
    """Response from an LLM provider"""
    content: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    model: str
    cost_usd: float = 0.0
    finish_reason: Optional[str] = None


class BaseLLMProvider(ABC):
    """
    Abstract base class for LLM providers.

    Supports: OpenAI, Anthropic, local models (Ollama)
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Name of this provider (e.g., 'openai', 'anthropic')"""
        pass

    @property
    @abstractmethod
    def default_model(self) -> str:
        """Default model to use if not specified"""
        pass

    @abstractmethod
    async def generate(
        self,
        system_message: str,
        user_message: str,
        model: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 4000,
    ) -> LLMResponse:
        """
        Generate a response from the LLM.

        Args:
            system_message: System/role instructions
            user_message: The actual prompt
            model: Model to use (None = default)
            temperature: Sampling temperature (0-1)
            max_tokens: Maximum tokens to generate

        Returns:
            LLMResponse with content and metadata
        """
        pass

    def calculate_cost(
        self,
        prompt_tokens: int,
        completion_tokens: int,
        model: str
    ) -> float:
        """
        Calculate cost in USD for this request.
        Override in subclasses with provider-specific pricing.
        """
        return 0.0  # Default: free (for local models)
