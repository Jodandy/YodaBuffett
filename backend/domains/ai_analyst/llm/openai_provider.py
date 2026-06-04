"""
OpenAI LLM provider implementation
"""
import os
from typing import Optional
from openai import AsyncOpenAI
from .base import BaseLLMProvider, LLMResponse


class OpenAIProvider(BaseLLMProvider):
    """
    OpenAI API provider.

    Supports: GPT-4o, GPT-4o-mini, GPT-4-turbo, etc.
    """

    def __init__(self, api_key: Optional[str] = None, organization: Optional[str] = None, project: Optional[str] = None):
        """
        Initialize OpenAI provider.

        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY env var)
            organization: Optional organization ID (for multi-org accounts)
            project: Optional project ID (for project-specific access)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key required (set OPENAI_API_KEY env var)")

        # Optional: organization/project for multi-tenant setups
        self.organization = organization or os.getenv("OPENAI_ORG_ID")
        self.project = project or os.getenv("OPENAI_PROJECT_ID")

        # Initialize client with optional org/project
        client_kwargs = {"api_key": self.api_key}
        if self.organization:
            client_kwargs["organization"] = self.organization
        if self.project:
            client_kwargs["project"] = self.project

        self.client = AsyncOpenAI(**client_kwargs)

    @property
    def provider_name(self) -> str:
        return "openai"

    @property
    def default_model(self) -> str:
        return "gpt-4o-mini"  # Cost-effective default

    async def generate(
        self,
        system_message: str,
        user_message: str,
        model: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 4000,
    ) -> LLMResponse:
        """Generate response using OpenAI API"""

        model = model or self.default_model

        response = await self.client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )

        choice = response.choices[0]
        usage = response.usage

        cost = self.calculate_cost(
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            model=model
        )

        return LLMResponse(
            content=choice.message.content,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
            model=model,
            cost_usd=cost,
            finish_reason=choice.finish_reason,
        )

    def calculate_cost(
        self,
        prompt_tokens: int,
        completion_tokens: int,
        model: str
    ) -> float:
        """
        Calculate cost based on OpenAI pricing (as of 2024).

        Prices per 1M tokens:
        - gpt-4o: $2.50 input / $10.00 output
        - gpt-4o-mini: $0.150 input / $0.600 output
        - gpt-4-turbo: $10.00 input / $30.00 output
        """

        pricing = {
            "gpt-4o": {"input": 2.50, "output": 10.00},
            "gpt-4o-mini": {"input": 0.150, "output": 0.600},
            "gpt-4-turbo": {"input": 10.00, "output": 30.00},
            "gpt-4-turbo-preview": {"input": 10.00, "output": 30.00},
            "gpt-4": {"input": 30.00, "output": 60.00},
            "gpt-3.5-turbo": {"input": 0.50, "output": 1.50},
        }

        # Find matching pricing (handle model versions like gpt-4o-2024-08-06)
        prices = None
        for key in pricing.keys():
            if model.startswith(key):
                prices = pricing[key]
                break

        if not prices:
            # Default to gpt-4o-mini pricing if unknown
            prices = pricing["gpt-4o-mini"]

        # Calculate cost (prices are per 1M tokens)
        input_cost = (prompt_tokens / 1_000_000) * prices["input"]
        output_cost = (completion_tokens / 1_000_000) * prices["output"]

        return input_cost + output_cost
