"""
Local LLM provider using Ollama

Completely free, runs on your Mac, no API keys needed.
"""
import httpx
from typing import Optional
from .base import BaseLLMProvider, LLMResponse


class LocalLLMProvider(BaseLLMProvider):
    """
    Local LLM provider using Ollama.

    Installation:
    1. Install Ollama: https://ollama.com/download
    2. Run: ollama serve
    3. Download model: ollama pull llama3.1:8b

    Free, private, unlimited usage!
    """

    def __init__(self, base_url: str = "http://localhost:11434"):
        """
        Initialize local LLM provider.

        Args:
            base_url: Ollama server URL (default: localhost:11434)
        """
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=120.0)  # Longer timeout for local inference

    @property
    def provider_name(self) -> str:
        return "local_ollama"

    @property
    def default_model(self) -> str:
        return "llama3.1:8b"  # Good balance of quality and speed

    async def generate(
        self,
        system_message: str,
        user_message: str,
        model: Optional[str] = None,
        temperature: float = 0.3,
        max_tokens: int = 4000,
    ) -> LLMResponse:
        """Generate response using local Ollama"""

        model = model or self.default_model

        # Combine system + user message (Ollama format)
        full_prompt = f"{system_message}\n\n{user_message}"

        # Call Ollama API
        response = await self.client.post(
            f"{self.base_url}/api/generate",
            json={
                "model": model,
                "prompt": full_prompt,
                "temperature": temperature,
                "stream": False,
                "options": {
                    "num_predict": max_tokens,
                }
            }
        )

        if response.status_code != 200:
            raise Exception(f"Ollama error: {response.text}")

        result = response.json()

        # Ollama doesn't return exact token counts, estimate them
        prompt_tokens = len(full_prompt.split()) * 1.3  # Rough estimate
        completion_tokens = len(result['response'].split()) * 1.3

        return LLMResponse(
            content=result['response'],
            prompt_tokens=int(prompt_tokens),
            completion_tokens=int(completion_tokens),
            total_tokens=int(prompt_tokens + completion_tokens),
            model=model,
            cost_usd=0.0,  # FREE!
            finish_reason="stop",
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
