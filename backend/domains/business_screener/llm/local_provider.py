"""
Local LLM Provider (Ollama)

Tier B analysis using locally-running LLMs via Ollama.
Supports Mistral, Llama, and other models.
"""

import asyncio
import json
import time
from typing import Optional, Dict, Any

import httpx

from .base import BaseLLM, LLMConfig, LLMResponse, LLMTier


class OllamaLLM(BaseLLM):
    """
    Ollama-based local LLM provider for Tier B analysis.

    Requires Ollama to be running locally:
        brew install ollama
        ollama serve
        ollama pull mistral:7b

    Default model: mistral:7b (good balance of speed and quality)
    Alternative: llama3:8b, phi3:medium, mixtral:8x7b
    """

    DEFAULT_BASE_URL = "http://localhost:11434"
    DEFAULT_MODEL = "llama3.1:8b"  # Also good: mistral:7b, llama3:latest

    def __init__(self, config: LLMConfig = None):
        if config is None:
            config = LLMConfig(
                model=self.DEFAULT_MODEL,
                base_url=self.DEFAULT_BASE_URL,
                temperature=0.1,
                max_tokens=4096,
                timeout_seconds=120.0
            )
        super().__init__(config)
        self._tier = LLMTier.TIER_B
        self.base_url = config.base_url or self.DEFAULT_BASE_URL

    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None
    ) -> LLMResponse:
        """
        Generate a response using Ollama.

        Args:
            prompt: The analysis prompt
            system: System prompt for financial analysis context
            context: Document text to analyze

        Returns:
            LLMResponse with raw text
        """
        full_prompt = self._build_full_prompt(prompt, context)

        # Default system prompt for financial analysis
        if system is None:
            system = self._get_default_system_prompt()

        start_time = time.time()

        try:
            async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.config.model,
                        "prompt": full_prompt,
                        "system": system,
                        "stream": False,
                        "options": {
                            "temperature": self.config.temperature,
                            "num_predict": self.config.max_tokens,
                        }
                    }
                )
                response.raise_for_status()
                data = response.json()

                latency_ms = (time.time() - start_time) * 1000

                return LLMResponse(
                    raw_text=data.get("response", ""),
                    model=self.config.model,
                    tier=self._tier,
                    prompt_tokens=data.get("prompt_eval_count", 0),
                    completion_tokens=data.get("eval_count", 0),
                    total_tokens=data.get("prompt_eval_count", 0) + data.get("eval_count", 0),
                    latency_ms=latency_ms,
                    success=True
                )

        except httpx.TimeoutException:
            return LLMResponse(
                raw_text="",
                model=self.config.model,
                tier=self._tier,
                latency_ms=(time.time() - start_time) * 1000,
                success=False,
                error="Request timed out"
            )
        except httpx.HTTPStatusError as e:
            return LLMResponse(
                raw_text="",
                model=self.config.model,
                tier=self._tier,
                latency_ms=(time.time() - start_time) * 1000,
                success=False,
                error=f"HTTP error: {e.response.status_code}"
            )
        except Exception as e:
            return LLMResponse(
                raw_text="",
                model=self.config.model,
                tier=self._tier,
                latency_ms=(time.time() - start_time) * 1000,
                success=False,
                error=str(e)
            )

    async def generate_json(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> LLMResponse:
        """
        Generate a JSON response using Ollama.

        Adds explicit JSON formatting instructions and parses the response.

        Args:
            prompt: The analysis prompt (should already ask for JSON)
            system: System prompt
            context: Document text to analyze
            schema: Optional JSON schema for validation

        Returns:
            LLMResponse with parsed_json populated
        """
        # Enhance prompt for JSON output
        json_prompt = self._enhance_prompt_for_json(prompt)

        # Get the raw response
        response = await self.generate(json_prompt, system, context)

        if not response.success:
            return response

        # Parse JSON from response
        try:
            parsed = self._extract_json(response.raw_text)
            response.parsed_json = parsed
        except json.JSONDecodeError as e:
            response.parse_error = f"JSON parse error: {str(e)}"
        except Exception as e:
            response.parse_error = f"Parse error: {str(e)}"

        return response

    async def health_check(self) -> bool:
        """
        Check if Ollama is running and the model is available.

        Returns:
            True if healthy, False otherwise
        """
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Check Ollama is running
                response = await client.get(f"{self.base_url}/api/tags")
                response.raise_for_status()

                # Check model is available
                data = response.json()
                models = [m.get("name", "") for m in data.get("models", [])]

                # Check for exact match or partial match (e.g., "mistral:7b" in "mistral:7b-instruct")
                model_available = any(
                    self.config.model in m or m.startswith(self.config.model.split(":")[0])
                    for m in models
                )

                if not model_available:
                    self._log(f"Model {self.config.model} not found. Available: {models}")
                    return False

                return True

        except Exception as e:
            self._log(f"Health check failed: {e}")
            return False

    def _get_default_system_prompt(self) -> str:
        """Get the default system prompt for financial analysis."""
        return """You are an expert financial analyst specializing in deep-dive company analysis.
You analyze annual reports, financial statements, and company filings with precision.

Key traits:
- You provide structured, factual analysis based on the documents provided
- You identify red flags and positive signals objectively
- You always respond in valid JSON format when requested
- You distinguish between facts from documents and your inferences
- You are conservative in your assessments - when uncertain, you say so

When analyzing companies, focus on:
- Quality of earnings and cash flow
- Balance sheet strength and risks
- Competitive position and moat durability
- Management quality signals
- Red flags or concerns"""

    def _enhance_prompt_for_json(self, prompt: str) -> str:
        """Add explicit JSON formatting instructions."""
        return f"""{prompt}

IMPORTANT: Your response must be ONLY valid JSON. Do not include any text before or after the JSON object.
Do not use markdown code blocks. Start directly with {{ and end with }}."""

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """
        Extract JSON from LLM response text.

        Handles common issues like:
        - Markdown code blocks
        - Leading/trailing text
        - Multiple JSON objects

        Args:
            text: Raw LLM response

        Returns:
            Parsed JSON dictionary

        Raises:
            json.JSONDecodeError if no valid JSON found
        """
        text = text.strip()

        # Try direct parse first
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Remove markdown code blocks
        if "```json" in text:
            start = text.find("```json") + 7
            end = text.find("```", start)
            if end > start:
                text = text[start:end].strip()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass

        if "```" in text:
            start = text.find("```") + 3
            end = text.find("```", start)
            if end > start:
                text = text[start:end].strip()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass

        # Find JSON object boundaries
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            json_str = text[start:end]
            return json.loads(json_str)

        raise json.JSONDecodeError("No valid JSON found", text, 0)


class MockLocalLLM(BaseLLM):
    """
    Mock local LLM for testing without Ollama running.

    Returns placeholder responses that match expected JSON schemas.
    """

    def __init__(self, config: LLMConfig = None):
        super().__init__(config or LLMConfig(model="mock-local"))
        self._tier = LLMTier.TIER_B

    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None
    ) -> LLMResponse:
        """Return a mock response."""
        await asyncio.sleep(0.1)  # Simulate latency

        return LLMResponse(
            raw_text='{"status": "mock_response", "analysis": "This is a mock response for testing."}',
            model="mock-local",
            tier=self._tier,
            latency_ms=100,
            success=True
        )

    async def generate_json(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None
    ) -> LLMResponse:
        """Return a mock JSON response."""
        response = await self.generate(prompt, system, context)
        response.parsed_json = {"status": "mock_response", "analysis": "Mock analysis"}
        return response

    async def health_check(self) -> bool:
        """Mock is always healthy."""
        return True
