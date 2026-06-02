"""
Claude LLM Provider (Anthropic API)

Tier C analysis using Claude for deep, high-quality analysis.
Used sparingly for top candidates due to API costs.
"""

import asyncio
import json
import os
import time
from typing import Optional, Dict, Any

from .base import BaseLLM, LLMConfig, LLMResponse, LLMTier


class ClaudeLLM(BaseLLM):
    """
    Claude-based LLM provider for Tier C deep analysis.

    Uses the Anthropic API for high-quality financial analysis.
    Should be used sparingly - only for top candidates that passed Tier A/B.

    Default model: claude-sonnet-4-20250514 (good balance of quality and cost)
    Alternative: claude-opus-4-20250514 (highest quality, higher cost)

    Requires ANTHROPIC_API_KEY environment variable.
    """

    DEFAULT_MODEL = "claude-sonnet-4-20250514"

    def __init__(self, config: LLMConfig = None):
        if config is None:
            config = LLMConfig(
                model=self.DEFAULT_MODEL,
                api_key=os.environ.get("ANTHROPIC_API_KEY"),
                temperature=0.1,
                max_tokens=4096,
                timeout_seconds=60.0
            )
        super().__init__(config)
        self._tier = LLMTier.TIER_C
        self._client = None

    def _get_client(self):
        """Lazy-load the Anthropic client."""
        if self._client is None:
            try:
                import anthropic
                api_key = self.config.api_key or os.environ.get("ANTHROPIC_API_KEY")
                if not api_key:
                    raise ValueError("ANTHROPIC_API_KEY not set")
                self._client = anthropic.Anthropic(api_key=api_key)
            except ImportError:
                raise ImportError("anthropic package not installed. Run: pip install anthropic")
        return self._client

    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None
    ) -> LLMResponse:
        """
        Generate a response using Claude API.

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
            client = self._get_client()

            # Run sync client in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            message = await loop.run_in_executor(
                None,
                lambda: client.messages.create(
                    model=self.config.model,
                    max_tokens=self.config.max_tokens,
                    system=system,
                    messages=[
                        {"role": "user", "content": full_prompt}
                    ]
                )
            )

            latency_ms = (time.time() - start_time) * 1000

            # Extract text from response
            raw_text = ""
            if message.content:
                raw_text = message.content[0].text

            return LLMResponse(
                raw_text=raw_text,
                model=self.config.model,
                tier=self._tier,
                prompt_tokens=message.usage.input_tokens,
                completion_tokens=message.usage.output_tokens,
                total_tokens=message.usage.input_tokens + message.usage.output_tokens,
                latency_ms=latency_ms,
                success=True
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
        Generate a JSON response using Claude API.

        Claude is excellent at following JSON format instructions.

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
        Check if Claude API is accessible.

        Does a minimal API call to verify credentials work.

        Returns:
            True if healthy, False otherwise
        """
        try:
            client = self._get_client()

            # Minimal API call
            loop = asyncio.get_event_loop()
            message = await loop.run_in_executor(
                None,
                lambda: client.messages.create(
                    model=self.config.model,
                    max_tokens=10,
                    messages=[
                        {"role": "user", "content": "Reply with just 'ok'"}
                    ]
                )
            )

            return message.content[0].text.lower().strip() in ["ok", "'ok'", '"ok"']

        except Exception as e:
            self._log(f"Health check failed: {e}")
            return False

    def _get_default_system_prompt(self) -> str:
        """Get the default system prompt for deep financial analysis."""
        return """You are an elite financial analyst with expertise in:
- Deep fundamental analysis and valuation
- Competitive moat assessment
- Quality of earnings analysis
- Management evaluation
- Risk identification

Your analysis style:
- Thorough and nuanced - you consider multiple perspectives
- Evidence-based - you cite specific data points from documents
- Balanced - you present both positives and concerns
- Actionable - your conclusions are clear and useful
- Conservative - you prefer understating rather than overstating

When providing JSON responses:
- Follow the exact schema requested
- Use consistent capitalization for enum values
- Include confidence levels when uncertain
- Provide specific evidence for your assessments"""

    def _enhance_prompt_for_json(self, prompt: str) -> str:
        """Add explicit JSON formatting instructions."""
        return f"""{prompt}

Respond with valid JSON only. Do not include any text before or after the JSON object."""

    def _extract_json(self, text: str) -> Dict[str, Any]:
        """
        Extract JSON from Claude response.

        Claude is generally good at producing clean JSON, but we handle
        edge cases anyway.

        Args:
            text: Raw Claude response

        Returns:
            Parsed JSON dictionary

        Raises:
            json.JSONDecodeError if no valid JSON found
        """
        text = text.strip()

        # Try direct parse first (Claude usually outputs clean JSON)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Handle markdown code blocks
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


class MockClaudeLLM(BaseLLM):
    """
    Mock Claude LLM for testing without API access.

    Returns placeholder responses that match expected JSON schemas.
    """

    def __init__(self, config: LLMConfig = None):
        super().__init__(config or LLMConfig(model="mock-claude"))
        self._tier = LLMTier.TIER_C

    async def generate(
        self,
        prompt: str,
        system: Optional[str] = None,
        context: Optional[str] = None
    ) -> LLMResponse:
        """Return a mock response."""
        await asyncio.sleep(0.2)  # Simulate API latency

        return LLMResponse(
            raw_text='{"status": "mock_claude_response", "deep_analysis": "This is a mock Claude response for testing."}',
            model="mock-claude",
            tier=self._tier,
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=200,
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
        response.parsed_json = {
            "status": "mock_claude_response",
            "deep_analysis": "Mock deep analysis from Claude",
            "confidence": "HIGH"
        }
        return response

    async def health_check(self) -> bool:
        """Mock is always healthy."""
        return True
