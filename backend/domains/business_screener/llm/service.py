"""
LLM Service Orchestrator

Main service that coordinates LLM analysis for screens.
Handles provider selection, document context, caching, and result storage.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from uuid import UUID
import json

import asyncpg

from .base import BaseLLM, LLMResponse, LLMTier, LLMConfig
from .local_provider import OllamaLLM, MockLocalLLM
from .claude_provider import ClaudeLLM, MockClaudeLLM
from .parser import ResponseParser, ValidationLevel, ValidationResult
from .document_context import DocumentContextFetcher, DocumentContext


@dataclass
class AnalysisResult:
    """Result of an LLM analysis."""
    company_id: UUID
    screen_type: int
    tier: str
    success: bool
    raw_response: str
    parsed_response: Optional[Dict[str, Any]]
    validation_result: Optional[ValidationResult]
    score_adjustment: float
    latency_ms: float
    model: str
    error: Optional[str] = None
    document_context_chars: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "company_id": str(self.company_id),
            "screen_type": self.screen_type,
            "tier": self.tier,
            "success": self.success,
            "parsed_response": self.parsed_response,
            "score_adjustment": self.score_adjustment,
            "latency_ms": self.latency_ms,
            "model": self.model,
            "error": self.error,
            "document_context_chars": self.document_context_chars,
        }


class LLMService:
    """
    Main LLM service for Business Screener analysis.

    Coordinates:
    - Provider selection (Ollama for Tier B, Claude for Tier C)
    - Document context fetching
    - Response parsing and validation
    - Result storage in database
    - Score adjustments based on LLM insights

    Usage:
        service = LLMService(conn)
        await service.initialize()

        result = await service.analyze_tier_b(
            company_id=uuid,
            screen_type=3,
            metrics={"price_to_book": 0.6, ...},
            prompt=screen.get_tier_b_prompt(company_name, metrics)
        )
    """

    def __init__(
        self,
        conn: asyncpg.Connection,
        use_mock: bool = False,
        ollama_model: str = "llama3.1:8b",
        claude_model: str = "claude-sonnet-4-20250514"
    ):
        """
        Initialize the LLM service.

        Args:
            conn: Database connection
            use_mock: Use mock providers (for testing)
            ollama_model: Model to use for Tier B
            claude_model: Model to use for Tier C
        """
        self.conn = conn
        self.use_mock = use_mock

        # Initialize providers
        if use_mock:
            self.tier_b_provider: BaseLLM = MockLocalLLM()
            self.tier_c_provider: BaseLLM = MockClaudeLLM()
        else:
            self.tier_b_provider = OllamaLLM(LLMConfig(model=ollama_model))
            self.tier_c_provider = ClaudeLLM(LLMConfig(model=claude_model))

        # Initialize helpers
        self.parser = ResponseParser(ValidationLevel.LENIENT)
        self.context_fetcher = DocumentContextFetcher(conn)

        # Status
        self._initialized = False
        self._tier_b_available = False
        self._tier_c_available = False

    async def initialize(self) -> Dict[str, bool]:
        """
        Initialize the service and check provider availability.

        Returns:
            Dict with 'tier_b' and 'tier_c' availability
        """
        self._tier_b_available = await self.tier_b_provider.health_check()
        self._tier_c_available = await self.tier_c_provider.health_check()
        self._initialized = True

        return {
            "tier_b": self._tier_b_available,
            "tier_c": self._tier_c_available,
            "tier_b_model": self.tier_b_provider.model,
            "tier_c_model": self.tier_c_provider.model,
        }

    @property
    def is_tier_b_available(self) -> bool:
        """Check if Tier B (local LLM) is available."""
        return self._tier_b_available

    @property
    def is_tier_c_available(self) -> bool:
        """Check if Tier C (Claude API) is available."""
        return self._tier_c_available

    async def analyze_tier_b(
        self,
        company_id: UUID,
        company_name: str,
        screen_type: int,
        metrics: Dict[str, Any],
        prompt: str,
        score_date: date = None,
        include_document_context: bool = True
    ) -> AnalysisResult:
        """
        Run Tier B (local LLM) analysis for a candidate.

        Args:
            company_id: Company UUID
            company_name: Company name for logging
            screen_type: Screen number
            metrics: Tier A metrics for this company
            prompt: The analysis prompt (from screen.get_tier_b_prompt())
            score_date: Point-in-time date
            include_document_context: Whether to fetch document context

        Returns:
            AnalysisResult with parsed response and score adjustment
        """
        if not self._tier_b_available and not self.use_mock:
            return AnalysisResult(
                company_id=company_id,
                screen_type=screen_type,
                tier="B",
                success=False,
                raw_response="",
                parsed_response=None,
                validation_result=None,
                score_adjustment=0,
                latency_ms=0,
                model=self.tier_b_provider.model,
                error="Tier B provider not available"
            )

        # Get document context
        context_text = None
        context_chars = 0
        if include_document_context:
            context = await self.context_fetcher.get_context_for_company(
                company_id=company_id,
                screen_type=screen_type,
                score_date=score_date
            )
            if context:
                context_text = context.to_text(max_chars=40000)
                context_chars = len(context_text)

        # Generate response
        response = await self.tier_b_provider.generate_json(
            prompt=prompt,
            context=context_text
        )

        # Validate response
        validation_result = None
        score_adjustment = 0.0

        if response.success and response.parsed_json:
            validation_result = self.parser.validate(
                response.parsed_json,
                screen_type,
                "B"
            )
            if validation_result.is_valid:
                score_adjustment = self.parser.extract_score_adjustment(
                    validation_result.cleaned_data,
                    screen_type
                )

        # Store result
        result = AnalysisResult(
            company_id=company_id,
            screen_type=screen_type,
            tier="B",
            success=response.success and (response.parsed_json is not None),
            raw_response=response.raw_text,
            parsed_response=validation_result.cleaned_data if validation_result else response.parsed_json,
            validation_result=validation_result,
            score_adjustment=score_adjustment,
            latency_ms=response.latency_ms,
            model=response.model,
            error=response.error or response.parse_error,
            document_context_chars=context_chars
        )

        # Save to database
        await self._store_analysis_result(result, company_name)

        self._log(f"Tier B analysis for {company_name}: success={result.success}, adjustment={score_adjustment:+.1f}")

        return result

    async def analyze_tier_c(
        self,
        company_id: UUID,
        company_name: str,
        screen_type: int,
        metrics: Dict[str, Any],
        prompt: str,
        tier_b_result: Optional[AnalysisResult] = None,
        score_date: date = None,
        include_document_context: bool = True
    ) -> AnalysisResult:
        """
        Run Tier C (Claude API) deep analysis for a candidate.

        Args:
            company_id: Company UUID
            company_name: Company name for logging
            screen_type: Screen number
            metrics: Combined Tier A + B metrics
            prompt: The analysis prompt (from screen.get_tier_c_prompt())
            tier_b_result: Previous Tier B result (for context)
            score_date: Point-in-time date
            include_document_context: Whether to fetch document context

        Returns:
            AnalysisResult with parsed response and score adjustment
        """
        if not self._tier_c_available and not self.use_mock:
            return AnalysisResult(
                company_id=company_id,
                screen_type=screen_type,
                tier="C",
                success=False,
                raw_response="",
                parsed_response=None,
                validation_result=None,
                score_adjustment=0,
                latency_ms=0,
                model=self.tier_c_provider.model,
                error="Tier C provider not available"
            )

        # Get document context
        context_text = None
        context_chars = 0
        if include_document_context:
            context = await self.context_fetcher.get_context_for_company(
                company_id=company_id,
                screen_type=screen_type,
                score_date=score_date,
                max_sections=15,  # More sections for Tier C
                max_chars=80000   # Larger context for Claude
            )
            if context:
                context_text = context.to_text(max_chars=80000)
                context_chars = len(context_text)

        # Add Tier B insights to context if available
        if tier_b_result and tier_b_result.parsed_response:
            tier_b_summary = f"""
Previous Tier B Analysis Summary:
{json.dumps(tier_b_result.parsed_response, indent=2)}

Please incorporate and expand upon these initial findings in your deep analysis.
"""
            context_text = (context_text or "") + "\n\n" + tier_b_summary

        # Generate response
        response = await self.tier_c_provider.generate_json(
            prompt=prompt,
            context=context_text
        )

        # Validate response
        validation_result = None
        score_adjustment = 0.0

        if response.success and response.parsed_json:
            validation_result = self.parser.validate(
                response.parsed_json,
                screen_type,
                "C"
            )
            if validation_result.is_valid:
                score_adjustment = self.parser.extract_score_adjustment(
                    validation_result.cleaned_data,
                    screen_type
                )

        # Store result
        result = AnalysisResult(
            company_id=company_id,
            screen_type=screen_type,
            tier="C",
            success=response.success and (response.parsed_json is not None),
            raw_response=response.raw_text,
            parsed_response=validation_result.cleaned_data if validation_result else response.parsed_json,
            validation_result=validation_result,
            score_adjustment=score_adjustment,
            latency_ms=response.latency_ms,
            model=response.model,
            error=response.error or response.parse_error,
            document_context_chars=context_chars
        )

        # Save to database
        await self._store_analysis_result(result, company_name)

        self._log(f"Tier C analysis for {company_name}: success={result.success}, adjustment={score_adjustment:+.1f}, tokens={response.total_tokens}")

        return result

    async def analyze_batch_tier_b(
        self,
        candidates: List[Dict[str, Any]],
        screen,
        score_date: date = None,
        max_concurrent: int = 3
    ) -> List[AnalysisResult]:
        """
        Run Tier B analysis on a batch of candidates.

        Args:
            candidates: List of dicts with company_id, company_name, metrics
            screen: The screen instance (for get_tier_b_prompt)
            score_date: Point-in-time date
            max_concurrent: Maximum concurrent analyses

        Returns:
            List of AnalysisResults
        """
        semaphore = asyncio.Semaphore(max_concurrent)
        results = []

        async def analyze_one(candidate: Dict[str, Any]) -> AnalysisResult:
            async with semaphore:
                prompt = screen.get_tier_b_prompt(
                    candidate['company_name'],
                    candidate['metrics']
                )
                return await self.analyze_tier_b(
                    company_id=candidate['company_id'],
                    company_name=candidate['company_name'],
                    screen_type=screen.screen_type,
                    metrics=candidate['metrics'],
                    prompt=prompt,
                    score_date=score_date
                )

        tasks = [analyze_one(c) for c in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle exceptions
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self._log(f"Error analyzing {candidates[i]['company_name']}: {result}")
                final_results.append(AnalysisResult(
                    company_id=candidates[i]['company_id'],
                    screen_type=screen.screen_type,
                    tier="B",
                    success=False,
                    raw_response="",
                    parsed_response=None,
                    validation_result=None,
                    score_adjustment=0,
                    latency_ms=0,
                    model=self.tier_b_provider.model,
                    error=str(result)
                ))
            else:
                final_results.append(result)

        return final_results

    async def _store_analysis_result(
        self,
        result: AnalysisResult,
        company_name: str
    ):
        """Store analysis result in database."""
        try:
            await self.conn.execute("""
                INSERT INTO bsd_llm_analysis_results (
                    company_id,
                    analysis_type,
                    tier,
                    model_used,
                    raw_response,
                    parsed_response,
                    confidence_score,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
                result.company_id,
                f"screen_{result.screen_type}",
                result.tier,
                result.model,
                result.raw_response[:10000] if result.raw_response else None,  # Truncate if too long
                json.dumps(result.parsed_response) if result.parsed_response else None,
                1.0 if result.success else 0.0,
                datetime.now()
            )
        except Exception as e:
            self._log(f"Failed to store analysis result for {company_name}: {e}")

    async def get_previous_analysis(
        self,
        company_id: UUID,
        screen_type: int,
        tier: str,
        max_age_days: int = 30
    ) -> Optional[Dict[str, Any]]:
        """
        Get a previous analysis result if still valid.

        Args:
            company_id: Company UUID
            screen_type: Screen number
            tier: 'B' or 'C'
            max_age_days: Maximum age of cached result

        Returns:
            Parsed response dict or None
        """
        row = await self.conn.fetchrow("""
            SELECT parsed_response, created_at
            FROM bsd_llm_analysis_results
            WHERE company_id = $1
              AND analysis_type = $2
              AND tier = $3
              AND created_at > NOW() - INTERVAL '$4 days'
            ORDER BY created_at DESC
            LIMIT 1
        """, company_id, f"screen_{screen_type}", tier, max_age_days)

        if row and row['parsed_response']:
            try:
                return json.loads(row['parsed_response'])
            except json.JSONDecodeError:
                pass

        return None

    def _log(self, message: str):
        """Log a message."""
        print(f"[LLMService] {message}")


async def create_llm_service(
    conn: asyncpg.Connection,
    use_mock: bool = False
) -> LLMService:
    """
    Factory function to create and initialize an LLM service.

    Args:
        conn: Database connection
        use_mock: Use mock providers for testing

    Returns:
        Initialized LLMService
    """
    service = LLMService(conn, use_mock=use_mock)
    status = await service.initialize()
    print(f"[LLMService] Initialized: Tier B={status['tier_b']} ({status['tier_b_model']}), Tier C={status['tier_c']} ({status['tier_c_model']})")
    return service
