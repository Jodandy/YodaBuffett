"""
AI Analyst Service - Main orchestrator
"""
import time
from datetime import date
from typing import Dict, Optional
import asyncpg

from .models import (
    AnalysisRequest,
    AnalysisResult,
    PromptType,
    LLMProvider,
    DataSourceResult
)
from .data_sources import (
    FinancialsDataSource,
    CompanyInfoDataSource,
    PricesDataSource,
)
from .prompts import InvestmentMemoPrompt
from .llm import OpenAIProvider, LocalLLMProvider


class AIAnalystService:
    """
    Main service for generating AI-powered investment analysis.

    Orchestrates:
    1. Data fetching from multiple sources
    2. Prompt building
    3. LLM generation
    4. Response formatting
    """

    def __init__(self, db_conn: asyncpg.Connection):
        self.db_conn = db_conn

        # Initialize data sources
        self.data_sources = {
            'financials': FinancialsDataSource(db_conn),
            'company_info': CompanyInfoDataSource(db_conn),
            'prices': PricesDataSource(db_conn),
        }

        # Initialize prompts
        self.prompts = {
            PromptType.INVESTMENT_MEMO: InvestmentMemoPrompt(),
        }

        # LLM providers (initialized on-demand)
        self._llm_providers: Dict[str, any] = {}

    def _get_llm_provider(self, provider: LLMProvider):
        """Get or create LLM provider instance"""
        if provider.value not in self._llm_providers:
            if provider == LLMProvider.OPENAI:
                self._llm_providers[provider.value] = OpenAIProvider()
            elif provider == LLMProvider.ANTHROPIC:
                raise NotImplementedError("Anthropic provider not yet implemented")
            elif provider == LLMProvider.LOCAL:
                self._llm_providers[provider.value] = LocalLLMProvider()
            else:
                raise ValueError(f"Unknown provider: {provider}")

        return self._llm_providers[provider.value]

    async def generate_analysis(
        self,
        request: AnalysisRequest
    ) -> AnalysisResult:
        """
        Generate an AI-powered analysis for a company.

        This is the main entry point for the service.
        """
        start_time = time.time()

        # Get company info for metadata
        company = await self.db_conn.fetchrow("""
            SELECT id, company_name
            FROM company_master
            WHERE id = $1
        """, request.company_id)

        if not company:
            raise ValueError(f"Company not found: {request.company_id}")

        # 1. Fetch data from required sources
        data_results = await self._fetch_data(request)

        # 2. Get prompt template
        prompt_template = self.prompts.get(request.prompt_type)
        if not prompt_template:
            raise ValueError(f"Unknown prompt type: {request.prompt_type}")

        # 3. Build the prompt
        formatted_data = {}
        for source_name, result in data_results.items():
            if result.success:
                data_source = self.data_sources[source_name]
                formatted_data[source_name] = data_source.format_for_prompt(result.data)
            else:
                formatted_data[source_name] = f"Error: {result.error}"

        user_prompt = prompt_template.build_prompt(formatted_data)
        system_message = prompt_template.system_message

        # 4. Generate LLM response
        llm_provider = self._get_llm_provider(request.llm_provider)
        llm_response = await llm_provider.generate(
            system_message=system_message,
            user_message=user_prompt,
            model=request.model,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
        )

        # 5. Build result
        processing_time = time.time() - start_time
        analysis_date = request.analysis_date or date.today()

        result = AnalysisResult(
            company_id=request.company_id,
            company_name=company['company_name'],
            prompt_type=request.prompt_type,
            analysis_date=analysis_date,
            raw_response=llm_response.content,
            prompt_tokens=llm_response.prompt_tokens,
            completion_tokens=llm_response.completion_tokens,
            cost_usd=llm_response.cost_usd,
            processing_time_seconds=processing_time,
            llm_provider=llm_provider.provider_name,
            model_used=llm_response.model,
            data_sources_used=list(data_results.keys()),
        )

        return result

    async def _fetch_data(
        self,
        request: AnalysisRequest
    ) -> Dict[str, DataSourceResult]:
        """
        Fetch data from all required sources based on request.

        Returns dictionary of {source_name: DataSourceResult}
        """
        results = {}

        # Get prompt template to know which sources are required
        prompt_template = self.prompts.get(request.prompt_type)
        if not prompt_template:
            raise ValueError(f"Unknown prompt type: {request.prompt_type}")

        required_sources = prompt_template.required_data_sources

        # Fetch from each required source
        for source_name in required_sources:
            # Skip if user explicitly disabled
            if source_name == 'financials' and not request.include_financials:
                continue
            if source_name == 'prices' and not request.include_prices:
                continue
            if source_name == 'documents' and not request.include_documents:
                continue

            data_source = self.data_sources.get(source_name)
            if not data_source:
                results[source_name] = DataSourceResult(
                    source_name=source_name,
                    data={},
                    success=False,
                    error=f"Unknown data source: {source_name}"
                )
                continue

            # Fetch the data
            fetch_start = time.time()
            try:
                data = await data_source.fetch(
                    company_id=request.company_id,
                    as_of_date=request.analysis_date,
                    years_back=request.years_back
                )
                fetch_time = time.time() - fetch_start

                results[source_name] = DataSourceResult(
                    source_name=source_name,
                    data=data,
                    success=True,
                    query_time_seconds=fetch_time
                )
            except Exception as e:
                fetch_time = time.time() - fetch_start
                results[source_name] = DataSourceResult(
                    source_name=source_name,
                    data={},
                    success=False,
                    error=str(e),
                    query_time_seconds=fetch_time
                )

        return results
