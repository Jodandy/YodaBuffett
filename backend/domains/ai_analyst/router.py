"""
FastAPI router for AI Analyst endpoints
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date
import asyncpg

from .service import AIAnalystService
from .models import (
    AnalysisRequest,
    AnalysisResult,
    PromptType,
    LLMProvider
)


router = APIRouter(prefix="/ai-analyst", tags=["AI Analyst"])


# Pydantic models for API
class AnalysisRequestAPI(BaseModel):
    """API request model for analysis generation"""
    company_id: str = Field(..., description="Company UUID")
    prompt_type: PromptType = Field(
        PromptType.INVESTMENT_MEMO,
        description="Type of analysis to generate"
    )
    analysis_date: Optional[date] = Field(
        None,
        description="Point-in-time date for analysis (None = latest data)"
    )

    # Data options
    include_financials: bool = Field(True, description="Include financial statements")
    include_prices: bool = Field(True, description="Include price data")
    include_documents: bool = Field(False, description="Include document text (experimental)")
    years_back: int = Field(3, ge=1, le=10, description="Years of historical data")

    # LLM options
    llm_provider: LLMProvider = Field(
        LLMProvider.OPENAI,
        description="LLM provider to use"
    )
    model: Optional[str] = Field(None, description="Specific model (None = provider default)")
    temperature: float = Field(0.3, ge=0.0, le=1.0, description="LLM temperature")
    max_tokens: int = Field(4000, ge=100, le=16000, description="Max tokens to generate")


class AnalysisResultAPI(BaseModel):
    """API response model for analysis result"""
    company_id: str
    company_name: str
    prompt_type: str
    analysis_date: date

    # The actual analysis
    analysis: str
    structured_data: Optional[dict] = None

    # Metadata
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    processing_time_seconds: float
    llm_provider: str
    model_used: str
    data_sources_used: List[str]


class PromptTypesResponse(BaseModel):
    """Available prompt types"""
    prompt_types: List[dict]


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    available_prompts: List[str]
    available_providers: List[str]


# Dependency: Get database connection
async def get_db_connection():
    """Get database connection (replace with proper connection pooling in production)"""
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    try:
        yield conn
    finally:
        await conn.close()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        available_prompts=[pt.value for pt in PromptType],
        available_providers=[lp.value for lp in LLMProvider]
    )


@router.get("/prompt-types", response_model=PromptTypesResponse)
async def get_prompt_types():
    """Get available prompt types"""
    return PromptTypesResponse(
        prompt_types=[
            {
                "name": PromptType.INVESTMENT_MEMO.value,
                "description": "Comprehensive investment analysis memo with thesis and recommendation",
                "required_data": ["financials", "company_info", "prices"]
            }
        ]
    )


@router.post("/analyze", response_model=AnalysisResultAPI)
async def generate_analysis(
    request: AnalysisRequestAPI,
    db_conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Generate an AI-powered investment analysis for a company.

    This endpoint:
    1. Fetches financial data, company metrics, and price history
    2. Formats the data into a structured prompt
    3. Sends to LLM (OpenAI, Anthropic, or local)
    4. Returns formatted analysis with cost/token metadata

    Example:
    ```
    POST /api/v1/ai-analyst/analyze
    {
        "company_id": "UUID-here",
        "prompt_type": "investment_memo",
        "llm_provider": "openai",
        "years_back": 3
    }
    ```
    """
    try:
        # Convert API request to domain model
        domain_request = AnalysisRequest(
            company_id=request.company_id,
            prompt_type=request.prompt_type,
            analysis_date=request.analysis_date,
            include_financials=request.include_financials,
            include_prices=request.include_prices,
            include_documents=request.include_documents,
            years_back=request.years_back,
            llm_provider=request.llm_provider,
            model=request.model,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
        )

        # Generate analysis
        service = AIAnalystService(db_conn)
        result = await service.generate_analysis(domain_request)

        # Convert to API response
        return AnalysisResultAPI(
            company_id=result.company_id,
            company_name=result.company_name,
            prompt_type=result.prompt_type.value,
            analysis_date=result.analysis_date,
            analysis=result.raw_response,
            structured_data=result.structured_data,
            prompt_tokens=result.prompt_tokens,
            completion_tokens=result.completion_tokens,
            cost_usd=result.cost_usd,
            processing_time_seconds=result.processing_time_seconds,
            llm_provider=result.llm_provider,
            model_used=result.model_used,
            data_sources_used=result.data_sources_used,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.get("/company/{company_id}/preview-data")
async def preview_company_data(
    company_id: str,
    years_back: int = 3,
    db_conn: asyncpg.Connection = Depends(get_db_connection)
):
    """
    Preview what data is available for a company without generating analysis.

    Useful for checking data quality before spending LLM tokens.
    """
    try:
        service = AIAnalystService(db_conn)

        # Fetch all data sources
        from .data_sources import (
            FinancialsDataSource,
            CompanyInfoDataSource,
            PricesDataSource
        )

        financials_ds = FinancialsDataSource(db_conn)
        company_info_ds = CompanyInfoDataSource(db_conn)
        prices_ds = PricesDataSource(db_conn)

        financials = await financials_ds.fetch(company_id, years_back=years_back)
        company_info = await company_info_ds.fetch(company_id)
        prices = await prices_ds.fetch(company_id, years_back=years_back)

        return {
            "company_id": company_id,
            "data_availability": {
                "financials": {
                    "available": "error" not in financials,
                    "annual_statements": len(financials.get('annual_statements', [])),
                    "quarterly_statements": len(financials.get('quarterly_statements', [])),
                },
                "company_info": {
                    "available": "error" not in company_info,
                },
                "prices": {
                    "available": "error" not in prices,
                    "current_price": prices.get('current_price'),
                    "price_history_days": len(prices.get('recent_prices', [])),
                }
            },
            "preview": {
                "financials": financials_ds.format_for_prompt(financials)[:500] + "...",
                "company_info": company_info_ds.format_for_prompt(company_info)[:500] + "...",
                "prices": prices_ds.format_for_prompt(prices)[:500] + "...",
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
