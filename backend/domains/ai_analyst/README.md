

# AI Analyst Domain

Clean, modular LLM-powered investment analysis using raw financial data.

## Architecture

```
domains/ai_analyst/
├── models.py                    # Core data models
├── service.py                   # Main orchestrator
├── router.py                    # FastAPI endpoints
│
├── data_sources/               # Fetch raw data from database
│   ├── base.py                # BaseDataSource interface
│   ├── financials.py          # yahoo_financials (income, balance, cash flow)
│   ├── company_info.py        # yahoo_company_info (P/E, ROE, margins)
│   └── prices.py              # daily_price_data (OHLCV, returns)
│
├── prompts/                    # Modular prompt templates
│   ├── base.py                # BasePrompt interface
│   └── investment_memo.py     # Full investment memo
│
└── llm/                        # LLM provider integrations
    ├── base.py                # BaseLLMProvider interface
    └── openai_provider.py     # OpenAI GPT integration
```

## Quick Start

### 1. Set up API key

```bash
export OPENAI_API_KEY="your-key-here"
```

### 2. Add router to main.py

```python
from domains.ai_analyst.router import router as ai_analyst_router

app.include_router(ai_analyst_router, prefix="/api/v1")
```

### 3. Generate analysis

```bash
curl -X POST "http://localhost:8000/api/v1/ai-analyst/analyze" \
  -H "Content-Type: application/json" \
  -d '{
    "company_id": "company-uuid-here",
    "prompt_type": "investment_memo",
    "llm_provider": "openai",
    "years_back": 3
  }'
```

## API Endpoints

### `POST /api/v1/ai-analyst/analyze`
Generate AI-powered investment analysis

**Request:**
```json
{
  "company_id": "uuid",
  "prompt_type": "investment_memo",
  "analysis_date": null,
  "include_financials": true,
  "include_prices": true,
  "years_back": 3,
  "llm_provider": "openai",
  "model": null,
  "temperature": 0.3,
  "max_tokens": 4000
}
```

**Response:**
```json
{
  "company_id": "uuid",
  "company_name": "Volvo",
  "prompt_type": "investment_memo",
  "analysis": "# Investment Memo\n\n...",
  "prompt_tokens": 2500,
  "completion_tokens": 1200,
  "cost_usd": 0.08,
  "processing_time_seconds": 8.5,
  "llm_provider": "openai",
  "model_used": "gpt-4o-mini"
}
```

### `GET /api/v1/ai-analyst/health`
Health check

### `GET /api/v1/ai-analyst/prompt-types`
List available prompts

### `GET /api/v1/ai-analyst/company/{company_id}/preview-data`
Preview available data without generating analysis (saves tokens)

## Data Sources

### 1. Financials (`yahoo_financials`)
- Annual income statements, balance sheets, cash flows
- Quarterly results
- Key metrics: revenue, net income, debt, cash flow
- Currency information
- Publish dates (for point-in-time accuracy)

### 2. Company Info (`yahoo_company_info`)
- Valuation: P/E, P/B, PEG ratio
- Profitability: margins, ROE, ROA
- Analyst data: recommendations, price targets
- Ownership: insider %, institutional %
- Dividend information

### 3. Prices (`daily_price_data`)
- Current price
- Historical OHLCV
- Returns: 1mo, 3mo, 6mo, 1yr
- 52-week high/low

## Prompt Templates

### Investment Memo
**Type:** `investment_memo`
**Description:** Comprehensive Buffett-style investment analysis

**Sections:**
1. Executive Summary
2. Business Quality Assessment
3. Valuation Analysis
4. Investment Thesis (Bull/Bear cases)
5. Key Questions / Data Gaps
6. Conclusion & Recommendation

**Data Required:** financials, company_info, prices

## Adding New Components

### New Data Source

```python
# data_sources/my_new_source.py
from .base import BaseDataSource

class MyNewDataSource(BaseDataSource):
    @property
    def source_name(self) -> str:
        return "my_source"

    async def fetch(self, company_id, as_of_date=None, years_back=3):
        # Query database
        return {"data": "..."}

    def format_for_prompt(self, data):
        # Format for LLM
        return "Formatted data..."
```

### New Prompt Template

```python
# prompts/my_new_prompt.py
from .base import BasePrompt

class MyNewPrompt(BasePrompt):
    @property
    def name(self) -> str:
        return "my_prompt"

    @property
    def required_data_sources(self) -> List[str]:
        return ['financials', 'prices']

    def build_prompt(self, data: Dict) -> str:
        return f"""
        Analyze this company...

        {data['financials']}
        {data['prices']}
        """
```

### New LLM Provider

```python
# llm/my_provider.py
from .base import BaseLLMProvider, LLMResponse

class MyProvider(BaseLLMProvider):
    @property
    def provider_name(self) -> str:
        return "my_provider"

    async def generate(self, system_message, user_message, **kwargs):
        # Call LLM API
        return LLMResponse(...)
```

## Cost Management

**OpenAI Pricing (per analysis):**
- gpt-4o-mini: ~$0.05-0.15 (recommended)
- gpt-4o: ~$0.20-0.50
- gpt-4-turbo: ~$0.80-2.00

**Tips:**
- Use `preview-data` endpoint first to check data quality
- Start with gpt-4o-mini
- Lower `temperature` for more conservative/consistent output
- Reduce `years_back` if you don't need full history

## Design Principles

1. **Clean Slate** - Only raw financial data, no opinionated scores
2. **Modular Prompts** - Each prompt is a separate class
3. **Composable Data** - Mix and match data sources
4. **Provider Agnostic** - Swap OpenAI ↔ Anthropic ↔ Local easily
5. **Point-in-Time Safe** - All data respects `as_of_date` parameter

## Next Steps

- [ ] Add Anthropic (Claude) provider
- [ ] Add local LLM provider (Ollama)
- [ ] Add more prompt types (risk analysis, growth analysis)
- [ ] Add document data source (extracted_documents)
- [ ] Add structured output parsing (JSON mode)
- [ ] Add caching for expensive data fetches
- [ ] Add batch analysis endpoint
