# Research Service - AI-Powered Company Analysis

## Overview

The Research Service provides AI-assisted deep research capabilities for individual companies. It processes financial documents (PDFs), extracts structured data, generates embeddings for semantic search, and uses LLMs to provide comprehensive analysis and insights.

## Features

### 🧠 **AI-Powered Document Analysis**
- Process PDFs (quarterly reports, annual reports, press releases)
- Extract financial metrics automatically
- Generate structured insights using GPT-4o-mini
- Support for Swedish and English documents

### 🔍 **Semantic Search**
- Vector-based document search
- Find documents by meaning, not just keywords
- Cross-document pattern recognition
- Intelligent query expansion

### 📊 **Deep Company Research**
- Comprehensive financial analysis
- Risk assessment and competitive analysis
- Growth trajectory evaluation
- Historical trend analysis

### 🎯 **Key Capabilities**
- **Document Processing**: Extract text, tables, and metadata from PDFs
- **Financial Parsing**: Automatically extract key metrics (revenue, EBITDA, margins)
- **Language Detection**: Handle Swedish/English documents seamlessly
- **Insight Generation**: AI-powered analysis with confidence scores
- **Timeline Analysis**: Track metrics over time

## Quick Start

### Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (for cloud LLM)
export OPENAI_API_KEY="your-openai-key"
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost/yodabuffett"

# OR for local LLM (completely free!)
export USE_LOCAL_LLM=true
export DATABASE_URL="postgresql+asyncpg://user:pass@localhost/yodabuffett"
```

### Local LLM Setup (Recommended for Cost-Free Analysis)

```bash
# 1. Install Ollama
# Visit: https://ollama.ai/download

# 2. Start Ollama server
ollama serve

# 3. Install recommended model (4.7GB)
ollama pull llama3.1:8b

# 4. Test the setup
python research-service/demo_local_llm.py

# 5. Run research service with local LLM
export USE_LOCAL_LLM=true
python -m research-service.main
```

### Run the Service

```bash
# Start the API server
python -m research-service.main

# Or run demo
python research-service/demo.py
```

### API Endpoints

- **Health Check**: `GET /api/v1/research/health`
- **Company Overview**: `GET /api/v1/research/company/{company_id}`
- **Deep Analysis**: `POST /api/v1/research/company/{company_id}/analyze`
- **Document Search**: `POST /api/v1/research/search`
- **Metric Timeline**: `GET /api/v1/research/company/{company_id}/timeline`

## Usage Examples

### 1. Company Analysis

```python
import httpx

async with httpx.AsyncClient() as client:
    # Analyze Volvo Group
    response = await client.post(
        "http://localhost:8002/api/v1/research/company/volvo-id/analyze",
        json={
            "analysis_type": "comprehensive",
            "years": [2024, 2025],
            "focus_areas": ["growth", "profitability", "ev_strategy"]
        }
    )
    
    analysis = response.json()
    print(f"Executive Summary: {analysis['executive_summary']}")
```

### 2. Document Search

```python
# Semantic search across documents
response = await client.post(
    "http://localhost:8002/api/v1/research/search",
    json={
        "query": "electric vehicle strategy and market position",
        "company_id": "volvo-id",
        "limit": 5
    }
)

results = response.json()
for result in results:
    print(f"Document: {result['document']['title']}")
    print(f"Relevance: {result['relevance_score']:.2f}")
```

### 3. Ask Questions

```python
# Q&A about a company
response = await client.post(
    "http://localhost:8002/api/v1/research/company/volvo-id/ask",
    json={
        "question": "What are the main risks mentioned in recent reports?",
        "include_sources": True
    }
)

qa = response.json()
print(f"Answer: {qa['answer']}")
print(f"Sources: {len(qa['sources'])} documents")
```

## Architecture

```
research-service/
├── api/                    # FastAPI endpoints
│   ├── router.py          # Main API routes
│   └── schemas.py         # Request/response models
├── services/              # Business logic
│   ├── document_service.py    # Document handling
│   ├── analysis_service.py    # LLM analysis
│   ├── embedding_service.py   # Vector operations
│   └── insight_service.py     # Insight extraction
├── processors/            # Document processing
│   ├── pdf_processor.py       # PDF text extraction
│   ├── financial_parser.py    # Metric extraction
│   └── language_detector.py   # Multi-language support
├── models/                # Database models
│   └── research_models.py     # SQLAlchemy models
└── config.py              # Configuration
```

## Configuration

### Environment Variables

```bash
# API Settings
API_HOST=0.0.0.0
API_PORT=8002

# Database
DATABASE_URL=postgresql+asyncpg://user:pass@localhost/yodabuffett

# AI/LLM
OPENAI_API_KEY=your-key
ANTHROPIC_API_KEY=your-key-optional
DEFAULT_LLM_MODEL=gpt-4o-mini

# Processing
MAX_CHUNK_SIZE=8000
MAX_PDF_SIZE_MB=50

# Caching
REDIS_URL=redis://localhost:6379
CACHE_TTL_SECONDS=3600
```

### Cost Control

```python
# Estimated costs per analysis:
# - PDF processing: ~$0.001 per document
# - Embeddings: ~$0.01 per document  
# - LLM analysis: ~$0.10-0.50 per analysis

# Cost controls in config.py:
MAX_TOKENS_PER_REQUEST = 100000
MAX_COST_PER_ANALYSIS = 1.0  # USD
```

## Database Schema

The service adds these tables to the existing YodaBuffett database:

- **research_sessions**: Track research sessions
- **analysis_results**: Cache analysis results
- **document_embeddings**: Store vector embeddings
- **research_insights**: Individual insights
- **company_metric_history**: Historical metrics
- **search_queries**: Query analytics

## Integration with Nordic Ingestion

The Research Service builds on top of the Nordic Ingestion Service:

1. **Documents**: Uses existing `nordic_documents` table
2. **Companies**: References `nordic_companies` table  
3. **Processing**: Processes PDFs already downloaded and stored
4. **Analysis**: Adds AI layer on top of collected documents

## Performance

### Processing Speed
- **PDF Processing**: ~2-3 seconds per document
- **Embedding Generation**: ~1 second per 1000 tokens
- **LLM Analysis**: ~10-30 seconds depending on complexity

### Scalability
- Async processing throughout
- Batch operations for embeddings
- Redis caching for frequent queries
- Database connection pooling

## Example Analysis Output

```json
{
  "executive_summary": "Volvo Group demonstrates strong financial performance in Q3 2025 with 15% revenue growth and improved margins. The company's electric vehicle strategy is gaining traction with increasing market share.",
  "insights": [
    {
      "category": "financial",
      "insight": "Revenue growth of 15% demonstrates strong market demand and operational efficiency",
      "confidence": 0.9,
      "supporting_evidence": ["Q3 revenue: SEK 35.2B vs Q3 2024: SEK 30.6B"],
      "metrics": {"revenue_growth_pct": 15.0}
    }
  ],
  "key_metrics": {
    "revenue_growth": "15%",
    "ebita_margin": "18.5%",
    "order_intake": "SEK 38.1B"
  },
  "risk_assessment": {
    "overall_risk": "medium",
    "key_risks": [
      {"risk": "Supply chain disruptions", "severity": "medium"}
    ]
  },
  "cost": 0.15,
  "processing_time": 12.3
}
```

## Development

### Running Tests

```bash
pytest research-service/tests/
```

### Adding New Analysis Types

1. Add prompt template to `AnalysisService.ANALYSIS_PROMPTS`
2. Update `AnalysisType` enum in schemas
3. Add specific processing logic if needed

### Extending Document Support

1. Add processor in `processors/` directory
2. Register in `DocumentService`
3. Update supported file types

## Production Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY research-service/ ./research-service/
EXPOSE 8002

CMD ["python", "-m", "research-service.main"]
```

### Health Monitoring

The service provides health endpoints and structured logging for production monitoring.

## Next Steps

1. **Vector Database**: Set up pgvector or Pinecone for production
2. **Streaming**: Add WebSocket support for real-time analysis
3. **Caching**: Implement Redis for frequently accessed insights  
4. **Batch Processing**: Add background job processing
5. **UI Integration**: Connect with frontend dashboard

## Cost Analysis

### Cloud LLM Option
For 6,047 Swedish PDFs:
- **Processing**: ~$6 (one-time)
- **Embeddings**: ~$60 (one-time) 
- **Per Analysis**: ~$0.25 average
- **Monthly Usage**: ~$50-200 depending on usage

### Local LLM Option (Recommended)
For 6,047 Swedish PDFs:
- **Setup Cost**: $0 (completely free!)
- **Processing**: $0 per document
- **Embeddings**: $0 (can use local embedding models)
- **Per Analysis**: $0 (unlimited usage)
- **Monthly Usage**: $0 (only electricity costs)

**Local LLM provides institutional-grade research capabilities at zero ongoing cost!**

Benefits:
- ✅ Complete data privacy (no external API calls)
- ✅ No usage limits or rate limiting
- ✅ Offline capability
- ✅ Zero ongoing costs
- ✅ Full control over model versions