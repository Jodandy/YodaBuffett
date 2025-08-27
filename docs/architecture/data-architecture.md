# YodaBuffett - Data Architecture

## Database Strategy
Polyglot persistence approach, optimizing each data type for its specific use case.

## Primary Database: PostgreSQL + TimescaleDB
**Purpose:** Structured financial data, user management, business logic  
**Location:** `/backend/shared/database/`

### Core Tables
```sql
-- Raw SEC filing storage
filings (
  id UUID PRIMARY KEY,
  company_symbol VARCHAR(10),
  form_type VARCHAR(10), -- '10-K', '10-Q', '8-K'
  filing_date DATE,
  raw_html TEXT, -- Full SEC filing
  extracted_text TEXT, -- Cleaned version
  processing_status VARCHAR(20)
);

-- LLM-extracted financial metrics
company_financials (
  company_symbol VARCHAR(10),
  fiscal_year INTEGER,
  fiscal_quarter INTEGER, -- NULL for annual
  revenue DECIMAL(15,2),
  net_income DECIMAL(15,2),
  roe DECIMAL(5,2), -- Return on Equity
  roa DECIMAL(5,2), -- Return on Assets
  extraction_confidence DECIMAL(3,2)
);

-- Business classification (enables queries like "outdoor companies")
company_business_info (
  company_symbol VARCHAR(10),
  primary_business TEXT,
  business_keywords JSONB, -- ["outdoor", "apparel", "retail"]
  product_categories JSONB, -- ["clothing", "footwear"]
  main_competitors JSONB
);

-- LLM analysis results (sentiment, competitive analysis)
llm_analyses (
  filing_id UUID,
  analysis_type VARCHAR(50), -- 'management_sentiment'
  analysis_result JSONB, -- Structured LLM output
  model_used VARCHAR(50),
  tokens_used INTEGER,
  cost_cents INTEGER
);

-- Track extraction versions for model improvements
extraction_versions (
  id UUID PRIMARY KEY,
  filing_id UUID REFERENCES filings(id),
  version INTEGER,
  model_version VARCHAR(50),
  extracted_at TIMESTAMP,
  metrics_extracted JSONB,
  UNIQUE(filing_id, version)
);

-- Learn from user behavior and improve search
query_performance (
  id UUID PRIMARY KEY,
  query_hash VARCHAR(64),
  query_type VARCHAR(20), -- 'structured', 'semantic', 'hybrid'
  query_text TEXT,
  response_time_ms INTEGER,
  result_count INTEGER,
  user_feedback INTEGER, -- -1, 0, 1 (thumbs down/none/up)
  clicked_results JSONB, -- Track which results users found useful
  created_at TIMESTAMP DEFAULT NOW()
);
```

### TimescaleDB Tables
```sql
-- Historical stock prices for backtesting
stock_prices (
  symbol VARCHAR(10),
  date DATE,
  close_price DECIMAL(10,4),
  volume BIGINT
) PARTITION BY RANGE (date);

-- Strategy performance tracking
backtest_results (
  strategy_name VARCHAR(100),
  total_return DECIMAL(8,4),
  sharpe_ratio DECIMAL(6,4),
  trades JSONB
);
```

## Vector Database: Pinecone/Weaviate
**Purpose:** Semantic search and RAG queries  
**Service:** `research-service`

### Document Structure
```json
{
  "id": "filing_123_chunk_5",
  "vector": [0.1, -0.3, 0.7, ...],
  "metadata": {
    "company_symbol": "AAPL",
    "section_name": "MD&A", 
    "filing_date": "2024-01-31",
    "chunk_text": "Revenue increased 15%...",
    "word_count": 347,
    "chunk_position": 5,
    "total_chunks": 42,
    "embedding_model": "text-embedding-3-large",
    "extracted_entities": ["revenue", "15%", "Q4"]
  }
}
```

### Embedding Strategy
- **Different models for different content:** 
  - Financial tables: `text-embedding-3-small` (faster, cheaper)
  - Narrative sections: `text-embedding-3-large` (better context)
- **Chunk overlap:** 10-20% overlap between chunks to preserve context
- **Metadata extraction before embedding:**
  - Extract dates, numbers, percentages
  - Identify section headers
  - Tag financial terms and metrics

## Cache Layer: Redis
**Purpose:** API caching, sessions, job queues  
**Services:** All services

### Key Patterns
- `query:{hash}` - Cached query results (1 hour TTL)
- `session:user_{id}` - User sessions
- `queue:filing_processing` - Background job queue
- `rate_limit:user_{id}:api_calls` - Rate limiting counters

## Data Processing Pipeline

### 1. Ingestion (`data-ingestion-service`)
```
SEC EDGAR → Raw HTML → Clean Text → PostgreSQL filings table
```

### 2. LLM Processing (`research-service`) 
```
Filing Text → LLM Analysis → Structured Data → PostgreSQL
├── Financial Metrics → company_financials
├── Business Info → company_business_info  
└── Qualitative Analysis → llm_analyses
```

### 3. Vector Processing (`research-service`)
```
Filing Text → Chunks → Embeddings → Vector Database
```

### 4. Query Processing
```
User Question → Vector Search → Relevant Chunks → LLM → Answer
              ↓
            PostgreSQL Structured Queries (for financial metrics)
```

## Query Examples

### Structured Financial Queries
```sql
-- "Show me outdoor companies ranked by ROE"
SELECT cf.company_symbol, cf.roe, cb.primary_business
FROM company_financials cf
JOIN company_business_info cb USING (company_symbol)
WHERE cb.business_keywords ? 'outdoor'
ORDER BY cf.roe DESC;
```

### Semantic Queries
```python
# "Which biotech companies mentioned FDA delays?"
vector_results = await vector_db.query(
    "FDA approval delays regulatory challenges",
    filter={"business_keywords": "biotech"},
    top_k=50
)
```

### Hybrid Queries  
```python
# "Find profitable Nordic biotech companies with confident management"
financial_filter = "roe > 15 AND country = 'Nordic'"
semantic_query = "management confident optimistic guidance"
results = await hybrid_search(financial_filter, semantic_query)
```

## Performance Optimization

### Database Optimization
- **Partitioning:** TimescaleDB partitions by date
- **Indexing:** Composite indexes on (company_symbol, fiscal_year)
- **JSONB:** GIN indexes on business_keywords for fast array searches

### Vector Database
- **Chunking:** 300-500 words per chunk for optimal embeddings
- **Metadata filtering:** Pre-filter by company/sector before vector search
- **Hierarchical search:** Coarse-to-fine filtering strategy

### Caching Strategy
- **Query results:** Hash-based caching of expensive LLM queries
- **Embeddings:** Cache frequently accessed document vectors
- **Rate limiting:** Redis counters for API quota management

## Cost Management

### LLM API Costs
- **Batch processing:** Analyze multiple filings per API call
- **Caching:** Never re-analyze same filing/section
- **Smart chunking:** Prioritize high-value sections (MD&A, Risk Factors)

### Storage Costs
- **Data lifecycle:** Archive old filings, keep metadata
- **Vector optimization:** Dimensionality reduction for less critical documents
- **Compression:** JSONB compression for large analysis results

## Monitoring & Observability

### Data Quality Metrics
- **Extraction confidence:** Track LLM confidence scores
- **Missing data:** Monitor NULL percentages in financial metrics  
- **Processing delays:** Alert on filing backlog

### Performance Metrics
- **Query latency:** 95th percentile response times
- **Cache hit rates:** Redis cache effectiveness
- **Vector search quality:** Relevance scores and user feedback