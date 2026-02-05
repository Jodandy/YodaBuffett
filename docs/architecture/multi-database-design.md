# Multi-Database Architecture Design

> **NOTE: This describes planned/aspirational architecture. See [docs/operations/human-operator-guide.md](../operations/human-operator-guide.md) for the actual current infrastructure.**

## Overview
YodaBuffett employs a **polyglot persistence strategy** where each database technology is optimized for specific data types and access patterns. This approach maximizes performance while maintaining data consistency across the platform.

## Database Specialization

### 1. PostgreSQL + TimescaleDB
**Primary Use Cases**:
- Core financial data and company information
- Time-series market data (prices, volumes, financial metrics)
- Relational data requiring ACID transactions
- Complex analytical queries across multiple entities

**Schema Examples**:
```sql
-- Companies and fundamental data
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE,
    name VARCHAR(200),
    market VARCHAR(10),
    sector VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Time-series financial metrics (TimescaleDB hypertable)
CREATE TABLE financial_metrics (
    company_id INT REFERENCES companies(id),
    metric_name VARCHAR(100),
    value DECIMAL(15,4),
    period_end DATE,
    report_type VARCHAR(20), -- 'quarterly', 'annual'
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('financial_metrics', 'timestamp');
```

**Performance Optimizations**:
- Partitioning by time for historical data
- Indexes on company_id + timestamp for fast lookups
- Materialized views for common aggregations

### 2. Vector Database (Pinecone/Weaviate/ChromaDB)
**Primary Use Cases**:
- Document embeddings for semantic search
- Similarity-based company/document recommendations
- Cross-document pattern detection
- Natural language query processing

**Data Structure**:
```python
# Document embedding storage
{
    "id": "doc_SE_AAK_2024_annual_report_page_5",
    "vector": [0.1, 0.3, -0.2, ...],  # 1536-dimensional embedding
    "metadata": {
        "company_id": 123,
        "document_type": "annual_report",
        "year": 2024,
        "page": 5,
        "section": "financial_highlights",
        "text_preview": "Revenue increased by 15% to SEK 2.1 billion..."
    }
}
```

**Query Examples**:
```python
# Semantic search across all documents
results = vector_db.query(
    vector=embed_text("companies discussing supply chain issues"),
    filter={"year": {"$gte": 2023}},
    top_k=50
)

# Find similar companies based on document content
similar = vector_db.query(
    vector=company_embedding,
    filter={"document_type": "annual_report"},
    top_k=10
)
```

### 3. ML Database (Specialized PostgreSQL)
**Primary Use Cases**:
- Pre-computed machine learning features
- Model outputs and predictions
- KNN distance tables for similarity analysis
- Ensemble model results

**Schema Examples**:
```sql
-- Pre-computed KNN distances for company similarity
CREATE TABLE company_similarities (
    company_a_id INT REFERENCES companies(id),
    company_b_id INT REFERENCES companies(id), 
    similarity_type VARCHAR(50), -- 'financial', 'textual', 'sector', 'combined'
    distance FLOAT,
    similarity_score FLOAT, -- normalized 0-1
    features_used TEXT[], -- array of features used in calculation
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (company_a_id, company_b_id, similarity_type)
);

-- ML feature store for model inputs
CREATE TABLE ml_features (
    company_id INT REFERENCES companies(id),
    feature_name VARCHAR(100),
    feature_value FLOAT,
    feature_category VARCHAR(50), -- 'financial', 'sentiment', 'technical'
    calculation_date DATE,
    model_version VARCHAR(20),
    PRIMARY KEY (company_id, feature_name, calculation_date)
);

-- Model predictions and confidence scores
CREATE TABLE model_predictions (
    id SERIAL PRIMARY KEY,
    company_id INT REFERENCES companies(id),
    model_name VARCHAR(100),
    prediction_type VARCHAR(50), -- 'price_target', 'earnings_surprise', 'risk_score'
    predicted_value FLOAT,
    confidence_score FLOAT,
    prediction_date DATE,
    target_date DATE, -- when prediction is for
    input_features JSONB, -- store feature values used
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Ensemble model results
CREATE TABLE ensemble_predictions (
    company_id INT REFERENCES companies(id),
    prediction_date DATE,
    prediction_type VARCHAR(50),
    weighted_average FLOAT,
    model_weights JSONB, -- {"model_1": 0.4, "model_2": 0.6}
    individual_predictions JSONB, -- store all model outputs
    confidence_interval FLOAT[2], -- [lower_bound, upper_bound]
    PRIMARY KEY (company_id, prediction_date, prediction_type)
);
```

**ML-Specific Optimizations**:
- Clustered indexes on company_id + calculation_date
- Partial indexes for recent predictions only
- BRIN indexes for time-based partitioning
- GiST indexes for similarity searches

### 4. Redis
**Primary Use Cases**:
- Real-time data caching
- Session management and user state
- Pub/sub for real-time notifications
- Rate limiting and API throttling
- Temporary computation results

**Data Structures**:
```python
# Real-time price caching
redis.hset("prices:AAPL", {
    "price": "150.25",
    "volume": "1250000", 
    "timestamp": "2025-01-12T10:30:00Z",
    "change": "+2.15"
})

# User session data
redis.setex("session:user_123", 3600, json.dumps({
    "user_id": 123,
    "permissions": ["read", "query"],
    "active_dashboards": ["portfolio", "watchlist"],
    "query_count": 47
}))

# Real-time notifications/alerts
redis.publish("alerts:user_123", {
    "type": "price_alert",
    "symbol": "AAPL", 
    "message": "AAPL reached target price of $150"
})

# API rate limiting
redis.incr("api_calls:user_123:minute")
redis.expire("api_calls:user_123:minute", 60)
```

## Cross-Database Data Flow

### Data Processing Pipeline
```
Raw Data → Primary Storage → Feature Engineering → ML Database → Vector Embeddings → Vector DB → API Layer
```

### Synchronization Strategy
1. **Event-Driven Updates**: Database changes trigger events for dependent systems
2. **Scheduled Jobs**: Batch updates for feature calculations and embeddings
3. **Change Data Capture (CDC)**: Real-time sync for critical data

### Example Data Flow
```python
# When new financial document is processed:
1. Extract text and save to PostgreSQL (documents table)
2. Generate embeddings and store in Vector DB
3. Extract financial metrics and update TimescaleDB
4. Calculate ML features and update ML Database
5. Update Redis cache for real-time access
6. Trigger similarity recalculation for related companies
```

## Data Consistency & Integrity

### Transaction Management
- **PostgreSQL**: ACID transactions for financial data integrity
- **Vector DB**: Eventually consistent with conflict resolution
- **Redis**: Atomic operations with expiration-based cleanup
- **Cross-DB**: Saga pattern for multi-database transactions

### Backup & Recovery Strategy
- **PostgreSQL**: WAL shipping + point-in-time recovery
- **Vector DB**: Snapshot backups + incremental updates  
- **Redis**: RDB snapshots + AOF for durability
- **Cross-validation**: Regular consistency checks between databases

## Performance Optimization

### Query Optimization
```sql
-- Example: Fast company similarity lookup
CREATE INDEX idx_company_similarities_lookup 
ON company_similarities (company_a_id, similarity_type, similarity_score DESC);

-- Example: Time-series queries with proper partitioning
CREATE INDEX idx_financial_metrics_time_company 
ON financial_metrics (company_id, timestamp DESC) 
WHERE timestamp > NOW() - INTERVAL '5 years';
```

### Caching Strategy
- **L1 (Redis)**: Sub-second response for frequent queries
- **L2 (PostgreSQL)**: Complex analytical queries with materialized views
- **L3 (Vector DB)**: Semantic similarity pre-computations

### Monitoring & Metrics
- Database-specific performance metrics
- Cross-database query latency tracking
- Data freshness monitoring
- Storage growth projection

This multi-database architecture provides the foundation for building sophisticated financial analytics while maintaining the performance and reliability required for institutional-grade applications.