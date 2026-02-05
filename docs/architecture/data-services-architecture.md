# Data Services Architecture

> **NOTE: This describes planned/aspirational architecture. See [docs/operations/human-operator-guide.md](../operations/human-operator-guide.md) for the actual current infrastructure.**

## Overview
YodaBuffett uses a **service-separated, multi-source data architecture** designed for institutional-grade reliability, flexibility, and performance. Each data type flows through specialized services optimized for their specific requirements.

## Data Service Types

### 1. Document Processing Service
**Purpose**: Handle static documents and reports
**Sources**: 
- Nordic PDFs (current: 47K documents)
- SEC filings (EDGAR API)
- Web scraping (company websites, news sites)
- Manual uploads (user documents, research reports)
- OCR processing (scanned documents)

**Characteristics**:
- Batch processing oriented
- Focus on text extraction and semantic analysis
- Slower update frequency (daily/weekly)
- Large document storage requirements

**Database Storage**:
- **PostgreSQL**: Document metadata, company mappings
- **Vector DB**: Document embeddings for semantic search
- **File Storage**: Original PDFs, processed text

### 2. Market Data Service
**Purpose**: Real-time and historical price/volume data
**Sources**:
- Bloomberg API (primary)
- Reuters/Refinitiv (secondary) 
- Yahoo Finance (backup/validation)
- Alpha Vantage (alternative)
- Exchange direct feeds (when available)

**Characteristics**:
- Real-time streaming data
- High-frequency updates (sub-second to minute)
- Critical uptime requirements
- Multiple source validation

**Database Storage**:
- **TimescaleDB**: High-frequency time-series data
- **Redis**: Real-time caching and streaming
- **PostgreSQL**: End-of-day summaries, corporate actions

**Multi-Source Reliability Features**:
```sql
-- Example: Price validation across sources
CREATE TABLE market_data_sources (
    symbol VARCHAR(20),
    source VARCHAR(50),
    price DECIMAL(10,4),
    volume BIGINT,
    timestamp TIMESTAMPTZ,
    quality_score FLOAT, -- 0-1 based on source reliability
    is_primary BOOLEAN
);

-- Cross-source validation
CREATE VIEW price_consensus AS
SELECT 
    symbol,
    timestamp,
    AVG(price) as consensus_price,
    STDDEV(price) as price_variance,
    COUNT(*) as source_count,
    MAX(CASE WHEN is_primary THEN source END) as primary_source
FROM market_data_sources 
WHERE timestamp > NOW() - INTERVAL '1 hour'
GROUP BY symbol, timestamp;
```

### 3. Fundamental Data Service  
**Purpose**: Financial statements, ratios, corporate information
**Sources**:
- S&P Capital IQ (institutional)
- Morningstar Direct (research)
- Company direct (annual reports, SEC filings)
- Manual extraction (from document service)

**Characteristics**:
- Quarterly/annual update cycle
- Complex data validation requirements
- Historical data completeness critical

**Database Storage**:
- **PostgreSQL**: Standardized financial statements
- **ML Database**: Pre-computed ratios, features for modeling

### 4. News & Sentiment Service
**Purpose**: Real-time news, press releases, social sentiment
**Sources**:
- Reuters/Bloomberg news feeds
- Company press releases  
- Social media APIs (Twitter, Reddit)
- News aggregators

**Characteristics**:
- Real-time streaming
- Natural language processing required
- High volume, varying quality

**Database Storage**:
- **PostgreSQL**: News articles, metadata
- **Vector DB**: News embeddings for semantic search
- **Redis**: Real-time sentiment scores

## Multi-Source Data Reliability

### Validation Pipeline
```
Raw Data → Quality Scoring → Cross-Source Validation → Consensus Building → Storage
```

### Quality Scoring Metrics
1. **Source Reliability Score**: Historical accuracy of each data provider
2. **Timeliness Score**: How fresh/delayed the data is
3. **Completeness Score**: Missing data points vs. expected coverage
4. **Consistency Score**: Agreement with other sources

### Automatic Failover Logic
```python
def get_market_data(symbol, timestamp):
    sources = [
        ('bloomberg', 0.95),  # (source, reliability_score)
        ('reuters', 0.90),
        ('yahoo', 0.75)
    ]
    
    for source, min_quality in sources:
        data = fetch_from_source(source, symbol, timestamp)
        if data and data.quality_score >= min_quality:
            return data
    
    raise DataUnavailableError(f"No reliable data for {symbol}")
```

### Data Quality Monitoring
- **Real-time alerts** when sources disagree significantly
- **Historical accuracy tracking** to adjust reliability scores
- **Coverage monitoring** to detect missing data
- **Audit trails** for compliance and debugging

## Service Communication

### Event-Driven Architecture
```
Document Service → Document Processed Event → Analytics Engine
Market Data Service → Price Update Event → Real-time Dashboard
News Service → Breaking News Event → Alert System
```

### API Contracts
Each service exposes standardized REST APIs:
- `/health` - Service health and data freshness
- `/data/{symbol}` - Core data retrieval
- `/validate/{data_id}` - Data quality validation
- `/sources` - Available data sources and status

## Performance & Reliability

### Service-Level Objectives (SLOs)
- **Document Service**: 99.5% uptime, <2 hour processing lag
- **Market Data Service**: 99.9% uptime, <100ms latency  
- **Fundamental Data Service**: 99.0% uptime, <24 hour update lag
- **News Service**: 99.8% uptime, <5 minute news delay

### Monitoring & Alerting
- Source availability monitoring
- Cross-source variance alerts
- Data freshness validation
- Quality score degradation detection

## Scalability Considerations

### Horizontal Scaling
- **Document Service**: Scale processing workers based on queue depth
- **Market Data Service**: Partition by symbol/market for parallel processing
- **Database Layer**: Read replicas and sharding as needed

### Cost Optimization  
- **Tiered Storage**: Hot (Redis) → Warm (PostgreSQL) → Cold (S3/Archive)
- **Data Source Optimization**: Use expensive sources only when necessary
- **Caching Strategy**: Aggressive caching for expensive API calls

This architecture ensures institutional-grade data reliability while maintaining the flexibility to add new sources and adapt to changing requirements.