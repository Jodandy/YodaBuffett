# Domain: Market Data

> **NOTE: This domain is partially implemented. What EXISTS: Yahoo Finance integration for historical price data (1,606 companies, 500K+ price points), daily automated market data updates, historical fundamentals collection (370 companies, 325K+ daily records), and technical indicator calculation. What does NOT exist: Bloomberg API, Reuters/Refinitiv, real-time feeds, DataValidator, QualityScorer, or multi-source cross-validation. Redis and TimescaleDB are not used -- the project uses plain PostgreSQL. Test coverage claims are fabricated.**

## AI Quick Start (Cold Start Context)
Manages historical market data from Yahoo Finance for Nordic markets.
Handles daily price updates, fundamentals collection, and historical backfill.

**Key AI Request Patterns**: "market data", "historical prices", "Yahoo Finance", "fundamentals", "daily updates"

**Start Files**: `services/historical_data.py`, `workers/daily_market_data_worker.py`, `historical_market_data_batch.py`

## When to Work Here
- User asks about price data, market feeds, or historical information
- Issues with data quality, missing prices, or feed reliability
- Requests to add new data sources or markets
- Problems with data validation or cross-source verification

---

## Current Implementation (AI-Maintained)
*Last updated: 2025-01-12 by AI Assistant*

### Business Purpose
Provides reliable, multi-source validated market data that serves as the foundation for analytics and user-facing products. Ensures data quality through redundant sources and real-time validation to maintain institutional-grade accuracy standards.

### Key Capabilities (Implemented)
- **Historical Data Management**: Comprehensive time-series data from Yahoo Finance (1,606 companies, 500K+ price points)
- **Daily Automated Updates**: macOS LaunchAgent runs at 3:00 AM for incremental data collection
- **Historical Fundamentals**: 370 companies with 325K+ daily fundamental ratio records (P/E, P/B, P/S, EV/EBITDA)
- **Financial Statements**: 3,810 quarterly and annual statements from Yahoo Finance
- **Nordic Market Specialization**: Deep coverage of Swedish, Norwegian, Danish, Finnish markets
- **Technical Indicators**: RSI, moving averages, volume analysis calculated from price data

### Key Capabilities (Planned - NOT IMPLEMENTED)
- **Real-time Data Feeds**: Live price and volume streams from multiple providers
- **Multi-source Validation**: Cross-validation between multiple data providers
- **Data Quality Assurance**: Automated outlier detection and reliability scoring

### Architecture Overview
```
Yahoo Finance → Data Ingestion → Storage → Technical Analysis
      ↓             ↓              ↓            ↓
  yfinance API   historical_ingestor → PostgreSQL → indicator_plugins
                 daily_worker          company_master
                 fundamentals_worker   market_data_daily
```

### Services in Production
- `HistoricalDataIngestor`: Comprehensive historical data ingestion from Yahoo Finance with technical indicators
- `YahooProvider`: Provider-agnostic interface for Yahoo Finance market data (domains/market_data/services/)
- `CompanyIngestionTracker`: Advanced tracking and error categorization for large-scale ingestion operations
- `DailyMarketDataWorker`: Automated daily price updates via macOS LaunchAgent
- `FundamentalsCollector`: Historical fundamental ratios and financial statement collection

### Services Planned (NOT YET IMPLEMENTED)
- `RealTimeFeedManager`: Manages connections to multiple data providers with failover
- `DataValidator`: Cross-source validation and outlier detection algorithms
- `QualityScorer`: Real-time data quality assessment and provider reliability tracking
- `NordicMarketSpecialist`: Specialized handling for Nordic market peculiarities

### Core Models
- `MarketDataPoint`: Individual price/volume observation with source and quality metadata
- `DataSource`: Configuration and status for external data providers
- `QualityMetrics`: Data quality scores and reliability statistics per source
- `MarketSession`: Trading session information and market status
- `CompanyListing`: Company information and symbol mappings across markets

### API Endpoints (Planned - NOT YET IMPLEMENTED)
- `GET /market-data/real-time/{symbol}`: Current price and volume data
- `GET /market-data/historical/{symbol}`: Historical price series with date range
- `GET /market-data/quality/{symbol}`: Data quality metrics and source reliability
- `POST /market-data/validate`: Cross-validate data points across sources
- `GET /market-data/sources`: Available data sources and their current status

### Performance Characteristics (Measured - implemented services only)
- **Historical Ingestion**: 1,606 companies with up to 20+ years of data each
- **Daily Updates**: Incremental collection (last 7 days) running at 3:00 AM daily
- **Fundamentals**: 370 companies, 325K+ daily records, 3,810 financial statements
- **Data Coverage**: 500K+ to 1M+ price points across Nordic markets

### Performance Targets (Aspirational - NOT MEASURED, services not implemented)
- **Real-time Latency**: <100ms for price updates from primary sources
- **Data Validation**: <50ms per data point cross-validation
- **Source Failover**: <30 seconds automatic failover to secondary sources

### Dependencies
- **Yahoo Finance (yfinance)**: Primary and currently only data source for prices and fundamentals
- **PostgreSQL**: Standard relational database for all market data storage (NOT TimescaleDB, NOT Redis)
- **Document Intelligence Domain**: Company symbol validation and enrichment

### Cross-Domain Integration
- **→ Analytics Domain**: Provides price data for correlation and risk analysis
- **→ Document Intelligence**: Validates extracted financial metrics against market data
- **← User Management**: API rate limiting and subscription-based access control
- **← Shared Database**: Company information and symbol mappings

### Testing Coverage
- **No automated test suite exists** - Previous "89% coverage" claim was fabricated
- **Manual validation**: Historical ingestion validated against Yahoo Finance data
- **Production testing**: Daily worker runs validated through log monitoring
- **Unit Tests**: Not yet implemented (planned)

### Recent Changes (AI-Generated Log)
- **2025-01-12**: Initial domain structure created with comprehensive documentation
- **2025-12-01**: Added comprehensive historical data ingestion system for 1,606 Nordic companies
  - Created production-ready ingestion scripts with error tracking and categorization
  - Implemented maximum historical data collection (up to 20+ years per company)
  - Added Swedish ticker mapping system using company-list.json for accurate symbol resolution
  - Built advanced progress tracking with priority-based processing (high-document companies first)
  - Established foreign key constraint handling for seamless database integration

---

## Common Patterns and Examples

### Historical Data Ingestion (IMPLEMENTED)
```bash
# Run historical market data batch ingestion
cd backend/
python3 historical_market_data_batch.py

# Run daily market data update manually
python3 workers/daily_market_data_worker.py --run-now

# Dry run to check what would be updated
python3 workers/daily_market_data_worker.py --dry-run
```

### Historical Data Query Pattern (IMPLEMENTED)
```python
# Retrieve historical data from PostgreSQL
import asyncpg
conn = await asyncpg.connect('postgresql://...')
data = await conn.fetch('''
    SELECT date, open, high, low, close, volume
    FROM market_data_daily
    WHERE company_id = $1 AND date BETWEEN $2 AND $3
    ORDER BY date
''', company_id, start_date, end_date)
```

### Real-time Data Access (PLANNED - NOT IMPLEMENTED)
```python
# This service does not exist yet
feed_manager = RealTimeFeedManager()
price_data = feed_manager.get_current_price("AAPL", validate=True)
```

---

## Data Source Configuration

### Active Sources
- **Yahoo Finance (yfinance)**:
  - Currently the ONLY data source in production
  - Rate limit: 2000 requests/hour (free tier)
  - Coverage: Major markets including Nordic, historical + delayed
  - Cost: Free
  - Used for: Daily prices, historical backfill, fundamentals, financial statements

### Nordic Specialized Sources (Active)
- **MFN.se Integration**: Swedish market document collection (via Nordic ingestion workers)

### Potential Future Sources (NOT INTEGRATED)
- **Bloomberg API**: Expensive, not currently justified for project scope
- **Reuters/Refinitiv**: Not integrated
- **Nasdaq Nordic APIs**: Direct exchange feeds for SE/NO/DK/FI markets (not integrated)

---

## AI Maintenance Instructions

### Auto-Update Triggers  
Update this file immediately when:
- ✅ New data sources added or removed
- ✅ API endpoints created, modified, or removed
- ✅ Performance characteristics change significantly
- ✅ Data quality metrics or validation rules change
- ✅ New markets or instruments supported
- ✅ Source reliability or costs change substantially

### Update Templates

**New Data Source:**
```markdown
- **[Source Name]**:
  - Rate limit: [requests per timeframe]
  - Coverage: [markets and data types]
  - Cost: [monthly cost and pricing model]
```

**Performance Change:**
```markdown
- [Service/Operation]: <[new_time] for [specific_scenario] (was [old_time])
```

**New API Endpoint:**
```markdown
- `[METHOD] [endpoint_path]`: [Description of what endpoint provides]
```

### AI Update Checklist
Before finalizing work in this domain:
- [ ] Added any new services to "Services in Production" section
- [ ] Updated performance characteristics if they changed  
- [ ] Added new API endpoints to the endpoint list
- [ ] Updated data source configurations if they changed
- [ ] Added entry to "Recent Changes" log with date and description
- [ ] Updated testing coverage statistics
- [ ] Verified cross-domain integration documentation is current
- [ ] Updated data source costs and rate limits if changed

### Cross-Reference Maintenance
When modifying services in this domain, check if documentation needs updates in:
- `ARCHITECTURE_MAP.md` (if new data sources or performance changes)
- Analytics domain documentation (if data structure or quality metrics change)
- Document intelligence domain documentation (if validation requirements change)