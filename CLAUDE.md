# YodaBuffett - AI-Powered Investment Research Platform

**🚀 For complete architecture details, see [CLAUDE-MASTER.md](./CLAUDE-MASTER.md)**

## Quick Context
Extensible platform for institutional-grade financial research. AI-assisted analysis, predictive modeling, and advanced backtesting capabilities.

## Architecture at a Glance
- **Microservices**: API Gateway, Research, Data Ingestion, Strategy, Prediction, User services
- **Stack**: Python (AI/ML), TypeScript/Node.js (APIs), PostgreSQL + Vector DB
- **AI**: Research acceleration, data analysis, ensemble predictions

## Current Focus
Building extensible platform for any financial analysis, not just specific features.

## Current Development Status
✅ **AI-Powered Report Analysis Platform**
- Successfully extracts & analyzes SEC filings + international reports
- Cost: ~$0.004 per document | Processing: 3-5 seconds
- Foundation learnings: [mvp1-report-analysis/MVP1_LEARNINGS.md](./mvp1-report-analysis/MVP1_LEARNINGS.md)

🚀 **Nordic Financial Data Platform** - PRODUCTION READY
- Complete Swedish market document collection (47,931 documents)
- **SMART PRIORITIZATION**: Annual & quarterly reports first (30,342 priority documents)
- **INTELLIGENT RETRY SYSTEM**: Case-insensitive matching + suffix pattern testing
- **ORGANIZED STORAGE**: `data/companies/SE/{A-Z}/{Company}/{Year}/{Type}/`
- Automated historical ingestion and PDF download batch processors
- Real-time calendar events and financial document monitoring
- RSS monitoring, document downloads, scheduled orchestration
- Management CLI, API integration, and operational monitoring

📄 **Document Processing Pipeline** - PRODUCTION READY
- **47,931 PDFs Catalogued**: Complete text extraction infrastructure with pause/resume
- **1,827 Documents Extracted**: Text extraction complete with PostgreSQL storage
- **Robust State Management**: Independent processing state tracking with batch control
- **Priority-Based Processing**: Annual reports (Priority 1) → Quarterly (Priority 2) → Other documents
- **Multi-Market Architecture**: Regional partitioning ready for Nordic, Europe, North America, Asia
- **Content Analysis**: Detects images, tables, scanned content for ML readiness
- **Performance**: 2-5 seconds per document, ~2GB storage for all extracted text
- **Documentation**: [docs/features/document-processing.md](./docs/features/document-processing.md)

🧠 **Section-Based Financial Intelligence** - PRODUCTION READY
- **Intelligent Financial Section Parsing**: Nordic language support for complete financial statements
- **CID Artifact Filtering**: Automatically skips documents with >1% CID artifacts for quality control
- **Production Performance**: ~1 second per document, ~40-70 meaningful sections per document
- **Multi-Provider Embeddings**: OpenAI, Cohere, local models - same sections, different providers (ready for testing)
- **85% Cost Reduction**: Intelligent section boundaries vs mechanical chunks
- **Independent Validation**: Test section quality before spending on embeddings
- **Complete Pipeline**: `cli_section_chunking.py` → `cli_multi_embeddings.py` → advanced analysis
- **Current Status**: 50 documents processed with 2,039 sections created successfully

🧠 **Vector Embedding System** - PRODUCTION ACTIVE (Legacy)
- **OpenAI Integration**: Real embeddings using `openai/text-embedding-3-small` model
- **PostgreSQL + pgvector**: 1536-dimensional vectors stored with semantic search capability
- **20 Documents Embedded**: Testing completed, production pipeline validated
- **Cost Efficient**: ~$0.026 per 1,000 documents, estimated $47 for full corpus
- **Provider Flexible**: Architecture supports OpenAI, Claude, local models with explicit tracking
- **Performance**: ~1 second per document including API calls and database storage

🤖 **Complete Daily Automation System** - PRODUCTION ACTIVE (macOS LaunchAgents)
- **Full Document Intelligence Pipeline**: Discovery → Download → Extract → Embed → Analyze (fully automated)
- **6-Stage Daily Schedule**: 
  - 3:00 AM (market data) → 7:00 AM (documents) → 9:00 AM (documents) → 10:00 AM (PDFs) → 11:00 AM (processing) → 12:00 PM (anomaly detection)
- **Complete Text Processing**: PDF download, text extraction, vector embeddings, section analysis
- **Temporal Anomaly Detection**: Automatic pattern analysis at noon daily
- **Event-Driven Intelligence**: Only processes companies with upcoming financial events
- **Calendar Integration**: Targets earnings, reports, AGMs, dividends automatically  
- **Performance Optimized**: 50x fewer companies processed (0-50 vs 1600+)
- **Zero Manual Intervention**: Native macOS scheduling with comprehensive logging
- **Ready for Analysis**: Documents are fully processed and searchable within hours of publication
- **Historical Catch-Up**: Comprehensive catch-up system for missed periods

📈 **Market Data Integration** - PRODUCTION ACTIVE
- **787 Companies**: Complete Nordic market coverage with Yahoo Finance integration  
- **Automated Daily Updates**: Native macOS scheduling runs at 3:00 AM daily
- **Incremental Collection**: Only fetches recent data (last 7 days) for efficiency
- **Provider Agnostic**: Architecture supports multiple data sources (Yahoo, Alpha Vantage, etc.)
- **Technical Indicators**: RSI, moving averages, volume analysis
- **Historical Backfill**: Complete price history for all tracked companies
- **Database Integration**: Seamless storage with document intelligence system

💰 **Historical Fundamentals System** - PRODUCTION COMPLETE ⭐ RICH DATASET
- **370 Companies**: Complete fundamental data coverage from Yahoo Finance
- **325,400 Daily Records**: 4+ years of historical fundamental ratios (2021-2025)
- **Complete Financial Statements**: Income statements, balance sheets, cash flows for 370 companies
- **Daily Calculated Metrics**: P/E, P/B, P/S, EV/EBITDA, debt ratios, current ratios
- **Automated Collection**: Daily fundamentals worker with full backfill system
- **3,810 Financial Statements**: Quarterly and annual data from Yahoo Finance
- **Valuation Ready**: Powers multi-method fundamental value strategies and backtesting
- **Documentation**: [docs/features/fundamentals-data-integration.md](./docs/features/fundamentals-data-integration.md)

🌍 **Multi-Market Worker System** - PRODUCTION DEPLOYED  
- **Specialized Workers**: Swedish, Norwegian, Danish, Finnish market ingestors
- **Event-Driven Architecture**: Calendar-targeted collection with smart scheduling
- **Unified Management**: Web dashboard + REST API for all Nordic markets
- **Docker Orchestration**: 12+ specialized workers with health monitoring
- **Market Configurations**: Complete setup for all Nordic countries
- **Professional Operations**: Production monitoring, logging, and error handling
- **Scalable Foundation**: Easy expansion to additional markets and worker types

🧠 **Hierarchical Embedding Architecture** - PRODUCTION READY ⭐ DUAL-LEVEL INTELLIGENCE
- **Document-Level Embeddings**: Macro patterns and overall communication shifts
- **Section-Level Embeddings**: Micro analysis and specific topic changes  
- **Unified Search**: Query across both granularities simultaneously
- **Three Embedding Methods**: Hierarchical (weighted sections), full text, section summary
- **Production Tools**: Complete CLI interface for generation, analysis, and monitoring
- **Temporal Anomaly Detection**: Both macro (document) and micro (section) level detection

🧠 **Temporal Anomaly Detection** - VALIDATED AT BOTH LEVELS ⭐ PROMISING EDGE
- **CONCEPT PROVEN**: Successfully detected real financial events in historical testing
- **AAK 2020-2021**: Balance sheet anomaly detected → Major asset/debt spike
- **AcadeMedia 2017-2018**: Risk factor changes → Swedish schooling law changes  
- **AddLife 2018-2019**: Income statement anomaly → 40% revenue growth
- Company-specific pattern baselines using local embeddings (FREE)
- Document and section-level analysis with 11,000+ embeddings
- Automated anomaly thresholds with similarity scoring
- Real-time deviation detection from historical communication patterns

📈 **Historical Market Data System** - PRODUCTION READY
- **Comprehensive Data Ingestion**: 787 Nordic companies with up to 20+ years of historical data each
- **Maximum Coverage**: 500,000+ to 1,000,000+ price points total for temporal anomaly validation
- **Swedish Ticker Intelligence**: Automated mapping using authoritative company-list.json with fuzzy matching
- **Robust Error Handling**: Categorized failure tracking with targeted recovery procedures
- **Provider-Agnostic Architecture**: Yahoo Finance integration with Bloomberg/Reuters expansion ready
- **Database Integration**: Seamless integration with company_master table and foreign key handling
- **Documentation**: [docs/market_data_ingestion.md](./backend/docs/market_data_ingestion.md)

🧠 **Advanced Analytics & Intelligence** - EXPANDING
- Vector-based semantic search across all financial documents
- Hidden connection discovery between companies and markets  
- Predictive modeling and systemic risk analysis
- Cross-company pattern detection and market intelligence

🤖 **Technical Analysis ML System** - PRODUCTION READY ⭐ COMPLETE ML PIPELINE
- **Time-Aware KNN Predictions**: Pre-computed neighbors with no look-ahead bias
- **Plugin-Based Indicators**: RSI, SMA, EMA, Bollinger Bands, MACD, Volume analysis
- **ML Strategy Framework**: Flexible architecture for technical/fundamental/ensemble strategies  
- **Realistic Backtesting**: Transaction costs, position sizing, risk management constraints
- **Production Database**: 729 labels across Nordic stocks, pre-computed neighbor tables
- **Interpretable Results**: See exactly which historical patterns drive each prediction
- **Documentation**: [docs/features/technical-analysis-ml.md](./docs/features/technical-analysis-ml.md)

💼 **Realistic Portfolio Simulator** - PRODUCTION READY ⭐ PORTFOLIO MANAGEMENT
- **Position Sizing**: 20% portfolio allocation per trade with 5 concurrent position maximum
- **Risk Management**: Transaction costs (0.2% total), quality stock filtering, cash management
- **Arbitration System**: Priority scoring for simultaneous signals, portfolio constraint enforcement
- **Realistic Execution**: Next-day open entry, fixed holding periods, actual market data
- **Performance Tracking**: Win rate, portfolio returns, transaction cost analysis
- **Comprehensive Testing**: Validated against EMA 10 strategy showing -7.3% vs +509% unrealistic returns
- **Documentation**: [docs/features/portfolio-management.md](./docs/features/portfolio-management.md)

## Documentation Structure

| Need This? | Go Here |
|------------|---------|
| **High-level overview** | [CLAUDE-MASTER.md](./CLAUDE-MASTER.md) |
| **Development roadmap** | [docs/roadmap/README.md](./docs/roadmap/README.md) |
| **Production progress** | [docs/roadmap/README.md](./docs/roadmap/README.md) |
| **Market data ingestion system** | [docs/market_data_ingestion.md](./backend/docs/market_data_ingestion.md) |
| **Technical analysis ML system** | [docs/features/technical-analysis-ml.md](./docs/features/technical-analysis-ml.md) |
| **Portfolio management & realistic backtesting** | [docs/features/portfolio-management.md](./docs/features/portfolio-management.md) |
| **Advanced analytics concepts** | [docs/features/advanced-analytics.md](./docs/features/advanced-analytics.md) |
| **Temporal anomaly detection** | [docs/features/temporal-anomaly-detection.md](./docs/features/temporal-anomaly-detection.md) |
| **Database/Data design** | [docs/architecture/data-architecture.md](./docs/architecture/data-architecture.md) |
| **Architecture rules** | [docs/development/principles.md](./docs/development/principles.md) |
| **System flexibility** | [docs/architecture/system-flexibility.md](./docs/architecture/system-flexibility.md) |
| **Known limitations** | [docs/architecture/limitations.md](./docs/architecture/limitations.md) |
| **Adding features** | [docs/development/extensibility.md](./docs/development/extensibility.md) |
| **Running the system** | [docs/operations/human-operator-guide.md](./docs/operations/human-operator-guide.md) |
| **Event-driven workers setup** | [docs/workers/setup-guide.md](./docs/workers/setup-guide.md) |
| **Worker operations & monitoring** | [backend/workers/CLAUDE.md](./backend/workers/CLAUDE.md) |
| **Financial terms** | [glossary.md](./glossary.md) |
| **Service-specific work** | `backend/[service]/CLAUDE.md` |
| **Nordic ingestion service** | [backend/README.md](./backend/README.md) |
| **Architecture decisions** | [docs/architecture/ARCHITECTURE_DECISIONS.md](./docs/architecture/ARCHITECTURE_DECISIONS.md) |
| **Production components added** | [docs/PRODUCTION_COMPONENTS_ADDED.md](./docs/PRODUCTION_COMPONENTS_ADDED.md) |
| **Multi-market worker system** | [docs/MULTI_MARKET_WORKERS_ADDED.md](./docs/MULTI_MARKET_WORKERS_ADDED.md) |
| **Document processing pipeline** | [docs/features/document-processing.md](./docs/features/document-processing.md) |
| **Section-based embeddings** | [docs/features/section-based-embeddings.md](./docs/features/section-based-embeddings.md) |
| **Human operator guide** | [docs/operations/human-operator-guide.md](./docs/operations/human-operator-guide.md) |

## 🚀 **Quick Commands - Daily Automation**

### Production Daily Workers (macOS LaunchAgents)
```bash
# Check all daily automation workers
launchctl list | grep yodabuffett

# View recent automation logs
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-market-data-worker.log
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-document-worker-morning.log
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-pdf-download.log

# Test workers manually (dry run)
cd /Users/jdandemar/Documents/YodaBuffett/backend
python -m workers.daily_event_worker --dry-run
python -m workers.daily_market_data_worker --dry-run
```

### Manual Worker Execution
```bash
# Trigger workers manually for testing
launchctl start com.yodabuffett.daily-market-data-worker
launchctl start com.yodabuffett.daily-document-worker-morning  
launchctl start com.yodabuffett.daily-document-worker-late
launchctl start com.yodabuffett.daily-pdf-download

# Run workers directly (bypass scheduler)
cd /Users/jdandemar/Documents/YodaBuffett/backend
python -m workers.daily_event_worker
python -m workers.daily_market_data_worker
python pdf_download_batch.py --year 2025 --delay 15
```

### Historical Catch-Up
```bash
# Catch up on missed document collection periods
python historical_document_catchup.py --days-back 7

# Run comprehensive historical market data ingestion
python historical_market_data_batch.py

# Retry failed companies with smart suffix testing
python retry_failed_companies.py
```

### System Management
```bash
# Stop/disable all automated workers
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-*.plist

# Re-enable all automated workers  
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-*.plist

# Check downloaded files and growth
ls -la /Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/
du -sh /Users/jdandemar/Documents/YodaBuffett/backend/data/companies/
```

## 🚀 **Quick Commands - Temporal Anomaly Detection**

### Complete Pipeline (Production Ready)
```bash
# 1. Extract text from PDFs (if needed)
cd backend/
python reset_extraction_processing.py 1  # Reset stuck extractions
python domains/document_intelligence/cli_nordic_extraction.py extract 100000

# 2. Create smart financial sections
python domains/document_intelligence/cli_section_chunking.py status
python domains/document_intelligence/cli_section_chunking.py process 1000

# 3. Generate section embeddings (FREE, LOCAL)
python domains/document_intelligence/cli_multi_embeddings.py local setup
python domains/document_intelligence/cli_multi_embeddings.py local process 10000

# 4. Generate document embeddings (HIERARCHICAL)
python domains/document_intelligence/cli_document_embeddings.py local setup
python domains/document_intelligence/cli_document_embeddings.py local process --count 1000 --method hierarchical

# 5. Run dual-level temporal anomaly detection (CORE EDGE)
python test_temporal_patterns.py                    # Section-level anomalies
python test_document_temporal_patterns.py           # Document-level anomalies
python test_unified_embedding_search.py             # Unified search across levels
python test_embedding_quality.py
```

### Quality Validation & Debugging
```bash
# Check embedding quality
python test_embedding_quality.py
python debug_embeddings.py

# Clean any dummy embeddings
python count_dummy_embeddings.py
python clean_dummy_embeddings.py

# Investigate specific anomalies
python investigate_embeddings.py
```

## 🚨 **Quick Commands - Temporal Anomaly Analysis (NEW)**

### Analyze Existing Embeddings Without Storage
```bash
# Latest anomalies by date (default - most recent communication changes)
cd backend/
python3 analyze_existing_embeddings.py --days 500

# Highest-scoring anomalies (most dramatic changes)
python3 analyze_existing_embeddings.py --days 500 --sort score

# Company-specific temporal analysis
python3 analyze_existing_embeddings.py --company "AAK" --days 500
python3 analyze_existing_embeddings.py --company "Volvo" --days 1000

# Check available data ranges first
python3 check_document_dates.py
python3 check_embeddings_schema.py
```

## 🤖 **Quick Commands - Technical Analysis ML System (NEW)**

### Setup and Database Creation
```bash
cd backend/

# Create all technical analysis tables
python3 create_ta_tables.py

# Check table status
python3 -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    for table in ['ml_models', 'ml_labels', 'knn_neighbors', 'strategies']:
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
        print(f'{table}: {count} rows')
    await conn.close()
asyncio.run(check())
"
```

### Generate Training Data
```bash
# Create ML labels from RSI patterns and future returns
python3 create_rsi_labels.py
# Output: ~729 labels across 10 Nordic stocks with 1d/5d/10d returns

# Build time-aware KNN neighbors (no look-ahead bias)  
python3 build_knn_neighbors.py
# Output: Pre-computed neighbors for fast predictions
```

### Run Backtests & Portfolio Simulation
```bash
# Full KNN strategy backtest with realistic constraints
python3 backtest_knn_strategy.py
# Output: Performance metrics, Sharpe ratio, max drawdown, win rate

# Test simple RSI strategy
python3 test_rsi_strategy.py
# Output: Signal generation and indicator calculations

# REALISTIC PORTFOLIO SIMULATION (NEW)
python3 realistic_portfolio_simulator.py
# Output: Proper position sizing, portfolio returns, transaction costs

# Multi-horizon indicator testing (comprehensive analysis)
python3 multi_horizon_indicator_tester.py
# Output: Fibonacci timeframe analysis, pattern matching across horizons

# Isolated indicator testing (individual predictor analysis)
python3 isolated_indicator_tester.py
# Output: Pure KNN performance per indicator, 100+ companies

# Adaptive exit strategy testing (dynamic holding periods)
python3 isolated_indicator_adaptive_exit.py
# Output: KNN-based entry and exit timing optimization
```

### Manual Analysis and Testing
```bash
# Check available training data
python3 -c "
import asyncio, asyncpg, json
async def check_labels():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    rows = await conn.fetch('SELECT metadata->>\"symbol\" as symbol, COUNT(*) as labels FROM ml_labels GROUP BY 1 ORDER BY 2 DESC')
    for row in rows:
        print(f'{row[\"symbol\"]}: {row[\"labels\"]} labels')
    await conn.close()
asyncio.run(check_labels())
"

# Check KNN neighbors status
python3 -c "
import asyncio, asyncpg
async def check_neighbors():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    count = await conn.fetchval('SELECT COUNT(*) FROM knn_neighbors')
    avg_neighbors = await conn.fetchval('SELECT AVG(jsonb_array_length(neighbors)) FROM knn_neighbors')
    print(f'KNN neighbor sets: {count}')
    print(f'Average neighbors per set: {avg_neighbors:.1f}')
    await conn.close()
asyncio.run(check_neighbors())
"

# Test individual predictions
python3 -c "
# Example: Query specific KNN prediction
import asyncio, asyncpg, json, hashlib
async def test_prediction():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    symbol = 'ERIC-B'
    company_id = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    row = await conn.fetchrow('SELECT * FROM knn_neighbors WHERE company_id = \$1 LIMIT 1', company_id)
    if row:
        neighbors = json.loads(row['neighbors'])
        print(f'Sample prediction for {symbol}:')
        print(f'Date: {row[\"prediction_date\"]}')
        print(f'Neighbors: {len(neighbors)}')
        print(f'Top neighbor: {neighbors[0][\"date\"]} (distance: {neighbors[0][\"distance\"]:.3f})')
    await conn.close()
asyncio.run(test_prediction())
"
```

### Add New Indicators
```bash
# Template for adding custom indicators
cat > services/technical_analysis/indicators/custom.py << 'EOF'
from .base import TechnicalIndicator, IndicatorResult, DataType
import pandas as pd

class MyIndicator(TechnicalIndicator):
    def __init__(self, period: int = 14):
        super().__init__(
            name=f"my_indicator_{period}",
            description=f"Custom indicator ({period}-period)",
            parameters={"period": period}
        )
        self.period = period
    
    async def calculate(self, company_id, market_data, start_date, end_date, timeframe):
        # Your calculation logic here
        values = {}  # {date: value}
        return IndicatorResult(values, {"period": self.period})

# Register it
from services.technical_analysis.indicators.base import indicator_registry
indicator_registry.register(MyIndicator())
EOF

echo "New indicator template created in services/technical_analysis/indicators/custom.py"
```

### Understanding Anomaly Output
- 🚨 **Significant** (Score ≥ 0.7): Major communication pattern shifts
- ⚠️ **Moderate** (Score 0.5-0.7): Notable changes worth investigating
- ℹ️ **Minor** (Score 0.3-0.5): Small variations, likely normal evolution

### Common Analysis Patterns
```bash
# Pre-market surprise check
python3 analyze_existing_embeddings.py --days 30 --sort date

# Quarterly earnings pattern analysis
python3 analyze_existing_embeddings.py --company "Ericsson" --days 365

# Market-wide communication shifts
python3 analyze_existing_embeddings.py --days 90 --sort date

# Historical anomaly research
python3 analyze_existing_embeddings.py --days 2000 --sort score
```

### Monitoring & Diagnostics
```bash
# Check pipeline status
python domains/document_intelligence/cli_nordic_extraction.py status
python domains/document_intelligence/cli_section_chunking.py status
python domains/document_intelligence/cli_multi_embeddings.py local status
python domains/document_intelligence/cli_document_embeddings.py local status

# Diagnostic tools
python check_embedding_tables.py
python diagnose_extraction_issue.py
```

## Quick Commands

### Docker-First Production (RECOMMENDED)
```bash
# DAILY WORKERS SETUP (BOTH DOCUMENT AND MARKET DATA)
cd backend
python3 setup_daily_workers.py                  # Interactive setup and management

# DAILY EVENT WORKER (PRODUCTION - AUTOMATIC AT 6:00 AM)
cd backend/docker
docker-compose up daily-event-scheduler -d      # Start document scheduler
docker ps | grep yodabuffett-daily-scheduler    # Check if scheduler running  
docker logs yodabuffett-daily-scheduler --tail 20 # View scheduler activity
curl http://localhost:8085/health               # Check scheduler health

# DAILY MARKET DATA WORKER (PRODUCTION - AUTOMATIC AT 6:30 AM)
docker-compose up daily-market-data-scheduler -d # Start market data scheduler
docker ps | grep yodabuffett-daily-market-data   # Check if scheduler running
docker logs yodabuffett-daily-market-data --tail 20 # View market data activity  
curl http://localhost:8086/health                # Check market data health

# DOCUMENT HISTORICAL CATCH-UP
cd backend
python3 historical_document_catchup.py --days-back 30  # Catch up last 30 days
python3 historical_document_catchup.py --start-date 2024-11-01 --end-date 2024-12-01

# MARKET DATA MANUAL OPERATIONS
python3 workers/daily_market_data_worker.py --run-now    # Run market data update now
python3 workers/daily_market_data_worker.py --dry-run    # Check what would be updated
```

### Direct Script Commands (Development)
```bash
# Historical document collection (production batch processor)
cd backend/
python3 historical_ingestion_batch.py

# PRIORITY PDF downloads - Annual & Quarterly Reports ONLY (DEFAULT)
python3 pdf_download_batch.py --year 2025 --delay 10

# Download ALL document types (press releases, governance, etc.)
python3 pdf_download_batch.py --year 2025 --all-types --delay 10

# Smart retry system for failed companies with suffix testing
python3 retry_failed_companies.py

# Slow background PDF downloads (1 per minute, ultra-respectful)
python3 pdf_download_batch.py --year 2025 --delay 60

# Analyze results
python3 analyze_ingestion_results.py
python3 analyze_download_results.py

# DOCUMENT PROCESSING PIPELINE (Production Ready)
# Activate virtual environment first
source venv/bin/activate

# Catalog all 47,931 PDFs without processing them
PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py discover

# Process documents in controllable batches with pause/resume
PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py process 100

# SECTION-BASED EMBEDDINGS (Intelligent Financial Analysis)
# Phase 1: Section Chunking (Rule-Based, Free) - PRODUCTION READY
python domains/document_intelligence/cli_section_chunking.py setup
python domains/document_intelligence/cli_section_chunking.py status
python domains/document_intelligence/cli_section_chunking.py test Volvo
python domains/document_intelligence/cli_section_chunking.py process 50
python domains/document_intelligence/cli_section_chunking.py inspect Volvo

# Phase 2: Section Embeddings (AI-Based, Flexible) - READY FOR TESTING
python domains/document_intelligence/cli_multi_embeddings.py local setup
python domains/document_intelligence/cli_multi_embeddings.py local status
python domains/document_intelligence/cli_multi_embeddings.py local process 1000

# Phase 3: Document Embeddings (Hierarchical Architecture) - PRODUCTION READY
python domains/document_intelligence/cli_document_embeddings.py local setup
python domains/document_intelligence/cli_document_embeddings.py local status
python domains/document_intelligence/cli_document_embeddings.py local process --count 500 --method hierarchical

# Analysis Tools
python domains/document_intelligence/cli_document_embeddings.py local similar --company "Volvo" --year 2023
python domains/document_intelligence/cli_document_embeddings.py local cluster --clusters 15
python domains/document_intelligence/cli_document_embeddings.py local outliers --company "AAK" --threshold 5.0

# Check processing status anytime
PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py status

# MULTI-MARKET WORKER SYSTEM (Production Monitoring)
# Individual worker testing
cd backend/
python3 -m workers.ingestors.swedish_document_ingestor --dry-run
python3 -m workers.ingestors.norwegian_document_ingestor --calendar

# Docker deployment (full production system)
cd docker/
docker-compose --profile production up swedish-document-ingestor
docker-compose --profile ingestors up    # All Nordic markets
docker-compose --profile management up   # Web dashboard + API

# Management interfaces
# Web Dashboard: http://localhost:8090/dashboard
# REST API: http://localhost:8091/
# CLI Access: docker exec -it yodabuffett-worker-cli bash

# Schedule & monitoring
python3 scripts/manage_workers.py schedule --days 7          # Preview 7-day schedule
python3 scripts/manage_workers.py analyze --days 30          # Analyze performance
python3 scripts/manage_workers.py health-check               # System diagnostics

# Docker operations
python3 scripts/manage_workers.py docker --action start      # Start all services
python3 scripts/manage_workers.py docker --action status     # Check status
python3 scripts/manage_workers.py docker --action logs --service daily-worker

# Legacy Nordic service commands
python main.py
python scripts/manage_nordic.py setup
python scripts/manage_nordic.py run-collection
```

## Key Principles
1. API-first design with contracts
2. Pure functional core
3. Hexagonal architecture
4. Immutable data
5. Explicit dependencies
6. Living documentation (always current)

**For detailed context, always check CLAUDE-MASTER.md first!**