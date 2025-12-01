# YodaBuffett - Human Operator Guide

## Overview
Everything a human operator needs to run, manage, and maintain the YodaBuffett system.

## Quick Start Commands

### System Startup (Full Stack)
```bash
# Start everything
docker-compose up -d

# View all services status
docker-compose ps

# View logs for all services
docker-compose logs -f

# Stop everything
docker-compose down
```

### Development Mode
```bash
# Backend only (for frontend development)
docker-compose up -d database redis vector-db
cd backend && python -m uvicorn main:app --reload --port 8000

# Frontend only (for backend development)  
cd frontend && npm run dev

# Single service development
docker-compose up -d database redis
cd backend/research-service && python main.py
```

### MVP 1 Specific (Report Analysis)
```bash
# Start MVP 1 environment
cd mvp1-report-analysis
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt
python app.py

# Process a document
curl -X POST -F "file=@sample-10k.pdf" http://localhost:5000/analyze
```

## Environment Configuration

### Required Environment Variables

#### Core System (.env)
```bash
# Database
DATABASE_URL=postgresql://yoda:buffett123@localhost:5432/yodabuffett
REDIS_URL=redis://localhost:6379

# AI Services
OPENAI_API_KEY=sk-...  # Get from OpenAI dashboard
ANTHROPIC_API_KEY=sk-ant-...  # Get from Anthropic console

# Vector Database
PINECONE_API_KEY=...  # Get from Pinecone dashboard
PINECONE_ENVIRONMENT=us-east-1-aws

# Security
JWT_SECRET=your-super-secret-jwt-key-here
ENCRYPTION_KEY=32-byte-encryption-key-here

# External APIs
SEC_API_KEY=...  # For enhanced SEC data (optional)
ALPHA_VANTAGE_KEY=...  # For market data (future)
```

#### Development Specific (.env.development)
```bash
# Debugging
DEBUG=true
LOG_LEVEL=DEBUG

# Local services
POSTGRES_USER=yoda
POSTGRES_PASSWORD=buffett123
POSTGRES_DB=yodabuffett

# Redis
REDIS_PASSWORD=""  # No password for local

# File Storage (local)
STORAGE_TYPE=local
STORAGE_PATH=./data/uploads
```

#### Production Specific (.env.production)
```bash
# Security (DO NOT COMMIT THESE)
DATABASE_URL=postgresql://prod_user:STRONG_PASSWORD@prod-db:5432/yodabuffett
REDIS_URL=redis://:STRONG_PASSWORD@prod-redis:6379

# Cloud Storage
STORAGE_TYPE=s3
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=yodabuffett-production
```

## Service Management

### Database Operations
```bash
# Connect to PostgreSQL
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett

# Run migrations
cd backend && python -m alembic upgrade head

# Reset database (CAUTION: Deletes all data)
cd backend && python -m alembic downgrade base
docker-compose down -v  # Removes volumes too
docker-compose up -d database

# Backup database
pg_dump postgresql://yoda:buffett123@localhost:5432/yodabuffett > backup.sql

# Restore database
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett < backup.sql
```

### Redis Operations
```bash
# Connect to Redis
redis-cli -h localhost -p 6379

# Clear all cache
redis-cli FLUSHALL

# Monitor Redis activity
redis-cli MONITOR

# Check memory usage
redis-cli INFO memory
```

### Vector Database (Pinecone)
```bash
# List indexes
curl -H "Api-Key: YOUR_API_KEY" https://api.pinecone.io/indexes

# Check index stats
curl -H "Api-Key: YOUR_API_KEY" https://api.pinecone.io/indexes/yodabuffett/stats
```

## API Keys & Credentials Management

### Development Phase (MVP/POC)
**Location**: Store in `.env` files (NOT committed to git)

#### OpenAI API Key
- **Where to get**: https://platform.openai.com/api-keys
- **Cost monitoring**: https://platform.openai.com/usage
- **Usage limits**: Set monthly spend limits in OpenAI dashboard

#### Anthropic API Key  
- **Where to get**: https://console.anthropic.com/
- **Cost monitoring**: Check console for usage
- **Rate limits**: Monitor in console

#### Pinecone API Key
- **Where to get**: https://app.pinecone.io/
- **Free tier limits**: 1 index, 5M vectors
- **Monitoring**: Dashboard shows usage

### Production Phase (Future)
**Location**: Use cloud secret management
- AWS Secrets Manager
- Azure Key Vault  
- Google Secret Manager
- HashiCorp Vault

## Helper Scripts & Tools

### Data Management Scripts
```bash
# Download sample SEC filings
./scripts/download-sample-filings.sh

# Parse and clean sample documents
python scripts/parse-sample-docs.py

# Generate test embeddings
python scripts/generate-embeddings.py
```

### Development Utilities
```bash
# Run all tests
./scripts/run-tests.sh

# Code formatting
./scripts/format-code.sh

# Lint checking
./scripts/lint-check.sh

# Generate API documentation
./scripts/generate-docs.sh
```

### Nordic Ingestion Scripts

#### Historical Document Ingestion
**Purpose**: Systematically collect financial documents from ALL Swedish companies

```bash
# Run complete historical ingestion (ALL Swedish companies)
cd backend
python3 historical_ingestion_batch.py

# What it does:
# - Loads ALL Swedish companies from nordic_companies database table
# - Collects up to 480 historical documents per company
# - 5-minute timeout per company (safety measure)
# - 5-second pause between companies (respectful to MFN.se)
# - Comprehensive success/failure tracking with detailed reasons
```

**Features**:
- **Resume capability**: Can continue from interrupted runs
- **Smart mapping**: Converts company names to MFN URL slugs automatically  
- **Failure categorization**: Tracks specific failure reasons (timeout, not found, storage error, etc.)
- **Progress persistence**: Saves results after each company
- **Graceful handling**: Moves to next company when MFN doesn't have data

**Expected Output**:
- **Scale**: 50,000+ documents from hundreds of Swedish companies
- **Coverage**: 4-5 years of historical data per company
- **Files**: `historical_ingestion_YYYYMMDD_HHMMSS.json` + `.log`

#### PDF Document Downloads
**Purpose**: Download catalogued PDF documents with intelligent prioritization

```bash
# Download HIGH-PRIORITY annual and quarterly reports only (DEFAULT)
python3 pdf_download_batch.py --year 2025 --delay 10

# Download all document types (press releases, governance, etc.)
python3 pdf_download_batch.py --year 2025 --all-types --delay 10

# Focus on specific company reports
python3 pdf_download_batch.py --year 2025 --company "Volvo" --delay 10

# Download reports from all years (comprehensive)
python3 pdf_download_batch.py --delay 10

# Super slow mode (1 PDF per minute, ultra-respectful)
python3 pdf_download_batch.py --year 2025 --delay 60
```

**Features**:
- **Smart Prioritization**: Defaults to annual/quarterly reports only (most valuable documents)
- **Resume Capability**: Can continue from interrupted downloads
- **File Organization**: `data/companies/SE/{first_letter}/{company}/{year}/{type}/filename.pdf`
- **PDF Validation**: Checks file integrity and PDF magic bytes
- **Deduplication**: Skips existing files automatically
- **Progress Tracking**: Saves results after each download
- **Respectful Rate Limiting**: 10-second delays between downloads (configurable)

**Expected Output**:
- **Reports Only**: ~3,463 high-priority documents (annual/quarterly reports)
- **All Types**: ~14,473 total documents (includes press releases, governance, etc.)
- **Storage**: Organized by `data/companies/SE/A/Company_Name/2025/quarterly_report/`
- **Files**: `pdf_download_YYYYMMDD_HHMMSS.json` + `.log`

#### Smart Company Retry System
**Purpose**: Retry failed companies with intelligent slug detection

```bash
# Retry companies that had 0 documents with smart suffix testing
python3 retry_failed_companies.py

# Will test variants like:
# - volvo-group (original)
# - volvo-group-holding
# - volvo-group-ab
# - etc.
```

**Features**:
- **Case-Insensitive Matching**: Handles "2Curex" vs "2cureX" automatically
- **Suffix Pattern Testing**: Tries -holding, -group, -ab, -corp variations
- **Chunked Processing**: Processes in batches to prevent crashes
- **Database Integration**: Actually saves documents to database
- **Smart Recovery**: Distinguishes between URL issues vs processing errors

### Daily Event Worker (Production Automation)

#### Docker-Based Automated Daily Collection
**Purpose**: Event-driven daily Swedish financial data collection running on Docker schedule

```bash
# Check Docker scheduler status
docker ps | grep yodabuffett-daily-scheduler

# View Docker scheduler logs
docker logs yodabuffett-daily-scheduler --tail 50

# View worker execution results (inside container)
docker exec yodabuffett-daily-scheduler ls -la /app/data/daily_worker_*.json

# Test immediately (dry-run)
docker exec yodabuffett-daily-scheduler python -m workers.daily_event_worker --dry-run

# Test for specific date
docker exec yodabuffett-daily-scheduler python -m workers.daily_event_worker --date 2025-09-01 --dry-run

# Run immediately for today
docker exec yodabuffett-daily-scheduler python -m workers.daily_event_worker
```

**What it does:**
- **Runs automatically at 6:00 AM daily** using Docker container with built-in scheduler
- **Event-driven targeting**: Only processes companies with upcoming financial events (earnings, reports, AGMs)
- **Intelligent scheduling**: Scrapes day-of or day-after events for optimal document availability
- **Batch-optimized**: Uses database query optimizations (10-100x faster than individual queries)
- **Smart company resolution**: Automatic slug resolution with centralized mappings
- **Progress tracking**: Saves detailed results to JSON files after each execution
- **Portable deployment**: Same behavior on any Docker-capable server

**Expected Performance:**
- **Daily targets**: 0-50 companies (instead of 1600+) based on calendar events
- **Processing time**: 2-3 seconds per company with batch optimizations
- **Success rate**: 85%+ (event-driven timing improves document availability)
- **Resource usage**: 256MB memory limit, 0.25 CPU cores

**Docker Service Management:**
```bash
# Start the daily scheduler
docker-compose up daily-event-scheduler -d

# Stop the daily scheduler
docker-compose stop daily-event-scheduler

# Restart the daily scheduler
docker-compose restart daily-event-scheduler

# View health check
curl http://localhost:8085/health

# Check container resources
docker stats yodabuffett-daily-scheduler

# Update scheduler configuration
# Edit docker-compose.yml environment variables
# Then: docker-compose up daily-event-scheduler -d --force-recreate
```

**Files Generated:**
- **Results**: `/app/data/daily_worker_YYYYMMDD_HHMMSS.json` (inside container)
- **Logs**: Available via `docker logs yodabuffett-daily-scheduler`
- **Health**: Available at `http://localhost:8085/health`

**Migration from macOS LaunchAgent:**
If you previously used the macOS LaunchAgent setup, disable it:
```bash
# Stop old macOS scheduler
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-scheduler.plist

# Remove old plist file (optional)
rm ~/Library/LaunchAgents/com.yodabuffett.daily-scheduler.plist

# Now use Docker approach exclusively
```

#### Document Processing Pipeline (Production Ready)
**Purpose**: Robust PDF text extraction with pause/resume capabilities

```bash
# Activate virtual environment (required)
source venv/bin/activate

# PHASE 1: Document Discovery (catalog all PDFs without processing)
PYTHONPATH=/Users/jdandemar/Documents/YodaBuffett/backend python3 domains/document_intelligence/cli_stateful.py discover

# PHASE 2: Process documents in controllable batches
PYTHONPATH=/Users/jdandemar/Documents/YodaBuffett/backend python3 domains/document_intelligence/cli_stateful.py process 50

# Check processing status anytime
PYTHONPATH=/Users/jdandemar/Documents/YodaBuffett/backend python3 domains/document_intelligence/cli_stateful.py status

# Discover limited number for testing
PYTHONPATH=/Users/jdandemar/Documents/YodaBuffett/backend python3 domains/document_intelligence/cli_stateful.py discover 100
```

#### Section-Based Embeddings Pipeline (Intelligent Financial Analysis)
**Purpose**: Transform extracted documents into intelligent financial sections with embeddings

**PHASE 1: Section Chunking (Rule-Based, Free) - PRODUCTION READY**
```bash
# Setup sections database
python domains/document_intelligence/cli_section_chunking.py setup

# Check current chunking status
python domains/document_intelligence/cli_section_chunking.py status

# Test chunking on single document (validation)
python domains/document_intelligence/cli_section_chunking.py test Volvo

# Process batch of documents into financial sections
python domains/document_intelligence/cli_section_chunking.py process 5 Volvo

# Process larger batches for production
python domains/document_intelligence/cli_section_chunking.py process 50

# Inspect quality of created sections
python domains/document_intelligence/cli_section_chunking.py inspect Volvo
```

**PHASE 2: Multi-Provider Embeddings (AI-Based, Flexible Cost) - PRODUCTION READY**

**LOCAL MODEL EMBEDDINGS (FREE, PRODUCTION-TESTED)** ⭐ RECOMMENDED
```bash
# Setup local embeddings database (uses sentence-transformers)
python domains/document_intelligence/cli_multi_embeddings.py local setup

# Check local embedding status
python domains/document_intelligence/cli_multi_embeddings.py local status

# Process sections with local embeddings (small batch test)
python domains/document_intelligence/cli_multi_embeddings.py local process 50

# Process larger batches for production (recommended)
python domains/document_intelligence/cli_multi_embeddings.py local process 1000

# Scale up to all sections (11K+ sections)
python domains/document_intelligence/cli_multi_embeddings.py local process 10000
```

**OPENAI EMBEDDINGS (PAID, HIGH QUALITY)**
```bash
# Setup OpenAI embeddings database
python domains/document_intelligence/cli_multi_embeddings.py openai setup

# Check OpenAI embedding status
python domains/document_intelligence/cli_multi_embeddings.py openai status

# Process sections with OpenAI embeddings (small batch)
python domains/document_intelligence/cli_multi_embeddings.py openai process 20 Volvo

# Process larger batches for production
python domains/document_intelligence/cli_multi_embeddings.py openai process 100

# Compare all embedding providers
python domains/document_intelligence/cli_multi_embeddings.py openai compare
```

**ALTERNATIVE PROVIDERS**
```bash
# Cohere embeddings (placeholder)
python domains/document_intelligence/cli_multi_embeddings.py cohere setup
python domains/document_intelligence/cli_multi_embeddings.py cohere process 20 Volvo
```

## 🚨 **PHASE 3: Document-Level Embeddings (HIERARCHICAL ANALYSIS)** ⭐ NEW CAPABILITY

**PURPOSE**: Complement section-level embeddings with document-level analysis for macro patterns

**PHASE 3A: Document Embedding Setup**
```bash
# Setup document embeddings database (hierarchical embedding architecture)
python domains/document_intelligence/cli_document_embeddings.py local setup

# Check document embedding status
python domains/document_intelligence/cli_document_embeddings.py local status

# Process documents using hierarchical method (weights section embeddings intelligently)
python domains/document_intelligence/cli_document_embeddings.py local process --count 50 --method hierarchical

# Process larger batches for production
python domains/document_intelligence/cli_document_embeddings.py local process --count 500 --method hierarchical

# Alternative methods:
# Full text method (embed complete document)
python domains/document_intelligence/cli_document_embeddings.py local process --count 20 --method full_text

# Section summary method (embed key sections only)
python domains/document_intelligence/cli_document_embeddings.py local process --count 20 --method section_summary
```

**PHASE 3B: Document-Level Analysis (PRODUCTION VALIDATED)**
```bash
# Document-level temporal anomaly detection
python test_document_temporal_patterns.py

# Unified search across both document and section levels
python test_unified_embedding_search.py

# Find similar documents at company level
python domains/document_intelligence/cli_document_embeddings.py local similar --company "Volvo" --year 2023

# Cluster documents by communication patterns
python domains/document_intelligence/cli_document_embeddings.py local cluster --clusters 15

# Detect document outliers for specific company
python domains/document_intelligence/cli_document_embeddings.py local outliers --company "Volvo" --threshold 5.0
```

**CAPABILITIES ENABLED**:
- **Dual-Level Analysis**: Document-level for macro patterns + Section-level for micro patterns
- **Temporal Anomalies**: Detect overall communication shifts vs specific topic changes
- **Unified Search**: Search across both granularities simultaneously
- **Document Classification**: Cluster companies by communication style
- **Outlier Detection**: Find unusual documents in company history
- **Hierarchical Architecture**: Three embedding methods (hierarchical, full_text, section_summary)

## 🚨 **PHASE 4: Temporal Anomaly Detection (PRODUCTION VALIDATED)** ⭐ CORE EDGE

**VALIDATED RESULTS**: Successfully detected real financial events:
- AAK 2020-2021: Balance sheet anomaly → Major asset/debt spike  
- AcadeMedia 2017-2018: Risk factor changes → Swedish schooling law changes
- AddLife 2018-2019: Income statement anomaly → 40% revenue growth

**EMBEDDING QUALITY VALIDATION**
```bash
# Test embedding quality and similarity patterns
python test_embedding_quality.py

# Debug any embedding issues
python debug_embeddings.py

# Check for dummy embeddings that need regeneration
python count_dummy_embeddings.py
python clean_dummy_embeddings.py
```

**TEMPORAL ANOMALY DETECTION (BOTH LEVELS)**
```bash
# Run section-level temporal pattern analysis (core edge detection)
python test_temporal_patterns.py

# Run document-level temporal pattern analysis (macro changes)
python test_document_temporal_patterns.py

# Search through embeddings semantically (unified across levels)
python test_unified_embedding_search.py

# Legacy section-level search
python test_embedding_search.py

# Investigate specific anomalies
python investigate_embeddings.py
```

**PRODUCTION MONITORING**
```bash
# Check table relationships and data integrity
python check_embedding_tables.py
python check_table_columns.py

# Reset stuck processing states if needed
python reset_stuck_documents.py
python reset_extraction_processing.py

# Diagnose extraction pipeline issues
python diagnose_extraction_issue.py
```

**Legacy Embeddings (Mechanical Chunks)**
```bash
# Generate embeddings from 8K character chunks
python domains/document_intelligence/cli_embedding_generation.py status
python domains/document_intelligence/cli_embedding_generation.py process 3 Volvo
```

**Features**:

**Document Processing:**
- **47,931 PDFs catalogued**: Complete Swedish market document collection
- **Robust Pause/Resume**: Can interrupt processing with Ctrl+C and resume exactly where left off
- **Processing Priorities**: Annual reports (Priority 1), Quarterly (Priority 2), Press releases (Priority 7)
- **Independent State Tracking**: Uses `document_processing_state` table for reliable progress tracking
- **Batch Processing**: Process any number of documents (10, 50, 100) with full control
- **Content Analysis**: Detects images, tables, scanned content in PDFs
- **Multi-Market Ready**: Regional partitioning supports Nordic, Europe, North America, Asia expansion

**Section-Based Embeddings:**
- **Intelligent Chunking**: Replaces mechanical 8K chunks with complete financial sections
- **CID Artifact Filtering**: Automatically skips documents with >1% CID artifacts for quality control
- **Nordic Language Support**: Handles Swedish, Norwegian, Danish, Finnish financial reports  
- **Section Types**: Balance sheet, income statement, cash flow, equity, management discussion, strategy, risk factors
- **Provider Flexibility**: OpenAI, Cohere, local models - same sections, different embeddings
- **Cost Optimization**: ~85% reduction in chunks (10-15 vs 50+ per document)
- **Independent Validation**: Test chunking quality before spending on embeddings
- **Multi-Provider Comparison**: Generate embeddings with multiple providers for same sections

**Expected Performance**:

**Document Processing:**
- **Text Extraction**: 2-5 seconds per PDF document
- **Memory Usage**: Processes in chunks of 100 documents to prevent memory issues
- **Storage**: ~2GB for all extracted text from 47,931 documents
- **Completion Tracking**: Real-time progress with percentage complete
- **Error Handling**: Continues processing even if individual documents fail

**Section Chunking:**
- **Parsing Speed**: ~1 second per document (rule-based, no API calls)
- **Section Production**: ~40-70 meaningful sections per document (varied by document complexity)
- **Quality Filtering**: CID artifact detection prevents processing of corrupted/scanned documents
- **Storage**: ~200MB for all section metadata (excluding embeddings)
- **Quality**: 85%+ confidence scores for major financial statements
- **Current Status**: Successfully processed 50 documents with 2,039 sections created

**Embedding Generation:**
- **OpenAI Cost**: ~$0.00003 per section (~$0.0003 per document with 10 sections)
- **Processing Speed**: ~0.5 seconds per section (including API calls)
- **Storage**: ~50GB for all section embeddings (1536D vectors)
- **Provider Comparison**: Same sections can be embedded with multiple models

**Current Status (as of implementation)**:

**Document Processing:**
- **📄 Total documents**: 47,931 catalogued
- **🔍 Discovered**: 47,929 ready for processing
- **✅ Completed**: 2 (test runs)
- **📋 Priority 1**: 11,795 annual reports
- **📋 Priority 2**: 18,547 quarterly reports
- **🎯 Next**: Process high-priority documents first

**Section-Based Embeddings:**
- **🧩 Documents sectioned**: 0 (ready to start)
- **🤖 Section embeddings**: 0 (ready for generation)
- **🎯 Recommended flow**: 
  1. Section chunk 100 documents → validate quality
  2. Generate OpenAI embeddings for validated sections
  3. Scale up to full document collection

#### Analyze Ingestion Results
**Purpose**: Quick analysis of batch ingestion results

```bash
# Quick overview of latest ingestion
python3 analyze_ingestion_results.py

# Detailed failure analysis
python3 analyze_ingestion_results.py --failures

# Analyze PDF download results
python3 analyze_download_results.py
```

**Output**:
- Success/failure counts and rates
- Documents collected per company (top performers)
- Failure reasons grouped and categorized
- Processing time analysis
- Resume recommendations
- Download progress and file organization stats

#### Single Company Testing
**Purpose**: Test/debug individual companies

```bash
# Test single company (limit 5 documents)  
python3 test_mfn_collector.py

# Test with clean output (saves to file)
python3 test_output.py
```

#### Debug Tools

```bash
# Save MFN HTML page for manual inspection
python3 save_mfn_html_simple.py

# Debug storage issues specifically
python3 debug_storage.py
```

### Monitoring & Debugging
```bash
# Check system health
./scripts/health-check.sh

# View service logs
./scripts/view-logs.sh [service-name]

# Monitor API costs
python scripts/monitor-costs.py

# Performance profiling
python scripts/profile-performance.py
```

## File Locations & Data Storage

### Local Development Structure
```
YodaBuffett/
├── .env                    # Main environment config
├── .env.development       # Dev-specific config
├── data/
│   ├── uploads/           # User-uploaded files
│   ├── processed/         # Processed documents
│   ├── cache/            # File cache
│   ├── backups/          # Database backups
│   └── companies/        # Downloaded PDF documents (organized)
│       └── SE/           # Swedish companies
│           ├── A/        # Companies starting with A
│           │   ├── ABB_Ltd/
│           │   │   └── 2025/
│           │   │       ├── annual_report/
│           │   │       ├── quarterly_report/
│           │   │       ├── press_release/
│           │   │       └── governance/
│           │   └── AstraZeneca/
│           ├── B/        # Companies starting with B
│           └── H/        # Companies starting with H
├── logs/
│   ├── app.log           # Application logs
│   ├── error.log         # Error logs
│   └── access.log        # API access logs
├── backend/
│   ├── historical_ingestion_*.json    # Ingestion results
│   ├── pdf_download_*.json            # Download results
│   ├── retry_results_*.json           # Retry results
│   └── *.log                         # Batch process logs
└── scripts/              # Helper scripts
```

### Important Paths to Monitor
- **Log files**: `./logs/` (check for errors)
- **Upload directory**: `./data/uploads/` (monitor disk space)
- **Cache directory**: `./data/cache/` (can be cleared)
- **Database backups**: `./data/backups/` (ensure regular backups)

## Monitoring & Maintenance

### Daily Checks
- [ ] Check service status: `docker-compose ps`
- [ ] Review error logs: `tail -f logs/error.log`
- [ ] Monitor API costs: OpenAI/Anthropic dashboards
- [ ] Check disk space: `df -h`
- [ ] Check Nordic ingestion progress: `python3 analyze_ingestion_results.py`
- [ ] Monitor PDF download progress: `python3 analyze_download_results.py`
- [ ] **Check document processing status**: `PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py status`
- [ ] **Process daily batch** (if actively processing): `PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py process 100`
- [ ] **Check section chunking status**: `python domains/document_intelligence/cli_section_chunking.py status`
- [ ] **Check section embedding status**: `python domains/document_intelligence/cli_multi_embeddings.py local status`
- [ ] **Check document embedding status**: `python domains/document_intelligence/cli_document_embeddings.py local status`
- [ ] Review document collection rates from latest batch run
- [ ] Check `data/companies/` folder size and organization

### Weekly Tasks
- [ ] Backup database: `pg_dump > weekly_backup.sql`
- [ ] Clear old cache files: `find data/cache -mtime +7 -delete`
- [ ] Review system performance logs
- [ ] Update API usage tracking
- [ ] Run fresh historical ingestion for new/updated companies
- [ ] **Run focused PDF downloads**: `python3 pdf_download_batch.py --delay 10` (reports only)
- [ ] **Process large document batches**: `PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py process 500`
- [ ] **Monitor document processing progress**: Check completion rate and processing errors
- [ ] **Process section chunking batches**: `python domains/document_intelligence/cli_section_chunking.py process 100`
- [ ] **Generate section embeddings for new sections**: `python domains/document_intelligence/cli_multi_embeddings.py local process 500`
- [ ] **Generate document embeddings**: `python domains/document_intelligence/cli_document_embeddings.py local process 200 --method hierarchical`
- [ ] **Run document-level temporal anomaly analysis**: `python test_document_temporal_patterns.py`
- [ ] **Test unified search capabilities**: `python test_unified_embedding_search.py`
- [ ] **Validate section quality**: `python domains/document_intelligence/cli_section_chunking.py inspect` (random companies)
- [ ] **Compare embedding providers**: `python domains/document_intelligence/cli_multi_embeddings.py openai compare`
- [ ] Retry failed companies: `python3 retry_failed_companies.py` (10-20 companies)
- [ ] Analyze ingestion failure patterns and optimize slugs/mappings
- [ ] Archive old ingestion result files: `gzip historical_ingestion_*.json pdf_download_*.json`
- [ ] Verify PDF file integrity: spot check downloaded PDFs can be opened

### Monthly Tasks
- [ ] Review and rotate API keys (production)
- [ ] Analyze cost trends and optimize
- [ ] Update dependencies: `pip freeze > requirements.txt`
- [ ] Archive old log files

## Troubleshooting Common Issues

### Service Won't Start
```bash
# Check ports
netstat -tulpn | grep LISTEN

# Check docker logs
docker-compose logs [service-name]

# Rebuild containers
docker-compose down
docker-compose build --no-cache
docker-compose up
```

### Database Connection Issues
```bash
# Test connection
pg_isready -h localhost -p 5432

# Check database logs
docker-compose logs database

# Reset database connection
docker-compose restart database
```

### High API Costs
```bash
# Check recent usage
python scripts/analyze-api-usage.py

# Enable caching
redis-cli CONFIG SET maxmemory 256mb

# Review LLM prompt efficiency
python scripts/audit-prompts.py
```

### Out of Disk Space
```bash
# Check disk usage
du -sh data/

# Clear old files
find data/uploads -mtime +30 -delete
find logs -mtime +7 -delete

# Compress old backups
gzip data/backups/*.sql

# Compress old ingestion results
gzip historical_ingestion_*.json historical_ingestion_*.log
```

### Nordic Ingestion Issues

#### High Failure Rates
```bash
# Analyze failure patterns
python3 analyze_ingestion_results.py --failures

# SOLUTION: Use smart retry system
python3 retry_failed_companies.py

# This will automatically:
# 1. Test case-insensitive company name matching
# 2. Try suffix variants (-holding, -group, -ab, -corp)
# 3. Distinguish URL issues from processing errors
# 4. Save successful collections to database

# For manual debugging of specific company:
python3 test_mfn_collector.py  # Edit to target specific company
```

#### PDF Download Issues
```bash
# Check download progress
python3 analyze_download_results.py

# Resume interrupted downloads
python3 pdf_download_batch.py --year 2025 --delay 10

# Common solutions:
# 1. Check disk space: df -h
# 2. Verify file permissions in data/companies/
# 3. Test single company: --company "Company Name"
# 4. Slow down if rate limited: --delay 60

# Verify downloaded PDFs
find data/companies -name "*.pdf" -size 0 -delete  # Remove empty files
find data/companies -name "*.pdf" | head -10 | xargs file  # Check file types
```

#### Slow Processing/Timeouts
```bash
# Check for stuck companies
tail -f historical_ingestion_*.log | grep "Processing:"

# Increase timeout if needed (edit historical_ingestion_batch.py):
# self.company_timeout = 600  # 10 minutes

# Resume from timeouts
python3 historical_ingestion_batch.py  # Choose resume option
```

#### Database Storage Errors  
```bash
# Check database connectivity
python3 debug_storage.py

# Check nordic_companies table
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett
\d nordic_companies

# Restart ingestion with fresh database connection
```

#### Document Processing Issues
```bash
# Check processing status
PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py status

# Restart interrupted processing (resumes automatically)
PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py process 100

# Reset stuck processing status (if documents stuck in 'processing' state)
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett
UPDATE document_processing_state SET processing_status = 'discovered' 
WHERE processing_status = 'processing' AND last_attempt_at < NOW() - INTERVAL '1 hour';

# Clear failed documents to retry (after fixing underlying issue)
UPDATE document_processing_state SET processing_status = 'discovered', 
attempt_count = 0, last_error = NULL 
WHERE processing_status = 'failed';

# Check database tables exist
\d document_processing_state;
\d batch_processing_sessions;

# Verify virtual environment is activated
source venv/bin/activate
pip list | grep pydantic-settings

# Common solutions:
# 1. Always activate venv first: source venv/bin/activate  
# 2. Use correct PYTHONPATH: PYTHONPATH=backend python3 ...
# 3. Check database connectivity: psql postgresql://...
# 4. Monitor disk space for text storage: df -h
# 5. Process in smaller batches if memory issues: process 20
```

#### Section-Based Embeddings Issues

**Section Chunking Problems:**
```bash
# Check section chunking status
python domains/document_intelligence/cli_section_chunking.py status

# Test chunking on single document for debugging
python domains/document_intelligence/cli_section_chunking.py test Volvo

# Inspect sections created for quality validation
python domains/document_intelligence/cli_section_chunking.py inspect Volvo

# Check database tables
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett
\d document_sections;

# Common solutions:
# 1. Low section count: Check if document text extraction succeeded first
# 2. Poor section quality: Review parser confidence scores in inspect output
# 3. Over-segmentation: Parser already refined to avoid 556→15 section reduction
# 4. Missing financial statements: Nordic reports may use different headers
# 5. Database errors: Check PostgreSQL connectivity and table creation
```

**Multi-Provider Embedding Problems:**
```bash
# Check embedding status for specific provider
python domains/document_intelligence/cli_multi_embeddings.py openai status
python domains/document_intelligence/cli_multi_embeddings.py cohere status

# Compare all providers
python domains/document_intelligence/cli_multi_embeddings.py openai compare

# Test single provider setup
python domains/document_intelligence/cli_multi_embeddings.py openai setup

# Check API key configuration
echo $OPENAI_API_KEY | head -c 20  # Should show sk-proj-...
echo $COHERE_API_KEY | head -c 10   # Should show co-...

# Check database tables
psql postgresql://yoda:buffett123@localhost:5432/yodabuffett
\d section_embeddings;
SELECT embedding_model, COUNT(*) FROM section_embeddings GROUP BY embedding_model;

# Common solutions:
# 1. API key missing: Set OPENAI_API_KEY or COHERE_API_KEY environment variables
# 2. Vector dimension mismatch: Recreate embedding table with correct dimensions
# 3. Rate limiting: Increase delays between API calls
# 4. High costs: Switch to cohere or local provider for testing
# 5. No sections to embed: Run section chunking first
# 6. Duplicate embeddings: Constraint prevents re-embedding same sections
```

#### MFN.se Structure Changes
```bash
# Save current HTML structure
python3 save_mfn_html_simple.py

# Compare with expected structure in mfn_collector.py
# Look for changes in:
# - <div class="short-item compressible"> (document containers)
# - <span class="compressed-date"> (date extraction)  
# - <span class="compressed-title"> (title extraction)
# - <a class="attachment-icon"> (PDF links)
```

## Security Checklist

### Development Security
- [ ] `.env` files in `.gitignore`
- [ ] No hardcoded passwords in code
- [ ] Local HTTPS certificates for testing
- [ ] Regular dependency updates

### API Security
- [ ] Rate limiting enabled
- [ ] API key rotation schedule
- [ ] Input validation on all endpoints
- [ ] Error messages don't leak sensitive info

### Data Security
- [ ] Database passwords are strong
- [ ] File upload validation
- [ ] Regular security scans
- [ ] Access logs monitored

## Emergency Procedures

### System Down
1. Check service status: `docker-compose ps`
2. Review logs: `docker-compose logs`
3. Restart services: `docker-compose restart`
4. If database issue, restore from backup
5. Update incident log

### Data Loss
1. Stop all services immediately
2. Restore from most recent backup
3. Check backup integrity
4. Restart services
5. Verify data consistency

### Security Incident
1. Rotate all API keys immediately
2. Review access logs for suspicious activity
3. Change all passwords
4. Scan for malware
5. Document incident

## Contact Information & Resources

### External Services Support
- **OpenAI Support**: https://help.openai.com/
- **Anthropic Support**: https://support.anthropic.com/
- **Pinecone Support**: https://support.pinecone.io/

### Internal Resources
- **Architecture docs**: `docs/architecture/`
- **API documentation**: `docs/api/` (when available)
- **Troubleshooting**: `docs/operations/`
- **Development guide**: `docs/development/`

### Emergency Contacts
- **System Administrator**: [Your contact info]
- **Database Admin**: [Contact info]
- **Security Team**: [Contact info]

## External Service Credentials

### Email Accounts

#### Investor Relations Email
- **Email**: yodabuffett.ir@gmail.com
- **Password**: !BuffayTime3214
- **Purpose**: Subscribe to company IR newsletters and receive automated financial reports
- **Setup Notes**:
  - Enable "Less secure app access" or use App Password
  - Check inbox regularly for report notifications
  - Configure email parsing pipeline to extract PDFs