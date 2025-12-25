# YodaBuffett - Human Operator Guide

## Overview
Everything a human operator needs to run, manage, and maintain the YodaBuffett system.

## Quick Start Commands

### Daily Automation Status (macOS LaunchAgents - ACTIVE)
```bash
# Check automation status
launchctl list | grep yodabuffett

# View current logs
tail -f backend/logs/daily-market-data-worker.log
tail -f backend/logs/daily-document-worker-morning.log  
tail -f backend/logs/daily-pdf-download.log
tail -f backend/logs/daily-document-pipeline.log

# Manual runs (for testing)
cd backend
python3 workers/daily_market_data_worker.py
python3 workers/daily_event_worker.py --dry-run
python3 pdf_download_batch.py --year=2025 --delay 10
python3 workers/daily_document_pipeline.py
```

### Daily Schedule (Automatic via LaunchAgents)
```
🌅 03:00 AM - Daily Market Data Worker
💰 03:30 AM - Daily Fundamentals Worker (NEW)
   └── Yahoo Finance fundamentals collection
   └── 100 symbols daily rotation
   └── P/E, P/B, ROE, dividend data
📄 07:00 AM - Document Discovery Worker (Morning)  
📄 09:00 AM - Document Discovery Worker (Late)
📥 10:00 AM - PDF Download Worker
🔄 11:00 AM - Document Processing Pipeline
   └── Text Extraction
   └── Vector Embeddings  
   └── Section Processing
🚨 12:00 PM - Temporal Anomaly Detection
   └── Document-level analysis
   └── Section-level analysis
   └── Notifications & alerts
```

### Fundamentals Data Management
```bash
# Historical fundamentals backfill (complete financial statements)
cd backend
python3 historical_fundamentals_backfill.py

# Daily fundamentals collection (runs automatically at 3:30 AM)
python3 -m workers.daily_fundamentals_worker --run-now

# Dry run to see what would be collected
python3 -m workers.daily_fundamentals_worker --dry-run

# Check fundamentals automation status
launchctl list | grep daily-fundamentals

# View recent automation logs
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-fundamentals-worker.log

# Check fundamentals database status
python3 -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    tables = [
        ('financial_statements', 'Financial statements'),
        ('balance_sheet_data', 'Balance sheet records'), 
        ('cash_flow_data', 'Cash flow records'),
        ('historical_fundamentals_daily', 'Historical daily metrics'),
        ('daily_fundamentals', 'Current daily fundamentals')
    ]
    for table, desc in tables:
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
        symbols = await conn.fetchval(f'SELECT COUNT(DISTINCT symbol) FROM {table}')
        print(f'{desc}: {count:,} records across {symbols} symbols')
    await conn.close()
asyncio.run(check())
"

# Test fundamental value strategy
python3 fundamental_value_strategy_enhanced.py
python3 fundamental_value_backtest.py

# Analyze fundamental changes for specific company
python3 -c "
import asyncio
from yahoo_fundamentals_daily_collector import YahooDailyFundamentalsCollector

async def analyze():
    collector = YahooDailyFundamentalsCollector()
    await collector.setup()
    
    changes = await collector.get_fundamentals_changes('VOLV-B', 30)
    print('Fundamental Changes (30 days) for VOLV-B:')
    for metric, data in changes.items():
        print(f'  {metric}: {data[\"earliest\"]:.2f} → {data[\"latest\"]:.2f} ({data[\"change_pct\"]:+.1f}%)')
        
    await collector.cleanup()

asyncio.run(analyze())
"
```

### Technical Analysis & Portfolio Management
```bash
# Run complete portfolio simulation with position sizing
cd backend
python3 realistic_portfolio_simulator.py

# Test individual indicators across many companies
python3 isolated_indicator_tester.py

# Multi-timeframe analysis using Fibonacci sequences
python3 multi_horizon_indicator_tester.py

# Adaptive exit strategy testing
python3 isolated_indicator_adaptive_exit.py

# Traditional KNN strategy backtest
python3 backtest_knn_strategy.py

# Check technical analysis database status
python3 -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    for table in ['ml_models', 'ml_labels', 'knn_neighbors']:
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
        print(f'{table}: {count} rows')
    await conn.close()
asyncio.run(check())
"
```

### View Anomaly Alerts
```bash
# View recent anomalies from database (if using automatic storage)
cd backend
python3 view_anomalies.py --days 7
python3 anomaly_cli.py stats

# Check anomaly detection logs
tail -f backend/logs/daily-anomaly-detection.log

# View latest notifications
ls -la backend/data/anomaly_notifications_*.txt
cat backend/data/anomaly_notifications_*.txt | head -50

# Run temporal anomaly analysis on existing embeddings (no database storage)
python3 analyze_existing_embeddings.py --days 500
python3 analyze_existing_embeddings.py --company "AAK" --days 500
```

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

### Daily Automation System (Production Active) ⭐

#### macOS LaunchAgent Automated Daily Pipeline
**Purpose**: Complete end-to-end daily financial data collection using native macOS scheduling

**🕒 Daily Schedule:**
- **3:00 AM** - Market Data Collection (all 787 companies)
- **7:00 AM** - Document Discovery (event-driven, metadata only)  
- **9:00 AM** - Document Discovery (catch stragglers)
- **10:00 AM** - PDF Download (actual files from discovered documents)

#### Quick Status Check
```bash
# Check all scheduled workers
launchctl list | grep yodabuffett

# View recent activity logs
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-market-data-worker.log
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-document-worker-morning.log
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-pdf-download.log

# Test workers manually (dry run)
cd /Users/jdandemar/Documents/YodaBuffett/backend
python -m workers.daily_event_worker --dry-run
python -m workers.daily_market_data_worker --dry-run
```

#### Manual Worker Execution
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

#### System Management
```bash
# Stop/disable all automated workers
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-document-worker-morning.plist
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-document-worker-late.plist
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-pdf-download.plist

# Re-enable all automated workers
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-document-worker-morning.plist
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-document-worker-late.plist
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-pdf-download.plist

# Check if workers are properly loaded
launchctl list | grep yodabuffett
```

**What Each Worker Does:**

**Market Data Worker (3:00 AM):**
- Updates stock prices for all companies using Yahoo Finance
- Calculates technical indicators (RSI, moving averages)
- Processes ~787 companies in 10-15 minutes
- Logs: `/Users/jdandemar/Documents/YodaBuffett/logs/daily-market-data-worker.log`

**Document Discovery Workers (7:00 AM & 9:00 AM):**
- Event-driven targeting: Only processes companies with scheduled financial events
- Scrapes MFN.se for new document metadata and PDF URLs
- Typical load: 0-50 companies per day (not all 1600+)
- Intelligent deduplication: Skips already processed documents
- Logs: `/Users/jdandemar/Documents/YodaBuffett/logs/daily-document-worker-*.log`

**PDF Download Worker (10:00 AM):**
- Downloads actual PDF files from URLs discovered by document workers
- Focuses on 2025 documents (most recent/relevant)
- 15-second delays between downloads (respectful rate limiting)
- Smart prioritization: Annual/quarterly reports first
- Organized storage: `data/companies/SE/A/CompanyName/2025/quarterly_report/`
- Logs: `/Users/jdandemar/Documents/YodaBuffett/logs/daily-pdf-download.log`

#### Expected Performance
- **Market Data**: ~787 companies, 10-15 minutes total
- **Document Discovery**: 0-50 companies based on events, 2-5 minutes total
- **PDF Downloads**: Variable based on discovered documents, respectful 15s delays
- **Storage Growth**: ~5-20 new PDFs per day (event-dependent)
- **Success Rate**: 85%+ (event-driven timing improves availability)

#### Configuration Files (LaunchAgent plists)
Located in `~/Library/LaunchAgents/`:
- `com.yodabuffett.daily-market-data-worker.plist` (3:00 AM)
- `com.yodabuffett.daily-document-worker-morning.plist` (7:00 AM)  
- `com.yodabuffett.daily-document-worker-late.plist` (9:00 AM)
- `com.yodabuffett.daily-pdf-download.plist` (10:00 AM)

#### Advantages over Docker Deployment
✅ **Native macOS integration** - No container networking issues  
✅ **Direct database access** - Uses existing PostgreSQL connection  
✅ **Simpler debugging** - Direct access to processes and logs  
✅ **Auto-restart on reboot** - macOS handles service persistence  
✅ **Lower resource overhead** - No virtualization layer  
✅ **Easier log monitoring** - Standard file system logs  

#### Migration to Cloud (Future)
Docker configurations remain available in `backend/docker/` for cloud deployment when ready. The same Python workers will run identically in containers.

## 📅 **Complete Daily Automation Management Guide** ⭐ PRODUCTION ACTIVE

### Overview of Automated Systems
The YodaBuffett platform runs **6 automated daily processes** via macOS LaunchAgents:

```
🌅 03:00 AM - Market Data Collection (787 companies, ~15 min)
📄 07:00 AM - Document Discovery Morning (event-driven, ~5 min)
📄 09:00 AM - Document Discovery Late (catch stragglers, ~5 min)
📥 10:00 AM - PDF Download (discovered documents, variable)
🔄 11:00 AM - Document Processing Pipeline (extract, embed, chunk)
🚨 12:00 PM - Temporal Anomaly Detection (pattern analysis)
```

### Quick Management Commands

#### Check System Status
```bash
# See all YodaBuffett automation services
launchctl list | grep yodabuffett

# Expected output (PID shows service is running):
# 12345  0  com.yodabuffett.daily-market-data-worker
# 23456  0  com.yodabuffett.daily-document-worker-morning
# 34567  0  com.yodabuffett.daily-document-worker-late
# 45678  0  com.yodabuffett.daily-pdf-download
# 56789  0  com.yodabuffett.daily-document-pipeline
# 67890  0  com.yodabuffett.daily-anomaly-detection
```

#### View Real-Time Logs
```bash
# Market data collection (runs at 3 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-market-data-worker.log

# Document discovery (runs at 7 AM and 9 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-document-worker-morning.log
tail -f ~/Documents/YodaBuffett/backend/logs/daily-document-worker-late.log

# PDF downloads (runs at 10 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-pdf-download.log

# Document processing pipeline (runs at 11 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-document-pipeline.log

# Anomaly detection (runs at 12 PM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-anomaly-detection.log

# View all automation logs together
tail -f ~/Documents/YodaBuffett/backend/logs/daily-*.log
```

#### Manual Control
```bash
# Force immediate run of any service
launchctl start com.yodabuffett.daily-market-data-worker
launchctl start com.yodabuffett.daily-document-worker-morning
launchctl start com.yodabuffett.daily-document-pipeline
launchctl start com.yodabuffett.daily-anomaly-detection

# Stop a service (won't run until next scheduled time)
launchctl stop com.yodabuffett.daily-market-data-worker

# Disable a service completely
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist

# Re-enable a disabled service
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
```

### Managing LaunchAgents

#### Fix Common Issues
```bash
# If services show errors or won't load, run the fix script
cd ~/Documents/YodaBuffett/backend
python3 fix_launchagents.py

# This script will:
# - Validate all plist files
# - Fix permissions
# - Reload services properly
# - Show status of each service
```

#### Modify Schedule Times
```bash
# Edit the plist file to change schedule
nano ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist

# Find the StartCalendarInterval section:
<key>StartCalendarInterval</key>
<dict>
    <key>Hour</key>
    <integer>3</integer>    <!-- Change this to new hour (0-23) -->
    <key>Minute</key>
    <integer>0</integer>    <!-- Change this to new minute (0-59) -->
</dict>

# After editing, reload the service:
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
```

### Monitor Daily Results

#### Check What Was Processed Today
```bash
# Market data updates
grep "✅ Completed processing" ~/Documents/YodaBuffett/backend/logs/daily-market-data-worker.log | tail -10

# Documents discovered
grep "new documents found" ~/Documents/YodaBuffett/backend/logs/daily-document-worker-*.log | tail -20

# PDFs downloaded
ls -la ~/Documents/YodaBuffett/backend/data/companies/SE/*/*/2025/*/*.pdf | wc -l

# Processing pipeline results
grep "documents processed successfully" ~/Documents/YodaBuffett/backend/logs/daily-document-pipeline.log | tail -5

# Anomalies detected
grep "anomalies found" ~/Documents/YodaBuffett/backend/logs/daily-anomaly-detection.log | tail -5
```

#### View Processing Statistics
```bash
# Check disk space usage growth
du -sh ~/Documents/YodaBuffett/backend/data/companies/

# Count total PDFs collected
find ~/Documents/YodaBuffett/backend/data/companies -name "*.pdf" | wc -l

# See latest downloaded files
find ~/Documents/YodaBuffett/backend/data/companies -name "*.pdf" -mtime -1 -ls

# Database statistics
cd ~/Documents/YodaBuffett/backend
python3 << EOF
from shared.database import AsyncSessionLocal
import asyncio
from sqlalchemy import text

async def check_stats():
    async with AsyncSessionLocal() as db:
        docs = await db.execute(text("SELECT COUNT(*) FROM nordic_documents"))
        print(f"Total documents: {docs.scalar():,}")
        
        embeddings = await db.execute(text("SELECT COUNT(*) FROM document_embeddings"))
        print(f"Total embeddings: {embeddings.scalar():,}")
        
        recent = await db.execute(text("""
            SELECT COUNT(*) FROM nordic_documents 
            WHERE created_at > CURRENT_DATE - INTERVAL '7 days'
        """))
        print(f"Documents added last 7 days: {recent.scalar():,}")

asyncio.run(check_stats())
EOF
```

### Troubleshooting Automation

#### Service Not Running
```bash
# Check if service is loaded
launchctl list | grep yodabuffett

# Check recent errors in system log
log show --predicate 'subsystem == "com.apple.launchd"' --last 1h | grep yodabuffett

# Check service-specific log for errors
tail -100 ~/Documents/YodaBuffett/backend/logs/daily-market-data-worker.log | grep ERROR

# Common fixes:
# 1. Run fix_launchagents.py
# 2. Check Python path in plist matches your system
# 3. Verify virtual environment exists
# 4. Check database connectivity
```

#### Service Running But Not Working
```bash
# Test the worker directly
cd ~/Documents/YodaBuffett/backend
source venv/bin/activate

# Test with dry run first
python3 -m workers.daily_market_data_worker --dry-run
python3 -m workers.daily_event_worker --dry-run
python3 workers/daily_document_pipeline.py --test

# Check for import errors
python3 -c "from workers.daily_market_data_worker import DailyMarketDataWorker"

# Verify environment variables
python3 -c "import os; print(os.environ.get('DATABASE_URL', 'NOT SET'))"
```

#### Reset and Restart Everything
```bash
# Complete reset of all automation
cd ~/Documents/YodaBuffett/backend

# 1. Stop all services
for service in daily-market-data-worker daily-document-worker-morning daily-document-worker-late daily-pdf-download daily-document-pipeline daily-anomaly-detection; do
    launchctl stop com.yodabuffett.$service
    launchctl unload ~/Library/LaunchAgents/com.yodabuffett.$service.plist 2>/dev/null
done

# 2. Clear logs (optional - backup first if needed)
mkdir -p logs/backup_$(date +%Y%m%d)
mv logs/daily-*.log logs/backup_$(date +%Y%m%d)/

# 3. Fix and reload all services
python3 fix_launchagents.py

# 4. Verify all services loaded
launchctl list | grep yodabuffett
```

### Best Practices

#### Daily Monitoring Routine
1. **Morning (8 AM)**: Check overnight market data collection
   ```bash
   tail -50 ~/Documents/YodaBuffett/backend/logs/daily-market-data-worker.log
   ```

2. **Mid-Morning (10 AM)**: Verify document discovery worked
   ```bash
   grep "Processing complete" ~/Documents/YodaBuffett/backend/logs/daily-document-worker-*.log | tail -5
   ```

3. **Afternoon (1 PM)**: Check full pipeline completion
   ```bash
   # Quick status of all services
   for log in ~/Documents/YodaBuffett/backend/logs/daily-*.log; do
       echo "=== $(basename $log) ==="
       tail -3 $log | grep -E "(Completed|ERROR|Failed)"
   done
   ```

#### Weekly Maintenance
```bash
# Archive old logs
cd ~/Documents/YodaBuffett/backend/logs
tar -czf archive_$(date +%Y%m%d).tar.gz daily-*.log
echo "" > daily-*.log  # Clear logs after archiving

# Check disk space
df -h ~/Documents/YodaBuffett/backend/data/

# Verify database growth
du -sh ~/Documents/YodaBuffett/backend/data/
```

### Advanced Configuration

#### Environment Variables
Each service reads from `~/.zshrc` or `~/.bash_profile`:
```bash
export DATABASE_URL="postgresql://yodabuffett:your_password@localhost:5432/yodabuffett"
export PYTHONPATH="/Users/jdandemar/Documents/YodaBuffett/backend"
export OPENAI_API_KEY="sk-..."  # For embeddings
```

#### Resource Limits
LaunchAgents respect system resources by default. To add limits:
```xml
<!-- Add to any plist file -->
<key>SoftResourceLimits</key>
<dict>
    <key>NumberOfFiles</key>
    <integer>1024</integer>
    <key>MemoryLock</key>
    <integer>67108864</integer>  <!-- 64MB -->
</dict>
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

## 🚨 **PHASE 5: Temporal Anomaly Analysis from Existing Embeddings** ⭐ NEW CAPABILITY

**PURPOSE**: Analyze temporal communication patterns from your 50,000+ existing embeddings without storing results

### Quick Analysis Commands
```bash
# Analyze latest anomalies by date (default sort)
cd backend/
python3 analyze_existing_embeddings.py --days 500

# Analyze highest-scoring anomalies 
python3 analyze_existing_embeddings.py --days 500 --sort score

# Analyze specific company patterns
python3 analyze_existing_embeddings.py --company "AAK" --days 500
python3 analyze_existing_embeddings.py --company "Volvo" --days 1000

# Check what data is available
python3 check_document_dates.py
python3 check_embeddings_schema.py
```

### Understanding the Output
The analyzer shows temporal anomalies sorted by your preference:

**Latest Anomalies Mode (--sort date):**
```
🕒 LATEST 10 ANOMALIES (Most Recent)
----------------------------------------------------------------------
 1. 🚨 2025-08-30 | AAK | Score: 0.78
    📄 Current:  Q2_2025_Interim_Report.pdf
    📄 Previous: 2025-05-15 - Q1_2025_Interim_Report.pdf
    📊 Gap: 107 days | Similarity: 0.22
```

**Severity Classifications:**
- 🚨 **Significant** (Score ≥ 0.7): Major communication pattern shifts
- ⚠️ **Moderate** (Score 0.5-0.7): Notable changes worth investigating  
- ℹ️ **Minor** (Score 0.3-0.5): Small variations, likely normal evolution

### Common Use Cases
```bash
# Check recent market-wide communication shifts
python3 analyze_existing_embeddings.py --days 90 --sort date

# Find most dramatic changes across all time
python3 analyze_existing_embeddings.py --days 2000 --sort score

# Investigate specific company before earnings
python3 analyze_existing_embeddings.py --company "Ericsson" --days 365

# Quick pre-market check for surprises
python3 analyze_existing_embeddings.py --days 30 --sort date --min-docs 3
```

### Troubleshooting Analysis Issues

**Issue: "0 documents in last X days"**
```bash
# Check actual date ranges in your data
python3 check_document_dates.py

# Use appropriate time window based on output
python3 analyze_existing_embeddings.py --days 1000  # Adjust as needed
```

**Issue: Embedding dimension errors**
- The analyzer handles multiple embedding models/dimensions automatically
- Documents are grouped by embedding model before comparison
- Mismatched dimensions return neutral similarity (0.5)

**Issue: JSON parsing errors**
- Your embeddings are stored as JSON strings in PostgreSQL
- The analyzer automatically parses these strings
- No manual intervention needed

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
│   ├── access.log        # API access logs
│   ├── daily-market-data-worker.log          # Daily market data automation
│   ├── daily-document-worker-morning.log     # Daily document discovery (7 AM)
│   ├── daily-document-worker-late.log        # Daily document discovery (9 AM)
│   └── daily-pdf-download.log                # Daily PDF download automation
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

#### Automated Workers (Check First)
- [ ] **Check daily automation status**: `launchctl list | grep yodabuffett`
- [ ] **Review market data automation**: `tail -30 logs/daily-market-data-worker.log`
- [ ] **Review document discovery automation**: `tail -30 logs/daily-document-worker-morning.log`
- [ ] **Review PDF download automation**: `tail -30 logs/daily-pdf-download.log`
- [ ] **Check new PDF files**: `ls -la data/companies/SE/*/*/2025/*/` (recent downloads)
- [ ] **Monitor disk usage growth**: `du -sh data/companies/`

#### Manual System Checks  
- [ ] Check disk space: `df -h`
- [ ] Review error logs: `tail -f logs/error.log`
- [ ] Monitor API costs: OpenAI/Anthropic dashboards
- [ ] **Check fundamentals automation**: `tail -30 logs/daily-fundamentals-worker.log`
- [ ] **Check fundamentals data coverage**: Run fundamentals database status check
- [ ] **Check document processing status**: `PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py status`
- [ ] **Process daily batch** (if actively processing): `PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py process 100`
- [ ] **Check section chunking status**: `python domains/document_intelligence/cli_section_chunking.py status`
- [ ] **Check section embedding status**: `python domains/document_intelligence/cli_multi_embeddings.py local status`
- [ ] **Check document embedding status**: `python domains/document_intelligence/cli_document_embeddings.py local status`

#### Legacy Manual Scripts (Now Automated)
- [ ] ~~Check Nordic ingestion progress~~ (Now automated via daily workers)
- [ ] ~~Monitor PDF download progress~~ (Now automated via daily workers)
- [ ] ~~Review document collection rates~~ (Check automation logs instead)

### Weekly Tasks

#### Automated System Maintenance
- [ ] **Review automation logs**: Check week's worth of daily worker performance
- [ ] **Monitor automation health**: `launchctl list | grep yodabuffett` (ensure all workers running)
- [ ] **Archive automation logs**: `gzip logs/daily-*.log` (keep logs manageable)
- [ ] **Check fundamentals collection**: Review daily fundamentals worker progress and data coverage
- [ ] **Monitor fundamental data quality**: Check for missing P/E ratios, unusual values, data gaps
- [ ] **Check PDF collection stats**: Count new PDFs downloaded this week
- [ ] **Verify automation is working**: Manual spot check of recent document discovery

#### Database & Storage
- [ ] Backup database: `pg_dump > weekly_backup.sql`
- [ ] Clear old cache files: `find data/cache -mtime +7 -delete`
- [ ] Monitor disk usage trends: `du -sh data/companies/` vs last week
- [ ] Archive old manual ingestion results: `gzip historical_ingestion_*.json pdf_download_*.json`

#### Document Processing & AI Pipeline
- [ ] **Process large document batches**: `PYTHONPATH=backend python3 domains/document_intelligence/cli_stateful.py process 500`
- [ ] **Monitor document processing progress**: Check completion rate and processing errors
- [ ] **Process section chunking batches**: `python domains/document_intelligence/cli_section_chunking.py process 100`
- [ ] **Generate section embeddings for new sections**: `python domains/document_intelligence/cli_multi_embeddings.py local process 500`
- [ ] **Generate document embeddings**: `python domains/document_intelligence/cli_document_embeddings.py local process 200 --method hierarchical`
- [ ] **Run document-level temporal anomaly analysis**: `python test_document_temporal_patterns.py`
- [ ] **Test unified search capabilities**: `python test_unified_embedding_search.py`
- [ ] **Validate section quality**: `python domains/document_intelligence/cli_section_chunking.py inspect` (random companies)
- [ ] **Compare embedding providers**: `python domains/document_intelligence/cli_multi_embeddings.py openai compare`

#### System Performance & Optimization
- [ ] Review system performance logs
- [ ] Update API usage tracking  
- [ ] Verify PDF file integrity: spot check downloaded PDFs can be opened
- [ ] Review automation failure patterns and optimize company slug mappings if needed

#### Legacy Manual Operations (Only if Automation Fails)
- [ ] ~~Run fresh historical ingestion~~ (Now automated daily)
- [ ] ~~Run focused PDF downloads~~ (Now automated daily)  
- [ ] ~~Retry failed companies~~ (Automated retries in daily workers)
- [ ] Manual fallback only if daily automation shows consistent failures

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