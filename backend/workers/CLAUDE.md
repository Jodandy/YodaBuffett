# Multi-Market Worker System - CLAUDE.md

**🌍 Production-ready multi-market financial data monitoring system**

## Quick Context
This is the **production monitoring system** for Nordic financial markets (Sweden, Norway, Denmark, Finland) with specialized workers for each market. Uses calendar events to intelligently target data collection and replaces manual batch processing with smart, event-driven automation.

## 🎯 **Core Strategy** 
**Multi-Market Event-Driven Architecture**
- **Market-Specific Ingestors**: Specialized workers for each Nordic country (Swedish, Norwegian, etc.)
- **Event Monitors**: Calendar-driven targeting and surprise detection
- **Market Data Workers**: Price collection and corporate actions tracking
- **Maintenance Workers**: Database cleanup and data quality auditing
- **Unified Management**: Single interface to orchestrate all workers

## 🏗️ Architecture Overview

```
workers/
├── base/                          # Base worker classes and utilities
│   ├── base_worker.py            # Abstract base worker class
│   ├── document_ingestor.py      # Base for document collection workers  
│   └── health_server.py          # Health check HTTP server
├── ingestors/                     # Market-specific document collectors
│   ├── swedish_document_ingestor.py     # MFN.se + Swedish sources
│   ├── norwegian_document_ingestor.py   # Newsweb + Norwegian sources
│   ├── danish_document_ingestor.py      # Nasdaq Copenhagen + Danish sources
│   └── finnish_document_ingestor.py     # Nasdaq Helsinki + Finnish sources
├── monitors/                      # Event monitoring and detection
│   ├── swedish_event_monitor.py   # Swedish calendar events
│   └── surprise_scanner.py        # Cross-market surprise detection
├── market_data/                   # Market data collection
│   ├── price_collector.py         # Real-time and historical prices
│   └── dividend_tracker.py        # Corporate actions and dividends
├── maintenance/                   # System maintenance
│   ├── database_cleanup.py        # Data retention and cleanup
│   └── data_quality_auditor.py    # Data quality validation
├── management/                    # Unified worker management
│   └── worker_manager.py          # Web UI + API + orchestration
├── config/                        # Configuration system
│   ├── market_configs.py          # Market-specific configurations
│   └── worker_registry.py         # Worker discovery and metadata
└── event_scheduler.py             # Smart calendar-driven targeting
```

**Docker Deployment:**
```
docker/
├── Dockerfile.worker          # Multi-stage production container (all workers)
├── docker-compose.yml         # Full multi-market orchestration
├── worker-entrypoint.sh       # Dynamic worker startup script
├── health-check.sh            # Container health check script
└── .env.example              # Configuration template
```

**Production Deployment:**
Daily workers run via **macOS LaunchAgents** (not Docker). See `docs/operations/human-operator-guide.md` for operational details.

Docker configs exist in `docker/` for future cloud deployment but are not actively used.

## 🚀 **Production Components Built**

### ✅ **EventScheduler Service** 
**Smart calendar-driven targeting:**
- Queries `nordic_calendar_events` for upcoming financial events
- Filters for high-value events (earnings, quarterly/annual reports)
- Calculates optimal scraping timing (day-of, day-after events)
- Company prioritization (Tier 1/2/3 based on market cap)
- Surprise target selection (companies with no recent activity)

**Key Methods:**
```python
await scheduler.get_daily_scrape_targets(date)     # Event-driven daily list
await scheduler.get_weekly_surprise_targets(50)    # Random quiet companies
```

### ✅ **Daily Event Worker**
**Production-ready event-driven worker:**
- **Event-driven**: Only processes companies with calendar events
- **Batch optimized**: Uses the database query optimizations we built
- **Production logging**: Structured logs, progress tracking, health checks
- **Automatic scheduling**: Runs daily via macOS LaunchAgent
- **Resume capability**: Progress persistence for interrupted runs

**Manual Usage:**
```bash
python -m workers.daily_event_worker                    # Production run
python -m workers.daily_event_worker --dry-run          # Preview targets
python -m workers.daily_event_worker --date 2025-01-15  # Specific date
```

### ✅ **Daily Scheduler (PRODUCTION ACTIVE - macOS LaunchAgents)**
**Automated daily execution via native macOS scheduling:**
- **Automatic execution**: 8 LaunchAgents run workers on schedule (3 AM - 12 PM)
- **Service persistence**: Auto-loads on login, survives restarts
- **Zero manual intervention**: Workers fire on schedule as long as PostgreSQL is running
- **Intelligent targeting**: Only processes companies with upcoming financial events

**Service Status:**
```bash
# Check all workers
launchctl list | grep yodabuffett

# View logs
tail -50 ~/Documents/YodaBuffett/logs/daily-market-data-worker.log
tail -50 ~/Documents/YodaBuffett/logs/daily-document-worker-morning.log

# Manual trigger
launchctl start com.yodabuffett.daily-market-data-worker
```

**What happens daily:
1. EventScheduler queries calendar events for next 3 days
2. Identifies companies with earnings, reports, dividends, AGMs
3. Calculates optimal scrape timing (day-of or day-after events)
4. Executes MFN collection with batch-optimized database queries
5. Stores documents and calendar events efficiently
6. Saves detailed execution metrics to `data/daily_worker_*.json`

**Expected Daily Performance:**
- **Companies processed**: 0-50 (vs 1600+ for full sweep)
- **Success rate**: 85%+ (event timing improves document availability)
- **Execution time**: 2-5 minutes total
- **Database efficiency**: 10-100x faster with batch optimizations

### ✅ **Weekly Surprise Scanner** 
**Broad-spectrum surprise detection:**
- **Targets quiet companies**: No recent calendar events (14+ days)
- **Randomized sampling**: Unpredictable patterns (50 companies default)
- **Surprise classification**: High/medium/low significance detection
- **Resource efficient**: Slower rate limits, smaller batches

**Surprise Detection:**
- New documents from previously quiet companies
- Unexpected corporate actions, M&A announcements
- Pattern analysis across document types and company tiers

### ✅ **Worker Configuration System**
**Environment-driven configuration:**
- **Database**: Connection strings, credentials
- **Scheduling**: Look-ahead/back days, sample sizes, rate limits  
- **Scraping**: Timeouts, retries, user agent strings
- **Operational**: Health check ports, log paths, data volumes

**Configuration Sources:**
1. Environment variables (production)
2. `.env` files (development)  
3. Sensible defaults (fallback)

### **Docker Deployment (Optional — Not Currently Used)**
Docker configs exist in `docker/` for future cloud deployment. The current production system uses macOS LaunchAgents instead.

```bash
# Only if migrating to Docker (not currently needed)
docker-compose -f docker/docker-compose.yml up -d
docker-compose -f docker/docker-compose.yml ps
# Note: Docker PostgreSQL uses port 5433 to avoid conflict with native on 5432
```

### ✅ **Management CLI**
**Complete operational interface:**
- **Worker operations**: Run daily/weekly workers with all options
- **Docker management**: Start/stop/status/logs for all services
- **Schedule preview**: See upcoming events and targets for next N days
- **Results analysis**: Aggregate performance metrics across runs
- **Health checks**: System diagnostics and configuration validation

**Key Commands:**
```bash
python scripts/manage_workers.py daily-worker --dry-run    # Preview daily targets
python scripts/manage_workers.py schedule --days 7        # 7-day schedule preview  
python scripts/manage_workers.py weekly-scanner           # Run surprise scanner
python scripts/manage_workers.py docker --action start    # Start Docker services
python scripts/manage_workers.py analyze --days 30        # Analyze last 30 days
python scripts/manage_workers.py health-check             # System diagnostics
```

## 📊 **How It Works In Practice**

### **Daily Operations (Automated):**
1. **6 AM Daily**: EventScheduler queries calendar events for next 3 days
2. **Event Filtering**: Focus on earnings, quarterly/annual reports
3. **Smart Timing**: Scrape day-of and day-after events for document availability  
4. **Targeted Scraping**: Only process companies with scheduled events
5. **Progress Tracking**: Persistent progress, resume capability, health monitoring

### **Weekly Operations (Triggered):**
1. **Weekly Surprise Scan**: Random sample of quiet companies (no recent events)
2. **Surprise Detection**: Identify unexpected corporate activity
3. **Pattern Analysis**: Classify significance and document types discovered
4. **Reporting**: Flag high-significance surprises for attention

### **Event-Driven Efficiency:**
- **Resource Optimization**: 1000 companies × 2% with events = 20 companies vs 1000
- **Higher Success Rate**: Scrape when documents most likely to be published
- **Respectful to MFN.se**: Reduced request volume, focused targeting
- **Scalable**: Linear scaling with events, not total companies

## 🔧 **Configuration Examples**

### **Production (.env):**
```bash
WORKER_MODE=production
LOG_LEVEL=INFO
DB_HOST=postgres.production.com
DB_PASSWORD=password
LOOK_AHEAD_DAYS=3
RATE_DELAY=2.0
WEEKLY_SAMPLE_SIZE=50
```

### **Development (.env):**
```bash
WORKER_MODE=development  
LOG_LEVEL=DEBUG
LOOK_AHEAD_DAYS=7
RATE_DELAY=1.0
WEEKLY_SAMPLE_SIZE=10
```

## 🧪 **Testing & Development**

### **Dry Run Everything:**
```bash
# Preview daily targets without scraping
python scripts/manage_workers.py daily-worker --dry-run

# Preview weekly targets  
python scripts/manage_workers.py weekly-scanner --dry-run

# Preview schedule for next week
python scripts/manage_workers.py schedule --days 7
```

### **Development Setup:**
```bash
# Ensure PostgreSQL is running
brew services start postgresql@15

# Activate virtual environment
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# Run workers manually
python -m workers.daily_event_worker --dry-run
python -m workers.daily_market_data_worker --dry-run
```

## 📈 **Performance & Monitoring**

### **Expected Performance:**
- **Daily Worker**: 20-100 companies based on calendar events
- **Processing Speed**: 2-3 seconds per company (with batch optimizations)
- **Success Rate**: 85%+ (event-driven targeting improves document availability)
- **Resource Usage**: <1GB RAM, <0.5 CPU cores per worker

### **Monitoring:**
- **LaunchAgent Status**: `launchctl list | grep yodabuffett`
- **Progress Tracking**: JSON result files in `data/daily_worker_*.json`
- **Structured Logs**: `~/Documents/YodaBuffett/logs/daily-*.log`

### **Results Analysis:**
```bash
# Analyze last 7 days of runs
python scripts/manage_workers.py analyze --days 7

# Check system health
python scripts/manage_workers.py health-check

# View recent logs
tail -100 ~/Documents/YodaBuffett/logs/daily-market-data-worker.log
```

## 🚨 **Troubleshooting**

### **Common Issues:**

**No daily targets found:**
- Check calendar events: `SELECT * FROM nordic_calendar_events WHERE event_date >= CURRENT_DATE`
- Verify scheduler look-ahead days configuration
- Run schedule preview: `python scripts/manage_workers.py schedule`

**Database connection failures:**
- Most common cause: PostgreSQL is not running. Start it: `brew services start postgresql@15`
- Check credentials in `backend/.env`
- Test connectivity: `/opt/homebrew/opt/postgresql@15/bin/pg_isready -h localhost -p 5432`

**Workers not firing:**
- Check LaunchAgent status: `launchctl list | grep yodabuffett`
- Reload plist: `launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist && launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist`
- Fix all LaunchAgents: `python3 fix_launchagents.py`

### **Debugging Commands:**
```bash
# Test event scheduler
python workers/event_scheduler.py

# Test configuration loading
python workers/worker_config.py

# Run worker directly with output in terminal
python -m workers.daily_event_worker --dry-run
python -m workers.daily_market_data_worker --dry-run
```

## 🎯 **Integration Points**

### **With Existing Systems:**
- **Uses nordic_ingestion batch optimizations** (eliminates excessive DB queries)
- **Leverages existing MFN collectors** (same scraping logic)
- **Integrates with calendar storage** (event-driven triggers)
- **Works with document catalog** (same storage pipelines)

### **Data Flow:**
1. **Calendar Events** → EventScheduler → **Target Companies**
2. **Target Companies** → Daily Worker → **MFN Collection** 
3. **MFN Collection** → **Document Catalog** (batch optimized)
4. **MFN Collection** → **Calendar Storage** (batch optimized)
5. **Results** → **JSON Files** → **Analysis Dashboard**

## 🎉 **Production Ready**

This system is **production-active** on macOS with LaunchAgent-based daily automation:

1. ✅ **Event-driven targeting** using calendar database
2. ✅ **Daily worker** for scheduled financial events
3. ✅ **Weekly surprise scanner** for unexpected news
4. ✅ **macOS LaunchAgent automation** — 8 workers on daily schedule
5. ✅ **Production monitoring** with logs and health checks

**Operational guide:** `docs/operations/human-operator-guide.md`