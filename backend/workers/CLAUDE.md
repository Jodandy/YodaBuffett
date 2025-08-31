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

**Management Interfaces:**
- **Web Dashboard**: http://localhost:8090/dashboard (Worker Manager UI)
- **REST API**: http://localhost:8091/ (Programmatic control)
- **CLI Container**: `docker exec -it yodabuffett-worker-cli bash`

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

### ✅ **Daily Scheduler (PRODUCTION ACTIVE - DOCKER DEPLOYMENT)**
**Fully automated Docker-based daily execution system:**
- **Automatic execution**: Runs daily event worker at 6:00 AM via Docker container with built-in scheduler
- **Service persistence**: Survives system restarts, auto-restarts on crash via Docker restart policies
- **Zero manual intervention**: Set-and-forget operation with Docker orchestration
- **Intelligent targeting**: Only processes companies with upcoming financial events
- **Portable deployment**: Same behavior on any Docker-capable server

**Service Status:**
```bash
# Check if scheduler container is running
docker ps | grep yodabuffett-daily-scheduler

# View scheduler logs
docker logs yodabuffett-daily-scheduler --tail 50

# View worker execution results (inside container)
docker exec yodabuffett-daily-scheduler ls -la /app/data/daily_worker_*.json

# Check health endpoint
curl http://localhost:8085/health
```

**Service Management:**
```bash
# Start scheduler container
docker-compose up daily-event-scheduler -d

# Stop scheduler container
docker-compose stop daily-event-scheduler

# Restart scheduler container
docker-compose restart daily-event-scheduler

# Update configuration and recreate
docker-compose up daily-event-scheduler -d --force-recreate

# View container resource usage
docker stats yodabuffett-daily-scheduler
```

**What happens daily at 6:00 AM:**
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

### ✅ **Docker Production Deployment**
**Complete container orchestration:**
- **Multi-stage Dockerfile**: Optimized for production size/security
- **Docker Compose**: PostgreSQL + Daily Worker + Weekly Scanner + CLI
- **Health checks**: Container health monitoring
- **Volume management**: Persistent data and logs
- **Resource limits**: Memory and CPU constraints
- **Non-root user**: Security best practices

**Deployment Commands:**
```bash
# Start full system
docker-compose -f docker/docker-compose.yml up -d

# Start just the daily scheduler
docker-compose -f docker/docker-compose.yml up daily-event-scheduler -d

# Check status
docker-compose -f docker/docker-compose.yml ps

# View daily scheduler logs  
docker-compose -f docker/docker-compose.yml logs -f daily-event-scheduler
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
DB_PASSWORD=secure_production_password
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
# Copy configuration template
cp docker/.env.example docker/.env

# Start development services
docker-compose -f docker/docker-compose.yml up postgres worker-cli

# Run workers manually in CLI container
docker exec -it yodabuffett-worker-cli python scripts/manage_workers.py daily-worker --dry-run
```

## 📈 **Performance & Monitoring**

### **Expected Performance:**
- **Daily Worker**: 20-100 companies based on calendar events
- **Processing Speed**: 2-3 seconds per company (with batch optimizations)
- **Success Rate**: 85%+ (event-driven targeting improves document availability)
- **Resource Usage**: <1GB RAM, <0.5 CPU cores per worker

### **Monitoring:**
- **Health Checks**: Built-in HTTP endpoints for container monitoring
- **Progress Tracking**: JSON result files with detailed metrics
- **Structured Logs**: Production-ready logging for analysis
- **Docker Health**: Container health status and restart policies

### **Results Analysis:**
```bash
# Analyze last 7 days of runs
python scripts/manage_workers.py analyze --days 7

# Check system health
python scripts/manage_workers.py health-check

# View recent logs
python scripts/manage_workers.py docker --action logs --service daily-worker --tail 100
```

## 🚨 **Troubleshooting**

### **Common Issues:**

**No daily targets found:**
- Check calendar events: `SELECT * FROM nordic_calendar_events WHERE event_date >= CURRENT_DATE`
- Verify scheduler look-ahead days configuration
- Run schedule preview: `python scripts/manage_workers.py schedule`

**Docker containers not starting:**
- Check configuration: `python scripts/manage_workers.py health-check`
- Verify environment variables in `.env` file
- Check Docker logs: `python scripts/manage_workers.py docker --action logs --service daily-worker`

**Database connection failures:**
- Verify PostgreSQL is running and accessible
- Check credentials in environment variables
- Test connection from worker-cli container

**Excessive memory usage:**
- Check batch sizes in worker configuration
- Monitor with: `docker stats yodabuffett-daily-worker`
- Adjust resource limits in docker-compose.yml

### **Debugging Commands:**
```bash
# Test event scheduler
python workers/event_scheduler.py

# Test configuration loading
python workers/worker_config.py  

# Check Docker services
python scripts/manage_workers.py docker --action status

# Interactive debugging
docker exec -it yodabuffett-worker-cli bash
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

This system is **production-ready** and can be deployed to keep Swedish financial data current with minimal manual intervention. It implements the exact strategy you outlined:

1. ✅ **Event-driven targeting** using calendar database
2. ✅ **Daily worker** for scheduled financial events  
3. ✅ **Weekly surprise scanner** for unexpected news
4. ✅ **Docker deployment** ready for your local machine
5. ✅ **Production monitoring** with health checks and analysis
6. ✅ **Comprehensive documentation** for operations and maintenance

**Next Steps:**
1. Configure environment variables in `docker/.env`
2. Deploy with: `python scripts/manage_workers.py docker --action start`  
3. Monitor with: `python scripts/manage_workers.py health-check`
4. Schedule daily runs or let Docker handle automatic restarts