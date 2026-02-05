# YodaBuffett - Human Operator Guide

## Infrastructure Overview

| Component | Technology | Details |
|-----------|-----------|---------|
| **Database** | PostgreSQL 15 (Docker container) | `localhost:5432`, database `yodabuffett`, container: `yodabuffett-db` |
| **Daily Automation** | macOS LaunchAgents | 8 scheduled workers in `~/Library/LaunchAgents/` |
| **Python Runtime** | venv | `/Users/jdandemar/Documents/YodaBuffett/backend/venv/` |

**Docker is required for the database.** PostgreSQL runs in a standalone Docker container (`yodabuffett-db`) on port 5432. Daily workers and the screener app run natively on macOS.

**Important:** Do NOT start Homebrew PostgreSQL (`brew services start postgresql@15`) — it will grab port 5432 and block the Docker database.

**Connection string:** `postgresql://yodabuffett:password@localhost:5432/yodabuffett`
**Config file:** `backend/.env`

---

## Cold Start (After Reboot)

### Step 1: Start PostgreSQL (Docker)
```bash
# Start the database container
docker start yodabuffett-db

# Verify it's running
docker ps | grep yodabuffett-db
# Expected: yodabuffett-db ... Up ... 0.0.0.0:5432->5432/tcp

# Test connectivity
PGPASSWORD=password psql -U yodabuffett -h localhost -p 5432 -d yodabuffett -c "SELECT 1"

# IMPORTANT: Do NOT start Homebrew PostgreSQL — it will conflict on port 5432
# If you accidentally started it: brew services stop postgresql@15
```

### Step 2: Verify Daily Workers
```bash
# LaunchAgents auto-load on login — check they're registered
launchctl list | grep yodabuffett

# If any show non-zero exit code, they failed (likely because PostgreSQL was off)
# Force an immediate retry:
launchctl start com.yodabuffett.daily-market-data-worker
```

### Step 3 (Optional): Start Screener
```bash
# Backend (terminal 1)
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/backend
source /Users/jdandemar/Documents/YodaBuffett/backend/venv/bin/activate
python start_simple.py
# http://localhost:8000 (API docs at /docs)

# Frontend (terminal 2)
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/frontend
npm run dev
# http://localhost:3000
```

---

## Daily Automation (LaunchAgents)

### Schedule

| Time | Worker | What it does |
|------|--------|-------------|
| 03:00 | `daily-market-data-worker` | Stock prices for 787 companies (Yahoo Finance) |
| 03:30 | `daily-fundamentals-worker` | P/E, P/B, ROE etc. — 100 symbols daily rotation |
| 07:00 | `daily-document-worker-morning` | Document discovery from MFN.se (event-driven) |
| 09:00 | `daily-document-worker-late` | Catch stragglers from morning run |
| 10:00 | `daily-pdf-download` | Download PDFs discovered by document workers |
| 11:00 | `daily-document-pipeline` | Text extraction, embeddings, section processing |
| 12:00 | `daily-anomaly-detection` | Temporal pattern analysis on embeddings |

The `daily-scheduler` service orchestrates the document workers.

### Status & Logs

```bash
# Check all workers
launchctl list | grep yodabuffett

# View logs
tail -50 ~/Documents/YodaBuffett/logs/daily-market-data-worker.log
tail -50 ~/Documents/YodaBuffett/logs/daily-document-worker-morning.log
tail -50 ~/Documents/YodaBuffett/logs/daily-pdf-download.log
tail -50 ~/Documents/YodaBuffett/logs/daily-document-pipeline.log
tail -50 ~/Documents/YodaBuffett/logs/daily-anomaly-detection.log

# Error logs (same name with -error suffix)
tail -50 ~/Documents/YodaBuffett/logs/daily-market-data-worker-error.log
```

### Manual Triggers

```bash
# Force a worker to run now (via LaunchAgent)
launchctl start com.yodabuffett.daily-market-data-worker
launchctl start com.yodabuffett.daily-document-worker-morning
launchctl start com.yodabuffett.daily-pdf-download
launchctl start com.yodabuffett.daily-document-pipeline
launchctl start com.yodabuffett.daily-anomaly-detection

# Or run directly (bypass LaunchAgent, see output in terminal)
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate
python3 -m workers.daily_market_data_worker
python3 -m workers.daily_event_worker
python3 -m workers.daily_fundamentals_worker --run-now

# Dry run (check what would happen without doing it)
python3 -m workers.daily_market_data_worker --dry-run
python3 -m workers.daily_event_worker --dry-run
```

### Managing LaunchAgents

```bash
# Disable a worker
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist

# Re-enable a worker
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist

# Disable ALL workers
for plist in ~/Library/LaunchAgents/com.yodabuffett.daily-*.plist; do
    launchctl unload "$plist"
done

# Re-enable ALL workers
for plist in ~/Library/LaunchAgents/com.yodabuffett.daily-*.plist; do
    launchctl load "$plist"
done

# Fix broken LaunchAgents
cd ~/Documents/YodaBuffett/backend
python3 fix_launchagents.py

# Edit schedule (change hour/minute in plist, then reload)
nano ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
launchctl unload ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
launchctl load ~/Library/LaunchAgents/com.yodabuffett.daily-market-data-worker.plist
```

### Plist Files

Located in `~/Library/LaunchAgents/`:
- `com.yodabuffett.daily-scheduler.plist`
- `com.yodabuffett.daily-market-data-worker.plist`
- `com.yodabuffett.daily-fundamentals-worker.plist`
- `com.yodabuffett.daily-document-worker-morning.plist`
- `com.yodabuffett.daily-document-worker-late.plist`
- `com.yodabuffett.daily-pdf-download.plist`
- `com.yodabuffett.daily-document-pipeline.plist`
- `com.yodabuffett.daily-anomaly-detection.plist`

---

## Data Pipelines

### Document Processing Pipeline

Complete flow: Discover PDFs → Extract text → Create sections → Generate embeddings → Detect anomalies

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# 1. Discover PDFs (catalog without processing)
PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py discover

# 2. Extract text from PDFs
PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py process 100

# 3. Create financial sections (free, rule-based)
python3 domains/document_intelligence/cli_section_chunking.py process 1000

# 4. Generate section embeddings (free, local model)
python3 domains/document_intelligence/cli_multi_embeddings.py local process 10000

# 5. Generate document-level embeddings (hierarchical)
python3 domains/document_intelligence/cli_document_embeddings.py local process --count 1000 --method hierarchical

# 6. Run temporal anomaly detection
python3 test_temporal_patterns.py              # Section-level
python3 test_document_temporal_patterns.py     # Document-level

# Check status at any point
PYTHONPATH=. python3 domains/document_intelligence/cli_stateful.py status
python3 domains/document_intelligence/cli_section_chunking.py status
python3 domains/document_intelligence/cli_multi_embeddings.py local status
python3 domains/document_intelligence/cli_document_embeddings.py local status
```

### Historical Data Collection

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# Historical document catch-up (missed periods)
python3 historical_document_catchup.py --days-back 7

# Historical market data backfill
python3 historical_market_data_batch.py

# Historical fundamentals backfill
python3 historical_fundamentals_backfill.py

# PDF downloads (manual batch)
python3 pdf_download_batch.py --year 2025 --delay 10        # Priority reports only
python3 pdf_download_batch.py --year 2025 --all-types --delay 10  # Everything

# Retry failed companies with smart slug detection
python3 retry_failed_companies.py
```

### Temporal Anomaly Analysis

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend

# Latest anomalies
python3 analyze_existing_embeddings.py --days 500

# Highest-scoring anomalies
python3 analyze_existing_embeddings.py --days 500 --sort score

# Company-specific
python3 analyze_existing_embeddings.py --company "AAK" --days 500

# Embedding quality checks
python3 test_embedding_quality.py
python3 debug_embeddings.py
```

### Technical Analysis & ML

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend

# Portfolio simulation (realistic constraints)
python3 realistic_portfolio_simulator.py

# KNN strategy backtest
python3 backtest_knn_strategy.py

# Individual indicator testing
python3 isolated_indicator_tester.py

# Multi-timeframe analysis
python3 multi_horizon_indicator_tester.py

# Adaptive exit testing
python3 isolated_indicator_adaptive_exit.py
```

---

## Screener App

### Starting

See [Cold Start - Step 3](#step-3-optional-start-screener) above.

### Testing

```bash
# Health check
curl http://localhost:8000/health/detailed

# Available metrics
curl http://localhost:8000/api/v1/metrics/available

# Sample screen: low P/E + high ROE
curl -X POST "http://localhost:8000/api/v1/screener/screen" \
  -H "Content-Type: application/json" \
  -d '{
    "groups": [{
      "conditions": [
        {"metric": "pe_ratio", "operator": "lt", "value": 20},
        {"metric": "roe", "operator": "gt", "value": 15}
      ]
    }],
    "displayColumns": ["pe_ratio", "roe", "market_cap"]
  }'
```

### Building for Production

```bash
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/frontend
npm run build
# Output in dist/
```

---

## Troubleshooting

### PostgreSQL won't start
```bash
# Check if something else is on port 5432
lsof -i :5432

# If Homebrew PostgreSQL grabbed the port, stop it first
brew services stop postgresql@15

# Start the Docker container
docker start yodabuffett-db

# Check container logs
docker logs yodabuffett-db --tail 50

# Verify data volume still exists
docker volume ls | grep postgres
```

### Workers failing
Most common cause: **PostgreSQL container is not running.** Start it first: `docker start yodabuffett-db`

```bash
# Check error logs for the specific worker
tail -50 ~/Documents/YodaBuffett/logs/daily-market-data-worker-error.log

# Look for "Connect call failed" → PostgreSQL is off
# Look for "ModuleNotFoundError" → venv issue, check plist ProgramArguments path

# Test a worker directly
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate
python3 -m workers.daily_market_data_worker --dry-run
```

### Disk space
```bash
# Check overall
df -h ~/Documents/YodaBuffett/

# Check PDF storage
du -sh ~/Documents/YodaBuffett/backend/data/companies/

# Count PDFs
find ~/Documents/YodaBuffett/backend/data/companies -name "*.pdf" | wc -l

# Archive old logs
cd ~/Documents/YodaBuffett/logs
tar -czf archive_$(date +%Y%m%d).tar.gz daily-*.log
```

### Database backup
```bash
docker exec yodabuffett-db pg_dump -U yodabuffett yodabuffett > backup_$(date +%Y%m%d).sql
```

---

## Environment & Credentials

### .env Files

| File | Purpose |
|------|---------|
| `backend/.env` | Main database URL, API keys |
| `products/screener/backend/.env` | Screener DB connection |
| `products/screener/frontend/.env` | Frontend API URL |

### Required Environment Variables

```bash
# Database (in backend/.env)
DATABASE_URL=postgresql://yodabuffett:password@localhost:5432/yodabuffett

# AI APIs (in backend/.env or shell profile)
OPENAI_API_KEY=sk-...         # For embeddings
ANTHROPIC_API_KEY=sk-ant-...  # For analysis (optional)
```

### API Key Management
- **OpenAI**: https://platform.openai.com/api-keys (monitor usage at /usage)
- **Anthropic**: https://console.anthropic.com/

---

## Docker

### What runs in Docker
- **PostgreSQL** — container `yodabuffett-db`, port 5432. This is the production database.

### What does NOT run in Docker
- Daily workers (macOS LaunchAgents)
- Screener app (native Python/Node)
- All Python scripts

### Docker commands
```bash
# Start database
docker start yodabuffett-db

# Stop database
docker stop yodabuffett-db

# Check database container
docker ps | grep yodabuffett-db

# View database logs
docker logs yodabuffett-db --tail 50

# Database shell
docker exec -it yodabuffett-db psql -U yodabuffett -d yodabuffett
```

### Additional Docker configs
Worker orchestration configs exist in `backend/docker/docker-compose.yml` for future cloud deployment but are not actively used. The compose file defines a separate PostgreSQL service on port 5433 — this is NOT the production database.
