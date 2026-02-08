# YodaBuffett Daily Automation - Quick Reference

## 🚀 Quick Status Check
```bash
# See all running services (shows PIDs if active)
launchctl list | grep yodabuffett

# View recent activity (last 50 lines of all logs)
tail -50 ~/Documents/YodaBuffett/backend/logs/daily-*.log
```

## 📅 Daily Schedule
```
03:00 AM 🌅 Market Data       → Updates stock prices for 1,606 companies
04:00 AM 📊 Dimensions        → Computes 5 dimension scores for all companies
07:00 AM 📄 Doc Discovery 1   → Finds new documents (event-driven)
09:00 AM 📄 Doc Discovery 2   → Catches any stragglers
10:00 AM 📥 PDF Download      → Downloads discovered PDFs
11:00 AM 🔄 Processing        → Extract text, generate embeddings
12:00 PM 🚨 Anomaly Detection → Analyze temporal patterns
```

## 🔧 Common Tasks

### Force Run Now
```bash
# Run any service immediately
launchctl start com.yodabuffett.daily-market-data-worker
launchctl start com.yodabuffett.daily-dimensions-worker
launchctl start com.yodabuffett.daily-document-worker-morning
launchctl start com.yodabuffett.daily-document-pipeline
launchctl start com.yodabuffett.daily-anomaly-detection
```

### Fix Issues
```bash
cd ~/Documents/YodaBuffett/backend
python3 fix_launchagents.py  # Fixes all LaunchAgent issues
```

### View Specific Logs
```bash
# Market data (3 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-market-data-worker.log

# Document discovery (7 AM & 9 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-document-worker-*.log

# Processing pipeline (11 AM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-document-pipeline.log

# Anomaly detection (12 PM)
tail -f ~/Documents/YodaBuffett/backend/logs/daily-anomaly-detection.log
```

### Check Today's Results
```bash
# Quick stats
cd ~/Documents/YodaBuffett/backend
python3 << 'EOF'
from shared.database import AsyncSessionLocal
import asyncio
from sqlalchemy import text
from datetime import datetime

async def today_stats():
    async with AsyncSessionLocal() as db:
        # Documents added today
        result = await db.execute(text("""
            SELECT COUNT(*) FROM nordic_documents 
            WHERE created_at::date = CURRENT_DATE
        """))
        docs_today = result.scalar()
        
        # Market data updates today
        result = await db.execute(text("""
            SELECT COUNT(DISTINCT company_id) FROM market_data_daily 
            WHERE date = CURRENT_DATE
        """))
        market_updates = result.scalar()
        
        print(f"📊 Today's Activity ({datetime.now().strftime('%Y-%m-%d')})")
        print(f"   Documents discovered: {docs_today}")
        print(f"   Market data updated: {market_updates} companies")

asyncio.run(today_stats())
EOF
```

## 📊 Dimensions Scoring

### Run Dimensions Manually
```bash
cd ~/Documents/YodaBuffett/backend
source venv/bin/activate

# All 5 dimensions
python -m workers.daily_dimensions_worker --run-now

# Single dimension
python -m workers.daily_dimensions_worker --run-now --dimension value
python -m workers.daily_dimensions_worker --run-now --dimension momentum
python -m workers.daily_dimensions_worker --run-now --dimension quality
python -m workers.daily_dimensions_worker --run-now --dimension risk
python -m workers.daily_dimensions_worker --run-now --dimension sentiment

# Dry run (preview only)
python -m workers.daily_dimensions_worker --dry-run

# Backfill a specific date
python -m workers.daily_dimensions_worker --run-now --date 2026-02-05
```

### Check Dimension Scores
```bash
# Quick summary by dimension
psql -d yodabuffett -c "
SELECT dimension_code, COUNT(*) as companies,
       AVG(score)::numeric(5,1) as avg_score,
       AVG(confidence)::numeric(4,2) as avg_conf
FROM daily_dimension_scores
WHERE score_date = CURRENT_DATE
GROUP BY 1 ORDER BY 1"

# Top 10 by value dimension
psql -d yodabuffett -c "
SELECT cm.company_name, ds.score, ds.percentile_rank
FROM daily_dimension_scores ds
JOIN company_master cm ON ds.company_id = cm.id
WHERE ds.dimension_code = 'value' AND ds.score_date = CURRENT_DATE
ORDER BY ds.score DESC LIMIT 10"

# All scores for a company
psql -d yodabuffett -c "
SELECT ds.dimension_code, ds.score, ds.confidence, ds.percentile_rank
FROM daily_dimension_scores ds
JOIN company_master cm ON ds.company_id = cm.id
WHERE cm.company_name LIKE '%Volvo%' AND ds.score_date = CURRENT_DATE"
```

### Dimensions Overview
| Dimension | Type | What it Measures |
|-----------|------|------------------|
| value | fundamental | Undervaluation (P/E, P/B, EV/EBITDA) |
| momentum | technical | Price/volume trends (RSI, SMA, KNN) |
| quality | fundamental | Financial health (ROE, margins, debt) |
| risk | fundamental | Downside exposure (volatility, drawdown) |
| sentiment | ai_derived | Communication patterns (embeddings) |

---

## 🚨 Temporal Anomaly Analysis

### Run Analysis Manually
```bash
# Latest anomalies (most recent first)
python3 analyze_existing_embeddings.py --days 500

# Highest-scoring anomalies
python3 analyze_existing_embeddings.py --days 500 --sort score

# Specific company
python3 analyze_existing_embeddings.py --company "AAK" --days 500
```

### Check Data Availability
```bash
# See date ranges in your embeddings
python3 check_document_dates.py

# Check embedding database structure
python3 check_embeddings_schema.py
```

## 🛠️ Troubleshooting

### Service Not Running?
```bash
# 1. Check if loaded
launchctl list | grep yodabuffett

# 2. Fix all services
cd ~/Documents/YodaBuffett/backend
python3 fix_launchagents.py

# 3. Check for errors
tail -100 ~/Documents/YodaBuffett/backend/logs/daily-*.log | grep ERROR
```

### Test Services Manually
```bash
cd ~/Documents/YodaBuffett/backend
source venv/bin/activate

# Test with dry run
python3 -m workers.daily_market_data_worker --dry-run
python3 -m workers.daily_event_worker --dry-run
python3 workers/daily_document_pipeline.py --test
```

### Complete Reset
```bash
cd ~/Documents/YodaBuffett/backend

# Stop all
for service in daily-market-data-worker daily-document-worker-morning daily-document-worker-late daily-pdf-download daily-document-pipeline daily-anomaly-detection; do
    launchctl unload ~/Library/LaunchAgents/com.yodabuffett.$service.plist 2>/dev/null
done

# Fix and reload
python3 fix_launchagents.py
```

## 📈 Monitor Growth
```bash
# Disk usage
du -sh ~/Documents/YodaBuffett/backend/data/companies/

# Total PDFs
find ~/Documents/YodaBuffett/backend/data/companies -name "*.pdf" | wc -l

# Database size
cd ~/Documents/YodaBuffett/backend
python3 -c "
from shared.database import AsyncSessionLocal
import asyncio
from sqlalchemy import text

async def db_stats():
    async with AsyncSessionLocal() as db:
        r = await db.execute(text('SELECT COUNT(*) FROM nordic_documents'))
        print(f'Documents: {r.scalar():,}')
        r = await db.execute(text('SELECT COUNT(*) FROM document_embeddings'))
        print(f'Embeddings: {r.scalar():,}')
        r = await db.execute(text('SELECT COUNT(*) FROM market_data_daily'))
        print(f'Market data points: {r.scalar():,}')

asyncio.run(db_stats())
"
```

---
💡 **Pro Tips:**
- Services auto-restart if they crash
- Logs rotate automatically (no cleanup needed)
- All services run with your user permissions
- Check morning logs around 8 AM for overnight results
- Anomaly analysis works on historical data (no daily requirement)