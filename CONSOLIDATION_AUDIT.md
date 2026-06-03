# YodaBuffett Platform Consolidation Audit
**Date:** 2026-06-02
**Status:** Foundation needs stabilization before building ensemble system

---

## 🚨 CRITICAL FINDING: Database Not Running

```
❌ PostgreSQL: Not running (Docker daemon not active)
❌ Workers: Running but failing silently (0/0 companies processed)
❌ Data pipeline: Broken
```

**Impact:** No data has been collected recently. Workers are scheduled but producing no output.

---

## ✅ WHAT'S ACTUALLY WORKING

### 1. Worker Infrastructure (Scheduled, but not executing)
- ✅ 9 LaunchAgents properly installed in `~/Library/LaunchAgents/`
- ✅ Workers trigger on schedule (3:00 AM, 6:00 AM, 7:00 AM, etc.)
- ❌ Workers fail silently when database is unavailable
- ❌ No alerting when workers fail

**Scheduled Workers:**
- `daily-market-data-worker` (6:00 AM)
- `daily-fundamentals-worker` (6:30 AM)
- `daily-document-worker-morning` (7:00 AM)
- `daily-document-worker-late` (9:00 AM)
- `daily-pdf-download` (10:00 AM)
- `daily-document-pipeline` (11:00 AM)
- `daily-anomaly-detection` (12:00 PM)
- `daily-dimensions-worker` (TBD)
- `daily-scheduler` (TBD)

### 2. Code Architecture
- ✅ Clean domain structure (`domains/`)
- ✅ 16 value investing screens defined
- ✅ 14 dimension calculators written
- ✅ API layer (FastAPI) structured
- ✅ Frontend app scaffolded

### 3. Historical Data Collection Scripts
- ✅ `historical_market_data_batch.py` - Works
- ✅ `historical_fundamentals_backfill.py` - Works
- ✅ `pdf_download_batch.py` - Works (53K+ PDFs collected)
- ✅ Document extraction pipeline - Works

---

## ❌ WHAT'S BROKEN OR UNCLEAR

### 1. Database Layer
```
Status: DOWN
Fix: Start Docker + PostgreSQL
Time: 5 minutes
Priority: IMMEDIATE
```

### 2. Daily Data Collection
```
Status: Workers running in dry-run or failing
Issues:
  - Market data worker: "0/0 companies processed"
  - Fundamentals worker: Last real run Feb 23 (dry-run mode)
  - Document worker: Last activity March 8

Fix: Restart database, verify workers, remove dry-run flags
Time: 30 minutes
Priority: CRITICAL
```

### 3. Data Quality Unknown
```
Status: Cannot verify (database down)
Questions:
  - How many companies have complete price data?
  - How many have recent financials (2024-2025)?
  - Are there gaps in daily price collection?
  - Which companies are "investable" (sufficient data)?

Fix: Database health check script
Time: 1 hour
Priority: HIGH
```

### 4. Worker Failure Detection
```
Status: None
Issue: Workers fail silently, no alerts
Fix: Add health checks, monitoring, error alerts
Time: 2 hours
Priority: HIGH
```

---

## 🗑️ WHAT'S EXPERIMENTAL NOISE

### Backend Directory Clutter
```bash
# Files that should NOT be in version control
backend/
├── test_*.py (50+ files)              # Exploratory scripts
├── check_*.py (30+ files)             # One-off diagnostics
├── debug_*.py (10+ files)             # Debugging scripts
├── fix_*.py (5+ files)                # Quick fixes
├── retry_*.py                         # Manual retries
├── quick_*.py                         # Quick experiments
├── *_backtest*.py (10+ variations)    # Different backtest attempts
├── monte_carlo_rankings_v*.xlsx (11 versions!)  # Weight tuning attempts
└── *.csv (6 files - now gitignored)   # Test results

Total experimental files: ~120+
```

**Recommendation:** Move to `backend/experiments/` or delete.

### Complex Ranking Systems
```
Files:
- backtest_monte_carlo_rankings.py
- mc_ranking_backtest.py
- simple_mc_backtest.py (v1, v2, v3, calibrated)
- fat_pitch_backtest.py
- test_fat_pitch.py

Status: Multiple attempts to find "optimal weights"
Issue: No clear winner, too many variables
Recommendation: Pause until foundation is solid
```

### 16 Value Investing Screens
```
Location: domains/business_screener/screens/
Status: Defined but not tested individually
Issue: Complex interdependencies, Fat Pitch Machine tries to combine all
Recommendation: Test each screen in isolation first
```

---

## 📋 CONSOLIDATION ROADMAP

### Phase 1: Foundation (Week 1)
**Goal:** Reliable daily data ingestion

#### Day 1: Database & Infrastructure
- [ ] Start Docker + PostgreSQL
- [ ] Verify database schema (all tables exist)
- [ ] Run data inventory script (what data exists?)
- [ ] Document any missing tables or data quality issues

#### Day 2: Daily Workers
- [ ] Remove dry-run flags from all workers
- [ ] Verify each worker runs successfully
- [ ] Add error logging and health checks
- [ ] Set up email/Slack alerts for failures

#### Day 3: Data Quality Baseline
- [ ] Identify "investable universe" (companies with sufficient data)
- [ ] Calculate data completeness per company:
  - Price data: % of trading days covered
  - Financials: Latest quarterly/annual data date
  - Documents: Number of reports ingested
- [ ] Store in `company_data_quality` table

#### Day 4: Missing Data Backfill
- [ ] Run historical price backfill for gaps
- [ ] Run fundamentals backfill for missing companies
- [ ] Prioritize large-cap, liquid companies first

#### Day 5: Monitoring Dashboard
- [ ] Create `data_health_check.py` script
- [ ] Daily report: Data freshness, worker status, error counts
- [ ] Run as cron job (6:00 PM daily)

### Phase 2: Validation (Week 2)
**Goal:** Know what actually predicts returns

#### Isolate & Test Each Screen
For each of 16 screens:
1. **Isolated backtest** (2020-2025)
2. **Key metrics:**
   - Hit rate (% of picks that beat market)
   - Average 12-month return
   - Sharpe ratio
   - Max drawdown
3. **Result:** Keep/Modify/Discard decision

Create: `SCREEN_VALIDATION_RESULTS.md`

#### Isolate & Test Each Dimension
For each of 14 dimensions:
1. **Correlation with future returns**
2. **Quartile analysis:** Top 25% vs Bottom 25%
3. **Stability over time**
4. **Result:** Predictive / Weak / No Signal

Create: `DIMENSION_VALIDATION_RESULTS.md`

### Phase 3: Clean Ensemble (Week 3)
**Goal:** Modular system with proven components

#### Database Schema
```sql
-- Store individual valuations
CREATE TABLE company_valuations (
    id UUID PRIMARY KEY,
    company_id UUID,
    evaluation_date DATE,
    trigger_reason VARCHAR(500),

    -- Ensemble results
    estimates JSONB,  -- Array of {method, base, bull, bear, confidence, reasoning}
    consensus_base FLOAT,
    consensus_bull FLOAT,
    consensus_bear FLOAT,

    created_at TIMESTAMP DEFAULT NOW()
);

-- Track which methods are active
CREATE TABLE valuation_methods (
    id UUID PRIMARY KEY,
    method_name VARCHAR(100) UNIQUE,
    is_active BOOLEAN DEFAULT true,
    config JSONB,
    last_tested DATE,
    hit_rate FLOAT
);
```

#### Valuation Framework
```python
# Only implement methods that passed validation
domains/valuations/
├── base.py                     # ValuationMethod ABC
├── earnings_power.py           # Graham (if validated)
├── dcf.py                      # DCF (if validated)
├── asset_based.py              # Net-net (if validated)
└── technical_support.py        # Price support levels
```

#### Event-Driven Pipeline
```python
domains/event_processor/
├── relevancy_filter.py         # Is this material info?
├── valuation_trigger.py        # Should we re-evaluate?
└── price_monitor.py            # Daily: price vs valuation
```

---

## 🎯 SUCCESS CRITERIA

### Week 1 (Foundation)
✅ Database running 24/7
✅ All 9 workers collecting data daily
✅ Zero worker failures for 3 consecutive days
✅ Data quality report generated daily
✅ Know exactly which companies are "investable"

### Week 2 (Validation)
✅ Each screen has win rate documented
✅ Each dimension has correlation score
✅ Clear list of "proven" vs "experimental" methods
✅ Discard methods with no predictive power

### Week 3 (Ensemble)
✅ Valuation framework with 3-5 proven methods
✅ Event-driven pipeline for re-evaluations
✅ Price monitoring service (daily)
✅ First working end-to-end flow:
   - New document arrives
   - Relevancy filter (AI)
   - Valuation ensemble runs
   - Saved to database
   - Price monitoring compares to valuation

---

## 🚫 WHAT TO STOP DOING

1. **Stop weight tuning** - No more `monte_carlo_rankings_v12`
2. **Stop adding screens** - Test existing 16 first
3. **Stop complex rankings** - Pause Fat Pitch Machine until screens validated
4. **Stop manual backtests** - Automate once, reuse forever
5. **Stop committing test scripts** - Use `experiments/` folder

---

## 💭 OPEN QUESTIONS

1. **How many companies should we track?**
   - All 1,788 Nordic companies?
   - Focus on 500-600 liquid names?
   - Narrow to ~200 "quality" businesses?

2. **What's the minimum data quality bar?**
   - 5 years price history?
   - 3 years financials?
   - At least 1 annual report ingested?

3. **What triggers a re-evaluation?**
   - Every quarterly/annual report?
   - Only if material (anomaly detection)?
   - On earnings surprises?

4. **LLM cost management?**
   - Which evaluations need Claude?
   - Can we use local models for some?
   - Budget per company per evaluation?

---

## 📊 CURRENT TECH DEBT ESTIMATE

| Category | Files | Impact | Cleanup Time |
|----------|-------|--------|--------------|
| Test scripts | 50+ | Low (ignored by git) | 2 hours (move to experiments/) |
| Experimental backtests | 10+ | Medium (confusion) | 1 hour (consolidate) |
| Excel exports | 20+ | Low (ignored) | Already gitignored ✅ |
| Unused screens | TBD | High (complexity) | 1 week (validate each) |
| Unused dimensions | TBD | High (noise) | 1 week (test each) |
| Stopped workers | 3-4? | Critical | 1 day (fix + monitor) |

**Total Cleanup:** ~2-3 weeks to get to solid foundation

---

## 🎬 IMMEDIATE NEXT STEPS (Today)

1. **Start database** (5 min)
   ```bash
   open -a Docker
   # Wait for Docker to start
   docker-compose -f backend/docker/docker-compose.yml up -d postgres
   ```

2. **Check what data exists** (10 min)
   ```bash
   cd backend
   python3 -c "
   import asyncio
   import asyncpg

   async def check():
       conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')

       # Run data inventory
       price_records = await conn.fetchval('SELECT COUNT(*) FROM daily_price_data')
       companies = await conn.fetchval('SELECT COUNT(*) FROM company_master')

       print(f'Companies: {companies}')
       print(f'Price records: {price_records}')

       await conn.close()

   asyncio.run(check())
   "
   ```

3. **Test one worker manually** (10 min)
   ```bash
   cd backend
   python -m workers.daily_market_data_worker --dry-run
   # If successful, run for real
   python -m workers.daily_market_data_worker
   ```

4. **Create consolidation plan** (30 min)
   - Review this audit
   - Decide: Focus on all 1,788 companies or narrow scope?
   - Commit to 3-week foundation plan

---

**Next Document to Create:** `FOUNDATION_WEEK1_PLAN.md` (detailed daily tasks)
