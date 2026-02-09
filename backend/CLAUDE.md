# Backend

Python backend with FastAPI, PostgreSQL 15, and asyncpg.

---

## 🎯 Fat Pitch Machine (Investment Filter)

The Fat Pitch Machine is a multi-stage investment opportunity filter that:
1. Routes companies to lifecycle stages based on financials
2. Scores them using stage-specific dimension weights
3. Ranks and surfaces top actionable investment candidates

### Architecture

```
domains/fat_pitch/
├── models.py           # BusinessStage, FatPitch, StageProfile dataclasses
├── business_router.py  # Routes companies to lifecycle stages
├── scorer.py           # Stage-specific scoring (14 dimension weights)
├── service.py          # Main orchestrator
├── router.py           # FastAPI REST endpoints
└── __init__.py
```

### Business Stages

| Stage | Criteria | Focus |
|-------|----------|-------|
| **Early Stage** | Revenue < $50M OR Growth > 40% | Growth trajectory, momentum |
| **Growth Stage** | Profitable AND Growth > 20% | Unit economics, margins |
| **Mature Yield** | Dividend > 3% AND Growth < 15% | Dividend safety, value traps |
| **Compounder** | Profitable AND Stable growth | Moat, returns on capital |
| **Established** | Default (profitable) | General quality |

### Stage-Specific Dimension Weights

Each stage values different dimensions:

**Early Stage** (speculative):
- Growth: 30%, Momentum: 20%, Capital Allocation: 10%
- Earnings Quality: 15%, Profitability: 10%
- Financial Health: 10%, Beneish M-Score: 5%

**Compounder** (Buffett-style):
- Quality: 15%, Returns: 15%, Profitability: 10%
- Capital Allocation: 15%, Earnings Quality: 10%
- Growth: 10%, Financial Health: 10%
- Beneish: 5%, Risk: 5%, Working Capital: 5%

### Quick Commands

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# Run all tests
python test_fat_pitch.py

# Test specific stage
python test_fat_pitch.py --stage growth_stage
python test_fat_pitch.py --stage compounder

# Analyze a company
python test_fat_pitch.py --company "Volvo"

# Get actionable pitches (Tier 1-3 + cheap)
python test_fat_pitch.py --actionable

# Show stage profiles and weights
python test_fat_pitch.py --profiles

# Stage summary stats
python test_fat_pitch.py --summary

# BACKTESTING (see docs/BACKTESTING.md for full details)
# Run backtest with smart lag (actual publish dates - recommended)
python fat_pitch_backtest.py --smart-lag

# Compare all strategies by ranking predictive power (slope analysis)
python fat_pitch_backtest.py --compare-slope --smart-lag

# Compare with fixed lag (60 days = no look-ahead bias)
python fat_pitch_backtest.py --compare --lag 60

# Test specific strategy
python fat_pitch_backtest.py --weights optimal --smart-lag
python fat_pitch_backtest.py --weights garp --smart-lag

# See actual top picks per quarter
python show_quarterly_picks.py --lag 60 --top 10

# Liquidity filters (alpha is in small caps)
python fat_pitch_backtest.py --weights optimal --max-liquidity 5 --smart-lag

# EXPORT TO EXCEL (full data with charts)
python fat_pitch_backtest.py --export --weights optimal --smart-lag
python fat_pitch_backtest.py --export --weights garp --output garp_analysis.xlsx
```

### Export Feature

The `--export` flag generates:
- **Excel file** with multiple sheets:
  - `All Companies` - Full ranked list (all quarters combined)
  - `Summary` - Aggregated stats per quarter
  - `Top 20 Picks` - Only top ranked companies
  - `Q YYYY-MM-DD` - Individual tabs for each quarter
  - `Weights` - Dimension weights used
  - `Metadata` - Export configuration

- **5 PNG charts**:
  - `_alpha_by_quarter.png` - Bar chart of alpha per quarter
  - `_cumulative_returns.png` - Top picks vs market growth
  - `_dimension_comparison.png` - Top vs bottom performer dimensions
  - `_rank_vs_return.png` - Scatter plot: does ranking predict returns?
  - `_return_distributions.png` - Histogram of return distributions

**Data cleaning**: Returns >500% or <-90% are filtered (likely stock splits).

### Backtesting Results (Smart Lag - actual publish dates)

**Strategy Ranking by Predictive Power (Slope Analysis)**

Uses linear regression of rank vs 12M return. More negative slope = better predictor.

| Strategy | Slope | P-value | Verdict |
|----------|-------|---------|---------|
| **optimal** | -0.0350 | 1.75e-13 | **BEST** - custom strategy from analysis |
| garp | -0.0349 | 1.39e-13 | Peter Lynch style (growth + value) |
| quality | -0.0326 | 7.53e-12 | Quality-focused |
| buffett | -0.0322 | 1.30e-11 | Moat + returns + fair price |
| equal | -0.0311 | 6.36e-11 | Equal weights baseline |
| ml | -0.0304 | 1.59e-10 | ML-optimized weights |
| magic_formula | -0.0281 | 3.15e-09 | Greenblatt (ROIC + cheap) |
| piotroski | -0.0274 | 8.70e-09 | Improving fundamentals |
| canslim | -0.0272 | 6.78e-09 | O'Neil growth + momentum |
| minervini | -0.0211 | 6.27e-06 | Trend template |
| value | -0.0201 | 2.29e-05 | Value-focused |
| graham | -0.0053 | 0.26 | ❌ No signal |
| horrible | -0.0032 | 0.50 | ❌ No signal (intentionally bad) |
| momentum_only | -0.0024 | 0.60 | ❌ No signal |
| deep_value | +0.0067 | 0.15 | ❌ **INVERSE** - value traps! |

**Key Findings (2026-02-09):**

1. **Optimal strategy weights** (best performer):
   - Growth: 20%, Profitability: 18%, Returns: 15%
   - Earnings Quality: 12%, Beneish: 12%, Quality: 10%
   - Value: 8%, Capital Allocation: 5%
   - Momentum: 0% (use as veto instead)

2. **What works:**
   - GARP (growth at reasonable price) is best named strategy
   - Quality metrics are strong predictors (profitability, returns, earnings_quality)
   - Beneish M-Score filters manipulation effectively
   - Smart lag (actual publish dates) validates no look-ahead bias

3. **What doesn't work:**
   - Pure momentum has NO signal alone
   - Deep value is actually INVERSE (value traps)
   - Graham defensive has no signal in Nordic markets
   - Historical cheapness (valuation_percentile) = falling knives

4. **Alpha concentration:**
   - Small caps (<5M daily volume): +8.2% alpha
   - Large caps (>20M daily volume): +0.6% alpha
   - Alpha is in smaller, less-followed stocks

See `docs/BACKTESTING.md` for complete documentation.

### API Endpoints (when main.py runs)

```
GET /fat-pitch/pitches                    # All pitches ranked
GET /fat-pitch/pitches/actionable         # Top actionable (quality + cheap)
GET /fat-pitch/pitches/stage/{stage}      # Pitches by stage
GET /fat-pitch/pitches/company/{id}       # Single company analysis
GET /fat-pitch/summary                    # Stage summary stats
GET /fat-pitch/profiles                   # View scoring profiles
GET /fat-pitch/profiles/{stage}           # Specific stage profile
```

### Dependency Chain

```
financial_statements + balance_sheet_data + daily_price_data
    ↓
calculate_and_store_dimensions.py (14 calculators)
    ↓
daily_dimension_scores table
    ↓
FatPitchService (routes + scores)
    ↓
Ranked fat pitches with quality tiers
```

---

## 📈 Market Data API

REST endpoints for price history, financial statements, documents, and calendar events.

### Architecture

```
domains/market_data/
├── schemas.py    # Pydantic response models
├── router.py     # FastAPI endpoints
└── __init__.py
```

### API Endpoints

```
GET /market-data/prices/{symbol}?days=365      # OHLCV price history
GET /market-data/financials/{symbol}           # Income, balance sheet, cash flow
GET /market-data/documents/{symbol}            # Company documents (reports, filings)
GET /market-data/events/{symbol}               # Calendar events (earnings, dividends, AGMs)
```

### Data Sources

| Endpoint | Tables | Notes |
|----------|--------|-------|
| prices | `daily_price_data` | Direct symbol lookup |
| financials | `financial_statements`, `balance_sheet_data`, `cash_flow_data` | Direct symbol lookup |
| documents | `nordic_documents` via `nordic_companies` | Swedish coverage primarily |
| events | `nordic_calendar_events` via `nordic_companies` | Swedish coverage primarily |

### Usage Example

```bash
# Price history (30 days)
curl "http://localhost:8000/api/v1/market-data/prices/ERIC-B?days=30"

# Financial statements
curl "http://localhost:8000/api/v1/market-data/financials/ERIC-B"

# Documents (annual reports, quarterly reports, etc.)
curl "http://localhost:8000/api/v1/market-data/documents/AAK?limit=20"

# Calendar events
curl "http://localhost:8000/api/v1/market-data/events/AAK"
```

---

## 📊 Dimension Scoring System

14 dimension calculators that score companies 0-100 on various metrics.

### The 14 Dimensions

**Business Fundamentals (price-independent):**
| Dimension | What it measures |
|-----------|------------------|
| `profitability` | Margin levels and trends |
| `returns` | ROE, ROIC, DuPont analysis |
| `growth` | Revenue/earnings growth, CAGR |
| `financial_health` | Debt coverage, liquidity, Z-score |
| `earnings_quality` | Accruals, cash backing |
| `capital_allocation` | Reinvestment, dividend coverage |
| `working_capital` | Operating efficiency |
| `beneish_mscore` | Earnings manipulation detection |

**Market Perception (price-dependent):**
| Dimension | What it measures |
|-----------|------------------|
| `value` | P/E, P/B, EV/EBITDA vs peers/history |
| `risk` | Volatility, drawdown, beta |
| `momentum` | Price trends, RSI, KNN patterns |
| `quality` | Overall quality composite |
| `valuation_percentile` | Historical cheapness ranking |

**AI-Derived:**
| Dimension | What it measures |
|-----------|------------------|
| `sentiment` | Embedding-based communication shifts |

### Calculate Dimensions

```bash
# Single company
python calculate_and_store_dimensions.py --company "Volvo"

# All companies (current date)
python calculate_and_store_dimensions.py --all

# Specific date
python calculate_and_store_dimensions.py --all --date 2024-06-30

# Limited batch
python calculate_and_store_dimensions.py --all --limit 100
```

### Historical Dimensions Backfill

For ML training and backtesting, calculate dimensions for historical dates:

```bash
# Dry run - see what will be calculated
python historical_dimensions_backfill.py --dry-run

# Quarterly snapshots (faster, good for ML) - RECOMMENDED START
python historical_dimensions_backfill.py --frequency quarterly --start-date 2021-06-01

# Monthly snapshots (more granular)
python historical_dimensions_backfill.py --start-date 2021-06-01

# Test with limited companies
python historical_dimensions_backfill.py --limit 50 --frequency quarterly

# Single company full history
python historical_dimensions_backfill.py --company "Volvo"

# Resume (skips already calculated - default behavior)
python historical_dimensions_backfill.py --resume
```

**Estimates:**
| Frequency | Dates | Companies | Est. Time |
|-----------|-------|-----------|-----------|
| Quarterly | ~15 | 1,589 | ~45 min |
| Monthly | ~45 | 1,589 | ~2-3 hrs |

**Data availability:**
- Financial statements: 2020-12-31 to 2025-12-31
- Price data: 1998-12-30 to present
- Realistic backfill start: 2021-06-01 (need trailing data)

### Database Tables

```sql
-- Main dimension scores
daily_dimension_scores (
    company_id UUID,
    score_date DATE,
    dimension_code VARCHAR,
    score FLOAT,           -- 0-100
    confidence FLOAT,      -- 0-1
    metadata JSONB,        -- Dimension-specific details
    PRIMARY KEY (company_id, score_date, dimension_code)
)

-- Check current state
SELECT COUNT(*) as records,
       COUNT(DISTINCT score_date) as dates,
       COUNT(DISTINCT company_id) as companies
FROM daily_dimension_scores;
```

---

## 🔮 ML/Backtesting Pipeline (Next Steps)

After historical dimensions are calculated:

### 1. Create Forward Returns Labels

```python
# For each (company, date): calculate 6/12mo forward return
# Label: "good pick" (>15%) / "bad pick" (<-10%) / "neutral"
```

### 2. Train Model

```python
# Input: 14 dimension scores
# Output: probability of "good pick"
# Model: gradient boosting (interpretable)
```

### 3. Extract Insights

- Feature importance: which dimensions actually predict returns?
- Optimal weights: what should the fat pitch scorer use?
- Stage validation: does routing add value?

---

## Database Connection

```python
# Standard connection pattern
import asyncpg

conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
```

## Database Migrations

We use an **idempotent script approach** - each migration is safe to run multiple times.

### Core Principle: IF NOT EXISTS

```sql
-- Safe to run 10 times - same result as running once
ALTER TABLE users ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'free';
CREATE INDEX IF NOT EXISTS idx_users_tier ON users(tier);
CREATE TABLE IF NOT EXISTS preferences (...);
```

### Creating a New Migration

1. Create script in appropriate location:
```bash
# For new tables
backend/create_<feature>_tables.py

# For schema changes
backend/domains/<domain>/migration_<description>.py
```

2. Use this template:
```python
#!/usr/bin/env python3
"""
Migration: Add user tiers and feature flags
Run: python create_user_tiers.py
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

async def migrate():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # 1. Add columns (IF NOT EXISTS = idempotent)
        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS tier VARCHAR(20) DEFAULT 'free'
        """)

        await conn.execute("""
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS features JSONB DEFAULT '[]'
        """)

        # 2. Create indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_users_tier ON users(tier)
        """)

        # 3. Create new tables
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS feature_flags (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(100) UNIQUE NOT NULL,
                enabled_tiers TEXT[] DEFAULT ARRAY['pro', 'enterprise'],
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        print("✅ Migration complete")

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(migrate())
```

3. Run it:
```bash
cd backend/
python create_user_tiers.py
```

### Operations That Need Conditional Checks

Some operations don't have `IF NOT EXISTS`:

```python
# Changing column type - check first
col_type = await conn.fetchval("""
    SELECT data_type FROM information_schema.columns
    WHERE table_name = 'users' AND column_name = 'tier'
""")
if col_type == 'character varying' and col_type != 'character varying(50)':
    await conn.execute("ALTER TABLE users ALTER COLUMN tier TYPE VARCHAR(50)")

# Dropping columns - check first
exists = await conn.fetchval("""
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'users' AND column_name = 'legacy_field'
""")
if exists:
    await conn.execute("ALTER TABLE users DROP COLUMN legacy_field")

# Data migrations - use upsert or check
await conn.execute("""
    INSERT INTO feature_flags (name, enabled_tiers)
    VALUES ('advanced_screener', ARRAY['pro', 'enterprise'])
    ON CONFLICT (name) DO NOTHING
""")
```

### Migration Tracking (Optional)

For visibility into what's been applied:

```python
# Add to any migration script
async def ensure_tracking_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS _migrations (
            id VARCHAR(100) PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT NOW()
        )
    """)

async def record_migration(conn, migration_id: str):
    await conn.execute("""
        INSERT INTO _migrations (id) VALUES ($1)
        ON CONFLICT (id) DO NOTHING
    """, migration_id)

async def was_applied(conn, migration_id: str) -> bool:
    return await conn.fetchval(
        "SELECT 1 FROM _migrations WHERE id = $1",
        migration_id
    ) is not None
```

### Existing Table Creation Scripts

| Script | Purpose |
|--------|---------|
| `create_market_data_tables.py` | Price data, daily prices |
| `create_ta_tables.py` | Technical analysis, ML models, KNN |
| `create_fundamentals_tables.py` | Financial statements, ratios |
| `create_dimensions_tables.py` | Dimension tables |

### Existing Migrations

| Script | Purpose |
|--------|---------|
| `domains/document_intelligence/migration_setup_pgvector.py` | pgvector extension |
| `domains/document_intelligence/migration_add_region.py` | Multi-market region column |
| `domains/document_intelligence/migration_add_extraction_tracking.py` | Document processing state |
| `domains/document_intelligence/migration_rename_filings_to_extracted_documents.py` | Table rename |
| `domains/document_intelligence/migration_update_column_names.py` | Column standardization |

## Quick Database Commands

```bash
# Connect to database
psql -d yodabuffett

# Check table structure
psql -d yodabuffett -c "\d users"

# List all tables
psql -d yodabuffett -c "\dt"

# Check if column exists
psql -d yodabuffett -c "SELECT column_name FROM information_schema.columns WHERE table_name = 'users'"
```

## Running the Backend

```bash
cd backend/
source venv/bin/activate
python main.py
# API at http://localhost:8000/docs
```

## Project Structure

```
backend/
├── main.py                          # FastAPI entry point
├── create_*_tables.py               # Table creation scripts
├── calculate_and_store_dimensions.py # Calculate dimensions for a date
├── historical_dimensions_backfill.py # Backfill historical dimensions
├── test_fat_pitch.py                # Test fat pitch system
├── domains/
│   ├── document_intelligence/       # Vector embeddings, extraction
│   │   ├── migration_*.py          # Schema migrations
│   │   └── cli_*.py                # CLI tools
│   ├── dimensions/                  # 14 dimension calculators
│   │   ├── calculators/            # Individual dimension calculators
│   │   ├── models/                 # DimensionScore dataclass
│   │   └── repositories/           # Database CRUD
│   ├── fat_pitch/                  # Fat Pitch Machine
│   │   ├── models.py               # BusinessStage, FatPitch, StageProfile
│   │   ├── business_router.py      # Routes companies to stages
│   │   ├── scorer.py               # Stage-specific scoring
│   │   ├── service.py              # Main orchestrator
│   │   └── router.py               # FastAPI endpoints
│   ├── portfolio/                  # Portfolio management
│   └── screener/                   # Stock screener
├── workers/                         # Background workers
│   ├── daily_dimensions_worker.py  # Daily dimension calculation
│   ├── daily_market_data_worker.py # Daily price updates
│   └── daily_event_worker.py       # Document collection
├── nordic_ingestion/                # Nordic market data
└── services/
    └── technical_analysis/          # TA indicators, ML
```
