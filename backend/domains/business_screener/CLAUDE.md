# Business Screener Deluxe

Investment screening platform with 15 screen types across three analysis tiers.

---

## Overview

The Business Screener Deluxe is a systematic investment screening system that identifies investment opportunities through:

- **Tier A**: Pure SQL/math screens - runs on all stocks, ~0 cost
- **Tier B**: Local LLM enhanced analysis - runs on Tier A candidates, low cost
- **Tier C**: API LLM deep analysis (Claude) - runs on top candidates, per-analysis cost

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    BUSINESS SCREENER DELUXE                   │
│                                                                │
│  ┌────────────┐   ┌────────────┐   ┌────────────────────┐     │
│  │  Tier A     │   │  Tier B     │   │  Tier C             │   │
│  │  SQL/Math   │──▶│  Local LLM  │──▶│  API LLM (Claude)   │   │
│  │  All stocks │   │  Candidates │   │  Top candidates     │   │
│  │  ~0 cost    │   │  Low cost   │   │  Per-analysis cost  │   │
│  └────────────┘   └────────────┘   └────────────────────┘     │
│         │                │                   │                  │
│         ▼                ▼                   ▼                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              SCREEN RESULTS TABLE                        │   │
│  │  company_id | screen_type | tier | score | metadata      │   │
│  └─────────────────────────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              CANDIDATE CARDS (API + UI)                  │   │
│  └─────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

---

## The 15 Screens

| # | Name | Tier | Frequency | Description |
|---|------|------|-----------|-------------|
| 1 | Net-Nets | A | Weekly | Below liquidation value (NCAV > market cap) |
| 2 | Defensive Bargains | A | Monthly | Graham's multi-factor safety screen |
| 3 | Asset Plays | A+B | Monthly | Real assets below book value |
| 4 | Revenue Turnarounds | A+B | Weekly | Intact unit economics at death prices |
| 5 | Distressed Stable Earners | A+B | Monthly | Temporary margin compression |
| 6 | Growth at Reasonable Prices | A+B | Monthly | Demonstrated growth, not hypothetical |
| 7 | Compressed Fundamentals | B/C | Quarterly | Coiled spring - temporary earnings suppression |
| 8 | Special Situations | B | Daily | Event-driven with defined timelines |
| 9 | Holding Company Discounts | A+B | Daily | Portfolios below sum of parts |
| 10 | Sum-of-Parts | B/C | Annually | Hidden value in the footnotes |
| 11 | Cannibal Companies | A | Quarterly | Buyback compounders |
| 12 | Wonderful Business at Fair Price | A+C | Quarterly | Munger's compounders |
| 13 | Crisis Bargains | A+B | Daily | Legal or regulatory overhang |
| 14 | Cyclicals | A+B | Monthly | Inverted screen for cyclical companies |
| 15 | Stalwarts | A+B | Weekly | Blue chip dip buys |

---

## Database Tables

### Core Tables

```sql
-- Screen results storage
bsd_screen_results (
    id SERIAL PRIMARY KEY,
    company_id UUID NOT NULL,           -- References company_master.id
    screen_type INT NOT NULL,           -- 1-15
    tier CHAR(1) NOT NULL,              -- 'A', 'B', 'C'
    score DECIMAL(5,2),                 -- 0-100 composite score
    metrics JSONB,                      -- Screen-specific calculated values
    flags TEXT[],                       -- Warnings or notable conditions
    is_active BOOLEAN DEFAULT TRUE,
    triggered_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
)

-- Screen definitions
bsd_screen_definitions (
    screen_type INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    short_name VARCHAR(50) NOT NULL,
    description TEXT,
    tier_a_enabled BOOLEAN DEFAULT TRUE,
    tier_b_enabled BOOLEAN DEFAULT FALSE,
    tier_c_enabled BOOLEAN DEFAULT FALSE,
    run_frequency VARCHAR(20),          -- 'daily', 'weekly', 'monthly', 'quarterly'
    is_active BOOLEAN DEFAULT TRUE
)

-- Company cyclical classifications
bsd_company_classifications (
    company_id UUID PRIMARY KEY,
    classification VARCHAR(20) NOT NULL, -- 'STABLE', 'CYCLICAL', 'GROWTH', 'TURNAROUND', 'EARLY_STAGE'
    cycle_driver TEXT,
    cycle_position VARCHAR(20),          -- 'TROUGH', 'EARLY_RECOVERY', 'MID_CYCLE', 'LATE_CYCLE', 'PEAK'
    mid_cycle_ebitda DECIMAL(15,2),
    classified_at TIMESTAMP DEFAULT NOW(),
    classification_source VARCHAR(20)    -- 'LLM', 'MANUAL'
)

-- LLM analysis results (Tier B/C)
bsd_llm_analysis_results (
    id SERIAL PRIMARY KEY,
    company_id UUID NOT NULL,
    analysis_type VARCHAR(50) NOT NULL,
    tier CHAR(1) NOT NULL,
    model_used VARCHAR(50),
    prompt_template VARCHAR(100),
    raw_response TEXT,
    parsed_response JSONB,
    confidence_score DECIMAL(3,2),
    report_period VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW()
)

-- Holding company portfolios (for Screen 9)
bsd_holding_company_portfolios (
    id SERIAL PRIMARY KEY,
    holding_company_id UUID NOT NULL,
    held_company_id UUID,
    held_company_name VARCHAR(200),
    shares_held BIGINT,
    market_value DECIMAL(15,2),
    book_value DECIMAL(15,2),
    is_listed BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMP DEFAULT NOW()
)
```

### Helper Views

All views are prefixed with `bsd_v_` (Business Screener Deluxe View).

- `bsd_v_latest_annual` - Latest annual financials per company
- `bsd_v_latest_quarterly` - Latest quarterly financials per company
- `bsd_v_5yr_averages` - 5-year average metrics
- `bsd_v_7yr_averages` - 7-year averages (for cyclicals)
- `bsd_v_growth_rates` - YoY and CAGR calculations
- `bsd_v_price_metrics` - Current valuation ratios (P/E, P/B, EV/EBITDA, etc.)
- `bsd_v_price_changes` - Recent price performance
- `bsd_v_companies` - Unified company view with market cap

---

## Symbol Resolution

Different tables use different symbol formats:

| Table | Symbol Column | Format | Example |
|-------|--------------|--------|---------|
| company_master | primary_ticker | Hyphen | VOLV-B |
| company_master | yahoo_symbol | Yahoo suffix | VOLV-B.ST |
| financial_statements | symbol | Space | VOLV B |
| balance_sheet_data | symbol | Space | VOLV B |
| cash_flow_data | symbol | Space | VOLV B |
| daily_price_data | symbol | Hyphen | VOLV-B |

The helper views handle this conversion automatically.

---

## Quick Commands

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# Run database migration (creates tables and views)
python domains/business_screener/migration_setup.py

# Run a specific screen
python domains/business_screener/cli.py run --screen 1   # Net-Nets
python domains/business_screener/cli.py run --screen 2   # Defensive Bargains
python domains/business_screener/cli.py run --all-tier-a # All Tier A screens

# View results
python domains/business_screener/cli.py results --screen 1
python domains/business_screener/cli.py results --active
python domains/business_screener/cli.py results --company "Volvo"

# Multi-screen hits (companies triggering 2+ screens)
python domains/business_screener/cli.py multi-hits

# Status check
python domains/business_screener/cli.py status
```

---

## File Structure

```
domains/business_screener/
├── CLAUDE.md                    # This file
├── __init__.py
├── migration_setup.py           # Database tables and views
├── cli.py                       # Command-line interface
├── router.py                    # FastAPI endpoints
├── service.py                   # Main orchestrator
├── models/
│   ├── __init__.py
│   ├── screen_result.py         # ScreenResult dataclass
│   └── screen_definition.py     # ScreenDefinition dataclass
├── repositories/
│   ├── __init__.py
│   └── screen_repository.py     # Database CRUD operations
├── screens/
│   ├── __init__.py
│   ├── base.py                  # BaseScreen abstract class
│   ├── screen_01_net_nets.py
│   ├── screen_02_defensive_bargains.py
│   ├── screen_03_asset_plays.py
│   ├── screen_04_revenue_turnarounds.py
│   ├── screen_05_distressed_stable_earners.py
│   ├── screen_06_garp.py
│   ├── screen_07_compressed_fundamentals.py
│   ├── screen_08_special_situations.py
│   ├── screen_09_holding_company_discounts.py
│   ├── screen_10_sum_of_parts.py
│   ├── screen_11_cannibal_companies.py
│   ├── screen_12_wonderful_business.py
│   ├── screen_13_crisis_bargains.py
│   ├── screen_14_cyclicals.py
│   └── screen_15_stalwarts.py
└── llm/
    ├── __init__.py
    ├── base.py                  # LLM provider interface
    ├── prompts/                 # Prompt templates
    └── providers/               # Local LLM, Claude API implementations
```

---

## API Endpoints

```
GET  /api/v1/business-screener/screens                    # List all screen definitions
GET  /api/v1/business-screener/screens/{type}/results     # Results for a specific screen
GET  /api/v1/business-screener/results?active=true        # All active results
GET  /api/v1/business-screener/companies/{id}/screens     # All screen hits for a company
GET  /api/v1/business-screener/dashboard/multi-hits       # Companies triggering 2+ screens
GET  /api/v1/business-screener/dashboard/warnings         # Cyclical warnings and red flags
POST /api/v1/business-screener/screens/{type}/run         # Manually trigger a screen
POST /api/v1/business-screener/analysis/tier-b/{id}       # Trigger Tier B LLM analysis
POST /api/v1/business-screener/analysis/tier-c/{id}       # Trigger Tier C deep analysis
GET  /api/v1/business-screener/analysis/{company_id}      # All LLM analysis for a company
GET  /api/v1/business-screener/classifications            # Company cyclical classifications
PUT  /api/v1/business-screener/classifications/{id}       # Manual override of classification
```

---

## Scoring Philosophy

Each screen produces a score from 0-100:

- **70-100**: Strong candidate, high conviction
- **50-69**: Moderate candidate, worth investigating
- **30-49**: Weak signal, may need additional factors
- **0-29**: Barely passed screen criteria

Composite scores for multi-screen hits weight by:
1. Individual screen scores
2. Number of screens triggered
3. Screen complementarity (value + quality > value + value)

---

## Implementation Status

| Screen | Tier A | Tier B | Tier C | Status |
|--------|--------|--------|--------|--------|
| 1. Net-Nets | ✅ | ⬜ | - | Implemented |
| 2. Defensive Bargains | ✅ | - | - | Implemented |
| 3. Asset Plays | ✅ | ⬜ | - | Implemented |
| 4. Revenue Turnarounds | ✅ | ⬜ | - | Implemented |
| 5. Distressed Stable Earners | ✅ | ⬜ | - | Implemented |
| 6. GARP | ✅ | ⬜ | - | Implemented |
| 7. Compressed Fundamentals | - | ⬜ | ⬜ | Pending |
| 8. Special Situations | - | ⬜ | - | Pending |
| 9. Holding Company Discounts | ✅ | ⬜ | - | Implemented |
| 10. Sum-of-Parts | - | ⬜ | ⬜ | Pending |
| 11. Cannibal Companies | ✅ | - | - | Implemented |
| 12. Wonderful Business | ✅ | - | ⬜ | Implemented |
| 13. Crisis Bargains | ✅ | ⬜ | - | Implemented |
| 14. Cyclicals | ✅ | ⬜ | - | Implemented |
| 15. Stalwarts | ✅ | ⬜ | - | Implemented |

---

## Look-Ahead Bias Prevention

**CRITICAL**: All screens must be point-in-time safe.

### Price Data
```python
# CORRECT: Get price on or before target date
price = await conn.fetchval("""
    SELECT close_price FROM daily_price_data
    WHERE symbol = $1 AND date <= $2
    ORDER BY date DESC LIMIT 1
""", symbol, target_date)
```

### Financial Data
```python
# CORRECT: Use publish_date to ensure data was available
WHERE (
    (publish_date IS NOT NULL AND publish_date <= $score_date)
    OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $score_date)
)
```

---

## Currency Handling

Nordic stocks trade in local currency but may report in different currency:
- Swedish (.ST) → SEK
- Norwegian (.OL) → NOK
- Danish (.CO) → DKK
- Finnish (.HE) → EUR

Financial statements have a `currency` column. When calculating ratios:
1. Determine stock trading currency from exchange suffix
2. Get report currency from financial statements
3. Convert financial values to stock currency if different
4. Never mix currencies in ratio calculations

---

## Dependencies

- `company_master` - Source of truth for companies
- `financial_statements` - Income statement data
- `balance_sheet_data` - Balance sheet data
- `cash_flow_data` - Cash flow data
- `daily_price_data` - Price history
- `extracted_documents` - For Tier B/C text extraction (from document_intelligence domain)

---

## Related Domains

- **fat_pitch/** - Investment filter with lifecycle routing (separate system)
- **dimensions/** - 14-dimension scoring (can be used as inputs)
- **document_intelligence/** - PDF extraction for Tier B/C analysis
- **quality_screener/** - Legacy screener (this replaces/enhances)
