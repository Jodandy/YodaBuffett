# Dimensions Domain

Multi-dimensional scoring system for companies. Each dimension is a "black box" that can use any methodology internally but produces a standardized score output.

## Quick Start

```bash
# Create tables
python create_dimensions_tables.py

# Run worker (dry run)
python -m workers.daily_dimensions_worker --dry-run

# Run worker for real
python -m workers.daily_dimensions_worker --run-now

# Run single dimension
python -m workers.daily_dimensions_worker --run-now --dimension value

# Backfill a date
python -m workers.daily_dimensions_worker --date 2025-02-05
```

## Data Sources

Dimensions calculate scores from **raw financial data** (not pre-computed ratios):

| Table | Contents | Records |
|-------|----------|---------|
| `financial_statements` | Income statement (revenue, profit, margins) | ~15K |
| `balance_sheet_data` | Assets, liabilities, equity | ~16K |
| `cash_flow_data` | Operating, investing, financing flows | ~16K |
| `daily_price_data` | Prices, volume, returns | ~4.8M |
| `document_embeddings` | AI embeddings for sentiment | ~50K |

See `docs/FINANCIAL_DATA_ARCHITECTURE.md` for full schema and calculable metrics.

## Architecture

```
domains/dimensions/
├── models/
│   └── dimension.py          # DimensionScore, DimensionDefinition dataclasses
├── calculators/
│   ├── base.py               # BaseDimensionCalculator + registry
│   ├── value_calculator.py   # Price vs fundamentals (NEEDS UPDATE)
│   ├── momentum_calculator.py # Technical indicators
│   ├── quality_calculator.py # Financial health (NEEDS UPDATE)
│   ├── risk_calculator.py    # Volatility, drawdown (NEEDS UPDATE)
│   └── sentiment_calculator.py # Embedding-based
├── repositories/
│   └── dimension_repository.py # Database CRUD
└── db/
    └── schema.sql            # Table definitions
```

## Dimension Philosophy

### Two Layers

**Layer 1: Business Fundamentals** (price-independent)
- What IS the business?
- Profitability, growth, financial health, efficiency
- Calculated from financial statements only

**Layer 2: Market Perception** (price-dependent)
- How does the market VIEW the business?
- Value (price vs fundamentals), momentum (price trends), risk (volatility)
- Requires price data

### Current Status

The existing calculators (value, quality, risk) were written to query `daily_fundamentals` table which has been removed. They need to be updated to query raw statement tables:

```python
# OLD (broken):
SELECT trailing_pe, price_to_book FROM daily_fundamentals WHERE ...

# NEW (correct):
SELECT fs.net_income, bs.total_equity, dp.close_price
FROM financial_statements fs
JOIN balance_sheet_data bs ON ...
JOIN daily_price_data dp ON ...
-- Then calculate: pe = price / eps, pb = price / book_value, etc.
```

## Key Concepts

### Black Box Calculators
Each calculator implements `calculate()` which can do ANYTHING internally:
- Weighted factor aggregation
- ML model inference
- External API calls
- Rule-based logic
- NLP/embedding analysis

Only requirement: return a `DimensionScore` with standardized fields.

### Standardized Output
```python
DimensionScore(
    company_id="uuid",
    score_date=date,
    dimension_code="value",
    score=72.5,              # 0-100
    confidence=0.85,         # 0-1
    data_quality=0.90,       # 0-1
    percentile_rank=82.0,    # 0-100
    score_low=68.0,          # Uncertainty range
    score_high=77.0,
    metadata={...},          # Dimension-specific details
)
```

### Metadata Flexibility
Each dimension puts whatever is relevant in `metadata`:

**Value**: `{"pe_ratio": 12.4, "pe_contribution": 18.5, "pb_ratio": 1.8}`
**Sentiment**: `{"document_stability": {...}, "risk_factor_stability": {...}}`
**Risk**: `{"volatility": {...}, "max_drawdown": {...}, "risk_level": "low"}`

## Database Tables

- `dimension_definitions` - Registry of available dimensions
- `daily_dimension_scores` - Main scores table
- `composite_scores` - Multi-dimension aggregates
- `dimension_score_history` - Aggregated history for trends
- `dimension_computation_log` - Worker run logs

## Current Dimensions

| Code | Layer | Status | Methodology |
|------|-------|--------|-------------|
| value | Market | ⚠️ Needs update | P/E, P/B vs peers |
| momentum | Market | ✅ Works | RSI, price/SMA, returns, KNN |
| quality | Business | ⚠️ Needs update | ROE, margins, debt ratios |
| risk | Market | ⚠️ Needs update | Volatility, drawdown, beta |
| sentiment | Business | ✅ Works | Embedding anomaly detection |

## Planned Dimensions (Business Layer)

| Code | What it measures | Data source |
|------|------------------|-------------|
| profitability | Margin levels and trends | financial_statements |
| growth | Revenue/earnings growth, CAGR | financial_statements (multi-period) |
| financial_health | Debt coverage, liquidity | balance_sheet_data |
| efficiency | Asset turnover, working capital | balance_sheet + financial_statements |
| cash_generation | FCF, cash conversion | cash_flow_data |
| stability | Earnings/margin consistency | financial_statements (multi-period) |

## Adding New Dimensions

### Step 1: Create the Calculator

Create `calculators/my_dimension_calculator.py`:

```python
from datetime import date
from typing import Optional
import asyncpg

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

@register_calculator
class MyDimensionCalculator(BaseDimensionCalculator):
    """
    My new dimension - describe what it measures.
    """

    @property
    def dimension_code(self) -> str:
        return "my_dimension"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="My Dimension",
            description="What this dimension measures",
            category="fundamental",  # or "technical", "ai_derived", "external"
            data_sources=["financial_statements", "balance_sheet_data"],
            update_frequency="daily",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate dimension score for a single company."""

        # Get company's yahoo symbol
        symbol = await self.conn.fetchval(
            "SELECT yahoo_symbol FROM company_master WHERE id = $1",
            company_id
        )

        # Fetch latest financial statement as of score_date
        data = await self.conn.fetchrow("""
            SELECT fs.net_income, fs.total_revenue,
                   bs.total_equity, bs.total_assets
            FROM financial_statements fs
            JOIN balance_sheet_data bs
                ON fs.symbol = bs.symbol AND fs.period_date = bs.period_date
            WHERE fs.symbol = $1 AND fs.period_date <= $2
            ORDER BY fs.period_date DESC
            LIMIT 1
        """, symbol, score_date)

        if not data:
            return None

        # Calculate metrics
        roe = data["net_income"] / data["total_equity"] if data["total_equity"] else None
        profit_margin = data["net_income"] / data["total_revenue"] if data["total_revenue"] else None

        # Convert to 0-100 score (your logic here)
        score = 50.0
        confidence = 0.8

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=score,
            confidence=confidence,
            data_quality=1.0,
            percentile_rank=None,  # Calculated by batch processor
            score_low=score - 5,
            score_high=score + 5,
            metadata={
                "roe": roe,
                "profit_margin": profit_margin,
            }
        )
```

### Step 2: Register in Worker

Edit `workers/daily_dimensions_worker.py`, add the import:

```python
# Import calculators to register them
from domains.dimensions.calculators.value_calculator import ValueCalculator
# ... existing imports ...
from domains.dimensions.calculators.my_dimension_calculator import MyDimensionCalculator  # ADD THIS
```

### Step 3: (Optional) Add to Composite Score

If the dimension should affect the overall composite score, edit the weights in `daily_dimensions_worker.py`:

```python
COMPOSITE_WEIGHTS = {
    'value': 0.20,
    'momentum': 0.20,
    'quality': 0.25,
    'sentiment': 0.15,
    'risk': 0.20,
    'my_dimension': 0.10,  # ADD THIS (adjust others to sum to 1.0)
}
```

### Step 4: Test

```bash
# Dry run to verify it's registered
python -m workers.daily_dimensions_worker --dry-run

# Run only your dimension
python -m workers.daily_dimensions_worker --run-now --dimension my_dimension

# Check results
psql -d yodabuffett -c "SELECT * FROM daily_dimension_scores WHERE dimension_code = 'my_dimension' LIMIT 5"
```

### Checklist for New Dimensions

- [ ] Calculator file created in `calculators/`
- [ ] Uses `@register_calculator` decorator
- [ ] Implements `dimension_code`, `definition`, and `calculate()`
- [ ] Queries raw statement tables (not deprecated `daily_fundamentals`)
- [ ] Returns `DimensionScore` with score 0-100
- [ ] Import added to `daily_dimensions_worker.py`
- [ ] (Optional) Added to `COMPOSITE_WEIGHTS` if affects overall score
- [ ] Tested with `--dimension my_dimension`

## Testing

```python
# Test single company
import asyncio
import asyncpg
from domains.dimensions.calculators.value_calculator import ValueCalculator

async def test():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    calc = ValueCalculator(db_conn=conn)
    score = await calc.calculate('company-uuid', date.today())
    print(score.score, score.metadata)

asyncio.run(test())
```
