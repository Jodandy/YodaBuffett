# Backtesting System

ML-validated backtesting for the Fat Pitch strategy using 14 dimension scores.

## Quick Start

```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# Run backtest with NO look-ahead bias (recommended)
python fat_pitch_backtest.py --lag 60

# Compare all weight strategies
python fat_pitch_backtest.py --compare --lag 60

# See actual top picks per quarter
python show_quarterly_picks.py --lag 60

# ML feature importance analysis
python dimension_ml_trainer.py --all-horizons
```

## Data Requirements

Before backtesting, ensure you have:

| Data | Command | Records Needed |
|------|---------|----------------|
| Dimension scores | `python historical_dimensions_backfill.py --frequency quarterly` | ~300K |
| Price data | `python ingest_all_max_history.py` | ~5M |
| Financial statements | `python historical_fundamentals_backfill.py` | ~15K |

Check status:
```bash
python3 -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    dims = await conn.fetchval('SELECT COUNT(*) FROM daily_dimension_scores')
    prices = await conn.fetchval('SELECT COUNT(*) FROM daily_price_data')
    fs = await conn.fetchval('SELECT COUNT(*) FROM financial_statements')
    print(f'Dimension scores: {dims:,}')
    print(f'Price records: {prices:,}')
    print(f'Financial statements: {fs:,}')
    await conn.close()
asyncio.run(check())
"
```

## Look-Ahead Bias

### The Problem

Financial statements have a `period_date` (quarter end) but are published 30-60 days later. Using `period_date <= score_date` in calculations could use data not yet public.

### The Solution

Use `--lag 60` to ensure only data available 60 days before the score date is used:

```bash
# Conservative (NO look-ahead bias) - USE THIS
python fat_pitch_backtest.py --lag 60

# Original (slight potential bias)
python fat_pitch_backtest.py --lag 0
```

### Validation Results

| Metric | 60-Day Lag |
|--------|------------|
| Avg Alpha 12M (equal) | +8.4% |
| Win Rate | 75% |

The 60-day lag ensures no look-ahead bias - only using data that would have been publicly available.

## Weight Profiles

### Available Profiles

| Profile | Description | Best For |
|---------|-------------|----------|
| `ml` | ML-optimized from quintile analysis | Balanced |
| `original` | Original Fat Pitch weights | Conservative |
| `equal` | Equal weights (1 each) | Simple baseline |
| `value` | Value-focused (P/E, P/B heavy) | Deep value |
| `quality` | Quality-focused (ROE, margins) | Quality premium |

### ML-Optimized Weights

Based on quintile analysis (top 20% vs bottom 20% returns):

```python
OPTIMAL_WEIGHTS = {
    'beneish_mscore': 15,      # Fraud detection - CRITICAL
    'capital_allocation': 15,  # How they spend - CRITICAL
    'profitability': 12,       # Margins matter
    'quality': 12,             # Composite quality
    'returns': 10,             # ROE/ROIC
    'earnings_quality': 10,    # Cash backing
    'growth': 8,               # Revenue growth
    'working_capital': 6,      # Operating efficiency
    'risk': 5,                 # Volatility
    'valuation_percentile': 4, # Historical cheapness
    'value': 3,                # Current valuation (weak signal)
    'momentum': 0,             # CONTRARIAN - ignore
    'sentiment': 0,            # Weak signal
    'financial_health': 0,     # Not predictive
}
```

### Surprising Findings

1. **Equal weights often win** - Simple beats complex
2. **Momentum is contrarian** - Lower momentum = better returns
3. **Beneish M-Score matters** - Fraud detection predicts returns
4. **Financial health doesn't predict** - Near zero difference between top/bottom quintiles

## Backtest Commands

### Basic Backtest

```bash
# Default: ML weights, top 20, no lag
python fat_pitch_backtest.py

# With options
python fat_pitch_backtest.py --weights equal --top 10 --lag 60
```

### Compare Strategies

```bash
python fat_pitch_backtest.py --compare --lag 60
```

Output:
```
Profile         Avg Alpha 3M    Avg Alpha 6M   Avg Alpha 12M    Win Rate 12M
---------------------------------------------------------------------------
ml                     +1.2%           +1.8%           +5.8%             67%
original               +2.4%           +3.8%           +6.9%             83%
equal                  +2.6%           +4.1%           +8.4%             75%
value                  +1.5%           +2.2%           +6.8%             83%
quality                +2.0%           +2.7%           +5.3%             58%
```

### View Quarterly Picks

```bash
# See top 20 picks per quarter with returns
python show_quarterly_picks.py --lag 60

# Top 10 only
python show_quarterly_picks.py --top 10 --lag 60

# Specific weight profile
python show_quarterly_picks.py --weights value --lag 60
```

## ML Analysis

### Feature Importance

```bash
# Single horizon
python dimension_ml_trainer.py --horizon 12M

# All horizons comparison
python dimension_ml_trainer.py --all-horizons

# Data stats only
python dimension_ml_trainer.py --dry-run
```

### Key ML Results

**Most Predictive Dimensions (12M forward):**

| Dimension | Top 20% Avg | Bottom 20% Avg | Difference |
|-----------|-------------|----------------|------------|
| capital_allocation | 38.1 | 23.8 | +14.3 |
| beneish_mscore | 64.7 | 51.1 | +13.6 |
| quality | 44.3 | 33.3 | +11.0 |
| profitability | 44.1 | 33.2 | +10.9 |
| returns | 39.9 | 29.9 | +9.9 |

## Interpreting Results

### Alpha

- **Positive alpha** = Beat the market median
- **Measured vs median** (not mean) to exclude outlier penny stocks
- **Clipped extremes** (-90% to +200%) for fair comparison

### Win Rate

- Percentage of quarters where top 20 beat market median
- 75%+ is strong (random would be 50%)

### Best/Worst Picks

- Shows individual companies to identify patterns
- Watch for recurring losers (e.g., Vidhance appeared multiple times)

## Production Recommendations

1. **Always use `--lag 60`** for real decisions
2. **Use `equal` or `value` weights** - they outperform ML-optimized
3. **Diversify** - Some high-scoring stocks still lose 50%+
4. **Quarterly rebalancing** - Matches the scoring frequency
5. **Check recurring losers** - Some companies score well but consistently underperform

## Files

| File | Purpose |
|------|---------|
| `fat_pitch_backtest.py` | Main backtest runner |
| `show_quarterly_picks.py` | Display picks per quarter |
| `dimension_ml_trainer.py` | ML feature importance |
| `historical_dimensions_backfill.py` | Calculate historical scores |

## Troubleshooting

### "No data for quarter X"

The 60-day lag requires scores from the previous quarter. Ensure you have continuous quarterly data:

```bash
python3 -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    rows = await conn.fetch('''
        SELECT score_date, COUNT(DISTINCT company_id) as n
        FROM daily_dimension_scores
        GROUP BY score_date
        HAVING COUNT(DISTINCT company_id) >= 500
        ORDER BY score_date
    ''')
    for r in rows:
        print(f'{r[\"score_date\"]}: {r[\"n\"]} companies')
    await conn.close()
asyncio.run(check())
"
```

### Memory issues during backfill

Run in smaller batches:
```bash
python historical_dimensions_backfill.py --limit 100 --frequency quarterly
```

### Slow backtest

The backtest queries price data for each company. For faster runs:
- Use `--top 10` instead of `--top 20`
- Limit date range in the code
