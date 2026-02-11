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

---

## Dimension Skewness Analysis (Feb 2026)

### Methodology

For each dimension independently:
1. Rank all companies by dimension score on each score date
2. Split into quintiles (Q1=lowest 20%, Q5=highest 20%)
3. Measure 21-day forward returns per quintile
4. Calculate Q5-Q1 spread (positive = high score wins)

### Results Summary

| Dimension | Q1 Return | Q5 Return | Spread | Signal |
|-----------|-----------|-----------|--------|--------|
| **growth** | -0.9% | +16.4% | **+17.3%** | ✓ High growth wins |
| value* | -1.2% | +3.3% | +4.5% | *(broken - see below)* |
| financial_health | -0.7% | -0.9% | -0.2% | No signal |
| valuation_percentile | +2.2% | +2.0% | -0.3% | No signal |
| momentum | +2.2% | +0.7% | -1.5% | Slight contrarian |
| beneish_mscore | +2.0% | -0.7% | -2.7% | Contrarian |
| capital_allocation | +6.7% | +0.5% | -6.2% | Contrarian |
| quality | +7.0% | -0.5% | **-7.5%** | Contrarian |
| working_capital | +7.4% | -0.3% | -7.7% | Contrarian |
| returns | +7.9% | -0.3% | **-8.2%** | Contrarian |
| risk | +9.0% | -0.3% | **-9.3%** | Risky wins |
| earnings_quality | +9.3% | -0.4% | **-9.8%** | Contrarian |
| profitability | +10.3% | -0.5% | **-10.8%** | Contrarian |

### Key Finding: Quality Dimensions Work in Reverse

Most "quality" dimensions have **negative spreads** - the lowest-scoring companies (worst fundamentals) have the highest forward returns. Interpretation:

1. **Quality is priced in** - high quality stocks are expensive, limiting upside
2. **Low quality = turnaround potential** - beaten down stocks can recover
3. **Crowded trade** - everyone chasing compounders drove up valuations

### Scripts

```bash
# Run dimension skewness analysis
python backtest_dimension_skewness.py --holding-days 21 --output dimension_skewness.xlsx

# Test different holding periods
python backtest_dimension_skewness.py --holding-days 126
```

---

## Quality + Mean Reversion Backtest (Feb 2026)

### Hypothesis

Buy quality companies when they're cheap relative to their own 5-year history.

### Strategy Definition

1. **Quality filter**: Composite >= 60
   - profitability (30%), returns (25%), earnings_quality (25%), financial_health (20%)
2. **Valuation filter**: `valuation_percentile` score

### CRITICAL: Valuation Percentile Score Interpretation

The score is **INVERTED** from raw percentile:

| Score | Meaning | Example |
|-------|---------|---------|
| **90** | At 10th percentile of own history | **CHEAP** (beaten down) |
| **50** | At 50th percentile | Fair value |
| **10** | At 90th percentile of own history | **EXPENSIVE** (momentum) |

### Results (126-day holding period)

| Strategy | Trades | Avg Return | Win Rate |
|----------|--------|------------|----------|
| Cheap Quality (score >= 70) | 2,044 | **-1.65%** | 42.3% |
| Expensive Quality (score <= 30) | 1,333 | **+4.15%** | 55.0% |

**Spread: -5.81%** (cheap underperforms expensive)
**Monthly Win Rate: 3%** (cheap won only 1 of 30 months)

### Conclusion: Mean Reversion Does NOT Work

Quality stocks that are cheap vs their own history (high valuation_percentile score) continue to **underperform**. Quality stocks that are expensive (low score, momentum) continue to **outperform**.

This is **momentum, not mean reversion**.

### Holding Period Analysis

| Period | Cheap Quality | Expensive Quality | Spread | Win Rate |
|--------|---------------|-------------------|--------|----------|
| 21 days | -0.72% | +0.38% | -1.10% | 30% |
| 42 days | -0.77% | +1.30% | -2.06% | 27% |
| 63 days | -0.95% | +2.02% | -2.97% | 13% |
| 126 days | -1.65% | +4.15% | -5.81% | 3% |

The momentum signal gets **stronger** with longer holding periods.

### Scripts

```bash
# Run quality + mean reversion backtest
python backtest_quality_meanreversion.py --holding-days 126

# Test different thresholds
python backtest_quality_meanreversion.py --quality-threshold 70 --valuation-threshold 20
```

---

## Value Calculator Bug (Fixed Feb 2026)

### Problem

`value_calculator.py` was using `period_date` instead of `publish_date` when matching prices to financial statements:

```python
# WRONG - uses period_date (Q4 = Dec 31)
WHERE period_date <= $score_date

# CORRECT - uses publish_date (actual release, e.g., Feb 15)
WHERE COALESCE(publish_date, period_date + 75 days) <= $score_date
```

### Impact

- Valuations only "updated" once per year when new annual report used
- Price changes during the year were ignored
- Caused artificial jumps in value dimension

### Fix Applied

Updated `_get_historical_financials` and `_calculate_valuation_metrics` to:
1. Return `effective_date = COALESCE(publish_date, period_date + 75 days)`
2. Match prices to `effective_date` instead of `period_date`

### Backfill

```bash
# Parallel recalculation (4 processes, ~4 hours)
./backfill_value_parallel.sh

# Monitor progress
tail -f /tmp/value_backfill_*.log
```

Until backfill completes, use `valuation_percentile` instead of `value`.

---

## Score Freshness

Dimension scores can become stale after major price moves. Example:

**Novo Nordisk (Feb 2026)**:
- Jan 31 momentum score: 77 (stock had rallied from Dec lows)
- Feb 5 price: 280 SEK (-24% after earnings crash)
- Momentum score was correct when calculated, but doesn't reflect post-crash reality

**Recommendation**: Recalculate scores after major price moves (>10% in a week).

---

## Current Stock Lists (Jan 2026)

### Cheap Quality (historically underperforms)

Quality >= 60, Valuation Percentile >= 70 (cheap vs own history)

| Ticker | Company | Quality | Val% | Sector |
|--------|---------|---------|------|--------|
| EVO | Evolution | 77 | 100 | Gaming |
| TRUE B | Truecaller | 81 | 100 | Telecom |
| NOVO B | Novo Nordisk | 69 | 88 | Pharma |
| BETS-B | Betsson | 75 | 90 | Gaming |
| HEXA-B | Hexagon | 68 | 91 | Industrial |

*85 total companies in this bucket*

### Expensive Quality (historically outperforms)

Quality >= 60, Valuation Percentile <= 30 (expensive = momentum)

| Ticker | Company | Quality | Val% | Mom | Sector |
|--------|---------|---------|------|-----|--------|
| AQ | AQ Group | 73 | 0 | 66 | Industrial |
| SAAB B | Saab | 61 | 0 | 84 | Defense |
| KOG | Kongsberg | 71 | 7 | 96 | Defense |
| LUG | Lundin Gold | 75 | 8 | 35 | Mining |
| SAVE | Nordnet | 71 | 3 | 86 | Finance |

*67 total companies in this bucket*

---

## Actionable Takeaways

### What Works
1. **Growth dimension** - only consistent positive signal (+17% spread)
2. **Expensive quality / momentum** - quality stocks at high valuations continue winning
3. **Longer holding periods** - signals strengthen at 3-6 months

### What Doesn't Work
1. **Mean reversion on quality** - cheap quality continues underperforming
2. **Most quality dimensions alone** - contrarian (low quality wins)
3. **Short holding periods** - 21 days is mostly noise

### Potential Improvements to Test
1. **Growth + Momentum combo**: High growth + price momentum
2. **Momentum filter**: Only buy cheap quality AFTER momentum turns positive
3. **Sector exclusions**: Avoid pharma/IT during disruption periods
