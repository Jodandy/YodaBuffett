# Financial Data Architecture

## Overview

Raw financial data is stored in normalized tables. Ratios and metrics are calculated on-demand, not pre-computed. This keeps the data clean and flexible.

## Data Sources

All financial statement data comes from **Yahoo Finance** via `yfinance` library.

- ~1,580 Nordic companies (SE, NO, DK, FI)
- Quarterly and annual statements
- ~5 years of history (2020-present)
- Updated by `daily_fundamentals_worker.py`

---

## Core Tables

### 1. `financial_statements` (Income Statement)
**~15,400 records** | Quarterly + Annual

| Column | Type | Description |
|--------|------|-------------|
| symbol | varchar | Yahoo ticker (e.g., VOLV-B) |
| period_date | date | End of fiscal period |
| statement_type | varchar | 'quarterly' or 'annual' |
| fiscal_year | int | e.g., 2024 |
| fiscal_quarter | int | 1-4 or NULL for annual |
| total_revenue | bigint | Top-line sales |
| gross_profit | bigint | Revenue - COGS |
| operating_income | bigint | Gross profit - OpEx |
| net_income | bigint | Bottom line |
| ebit | bigint | Earnings before interest & tax |
| ebitda | bigint | EBIT + depreciation/amortization |
| basic_eps | float | Earnings per share |
| diluted_eps | float | Diluted EPS |
| research_development | bigint | R&D expense |
| selling_general_administrative | bigint | SG&A expense |
| interest_expense | bigint | Interest paid |
| tax_expense | bigint | Income tax |
| currency | varchar | e.g., 'SEK' |
| publish_date | date | When statement was released |

---

### 2. `balance_sheet_data`
**~16,500 records** | Quarterly + Annual

| Column | Type | Description |
|--------|------|-------------|
| symbol | varchar | Yahoo ticker |
| period_date | date | As-of date |
| statement_type | varchar | 'quarterly' or 'annual' |
| **Assets** | | |
| total_assets | bigint | Everything owned |
| current_assets | bigint | Cash + receivables + inventory |
| cash_and_equivalents | bigint | Liquid cash |
| accounts_receivable | bigint | Owed by customers |
| inventory | bigint | Goods for sale |
| **Liabilities** | | |
| total_liabilities | bigint | Everything owed |
| current_liabilities | bigint | Due within 1 year |
| total_debt | bigint | All debt |
| long_term_debt | bigint | Due after 1 year |
| accounts_payable | bigint | Owed to suppliers |
| **Equity** | | |
| total_equity | bigint | Assets - liabilities |
| retained_earnings | bigint | Accumulated profits |
| shares_outstanding | bigint | Share count |
| currency | varchar | e.g., 'SEK' |

---

### 3. `cash_flow_data`
**~15,800 records** | Quarterly + Annual

| Column | Type | Description |
|--------|------|-------------|
| symbol | varchar | Yahoo ticker |
| period_date | date | Period end date |
| statement_type | varchar | 'quarterly' or 'annual' |
| **Operating** | | |
| operating_cash_flow | bigint | Cash from operations |
| depreciation_amortization | bigint | Non-cash expense add-back |
| **Investing** | | |
| investing_cash_flow | bigint | Cash for investments (usually negative) |
| capital_expenditure | bigint | CapEx (usually negative) |
| **Financing** | | |
| financing_cash_flow | bigint | Debt/equity transactions |
| dividends_paid | bigint | Cash to shareholders (negative) |
| **Summary** | | |
| free_cash_flow | bigint | Operating - CapEx |
| net_income | bigint | (duplicated from income statement) |

---

### 4. `daily_price_data`
**~4.8M records** | Daily

| Column | Type | Description |
|--------|------|-------------|
| symbol | varchar | Yahoo ticker |
| date | date | Trading date |
| open_price | numeric | Opening price |
| high_price | numeric | Daily high |
| low_price | numeric | Daily low |
| close_price | numeric | Closing price |
| adjusted_close | numeric | Adjusted for splits/dividends |
| volume | bigint | Shares traded |
| daily_return | numeric | Day-over-day return |
| volatility_5d | numeric | 5-day rolling volatility |
| volatility_20d | numeric | 20-day rolling volatility |
| company_id | uuid | Link to company_master |

---

## Calculable Metrics

These are **not stored** - calculate on-demand from raw data.

### Profitability Ratios
```sql
-- Gross margin
gross_profit / total_revenue

-- Operating margin
operating_income / total_revenue

-- Net profit margin
net_income / total_revenue

-- EBITDA margin
ebitda / total_revenue
```

### Return Metrics
```sql
-- ROE (Return on Equity)
net_income / total_equity

-- ROA (Return on Assets)
net_income / total_assets

-- ROIC (Return on Invested Capital)
ebit * (1 - tax_rate) / (total_equity + total_debt - cash_and_equivalents)
```

### Liquidity Ratios
```sql
-- Current ratio
current_assets / current_liabilities

-- Quick ratio (acid test)
(current_assets - inventory) / current_liabilities

-- Cash ratio
cash_and_equivalents / current_liabilities
```

### Leverage Ratios
```sql
-- Debt to equity
total_debt / total_equity

-- Debt to assets
total_debt / total_assets

-- Interest coverage
ebit / interest_expense
```

### Efficiency Ratios
```sql
-- Asset turnover
total_revenue / total_assets

-- Inventory turnover
total_revenue / inventory  -- (or COGS if available)

-- Receivables turnover
total_revenue / accounts_receivable
```

### Cash Flow Metrics
```sql
-- FCF margin
free_cash_flow / total_revenue

-- Cash conversion
operating_cash_flow / net_income

-- CapEx ratio
capital_expenditure / total_revenue
```

### Valuation Ratios (requires price data)
```sql
-- Market cap
close_price * shares_outstanding

-- P/E ratio
close_price / (net_income / shares_outstanding)

-- P/B ratio
close_price / (total_equity / shares_outstanding)

-- EV/EBITDA
(market_cap + total_debt - cash_and_equivalents) / ebitda
```

---

## Historical Analysis

Since we have ~5 years of quarterly data, we can compute:

### Trend Metrics
- Revenue growth (YoY, QoQ, CAGR)
- Margin trends (expanding vs contracting)
- Debt trajectory (deleveraging vs leveraging)

### Stability Metrics
- Margin volatility (std dev over time)
- Earnings consistency
- Cash flow predictability

### Quality Signals
- Consistent positive FCF
- Stable or improving ROE
- Reasonable payout ratios

---

## Query Patterns

### Get latest financials for a company
```sql
SELECT * FROM financial_statements
WHERE symbol = 'VOLV-B'
ORDER BY period_date DESC
LIMIT 1;
```

### Get financials as of a specific date
```sql
SELECT * FROM financial_statements
WHERE symbol = 'VOLV-B'
AND period_date <= '2024-06-30'
ORDER BY period_date DESC
LIMIT 1;
```

### Calculate ROE for all companies (latest)
```sql
SELECT DISTINCT ON (fs.symbol)
    fs.symbol,
    fs.net_income::float / NULLIF(bs.total_equity, 0) as roe
FROM financial_statements fs
JOIN balance_sheet_data bs
    ON fs.symbol = bs.symbol
    AND fs.period_date = bs.period_date
WHERE fs.statement_type = 'annual'
ORDER BY fs.symbol, fs.period_date DESC;
```

### Get 3-year revenue history
```sql
SELECT symbol, period_date, total_revenue
FROM financial_statements
WHERE symbol = 'VOLV-B'
AND statement_type = 'annual'
AND period_date >= CURRENT_DATE - INTERVAL '3 years'
ORDER BY period_date;
```

---

## Data Refresh

| Data | Schedule | Worker |
|------|----------|--------|
| Financial statements | Daily at 3:45 AM | `daily_fundamentals_worker.py` |
| Price data | Daily at 3:00 AM | `daily_market_data_worker.py` |

Note: Statement data only changes quarterly, but we check daily to catch new releases.

---

## Backfill Scripts

For initial setup or catching up on missing data:

### Fundamentals Backfill
```bash
# All companies (~1,800)
python historical_fundamentals_backfill.py

# Only companies missing data
python historical_fundamentals_backfill.py --only-missing

# Limited batch (for testing)
python historical_fundamentals_backfill.py --limit 50
```

Fetches 3 components per company from Yahoo Finance:
- Financial statements (income statement)
- Balance sheet data
- Cash flow data

### Price Data Backfill
```bash
# All companies - max available history (up to 20+ years)
python ingest_all_max_history.py

# Only companies missing price data
python ingest_all_max_history.py --only-missing

# Limited batch (for testing)
python ingest_all_max_history.py --limit 50
```

Note: Both scripts exclude today's date to only store confirmed closing prices.

### Error Tracking

Both backfill scripts save detailed results to JSON files:

| Script | Output File | Location |
|--------|-------------|----------|
| Fundamentals | `fundamentals_backfill_YYYYMMDD_HHMMSS.json` | `data/` |
| Price data | `max_history_ingestion_YYYYMMDD_HHMMSS.json` | `backend/` |

**Check failures:**
```bash
# Fundamentals - companies with no Yahoo data
cat data/fundamentals_backfill_*.json | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Skipped (no data): {len(data[\"skipped_no_data\"])}')
for item in data['skipped_no_data'][:5]:
    print(f'  {item[\"symbol\"]}')"

# Price data - wrong tickers needing manual resolution
cat max_history_ingestion_*.json | python3 -c "
import sys, json
data = json.load(sys.stdin)
print(f'Wrong tickers: {len(data[\"failed_wrong_ticker\"])}')
for item in data['failed_wrong_ticker'][:5]:
    print(f'  {item[\"primary_ticker\"]} -> {item[\"yahoo_symbol\"]}')"
```

**Daily worker logs:**
```bash
# LaunchAgent logs location
ls -la ~/Documents/YodaBuffett/logs/

# Check recent errors
tail -100 ~/Documents/YodaBuffett/logs/daily-market-data-worker-error.log
```

---

## Removed Tables

These pre-computed ratio tables were removed (Feb 2026) in favor of on-demand calculation:

- `historical_fundamentals_daily` (was 1.37M rows)
- `daily_fundamentals` (was 7 rows)
- `yahoo_fundamentals` (was 10 rows)

Rationale: Fundamentals change quarterly, not daily. Pre-computing daily ratios was wasteful. Calculate on-demand instead.
