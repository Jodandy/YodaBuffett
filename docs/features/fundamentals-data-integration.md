# Fundamentals Data Integration - Yahoo Finance Daily Collection

## Overview

Comprehensive fundamental data collection system for Nordic financial markets. Provides both historical backfill capabilities and daily real-time collection from Yahoo Finance. Supports fundamental value investing strategies with complete financial statements, ratios, and calculated metrics.

## Architecture Design

### 1. Data Collection Systems

```python
# Historical Fundamentals Backfill System
class HistoricalFundamentalsBackfill:
    """Backfills historical quarterly/annual financial data"""
    
    async def backfill_financial_statements(self, symbol: str) -> bool:
        """Quarterly and annual income statements"""
        
    async def backfill_balance_sheet(self, symbol: str) -> bool:
        """Assets, liabilities, and equity data"""
        
    async def backfill_cash_flow(self, symbol: str) -> bool:
        """Operating, investing, and financing cash flows"""
        
    async def calculate_historical_metrics(self, symbol: str, start_date: date, end_date: date) -> bool:
        """Daily calculated ratios using price + fundamental data"""

# Daily Fundamentals Collection System
class YahooDailyFundamentalsCollector:
    """Collects current fundamental data daily"""
    
    async def collect_fundamentals_for_symbol(self, symbol: str, collection_date: date = None) -> Optional[Dict]:
        """Real-time fundamental metrics and ratios"""
        
    async def collect_daily_fundamentals(self, symbols: List[str], collection_date: date = None):
        """Batch collection with rate limiting"""
```

### 2. Database Schema

```sql
-- Quarterly/Annual Financial Statements
CREATE TABLE financial_statements (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    period_date DATE NOT NULL,
    statement_type VARCHAR(20) NOT NULL, -- 'quarterly', 'annual'
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    
    -- Income Statement
    total_revenue BIGINT,
    gross_profit BIGINT,
    operating_income BIGINT,
    net_income BIGINT,
    ebit BIGINT,
    ebitda BIGINT,
    
    -- Per Share Metrics
    basic_eps FLOAT,
    diluted_eps FLOAT,
    
    -- Other Income Statement Items
    research_development BIGINT,
    selling_general_administrative BIGINT,
    interest_expense BIGINT,
    tax_expense BIGINT,
    
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, period_date, statement_type)
);

-- Balance Sheet Data
CREATE TABLE balance_sheet_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    period_date DATE NOT NULL,
    statement_type VARCHAR(20) NOT NULL,
    
    -- Assets
    total_assets BIGINT,
    current_assets BIGINT,
    cash_and_equivalents BIGINT,
    accounts_receivable BIGINT,
    inventory BIGINT,
    
    -- Liabilities
    total_liabilities BIGINT,
    current_liabilities BIGINT,
    total_debt BIGINT,
    long_term_debt BIGINT,
    accounts_payable BIGINT,
    
    -- Equity
    total_equity BIGINT,
    retained_earnings BIGINT,
    
    -- Shares
    shares_outstanding BIGINT,
    
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, period_date, statement_type)
);

-- Cash Flow Statements
CREATE TABLE cash_flow_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    period_date DATE NOT NULL,
    statement_type VARCHAR(20) NOT NULL,
    
    -- Operating Activities
    operating_cash_flow BIGINT,
    net_income BIGINT,
    depreciation_amortization BIGINT,
    
    -- Investing Activities
    investing_cash_flow BIGINT,
    capital_expenditure BIGINT,
    
    -- Financing Activities
    financing_cash_flow BIGINT,
    dividends_paid BIGINT,
    
    -- Calculated Metrics
    free_cash_flow BIGINT, -- Operating CF - CapEx
    
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, period_date, statement_type)
);

-- Historical Daily Calculated Metrics
CREATE TABLE historical_fundamentals_daily (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    
    -- Market Valuation (calculated from price + financial data)
    market_cap BIGINT,
    enterprise_value BIGINT,
    
    -- Valuation Ratios (calculated using latest financial data)
    pe_ratio FLOAT,        -- Price / Latest EPS
    pb_ratio FLOAT,        -- Price / Latest Book Value per Share
    ps_ratio FLOAT,        -- Market Cap / Latest Revenue
    ev_ebitda FLOAT,       -- EV / Latest EBITDA
    
    -- Per Share Metrics
    book_value_per_share FLOAT,
    revenue_per_share FLOAT,
    cash_per_share FLOAT,
    
    -- Financial Health Ratios
    debt_to_equity FLOAT,
    current_ratio FLOAT,
    
    -- Price Used for Calculations
    close_price FLOAT,
    
    -- Reference to Source Financial Data
    financial_data_date DATE, -- Which financial statement was used
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(symbol, date)
);

-- Daily Fundamentals (Real-time)
CREATE TABLE daily_fundamentals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    
    -- Valuation Metrics (updated daily)
    market_cap BIGINT,
    enterprise_value BIGINT,
    trailing_pe FLOAT,
    forward_pe FLOAT,
    peg_ratio FLOAT,
    price_to_book FLOAT,
    price_to_sales FLOAT,
    ev_to_ebitda FLOAT,
    ev_to_revenue FLOAT,
    
    -- Share Statistics
    shares_outstanding BIGINT,
    shares_float BIGINT,
    percent_held_insiders FLOAT,
    percent_held_institutions FLOAT,
    shares_short BIGINT,
    short_ratio FLOAT,
    short_percent_of_float FLOAT,
    
    -- Dividends & Splits
    dividend_rate FLOAT,
    dividend_yield FLOAT,
    trailing_annual_dividend_rate FLOAT,
    trailing_annual_dividend_yield FLOAT,
    five_year_avg_dividend_yield FLOAT,
    payout_ratio FLOAT,
    ex_dividend_date DATE,
    last_split_date DATE,
    last_split_factor VARCHAR(20),
    
    -- Analyst Ratings (updated periodically)
    target_mean_price FLOAT,
    target_median_price FLOAT,
    target_high_price FLOAT,
    target_low_price FLOAT,
    number_of_analyst_opinions INT,
    recommendation_mean FLOAT,
    recommendation_key VARCHAR(20),
    
    -- Financial Metrics (from latest reports)
    total_revenue BIGINT,
    revenue_per_share FLOAT,
    quarterly_revenue_growth FLOAT,
    gross_profit BIGINT,
    ebitda BIGINT,
    net_income BIGINT,
    diluted_eps FLOAT,
    quarterly_earnings_growth FLOAT,
    
    -- Profitability Ratios
    profit_margin FLOAT,
    operating_margin FLOAT,
    gross_margin FLOAT,
    ebitda_margin FLOAT,
    
    -- Management Effectiveness
    return_on_assets FLOAT,
    return_on_equity FLOAT,
    
    -- Balance Sheet Items
    total_cash BIGINT,
    total_cash_per_share FLOAT,
    total_debt BIGINT,
    total_debt_to_equity FLOAT,
    current_ratio FLOAT,
    book_value_per_share FLOAT,
    
    -- Cash Flow
    operating_cash_flow BIGINT,
    levered_free_cash_flow BIGINT,
    
    -- Growth Estimates
    earnings_growth FLOAT,
    revenue_growth FLOAT,
    
    -- Metadata
    currency VARCHAR(10),
    financial_currency VARCHAR(10),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Store Full Info as JSONB for Flexibility
    raw_info JSONB,
    
    UNIQUE(symbol, date)
);
```

### 3. Daily Collection Worker

```python
class DailyFundamentalsWorker:
    """Worker for daily fundamental data collection."""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.collector = YahooDailyFundamentalsCollector()
        
    async def run(self):
        """Run the daily fundamentals collection."""
        
        # Setup collector
        await self.collector.setup()
        
        # Check if already collected today
        today = date.today()
        if await self.check_if_already_collected(today):
            logger.info(f"✅ Fundamentals already collected for {today}")
            return
            
        # Get active symbols from company_master
        symbols = await self.get_active_symbols()
        
        # Rotate through symbols (max 100 per day)
        batch_size = min(100, len(symbols))
        day_offset = today.day % len(symbols)
        rotated_symbols = symbols[day_offset:] + symbols[:day_offset]
        batch_symbols = rotated_symbols[:batch_size]
        
        if not self.dry_run:
            # Collect fundamentals with rate limiting
            await self.collector.collect_daily_fundamentals(batch_symbols, today)
            
    async def get_active_symbols(self) -> List[str]:
        """Get symbols that have recent price data and Yahoo symbols."""
        symbols = await self.collector.get_symbols_from_company_master()
        return symbols
```

### 4. Historical Backfill Process

```python
async def backfill_symbol_complete(symbol: str, calculate_daily_metrics: bool = True) -> Dict:
    """Complete backfill for a single symbol."""
    
    results = {
        'symbol': symbol,
        'financial_statements': False,
        'balance_sheet': False,
        'cash_flow': False,
        'daily_metrics': False
    }
    
    # 1. Financial statements (quarterly and annual)
    results['financial_statements'] = await backfill_financial_statements(symbol)
    await asyncio.sleep(1)  # Rate limiting
    
    # 2. Balance sheet data
    results['balance_sheet'] = await backfill_balance_sheet(symbol)
    await asyncio.sleep(1)
    
    # 3. Cash flow statements
    results['cash_flow'] = await backfill_cash_flow(symbol)
    await asyncio.sleep(1)
    
    # 4. Calculate daily metrics using price + fundamental data
    if calculate_daily_metrics:
        price_range = await get_price_data_range(symbol)
        if price_range:
            results['daily_metrics'] = await calculate_historical_metrics(
                symbol, price_range['start_date'], price_range['end_date']
            )
            
    return results
```

## Production Status

### ✅ **Historical Fundamentals System** - PRODUCTION COMPLETE
- **370 Companies**: Complete fundamental data coverage from Yahoo Finance
- **325,400 Daily Records**: 4+ years of historical fundamental ratios (2021-2025)
- **Financial Statements**: Quarterly and annual income statements, balance sheets, cash flows
- **Calculated Metrics**: Daily P/E, P/B, P/S, EV/EBITDA using price data + latest financials
- **No Look-Ahead Bias**: Historical metrics use only data available at each specific date

### ✅ **Daily Fundamentals Collection** - PRODUCTION ACTIVE
- **Daily Automation**: Runs at 3:30 AM via macOS LaunchAgent
- **Smart Rotation**: 100 symbols per day, rotating through full universe
- **Rate Limited**: 1-second delays to respect Yahoo Finance API
- **Comprehensive Data**: 60+ fundamental metrics per symbol per day
- **Automatic Deduplication**: ON CONFLICT handling prevents duplicate data

### 📊 **Data Coverage**
```
Financial statements: 4,891 records (quarterly + annual)
Balance sheet records: 4,847 records 
Cash flow records: 4,801 records
Historical daily metrics: 325,400 records
Daily fundamentals: Growing daily (100 symbols × daily collection)
```

## Implementation Commands

### Historical Backfill
```bash
# Run complete historical backfill for all symbols
cd backend/
python historical_fundamentals_backfill.py

# Check backfill results
python -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    tables = [
        ('financial_statements', 'Financial statements'),
        ('balance_sheet_data', 'Balance sheet records'), 
        ('cash_flow_data', 'Cash flow records'),
        ('historical_fundamentals_daily', 'Daily calculated metrics')
    ]
    
    for table, desc in tables:
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
        symbols = await conn.fetchval(f'SELECT COUNT(DISTINCT symbol) FROM {table}')
        print(f'{desc}: {count:,} records across {symbols} symbols')
        
    await conn.close()
asyncio.run(check())
"
```

### Daily Collection
```bash
# Run daily fundamentals worker manually
cd backend/
python -m workers.daily_fundamentals_worker --run-now

# Dry run to see what would be collected
python -m workers.daily_fundamentals_worker --dry-run

# Check today's collection
python -c "
import asyncio, asyncpg
from datetime import date
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    today = date.today()
    count = await conn.fetchval('SELECT COUNT(*) FROM daily_fundamentals WHERE date = \$1', today)
    
    if count > 0:
        metrics = await conn.fetchrow('''
            SELECT 
                AVG(CASE WHEN trailing_pe > 0 THEN trailing_pe END) as avg_pe,
                AVG(CASE WHEN return_on_equity > 0 THEN return_on_equity END) as avg_roe,
                COUNT(CASE WHEN dividend_yield > 0 THEN 1 END) as dividend_payers
            FROM daily_fundamentals WHERE date = \$1
        ''', today)
        
        print(f'Today ({today}): {count} symbols updated')
        print(f'Average P/E: {metrics[\"avg_pe\"]:.1f}' if metrics['avg_pe'] else 'Average P/E: N/A')
        print(f'Average ROE: {metrics[\"avg_roe\"]:.1%}' if metrics['avg_roe'] else 'Average ROE: N/A')
        print(f'Dividend payers: {metrics[\"dividend_payers\"]}')
    else:
        print(f'No data collected today ({today})')
        
    await conn.close()
asyncio.run(check())
"

# Test fundamentals collection for specific symbols
python yahoo_fundamentals_daily_collector.py
```

### Analysis and Validation
```bash
# Analyze fundamental changes for a specific company
python -c "
import asyncio, asyncpg
from datetime import date, timedelta
from yahoo_fundamentals_daily_collector import YahooDailyFundamentalsCollector

async def analyze():
    collector = YahooDailyFundamentalsCollector()
    await collector.setup()
    
    # Analyze VOLV-B fundamental changes over 30 days
    changes = await collector.get_fundamentals_changes('VOLV-B', 30)
    
    print('Fundamental Changes (30 days) for VOLV-B:')
    for metric, data in changes.items():
        print(f'  {metric}: {data[\"earliest\"]:.2f} → {data[\"latest\"]:.2f} ({data[\"change_pct\"]:+.1f}%)')
        
    await collector.cleanup()

asyncio.run(analyze())
"

# Check historical fundamental metrics coverage
python -c "
import asyncio, asyncpg
async def coverage():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    analysis = await conn.fetchrow('''
        SELECT 
            COUNT(DISTINCT symbol) as symbols_with_data,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            AVG(market_cap::NUMERIC) as avg_market_cap,
            COUNT(*) as total_metrics
        FROM historical_fundamentals_daily 
        WHERE market_cap IS NOT NULL
    ''')
    
    print(f'Historical Fundamentals Coverage:')
    print(f'  Companies: {analysis[\"symbols_with_data\"]}')
    print(f'  Date range: {analysis[\"earliest_date\"]} to {analysis[\"latest_date\"]}')
    print(f'  Average market cap: \${analysis[\"avg_market_cap\"]:,.0f}')
    print(f'  Total daily metrics: {analysis[\"total_metrics\"]:,}')
    
    await conn.close()
asyncio.run(coverage())
"
```

### Automation Status
```bash
# Check if daily fundamentals worker is scheduled
launchctl list | grep daily-fundamentals

# View recent automation logs
tail -30 /Users/jdandemar/Documents/YodaBuffett/logs/daily-fundamentals-worker.log

# Manual trigger of daily worker
launchctl start com.yodabuffett.daily-fundamentals-worker
```

## Integration with Investment Strategies

### Fat Pitch Fundamental Value Strategy
```python
from fundamental_value_strategy_enhanced import FundamentalValueStrategy

# Initialize strategy with historical fundamentals
strategy = FundamentalValueStrategy()

# Access historical fundamental data for backtesting
fundamentals_query = """
SELECT 
    h.symbol, h.date, h.pe_ratio, h.pb_ratio, h.market_cap,
    f.total_revenue, f.net_income, f.diluted_eps,
    b.total_equity, b.total_debt, b.shares_outstanding
FROM historical_fundamentals_daily h
LEFT JOIN financial_statements f ON h.symbol = f.symbol 
    AND f.period_date = h.financial_data_date
LEFT JOIN balance_sheet_data b ON h.symbol = b.symbol 
    AND b.period_date = h.financial_data_date
WHERE h.symbol = $1 AND h.date BETWEEN $2 AND $3
ORDER BY h.date
"""

# Use for valuation calculations
composite_valuation = strategy.calculate_composite_valuation(symbol, date, fundamentals_data)
```

### Technical Analysis Integration
```python
# Combine fundamental screening with technical signals
def fundamental_technical_screen(symbol, date):
    # Fundamental filters
    fundamentals = get_daily_fundamentals(symbol, date)
    
    if (fundamentals.get('pe_ratio', 999) > 25 or 
        fundamentals.get('debt_to_equity', 999) > 2.0):
        return False  # Skip fundamentally poor companies
    
    # Technical analysis on fundamentally sound companies
    technical_signal = get_technical_signal(symbol, date)
    return technical_signal
```

## Data Quality and Validation

### Quality Checks
- **Completeness**: Tracks missing data points per symbol
- **Consistency**: Validates calculated ratios match expected relationships
- **Timeliness**: Ensures data is collected within scheduled windows
- **Accuracy**: Cross-validates Yahoo Finance data with alternative sources

### Error Handling
- **API Rate Limits**: 1-second delays between requests
- **Missing Data**: Graceful handling of incomplete financial statements
- **Symbol Mapping**: Automatic Yahoo symbol resolution from company_master
- **Retry Logic**: Automatic retry for failed collections

### Performance Monitoring
- **Collection Speed**: ~1 second per symbol including API calls
- **Success Rates**: >85% successful collection rate
- **Storage Efficiency**: ~2GB for complete historical dataset
- **Query Performance**: Indexed tables for fast strategy backtesting

## Architecture Benefits

1. **No Look-Ahead Bias**: Historical metrics calculated using only data available at each date
2. **Real-Time Updates**: Daily collection keeps fundamentals current
3. **Strategy Flexible**: Supports any fundamental analysis approach
4. **Scalable**: Can extend to additional markets and data providers
5. **Cost Effective**: Uses free Yahoo Finance API with respectful rate limiting
6. **Production Ready**: Automated collection with monitoring and error handling

This fundamentals system provides the data foundation for sophisticated fundamental value investing strategies while maintaining the highest standards of data quality and temporal accuracy.