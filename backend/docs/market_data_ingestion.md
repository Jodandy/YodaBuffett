# Market Data Ingestion System

## Overview

The Market Data Ingestion System provides comprehensive historical market data collection for all Nordic companies. It's designed to handle large-scale ingestion (1,606+ companies) with maximum historical coverage (up to 20+ years per company) and robust error handling.

## Architecture

### Domain Location
The ingestion system follows our hexagonal architecture and is part of the **Market Data Domain**:
```
backend/domains/market_data/
├── services/
│   ├── historical_data_ingestor.py    # Core ingestion service (domain-based)
│   └── yahoo_provider.py              # Provider-agnostic Yahoo Finance interface
```

### Root-Level Ingestion Scripts
For ease of use, production ingestion scripts are at the backend root:
```
backend/
├── ingest_787_fixed.py                # Original ingestion script (1,606 companies now)
├── ingest_all_max_history.py          # Maximum historical data ingestion
├── simple_price_ingestion.py          # Conservative testing approach
├── fix_tickers_from_company_list.py   # Swedish ticker mapping fixes
├── complete_fix.py                    # Combined constraint + ticker fixes
└── company-list.json                  # Swedish companies reference data
```

## Ingestion Scripts

### 1. Maximum Historical Data Ingestion (`ingest_all_max_history.py`)

**Purpose**: Get ALL available historical data for ALL companies - no skipping!

**Features**:
- Processes all 1,606 companies from `company_master` table
- Gets maximum available history (up to 20+ years per company)
- No data limits or skipping based on existing data
- Comprehensive error tracking and categorization
- Priority-based processing (high-document companies first)
- Detailed progress reporting every 20 companies

**Usage**:
```bash
python3 ingest_all_max_history.py
```

**Expected Results**:
- **High-priority companies**: 4,000-5,000+ price points each (15-20 years)
- **Medium companies**: 1,500-4,000 price points each (5-15 years)
- **Total dataset**: 500,000+ to 1,000,000+ price points
- **Duration**: 30-60 minutes depending on data availability

**Output**:
- JSON results file: `max_history_ingestion_YYYYMMDD_HHMMSS.json`
- Comprehensive success/failure categorization
- Performance metrics and processing statistics

### 2. Conservative Testing (`simple_price_ingestion.py`)

**Purpose**: Quick testing with top 50 companies and 1 year of data

**Features**:
- Limited to 50 companies with highest document counts
- 1 year of historical data per company
- Minimal constraint checking
- Fast execution for testing database connectivity

**Usage**:
```bash
python3 simple_price_ingestion.py
```

### 3. Ticker Mapping Fixes (`fix_tickers_from_company_list.py`)

**Purpose**: Fix Swedish ticker mappings using authoritative company-list.json

**Features**:
- Uses fuzzy string matching to map company names
- Converts Swedish tickers to Yahoo Finance format (TICKER → TICKER.ST)
- Handles special cases (SDB shares, spaces → dashes)
- Updates `symbol_confidence` levels based on match quality
- Prioritizes companies with high document counts

**Usage**:
```bash
python3 fix_tickers_from_company_list.py
```

**Key Mappings**:
- Swedish format: "ASSA B" → Yahoo format: "ASSA-B.ST"
- Special cases: "ALIV SDB" → "ALIV-SDB.ST"
- ISIN codes preserved from company-list.json

### 4. Database Constraint Fixes (`complete_fix.py`)

**Purpose**: Combined fix for database constraints + ticker mappings

**Features**:
- Removes foreign key constraints blocking price data insertion
- Applies Swedish ticker fixes in one operation
- Validates database connectivity with test insertions
- Comprehensive fix reporting

## Data Sources and Providers

### Primary: Yahoo Finance API
- **Coverage**: Global markets including Nordic (.ST, .OL, .CO, .HE suffixes)
- **Historical Range**: Up to 20+ years depending on listing date
- **Rate Limits**: ~2000 requests/hour (free tier)
- **Data Quality**: Good for major markets, some gaps in smaller companies
- **Cost**: Free with attribution requirements

### Swedish Market Data (`company-list.json`)
- **Source**: Authoritative Swedish market data
- **Contains**: 1,400+ Swedish companies with proper ticker mappings
- **Fields**: ticker, name, ISIN, market cap classification, sector
- **Usage**: Ticker validation and symbol resolution for Swedish companies

## Database Schema

### Core Tables

#### `daily_price_data`
```sql
CREATE TABLE daily_price_data (
    symbol VARCHAR(20) NOT NULL,           -- Company primary ticker
    date DATE NOT NULL,                    -- Trading date
    open_price DECIMAL(12, 4) NOT NULL,    -- Opening price
    high_price DECIMAL(12, 4) NOT NULL,    -- Daily high
    low_price DECIMAL(12, 4) NOT NULL,     -- Daily low
    close_price DECIMAL(12, 4) NOT NULL,   -- Closing price
    adjusted_close DECIMAL(12, 4),         -- Dividend/split adjusted close
    volume BIGINT,                         -- Trading volume
    provider VARCHAR(50) NOT NULL,         -- Data provider ('yahoo_finance')
    company_id UUID,                       -- Reference to company_master
    created_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (symbol, date, provider)
);
```

#### `company_master`
```sql
CREATE TABLE company_master (
    id UUID PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    primary_ticker VARCHAR(20),            -- Main trading symbol
    yahoo_symbol VARCHAR(30),              -- Yahoo Finance symbol format
    isin_code VARCHAR(20),                 -- International identifier
    symbol_confidence VARCHAR(20),         -- 'high', 'medium', 'low', 'unknown'
    document_count INTEGER DEFAULT 0,      -- Number of documents available
    data_quality_score DECIMAL(3,2),       -- 0.0 to 1.0 quality score
    country VARCHAR(2) DEFAULT 'SE',       -- ISO country code
    primary_exchange VARCHAR(50),          -- Primary exchange
    updated_at TIMESTAMP DEFAULT NOW()
);
```

## Error Handling and Recovery

### Error Categories

1. **Wrong Tickers**: Companies with invalid or outdated Yahoo symbols
   - **Resolution**: Run ticker mapping fixes using company-list.json
   - **Common causes**: Mergers, delistings, symbol changes

2. **Database Constraints**: Foreign key violations preventing insertion
   - **Resolution**: Run constraint fix scripts
   - **Common causes**: Missing company_master references

3. **No Data Available**: Yahoo Finance returns empty datasets
   - **Resolution**: Manual research for alternative symbols or delisting confirmation
   - **Common causes**: Delisted companies, incorrect symbols

4. **Network/API Errors**: Temporary Yahoo Finance API issues
   - **Resolution**: Retry with exponential backoff
   - **Common causes**: Rate limiting, network timeouts

### Recovery Procedures

1. **Re-run Failed Companies**:
   ```bash
   # After fixing tickers
   python3 fix_tickers_from_company_list.py
   python3 ingest_all_max_history.py
   ```

2. **Database Constraint Issues**:
   ```bash
   python3 complete_fix.py  # Fixes constraints + tickers
   ```

3. **Partial Data Recovery**:
   - Scripts use `ON CONFLICT DO UPDATE` to handle existing data
   - Safe to re-run on same companies multiple times
   - Will update existing records with complete historical data

## Performance and Monitoring

### Expected Performance
- **Processing Rate**: 30-60 companies per minute
- **Total Ingestion Time**: 30-60 minutes for 1,606 companies
- **Data Volume**: 500,000+ to 1,000,000+ price points total
- **Database Growth**: ~50-100MB for complete dataset

### Progress Monitoring
- Real-time progress updates every 20 companies
- Success/failure categorization during processing
- Total price points accumulated across all companies
- Processing rate and estimated completion time

### Results Tracking
- JSON output files with comprehensive statistics
- Company-by-company success/failure details
- Error categorization for targeted fixes
- Performance metrics and timing information

## Integration with Temporal Anomaly System

### Purpose
The ingested market data serves as validation for document-based temporal anomalies:

1. **Document Event Validation**: Check if detected document anomalies correlate with actual price movements
2. **Backtesting Framework**: Historical price data enables strategy backtesting against document signals
3. **Performance Attribution**: Measure strategy performance against market benchmarks

### Data Flow
```
Document Analysis → Temporal Anomalies → Market Data Validation → Strategy Signals → Backtesting
```

## Production Deployment

### Prerequisites
1. PostgreSQL database with `company_master` table populated
2. Database credentials configured (`postgresql://yodabuffett:password@localhost:5432/yodabuffett`)
3. Python dependencies: `asyncpg`, `yfinance`, `numpy`
4. `company-list.json` file available for Swedish ticker mapping

### Recommended Execution Order
1. **Fix constraints and tickers**: `python3 complete_fix.py`
2. **Test with small dataset**: `python3 simple_price_ingestion.py`
3. **Full historical ingestion**: `python3 ingest_all_max_history.py`
4. **Review results and fix remaining issues**

### Maintenance
- **Re-run daily**: New price data for existing companies
- **Re-run weekly**: Check for new companies or symbol changes
- **Monitor errors**: Review JSON output files for systematic issues
- **Update tickers**: Refresh company-list.json quarterly from data provider

## Future Enhancements

### Planned Features
1. **Multiple Data Providers**: Bloomberg, Reuters integration
2. **Real-time Feeds**: Live price streaming capabilities
3. **Data Quality Validation**: Cross-source verification
4. **Automated Retry Logic**: Intelligent retry with exponential backoff
5. **Nordic Exchange APIs**: Direct exchange feed integration

### Technical Debt
1. **Move scripts to domain**: Relocate root-level scripts to `domains/market_data/scripts/`
2. **Configuration Management**: Environment-based configuration system
3. **Async Optimization**: Parallel processing for faster ingestion
4. **Data Validation**: Price sanity checks and outlier detection

## Troubleshooting

### Common Issues

**Issue**: "Foreign key constraint violation"
```
Solution: Run complete_fix.py to remove blocking constraints
```

**Issue**: "Wrong/invalid Yahoo ticker" 
```
Solution: Run fix_tickers_from_company_list.py for Swedish companies
```

**Issue**: "No data from Yahoo Finance"
```
Solution: Check if company is delisted or symbol has changed
```

**Issue**: Script stops or hangs
```
Solution: Check network connectivity, restart with rate limiting
```

### Debug Commands
```bash
# Check database connectivity
python3 -c "import asyncpg; print('Database connection test')"

# Verify table structure
psql postgresql://yodabuffett:password@localhost:5432/yodabuffett -c "\d daily_price_data"

# Check existing data
psql postgresql://yodabuffett:password@localhost:5432/yodabuffett -c "SELECT COUNT(*) FROM daily_price_data;"
```