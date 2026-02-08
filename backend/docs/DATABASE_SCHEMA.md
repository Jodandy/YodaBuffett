# Database Schema Reference

Quick reference for AI assistants. Last updated: Feb 2026.

## Key Tables by Domain

### Company Master (source of truth)
- **`company_master`** - 1,789 Nordic companies with `primary_ticker`, `yahoo_symbol`
- Links to all other tables via `company_id` or `symbol`

### Financial Data (from Yahoo Finance)
- **`financial_statements`** - Income statements (quarterly/annual)
- **`balance_sheet_data`** - Assets, liabilities, equity
- **`cash_flow_data`** - Operating, investing, financing flows
- **`daily_price_data`** - 5M+ price records, daily OHLCV

### Document Intelligence
- **`nordic_documents`** - 424K document metadata from MFN
- **`extracted_documents`** - 107K PDFs with extracted text
- **`document_sections`** - 384K parsed financial sections
- **`section_embeddings`** - Vector embeddings per section
- **`document_embeddings`** - 50K document-level embeddings

### Dimensions (scoring system)
- **`dimension_definitions`** - 5 dimensions (value, momentum, quality, risk, sentiment)
- **`daily_dimension_scores`** - Per-company dimension scores
- **`composite_scores`** - Combined multi-dimension scores

### ML/Backtesting
- **`ml_labels`** - Training labels (RSI patterns, future returns)
- **`knn_neighbors`** - Pre-computed KNN for predictions
- **`strategies`** / **`strategy_signals`** - Backtesting framework

## Key Relationships

```
company_master.primary_ticker = financial_statements.symbol
company_master.primary_ticker = daily_price_data.symbol
company_master.id = nordic_documents.company_id
company_master.id = daily_dimension_scores.company_id
```

## Tables Overview

| Table | Rows | Size | Purpose |
|-------|------|------|---------|
| backtest_runs | 0 | 0.0MB | ML/Backtesting |
| backtest_trades | 0 | 0.0MB | ML/Backtesting |
| balance_sheet_data | 16,797 | 5.4MB | Fundamentals |
| batch_processing_sessions | 0 | 0.0MB | System |
| cash_flow_data | 16,048 | 4.4MB | Fundamentals |
| company_aliases | 0 | 0.0MB | Company master |
| company_master | 1,789 | 2.2MB | Company master |
| company_relationships | 0 | 0.0MB | Company master |
| composite_scores | 34 | 0.1MB | Dimensions |
| daily_dimension_scores | 136 | 0.4MB | Dimensions |
| daily_price_data | 5,001,358 | 1899.8MB | Market data |
| dcf_backtest_results | 0 | 0.0MB | ML/Backtesting |
| dcf_model_parameters | 4,915 | 0.7MB | DCF valuation |
| dcf_valuations | 0 | 0.1MB | DCF valuation |
| dimension_computation_log | 7 | 0.1MB | Dimensions |
| dimension_definitions | 5 | 0.1MB | Dimensions |
| dimension_score_history | 0 | 0.0MB | Dimensions |
| document_embeddings | 50,050 | 186.5MB | Documents |
| document_processing_state | 0 | 0.0MB | Documents |
| document_sections | 383,991 | 1871.3MB | Documents |
| embedding_batches | 0 | 0.0MB | Embeddings |
| extracted_document_chunks | 654,915 | 2980.8MB | Documents |
| extracted_documents | 106,683 | 4572.6MB | Documents |
| extraction_results | 0 | 0.0MB | System |
| financial_metrics | 5 | 0.1MB | Fundamentals |
| financial_statements | 15,650 | 5.6MB | Fundamentals |
| historical_fundamentals_daily | 0 | 0.0MB | System |
| knn_neighbors | 679 | 0.9MB | ML/Backtesting |
| manual_collection_tasks | 0 | 0.0MB | System |
| market_anomaly_correlations | 0 | 0.0MB | Market data |
| market_data_symbols | 36 | 0.1MB | Market data |
| market_performance_metrics | 3,444 | 1.3MB | Market data |
| ml_labels | 729 | 0.5MB | ML/Backtesting |
| ml_models | 3 | 0.0MB | ML/Backtesting |
| nordic_calendar_events | 39,914 | 23.5MB | Nordic ingestion |
| nordic_companies | 1,606 | 1.8MB | Nordic ingestion |
| nordic_data_sources | 17 | 0.1MB | Nordic ingestion |
| nordic_documents | 424,074 | 723.2MB | Documents |
| nordic_ingestion_logs | 0 | 0.0MB | Nordic ingestion |
| processing_log | 109,246 | 31.5MB | System |
| section_embeddings | 383,991 | 1420.7MB | Documents |
| strategies | 1 | 0.0MB | System |
| strategy_signals | 0 | 0.0MB | ML/Backtesting |

## Notes

### Deprecated Tables (0 rows, safe to ignore)
- `historical_fundamentals_daily` - Removed Feb 2026, ratios calculated on-demand now
- `batch_processing_sessions`, `extraction_results` - Legacy processing

### Symbol Mapping
- `primary_ticker` = Short ticker (e.g., "VOLV-B")
- `yahoo_symbol` = Yahoo Finance format (e.g., "VOLV-B.ST")
- Always join on `primary_ticker` for financial/price data

### Data Freshness
| Data Type | Update Frequency | Worker |
|-----------|-----------------|--------|
| Price data | Daily 3:00 AM | `daily_market_data_worker` |
| Fundamentals | Daily 3:45 AM | `daily_fundamentals_worker` |
| Documents | Daily 7:00/9:00 AM | `daily_document_worker` |
| Dimensions | Daily 4:00 AM | `daily_dimensions_worker` |
