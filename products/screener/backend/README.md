# Screener Backend

FastAPI-based backend service for the YodaBuffett Screener Pro.

## Overview

The screener backend provides REST APIs for:
- Complex stock screening with point-in-time data
- Historical backtesting of screening strategies
- Dynamic query building with AND/OR logic
- Forward return calculations

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Setup database
python scripts/setup_db.py

# Run development server
uvicorn app.main:app --reload

# Run tests
pytest tests/
```

## Architecture

- **FastAPI**: Modern Python web framework
- **SQLAlchemy**: Database ORM
- **Pydantic**: Data validation and serialization
- **PostgreSQL**: Leverages existing YodaBuffett database
- **Redis**: Caching for performance

## API Endpoints

- `GET /health` - Health check
- `POST /api/v1/screener/run` - Execute screen
- `POST /api/v1/screener/backtest` - Run backtest
- `GET /api/v1/metrics/available` - Get available metrics
- `GET /api/v1/screener/saved` - Get saved screens

## Database Integration

Leverages existing YodaBuffett tables:
- `historical_fundamentals` - Fundamental data
- `market_data_history` - Price and technical data
- `companies` - Company metadata

New tables for screener:
- `screener_queries` - Saved screening queries
- `screener_results` - Cached results
- `backtest_runs` - Backtest execution history