# YodaBuffett Backend

Production-ready modular monolith serving both Research and Nordic Ingestion services.

## Services

- **Research Service** (`/api/v1/research`) - MVP1 document analysis
- **Nordic Ingestion Service** (`/api/v1/nordic`) - Swedish financial data ingestion
- **Document Intelligence** (`domains/document_intelligence/`) - Vector embedding pipeline with OpenAI integration
- **Analytics Domain** (`domains/analytics/`) - Vector-based predictive modeling (planned)

## Quick Start

### Prerequisites
- **Docker** running with the `yodabuffett-db` PostgreSQL container on `localhost:5432`
- **Python 3.12** with venv

```bash
# Ensure PostgreSQL container is running
docker start yodabuffett-db

# Setup Python environment
cd backend/
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env — the default DATABASE_URL is:
# postgresql://yodabuffett:password@localhost:5432/yodabuffett

# Run the service
python main.py
```

The API will be available at:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## API Endpoints

### Nordic Ingestion Service

```bash
# List Swedish companies
GET /api/v1/nordic/companies?country=SE

# List financial documents  
GET /api/v1/nordic/documents?document_type=Q1

# Get upcoming events
GET /api/v1/nordic/calendar/this-week

# Service health
GET /api/v1/nordic/health

# Collection statistics
GET /api/v1/nordic/stats
```

### Research Service

```bash  
# Research service health
GET /api/v1/research/health

# Document analysis (MVP1 integration)
GET /api/v1/research/analyze
```

## Batch Processing (Nordic Ingestion)

The Nordic Ingestion service includes production-ready batch processors for large-scale financial document collection:

### Historical Document Collection
```bash
# Collect financial documents from ALL Swedish companies (~50,000 documents)
python3 historical_ingestion_batch.py

# Resume interrupted collection
python3 historical_ingestion_batch.py  # Choose resume option when prompted
```

### PDF Downloads with Smart Prioritization
```bash
# PRIORITY: Download annual & quarterly reports ONLY (DEFAULT)
# ~3,463 high-priority financial documents
python3 pdf_download_batch.py --year 2025 --delay 10

# Download ALL document types (press releases, governance, etc.)
# ~14,473 total documents
python3 pdf_download_batch.py --year 2025 --all-types --delay 10

# Focus on specific company
python3 pdf_download_batch.py --year 2025 --company "Volvo" --delay 10

# Ultra-respectful mode (1 PDF per minute)
python3 pdf_download_batch.py --year 2025 --delay 60
```

### Smart Company Retry System
```bash
# Automatically retry failed companies with intelligent slug detection
# Tests case-insensitive matching and suffix variants (-holding, -group, etc.)
python3 retry_failed_companies.py
```

### Analysis & Monitoring
```bash
# Analyze collection results
python3 analyze_ingestion_results.py --failures
python3 analyze_download_results.py

# Quick company testing
python3 test_mfn_collector.py
```

## Vector Embeddings (Document Intelligence)

Generate semantic embeddings for extracted financial documents using OpenAI's text-embedding-3-small model.

### Setup
```bash
# Ensure OpenAI API key is set in backend/.env
OPENAI_API_KEY=sk-your-api-key-here

# Activate virtual environment
source venv/bin/activate
cd domains/document_intelligence/
```

### Commands
```bash
# Check embedding status
python cli_embedding_generation.py status

# Preview next documents for embedding
python cli_embedding_generation.py preview 10

# Generate embeddings (small batch)
python cli_embedding_generation.py generate 5

# Test with single document
python cli_embedding_generation.py test

# Filter by company
python cli_embedding_generation.py generate 3 --company=Volvo
```

### Performance
- **Cost**: ~$0.026 per 1,000 documents (~$47 for full 1,827 document corpus)
- **Speed**: ~1 second per document (including OpenAI API call and database storage)
- **Storage**: 1536-dimensional vectors in PostgreSQL with pgvector extension
- **Providers**: Currently OpenAI, architecture supports Claude, local models

### File Organization
Downloaded PDFs are organized in a scalable structure:
```
data/companies/SE/
├── A/ABB_Ltd/2025/
│   ├── annual_report/
│   ├── quarterly_report/
│   ├── press_release/
│   └── governance/
├── B/Bambuser/2025/...
└── H/Hexagon_AB/2025/...
```

## Development

### Database Migrations

```bash
# Create migration
alembic revision --autogenerate -m "Add new table"

# Run migrations
alembic upgrade head
```

### Running Tests

```bash
pytest tests/
```

### Code Quality

```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .
```

## Production Deployment

The production system runs on macOS with PostgreSQL in Docker (`yodabuffett-db` container on port 5432) and LaunchAgent-based daily automation. See `docs/operations/human-operator-guide.md` for full details.

### Environment Variables

Set these in `backend/.env`:

```bash
DATABASE_URL=postgresql://yodabuffett:password@localhost:5432/yodabuffett
OPENAI_API_KEY=sk-...
```

## Monitoring

- **Metrics**: Available at `/metrics` (Prometheus format)
- **Health Checks**: `/health`, `/api/v1/nordic/health`, `/api/v1/research/health`
- **Logs**: Structured JSON logging to stdout

## Architecture

```
backend/
├── main.py                 # FastAPI application entry point
├── shared/                 # Shared utilities
├── research/               # MVP1 research service
├── nordic_ingestion/       # Nordic data ingestion service
│   ├── api/               # REST API endpoints
│   ├── models/            # Database models
│   ├── collectors/        # Data collection modules
│   │   ├── rss/          # RSS feed collectors
│   │   ├── calendar/     # Financial calendar collectors
│   │   ├── email/        # Email subscription parsers
│   │   └── web/          # Web scraping modules
│   └── storage/          # Data storage utilities
└── migrations/           # Database migrations
```

## Quick Start Guide

### 1. Set Up System
```bash
# Initialize database and load sample companies
cd backend/
python scripts/manage_nordic.py setup
python scripts/manage_nordic.py load-companies
```

### 2. Test Collection
```bash
# Test RSS collection
python scripts/manage_nordic.py test-rss

# Run full collection workflow  
python scripts/manage_nordic.py run-collection

# Check system status
python scripts/manage_nordic.py status
```

### 3. Start Automated Collection
```bash  
# Start daily scheduler (runs continuously)
python scripts/manage_nordic.py start-scheduler
```

### 4. Use API Endpoints
```bash
# Trigger collection via API
curl -X POST http://localhost:8000/api/v1/nordic/collect/run

# Check collection status
curl http://localhost:8000/api/v1/nordic/collect/status

# View system statistics
curl http://localhost:8000/api/v1/nordic/stats
```

## Production Features

### ✅ **Complete Collection Workflow**
- **RSS Monitoring**: Discovers new documents from company feeds
- **Calendar Collection**: Scrapes IR calendar pages for upcoming events  
- **Document Downloads**: Automatically downloads PDFs with validation
- **Scheduled Orchestration**: Daily/hourly collection with retry logic
- **Manual Fallback**: Creates GitHub issues when automation fails

### ✅ **Real Company Data**
- **5 Swedish Companies**: Volvo, H&M, Ericsson, Atlas Copco, Sandvik
- **Configured RSS Feeds**: Real company RSS URLs 
- **Calendar Sources**: Actual IR calendar pages
- **Production Ready**: Handles rate limiting, errors, validation

### ✅ **Enterprise Storage & Processing**
- **PDF Download**: Validates and stores financial documents
- **Deduplication**: Prevents duplicate documents via SHA256 hashing
- **Scalable File Organization**: `data/companies/{country}/{letter}/{company}/{year}/{type}/`
- **Example**: `data/companies/SE/V/volvo/2025/Q2/q2-2025-quarterly-report.pdf`
- **Metadata Tracking**: Complete audit trail of all operations

### ✅ **Operations & Monitoring**  
- **Scheduled Collection**: Automatic daily/hourly runs
- **Management CLI**: Easy setup, testing, and monitoring
- **API Integration**: Trigger collections via REST API
- **Status Monitoring**: Real-time system health and statistics

## Architecture Components

```
nordic_ingestion/
├── collectors/           # Data discovery (RSS, calendars)
├── storage/             # Document download and storage
├── orchestrator/        # Scheduled collection coordination  
├── companies/           # Sample Swedish company configs
├── api/                 # REST API endpoints
└── models/              # Database schemas
```

## Adding New Companies

Use the management CLI or API:

```bash
# Via CLI (add to sample_companies.py)
python scripts/manage_nordic.py load-companies

# Via API  
curl -X POST http://localhost:8000/api/v1/nordic/companies \
  -H "Content-Type: application/json" \
  -d '{
    "name": "New Company AB",
    "ticker": "NEW-B", 
    "country": "SE",
    "ir_website": "https://example.com/investors/"
  }'
```