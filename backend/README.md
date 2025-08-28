# YodaBuffett Backend

Production-ready modular monolith serving both Research and Nordic Ingestion services.

## Services

- **Research Service** (`/api/v1/research`) - MVP1 document analysis
- **Nordic Ingestion Service** (`/api/v1/nordic`) - Swedish financial data ingestion

## Quick Start

### 1. Install Dependencies

```bash
cd backend/
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

pip install -r requirements.txt
```

### 2. Setup Environment

```bash
cp .env.example .env
# Edit .env with your database credentials
```

### 3. Setup Database

```bash
# Start PostgreSQL (Docker example)
docker run --name yodabuffett-db -e POSTGRES_PASSWORD=password -e POSTGRES_USER=yodabuffett -e POSTGRES_DB=yodabuffett -p 5432:5432 -d postgres:15

# Or use your existing PostgreSQL instance
```

### 4. Run the Service

```bash
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

### Docker

```bash
docker build -t yodabuffett-backend .
docker run -p 8000:8000 yodabuffett-backend
```

### Environment Variables

Set these in production:

```bash
DEBUG=False
ENVIRONMENT=production
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
OPENAI_API_KEY=...
GITHUB_TOKEN=...
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