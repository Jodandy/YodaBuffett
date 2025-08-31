# YodaBuffett - AI-Powered Investment Research Platform

**🚀 For complete architecture details, see [CLAUDE-MASTER.md](./CLAUDE-MASTER.md)**

## Quick Context
Extensible platform for institutional-grade financial research. AI-assisted analysis, predictive modeling, and advanced backtesting capabilities.

## Architecture at a Glance
- **Microservices**: API Gateway, Research, Data Ingestion, Strategy, Prediction, User services
- **Stack**: Python (AI/ML), TypeScript/Node.js (APIs), PostgreSQL + Vector DB
- **AI**: Research acceleration, data analysis, ensemble predictions

## Current Focus
Building extensible platform for any financial analysis, not just specific features.

## Current Development Status
✅ **AI-Powered Report Analysis Platform**
- Successfully extracts & analyzes SEC filings + international reports
- Cost: ~$0.004 per document | Processing: 3-5 seconds
- Foundation learnings: [mvp1-report-analysis/MVP1_LEARNINGS.md](./mvp1-report-analysis/MVP1_LEARNINGS.md)

🚀 **Nordic Financial Data Platform** - PRODUCTION READY
- Complete Swedish market document collection (50,000+ documents)
- **SMART PRIORITIZATION**: Annual & quarterly reports first (3,463 priority documents)
- **INTELLIGENT RETRY SYSTEM**: Case-insensitive matching + suffix pattern testing
- **ORGANIZED STORAGE**: `data/companies/SE/{A-Z}/{Company}/{Year}/{Type}/`
- Automated historical ingestion and PDF download batch processors
- Real-time calendar events and financial document monitoring
- RSS monitoring, document downloads, scheduled orchestration
- Management CLI, API integration, and operational monitoring

🤖 **Daily Event Worker System** - PRODUCTION ACTIVE (DOCKER)
- **Automated Daily Collection**: Docker container runs daily at 6:00 AM with built-in scheduler
- **Event-Driven Intelligence**: Only processes companies with upcoming financial events
- **Calendar Integration**: Targets earnings, reports, AGMs, dividends automatically  
- **Performance Optimized**: 50x fewer companies processed (0-50 vs 1600+)
- **Batch Database Queries**: 10-100x faster duplicate checking and storage
- **Zero Manual Intervention**: Set-and-forget Docker operation with health monitoring
- **Portable Deployment**: Same behavior on any Docker-capable server

🌍 **Multi-Market Worker System** - PRODUCTION DEPLOYED  
- **Specialized Workers**: Swedish, Norwegian, Danish, Finnish market ingestors
- **Event-Driven Architecture**: Calendar-targeted collection with smart scheduling
- **Unified Management**: Web dashboard + REST API for all Nordic markets
- **Docker Orchestration**: 12+ specialized workers with health monitoring
- **Market Configurations**: Complete setup for all Nordic countries
- **Professional Operations**: Production monitoring, logging, and error handling
- **Scalable Foundation**: Easy expansion to additional markets and worker types

🧠 **Advanced Analytics & Intelligence** - DESIGNING
- Vector-based semantic search across all financial documents
- Hidden connection discovery between companies and markets  
- Predictive modeling and systemic risk analysis
- Cross-company pattern detection and market intelligence

## Documentation Structure

| Need This? | Go Here |
|------------|---------|
| **High-level overview** | [CLAUDE-MASTER.md](./CLAUDE-MASTER.md) |
| **Development roadmap** | [docs/roadmap/README.md](./docs/roadmap/README.md) |
| **Production progress** | [docs/roadmap/README.md](./docs/roadmap/README.md) |
| **Advanced analytics concepts** | [docs/features/advanced-analytics.md](./docs/features/advanced-analytics.md) |
| **Database/Data design** | [docs/architecture/data-architecture.md](./docs/architecture/data-architecture.md) |
| **Architecture rules** | [docs/development/principles.md](./docs/development/principles.md) |
| **System flexibility** | [docs/architecture/system-flexibility.md](./docs/architecture/system-flexibility.md) |
| **Known limitations** | [docs/architecture/limitations.md](./docs/architecture/limitations.md) |
| **Adding features** | [docs/development/extensibility.md](./docs/development/extensibility.md) |
| **Running the system** | [docs/operations/human-operator-guide.md](./docs/operations/human-operator-guide.md) |
| **Event-driven workers setup** | [docs/workers/setup-guide.md](./docs/workers/setup-guide.md) |
| **Worker operations & monitoring** | [backend/workers/CLAUDE.md](./backend/workers/CLAUDE.md) |
| **Financial terms** | [glossary.md](./glossary.md) |
| **Service-specific work** | `backend/[service]/CLAUDE.md` |
| **Nordic ingestion service** | [backend/README.md](./backend/README.md) |
| **Architecture decisions** | [docs/architecture/ARCHITECTURE_DECISIONS.md](./docs/architecture/ARCHITECTURE_DECISIONS.md) |
| **Production components added** | [docs/PRODUCTION_COMPONENTS_ADDED.md](./docs/PRODUCTION_COMPONENTS_ADDED.md) |
| **Multi-market worker system** | [docs/MULTI_MARKET_WORKERS_ADDED.md](./docs/MULTI_MARKET_WORKERS_ADDED.md) |

## Quick Commands

### Docker-First Production (RECOMMENDED)
```bash
# DAILY EVENT WORKER (PRODUCTION - AUTOMATIC AT 6:00 AM)
cd backend/docker
docker-compose up daily-event-scheduler -d      # Start production scheduler
docker ps | grep yodabuffett-daily-scheduler    # Check if scheduler running  
docker logs yodabuffett-daily-scheduler --tail 20 # View scheduler activity
curl http://localhost:8085/health               # Check scheduler health

# Test daily worker manually
docker exec yodabuffett-daily-scheduler python -m workers.daily_event_worker --dry-run

# View execution results (inside container)
docker exec yodabuffett-daily-scheduler ls -la /app/data/daily_worker_*.json
```

### Direct Script Commands (Development)
```bash
# Historical document collection (production batch processor)
cd backend/
python3 historical_ingestion_batch.py

# PRIORITY PDF downloads - Annual & Quarterly Reports ONLY (DEFAULT)
python3 pdf_download_batch.py --year 2025 --delay 10

# Download ALL document types (press releases, governance, etc.)
python3 pdf_download_batch.py --year 2025 --all-types --delay 10

# Smart retry system for failed companies with suffix testing
python3 retry_failed_companies.py

# Slow background PDF downloads (1 per minute, ultra-respectful)
python3 pdf_download_batch.py --year 2025 --delay 60

# Analyze results
python3 analyze_ingestion_results.py
python3 analyze_download_results.py

# MULTI-MARKET WORKER SYSTEM (Production Monitoring)
# Individual worker testing
cd backend/
python3 -m workers.ingestors.swedish_document_ingestor --dry-run
python3 -m workers.ingestors.norwegian_document_ingestor --calendar

# Docker deployment (full production system)
cd docker/
docker-compose --profile production up swedish-document-ingestor
docker-compose --profile ingestors up    # All Nordic markets
docker-compose --profile management up   # Web dashboard + API

# Management interfaces
# Web Dashboard: http://localhost:8090/dashboard
# REST API: http://localhost:8091/
# CLI Access: docker exec -it yodabuffett-worker-cli bash

# Schedule & monitoring
python3 scripts/manage_workers.py schedule --days 7          # Preview 7-day schedule
python3 scripts/manage_workers.py analyze --days 30          # Analyze performance
python3 scripts/manage_workers.py health-check               # System diagnostics

# Docker operations
python3 scripts/manage_workers.py docker --action start      # Start all services
python3 scripts/manage_workers.py docker --action status     # Check status
python3 scripts/manage_workers.py docker --action logs --service daily-worker

# Legacy Nordic service commands
python main.py
python scripts/manage_nordic.py setup
python scripts/manage_nordic.py run-collection
```

## Key Principles
1. API-first design with contracts
2. Pure functional core
3. Hexagonal architecture
4. Immutable data
5. Explicit dependencies
6. Living documentation (always current)

**For detailed context, always check CLAUDE-MASTER.md first!**