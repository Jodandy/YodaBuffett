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

## Current Development Focus
✅ **MVP 1: AI-Powered Report Analysis** - COMPLETE! 
- Successfully extracts & analyzes SEC filings + international reports
- Cost: ~$0.004 per document | Processing: 3-5 seconds
- Learnings: [mvp1-report-analysis/MVP1_LEARNINGS.md](./mvp1-report-analysis/MVP1_LEARNINGS.md)

✅ **Nordic Ingestion Service** - PRODUCTION COMPLETE!
- Complete automated Swedish financial data collection system
- RSS monitoring, document downloads, scheduled orchestration
- 5 Swedish companies configured with real RSS feeds
- Management CLI, API integration, and operational monitoring
- Ready for immediate deployment and automated data collection

## Documentation Structure

| Need This? | Go Here |
|------------|---------|
| **High-level overview** | [CLAUDE-MASTER.md](./CLAUDE-MASTER.md) |
| **Development roadmap** | [docs/roadmap/README.md](./docs/roadmap/README.md) |
| **Current MVP progress** | [docs/roadmap/mvp1-report-analysis.md](./docs/roadmap/mvp1-report-analysis.md) |
| **Database/Data design** | [docs/architecture/data-architecture.md](./docs/architecture/data-architecture.md) |
| **Architecture rules** | [docs/development/principles.md](./docs/development/principles.md) |
| **System flexibility** | [docs/architecture/system-flexibility.md](./docs/architecture/system-flexibility.md) |
| **Known limitations** | [docs/architecture/limitations.md](./docs/architecture/limitations.md) |
| **Adding features** | [docs/development/extensibility.md](./docs/development/extensibility.md) |
| **Running the system** | [docs/operations/human-operator-guide.md](./docs/operations/human-operator-guide.md) |
| **Financial terms** | [glossary.md](./glossary.md) |
| **Service-specific work** | `backend/[service]/CLAUDE.md` |
| **Nordic ingestion service** | [backend/README.md](./backend/README.md) |
| **Architecture decisions** | [docs/architecture/ARCHITECTURE_DECISIONS.md](./docs/architecture/ARCHITECTURE_DECISIONS.md) |
| **Production components added** | [docs/PRODUCTION_COMPONENTS_ADDED.md](./docs/PRODUCTION_COMPONENTS_ADDED.md) |

## Quick Commands
```bash
# Start Nordic ingestion service
cd backend/
python main.py

# Set up Nordic system
python scripts/manage_nordic.py setup
python scripts/manage_nordic.py load-companies

# Run Swedish data collection
python scripts/manage_nordic.py run-collection

# Start automated collection
python scripts/manage_nordic.py start-scheduler
```

## Key Principles
1. API-first design with contracts
2. Pure functional core
3. Hexagonal architecture
4. Immutable data
5. Explicit dependencies
6. Living documentation (always current)

**For detailed context, always check CLAUDE-MASTER.md first!**