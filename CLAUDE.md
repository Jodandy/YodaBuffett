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
🔬 **MVP 1: AI-Powered Report Analysis** - Building proof-of-concept for LLM-based financial document analysis

See: [Development Roadmap](./docs/roadmap/README.md) | [Current MVP Details](./docs/roadmap/mvp1-report-analysis.md)

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
| **Service-specific work** | `backend/[service]/CLAUDE.md` |

## Quick Commands
```bash
# Start development
docker-compose up

# Run tests  
npm test && pytest

# Add new service
cp -r templates/service backend/new-service
```

## Key Principles
1. API-first design with contracts
2. Pure functional core
3. Hexagonal architecture
4. Immutable data
5. Explicit dependencies
6. Living documentation (always current)

**For detailed context, always check CLAUDE-MASTER.md first!**