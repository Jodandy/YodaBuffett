# YodaBuffett - AI-Powered Investment Research Platform

## Project Overview
Extensible platform for institutional-grade financial research and analysis. AI accelerates research workflows, powers predictive models, and enables sophisticated backtesting. Architecture supports evolution from research assistant to full-featured fintech platform.

## Quick Start
```bash
# Start development
docker-compose up

# Run tests
npm test && pytest

# Deploy
git push origin main
```

## Core Architecture

### Services
1. **API Gateway** (`/backend/api-gateway/`) - Authentication, routing, rate limiting
2. **Research Service** (`/backend/research-service/`) - RAG-based Q&A, embeddings
3. **Data Ingestion** (`/backend/data-ingestion/`) - SEC filings, data processing
4. **Strategy Engine** (`/backend/strategy-engine/`) - Backtesting, strategy DSL
5. **Prediction Service** (`/backend/prediction-service/`) - ML/LLM ensemble predictions
6. **User Service** (`/backend/user-service/`) - Auth, subscriptions
7. **Frontend** (`/frontend/`) - Next.js interface

### Technology Stack
- **Backend**: Python (AI/ML), TypeScript/Node.js (APIs)
- **Database**: PostgreSQL + TimescaleDB + Vector DB
- **Infrastructure**: Docker, K8s, Terraform
- **AI**: OpenAI/Anthropic APIs, local models

## HARD Architecture Principles
1. **API-First**: Define contracts before implementation
2. **Pure Functional Core**: Business logic with no side effects
3. **Hexagonal Architecture**: Core logic independent of infrastructure
4. **Immutable Data**: No mutations, only new versions
5. **Explicit Dependencies**: All dependencies injected
6. **Contract Testing**: Guarantee API compatibility
7. **Living Documentation**: Keep docs current with every code change

## Project Structure
```
YodaBuffett/
├── CLAUDE-MASTER.md           # This file - high-level overview
├── docs/
│   ├── architecture/
│   │   ├── data-architecture.md      # Database schemas, data flow
│   │   ├── system-flexibility.md     # Change management, migrations
│   │   └── limitations.md            # Known bottlenecks, mitigations
│   ├── development/
│   │   ├── principles.md            # HARD architecture rules
│   │   └── extensibility.md         # Plugin system, analysis modules
│   ├── deployment/
│   │   └── getting-started.md       # Local setup, deployment
│   ├── security/
│   │   └── security-architecture.md # Security design, compliance
│   ├── compliance/
│   │   └── financial-regulations.md # SEC, FINRA, GDPR requirements
│   ├── operations/
│   │   ├── human-operator-guide.md  # Commands, keys, maintenance
│   │   ├── monitoring-observability.md # Monitoring, alerting
│   │   └── disaster-recovery.md     # Business continuity, DR
│   ├── business/
│   │   └── monetization-strategy.md # Pricing, market strategy
│   └── user-experience/
│       └── personalization-architecture.md # UX, personalization
├── backend/
│   ├── api-gateway/
│   │   └── CLAUDE.md               # Service-specific context
│   ├── research-service/
│   │   └── CLAUDE.md
│   └── [other services]/
│       └── CLAUDE.md
├── frontend/
│   └── CLAUDE.md
└── shared/                        # Types, contracts, utilities
    └── CLAUDE.md
```

## Common Tasks

### Add New Analysis Type
1. Define contract in `shared/contracts/`
2. Implement analysis module
3. Register with analysis registry
4. Add tests

### Add New Service
1. Copy service template
2. Define API contract
3. Implement service logic
4. Add to docker-compose
5. Update API gateway routing

### Database Changes
1. Create migration file
2. Update models/types
3. Run migration locally
4. Deploy via CI/CD

## Documentation Map

**Need high-level architecture?** → Read this file  
**Working on data/database?** → `docs/architecture/data-architecture.md`  
**Adding new features?** → `docs/development/extensibility.md`  
**System not flexible enough?** → `docs/architecture/system-flexibility.md`  
**Performance issues?** → `docs/architecture/limitations.md`  
**Security concerns?** → `docs/security/security-architecture.md`  
**Compliance requirements?** → `docs/compliance/financial-regulations.md`  
**Operations/monitoring?** → `docs/operations/monitoring-observability.md`  
**Need to run/manage system?** → `docs/operations/human-operator-guide.md`  
**Business planning?** → `docs/business/monetization-strategy.md`  
**UX/personalization?** → `docs/user-experience/personalization-architecture.md`  
**Service-specific work?** → `backend/[service]/CLAUDE.md`  
**Local development setup?** → `docs/deployment/getting-started.md`

## Key Design Decisions

### Extensible Platform
Built as composable analysis modules, not fixed features. Platform can support any research workflow:
- **AI-Assisted Research**: Speed up human analysis with intelligent data extraction
- **Predictive Modeling**: Ensemble methods combining traditional ML with LLM insights
- **Advanced Backtesting**: Sophisticated strategy testing and validation
- **Traditional Fintech**: Portfolio management, risk analysis, performance tracking

Modules combine flexibly - from simple research acceleration to complex trading systems.

### Change Management
- Contract versioning for breaking changes
- Repository pattern for database abstraction
- Provider interfaces for external services
- Event sourcing for audit trails

## Performance Targets
- API response: < 200ms (p95)
- AI queries: < 3s
- Document ingestion: 100 docs/min
- Concurrent users: 10,000+

## Questions?
Check the appropriate documentation file above, or service-specific CLAUDE.md files.