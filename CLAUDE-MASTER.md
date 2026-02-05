# YodaBuffett - Multi-Product Financial Intelligence Platform

## Project Overview
Next-generation financial research platform that powers multiple products on a unified foundation. From Bloomberg competitors to retail tools, all built on the same multi-database architecture with superior UX/UI, better pricing, and institutional-grade reliability. Platform enables custom hypothesis testing for sophisticated users while maintaining simplicity for retail customers.

## Quick Start
```bash
# Start PostgreSQL (Docker container, required)
docker start yodabuffett-db

# Check daily workers are running
launchctl list | grep yodabuffett

# See full startup guide
cat docs/operations/human-operator-guide.md
```

## Core Architecture

### Modular Monolith Design
The system uses a **modular monolith** architecture for optimal development velocity and operational simplicity at current scale.

### Core Modules
1. **Analytics Domain** (`/backend/domains/analytics/`) - Backtesting, temporal anomaly detection, predictive modeling
2. **Document Intelligence Domain** (`/backend/domains/document_intelligence/`) - PDF processing, embeddings, section analysis
3. **Market Data Domain** (`/backend/domains/market_data/`) - Market data feeds and normalization
4. **User Management Domain** (`/backend/domains/user_management/`) - Authentication, subscriptions
5. **Nordic Ingestion Service** (`/backend/nordic_ingestion/`) - Multi-market document collection
6. **Multi-Market Workers** (`/backend/workers/`) - Specialized market data workers
7. **Research Service** (`/backend/research-service/`) - RAG-based Q&A system

### Technology Stack
- **Backend**: Python (AI/ML, APIs, workers)
- **Database**: PostgreSQL 15 (Docker container `yodabuffett-db`) with pgvector extension
- **Data Services**: Document processing, market data feeds, web scraping
- **Infrastructure**: macOS LaunchAgents (daily automation), Docker configs available for future cloud deployment
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
├── AI_QUICK_START.md          # AI assistant cold-start guide (READ FIRST!)
├── ARCHITECTURE_MAP.md        # Complete system overview and navigation
├── CLAUDE-MASTER.md           # This file - high-level overview
├── backend/
│   └── domains/               # Business domain organization
│       ├── document_intelligence/__domain__.md  # Document processing
│       ├── market_data/__domain__.md           # Market feeds & data
│       ├── analytics/__domain__.md             # Cross-company analysis
│       └── user_management/__domain__.md       # Auth & subscriptions
├── tools/
│   └── ai_docs_validator.py   # Validate documentation currency
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
├── glossary.md                       # Financial terms and definitions
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

**🚀 NEW TO YODABUFFETT?** → `AI_QUICK_START.md` (AI cold-start guide)  
**Need system overview?** → `ARCHITECTURE_MAP.md` (complete architecture map)  
**Platform strategy & vision?** → `docs/strategy/platform-vision.md`  
**Multi-product roadmap?** → `docs/roadmap/README.md`  
**AI-first development methodology?** → `docs/development/ai-first-methodology.md`  
**AI code standards?** → `docs/development/ai-code-standards.md`  
**Competitive analysis?** → `docs/business/competitive-analysis.md`  
**Multi-database design?** → `docs/architecture/multi-database-design.md`  
**Data services architecture?** → `docs/architecture/data-services-architecture.md`

**Domain-Specific Work:**  
**Document processing?** → `backend/domains/document_intelligence/__domain__.md`  
**Market data & feeds?** → `backend/domains/market_data/__domain__.md`  
**Analytics & correlations?** → `backend/domains/analytics/__domain__.md`  
**User management & auth?** → `backend/domains/user_management/__domain__.md`  
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
**Need financial term definitions?** → `glossary.md`  
**Service-specific work?** → `backend/[service]/CLAUDE.md`  
**Multi-market workers system?** → `backend/workers/CLAUDE.md`  
**Nordic data collection?** → `backend/README.md`  
**Local development setup?** → `docs/deployment/getting-started.md`

## Key Design Decisions

### Production Platform Features
Built as composable analysis modules for maximum flexibility. Current capabilities include:
- **Multi-Market Data Collection**: Specialized workers for Swedish, Norwegian, Danish, and Finnish markets
- **Event-Driven Architecture**: Calendar-targeted data collection with smart scheduling
- **Document Intelligence**: Comprehensive Nordic market coverage (50,000+ documents)
- **Real-time Data Processing**: Live ingestion and analysis pipeline
- **Advanced Analytics**: Hidden network models, predictive signals, cross-company patterns
- **Market Intelligence**: Sector rotation prediction, systemic risk detection
- **Investment Research**: AI-powered insights, competitive intelligence
- **Unified Management**: Web-based worker orchestration and monitoring

Platform scales from individual research to institutional-grade market intelligence across multiple financial markets.

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