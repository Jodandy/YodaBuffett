# YodaBuffett: AI Assistant Quick Start Guide

> **NOTE: This describes planned/aspirational architecture. See [docs/operations/human-operator-guide.md](docs/operations/human-operator-guide.md) for the actual current infrastructure.**

## For AI Assistants: How to Understand This Project from Scratch

You've been asked to work on YodaBuffett - a next-generation financial intelligence platform. This guide gets you productive within 3 minutes from a completely cold start.

## Step 1: Project Overview (30 seconds)

### What YodaBuffett Is
**Multi-product financial intelligence platform** competing with Bloomberg Terminal, FactSet, and Refinitiv through:
- **Superior UX/UI**: Modern interfaces vs 1990s legacy platforms  
- **Better Pricing**: $500-800/month vs $2,000+ competitors
- **Unique Coverage**: Comprehensive Nordic market data (47K+ documents)
- **Platform Approach**: Multiple products on unified infrastructure

### Technical Foundation
- **Multi-Database Architecture**: PostgreSQL + Vector DB + ML Database + Redis
- **Data Sources**: PDFs, real-time feeds, APIs, web scraping
- **AI-First Design**: Built for LLM integration and AI-assisted development
- **Domain-Driven Architecture**: Business logic organized by domains

## Step 2: Domain Mapping (60 seconds)

### Primary Domains
| Domain | When User Asks About | Key Capabilities |
|--------|---------------------|------------------|
| **Document Intelligence** | "process documents", "extract data", "PDF analysis" | PDF processing, financial data extraction, 47K Nordic docs |
| **Market Data** | "real-time prices", "historical data", "market feeds" | Multi-source feeds, data validation, price streams |
| **Analytics** | "correlations", "patterns", "predictions", "risk analysis" | Cross-company analysis, ML models, predictive insights |
| **User Management** | "authentication", "subscriptions", "user access" | Auth, multi-tenant, API access control |

### Domain Structure Pattern
```
backend/domains/[domain_name]/
├── __domain__.md          ← Read this for complete domain context
├── models/               ← Data structures and schemas
├── services/             ← Business logic and core functionality  
├── repositories/         ← Database access layer
└── api/                  ← REST endpoints and external interfaces
```

## Step 3: Quick Navigation (90 seconds)

### Common AI Task Mapping
| User Request Examples | Go To Domain | Start With Files |
|--------------------- |--------------|------------------|
| "analytics of cross-company correlations" | `domains/analytics/` | `services/correlation_analysis.py` |
| "process financial documents" | `domains/document_intelligence/` | `services/pdf_processor.py` |
| "fix market data validation" | `domains/market_data/` | `services/data_validation.py` |
| "user authentication issues" | `domains/user_management/` | `services/auth_service.py` |
| "new API endpoint for X" | Relevant domain + `api/` | Domain-specific API files |
| "database schema changes" | `shared/database/` | Plus affected domain models |

### Essential Files to Know
- **`ARCHITECTURE_MAP.md`**: Complete system overview and relationships
- **`domains/[name]/__domain__.md`**: Complete domain context (READ FIRST)
- **`docs/development/ai-first-methodology.md`**: How we develop with AI
- **`docs/strategy/platform-vision.md`**: Business strategy and competitive position

## Step 4: Understanding YodaBuffett Patterns (30 seconds)

### Consistent Architecture Patterns
Once you understand one domain, others follow the same pattern:
```
Models (data) → Services (logic) → Repositories (storage) → APIs (external)
```

### Cross-Domain Integration
- **Shared Database Layer**: Cross-domain data access
- **Event-Driven Communication**: Domains notify each other of changes
- **API-First Design**: Clear contracts between domains
- **Dependency Injection**: Explicit dependencies, easy testing

## AI Assistant Success Workflow

### For Any Request:
1. **📖 Read the relevant domain's `__domain__.md`** - This gives you complete context
2. **🎯 Identify specific services** - Domain README tells you which files to examine
3. **🔍 Check dependencies** - Understand what other domains/services you need
4. **⚡ Start with small changes** - Test your understanding before major modifications
5. **📝 Update documentation** - Maintain the domain README as you work

### Cold Start Success Pattern:
```
User: "Let's work on analytics of supply chain dependencies"

AI Process:
1. Read domains/analytics/__domain__.md (2 minutes)
2. Understand it's about cross-company pattern detection
3. Check services/pattern_detection.py and correlation_analysis.py
4. See dependencies on Document Intelligence and ML Database
5. Ready to work productively!

Total time: ~3 minutes from zero context to productive work
```

## Common Gotchas for AI Assistants

### ⚠️ Important Notes
- **Read domain README completely** - Don't skip to code without understanding context
- **Check performance requirements** - Each domain has specific speed/accuracy targets
- **Understand data flow** - Nordic documents → Multi-database → Analytics → Products
- **Follow AI-first standards** - Update documentation as you make changes
- **Validate cross-domain impacts** - Changes in one domain may affect others

### 🔧 Maintenance Responsibilities  
As you work, automatically update:
- Domain `__domain__.md` files with new services/changes
- Performance characteristics if they change
- API endpoint lists when adding/modifying endpoints
- Cross-references when dependencies change

## Quick Reference: File Structure

```
YodaBuffett/
├── AI_QUICK_START.md              ← You are here
├── ARCHITECTURE_MAP.md            ← System overview and relationships
├── CLAUDE-MASTER.md               ← High-level project overview
├── docs/
│   ├── strategy/platform-vision.md    ← Business strategy
│   ├── development/ai-first-methodology.md  ← How we develop
│   ├── architecture/multi-database-design.md ← Database architecture
│   └── business/competitive-analysis.md ← Market positioning
├── backend/
│   ├── domains/
│   │   ├── document_intelligence/__domain__.md  ← Domain context
│   │   ├── market_data/__domain__.md
│   │   ├── analytics/__domain__.md
│   │   └── user_management/__domain__.md
│   └── shared/                    ← Cross-domain utilities
└── tools/
    └── ai_docs_validator.py       ← Validate documentation currency
```

## Success Metrics

### You'll Know You're Productive When:
- ✅ You can navigate to relevant code within 3 minutes
- ✅ You understand domain context without reading all the code
- ✅ You know which other domains your changes might affect
- ✅ You're updating documentation as you work
- ✅ You can work on any domain after reading its README

### If You're Struggling:
- 🔄 Re-read the domain `__domain__.md` - it contains everything you need
- 🔗 Check `ARCHITECTURE_MAP.md` for system relationships
- 📋 Run `python tools/ai_docs_validator.py` to check doc currency
- 🤔 Ask clarifying questions about business requirements

---

**Welcome to YodaBuffett! With this foundation, you should be able to contribute productively from day one. The domain README files are your primary resource - they contain comprehensive, AI-maintained context that evolves with the codebase.**