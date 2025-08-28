# Architecture Decision Record

## Decision: Modular Monolith vs Microservices

**Date**: 2025-01-28  
**Status**: DECIDED  
**Context**: Nordic Reports Service Architecture  

### Decision Made: Modular Monolith

We chose to implement the Nordic ingestion service as a **modular monolith** rather than separate microservices.

### Rationale

**Current Stage Benefits:**
- ✅ **Faster Development** - Single codebase, shared utilities, easier debugging
- ✅ **Simpler Deployment** - One service to deploy and manage
- ✅ **Lower Operational Overhead** - No service discovery, network latency, or distributed tracing needed
- ✅ **Shared Database Transactions** - ACID guarantees across modules
- ✅ **Team Size** - Optimal for teams of 1-5 engineers

**Technical Implementation:**
```
backend/
├── main.py                     # Single FastAPI app
├── shared/                     # Shared utilities
├── research/                   # MVP1 research service (module)
└── nordic_ingestion/          # Nordic data service (module)
    ├── api/                   # REST endpoints
    ├── models/                # Database models  
    ├── collectors/            # Data collection
    └── processing/            # Document processing
```

**Migration Path:**
When we reach 5+ engineers or need independent scaling:
1. Extract `nordic_ingestion/` → `nordic-reports-service/`
2. Replace internal calls with HTTP APIs
3. Split database schemas
4. Add API gateway

### What This Means

**Current APIs:**
- All services accessible via single FastAPI app
- Internal module imports (fast)
- Shared database connection pool
- Single deployment artifact

**Future Migration:**
- Easy extraction to microservices when needed
- Clear module boundaries already established
- No architectural debt created

### Updated Documentation

The following docs have been updated to reflect this decision:

- `backend/README.md` - Reflects modular monolith structure
- `docs/project-management/github-issues-roadmap.md` - Updated issue priorities
- API endpoints now include POST/PUT operations as documented

### Alternative Considered: Microservices

**Why we didn't choose microservices now:**
- ❌ **Premature optimization** - No scaling requirements yet
- ❌ **Higher complexity** - Service discovery, API contracts, network failures
- ❌ **Development overhead** - Multiple deployments, testing complexity
- ❌ **Team size** - Microservices work best with 6+ engineers per service

**When to reconsider:**
- Team grows beyond 8 engineers
- Need independent scaling of nordic_ingestion
- Different technology requirements (e.g., Go for performance-critical parts)
- Regulatory requirements for service isolation

This decision aligns with the "start monolith, extract services" pattern used successfully by companies like Shopify, GitHub, and Segment.