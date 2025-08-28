# YodaBuffett GitHub Issues Roadmap

## 🎉 Current Status: Nordic Service Production Ready!

**✅ COMPLETED WORK:**
- Complete FastAPI backend service with all endpoints
- PostgreSQL database schemas and models  
- Swedish RSS and calendar collectors implemented
- Production monitoring and health checks
- API documentation and validation

**📋 REMAINING WORK:**
- Database setup and deployment
- Swedish company data configuration  
- Production testing and validation

---

## How to Set Up

1. **Create GitHub Projects**:
   - Go to your repo → Projects tab → New Project
   - Create "Nordic Service Development" (Kanban board)
   - Create "Manual Collection Queue" (Table view)

2. **Copy/paste these issues** into GitHub Issues
3. **Add appropriate labels** and milestones
4. **Assign to project boards**

---

## 🎯 Milestone: Production Deployment

### Core Infrastructure Issues

**Issue #1: Set up production database**
```
Title: Set up PostgreSQL database with Nordic schemas
Labels: infrastructure, database, high-priority
Status: READY TO IMPLEMENT

Description:
Set up PostgreSQL database for Nordic reports service.

Acceptance Criteria:
- [ ] PostgreSQL running (local + production)
- [ ] Database schemas created (nordic_companies, nordic_documents, etc.)
- [ ] Migration scripts working
- [ ] Connection pooling configured

Technical Notes:
✅ Database models implemented in backend/nordic_ingestion/models/
✅ Schemas match documentation
✅ Connection pooling configured in shared/database.py

Estimate: 2 days (reduced from 1 week - models already built)
```

**Issue #2: Build FastAPI service**  
```
Title: Build FastAPI service with basic endpoints
Labels: backend, api, high-priority
Status: ✅ COMPLETED

Description:
Create the main Nordic reports FastAPI service.

Acceptance Criteria:
✅ FastAPI app structure created (backend/main.py)
✅ Complete endpoints: /companies, /documents, /calendar, /health, /stats
✅ Database models with SQLAlchemy (all models implemented)
✅ API documentation working (Swagger UI at /docs)
✅ POST/PUT endpoints for creating companies and calendar events

Technical Notes:
✅ Implemented as modular monolith in backend/nordic_ingestion/
✅ All endpoints documented in api/router.py
✅ Pydantic schemas for request/response validation

Status: COMPLETED ✅
```

**Issue #3: Implement RSS collectors**
```
Title: Build RSS collectors for Swedish companies  
Labels: collector, rss, sweden, medium-priority
Status: ✅ COMPLETED

Description:
Implement RSS feed monitoring for Swedish companies.

Acceptance Criteria:
✅ RSS collector can parse major Swedish company feeds
✅ Extracts PDF URLs and metadata from RSS content
✅ Handles rate limiting and errors with retry logic
✅ Integrates with database storage system
✅ Classification of financial vs non-financial content
✅ Document type detection (Q1/Q2/Q3/annual/press_release)

Technical Notes:
✅ Implemented in collectors/rss/swedish_rss_collector.py
✅ Production-ready async collector with batching
✅ Configurable per-company RSS sources
✅ Deduplication and priority scoring

Status: COMPLETED ✅
```

**Issue #4: Build document download system**
```
Title: Implement multi-tier document download
Labels: downloader, core, medium-priority

Description:
Build the download orchestrator with fallback tiers.

Acceptance Criteria:
- [ ] Direct download attempts
- [ ] Browser automation fallback
- [ ] Manual task creation when all fails
- [ ] Document storage (S3/local)
- [ ] Deduplication working

Technical Notes:
Use mvp2-web-scraping/src/download_orchestrator.py as starting point

Estimate: 2 weeks
```

**Issue #5: GitHub Issues integration**
```
Title: Integrate GitHub Issues for manual tasks
Labels: integration, github, operational, medium-priority  

Description:  
Auto-create GitHub issues when manual collection needed.

Acceptance Criteria:
- [ ] Creates issues when automation fails
- [ ] Uses manual-collection issue template
- [ ] Closes issues when documents uploaded
- [ ] Slack notifications working

Technical Notes:
Use GitHub API v4, need personal access token

Estimate: 3 days
```

---

## 🏗️ Milestone: 20 Swedish Companies

**Issue #6: Company configuration system**
```
Title: Build company-specific configuration loading
Labels: configuration, companies, medium-priority

Description:
System to load and manage per-company configurations.

Acceptance Criteria:  
- [ ] YAML config loading from companies/ directory
- [ ] Company-specific URL patterns
- [ ] Custom scraper support
- [ ] Configuration validation

Technical Notes:
Follow structure from docs/architecture/company-configuration-storage.md

Estimate: 1 week
```

**Issue #7: Add major Swedish companies**  
```
Title: Add configurations for 20 major Swedish companies
Labels: data, companies, sweden, low-priority

Description:
Create configuration files for major Swedish companies.

Acceptance Criteria:
- [ ] Volvo Group configuration
- [ ] H&M configuration  
- [ ] Ericsson configuration
- [ ] Atlas Copco configuration
- [ ] 16 more companies from OMXS30

Technical Notes:
Start with companies that have reliable RSS feeds
Test each configuration before merging

Estimate: 2 weeks
```

---

## 🚀 Milestone: Full Automation

**Issue #8: Email subscription system**
```
Title: Build IR email subscription monitoring
Labels: collector, email, medium-priority

Description:
Monitor yodabuffett.ir@gmail.com for IR newsletters.

Acceptance Criteria:
- [ ] IMAP connection to Gmail
- [ ] Email parsing and classification  
- [ ] PDF extraction from emails
- [ ] Integration with download system

Technical Notes:
Use credentials from docs/operations/human-operator-guide.md

Estimate: 1 week
```

**Issue #9: Financial calendar system**
```
Title: Implement financial calendar collection
Labels: calendar, collector, medium-priority

Description:
Collect and predict upcoming financial report dates.

Acceptance Criteria:
- [ ] Scrape company IR calendar pages
- [ ] Store upcoming events in database
- [ ] Predict report dates based on history
- [ ] Alert system for expected reports

Technical Notes:  
Use patterns from docs/architecture/data-ingestion-architecture.md

Estimate: 1.5 weeks
```

**Issue #10: Monitoring and alerting**
```
Title: Add monitoring and alerting
Labels: monitoring, ops, medium-priority

Description:
Monitor system health and send alerts.

Acceptance Criteria:
- [ ] Health check endpoints
- [ ] Prometheus metrics
- [ ] Slack notifications for failures
- [ ] Daily collection reports

Technical Notes:
Integrate with existing monitoring if available

Estimate: 1 week  
```

---

## 📊 Milestone: Production Ready

**Issue #11: Deployment automation**
```
Title: Set up production deployment
Labels: deployment, devops, high-priority

Description:
Automated deployment to production environment.

Acceptance Criteria:
- [ ] Docker containers
- [ ] Kubernetes deployment (if using k8s)
- [ ] Environment configuration
- [ ] Database migrations in deploy pipeline

Technical Notes:
Follow patterns from other YodaBuffett services

Estimate: 1 week
```

**Issue #12: Performance optimization**
```
Title: Optimize for 500+ companies
Labels: performance, optimization, low-priority

Description:
Optimize system to handle 500+ Nordic companies.

Acceptance Criteria:
- [ ] Async processing working
- [ ] Database query optimization
- [ ] Caching implementation
- [ ] Rate limiting for external APIs

Estimate: 1 week
```

---

## 🛠️ Project Board Setup

### "Nordic Service Development" (Kanban)
```
📋 Backlog        🔄 In Progress     👀 Review         ✅ Done
Issue #1          Issue #2           Issue #3          
Issue #4                             
Issue #5
Issue #6
...
```

### "Manual Collection Queue" (Table)  
```
| Issue | Company | Report | Priority | Assignee | Status |
|-------|---------|--------|----------|----------|---------|
| #101  | Volvo   | Q1 2025| High     | @ops     | Open    |
| #102  | H&M     | Q4 2024| Medium   | @ops     | Closed  |
```

---

## 🚀 Quick Start Commands

```bash
# Create the issues (copy/paste from above)
# Then organize with labels:

# High priority infrastructure
gh issue edit 1 --add-label "milestone:mvp2-production,priority:high"
gh issue edit 2 --add-label "milestone:mvp2-production,priority:high"

# Medium priority features  
gh issue edit 3 --add-label "milestone:mvp2-production,priority:medium"
gh issue edit 4 --add-label "milestone:mvp2-production,priority:medium"

# Create milestones
gh api repos/:owner/:repo/milestones -f title="MVP2 → Production" -f description="Core infrastructure ready"
gh api repos/:owner/:repo/milestones -f title="20 Swedish Companies" -f description="Handle 20 major Swedish companies"
```

This gives you a solid tracking system without too much overhead! 🎯