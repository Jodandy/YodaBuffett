# Nordic Ingestion Service - File Structure

> **NOTE: This describes planned/aspirational architecture. See [docs/operations/human-operator-guide.md](../operations/human-operator-guide.md) for the actual current infrastructure.**

## Current Implementation: Modular Monolith

**Decision**: We implemented Nordic ingestion as a module within a monolithic backend service for faster development and simpler operations.

### Current Structure (Production Ready)

```
backend/
├── main.py                        # Single FastAPI app entry point
├── requirements.txt
├── .env.example
├── README.md
│
├── shared/                        # Utilities shared across services
│   ├── __init__.py
│   ├── config.py                  # Application configuration
│   ├── database.py                # Database connection & models
│   └── monitoring.py              # Metrics and health checks
│
├── research/                      # MVP1 Research service module
│   ├── __init__.py
│   ├── api/
│   │   └── router.py              # Research API endpoints
│   └── models/
│
├── nordic_ingestion/              # Nordic data ingestion module
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   ├── router.py              # All Nordic API endpoints
│   │   └── schemas.py             # Pydantic request/response models
│   ├── models/
│   │   ├── __init__.py
│   │   ├── companies.py           # NordicCompany, NordicDataSource
│   │   ├── documents.py           # NordicDocument, NordicIngestionLog  
│   │   ├── calendar.py            # NordicCalendarEvent
│   │   └── manual_tasks.py        # ManualCollectionTask
│
│   ├── collectors/                 # Data collection modules
│   │   ├── __init__.py
│   │   ├── rss/
│   │   │   ├── __init__.py
│   │   │   └── swedish_rss_collector.py    # RSS feed collector
│   │   ├── calendar/
│   │   │   ├── __init__.py  
│   │   │   └── swedish_calendar_collector.py # IR calendar collector
│   │   ├── email/              # (Future) IR email parsing
│   │   └── web/                # (Future) Web scraping automation
│   └── companies/              # Company-specific configurations
│       └── sweden/             # Swedish company configs
│
└── migrations/                    # Database migrations (Alembic)
```

### Benefits of Current Approach

**✅ Development Speed**
- Single codebase with shared utilities
- Fast iteration and debugging
- Simple dependency management

**✅ Operational Simplicity**  
- One service to deploy and monitor
- Shared database transactions
- Simplified configuration

**✅ Team Efficiency**
- Optimal for teams of 1-8 engineers
- Easy knowledge sharing
- Reduced coordination overhead

## Future Migration Path: Microservices

### When to Extract to Microservices

**Triggers for Migration:**
- Team grows beyond 8 engineers
- Need independent scaling of Nordic ingestion
- Different technology requirements
- Regulatory requirements for service isolation

### Migration Steps

**Phase 1: Prepare for Extraction**
```bash
# Already done - clear module boundaries
backend/nordic_ingestion/  # Self-contained module
```

**Phase 2: Extract Service** 
```bash
# Move module to separate repository
mv backend/nordic_ingestion/ → nordic-reports-service/

# Add FastAPI app structure
# Replace internal calls with HTTP APIs
# Split database schemas
# Add service discovery
```

**Phase 3: Independent Operations**
```bash
# Separate deployment pipeline
# Independent scaling
# Service-specific monitoring
# API versioning and contracts
```

### Target Microservice Structure

When extracted, the service would look like:

```
nordic-reports-service/
├── app/
│   └── main.py                     # Independent FastAPI app
├── api/
│   ├── v1/
│   │   ├── endpoints/
│   │   │   ├── companies.py        # Separate endpoint files
│   │   │   ├── documents.py
│   │   │   ├── calendar.py
│   │   │   └── manual.py
│   │   └── schemas/
│   └── dependencies.py
├── core/                           # Business logic
├── db/                            # Database layer
├── external/                      # External API clients
├── workers/                       # Background tasks (Celery)
└── infrastructure/               # K8s, Docker, Terraform
```

This preserves all the architectural benefits while enabling independent scaling and development when needed.

## Current Production Status

**✅ Ready for Deployment**
- Complete FastAPI service with all endpoints
- PostgreSQL database schema implemented  
- Swedish RSS and calendar collectors working
- Production monitoring and health checks
- Comprehensive API documentation

**📋 Next Steps for Production**
1. Set up PostgreSQL database
2. Configure Swedish company data sources
3. Test collectors with real company feeds
4. Deploy to production environment
5. Add Swedish companies to system
│
├── models/
│   ├── __init__.py
│   ├── base.py                     # Base SQLAlchemy model
│   ├── companies.py                # Nordic company models
│   ├── documents.py                # Document storage models
│   ├── calendar.py                 # Calendar event models
│   ├── sources.py                  # Data source models
│   ├── collections.py              # Collection log models
│   └── manual_tasks.py             # Manual task models
│
├── companies/                      # Company-specific configurations
│   ├── sweden/
│   │   ├── volvo_group/
│   │   │   ├── config.yaml
│   │   │   ├── url_patterns.py
│   │   │   ├── scrapers.py
│   │   │   ├── email_patterns.py
│   │   │   └── calendar_config.py
│   │   ├── hm/
│   │   │   ├── config.yaml
│   │   │   ├── url_patterns.py
│   │   │   └── custom_scraper.py
│   │   ├── ericsson/
│   │   │   ├── config.yaml
│   │   │   └── url_patterns.py
│   │   ├── atlas_copco/
│   │   ├── sandvik/
│   │   ├── skanska/
│   │   ├── telia/
│   │   ├── seb/
│   │   ├── nordea/
│   │   └── swedbank/
│   ├── norway/
│   │   ├── equinor/
│   │   ├── telenor/
│   │   ├── norsk_hydro/
│   │   ├── dnb/
│   │   └── yara/
│   ├── denmark/
│   │   ├── novo_nordisk/
│   │   ├── carlsberg/
│   │   ├── maersk/
│   │   └── danske_bank/
│   └── finland/
│       ├── nokia/
│       ├── upm/
│       ├── stora_enso/
│       └── kone/
│
├── tasks/                          # Celery async tasks
│   ├── __init__.py
│   ├── celery_app.py               # Celery configuration
│   ├── document_tasks.py           # Document processing tasks
│   ├── collection_tasks.py         # Collection tasks
│   ├── notification_tasks.py       # Email/Slack notifications
│   └── maintenance_tasks.py        # Cleanup and maintenance
│
├── utils/
│   ├── __init__.py
│   ├── date_parser.py              # Nordic date parsing utilities
│   ├── url_utils.py                # URL manipulation utilities
│   ├── text_utils.py               # Text processing utilities
│   ├── file_utils.py               # File handling utilities
│   ├── logging_config.py           # Logging configuration
│   └── security.py                 # Security utilities
│
├── migrations/                     # Database migrations
│   ├── versions/
│   ├── alembic.ini
│   ├── env.py
│   └── script.py.mako
│
├── scripts/                        # Operational scripts
│   ├── init_database.py            # Database initialization
│   ├── load_companies.py           # Load company configurations
│   ├── manual_upload_cli.py        # Manual document upload tool
│   ├── health_check.py             # Health check script
│   ├── backup_database.py          # Database backup
│   └── deploy.sh                   # Deployment script
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py                 # Pytest configuration
│   ├── unit/
│   │   ├── test_collectors/
│   │   ├── test_orchestrator/
│   │   ├── test_storage/
│   │   └── test_processing/
│   ├── integration/
│   │   ├── test_api/
│   │   ├── test_database/
│   │   └── test_workflows/
│   └── e2e/
│       ├── test_full_pipeline.py
│       └── test_manual_workflows.py
│
├── docs/
│   ├── api/
│   │   ├── openapi.yaml            # OpenAPI specification
│   │   └── postman_collection.json # Postman API collection
│   ├── deployment/
│   │   ├── kubernetes/
│   │   ├── docker-compose/
│   │   └── terraform/
│   ├── runbooks/
│   │   ├── incident_response.md
│   │   ├── deployment_guide.md
│   │   └── troubleshooting.md
│   └── company_onboarding/
│       ├── template_config.yaml
│       └── onboarding_guide.md
│
├── monitoring/                     # External monitoring configs
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alert_rules.yml
│   ├── grafana/
│   │   ├── dashboards/
│   │   └── provisioning/
│   └── alertmanager/
│       └── alertmanager.yml
│
└── airflow/                        # Apache Airflow DAGs
    ├── dags/
    │   ├── daily_ingestion_dag.py
    │   ├── weekly_health_check_dag.py
    │   ├── monthly_company_update_dag.py
    │   └── manual_task_cleanup_dag.py
    └── plugins/
        └── nordic_operators.py
```

## Infrastructure & Deployment Files

```
infrastructure/
├── kubernetes/
│   ├── namespace.yaml
│   ├── configmaps/
│   ├── secrets/
│   ├── deployments/
│   │   ├── nordic-reports-api.yaml
│   │   ├── nordic-collectors.yaml
│   │   └── nordic-processors.yaml
│   ├── services/
│   ├── ingress/
│   └── monitoring/
│
├── terraform/
│   ├── main.tf
│   ├── database.tf                 # PostgreSQL setup
│   ├── storage.tf                  # S3/Azure blob setup
│   ├── cache.tf                    # Redis setup
│   ├── monitoring.tf               # Prometheus/Grafana
│   └── variables.tf
│
└── docker/
    ├── api.Dockerfile
    ├── collector.Dockerfile
    ├── processor.Dockerfile
    └── nginx.Dockerfile
```

## Key Production Features

### 🏗️ **Service Architecture**
- **Microservice design** with separate collectors, processors, API
- **Async task processing** with Celery
- **Database migrations** with Alembic
- **Configuration management** per environment

### 📊 **Monitoring & Observability**
- **Prometheus metrics** for all operations
- **Grafana dashboards** for visualization
- **Structured logging** with correlation IDs
- **Health checks** and alerting

### 🔒 **Security & Compliance**
- **Input validation** on all endpoints
- **File security scanning** for uploads
- **Audit logging** for all operations
- **Secrets management** with environment variables

### 🚀 **Scalability**
- **Horizontal scaling** with Kubernetes
- **Database connection pooling**
- **Redis caching** for performance
- **Async processing** for heavy operations

### 🛠️ **Operations**
- **Blue/green deployments**
- **Database backup strategies**
- **Incident response runbooks**
- **Performance monitoring**

This structure supports:
- **100+ Nordic companies**
- **2,000+ documents/year**
- **95% automation target**
- **Sub-second API response times**
- **Multi-region deployment**