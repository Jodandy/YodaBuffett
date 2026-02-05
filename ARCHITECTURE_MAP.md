# YodaBuffett: System Architecture Map

> **NOTE: This describes planned/aspirational architecture. See [docs/operations/human-operator-guide.md](docs/operations/human-operator-guide.md) for the actual current infrastructure.**

<!-- AI: This file should be updated whenever domain relationships or data flows change -->
<!-- AI: Update domain count in "System Overview" when new domains added -->
<!-- AI: Update data flow diagram when new services or databases added -->

## System Overview
Multi-product financial intelligence platform with **4 core domains** and **specialized multi-database architecture**. Designed to compete with Bloomberg Terminal and FactSet through superior UX/UI, competitive pricing, and comprehensive Nordic market coverage.

## High-Level Architecture

### Platform Strategy
```
Single Platform Foundation
├── Professional Research Interface (Bloomberg competitor)
├── Retail Investment Tools (simplified UX)  
├── Developer API Access (quant/algorithmic traders)
└── Custom Analytics (institutional clients)
```

### Data Architecture
```
External Sources → Data Services → Multi-Database → API Layer → Product UIs
      │               │              │              │            │
   MFN.se          Document      PostgreSQL      Core APIs    Professional
Bloomberg API     Processing    Vector DB       Analytics     Research UI
Reuters Feed     Market Data    ML Database     Search APIs   Retail UI  
User Upload      Analytics      Redis Cache     User APIs     Developer API
```

## Domain Relationships

<!-- AI: Update this diagram when adding new domains or changing relationships -->
```
┌─ Document Intelligence ──────────┐    ┌─ Market Data ─────────────┐
│ • PDF Processing (47K docs)      │    │ • Real-time Feeds          │
│ • Financial Extraction           │────│ • Historical Data          │
│ • Multi-language (Nordic)        │    │ • Multi-source Validation  │
│ • Semantic Search                │    │ • Price/Volume Streams     │
└───────────────────────────────────┘    └────────────────────────────┘
            │                                        │
            ▼                                        ▼
┌─ Analytics ───────────────────────┐    ┌─ User Management ──────────┐
│ • Cross-company Analysis          │    │ • Authentication           │
│ • Predictive Models              │    │ • Multi-tenant Support     │ 
│ • Pattern Detection              │    │ • Subscription Management  │
│ • Risk Correlation               │    │ • API Access Control      │
└───────────────────────────────────┘    └────────────────────────────┘
```

<!-- AI: When adding new domain, copy template and update connections -->
<!-- Template:
┌─ [DOMAIN_NAME] ───────────────────┐
│ • [Key capability 1]              │
│ • [Key capability 2]              │
│ • [Key capability 3]              │
└───────────────────────────────────┘
-->

## Database Architecture

### Specialized Database Types
```
┌─ PostgreSQL + TimescaleDB ───────┐    ┌─ Vector Database ──────────┐
│ • Core financial data            │    │ • Document embeddings      │
│ • Company information            │    │ • Semantic search          │
│ • Time-series metrics            │────│ • Similarity matching      │
│ • Relational data integrity      │    │ • Cross-document patterns  │
└───────────────────────────────────┘    └────────────────────────────┘
            │                                        │
            ▼                                        ▼
┌─ ML Database (PostgreSQL) ────────┐    ┌─ Redis Cache ──────────────┐
│ • Pre-computed KNN distances      │    │ • Real-time data           │
│ • Feature stores                  │    │ • Session management       │
│ • Model outputs                   │    │ • API rate limiting        │
│ • Ensemble predictions            │    │ • Temporary computations   │
└───────────────────────────────────┘    └────────────────────────────┘
```

## Key Integration Points

### Cross-Domain Data Flows
1. **Document → Market Data**: Company validation and enrichment
2. **Market Data → Analytics**: Real-time correlation analysis
3. **Analytics → All Domains**: Pattern insights and risk signals
4. **User Management → All**: Authentication and authorization
5. **Shared Database**: Cross-domain queries and relationships

### Performance Requirements
- **API Response**: <200ms (p95) vs competitors' 1-3 seconds
- **Document Processing**: <2 minutes per document
- **Real-time Data**: <100ms latency for market feeds
- **Analytics**: <30 seconds for cross-company correlations
- **System Uptime**: 99.9% availability

## Navigation Guide for AI Assistants

<!-- AI: Update these paths when moving or renaming files -->
<!-- AI: Add new sections when creating new domains -->

### For Document Processing Tasks:
- **Text Extraction**: `backend/domains/document_intelligence/services/pdf_processor.py`
- **Financial Data Extraction**: `backend/domains/document_intelligence/services/financial_data_extractor.py`
- **Document Models**: `backend/domains/document_intelligence/models/`
- **Document API**: `backend/domains/document_intelligence/api/`

### For Market Data Tasks:
- **Real-time Feeds**: `backend/domains/market_data/services/real_time_feeds.py`
- **Historical Data**: `backend/domains/market_data/services/historical_data.py`
- **Data Validation**: `backend/domains/market_data/services/data_validation.py`
- **Market Models**: `backend/domains/market_data/models/`

### For Analytics Tasks:
- **Correlation Analysis**: `backend/domains/analytics/services/correlation_analysis.py`
- **Pattern Detection**: `backend/domains/analytics/services/pattern_detection.py`
- **Risk Modeling**: `backend/domains/analytics/services/risk_modeling.py`
- **ML Models**: `backend/domains/analytics/models/`

### For User Management Tasks:
- **Authentication**: `backend/domains/user_management/services/auth_service.py`
- **Subscriptions**: `backend/domains/user_management/services/subscription_service.py`
- **API Access**: `backend/domains/user_management/services/api_access_service.py`
- **User Models**: `backend/domains/user_management/models/`

### For Cross-Domain Tasks:
- **Shared Utilities**: `backend/shared/`
- **Database Abstractions**: `backend/shared/database/`
- **External Integrations**: `backend/integrations/`
- **Common Models**: `backend/shared/models/`

<!-- AI: Add new domain section template:
### For [DOMAIN_NAME] Tasks:
- **[Key functionality]**: `backend/domains/[domain_name]/services/[main_service].py`
- **[Another functionality]**: `backend/domains/[domain_name]/services/[other_service].py`
- **Models**: `backend/domains/[domain_name]/models/`
- **API**: `backend/domains/[domain_name]/api/`
-->

## Competitive Advantages

### Technical Differentiators
- **Multi-Database Architecture**: Optimized for different data types and access patterns
- **AI-First Development**: Built with LLM integration from the ground up
- **Real-Time Multi-Source Validation**: Data quality through redundant sources
- **Event-Driven Design**: Efficient updates and notifications

### Business Differentiators  
- **Superior UX/UI**: Modern interfaces vs legacy 1990s platforms
- **Transparent Pricing**: $500-800/month vs Bloomberg's $2,000+
- **Nordic Market Leadership**: Comprehensive coverage competitors lack
- **Custom Hypothesis Testing**: Users validate their own investment theories

## External Dependencies

### Data Providers
- **MFN.se**: Swedish market data (primary Nordic source)
- **Bloomberg API**: Global market data and news
- **Reuters/Refinitiv**: Alternative market data and news
- **Company Direct**: Annual reports, SEC filings

### Infrastructure Services
- **Cloud Providers**: AWS/Azure for compute and storage
- **AI/ML Services**: OpenAI, Anthropic for document processing
- **Database Services**: Managed PostgreSQL, Vector DB hosting
- **Monitoring**: Application performance and error tracking

## Development Workflow Integration

### AI-First Development
- **Living Documentation**: AI assistants maintain domain READMEs
- **Code Generation**: LLM-assisted development with domain context
- **Quality Assurance**: AI-generated tests and validation
- **Performance Monitoring**: Automated tracking and optimization

### Domain Development Pattern
1. **Business Logic**: Pure functions in services/
2. **Data Access**: Repository pattern in repositories/
3. **External Interface**: REST APIs in api/
4. **Data Modeling**: Domain-specific models in models/
5. **Documentation**: Self-maintaining __domain__.md files

---

## For AI Assistants: Maintenance Instructions

### Update This File When:
- ✅ New domains added to the platform
- ✅ Domain relationships change significantly  
- ✅ New database types or external services added
- ✅ Performance requirements change
- ✅ New competitive advantages identified

### Templates for Updates:

**New Domain Addition:**
```
┌─ [DOMAIN_NAME] ───────────────────┐
│ • [Key capability 1]              │
│ • [Key capability 2]              │  
│ • [Key capability 3]              │
└───────────────────────────────────┘
```

**New Navigation Section:**
```
### For [DOMAIN_NAME] Tasks:
- **[Primary Function]**: `backend/domains/[domain]/services/[main_service].py`
- **[Secondary Function]**: `backend/domains/[domain]/services/[other_service].py`
- **Models**: `backend/domains/[domain]/models/`
- **API**: `backend/domains/[domain]/api/`
```

This architecture map provides the complete system overview needed for AI assistants to understand YodaBuffett's structure and navigate effectively across all domains.