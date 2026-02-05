# YodaBuffett Screener - Folder Structure

## Project Organization

```
products/screener/
├── README.md                           # Main product overview
├── docs/                              # Product documentation
│   ├── FOLDER_STRUCTURE.md            # This file
│   ├── PRODUCT_SPEC.md                 # Detailed product specification
│   ├── TECHNICAL_ARCHITECTURE.md      # System design and architecture
│   ├── API.md                          # API documentation
│   ├── DATABASE_SCHEMA.md              # Database design
│   ├── DEVELOPMENT.md                  # Development setup and guidelines
│   ├── USER_STORIES.md                 # User stories and use cases
│   └── DEPLOYMENT.md                   # Deployment and DevOps
├── backend/                           # Screener backend service
│   ├── README.md                       # Backend-specific documentation
│   ├── requirements.txt                # Python dependencies
│   ├── pyproject.toml                  # Python project configuration
│   ├── app/                           # Main application code
│   │   ├── __init__.py
│   │   ├── main.py                     # FastAPI application entry point
│   │   ├── api/                       # REST API endpoints
│   │   │   ├── __init__.py
│   │   │   ├── v1/                     # API version 1
│   │   │   │   ├── __init__.py
│   │   │   │   ├── screener.py         # Screening endpoints
│   │   │   │   ├── backtest.py         # Backtesting endpoints
│   │   │   │   └── metrics.py          # Available metrics endpoints
│   │   │   └── dependencies.py         # API dependencies
│   │   ├── core/                      # Core business logic
│   │   │   ├── __init__.py
│   │   │   ├── config.py               # Configuration management
│   │   │   ├── database.py             # Database connections
│   │   │   └── security.py             # Authentication and authorization
│   │   ├── models/                    # Data models
│   │   │   ├── __init__.py
│   │   │   ├── screener.py             # Screener data models
│   │   │   ├── query.py                # Query builder models
│   │   │   └── results.py              # Result data models
│   │   ├── services/                  # Business logic services
│   │   │   ├── __init__.py
│   │   │   ├── screener_service.py     # Main screening logic
│   │   │   ├── query_builder.py        # Complex query parsing
│   │   │   ├── backtest_service.py     # Backtesting engine
│   │   │   ├── metrics_service.py      # Metric calculation service
│   │   │   └── data_service.py         # Data retrieval service
│   │   ├── utils/                     # Utility functions
│   │   │   ├── __init__.py
│   │   │   ├── query_parser.py         # SQL query parsing
│   │   │   ├── date_utils.py           # Date handling utilities
│   │   │   └── calculations.py         # Financial calculations
│   │   └── schemas/                   # Pydantic schemas for API
│   │       ├── __init__.py
│   │       ├── screener.py             # Screener request/response schemas
│   │       ├── query.py                # Query schemas
│   │       └── results.py              # Result schemas
│   ├── tests/                         # Test suite
│   │   ├── __init__.py
│   │   ├── conftest.py                 # Test configuration
│   │   ├── unit/                      # Unit tests
│   │   │   ├── test_query_builder.py
│   │   │   ├── test_screener_service.py
│   │   │   └── test_metrics_service.py
│   │   ├── integration/               # Integration tests
│   │   │   ├── test_api.py
│   │   │   └── test_database.py
│   │   └── fixtures/                  # Test data fixtures
│   ├── migrations/                    # Database migrations
│   │   ├── 001_create_screener_tables.sql
│   │   └── 002_add_indexes.sql
│   └── scripts/                       # Utility scripts
│       ├── setup_db.py                 # Database setup script
│       └── seed_data.py                # Test data seeding
├── frontend/                          # Screener web application
│   ├── README.md                       # Frontend-specific documentation
│   ├── package.json                    # Node.js dependencies
│   ├── tsconfig.json                   # TypeScript configuration
│   ├── tailwind.config.js              # Tailwind CSS configuration
│   ├── vite.config.ts                  # Vite build configuration
│   ├── public/                        # Static assets
│   │   ├── index.html
│   │   └── favicon.ico
│   ├── src/                           # Source code
│   │   ├── main.tsx                    # Application entry point
│   │   ├── App.tsx                     # Root component
│   │   ├── components/                # Reusable components
│   │   │   ├── ui/                    # Basic UI components
│   │   │   │   ├── Button.tsx
│   │   │   │   ├── Input.tsx
│   │   │   │   ├── Select.tsx
│   │   │   │   └── Table.tsx
│   │   │   ├── screener/              # Screener-specific components
│   │   │   │   ├── QueryBuilder.tsx    # Visual query builder
│   │   │   │   ├── MetricSelector.tsx  # Metric selection component
│   │   │   │   ├── ResultsTable.tsx    # Results display table
│   │   │   │   └── BacktestChart.tsx   # Backtesting visualizations
│   │   │   └── layout/                # Layout components
│   │   │       ├── Header.tsx
│   │   │       ├── Sidebar.tsx
│   │   │       └── Footer.tsx
│   │   ├── pages/                     # Page components
│   │   │   ├── ScreenerPage.tsx        # Main screener interface
│   │   │   ├── BacktestPage.tsx        # Backtesting interface
│   │   │   └── SavedScreensPage.tsx    # Saved screens management
│   │   ├── hooks/                     # Custom React hooks
│   │   │   ├── useScreener.ts          # Screener data fetching
│   │   │   ├── useBacktest.ts          # Backtesting logic
│   │   │   └── useDebounce.ts          # Utility hooks
│   │   ├── services/                  # API integration
│   │   │   ├── api.ts                  # Base API client
│   │   │   ├── screener.ts             # Screener API calls
│   │   │   └── backtest.ts             # Backtest API calls
│   │   ├── types/                     # TypeScript type definitions
│   │   │   ├── screener.ts             # Screener types
│   │   │   ├── query.ts                # Query types
│   │   │   └── api.ts                  # API response types
│   │   ├── utils/                     # Utility functions
│   │   │   ├── formatters.ts           # Data formatting utilities
│   │   │   ├── validators.ts           # Form validation
│   │   │   └── constants.ts            # Application constants
│   │   └── styles/                    # Styling
│   │       ├── globals.css             # Global styles
│   │       └── components.css          # Component-specific styles
│   ├── tests/                         # Frontend tests
│   │   ├── setup.ts                    # Test setup
│   │   ├── components/                # Component tests
│   │   └── utils/                     # Utility tests
│   └── docs/                          # Frontend documentation
│       ├── COMPONENTS.md               # Component documentation
│       └── STYLING.md                  # Styling guidelines
├── shared/                            # Shared code between frontend/backend
│   ├── types/                         # Shared TypeScript types
│   │   ├── screener.ts                 # Core screener types
│   │   ├── query.ts                    # Query definition types
│   │   └── metrics.ts                  # Metric definition types
│   ├── constants/                     # Shared constants
│   │   ├── metrics.ts                  # Available metrics definitions
│   │   └── operators.ts                # Query operators
│   └── utils/                         # Shared utilities
│       ├── validation.ts               # Shared validation logic
│       └── formatters.ts               # Shared formatting functions
└── docker/                           # Docker configuration
    ├── Dockerfile.backend              # Backend container
    ├── Dockerfile.frontend             # Frontend container
    ├── docker-compose.yml              # Local development setup
    └── docker-compose.prod.yml         # Production setup
```

## Integration with Main Platform

The screener product integrates with the existing YodaBuffett platform:

### Database Integration
- Leverages existing `historical_fundamentals` table (325,400 records)
- Uses existing `market_data_history` for price data
- Extends existing `companies` table for metadata
- Creates new screener-specific tables for saved searches and backtests

### Shared Services
- Uses platform's authentication system
- Leverages existing data ingestion pipelines
- Integrates with platform monitoring and logging
- Shares Redis cache for performance optimization

### API Gateway Integration
```
Platform API Gateway
├── /api/v1/auth/*          # Shared authentication
├── /api/v1/companies/*     # Shared company data
├── /api/v1/market-data/*   # Shared market data
└── /api/v1/screener/*      # New screener endpoints
```

## Development Workflow

1. **Documentation First**: Update specs in `docs/` before coding
2. **Backend API**: Develop API endpoints in `backend/app/api/`
3. **Frontend Components**: Build UI components in `frontend/src/components/`
4. **Testing**: Write tests alongside development
5. **Integration**: Test end-to-end with existing platform

## Deployment Structure

```
Production Environment
├── screener-backend/       # Screener API service
├── screener-frontend/      # Screener web app
├── shared-database/        # Shared with main platform
└── shared-cache/          # Shared Redis instance
```

This structure keeps the screener product modular while leveraging the existing platform infrastructure efficiently.