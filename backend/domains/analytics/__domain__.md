# Domain: Analytics

> **NOTE: This domain is partially implemented. What EXISTS: temporal anomaly detection (validated with real financial events), KNN-based technical analysis with pre-computed neighbors, realistic portfolio simulation with backtesting, and plugin-based technical indicators (RSI, SMA, EMA, Bollinger, MACD). What does NOT exist: CorrelationAnalyzer, PatternDetector, RiskModeler, TrendAnalyzer, SignalGenerator, and most API endpoints listed below. Test coverage claims are fabricated. TimescaleDB is not used -- the project uses plain PostgreSQL with pgvector.**

## AI Quick Start (Cold Start Context)
Cross-company analysis, pattern detection, and predictive modeling using the 419K+ Nordic document corpus.
Temporal anomaly detection and KNN technical analysis are production-ready. Other analytics services are planned.

**Key AI Request Patterns**: "analytics", "temporal anomalies", "KNN predictions", "backtesting", "technical analysis", "correlations", "patterns"

**Start Files**: `test_temporal_patterns.py`, `backtest_knn_strategy.py`, `realistic_portfolio_simulator.py`, `services/technical_analysis/`

## When to Work Here
- User asks for company correlations, market patterns, or cross-company analysis
- Requests for predictive analytics, risk assessments, or investment signals  
- Pattern detection across documents or hidden relationship discovery
- Machine learning model development and ensemble predictions

---

## Current Implementation (AI-Maintained)
*Last updated: 2025-01-12 by AI Assistant*

### Business Purpose
Transforms raw document and market data into actionable investment intelligence. Discovers hidden patterns, correlations, and predictive signals that provide competitive advantages for YodaBuffett platform users.

### Key Capabilities
- **Cross-Company Correlation**: Multi-dimensional similarity analysis using financial + textual data
- **Hidden Pattern Detection**: Identifies supply chain dependencies, talent flows, market signals
- **Predictive Modeling**: Ensemble machine learning models for earnings, risk, and market movements  
- **Vector-Based Predictions**: Document semantic analysis for stock performance, ESG, and credit risk forecasting
- **Risk Assessment**: Systematic risk scoring and correlation analysis
- **Market Intelligence**: Early warning systems and sector rotation prediction
- **Backtesting Framework**: Walk-forward validation of predictive models with performance tracking

### Architecture Overview
```
Multi-Source Data → Feature Engineering → ML Models → Pattern Detection → Insights → User Interface
       ↓                ↓                 ↓              ↓             ↓           ↓
  Documents         feature_store → ensemble_models → pattern_detector → insights → dashboards
  Market Data       correlation_calc   risk_models     trend_analyzer    alerts
  Company Info      similarity_calc    pred_models     signal_generator  reports
```

### Services in Production
- `TemporalAnomalyDetection`: Company-specific temporal pattern baselines with anomaly scoring (validated with real events)
- `KNNPredictor`: Time-aware KNN predictions with pre-computed neighbors and no look-ahead bias
- `PortfolioSimulator`: Realistic portfolio simulation with position sizing, transaction costs, and risk management
- `TechnicalIndicators`: Plugin-based indicators (RSI, SMA, EMA, Bollinger Bands, MACD, Volume analysis)

### Services Planned (NOT YET IMPLEMENTED)
- `CorrelationAnalyzer`: Multi-dimensional company similarity using financial metrics + document embeddings
- `PatternDetector`: Identifies recurring themes and hidden connections across company reports
- `RiskModeler`: Builds ensemble risk models combining multiple data sources and timeframes
- `TrendAnalyzer`: Time-series analysis of financial metrics, sentiment, and market patterns
- `SignalGenerator`: Creates actionable investment signals from detected patterns and predictions

### Services in Development (Vector Predictions)
- `VectorPredictionManager`: Orchestrates multiple ML models using document vectors for stock, ESG, and credit predictions
- `VectorFeatureExtractor`: Extracts semantic features from document embeddings for ML model training
- `VectorBacktester`: Walk-forward backtesting framework with performance tracking and risk-adjusted metrics
- `ModelPerformanceTracker`: Monitors prediction accuracy and automatically triggers model retraining

### Core Models
- `CorrelationMatrix`: Cross-company similarity scores with confidence intervals
- `PatternResult`: Detected patterns with supporting evidence and confidence scores
- `RiskAssessment`: Multi-factor risk scores with component breakdowns
- `MarketSignal`: Actionable investment insights with timing and confidence
- `AnalyticsReport`: Comprehensive analysis combining multiple analytical outputs

### API Endpoints (Planned - NOT YET IMPLEMENTED)
- `GET /analytics/correlations/{company_id}`: Company correlation matrix with similar companies
- `POST /analytics/patterns/detect`: Run pattern detection across specified document sets
- `GET /analytics/risk/{company_id}`: Comprehensive risk assessment with component scores
- `POST /analytics/signals/generate`: Generate investment signals based on current analysis
- `GET /analytics/trends/{company_id}`: Historical trend analysis with forward projections

### Vector Prediction Endpoints (Planned)
- `POST /analytics/predictions/{document_id}`: Generate all model predictions for new document
- `GET /analytics/predictions/stock/{company_id}`: Stock performance predictions (1m, 3m, 6m horizons)
- `GET /analytics/predictions/esg/{company_id}`: ESG rating change predictions
- `GET /analytics/predictions/credit/{company_id}`: Credit risk assessment and rating predictions
- `GET /analytics/models/{model_name}/performance`: Model performance metrics and backtesting results
- `POST /analytics/models/retrain`: Trigger model retraining with new data

### Performance Characteristics (Measured for implemented services only)
- **KNN Prediction**: Pre-computed neighbors for fast lookup, ~729 labels across Nordic stocks
- **Technical Indicators**: Plugin-based calculation across 100+ companies
- **Backtesting**: Realistic portfolio simulation with transaction costs (0.2%), position sizing (20% per trade)
- **Temporal Anomaly Detection**: Section and document-level analysis with 11,000+ embeddings

### Performance Targets (Aspirational - NOT MEASURED, services not implemented)
- **Cross-Company Correlation**: <30 seconds for 500 Nordic companies
- **Pattern Detection**: <2 minutes across full document corpus
- **Risk Assessment**: <10 seconds per company with confidence intervals

### Dependencies
- **PostgreSQL + pgvector**: Pre-computed KNN distances, embeddings, model outputs (NOT TimescaleDB)
- **Document Intelligence Domain**: Structured financial data, section embeddings, and text analysis
- **Market Data Domain**: Historical price data from Yahoo Finance for backtesting and indicator calculation

### Cross-Domain Integration
- **← Document Intelligence**: Consumes structured financial data and document embeddings
- **← Market Data**: Uses price data for correlation validation and performance tracking
- **→ User Management**: Provides analytics for user dashboards and subscription tiers
- **← Shared Database**: Company information and cross-domain data relationships

### Testing Coverage
- **No automated test suite exists** - Previous "85% coverage" claim was fabricated
- **Manual validation**: Temporal anomaly detection validated against known financial events (AAK, AcadeMedia, AddLife)
- **Backtesting validation**: KNN strategy and portfolio simulator tested with historical market data
- **Unit Tests**: Not yet implemented (planned)

### Recent Changes (AI-Generated Log)
- **2025-11-13**: Documented sectional embeddings architecture for targeted financial section analysis (balance sheet, income statement, etc.)
- **2025-11-13**: Designed vector-based predictive modeling system for stock performance, ESG, and credit risk forecasting  
- **2025-11-13**: Planned VectorPredictionManager, VectorFeatureExtractor, and VectorBacktester services
- **2025-11-13**: Defined comprehensive database schema for model storage and prediction tracking
- **2025-01-12**: Initial domain structure created with comprehensive documentation
- **[Future updates will be added here by AI assistants]**

---

## Common Patterns and Examples

### Temporal Anomaly Detection (IMPLEMENTED)
```bash
# Run temporal anomaly detection on existing embeddings
cd backend/
python3 analyze_existing_embeddings.py --days 500 --sort score
python3 analyze_existing_embeddings.py --company "AAK" --days 500
```

### KNN Backtesting (IMPLEMENTED)
```bash
# Run KNN strategy backtest with realistic portfolio constraints
cd backend/
python3 backtest_knn_strategy.py
python3 realistic_portfolio_simulator.py
```

### Company Correlation Analysis (PLANNED - NOT IMPLEMENTED)
```python
# This service does not exist yet
analyzer = CorrelationAnalyzer()
correlations = analyzer.calculate_similarity_matrix(
    companies=nordic_companies,
    features=["financial", "textual", "market"],
    timeframe="12_months"
)
```

---

## Machine Learning Infrastructure

### Model Types in Production
- **KNN Technical Analysis**: Time-aware K-nearest-neighbors with pre-computed neighbors, RSI-based labels
- **Temporal Anomaly Detection**: Embedding similarity-based anomaly scoring for company communication shifts
- **Portfolio Simulation**: Realistic backtesting with position sizing, transaction costs, and arbitration

### Model Types Planned (NOT YET IMPLEMENTED)
- **Correlation Models**: Financial similarity, textual similarity, combined similarity
- **Pattern Detection Models**: Supply chain analysis, talent flow detection, market theme extraction
- **Risk Models**: Systematic risk, credit risk, operational risk scoring
- **Prediction Models**: Earnings direction, price volatility, sector rotation

### Feature Engineering (Partial)
- **Technical Features (Implemented)**: RSI, SMA, EMA, Bollinger Bands, MACD, Volume analysis
- **Financial Features (Planned)**: Ratios, growth rates, profitability metrics, debt analysis
- **Textual Features (Planned)**: Sentiment scores, topic modeling, language complexity analysis
- **Market Features (Partial)**: Price momentum from historical data, volatility patterns

### Model Performance Tracking
- **KNN Backtesting**: Validated with realistic portfolio simulation (EMA 10 showing -7.3% realistic vs +509% unrealistic)
- **Temporal Anomaly Detection**: Validated against known financial events (AAK, AcadeMedia, AddLife)

---

## AI Maintenance Instructions

### Auto-Update Triggers
Update this file immediately when:
- ✅ New analytical services or models added
- ✅ API endpoints created, modified, or removed
- ✅ Performance characteristics change significantly (>20% improvement/degradation)
- ✅ Model accuracy or prediction performance changes
- ✅ New data sources integrated into analysis
- ✅ Feature engineering pipeline modifications

### Update Templates

**New Service Added:**
```markdown
- `[ServiceName]`: [Brief description of analytical capability and key features]
```

**Performance Change:**
```markdown
- [Service/Operation]: <[new_time] for [specific_scenario] (was [old_time])
```

**New Model Type:**
```markdown
- **[Model Category]**: [Description of what it predicts/analyzes and key capabilities]
```

**API Endpoint:**
```markdown
- `[METHOD] [endpoint_path]`: [Description of analytical output provided]
```

### AI Update Checklist
Before finalizing work in this domain:
- [ ] Added any new services to "Services in Production" section
- [ ] Updated performance characteristics if they changed
- [ ] Added new API endpoints to the endpoint list  
- [ ] Updated model performance metrics if they changed
- [ ] Added entry to "Recent Changes" log with date and description
- [ ] Updated testing coverage and backtesting results
- [ ] Verified dependencies and cross-domain integration documentation

### Cross-Reference Maintenance
When modifying services in this domain, check if documentation needs updates in:
- `ARCHITECTURE_MAP.md` (if new analytical capabilities or performance changes)
- Document intelligence domain (if new data requirements or validation needs)
- Market data domain (if new data sources or validation requirements)
- User management domain (if new analytics affect subscription tiers or access control)