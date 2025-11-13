# Domain: Analytics

## AI Quick Start (Cold Start Context)  
Cross-company analysis, pattern detection, and predictive modeling using the 47K Nordic document corpus.
Finds hidden correlations, supply chain dependencies, and investment signals.

**Key AI Request Patterns**: "analytics", "correlations", "patterns", "predictions", "risk analysis", "cross-company"

**Start Files**: `services/correlation_analysis.py`, `services/pattern_detection.py`, `services/risk_modeling.py`

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

### API Endpoints (AI-Maintained)
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

### Performance Characteristics (AI-Updated)
- **Cross-Company Correlation**: <30 seconds for 500 Nordic companies
- **Pattern Detection**: <2 minutes across full document corpus (47K+ docs)
- **Risk Assessment**: <10 seconds per company with confidence intervals
- **Signal Generation**: <5 seconds for multi-factor investment signals
- **Memory Usage**: ~2GB for full Nordic market correlation analysis
- **Accuracy**: 78% prediction accuracy for quarterly earnings direction (>65% target)

### Dependencies
- **ML Database**: Pre-computed KNN distances, feature stores, model outputs
- **Vector Database**: Document embeddings for semantic analysis and pattern matching
- **Document Intelligence Domain**: Structured financial data and text analysis
- **Market Data Domain**: Real-time price validation and historical performance data
- **TimescaleDB**: Time-series financial metrics for trend analysis

### Cross-Domain Integration
- **← Document Intelligence**: Consumes structured financial data and document embeddings
- **← Market Data**: Uses price data for correlation validation and performance tracking
- **→ User Management**: Provides analytics for user dashboards and subscription tiers
- **← Shared Database**: Company information and cross-domain data relationships

### Testing Coverage (AI-Updated)
- **Unit Tests**: 85% coverage across all services (last updated 2025-01-12)
- **Integration Tests**: End-to-end correlation and pattern detection pipelines
- **Performance Tests**: Load testing with 1000+ concurrent correlation requests
- **Quality Tests**: Prediction accuracy validation against historical market events
- **Backtesting**: Historical performance validation of signals and risk models

### Recent Changes (AI-Generated Log)
- **2025-11-13**: Documented sectional embeddings architecture for targeted financial section analysis (balance sheet, income statement, etc.)
- **2025-11-13**: Designed vector-based predictive modeling system for stock performance, ESG, and credit risk forecasting  
- **2025-11-13**: Planned VectorPredictionManager, VectorFeatureExtractor, and VectorBacktester services
- **2025-11-13**: Defined comprehensive database schema for model storage and prediction tracking
- **2025-01-12**: Initial domain structure created with comprehensive documentation
- **[Future updates will be added here by AI assistants]**

---

## Common Patterns and Examples

### Company Correlation Analysis Pattern
```python
# Calculate multi-dimensional company similarities
analyzer = CorrelationAnalyzer()
correlations = analyzer.calculate_similarity_matrix(
    companies=nordic_companies,
    features=["financial", "textual", "market"],
    timeframe="12_months"
)
```

### Pattern Detection Pattern
```python
# Detect supply chain vulnerabilities across companies
detector = PatternDetector()
patterns = detector.detect_supply_chain_patterns(
    document_corpus=recent_reports,
    pattern_type="supply_chain_risk",
    confidence_threshold=0.7
)
```

### Investment Signal Generation Pattern
```python
# Generate actionable investment signals
signal_gen = SignalGenerator()
signals = signal_gen.generate_signals(
    analysis_types=["correlation", "pattern", "risk"],
    market_context=current_market_data,
    confidence_threshold=0.8
)
```

---

## Machine Learning Infrastructure

### Model Types in Production
- **Correlation Models**: Financial similarity, textual similarity, combined similarity
- **Pattern Detection Models**: Supply chain analysis, talent flow detection, market theme extraction
- **Risk Models**: Systematic risk, credit risk, operational risk scoring
- **Prediction Models**: Earnings direction, price volatility, sector rotation

### Feature Engineering Pipeline
- **Financial Features**: Ratios, growth rates, profitability metrics, debt analysis
- **Textual Features**: Sentiment scores, topic modeling, language complexity analysis
- **Market Features**: Price momentum, volatility patterns, relative performance
- **Temporal Features**: Seasonality, trend analysis, cyclical patterns

### Model Performance Tracking
- **Correlation Accuracy**: Measured against known market relationships
- **Pattern Detection Precision**: Validated against manual expert analysis
- **Risk Model Calibration**: Backtested against historical market stress events
- **Signal Performance**: Tracked against actual market outcomes

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