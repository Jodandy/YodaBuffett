# Predictive Classification Framework for Embedding-Based ML

## Overview

This framework enables sophisticated pattern discovery by classifying companies across **multiple dimensions of performance and timeframes**. Instead of simple "up/down" labels, we create rich, multi-dimensional classifications that enable granular clustering and pattern detection.

## Multi-Dimensional Classification Schema

### 1. Performance Timeframe Classifications

```python
TIMEFRAME_LABELS = {
    # Short-term momentum
    "1mo_top_1pct": "Top 1% performers over 1 month",
    "1mo_top_5pct": "Top 5% performers over 1 month", 
    "1mo_top_decile": "Top 10% performers over 1 month",
    
    # Medium-term trends
    "3mo_top_1pct": "Top 1% performers over 3 months",
    "6mo_top_1pct": "Top 1% performers over 6 months",
    
    # Long-term winners
    "12mo_top_1pct": "Top 1% performers over 12 months",
    "24mo_top_1pct": "Top 1% performers over 24 months",
    "36mo_top_1pct": "Top 1% performers over 36 months",
    
    # Consistency patterns
    "consistent_outperformer": "Top quartile in 3+ timeframes",
    "momentum_winner": "Accelerating performance across timeframes"
}
```

### 2. Performance Quality Classifications

```python
QUALITY_LABELS = {
    # Risk-adjusted performance
    "high_sharpe_winner": "High returns with low volatility",
    "low_drawdown_winner": "Strong returns with minimal drawdowns",
    "high_vol_winner": "High returns despite high volatility",
    
    # Fundamental backing
    "earnings_driven_winner": "Performance backed by earnings growth",
    "multiple_expansion_winner": "Performance driven by valuation re-rating",
    "quality_compounder": "Consistent fundamental improvement",
    
    # Market context
    "bear_market_winner": "Outperformed during market stress",
    "sector_leader": "Outperformed sector peers consistently",
    "market_disruptor": "Created new performance category"
}
```

### 3. Pattern Discovery Classifications

```python
PATTERN_LABELS = {
    # Communication patterns
    "transformation_signaler": "Communicated transformation before breakout",
    "confidence_builder": "Management confidence preceded performance", 
    "risk_manager": "Proactive risk communication during volatility",
    
    # Operational patterns
    "efficiency_improver": "Operational efficiency gains preceded growth",
    "capital_optimizer": "Smart capital allocation preceded performance",
    "innovation_leader": "Innovation narrative preceded market success",
    
    # Market timing patterns
    "cycle_predictor": "Positioned ahead of market cycles",
    "trend_anticipator": "Anticipated industry trends early",
    "contrarian_winner": "Won by going against conventional wisdom"
}
```

## Database Schema for Multi-Label Predictions

```sql
-- Company performance classifications across multiple dimensions
CREATE TABLE company_performance_labels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name VARCHAR(100) NOT NULL,
    classification_date DATE NOT NULL,  -- When classification was made
    measurement_start_date DATE NOT NULL,  -- Start of performance measurement
    measurement_end_date DATE NOT NULL,    -- End of performance measurement
    
    -- Performance dimensions
    timeframe VARCHAR(20) NOT NULL,  -- 1mo, 3mo, 6mo, 12mo, 24mo, 36mo
    performance_tier VARCHAR(30) NOT NULL,  -- top_1pct, top_5pct, etc.
    quality_profile VARCHAR(50),  -- high_sharpe_winner, etc.
    pattern_type VARCHAR(50),     -- transformation_signaler, etc.
    
    -- Performance metrics
    total_return FLOAT NOT NULL,
    excess_return FLOAT,  -- vs benchmark
    sharpe_ratio FLOAT,
    max_drawdown FLOAT,
    volatility FLOAT,
    
    -- Context
    market_regime VARCHAR(30),  -- bull_market, bear_market, sideways
    sector VARCHAR(50),
    market_cap_quintile INTEGER,
    
    -- Meta
    created_at TIMESTAMP DEFAULT NOW(),
    data_source VARCHAR(50) DEFAULT 'nordic_markets',
    
    UNIQUE(company_name, classification_date, timeframe, performance_tier)
);

-- Indexes for fast pattern queries
CREATE INDEX idx_perf_labels_timeframe ON company_performance_labels(timeframe, performance_tier);
CREATE INDEX idx_perf_labels_company ON company_performance_labels(company_name, classification_date);
CREATE INDEX idx_perf_labels_pattern ON company_performance_labels(pattern_type, timeframe);
CREATE INDEX idx_perf_labels_quality ON company_performance_labels(quality_profile, performance_tier);
```

## Pattern Discovery Queries

### 1. Find Common Embedding Patterns Among Winners

```sql
-- Find companies that were top 1% performers over 12 months
WITH top_performers AS (
    SELECT DISTINCT company_name, classification_date
    FROM company_performance_labels
    WHERE timeframe = '12mo'
    AND performance_tier = 'top_1pct'
    AND classification_date >= '2020-01-01'
),
-- Get their document embeddings 6 months before classification
winner_embeddings AS (
    SELECT 
        tp.company_name,
        tp.classification_date,
        de.embedding,
        ed.form_type,
        ed.year
    FROM top_performers tp
    JOIN extracted_documents ed ON ed.company_name = tp.company_name
    JOIN document_embeddings de ON de.extracted_document_id = ed.id
    WHERE ed.filing_date BETWEEN (tp.classification_date - INTERVAL '9 months') 
                             AND (tp.classification_date - INTERVAL '3 months')
    AND de.embedding_model LIKE 'local/%'
)
SELECT 
    company_name,
    classification_date,
    form_type,
    COUNT(*) as doc_count
FROM winner_embeddings
GROUP BY company_name, classification_date, form_type
ORDER BY company_name, classification_date;
```

### 2. Cluster Analysis for Winner Patterns

```python
async def cluster_winner_patterns(
    timeframe: str = "12mo",
    performance_tier: str = "top_1pct", 
    lookback_months: int = 6
):
    """
    Find common embedding patterns among historical winners.
    """
    
    # Get historical winners
    winners = await get_historical_winners(timeframe, performance_tier)
    
    # Get their embeddings before the performance period
    winner_embeddings = []
    for winner in winners:
        embeddings = await get_company_embeddings_before_date(
            winner['company_name'],
            winner['classification_date'],
            lookback_months
        )
        winner_embeddings.extend(embeddings)
    
    # Get control group (non-winners)
    control_embeddings = await get_control_group_embeddings(
        timeframe, performance_tier, lookback_months
    )
    
    # Perform clustering analysis
    from sklearn.cluster import KMeans
    from sklearn.decomposition import PCA
    
    all_embeddings = winner_embeddings + control_embeddings
    labels = ['winner'] * len(winner_embeddings) + ['control'] * len(control_embeddings)
    
    # Find optimal clusters
    kmeans = KMeans(n_clusters=10)
    clusters = kmeans.fit_predict(all_embeddings)
    
    # Analyze which clusters are enriched for winners
    cluster_analysis = analyze_winner_enrichment(clusters, labels)
    
    return {
        'winner_clusters': cluster_analysis['enriched_clusters'],
        'cluster_centers': kmeans.cluster_centers_,
        'winner_patterns': identify_semantic_patterns(cluster_analysis)
    }
```

### 3. Multi-Timeframe Pattern Discovery

```python
async def find_multi_timeframe_patterns():
    """
    Find companies that are consistent winners across multiple timeframes.
    """
    
    query = """
    SELECT 
        company_name,
        COUNT(DISTINCT timeframe) as timeframes_won,
        ARRAY_AGG(DISTINCT timeframe) as winning_timeframes,
        ARRAY_AGG(DISTINCT performance_tier) as performance_tiers,
        ARRAY_AGG(DISTINCT quality_profile) as quality_profiles,
        AVG(total_return) as avg_return,
        AVG(sharpe_ratio) as avg_sharpe
    FROM company_performance_labels
    WHERE performance_tier IN ('top_1pct', 'top_5pct')
    AND classification_date >= CURRENT_DATE - INTERVAL '3 years'
    GROUP BY company_name
    HAVING COUNT(DISTINCT timeframe) >= 3  -- Winners across 3+ timeframes
    ORDER BY timeframes_won DESC, avg_sharpe DESC
    """
    
    consistent_winners = await conn.fetch(query)
    
    # Get their embedding patterns
    patterns = []
    for winner in consistent_winners:
        company_embeddings = await get_company_embedding_evolution(
            winner['company_name']
        )
        
        patterns.append({
            'company': winner['company_name'],
            'timeframes_won': winner['timeframes_won'], 
            'embedding_evolution': company_embeddings,
            'semantic_themes': await extract_semantic_themes(company_embeddings)
        })
    
    return patterns
```

## Advanced Pattern Analysis

### 1. Semantic Theme Evolution for Winners

```python
async def analyze_winner_communication_evolution(company_name: str, classification_date: date):
    """
    Analyze how a company's communication evolved before becoming a winner.
    """
    
    # Get quarterly embeddings for 2 years before classification
    timeline_embeddings = await conn.fetch("""
        SELECT 
            de.embedding,
            ed.filing_date,
            ed.form_type,
            EXTRACT(QUARTER FROM ed.filing_date) as quarter,
            EXTRACT(YEAR FROM ed.filing_date) as year
        FROM document_embeddings de
        JOIN extracted_documents ed ON de.extracted_document_id = ed.id
        WHERE ed.company_name = $1
        AND ed.filing_date BETWEEN $2 - INTERVAL '24 months' AND $2
        AND ed.form_type IN ('quarterly_report', 'annual_report')
        ORDER BY ed.filing_date
    """, company_name, classification_date)
    
    # Analyze evolution patterns
    evolution_analysis = {
        'communication_consistency': analyze_embedding_drift(timeline_embeddings),
        'thematic_shifts': detect_thematic_shifts(timeline_embeddings),
        'confidence_evolution': track_confidence_signals(timeline_embeddings),
        'strategic_pivots': identify_strategic_pivots(timeline_embeddings)
    }
    
    return evolution_analysis

def analyze_embedding_drift(embeddings):
    """Calculate how communication consistency changed over time."""
    
    similarities = []
    for i in range(1, len(embeddings)):
        prev_embedding = eval(embeddings[i-1]['embedding'])
        curr_embedding = eval(embeddings[i]['embedding']) 
        
        similarity = cosine_similarity([prev_embedding], [curr_embedding])[0][0]
        similarities.append({
            'period': f"Q{embeddings[i]['quarter']} {embeddings[i]['year']}",
            'similarity_to_previous': similarity,
            'date': embeddings[i]['filing_date']
        })
    
    return {
        'avg_consistency': np.mean([s['similarity_to_previous'] for s in similarities]),
        'consistency_trend': calculate_trend([s['similarity_to_previous'] for s in similarities]),
        'volatility_periods': [s for s in similarities if s['similarity_to_previous'] < 0.7]
    }
```

### 2. Cross-Company Pattern Validation

```python
async def validate_winner_patterns_across_companies():
    """
    Validate that patterns found in one set of winners apply to others.
    """
    
    # Split historical winners into train/test sets
    all_winners = await get_all_historical_winners(['12mo', '24mo'])
    
    train_set = all_winners[:int(len(all_winners) * 0.7)]
    test_set = all_winners[int(len(all_winners) * 0.7):]
    
    # Find patterns in training set
    train_patterns = await cluster_winner_patterns(train_set)
    
    # Test if patterns predict winners in test set
    test_predictions = []
    for test_company in test_set:
        company_embeddings = await get_pre_performance_embeddings(test_company)
        
        # Calculate similarity to each training pattern
        pattern_similarities = calculate_pattern_similarity(
            company_embeddings, 
            train_patterns['cluster_centers']
        )
        
        predicted_winner = max(pattern_similarities) > 0.75  # Threshold
        actual_winner = test_company['performance_tier'] in ['top_1pct', 'top_5pct']
        
        test_predictions.append({
            'company': test_company['company_name'],
            'predicted': predicted_winner,
            'actual': actual_winner,
            'max_similarity': max(pattern_similarities)
        })
    
    # Calculate validation metrics
    accuracy = calculate_prediction_accuracy(test_predictions)
    precision = calculate_precision(test_predictions)
    recall = calculate_recall(test_predictions)
    
    return {
        'validation_accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'successful_patterns': [p for p in train_patterns if p['predictive_power'] > 0.6],
        'failed_predictions': [p for p in test_predictions if p['predicted'] != p['actual']]
    }
```

## Trading Strategy Integration

### 1. Real-Time Pattern Matching

```python
async def find_current_pattern_matches():
    """
    Find companies currently showing patterns similar to historical winners.
    """
    
    # Get recent embeddings for all companies
    recent_embeddings = await get_recent_company_embeddings(months_back=3)
    
    # Load validated winner patterns
    winner_patterns = await load_validated_winner_patterns()
    
    current_candidates = []
    for company, embeddings in recent_embeddings.items():
        for pattern in winner_patterns:
            similarity = calculate_pattern_similarity(embeddings, pattern['signature'])
            
            if similarity > pattern['threshold']:
                current_candidates.append({
                    'company': company,
                    'pattern_match': pattern['name'],
                    'similarity': similarity,
                    'historical_success_rate': pattern['success_rate'],
                    'typical_timeframe': pattern['performance_window'],
                    'expected_return': pattern['avg_return'],
                    'risk_metrics': pattern['risk_profile']
                })
    
    return sorted(current_candidates, key=lambda x: x['similarity'], reverse=True)
```

## Key Benefits

1. **Granular Pattern Discovery**: Find specific patterns for different winner types
2. **Multi-Timeframe Analysis**: Different patterns for short vs long-term winners  
3. **Quality-Adjusted Classification**: Risk-adjusted vs absolute performance patterns
4. **Predictive Validation**: Test patterns on out-of-sample data
5. **Real-Time Application**: Apply learned patterns to current market
6. **Systematic Alpha**: Discover repeatable, data-driven edge

This framework transforms our embeddings from descriptive analysis to **predictive pattern recognition** - exactly the systematic alpha generation you're envisioning!