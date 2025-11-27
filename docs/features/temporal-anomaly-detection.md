# Temporal Anomaly Detection for Financial Communications

## Overview

A powerful edge-finding approach that detects significant changes in individual companies' communication patterns over time. By analyzing how a company's reports deviate from their own historical baseline, we can identify potential inflection points before the market fully prices them in.

## Why This Approach Works

### The Edge
- **Company-specific patterns**: Each company has consistent communication styles and topics
- **Early signals**: Management tone/topic changes often precede financial impacts
- **Less competition**: Most analysis compares across companies, not within company timelines
- **Subtle shifts**: Embeddings can detect nuanced language changes humans might miss

### No Infrastructure Changes Needed
This approach uses the **exact same embeddings** we're already generating:
- Same document sections (balance sheet, risks, management discussion)
- Same embedding vectors (OpenAI, Cohere, or local)
- Same storage (pgvector)
- Just different analysis!

## Implementation Architecture

```python
class TemporalAnomalyDetector:
    """
    Detects anomalies in company communications over time
    using existing section embeddings
    """
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
    
    async def analyze_company_timeline(
        self, 
        company_name: str,
        lookback_quarters: int = 8
    ) -> List[AnomalyReport]:
        """
        Analyze a company's reporting timeline for anomalies
        
        Steps:
        1. Fetch all historical embeddings for company
        2. Build baseline clusters for each section type
        3. Compare recent reports to baseline
        4. Flag significant deviations
        """
        pass
```

## Detection Methodology

### 1. Baseline Establishment
For each company and section type:
```sql
-- Get historical embeddings for "risk_factors" sections
SELECT ds.section_content, de.embedding_vector
FROM document_sections ds
JOIN document_embeddings de ON ds.id = de.section_id
WHERE ds.company_name = 'Volvo'
  AND ds.section_type = 'risk_factors'
  AND ds.created_at < NOW() - INTERVAL '1 year'
ORDER BY ds.year, ds.quarter;
```

### 2. Clustering Normal Patterns
- Use embeddings to cluster similar historical sections
- Identify recurring themes (normal operations, standard risks)
- Calculate centroid for each cluster

### 3. Anomaly Detection
For new reports:
```python
def detect_anomalies(new_section_embedding, historical_clusters):
    # Calculate distance to nearest historical cluster
    min_similarity = min([
        cosine_similarity(new_section_embedding, cluster.centroid)
        for cluster in historical_clusters
    ])
    
    if min_similarity < ANOMALY_THRESHOLD:
        return Anomaly(
            severity=1 - min_similarity,
            nearest_cluster=closest_cluster,
            deviation_topics=extract_key_differences()
        )
```

### 4. Types of Detectable Anomalies

#### Content Anomalies
- **New risk factors**: Sudden appearance of previously unmentioned risks
- **Topic shifts**: Change in business focus or concerns
- **Missing topics**: Regular discussions that disappear

#### Style Anomalies  
- **Tone changes**: Shift from confident to cautious language
- **Complexity changes**: Sudden increase in vague or complex language
- **Length changes**: Unusual expansion/contraction of sections

## Backtesting Framework

### Historical Analysis
```python
async def backtest_anomaly_detection(
    company: str,
    start_date: date,
    end_date: date,
    forward_days: int = 60
) -> BacktestResults:
    """
    Test if anomalies preceded significant stock movements
    """
    # 1. Detect all historical anomalies
    anomalies = await detect_historical_anomalies(company, start_date, end_date)
    
    # 2. For each anomaly, check subsequent stock performance
    for anomaly in anomalies:
        stock_return = calculate_forward_return(
            company, 
            anomaly.date, 
            forward_days
        )
        
        # 3. Compare to baseline periods
        if abs(stock_return) > SIGNIFICANT_MOVE_THRESHOLD:
            anomaly.predicted_move = True
    
    # 4. Calculate prediction accuracy
    return calculate_metrics(anomalies)
```

### Metrics to Track
- **Precision**: What % of flagged anomalies preceded moves?
- **Recall**: What % of significant moves had anomaly warnings?
- **Lead time**: How early did anomalies appear before moves?
- **Severity correlation**: Do bigger anomalies → bigger moves?

## Example Anomaly Patterns

### Case 1: Volvo Q2 2024 - New Risk Factor
```json
{
  "company": "Volvo",
  "report": "Q2-2024",
  "section": "risk_factors",
  "anomaly_score": 0.92,
  "description": "First mention of 'semiconductor shortage' and 'supply chain disruption'",
  "historical_similarity": 0.62,
  "subsequent_stock_move": -8.3%
}
```

### Case 2: Ericsson Annual 2023 - Tone Shift
```json
{
  "company": "Ericsson",
  "report": "Annual-2023",
  "section": "management_discussion",
  "anomaly_score": 0.87,
  "description": "Shift from 'growth' language to 'optimization' and 'efficiency'",
  "historical_similarity": 0.71,
  "subsequent_stock_move": -12.1%
}
```

## Integration with Existing Pipeline

### No Changes Needed!
```bash
# 1. Continue generating embeddings as normal
python domains/document_intelligence/cli_multi_embeddings.py batch

# 2. Run anomaly detection on accumulated embeddings
python domains/analytics/cli_temporal_anomaly.py analyze --company=Volvo

# 3. Backtest historical performance
python domains/analytics/cli_temporal_anomaly.py backtest --years=5
```

### Storage Optimization
- Embeddings are already stored with metadata (company, year, section_type)
- Just need to add anomaly detection results table:

```sql
CREATE TABLE temporal_anomalies (
    id UUID PRIMARY KEY,
    company_name VARCHAR(200),
    report_date DATE,
    section_type VARCHAR(50),
    anomaly_score FLOAT,
    baseline_similarity FLOAT,
    deviation_summary TEXT,
    detected_at TIMESTAMP DEFAULT NOW()
);
```

## Competitive Advantages

1. **Unique approach**: Most competitors compare across companies, not within
2. **Early detection**: Language changes precede reported numbers
3. **Scalable**: Works automatically across all companies
4. **Interpretable**: Can explain what changed and why it matters
5. **Low false positives**: Company-specific baselines reduce noise

## Next Steps

1. **Generate embeddings** for historical documents (continue current work)
2. **Build prototype** anomaly detector for single company
3. **Backtest** on 5 Swedish companies over 5 years
4. **Tune thresholds** based on backtest results
5. **Deploy monitoring** for real-time anomaly alerts

## Key Insight

The brilliant realization is that **temporal patterns within a company** are often more predictive than **cross-sectional patterns across companies**. A sudden change in how Volvo discusses risks is more meaningful than comparing Volvo's risks to Ericsson's risks.

This approach finds the **delta** - the change that matters - rather than the **absolute** comparison that everyone else is doing.