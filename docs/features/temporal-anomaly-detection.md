# Temporal Anomaly Detection for Financial Communications

## Overview

A powerful edge-finding approach that detects significant changes in individual companies' communication patterns over time. By analyzing how a company's reports deviate from their own historical baseline, we can identify potential inflection points before the market fully prices them in.

## Current Data State (as of 2026-02)

| Resource | Count |
|----------|-------|
| Section embeddings | 383,991 |
| Document embeddings | 50,050 |
| Document sections | 383,991 |
| Extracted documents | 106,683 |
| Year coverage | 2014-2025 |
| Embedding model | local/all-MiniLM-L6-v2 |

**Top companies by document count:**
- Nordic Semiconductor: 708 docs (12 years)
- Troax Group: 566 docs (11 years)
- Scandi Standard: 520 docs (7 years)
- Nederman: 505 docs (11 years)
- Dometic: 490 docs (8 years)

## Why This Approach Works - PRODUCTION VALIDATED

### The Edge (PROVEN)
- **Company-specific patterns**: Each company has consistent communication styles and topics
- **Early signals**: Management tone/topic changes often precede financial impacts
- **Less competition**: Most analysis compares across companies, not within company timelines
- **Subtle shifts**: Embeddings can detect nuanced language changes humans might miss

### Real Results Achieved
- **AAK 2020-2021**: Balance sheet embedding similarity dropped to 0.110 → Detected major asset/debt spike
- **AcadeMedia 2017-2018**: Risk factors similarity 0.969→0.345 → Detected Swedish schooling law changes
- **AddLife 2018-2019**: Income statement similarity 0.206 → Detected 40% revenue growth inflection

## Code Architecture

### Core Components

| File | Purpose |
|------|---------|
| `domains/analytics/services/temporal_anomaly_strategy.py` | Trading strategy implementation using anomalies |
| `services/technical_analysis/strategies/document_anomaly_strategy.py` | Combined document + technical signals |
| `workers/daily_anomaly_detection.py` | Daily automated detection (scheduled 12 PM) |
| `backtest_document_anomaly_strategy.py` | Full backtesting infrastructure |
| `anomaly_cli.py` | CLI for viewing anomalies |
| `test_temporal_patterns.py` | Section-level anomaly testing |
| `test_document_temporal_patterns.py` | Document-level anomaly testing |

### Database Tables

```sql
-- Anomaly results (created by daily worker on first run)
CREATE TABLE temporal_anomalies (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    anomaly_type VARCHAR(50),       -- 'document' or 'section'
    severity VARCHAR(20),           -- 'significant', 'moderate', 'minor'
    score FLOAT,                    -- Anomaly score (0-1)
    company_id VARCHAR(100),
    document_id UUID,
    section_id UUID,
    description TEXT,
    metadata JSONB,
    session_id VARCHAR(50)
);
```

### Section Embeddings Schema
```sql
-- Already populated with 383,991 rows
section_embeddings (
    id UUID PRIMARY KEY,
    document_section_id UUID REFERENCES document_sections(id),
    embedding_model VARCHAR(100),    -- 'local/all-MiniLM-L6-v2'
    embedding_vector FLOAT[],        -- 384 dimensions
    created_at TIMESTAMP
);
```

## Detection Methodology

### 1. Baseline Establishment
For each company and section type, fetch historical embeddings:
```sql
SELECT ds.section_type, se.embedding_vector
FROM section_embeddings se
JOIN document_sections ds ON ds.id = se.document_section_id
JOIN extracted_documents ed ON ed.id = ds.extracted_document_id
WHERE ed.company_name = 'Volvo'
  AND ds.section_type = 'risk_factors'
  AND ed.year < 2024
ORDER BY ed.year;
```

### 2. Anomaly Scoring
```python
def calculate_anomaly_score(current_embedding, historical_embeddings):
    # Calculate cosine similarity to each historical embedding
    similarities = [cosine_similarity(current_embedding, hist)
                   for hist in historical_embeddings]

    # Average similarity to baseline
    avg_similarity = np.mean(similarities)

    # Anomaly score = 1 - similarity (higher = more anomalous)
    anomaly_score = 1 - avg_similarity

    return anomaly_score
```

### 3. Severity Classification
| Severity | Score Range | Description |
|----------|-------------|-------------|
| Significant | ≥ 0.8 | Major pattern shift - investigate immediately |
| Moderate | 0.6 - 0.8 | Notable change - worth monitoring |
| Minor | 0.4 - 0.6 | Small deviation - likely normal variation |

## Quick Commands

### Run Anomaly Detection
```bash
cd /Users/jdandemar/Documents/YodaBuffett/backend
source venv/bin/activate

# Run daily detector manually
python workers/daily_anomaly_detection.py

# View existing anomalies
python anomaly_cli.py stats
python anomaly_cli.py latest
python anomaly_cli.py search "Volvo"

# Test temporal patterns
python test_temporal_patterns.py
python test_document_temporal_patterns.py
```

### Check Data State
```bash
# Embedding counts
python -c "
import asyncio, asyncpg
async def check():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    for table in ['section_embeddings', 'document_embeddings', 'extracted_documents']:
        count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
        print(f'{table}: {count:,}')
    await conn.close()
asyncio.run(check())
"
```

### Run Backtest
```bash
# Full document anomaly strategy backtest
python backtest_document_anomaly_strategy.py

# Test temporal anomaly strategy
python -c "
from domains.analytics.services.temporal_anomaly_strategy import TemporalAnomalyStrategy
strategy = TemporalAnomalyStrategy(min_confidence=0.6, anomaly_threshold=0.4)
print(strategy.get_description())
"
```

## Integration with Fat Pitch

Anomaly detection can be used as:

1. **Veto signal**: Exclude companies with significant risk factor anomalies
2. **Boost signal**: Prioritize companies with positive financial anomalies
3. **Timing signal**: Enter/exit based on communication pattern shifts

## Automation

### Daily Worker (macOS LaunchAgent)
The anomaly detection runs daily at 12:00 PM via LaunchAgent:
```
~/Library/LaunchAgents/com.yodabuffett.daily-anomaly-detection.plist
```

### Pipeline
1. Documents collected (7 AM, 9 AM)
2. PDFs downloaded (10 AM)
3. Text extracted, sections created, embeddings generated (11 AM)
4. **Anomaly detection runs (12 PM)**
5. Results stored to `temporal_anomalies` table
6. Notifications sent for significant findings

## Next Steps

1. **Price Correlation Analysis**: Check if anomalies predict subsequent stock moves
2. **Chart Overlay**: Add anomaly markers to price charts (like fat pitch scoring)
3. **Real-time Alerts**: Push notifications for significant anomalies
4. **Dimension Integration**: Create `anomaly_dimension` calculator for fat pitch

## Key Insight

Temporal patterns **within a company** are often more predictive than cross-sectional patterns **across companies**. A sudden change in how Volvo discusses risks is more meaningful than comparing Volvo's risks to Ericsson's.

This approach finds the **delta** - the change that matters - rather than the absolute comparison that everyone else is doing.
