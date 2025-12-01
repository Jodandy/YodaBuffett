# Multi-Label Classification Design for Financial Document Embeddings

## Overview

Financial document sections often contain **multiple overlapping themes** that require **multi-label classification** rather than single-label approaches. This document outlines the design for implementing multi-label classification in the YodaBuffett embedding system.

## Current Single-Label Limitations

### Existing Approach
```python
# Current section classifier (single label only)
SECTION_TYPES = {
    'balance_sheet': 'Balance Sheet - Assets, liabilities, equity',
    'income_statement': 'Income Statement - Revenue, expenses, profit/loss', 
    'risk_factors': 'Risk Factors - Risk identification, uncertainties',
    'strategy': 'Strategic Direction - Future plans, outlook'
}
```

### Problems with Single Labels
1. **Missing Semantic Richness**: A section discussing "strategic risks in expansion" gets labeled as either "risk_factors" OR "strategy", losing the intersection
2. **Complex Financial Discussions**: Balance sheet discussions that include risk assessment are forced into one category
3. **Lost Trading Signals**: Multi-dimensional insights are flattened into single dimensions

## Multi-Label Classification Architecture

### 1. Extended Classification Schema

```python
# Multi-label financial themes
FINANCIAL_THEMES = {
    # Core Financial Statements
    'balance_sheet': 'Assets, liabilities, equity, financial position',
    'income_statement': 'Revenue, expenses, profit/loss, EBIT, margins',
    'cash_flow': 'Operating, investing, financing cash flows, liquidity',
    
    # Business Themes  
    'growth_strategy': 'Expansion plans, market entry, investment strategy',
    'risk_management': 'Risk identification, mitigation, uncertainties',
    'competitive_position': 'Market share, competitive advantages, positioning',
    'operational_efficiency': 'Cost management, productivity, optimization',
    'capital_allocation': 'Investments, acquisitions, dividends, buybacks',
    
    # Market & Environment
    'market_outlook': 'Industry trends, market conditions, forecasts',
    'regulatory_environment': 'Compliance, regulations, policy changes',
    'economic_conditions': 'Macro environment, inflation, interest rates',
    
    # ESG & Governance
    'sustainability': 'ESG metrics, environmental initiatives, social impact',
    'corporate_governance': 'Board composition, governance practices, transparency',
    
    # Communication Themes
    'management_optimism': 'Positive outlook, confidence, opportunity language',
    'management_caution': 'Conservative tone, warning signals, risk emphasis',
    'strategic_transformation': 'Business model changes, pivots, restructuring',
    'financial_stress': 'Liquidity concerns, covenant issues, distress signals'
}
```

### 2. Multi-Label Database Schema

```sql
-- Enhanced section classification table
CREATE TABLE section_classifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_section_id UUID NOT NULL REFERENCES document_sections(id),
    theme VARCHAR(50) NOT NULL,
    confidence FLOAT NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    classification_model VARCHAR(100) NOT NULL,
    reasoning TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- Composite index for efficient multi-label queries
    UNIQUE(document_section_id, theme, classification_model)
);

-- Index for fast theme-based searches
CREATE INDEX idx_classifications_theme ON section_classifications(theme, confidence DESC);
CREATE INDEX idx_classifications_section ON section_classifications(document_section_id);

-- View for easy multi-label analysis
CREATE VIEW section_multi_labels AS
SELECT 
    ds.id as section_id,
    ds.extracted_document_id,
    ed.company_name,
    ed.form_type,
    ed.year,
    ds.section_type,
    ds.section_title,
    ARRAY_AGG(sc.theme ORDER BY sc.confidence DESC) as themes,
    ARRAY_AGG(sc.confidence ORDER BY sc.confidence DESC) as confidences
FROM document_sections ds
JOIN section_classifications sc ON ds.id = sc.document_section_id  
JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
GROUP BY ds.id, ds.extracted_document_id, ed.company_name, ed.form_type, ed.year, ds.section_type, ds.section_title;
```

### 3. Enhanced Classification Prompt

```python
def _build_multi_label_prompt(self, chunk_text: str, context: Dict = None) -> str:
    """Build multi-label classification prompt"""
    
    themes_list = '\n'.join([f"- {key}: {desc}" for key, desc in self.FINANCIAL_THEMES.items()])
    
    prompt = f"""You are analyzing a section from a Nordic financial report. 
This section may contain MULTIPLE overlapping themes. Your task is to identify ALL relevant themes present.

Available themes:
{themes_list}

Context: {json.dumps(context, indent=2) if context else "No context"}

Text to classify:
\"\"\"{chunk_text[:1200]}\"\"\"

Instructions:
1. Identify ALL themes present in this text (not just the primary one)
2. Assign a confidence score (0.0-1.0) for each theme
3. Only include themes with confidence >= 0.3
4. Consider Nordic financial reporting standards and language
5. Look for overlapping concepts (e.g., growth strategy + competitive position)

Respond in this exact JSON format:
{{
    "themes": [
        {{"theme": "balance_sheet", "confidence": 0.92, "reasoning": "Contains asset and liability discussions"}},
        {{"theme": "risk_management", "confidence": 0.67, "reasoning": "Mentions credit risk and exposure management"}},
        {{"theme": "regulatory_environment", "confidence": 0.45, "reasoning": "References Basel III compliance requirements"}}
    ],
    "primary_theme": "balance_sheet",
    "complexity_score": 0.75
}}"""
    
    return prompt
```

### 4. Multi-Label Analysis Queries

```sql
-- Find sections with specific theme combinations
SELECT ed.company_name, ed.year, ds.section_title, sml.themes, sml.confidences
FROM section_multi_labels sml
JOIN extracted_documents ed ON sml.extracted_document_id = ed.id  
JOIN document_sections ds ON sml.section_id = ds.id
WHERE 'growth_strategy' = ANY(sml.themes) 
  AND 'risk_management' = ANY(sml.themes)
  AND ed.company_name = 'Volvo'
ORDER BY ed.year DESC;

-- Theme co-occurrence analysis across companies
SELECT 
    t1.theme as theme_1,
    t2.theme as theme_2, 
    COUNT(*) as co_occurrence_count
FROM section_classifications t1
JOIN section_classifications t2 ON t1.document_section_id = t2.document_section_id
WHERE t1.theme < t2.theme  -- Avoid duplicates
  AND t1.confidence > 0.5 
  AND t2.confidence > 0.5
GROUP BY t1.theme, t2.theme
ORDER BY co_occurrence_count DESC
LIMIT 20;

-- Company theme evolution over time
SELECT 
    ed.company_name,
    ed.year,
    sc.theme,
    COUNT(*) as section_count,
    AVG(sc.confidence) as avg_confidence
FROM section_classifications sc
JOIN document_sections ds ON sc.document_section_id = ds.id
JOIN extracted_documents ed ON ds.extracted_document_id = ed.id  
WHERE ed.company_name = 'AAK'
  AND sc.confidence > 0.5
GROUP BY ed.company_name, ed.year, sc.theme
ORDER BY ed.year, avg_confidence DESC;
```

## Trading Signal Applications

### 1. Multi-Dimensional Anomaly Detection

```python
async def detect_multi_label_anomalies(company: str, baseline_years: List[int], current_year: int):
    """Detect anomalies in multi-label theme distributions"""
    
    # Get baseline theme distribution
    baseline_themes = await get_theme_distribution(company, baseline_years)
    current_themes = await get_theme_distribution(company, [current_year])
    
    anomalies = []
    for theme, current_freq in current_themes.items():
        baseline_freq = baseline_themes.get(theme, 0)
        change = (current_freq - baseline_freq) / max(baseline_freq, 0.01)
        
        if abs(change) > 0.5:  # 50% change threshold
            anomalies.append({
                'theme': theme,
                'change': change,
                'signal': 'increase' if change > 0 else 'decrease',
                'magnitude': abs(change)
            })
    
    return anomalies

# Example output:
# [
#   {'theme': 'financial_stress', 'change': 2.3, 'signal': 'increase', 'magnitude': 2.3},
#   {'theme': 'growth_strategy', 'change': -0.6, 'signal': 'decrease', 'magnitude': 0.6}
# ]
```

### 2. Complex Pattern Search

```python
# Find companies with specific theme combinations
companies_with_growth_and_risk = await search_multi_themes([
    ('growth_strategy', 0.7),
    ('risk_management', 0.6), 
    ('financial_stress', 0.4)
], year=2025)

# Strategic transformation detection
transformation_signals = await search_multi_themes([
    ('strategic_transformation', 0.6),
    ('operational_efficiency', 0.5),
    ('capital_allocation', 0.5)
], exclude_themes=['financial_stress'])
```

### 3. Enhanced Temporal Analysis

Instead of single-dimension temporal analysis, we can track multi-dimensional shifts:

```python
# Multi-theme temporal anomaly
{
    'company': 'AAK',
    'year': 2021,
    'anomaly_type': 'multi_theme_shift',
    'themes_increased': ['financial_stress', 'risk_management', 'regulatory_environment'],
    'themes_decreased': ['growth_strategy', 'management_optimism'],
    'complexity_change': +0.3,  # More complex multi-theme discussions
    'signal_strength': 0.82
}
```

## Implementation Strategy

### Phase 1: Extend Current System
1. Add multi-label classification table
2. Modify existing classifier to support multiple themes
3. Update CLI tools to show multi-label results

### Phase 2: Multi-Label Analytics
1. Implement theme co-occurrence analysis
2. Build multi-dimensional anomaly detection
3. Create temporal multi-theme tracking

### Phase 3: Trading Integration
1. Connect multi-label signals to backtesting framework
2. Build theme combination strategies
3. Implement multi-dimensional risk signals

## Database Migration Script

```sql
-- Add multi-label classification support
BEGIN;

CREATE TABLE section_classifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_section_id UUID NOT NULL REFERENCES document_sections(id) ON DELETE CASCADE,
    theme VARCHAR(50) NOT NULL,
    confidence FLOAT NOT NULL CHECK (confidence BETWEEN 0 AND 1),
    classification_model VARCHAR(100) NOT NULL DEFAULT 'gpt-4o-mini',
    reasoning TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(document_section_id, theme, classification_model)
);

CREATE INDEX idx_classifications_theme ON section_classifications(theme, confidence DESC);
CREATE INDEX idx_classifications_section ON section_classifications(document_section_id);
CREATE INDEX idx_classifications_confidence ON section_classifications(confidence DESC);

-- Migration view to preserve existing single-label queries
CREATE VIEW section_primary_classification AS
SELECT 
    sc.document_section_id,
    sc.theme as section_type,
    sc.confidence,
    sc.classification_model,
    sc.created_at
FROM section_classifications sc
WHERE sc.confidence = (
    SELECT MAX(confidence) 
    FROM section_classifications sc2 
    WHERE sc2.document_section_id = sc.document_section_id
    AND sc2.classification_model = sc.classification_model
);

COMMIT;
```

## Key Benefits

1. **Richer Semantic Understanding**: Capture complex financial narratives with multiple themes
2. **Better Trading Signals**: Multi-dimensional anomaly detection vs single-dimension
3. **Pattern Recognition**: Find companies with similar theme combinations
4. **Temporal Complexity**: Track how communication complexity changes over time
5. **Systematic Alpha**: Discover theme combinations that predict performance

This multi-label approach transforms our classification from "what type of section is this?" to "what themes are present in this communication?" - much more powerful for trading signal generation.