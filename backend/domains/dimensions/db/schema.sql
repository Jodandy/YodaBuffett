-- Dimensions Scoring System Database Schema
-- Flexible, extensible architecture for any type of dimension scoring

-- =====================================================
-- DIMENSION REGISTRY
-- =====================================================

-- Dimension definitions - what dimensions exist and their metadata
-- Each dimension is a "black box" - this just registers it
CREATE TABLE IF NOT EXISTS dimension_definitions (
    id SERIAL PRIMARY KEY,
    dimension_code VARCHAR(50) NOT NULL,      -- 'value', 'momentum', 'sentiment', 'moat', 'regulatory_risk', etc.
    version INTEGER NOT NULL DEFAULT 1,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50),                      -- 'fundamental', 'technical', 'alternative', 'ai_derived', 'external'

    -- Flexible configuration (dimension-specific)
    config JSONB DEFAULT '{}',                 -- Whatever config this dimension needs

    -- Metadata about the dimension
    data_sources TEXT[],                       -- ['daily_fundamentals', 'news_api', 'embeddings', etc.]
    update_frequency VARCHAR(20) DEFAULT 'daily',  -- 'realtime', 'daily', 'weekly', 'quarterly'
    requires_external_api BOOLEAN DEFAULT false,

    -- Status
    is_active BOOLEAN DEFAULT true,
    effective_from DATE NOT NULL DEFAULT CURRENT_DATE,
    effective_to DATE,                         -- NULL = currently active

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(dimension_code, version)
);

CREATE INDEX IF NOT EXISTS idx_dimension_definitions_code ON dimension_definitions(dimension_code);
CREATE INDEX IF NOT EXISTS idx_dimension_definitions_active ON dimension_definitions(is_active, effective_from);

-- =====================================================
-- DAILY DIMENSION SCORES
-- =====================================================

-- The core output table - standardized scores from any dimension
CREATE TABLE IF NOT EXISTS daily_dimension_scores (
    id BIGSERIAL PRIMARY KEY,
    company_id UUID NOT NULL,                  -- References company_master(id)
    score_date DATE NOT NULL,
    dimension_code VARCHAR(50) NOT NULL,
    definition_version INTEGER NOT NULL DEFAULT 1,

    -- Core score (standardized 0-100 scale)
    score DECIMAL(6,3) NOT NULL,               -- 0.000 to 100.000

    -- Confidence and reliability
    confidence DECIMAL(5,4),                   -- 0.0000 to 1.0000
    data_quality DECIMAL(5,4),                 -- How complete/reliable was input data

    -- Percentile ranking within universe (optional, calculated post-hoc)
    percentile_rank DECIMAL(6,3),              -- 0-100, where this company ranks
    universe_size INTEGER,                     -- How many companies in comparison
    universe_filter JSONB,                     -- What filter was used (sector, country, etc.)

    -- Score uncertainty/range (optional)
    score_low DECIMAL(6,3),                    -- Lower bound estimate
    score_high DECIMAL(6,3),                   -- Upper bound estimate

    -- Flexible metadata (dimension-specific details)
    -- This is where each dimension puts its unique breakdown
    metadata JSONB DEFAULT '{}',
    -- Examples:
    -- Value: {"pe_contribution": 18.5, "pb_contribution": 12.3, "ev_ebitda_raw": 8.4}
    -- Sentiment: {"news_score": 0.72, "social_score": 0.45, "filing_tone": 0.68, "sources_analyzed": 47}
    -- Moat: {"moat_type": "wide", "durability": "high", "key_factors": ["brand", "network_effects"]}
    -- Credit: {"rating": "BBB+", "outlook": "stable", "altman_z": 3.2, "interest_coverage": 8.5}

    -- Computation metadata
    computed_at TIMESTAMP DEFAULT NOW(),
    computation_time_ms INTEGER,
    calculator_version VARCHAR(20),

    UNIQUE(company_id, score_date, dimension_code)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_dimension_scores_lookup
    ON daily_dimension_scores(company_id, score_date, dimension_code);
CREATE INDEX IF NOT EXISTS idx_dimension_scores_date
    ON daily_dimension_scores(score_date DESC);
CREATE INDEX IF NOT EXISTS idx_dimension_scores_dimension_date
    ON daily_dimension_scores(dimension_code, score_date DESC);
CREATE INDEX IF NOT EXISTS idx_dimension_scores_ranking
    ON daily_dimension_scores(dimension_code, score_date, percentile_rank DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_dimension_scores_score
    ON daily_dimension_scores(dimension_code, score_date, score DESC);
CREATE INDEX IF NOT EXISTS idx_dimension_scores_metadata
    ON daily_dimension_scores USING GIN(metadata);

-- =====================================================
-- COMPOSITE SCORES (Multi-Dimension Aggregates)
-- =====================================================

-- Pre-computed combinations of dimensions
CREATE TABLE IF NOT EXISTS composite_scores (
    id BIGSERIAL PRIMARY KEY,
    company_id UUID NOT NULL,
    score_date DATE NOT NULL,
    composite_code VARCHAR(50) NOT NULL,       -- 'overall', 'quality_value', 'risk_adjusted_momentum', etc.

    -- Combined score
    score DECIMAL(6,3) NOT NULL,
    confidence DECIMAL(5,4),
    percentile_rank DECIMAL(6,3),

    -- What dimensions contributed and their weights
    dimension_scores JSONB NOT NULL,           -- {"value": 72.5, "momentum": 68.3, "quality": 81.2}
    dimension_weights JSONB NOT NULL,          -- {"value": 0.25, "momentum": 0.20, "quality": 0.25}

    -- Which dimensions were missing/unavailable
    missing_dimensions TEXT[],

    computed_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(company_id, score_date, composite_code)
);

CREATE INDEX IF NOT EXISTS idx_composite_scores_lookup
    ON composite_scores(company_id, score_date);
CREATE INDEX IF NOT EXISTS idx_composite_scores_ranking
    ON composite_scores(composite_code, score_date, score DESC);

-- =====================================================
-- SCORE HISTORY (Aggregated for Trends)
-- =====================================================

-- Weekly/monthly aggregates for efficient trend queries
CREATE TABLE IF NOT EXISTS dimension_score_history (
    id BIGSERIAL PRIMARY KEY,
    company_id UUID NOT NULL,
    dimension_code VARCHAR(50) NOT NULL,
    period_type VARCHAR(10) NOT NULL,          -- 'weekly', 'monthly', 'quarterly'
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,

    -- Period statistics
    avg_score DECIMAL(6,3),
    min_score DECIMAL(6,3),
    max_score DECIMAL(6,3),
    end_score DECIMAL(6,3),                    -- Score at period end
    score_volatility DECIMAL(6,3),             -- Std dev within period

    -- Trend indicators
    score_change DECIMAL(7,3),                 -- Change from previous period
    score_change_pct DECIMAL(7,3),
    trend_direction VARCHAR(15),               -- 'improving', 'declining', 'stable', 'volatile'

    -- Percentile at period end
    end_percentile DECIMAL(6,3),

    computed_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(company_id, dimension_code, period_type, period_start)
);

CREATE INDEX IF NOT EXISTS idx_score_history_lookup
    ON dimension_score_history(company_id, dimension_code, period_type, period_start DESC);

-- =====================================================
-- DIMENSION COMPUTATION LOG
-- =====================================================

-- Track batch computation runs for monitoring/debugging
CREATE TABLE IF NOT EXISTS dimension_computation_log (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID DEFAULT gen_random_uuid(),
    dimension_code VARCHAR(50) NOT NULL,
    score_date DATE NOT NULL,

    -- Run statistics
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'running',      -- 'running', 'completed', 'failed', 'partial'

    -- Results
    companies_processed INTEGER DEFAULT 0,
    companies_succeeded INTEGER DEFAULT 0,
    companies_failed INTEGER DEFAULT 0,
    companies_skipped INTEGER DEFAULT 0,

    -- Performance
    total_duration_ms INTEGER,
    avg_company_time_ms INTEGER,

    -- Errors
    error_summary JSONB,                       -- {"timeout": 5, "missing_data": 12, "api_error": 2}

    -- Config used
    config_snapshot JSONB,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_computation_log_lookup
    ON dimension_computation_log(dimension_code, score_date DESC);
CREATE INDEX IF NOT EXISTS idx_computation_log_status
    ON dimension_computation_log(status, started_at DESC);

-- =====================================================
-- HELPER VIEWS
-- =====================================================

-- Latest scores for all companies (most recent date per dimension)
CREATE OR REPLACE VIEW latest_dimension_scores AS
SELECT DISTINCT ON (company_id, dimension_code)
    ds.*
FROM daily_dimension_scores ds
WHERE ds.score_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY company_id, dimension_code, score_date DESC;

-- Company dimension summary (pivot of all dimensions)
CREATE OR REPLACE VIEW company_dimension_summary AS
SELECT
    company_id,
    MAX(score_date) as score_date,
    MAX(CASE WHEN dimension_code = 'value' THEN score END) as value_score,
    MAX(CASE WHEN dimension_code = 'momentum' THEN score END) as momentum_score,
    MAX(CASE WHEN dimension_code = 'quality' THEN score END) as quality_score,
    MAX(CASE WHEN dimension_code = 'sentiment' THEN score END) as sentiment_score,
    MAX(CASE WHEN dimension_code = 'risk' THEN score END) as risk_score,
    MAX(CASE WHEN dimension_code = 'value' THEN percentile_rank END) as value_percentile,
    MAX(CASE WHEN dimension_code = 'momentum' THEN percentile_rank END) as momentum_percentile,
    MAX(CASE WHEN dimension_code = 'quality' THEN percentile_rank END) as quality_percentile,
    MAX(CASE WHEN dimension_code = 'sentiment' THEN percentile_rank END) as sentiment_percentile,
    MAX(CASE WHEN dimension_code = 'risk' THEN percentile_rank END) as risk_percentile
FROM daily_dimension_scores
WHERE score_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY company_id;
