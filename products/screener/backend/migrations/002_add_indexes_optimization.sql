-- Migration 002: Performance optimization indexes
-- Optimizes for common screener query patterns

-- Add composite indexes for common screening patterns on historical_fundamentals
-- (These may already exist, but ensuring they're optimized for screener use)

-- Point-in-time queries with date and company
CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_date_company 
ON historical_fundamentals(date DESC, company_id);

-- Metric-specific queries for screening
CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_pe_ratio 
ON historical_fundamentals(pe_ratio) WHERE pe_ratio IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_pb_ratio 
ON historical_fundamentals(pb_ratio) WHERE pb_ratio IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_ps_ratio 
ON historical_fundamentals(ps_ratio) WHERE ps_ratio IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_revenue_growth_yoy 
ON historical_fundamentals(revenue_growth_yoy) WHERE revenue_growth_yoy IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_roe 
ON historical_fundamentals(roe) WHERE roe IS NOT NULL;

-- Market data indexes for technical screening
CREATE INDEX IF NOT EXISTS idx_market_data_history_date_symbol 
ON market_data_history(date DESC, symbol);

CREATE INDEX IF NOT EXISTS idx_market_data_history_rsi 
ON market_data_history(rsi_14) WHERE rsi_14 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_market_data_history_volume 
ON market_data_history(volume) WHERE volume > 0;

-- Companies table optimization
CREATE INDEX IF NOT EXISTS idx_companies_market_cap 
ON companies(market_cap) WHERE market_cap IS NOT NULL;

-- Screening performance - multi-column indexes for common combinations
CREATE INDEX IF NOT EXISTS idx_fundamentals_valuation_screening 
ON historical_fundamentals(date, pe_ratio, pb_ratio, ps_ratio) 
WHERE pe_ratio IS NOT NULL OR pb_ratio IS NOT NULL OR ps_ratio IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fundamentals_growth_screening 
ON historical_fundamentals(date, revenue_growth_yoy, earnings_growth_yoy, roe) 
WHERE revenue_growth_yoy IS NOT NULL OR earnings_growth_yoy IS NOT NULL OR roe IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fundamentals_quality_screening 
ON historical_fundamentals(date, roe, roa, debt_to_equity, current_ratio) 
WHERE roe IS NOT NULL OR roa IS NOT NULL OR debt_to_equity IS NOT NULL OR current_ratio IS NOT NULL;

-- Forward return calculation optimization - need efficient price lookups
CREATE INDEX IF NOT EXISTS idx_market_data_forward_returns 
ON market_data_history(symbol, date, close) 
WHERE close IS NOT NULL;

-- Partial indexes for specific screening ranges (common filters)
CREATE INDEX IF NOT EXISTS idx_fundamentals_low_pe 
ON historical_fundamentals(date, company_id) 
WHERE pe_ratio BETWEEN 1 AND 20;

CREATE INDEX IF NOT EXISTS idx_fundamentals_high_growth 
ON historical_fundamentals(date, company_id) 
WHERE revenue_growth_yoy > 10;

CREATE INDEX IF NOT EXISTS idx_fundamentals_profitable 
ON historical_fundamentals(date, company_id) 
WHERE roe > 0;

-- Create materialized view for latest metrics (for current screening)
CREATE MATERIALIZED VIEW latest_fundamentals AS
SELECT DISTINCT ON (company_id)
    company_id,
    date,
    pe_ratio,
    pb_ratio,
    ps_ratio,
    pfcf_ratio,
    ev_ebitda,
    roe,
    roa,
    debt_to_equity,
    current_ratio,
    revenue_growth_yoy,
    revenue_growth_qoq,
    earnings_growth_yoy
FROM historical_fundamentals
ORDER BY company_id, date DESC;

-- Index the materialized view
CREATE INDEX idx_latest_fundamentals_company_id ON latest_fundamentals(company_id);
CREATE INDEX idx_latest_fundamentals_pe_ratio ON latest_fundamentals(pe_ratio) WHERE pe_ratio IS NOT NULL;
CREATE INDEX idx_latest_fundamentals_pb_ratio ON latest_fundamentals(pb_ratio) WHERE pb_ratio IS NOT NULL;
CREATE INDEX idx_latest_fundamentals_revenue_growth ON latest_fundamentals(revenue_growth_yoy) WHERE revenue_growth_yoy IS NOT NULL;

-- Create materialized view for latest market data
CREATE MATERIALIZED VIEW latest_market_data AS
SELECT DISTINCT ON (symbol)
    symbol,
    date,
    close as price,
    volume,
    rsi_14,
    sma_20,
    sma_50,
    sma_200,
    volume_avg_20
FROM market_data_history
ORDER BY symbol, date DESC;

-- Index the market data materialized view
CREATE INDEX idx_latest_market_data_symbol ON latest_market_data(symbol);
CREATE INDEX idx_latest_market_data_price ON latest_market_data(price) WHERE price IS NOT NULL;
CREATE INDEX idx_latest_market_data_rsi ON latest_market_data(rsi_14) WHERE rsi_14 IS NOT NULL;

-- Function to refresh materialized views (for scheduled updates)
CREATE OR REPLACE FUNCTION refresh_screener_views() 
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW latest_fundamentals;
    REFRESH MATERIALIZED VIEW latest_market_data;
END;
$$ LANGUAGE plpgsql;

-- Create a scheduled job to refresh views (adjust timing as needed)
-- This would typically be done via cron or application scheduler
-- SELECT cron.schedule('refresh-screener-views', '0 6 * * *', 'SELECT refresh_screener_views();');