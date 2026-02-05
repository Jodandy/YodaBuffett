-- Migration 001: Create core screener tables
-- Integrates with existing YodaBuffett infrastructure

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create screener_queries table for saved screening criteria
CREATE TABLE screener_queries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    query_json JSONB NOT NULL,
    user_id UUID, -- Will integrate with existing user system
    is_public BOOLEAN DEFAULT false,
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT screener_queries_name_check CHECK (char_length(name) >= 1)
);

-- Create screener_results table for caching results
CREATE TABLE screener_results (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL REFERENCES screener_queries(id) ON DELETE CASCADE,
    as_of_date DATE NOT NULL,
    results_json JSONB NOT NULL,
    summary_json JSONB NOT NULL, -- Summary statistics
    execution_time_ms INTEGER NOT NULL,
    total_matches INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(query_id, as_of_date)
);

-- Create backtest_runs table for backtesting history
CREATE TABLE backtest_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    query_id UUID NOT NULL REFERENCES screener_queries(id) ON DELETE CASCADE,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    frequency VARCHAR(20) NOT NULL CHECK (frequency IN ('daily', 'weekly', 'monthly')),
    forward_periods TEXT[] NOT NULL, -- ['1M', '3M', '1Y']
    results_json JSONB NOT NULL,
    summary_json JSONB NOT NULL,
    total_execution_time_ms INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create user_screens table for user preferences
CREATE TABLE user_screens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL, -- References existing user table
    query_id UUID NOT NULL REFERENCES screener_queries(id) ON DELETE CASCADE,
    is_favorite BOOLEAN DEFAULT false,
    custom_name VARCHAR(255), -- User can override query name
    last_run_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_id, query_id)
);

-- Create metric_definitions table for available metrics
CREATE TABLE metric_definitions (
    id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    category VARCHAR(50) NOT NULL CHECK (category IN ('fundamental', 'technical', 'derived', 'market')),
    data_type VARCHAR(20) NOT NULL CHECK (data_type IN ('number', 'percentage', 'ratio', 'currency')),
    unit VARCHAR(20),
    is_relative BOOLEAN DEFAULT false, -- Can be used in relative comparisons
    source_table VARCHAR(100), -- Which table contains this metric
    source_column VARCHAR(100), -- Which column contains this metric
    calculation_method TEXT, -- How to calculate if derived
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_screener_queries_user_id ON screener_queries(user_id);
CREATE INDEX idx_screener_queries_public ON screener_queries(is_public) WHERE is_public = true;
CREATE INDEX idx_screener_queries_created_at ON screener_queries(created_at DESC);

CREATE INDEX idx_screener_results_query_date ON screener_results(query_id, as_of_date);
CREATE INDEX idx_screener_results_created_at ON screener_results(created_at DESC);

CREATE INDEX idx_backtest_runs_query_id ON backtest_runs(query_id);
CREATE INDEX idx_backtest_runs_created_at ON backtest_runs(created_at DESC);

CREATE INDEX idx_user_screens_user_id ON user_screens(user_id);
CREATE INDEX idx_user_screens_favorites ON user_screens(user_id, is_favorite) WHERE is_favorite = true;

CREATE INDEX idx_metric_definitions_category ON metric_definitions(category);
CREATE INDEX idx_metric_definitions_active ON metric_definitions(is_active) WHERE is_active = true;

-- Create GIN indexes for JSONB columns for fast searching
CREATE INDEX idx_screener_queries_json ON screener_queries USING GIN(query_json);
CREATE INDEX idx_screener_results_json ON screener_results USING GIN(results_json);
CREATE INDEX idx_backtest_runs_json ON backtest_runs USING GIN(results_json);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for screener_queries
CREATE TRIGGER update_screener_queries_updated_at 
    BEFORE UPDATE ON screener_queries 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial metric definitions
INSERT INTO metric_definitions (id, name, description, category, data_type, unit, is_relative, source_table, source_column) VALUES
-- Fundamental metrics from historical_fundamentals
('pe_ratio', 'P/E Ratio', 'Price to Earnings Ratio', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'pe_ratio'),
('pb_ratio', 'P/B Ratio', 'Price to Book Ratio', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'pb_ratio'),
('ps_ratio', 'P/S Ratio', 'Price to Sales Ratio', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'ps_ratio'),
('pfcf_ratio', 'P/FCF Ratio', 'Price to Free Cash Flow Ratio', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'pfcf_ratio'),
('ev_ebitda', 'EV/EBITDA', 'Enterprise Value to EBITDA', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'ev_ebitda'),
('roe', 'ROE', 'Return on Equity', 'fundamental', 'percentage', '%', true, 'historical_fundamentals', 'roe'),
('roa', 'ROA', 'Return on Assets', 'fundamental', 'percentage', '%', true, 'historical_fundamentals', 'roa'),
('debt_to_equity', 'Debt/Equity', 'Debt to Equity Ratio', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'debt_to_equity'),
('current_ratio', 'Current Ratio', 'Current Assets / Current Liabilities', 'fundamental', 'ratio', '', true, 'historical_fundamentals', 'current_ratio'),
('revenue_growth_yoy', 'Revenue Growth YoY', 'Year over Year Revenue Growth', 'fundamental', 'percentage', '%', true, 'historical_fundamentals', 'revenue_growth_yoy'),
('revenue_growth_qoq', 'Revenue Growth QoQ', 'Quarter over Quarter Revenue Growth', 'fundamental', 'percentage', '%', true, 'historical_fundamentals', 'revenue_growth_qoq'),
('earnings_growth_yoy', 'Earnings Growth YoY', 'Year over Year Earnings Growth', 'fundamental', 'percentage', '%', true, 'historical_fundamentals', 'earnings_growth_yoy'),

-- Technical metrics from market_data_history (assuming these columns exist)
('rsi_14', 'RSI (14)', '14-day Relative Strength Index', 'technical', 'number', '', true, 'market_data_history', 'rsi_14'),
('sma_20', 'SMA (20)', '20-day Simple Moving Average', 'technical', 'currency', '$', true, 'market_data_history', 'sma_20'),
('sma_50', 'SMA (50)', '50-day Simple Moving Average', 'technical', 'currency', '$', true, 'market_data_history', 'sma_50'),
('sma_200', 'SMA (200)', '200-day Simple Moving Average', 'technical', 'currency', '$', true, 'market_data_history', 'sma_200'),
('volume_avg_20', 'Avg Volume (20)', '20-day Average Volume', 'technical', 'number', '', true, 'market_data_history', 'volume_avg_20'),

-- Market/derived metrics
('market_cap', 'Market Cap', 'Market Capitalization', 'market', 'currency', '$', true, 'companies', 'market_cap'),
('price', 'Price', 'Current Stock Price', 'market', 'currency', '$', false, 'market_data_history', 'close'),
('volume', 'Volume', 'Trading Volume', 'market', 'number', '', true, 'market_data_history', 'volume'),

-- Derived metrics (calculated on-the-fly)
('price_change_1d', '1D Price Change', '1-day price change', 'derived', 'percentage', '%', true, NULL, NULL),
('price_change_1w', '1W Price Change', '1-week price change', 'derived', 'percentage', '%', true, NULL, NULL),
('price_change_1m', '1M Price Change', '1-month price change', 'derived', 'percentage', '%', true, NULL, NULL),
('price_change_3m', '3M Price Change', '3-month price change', 'derived', 'percentage', '%', true, NULL, NULL),
('price_change_1y', '1Y Price Change', '1-year price change', 'derived', 'percentage', '%', true, NULL, NULL),
('distance_52w_high', 'Distance from 52W High', 'Percentage below 52-week high', 'derived', 'percentage', '%', true, NULL, NULL),
('distance_52w_low', 'Distance from 52W Low', 'Percentage above 52-week low', 'derived', 'percentage', '%', true, NULL, NULL);

-- Create a view for easy metric access
CREATE VIEW available_metrics AS
SELECT 
    id,
    name,
    description,
    category,
    data_type,
    unit,
    is_relative,
    CASE 
        WHEN source_table IS NOT NULL THEN 'database'
        ELSE 'calculated'
    END as source_type
FROM metric_definitions
WHERE is_active = true
ORDER BY category, name;