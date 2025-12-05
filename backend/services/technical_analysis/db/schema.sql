-- Technical Analysis ML Database Schema  
-- Focused on core ML models, labels, and backtesting results
-- Indicators calculated on-demand from market data

-- =====================================================
-- ML MODEL MANAGEMENT
-- =====================================================

-- ML Model definitions
CREATE TABLE ml_models (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    model_type VARCHAR(50) NOT NULL, -- 'knn', 'random_forest', 'neural_net', 'svm'
    description TEXT,
    
    -- Model configuration
    parameters JSONB, -- {k: 5, distance: 'cosine'} for KNN, etc.
    
    -- Indicator configuration (what indicators + settings this model uses)
    indicator_config JSONB, -- {"rsi": {"period": 14}, "sma": {"period": 20}, "bb": {"period": 20, "std_dev": 2.0}}
    
    output_labels TEXT[], -- what we're predicting ['buy', 'hold', 'sell']
    
    -- Model artifacts storage  
    model_data JSONB, -- trained weights, tree structures, feature vectors for KNN, etc.
    feature_scaler JSONB, -- normalization parameters
    
    -- Performance tracking
    training_accuracy NUMERIC,
    validation_accuracy NUMERIC,
    last_trained TIMESTAMP,
    
    version INTEGER DEFAULT 1,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Pre-computed KNN neighbors (time-aware, no look-ahead bias)
CREATE TABLE knn_neighbors (
    id SERIAL PRIMARY KEY,
    model_id INTEGER REFERENCES ml_models(id),
    company_id INTEGER NOT NULL,
    prediction_date DATE NOT NULL,
    
    -- Pre-computed neighbors sorted by distance (only from dates BEFORE prediction_date)
    -- Each neighbor: {"date": "2024-01-10", "distance": 0.05, "label": "buy", "features": [...]}
    neighbors JSONB NOT NULL,
    
    -- Cache metadata
    feature_vector JSONB, -- the calculated feature vector for this date
    num_neighbors_available INTEGER, -- how many historical points were available
    calculation_time_ms INTEGER, -- performance tracking
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(model_id, company_id, prediction_date)
);

CREATE INDEX idx_knn_neighbors_lookup ON knn_neighbors(model_id, company_id, prediction_date);
CREATE INDEX idx_knn_neighbors_date ON knn_neighbors(prediction_date);
CREATE INDEX idx_knn_neighbors_company ON knn_neighbors(company_id);

-- Training labels - what we're trying to predict
CREATE TABLE ml_labels (
    id SERIAL PRIMARY KEY,
    company_id INTEGER NOT NULL,
    date DATE NOT NULL,
    timeframe VARCHAR(10) DEFAULT 'daily',
    
    -- Future outcomes we want to predict (flexible structure)
    labels JSONB NOT NULL, -- {"1d_return": 0.05, "5d_direction": "up", "20d_volatility": 0.15}
    
    -- Label metadata
    label_type VARCHAR(50) NOT NULL, -- 'price_returns', 'direction', 'volatility', 'custom'
    prediction_horizons INTEGER[], -- [1, 5, 20] - days ahead being predicted
    
    -- Source and confidence
    source VARCHAR(50), -- 'calculated', 'manual', 'external'
    confidence NUMERIC, -- quality/reliability of this label
    metadata JSONB,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(company_id, date, timeframe, label_type)
);

CREATE INDEX idx_ml_labels_lookup ON ml_labels(company_id, date);

-- =====================================================
-- STRATEGY FRAMEWORK
-- =====================================================

-- Strategy definitions (combines indicators + ML models + rules)
CREATE TABLE strategies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    strategy_type VARCHAR(50), -- 'technical', 'fundamental', 'ml_hybrid', 'anomaly_detection'
    
    -- Strategy configuration
    config JSONB, -- All strategy parameters, thresholds, required indicators, etc.
    ml_models INTEGER[], -- ml_model id's used
    
    -- Performance tracking
    last_backtest_date DATE,
    sharpe_ratio NUMERIC,
    max_drawdown NUMERIC,
    total_return NUMERIC,
    
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Strategy signals/predictions
CREATE TABLE strategy_signals (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES strategies(id),
    company_id INTEGER NOT NULL,
    date DATE NOT NULL,
    
    signal VARCHAR(20) NOT NULL, -- 'buy', 'sell', 'hold'
    confidence NUMERIC, -- 0.0 to 1.0
    strength NUMERIC, -- signal strength/magnitude
    
    -- What drove this signal
    contributing_factors JSONB, -- {rsi_14: 73.2, sma_20: 156.78, ml_prediction: "buy"}
    metadata JSONB,
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(strategy_id, company_id, date)
);

CREATE INDEX idx_strategy_signals_lookup ON strategy_signals(strategy_id, company_id, date);
CREATE INDEX idx_strategy_signals_date ON strategy_signals(date);

-- =====================================================
-- BACKTESTING INFRASTRUCTURE
-- =====================================================

-- Backtest runs
CREATE TABLE backtest_runs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    strategy_id INTEGER REFERENCES strategies(id),
    
    -- Test period
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    
    -- Configuration
    initial_capital NUMERIC DEFAULT 100000,
    transaction_cost NUMERIC DEFAULT 0.001, -- 0.1%
    config JSONB, -- position sizing, rebalancing, etc.
    
    -- Results
    final_value NUMERIC,
    total_return NUMERIC,
    sharpe_ratio NUMERIC,
    max_drawdown NUMERIC,
    num_trades INTEGER,
    win_rate NUMERIC,
    
    status VARCHAR(20) DEFAULT 'running', -- 'running', 'completed', 'failed'
    created_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

-- Individual backtest trades
CREATE TABLE backtest_trades (
    id SERIAL PRIMARY KEY,
    backtest_run_id INTEGER REFERENCES backtest_runs(id),
    company_id INTEGER NOT NULL,
    
    -- Trade details
    action VARCHAR(10) NOT NULL, -- 'buy', 'sell'
    date DATE NOT NULL,
    price NUMERIC NOT NULL,
    quantity INTEGER NOT NULL,
    
    -- Context
    signal_confidence NUMERIC,
    portfolio_weight NUMERIC,
    reason TEXT,
    
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_backtest_trades_run ON backtest_trades(backtest_run_id);

-- =====================================================
-- SAMPLE DATA INSERTS
-- =====================================================

-- Sample KNN model for RSI classification  
INSERT INTO ml_models (name, model_type, description, parameters, indicator_config, output_labels) VALUES
(
    'rsi_reversal_knn',
    'knn', 
    'KNN model for RSI mean reversion signals',
    '{"k": 5, "distance": "euclidean", "weights": "distance"}',
    '{"rsi": {"period": 14}, "sma": {"period": 20}, "volume_sma": {"period": 20}}',
    ARRAY['buy', 'sell', 'hold']
),
(
    'multi_timeframe_momentum',
    'knn',
    'Multi-timeframe momentum classification using RSI and price changes', 
    '{"k": 7, "distance": "cosine"}',
    '{"rsi": {"period": 14}, "price_change_1d": {}, "price_change_5d": {}, "bb_upper": {"period": 20, "std_dev": 2.0}, "bb_lower": {"period": 20, "std_dev": 2.0}}',
    ARRAY['strong_buy', 'buy', 'hold', 'sell', 'strong_sell']
);

-- Sample technical strategy using on-demand indicators
INSERT INTO strategies (name, description, strategy_type, config) VALUES
(
    'rsi_mean_reversion',
    'Buy when RSI < 30, sell when RSI > 70', 
    'technical',
    '{"rsi_buy_threshold": 30, "rsi_sell_threshold": 70, "rsi_period": 14, "position_size": 0.05}'
);