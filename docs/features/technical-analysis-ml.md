# Technical Analysis ML System

## Overview

Complete machine learning system for technical analysis with time-aware KNN predictions, realistic backtesting, and production-ready architecture.

## 🏗️ Architecture Components

### **1. Database Schema**
- **`ml_models`**: Model definitions with flexible indicator configurations
- **`ml_labels`**: Future outcomes (price returns, directions) stored as JSONB
- **`knn_neighbors`**: Pre-computed time-aware neighbors (no look-ahead bias)
- **`strategies`**: Strategy definitions and performance tracking
- **`backtest_runs`** & **`backtest_trades`**: Backtesting results

### **2. Plugin-Based Indicator System**
- **Base Classes**: `TechnicalIndicator`, `FundamentalIndicator`, `MLDerivedIndicator`
- **Registry Pattern**: Dynamic indicator discovery and calculation
- **On-Demand Calculation**: Features computed from market data when needed
- **Flexible Parameters**: Each indicator stores its own configuration

### **3. ML Strategy Framework**
- **Base Strategy Classes**: `TechnicalStrategy`, `MLStrategy`, `AnomalyDetectionStrategy`
- **Signal Generation**: Standardized signal format with confidence/strength scoring
- **Time-Aware Predictions**: Only uses historical data available at prediction time
- **Ensemble Support**: Combine multiple strategies with weighted voting

### **4. KNN Implementation**
- **Pre-computed Neighbors**: Historical similar patterns stored for fast lookup
- **Distance Weighting**: Closer patterns have more influence on predictions
- **Feature Engineering**: RSI, price ratios, volume ratios, momentum indicators
- **Label Mapping**: Future returns (1d, 5d, 10d) and direction classifications

## 🚀 Current Implementation

### **Production-Ready Components:**

#### **✅ Indicators (services/technical_analysis/indicators/)**
```python
# Available indicators
RSI(period=14)           # Relative Strength Index
SMA(period=20)           # Simple Moving Average  
EMA(period=20)           # Exponential Moving Average
BollingerBands(period=20, std_dev=2.0)  # Upper/Lower bands
VolumeMA(period=20)      # Volume moving average
PriceChange(period=1)    # Price change percentage
MACD(fast=12, slow=26, signal=9)  # MACD indicator
```

#### **✅ KNN Strategy (services/technical_analysis/strategies/knn_strategy.py)**
```python
# Uses pre-computed neighbors for predictions
KNNStrategy(
    prediction_horizon="5d",     # Predict 5-day returns
    buy_threshold=0.02,          # Buy if predicted return > 2%
    sell_threshold=-0.02,        # Sell if predicted return < -2%
    min_confidence=0.6           # Minimum confidence for signals
)
```

#### **✅ Backtesting Engine (backtest_knn_strategy.py)**
```python
KNNBacktester(
    initial_capital=100000,
    transaction_cost=0.001,      # 0.1% transaction cost
    max_position_size=0.1,       # Max 10% per position
    rebalance_frequency=5        # Rebalance every 5 days
)
```

### **Database Tables Created:**
- ✅ `ml_models` - 2 sample KNN models
- ✅ `ml_labels` - 729 labels across 10 Nordic companies
- ✅ `knn_neighbors` - Pre-computed neighbors for fast predictions
- ✅ `strategies` - Strategy definitions and performance tracking

### **Scripts Available:**
- ✅ `create_ta_tables.py` - Create all technical analysis tables
- ✅ `create_rsi_labels.py` - Generate ML labels from market data
- ✅ `build_knn_neighbors.py` - Pre-compute time-aware neighbors
- ✅ `backtest_knn_strategy.py` - Full backtesting with realistic constraints
- ✅ `multi_horizon_indicator_tester.py` - Multi-timeframe Fibonacci analysis
- ✅ `isolated_indicator_tester.py` - Individual indicator testing (100+ companies)
- ✅ `isolated_indicator_adaptive_exit.py` - Adaptive exit strategy with KNN
- ✅ `realistic_portfolio_simulator.py` - Complete portfolio management system

## 📊 Example Results

### **KNN Strategy Performance:**
```
Period: 60-day backtest on Nordic stocks
Initial Capital: $100,000
Strategy: RSI-based KNN with 5d prediction horizon

Expected Metrics:
- Sharpe Ratio: 0.5 - 1.2
- Max Drawdown: 5% - 15%
- Win Rate: 45% - 65%
- Signals Generated: 50-150 per month
```

### **Sample Predictions:**
```
Date: 2024-11-15, Symbol: ERIC-B
Prediction: +1.8% (5-day return)
Confidence: 0.73
K=5 Neighbors: 2024-09-12, 2024-08-03, 2024-07-21, 2024-06-15, 2024-05-28
Action: BUY (predicted return > 1.5% threshold)
```

## 🔧 Technical Details

### **Feature Engineering:**
```python
features = {
    'rsi_14': 73.2,                    # RSI(14) value
    'price_to_sma': 1.05,              # Current price / SMA(20)
    'volume_ratio': 1.23,              # Current volume / Volume MA(20)
    'price_change_5d': 0.0234          # 5-day price change %
}
```

### **Label Structure:**
```json
{
  "1d_return": 0.023,       // 1-day ahead return
  "5d_return": -0.018,      // 5-day ahead return  
  "10d_return": 0.045,      // 10-day ahead return
  "1d_direction": "up",     // Direction classification
  "5d_direction": "down",   // Direction classification
  "10d_direction": "up"     // Direction classification
}
```

### **KNN Neighbors Format:**
```json
{
  "neighbors": [
    {
      "date": "2024-09-12",
      "distance": 0.05,
      "features": {"rsi_14": 72.1, "price_to_sma": 1.04, ...},
      "label": {"5d_return": 0.028, "5d_direction": "up"}
    }
  ],
  "feature_vector": [73.2, 1.05, 1.23, 0.0234],
  "num_neighbors_available": 45
}
```

## 🎯 Key Advantages

### **No Look-Ahead Bias:**
- KNN neighbors only use historical data available at prediction time
- Realistic backtesting that matches real-world trading constraints

### **Interpretable ML:**
- See exactly which historical patterns influenced each prediction
- Understand why the model made specific trading decisions

### **Flexible Architecture:**
- Easy to add new indicators, features, or prediction horizons
- Plugin-based system supports experimentation

### **Production-Ready:**
- Handles transaction costs, position sizing, risk management
- Scalable to hundreds of stocks and multiple strategies

## 🛠️ Usage Instructions

### **1. Setup Database Tables:**
```bash
python3 create_ta_tables.py
```

### **2. Generate Training Labels:**
```bash
python3 create_rsi_labels.py
# Creates 729 labels for 10 Nordic stocks
```

### **3. Build KNN Neighbors:**
```bash
python3 build_knn_neighbors.py  
# Pre-computes time-aware neighbors
```

### **4. Run Backtests & Analysis:**
```bash
# Traditional backtest
python3 backtest_knn_strategy.py
# Full strategy backtest with performance metrics

# Advanced analysis scripts
python3 multi_horizon_indicator_tester.py
# Multi-timeframe analysis using Fibonacci sequences

python3 isolated_indicator_tester.py  
# Test individual indicators across 100+ companies

python3 isolated_indicator_adaptive_exit.py
# Adaptive exit timing with KNN-based decisions

python3 realistic_portfolio_simulator.py
# Complete portfolio simulation with position sizing
```

### **5. Add New Indicators:**
```python
# In services/technical_analysis/indicators/technical.py
class NewIndicator(TechnicalIndicator):
    async def calculate(self, company_id, market_data, start_date, end_date):
        # Your indicator logic here
        return IndicatorResult(values, metadata)

# Register it
indicator_registry.register(NewIndicator())
```

## 🚧 Current Limitations

### **Schema Mismatch:**
- `company_master` uses UUID, `ml_labels` expects INTEGER
- Currently using hash-based workaround for company IDs

### **Limited Market Data:**
- Currently tested on 10 Nordic stocks
- Needs expansion to full 787 company dataset

### **Single Strategy:**
- Only RSI-based KNN implemented
- Framework supports multiple strategies but needs more examples

## 🎯 Next Steps

### **Immediate:**
1. Fix company_id schema mismatch
2. Expand to full Nordic market (787 companies)
3. Add more technical indicators (Stochastic, Williams %R, etc.)

### **Medium-term:**
1. Implement ensemble strategies combining multiple models
2. Add fundamental analysis indicators from document data
3. Create web dashboard for strategy monitoring

### **Advanced:**
1. Real-time signal generation system
2. Paper trading integration
3. Multi-asset correlation analysis
4. Alternative ML models (Random Forest, Neural Networks)

## 📁 File Structure

```
backend/services/technical_analysis/
├── db/
│   └── schema.sql                 # Database schema
├── indicators/
│   ├── base.py                   # Base indicator classes
│   └── technical.py              # Technical indicators
├── strategies/
│   ├── base.py                   # Strategy framework
│   ├── rsi_strategy.py           # Simple RSI strategies
│   └── knn_strategy.py           # KNN ML strategy
└── 

backend/
├── create_ta_tables.py                    # Setup script
├── create_rsi_labels.py                   # Label generation
├── build_knn_neighbors.py                 # KNN pre-computation
├── backtest_knn_strategy.py               # Traditional backtesting
├── multi_horizon_indicator_tester.py      # Multi-timeframe analysis
├── isolated_indicator_tester.py           # Individual indicator testing
├── isolated_indicator_adaptive_exit.py    # Adaptive exit strategy
├── realistic_portfolio_simulator.py       # Portfolio management system
└── ema10_knn_strategy.pine                # TradingView Pine Script version
```

This system represents a complete, production-ready foundation for machine learning-based technical analysis with realistic trading constraints and interpretable results.