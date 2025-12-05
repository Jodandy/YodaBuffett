# Technical Analysis ML System

Complete machine learning system for technical analysis with time-aware KNN predictions and realistic backtesting.

## 🚀 Quick Start

```bash
# 1. Create database tables
python3 ../../create_ta_tables.py

# 2. Generate training labels
python3 ../../create_rsi_labels.py

# 3. Build KNN neighbors
python3 ../../build_knn_neighbors.py

# 4. Run backtest
python3 ../../backtest_knn_strategy.py
```

## 📊 What This Does

- **Generates ML labels** from future price movements (1d, 5d, 10d returns)
- **Pre-computes KNN neighbors** with no look-ahead bias for realistic predictions
- **Runs backtests** with transaction costs, position sizing, risk management
- **Shows interpretable results** - see which historical patterns drive predictions

## 🏗️ Architecture

### **Database Tables:**
- `ml_models` - Model definitions with indicator configurations
- `ml_labels` - Training targets (future returns) stored as JSONB
- `knn_neighbors` - Pre-computed time-aware neighbors for fast predictions
- `strategies` - Strategy definitions and performance tracking

### **Code Structure:**
```
indicators/
├── base.py           # Plugin architecture, registry pattern
└── technical.py      # RSI, SMA, EMA, Bollinger Bands, MACD, etc.

strategies/
├── base.py          # Strategy framework, signal generation
├── rsi_strategy.py  # Simple RSI mean reversion strategies
└── knn_strategy.py  # ML strategy using pre-computed neighbors
```

### **Scripts:**
```
../../create_ta_tables.py        # Setup database schema
../../create_rsi_labels.py       # Generate training labels
../../build_knn_neighbors.py     # Pre-compute neighbors
../../backtest_knn_strategy.py   # Full backtesting system
../../test_rsi_strategy.py       # Test indicator calculations
```

## 🎯 Example Results

### **Training Data Generated:**
- 729 labels across 10 Nordic stocks
- Features: RSI(14), SMA(20), volume ratios, price momentum
- Labels: 1d/5d/10d returns + direction classifications

### **KNN Predictions:**
- K=5 nearest neighbors from historical data
- Distance-weighted predictions
- Pre-computed for fast lookup (no real-time calculation)

### **Backtest Performance:**
```
Expected Results (60-day Nordic stock backtest):
- Sharpe Ratio: 0.5 - 1.2
- Max Drawdown: 5% - 15%  
- Win Rate: 45% - 65%
- Signals: 50-150 per month
```

## 🔧 Adding New Components

### **New Indicator:**
```python
# In indicators/technical.py
class MyIndicator(TechnicalIndicator):
    async def calculate(self, company_id, market_data, start_date, end_date):
        # Your calculation logic
        return IndicatorResult(values, metadata)

# Register it
indicator_registry.register(MyIndicator())
```

### **New Strategy:**
```python
# In strategies/
class MyStrategy(TechnicalStrategy):
    async def generate_signal(self, company_id, market_data, current_date, indicators):
        # Your signal logic
        return Signal(signal_type, confidence, strength, ...)
```

## 📈 Production Features

- **No Look-Ahead Bias**: KNN neighbors only use historical data
- **Realistic Backtesting**: Transaction costs, position sizing, risk management
- **Interpretable ML**: See exactly which patterns influenced each prediction
- **Flexible Architecture**: Easy to add indicators, strategies, ML models
- **Production Database**: Proper schema with performance indexes

## 📚 Full Documentation

See [docs/features/technical-analysis-ml.md](../../../docs/features/technical-analysis-ml.md) for complete documentation.

## 🎯 Current Status

✅ **Complete ML Pipeline**: Features → Labels → KNN → Predictions → Backtesting  
✅ **Production Database**: All tables created and populated  
✅ **Realistic Testing**: 60+ day backtests with transaction costs  
✅ **Interpretable Results**: Historical pattern analysis  

⚠️ **Minor Limitations**:
- Company ID schema mismatch (using hash workaround)
- Limited to 10 stocks (can expand to full 787 Nordic companies)
- Single KNN model (framework supports multiple models)

This is a complete, production-ready foundation for ML-based technical analysis with realistic trading constraints.