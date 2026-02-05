# YodaBuffett Screener Pro 🎯

A professional-grade stock screener with advanced backtesting capabilities, built on the YodaBuffett financial intelligence platform.

## 🎯 Features

✅ **Complex Query Building**
- Visual query builder with AND/OR logic
- Relative metric comparisons (P/E vs Industry P/E)
- 31 comprehensive metrics (fundamental, technical, derived)
- Support for complex conditions and grouping

✅ **Point-in-Time Screening**
- Historical screening without look-ahead bias
- Backtest strategies on 4+ years of data
- Forward return calculation (1W/1M/3M/6M/1Y/2Y)
- 1,585+ companies ready for screening

✅ **Advanced Analytics**
- Performance metrics (Sharpe ratio, win rate, max drawdown)
- Interactive charts and visualizations
- Strategy comparison and optimization
- Real-time calculation from 1.3M+ fundamental records

✅ **Professional Interface**
- Modern React + TypeScript frontend
- Responsive design with Tailwind CSS
- Interactive API documentation
- Export to CSV/Excel/JSON

## 📋 Prerequisites

Before starting, ensure you have:
- **Python 3.8+** with virtual environment
- **Node.js 18+** and npm
- **PostgreSQL** with YodaBuffett database running
- **Git** for version control

## 🚀 Complete Setup & Running Instructions

### Step 1: Prepare Your Environment

```bash
# Navigate to the YodaBuffett directory
cd /Users/jdandemar/Documents/YodaBuffett

# Ensure you have the main virtual environment
cd backend
python -m venv venv  # Create if doesn't exist
source venv/bin/activate  # Activate it
```

### Step 2: Backend Setup & Running

```bash
# Navigate to screener backend (keep venv activated)
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/backend

# Verify environment file exists
cat .env
# Should contain:
# DATABASE_URL=postgresql://yodabuffett:password@localhost:5432/yodabuffett
# ENVIRONMENT=development
# LOG_LEVEL=INFO
# DEBUG=True

# Install dependencies (if not already done)
pip install -r requirements.txt

# Test database connection first
python simple_db_test.py
# Should show: ✅ Database connection successful!

# Start the backend server
python start_simple.py
# Or use uvicorn directly:
# uvicorn app.main:app --host 0.0.0.0 --port 8000 --log-level info
```

**Backend will be available at:**
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Health Check: http://localhost:8000/health/detailed

### Step 3: Frontend Setup & Running

**Open a new terminal** (keep backend running):

```bash
# Navigate to frontend directory
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/frontend

# Fix npm permissions if needed
sudo chown -R $(whoami) ~/.npm

# Clear npm cache if having issues
npm cache clean --force

# Install dependencies
npm install

# Start the development server
npm run dev
```

**Frontend will be available at:**
- Web Interface: http://localhost:3000

### Step 4: Verify Everything is Working

1. **Check Backend Health:**
   ```bash
   curl http://localhost:8000/health/detailed
   ```
   Should show database connectivity and data availability.

2. **Check Available Metrics:**
   ```bash
   curl http://localhost:8000/api/v1/metrics/available
   ```
   Should list 31 available metrics.

3. **Open Frontend:**
   - Navigate to http://localhost:3000
   - You should see the YodaBuffett Screener interface

## 🛠️ Operating Instructions

### Starting Order (Important!)

1. **PostgreSQL Database** - Must be running first
2. **Backend API** - Start with venv activated
3. **Frontend** - Start in separate terminal

### Stopping the Services

1. **Frontend**: Press `Ctrl+C` in frontend terminal
2. **Backend**: Press `Ctrl+C` in backend terminal
3. **Deactivate venv**: Type `deactivate`

### Common Commands Reference

```bash
# Backend Commands
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/backend
source /Users/jdandemar/Documents/YodaBuffett/backend/venv/bin/activate
python start_simple.py  # Start backend

# Frontend Commands  
cd /Users/jdandemar/Documents/YodaBuffett/products/screener/frontend
npm run dev            # Start frontend
npm run build          # Build for production

# Data Quality Check
python run_data_population.py  # Check data availability

# Database Test
python simple_db_test.py  # Test DB connection
```

## 🏗️ Architecture

### Backend (FastAPI + PostgreSQL)
- **Database**: 5 tables with screener-specific features
- **Services**: Screening, backtesting, metrics, query building
- **API**: 20+ REST endpoints with comprehensive validation
- **Data**: Leverages 1,369,413 fundamental records from YodaBuffett platform

### Frontend (React + TypeScript)
- **Pages**: Screener, Backtest, Saved Queries
- **Components**: QueryBuilder, ResultsDisplay, BacktestResults
- **State**: Zustand + React Query for data management
- **UI**: Tailwind CSS + Heroicons

## 💡 Usage Examples

### Basic Value Screening
```typescript
const valueQuery: ScreenerQuery = {
  groups: [{
    id: "value_criteria",
    conditions: [
      { leftOperand: "pe_ratio", operator: "<", rightOperand: 15 },
      { leftOperand: "pb_ratio", operator: "<", rightOperand: 2 },
      { leftOperand: "roe", operator: ">", rightOperand: 10 }
    ],
    logicalOperator: "AND"
  }],
  groupLogic: "AND",
  columns: ["pe_ratio", "pb_ratio", "roe", "market_cap"],
  includeForwardReturns: ["1M", "3M", "1Y"]
}
```

### Historical Backtesting
```typescript
const backtestRequest: BacktestRequest = {
  query: valueQuery,
  startDate: "2022-01-01",
  endDate: "2024-12-01", 
  frequency: "monthly",
  forwardPeriods: ["1M", "3M", "1Y"]
}
```

## 🛠️ Development

### Project Structure
```
products/screener/
├── backend/              # FastAPI backend
│   ├── app/
│   │   ├── api/v1/      # API endpoints
│   │   ├── services/     # Business logic
│   │   ├── models/       # Database models
│   │   └── schemas/      # Pydantic schemas
│   └── migrations/       # Database migrations
├── frontend/             # React frontend
│   ├── src/
│   │   ├── components/   # React components
│   │   ├── pages/        # Page components
│   │   ├── services/     # API integration
│   │   └── types/        # TypeScript types
└── docs/                # Documentation
```

### Testing & Deployment

```bash
# Backend tests
cd backend/ && pytest

# Frontend tests  
cd frontend/ && npm test

# Production build
npm run build
```

## 🔌 Integration with YodaBuffett Platform

This screener leverages the comprehensive YodaBuffett financial data infrastructure:

- **1,606 Nordic Companies**: Complete market coverage
- **1,369,413 Fundamental Records**: 4+ years of daily data
- **Point-in-Time Architecture**: No look-ahead bias
- **Document Intelligence**: AI-powered financial analysis
- **Market Data**: Real-time and historical price data

## 📊 Performance

- **Screening**: 2-5 seconds for complex queries
- **Backtesting**: 30-120 seconds for 24-month periods
- **Database**: Optimized indexes for point-in-time queries

## 🎉 Status

✅ **Complete** - Full-stack implementation ready for deployment

**🚀 Ready to screen like a pro? Start with the value stock example above!**