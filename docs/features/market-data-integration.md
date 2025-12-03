# Market Data Integration - Provider Agnostic Design

## Overview

Provider-agnostic market data system to support performance validation and predictive classification. Starts with Yahoo Finance API but designed for easy provider switching (Börsdata, Alpha Vantage, etc.).

## Architecture Design

### 1. Provider Interface (Hexagonal Architecture)

```python
# Domain port (interface)
from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import date
from dataclasses import dataclass

@dataclass
class PriceData:
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None
    adjusted_close: Optional[float] = None

@dataclass
class CompanyInfo:
    symbol: str
    name: str
    market: str
    sector: Optional[str] = None
    market_cap: Optional[float] = None

class MarketDataProvider(ABC):
    """Abstract interface for market data providers"""
    
    @abstractmethod
    async def get_historical_prices(
        self, 
        symbol: str, 
        start_date: date, 
        end_date: date
    ) -> List[PriceData]:
        pass
    
    @abstractmethod
    async def get_company_info(self, symbol: str) -> Optional[CompanyInfo]:
        pass
    
    @abstractmethod
    async def search_symbol(self, company_name: str) -> List[str]:
        pass
    
    @abstractmethod
    def get_provider_name(self) -> str:
        pass
```

### 2. Yahoo Finance Implementation

```python
# Infrastructure adapter
import yfinance as yf
from datetime import datetime, date
import asyncio

class YahooFinanceProvider(MarketDataProvider):
    """Yahoo Finance API implementation"""
    
    def __init__(self):
        self.provider_name = "yahoo_finance"
    
    async def get_historical_prices(
        self, 
        symbol: str, 
        start_date: date, 
        end_date: date
    ) -> List[PriceData]:
        """Get historical price data from Yahoo Finance"""
        
        # Convert Nordic symbols to Yahoo format
        yahoo_symbol = self._to_yahoo_symbol(symbol)
        
        try:
            ticker = yf.Ticker(yahoo_symbol)
            hist = ticker.history(start=start_date, end=end_date)
            
            prices = []
            for date_idx, row in hist.iterrows():
                prices.append(PriceData(
                    symbol=symbol,
                    date=date_idx.date(),
                    open=row['Open'],
                    high=row['High'], 
                    low=row['Low'],
                    close=row['Close'],
                    volume=int(row['Volume']),
                    adjusted_close=row['Close']  # Yahoo auto-adjusts
                ))
            
            return prices
            
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return []
    
    async def get_company_info(self, symbol: str) -> Optional[CompanyInfo]:
        """Get company metadata"""
        yahoo_symbol = self._to_yahoo_symbol(symbol)
        
        try:
            ticker = yf.Ticker(yahoo_symbol)
            info = ticker.info
            
            return CompanyInfo(
                symbol=symbol,
                name=info.get('longName', ''),
                market=self._extract_market(yahoo_symbol),
                sector=info.get('sector'),
                market_cap=info.get('marketCap')
            )
        except:
            return None
    
    async def search_symbol(self, company_name: str) -> List[str]:
        """Search for ticker symbols by company name"""
        # Yahoo doesn't have a great search API
        # For Nordic companies, we'll need symbol mapping
        return await self._nordic_symbol_lookup(company_name)
    
    def get_provider_name(self) -> str:
        return self.provider_name
    
    def _to_yahoo_symbol(self, symbol: str) -> str:
        """Convert Nordic symbol to Yahoo format"""
        # Stockholm: Add .ST suffix
        # Oslo: Add .OL suffix  
        # Copenhagen: Add .CO suffix
        # Helsinki: Add .HE suffix
        
        symbol_mappings = {
            # Common Swedish symbols
            'VOLV-B': 'VOLV-B.ST',
            'ERICB': 'ERIC-B.ST', 
            'ABB': 'ABB.ST',
            'ATCO-A': 'ATCO-A.ST',
            'SEB-A': 'SEB-A.ST',
            'HM-B': 'HM-B.ST'
        }
        
        if symbol in symbol_mappings:
            return symbol_mappings[symbol]
        
        # Default: assume Swedish (.ST)
        return f"{symbol}.ST"
    
    def _extract_market(self, yahoo_symbol: str) -> str:
        """Extract market from Yahoo symbol"""
        if yahoo_symbol.endswith('.ST'):
            return 'Stockholm'
        elif yahoo_symbol.endswith('.OL'):
            return 'Oslo'
        elif yahoo_symbol.endswith('.CO'):
            return 'Copenhagen'
        elif yahoo_symbol.endswith('.HE'):
            return 'Helsinki'
        else:
            return 'Unknown'
    
    async def _nordic_symbol_lookup(self, company_name: str) -> List[str]:
        """Basic Nordic company name to symbol mapping"""
        # This would be enhanced with a proper symbol database
        name_to_symbol = {
            'Volvo': ['VOLV-B', 'VOLV-A'],
            'Ericsson': ['ERICB', 'ERICA'], 
            'ABB': ['ABB'],
            'Atlas Copco': ['ATCO-A', 'ATCO-B'],
            'H&M': ['HM-B'],
            'AAK': ['AAK'],
            'AstraZeneca': ['AZN']
        }
        
        # Fuzzy matching
        for name, symbols in name_to_symbol.items():
            if name.lower() in company_name.lower():
                return symbols
        
        return []
```

### 3. Market Data Service (Domain Layer)

```python
class MarketDataService:
    """Domain service for market data operations"""
    
    def __init__(self, provider: MarketDataProvider):
        self.provider = provider
    
    async def calculate_performance(
        self, 
        symbol: str, 
        start_date: date, 
        end_date: date
    ) -> Dict[str, float]:
        """Calculate performance metrics"""
        
        prices = await self.provider.get_historical_prices(
            symbol, start_date, end_date
        )
        
        if len(prices) < 2:
            return {}
        
        start_price = prices[0].close
        end_price = prices[-1].close
        
        # Calculate metrics
        total_return = (end_price - start_price) / start_price
        
        # Calculate volatility
        returns = []
        for i in range(1, len(prices)):
            daily_return = (prices[i].close - prices[i-1].close) / prices[i-1].close
            returns.append(daily_return)
        
        volatility = np.std(returns) * np.sqrt(252)  # Annualized
        
        # Max drawdown
        max_drawdown = self._calculate_max_drawdown([p.close for p in prices])
        
        return {
            'total_return': total_return,
            'volatility': volatility,
            'max_drawdown': max_drawdown,
            'start_price': start_price,
            'end_price': end_price,
            'trading_days': len(prices)
        }
    
    def _calculate_max_drawdown(self, prices: List[float]) -> float:
        """Calculate maximum drawdown"""
        peak = prices[0]
        max_dd = 0.0
        
        for price in prices:
            if price > peak:
                peak = price
            
            drawdown = (peak - price) / peak
            if drawdown > max_dd:
                max_dd = drawdown
        
        return max_dd
```

### 4. Company Symbol Mapping

```python
# Need a mapping from our document company names to ticker symbols
class CompanySymbolMapper:
    """Maps document company names to ticker symbols"""
    
    def __init__(self):
        self.mappings = self._load_symbol_mappings()
    
    def get_symbol(self, company_name: str) -> Optional[str]:
        """Get ticker symbol for company name"""
        
        # Direct mapping first
        if company_name in self.mappings:
            return self.mappings[company_name]
        
        # Fuzzy matching
        return self._fuzzy_match(company_name)
    
    def _load_symbol_mappings(self) -> Dict[str, str]:
        """Load company name to symbol mappings"""
        return {
            # Our document names → Yahoo symbols
            'Volvo Group': 'VOLV-B',
            'Atlas Copco AB': 'ATCO-A', 
            'AAK': 'AAK',
            'AstraZeneca': 'AZN',
            'Ericsson': 'ERICB',
            'ABB Ltd': 'ABB',
            'H&M': 'HM-B',
            'Electrolux AB': 'ELUX-B',
            'Sandvik AB': 'SAND',
            'SKF': 'SKF-B',
            'Svenska Cellulosa': 'SCA-B',
            'Tele2 AB': 'TEL2-B',
            'TeliaSonera': 'TELIA',
            # ... add more as needed
        }
    
    def _fuzzy_match(self, company_name: str) -> Optional[str]:
        """Fuzzy matching for unmapped companies"""
        # Implementation using fuzzywuzzy or similar
        pass
```

### 5. Database Schema

```sql
-- Market data tables
CREATE TABLE market_data_symbols (
    symbol VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(200) NOT NULL,
    market VARCHAR(50) NOT NULL,
    sector VARCHAR(100),
    market_cap BIGINT,
    provider VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE price_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol VARCHAR(20) NOT NULL REFERENCES market_data_symbols(symbol),
    date DATE NOT NULL,
    open_price DECIMAL(12, 4) NOT NULL,
    high_price DECIMAL(12, 4) NOT NULL,
    low_price DECIMAL(12, 4) NOT NULL,
    close_price DECIMAL(12, 4) NOT NULL,
    volume BIGINT,
    adjusted_close DECIMAL(12, 4),
    provider VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(symbol, date, provider)
);

CREATE INDEX idx_price_data_symbol_date ON price_data(symbol, date);
CREATE INDEX idx_price_data_date ON price_data(date);

-- Performance calculations
CREATE TABLE company_performance_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_name VARCHAR(200) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    measurement_start_date DATE NOT NULL,
    measurement_end_date DATE NOT NULL,
    period_days INTEGER NOT NULL,
    
    total_return DECIMAL(10, 6) NOT NULL,
    annualized_return DECIMAL(10, 6),
    volatility DECIMAL(10, 6),
    max_drawdown DECIMAL(10, 6),
    sharpe_ratio DECIMAL(10, 6),
    
    start_price DECIMAL(12, 4) NOT NULL,
    end_price DECIMAL(12, 4) NOT NULL,
    
    provider VARCHAR(50) NOT NULL,
    calculated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(company_name, measurement_start_date, measurement_end_date, provider)
);
```

## Implementation Steps

### Phase 1: Basic Yahoo Integration
1. **Install dependencies**: `pip install yfinance`
2. **Build Yahoo provider** with Nordic symbol mapping
3. **Test with 5-10 companies** from our document database
4. **Validate data quality** vs manual checks

### Phase 2: Performance Calculation  
1. **Build performance calculation service**
2. **Create company-symbol mapping** for our document companies
3. **Calculate historical performance** for document anomaly dates
4. **Manual validation** of top anomalies vs price moves

### Phase 3: Integration with Embeddings
1. **Link market data to document database**
2. **Generate performance labels** for predictive classification
3. **Validate embedding anomalies** vs actual price movements
4. **Build systematic screening** for current anomaly patterns

### Phase 4: Provider Expansion
1. **Easy to add Börsdata provider** when commercial restrictions change
2. **Add other providers** (Alpha Vantage, etc.) for redundancy
3. **Provider comparison** and quality scoring

This architecture lets us start with Yahoo Finance immediately while keeping the door open for better data sources later!