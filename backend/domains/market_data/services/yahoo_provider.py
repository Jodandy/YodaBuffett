#!/usr/bin/env python3
"""
Yahoo Finance provider for Nordic market data.
Provider-agnostic implementation following hexagonal architecture.
"""

import yfinance as yf
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict
from dataclasses import dataclass
import asyncio
import numpy as np


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
class PerformanceMetrics:
    symbol: str
    start_date: date
    end_date: date
    total_return: float
    volatility: float
    max_drawdown: float
    sharpe_ratio: float
    trading_days: int


class YahooFinanceProvider:
    """Yahoo Finance provider for Nordic markets"""
    
    def __init__(self):
        self.provider_name = "yahoo_finance"
        self.symbol_mappings = self._load_nordic_mappings()
    
    async def get_historical_prices(
        self, 
        company_name: str, 
        start_date: date, 
        end_date: date
    ) -> List[PriceData]:
        """Get historical price data"""
        
        symbol = self._get_yahoo_symbol(company_name)
        if not symbol:
            print(f"❌ No symbol mapping found for {company_name}")
            return []
        
        try:
            print(f"📊 Fetching {symbol} from {start_date} to {end_date}")
            
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=start_date, end=end_date)
            
            if hist.empty:
                print(f"❌ No data returned for {symbol}")
                return []
            
            prices = []
            for date_idx, row in hist.iterrows():
                prices.append(PriceData(
                    symbol=company_name,  # Use company name for consistency
                    date=date_idx.date(),
                    open=float(row['Open']),
                    high=float(row['High']), 
                    low=float(row['Low']),
                    close=float(row['Close']),
                    volume=int(row['Volume']) if not np.isnan(row['Volume']) else None,
                    adjusted_close=float(row['Close'])  # Yahoo auto-adjusts
                ))
            
            print(f"✅ Retrieved {len(prices)} price points for {company_name}")
            return prices
            
        except Exception as e:
            print(f"❌ Error fetching data for {company_name} ({symbol}): {e}")
            return []
    
    async def calculate_performance(
        self, 
        company_name: str, 
        start_date: date, 
        end_date: date
    ) -> Optional[PerformanceMetrics]:
        """Calculate performance metrics for a period"""
        
        prices = await self.get_historical_prices(company_name, start_date, end_date)
        
        if len(prices) < 2:
            return None
        
        # Sort by date to ensure correct order
        prices.sort(key=lambda p: p.date)
        
        start_price = prices[0].close
        end_price = prices[-1].close
        
        # Total return
        total_return = (end_price - start_price) / start_price
        
        # Calculate daily returns for volatility
        daily_returns = []
        for i in range(1, len(prices)):
            daily_return = (prices[i].close - prices[i-1].close) / prices[i-1].close
            daily_returns.append(daily_return)
        
        # Annualized volatility
        volatility = np.std(daily_returns) * np.sqrt(252) if daily_returns else 0.0
        
        # Max drawdown
        max_drawdown = self._calculate_max_drawdown([p.close for p in prices])
        
        # Sharpe ratio (assuming risk-free rate of 2%)
        risk_free_rate = 0.02
        excess_return = total_return * 252 / len(prices) - risk_free_rate  # Annualized
        sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0
        
        return PerformanceMetrics(
            symbol=company_name,
            start_date=start_date,
            end_date=end_date,
            total_return=total_return,
            volatility=volatility,
            max_drawdown=max_drawdown,
            sharpe_ratio=sharpe_ratio,
            trading_days=len(prices)
        )
    
    def _get_yahoo_symbol(self, company_name: str) -> Optional[str]:
        """Get Yahoo symbol for company name"""
        
        # Direct mapping first
        if company_name in self.symbol_mappings:
            return self.symbol_mappings[company_name]
        
        # Try fuzzy matching
        for mapped_name, symbol in self.symbol_mappings.items():
            if self._fuzzy_match(company_name, mapped_name):
                return symbol
        
        return None
    
    def _fuzzy_match(self, input_name: str, mapped_name: str, threshold: float = 0.8) -> bool:
        """Simple fuzzy matching"""
        input_clean = input_name.lower().replace('_', ' ')
        mapped_clean = mapped_name.lower().replace('_', ' ')
        
        # Simple contains check for now
        return mapped_clean in input_clean or input_clean in mapped_clean
    
    def _calculate_max_drawdown(self, prices: List[float]) -> float:
        """Calculate maximum drawdown"""
        if not prices:
            return 0.0
        
        peak = prices[0]
        max_dd = 0.0
        
        for price in prices:
            if price > peak:
                peak = price
            
            drawdown = (peak - price) / peak
            if drawdown > max_dd:
                max_dd = drawdown
        
        return max_dd
    
    def _load_nordic_mappings(self) -> Dict[str, str]:
        """Load company name to Yahoo symbol mappings"""
        return {
            # Major Swedish companies in our dataset
            'Volvo Group': 'VOLV-B.ST',
            'Volvo_Group': 'VOLV-B.ST',
            'Atlas Copco AB': 'ATCO-A.ST',
            'Atlas_Copco_AB': 'ATCO-A.ST',
            'AAK': 'AAK.ST',
            'AstraZeneca': 'AZN.ST',
            'Ericsson': 'ERIC-B.ST',
            'Telefonaktiebolaget_LM_Ericsson': 'ERIC-B.ST',
            'ABB Ltd': 'ABB.ST',
            'ABB_Ltd': 'ABB.ST',
            'HM Hennes & Mauritz AB': 'HM-B.ST',
            'HM_Hennes__Mauritz_AB': 'HM-B.ST',
            'Electrolux AB': 'ELUX-B.ST',
            'Electrolux_AB': 'ELUX-B.ST',
            'Sandvik AB': 'SAND.ST',
            'Sandvik_AB': 'SAND.ST',
            'Svenska Cellulosa': 'SCA-B.ST',
            'Tele2 AB': 'TEL2-B.ST',
            'Tele2_AB': 'TEL2-B.ST',
            'Swedbank': 'SWED-A.ST',
            'ICA Gruppen AB': 'ICA.ST',
            'ICA_Gruppen_AB': 'ICA.ST',
            'Investor AB': 'INVE-B.ST',
            'Investor_AB': 'INVE-B.ST',
            'SSAB AB': 'SSAB-A.ST',
            'SSAB_AB': 'SSAB-A.ST',
            'Hexagon AB': 'HEXA-B.ST',
            'Hexagon_AB': 'HEXA-B.ST',
            'Evolution AB': 'EVO.ST',
            'Evolution_AB': 'EVO.ST',
            'Getinge': 'GETI-B.ST',
            'Kinnevik AB': 'KINV-B.ST',
            'Kinnevik_AB': 'KINV-B.ST',
            
            # Add more as needed
            'Addtech': 'ADDT-B.ST',
            'Attendo': 'ATT.ST',
            'Avanza Bank': 'AZA.ST',
            'Avanza_Bank': 'AZA.ST',
            'BioArctic': 'BIOA-B.ST',
            'BioArctic': 'BIOA-B.ST',
            'Castellum': 'CAST.ST',
            'Epiroc AB': 'EPI-A.ST',
            'Epiroc_AB': 'EPI-A.ST',
        }


# Test function
async def test_yahoo_provider():
    """Test the Yahoo Finance provider"""
    provider = YahooFinanceProvider()
    
    # Test companies
    test_companies = ['AAK', 'Volvo_Group', 'Atlas_Copco_AB']
    
    # Test period: 6 months ago to now
    end_date = date.today()
    start_date = end_date - timedelta(days=180)
    
    print("🧪 Testing Yahoo Finance Provider")
    print("=" * 50)
    
    for company in test_companies:
        print(f"\n📊 Testing {company}")
        
        # Get price data
        prices = await provider.get_historical_prices(company, start_date, end_date)
        
        if prices:
            print(f"   ✅ Retrieved {len(prices)} price points")
            print(f"   📈 Price range: {min(p.close for p in prices):.2f} - {max(p.close for p in prices):.2f}")
            
            # Calculate performance
            performance = await provider.calculate_performance(company, start_date, end_date)
            if performance:
                print(f"   📊 6M Return: {performance.total_return:.1%}")
                print(f"   📊 Volatility: {performance.volatility:.1%}")
                print(f"   📊 Max Drawdown: {performance.max_drawdown:.1%}")
                print(f"   📊 Sharpe Ratio: {performance.sharpe_ratio:.2f}")
        else:
            print(f"   ❌ No data retrieved")
    
    print(f"\n✅ Yahoo Finance Provider Test Complete")


if __name__ == "__main__":
    asyncio.run(test_yahoo_provider())