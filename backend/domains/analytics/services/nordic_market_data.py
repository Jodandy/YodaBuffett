#!/usr/bin/env python3
"""
Nordic Market Data Provider for YodaBuffett Backtesting.

Provides historical stock price data for Nordic companies (Sweden, Norway, Denmark, Finland).
Supports both real data sources and simulated data for testing.
"""

import logging
from typing import List, Dict, Optional, Any
from datetime import datetime, date, timedelta
import asyncio
import random
import math

from ..models.backtesting import MarketData, MarketDataProvider

logger = logging.getLogger(__name__)


class MockNordicMarketData(MarketDataProvider):
    """
    Mock market data provider for testing and development.
    
    Generates realistic-looking stock price data with:
    - Realistic volatility patterns
    - Trend periods (bull/bear markets)
    - Sector correlations
    - Corporate events simulation
    """
    
    def __init__(self):
        self.symbol_info = self._create_nordic_symbols()
        self.market_regime = "normal"  # normal, bull, bear
        self.base_date = date(2020, 1, 1)
        
        # Cache for generated data to ensure consistency
        self._price_cache: Dict[str, List[MarketData]] = {}
        
        logger.info(f"🇸🇪 Initialized Nordic market data with {len(self.symbol_info)} symbols")
    
    def _create_nordic_symbols(self) -> Dict[str, Dict[str, Any]]:
        """Create metadata for major Nordic companies"""
        return {
            # Swedish companies (major ones)
            "VOLV-B.ST": {
                "name": "Volvo Group",
                "sector": "Automotive",
                "market_cap": 250_000_000_000,  # 250B SEK
                "currency": "SEK",
                "base_price": 180.0,
                "volatility": 0.25
            },
            "ERIC-B.ST": {
                "name": "Ericsson",
                "sector": "Technology",
                "market_cap": 200_000_000_000,
                "currency": "SEK", 
                "base_price": 60.0,
                "volatility": 0.30
            },
            "HM-B.ST": {
                "name": "H&M",
                "sector": "Consumer Discretionary",
                "market_cap": 150_000_000_000,
                "currency": "SEK",
                "base_price": 120.0,
                "volatility": 0.28
            },
            "SEB-A.ST": {
                "name": "SEB Bank",
                "sector": "Financial Services",
                "market_cap": 180_000_000_000,
                "currency": "SEK",
                "base_price": 140.0,
                "volatility": 0.22
            },
            "SAND.ST": {
                "name": "Sandvik",
                "sector": "Industrials",
                "market_cap": 220_000_000_000,
                "currency": "SEK",
                "base_price": 200.0,
                "volatility": 0.24
            },
            "TEL2-B.ST": {
                "name": "Tele2",
                "sector": "Telecommunications",
                "market_cap": 80_000_000_000,
                "currency": "SEK",
                "base_price": 110.0,
                "volatility": 0.20
            },
            "ASSA-B.ST": {
                "name": "ASSA ABLOY",
                "sector": "Industrials", 
                "market_cap": 300_000_000_000,
                "currency": "SEK",
                "base_price": 250.0,
                "volatility": 0.23
            },
            "SKF-B.ST": {
                "name": "SKF",
                "sector": "Industrials",
                "market_cap": 120_000_000_000,
                "currency": "SEK",
                "base_price": 160.0,
                "volatility": 0.26
            },
            "ALFA.ST": {
                "name": "Alfa Laval",
                "sector": "Industrials",
                "market_cap": 140_000_000_000,
                "currency": "SEK",
                "base_price": 320.0,
                "volatility": 0.24
            },
            "INVE-B.ST": {
                "name": "Investor B",
                "sector": "Financial Services",
                "market_cap": 400_000_000_000,
                "currency": "SEK",
                "base_price": 240.0,
                "volatility": 0.20
            },
            
            # Add some key Norwegian companies
            "EQNR.OL": {
                "name": "Equinor",
                "sector": "Energy",
                "market_cap": 600_000_000_000,  # NOK
                "currency": "NOK",
                "base_price": 280.0,
                "volatility": 0.35
            },
            "DNB.OL": {
                "name": "DNB Bank",
                "sector": "Financial Services",
                "market_cap": 300_000_000_000,
                "currency": "NOK",
                "base_price": 200.0,
                "volatility": 0.25
            },
            
            # Danish companies
            "MAERSK-B.CO": {
                "name": "Maersk",
                "sector": "Transportation",
                "market_cap": 800_000_000_000,  # DKK
                "currency": "DKK", 
                "base_price": 12000.0,
                "volatility": 0.30
            },
            "NOVO-B.CO": {
                "name": "Novo Nordisk",
                "sector": "Healthcare",
                "market_cap": 2_500_000_000_000,  # DKK
                "currency": "DKK",
                "base_price": 800.0,
                "volatility": 0.18
            },
        }
    
    async def get_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[MarketData]:
        """Generate realistic historical market data for a symbol"""
        
        if symbol not in self.symbol_info:
            logger.warning(f"Unknown symbol: {symbol}")
            return []
        
        # Check cache first
        cache_key = f"{symbol}_{start_date}_{end_date}"
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
        
        symbol_data = self.symbol_info[symbol]
        data_points = []
        
        current_date = start_date
        current_price = symbol_data["base_price"]
        volatility = symbol_data["volatility"]
        
        # Add some trend component based on sector and time period
        trend_factor = self._get_trend_factor(symbol_data["sector"], start_date)
        
        while current_date <= end_date:
            # Skip weekends (basic approximation)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                
                # Generate daily return with trend and noise
                daily_trend = trend_factor / 252  # Annualized to daily
                daily_volatility = volatility / math.sqrt(252)  # Daily volatility
                
                # Random walk with drift
                daily_return = daily_trend + random.gauss(0, daily_volatility)
                
                # Add some autocorrelation (momentum)
                if data_points:
                    prev_return = (data_points[-1].close_price - data_points[-1].open_price) / data_points[-1].open_price
                    daily_return += 0.05 * prev_return  # 5% momentum
                
                # Update price
                open_price = current_price
                close_price = current_price * (1 + daily_return)
                
                # Generate intraday high/low
                daily_range = abs(daily_return) * random.uniform(1.2, 2.0)  # Range is usually larger than close-to-close
                high_price = max(open_price, close_price) * (1 + daily_range/2)
                low_price = min(open_price, close_price) * (1 - daily_range/2)
                
                # Generate volume (inversely correlated with price changes)
                base_volume = 1_000_000
                volume_multiplier = 1 + abs(daily_return) * 5  # Higher volume on big moves
                volume = int(base_volume * volume_multiplier * random.uniform(0.5, 2.0))
                
                # Create market data point
                data_point = MarketData(
                    symbol=symbol,
                    timestamp=datetime.combine(current_date, datetime.min.time().replace(hour=16)),  # 4 PM close
                    open_price=round(open_price, 2),
                    high_price=round(high_price, 2),
                    low_price=round(low_price, 2),
                    close_price=round(close_price, 2),
                    volume=volume,
                    adjusted_close=round(close_price, 2),  # No adjustments for simplicity
                    market_cap=symbol_data["market_cap"],
                    sector=symbol_data["sector"],
                    currency=symbol_data["currency"]
                )
                
                data_points.append(data_point)
                current_price = close_price
            
            current_date += timedelta(days=1)
        
        # Cache the generated data
        self._price_cache[cache_key] = data_points
        
        logger.debug(f"Generated {len(data_points)} data points for {symbol}")
        return data_points
    
    def _get_trend_factor(self, sector: str, start_date: date) -> float:
        """Get annualized trend factor based on sector and time period"""
        
        # Sector-based base trends (annualized)
        sector_trends = {
            "Technology": 0.12,      # 12% annual trend for tech
            "Healthcare": 0.08,      # 8% for healthcare
            "Financial Services": 0.06,  # 6% for financials
            "Energy": 0.03,          # 3% for energy (cyclical)
            "Industrials": 0.07,     # 7% for industrials
            "Consumer Discretionary": 0.05,  # 5% for consumer
            "Telecommunications": 0.04,      # 4% for telecom
            "Transportation": 0.06,  # 6% for transportation
            "Automotive": 0.05       # 5% for automotive
        }
        
        base_trend = sector_trends.get(sector, 0.05)  # Default 5%
        
        # Add time-based adjustments (simulate market cycles)
        year = start_date.year
        
        if 2020 <= year <= 2021:
            # COVID period - high volatility, tech outperformance
            if sector in ["Technology", "Healthcare"]:
                base_trend += 0.15  # Tech/health did well
            elif sector in ["Energy", "Transportation", "Consumer Discretionary"]:
                base_trend -= 0.10  # These struggled
        
        elif 2022 <= year <= 2023:
            # Post-COVID normalization, rate hikes
            if sector == "Financial Services":
                base_trend += 0.05  # Banks benefit from higher rates
            elif sector == "Technology":
                base_trend -= 0.08  # Tech hurt by higher rates
        
        # Add some randomness to avoid deterministic patterns
        base_trend += random.uniform(-0.02, 0.02)  # +/- 2% random adjustment
        
        return base_trend
    
    async def get_symbols_list(self) -> List[str]:
        """Get list of all available symbols"""
        return list(self.symbol_info.keys())
    
    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """Get metadata about a symbol"""
        if symbol not in self.symbol_info:
            return {}
        
        return dict(self.symbol_info[symbol])


class YahooFinanceNordicData(MarketDataProvider):
    """
    Real market data provider using Yahoo Finance API.
    
    This would implement actual data fetching from Yahoo Finance
    for Nordic stocks. For now, it's a placeholder.
    """
    
    def __init__(self):
        logger.warning("⚠️ YahooFinanceNordicData not implemented yet - use MockNordicMarketData for testing")
        self.mock_provider = MockNordicMarketData()
    
    async def get_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[MarketData]:
        """Fetch real market data from Yahoo Finance"""
        # TODO: Implement real Yahoo Finance integration
        # For now, delegate to mock provider
        logger.info(f"📊 Fetching {symbol} data from {start_date} to {end_date} (using mock data)")
        return await self.mock_provider.get_market_data(symbol, start_date, end_date)
    
    async def get_symbols_list(self) -> List[str]:
        """Get list of available Nordic symbols"""
        return await self.mock_provider.get_symbols_list()
    
    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """Get symbol metadata"""
        return await self.mock_provider.get_symbol_info(symbol)


def create_market_data_provider(provider_type: str = "mock") -> MarketDataProvider:
    """Factory function to create market data providers"""
    
    if provider_type == "mock":
        return MockNordicMarketData()
    elif provider_type == "yahoo":
        return YahooFinanceNordicData()
    else:
        raise ValueError(f"Unknown provider type: {provider_type}")


async def main():
    """Test the market data provider"""
    print("🧪 Testing Nordic Market Data Provider...")
    
    # Create provider
    provider = create_market_data_provider("mock")
    
    # Get available symbols
    symbols = await provider.get_symbols_list()
    print(f"📋 Available symbols: {len(symbols)}")
    print(f"🇸🇪 Swedish stocks: {[s for s in symbols if s.endswith('.ST')]}")
    print(f"🇳🇴 Norwegian stocks: {[s for s in symbols if s.endswith('.OL')]}")
    print(f"🇩🇰 Danish stocks: {[s for s in symbols if s.endswith('.CO')]}")
    
    # Test data generation
    test_symbol = "VOLV-B.ST"
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)
    
    print(f"\n📊 Generating data for {test_symbol}...")
    data = await provider.get_market_data(test_symbol, start_date, end_date)
    
    print(f"Generated {len(data)} trading days")
    if data:
        first_day = data[0]
        last_day = data[-1]
        
        total_return = (last_day.close_price - first_day.close_price) / first_day.close_price
        
        print(f"📈 Price: {first_day.close_price:.1f} → {last_day.close_price:.1f} SEK")
        print(f"💰 Total return: {total_return:.1%}")
        
        # Show sample of data
        print("\n📋 Sample data (first 5 days):")
        for i, day in enumerate(data[:5]):
            daily_return = (day.close_price - day.open_price) / day.open_price
            print(f"  {day.timestamp.date()}: {day.open_price:.1f} → {day.close_price:.1f} ({daily_return:+.1%})")
    
    # Test symbol info
    info = await provider.get_symbol_info(test_symbol)
    print(f"\n📊 {test_symbol} info:")
    for key, value in info.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Market data provider test completed!")


if __name__ == "__main__":
    asyncio.run(main())