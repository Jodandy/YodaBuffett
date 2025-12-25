#!/usr/bin/env python3
"""
Combined Fundamental + Technical Analysis

Integrates Yahoo Finance fundamentals with technical indicators for enhanced predictions.
Uses both fundamental health metrics and technical patterns for trading decisions.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, NamedTuple
import json
import hashlib
import yfinance as yf

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, EMA, VolumeMA

class FundamentalTechnicalPattern(NamedTuple):
    """Combined pattern with fundamentals and technicals."""
    # Technical indicators
    rsi_14: float
    price_to_sma20: float
    volume_ratio: float
    price_change_5d: float
    
    # Fundamental indicators
    pe_ratio: float
    price_to_book: float
    debt_to_equity: float
    roe: float
    current_ratio: float
    profit_margin: float
    
    # Context
    date: date
    symbol: str
    price: float

class FundamentalTechnicalAnalyzer:
    """Combines fundamental and technical analysis."""
    
    def __init__(self):
        self.db_conn = None
        self.indicator_engine = None
        
    async def setup(self):
        """Initialize database and indicators."""
        print("🔬 Setting up Fundamental + Technical Analyzer...")
        
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Register indicators
        indicators = [
            ('rsi_14', RSI(period=14)),
            ('sma_20', SMA(period=20)),
            ('ema_10', EMA(period=10)),
            ('volume_sma_20', VolumeMA(period=20)),
        ]
        
        for name, indicator in indicators:
            indicator_registry.register(indicator)
            
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        print("✅ Setup complete!")
        
    def get_company_id(self, symbol: str) -> int:
        """Convert symbol to company ID."""
        return int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
        
    async def get_yahoo_fundamentals(self, symbol: str) -> Dict:
        """Get latest fundamentals from Yahoo Finance."""
        try:
            # Get Nordic ticker format
            yahoo_ticker = f"{symbol.split('.')[0]}.ST"
            ticker = yf.Ticker(yahoo_ticker)
            info = ticker.info
            
            if not info:
                return {}
                
            # Extract key fundamental metrics
            fundamentals = {
                'pe_ratio': info.get('trailingPE', 20.0),  # Default to 20 if missing
                'price_to_book': info.get('priceToBook', 1.0),
                'debt_to_equity': info.get('debtToEquity', 1.0), 
                'roe': info.get('returnOnEquity', 0.1),
                'current_ratio': info.get('currentRatio', 1.0),
                'profit_margin': info.get('profitMargins', 0.1),
                'market_cap': info.get('marketCap'),
                'dividend_yield': info.get('dividendYield', 0.0),
                'peg_ratio': info.get('pegRatio', 1.0),
                'revenue_growth': info.get('revenueGrowth', 0.0)
            }
            
            # Convert None to reasonable defaults
            for key, value in fundamentals.items():
                if value is None:
                    if key == 'pe_ratio':
                        fundamentals[key] = 20.0
                    elif key in ['price_to_book', 'debt_to_equity', 'current_ratio', 'peg_ratio']:
                        fundamentals[key] = 1.0
                    else:
                        fundamentals[key] = 0.0
                        
            return fundamentals
            
        except Exception as e:
            print(f"⚠️ Error fetching Yahoo fundamentals for {symbol}: {e}")
            # Return defaults if Yahoo fails
            return {
                'pe_ratio': 20.0,
                'price_to_book': 1.0,
                'debt_to_equity': 1.0,
                'roe': 0.1,
                'current_ratio': 1.0,
                'profit_margin': 0.1,
                'market_cap': 0,
                'dividend_yield': 0.0,
                'peg_ratio': 1.0,
                'revenue_growth': 0.0
            }
            
    async def get_market_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get market data for analysis."""
        buffer_start = start_date - timedelta(days=100)
        
        query = """
        SELECT date, 
               open_price::NUMERIC as open, 
               high_price::NUMERIC as high,
               low_price::NUMERIC as low, 
               close_price::NUMERIC as close, 
               volume::BIGINT as volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, buffer_start, end_date)
        if not rows:
            return pd.DataFrame()
            
        df = pd.DataFrame([dict(row) for row in rows])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        return df
        
    def calculate_fundamental_score(self, fundamentals: Dict) -> float:
        """Calculate a composite fundamental health score (0-100)."""
        score = 0.0
        
        # P/E Ratio (lower is better, but not too low)
        pe = fundamentals.get('pe_ratio', 20)
        if 10 <= pe <= 20:
            score += 20  # Ideal range
        elif 5 <= pe < 10 or 20 < pe <= 30:
            score += 10  # Acceptable
        elif pe > 0 and pe < 5:
            score += 5   # Might be a value trap
            
        # P/B Ratio (lower is generally better)
        pb = fundamentals.get('price_to_book', 1)
        if 0 < pb <= 1:
            score += 20  # Trading below book value
        elif 1 < pb <= 2:
            score += 15
        elif 2 < pb <= 3:
            score += 10
        elif 3 < pb <= 5:
            score += 5
            
        # Debt to Equity (lower is better)
        de = fundamentals.get('debt_to_equity', 1)
        if 0 <= de <= 0.5:
            score += 20  # Low debt
        elif 0.5 < de <= 1:
            score += 15
        elif 1 < de <= 2:
            score += 10
        elif de > 2:
            score += 0   # High debt risk
            
        # ROE (higher is better)
        roe = fundamentals.get('roe', 0.1)
        if roe >= 0.20:
            score += 20  # Excellent
        elif roe >= 0.15:
            score += 15
        elif roe >= 0.10:
            score += 10
        elif roe >= 0.05:
            score += 5
            
        # Current Ratio (liquidity)
        cr = fundamentals.get('current_ratio', 1)
        if 1.5 <= cr <= 3:
            score += 10  # Ideal liquidity
        elif 1 <= cr < 1.5:
            score += 7
        elif cr > 3:
            score += 5   # Maybe too conservative
        elif 0.5 <= cr < 1:
            score += 3   # Liquidity concern
            
        # Profit Margin
        pm = fundamentals.get('profit_margin', 0.1)
        if pm >= 0.20:
            score += 10  # High margin business
        elif pm >= 0.10:
            score += 7
        elif pm >= 0.05:
            score += 5
        elif pm >= 0:
            score += 3
            
        return score
        
    def create_combined_signals(self, technical_signal: Dict, fundamental_score: float) -> Dict:
        """Combine technical and fundamental signals."""
        
        # Weight the signals
        technical_weight = 0.6  # 60% technical
        fundamental_weight = 0.4  # 40% fundamental
        
        # Convert fundamental score to signal strength (0-100 to -1 to 1)
        fundamental_signal = (fundamental_score - 50) / 50  # -1 to 1
        
        # Get technical signal strength
        technical_strength = technical_signal.get('confidence', 0.5)
        if technical_signal.get('direction') == 'down':
            technical_strength = -technical_strength
            
        # Combined signal
        combined_strength = (technical_strength * technical_weight + 
                           fundamental_signal * fundamental_weight)
        
        # Decision logic
        buy_threshold = 0.3
        sell_threshold = -0.3
        
        if combined_strength >= buy_threshold:
            action = 'BUY'
        elif combined_strength <= sell_threshold:
            action = 'SELL'
        else:
            action = 'HOLD'
            
        return {
            'action': action,
            'combined_strength': combined_strength,
            'technical_strength': technical_strength,
            'fundamental_signal': fundamental_signal,
            'fundamental_score': fundamental_score,
            'confidence': abs(combined_strength)
        }
        
    async def analyze_stock(self, symbol: str, analysis_date: date) -> Dict:
        """Perform combined fundamental and technical analysis."""
        
        print(f"\n📊 Analyzing {symbol} on {analysis_date}")
        
        # Get market data
        start_date = analysis_date - timedelta(days=100)
        market_data = await self.get_market_data(symbol, start_date, analysis_date)
        
        if market_data.empty:
            return {'error': f'No market data for {symbol}'}
            
        # Get technical indicators
        company_id = self.get_company_id(symbol)
        
        technical_values = await self.indicator_engine.calculate_multiple(
            ['rsi_14', 'sma_20', 'volume_sma_20'],
            company_id,
            market_data,
            start_date,
            analysis_date
        )
        
        # Extract latest technical values
        latest_idx = market_data.index[-1]
        latest_price = float(market_data.loc[latest_idx, 'close'])
        
        rsi_values = technical_values.get('rsi_14', {}).values
        rsi_current = list(rsi_values.values())[-1] if rsi_values else 50
        
        sma_values = technical_values.get('sma_20', {}).values
        sma_current = float(list(sma_values.values())[-1]) if sma_values else latest_price
        
        # Simple technical signal
        technical_signal = {
            'direction': 'up' if rsi_current < 30 else 'down' if rsi_current > 70 else 'neutral',
            'confidence': abs(rsi_current - 50) / 50,
            'rsi': rsi_current,
            'price_to_sma': latest_price / sma_current if sma_current > 0 else 1.0
        }
        
        # Get fundamentals
        fundamentals = await self.get_yahoo_fundamentals(symbol)
        fundamental_score = self.calculate_fundamental_score(fundamentals)
        
        # Combine signals
        combined_signal = self.create_combined_signals(technical_signal, fundamental_score)
        
        # Create comprehensive result
        result = {
            'symbol': symbol,
            'date': analysis_date,
            'price': latest_price,
            
            # Technical analysis
            'technical': {
                'rsi': rsi_current,
                'price_to_sma': technical_signal['price_to_sma'],
                'signal': technical_signal['direction'],
                'confidence': technical_signal['confidence']
            },
            
            # Fundamental analysis
            'fundamental': {
                'score': fundamental_score,
                'pe_ratio': fundamentals.get('pe_ratio'),
                'price_to_book': fundamentals.get('price_to_book'),
                'debt_to_equity': fundamentals.get('debt_to_equity'),
                'roe': fundamentals.get('roe'),
                'current_ratio': fundamentals.get('current_ratio'),
                'profit_margin': fundamentals.get('profit_margin')
            },
            
            # Combined signal
            'combined': combined_signal
        }
        
        # Display results
        print(f"  💹 Price: ${latest_price:.2f}")
        print(f"  📈 Technical: RSI={rsi_current:.1f}, Signal={technical_signal['direction']}")
        print(f"  💰 Fundamental Score: {fundamental_score:.1f}/100")
        print(f"     P/E: {fundamentals.get('pe_ratio', 'N/A')}")
        print(f"     P/B: {fundamentals.get('price_to_book', 'N/A')}")
        print(f"     ROE: {fundamentals.get('roe', 'N/A')}")
        print(f"  🎯 Combined Signal: {combined_signal['action']} "
              f"(Strength: {combined_signal['combined_strength']:.2f})")
        
        return result
        
    async def analyze_multiple(self, symbols: List[str], analysis_date: date = None) -> List[Dict]:
        """Analyze multiple stocks."""
        if analysis_date is None:
            analysis_date = date.today()
            
        results = []
        
        for symbol in symbols:
            try:
                result = await self.analyze_stock(symbol, analysis_date)
                results.append(result)
                
                # Be respectful to APIs
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"❌ Error analyzing {symbol}: {e}")
                results.append({'symbol': symbol, 'error': str(e)})
                
        return results
        
    def rank_opportunities(self, results: List[Dict]) -> List[Dict]:
        """Rank stocks by combined opportunity score."""
        
        # Filter out errors
        valid_results = [r for r in results if 'error' not in r]
        
        # Sort by combined strength
        sorted_results = sorted(
            valid_results,
            key=lambda x: x['combined']['combined_strength'],
            reverse=True
        )
        
        print("\n🏆 TOP OPPORTUNITIES (Combined Analysis)")
        print("=" * 80)
        print(f"{'Rank':<5} {'Symbol':<10} {'Action':<8} {'Strength':<10} "
              f"{'Tech':<8} {'Fund':<8} {'F-Score':<8} {'Price':<10}")
        print("-" * 80)
        
        for i, result in enumerate(sorted_results[:10], 1):
            print(f"{i:<5} {result['symbol']:<10} "
                  f"{result['combined']['action']:<8} "
                  f"{result['combined']['combined_strength']:<10.3f} "
                  f"{result['combined']['technical_strength']:<8.3f} "
                  f"{result['combined']['fundamental_signal']:<8.3f} "
                  f"{result['fundamental']['score']:<8.1f} "
                  f"${result['price']:<10.2f}")
                  
        return sorted_results
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run combined analysis."""
    
    analyzer = FundamentalTechnicalAnalyzer()
    
    try:
        await analyzer.setup()
        
        # Test with Nordic stocks
        test_symbols = [
            'VOLV-B',    # Volvo
            'ERIC-B',    # Ericsson  
            'HM-B',      # H&M
            'SAND',      # Sandvik
            'ABB',       # ABB
            'ATCO-A',    # Atlas Copco
            'SEB-A',     # SEB Bank
            'SSAB-A',    # SSAB
            'SKF-B',     # SKF
            'INVE-B'     # Investor
        ]
        
        print("🚀 Combined Fundamental + Technical Analysis")
        print("=" * 60)
        
        # Analyze all stocks
        results = await analyzer.analyze_multiple(test_symbols)
        
        # Rank opportunities
        analyzer.rank_opportunities(results)
        
        print("\n💡 Analysis Legend:")
        print("  - Strength: Combined signal (-1 to +1)")
        print("  - Tech: Technical signal component")
        print("  - Fund: Fundamental signal component")
        print("  - F-Score: Fundamental health score (0-100)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())