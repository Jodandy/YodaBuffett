#!/usr/bin/env python3
"""
Enhanced Fundamental Value Strategy - Multi-Method Valuation Framework

Based on PineScript Fat Pitch strategy, implements 5 valuation methods:
1. Graham Number (asset + earnings based)
2. Earnings Power Value (normalized earnings)
3. Residual Income Model (book value + excess returns)
4. Free Cash Flow Yield
5. Net Current Asset Value (liquidation floor)

Uses yahoo_fundamentals table for Nordic stocks
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import numpy as np
import pandas as pd
from dataclasses import dataclass
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ValuationResult:
    """Result from a single valuation method"""
    method: str
    bear_value: Optional[float]
    base_value: Optional[float]
    bull_value: Optional[float]
    confidence: float  # 0-1 confidence in the valuation
    details: Dict[str, Any]


@dataclass
class CompositeValuation:
    """Composite valuation combining all methods"""
    fat_pitch_price: Optional[float]  # Entry price with asymmetric risk/reward
    fair_value: Optional[float]       # Base case fair value
    overvalued_price: Optional[float] # Exit consideration price
    upside_target: Optional[float]    # Target exit price
    downside_target: Optional[float]  # Risk level
    current_asymmetry: Optional[float] # Current upside/downside ratio
    method_count: int                  # Number of methods agreeing
    valuations: List[ValuationResult]
    

class FundamentalValueStrategy:
    """Multi-method fundamental value strategy using yahoo_fundamentals"""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        
        # Strategy parameters (matching PineScript defaults)
        self.bear_return = 0.15  # 15% required return
        self.base_return = 0.10  # 10% required return
        self.bull_return = 0.08  # 8% required return
        self.asymmetry_ratio = 3.0  # 3:1 upside/downside for Fat Pitch
        
        # Graham multipliers
        self.graham_bear_mult = 15.0
        self.graham_base_mult = 22.5
        self.graham_bull_mult = 30.0
        
        # RIM settings
        self.rim_fade_years = 10  # Years of excess returns
        
        # Position management
        self.max_positions = 5
        self.position_size = 0.20  # 20% per position
        
    async def get_fundamental_data(self, symbol: str, date: datetime) -> Dict[str, Optional[float]]:
        """Fetch fundamental data from historical_fundamentals_daily table"""
        # Query the historical_fundamentals_daily table for historical data
        query = """
            SELECT 
                book_value_per_share,
                revenue_per_share,
                market_cap,
                enterprise_value,
                current_ratio,
                debt_to_equity,
                pe_ratio,
                pb_ratio,
                ps_ratio,
                ev_ebitda,
                cash_per_share
            FROM historical_fundamentals_daily
            WHERE symbol = $1
                AND date <= $2
            ORDER BY date DESC
            LIMIT 1
        """
        
        row = await self.conn.fetchrow(query, symbol, date)
        
        if not row:
            return {
                'book_value_per_share': None,
                'earnings_per_share': None,
                'roe': None,
                'free_cash_flow_per_share': None,
                'current_assets_per_share': None,
                'total_liabilities_per_share': None,
                'shares_outstanding': None
            }
        
        # Calculate shares outstanding from market cap and P/E ratio
        shares_outstanding = None
        if row['market_cap']:
            # Use price from the historical data if available, otherwise query
            price_query = """
                SELECT close_price FROM daily_price_data 
                WHERE symbol = $1 AND date <= $2 
                ORDER BY date DESC LIMIT 1
            """
            price_row = await self.conn.fetchrow(price_query, symbol, date)
            if price_row and price_row['close_price'] > 0:
                shares_outstanding = row['market_cap'] / float(price_row['close_price'])
        
        # Calculate EPS from P/E ratio and price
        earnings_per_share = None
        if row['pe_ratio'] and price_row:
            current_price = float(price_row['close_price'])
            earnings_per_share = current_price / row['pe_ratio'] if row['pe_ratio'] > 0 else None
        
        # Calculate ROE from P/B and P/E ratios
        roe = None
        if row['pe_ratio'] and row['pb_ratio'] and row['pe_ratio'] > 0 and row['pb_ratio'] > 0:
            # ROE = 1 / (P/E * P/B) * 100
            roe = (1 / (row['pe_ratio'] * row['pb_ratio'])) * 100
        
        # Estimate FCF per share (rough approximation)
        fcf_per_share = None
        if earnings_per_share and earnings_per_share > 0:
            # Assume FCF is ~80% of earnings (conservative estimate)
            fcf_per_share = earnings_per_share * 0.8
        
        # For NCAV calculation, estimate using available ratios
        current_assets_per_share = None
        total_liabilities_per_share = None
        
        if shares_outstanding and row['current_ratio'] and row['debt_to_equity']:
            # Estimate current assets from cash and book value
            if row['cash_per_share'] and row['current_ratio']:
                # Rough estimation: current assets = cash + (current ratio - 1) * cash
                current_assets_per_share = row['cash_per_share'] * row['current_ratio']
            
            # Estimate total liabilities from debt-to-equity and book value
            if row['book_value_per_share'] and row['debt_to_equity']:
                equity_per_share = row['book_value_per_share']
                debt_per_share = equity_per_share * (row['debt_to_equity'] / 100)  # Convert percentage
                total_liabilities_per_share = debt_per_share * 1.2  # Add other liabilities
        
        return {
            'book_value_per_share': row['book_value_per_share'],
            'earnings_per_share': earnings_per_share,
            'roe': roe,
            'free_cash_flow_per_share': fcf_per_share,
            'current_assets_per_share': current_assets_per_share,
            'total_liabilities_per_share': total_liabilities_per_share,
            'shares_outstanding': shares_outstanding
        }
    
    async def calculate_graham_number(self, fundamentals: Dict) -> ValuationResult:
        """Calculate Graham Number valuation"""
        bvps = fundamentals.get('book_value_per_share')
        eps = fundamentals.get('earnings_per_share')
        
        # Graham Number only valid for positive EPS and BVPS
        if not bvps or not eps or bvps <= 0 or eps <= 0:
            return ValuationResult(
                method='Graham Number',
                bear_value=None,
                base_value=None,
                bull_value=None,
                confidence=0.0,
                details={'error': 'Invalid or negative EPS/BVPS'}
            )
        
        bear_value = np.sqrt(self.graham_bear_mult * eps * bvps)
        base_value = np.sqrt(self.graham_base_mult * eps * bvps)
        bull_value = np.sqrt(self.graham_bull_mult * eps * bvps)
        
        return ValuationResult(
            method='Graham Number',
            bear_value=bear_value,
            base_value=base_value,
            bull_value=bull_value,
            confidence=0.8,  # High confidence for profitable companies
            details={
                'eps': eps,
                'bvps': bvps,
                'formula': 'sqrt(multiplier × EPS × BVPS)'
            }
        )
    
    async def calculate_earnings_power_value(self, fundamentals: Dict) -> ValuationResult:
        """Calculate Earnings Power Value (EPV)"""
        eps = fundamentals.get('earnings_per_share')
        
        # EPV only valid for positive earnings
        if not eps or eps <= 0:
            return ValuationResult(
                method='Earnings Power Value',
                bear_value=None,
                base_value=None,
                bull_value=None,
                confidence=0.0,
                details={'error': 'Negative or no earnings'}
            )
        
        bear_value = eps / self.bear_return
        base_value = eps / self.base_return
        bull_value = eps / self.bull_return
        
        return ValuationResult(
            method='Earnings Power Value',
            bear_value=bear_value,
            base_value=base_value,
            bull_value=bull_value,
            confidence=0.7,  # Moderate confidence
            details={
                'eps': eps,
                'formula': 'EPS / required_return'
            }
        )
    
    async def calculate_residual_income_model(self, fundamentals: Dict) -> ValuationResult:
        """Calculate Residual Income Model (RIM) valuation"""
        bvps = fundamentals.get('book_value_per_share')
        roe = fundamentals.get('roe')
        
        if not bvps or not roe or bvps <= 0:
            return ValuationResult(
                method='Residual Income Model',
                bear_value=None,
                base_value=None,
                bull_value=None,
                confidence=0.0,
                details={'error': 'Invalid BVPS or ROE'}
            )
        
        # Convert ROE to decimal
        roe_decimal = roe / 100 if roe > 1 else roe
        
        # Calculate annuity factors
        annuity_bear = (1 - (1 + self.bear_return) ** (-self.rim_fade_years)) / self.bear_return
        annuity_base = (1 - (1 + self.base_return) ** (-self.rim_fade_years)) / self.base_return
        annuity_bull = (1 - (1 + self.bull_return) ** (-self.rim_fade_years)) / self.bull_return
        
        # Value = Book Value + Present Value of Excess Returns
        bear_value = bvps + (roe_decimal - self.bear_return) * bvps * annuity_bear
        base_value = bvps + (roe_decimal - self.base_return) * bvps * annuity_base
        bull_value = bvps + (roe_decimal - self.bull_return) * bvps * annuity_bull
        
        # Ensure non-negative values
        bear_value = max(bear_value, bvps * 0.5)  # Floor at 50% of book
        base_value = max(base_value, bvps * 0.7)  # Floor at 70% of book
        bull_value = max(bull_value, bvps * 0.9)  # Floor at 90% of book
        
        return ValuationResult(
            method='Residual Income Model',
            bear_value=bear_value,
            base_value=base_value,
            bull_value=bull_value,
            confidence=0.75,
            details={
                'bvps': bvps,
                'roe': roe,
                'fade_years': self.rim_fade_years,
                'formula': 'BVPS + (ROE - r) × BVPS × annuity_factor'
            }
        )
    
    async def calculate_fcf_value(self, fundamentals: Dict) -> ValuationResult:
        """Calculate Free Cash Flow based valuation"""
        fcfps = fundamentals.get('free_cash_flow_per_share')
        
        if not fcfps or fcfps <= 0:
            return ValuationResult(
                method='FCF Yield Value',
                bear_value=None,
                base_value=None,
                bull_value=None,
                confidence=0.0,
                details={'error': 'Negative or no free cash flow'}
            )
        
        bear_value = fcfps / self.bear_return
        base_value = fcfps / self.base_return
        bull_value = fcfps / self.bull_return
        
        return ValuationResult(
            method='FCF Yield Value',
            bear_value=bear_value,
            base_value=base_value,
            bull_value=bull_value,
            confidence=0.85,  # High confidence for cash-generative businesses
            details={
                'fcfps': fcfps,
                'formula': 'FCFPS / required_yield'
            }
        )
    
    async def calculate_ncav(self, fundamentals: Dict) -> ValuationResult:
        """Calculate Net Current Asset Value (NCAV)"""
        current_assets = fundamentals.get('current_assets_per_share')
        total_liabilities = fundamentals.get('total_liabilities_per_share')
        
        if not current_assets or not total_liabilities:
            return ValuationResult(
                method='Net Current Asset Value',
                bear_value=None,
                base_value=None,
                bull_value=None,
                confidence=0.0,
                details={'error': 'Missing balance sheet data'}
            )
        
        ncav_per_share = current_assets - total_liabilities
        
        # Graham's 2/3 rule for NCAV
        bear_value = ncav_per_share * 0.67 if ncav_per_share > 0 else None
        base_value = ncav_per_share if ncav_per_share > 0 else None
        bull_value = None  # No bull case for liquidation value
        
        return ValuationResult(
            method='Net Current Asset Value',
            bear_value=bear_value,
            base_value=base_value,
            bull_value=bull_value,
            confidence=0.9 if ncav_per_share > 0 else 0.0,
            details={
                'current_assets_ps': current_assets,
                'total_liabilities_ps': total_liabilities,
                'ncav_ps': ncav_per_share,
                'formula': '(Current Assets - Total Liabilities) / Shares'
            }
        )
    
    async def calculate_composite_valuation(self, 
                                          valuations: List[ValuationResult],
                                          current_price: float) -> CompositeValuation:
        """Calculate composite valuation from multiple methods"""
        
        # Collect valid values from each scenario
        bear_values = [v.bear_value for v in valuations 
                      if v.bear_value is not None and v.confidence > 0.5]
        base_values = [v.base_value for v in valuations 
                      if v.base_value is not None and v.confidence > 0.5]
        bull_values = [v.bull_value for v in valuations 
                      if v.bull_value is not None and v.confidence > 0.5]
        
        # Need at least 2 methods to agree
        if len(base_values) < 2:
            return CompositeValuation(
                fat_pitch_price=None,
                fair_value=None,
                overvalued_price=None,
                upside_target=None,
                downside_target=None,
                current_asymmetry=None,
                method_count=0,
                valuations=valuations
            )
        
        # Calculate weighted means (weighted by confidence)
        weights = [v.confidence for v in valuations if v.base_value is not None and v.confidence > 0.5]
        
        mean_bears = np.average(bear_values, weights=weights[:len(bear_values)]) if bear_values else None
        mean_bases = np.average(base_values, weights=weights) if base_values else None
        mean_bulls = np.average(bull_values, weights=weights[:len(bull_values)]) if bull_values else None
        
        # Calculate targets
        fair_value = mean_bases
        upside_target = (mean_bases + mean_bulls) / 2 if mean_bases and mean_bulls else None
        downside_target = mean_bears
        
        # Fat Pitch calculation using asymmetry ratio
        fat_pitch_price = None
        overvalued_price = None
        
        if upside_target and downside_target:
            # Fat Pitch = (Upside + ratio × Downside) / (ratio + 1)
            fat_pitch_price = (upside_target + self.asymmetry_ratio * downside_target) / (self.asymmetry_ratio + 1)
            # Overvalued = (ratio × Upside + Downside) / (ratio + 1)
            overvalued_price = (self.asymmetry_ratio * upside_target + downside_target) / (self.asymmetry_ratio + 1)
        
        # Calculate current asymmetry
        current_asymmetry = None
        if upside_target and downside_target and current_price > downside_target:
            upside = upside_target - current_price
            downside = current_price - downside_target
            if downside > 0:
                current_asymmetry = upside / downside
        
        return CompositeValuation(
            fat_pitch_price=fat_pitch_price,
            fair_value=fair_value,
            overvalued_price=overvalued_price,
            upside_target=upside_target,
            downside_target=downside_target,
            current_asymmetry=current_asymmetry,
            method_count=len(base_values),
            valuations=valuations
        )
    
    async def evaluate_opportunity(self, symbol: str, date: datetime, current_price: float) -> CompositeValuation:
        """Evaluate a stock using all valuation methods"""
        
        # Get fundamental data
        fundamentals = await self.get_fundamental_data(symbol, date)
        
        # Calculate all valuations
        valuations = []
        
        # 1. Graham Number
        graham = await self.calculate_graham_number(fundamentals)
        valuations.append(graham)
        
        # 2. Earnings Power Value
        epv = await self.calculate_earnings_power_value(fundamentals)
        valuations.append(epv)
        
        # 3. Residual Income Model
        rim = await self.calculate_residual_income_model(fundamentals)
        valuations.append(rim)
        
        # 4. Free Cash Flow Value
        fcf = await self.calculate_fcf_value(fundamentals)
        valuations.append(fcf)
        
        # 5. Net Current Asset Value
        ncav = await self.calculate_ncav(fundamentals)
        valuations.append(ncav)
        
        # Calculate composite
        composite = await self.calculate_composite_valuation(valuations, current_price)
        
        return composite
    
    async def screen_nordic_opportunities(self, date: datetime = None, min_asymmetry: float = 2.0) -> List[Dict]:
        """Screen Nordic stocks for Fat Pitch opportunities"""
        
        if date is None:
            date = datetime.now()
        
        # Get all Nordic stocks with recent fundamentals from historical data
        query = """
            WITH latest_fundamentals AS (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    date,
                    market_cap
                FROM historical_fundamentals_daily
                WHERE date <= $1
                    AND date >= $1 - INTERVAL '30 days'
                    AND market_cap >= $2
                ORDER BY symbol, date DESC
            ),
            recent_prices AS (
                SELECT DISTINCT ON (m.symbol)
                    m.symbol,
                    m.close_price,
                    m.date
                FROM daily_price_data m
                INNER JOIN latest_fundamentals f ON m.symbol = f.symbol
                WHERE m.date <= $1
                    AND m.date >= $1 - INTERVAL '7 days'
                ORDER BY m.symbol, m.date DESC
            )
            SELECT 
                p.symbol,
                p.close_price as current_price,
                f.market_cap
            FROM recent_prices p
            INNER JOIN latest_fundamentals f ON p.symbol = f.symbol
            WHERE p.close_price > 0
            ORDER BY f.market_cap DESC NULLS LAST
        """
        
        rows = await self.conn.fetch(query, date, 1000000000)  # 1B SEK minimum market cap
        
        opportunities = []
        
        for row in rows:
            try:
                symbol = row['symbol']
                current_price = float(row['close_price'])
                
                # Evaluate opportunity
                composite = await self.evaluate_opportunity(symbol, date, current_price)
                
                if composite.fat_pitch_price and composite.current_asymmetry and composite.current_asymmetry >= min_asymmetry:
                    opportunities.append({
                        'symbol': symbol,
                        'current_price': current_price,
                        'fat_pitch_price': composite.fat_pitch_price,
                        'fair_value': composite.fair_value,
                        'upside_target': composite.upside_target,
                        'downside_target': composite.downside_target,
                        'current_asymmetry': composite.current_asymmetry,
                        'method_count': composite.method_count,
                        'upside_pct': ((composite.upside_target - current_price) / current_price * 100) if composite.upside_target else 0,
                        'discount_pct': ((composite.fat_pitch_price - current_price) / composite.fat_pitch_price * 100),
                        'market_cap': row['market_cap']
                    })
                    
            except Exception as e:
                logger.debug(f"Error evaluating {symbol}: {e}")
                continue
        
        # Sort by asymmetry ratio (best opportunities first)
        opportunities.sort(key=lambda x: x['current_asymmetry'], reverse=True)
        
        return opportunities


async def main():
    """Test the enhanced fundamental value strategy with Nordic stocks"""
    
    # Connect to database
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        # Initialize strategy
        strategy = FundamentalValueStrategy(conn)
        
        print("\n" + "="*80)
        print("FUNDAMENTAL VALUE STRATEGY - FAT PITCH FRAMEWORK")
        print("Nordic Stock Analysis")
        print("="*80)
        
        # Test with a specific Nordic stock
        test_symbol = 'VOLV-B'  # Volvo B
        
        # Get current price
        price_query = "SELECT close_price FROM daily_price_data WHERE symbol = $1 ORDER BY date DESC LIMIT 1"
        price_row = await conn.fetchrow(price_query, test_symbol)
        
        if price_row:
            current_price = float(price_row['close_price'])
            print(f"\n📊 ANALYZING {test_symbol}")
            print(f"Current Price: ${current_price:.2f}")
            
            # Get fundamental data
            fundamentals = await strategy.get_fundamental_data(test_symbol, datetime.now())
            
            print("\nFundamental Data:")
            for metric, value in fundamentals.items():
                if value is not None:
                    print(f"  {metric}: {value:.2f}")
            
            # Evaluate opportunity
            composite = await strategy.evaluate_opportunity(test_symbol, datetime.now(), current_price)
            
            print("\nValuation Methods:")
            for val in composite.valuations:
                if val.confidence > 0:
                    print(f"\n  {val.method}:")
                    if val.bear_value:
                        print(f"    Bear: ${val.bear_value:.2f}")
                    if val.base_value:
                        print(f"    Base: ${val.base_value:.2f}")
                    if val.bull_value:
                        print(f"    Bull: ${val.bull_value:.2f}")
                    print(f"    Confidence: {val.confidence:.0%}")
                else:
                    print(f"\n  {val.method}: {val.details.get('error', 'No data')}")
            
            if composite.fair_value:
                print(f"\n🎯 COMPOSITE VALUATION:")
                print(f"  Fat Pitch Entry: ${composite.fat_pitch_price:.2f}")
                print(f"  Fair Value: ${composite.fair_value:.2f}")
                print(f"  Upside Target: ${composite.upside_target:.2f}")
                print(f"  Downside Risk: ${composite.downside_target:.2f}")
                print(f"  Current Asymmetry: {composite.current_asymmetry:.1f}:1")
                print(f"  Methods Agreeing: {composite.method_count}")
                
                # Investment decision
                if current_price < composite.fat_pitch_price and composite.current_asymmetry >= strategy.asymmetry_ratio:
                    print(f"\n✅ FAT PITCH BUY SIGNAL!")
                elif current_price < composite.fair_value:
                    print(f"\n🔵 Undervalued but not a Fat Pitch")
                else:
                    print(f"\n⚪ Fairly valued or overvalued")
        
        # Screen for opportunities
        print("\n" + "="*80)
        print("SCREENING FOR FAT PITCH OPPORTUNITIES")
        print("="*80)
        
        opportunities = await strategy.screen_nordic_opportunities(min_asymmetry=2.5)
        
        if opportunities:
            print(f"\nFound {len(opportunities)} opportunities with 2.5:1+ asymmetry:\n")
            print(f"{'Symbol':8} {'Price':>8} {'Fat Pitch':>10} {'Fair Value':>11} {'Asymmetry':>10} {'Upside':>8}")
            print("-" * 65)
            
            for opp in opportunities[:15]:
                indicator = "🟢" if opp['current_asymmetry'] >= 3.0 else "🟡"
                print(f"{indicator} {opp['symbol']:6} ${opp['current_price']:7.2f} "
                      f"${opp['fat_pitch_price']:9.2f} ${opp['fair_value']:10.2f} "
                      f"{opp['current_asymmetry']:9.1f}:1 {opp['upside_pct']:7.1f}%")
        else:
            print("\nNo Fat Pitch opportunities found currently.")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())