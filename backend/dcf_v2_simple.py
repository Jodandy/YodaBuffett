#!/usr/bin/env python3
"""
DCF v2.0 - Simplified Market-Aware Model

Key improvements:
1. Mean reversion in growth rates
2. Sector-specific discounting
3. Shorter projection periods
4. Market sentiment adjustment
"""

import asyncio
import asyncpg
import numpy as np
from datetime import datetime
from typing import Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleDCF2:
    """Simplified DCF 2.0"""
    
    def __init__(self):
        self.db_conn = None
        
        # Sector adjustments
        self.sector_wacc_premium = {
            'Vindkraft': -0.01,  # Lower risk
            'Internettjänster': 0.03,  # Higher risk
            'Gaming & Spel': 0.04,
            'Affärs- & IT-System': 0.02,
        }
        
        self.sector_growth_decay = {
            'Vindkraft': 0.9,  # Slower decay
            'Internettjänster': 0.7,  # Faster decay
            'Gaming & Spel': 0.6,
            'Affärs- & IT-System': 0.8,
        }
    
    async def setup(self):
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()
    
    async def get_company_fundamentals(self, symbol: str, valuation_date: datetime):
        """Get simplified company fundamentals"""
        
        # Get basic info
        info_query = """
        SELECT 
            cm.sector,
            cm.stock_currency,
            cm.report_currency,
            bs.shares_outstanding
        FROM company_master cm
        LEFT JOIN LATERAL (
            SELECT shares_outstanding
            FROM balance_sheet_data
            WHERE symbol = cm.primary_ticker
            AND publish_date <= $2
            AND shares_outstanding > 0
            ORDER BY publish_date DESC
            LIMIT 1
        ) bs ON true
        WHERE cm.primary_ticker = $1
        """
        
        info = await self.db_conn.fetchrow(info_query, symbol, valuation_date.date())
        
        if not info or not info['shares_outstanding']:
            logger.warning(f"No basic info for {symbol}")
            return None
        
        # Get recent financials (last 8 quarters or 2 years)
        financials_query = """
        SELECT 
            total_revenue,
            operating_income,
            publish_date,
            period_date
        FROM financial_statements
        WHERE symbol = $1
        AND publish_date <= $2
        AND publish_date >= $2 - INTERVAL '2 years'
        AND total_revenue > 0
        ORDER BY publish_date DESC
        LIMIT 8
        """
        
        financials = await self.db_conn.fetch(financials_query, symbol, valuation_date.date())
        
        if len(financials) < 3:
            logger.warning(f"Insufficient financials for {symbol}: {len(financials)} periods")
            return None
        
        # Calculate metrics
        revenues = [float(f['total_revenue']) for f in financials]
        margins = []
        
        for f in financials[:4]:  # Recent margins
            if float(f['total_revenue']) > 0:
                margin = float(f['operating_income']) / float(f['total_revenue'])
                margins.append(margin)
        
        # Growth calculation (year-over-year for more stability)
        yoy_growth = None
        if len(revenues) >= 4:
            recent_revenue = np.mean(revenues[:2])  # Average of 2 most recent
            older_revenue = np.mean(revenues[2:4])  # Average of next 2
            if older_revenue > 0:
                yoy_growth = (recent_revenue - older_revenue) / older_revenue
        
        # Get debt
        debt_query = """
        SELECT 
            COALESCE(total_debt, 0) - COALESCE(cash_and_equivalents, 0) as net_debt
        FROM balance_sheet_data
        WHERE symbol = $1
        AND publish_date <= $2
        ORDER BY publish_date DESC
        LIMIT 1
        """
        
        debt_result = await self.db_conn.fetchrow(debt_query, symbol, valuation_date.date())
        net_debt = float(debt_result['net_debt']) if debt_result else 0
        
        # Get recent price momentum
        momentum_query = """
        WITH prices AS (
            SELECT 
                (SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date <= $2 ORDER BY date DESC LIMIT 1) as current,
                (SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date <= $2 - INTERVAL '90 days' ORDER BY date DESC LIMIT 1) as prev_90d
        )
        SELECT 
            current,
            prev_90d,
            CASE WHEN prev_90d > 0 THEN (current - prev_90d) / prev_90d ELSE NULL END as momentum
        FROM prices
        """
        
        momentum_result = await self.db_conn.fetchrow(momentum_query, symbol, valuation_date.date())
        momentum = momentum_result['momentum'] if momentum_result and momentum_result['momentum'] else 0
        
        return {
            'symbol': symbol,
            'sector': info['sector'] or 'Other',
            'shares_outstanding': float(info['shares_outstanding']),
            'current_revenue': revenues[0],
            'current_margin': margins[0] if margins else 0.1,
            'avg_margin': np.mean(margins) if margins else 0.1,
            'yoy_growth': yoy_growth if yoy_growth else 0.02,
            'net_debt': net_debt,
            'momentum': momentum,
            'periods': len(financials)
        }
    
    def calculate_dcf_v2(self, fundamentals: Dict, market_price: float) -> Dict:
        """Run simplified DCF v2 calculation"""
        
        # Extract data
        sector = fundamentals['sector']
        current_revenue = fundamentals['current_revenue']
        current_margin = fundamentals['current_margin']
        avg_margin = fundamentals['avg_margin']
        yoy_growth = fundamentals['yoy_growth']
        net_debt = fundamentals['net_debt']
        shares = fundamentals['shares_outstanding']
        momentum = fundamentals['momentum']
        
        # Constrain inputs
        yoy_growth = np.clip(yoy_growth, -0.2, 0.3)  # -20% to +30%
        current_margin = np.clip(current_margin, -0.1, 0.4)
        
        # Base WACC with sector adjustment
        base_wacc = 0.10  # 10% base
        sector_premium = self.sector_wacc_premium.get(sector, 0.02)
        wacc = base_wacc + sector_premium
        
        # Adjust WACC for momentum
        if momentum < -0.3:  # Falling knife
            wacc += 0.02
        elif momentum > 0.5:  # Too hot
            wacc += 0.01
        
        # Growth decay factor
        decay_factor = self.sector_growth_decay.get(sector, 0.8)
        
        # Project 5 years of cash flows
        fcfs = []
        revenue = current_revenue
        
        for year in range(5):
            # Decay growth towards long-term rate
            year_growth = yoy_growth * (decay_factor ** year)
            year_growth = max(year_growth, 0.02)  # Floor at 2%
            
            # Project revenue
            revenue *= (1 + year_growth)
            
            # Mean-reverting margin
            margin_reversion = 0.3
            year_margin = current_margin + (avg_margin - current_margin) * margin_reversion * (year + 1) / 5
            year_margin = np.clip(year_margin, 0, 0.3)
            
            # Operating income
            operating_income = revenue * year_margin
            
            # Simple FCF (70% conversion)
            fcf = operating_income * 0.78 * 0.7  # Tax and conversion
            fcfs.append(fcf)
        
        # Terminal value with conservative growth
        terminal_growth = 0.02  # 2% perpetual
        terminal_fcf = fcfs[-1] * (1 + terminal_growth)
        terminal_value = terminal_fcf / (wacc - terminal_growth)
        
        # Discount all cash flows
        pv_fcfs = sum(fcf / ((1 + wacc) ** (i+1)) for i, fcf in enumerate(fcfs))
        pv_terminal = terminal_value / ((1 + wacc) ** 5)
        
        # Enterprise and equity value
        enterprise_value = pv_fcfs + pv_terminal
        equity_value = enterprise_value - net_debt
        fair_value_per_share = equity_value / shares
        
        # Apply market sentiment discount
        if momentum < -0.3:
            fair_value_per_share *= 0.9  # 10% discount
        
        # Confidence based on data quality and sector
        base_confidence = min(fundamentals['periods'] / 8, 1.0)
        if sector in ['Internettjänster', 'Gaming & Spel']:
            base_confidence *= 0.7
        
        implied_return = (fair_value_per_share - market_price) / market_price
        
        return {
            'fair_value': fair_value_per_share,
            'implied_return': implied_return,
            'confidence': base_confidence,
            'wacc_used': wacc,
            'growth_used': yoy_growth,
            'margin_used': current_margin
        }
    
    async def value_company(self, symbol: str, valuation_date: datetime, market_price: float):
        """Value a company using DCF v2"""
        
        fundamentals = await self.get_company_fundamentals(symbol, valuation_date)
        
        if not fundamentals:
            return None
        
        result = self.calculate_dcf_v2(fundamentals, market_price)
        
        return {
            'symbol': symbol,
            'sector': fundamentals['sector'],
            'market_price': market_price,
            **result
        }

async def test_simple_dcf2():
    """Test the simplified DCF v2"""
    
    dcf = SimpleDCF2()
    await dcf.setup()
    
    test_cases = [
        ('VOLV-B', datetime(2024, 2, 1), 280.0),
        ('EOLU B', datetime(2024, 2, 15), 75.0),
        ('BUSER', datetime(2024, 2, 14), 0.97),
        ('AAK', datetime(2024, 2, 1), 170.0),
    ]
    
    print('🚀 SIMPLIFIED DCF v2.0 RESULTS')
    print('=' * 60)
    
    for symbol, date, price in test_cases:
        try:
            result = await dcf.value_company(symbol, date, price)
            
            if result:
                print(f'\n📊 {symbol}')
                print(f'   Sector: {result["sector"]}')
                print(f'   Market Price: {price:.2f}')
                print(f'   Fair Value: {result["fair_value"]:.2f}')
                print(f'   Implied Return: {result["implied_return"]:+.1%}')
                print(f'   Confidence: {result["confidence"]:.0%}')
                print(f'   WACC: {result["wacc_used"]:.1%}')
            else:
                print(f'\n❌ {symbol}: Valuation failed')
                
        except Exception as e:
            print(f'\n❌ {symbol}: Error - {e}')
    
    await dcf.cleanup()

if __name__ == "__main__":
    asyncio.run(test_simple_dcf2())