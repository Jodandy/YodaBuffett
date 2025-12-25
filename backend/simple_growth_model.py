#!/usr/bin/env python3
"""
Simple Debt-Adjusted Topline Growth Model

Instead of DCF complexity, focus on:
1. Revenue growth (YoY)
2. Price-to-Sales ratio vs growth-adjusted fair value
3. Debt burden adjustment
4. Much simpler, faster, more intuitive
"""

import asyncio
import asyncpg
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleGrowthModel:
    """Simple growth-based valuation model"""
    
    def __init__(self):
        self.db_conn = None
        
        # Model parameters
        self.base_ps_multiple = 1.5  # Base P/S for 0% growth
        self.growth_multiplier = 0.15  # Extra P/S per 1% growth
        self.debt_penalty = 0.3  # Penalty factor for debt
        self.min_buy_discount = 0.20  # Need 20% discount to buy
    
    async def setup(self):
        """Initialize database connection"""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    async def cleanup(self):
        """Close database connection"""
        if self.db_conn:
            await self.db_conn.close()
    
    async def get_company_metrics(self, symbol: str, analysis_date: datetime) -> Optional[Dict]:
        """Get key metrics for growth model"""
        
        # Get current fundamentals
        fundamentals_query = """
        SELECT 
            revenue_per_share,
            market_cap,
            pe_ratio,
            ps_ratio as price_to_sales,
            debt_to_equity,
            close_price,
            date as metric_date
        FROM historical_fundamentals_daily
        WHERE symbol = $1
        AND date <= $2
        ORDER BY date DESC
        LIMIT 1
        """
        
        current = await self.db_conn.fetchrow(fundamentals_query, symbol, analysis_date.date())
        
        if not current or not current['revenue_per_share']:
            return None
        
        # Get year-ago fundamentals for growth calculation
        year_ago_query = """
        SELECT revenue_per_share
        FROM historical_fundamentals_daily
        WHERE symbol = $1
        AND date <= $2
        AND date >= $3
        ORDER BY date DESC
        LIMIT 1
        """
        
        year_ago_date = analysis_date - timedelta(days=365)
        target_date = year_ago_date - timedelta(days=30)
        
        year_ago = await self.db_conn.fetchrow(
            year_ago_query, 
            symbol, 
            year_ago_date.date(), 
            target_date.date()
        )
        
        if not year_ago or not year_ago['revenue_per_share']:
            return None
        
        # Calculate year-over-year growth
        current_revenue = float(current['revenue_per_share'])
        year_ago_revenue = float(year_ago['revenue_per_share'])
        
        if year_ago_revenue <= 0:
            return None
        
        revenue_growth = (current_revenue - year_ago_revenue) / year_ago_revenue
        
        # Use price from fundamentals data
        current_price = float(current['close_price']) if current['close_price'] else None
        
        if not current_price:
            return None
        
        return {
            'symbol': symbol,
            'analysis_date': analysis_date,
            'current_price': current_price,
            'revenue_growth': revenue_growth,
            'current_ps': float(current['price_to_sales']) if current['price_to_sales'] else None,
            'debt_to_equity': float(current['debt_to_equity']) if current['debt_to_equity'] else 0,
            'market_cap': float(current['market_cap']) if current['market_cap'] else 0,
            'current_revenue': current_revenue
        }
    
    def calculate_fair_ps_multiple(self, revenue_growth: float, debt_to_equity: float) -> float:
        """Calculate fair P/S multiple based on growth and debt"""
        
        # Base multiple + growth premium
        growth_pct = revenue_growth * 100  # Convert to percentage
        fair_ps = self.base_ps_multiple + (growth_pct * self.growth_multiplier)
        
        # Apply debt penalty
        debt_adjustment = 1 - (self.debt_penalty * min(debt_to_equity, 1.0))  # Cap at 100% equity
        fair_ps *= debt_adjustment
        
        # Floor at 0.5x sales
        fair_ps = max(fair_ps, 0.5)
        
        return fair_ps
    
    def generate_signal(self, metrics: Dict) -> Dict:
        """Generate buy/hold/sell signal"""
        
        revenue_growth = metrics['revenue_growth']
        current_ps = metrics['current_ps']
        debt_to_equity = metrics['debt_to_equity']
        
        # Skip if no P/S ratio
        if current_ps is None or current_ps <= 0:
            return {
                'signal': 'NO_DATA',
                'fair_ps': None,
                'discount': None,
                'reason': 'Missing P/S ratio'
            }
        
        # Calculate fair P/S
        fair_ps = self.calculate_fair_ps_multiple(revenue_growth, debt_to_equity)
        
        # Calculate discount/premium
        discount = (fair_ps - current_ps) / fair_ps
        
        # Generate signal
        if discount >= self.min_buy_discount:
            signal = 'BUY'
            reason = f'{discount:.1%} undervalued vs growth-adjusted fair value'
        elif discount <= -0.1:  # 10% overvalued
            signal = 'SELL'
            reason = f'{abs(discount):.1%} overvalued'
        else:
            signal = 'HOLD'
            reason = 'Fairly valued'
        
        return {
            'signal': signal,
            'fair_ps': fair_ps,
            'current_ps': current_ps,
            'discount': discount,
            'revenue_growth': revenue_growth,
            'debt_to_equity': debt_to_equity,
            'reason': reason
        }
    
    async def analyze_company(self, symbol: str, analysis_date: datetime) -> Optional[Dict]:
        """Complete analysis of a company"""
        
        metrics = await self.get_company_metrics(symbol, analysis_date)
        
        if not metrics:
            return None
        
        signal_data = self.generate_signal(metrics)
        
        return {
            **metrics,
            **signal_data
        }
    
    async def screen_universe(self, analysis_date: datetime, signal_filter: str = 'BUY') -> List[Dict]:
        """Screen entire universe for signals"""
        
        # Get all active companies
        companies_query = """
        SELECT DISTINCT symbol 
        FROM historical_fundamentals_daily 
        WHERE date <= $1
        AND date >= $1 - INTERVAL '30 days'
        AND revenue_per_share > 0
        ORDER BY symbol
        """
        
        companies = await self.db_conn.fetch(companies_query, analysis_date.date())
        
        logger.info(f"Screening {len(companies)} companies for {signal_filter} signals...")
        
        results = []
        
        for company in companies:
            symbol = company['symbol']
            
            try:
                analysis = await self.analyze_company(symbol, analysis_date)
                
                if analysis and analysis['signal'] == signal_filter:
                    results.append(analysis)
            
            except Exception as e:
                logger.debug(f"Error analyzing {symbol}: {e}")
        
        # Sort by discount (best opportunities first)
        if signal_filter == 'BUY':
            results.sort(key=lambda x: x['discount'], reverse=True)
        
        return results

async def test_simple_growth_model():
    """Test the simple growth model"""
    
    model = SimpleGrowthModel()
    await model.setup()
    
    try:
        print("🚀 SIMPLE DEBT-ADJUSTED GROWTH MODEL")
        print("=" * 60)
        
        # Test on specific companies
        test_date = datetime(2024, 12, 1)
        test_companies = ['VOLV-B', 'EOLU B', 'AAK', 'ERIC-B', 'BUSER']
        
        print(f"\n📊 INDIVIDUAL COMPANY ANALYSIS ({test_date.date()}):")
        print(f"{'Symbol':<10} {'Growth':<8} {'Current PS':<10} {'Fair PS':<8} {'Discount':<9} {'Signal':<6}")
        print("-" * 70)
        
        for symbol in test_companies:
            analysis = await model.analyze_company(symbol, test_date)
            
            if analysis:
                growth_pct = analysis['revenue_growth'] * 100
                signal = analysis['signal']
                signal_color = '🟢' if signal == 'BUY' else '🔴' if signal == 'SELL' else '🟡'
                
                print(f"{symbol:<10} {growth_pct:+6.1f}% {analysis['current_ps']:<10.2f} "
                      f"{analysis['fair_ps']:<8.2f} {analysis['discount']:+7.1%} "
                      f"{signal_color} {signal}")
            else:
                print(f"{symbol:<10} No data available")
        
        # Screen for BUY opportunities
        print(f"\n🔍 SCREENING FOR BUY OPPORTUNITIES:")
        
        buy_signals = await model.screen_universe(test_date, 'BUY')
        
        if buy_signals:
            print(f"\nFound {len(buy_signals)} BUY signals:")
            print(f"{'Symbol':<10} {'Growth':<8} {'Discount':<9} {'Fair PS':<8} {'Debt/Eq':<8} {'Reason'}")
            print("-" * 80)
            
            for signal in buy_signals[:10]:  # Top 10
                growth_pct = signal['revenue_growth'] * 100
                print(f"{signal['symbol']:<10} {growth_pct:+6.1f}% {signal['discount']:+7.1%} "
                      f"{signal['fair_ps']:<8.2f} {signal['debt_to_equity']:<8.1f} "
                      f"{signal['reason'][:30]}")
        else:
            print("No BUY signals found")
        
        # Calculate model statistics
        print(f"\n📈 MODEL PARAMETERS:")
        print(f"   Base P/S Multiple: {model.base_ps_multiple}x")
        print(f"   Growth Multiplier: {model.growth_multiplier}x per 1% growth")
        print(f"   Debt Penalty: {model.debt_penalty}x")
        print(f"   Min Buy Discount: {model.min_buy_discount:.0%}")
        
        print(f"\n💡 EXAMPLE CALCULATIONS:")
        print(f"   0% growth, no debt: {model.calculate_fair_ps_multiple(0.0, 0.0):.1f}x P/S")
        print(f"   10% growth, no debt: {model.calculate_fair_ps_multiple(0.10, 0.0):.1f}x P/S")
        print(f"   10% growth, 50% debt/equity: {model.calculate_fair_ps_multiple(0.10, 0.5):.1f}x P/S")
        print(f"   20% growth, 100% debt/equity: {model.calculate_fair_ps_multiple(0.20, 1.0):.1f}x P/S")
    
    finally:
        await model.cleanup()

if __name__ == "__main__":
    asyncio.run(test_simple_growth_model())