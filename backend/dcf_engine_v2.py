#!/usr/bin/env python3
"""
DCF Engine v2.0 - Market-Aware Edition

Key improvements over v1.0:
1. Sector-specific parameters
2. Market cycle awareness
3. Mean reversion in growth rates
4. Better uncertainty modeling
5. Reality-based constraints
"""

import asyncio
import asyncpg
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

@dataclass
class DCFConfig2:
    """DCF 2.0 Configuration with market awareness"""
    
    # Simulation settings
    num_simulations: int = 5000
    projection_years: int = 5  # Shorter horizon
    
    # Market parameters (will be adjusted by sector and market conditions)
    base_risk_free_rate: float = 0.03
    base_market_premium: float = 0.08  # More conservative
    
    # Terminal growth - sector specific
    terminal_growth_by_sector = {
        'Vindkraft': 0.025,  # Wind power - stable
        'Internettjänster': 0.015,  # Internet services - lower
        'Gaming & Spel': 0.01,  # Gaming - very low
        'default': 0.02  # GDP-like growth
    }
    
    # Sector risk adjustments
    sector_beta = {
        'Vindkraft': 0.8,  # Lower risk
        'Internettjänster': 1.5,  # Higher risk
        'Gaming & Spel': 1.8,  # Very high risk
        'Affärs- & IT-System': 1.2,
        'default': 1.0
    }
    
    # Mean reversion parameters
    revenue_growth_mean_reversion: float = 0.5  # Pull towards mean
    margin_mean_reversion: float = 0.3  # Pull towards industry avg
    
    # Reality constraints
    max_revenue_growth: float = 0.3  # 30% max
    min_revenue_growth: float = -0.2  # -20% min
    max_operating_margin: float = 0.3  # 30% max
    min_operating_margin: float = -0.1  # -10% min

@dataclass 
class MarketContext:
    """Market context for valuation adjustments"""
    sector: str
    market_cap: float
    pe_ratio: Optional[float]
    industry_avg_margin: Optional[float]
    volatility_90d: Optional[float]
    recent_momentum: Optional[float]  # 90-day price change

class DCFEngine2:
    """DCF Engine v2.0 with market awareness"""
    
    def __init__(self, config: DCFConfig2):
        self.config = config
        self.db_conn = None
        
        # Exchange rates (same as v1)
        self.exchange_rates = {
            ('USD', 'SEK'): 11.0, ('EUR', 'SEK'): 12.0, ('CHF', 'SEK'): 12.5,
            ('DKK', 'SEK'): 1.6, ('NOK', 'SEK'): 1.0, ('USD', 'NOK'): 11.0,
            ('EUR', 'NOK'): 12.0, ('DKK', 'NOK'): 1.5, ('USD', 'DKK'): 7.0,
        }
    
    async def setup(self):
        """Initialize database connection"""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    async def cleanup(self):
        """Close database connection"""
        if self.db_conn:
            await self.db_conn.close()
    
    def get_exchange_rate(self, from_currency: str, to_currency: str) -> float:
        """Get exchange rate between currencies"""
        if from_currency == to_currency:
            return 1.0
        
        key = (from_currency, to_currency)
        if key in self.exchange_rates:
            return self.exchange_rates[key]
        
        inverse_key = (to_currency, from_currency)
        if inverse_key in self.exchange_rates:
            return 1.0 / self.exchange_rates[inverse_key]
        
        return 1.0
    
    async def get_market_context(self, symbol: str, valuation_date: datetime) -> MarketContext:
        """Get market context for valuation adjustments"""
        
        # Get basic company info
        company_query = """
        SELECT 
            cm.sector,
            cm.report_currency
        FROM company_master cm
        WHERE cm.primary_ticker = $1
        """
        
        company_info = await self.db_conn.fetchrow(company_query, symbol)
        
        if not company_info:
            return MarketContext(
                sector='default',
                market_cap=0,
                pe_ratio=None,
                industry_avg_margin=None,
                volatility_90d=None,
                recent_momentum=None
            )
        
        sector = company_info['sector'] or 'default'
        market_cap = 0  # We'll calculate from shares * price later if needed
        
        # Get recent price momentum and volatility
        price_query = """
        SELECT 
            (SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date <= $2 ORDER BY date DESC LIMIT 1) as current_price,
            (SELECT close_price FROM daily_price_data WHERE symbol = $1 AND date <= $2 - INTERVAL '90 days' ORDER BY date DESC LIMIT 1) as price_90d_ago,
            (SELECT STDDEV(close_price) FROM daily_price_data WHERE symbol = $1 AND date BETWEEN $2 - INTERVAL '90 days' AND $2) as volatility_90d
        """
        
        price_info = await self.db_conn.fetchrow(price_query, symbol, valuation_date.date())
        
        recent_momentum = None
        volatility_90d = None
        
        if price_info and price_info['current_price'] and price_info['price_90d_ago']:
            recent_momentum = (float(price_info['current_price']) - float(price_info['price_90d_ago'])) / float(price_info['price_90d_ago'])
            
        if price_info and price_info['volatility_90d']:
            volatility_90d = float(price_info['volatility_90d'])
        
        # Get industry average margin
        industry_margin_query = """
        SELECT AVG(operating_income::numeric / NULLIF(total_revenue::numeric, 0)) as avg_margin
        FROM financial_statements
        WHERE symbol IN (SELECT primary_ticker FROM company_master WHERE sector = $1)
        AND publish_date <= $2
        AND publish_date >= $2 - INTERVAL '1 year'
        AND total_revenue > 0
        """
        
        margin_result = await self.db_conn.fetchrow(industry_margin_query, sector, valuation_date.date())
        industry_avg_margin = float(margin_result['avg_margin']) if margin_result and margin_result['avg_margin'] else None
        
        return MarketContext(
            sector=sector,
            market_cap=market_cap,
            pe_ratio=None,  # Could calculate if needed
            industry_avg_margin=industry_avg_margin,
            volatility_90d=volatility_90d,
            recent_momentum=recent_momentum
        )
    
    async def extract_company_data_v2(self, symbol: str, valuation_date: datetime):
        """Extract company data with focus on recent trends"""
        
        # Get currency information
        currency_query = """
        SELECT stock_currency, report_currency
        FROM company_master
        WHERE primary_ticker = $1
        """
        
        currency_info = await self.db_conn.fetchrow(currency_query, symbol)
        if not currency_info:
            return None
        
        stock_currency = currency_info['stock_currency'] or 'SEK'
        report_currency = currency_info['report_currency'] or 'SEK'
        
        # Get shares outstanding
        shares_query = """
        SELECT shares_outstanding
        FROM balance_sheet_data
        WHERE symbol = $1
        AND publish_date <= $2
        AND shares_outstanding > 0
        ORDER BY publish_date DESC
        LIMIT 1
        """
        
        shares_record = await self.db_conn.fetchrow(shares_query, symbol, valuation_date.date())
        if not shares_record:
            return None
        
        shares_outstanding = float(shares_record['shares_outstanding'])
        
        # Get RECENT financial data (last 2 years only for more relevance)
        financial_query = """
        SELECT 
            period_date,
            publish_date,
            total_revenue,
            operating_income,
            COALESCE(interest_expense, 0) as interest_expense,
            COALESCE(tax_expense, 0) as tax_expense,
            CASE 
                WHEN EXTRACT(MONTH FROM period_date) IN (12, 1) 
                     AND EXTRACT(DAY FROM period_date) IN (28, 29, 30, 31, 1, 2, 3)
                THEN 'annual'
                ELSE 'quarterly'
            END as period_type
        FROM financial_statements
        WHERE symbol = $1
        AND publish_date <= $2
        AND publish_date >= $2 - INTERVAL '2 years'  -- Only recent data
        AND total_revenue > 0
        ORDER BY publish_date DESC
        LIMIT 8  -- Max 8 periods
        """
        
        financial_data = await self.db_conn.fetch(financial_query, symbol, valuation_date.date())
        
        if len(financial_data) < 3:  # Need at least 3 periods
            return None
        
        # Process into time series
        revenues = [float(row['total_revenue']) for row in financial_data]
        operating_incomes = [float(row['operating_income']) for row in financial_data]
        
        # Calculate trailing growth rates with decay weighting
        growth_rates = []
        for i in range(1, min(4, len(revenues))):  # Only use last 3 growth periods
            if revenues[i] > 0:
                growth = (revenues[i-1] - revenues[i]) / revenues[i]
                # Weight recent growth more heavily
                weight = 1.0 / (i ** 0.5)  # Square root decay
                growth_rates.append((growth, weight))
        
        # Weighted average growth
        if growth_rates:
            total_weight = sum(w for _, w in growth_rates)
            weighted_growth = sum(g * w for g, w in growth_rates) / total_weight
        else:
            weighted_growth = 0.0
        
        # Calculate margins
        margins = []
        for i, revenue in enumerate(revenues[:4]):  # Recent margins only
            if revenue > 0:
                margin = operating_incomes[i] / revenue
                margins.append(margin)
        
        current_margin = margins[0] if margins else 0.0
        avg_margin = np.mean(margins) if margins else 0.0
        
        # Get balance sheet data
        balance_query = """
        SELECT
            COALESCE(total_debt, 0) as total_debt,
            COALESCE(cash_and_equivalents, 0) as cash_equivalents
        FROM balance_sheet_data
        WHERE symbol = $1
        AND publish_date <= $2
        ORDER BY publish_date DESC
        LIMIT 1
        """
        
        balance_data = await self.db_conn.fetchrow(balance_query, symbol, valuation_date.date())
        
        if balance_data:
            total_debt = float(balance_data['total_debt'])
            cash = float(balance_data['cash_equivalents'])
            net_debt = total_debt - cash
        else:
            net_debt = 0.0
        
        return {
            'symbol': symbol,
            'shares_outstanding': shares_outstanding,
            'stock_currency': stock_currency,
            'report_currency': report_currency,
            'current_revenue': revenues[0],
            'current_margin': current_margin,
            'avg_margin': avg_margin,
            'weighted_growth': weighted_growth,
            'net_debt': net_debt,
            'periods_available': len(financial_data)
        }
    
    def calculate_sector_wacc(self, sector: str, base_wacc: float, market_context: MarketContext) -> float:
        """Calculate sector-adjusted WACC"""
        
        # Get sector beta
        beta = self.config.sector_beta.get(sector, self.config.sector_beta['default'])
        
        # Base WACC calculation
        risk_free = self.config.base_risk_free_rate
        market_premium = self.config.base_market_premium
        
        # Cost of equity with sector beta
        cost_of_equity = risk_free + beta * market_premium
        
        # Adjust for market conditions
        if market_context.recent_momentum is not None:
            # If stock has been falling, increase discount rate
            if market_context.recent_momentum < -0.2:
                cost_of_equity += 0.02
            elif market_context.recent_momentum > 0.5:
                cost_of_equity += 0.01  # Momentum stocks also riskier
        
        # Add volatility premium
        if market_context.volatility_90d is not None and market_context.volatility_90d > 50:
            cost_of_equity += 0.01
        
        return cost_of_equity
    
    def run_monte_carlo_v2(self, company_data: Dict, market_context: MarketContext) -> Optional[Dict]:
        """Run improved Monte Carlo simulation"""
        
        current_revenue = company_data['current_revenue']
        current_margin = company_data['current_margin']
        avg_margin = company_data['avg_margin']
        weighted_growth = company_data['weighted_growth']
        net_debt = company_data['net_debt']
        
        # Sector-specific parameters
        sector = market_context.sector
        terminal_growth = self.config.terminal_growth_by_sector.get(
            sector, self.config.terminal_growth_by_sector['default']
        )
        
        # Calculate WACC with market awareness
        wacc = self.calculate_sector_wacc(sector, self.config.base_risk_free_rate, market_context)
        
        # Mean reversion targets
        if market_context.industry_avg_margin is not None:
            margin_target = market_context.industry_avg_margin
        else:
            margin_target = avg_margin
        
        # Growth mean reversion
        long_term_growth = 0.03  # Long-term GDP growth
        
        # Monte Carlo simulations
        fair_values = []
        
        for _ in range(self.config.num_simulations):
            
            # Model uncertainty in growth with mean reversion
            growth_volatility = min(0.2, abs(weighted_growth) * 0.5)  # Less volatile
            
            # Mean reverting growth
            year_growths = []
            prev_growth = weighted_growth
            
            for year in range(self.config.projection_years):
                # Pull towards long-term mean
                mean_reversion_pull = (long_term_growth - prev_growth) * self.config.revenue_growth_mean_reversion
                
                # Add random shock
                growth_shock = np.random.normal(0, growth_volatility)
                
                # Calculate growth for this year
                year_growth = prev_growth + mean_reversion_pull + growth_shock
                
                # Apply constraints
                year_growth = np.clip(
                    year_growth, 
                    self.config.min_revenue_growth, 
                    self.config.max_revenue_growth
                )
                
                year_growths.append(year_growth)
                prev_growth = year_growth
            
            # Mean reverting margins
            projected_margins = []
            prev_margin = current_margin
            
            for year in range(self.config.projection_years):
                # Pull towards industry average
                margin_pull = (margin_target - prev_margin) * self.config.margin_mean_reversion
                
                # Add uncertainty
                margin_shock = np.random.normal(0, 0.02)  # 2% volatility
                
                # Calculate margin
                year_margin = prev_margin + margin_pull + margin_shock
                
                # Apply constraints
                year_margin = np.clip(
                    year_margin,
                    self.config.min_operating_margin,
                    self.config.max_operating_margin
                )
                
                projected_margins.append(year_margin)
                prev_margin = year_margin
            
            # Project cash flows
            revenue = current_revenue
            fcfs = []
            
            for i in range(self.config.projection_years):
                # Apply growth
                revenue *= (1 + year_growths[i])
                
                # Operating income
                operating_income = revenue * projected_margins[i]
                
                # Simple tax assumption
                nopat = operating_income * 0.78  # 22% tax
                
                # Conservative FCF conversion
                fcf = nopat * 0.7  # Assume 30% reinvestment
                
                fcfs.append(fcf)
            
            # Terminal value with sector-specific growth
            terminal_fcf = fcfs[-1] * (1 + terminal_growth)
            terminal_value = terminal_fcf / (wacc - terminal_growth)
            
            # Discount everything
            pv_fcfs = [fcf / ((1 + wacc) ** (i+1)) for i, fcf in enumerate(fcfs)]
            pv_terminal = terminal_value / ((1 + wacc) ** self.config.projection_years)
            
            # Enterprise value
            enterprise_value = sum(pv_fcfs) + pv_terminal
            
            # Equity value
            equity_value = enterprise_value - net_debt
            
            # Per share
            fair_value_per_share = equity_value / company_data['shares_outstanding']
            
            # Only keep positive values
            if fair_value_per_share > 0:
                fair_values.append(fair_value_per_share)
        
        if len(fair_values) < self.config.num_simulations * 0.5:
            return None  # Too many failed simulations
        
        fair_values = np.array(fair_values)
        
        # Apply market sentiment adjustment
        if market_context.recent_momentum is not None:
            if market_context.recent_momentum < -0.3:
                # Market hates this stock - apply discount
                fair_values *= 0.9
            elif market_context.recent_momentum > 0.5:
                # Momentum premium (but be careful)
                fair_values *= 1.05
        
        return {
            'fair_value_mean': float(np.mean(fair_values)),
            'fair_value_median': float(np.median(fair_values)),
            'fair_value_std': float(np.std(fair_values)),
            'fair_value_p25': float(np.percentile(fair_values, 25)),
            'fair_value_p75': float(np.percentile(fair_values, 75)),
            'wacc_used': wacc,
            'terminal_growth_used': terminal_growth,
            'current_margin': current_margin,
            'target_margin': margin_target,
            'weighted_growth': weighted_growth
        }
    
    async def value_company_v2(self, symbol: str, valuation_date: datetime, market_price: float) -> Optional[Dict]:
        """Complete DCF 2.0 valuation"""
        
        # Get market context
        market_context = await self.get_market_context(symbol, valuation_date)
        
        # Extract company data
        company_data = await self.extract_company_data_v2(symbol, valuation_date)
        
        if not company_data:
            return None
        
        # Run improved Monte Carlo
        dcf_results = self.run_monte_carlo_v2(company_data, market_context)
        
        if not dcf_results:
            return None
        
        # Apply currency conversion
        exchange_rate = self.get_exchange_rate(
            company_data['report_currency'], 
            company_data['stock_currency']
        )
        
        # Convert fair values
        for field in ['fair_value_mean', 'fair_value_median', 'fair_value_std', 'fair_value_p25', 'fair_value_p75']:
            dcf_results[field] *= exchange_rate
        
        # Calculate confidence with market awareness
        fair_value = dcf_results['fair_value_median']
        
        # Base confidence on data quality
        confidence = min(1.0, company_data['periods_available'] / 8.0)
        
        # Adjust for volatility
        if market_context.volatility_90d:
            if market_context.volatility_90d > 100:
                confidence *= 0.7
            elif market_context.volatility_90d > 50:
                confidence *= 0.85
        
        # Adjust for sector
        if market_context.sector in ['Gaming & Spel', 'Internettjänster']:
            confidence *= 0.8  # Less confident in volatile sectors
        
        # Calculate signal
        implied_return = (fair_value - market_price) / market_price
        
        return {
            'symbol': symbol,
            'valuation_date': valuation_date,
            'market_price': market_price,
            'fair_value_median': fair_value,
            'implied_return': implied_return,
            'confidence': confidence,
            'sector': market_context.sector,
            'model_version': 'dcf_v2.0',
            **dcf_results
        }

async def test_dcf_v2():
    """Test DCF 2.0 on sample companies"""
    
    config = DCFConfig2(num_simulations=1000)  # Faster for testing
    engine = DCFEngine2(config)
    await engine.setup()
    
    # Test on companies that failed in v1
    test_cases = [
        ('BUSER', datetime(2024, 2, 14), 0.97),  # Failed in v1
        ('EOLU B', datetime(2024, 2, 15), 75.0),  # Mixed results
        ('VOLV-B', datetime(2024, 12, 8), 280.0),  # Large cap
    ]
    
    print('🚀 DCF 2.0 TEST RESULTS')
    print('=' * 70)
    
    for symbol, date, price in test_cases:
        print(f'\n📊 {symbol} @ {date.date()}')
        
        try:
            result = await engine.value_company_v2(symbol, date, price)
            
            if result:
                print(f'   Market Price: {price:.2f}')
                print(f'   Fair Value: {result["fair_value_median"]:.2f}')
                print(f'   Implied Return: {result["implied_return"]:+.1%}')
                print(f'   Confidence: {result["confidence"]:.1%}')
                print(f'   Sector: {result["sector"]}')
                print(f'   WACC: {result["wacc_used"]:.1%}')
            else:
                print('   ❌ Valuation failed')
                
        except Exception as e:
            print(f'   ❌ Error: {e}')
    
    await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(test_dcf_v2())