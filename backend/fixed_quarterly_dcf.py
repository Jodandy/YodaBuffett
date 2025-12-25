#!/usr/bin/env python3
"""
Fixed Quarterly DCF Engine

CORRECT handling of quarterly vs annual data:
- Annual reports: Use directly (already R12/TTM)
- Quarterly reports: Use for that quarter only, DON'T try to roll up to R12
- For DCF: Use annualized quarterly data (multiply by 4) for projections
"""

import asyncio
import asyncpg
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters, CompanyFinancials
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class CorrectQuarterlyDCF(DCFMonteCarloFixed):
    """DCF engine with CORRECT quarterly vs annual handling"""
    
    async def get_real_financials(self, symbol: str, valuation_date: datetime):
        """CORRECTLY handle quarterly vs annual data - NO fake R12 calculations"""
        
        # Get actual shares outstanding (same as parent)
        shares_query = """
        SELECT 
            date,
            market_cap,
            close_price,
            market_cap / NULLIF(close_price, 0) as shares_outstanding
        FROM historical_fundamentals_daily
        WHERE symbol = $1
        AND date <= $2
        AND market_cap > 0
        AND close_price > 0
        ORDER BY date DESC
        LIMIT 1
        """
        
        shares_record = await self.db_conn.fetchrow(shares_query, symbol, valuation_date.date())
        
        if not shares_record or not shares_record['shares_outstanding']:
            logger.warning(f"Cannot calculate shares outstanding for {symbol}")
            return None
            
        shares_outstanding = float(shares_record['shares_outstanding'])
        
        # CORRECT financial data query - treat quarterly and annual separately
        financials_query = """
        SELECT 
            period_date,
            fiscal_year,
            fiscal_quarter,
            total_revenue,
            operating_income,
            COALESCE(interest_expense, 0) as interest_expense,
            CASE 
                WHEN fiscal_quarter IS NOT NULL THEN 'QUARTERLY'
                ELSE 'ANNUAL'
            END as report_type,
            -- For quarterly: annualize by multiplying by 4
            -- For annual: use as is
            CASE 
                WHEN fiscal_quarter IS NOT NULL THEN total_revenue * 4
                ELSE total_revenue
            END as annualized_revenue,
            CASE 
                WHEN fiscal_quarter IS NOT NULL THEN operating_income * 4
                ELSE operating_income
            END as annualized_operating_income,
            CASE 
                WHEN fiscal_quarter IS NOT NULL THEN COALESCE(interest_expense, 0) * 4
                ELSE COALESCE(interest_expense, 0)
            END as annualized_interest_expense
        FROM financial_statements
        WHERE symbol = $1
        AND period_date <= $2
        AND period_date >= $2 - INTERVAL '6 years'
        AND total_revenue > 0
        AND operating_income IS NOT NULL
        ORDER BY period_date DESC
        LIMIT 12
        """
        
        financial_records = await self.db_conn.fetch(financials_query, symbol, valuation_date.date())
        
        if len(financial_records) < 2:
            logger.warning(f"Insufficient financial data for {symbol} on {valuation_date.date()}: {len(financial_records)} records")
            return None
        
        # Extract data correctly
        revenues = []
        operating_margins = []
        interest_expenses = []
        
        for record in financial_records:
            # Use annualized figures for consistency
            annualized_revenue = float(record['annualized_revenue'])
            annualized_operating_income = float(record['annualized_operating_income'])
            annualized_interest = float(record['annualized_interest_expense'])
            
            revenues.append(annualized_revenue)
            
            # Operating margin from annualized figures
            if annualized_revenue > 0:
                margin = annualized_operating_income / annualized_revenue
                operating_margins.append(margin)
            
            # Interest expense as ratio of revenue
            interest_expenses.append(annualized_interest)
            
            # Debug output for first few records
            if len(revenues) <= 3:
                report_type = record['report_type']
                quarter = record['fiscal_quarter'] or 'N/A'
                print(f"   {record['period_date']} ({report_type}, Q{quarter}): "
                      f"Rev {record['total_revenue']/1e6:.0f}M → {annualized_revenue/1e6:.0f}M annualized, "
                      f"Margin {margin*100:.1f}%")
        
        if len(revenues) < 2 or len(operating_margins) < 2:
            logger.warning(f"Insufficient valid data for {symbol}")
            return None
        
        return CompanyFinancials(
            symbol=symbol,
            revenues_ttm=revenues,  # These are now properly annualized
            operating_margins=operating_margins,
            interest_expenses=interest_expenses,
            free_cash_flows=[],  # Will calculate in simulation
            shares_outstanding=shares_outstanding,
            net_debt=0.0,
            beta=1.0
        )

async def test_quarterly_handling():
    """Test the corrected quarterly vs annual handling"""
    
    # Initialize corrected DCF engine
    params = DCFParameters(num_simulations=1000)
    engine = CorrectQuarterlyDCF(params)
    await engine.setup()
    
    test_cases = [
        ('AAK', '2022-06-01', 250.0),    # Early period - should work now
        ('AAK', '2023-09-01', 300.0),    # Mid period
        ('AAK', '2024-07-01', 310.0),    # Recent period
    ]
    
    print('🔧 TESTING CORRECTED QUARTERLY HANDLING')
    print('=' * 60)
    
    for symbol, date_str, price in test_cases:
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        print(f'\n📅 Testing {symbol} on {date_str}')
        
        try:
            financials = await engine.get_real_financials(symbol, test_date)
            
            if financials:
                print(f'✅ Financial data extracted:')
                print(f'   {len(financials.revenues_ttm)} revenue periods')
                print(f'   Latest annualized revenue: ${financials.revenues_ttm[0]/1e6:.0f}M')
                print(f'   Average margin: {sum(financials.operating_margins)/len(financials.operating_margins)*100:.1f}%')
                print(f'   Average interest expense: ${sum(financials.interest_expenses)/len(financials.interest_expenses)/1e6:.0f}M')
                
                # Try DCF calculation
                result = await engine.value_company(symbol, test_date, price)
                
                if result:
                    print(f'✅ DCF successful: Fair value ${result["fair_value_median"]:.2f} vs market ${price}')
                    rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                    print(f'   Rate sensitivity: {rate_sens:.1f}/10')
                else:
                    print(f'❌ DCF calculation failed')
            else:
                print(f'❌ Could not extract financial data')
                
        except Exception as e:
            print(f'❌ Error: {str(e)[:100]}...')
    
    await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(test_quarterly_handling())