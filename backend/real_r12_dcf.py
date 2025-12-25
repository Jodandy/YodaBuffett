#!/usr/bin/env python3
"""
Real R12 DCF Engine

PROPER R12 calculation using actual quarterly results:
- Find last 4 quarters of actual quarterly data
- Handle that Q4 doesn't exist as separate quarterly (annual replaces it)
- Extract Q4 = Annual - (Q1 + Q2 + Q3) when needed
- Build real trailing twelve months, not fake quarterly×4
"""

import asyncio
import asyncpg
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters, CompanyFinancials
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class RealR12DCF(DCFMonteCarloFixed):
    """DCF engine with REAL R12 calculations from actual quarterly data"""
    
    async def get_real_financials(self, symbol: str, valuation_date: datetime):
        """Calculate REAL R12 from actual quarterly results"""
        
        # Get actual shares outstanding
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
        
        # Get all financial statements for R12 calculation
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
            END as report_type
        FROM financial_statements
        WHERE symbol = $1
        AND period_date <= $2
        AND period_date >= $2 - INTERVAL '8 years'  -- Look back far enough
        AND total_revenue > 0
        AND operating_income IS NOT NULL
        ORDER BY period_date DESC
        """
        
        all_records = await self.db_conn.fetch(financials_query, symbol, valuation_date.date())
        
        if len(all_records) < 2:
            logger.warning(f"Insufficient financial data for {symbol}")
            return None
        
        # Build R12 calculations for multiple periods
        r12_periods = []
        
        # Try to build R12 for several recent periods to get a historical pattern
        for period_start_idx in range(0, min(8, len(all_records))):
            
            r12_result = await self._calculate_r12_for_period(
                all_records, period_start_idx, symbol, valuation_date
            )
            
            if r12_result:
                r12_periods.append(r12_result)
                
                # Debug output for first few
                if len(r12_periods) <= 3:
                    print(f"   R12 Period {len(r12_periods)}: ${r12_result['revenue']/1e6:.0f}M revenue, "
                          f"{r12_result['margin']*100:.1f}% margin, ${r12_result['interest']/1e6:.0f}M interest")
                    print(f"     Quarters used: {r12_result['quarters_info']}")
            
            # We need at least 3 R12 periods for analysis
            if len(r12_periods) >= 5:
                break
        
        if len(r12_periods) < 2:
            logger.warning(f"Could not calculate sufficient R12 periods for {symbol}")
            return None
        
        # Extract data for DCF analysis
        revenues = [p['revenue'] for p in r12_periods]
        operating_margins = [p['margin'] for p in r12_periods] 
        interest_expenses = [p['interest'] for p in r12_periods]
        
        return CompanyFinancials(
            symbol=symbol,
            revenues_ttm=revenues,  # These are now REAL R12 figures
            operating_margins=operating_margins,
            interest_expenses=interest_expenses,
            free_cash_flows=[],
            shares_outstanding=shares_outstanding,
            net_debt=0.0,
            beta=1.0
        )
    
    async def _calculate_r12_for_period(self, all_records, start_idx, symbol, valuation_date):
        """Calculate R12 for a specific period using actual quarterly data"""
        
        # Find a reference point (annual or latest quarterly)
        ref_record = all_records[start_idx]
        ref_date = ref_record['period_date']
        ref_year = ref_record['fiscal_year']
        
        if ref_record['report_type'] == 'ANNUAL':
            # Annual report gives us perfect R12
            return {
                'period_end': ref_date,
                'revenue': float(ref_record['total_revenue']),
                'operating_income': float(ref_record['operating_income']),
                'interest': float(ref_record['interest_expense']),
                'margin': float(ref_record['operating_income']) / float(ref_record['total_revenue']),
                'quarters_info': f"Annual {ref_year}"
            }
        
        else:
            # Quarterly report - need to build R12 from last 4 quarters
            return await self._build_r12_from_quarters(all_records, start_idx, ref_date, ref_year)
    
    async def _build_r12_from_quarters(self, all_records, start_idx, ref_date, ref_year):
        """Build R12 from actual quarterly data"""
        
        # We need to find the last 4 quarters before this point
        quarters_needed = 4
        quarters_found = []
        
        # Look for quarters in the records
        for i in range(start_idx, len(all_records)):
            record = all_records[i]
            
            if record['report_type'] == 'QUARTERLY':
                quarters_found.append({
                    'date': record['period_date'],
                    'year': record['fiscal_year'], 
                    'quarter': record['fiscal_quarter'],
                    'revenue': float(record['total_revenue']),
                    'operating_income': float(record['operating_income']),
                    'interest': float(record['interest_expense'])
                })
                
                if len(quarters_found) >= quarters_needed:
                    break
        
        # Also look for annual reports that can give us Q4 data
        annual_records = [r for r in all_records[start_idx:] if r['report_type'] == 'ANNUAL']
        
        # Try to extract Q4 from annual reports if we're missing quarters
        if len(quarters_found) < 4 and annual_records:
            
            # Find annual report for the year we need
            relevant_annual = None
            target_year = ref_year
            
            for annual in annual_records:
                if annual['fiscal_year'] == target_year or annual['fiscal_year'] == target_year - 1:
                    relevant_annual = annual
                    break
            
            if relevant_annual:
                # Try to extract Q4 by finding Q1+Q2+Q3 for that year
                year_quarters = [q for q in quarters_found if q['year'] == relevant_annual['fiscal_year']]
                
                if len(year_quarters) == 3:  # We have Q1, Q2, Q3
                    q123_revenue = sum(q['revenue'] for q in year_quarters)
                    q123_operating = sum(q['operating_income'] for q in year_quarters)
                    q123_interest = sum(q['interest'] for q in year_quarters)
                    
                    annual_revenue = float(relevant_annual['total_revenue'])
                    annual_operating = float(relevant_annual['operating_income'])
                    annual_interest = float(relevant_annual['interest_expense'])
                    
                    # Extract Q4 = Annual - (Q1+Q2+Q3)
                    q4_revenue = annual_revenue - q123_revenue
                    q4_operating = annual_operating - q123_operating
                    q4_interest = annual_interest - q123_interest
                    
                    if q4_revenue > 0:  # Sanity check
                        quarters_found.append({
                            'date': relevant_annual['period_date'],
                            'year': relevant_annual['fiscal_year'],
                            'quarter': 4,
                            'revenue': q4_revenue,
                            'operating_income': q4_operating,
                            'interest': q4_interest
                        })
        
        # If we still don't have 4 quarters, we can't build reliable R12
        if len(quarters_found) < 4:
            return None
        
        # Sort quarters by date and take the most recent 4
        quarters_found.sort(key=lambda x: x['date'], reverse=True)
        last_4_quarters = quarters_found[:4]
        
        # Sum up the R12
        total_revenue = sum(q['revenue'] for q in last_4_quarters)
        total_operating = sum(q['operating_income'] for q in last_4_quarters)
        total_interest = sum(q['interest'] for q in last_4_quarters)
        
        margin = total_operating / total_revenue if total_revenue > 0 else 0
        
        # Create quarters info string
        quarter_info = ", ".join([f"Q{q['quarter']}/{q['year']}" for q in reversed(last_4_quarters)])
        
        return {
            'period_end': ref_date,
            'revenue': total_revenue,
            'operating_income': total_operating, 
            'interest': total_interest,
            'margin': margin,
            'quarters_info': quarter_info
        }

async def test_real_r12():
    """Test real R12 calculations vs simple quarterly×4"""
    
    # Initialize real R12 DCF engine
    params = DCFParameters(num_simulations=1000)
    engine = RealR12DCF(params)
    await engine.setup()
    
    test_cases = [
        ('AAK', '2024-10-01', 300.0),    # Should have good quarterly data
        ('SOBI', '2024-07-01', 280.0),   # Pharma - likely seasonal
        ('VOLV B', '2023-09-01', 250.0), # Industrial - seasonal patterns
    ]
    
    print('🔧 TESTING REAL R12 CALCULATIONS')
    print('=' * 70)
    
    for symbol, date_str, price in test_cases:
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        print(f'\n📅 Testing {symbol} on {date_str}')
        
        try:
            financials = await engine.get_real_financials(symbol, test_date)
            
            if financials:
                print(f'✅ R12 calculations successful:')
                print(f'   {len(financials.revenues_ttm)} R12 periods calculated')
                print(f'   Latest R12 revenue: ${financials.revenues_ttm[0]/1e6:.0f}M')
                print(f'   R12 revenue pattern: {[f"${r/1e6:.0f}M" for r in financials.revenues_ttm[:3]]}')
                print(f'   R12 margin pattern: {[f"{m*100:.1f}%" for m in financials.operating_margins[:3]]}')
                
                # Try DCF calculation
                result = await engine.value_company(symbol, test_date, price)
                
                if result:
                    print(f'✅ DCF with real R12: Fair value ${result["fair_value_median"]:.2f} vs ${price}')
                    rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                    print(f'   Rate sensitivity: {rate_sens:.1f}/10')
                    print(f'   Valuation signal: {result["valuation_signal"]}')
                else:
                    print(f'❌ DCF calculation failed')
            else:
                print(f'❌ Could not calculate R12 data')
                
        except Exception as e:
            print(f'❌ Error: {str(e)[:100]}...')
    
    await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(test_real_r12())