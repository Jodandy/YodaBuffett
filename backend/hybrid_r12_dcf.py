#!/usr/bin/env python3
"""
Hybrid R12 DCF Engine

SMART approach based on data availability:
- Pre-2024: Use annual reports directly (perfect R12, only data available)
- 2024+: Use real quarterly R12 construction when possible
- Fallback to annual when quarterly not sufficient
"""

import asyncio
import asyncpg
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters, CompanyFinancials
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class HybridR12DCF(DCFMonteCarloFixed):
    """Smart R12 calculations: Annual for pre-2024, Quarterly R12 for 2024+"""
    
    async def get_real_financials(self, symbol: str, valuation_date: datetime):
        """Smart R12 based on data era and availability"""
        
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
        
        # Get all financial statements
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
        AND period_date >= $2 - INTERVAL '8 years'
        AND total_revenue > 0
        AND operating_income IS NOT NULL
        ORDER BY period_date DESC
        """
        
        all_records = await self.db_conn.fetch(financials_query, symbol, valuation_date.date())
        
        if len(all_records) < 2:
            logger.warning(f"Insufficient financial data for {symbol}")
            return None
        
        # Build R12 periods using smart approach
        r12_periods = []
        
        for period_start_idx in range(0, min(8, len(all_records))):
            
            r12_result = await self._smart_r12_calculation(
                all_records, period_start_idx, symbol, valuation_date
            )
            
            if r12_result:
                r12_periods.append(r12_result)
                
                # Debug output for first few
                if len(r12_periods) <= 3:
                    print(f"   R12 Period {len(r12_periods)}: ${r12_result['revenue']/1e6:.0f}M revenue, "
                          f"{r12_result['margin']*100:.1f}% margin ({r12_result['method']})")
            
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
            revenues_ttm=revenues,
            operating_margins=operating_margins,
            interest_expenses=interest_expenses,
            free_cash_flows=[],
            shares_outstanding=shares_outstanding,
            net_debt=0.0,
            beta=1.0
        )
    
    async def _smart_r12_calculation(self, all_records, start_idx, symbol, valuation_date):
        """Smart R12 calculation based on data era"""
        
        ref_record = all_records[start_idx]
        ref_date = ref_record['period_date']
        ref_year = ref_record['fiscal_year']
        
        # For pre-2024 or annual reports: Use annual directly
        if ref_year < 2024 or ref_record['report_type'] == 'ANNUAL':
            return {
                'period_end': ref_date,
                'revenue': float(ref_record['total_revenue']),
                'operating_income': float(ref_record['operating_income']),
                'interest': float(ref_record['interest_expense']),
                'margin': float(ref_record['operating_income']) / float(ref_record['total_revenue']),
                'method': f"Annual {ref_year}" if ref_record['report_type'] == 'ANNUAL' else f"Pre-2024 Annual {ref_year}"
            }
        
        # For 2024+ quarterly reports: Try to build real quarterly R12
        else:
            return await self._build_quarterly_r12(all_records, start_idx, ref_date, ref_year)
    
    async def _build_quarterly_r12(self, all_records, start_idx, ref_date, ref_year):
        """Build R12 from actual quarterly data (2024+ era)"""
        
        # Find the last 4 quarters of actual data
        quarters_found = []
        
        # Look for quarterly reports
        for i in range(start_idx, len(all_records)):
            record = all_records[i]
            
            if record['report_type'] == 'QUARTERLY' and record['fiscal_year'] >= 2024:
                quarters_found.append({
                    'date': record['period_date'],
                    'year': record['fiscal_year'], 
                    'quarter': record['fiscal_quarter'],
                    'revenue': float(record['total_revenue']),
                    'operating_income': float(record['operating_income']),
                    'interest': float(record['interest_expense'])
                })
                
                if len(quarters_found) >= 4:
                    break
        
        # If we don't have 4 quarters, fall back to most recent annual
        if len(quarters_found) < 4:
            # Find most recent annual for fallback
            for record in all_records[start_idx:]:
                if record['report_type'] == 'ANNUAL':
                    return {
                        'period_end': ref_date,
                        'revenue': float(record['total_revenue']),
                        'operating_income': float(record['operating_income']),
                        'interest': float(record['interest_expense']),
                        'margin': float(record['operating_income']) / float(record['total_revenue']),
                        'method': f"Fallback Annual {record['fiscal_year']} (insufficient quarters)"
                    }
            return None
        
        # Sort and take most recent 4 quarters
        quarters_found.sort(key=lambda x: x['date'], reverse=True)
        last_4_quarters = quarters_found[:4]
        
        # Sum up the R12
        total_revenue = sum(q['revenue'] for q in last_4_quarters)
        total_operating = sum(q['operating_income'] for q in last_4_quarters)
        total_interest = sum(q['interest'] for q in last_4_quarters)
        
        margin = total_operating / total_revenue if total_revenue > 0 else 0
        
        # Create quarters info
        quarter_info = ", ".join([f"Q{q['quarter']}/{q['year']}" for q in reversed(last_4_quarters)])
        
        return {
            'period_end': ref_date,
            'revenue': total_revenue,
            'operating_income': total_operating, 
            'interest': total_interest,
            'margin': margin,
            'method': f"Quarterly R12: {quarter_info}"
        }

async def test_hybrid_r12():
    """Test hybrid approach across different eras"""
    
    params = DCFParameters(num_simulations=1000)
    engine = HybridR12DCF(params)
    await engine.setup()
    
    test_cases = [
        ('AAK', '2022-09-01', 200.0),   # Pre-2024: Should use annual
        ('AAK', '2023-06-01', 180.0),   # Pre-2024: Should use annual  
        ('AAK', '2024-08-01', 300.0),   # 2024+: Should use quarterly R12
        ('AAK', '2025-10-01', 265.0),   # 2024+: Should use quarterly R12
    ]
    
    print('🔧 TESTING HYBRID R12 APPROACH')
    print('=' * 70)
    
    for symbol, date_str, price in test_cases:
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        era = "Pre-2024" if test_date.year < 2024 else "2024+"
        
        print(f'\n📅 {era}: {symbol} on {date_str}')
        
        try:
            financials = await engine.get_real_financials(symbol, test_date)
            
            if financials:
                print(f'✅ Hybrid R12 successful: {len(financials.revenues_ttm)} R12 periods')
                print(f'   Latest R12: ${financials.revenues_ttm[0]/1e6:.0f}M revenue, {financials.operating_margins[0]*100:.1f}% margin')
                
                # DCF result
                result = await engine.value_company(symbol, test_date, price)
                if result:
                    return_pct = result['implied_return'] * 100
                    rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                    print(f'   DCF: ${result["fair_value_median"]:.0f} vs ${price} ({return_pct:+.0f}%) | Rate: {rate_sens:.1f}/10 | {result["valuation_signal"]}')
            else:
                print(f'❌ Could not calculate R12')
                
        except Exception as e:
            print(f'❌ Error: {str(e)[:80]}...')
    
    await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(test_hybrid_r12())