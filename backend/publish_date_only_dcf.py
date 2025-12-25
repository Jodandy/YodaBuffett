#!/usr/bin/env python3
"""
Publish Date Only DCF Engine

CLEAN approach - only use financial statements with real publish_date.
NO historical_fundamentals_daily (lookahead bias corrupted data).

Only valuations based on information that was actually available at valuation_date.
"""

import asyncio
import asyncpg
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters, CompanyFinancials
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class PublishDateOnlyDCF(DCFMonteCarloFixed):
    """DCF using ONLY financial statements with real publish_date - NO LOOKAHEAD BIAS"""
    
    async def get_real_financials(self, symbol: str, valuation_date: datetime):
        """Get financials ONLY using data actually available at valuation_date (via publish_date)"""
        
        # Get shares outstanding from balance sheet data (most reliable source)
        shares_query = """
        SELECT shares_outstanding
        FROM balance_sheet_data
        WHERE symbol = $1
        AND publish_date IS NOT NULL
        AND publish_date <= $2
        AND shares_outstanding > 0
        ORDER BY publish_date DESC
        LIMIT 1
        """
        
        shares_record = await self.db_conn.fetchrow(shares_query, symbol, valuation_date.date())
        
        if not shares_record:
            logger.warning(f"No shares outstanding data for {symbol} at {valuation_date.date()}")
            return None
            
        shares_outstanding = float(shares_record['shares_outstanding'])
        
        # Get financial statements that were ACTUALLY PUBLISHED by valuation_date
        # This is the key - only use data that was available then
        financials_query = """
        SELECT 
            period_date,
            publish_date,
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
        AND publish_date IS NOT NULL
        AND publish_date <= $2  -- CRITICAL: Only data published by valuation date
        AND period_date >= $2 - INTERVAL '8 years'
        AND total_revenue > 0
        AND operating_income IS NOT NULL
        ORDER BY publish_date DESC
        """
        
        all_records = await self.db_conn.fetch(financials_query, symbol, valuation_date.date())
        
        if len(all_records) < 2:
            logger.warning(f"Insufficient published financial data for {symbol} at {valuation_date.date()}")
            return None
        
        # Build R12 periods using only published data
        r12_periods = []
        
        for period_start_idx in range(0, min(8, len(all_records))):
            
            r12_result = await self._calculate_published_r12(
                all_records, period_start_idx, symbol, valuation_date
            )
            
            if r12_result:
                r12_periods.append(r12_result)
                
                # Debug output for first few
                if len(r12_periods) <= 3:
                    print(f"   R12 Period {len(r12_periods)}: ${r12_result['revenue']/1e6:.0f}M revenue, "
                          f"{r12_result['margin']*100:.1f}% margin, Published: {r12_result['publish_date']}")
            
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
    
    async def _calculate_published_r12(self, all_records, start_idx, symbol, valuation_date):
        """Calculate R12 using only data that was actually published"""
        
        ref_record = all_records[start_idx]
        ref_period = ref_record['period_date']
        ref_publish = ref_record['publish_date']
        ref_year = ref_record['fiscal_year']
        
        # For annual reports or pre-2024: Use annual directly
        if ref_record['report_type'] == 'ANNUAL' or ref_year < 2024:
            return {
                'period_end': ref_period,
                'publish_date': ref_publish,
                'revenue': float(ref_record['total_revenue']),
                'operating_income': float(ref_record['operating_income']),
                'interest': float(ref_record['interest_expense']),
                'margin': float(ref_record['operating_income']) / float(ref_record['total_revenue']),
                'method': f"Annual {ref_year}" if ref_record['report_type'] == 'ANNUAL' else f"Pre-2024 {ref_year}"
            }
        
        # For 2024+ quarterly reports: Try to build quarterly R12 from published data
        else:
            return await self._build_published_quarterly_r12(all_records, start_idx, ref_period, ref_year)
    
    async def _build_published_quarterly_r12(self, all_records, start_idx, ref_period, ref_year):
        """Build R12 from quarterly data that was actually published"""
        
        # Find the last 4 quarters that were published before our valuation date
        quarters_found = []
        
        for i in range(start_idx, len(all_records)):
            record = all_records[i]
            
            if record['report_type'] == 'QUARTERLY' and record['fiscal_year'] >= 2024:
                quarters_found.append({
                    'period': record['period_date'],
                    'publish': record['publish_date'],
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
            for record in all_records[start_idx:]:
                if record['report_type'] == 'ANNUAL':
                    return {
                        'period_end': ref_period,
                        'publish_date': record['publish_date'],
                        'revenue': float(record['total_revenue']),
                        'operating_income': float(record['operating_income']),
                        'interest': float(record['interest_expense']),
                        'margin': float(record['operating_income']) / float(record['total_revenue']),
                        'method': f"Fallback Annual {record['fiscal_year']} (insufficient published quarters)"
                    }
            return None
        
        # Sort by period date and take most recent 4 quarters
        quarters_found.sort(key=lambda x: x['period'], reverse=True)
        last_4_quarters = quarters_found[:4]
        
        # Sum up the R12
        total_revenue = sum(q['revenue'] for q in last_4_quarters)
        total_operating = sum(q['operating_income'] for q in last_4_quarters)
        total_interest = sum(q['interest'] for q in last_4_quarters)
        
        margin = total_operating / total_revenue if total_revenue > 0 else 0
        
        # Create quarters info for debugging
        quarter_info = ", ".join([f"Q{q['quarter']}/{q['year']}" for q in reversed(last_4_quarters)])
        latest_publish = max(q['publish'] for q in last_4_quarters)
        
        return {
            'period_end': ref_period,
            'publish_date': latest_publish,
            'revenue': total_revenue,
            'operating_income': total_operating,
            'interest': total_interest,
            'margin': margin,
            'method': f"Published Quarterly R12: {quarter_info}"
        }

async def test_publish_date_only():
    """Test the publish_date only approach"""
    
    params = DCFParameters(num_simulations=1000)
    engine = PublishDateOnlyDCF(params)
    await engine.setup()
    
    test_cases = [
        ('AAK', '2022-03-15', 200.0),   # Early 2022: What was actually available then
        ('AAK', '2023-04-01', 180.0),   # Q1 2023: What was published by then
        ('AAK', '2024-05-01', 300.0),   # Q1 2024: Real time data availability
        ('ABB', '2021-03-01', 30.0),    # Test ABB timing
        ('VOLV-B', '2024-08-01', 280.0), # Test Volvo
    ]
    
    print('🔒 TESTING PUBLISH DATE ONLY DCF (NO LOOKAHEAD BIAS)')
    print('=' * 70)
    print('Only using financial statements with publish_date <= valuation_date')
    
    for symbol, date_str, price in test_cases:
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        print(f'\n📅 {symbol} on {date_str} (Price: ${price})')
        
        try:
            financials = await engine.get_real_financials(symbol, test_date)
            
            if financials:
                print(f'✅ Published data available: {len(financials.revenues_ttm)} R12 periods')
                
                # DCF result
                result = await engine.value_company(symbol, test_date, price)
                if result:
                    return_pct = result['implied_return'] * 100
                    rate_sens = result['rate_sensitivity']['rate_sensitivity_score']
                    print(f'   DCF: ${result["fair_value_median"]:.0f} vs ${price} ({return_pct:+.0f}%) | Rate: {rate_sens:.1f}/10 | {result["valuation_signal"]}')
            else:
                print(f'❌ No published data available at valuation date')
                
        except Exception as e:
            print(f'❌ Error: {str(e)[:80]}...')
    
    await engine.cleanup()

async def run_all_companies_published_only():
    """Run DCF for ALL companies but only using published data"""
    
    params = DCFParameters(num_simulations=1000)
    engine = PublishDateOnlyDCF(params)
    await engine.setup()
    
    print('🌍 RUNNING PUBLISHED-ONLY DCF FOR ALL COMPANIES')
    print('=' * 70)
    
    # Get all companies with published financial data
    companies_query = """
    SELECT DISTINCT 
        fs.symbol,
        COUNT(*) as published_reports,
        MIN(fs.publish_date) as earliest_publish,
        MAX(fs.publish_date) as latest_publish
    FROM financial_statements fs
    WHERE fs.publish_date IS NOT NULL
    GROUP BY fs.symbol
    HAVING COUNT(*) >= 3
    ORDER BY published_reports DESC
    """
    
    companies = await engine.db_conn.fetch(companies_query)
    
    print(f'📊 Found {len(companies):,} companies with ≥3 published reports')
    
    results = []
    
    for i, company in enumerate(companies):
        symbol = company['symbol']
        latest_publish = company['latest_publish']
        
        if i % 10 == 0:
            progress = i / len(companies) * 100
            print(f'   📈 {progress:.1f}% | Processing {symbol}...')
        
        try:
            # Use latest publish date + 30 days as valuation date
            valuation_date = datetime.combine(latest_publish + __import__('datetime').timedelta(days=30), datetime.min.time())
            
            # Get current market price (estimate)
            price_query = """
            SELECT close_price
            FROM daily_price_data
            WHERE symbol = $1
            AND date <= $2
            ORDER BY date DESC
            LIMIT 1
            """
            
            price_record = await engine.db_conn.fetchrow(price_query, symbol, valuation_date.date())
            
            if not price_record:
                continue
                
            current_price = float(price_record['close_price'])
            
            # Run DCF
            result = await engine.value_company(symbol, valuation_date, current_price)
            
            if result:
                results.append({
                    'symbol': symbol,
                    'valuation_date': valuation_date.date(),
                    'fair_value': result['fair_value_median'],
                    'current_price': current_price,
                    'implied_return': result['implied_return'],
                    'signal': result['valuation_signal']
                })
                
                if len(results) <= 10:
                    return_pct = result['implied_return'] * 100
                    print(f'   {symbol:8} | ${result["fair_value_median"]:6.0f} vs ${current_price:6.0f} ({return_pct:+5.0f}%) | {result["valuation_signal"]}')
        
        except Exception as e:
            continue
    
    await engine.cleanup()
    
    print(f'\n✅ PUBLISHED-ONLY DCF COMPLETE')
    print(f'   Successfully valued {len(results):,} companies using only published data')
    print(f'   NO LOOKAHEAD BIAS - only information available at valuation time')
    
    return results

if __name__ == "__main__":
    # Test first
    asyncio.run(test_publish_date_only())
    
    # Then run comprehensive analysis
    print('\n' + '='*80 + '\n')
    asyncio.run(run_all_companies_published_only())