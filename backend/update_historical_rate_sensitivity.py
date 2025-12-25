#!/usr/bin/env python3
"""
Update Historical DCF Records with Rate Sensitivity Data

Add the missing rate sensitivity metrics to existing DCF valuations.
"""

import asyncio
import asyncpg
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def update_missing_rate_sensitivity():
    """Update existing DCF records with missing rate sensitivity data"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Initialize DCF engine
    params = DCFParameters(num_simulations=1000)
    engine = DCFMonteCarloFixed(params)
    await engine.setup()
    
    try:
        # Get records missing rate sensitivity data
        missing_records = await conn.fetch("""
            SELECT id, symbol, valuation_date, market_price
            FROM dcf_valuations 
            WHERE rate_sensitivity_score IS NULL
            ORDER BY valuation_date DESC, symbol
            LIMIT 20
        """)
        
        print(f"🔧 Updating {len(missing_records)} records with rate sensitivity data")
        
        updated_count = 0
        
        for record in missing_records:
            try:
                # Re-calculate the DCF with current engine to get rate sensitivity
                valuation_date = datetime.combine(record['valuation_date'], datetime.min.time())
                market_price = float(record['market_price'])
                
                dcf_result = await engine.value_company(
                    record['symbol'],
                    valuation_date,
                    market_price
                )
                
                if dcf_result and 'rate_sensitivity' in dcf_result:
                    rate_sens = dcf_result['rate_sensitivity']
                    
                    # Update the existing record with rate sensitivity data
                    update_query = """
                    UPDATE dcf_valuations 
                    SET rate_sensitivity_score = $1,
                        fair_value_change_per_100bp = $2,
                        debt_burden_category = $3,
                        interest_to_revenue_ratio = $4,
                        actual_wacc = $5
                    WHERE id = $6
                    """
                    
                    await conn.execute(
                        update_query,
                        rate_sens['rate_sensitivity_score'],
                        rate_sens['fair_value_change_per_100bp'],
                        rate_sens['debt_burden_category'],
                        rate_sens['interest_to_revenue_ratio'],
                        dcf_result['wacc'],
                        record['id']
                    )
                    
                    updated_count += 1
                    print(f"✓ Updated {record['symbol']} {record['valuation_date']} - "
                          f"Rate sensitivity: {rate_sens['rate_sensitivity_score']:.1f}/10")
                          
            except Exception as e:
                logger.warning(f"Failed to update record {record['id']}: {e}")
                
        print(f"\n✅ Updated {updated_count}/{len(missing_records)} records")
        
        # Show summary
        final_count = await conn.fetchval("""
            SELECT COUNT(*) FROM dcf_valuations 
            WHERE rate_sensitivity_score IS NOT NULL
        """)
        total_count = await conn.fetchval("SELECT COUNT(*) FROM dcf_valuations")
        
        print(f"📊 Final status: {final_count}/{total_count} records have rate sensitivity data")
        
    finally:
        await engine.cleanup()
        await conn.close()

if __name__ == "__main__":
    asyncio.run(update_missing_rate_sensitivity())