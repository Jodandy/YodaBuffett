#!/usr/bin/env python3
"""
Analyze Period Patterns

Check what quarterly/annual patterns exist in our financial data.
"""

import asyncio
import asyncpg

async def analyze_period_patterns():
    """Analyze the patterns of quarterly vs annual data"""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    # Check overall data patterns
    pattern_query = """
    WITH financial_periods AS (
        SELECT 
            symbol,
            EXTRACT(YEAR FROM period_date) as year,
            EXTRACT(MONTH FROM period_date) as month,
            period_date,
            total_revenue,
            CASE 
                WHEN (EXTRACT(MONTH FROM period_date) = 12 AND EXTRACT(DAY FROM period_date) >= 28)
                     OR (EXTRACT(MONTH FROM period_date) = 1 AND EXTRACT(DAY FROM period_date) <= 3)
                THEN 'annual'
                ELSE 'quarterly'
            END as period_type
        FROM financial_statements 
        WHERE EXTRACT(YEAR FROM period_date) >= 2023
        AND total_revenue > 100000000
        AND publish_date IS NOT NULL
    ),
    company_patterns AS (
        SELECT 
            symbol,
            year,
            COUNT(CASE WHEN period_type = 'quarterly' THEN 1 END) as quarterly_count,
            COUNT(CASE WHEN period_type = 'annual' THEN 1 END) as annual_count,
            ARRAY_AGG(month ORDER BY month) FILTER (WHERE period_type = 'quarterly') as quarterly_months,
            ARRAY_AGG(month ORDER BY month) FILTER (WHERE period_type = 'annual') as annual_months
        FROM financial_periods
        GROUP BY symbol, year
        HAVING COUNT(*) > 1  -- At least 2 periods
    )
    SELECT 
        quarterly_count,
        annual_count,
        COUNT(*) as company_year_combinations,
        ARRAY_AGG(DISTINCT symbol) as sample_symbols
    FROM company_patterns
    GROUP BY quarterly_count, annual_count
    ORDER BY company_year_combinations DESC
    """
    
    patterns = await conn.fetch(pattern_query)
    
    print("📊 PERIOD PATTERNS IN FINANCIAL DATA")
    print("=" * 60)
    print(f"{'Quarterly':<12}{'Annual':<8}{'Count':<8}{'Sample Companies'}")
    print("-" * 60)
    
    for row in patterns:
        quarterly = row['quarterly_count']
        annual = row['annual_count']
        count = row['company_year_combinations']
        samples = str(row['sample_symbols'][:3])[1:-1]  # First 3 samples
        
        print(f"{quarterly:<12}{annual:<8}{count:<8}{samples}")
    
    # Look for specific Q1+Q2+Q3 pattern (check the "3 quarterly + 1 annual" pattern from above)
    print(f"\n🔍 EXAMINING THE '3 quarterly + 1 annual' PATTERN")
    
    companies_with_3q1a = ['ACAD', 'ADDT B', 'BERG B']  # From the results above
    
    for symbol in companies_with_3q1a:
        detail_query = """
        SELECT 
            period_date,
            EXTRACT(MONTH FROM period_date) as month,
            total_revenue / 1e6 as revenue_m,
            CASE 
                WHEN (EXTRACT(MONTH FROM period_date) = 12 AND EXTRACT(DAY FROM period_date) >= 28)
                     OR (EXTRACT(MONTH FROM period_date) = 1 AND EXTRACT(DAY FROM period_date) <= 3)
                THEN 'annual'
                ELSE 'quarterly'
            END as period_type
        FROM financial_statements 
        WHERE symbol = $1
        AND EXTRACT(YEAR FROM period_date) >= 2023
        AND total_revenue > 100000000
        AND publish_date IS NOT NULL
        ORDER BY period_date DESC
        """
        
        details = await conn.fetch(detail_query, symbol)
        
        if details:
            print(f"\n   {symbol}:")
            for row in details:
                period_type = row['period_type']
                month = int(row['month'])
                revenue = row['revenue_m']
                date = row['period_date']
                print(f"     {date} (Month {month:2d}): {revenue:8.0f}M - {period_type}")
    
    print(f"\n❌ No perfect Q1+Q2+Q3+Annual patterns found in current data")
    
    # Check what quarterly months we actually have
    print(f"\n📅 QUARTERLY MONTH DISTRIBUTION")
    
    month_query = """
    SELECT 
        EXTRACT(MONTH FROM period_date) as month,
        COUNT(*) as count
    FROM financial_statements 
    WHERE EXTRACT(YEAR FROM period_date) >= 2023
    AND total_revenue > 100000000
    AND publish_date IS NOT NULL
    AND NOT (EXTRACT(MONTH FROM period_date) = 12 AND EXTRACT(DAY FROM period_date) >= 28)  -- Exclude annual
    AND NOT (EXTRACT(MONTH FROM period_date) = 1 AND EXTRACT(DAY FROM period_date) <= 3)
    GROUP BY EXTRACT(MONTH FROM period_date)
    ORDER BY month
    """
    
    months = await conn.fetch(month_query)
    
    for row in months:
        month = int(row['month'])
        count = row['count']
        quarter = f"Q{((month-1)//3) + 1}"
        print(f"   Month {month:2d} ({quarter}): {count:4d} reports")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_period_patterns())