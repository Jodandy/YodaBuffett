#!/usr/bin/env python3
"""
Simple database test using asyncpg directly
"""

import asyncio
import asyncpg


async def test_db_simple():
    """Simple database connectivity test"""
    try:
        # Connect to YodaBuffett database
        conn = await asyncpg.connect(
            user='yodabuffett',
            password='password', 
            host='localhost',
            port=5432,
            database='yodabuffett'
        )
        
        # Test basic query
        result = await conn.fetchrow("SELECT NOW() as current_time, version() as version")
        print("✅ Database connection successful!")
        print(f"Time: {result['current_time']}")
        print(f"Version: {result['version']}")
        
        # Check key tables
        tables = ['company_master', 'historical_fundamentals_daily', 'daily_price_data']
        
        for table in tables:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                print(f"📊 {table}: {count:,} rows")
            except Exception as e:
                print(f"❌ {table}: Error - {e}")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_db_simple())
    exit(0 if success else 1)