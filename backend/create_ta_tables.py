"""
Create technical analysis database tables.
"""

import asyncio
import asyncpg
import os
from shared.database import engine

async def create_tables():
    try:
        # Read the schema
        with open('services/technical_analysis/db/schema.sql', 'r') as f:
            schema_sql = f.read()
        
        # Use the known working database URL
        database_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        print(f"Connecting to: {database_url}")
        
        conn = await asyncpg.connect(database_url)
        
        # Execute schema
        await conn.execute(schema_sql)
        print('✅ Technical analysis tables created successfully!')
        
        # Create simple market_data view for compatibility
        view_sql = """
        CREATE OR REPLACE VIEW market_data AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY symbol, date) as id,
            s.id as company_id,
            d.symbol,
            d.date,
            d.open_price as open,
            d.high_price as high,
            d.low_price as low,
            d.close_price as close,
            d.adjusted_close,
            d.volume
        FROM daily_price_data d
        JOIN company_master s ON s.ticker_symbol = d.symbol
        WHERE s.ticker_symbol IS NOT NULL;
        """
        
        await conn.execute(view_sql)
        print('✅ Created market_data view for compatibility')
        
        await conn.close()
    except Exception as e:
        print(f'❌ Error: {e}')

if __name__ == "__main__":
    asyncio.run(create_tables())