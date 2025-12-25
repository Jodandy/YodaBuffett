#!/usr/bin/env python3
"""
Create fundamental data tables for value strategy
"""

import asyncio
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_tables(conn: asyncpg.Connection):
    """Create fundamental data tables"""
    
    # Table for storing fundamental metrics
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            company_id INTEGER,
            metric VARCHAR(100) NOT NULL,
            value DOUBLE PRECISION,
            period_type VARCHAR(10) NOT NULL, -- 'annual' or 'quarterly'
            period_end_date DATE NOT NULL,
            reporting_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Ensure we only have one value per symbol/metric/period
            UNIQUE(symbol, metric, period_type, period_end_date),
            
            -- Index for queries
            INDEX idx_fundamentals_symbol_date (symbol, period_end_date),
            INDEX idx_fundamentals_metric (metric)
        )
    """)
    
    # Table for storing valuation signals
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS value_signals (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            signal_date DATE NOT NULL,
            price DOUBLE PRECISION NOT NULL,
            
            -- Valuation results
            fat_pitch_price DOUBLE PRECISION,
            fair_value DOUBLE PRECISION,
            upside_target DOUBLE PRECISION,
            downside_target DOUBLE PRECISION,
            current_asymmetry DOUBLE PRECISION,
            
            -- Signal
            signal INTEGER, -- 1 = buy, -1 = sell, 0 = neutral
            signal_type VARCHAR(50),
            confidence DOUBLE PRECISION,
            method_count INTEGER,
            
            -- Method details (JSONB for flexibility)
            method_details JSONB,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Ensure one signal per symbol per date
            UNIQUE(symbol, signal_date),
            
            -- Index for queries
            INDEX idx_value_signals_date (signal_date),
            INDEX idx_value_signals_symbol (symbol)
        )
    """)
    
    logger.info("Fundamental tables created successfully")


async def main():
    """Create all tables"""
    
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        await create_tables(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())