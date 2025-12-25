#!/usr/bin/env python3
"""
Create DCF valuations table for storing pre-computed Monte Carlo valuations.

This table stores the results of DCF analysis for each financial report,
allowing us to separate valuation computation from price comparison.
"""

import asyncio
import asyncpg

async def create_dcf_valuations_table():
    """Create the DCF valuations table with proper indexing."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        # Drop existing table if it exists
        await conn.execute("DROP TABLE IF EXISTS dcf_valuations CASCADE")
        
        # Create DCF valuations table
        create_table_sql = """
        CREATE TABLE dcf_valuations (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            report_date DATE NOT NULL,
            publish_date DATE NOT NULL,
            report_type VARCHAR(20) NOT NULL, -- 'annual', 'quarterly', 'r12'
            
            -- Model versioning
            model_version VARCHAR(50) NOT NULL DEFAULT 'clean_dcf_v1.0',
            computation_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            
            -- DCF parameters used
            simulation_count INTEGER NOT NULL,
            projection_years INTEGER NOT NULL,
            risk_free_rate DECIMAL(6,4),
            market_premium DECIMAL(6,4),
            terminal_growth DECIMAL(6,4),
            
            -- Fair value results (in report currency)
            fair_value_mean DECIMAL(12,2),
            fair_value_median DECIMAL(12,2),
            fair_value_std DECIMAL(12,2),
            fair_value_p5 DECIMAL(12,2),
            fair_value_p25 DECIMAL(12,2),
            fair_value_p75 DECIMAL(12,2),
            fair_value_p95 DECIMAL(12,2),
            
            -- Currency information
            report_currency VARCHAR(3) NOT NULL,
            stock_currency VARCHAR(3) NOT NULL,
            exchange_rate DECIMAL(10,4),
            
            -- Fair value in stock currency (for direct comparison)
            fair_value_stock_mean DECIMAL(12,2),
            fair_value_stock_median DECIMAL(12,2),
            fair_value_stock_std DECIMAL(12,2),
            fair_value_stock_p5 DECIMAL(12,2),
            fair_value_stock_p25 DECIMAL(12,2),
            fair_value_stock_p75 DECIMAL(12,2),
            fair_value_stock_p95 DECIMAL(12,2),
            
            -- Key financial metrics used
            shares_outstanding BIGINT,
            latest_revenue DECIMAL(15,2),
            latest_operating_income DECIMAL(15,2),
            latest_free_cash_flow DECIMAL(15,2),
            operating_margin DECIMAL(6,4),
            effective_tax_rate DECIMAL(6,4),
            wacc DECIMAL(6,4),
            
            -- Quality indicators
            data_quality_score DECIMAL(3,2), -- 0-1 score based on data completeness
            periods_used INTEGER,
            estimation_flags TEXT[], -- Array of any estimation/interpolation flags
            
            -- Model confidence
            valuation_confidence DECIMAL(3,2), -- 0-1 based on consistency/volatility
            
            -- Performance tracking
            computation_time_ms INTEGER,
            
            UNIQUE(symbol, report_date, model_version)
        );
        """
        
        await conn.execute(create_table_sql)
        
        # Create indexes
        indexes = [
            "CREATE INDEX idx_dcf_valuations_symbol ON dcf_valuations(symbol)",
            "CREATE INDEX idx_dcf_valuations_publish_date ON dcf_valuations(publish_date)",
            "CREATE INDEX idx_dcf_valuations_symbol_publish ON dcf_valuations(symbol, publish_date DESC)",
            "CREATE INDEX idx_dcf_valuations_model_version ON dcf_valuations(model_version)",
            "CREATE INDEX idx_dcf_valuations_computation_date ON dcf_valuations(computation_date DESC)"
        ]
        
        for index_sql in indexes:
            await conn.execute(index_sql)
        
        print("✅ Created dcf_valuations table with indexes")
        
        # Create a view for latest valuations per company
        latest_valuations_view = """
        CREATE OR REPLACE VIEW latest_dcf_valuations AS
        SELECT DISTINCT ON (symbol) *
        FROM dcf_valuations
        ORDER BY symbol, publish_date DESC, computation_date DESC;
        """
        
        await conn.execute(latest_valuations_view)
        print("✅ Created latest_dcf_valuations view")
        
        # Create a function to get valuation at specific date
        valuation_function = """
        CREATE OR REPLACE FUNCTION get_dcf_valuation_at_date(
            p_symbol VARCHAR(20), 
            p_date DATE,
            p_model_version VARCHAR(50) DEFAULT 'clean_dcf_v1.0'
        )
        RETURNS TABLE(
            fair_value_median DECIMAL(12,2),
            fair_value_p25 DECIMAL(12,2),
            fair_value_p75 DECIMAL(12,2),
            report_date DATE,
            publish_date DATE,
            valuation_confidence DECIMAL(3,2)
        )
        LANGUAGE SQL
        AS $$
            SELECT 
                d.fair_value_stock_median,
                d.fair_value_stock_p25,
                d.fair_value_stock_p75,
                d.report_date,
                d.publish_date,
                d.valuation_confidence
            FROM dcf_valuations d
            WHERE d.symbol = p_symbol
            AND d.publish_date <= p_date
            AND d.model_version = p_model_version
            ORDER BY d.publish_date DESC
            LIMIT 1;
        $$;
        """
        
        await conn.execute(valuation_function)
        print("✅ Created get_dcf_valuation_at_date function")
        
        # Show table structure
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'dcf_valuations'
            ORDER BY ordinal_position
        """)
        
        print(f"\n📊 DCF Valuations Table Structure ({len(columns)} columns):")
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"  {col['column_name']}: {col['data_type']} {nullable}")
    
    finally:
        await conn.close()

async def test_table():
    """Test the table structure."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        # Test the function
        result = await conn.fetch("""
            SELECT get_dcf_valuation_at_date('VOLV-B', '2024-06-01')
        """)
        
        print(f"\n🧪 Test function result: {result}")
        
        # Check if we can insert a test record
        test_insert = """
        INSERT INTO dcf_valuations (
            symbol, report_date, publish_date, report_type,
            simulation_count, projection_years,
            fair_value_median, report_currency, stock_currency,
            exchange_rate, fair_value_stock_median,
            shares_outstanding, data_quality_score, periods_used,
            valuation_confidence
        ) VALUES (
            'TEST', '2024-03-31', '2024-04-15', 'quarterly',
            1000, 10,
            100.50, 'SEK', 'SEK',
            1.0, 100.50,
            1000000, 0.85, 4,
            0.75
        )
        ON CONFLICT (symbol, report_date, model_version) DO NOTHING
        """
        
        await conn.execute(test_insert)
        print("✅ Test insert successful")
        
        # Clean up test data
        await conn.execute("DELETE FROM dcf_valuations WHERE symbol = 'TEST'")
        print("✅ Test data cleaned up")
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_dcf_valuations_table())
    asyncio.run(test_table())