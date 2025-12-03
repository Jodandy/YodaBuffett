#!/usr/bin/env python3
"""
Create market data tables for TimescaleDB
Optimized for time-series market data storage and analysis
"""

import asyncio
import asyncpg
import os
from typing import Optional

async def create_market_data_tables():
    """Create TimescaleDB-optimized market data tables"""
    
    # Database connection - use same as document intelligence system
    DATABASE_URL = os.getenv(
        'DATABASE_URL', 
        'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    )
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Connected to database")
        
        # Check if TimescaleDB is available (optional enhancement)
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            print("✅ TimescaleDB extension enabled")
            timescale_available = True
        except Exception as e:
            print("⚠️  TimescaleDB not available, using standard PostgreSQL tables")
            print("   (This is fine - we'll use indexes for performance)")
            timescale_available = False
        
        # 1. Market data symbols table (company metadata)
        symbols_table = """
        CREATE TABLE IF NOT EXISTS market_data_symbols (
            symbol VARCHAR(20) PRIMARY KEY,
            company_name VARCHAR(200) NOT NULL,
            yahoo_symbol VARCHAR(30) NOT NULL,
            market VARCHAR(50) NOT NULL,
            country VARCHAR(10) NOT NULL,
            sector VARCHAR(100),
            industry VARCHAR(100),
            market_cap BIGINT,
            currency VARCHAR(10) DEFAULT 'SEK',
            
            -- Mapping to our document database
            document_company_name VARCHAR(200), -- Links to extracted_documents
            
            -- Provider tracking
            provider VARCHAR(50) NOT NULL DEFAULT 'yahoo_finance',
            data_quality_score DECIMAL(3,2) DEFAULT 1.0,
            
            -- Metadata
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            last_data_fetch TIMESTAMP,
            
            UNIQUE(yahoo_symbol, provider)
        );
        """
        
        await conn.execute(symbols_table)
        print("✅ Created market_data_symbols table")
        
        # 2. Daily price data table (TimescaleDB hypertable)
        price_data_table = """
        CREATE TABLE IF NOT EXISTS daily_price_data (
            symbol VARCHAR(20) NOT NULL REFERENCES market_data_symbols(symbol),
            date DATE NOT NULL,
            
            -- OHLCV data
            open_price DECIMAL(12, 4) NOT NULL,
            high_price DECIMAL(12, 4) NOT NULL,
            low_price DECIMAL(12, 4) NOT NULL,
            close_price DECIMAL(12, 4) NOT NULL,
            adjusted_close DECIMAL(12, 4),
            volume BIGINT,
            
            -- Calculated fields
            daily_return DECIMAL(8, 6),
            log_return DECIMAL(8, 6),
            volatility_5d DECIMAL(8, 6),
            volatility_20d DECIMAL(8, 6),
            
            -- Provider metadata
            provider VARCHAR(50) NOT NULL DEFAULT 'yahoo_finance',
            data_quality VARCHAR(20) DEFAULT 'good',
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT NOW(),
            
            PRIMARY KEY (symbol, date, provider)
        );
        """
        
        await conn.execute(price_data_table)
        print("✅ Created daily_price_data table")
        
        # 3. Convert to TimescaleDB hypertable if available
        if timescale_available:
            try:
                await conn.execute("""
                    SELECT create_hypertable('daily_price_data', 'date', 
                                           chunk_time_interval => INTERVAL '1 month',
                                           if_not_exists => TRUE);
                """)
                print("✅ Converted daily_price_data to TimescaleDB hypertable")
            except Exception as e:
                print(f"⚠️  Could not create hypertable: {e}")
        else:
            # Create partitioning for PostgreSQL (alternative to hypertables)
            print("📊 Using PostgreSQL partitioning for time-series optimization")
        
        # 4. Performance metrics table (calculated from price data)
        performance_table = """
        CREATE TABLE IF NOT EXISTS market_performance_metrics (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            symbol VARCHAR(20) NOT NULL REFERENCES market_data_symbols(symbol),
            
            -- Time period
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            period_days INTEGER NOT NULL,
            timeframe VARCHAR(20) NOT NULL, -- '1mo', '3mo', '6mo', '1yr'
            
            -- Performance metrics
            total_return DECIMAL(10, 6) NOT NULL,
            annualized_return DECIMAL(10, 6),
            volatility DECIMAL(10, 6),
            max_drawdown DECIMAL(10, 6),
            sharpe_ratio DECIMAL(10, 6),
            sortino_ratio DECIMAL(10, 6),
            
            -- Price points
            start_price DECIMAL(12, 4) NOT NULL,
            end_price DECIMAL(12, 4) NOT NULL,
            max_price DECIMAL(12, 4),
            min_price DECIMAL(12, 4),
            
            -- Comparison metrics
            market_return DECIMAL(10, 6), -- vs OMXS30
            alpha DECIMAL(10, 6),
            beta DECIMAL(10, 6),
            
            -- Quality and metadata
            trading_days INTEGER,
            data_completeness DECIMAL(3, 2) DEFAULT 1.0,
            provider VARCHAR(50) NOT NULL DEFAULT 'yahoo_finance',
            calculated_at TIMESTAMP DEFAULT NOW(),
            
            UNIQUE(symbol, start_date, end_date, provider)
        );
        """
        
        await conn.execute(performance_table)
        print("✅ Created market_performance_metrics table")
        
        # 5. Market anomaly correlation table (links to temporal anomalies)
        anomaly_correlation_table = """
        CREATE TABLE IF NOT EXISTS market_anomaly_correlations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            
            -- Links to document anomaly
            company_name VARCHAR(200) NOT NULL,
            document_date DATE NOT NULL,
            anomaly_score DECIMAL(6, 4),
            anomaly_type VARCHAR(100),
            
            -- Links to market data
            symbol VARCHAR(20) REFERENCES market_data_symbols(symbol),
            
            -- Performance after anomaly
            performance_1w DECIMAL(8, 6),
            performance_1m DECIMAL(8, 6),
            performance_3m DECIMAL(8, 6),
            performance_6m DECIMAL(8, 6),
            
            -- Market context
            market_performance_1m DECIMAL(8, 6), -- OMXS30 performance
            relative_performance_1m DECIMAL(8, 6), -- vs market
            
            -- Analysis metadata
            analyzed_at TIMESTAMP DEFAULT NOW(),
            data_quality VARCHAR(20) DEFAULT 'pending',
            
            UNIQUE(company_name, document_date, symbol)
        );
        """
        
        await conn.execute(anomaly_correlation_table)
        print("✅ Created market_anomaly_correlations table")
        
        # 6. Create indexes for performance
        indexes = [
            # Primary access patterns
            "CREATE INDEX IF NOT EXISTS idx_daily_price_symbol_date ON daily_price_data(symbol, date DESC);",
            "CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price_data(date DESC);",
            
            # For latest price lookups
            "CREATE INDEX IF NOT EXISTS idx_daily_price_symbol_date_desc ON daily_price_data(symbol, date DESC) INCLUDE (close_price, volume);",
            
            # For volatility calculations  
            "CREATE INDEX IF NOT EXISTS idx_daily_price_volatility ON daily_price_data(symbol, date) WHERE volatility_20d IS NOT NULL;",
            
            # Performance metrics
            "CREATE INDEX IF NOT EXISTS idx_performance_symbol ON market_performance_metrics(symbol);",
            "CREATE INDEX IF NOT EXISTS idx_performance_timeframe ON market_performance_metrics(timeframe);",
            "CREATE INDEX IF NOT EXISTS idx_performance_latest ON market_performance_metrics(symbol, timeframe, calculated_at DESC);",
            
            # Anomaly correlations
            "CREATE INDEX IF NOT EXISTS idx_anomaly_company ON market_anomaly_correlations(company_name);",
            "CREATE INDEX IF NOT EXISTS idx_anomaly_date ON market_anomaly_correlations(document_date);",
            "CREATE INDEX IF NOT EXISTS idx_anomaly_performance ON market_anomaly_correlations(symbol, performance_1m) WHERE performance_1m IS NOT NULL;",
        ]
        
        for index_sql in indexes:
            await conn.execute(index_sql)
        
        print("✅ Created performance indexes")
        
        # 7. Create views for common queries
        performance_view = """
        CREATE OR REPLACE VIEW latest_performance AS
        SELECT 
            s.symbol,
            s.company_name,
            s.yahoo_symbol,
            s.market,
            p.total_return as return_1m,
            p.volatility,
            p.sharpe_ratio,
            p.max_drawdown,
            p.calculated_at,
            latest.close_price as current_price,
            latest.date as last_price_date
        FROM market_data_symbols s
        LEFT JOIN market_performance_metrics p ON s.symbol = p.symbol 
            AND p.timeframe = '1mo'
        LEFT JOIN LATERAL (
            SELECT close_price, date 
            FROM daily_price_data 
            WHERE symbol = s.symbol 
            ORDER BY date DESC 
            LIMIT 1
        ) latest ON true
        ORDER BY p.total_return DESC NULLS LAST;
        """
        
        await conn.execute(performance_view)
        print("✅ Created latest_performance view")
        
        # 8. Insert sample Nordic companies for testing
        sample_companies = [
            ('AAK', 'AAK AB', 'AAK.ST', 'Stockholm', 'SE', 'Industrial', 'Chemicals'),
            ('VOLV-B', 'Volvo Group', 'VOLV-B.ST', 'Stockholm', 'SE', 'Industrial', 'Automotive'),
            ('ATCO-A', 'Atlas Copco AB', 'ATCO-A.ST', 'Stockholm', 'SE', 'Industrial', 'Machinery'),
            ('ERIC-B', 'Ericsson', 'ERIC-B.ST', 'Stockholm', 'SE', 'Technology', 'Telecommunications'),
            ('ABB', 'ABB Ltd', 'ABB.ST', 'Stockholm', 'SE', 'Industrial', 'Electrical Equipment'),
        ]
        
        for symbol, name, yahoo, market, country, sector, industry in sample_companies:
            try:
                await conn.execute("""
                    INSERT INTO market_data_symbols 
                    (symbol, company_name, yahoo_symbol, market, country, sector, industry, document_company_name)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        updated_at = NOW()
                """, symbol, name, yahoo, market, country, sector, industry, name.replace(' ', '_'))
            except Exception as e:
                print(f"⚠️  Sample data insert failed for {symbol}: {e}")
        
        print("✅ Inserted sample Nordic companies")
        
        await conn.close()
        print("\n🎉 Market data tables created successfully!")
        print("\nNext steps:")
        print("1. Run historical data ingestion")
        print("2. Test with: SELECT * FROM market_data_symbols;")
        print("3. Start collecting price data from Yahoo Finance")
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    asyncio.run(create_market_data_tables())