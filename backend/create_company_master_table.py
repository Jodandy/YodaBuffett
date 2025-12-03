#!/usr/bin/env python3
"""
Create a comprehensive company master table.
Central repository for ALL company information and metadata.
"""

import asyncio
import asyncpg
import json
from datetime import datetime
from typing import Optional

async def create_company_master_table():
    """Create the comprehensive company master table"""
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        print("🏗️  Creating Company Master Table...")
        
        # Drop existing simple market_data_symbols if it exists
        # We'll replace it with the comprehensive company_master
        
        company_master_sql = """
        CREATE TABLE IF NOT EXISTS company_master (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            
            -- Core Identity
            company_name VARCHAR(200) NOT NULL,
            company_name_clean VARCHAR(200), -- Cleaned version for matching
            company_slug VARCHAR(100) UNIQUE, -- URL-safe identifier
            
            -- Stock Exchange Information
            primary_ticker VARCHAR(20),
            yahoo_symbol VARCHAR(30),
            bloomberg_symbol VARCHAR(30),
            reuters_symbol VARCHAR(30),
            isin_code VARCHAR(20),
            sedol_code VARCHAR(10),
            
            -- Exchange Details
            primary_exchange VARCHAR(50), -- 'Stockholm', 'Oslo', 'Copenhagen', 'Helsinki'
            exchange_mic_code VARCHAR(10), -- Market Identifier Code
            currency VARCHAR(10) DEFAULT 'SEK',
            listing_status VARCHAR(20) DEFAULT 'active', -- active, delisted, suspended
            
            -- Geographic Information
            country VARCHAR(10), -- SE, NO, DK, FI
            region VARCHAR(20), -- nordic, europe, etc.
            headquarters_city VARCHAR(100),
            headquarters_country VARCHAR(10),
            
            -- Business Classification
            sector VARCHAR(100),
            industry VARCHAR(100),
            sub_industry VARCHAR(100),
            gics_sector_code VARCHAR(10),
            gics_industry_code VARCHAR(20),
            
            -- Company Size & Status
            market_cap_tier VARCHAR(20), -- large_cap, mid_cap, small_cap, micro_cap
            market_cap_usd BIGINT,
            employee_count INTEGER,
            founded_year INTEGER,
            
            -- Document Tracking
            document_company_names TEXT[], -- All variations found in documents
            document_count INTEGER DEFAULT 0,
            first_document_year INTEGER,
            last_document_year INTEGER,
            document_types TEXT[], -- Types of docs we have
            
            -- Data Source URLs and Identifiers
            mfn_company_id VARCHAR(50),
            mfn_company_url TEXT,
            mfn_company_slug VARCHAR(100),
            
            ir_website_url TEXT,
            company_website_url TEXT,
            
            -- Financial Data Sources
            yahoo_finance_available BOOLEAN DEFAULT false,
            bloomberg_available BOOLEAN DEFAULT false,
            refinitiv_available BOOLEAN DEFAULT false,
            
            -- Data Quality and Status
            data_quality_score DECIMAL(3,2) DEFAULT 0.5, -- 0.0 to 1.0
            symbol_confidence VARCHAR(20) DEFAULT 'unknown', -- high, medium, low, unknown
            last_data_validation TIMESTAMP,
            
            -- Operational Metadata
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            created_by VARCHAR(50) DEFAULT 'system',
            
            -- Constraints
            CONSTRAINT company_master_name_check CHECK (LENGTH(company_name) >= 2),
            CONSTRAINT company_master_quality_check CHECK (data_quality_score BETWEEN 0.0 AND 1.0)
        );
        """
        
        await conn.execute(company_master_sql)
        print("✅ Created company_master table")
        
        # Create indexes for performance
        indexes = [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_company_master_slug ON company_master(company_slug);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_name ON company_master(company_name);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_ticker ON company_master(primary_ticker);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_yahoo ON company_master(yahoo_symbol);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_country ON company_master(country);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_exchange ON company_master(primary_exchange);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_sector ON company_master(sector);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_doc_count ON company_master(document_count DESC);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_market_cap ON company_master(market_cap_usd DESC) WHERE market_cap_usd IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_company_master_updated ON company_master(updated_at DESC);",
            
            # GIN index for array searches
            "CREATE INDEX IF NOT EXISTS idx_company_master_doc_names ON company_master USING GIN(document_company_names);",
            "CREATE INDEX IF NOT EXISTS idx_company_master_doc_types ON company_master USING GIN(document_types);",
        ]
        
        for index_sql in indexes:
            await conn.execute(index_sql)
        
        print("✅ Created performance indexes")
        
        # Create related tables
        
        # Company aliases/alternative names
        aliases_sql = """
        CREATE TABLE IF NOT EXISTS company_aliases (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            company_id UUID NOT NULL REFERENCES company_master(id) ON DELETE CASCADE,
            alias_name VARCHAR(200) NOT NULL,
            alias_type VARCHAR(50) NOT NULL, -- 'trading_name', 'former_name', 'subsidiary', 'document_variant'
            source VARCHAR(50), -- 'document', 'manual', 'api'
            confidence DECIMAL(3,2) DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT NOW(),
            
            UNIQUE(company_id, alias_name, alias_type)
        );
        """
        
        await conn.execute(aliases_sql)
        print("✅ Created company_aliases table")
        
        # Company relationships (parent/subsidiary, mergers, etc.)
        relationships_sql = """
        CREATE TABLE IF NOT EXISTS company_relationships (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            parent_company_id UUID NOT NULL REFERENCES company_master(id),
            child_company_id UUID NOT NULL REFERENCES company_master(id),
            relationship_type VARCHAR(50) NOT NULL, -- 'subsidiary', 'division', 'merger', 'spinoff'
            relationship_start_date DATE,
            relationship_end_date DATE,
            ownership_percentage DECIMAL(5,2), -- 0.00 to 100.00
            created_at TIMESTAMP DEFAULT NOW(),
            
            CONSTRAINT no_self_relationship CHECK (parent_company_id != child_company_id)
        );
        """
        
        await conn.execute(relationships_sql)
        print("✅ Created company_relationships table")
        
        # Update the daily_price_data table to reference company_master
        price_data_update = """
        -- Add company_id column to daily_price_data for proper foreign key
        ALTER TABLE daily_price_data 
        ADD COLUMN IF NOT EXISTS company_id UUID REFERENCES company_master(id);
        
        CREATE INDEX IF NOT EXISTS idx_daily_price_company_id ON daily_price_data(company_id);
        """
        
        await conn.execute(price_data_update)
        print("✅ Updated daily_price_data table structure")
        
        # Create views for common queries
        views = [
            # Active Nordic companies with market data
            """
            CREATE OR REPLACE VIEW nordic_companies_with_data AS
            SELECT 
                cm.*,
                COUNT(dpd.date) as price_data_points,
                MIN(dpd.date) as first_price_date,
                MAX(dpd.date) as last_price_date
            FROM company_master cm
            LEFT JOIN daily_price_data dpd ON cm.primary_ticker = dpd.symbol
            WHERE cm.country IN ('SE', 'NO', 'DK', 'FI')
            AND cm.listing_status = 'active'
            GROUP BY cm.id
            ORDER BY cm.document_count DESC, price_data_points DESC;
            """,
            
            # Companies prioritized for data collection
            """
            CREATE OR REPLACE VIEW high_priority_companies AS
            SELECT 
                cm.*,
                CASE 
                    WHEN cm.document_count >= 50 THEN 'high'
                    WHEN cm.document_count >= 10 THEN 'medium'
                    WHEN cm.document_count >= 5 THEN 'low'
                    ELSE 'minimal'
                END as data_priority
            FROM company_master cm
            WHERE cm.document_count > 0
            ORDER BY cm.document_count DESC, cm.market_cap_usd DESC NULLS LAST;
            """,
            
            # Document companies not yet matched
            """
            CREATE OR REPLACE VIEW unmatched_document_companies AS
            SELECT 
                ed.company_name,
                COUNT(*) as doc_count,
                MIN(ed.year) as first_year,
                MAX(ed.year) as last_year,
                COUNT(DISTINCT ed.year) as year_span
            FROM extracted_documents ed
            LEFT JOIN company_master cm ON ed.company_name = ANY(cm.document_company_names)
            WHERE cm.id IS NULL
            AND ed.company_name IS NOT NULL
            AND ed.company_name != ''
            AND ed.company_name != 'None'
            GROUP BY ed.company_name
            ORDER BY COUNT(*) DESC;
            """
        ]
        
        for view_sql in views:
            await conn.execute(view_sql)
        
        print("✅ Created analytical views")
        
        # Create functions for common operations
        functions = [
            # Function to generate company slug
            """
            CREATE OR REPLACE FUNCTION generate_company_slug(company_name TEXT)
            RETURNS TEXT AS $$
            BEGIN
                RETURN lower(
                    regexp_replace(
                        regexp_replace(company_name, '[^a-zA-Z0-9\\s]', '', 'g'),
                        '\\s+', '-', 'g'
                    )
                );
            END;
            $$ LANGUAGE plpgsql IMMUTABLE;
            """,
            
            # Function to update company document stats
            """
            CREATE OR REPLACE FUNCTION update_company_document_stats()
            RETURNS TRIGGER AS $$
            BEGIN
                UPDATE company_master SET
                    document_count = (
                        SELECT COUNT(*)
                        FROM extracted_documents ed
                        WHERE ed.company_name = ANY(company_master.document_company_names)
                    ),
                    first_document_year = (
                        SELECT MIN(ed.year)
                        FROM extracted_documents ed
                        WHERE ed.company_name = ANY(company_master.document_company_names)
                    ),
                    last_document_year = (
                        SELECT MAX(ed.year)
                        FROM extracted_documents ed
                        WHERE ed.company_name = ANY(company_master.document_company_names)
                    ),
                    updated_at = NOW()
                WHERE id = NEW.company_id;
                
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
        ]
        
        for function_sql in functions:
            await conn.execute(function_sql)
        
        print("✅ Created utility functions")
        
        print(f"\n🎉 Company Master Table System Created!")
        print(f"   📊 Central company information repository")
        print(f"   🔗 Handles multiple data sources and identifiers")  
        print(f"   📈 Tracks document and market data relationships")
        print(f"   🔍 Optimized for fast lookups and analysis")
        
    finally:
        await conn.close()

async def show_table_structure():
    """Show the structure of the new company master system"""
    
    print("\n📋 COMPANY MASTER TABLE STRUCTURE")
    print("=" * 50)
    print("""
🏢 company_master (Main Table)
   ├── Core Identity: company_name, slug, clean_name
   ├── Stock Symbols: primary_ticker, yahoo_symbol, bloomberg_symbol
   ├── Exchange Info: primary_exchange, currency, listing_status
   ├── Geography: country, region, headquarters
   ├── Classification: sector, industry, GICS codes
   ├── Size & Status: market_cap, employees, founding
   ├── Document Tracking: doc_count, years, types, name_variants
   ├── Data Sources: MFN IDs, URLs, IR websites
   └── Quality Control: confidence scores, validation dates

📝 company_aliases
   └── Alternative names, trading names, former names

🔗 company_relationships  
   └── Parent/subsidiary, mergers, spinoffs

📈 Updated daily_price_data
   └── Links to company_master.id

📊 Views Created:
   ├── nordic_companies_with_data
   ├── high_priority_companies
   └── unmatched_document_companies
""")
    
    print("\n💡 Next Steps:")
    print("   1. Populate from existing data")
    print("   2. Add document company mappings") 
    print("   3. Enhance with external data sources")
    print("   4. Build automated matching algorithms")

if __name__ == "__main__":
    print("🏗️  COMPANY MASTER TABLE CREATION")
    print("Building comprehensive company information system")
    print("=" * 60)
    
    asyncio.run(create_company_master_table())
    asyncio.run(show_table_structure())