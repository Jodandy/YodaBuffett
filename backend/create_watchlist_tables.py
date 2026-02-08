#!/usr/bin/env python3
"""
Create watchlist tables for targeted LLM analysis.

This allows you to manually select which companies get expensive LLM analysis,
keeping costs down by only analyzing companies you're actively researching.

Usage:
    python create_watchlist_tables.py
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def create_tables():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Watchlist table - companies flagged for deep analysis
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_watchlist (
                id SERIAL PRIMARY KEY,
                company_id UUID NOT NULL REFERENCES company_master(id),

                -- Analysis flags
                llm_analysis_enabled BOOLEAN DEFAULT TRUE,
                priority INTEGER DEFAULT 1,  -- 1=high, 2=medium, 3=low

                -- Tracking
                added_at TIMESTAMP DEFAULT NOW(),
                added_reason TEXT,  -- Why you're watching this company
                notes TEXT,

                -- Status
                last_analyzed_at TIMESTAMP,
                analysis_count INTEGER DEFAULT 0,

                UNIQUE(company_id)
            )
        """)
        print("✅ Created analysis_watchlist table")

        # Index for quick lookups
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_watchlist_enabled
            ON analysis_watchlist(llm_analysis_enabled)
            WHERE llm_analysis_enabled = TRUE
        """)
        print("✅ Created index on llm_analysis_enabled")

        # Analysis results table - stores LLM analysis outputs
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS llm_analysis_results (
                id SERIAL PRIMARY KEY,
                company_id UUID NOT NULL REFERENCES company_master(id),

                -- Analysis metadata
                analysis_type VARCHAR(50) NOT NULL,  -- 'fat_pitch', 'fraud_check', 'deep_dive', etc.
                analysis_date DATE NOT NULL,
                model_used VARCHAR(100),  -- 'gpt-4', 'claude-3-opus', etc.

                -- Results
                score DECIMAL(5,2),  -- If applicable (e.g., fat pitch score)
                recommendation VARCHAR(20),  -- 'INVEST', 'PASS', 'WATCHLIST', 'NEEDS_FRAUD_CHECK'
                summary TEXT,  -- Brief summary
                full_analysis JSONB,  -- Complete structured analysis

                -- Cost tracking
                input_tokens INTEGER,
                output_tokens INTEGER,
                estimated_cost_usd DECIMAL(10,4),

                -- Metadata
                created_at TIMESTAMP DEFAULT NOW(),

                UNIQUE(company_id, analysis_type, analysis_date)
            )
        """)
        print("✅ Created llm_analysis_results table")

        # Index for finding latest analysis
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_analysis_company_date
            ON llm_analysis_results(company_id, analysis_date DESC)
        """)
        print("✅ Created index on analysis results")

        # View for easy querying of watchlist with company info
        await conn.execute("""
            CREATE OR REPLACE VIEW watchlist_companies AS
            SELECT
                w.id as watchlist_id,
                w.company_id,
                cm.company_name,
                cm.primary_ticker,
                cm.sector,
                cm.country,
                w.llm_analysis_enabled,
                w.priority,
                w.added_at,
                w.added_reason,
                w.notes,
                w.last_analyzed_at,
                w.analysis_count,
                -- Latest analysis info
                (SELECT recommendation FROM llm_analysis_results
                 WHERE company_id = w.company_id
                 ORDER BY analysis_date DESC LIMIT 1) as latest_recommendation,
                (SELECT analysis_date FROM llm_analysis_results
                 WHERE company_id = w.company_id
                 ORDER BY analysis_date DESC LIMIT 1) as latest_analysis_date
            FROM analysis_watchlist w
            JOIN company_master cm ON w.company_id = cm.id
            ORDER BY w.priority ASC, w.added_at DESC
        """)
        print("✅ Created watchlist_companies view")

        print("\n" + "="*60)
        print("WATCHLIST SYSTEM READY")
        print("="*60)
        print("""
Use the CLI to manage your watchlist:
    python manage_watchlist.py add "Volvo"
    python manage_watchlist.py add "Ericsson" --reason "Potential turnaround"
    python manage_watchlist.py list
    python manage_watchlist.py remove "Volvo"

Or query directly:
    SELECT * FROM watchlist_companies WHERE llm_analysis_enabled = TRUE;
""")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(create_tables())
