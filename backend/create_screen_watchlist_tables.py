#!/usr/bin/env python3
"""
Migration: Create screen watchlist tables

Simple watchlists for saving companies from screens.
No need to store fundamentals - just reference company_master and look up live data.

Run: python create_screen_watchlist_tables.py
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def migrate():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Watchlists table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS screen_watchlists (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name VARCHAR(100) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        print("✅ Created screen_watchlists table")

        # Watchlist items table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS screen_watchlist_items (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                watchlist_id UUID NOT NULL REFERENCES screen_watchlists(id) ON DELETE CASCADE,
                company_id UUID NOT NULL REFERENCES company_master(id) ON DELETE CASCADE,
                added_at TIMESTAMP DEFAULT NOW(),
                source VARCHAR(200),       -- Which screen found this, e.g. "Tier 2 + Good Cash + Compounder"
                notes TEXT,                -- Personal notes
                
                UNIQUE(watchlist_id, company_id)
            )
        """)
        print("✅ Created screen_watchlist_items table")

        # Index for fast lookups
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_screen_watchlist_items_watchlist
            ON screen_watchlist_items(watchlist_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_screen_watchlist_items_company
            ON screen_watchlist_items(company_id)
        """)
        print("✅ Created indexes")

        # Helpful view joining watchlist items with company info
        await conn.execute("""
            CREATE OR REPLACE VIEW screen_watchlist_view AS
            SELECT 
                sw.id as watchlist_id,
                sw.name as watchlist_name,
                swi.id as item_id,
                swi.company_id,
                cm.company_name,
                cm.primary_ticker,
                cm.yahoo_symbol,
                swi.added_at,
                swi.source,
                swi.notes
            FROM screen_watchlists sw
            JOIN screen_watchlist_items swi ON sw.id = swi.watchlist_id
            JOIN company_master cm ON swi.company_id = cm.id
            ORDER BY sw.name, swi.added_at DESC
        """)
        print("✅ Created screen_watchlist_view")

        # Verify
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name IN ('screen_watchlists', 'screen_watchlist_items')
        """)
        print(f"\n✅ Migration complete - {count} tables created")
        
        print("""
Example usage:
    
    -- Create a watchlist
    INSERT INTO screen_watchlists (name, description) 
    VALUES ('Quality Jan 2025', 'Tier 2-3 + Good/Excellent Cash');
    
    -- Add companies (get watchlist_id from above)
    INSERT INTO screen_watchlist_items (watchlist_id, company_id, source)
    SELECT 'your-watchlist-uuid', id, 'Tier 2 + Good Cash + Compounder'
    FROM company_master WHERE primary_ticker = 'VOLV-B';
    
    -- View your watchlists
    SELECT * FROM screen_watchlist_view;
    
    -- Get price on added_at date
    SELECT swv.*, dpd.close_price as price_when_added
    FROM screen_watchlist_view swv
    JOIN daily_price_data dpd ON dpd.symbol = swv.primary_ticker 
        AND dpd.date = swv.added_at::date;
""")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
