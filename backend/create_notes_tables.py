#!/usr/bin/env python3
"""
Create company notes/research journal tables.

Track your research, thoughts, and observations over time for each company.

Usage:
    python create_notes_tables.py
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def create_tables():
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Company notes - research journal
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS company_notes (
                id SERIAL PRIMARY KEY,
                company_id UUID NOT NULL REFERENCES company_master(id),

                -- Note content
                note_type VARCHAR(30) NOT NULL DEFAULT 'general',
                title VARCHAR(200),
                content TEXT NOT NULL,

                -- Context
                source VARCHAR(100),  -- 'earnings_call', 'annual_report', 'news', 'own_research', etc.
                source_url TEXT,

                -- Sentiment/action
                sentiment VARCHAR(20),  -- 'bullish', 'bearish', 'neutral', 'cautious'
                action_item BOOLEAN DEFAULT FALSE,
                action_completed BOOLEAN DEFAULT FALSE,

                -- Timestamps
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),

                -- For linking to specific reports/events
                related_date DATE,  -- e.g., earnings date this note refers to

                -- Tags for filtering
                tags TEXT[]  -- ['thesis', 'risk', 'catalyst', 'valuation', etc.]
            )
        """)
        print("✅ Created company_notes table")

        # Indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notes_company
            ON company_notes(company_id, created_at DESC)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notes_type
            ON company_notes(note_type)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_notes_action
            ON company_notes(action_item)
            WHERE action_item = TRUE AND action_completed = FALSE
        """)
        print("✅ Created indexes")

        # View for easy querying with company info
        await conn.execute("""
            CREATE OR REPLACE VIEW company_notes_view AS
            SELECT
                n.id,
                n.company_id,
                cm.company_name,
                cm.primary_ticker,
                n.note_type,
                n.title,
                n.content,
                n.source,
                n.sentiment,
                n.action_item,
                n.action_completed,
                n.created_at,
                n.updated_at,
                n.related_date,
                n.tags
            FROM company_notes n
            JOIN company_master cm ON n.company_id = cm.id
            ORDER BY n.created_at DESC
        """)
        print("✅ Created company_notes_view")

        print("\n" + "="*60)
        print("NOTES SYSTEM READY")
        print("="*60)
        print("""
Note types available:
  - general     : General observations
  - thesis      : Investment thesis points
  - risk        : Risk factors identified
  - catalyst    : Potential catalysts
  - earnings    : Earnings call notes
  - valuation   : Valuation thoughts
  - management  : Management observations
  - competitor  : Competitive analysis
  - news        : News/events

Use the CLI:
    python manage_notes.py add "Volvo" "Strong Q4 margins surprised"
    python manage_notes.py add "Volvo" "CEO mentioned expansion" --type earnings --sentiment bullish
    python manage_notes.py list "Volvo"
    python manage_notes.py list "Volvo" --type thesis
    python manage_notes.py actions  # Show all action items
""")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(create_tables())
