#!/usr/bin/env python3
"""
Create table for labeled document embeddings.

Each embedding gets labeled with forward returns at multiple horizons,
enabling KNN-based prediction of future price direction.
"""

import asyncio
import asyncpg

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def create_tables():
    """Create the embedding labels table."""

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("Creating embedding_labels table...")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS embedding_labels (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                -- Link to source embedding
                section_embedding_id UUID REFERENCES section_embeddings(id),

                -- Company info for quick lookups
                company_name VARCHAR(200),
                symbol VARCHAR(50),

                -- Document context
                document_date DATE,
                section_type VARCHAR(50),
                year INTEGER,

                -- The embedding vector (copied for fast KNN)
                embedding VECTOR(384),

                -- Forward returns at multiple horizons
                return_30d FLOAT,
                return_60d FLOAT,
                return_90d FLOAT,

                -- Direction labels (bullish/bearish/neutral)
                label_30d VARCHAR(20),  -- 'bullish', 'bearish', 'neutral'
                label_60d VARCHAR(20),
                label_90d VARCHAR(20),

                -- Consensus label (if all horizons agree)
                consensus_label VARCHAR(20),
                consensus_strength FLOAT,  -- How many horizons agree (0.33, 0.67, 1.0)

                -- Price context
                price_at_document FLOAT,
                price_30d FLOAT,
                price_60d FLOAT,
                price_90d FLOAT,

                -- Metadata
                created_at TIMESTAMP DEFAULT NOW(),

                -- Prevent duplicates
                UNIQUE(section_embedding_id)
            )
        """)

        print("Creating indexes...")

        # Index for KNN searches (requires pgvector)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_embedding_labels_vector
            ON embedding_labels
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
        """)

        # Index for filtering by label
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_embedding_labels_consensus
            ON embedding_labels(consensus_label)
        """)

        # Index for filtering by section type
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_embedding_labels_section
            ON embedding_labels(section_type)
        """)

        # Index for filtering by company
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_embedding_labels_company
            ON embedding_labels(company_name)
        """)

        # Index for date range queries
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_embedding_labels_date
            ON embedding_labels(document_date)
        """)

        print("✅ embedding_labels table created successfully!")

        # Show table info
        count = await conn.fetchval("SELECT COUNT(*) FROM embedding_labels")
        print(f"   Current rows: {count}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(create_tables())
