#!/usr/bin/env python3
"""
Create dimension scoring database tables.

Run this script to set up the dimensions system:
    python create_dimensions_tables.py
"""

import asyncio
import asyncpg
import json
import os


async def create_tables():
    """Create the dimension scoring tables."""
    try:
        # Read the schema
        schema_path = os.path.join(
            os.path.dirname(__file__),
            'domains/dimensions/db/schema.sql'
        )
        with open(schema_path, 'r') as f:
            schema_sql = f.read()

        # Connect to database
        database_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        print(f"Connecting to database...")

        conn = await asyncpg.connect(database_url)

        # Execute schema
        await conn.execute(schema_sql)
        print('Dimension tables created successfully!')

        # Insert initial dimension definitions
        await insert_initial_definitions(conn)

        # Verify tables exist
        tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name LIKE '%dimension%'
            ORDER BY table_name
        """)

        print(f"\nCreated tables:")
        for table in tables:
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table['table_name']}")
            print(f"  - {table['table_name']}: {count} rows")

        await conn.close()
        print('\nDimension system ready!')

    except Exception as e:
        print(f'Error: {e}')
        import traceback
        traceback.print_exc()


async def insert_initial_definitions(conn: asyncpg.Connection):
    """Insert the initial set of dimension definitions."""

    definitions = [
        {
            "dimension_code": "value",
            "display_name": "Value",
            "description": "Measures how undervalued a company is relative to its fundamentals and peers. "
                          "Higher scores indicate more attractive valuations.",
            "category": "fundamental",
            "data_sources": ["daily_fundamentals", "daily_price_data"],
            "update_frequency": "daily",
            "requires_external_api": False,
            "config": {
                "factors": ["trailing_pe", "price_to_book", "price_to_sales", "ev_to_ebitda", "price_52w_position"],
                "methodology": "percentile_weighted"
            }
        },
        {
            "dimension_code": "momentum",
            "display_name": "Momentum",
            "description": "Captures price and volume trends indicating directional strength. "
                          "Higher scores suggest positive momentum.",
            "category": "technical",
            "data_sources": ["daily_price_data", "knn_neighbors"],
            "update_frequency": "daily",
            "requires_external_api": False,
            "config": {
                "factors": ["rsi", "price_vs_sma", "volume_trend", "recent_returns", "knn_prediction"],
                "methodology": "weighted_signals"
            }
        },
        {
            "dimension_code": "quality",
            "display_name": "Quality",
            "description": "Assesses financial health, profitability, and operational efficiency. "
                          "Higher scores indicate stronger, more stable businesses.",
            "category": "fundamental",
            "data_sources": ["daily_fundamentals"],
            "update_frequency": "daily",
            "requires_external_api": False,
            "config": {
                "factors": ["roe", "profit_margin", "debt_to_equity", "current_ratio", "earnings_stability"],
                "methodology": "percentile_weighted"
            }
        },
        {
            "dimension_code": "sentiment",
            "display_name": "Sentiment",
            "description": "Analyzes communication patterns, document anomalies, and tone changes. "
                          "Uses AI to detect shifts in company communications.",
            "category": "ai_derived",
            "data_sources": ["document_embeddings", "section_embeddings", "extracted_documents"],
            "update_frequency": "daily",
            "requires_external_api": False,
            "config": {
                "methodology": "embedding_anomaly_detection",
                "baseline_window_days": 730,
                "anomaly_threshold": 0.3
            }
        },
        {
            "dimension_code": "risk",
            "display_name": "Risk",
            "description": "Quantifies downside exposure through volatility, drawdowns, and financial leverage. "
                          "Higher scores indicate LOWER risk (inverse scale for consistency).",
            "category": "fundamental",
            "data_sources": ["daily_price_data", "daily_fundamentals"],
            "update_frequency": "daily",
            "requires_external_api": False,
            "config": {
                "factors": ["volatility_20d", "max_drawdown_1y", "beta", "debt_to_equity"],
                "methodology": "inverse_risk_score",
                "note": "Score is inverted so higher = lower risk (consistent with other dimensions where higher = better)"
            }
        },
    ]

    for defn in definitions:
        await conn.execute("""
            INSERT INTO dimension_definitions
                (dimension_code, version, display_name, description, category,
                 config, data_sources, update_frequency, requires_external_api)
            VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (dimension_code, version) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                category = EXCLUDED.category,
                config = EXCLUDED.config,
                data_sources = EXCLUDED.data_sources,
                update_frequency = EXCLUDED.update_frequency,
                requires_external_api = EXCLUDED.requires_external_api,
                updated_at = NOW()
        """,
            defn["dimension_code"],
            defn["display_name"],
            defn["description"],
            defn["category"],
            json.dumps(defn["config"]),  # JSONB needs string
            defn["data_sources"],
            defn["update_frequency"],
            defn["requires_external_api"],
        )

    print(f"Inserted {len(definitions)} dimension definitions")


if __name__ == "__main__":
    asyncio.run(create_tables())
