#!/usr/bin/env python3
"""
Migration: Create portfolio and holdings tables for the Hub app.
Run: python create_portfolio_tables.py
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://yodabuffett:password@localhost:5432/yodabuffett"
)


async def migrate():
    """Create portfolio tables with IF NOT EXISTS for idempotency."""
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        print("Creating portfolio tables...")

        # 1. Portfolios table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolios (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                -- Ownership (for future multi-user)
                user_id UUID,

                -- Portfolio info
                name VARCHAR(200) NOT NULL,
                description TEXT,
                currency VARCHAR(10) DEFAULT 'SEK',

                -- Status
                is_active BOOLEAN DEFAULT true,
                is_default BOOLEAN DEFAULT false,

                -- Timestamps
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),

                CONSTRAINT portfolio_name_length CHECK (LENGTH(name) >= 1)
            )
        """)
        print("  ✓ portfolios table")

        # 2. Holdings table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_holdings (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                portfolio_id UUID NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,

                -- Stock reference
                company_id UUID,
                symbol VARCHAR(30) NOT NULL,
                company_name VARCHAR(200),

                -- Position details
                quantity DECIMAL(15,4) NOT NULL,
                purchase_price DECIMAL(15,4) NOT NULL,
                purchase_date DATE NOT NULL,
                currency VARCHAR(10) NOT NULL,

                -- Optional
                notes TEXT,

                -- Timestamps
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),

                CONSTRAINT holding_quantity_positive CHECK (quantity > 0),
                CONSTRAINT holding_price_positive CHECK (purchase_price >= 0)
            )
        """)
        print("  ✓ portfolio_holdings table")

        # 3. Indexes
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_portfolios_user
            ON portfolios(user_id) WHERE user_id IS NOT NULL
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_holdings_portfolio
            ON portfolio_holdings(portfolio_id)
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_holdings_symbol
            ON portfolio_holdings(symbol)
        """)
        print("  ✓ indexes")

        # 4. Update timestamp trigger function
        await conn.execute("""
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """)

        # 5. Triggers for updated_at
        await conn.execute("""
            DROP TRIGGER IF EXISTS portfolios_updated_at ON portfolios
        """)
        await conn.execute("""
            CREATE TRIGGER portfolios_updated_at
                BEFORE UPDATE ON portfolios
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
        """)

        await conn.execute("""
            DROP TRIGGER IF EXISTS holdings_updated_at ON portfolio_holdings
        """)
        await conn.execute("""
            CREATE TRIGGER holdings_updated_at
                BEFORE UPDATE ON portfolio_holdings
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column()
        """)
        print("  ✓ update triggers")

        # 6. View for holdings with current prices
        await conn.execute("""
            CREATE OR REPLACE VIEW portfolio_holdings_with_prices AS
            SELECT
                h.id,
                h.portfolio_id,
                h.company_id,
                h.symbol,
                h.company_name,
                h.quantity,
                h.purchase_price,
                h.purchase_date,
                h.currency,
                h.notes,
                h.created_at,
                h.updated_at,
                dpd.close_price as current_price,
                dpd.date as price_date,
                (h.quantity * h.purchase_price) as cost_basis,
                (h.quantity * COALESCE(dpd.close_price, h.purchase_price)) as current_value,
                (h.quantity * COALESCE(dpd.close_price, h.purchase_price)) - (h.quantity * h.purchase_price) as gain_loss,
                CASE
                    WHEN h.purchase_price > 0
                    THEN ((COALESCE(dpd.close_price, h.purchase_price) - h.purchase_price) / h.purchase_price * 100)
                    ELSE 0
                END as gain_loss_percent
            FROM portfolio_holdings h
            LEFT JOIN LATERAL (
                SELECT close_price, date
                FROM daily_price_data dpd
                WHERE dpd.symbol = h.symbol
                ORDER BY dpd.date DESC
                LIMIT 1
            ) dpd ON true
        """)
        print("  ✓ portfolio_holdings_with_prices view")

        print("\n✅ Portfolio tables created successfully!")

        # Show table info
        tables = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name IN ('portfolios', 'portfolio_holdings')
            AND table_schema = 'public'
        """)
        print(f"\nTables created: {[t['table_name'] for t in tables]}")

    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        raise

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(migrate())
