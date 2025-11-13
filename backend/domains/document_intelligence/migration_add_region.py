#!/usr/bin/env python3
"""
Add region column and improve metadata structure for multi-market scaling
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Add backend to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from domains.document_intelligence.factory import get_database_url

REGION_MAPPING = {
    'SE': 'nordic',    'NO': 'nordic',    'DK': 'nordic',    'FI': 'nordic',
    'US': 'north_america',    'CA': 'north_america',
    'DE': 'europe',    'FR': 'europe',    'GB': 'europe',    'NL': 'europe',
    'JP': 'asia',      'CN': 'asia',      'SG': 'asia',      'HK': 'asia'
}

async def migrate_database():
    """Add region column and enhance metadata structure"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔧 Adding region column to filings table...")
        
        # Add region column
        await conn.execute("""
            ALTER TABLE filings 
            ADD COLUMN IF NOT EXISTS region VARCHAR(20) DEFAULT 'nordic'
        """)
        
        # Update existing records with correct region
        await conn.execute("""
            UPDATE filings 
            SET region = CASE 
                WHEN country = 'SE' THEN 'nordic'
                WHEN country = 'NO' THEN 'nordic' 
                WHEN country = 'DK' THEN 'nordic'
                WHEN country = 'FI' THEN 'nordic'
                ELSE 'unknown'
            END
            WHERE region IS NULL OR region = 'nordic'
        """)
        
        # Create index for region-based queries
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_filings_region ON filings(region)
        """)
        
        # Add enhanced metadata columns
        await conn.execute("""
            ALTER TABLE filings 
            ADD COLUMN IF NOT EXISTS content_analysis JSONB DEFAULT '{}'
        """)
        
        print("✅ Database migration completed successfully!")
        
        # Show updated schema
        print("\n📊 Updated Schema:")
        columns = await conn.fetch("""
            SELECT column_name, data_type, column_default 
            FROM information_schema.columns 
            WHERE table_name = 'filings' 
            AND column_name IN ('region', 'content_analysis', 'country')
            ORDER BY ordinal_position
        """)
        
        for col in columns:
            print(f"  {col['column_name']}: {col['data_type']} (default: {col['column_default']})")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(migrate_database())