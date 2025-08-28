#!/usr/bin/env python3
"""
Quick script to update Volvo's RSS feed URL for testing
"""
import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDataSource
from sqlalchemy import select, update

async def update_volvo_rss():
    """Update Volvo's RSS feed to a working test URL"""
    async with AsyncSessionLocal() as db:
        # Find Volvo company
        result = await db.execute(
            select(NordicDataSource)
            .join(NordicDataSource.company)
            .where(
                NordicDataSource.company.has(name="Volvo Group"),
                NordicDataSource.source_type == "rss_feed"
            )
        )
        
        volvo_source = result.scalar_one_or_none()
        
        if not volvo_source:
            print("❌ Volvo RSS source not found")
            return
        
        # Update the RSS URL to a working test feed
        volvo_source.config = {
            "urls": ["https://rss.cnn.com/rss/money_news_international.rss"]
        }
        
        await db.commit()
        print("✅ Updated Volvo RSS feed URL for testing")
        print("🔗 New URL: https://rss.cnn.com/rss/money_news_international.rss")

if __name__ == "__main__":
    asyncio.run(update_volvo_rss())