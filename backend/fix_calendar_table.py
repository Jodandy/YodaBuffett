"""
Fix calendar table schema - add dividend fields
"""
import asyncio
from shared.database import AsyncSessionLocal, Base
from nordic_ingestion.models import *

async def recreate_calendar_table():
    from sqlalchemy import text
    
    print("🔄 Recreating calendar table with dividend fields...")
    
    # Drop the table
    async with AsyncSessionLocal() as session:
        try:
            await session.execute(text("DROP TABLE IF EXISTS nordic_calendar_events CASCADE"))
            await session.commit()
            print("🗑️  Dropped old calendar table")
        except Exception as e:
            print(f"⚠️  Could not drop table: {e}")
    
    # Create the table with new schema using engine directly
    from shared.database import engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[NordicCalendarEvent.__table__])
    
    print("✅ Calendar table recreated with dividend fields")
    print("   - dividend_amount (Numeric)")
    print("   - dividend_currency (VARCHAR)")
    print("   - dividend_type (VARCHAR)")
    print("   - ex_dividend_date (Date)")
    print("   - record_date (Date)")
    print("   - payment_date (Date)")

if __name__ == "__main__":
    asyncio.run(recreate_calendar_table())