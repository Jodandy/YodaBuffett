"""
Clean documents table and test with fresh data to see improved classification
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument

async def clean_and_test():
    print("🧹 Cleaning documents table to test improved classification...")
    
    # Clear existing documents 
    async with AsyncSessionLocal() as db:
        # Delete all documents
        from sqlalchemy import text
        result = await db.execute(text("DELETE FROM nordic_documents"))
        deleted_count = result.rowcount
        await db.commit()
        print(f"✅ Deleted {deleted_count} existing documents")
    
    print("\n🧪 Now running MFN collector to see improved classification...")

if __name__ == "__main__":
    asyncio.run(clean_and_test())