#!/usr/bin/env python3
"""
Simple check for existing embeddings
"""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from sqlalchemy import text


async def main():
    print("🔍 Simple Embedding Check")
    print("="*50)
    
    async with AsyncSessionLocal() as db:
        # First, check if document_embeddings table exists and get its structure
        try:
            result = await db.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'document_embeddings' 
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            
            if columns:
                print("📊 document_embeddings table structure:")
                for col in columns:
                    print(f"   {col.column_name} ({col.data_type})")
                
                # Count embeddings
                count_result = await db.execute(text("SELECT COUNT(*) FROM document_embeddings"))
                count = count_result.scalar()
                print(f"\n📈 Total embeddings: {count:,}")
                
                if count > 0:
                    # Show sample
                    sample_result = await db.execute(text("""
                        SELECT * FROM document_embeddings LIMIT 2
                    """))
                    
                    print("\n📋 Sample embedding records:")
                    for i, row in enumerate(sample_result.fetchall(), 1):
                        print(f"   Record {i}:")
                        for j, col in enumerate(columns):
                            value = row[j]
                            if col.column_name == 'embedding' and hasattr(value, '__len__'):
                                print(f"     {col.column_name}: [{len(value)} dimensions]")
                            else:
                                print(f"     {col.column_name}: {str(value)[:50]}...")
                
            else:
                print("❌ document_embeddings table not found")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        # Also check nordic_documents
        try:
            nd_result = await db.execute(text("SELECT COUNT(*) FROM nordic_documents"))
            nd_count = nd_result.scalar()
            print(f"\n📄 nordic_documents: {nd_count:,} documents")
            
            # Sample of companies
            companies_result = await db.execute(text("""
                SELECT DISTINCT name 
                FROM nordic_documents 
                WHERE name IS NOT NULL 
                ORDER BY name 
                LIMIT 10
            """))
            
            companies = [row.name for row in companies_result.fetchall()]
            print(f"\n🏢 Sample companies: {', '.join(companies[:5])}...")
            
        except Exception as e:
            print(f"❌ Error checking nordic_documents: {e}")


if __name__ == "__main__":
    asyncio.run(main())