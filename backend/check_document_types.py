"""
Check the document types that were stored
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument
from sqlalchemy import select

async def check_document_types():
    print("📊 Checking stored document types...")
    
    async with AsyncSessionLocal() as db:
        # Get all documents with their types and titles
        result = await db.execute(
            select(NordicDocument.document_type, NordicDocument.title, NordicDocument.metadata_)
            .order_by(NordicDocument.created_at)
        )
        documents = result.all()
        
        print(f"📄 Found {len(documents)} documents:")
        print("=" * 80)
        
        type_counts = {}
        for doc_type, title, metadata in documents:
            # Count types
            if doc_type not in type_counts:
                type_counts[doc_type] = 0
            type_counts[doc_type] += 1
            
            # Show document details
            pdf_url = metadata.get('pdf_url', 'No URL') if metadata else 'No metadata'
            short_url = pdf_url[-40:] if len(pdf_url) > 40 else pdf_url
            
            print(f"🏷️  {doc_type:<18} | {title:<30} | ...{short_url}")
        
        print("=" * 80)
        print("📊 Document type summary:")
        for doc_type, count in type_counts.items():
            print(f"   {doc_type}: {count} documents")
        
        # Check for Swedish quarterly reports specifically
        quarterly_docs = [doc for doc_type, title, metadata in documents if doc_type == "quarterly_report"]
        if quarterly_docs:
            print(f"\n✅ SUCCESS: Found {len(quarterly_docs)} quarterly_report documents!")
            print("📋 This means Swedish classification is working!")
        else:
            print(f"\n❌ No quarterly reports found - Swedish classification may not be working")

if __name__ == "__main__":
    asyncio.run(check_document_types())