"""
Test just document storage to isolate the issue
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from nordic_ingestion.collectors.aggregator.mfn_collector import MFNNewsItem
from nordic_ingestion.storage.document_catalog import catalog_mfn_documents
from datetime import datetime

async def test_document_storage_only():
    print("🔍 Testing document storage only...")
    
    # Create a fake MFN item for testing
    fake_item = MFNNewsItem(
        company_name="Volvo Group",
        title="Test Financial Document",
        date_published=datetime.now(),
        content="Test content",
        pdf_urls=["https://example.com/test.pdf", "https://example.com/test2.pdf"],
        source_url="https://mfn.se/test",
        document_type="earnings",
        calendar_info={"test": "data"}
    )
    
    items = [fake_item]
    
    print(f"📄 Created test item: {fake_item.company_name}")
    print(f"📄 PDFs: {len(fake_item.pdf_urls)}")
    
    # Test document storage
    print("\n💾 Testing document storage...")
    results = await catalog_mfn_documents(items)
    
    print(f"\n📊 Results: {results}")

if __name__ == "__main__":
    asyncio.run(test_document_storage_only())