"""
Test the document downloader on our cataloged documents
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument
from nordic_ingestion.storage.document_catalog import get_catalog_summary
from sqlalchemy import select

async def test_downloader():
    print("📥 Testing Document Downloader")
    print("=" * 60)
    
    # First, show what we have cataloged
    catalog_summary = await get_catalog_summary()
    print(f"📊 CATALOG SUMMARY:")
    print(f"   📋 Catalogued (ready to download): {catalog_summary['catalogued']}")
    print(f"   ⏳ Pending download: {catalog_summary['pending_download']}")
    print(f"   ✅ Downloaded: {catalog_summary['downloaded']}")
    print(f"   ❌ Failed downloads: {catalog_summary['failed']}")
    print(f"   📊 Total discovered: {catalog_summary['total_discovered']}")
    
    # Show some example documents ready for download
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(NordicDocument.title, NordicDocument.document_type, NordicDocument.metadata_)
            .where(NordicDocument.processing_status == "catalogued")
            .limit(5)
        )
        catalogued_docs = result.all()
        
        print(f"\n📄 DOCUMENTS READY FOR DOWNLOAD:")
        for i, (title, doc_type, metadata) in enumerate(catalogued_docs, 1):
            pdf_url = metadata.get('pdf_url', 'No URL') if metadata else 'No metadata'
            print(f"   {i}. {doc_type} | {title[:50]}...")
            print(f"      URL: {pdf_url}")
    
    if catalog_summary['catalogued'] > 0:
        print(f"\n🚀 Ready to test downloader!")
        print(f"We have {catalog_summary['catalogued']} documents ready to download.")
        
        # Ask if we should proceed with downloading
        print(f"\n⚡ Let's test downloading a few documents...")
        return True
    else:
        print(f"\n⚠️  No documents available for download.")
        print(f"Run the MFN collector first to catalog some documents.")
        return False

if __name__ == "__main__":
    asyncio.run(test_downloader())