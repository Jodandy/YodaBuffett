"""
Test the document downloader on cataloged documents
Download a few documents to verify the downloader works correctly
"""
import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument
from nordic_ingestion.storage.document_downloader import DocumentDownloader
from nordic_ingestion.storage.document_catalog import get_catalog_summary
from sqlalchemy import select

async def test_document_downloader():
    print("📥 Testing Document Downloader")
    print("=" * 60)
    
    # Show current catalog status
    catalog_summary = await get_catalog_summary()
    print(f"📊 CATALOG STATUS:")
    print(f"   📋 Catalogued (ready to download): {catalog_summary['catalogued']}")
    print(f"   ⏳ Pending download: {catalog_summary['pending_download']}")
    print(f"   ✅ Downloaded: {catalog_summary['downloaded']}")
    print(f"   ❌ Failed downloads: {catalog_summary['failed']}")
    
    if catalog_summary['catalogued'] == 0:
        print(f"\n⚠️  No documents available for download.")
        print(f"Run the MFN collector first to catalog some documents.")
        return
    
    # Get a few documents to test download
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(NordicDocument)
            .where(NordicDocument.processing_status == "catalogued")
            .limit(3)  # Just test 3 documents
        )
        catalogued_docs = result.scalars().all()
        
        print(f"\n🎯 TESTING DOWNLOAD OF {len(catalogued_docs)} DOCUMENTS:")
        
        # Download each document
        async with DocumentDownloader() as downloader:
            for i, doc in enumerate(catalogued_docs, 1):
                pdf_url = doc.metadata_.get('pdf_url') if doc.metadata_ else None
                if not pdf_url:
                    print(f"   {i}. ❌ No PDF URL found for: {doc.title[:50]}...")
                    continue
                
                print(f"\n   {i}. 📄 {doc.document_type} | {doc.title[:50]}...")
                print(f"      🔗 URL: {pdf_url}")
                print(f"      📥 Downloading...")
                
                # Download the document
                download_result = await downloader.download_document(
                    str(doc.id), 
                    pdf_url, 
                    db
                )
                
                if download_result["success"]:
                    print(f"      ✅ SUCCESS: {download_result['file_size_mb']:.2f}MB")
                    print(f"      💾 Stored: {download_result['file_path']}")
                else:
                    print(f"      ❌ FAILED: {download_result['error']}")
    
    # Show updated catalog status
    print(f"\n" + "=" * 60)
    final_summary = await get_catalog_summary()
    print(f"📊 FINAL CATALOG STATUS:")
    print(f"   📋 Catalogued (ready to download): {final_summary['catalogued']}")
    print(f"   ⏳ Pending download: {final_summary['pending_download']}")
    print(f"   ✅ Downloaded: {final_summary['downloaded']}")
    print(f"   ❌ Failed downloads: {final_summary['failed']}")
    
    if final_summary['downloaded'] > catalog_summary['downloaded']:
        new_downloads = final_summary['downloaded'] - catalog_summary['downloaded']
        print(f"\n🎉 SUCCESS: Downloaded {new_downloads} new documents!")
        print(f"📁 Documents are stored in the configured storage path")
        print(f"🔍 File paths are stored in the database for easy retrieval")
    else:
        print(f"\n⚠️  No new documents were downloaded successfully")

if __name__ == "__main__":
    asyncio.run(test_document_downloader())