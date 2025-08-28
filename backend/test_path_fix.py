"""
Test the fixed storage path by creating a single new document and downloading it
"""
import asyncio
import sys
import os
import uuid
from datetime import datetime
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from nordic_ingestion.storage.document_downloader import DocumentDownloader
from sqlalchemy import select

async def test_path_fix():
    print("🔧 Testing Fixed Storage Path")
    print("=" * 60)
    
    async with AsyncSessionLocal() as db:
        # Find Sandvik company
        result = await db.execute(
            select(NordicCompany).where(NordicCompany.name == "Sandvik AB")
        )
        sandvik = result.scalar_one_or_none()
        
        if not sandvik:
            print("❌ Sandvik company not found in database")
            return
        
        print(f"✅ Found company: {sandvik.name}")
        
        # Create a test document manually (simulating a new collection)
        test_document = NordicDocument(
            id=uuid.uuid4(),
            company_id=sandvik.id,
            document_type="press_release",
            report_period="Unknown",
            title="Test: Sandvik storage path fix verification document",
            source_url="https://mfn.se/test",
            storage_path=None,
            file_hash=None,
            language="en",
            ingestion_date=datetime.utcnow(),
            processing_status="catalogued",
            page_count=None,
            file_size_mb=None,
            metadata_={
                "pdf_url": "https://mb.cision.com/Main/208/4220477/3618929.pdf",  # Use existing URL
                "mfn_source": "https://mfn.se/all/a/sandvik?limit=5",  # Key for path extraction
                "discovery_date": datetime.utcnow().isoformat(),
                "test_document": True
            }
        )
        
        db.add(test_document)
        await db.commit()
        
        print(f"📄 Created test document: {test_document.id}")
        
        # Now test the downloader path generation
        async with DocumentDownloader() as downloader:
            print(f"🎯 Testing path generation...")
            
            # Test the _create_storage_path method
            storage_path = downloader._create_storage_path(test_document)
            print(f"📁 Generated path: {storage_path}")
            
            # Should be: data/documents/companies/SE/S/sandvik/2025/press_release/
            expected_parts = ["SE", "S", "sandvik", "2025", "press_release"]
            path_str = str(storage_path)
            
            all_parts_found = all(part in path_str for part in expected_parts)
            if all_parts_found:
                print(f"✅ Path generation FIXED! Contains all expected parts:")
                for part in expected_parts:
                    print(f"   ✓ {part}")
            else:
                print(f"❌ Path generation still broken. Missing parts:")
                for part in expected_parts:
                    if part not in path_str:
                        print(f"   ✗ {part}")
            
            # Test filename generation
            filename = downloader._generate_filename(test_document, test_document.metadata_["pdf_url"])
            print(f"📄 Generated filename: {filename}")
            
        # Clean up test document
        await db.delete(test_document)
        await db.commit()
        print(f"🧹 Cleaned up test document")

if __name__ == "__main__":
    asyncio.run(test_path_fix())