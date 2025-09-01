#!/usr/bin/env python3
"""
Query the database for a specific PDF URL
"""
import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument, NordicCompany
from sqlalchemy import select, and_

async def main():
    pdf_url = "https://storage.mfn.se/2eac4101-1331-4b3f-8194-5717b70240a0/abelco-h1-2025.pdf"
    
    async with AsyncSessionLocal() as db:
        # Query for documents with this PDF URL
        result = await db.execute(
            select(NordicDocument, NordicCompany.name).join(
                NordicCompany, NordicDocument.company_id == NordicCompany.id
            ).where(
                NordicDocument.metadata_['pdf_url'].astext == pdf_url
            )
        )
        
        documents = result.fetchall()
        
        if documents:
            print(f"✅ Found {len(documents)} document(s) with PDF URL:")
            print(f"   {pdf_url}")
            print()
            
            for doc, company_name in documents:
                print(f"📄 Document ID: {doc.id}")
                print(f"🏢 Company: {company_name}")
                print(f"📋 Title: {doc.title}")
                print(f"📅 Date: {doc.date_published}")
                print(f"🏷️  Type: {doc.document_type}")
                print(f"📊 Metadata: {doc.metadata_}")
                print("-" * 60)
                
        else:
            print(f"❌ No documents found with PDF URL:")
            print(f"   {pdf_url}")
            print()
            
            # Try partial match on the filename
            filename = "abelco-h1-2025.pdf"
            print(f"🔍 Trying partial match on filename: {filename}")
            
            result = await db.execute(
                select(NordicDocument, NordicCompany.name).join(
                    NordicCompany, NordicDocument.company_id == NordicCompany.id
                ).where(
                    NordicDocument.metadata_['pdf_url'].astext.contains(filename)
                )
            )
            
            partial_docs = result.fetchall()
            if partial_docs:
                print(f"✅ Found {len(partial_docs)} document(s) with filename match:")
                for doc, company_name in partial_docs:
                    print(f"   🏢 {company_name}: {doc.metadata_.get('pdf_url', 'No URL')}")
            else:
                print(f"❌ No documents found with filename: {filename}")

if __name__ == "__main__":
    asyncio.run(main())