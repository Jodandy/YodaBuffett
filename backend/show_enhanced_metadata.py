"""
Show the enhanced metadata that's perfect for LLM filtering
"""
import asyncio
import sys
import os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from nordic_ingestion.models import NordicDocument
from sqlalchemy import select

async def show_enhanced_metadata():
    print("🧠 Enhanced Metadata for LLM Filtering")
    print("=" * 80)
    
    async with AsyncSessionLocal() as db:
        # Get all documents with their enhanced metadata
        result = await db.execute(
            select(NordicDocument.title, NordicDocument.document_type, NordicDocument.metadata_)
            .order_by(NordicDocument.created_at)
        )
        documents = result.all()
        
        for i, (title, doc_type, metadata) in enumerate(documents, 1):
            print(f"\n📄 DOCUMENT {i}: {doc_type.upper()}")
            print(f"Title: {title[:70]}...")
            
            if metadata:
                # Show key metadata for LLM filtering
                print(f"🏷️  Classification: {metadata.get('document_classification', 'unknown')}")
                print(f"🌍 Language: {metadata.get('title_language', 'unknown')}")
                print(f"📊 Financial Data: {metadata.get('contains_financial_data', False)}")
                print(f"⭐ Relevance: {metadata.get('llm_filter_context', {}).get('suggested_relevance', 'unknown')}")
                print(f"🎯 Purpose: {metadata.get('llm_filter_context', {}).get('document_purpose', 'unknown')}")
                
                # Financial keywords found
                keywords = metadata.get('financial_keywords', [])
                if keywords:
                    print(f"🔑 Keywords: {', '.join(keywords[:5])}{'...' if len(keywords) > 5 else ''}")
                
                # Key phrases for context
                key_phrases = metadata.get('llm_filter_context', {}).get('key_phrases', [])
                if key_phrases:
                    print(f"💬 Key Phrases: {', '.join(key_phrases[:3])}")
                
                # Content preview for LLM
                content_preview = metadata.get('content_preview')
                if content_preview:
                    print(f"📝 Content Preview: {content_preview[:100]}...")
                
            print("-" * 60)
        
        print(f"\n🤖 LLM FILTERING USE CASES:")
        print(f"""
With this rich metadata, an LLM can easily filter documents by:

1. **Investment Relevance**:
   - High: Quarterly/Annual reports with financial data
   - Medium: Corporate actions, governance changes  
   - Low: PR announcements, awards

2. **Document Purpose**:
   - financial_reporting: Core investment analysis
   - corporate_action: M&A, strategic moves
   - governance: Leadership changes, AGM
   - corporate_pr: Awards, research prizes (often irrelevant)

3. **Financial Content**:
   - Documents with revenue, profit, EBITDA mentions
   - Contains actual financial figures (millions, billions)
   - Has quarterly/annual context

4. **Language Processing**:
   - Swedish vs English content
   - Key financial terms in both languages
   - Context-aware phrase extraction

5. **Smart Filtering Prompts**:
   "Filter out documents with purpose='corporate_pr' and relevance='low'"
   "Keep only documents with financial_data=true and classification='quarterly_report'"
   "Exclude awards/prizes but keep all financial reporting documents"
        """)

if __name__ == "__main__":
    asyncio.run(show_enhanced_metadata())