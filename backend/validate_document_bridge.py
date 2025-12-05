#!/usr/bin/env python3
"""
Quick validation script for document trading bridge.
"""

import asyncio
import asyncpg

async def main():
    print("🔗 Validating Document Trading Bridge...")
    
    try:
        # Test database connection
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Check document embeddings exist
        doc_count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE se.embedding_model LIKE 'local/%'
        """)
        
        print(f"✅ Document embeddings: {doc_count:,}")
        
        # Check market data exists
        market_count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM daily_price_data
            WHERE date >= '2024-01-01'
        """)
        
        print(f"✅ Market data points: {market_count:,}")
        
        # Test strategy import
        from services.technical_analysis.strategies.document_anomaly_strategy import DocumentAnomalyStrategy
        strategy = DocumentAnomalyStrategy()
        print(f"✅ Strategy created: {strategy.name}")
        
        await conn.close()
        print("🎯 Document Trading Bridge validation successful!")
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())