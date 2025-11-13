#!/usr/bin/env python3
"""
Analyze scale and performance requirements for vector embeddings
"""

import asyncio
import asyncpg

async def analyze_scale():
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    try:
        # Get current document stats
        stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_docs,
                COUNT(*) FILTER (WHERE extraction_status = 'completed') as extracted_docs,
                AVG(text_length) as avg_text_length,
                SUM(text_length) as total_text_chars
            FROM nordic_documents 
            WHERE processing_status = 'downloaded'
        ''')
        
        # Get chunk estimates from existing extracted documents
        chunk_stats = await conn.fetchrow('''
            SELECT 
                COUNT(*) as total_chunks,
                AVG(LENGTH(chunk_text)) as avg_chunk_length
            FROM extracted_document_chunks
        ''')
        
        print('📊 CURRENT DATA SCALE ANALYSIS')
        print('=' * 50)
        print(f'📄 Total downloaded documents: {stats["total_docs"]:,}')
        print(f'✅ Already extracted: {stats["extracted_docs"]:,}')
        
        if stats['avg_text_length']:
            print(f'📝 Average text length: {stats["avg_text_length"]:,.0f} characters')
        if stats['total_text_chars']:
            print(f'📚 Total text volume: {stats["total_text_chars"]:,.0f} characters')
        
        if chunk_stats['total_chunks']:
            print(f'🧩 Existing chunks: {chunk_stats["total_chunks"]:,}')
            print(f'📏 Average chunk size: {chunk_stats["avg_chunk_length"]:,.0f} characters')
        
        # Estimate future scale based on what we know
        if stats['avg_text_length'] and stats['total_docs'] > 1000:
            estimated_total_text = stats['total_docs'] * stats['avg_text_length']
            estimated_chunks = estimated_total_text / 8000  # 8K chars per chunk
            
            print(f'\n🔮 PROJECTED FULL SCALE:')
            print(f'📄 All {stats["total_docs"]:,} docs estimated text: {estimated_total_text:,.0f} characters')
            print(f'🧩 Estimated total chunks: {estimated_chunks:,.0f}')
            print(f'🔢 Vector embeddings needed: {estimated_chunks:,.0f} x 1536 dimensions')
            
            # Storage calculations
            storage_per_vector = 1536 * 4  # 4 bytes per float32
            total_vector_storage = estimated_chunks * storage_per_vector / (1024**3)  # GB
            
            print(f'\n💾 STORAGE REQUIREMENTS:')
            print(f'🗄️  Raw vectors: {total_vector_storage:.2f} GB')
            print(f'📚 Text + metadata: ~{total_vector_storage * 2:.2f} GB estimated')
            print(f'🔍 Indexes (HNSW): ~{total_vector_storage * 1.5:.2f} GB estimated')
            print(f'📊 Total storage need: ~{total_vector_storage * 4.5:.2f} GB')
            
            # Performance estimates
            print(f'\n⚡ PERFORMANCE ESTIMATES:')
            print(f'🔍 Vector search time: <200ms (with HNSW index)')
            print(f'💾 Memory for search: ~{total_vector_storage * 0.3:.2f} GB (index in RAM)')
            print(f'🔄 Concurrent searches: 100+ simultaneous queries')
            
            # Cost calculations (OpenAI pricing)
            tokens_per_chunk = 2000  # rough estimate
            total_tokens = estimated_chunks * tokens_per_chunk
            cost_per_1k_tokens = 0.00002  # text-embedding-3-small
            total_cost = (total_tokens / 1000) * cost_per_1k_tokens
            
            print(f'\n💰 EMBEDDING COST ESTIMATES:')
            print(f'🎫 Total tokens: {total_tokens:,.0f}')
            print(f'💵 Embedding cost: ${total_cost:.2f}')
            print(f'⏱️  Processing time: ~{estimated_chunks / 2000:.1f} hours (batch processing)')
            
            # Feasibility assessment
            print(f'\n🎯 FEASIBILITY ASSESSMENT:')
            if estimated_chunks < 1_000_000:
                print('✅ SCALE: Highly manageable (<1M vectors)')
            elif estimated_chunks < 10_000_000:
                print('⚡ SCALE: Large but very doable (<10M vectors)')
            else:
                print('⚠️  SCALE: Very large, need optimization')
                
            if total_vector_storage < 10:
                print('✅ STORAGE: Easy to handle (<10 GB)')
            elif total_vector_storage < 100:
                print('⚡ STORAGE: Manageable (<100 GB)')
            else:
                print('⚠️  STORAGE: Need distributed setup')
                
            if total_cost < 50:
                print('✅ COST: Very affordable (<$50)')
            elif total_cost < 500:
                print('⚡ COST: Reasonable (<$500)')
            else:
                print('⚠️  COST: Consider optimization strategies')
        
        else:
            # Use smaller sample for estimation
            print(f'\n📊 SAMPLE-BASED ESTIMATION:')
            if chunk_stats['total_chunks'] and chunk_stats['avg_chunk_length']:
                chunks_per_doc = chunk_stats['total_chunks'] / stats['extracted_docs'] if stats['extracted_docs'] > 0 else 50
                total_chunks_estimate = stats['total_docs'] * chunks_per_doc
                
                print(f'🧩 Estimated chunks per document: {chunks_per_doc:.1f}')
                print(f'📊 Total chunks estimate: {total_chunks_estimate:,.0f}')
                
                # Storage for this estimate
                storage_gb = (total_chunks_estimate * 1536 * 4) / (1024**3)
                print(f'💾 Estimated storage: ~{storage_gb * 4.5:.1f} GB total')
                
                # Cost estimate
                cost = (total_chunks_estimate * 2000 / 1000) * 0.00002
                print(f'💵 Estimated cost: ~${cost:.2f}')
                
                if total_chunks_estimate < 1_000_000 and storage_gb < 20 and cost < 200:
                    print('✅ VERDICT: Definitely feasible!')
                else:
                    print('⚡ VERDICT: Feasible with proper planning')
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_scale())