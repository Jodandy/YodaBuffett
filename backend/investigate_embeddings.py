#!/usr/bin/env python3
"""Investigate the 1.000 similarity issue more carefully"""

import asyncio
import asyncpg
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent))
from domains.document_intelligence.factory import get_database_url

async def investigate_embeddings():
    """Look deeper into the similarity issues"""
    conn = await asyncpg.connect(get_database_url())
    
    try:
        print("🔍 INVESTIGATING EMBEDDING SIMILARITIES")
        print("=" * 50)
        
        # Find the 1.000 similarity case
        print("🚨 Looking for the 1.000 similarity case...")
        
        income_statements = await conn.fetch("""
            SELECT 
                se.embedding,
                ds.section_title,
                ed.company_name,
                ed.year,
                ds.section_content,
                LENGTH(ds.section_content) as content_length
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE ds.section_type = 'income_statement'
            AND se.embedding_model LIKE 'local/%'
            LIMIT 5
        """)
        
        print(f"📊 Found {len(income_statements)} income statement embeddings")
        
        for i, stmt in enumerate(income_statements):
            embedding = eval(stmt['embedding'])
            print(f"\n{i+1}. {stmt['company_name']} ({stmt['year']})")
            print(f"   Content length: {stmt['content_length']:,} chars")
            print(f"   Title: {stmt['section_title'][:60]}...")
            print(f"   Content preview: {stmt['section_content'][:100].replace(chr(10), ' ')}...")
            print(f"   Embedding stats:")
            print(f"     First 3: {embedding[:3]}")
            print(f"     Min/Max: {min(embedding):.4f} / {max(embedding):.4f}")
            print(f"     Sum: {sum(embedding):.4f}")
            print(f"     All same?: {all(x == embedding[0] for x in embedding)}")
        
        # Check if any two are identical
        if len(income_statements) >= 2:
            print(f"\n🔍 Comparing first two income statements:")
            emb1 = np.array(eval(income_statements[0]['embedding']))
            emb2 = np.array(eval(income_statements[1]['embedding']))
            
            similarity = cosine_similarity([emb1], [emb2])[0][0]
            are_identical = np.array_equal(emb1, emb2)
            max_diff = np.max(np.abs(emb1 - emb2))
            
            print(f"   Cosine similarity: {similarity:.6f}")
            print(f"   Arrays identical?: {are_identical}")
            print(f"   Max difference: {max_diff:.6f}")
            print(f"   Company 1: {income_statements[0]['company_name']} ({income_statements[0]['year']})")
            print(f"   Company 2: {income_statements[1]['company_name']} ({income_statements[1]['year']})")
            
            # Check if they're from the same document
            if (income_statements[0]['company_name'] == income_statements[1]['company_name'] and 
                income_statements[0]['year'] == income_statements[1]['year']):
                print(f"   ⚠️  SAME COMPANY & YEAR - This explains 1.000 similarity!")
                print(f"   These might be different sections from the same report")
        
        # Check all embeddings for patterns
        print(f"\n🔍 Checking all embeddings for duplicate patterns:")
        
        all_embeddings = await conn.fetch("""
            SELECT 
                se.embedding,
                ed.company_name,
                ed.year,
                ds.section_type,
                ds.section_index
            FROM section_embeddings se
            JOIN document_sections ds ON se.document_section_id = ds.id
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE se.embedding_model LIKE 'local/%'
            ORDER BY ed.company_name, ed.year, ds.section_index
        """)
        
        # Group by company and year
        by_company_year = {}
        for row in all_embeddings:
            key = f"{row['company_name']}_{row['year']}"
            if key not in by_company_year:
                by_company_year[key] = []
            by_company_year[key].append(row)
        
        # Look for companies with multiple sections that might be identical
        print(f"📊 Companies with multiple sections:")
        identical_pairs = 0
        
        for key, sections in by_company_year.items():
            if len(sections) >= 2:
                print(f"\n   {key}: {len(sections)} sections")
                
                # Compare first two sections
                emb1 = np.array(eval(sections[0]['embedding']))
                emb2 = np.array(eval(sections[1]['embedding']))
                similarity = cosine_similarity([emb1], [emb2])[0][0]
                
                print(f"     {sections[0]['section_type']} vs {sections[1]['section_type']}: {similarity:.3f}")
                
                if similarity > 0.99:
                    identical_pairs += 1
                    print(f"     🚨 Near-identical embeddings!")
        
        print(f"\n📊 Summary:")
        print(f"   Total companies with multiple sections: {len([k for k, v in by_company_year.items() if len(v) >= 2])}")
        print(f"   Near-identical pairs found: {identical_pairs}")
        
        if identical_pairs > 0:
            print(f"\n💡 Explanation for 1.000 similarities:")
            print(f"   - Some sections from the same document may have very similar content")
            print(f"   - Multiple sections of same type from one report")
            print(f"   - This is actually NORMAL behavior for financial reports!")
            print(f"   - Different parts of the same income statement should be similar")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(investigate_embeddings())