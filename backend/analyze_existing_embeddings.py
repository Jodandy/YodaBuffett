#!/usr/bin/env python3
"""
Analyze Temporal Anomalies from Existing Embeddings
Generate anomaly analysis on-the-fly from embeddings already in database
"""

import asyncio
import sys
import os
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
import numpy as np
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from shared.database import AsyncSessionLocal
from sqlalchemy import select, and_
from sqlalchemy.orm import selectinload


async def get_companies_with_embeddings():
    """Get companies that have document embeddings"""
    
    async with AsyncSessionLocal() as db:
        try:
            from sqlalchemy import text
            
            # Check if extracted_documents table exists and get the proper join
            result = await db.execute(text("""
                SELECT DISTINCT 
                    ed.company_name,
                    COUNT(de.id) as embedding_count
                FROM extracted_documents ed
                JOIN document_embeddings de ON de.extracted_document_id = ed.id  
                WHERE ed.company_name IS NOT NULL
                GROUP BY ed.company_name
                HAVING COUNT(de.id) >= 2  -- At least 2 embeddings for comparison
                ORDER BY ed.company_name
            """))
            
            companies = {}
            for row in result.fetchall():
                company_name = row.company_name
                companies[company_name] = company_name  # Use name as both key and value
            
            return companies
            
        except Exception as e:
            print(f"❌ Error getting companies: {e}")
            return {}


async def get_company_document_embeddings(company_name: str, days_back: int = 30):
    """Get document embeddings for a specific company"""
    
    async with AsyncSessionLocal() as db:
        try:
            from sqlalchemy import text
            
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            # Query through extracted_documents to get embeddings with correct join
            result = await db.execute(text("""
                SELECT 
                    ed.id,
                    ed.file_name as title,
                    ed.filing_date as publish_date,
                    ed.form_type as document_type,
                    de.embedding,
                    de.embedding_model
                FROM extracted_documents ed
                JOIN document_embeddings de ON de.extracted_document_id = ed.id
                WHERE ed.company_name = :company_name
                  AND ed.filing_date >= :cutoff_date
                ORDER BY ed.filing_date, ed.id
            """), {
                "company_name": company_name,
                "cutoff_date": cutoff_date.date()
            })
            
            documents = result.fetchall()
            
            # Extract embeddings and metadata
            doc_data = []
            for doc in documents:
                try:
                    # Convert embedding to numpy array - handle different storage formats
                    embedding_raw = doc.embedding
                    
                    if isinstance(embedding_raw, str):
                        # Parse JSON string format
                        import json
                        try:
                            embedding_vector = np.array(json.loads(embedding_raw))
                        except json.JSONDecodeError:
                            # Try eval as fallback for Python literal format
                            try:
                                embedding_vector = np.array(eval(embedding_raw))
                            except:
                                print(f"   ⚠️  Skipping document {doc.id}: could not parse embedding string")
                                continue
                    elif isinstance(embedding_raw, list):
                        embedding_vector = np.array(embedding_raw)
                    elif hasattr(embedding_raw, '__iter__'):  # Handle other iterable types
                        embedding_vector = np.array(list(embedding_raw))
                    else:
                        print(f"   ⚠️  Skipping document {doc.id}: embedding format not recognized")
                        continue
                    
                    # Validate the embedding
                    if embedding_vector.size == 0 or not np.isfinite(embedding_vector).all():
                        print(f"   ⚠️  Skipping document {doc.id}: invalid embedding data")
                        continue
                    
                    title = doc.title
                    if title and len(title) > 60:
                        title = title[:60] + '...'
                    
                    doc_data.append({
                        'id': str(doc.id),
                        'title': title or 'Untitled',
                        'publish_date': doc.publish_date,
                        'embedding': embedding_vector,
                        'embedding_model': doc.embedding_model or 'unknown',
                        'embedding_dim': len(embedding_vector),
                        'document_type': doc.document_type or 'unknown'
                    })
                    
                except Exception as e:
                    print(f"   ⚠️  Error processing document {doc.id}: {e}")
                    continue
            
            return doc_data
            
        except Exception as e:
            print(f"❌ Error getting embeddings for {company_name}: {e}")
            return []


def calculate_embedding_similarity(embedding1: np.ndarray, embedding2: np.ndarray) -> float:
    """Calculate cosine similarity between two embeddings"""
    try:
        # Convert to numpy arrays and ensure they are float32
        emb1 = np.array(embedding1, dtype=np.float32)
        emb2 = np.array(embedding2, dtype=np.float32)
        
        # Check if embeddings have the same dimension
        if emb1.shape != emb2.shape:
            # Skip comparison if dimensions don't match
            return 0.5  # Return neutral similarity for mismatched dimensions
            
        # Check for empty or invalid embeddings
        if emb1.size == 0 or emb2.size == 0:
            return 0.0
            
        # Check for NaN or infinite values
        if np.any(np.isnan(emb1)) or np.any(np.isnan(emb2)) or np.any(np.isinf(emb1)) or np.any(np.isinf(emb2)):
            return 0.0
        
        # Cosine similarity
        dot_product = np.dot(emb1, emb2)
        norm1 = np.linalg.norm(emb1)
        norm2 = np.linalg.norm(emb2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
            
        similarity = dot_product / (norm1 * norm2)
        
        # Ensure similarity is in valid range [-1, 1]
        similarity = np.clip(similarity, -1.0, 1.0)
        
        return float(similarity)
        
    except Exception as e:
        # For debugging: show specific error types
        if "shapes" in str(e):
            return 0.5  # Neutral similarity for shape mismatches
        elif "itemsize" in str(e):
            return 0.5  # Neutral similarity for data type issues
        else:
            print(f"❌ Unexpected similarity error: {e}")
            return 0.0


def detect_temporal_anomalies(doc_data: List[Dict], company_name: str) -> List[Dict]:
    """Detect anomalies in document sequence"""
    
    if len(doc_data) < 2:
        return []
    
    anomalies = []
    
    # Sort by date to ensure chronological order
    doc_data.sort(key=lambda x: x['publish_date'])
    
    # Group documents by embedding model to avoid dimension mismatches
    from collections import defaultdict
    model_groups = defaultdict(list)
    for doc in doc_data:
        model_groups[doc['embedding_model']].append(doc)
    
    # Analyze each model group separately
    for model, docs in model_groups.items():
        if len(docs) < 2:
            continue
            
        # Compare consecutive documents within the same model
        for i in range(1, len(docs)):
            prev_doc = docs[i-1]
            curr_doc = docs[i]
            
            # Skip if embedding dimensions don't match (extra safety)
            if prev_doc['embedding_dim'] != curr_doc['embedding_dim']:
                continue
            
            # Calculate similarity
            similarity = calculate_embedding_similarity(
                prev_doc['embedding'], 
                curr_doc['embedding']
            )
            
            # Skip if similarity calculation failed (returned 0.0)
            if similarity == 0.0:
                continue
            
            # Anomaly score (1 - similarity for dissimilarity)
            anomaly_score = 1 - similarity
            
            # Classify anomaly severity
            if anomaly_score >= 0.7:  # Very different
                severity = "significant"
                emoji = "🚨"
            elif anomaly_score >= 0.5:  # Moderately different
                severity = "moderate"  
                emoji = "⚠️"
            elif anomaly_score >= 0.3:  # Somewhat different
                severity = "minor"
                emoji = "ℹ️"
            else:
                continue  # Skip low anomalies
            
            # Calculate days between documents
            days_diff = (curr_doc['publish_date'] - prev_doc['publish_date']).days
            
            anomaly = {
                'company': company_name,
                'severity': severity,
                'score': anomaly_score,
                'similarity': similarity,
                'emoji': emoji,
                'current_doc': curr_doc,
                'previous_doc': prev_doc,
                'days_between': days_diff,
                'embedding_model': model,
                'description': f"Communication pattern shift detected in {company_name}"
            }
            
            anomalies.append(anomaly)
    
    return anomalies


async def check_date_range():
    """Check the actual date range of documents"""
    async with AsyncSessionLocal() as db:
        try:
            from sqlalchemy import text
            result = await db.execute(text("""
                SELECT 
                    MIN(ed.filing_date) as earliest,
                    MAX(ed.filing_date) as latest,
                    COUNT(DISTINCT ed.company_name) as companies
                FROM extracted_documents ed
                JOIN document_embeddings de ON de.extracted_document_id = ed.id
                WHERE ed.filing_date IS NOT NULL
            """))
            
            row = result.fetchone()
            return row.earliest, row.latest, row.companies
        except:
            return None, None, 0

async def analyze_all_companies(days_back=365, min_docs=2, sort_by='date'):
    """Analyze temporal anomalies for all companies with embeddings"""
    
    print("🧠 TEMPORAL ANOMALY ANALYSIS FROM EXISTING EMBEDDINGS")
    print("="*70)
    
    # Check actual date range first
    earliest, latest, total_companies = await check_date_range()
    if earliest and latest:
        print(f"📅 Document date range: {earliest} to {latest}")
        cutoff = datetime.now() - timedelta(days=days_back)
        print(f"📅 Analyzing documents since: {cutoff.date()} (last {days_back} days)")
        
        if latest < cutoff.date():
            print(f"⚠️  WARNING: Latest document is older than cutoff date!")
            print(f"   Suggest using --days {(datetime.now().date() - earliest).days} or more")
    
    print(f"📊 Minimum {min_docs} documents required per company")
    print()
    
    # Get companies with embeddings
    companies = await get_companies_with_embeddings()
    
    if not companies:
        print("❌ No companies found with embeddings")
        print("💡 Run document embedding generation first")
        return
    
    print(f"🏢 Found {len(companies)} companies with embeddings")
    print()
    
    all_anomalies = []
    company_stats = {}
    
    # Analyze each company
    for i, (company_name, company_id) in enumerate(companies.items(), 1):
        print(f"📄 [{i:2}/{len(companies)}] Analyzing {company_name}...")
        
        # Get document embeddings
        doc_data = await get_company_document_embeddings(company_name, days_back)
        
        if len(doc_data) < min_docs:
            print(f"   ⏭️  Skipping (only {len(doc_data)} documents in last {days_back} days)")
            continue
        
        # Detect anomalies
        company_anomalies = detect_temporal_anomalies(doc_data, company_name)
        
        company_stats[company_name] = {
            'documents': len(doc_data),
            'anomalies': len(company_anomalies),
            'significant': len([a for a in company_anomalies if a['severity'] == 'significant']),
            'moderate': len([a for a in company_anomalies if a['severity'] == 'moderate']),
            'minor': len([a for a in company_anomalies if a['severity'] == 'minor'])
        }
        
        if company_anomalies:
            all_anomalies.extend(company_anomalies)
            significant_count = company_stats[company_name]['significant']
            moderate_count = company_stats[company_name]['moderate']
            print(f"   🚨 Found {len(company_anomalies)} anomalies (🚨{significant_count} ⚠️{moderate_count})")
        else:
            print(f"   ✅ No significant anomalies")
    
    # Sort anomalies based on user preference
    if sort_by == 'date':
        all_anomalies.sort(key=lambda x: x['current_doc']['publish_date'], reverse=True)
        sort_title = "LATEST 10 ANOMALIES (Most Recent)"
    else:
        all_anomalies.sort(key=lambda x: x['score'], reverse=True)
        sort_title = "TOP 10 ANOMALIES (Highest Score)"
    
    print(f"\n📊 ANALYSIS COMPLETE")
    print("="*70)
    
    # Overall stats
    total_significant = len([a for a in all_anomalies if a['severity'] == 'significant'])
    total_moderate = len([a for a in all_anomalies if a['severity'] == 'moderate'])
    total_minor = len([a for a in all_anomalies if a['severity'] == 'minor'])
    
    print(f"📈 Total anomalies found: {len(all_anomalies)}")
    print(f"   🚨 Significant: {total_significant}")
    print(f"   ⚠️  Moderate: {total_moderate}")
    print(f"   ℹ️  Minor: {total_minor}")
    
    # Show top anomalies
    if all_anomalies:
        print(f"\n🕒 {sort_title}")
        print("-"*70)
        
        for i, anomaly in enumerate(all_anomalies[:10], 1):
            curr_date = anomaly['current_doc']['publish_date'].strftime("%Y-%m-%d")
            prev_date = anomaly['previous_doc']['publish_date'].strftime("%Y-%m-%d")
            
            print(f"\n{i:2}. {anomaly['emoji']} {curr_date} | {anomaly['company']} | Score: {anomaly['score']:.2f}")
            print(f"    📄 Current:  {anomaly['current_doc']['title']}")
            print(f"    📄 Previous: {prev_date} - {anomaly['previous_doc']['title']}")
            print(f"    📊 Gap: {anomaly['days_between']} days | Similarity: {anomaly['similarity']:.2f}")
    
    # Show company summary
    print(f"\n🏢 COMPANY SUMMARY (Top 10 by anomalies)")
    print("-"*70)
    
    top_companies = sorted(
        company_stats.items(), 
        key=lambda x: (x[1]['significant'], x[1]['anomalies']), 
        reverse=True
    )[:10]
    
    print(f"{'Company':<30} │ {'Docs':<5} │ {'🚨':<3} │ {'⚠️':<3} │ {'ℹ️':<3} │ {'Total':<5}")
    print("-" * 70)
    
    for company, stats in top_companies:
        if stats['anomalies'] > 0:
            print(f"{company[:29]:<30} │ {stats['documents']:<5} │ {stats['significant']:<3} │ {stats['moderate']:<3} │ {stats['minor']:<3} │ {stats['anomalies']:<5}")
    
    return all_anomalies


async def analyze_specific_company(company_name: str, days_back=60):
    """Detailed analysis for a specific company"""
    
    print(f"🔍 DETAILED ANALYSIS: {company_name}")
    print("="*70)
    
    doc_data = await get_company_document_embeddings(company_name, days_back)
    
    if not doc_data:
        print(f"❌ No embeddings found for {company_name}")
        return
    
    print(f"📄 Found {len(doc_data)} documents with embeddings")
    
    if len(doc_data) < 2:
        print("❌ Need at least 2 documents for comparison")
        return
    
    anomalies = detect_temporal_anomalies(doc_data, company_name)
    
    if not anomalies:
        print("✅ No significant anomalies detected")
        return
    
    print(f"\n🚨 Found {len(anomalies)} anomalies:")
    print()
    
    for i, anomaly in enumerate(anomalies, 1):
        curr_date = anomaly['current_doc']['publish_date'].strftime("%Y-%m-%d")
        prev_date = anomaly['previous_doc']['publish_date'].strftime("%Y-%m-%d")
        
        print(f"{i:2}. {anomaly['emoji']} {anomaly['severity'].upper()} | Score: {anomaly['score']:.2f}")
        print(f"    📅 {prev_date} → {curr_date} ({anomaly['days_between']} days)")
        print(f"    📊 Similarity: {anomaly['similarity']:.2f}")
        print(f"    📝 Current:  {anomaly['current_doc']['title']}")
        print(f"    📝 Previous: {anomaly['previous_doc']['title']}")
        print()


async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze Temporal Anomalies from Existing Embeddings")
    parser.add_argument("--company", type=str, help="Analyze specific company")
    parser.add_argument("--days", type=int, default=365, help="Days back to analyze (default: 365)")
    parser.add_argument("--min-docs", type=int, default=2, help="Minimum documents per company (default: 2)")
    parser.add_argument("--sort", type=str, choices=['date', 'score'], default='date', 
                        help="Sort anomalies by date (latest first) or score (highest first)")
    
    args = parser.parse_args()
    
    if args.company:
        await analyze_specific_company(args.company, args.days)
    else:
        await analyze_all_companies(args.days, args.min_docs, args.sort)


if __name__ == "__main__":
    asyncio.run(main())