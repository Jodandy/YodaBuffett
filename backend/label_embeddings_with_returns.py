#!/usr/bin/env python3
"""
Label Document Embeddings with Forward Returns

For each section embedding:
1. Find the document date
2. Match to a stock symbol
3. Get price at document date
4. Calculate forward returns (30d, 60d, 90d)
5. Label as bullish/bearish/neutral
6. Store in embedding_labels table for KNN lookups
"""

import asyncio
import asyncpg
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
import json
import ast
import time

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Thresholds for labeling
BULLISH_THRESHOLD = 0.05   # >5% = bullish
BEARISH_THRESHOLD = -0.05  # <-5% = bearish


def classify_return(ret: Optional[float]) -> str:
    """Classify a return as bullish/bearish/neutral."""
    if ret is None:
        return 'unknown'
    if ret > BULLISH_THRESHOLD:
        return 'bullish'
    elif ret < BEARISH_THRESHOLD:
        return 'bearish'
    else:
        return 'neutral'


def calculate_consensus(labels: List[str]) -> Tuple[str, float]:
    """Calculate consensus label and strength from multiple horizon labels."""
    valid_labels = [l for l in labels if l != 'unknown']

    if not valid_labels:
        return 'unknown', 0.0

    # Count votes
    bullish = sum(1 for l in valid_labels if l == 'bullish')
    bearish = sum(1 for l in valid_labels if l == 'bearish')
    neutral = sum(1 for l in valid_labels if l == 'neutral')

    total = len(valid_labels)

    # Determine consensus
    if bullish > bearish and bullish > neutral:
        return 'bullish', bullish / total
    elif bearish > bullish and bearish > neutral:
        return 'bearish', bearish / total
    elif neutral >= bullish and neutral >= bearish:
        return 'neutral', neutral / total
    else:
        # Tie between bullish and bearish - no clear signal
        return 'mixed', 0.5


async def get_company_symbol_mapping(conn) -> Dict[str, str]:
    """Get mapping from company_name to stock symbol."""
    rows = await conn.fetch("""
        SELECT company_name, primary_ticker
        FROM company_master
        WHERE primary_ticker IS NOT NULL
    """)

    mapping = {}
    for row in rows:
        name = row['company_name']
        symbol = row['primary_ticker']
        if name and symbol:
            mapping[name.lower()] = symbol
            # Also add without common suffixes
            clean_name = name.lower().replace(' ab', '').replace(' a', '').replace(' b', '').strip()
            mapping[clean_name] = symbol

    return mapping


async def find_symbol(company_name: str, symbol_mapping: Dict[str, str]) -> Optional[str]:
    """Find stock symbol for a company name."""
    name_lower = company_name.lower()

    # Exact match
    if name_lower in symbol_mapping:
        return symbol_mapping[name_lower]

    # Partial match
    for mapped_name, symbol in symbol_mapping.items():
        if mapped_name in name_lower or name_lower in mapped_name:
            return symbol

    return None


async def get_price_at_date(conn, symbol: str, target_date: date) -> Optional[float]:
    """Get closing price at or before target date."""
    row = await conn.fetchrow("""
        SELECT close_price
        FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
    """, symbol, target_date)

    return float(row['close_price']) if row else None


async def get_forward_price(conn, symbol: str, base_date: date, days: int) -> Optional[float]:
    """Get price N days after base date."""
    target_date = base_date + timedelta(days=days)
    row = await conn.fetchrow("""
        SELECT close_price
        FROM daily_price_data
        WHERE symbol = $1 AND date >= $2
        ORDER BY date
        LIMIT 1
    """, symbol, target_date)

    return float(row['close_price']) if row else None


async def process_batch(
    conn,
    embeddings: List[Dict],
    symbol_mapping: Dict[str, str]
) -> int:
    """Process a batch of embeddings and insert labels."""

    labeled = []

    for emb in embeddings:
        company_name = emb['company_name']
        if not company_name:
            continue

        # Find symbol
        symbol = await find_symbol(company_name, symbol_mapping)
        if not symbol:
            continue

        # Get document date
        doc_date = emb['filing_date']
        if not doc_date:
            # Approximate: reports for year X typically published in Q1 of X+1
            year = emb['year']
            if year:
                doc_date = date(year + 1, 3, 1)
            else:
                continue

        if isinstance(doc_date, str):
            doc_date = date.fromisoformat(doc_date)

        # Get prices
        price_at_doc = await get_price_at_date(conn, symbol, doc_date)
        if not price_at_doc:
            continue

        price_30d = await get_forward_price(conn, symbol, doc_date, 30)
        price_60d = await get_forward_price(conn, symbol, doc_date, 60)
        price_90d = await get_forward_price(conn, symbol, doc_date, 90)

        # Calculate returns
        ret_30d = (price_30d - price_at_doc) / price_at_doc if price_30d else None
        ret_60d = (price_60d - price_at_doc) / price_at_doc if price_60d else None
        ret_90d = (price_90d - price_at_doc) / price_at_doc if price_90d else None

        # Classify
        label_30d = classify_return(ret_30d)
        label_60d = classify_return(ret_60d)
        label_90d = classify_return(ret_90d)

        # Consensus
        consensus_label, consensus_strength = calculate_consensus([label_30d, label_60d, label_90d])

        # Parse embedding
        emb_str = emb['embedding']
        if isinstance(emb_str, str):
            try:
                embedding_list = json.loads(emb_str)
            except json.JSONDecodeError:
                embedding_list = ast.literal_eval(emb_str)
        else:
            embedding_list = list(emb_str)

        # Format as pgvector string
        embedding_str = '[' + ','.join(str(x) for x in embedding_list) + ']'

        labeled.append({
            'section_embedding_id': emb['id'],
            'company_name': company_name,
            'symbol': symbol,
            'document_date': doc_date,
            'section_type': emb['section_type'],
            'year': emb['year'],
            'embedding': embedding_str,
            'return_30d': ret_30d,
            'return_60d': ret_60d,
            'return_90d': ret_90d,
            'label_30d': label_30d,
            'label_60d': label_60d,
            'label_90d': label_90d,
            'consensus_label': consensus_label,
            'consensus_strength': consensus_strength,
            'price_at_document': price_at_doc,
            'price_30d': price_30d,
            'price_60d': price_60d,
            'price_90d': price_90d
        })

    # Bulk insert
    if labeled:
        await conn.executemany("""
            INSERT INTO embedding_labels (
                section_embedding_id, company_name, symbol, document_date,
                section_type, year, embedding,
                return_30d, return_60d, return_90d,
                label_30d, label_60d, label_90d,
                consensus_label, consensus_strength,
                price_at_document, price_30d, price_60d, price_90d
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::vector,
                $8, $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19
            )
            ON CONFLICT (section_embedding_id) DO NOTHING
        """, [
            (
                r['section_embedding_id'], r['company_name'], r['symbol'],
                r['document_date'], r['section_type'], r['year'], r['embedding'],
                r['return_30d'], r['return_60d'], r['return_90d'],
                r['label_30d'], r['label_60d'], r['label_90d'],
                r['consensus_label'], r['consensus_strength'],
                r['price_at_document'], r['price_30d'], r['price_60d'], r['price_90d']
            )
            for r in labeled
        ])

    return len(labeled)


async def main():
    """Label all embeddings with forward returns."""

    print("=" * 70)
    print("LABEL EMBEDDINGS WITH FORWARD RETURNS")
    print("=" * 70)

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Check if table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'embedding_labels'
            )
        """)

        if not table_exists:
            print("❌ embedding_labels table doesn't exist. Run create_embedding_labels_table.py first.")
            return

        # Get symbol mapping
        print("\n1. Loading company-symbol mapping...")
        symbol_mapping = await get_company_symbol_mapping(conn)
        print(f"   Found {len(symbol_mapping)} mappings")

        # Count existing labels
        existing = await conn.fetchval("SELECT COUNT(*) FROM embedding_labels")
        print(f"   Existing labels: {existing}")

        # Get embeddings that need labeling
        print("\n2. Finding embeddings to label...")

        # Only process key financial sections
        total_to_process = await conn.fetchval("""
            SELECT COUNT(*)
            FROM section_embeddings se
            JOIN extracted_documents ed ON ed.id = se.extracted_document_id
            WHERE se.section_type IN ('balance_sheet', 'income_statement', 'cash_flow',
                                      'risk_factors', 'management_discussion')
            AND se.id NOT IN (SELECT section_embedding_id FROM embedding_labels)
        """)

        print(f"   Embeddings to process: {total_to_process:,}")

        if total_to_process == 0:
            print("   ✅ All embeddings already labeled!")
            return

        # Process in batches
        print("\n3. Labeling embeddings...")
        batch_size = 100
        total_labeled = 0
        offset = 0
        start_time = time.time()

        while True:
            # Fetch batch
            rows = await conn.fetch("""
                SELECT
                    se.id,
                    se.embedding,
                    se.section_type,
                    ed.company_name,
                    ed.filing_date,
                    ed.year
                FROM section_embeddings se
                JOIN extracted_documents ed ON ed.id = se.extracted_document_id
                WHERE se.section_type IN ('balance_sheet', 'income_statement', 'cash_flow',
                                          'risk_factors', 'management_discussion')
                AND se.id NOT IN (SELECT section_embedding_id FROM embedding_labels)
                LIMIT $1
            """, batch_size)

            if not rows:
                break

            embeddings = [dict(row) for row in rows]
            labeled = await process_batch(conn, embeddings, symbol_mapping)
            total_labeled += labeled
            offset += len(rows)

            # Progress
            elapsed = time.time() - start_time
            rate = total_labeled / elapsed if elapsed > 0 else 0
            eta = (total_to_process - offset) / rate / 60 if rate > 0 else 0

            print(f"   Processed: {offset:,} | Labeled: {total_labeled:,} | "
                  f"Rate: {rate:.1f}/s | ETA: {eta:.1f}m")

            # Avoid overwhelming the database
            if len(rows) == batch_size:
                await asyncio.sleep(0.1)

        # Final stats
        print("\n" + "=" * 70)
        print("LABELING COMPLETE")
        print("=" * 70)

        total_time = time.time() - start_time
        print(f"\n⏱️  Total time: {total_time / 60:.1f} minutes")
        print(f"📊 Total labeled: {total_labeled:,}")

        # Show label distribution
        print("\n--- Label Distribution ---")
        dist = await conn.fetch("""
            SELECT consensus_label, COUNT(*) as count
            FROM embedding_labels
            GROUP BY consensus_label
            ORDER BY count DESC
        """)

        for row in dist:
            print(f"   {row['consensus_label']}: {row['count']:,}")

        # Show by section type
        print("\n--- By Section Type ---")
        by_section = await conn.fetch("""
            SELECT section_type, COUNT(*) as total,
                   COUNT(*) FILTER (WHERE consensus_label = 'bullish') as bullish,
                   COUNT(*) FILTER (WHERE consensus_label = 'bearish') as bearish
            FROM embedding_labels
            GROUP BY section_type
            ORDER BY total DESC
        """)

        for row in by_section:
            total = row['total']
            bullish_pct = row['bullish'] / total * 100 if total > 0 else 0
            bearish_pct = row['bearish'] / total * 100 if total > 0 else 0
            print(f"   {row['section_type']}: {total:,} "
                  f"(bullish: {bullish_pct:.1f}%, bearish: {bearish_pct:.1f}%)")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
