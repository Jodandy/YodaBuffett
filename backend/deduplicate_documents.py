#!/usr/bin/env python3
"""
Deduplicate nordic_documents by PDF URL

Keeps ONE document per unique PDF URL, with priority:
1. Document with company_id that exists in company_master
2. Document with company_id that exists in nordic_companies
3. Oldest document (by ingestion_date)

Deletes all duplicates.

Usage:
    python deduplicate_documents.py              # Dry run
    python deduplicate_documents.py --apply      # Actually delete duplicates
"""

import asyncio
import asyncpg
import argparse
import json
from datetime import datetime
from typing import List, Dict, Set, Optional
from collections import defaultdict


DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def get_company_master_ids(conn) -> Set[str]:
    """Get all company IDs from company_master"""
    rows = await conn.fetch('SELECT id FROM company_master')
    return {str(row['id']) for row in rows}


async def get_nordic_company_ids(conn) -> Set[str]:
    """Get all company IDs from nordic_companies"""
    rows = await conn.fetch('SELECT id FROM nordic_companies')
    return {str(row['id']) for row in rows}


async def load_all_documents(conn) -> List[Dict]:
    """Load all documents with metadata"""
    print("Loading all documents...")

    rows = await conn.fetch('''
        SELECT id, company_id, ingestion_date, metadata
        FROM nordic_documents
        WHERE metadata IS NOT NULL
    ''')

    documents = []
    skipped = 0

    for row in rows:
        metadata = row['metadata']

        # Parse JSON in Python
        if metadata is None:
            skipped += 1
            continue

        try:
            if isinstance(metadata, str):
                metadata_dict = json.loads(metadata)
            elif isinstance(metadata, dict):
                metadata_dict = metadata
            else:
                # Try converting to string first
                metadata_dict = json.loads(str(metadata))
        except (json.JSONDecodeError, TypeError) as e:
            skipped += 1
            continue

        pdf_url = metadata_dict.get('pdf_url')
        if not pdf_url:
            skipped += 1
            continue

        documents.append({
            'id': str(row['id']),
            'company_id': str(row['company_id']) if row['company_id'] else None,
            'ingestion_date': row['ingestion_date'],
            'pdf_url': pdf_url,
        })

    print(f"Loaded {len(documents):,} documents with valid PDF URLs")
    print(f"Skipped {skipped:,} documents without valid metadata/PDF URL")

    return documents


def find_best_document(docs: List[Dict], cm_ids: Set[str], nc_ids: Set[str]) -> str:
    """
    Given a list of duplicate documents, return the ID of the one to keep.

    Priority:
    1. Has company_id in company_master
    2. Has company_id in nordic_companies
    3. Oldest by ingestion_date
    """

    # Score each document
    scored_docs = []
    for doc in docs:
        company_id = doc['company_id']

        if company_id and company_id in cm_ids:
            priority = 1  # Best - in company_master
        elif company_id and company_id in nc_ids:
            priority = 2  # Good - in nordic_companies
        else:
            priority = 3  # Fallback - orphaned or no company_id

        scored_docs.append({
            'id': doc['id'],
            'priority': priority,
            'ingestion_date': doc['ingestion_date'] or datetime.max,
        })

    # Sort by priority (ascending), then by ingestion_date (ascending = oldest first)
    scored_docs.sort(key=lambda x: (x['priority'], x['ingestion_date']))

    return scored_docs[0]['id']


async def analyze_duplicates(documents: List[Dict], cm_ids: Set[str], nc_ids: Set[str]) -> Dict:
    """Analyze the duplicate situation"""

    # Group by PDF URL
    by_url = defaultdict(list)
    for doc in documents:
        by_url[doc['pdf_url']].append(doc)

    total_docs = len(documents)
    unique_urls = len(by_url)

    urls_with_dupes = 0
    excess_duplicates = 0
    max_dupes = 0

    for url, docs in by_url.items():
        if len(docs) > 1:
            urls_with_dupes += 1
            excess_duplicates += len(docs) - 1
            max_dupes = max(max_dupes, len(docs))

    return {
        'total_docs': total_docs,
        'unique_urls': unique_urls,
        'excess_duplicates': excess_duplicates,
        'urls_with_dupes': urls_with_dupes,
        'max_dupes': max_dupes,
    }


async def find_documents_to_keep(
    documents: List[Dict],
    cm_ids: Set[str],
    nc_ids: Set[str]
) -> Set[str]:
    """
    For each PDF URL, determine which document to keep.
    Returns set of document IDs to keep.
    """

    # Group by PDF URL
    by_url = defaultdict(list)
    for doc in documents:
        by_url[doc['pdf_url']].append(doc)

    keep_ids = set()

    for url, docs in by_url.items():
        best_id = find_best_document(docs, cm_ids, nc_ids)
        keep_ids.add(best_id)

    return keep_ids


async def delete_duplicates(conn, keep_ids: Set[str], dry_run: bool = True) -> Dict:
    """Delete all documents except the ones we're keeping"""

    keep_list = list(keep_ids)

    if dry_run:
        # Count what would be deleted
        doc_result = await conn.fetchrow('''
            SELECT COUNT(*) as cnt
            FROM nordic_documents
            WHERE id != ALL($1::uuid[])
              AND metadata IS NOT NULL
        ''', keep_list)

        # Count related financial_metrics that would be deleted
        fm_result = await conn.fetchrow('''
            SELECT COUNT(*) as cnt
            FROM financial_metrics fm
            JOIN nordic_documents nd ON fm.document_id = nd.id
            WHERE nd.id != ALL($1::uuid[])
              AND nd.metadata IS NOT NULL
        ''', keep_list)

        return {
            'documents': doc_result['cnt'],
            'financial_metrics': fm_result['cnt'],
        }
    else:
        # First delete related financial_metrics records
        fm_result = await conn.execute('''
            DELETE FROM financial_metrics
            WHERE document_id IN (
                SELECT id FROM nordic_documents
                WHERE id != ALL($1::uuid[])
                  AND metadata IS NOT NULL
            )
        ''', keep_list)
        fm_count = int(fm_result.split()[-1])
        print(f"  Deleted {fm_count:,} related financial_metrics records")

        # Now delete the duplicate documents
        result = await conn.execute('''
            DELETE FROM nordic_documents
            WHERE id != ALL($1::uuid[])
              AND metadata IS NOT NULL
        ''', keep_list)
        doc_count = int(result.split()[-1])

        return {
            'documents': doc_count,
            'financial_metrics': fm_count,
        }


async def show_sample_duplicates(documents: List[Dict], cm_ids: Set[str], nc_ids: Set[str], limit: int = 5):
    """Show a sample of duplicate groups and which would be kept"""

    # Group by PDF URL
    by_url = defaultdict(list)
    for doc in documents:
        by_url[doc['pdf_url']].append(doc)

    # Find groups with duplicates
    dupe_groups = [(url, docs) for url, docs in by_url.items() if len(docs) > 1]

    print(f"\n{'='*70}")
    print(f"SAMPLE DUPLICATE GROUPS (showing {min(limit, len(dupe_groups))} of {len(dupe_groups)})")
    print(f"{'='*70}")

    for url, docs in dupe_groups[:limit]:
        print(f"\nPDF URL: {url[:80]}...")
        print(f"  {len(docs)} copies found:")

        best_id = find_best_document(docs, cm_ids, nc_ids)

        for doc in sorted(docs, key=lambda x: x['ingestion_date'] or datetime.max):
            company_id = doc['company_id']

            if company_id and company_id in cm_ids:
                status = "✓ company_master"
            elif company_id and company_id in nc_ids:
                status = "○ nordic_companies"
            else:
                status = "✗ orphaned"

            keep_marker = " ← KEEP" if doc['id'] == best_id else ""
            date_str = doc['ingestion_date'].strftime('%Y-%m-%d') if doc['ingestion_date'] else 'no date'

            print(f"    [{status}] {date_str} - {doc['id'][:8]}...{keep_marker}")


async def main():
    parser = argparse.ArgumentParser(description='Deduplicate nordic_documents by PDF URL')
    parser.add_argument('--apply', action='store_true', help='Actually delete duplicates (default is dry run)')
    parser.add_argument('--show-samples', action='store_true', help='Show sample duplicate groups')
    args = parser.parse_args()

    dry_run = not args.apply

    print("=" * 70)
    print("Document Deduplication (Python-based JSON parsing)")
    print("=" * 70)
    print(f"Mode: {'DRY RUN' if dry_run else 'APPLY CHANGES'}")
    print()

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Load reference data
        print("Loading company reference data...")
        cm_ids = await get_company_master_ids(conn)
        nc_ids = await get_nordic_company_ids(conn)
        print(f"  company_master: {len(cm_ids):,} companies")
        print(f"  nordic_companies: {len(nc_ids):,} companies")
        print()

        # Load all documents
        documents = await load_all_documents(conn)

        if not documents:
            print("No documents found!")
            return

        # Analyze duplicates
        print("\nAnalyzing duplicates...")
        stats = await analyze_duplicates(documents, cm_ids, nc_ids)

        print(f"Total documents with PDF URL: {stats['total_docs']:,}")
        print(f"Unique PDF URLs: {stats['unique_urls']:,}")
        print(f"URLs with duplicates: {stats['urls_with_dupes']:,}")
        print(f"Excess duplicates to remove: {stats['excess_duplicates']:,}")
        print(f"Worst case: {stats['max_dupes']} copies of same PDF")
        print()

        if stats['excess_duplicates'] == 0:
            print("No duplicates found!")
            return

        # Show sample duplicates if requested
        if args.show_samples:
            await show_sample_duplicates(documents, cm_ids, nc_ids)

        # Determine which documents to keep
        print("Determining which documents to keep...")
        print("(Prioritizing: company_master match > nordic_companies match > oldest)")
        keep_ids = await find_documents_to_keep(documents, cm_ids, nc_ids)
        print(f"Documents to keep: {len(keep_ids):,}")
        print()

        # Delete or count duplicates
        if dry_run:
            print("Counting documents that would be deleted...")
            delete_counts = await delete_duplicates(conn, keep_ids, dry_run=True)
            print(f"Would delete: {delete_counts['documents']:,} duplicate documents")
            print(f"Would delete: {delete_counts['financial_metrics']:,} related financial_metrics")
            print()
            print("This was a DRY RUN. To actually delete, run with --apply")
            print("Use --show-samples to see which duplicates would be kept/deleted")
        else:
            print("Deleting duplicates...")
            delete_counts = await delete_duplicates(conn, keep_ids, dry_run=False)
            print(f"Deleted: {delete_counts['documents']:,} duplicate documents")
            print()

            # Verify
            print("Verifying...")
            remaining_docs = await load_all_documents(conn)
            new_stats = await analyze_duplicates(remaining_docs, cm_ids, nc_ids)
            print(f"Documents remaining: {new_stats['total_docs']:,}")
            print(f"Unique PDF URLs: {new_stats['unique_urls']:,}")
            print(f"Remaining duplicates: {new_stats['excess_duplicates']:,}")

        # Save results
        results = {
            'run_time': datetime.now().isoformat(),
            'dry_run': dry_run,
            'before': stats,
            'documents_kept': len(keep_ids),
            'documents_deleted': delete_counts['documents'] if not dry_run else 'N/A (dry run)',
            'financial_metrics_deleted': delete_counts['financial_metrics'] if not dry_run else 'N/A (dry run)',
        }

        filename = f"dedup_{'dry_run' if dry_run else 'applied'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print()
        print(f"Results saved to: {filename}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
