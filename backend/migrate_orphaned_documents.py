#!/usr/bin/env python3
"""
Migration Script: Fix Orphaned Documents

Finds documents in nordic_documents where company_id doesn't match any company
in company_master, and attempts to re-link them based on metadata.

Usage:
    python migrate_orphaned_documents.py              # Dry run (show what would change)
    python migrate_orphaned_documents.py --apply      # Actually apply the changes
    python migrate_orphaned_documents.py --limit 100  # Process only 100 documents
"""

import asyncio
import asyncpg
import argparse
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple


DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


class OrphanedDocumentMigrator:
    def __init__(self, dry_run: bool = True, limit: Optional[int] = None):
        self.dry_run = dry_run
        self.limit = limit
        self.conn = None

        # Stats
        self.stats = {
            'orphaned_found': 0,
            'matched': 0,
            'unmatched': 0,
            'updated': 0,
            'errors': 0,
        }

        # Stock class suffixes to strip when matching
        self.stock_class_suffixes = [
            " Pref B", " Pref A", " Pref", " SDB",
            " A", " B", " C", " D"
        ]

    async def connect(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def find_orphaned_documents(self) -> List[Dict]:
        """Find documents where company_id doesn't exist in company_master"""

        query = """
            SELECT
                nd.id,
                nd.company_id,
                nd.title,
                nd.document_type,
                nd.publish_date,
                nd.metadata as metadata_,
                nc.name as nordic_company_name,
                nc.ticker as nordic_company_ticker
            FROM nordic_documents nd
            LEFT JOIN company_master cm ON nd.company_id = cm.id
            LEFT JOIN nordic_companies nc ON nd.company_id = nc.id
            WHERE cm.id IS NULL
            ORDER BY nd.ingestion_date DESC
        """

        if self.limit:
            query += f" LIMIT {self.limit}"

        rows = await self.conn.fetch(query)

        documents = []
        for row in rows:
            metadata = row['metadata_']
            if isinstance(metadata, str):
                metadata = json.loads(metadata)

            documents.append({
                'id': row['id'],
                'company_id': row['company_id'],
                'title': row['title'],
                'document_type': row['document_type'],
                'publish_date': row['publish_date'],
                'metadata': metadata or {},
                'nordic_company_name': row['nordic_company_name'],
                'nordic_company_ticker': row['nordic_company_ticker'],
            })

        return documents

    def extract_company_hints(self, doc: Dict) -> List[str]:
        """Extract possible company names from document metadata"""
        hints = []

        metadata = doc.get('metadata', {})

        # 1. Company name from LLM filter context
        llm_context = metadata.get('llm_filter_context', {})
        if llm_context.get('company'):
            hints.append(llm_context['company'])

        # 2. Company name from nordic_companies (if exists)
        if doc.get('nordic_company_name'):
            hints.append(doc['nordic_company_name'])

        # 3. Extract from MFN source URL
        mfn_source = metadata.get('mfn_source', '')
        if '/a/' in mfn_source:
            # URL like https://mfn.se/all/a/volvo -> extract "volvo"
            slug = mfn_source.split('/a/')[-1].split('?')[0].split('/')[0]
            if slug:
                # Convert slug to title case
                company_from_slug = slug.replace('-', ' ').title()
                hints.append(company_from_slug)
                hints.append(slug)  # Also try raw slug

        # 4. Extract from title (company name often at the start)
        title = doc.get('title', '')
        if ':' in title:
            # Titles like "Volvo Group: Q2 Report" -> extract "Volvo Group"
            company_from_title = title.split(':')[0].strip()
            if len(company_from_title) > 2 and len(company_from_title) < 50:
                hints.append(company_from_title)

        # Remove duplicates while preserving order
        seen = set()
        unique_hints = []
        for hint in hints:
            if hint and hint.lower() not in seen:
                seen.add(hint.lower())
                unique_hints.append(hint)

        return unique_hints

    def strip_stock_class(self, name: str) -> str:
        """Strip stock class suffix from company name"""
        for suffix in self.stock_class_suffixes:
            if name.endswith(suffix):
                return name[:-len(suffix)].strip()
        return name

    async def find_matching_company(self, hints: List[str]) -> Optional[Tuple[str, str, str]]:
        """
        Try to find a matching company in company_master.
        Returns (company_id, company_name, match_method) or None.
        """

        for hint in hints:
            # Try exact match first
            row = await self.conn.fetchrow("""
                SELECT id, company_name
                FROM company_master
                WHERE LOWER(company_name) = LOWER($1)
                LIMIT 1
            """, hint)

            if row:
                return (row['id'], row['company_name'], 'exact')

            # Try with stock class stripped
            base_name = self.strip_stock_class(hint)
            if base_name != hint:
                row = await self.conn.fetchrow("""
                    SELECT id, company_name
                    FROM company_master
                    WHERE LOWER(company_name) = LOWER($1)
                    LIMIT 1
                """, base_name)

                if row:
                    return (row['id'], row['company_name'], 'base_name')

            # Try pattern match (company_master name contains our hint)
            row = await self.conn.fetchrow("""
                SELECT id, company_name
                FROM company_master
                WHERE LOWER(company_name) LIKE LOWER($1)
                ORDER BY LENGTH(company_name)
                LIMIT 1
            """, f"%{base_name}%")

            if row:
                # Verify it's a good match (base names should match)
                cm_base = self.strip_stock_class(row['company_name'])
                if cm_base.lower() == base_name.lower():
                    return (row['id'], row['company_name'], 'pattern')

        return None

    async def update_document_company_id(self, doc_id: str, new_company_id: str) -> bool:
        """Update a document's company_id"""
        try:
            await self.conn.execute("""
                UPDATE nordic_documents
                SET company_id = $1
                WHERE id = $2
            """, new_company_id, doc_id)
            return True
        except Exception as e:
            print(f"    Error updating document {doc_id}: {e}")
            return False

    async def run(self):
        """Run the migration"""
        print("=" * 70)
        print("Orphaned Document Migration")
        print("=" * 70)
        print(f"Mode: {'DRY RUN (no changes)' if self.dry_run else 'APPLY CHANGES'}")
        print(f"Limit: {self.limit or 'None (all documents)'}")
        print()

        await self.connect()

        try:
            # Find orphaned documents
            print("Finding orphaned documents...")
            orphaned_docs = await self.find_orphaned_documents()
            self.stats['orphaned_found'] = len(orphaned_docs)

            print(f"Found {len(orphaned_docs)} orphaned documents")
            print()

            if not orphaned_docs:
                print("No orphaned documents found!")
                return

            # Process each document
            matched_docs = []
            unmatched_docs = []

            for i, doc in enumerate(orphaned_docs, 1):
                doc_id = doc['id']
                title = doc['title'][:50] if doc['title'] else 'Untitled'

                # Extract hints
                hints = self.extract_company_hints(doc)

                if not hints:
                    print(f"{i}. {title}...")
                    print(f"    No company hints found in metadata")
                    unmatched_docs.append(doc)
                    continue

                # Try to find matching company
                match = await self.find_matching_company(hints)

                if match:
                    company_id, company_name, method = match
                    matched_docs.append({
                        'doc': doc,
                        'new_company_id': company_id,
                        'company_name': company_name,
                        'match_method': method,
                        'hints': hints,
                    })

                    print(f"{i}. {title}...")
                    print(f"    Hints: {hints[:3]}")
                    print(f"    Match: {company_name} (method: {method})")

                    if not self.dry_run:
                        success = await self.update_document_company_id(doc_id, company_id)
                        if success:
                            self.stats['updated'] += 1
                            print(f"    Updated!")
                        else:
                            self.stats['errors'] += 1
                else:
                    print(f"{i}. {title}...")
                    print(f"    Hints: {hints[:3]}")
                    print(f"    No match found")
                    unmatched_docs.append(doc)

            self.stats['matched'] = len(matched_docs)
            self.stats['unmatched'] = len(unmatched_docs)

            # Summary
            print()
            print("=" * 70)
            print("SUMMARY")
            print("=" * 70)
            print(f"Orphaned documents found: {self.stats['orphaned_found']}")
            print(f"Matched to company_master: {self.stats['matched']}")
            print(f"Could not match: {self.stats['unmatched']}")

            if not self.dry_run:
                print(f"Successfully updated: {self.stats['updated']}")
                print(f"Errors: {self.stats['errors']}")
            else:
                print()
                print("This was a DRY RUN. To apply changes, run with --apply")

            # Show unmatched documents
            if unmatched_docs:
                print()
                print("=" * 70)
                print(f"UNMATCHED DOCUMENTS ({len(unmatched_docs)})")
                print("=" * 70)
                for doc in unmatched_docs[:20]:  # Show first 20
                    title = doc['title'][:60] if doc['title'] else 'Untitled'
                    hints = self.extract_company_hints(doc)
                    print(f"  - {title}")
                    print(f"    Hints tried: {hints[:3] if hints else 'None'}")

                if len(unmatched_docs) > 20:
                    print(f"  ... and {len(unmatched_docs) - 20} more")

            # Save results
            results = {
                'run_time': datetime.now().isoformat(),
                'dry_run': self.dry_run,
                'stats': self.stats,
                'matched': [
                    {
                        'doc_id': str(m['doc']['id']),
                        'title': m['doc']['title'],
                        'new_company_id': str(m['new_company_id']),
                        'company_name': m['company_name'],
                        'match_method': m['match_method'],
                    }
                    for m in matched_docs
                ],
                'unmatched': [
                    {
                        'doc_id': str(d['id']),
                        'title': d['title'],
                        'hints': self.extract_company_hints(d),
                    }
                    for d in unmatched_docs
                ],
            }

            filename = f"orphaned_migration_{'dry_run' if self.dry_run else 'applied'}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print()
            print(f"Results saved to: {filename}")

        finally:
            await self.close()


async def main():
    parser = argparse.ArgumentParser(description='Migrate orphaned documents to correct company_id')
    parser.add_argument('--apply', action='store_true', help='Actually apply the changes (default is dry run)')
    parser.add_argument('--limit', type=int, help='Limit number of documents to process')
    args = parser.parse_args()

    migrator = OrphanedDocumentMigrator(
        dry_run=not args.apply,
        limit=args.limit
    )

    await migrator.run()


if __name__ == "__main__":
    asyncio.run(main())
