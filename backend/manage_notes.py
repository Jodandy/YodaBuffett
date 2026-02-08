#!/usr/bin/env python3
"""
Manage company research notes.

Your personal research journal for tracking observations, thesis points,
risks, and catalysts over time.

Usage:
    # Add notes
    python manage_notes.py add "Volvo" "Strong Q4 margins surprised positively"
    python manage_notes.py add "Volvo" "CEO mentioned Asia expansion" --type earnings --sentiment bullish
    python manage_notes.py add "Volvo" "Check competitor pricing" --type risk --action
    python manage_notes.py add "Volvo" "Valuation at 10x EV/EBITDA" --type valuation --tags valuation,cheap

    # View notes
    python manage_notes.py list "Volvo"
    python manage_notes.py list "Volvo" --type thesis
    python manage_notes.py list "Volvo" --last 5
    python manage_notes.py search "margin"

    # Action items
    python manage_notes.py actions
    python manage_notes.py complete 123

    # Recent activity
    python manage_notes.py recent
    python manage_notes.py recent --days 7
"""

import asyncio
import asyncpg
import argparse
from datetime import datetime, timedelta

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

NOTE_TYPES = [
    'general', 'thesis', 'risk', 'catalyst', 'earnings',
    'valuation', 'management', 'competitor', 'news'
]

SENTIMENTS = ['bullish', 'bearish', 'neutral', 'cautious']


async def find_company(conn, name: str):
    """Find company by name (fuzzy match)."""
    row = await conn.fetchrow("""
        SELECT id, company_name, primary_ticker, sector
        FROM company_master
        WHERE company_name ILIKE $1
        LIMIT 1
    """, f"%{name}%")
    return dict(row) if row else None


async def add_note(conn, company_name: str, content: str, note_type: str = 'general',
                   title: str = None, sentiment: str = None, source: str = None,
                   action: bool = False, tags: list = None):
    """Add a note for a company."""
    company = await find_company(conn, company_name)
    if not company:
        print(f"❌ Company not found: {company_name}")
        return False

    # Auto-generate title from content if not provided
    if not title:
        title = content[:50] + "..." if len(content) > 50 else content

    await conn.execute("""
        INSERT INTO company_notes
            (company_id, note_type, title, content, sentiment, source, action_item, tags)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    """, company['id'], note_type, title, content, sentiment, source, action, tags)

    print(f"✅ Note added for {company['company_name']}")
    print(f"   Type: {note_type}")
    if sentiment:
        emoji = {'bullish': '🟢', 'bearish': '🔴', 'neutral': '⚪', 'cautious': '🟡'}.get(sentiment, '')
        print(f"   Sentiment: {emoji} {sentiment}")
    if action:
        print(f"   ⚡ Marked as action item")
    if tags:
        print(f"   Tags: {', '.join(tags)}")
    return True


async def list_notes(conn, company_name: str, note_type: str = None, limit: int = 10):
    """List notes for a company."""
    company = await find_company(conn, company_name)
    if not company:
        print(f"❌ Company not found: {company_name}")
        return

    query = """
        SELECT id, note_type, title, content, sentiment, source,
               action_item, action_completed, created_at, tags
        FROM company_notes
        WHERE company_id = $1
    """
    params = [company['id']]

    if note_type:
        query += " AND note_type = $2"
        params.append(note_type)

    query += " ORDER BY created_at DESC LIMIT $" + str(len(params) + 1)
    params.append(limit)

    rows = await conn.fetch(query, *params)

    if not rows:
        print(f"📝 No notes found for {company['company_name']}")
        return

    print(f"\n{'='*70}")
    print(f"NOTES: {company['company_name']} ({company['primary_ticker']})")
    if note_type:
        print(f"Filter: {note_type}")
    print(f"{'='*70}\n")

    for row in rows:
        # Header line
        date_str = row['created_at'].strftime('%Y-%m-%d %H:%M')
        type_badge = f"[{row['note_type'].upper()}]"

        sentiment_emoji = ''
        if row['sentiment']:
            sentiment_emoji = {'bullish': '🟢', 'bearish': '🔴', 'neutral': '⚪', 'cautious': '🟡'}.get(row['sentiment'], '')

        action_marker = ''
        if row['action_item']:
            action_marker = '⚡ ACTION' if not row['action_completed'] else '✓ DONE'

        print(f"#{row['id']} | {date_str} | {type_badge} {sentiment_emoji} {action_marker}")

        # Title
        if row['title']:
            print(f"   {row['title']}")

        # Content (if different from title)
        if row['content'] != row['title']:
            # Wrap long content
            content = row['content']
            if len(content) > 200:
                content = content[:200] + "..."
            print(f"   {content}")

        # Tags
        if row['tags']:
            print(f"   Tags: {', '.join(row['tags'])}")

        # Source
        if row['source']:
            print(f"   Source: {row['source']}")

        print()


async def search_notes(conn, query: str, limit: int = 20):
    """Search across all notes."""
    rows = await conn.fetch("""
        SELECT n.id, cm.company_name, cm.primary_ticker, n.note_type,
               n.title, n.content, n.created_at, n.sentiment
        FROM company_notes n
        JOIN company_master cm ON n.company_id = cm.id
        WHERE n.content ILIKE $1 OR n.title ILIKE $1
        ORDER BY n.created_at DESC
        LIMIT $2
    """, f"%{query}%", limit)

    if not rows:
        print(f"🔍 No notes found matching: {query}")
        return

    print(f"\n{'='*70}")
    print(f"SEARCH RESULTS: '{query}' ({len(rows)} matches)")
    print(f"{'='*70}\n")

    for row in rows:
        date_str = row['created_at'].strftime('%Y-%m-%d')
        print(f"#{row['id']} | {row['company_name']} ({row['primary_ticker']}) | {date_str}")
        print(f"   [{row['note_type']}] {row['title'] or row['content'][:60]}")
        print()


async def list_actions(conn, include_completed: bool = False):
    """List all action items."""
    query = """
        SELECT n.id, cm.company_name, cm.primary_ticker, n.title, n.content,
               n.created_at, n.action_completed
        FROM company_notes n
        JOIN company_master cm ON n.company_id = cm.id
        WHERE n.action_item = TRUE
    """
    if not include_completed:
        query += " AND n.action_completed = FALSE"
    query += " ORDER BY n.created_at DESC"

    rows = await conn.fetch(query)

    if not rows:
        print("✅ No pending action items!")
        return

    print(f"\n{'='*70}")
    print(f"ACTION ITEMS ({len(rows)} pending)")
    print(f"{'='*70}\n")

    for row in rows:
        status = "✓ DONE" if row['action_completed'] else "⚡ TODO"
        date_str = row['created_at'].strftime('%Y-%m-%d')
        print(f"#{row['id']} | {status} | {row['company_name']} | {date_str}")
        print(f"   {row['title'] or row['content'][:80]}")
        print()

    print(f"Complete an action: python manage_notes.py complete <id>")


async def complete_action(conn, note_id: int):
    """Mark an action item as completed."""
    result = await conn.execute("""
        UPDATE company_notes
        SET action_completed = TRUE, updated_at = NOW()
        WHERE id = $1 AND action_item = TRUE
    """, note_id)

    if "UPDATE 1" in result:
        print(f"✅ Action #{note_id} marked as complete")
    else:
        print(f"❌ Note #{note_id} not found or not an action item")


async def recent_notes(conn, days: int = 7):
    """Show recent notes across all companies."""
    since = datetime.now() - timedelta(days=days)

    rows = await conn.fetch("""
        SELECT n.id, cm.company_name, cm.primary_ticker, n.note_type,
               n.title, n.content, n.created_at, n.sentiment
        FROM company_notes n
        JOIN company_master cm ON n.company_id = cm.id
        WHERE n.created_at >= $1
        ORDER BY n.created_at DESC
    """, since)

    if not rows:
        print(f"📝 No notes in the last {days} days")
        return

    print(f"\n{'='*70}")
    print(f"RECENT NOTES (last {days} days) - {len(rows)} notes")
    print(f"{'='*70}\n")

    current_date = None
    for row in rows:
        note_date = row['created_at'].date()
        if note_date != current_date:
            current_date = note_date
            print(f"--- {current_date.strftime('%A, %B %d, %Y')} ---\n")

        time_str = row['created_at'].strftime('%H:%M')
        sentiment_emoji = ''
        if row['sentiment']:
            sentiment_emoji = {'bullish': '🟢', 'bearish': '🔴', 'neutral': '⚪', 'cautious': '🟡'}.get(row['sentiment'], '')

        print(f"{time_str} | {row['company_name']} | [{row['note_type']}] {sentiment_emoji}")
        print(f"   {row['title'] or row['content'][:70]}")
        print()


async def delete_note(conn, note_id: int):
    """Delete a note."""
    # First show what we're deleting
    row = await conn.fetchrow("""
        SELECT n.*, cm.company_name
        FROM company_notes n
        JOIN company_master cm ON n.company_id = cm.id
        WHERE n.id = $1
    """, note_id)

    if not row:
        print(f"❌ Note #{note_id} not found")
        return

    print(f"Deleting note #{note_id}:")
    print(f"   Company: {row['company_name']}")
    print(f"   Content: {row['content'][:50]}...")

    await conn.execute("DELETE FROM company_notes WHERE id = $1", note_id)
    print(f"✅ Note deleted")


async def main():
    parser = argparse.ArgumentParser(description='Manage company research notes')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add a note')
    add_parser.add_argument('company', help='Company name')
    add_parser.add_argument('content', help='Note content')
    add_parser.add_argument('--type', '-t', choices=NOTE_TYPES, default='general', help='Note type')
    add_parser.add_argument('--title', help='Optional title')
    add_parser.add_argument('--sentiment', '-s', choices=SENTIMENTS, help='Sentiment')
    add_parser.add_argument('--source', help='Source (e.g., Q4 earnings, annual report)')
    add_parser.add_argument('--action', '-a', action='store_true', help='Mark as action item')
    add_parser.add_argument('--tags', help='Comma-separated tags')

    # List command
    list_parser = subparsers.add_parser('list', help='List notes for a company')
    list_parser.add_argument('company', help='Company name')
    list_parser.add_argument('--type', '-t', choices=NOTE_TYPES, help='Filter by type')
    list_parser.add_argument('--last', '-n', type=int, default=10, help='Number of notes')

    # Search command
    search_parser = subparsers.add_parser('search', help='Search all notes')
    search_parser.add_argument('query', help='Search term')
    search_parser.add_argument('--limit', '-n', type=int, default=20, help='Max results')

    # Actions command
    actions_parser = subparsers.add_parser('actions', help='List action items')
    actions_parser.add_argument('--all', '-a', action='store_true', help='Include completed')

    # Complete command
    complete_parser = subparsers.add_parser('complete', help='Mark action as done')
    complete_parser.add_argument('id', type=int, help='Note ID')

    # Recent command
    recent_parser = subparsers.add_parser('recent', help='Recent notes across all companies')
    recent_parser.add_argument('--days', '-d', type=int, default=7, help='Days to look back')

    # Delete command
    delete_parser = subparsers.add_parser('delete', help='Delete a note')
    delete_parser.add_argument('id', type=int, help='Note ID')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        print("\n📝 Note types:", ", ".join(NOTE_TYPES))
        print("💭 Sentiments:", ", ".join(SENTIMENTS))
        return

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        if args.command == 'add':
            tags = args.tags.split(',') if args.tags else None
            await add_note(conn, args.company, args.content, args.type,
                          args.title, args.sentiment, args.source, args.action, tags)
        elif args.command == 'list':
            await list_notes(conn, args.company, args.type, args.last)
        elif args.command == 'search':
            await search_notes(conn, args.query, args.limit)
        elif args.command == 'actions':
            await list_actions(conn, args.all)
        elif args.command == 'complete':
            await complete_action(conn, args.id)
        elif args.command == 'recent':
            await recent_notes(conn, args.days)
        elif args.command == 'delete':
            await delete_note(conn, args.id)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
