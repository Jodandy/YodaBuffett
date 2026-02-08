#!/usr/bin/env python3
"""
Manage your analysis watchlist.

Add/remove companies for targeted LLM analysis to keep costs down.
Only companies on the watchlist will be analyzed by expensive LLM calls.

Usage:
    python manage_watchlist.py add "Volvo"
    python manage_watchlist.py add "Ericsson" --reason "Potential turnaround" --priority 1
    python manage_watchlist.py list
    python manage_watchlist.py list --enabled-only
    python manage_watchlist.py remove "Volvo"
    python manage_watchlist.py toggle "Volvo"  # Enable/disable without removing
    python manage_watchlist.py info "Volvo"
    python manage_watchlist.py stats
"""

import asyncio
import asyncpg
import argparse
from datetime import datetime

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def find_company(conn, name: str):
    """Find company by name (fuzzy match)."""
    row = await conn.fetchrow("""
        SELECT id, company_name, primary_ticker, sector, country
        FROM company_master
        WHERE company_name ILIKE $1
        LIMIT 1
    """, f"%{name}%")
    return dict(row) if row else None


async def add_to_watchlist(conn, name: str, reason: str = None, priority: int = 2):
    """Add a company to the watchlist."""
    company = await find_company(conn, name)
    if not company:
        print(f"❌ Company not found: {name}")
        return False

    try:
        await conn.execute("""
            INSERT INTO analysis_watchlist (company_id, added_reason, priority)
            VALUES ($1, $2, $3)
            ON CONFLICT (company_id) DO UPDATE SET
                llm_analysis_enabled = TRUE,
                added_reason = COALESCE(EXCLUDED.added_reason, analysis_watchlist.added_reason),
                priority = EXCLUDED.priority
        """, company['id'], reason, priority)

        priority_label = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}.get(priority, "MEDIUM")
        print(f"✅ Added to watchlist: {company['company_name']} ({company['primary_ticker']})")
        print(f"   Sector: {company['sector'] or 'N/A'}")
        print(f"   Priority: {priority_label}")
        if reason:
            print(f"   Reason: {reason}")
        return True

    except Exception as e:
        print(f"❌ Error adding to watchlist: {e}")
        return False


async def remove_from_watchlist(conn, name: str):
    """Remove a company from the watchlist."""
    company = await find_company(conn, name)
    if not company:
        print(f"❌ Company not found: {name}")
        return False

    result = await conn.execute("""
        DELETE FROM analysis_watchlist
        WHERE company_id = $1
    """, company['id'])

    if "DELETE 1" in result:
        print(f"✅ Removed from watchlist: {company['company_name']}")
        return True
    else:
        print(f"ℹ️  {company['company_name']} was not on the watchlist")
        return False


async def toggle_watchlist(conn, name: str):
    """Toggle LLM analysis enabled/disabled for a company."""
    company = await find_company(conn, name)
    if not company:
        print(f"❌ Company not found: {name}")
        return False

    row = await conn.fetchrow("""
        UPDATE analysis_watchlist
        SET llm_analysis_enabled = NOT llm_analysis_enabled
        WHERE company_id = $1
        RETURNING llm_analysis_enabled
    """, company['id'])

    if row:
        status = "ENABLED" if row['llm_analysis_enabled'] else "DISABLED"
        print(f"✅ {company['company_name']}: LLM analysis {status}")
        return True
    else:
        print(f"ℹ️  {company['company_name']} is not on the watchlist. Use 'add' first.")
        return False


async def list_watchlist(conn, enabled_only: bool = False):
    """List all companies on the watchlist."""
    query = """
        SELECT * FROM watchlist_companies
    """
    if enabled_only:
        query += " WHERE llm_analysis_enabled = TRUE"

    rows = await conn.fetch(query)

    if not rows:
        print("📋 Watchlist is empty")
        print("   Use: python manage_watchlist.py add \"Company Name\"")
        return

    print(f"\n{'='*80}")
    print(f"ANALYSIS WATCHLIST ({len(rows)} companies)")
    print(f"{'='*80}\n")

    for row in rows:
        status = "🟢" if row['llm_analysis_enabled'] else "⚪"
        priority_label = {1: "HIGH", 2: "MED", 3: "LOW"}.get(row['priority'], "MED")

        print(f"{status} {row['company_name']} ({row['primary_ticker']})")
        print(f"   Sector: {row['sector'] or 'N/A'} | Priority: {priority_label}")

        if row['added_reason']:
            print(f"   Reason: {row['added_reason']}")

        if row['latest_recommendation']:
            print(f"   Latest: {row['latest_recommendation']} ({row['latest_analysis_date']})")

        if row['notes']:
            print(f"   Notes: {row['notes']}")

        print()

    # Summary
    enabled = sum(1 for r in rows if r['llm_analysis_enabled'])
    print(f"{'='*80}")
    print(f"Total: {len(rows)} | Enabled for analysis: {enabled}")


async def show_company_info(conn, name: str):
    """Show detailed info for a company on the watchlist."""
    company = await find_company(conn, name)
    if not company:
        print(f"❌ Company not found: {name}")
        return

    watchlist = await conn.fetchrow("""
        SELECT * FROM analysis_watchlist WHERE company_id = $1
    """, company['id'])

    analyses = await conn.fetch("""
        SELECT analysis_type, analysis_date, recommendation, model_used,
               estimated_cost_usd, summary
        FROM llm_analysis_results
        WHERE company_id = $1
        ORDER BY analysis_date DESC
        LIMIT 5
    """, company['id'])

    print(f"\n{'='*60}")
    print(f"{company['company_name']} ({company['primary_ticker']})")
    print(f"{'='*60}")
    print(f"Sector: {company['sector'] or 'N/A'}")
    print(f"Country: {company['country']}")

    if watchlist:
        status = "🟢 ENABLED" if watchlist['llm_analysis_enabled'] else "⚪ DISABLED"
        priority_label = {1: "HIGH", 2: "MEDIUM", 3: "LOW"}.get(watchlist['priority'], "MEDIUM")
        print(f"\nWatchlist Status: {status}")
        print(f"Priority: {priority_label}")
        print(f"Added: {watchlist['added_at'].strftime('%Y-%m-%d')}")
        if watchlist['added_reason']:
            print(f"Reason: {watchlist['added_reason']}")
        print(f"Analysis Count: {watchlist['analysis_count']}")
    else:
        print(f"\n⚠️  Not on watchlist")

    if analyses:
        print(f"\nRecent Analyses:")
        total_cost = 0
        for a in analyses:
            cost = a['estimated_cost_usd'] or 0
            total_cost += float(cost)
            print(f"  • {a['analysis_date']} | {a['analysis_type']} | {a['recommendation'] or 'N/A'} | ${cost:.4f}")
        print(f"\nTotal spent on this company: ${total_cost:.4f}")
    else:
        print(f"\nNo analyses yet")


async def show_stats(conn):
    """Show watchlist and cost statistics."""
    watchlist_stats = await conn.fetchrow("""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE llm_analysis_enabled) as enabled,
            COUNT(*) FILTER (WHERE priority = 1) as high_priority,
            COUNT(*) FILTER (WHERE priority = 2) as med_priority,
            COUNT(*) FILTER (WHERE priority = 3) as low_priority
        FROM analysis_watchlist
    """)

    cost_stats = await conn.fetchrow("""
        SELECT
            COUNT(*) as total_analyses,
            SUM(estimated_cost_usd) as total_cost,
            AVG(estimated_cost_usd) as avg_cost,
            COUNT(DISTINCT company_id) as companies_analyzed
        FROM llm_analysis_results
    """)

    print(f"\n{'='*60}")
    print("WATCHLIST & COST STATISTICS")
    print(f"{'='*60}\n")

    print("📋 Watchlist:")
    print(f"   Total companies: {watchlist_stats['total']}")
    print(f"   Enabled for analysis: {watchlist_stats['enabled']}")
    print(f"   High priority: {watchlist_stats['high_priority']}")
    print(f"   Medium priority: {watchlist_stats['med_priority']}")
    print(f"   Low priority: {watchlist_stats['low_priority']}")

    print(f"\n💰 LLM Analysis Costs:")
    total_cost = cost_stats['total_cost'] or 0
    avg_cost = cost_stats['avg_cost'] or 0
    print(f"   Total analyses run: {cost_stats['total_analyses']}")
    print(f"   Companies analyzed: {cost_stats['companies_analyzed']}")
    print(f"   Total cost: ${float(total_cost):.2f}")
    print(f"   Average per analysis: ${float(avg_cost):.4f}")


async def main():
    parser = argparse.ArgumentParser(description='Manage analysis watchlist')
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Add command
    add_parser = subparsers.add_parser('add', help='Add company to watchlist')
    add_parser.add_argument('name', help='Company name (partial match OK)')
    add_parser.add_argument('--reason', '-r', help='Why you\'re watching this company')
    add_parser.add_argument('--priority', '-p', type=int, choices=[1, 2, 3], default=2,
                           help='Priority: 1=high, 2=medium, 3=low')

    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove company from watchlist')
    remove_parser.add_argument('name', help='Company name')

    # Toggle command
    toggle_parser = subparsers.add_parser('toggle', help='Toggle LLM analysis on/off')
    toggle_parser.add_argument('name', help='Company name')

    # List command
    list_parser = subparsers.add_parser('list', help='List watchlist')
    list_parser.add_argument('--enabled-only', '-e', action='store_true',
                            help='Only show companies with LLM analysis enabled')

    # Info command
    info_parser = subparsers.add_parser('info', help='Show detailed company info')
    info_parser.add_argument('name', help='Company name')

    # Stats command
    subparsers.add_parser('stats', help='Show watchlist and cost statistics')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        if args.command == 'add':
            await add_to_watchlist(conn, args.name, args.reason, args.priority)
        elif args.command == 'remove':
            await remove_from_watchlist(conn, args.name)
        elif args.command == 'toggle':
            await toggle_watchlist(conn, args.name)
        elif args.command == 'list':
            await list_watchlist(conn, args.enabled_only)
        elif args.command == 'info':
            await show_company_info(conn, args.name)
        elif args.command == 'stats':
            await show_stats(conn)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
