#!/usr/bin/env python3
"""
Manage screen watchlists.

Usage:
    python manage_screen_watchlist.py create "My Watchlist" --desc "Quality picks"
    python manage_screen_watchlist.py list
    python manage_screen_watchlist.py show "My Watchlist"
    python manage_screen_watchlist.py add "My Watchlist" VOLV-B --source "Tier 2 + Good Cash"
    python manage_screen_watchlist.py add-bulk "My Watchlist" VOLV-B,SAND,ERIC-B --source "Screen Jan 2025"
    python manage_screen_watchlist.py remove "My Watchlist" VOLV-B
    python manage_screen_watchlist.py delete "My Watchlist"
"""

import asyncio
import asyncpg
import argparse
from datetime import date

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def create_watchlist(name: str, description: str = None):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        result = await conn.fetchrow("""
            INSERT INTO screen_watchlists (name, description)
            VALUES ($1, $2)
            RETURNING id, name
        """, name, description)
        print(f"✅ Created watchlist: {result['name']} (id: {result['id']})")
    except asyncpg.UniqueViolationError:
        print(f"❌ Watchlist '{name}' already exists")
    finally:
        await conn.close()


async def list_watchlists():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        watchlists = await conn.fetch("""
            SELECT sw.id, sw.name, sw.description, sw.created_at, sw.updated_at,
                   COUNT(swi.id) as company_count
            FROM screen_watchlists sw
            LEFT JOIN screen_watchlist_items swi ON sw.id = swi.watchlist_id
            GROUP BY sw.id
            ORDER BY sw.updated_at DESC
        """)

        if not watchlists:
            print("No watchlists found. Create one with: python manage_screen_watchlist.py create \"Name\"")
            return

        print(f"\n{'Name':<25} {'Companies':>8} {'Created':>12} {'Modified':>12} {'Description'}")
        print("-" * 95)
        for w in watchlists:
            desc = (w['description'] or '')[:25]
            print(f"{w['name']:<25} {w['company_count']:>8} {str(w['created_at'].date()):>12} {str(w['updated_at'].date()):>12} {desc}")
    finally:
        await conn.close()


async def show_watchlist(name: str):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Get watchlist
        watchlist = await conn.fetchrow("""
            SELECT id, name, description, created_at, updated_at FROM screen_watchlists WHERE name = $1
        """, name)
        
        if not watchlist:
            print(f"❌ Watchlist '{name}' not found")
            return
        
        # Get items with current price
        items = await conn.fetch("""
            SELECT 
                cm.company_name,
                cm.primary_ticker,
                swi.added_at,
                swi.source,
                swi.notes,
                (SELECT close_price FROM daily_price_data 
                 WHERE symbol = cm.primary_ticker 
                 ORDER BY date DESC LIMIT 1) as current_price,
                (SELECT close_price FROM daily_price_data 
                 WHERE symbol = cm.primary_ticker AND date <= swi.added_at::date
                 ORDER BY date DESC LIMIT 1) as price_when_added
            FROM screen_watchlist_items swi
            JOIN company_master cm ON swi.company_id = cm.id
            WHERE swi.watchlist_id = $1
            ORDER BY swi.added_at DESC
        """, watchlist['id'])
        
        print(f"\n📋 {watchlist['name']}")
        if watchlist['description']:
            print(f"   {watchlist['description']}")
        print(f"   {len(items)} companies | Created: {watchlist['created_at'].date()} | Modified: {watchlist['updated_at'].date()}\n")
        
        if not items:
            print("   (empty)")
            return
        
        print(f"{'Ticker':<12} {'Company':<25} {'Added':>12} {'Price Then':>12} {'Price Now':>12} {'Return':>10} {'Source'}")
        print("-" * 110)
        
        for item in items:
            price_then = float(item['price_when_added']) if item['price_when_added'] else None
            price_now = float(item['current_price']) if item['current_price'] else None
            
            if price_then and price_now:
                ret = (price_now - price_then) / price_then * 100
                ret_str = f"{ret:+.1f}%"
            else:
                ret_str = "-"
            
            price_then_str = f"{price_then:.2f}" if price_then else "-"
            price_now_str = f"{price_now:.2f}" if price_now else "-"
            source = (item['source'] or '')[:25]
            
            print(f"{item['primary_ticker']:<12} {item['company_name'][:24]:<25} {str(item['added_at'].date()):>12} {price_then_str:>12} {price_now_str:>12} {ret_str:>10} {source}")
    finally:
        await conn.close()


async def add_company(watchlist_name: str, ticker: str, source: str = None, notes: str = None):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Get watchlist
        watchlist = await conn.fetchrow("""
            SELECT id FROM screen_watchlists WHERE name = $1
        """, watchlist_name)
        
        if not watchlist:
            print(f"❌ Watchlist '{watchlist_name}' not found")
            return
        
        # Get company
        ticker_space = ticker.replace('-', ' ')
        company = await conn.fetchrow("""
            SELECT id, company_name FROM company_master 
            WHERE primary_ticker = $1 OR primary_ticker = $2
        """, ticker, ticker_space)
        
        if not company:
            print(f"❌ Company '{ticker}' not found")
            return
        
        # Add to watchlist
        try:
            await conn.execute("""
                INSERT INTO screen_watchlist_items (watchlist_id, company_id, source, notes)
                VALUES ($1, $2, $3, $4)
            """, watchlist['id'], company['id'], source, notes)
            # Update watchlist modified date
            await conn.execute("""
                UPDATE screen_watchlists SET updated_at = NOW() WHERE id = $1
            """, watchlist['id'])
            print(f"✅ Added {ticker} ({company['company_name']}) to '{watchlist_name}'")
        except asyncpg.UniqueViolationError:
            print(f"⚠️  {ticker} already in '{watchlist_name}'")
    finally:
        await conn.close()


async def add_bulk(watchlist_name: str, tickers: list, source: str = None):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Get watchlist
        watchlist = await conn.fetchrow("""
            SELECT id FROM screen_watchlists WHERE name = $1
        """, watchlist_name)
        
        if not watchlist:
            print(f"❌ Watchlist '{watchlist_name}' not found")
            return
        
        added = 0
        skipped = 0
        not_found = []
        
        for ticker in tickers:
            ticker = ticker.strip()
            ticker_space = ticker.replace('-', ' ')
            
            company = await conn.fetchrow("""
                SELECT id, company_name FROM company_master 
                WHERE primary_ticker = $1 OR primary_ticker = $2
            """, ticker, ticker_space)
            
            if not company:
                not_found.append(ticker)
                continue
            
            try:
                await conn.execute("""
                    INSERT INTO screen_watchlist_items (watchlist_id, company_id, source)
                    VALUES ($1, $2, $3)
                """, watchlist['id'], company['id'], source)
                added += 1
            except asyncpg.UniqueViolationError:
                skipped += 1

        # Update watchlist modified date if we added any
        if added > 0:
            await conn.execute("""
                UPDATE screen_watchlists SET updated_at = NOW() WHERE id = $1
            """, watchlist['id'])

        print(f"✅ Added {added} companies to '{watchlist_name}'")
        if skipped:
            print(f"⚠️  Skipped {skipped} (already in watchlist)")
        if not_found:
            print(f"❌ Not found: {', '.join(not_found)}")
    finally:
        await conn.close()


async def remove_company(watchlist_name: str, ticker: str):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        result = await conn.execute("""
            DELETE FROM screen_watchlist_items
            WHERE watchlist_id = (SELECT id FROM screen_watchlists WHERE name = $1)
              AND company_id = (SELECT id FROM company_master WHERE primary_ticker = $2 OR primary_ticker = $3)
        """, watchlist_name, ticker, ticker.replace('-', ' '))

        if result == "DELETE 1":
            # Update watchlist modified date
            await conn.execute("""
                UPDATE screen_watchlists SET updated_at = NOW() WHERE name = $1
            """, watchlist_name)
            print(f"✅ Removed {ticker} from '{watchlist_name}'")
        else:
            print(f"❌ {ticker} not found in '{watchlist_name}'")
    finally:
        await conn.close()


async def delete_watchlist(name: str):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        result = await conn.execute("""
            DELETE FROM screen_watchlists WHERE name = $1
        """, name)
        
        if result == "DELETE 1":
            print(f"✅ Deleted watchlist '{name}'")
        else:
            print(f"❌ Watchlist '{name}' not found")
    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(description='Manage screen watchlists')
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # create
    create_parser = subparsers.add_parser('create', help='Create a new watchlist')
    create_parser.add_argument('name', help='Watchlist name')
    create_parser.add_argument('--desc', help='Description')
    
    # list
    subparsers.add_parser('list', help='List all watchlists')
    
    # show
    show_parser = subparsers.add_parser('show', help='Show watchlist contents')
    show_parser.add_argument('name', help='Watchlist name')
    
    # add
    add_parser = subparsers.add_parser('add', help='Add company to watchlist')
    add_parser.add_argument('name', help='Watchlist name')
    add_parser.add_argument('ticker', help='Company ticker')
    add_parser.add_argument('--source', help='Source/screen that found this')
    add_parser.add_argument('--notes', help='Personal notes')
    
    # add-bulk
    bulk_parser = subparsers.add_parser('add-bulk', help='Add multiple companies')
    bulk_parser.add_argument('name', help='Watchlist name')
    bulk_parser.add_argument('tickers', help='Comma-separated tickers')
    bulk_parser.add_argument('--source', help='Source/screen')
    
    # remove
    remove_parser = subparsers.add_parser('remove', help='Remove company from watchlist')
    remove_parser.add_argument('name', help='Watchlist name')
    remove_parser.add_argument('ticker', help='Company ticker')
    
    # delete
    delete_parser = subparsers.add_parser('delete', help='Delete a watchlist')
    delete_parser.add_argument('name', help='Watchlist name')
    
    args = parser.parse_args()
    
    if args.command == 'create':
        asyncio.run(create_watchlist(args.name, args.desc))
    elif args.command == 'list':
        asyncio.run(list_watchlists())
    elif args.command == 'show':
        asyncio.run(show_watchlist(args.name))
    elif args.command == 'add':
        asyncio.run(add_company(args.name, args.ticker, args.source, args.notes))
    elif args.command == 'add-bulk':
        tickers = args.tickers.split(',')
        asyncio.run(add_bulk(args.name, tickers, args.source))
    elif args.command == 'remove':
        asyncio.run(remove_company(args.name, args.ticker))
    elif args.command == 'delete':
        asyncio.run(delete_watchlist(args.name))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
