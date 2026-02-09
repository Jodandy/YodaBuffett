#!/usr/bin/env python3
"""
Backfill Publication Dates from Yahoo Finance Earnings Dates

Yahoo Finance's income/balance/cash flow statements don't include publication dates,
but the earnings_dates endpoint does. This script:
1. Fetches earnings_dates for each company
2. Matches them to financial statement periods
3. Updates the publish_date column in all three tables

Usage:
    python backfill_earnings_dates.py                    # Process all companies
    python backfill_earnings_dates.py --limit 50         # Process 50 companies
    python backfill_earnings_dates.py --symbol VOLV-B.ST # Single company
    python backfill_earnings_dates.py --dry-run          # Preview without updating
"""

import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import asyncpg
import yfinance as yf
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def get_earnings_dates(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch earnings dates from Yahoo Finance."""
    try:
        ticker = yf.Ticker(symbol)
        earnings = ticker.earnings_dates
        if earnings is not None and not earnings.empty:
            return earnings
        return None
    except Exception as e:
        logger.debug(f"Error fetching earnings dates for {symbol}: {e}")
        return None


def match_period_to_earnings_date(
    period_date: datetime.date,
    earnings_dates: pd.DataFrame,
    statement_type: str
) -> Optional[datetime.date]:
    """
    Match a financial statement period to its publication date.

    Logic:
    - The earnings date for a period is the first announcement date AFTER the period ends
    - For Q4/Annual (Dec 31), publication is typically late Jan/early Feb
    - For Q1 (Mar 31), publication is typically late Apr/early May
    - etc.
    """
    if earnings_dates is None or earnings_dates.empty:
        return None

    # Convert earnings dates index to dates
    earnings_list = []
    for dt in earnings_dates.index:
        if pd.notna(dt):
            earnings_list.append(dt.date() if hasattr(dt, 'date') else dt)

    if not earnings_list:
        return None

    # Sort chronologically
    earnings_list.sort()

    # Find the first earnings date AFTER the period end
    # Allow up to 120 days gap (some companies report late)
    period_dt = period_date if isinstance(period_date, datetime) else datetime.combine(period_date, datetime.min.time())

    for earn_date in earnings_list:
        earn_dt = earn_date if isinstance(earn_date, datetime) else datetime.combine(earn_date, datetime.min.time())
        days_after = (earn_dt - period_dt).days

        # Publication should be 15-120 days after period end
        if 15 <= days_after <= 120:
            return earn_date

    return None


def normalize_symbol(symbol: str) -> List[str]:
    """Generate symbol variants for lookup (space vs hyphen, A/B shares)."""
    variants = [symbol]

    # Space to hyphen (e.g., "VOLV B" -> "VOLV-B")
    if ' ' in symbol:
        variants.append(symbol.replace(' ', '-'))

    # Hyphen to space
    if '-' in symbol:
        variants.append(symbol.replace('-', ' '))

    # A-share to B-share (same financials)
    if symbol.endswith('-A') or symbol.endswith(' A'):
        variants.append(symbol[:-1] + 'B')

    return variants


async def get_companies_needing_update(conn: asyncpg.Connection, limit: Optional[int] = None) -> List[Tuple[str, str]]:
    """Get list of (symbol, yahoo_symbol) pairs that have statements missing publish_date."""

    # First get direct matches
    query = """
        SELECT DISTINCT fs.symbol, cm.yahoo_symbol
        FROM financial_statements fs
        JOIN company_master cm ON fs.symbol = cm.primary_ticker
        WHERE fs.publish_date IS NULL
        AND cm.yahoo_symbol IS NOT NULL
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = await conn.fetch(query)
    result = [(row['symbol'], row['yahoo_symbol']) for row in rows]

    # Now find symbols that don't have direct matches but might have variants
    unmatched_query = """
        SELECT DISTINCT fs.symbol
        FROM financial_statements fs
        LEFT JOIN company_master cm ON fs.symbol = cm.primary_ticker
        WHERE fs.publish_date IS NULL
        AND cm.primary_ticker IS NULL
    """
    unmatched_rows = await conn.fetch(unmatched_query)

    for row in unmatched_rows:
        symbol = row['symbol']
        variants = normalize_symbol(symbol)

        for variant in variants:
            if variant == symbol:
                continue
            # Try to find this variant in company_master
            yahoo_sym = await conn.fetchval(
                "SELECT yahoo_symbol FROM company_master WHERE primary_ticker = $1",
                variant
            )
            if yahoo_sym:
                result.append((symbol, yahoo_sym))
                break

    return result


async def get_statement_periods(
    conn: asyncpg.Connection,
    symbol: str
) -> List[Tuple[str, datetime.date, str]]:
    """Get all periods needing publish dates for a symbol."""
    # Get from all three tables
    periods = []

    for table in ['financial_statements', 'balance_sheet_data', 'cash_flow_data']:
        rows = await conn.fetch(f"""
            SELECT period_date, statement_type
            FROM {table}
            WHERE symbol = $1 AND publish_date IS NULL
            ORDER BY period_date DESC
        """, symbol)

        for row in rows:
            periods.append((table, row['period_date'], row['statement_type']))

    return periods


async def update_publish_dates(
    conn: asyncpg.Connection,
    symbol: str,
    updates: List[Tuple[str, datetime.date, datetime.date]],
    dry_run: bool = False
) -> int:
    """Update publish_date for matched periods."""
    if dry_run:
        return len(updates)

    count = 0
    for table, period_date, publish_date in updates:
        try:
            result = await conn.execute(f"""
                UPDATE {table}
                SET publish_date = $1
                WHERE symbol = $2 AND period_date = $3 AND publish_date IS NULL
            """, publish_date, symbol, period_date)

            # Extract number of rows updated
            if 'UPDATE' in result:
                count += int(result.split()[-1])
        except Exception as e:
            logger.error(f"Error updating {table} for {symbol}/{period_date}: {e}")

    return count


async def process_company(
    conn: asyncpg.Connection,
    symbol: str,
    yahoo_symbol: str,
    dry_run: bool = False
) -> Tuple[int, int]:
    """Process a single company. Returns (periods_found, periods_updated)."""
    # Get periods needing dates
    periods = await get_statement_periods(conn, symbol)
    if not periods:
        return 0, 0

    # Fetch earnings dates from Yahoo using the proper Yahoo symbol
    earnings_df = get_earnings_dates(yahoo_symbol)
    if earnings_df is None:
        logger.debug(f"No earnings dates found for {symbol} (yahoo: {yahoo_symbol})")
        return len(periods), 0

    # Match periods to earnings dates
    updates = []
    for table, period_date, statement_type in periods:
        publish_date = match_period_to_earnings_date(period_date, earnings_df, statement_type)
        if publish_date:
            updates.append((table, period_date, publish_date))

    if not updates:
        return len(periods), 0

    # Apply updates
    updated_count = await update_publish_dates(conn, symbol, updates, dry_run)

    return len(periods), updated_count


async def main():
    parser = argparse.ArgumentParser(description='Backfill publication dates from Yahoo Finance earnings dates')
    parser.add_argument('--limit', type=int, help='Limit number of companies to process')
    parser.add_argument('--symbol', type=str, help='Process single symbol')
    parser.add_argument('--dry-run', action='store_true', help='Preview without updating')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Get current stats
        total_fs = await conn.fetchval('SELECT COUNT(*) FROM financial_statements')
        missing_fs = await conn.fetchval('SELECT COUNT(*) FROM financial_statements WHERE publish_date IS NULL')

        logger.info(f"Current state: {missing_fs:,}/{total_fs:,} financial statements missing publish_date ({100*missing_fs/total_fs:.1f}%)")

        # Get companies to process
        if args.symbol:
            # Look up yahoo_symbol for the given symbol
            yahoo_sym = await conn.fetchval(
                "SELECT yahoo_symbol FROM company_master WHERE primary_ticker = $1",
                args.symbol
            )
            if not yahoo_sym:
                logger.error(f"Could not find yahoo_symbol for {args.symbol}")
                return
            companies = [(args.symbol, yahoo_sym)]
        else:
            companies = await get_companies_needing_update(conn, args.limit)

        logger.info(f"Processing {len(companies)} companies...")

        total_periods = 0
        total_updated = 0
        processed = 0

        for symbol, yahoo_symbol in companies:
            periods_found, periods_updated = await process_company(conn, symbol, yahoo_symbol, args.dry_run)
            total_periods += periods_found
            total_updated += periods_updated
            processed += 1

            if periods_updated > 0:
                action = "Would update" if args.dry_run else "Updated"
                logger.info(f"  {symbol}: {action} {periods_updated}/{periods_found} periods")

            # Progress update every 100 companies
            if processed % 100 == 0:
                logger.info(f"Progress: {processed}/{len(companies)} companies, {total_updated} dates {'found' if args.dry_run else 'updated'}")

        # Final summary
        logger.info("")
        logger.info("=" * 50)
        if args.dry_run:
            logger.info(f"DRY RUN - Would update {total_updated:,} publish dates across {processed} companies")
        else:
            logger.info(f"Updated {total_updated:,} publish dates across {processed} companies")

        # Show new stats
        if not args.dry_run:
            new_missing = await conn.fetchval('SELECT COUNT(*) FROM financial_statements WHERE publish_date IS NULL')
            logger.info(f"Remaining missing: {new_missing:,}/{total_fs:,} ({100*new_missing/total_fs:.1f}%)")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
