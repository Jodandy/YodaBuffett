#!/usr/bin/env python3
"""
IMPROVED Daily Market Data Worker with Intelligent Gap Detection

Key Improvement: Instead of blindly fetching last 7 days, this worker:
1. Checks the actual last date we have data for each company
2. Calculates the gap
3. Backfills exactly what's missing (up to max 90 days to avoid huge backlogs)

This means:
- No manual backfill needed for gaps < 90 days
- Workers automatically catch up if they miss a few days
- More efficient (don't re-fetch data we already have)
"""

import asyncio
import sys
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional
import logging

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workers.worker_config import get_config, setup_worker_logging
from domains.market_data.services.historical_data_ingestor import HistoricalDataIngestor


class SmartMarketDataWorker:
    """
    Improved worker with intelligent gap detection
    """

    def __init__(self):
        self.config = get_config()
        self.logger = setup_worker_logging()
        self.ingestor = HistoricalDataIngestor()

        # Configuration
        self.max_backfill_days = 90  # Don't backfill more than 90 days automatically
        self.default_lookback = 7     # If no data exists, fetch last 7 days

    async def get_last_price_date(self, symbol: str) -> Optional[date]:
        """Get the last date we have price data for this company"""
        return await self.ingestor.conn.fetchval("""
            SELECT MAX(date) FROM daily_price_data
            WHERE symbol = $1
        """, symbol)

    async def calculate_gap_days(self, symbol: str) -> int:
        """Calculate how many days of data are missing"""
        last_date = await self.get_last_price_date(symbol)

        if not last_date:
            # No data at all - fetch default lookback
            return self.default_lookback

        today = date.today()
        gap_days = (today - last_date).days

        # Cap at max_backfill_days to avoid massive backlogs
        if gap_days > self.max_backfill_days:
            self.logger.warning(
                f"   ⚠️  {symbol}: {gap_days} day gap (capping at {self.max_backfill_days} days)"
            )
            return self.max_backfill_days

        return gap_days

    async def update_company_with_gap_detection(self, company: Dict) -> Dict:
        """Update a single company, intelligently detecting and filling gaps"""

        symbol = company['primary_ticker']
        result = {
            'symbol': symbol,
            'company_name': company['company_name'],
            'success': False,
            'gap_days': 0,
            'points_added': 0,
            'error': None
        }

        try:
            # Calculate gap
            gap_days = await self.calculate_gap_days(symbol)
            result['gap_days'] = gap_days

            if gap_days == 0:
                self.logger.info(f"   ✅ {symbol}: Already up to date")
                result['success'] = True
                return result

            # Fetch the gap
            self.logger.info(f"   🔄 {symbol}: Backfilling {gap_days} days...")

            success = await self.ingestor.ingest_historical_data(
                symbol=symbol,
                days_back=gap_days + 2,  # +2 buffer for weekends
                calculate_metrics=True,
                yahoo_symbol=company['yahoo_symbol']
            )

            if success:
                # Check how many points we actually added
                last_date = await self.get_last_price_date(symbol)
                result['success'] = True
                result['last_date'] = last_date
                self.logger.info(f"   ✅ {symbol}: Updated to {last_date}")
            else:
                result['error'] = "Failed to fetch data"
                self.logger.warning(f"   ❌ {symbol}: Update failed")

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"   ❌ {symbol}: {e}")

        return result

    async def run_smart_update(self, dry_run: bool = False, limit: Optional[int] = None):
        """Run the smart update with gap detection"""

        self.logger.info("=" * 70)
        self.logger.info("📈 SMART MARKET DATA WORKER (Gap Detection)")
        self.logger.info("=" * 70)

        if dry_run:
            self.logger.info("🧪 DRY RUN - Checking gaps without updating")

        await self.ingestor.connect()

        try:
            # Get companies
            companies = await self.ingestor.conn.fetch("""
                SELECT
                    id,
                    company_name,
                    primary_ticker,
                    yahoo_symbol
                FROM company_master
                WHERE primary_ticker IS NOT NULL
                AND yahoo_symbol IS NOT NULL
                AND yahoo_symbol LIKE '%.ST'  -- Swedish companies
                ORDER BY document_count DESC NULLS LAST
                LIMIT $1
            """, limit or 10000)

            self.logger.info(f"📊 Found {len(companies)} companies to check")
            print()

            results = []
            for i, company in enumerate(companies[:limit] if limit else companies, 1):
                symbol = company['primary_ticker']

                # Check gap
                gap_days = await self.calculate_gap_days(symbol)

                if dry_run:
                    last_date = await self.get_last_price_date(symbol)
                    status = "✅" if gap_days == 0 else f"🔄 {gap_days}d"
                    print(f"[{i:4d}/{len(companies)}] {status} {symbol:12s} | Last: {last_date or 'Never'}")
                else:
                    if gap_days > 0:
                        result = await self.update_company_with_gap_detection(
                            dict(company)
                        )
                        results.append(result)
                    else:
                        self.logger.info(f"[{i:4d}/{len(companies)}] ✅ {symbol}: Up to date")

            # Summary
            if not dry_run and results:
                print()
                print("=" * 70)
                print("📊 UPDATE SUMMARY")
                print("=" * 70)
                successful = [r for r in results if r['success']]
                failed = [r for r in results if not r['success']]

                print(f"✅ Successful: {len(successful)}")
                print(f"❌ Failed: {len(failed)}")
                print(f"📈 Total gaps filled: {sum(r['gap_days'] for r in successful)} days")

                if failed:
                    print()
                    print("Failed companies:")
                    for r in failed:
                        print(f"   - {r['symbol']}: {r['error']}")

        finally:
            await self.ingestor.disconnect()


async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Smart Market Data Worker with Gap Detection')
    parser.add_argument('--dry-run', action='store_true', help='Show gaps without updating')
    parser.add_argument('--limit', type=int, help='Limit number of companies to process')

    args = parser.parse_args()

    worker = SmartMarketDataWorker()
    await worker.run_smart_update(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    asyncio.run(main())
