#!/usr/bin/env python3
"""
Historical Dimensions Backfill

Calculates dimension scores for historical dates to enable:
- Backtesting the fat pitch system
- ML training to find optimal dimension weights
- Comparing predictions to actual forward returns

Usage:
    # Full backfill (monthly snapshots, 3 years)
    python historical_dimensions_backfill.py

    # Custom date range
    python historical_dimensions_backfill.py --start-date 2022-01-01 --end-date 2024-12-31

    # Limit companies (for testing)
    python historical_dimensions_backfill.py --limit 50

    # Weekly snapshots instead of monthly
    python historical_dimensions_backfill.py --frequency weekly

    # Resume (skips already calculated)
    python historical_dimensions_backfill.py --resume

    # Specific companies only
    python historical_dimensions_backfill.py --company "Volvo"

    # Dry run (show what would be calculated)
    python historical_dimensions_backfill.py --dry-run
"""

import asyncio
import asyncpg
import argparse
import time
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional, Set, Tuple
from calendar import monthrange
import logging

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Import calculators
from domains.dimensions.calculators.profitability_calculator import ProfitabilityCalculator
from domains.dimensions.calculators.returns_calculator import ReturnsCalculator
from domains.dimensions.calculators.growth_calculator import GrowthCalculator
from domains.dimensions.calculators.financial_health_calculator import FinancialHealthCalculator
from domains.dimensions.calculators.value_calculator import ValueCalculator
from domains.dimensions.calculators.risk_calculator import RiskCalculator
from domains.dimensions.calculators.momentum_calculator import MomentumCalculator
from domains.dimensions.calculators.quality_calculator import QualityCalculator
from domains.dimensions.calculators.sentiment_calculator import SentimentCalculator
from domains.dimensions.calculators.earnings_quality_calculator import EarningsQualityCalculator
from domains.dimensions.calculators.capital_allocation_calculator import CapitalAllocationCalculator
from domains.dimensions.calculators.valuation_percentile_calculator import ValuationPercentileCalculator
from domains.dimensions.calculators.beneish_mscore_calculator import BeneishMScoreCalculator
from domains.dimensions.calculators.working_capital_calculator import WorkingCapitalCalculator
from domains.dimensions.repositories.dimension_repository import DimensionRepository

logging.basicConfig(
    level=logging.WARNING,  # Reduce noise from calculators
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # But keep our own logs visible


CALCULATORS = {
    # Business fundamentals (price-independent)
    "profitability": ProfitabilityCalculator,
    "returns": ReturnsCalculator,
    "growth": GrowthCalculator,
    "financial_health": FinancialHealthCalculator,
    "earnings_quality": EarningsQualityCalculator,
    "capital_allocation": CapitalAllocationCalculator,
    "working_capital": WorkingCapitalCalculator,
    "beneish_mscore": BeneishMScoreCalculator,
    # Market perception (price-dependent)
    "value": ValueCalculator,
    "risk": RiskCalculator,
    "momentum": MomentumCalculator,
    "quality": QualityCalculator,
    "valuation_percentile": ValuationPercentileCalculator,
    # AI-derived
    "sentiment": SentimentCalculator,
}


def generate_monthly_dates(start_date: date, end_date: date) -> List[date]:
    """Generate end-of-month dates between start and end."""
    dates = []
    current = date(start_date.year, start_date.month, 1)

    while current <= end_date:
        # Get last day of current month
        _, last_day = monthrange(current.year, current.month)
        month_end = date(current.year, current.month, last_day)

        if month_end <= end_date and month_end >= start_date:
            dates.append(month_end)

        # Move to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return dates


def generate_weekly_dates(start_date: date, end_date: date) -> List[date]:
    """Generate Friday dates (end of trading week) between start and end."""
    dates = []
    current = start_date

    # Find first Friday
    while current.weekday() != 4:  # 4 = Friday
        current += timedelta(days=1)

    while current <= end_date:
        dates.append(current)
        current += timedelta(days=7)

    return dates


def generate_quarterly_dates(start_date: date, end_date: date) -> List[date]:
    """Generate quarter-end dates between start and end."""
    quarter_ends = [
        (3, 31),   # Q1
        (6, 30),   # Q2
        (9, 30),   # Q3
        (12, 31),  # Q4
    ]

    dates = []
    for year in range(start_date.year, end_date.year + 1):
        for month, day in quarter_ends:
            qe = date(year, month, day)
            if start_date <= qe <= end_date:
                dates.append(qe)

    return sorted(dates)


async def get_companies(conn, limit: Optional[int] = None, company_name: Optional[str] = None) -> List[Dict]:
    """Get companies to process - only those with actual financial data."""
    nordic_countries = ['SE', 'NO', 'DK', 'FI', 'Sverige', 'Norge', 'Danmark', 'Finland', 'Nordic']

    if company_name:
        row = await conn.fetchrow("""
            SELECT id, company_name, primary_ticker, sector
            FROM company_master
            WHERE company_name ILIKE $1
            LIMIT 1
        """, f"%{company_name}%")
        return [dict(row)] if row else []

    # Only get companies that have financial statement data
    query = """
        SELECT DISTINCT cm.id, cm.company_name, cm.primary_ticker, cm.sector
        FROM company_master cm
        INNER JOIN financial_statements fs ON cm.primary_ticker = fs.symbol
        WHERE cm.country = ANY($1)
        ORDER BY cm.company_name
    """

    rows = await conn.fetch(query, nordic_countries)

    # Sort by market cap if we need to limit
    if limit:
        # Re-query with market cap ordering
        query = """
            SELECT DISTINCT cm.id, cm.company_name, cm.primary_ticker, cm.sector, cm.market_cap_usd
            FROM company_master cm
            INNER JOIN financial_statements fs ON cm.primary_ticker = fs.symbol
            WHERE cm.country = ANY($1)
            ORDER BY cm.market_cap_usd DESC NULLS LAST
            LIMIT $2
        """
        rows = await conn.fetch(query, nordic_countries, limit)

    return [dict(row) for row in rows]


async def get_existing_scores(conn, score_dates: List[date]) -> Set[Tuple[str, date, str]]:
    """Get already calculated (company_id, score_date, dimension_code) tuples."""
    if not score_dates:
        return set()

    rows = await conn.fetch("""
        SELECT company_id::text, score_date, dimension_code
        FROM daily_dimension_scores
        WHERE score_date = ANY($1)
    """, score_dates)

    return {(row['company_id'], row['score_date'], row['dimension_code']) for row in rows}


async def get_data_availability(conn) -> Dict[str, date]:
    """Check earliest available data for key tables."""
    availability = {}

    # Financial statements
    row = await conn.fetchrow("SELECT MIN(period_date) as min_date FROM financial_statements")
    availability['financial_statements'] = row['min_date'] if row else None

    # Balance sheet
    row = await conn.fetchrow("SELECT MIN(period_date) as min_date FROM balance_sheet_data")
    availability['balance_sheet_data'] = row['min_date'] if row else None

    # Price data
    row = await conn.fetchrow("SELECT MIN(date) as min_date FROM daily_price_data")
    availability['daily_price_data'] = row['min_date'] if row else None

    return availability


async def calculate_dimensions_for_date(
    conn,
    repo: DimensionRepository,
    companies: List[Dict],
    score_date: date,
    existing_scores: Set[Tuple[str, date, str]],
    resume: bool = True,
) -> Dict[str, int]:
    """Calculate all dimensions for all companies on a specific date."""

    stats = {
        'calculated': 0,
        'skipped': 0,
        'failed': 0,
        'no_data': 0,
    }

    for company in companies:
        company_id = str(company['id'])
        company_name = company['company_name']

        for dim_code, calculator_class in CALCULATORS.items():
            # Skip if already calculated (resume mode)
            if resume and (company_id, score_date, dim_code) in existing_scores:
                stats['skipped'] += 1
                continue

            try:
                calc = calculator_class(db_conn=conn)
                score = await calc.calculate(company_id, score_date)

                if score:
                    score.computation_time_ms = 0
                    score.computed_at = datetime.now()
                    score.calculator_version = "2.0.0-historical"

                    await repo.store_dimension_score(score)
                    stats['calculated'] += 1
                else:
                    stats['no_data'] += 1

            except Exception as e:
                stats['failed'] += 1
                logger.debug(f"Error calculating {dim_code} for {company_name} on {score_date}: {e}")

    return stats


async def main():
    parser = argparse.ArgumentParser(description='Historical dimensions backfill')
    parser.add_argument('--start-date', help='Start date (YYYY-MM-DD), default: 3 years ago')
    parser.add_argument('--end-date', help='End date (YYYY-MM-DD), default: yesterday')
    parser.add_argument('--frequency', choices=['monthly', 'weekly', 'quarterly'],
                        default='monthly', help='Snapshot frequency')
    parser.add_argument('--limit', type=int, help='Max companies to process')
    parser.add_argument('--company', help='Process specific company only')
    parser.add_argument('--resume', action='store_true', default=True,
                        help='Skip already calculated scores (default: True)')
    parser.add_argument('--no-resume', action='store_true', help='Recalculate everything')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be calculated')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    # Parse dates
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    else:
        end_date = date.today() - timedelta(days=1)

    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = end_date - timedelta(days=3*365)  # 3 years back

    resume = not args.no_resume

    # Generate snapshot dates
    if args.frequency == 'monthly':
        snapshot_dates = generate_monthly_dates(start_date, end_date)
    elif args.frequency == 'weekly':
        snapshot_dates = generate_weekly_dates(start_date, end_date)
    else:
        snapshot_dates = generate_quarterly_dates(start_date, end_date)

    print(f"\n{'='*70}")
    print("HISTORICAL DIMENSIONS BACKFILL")
    print(f"{'='*70}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Frequency: {args.frequency}")
    print(f"Snapshot dates: {len(snapshot_dates)}")
    print(f"Resume mode: {resume}")

    if args.dry_run:
        print(f"\n[DRY RUN MODE]")
        print(f"\nSnapshot dates to process:")
        for d in snapshot_dates[:10]:
            print(f"  {d}")
        if len(snapshot_dates) > 10:
            print(f"  ... and {len(snapshot_dates) - 10} more")
        return

    conn = await asyncpg.connect(DATABASE_URL)
    repo = DimensionRepository(conn)

    try:
        # Check data availability
        availability = await get_data_availability(conn)
        print(f"\nData availability:")
        for table, min_date in availability.items():
            print(f"  {table}: from {min_date}")

        # Filter dates to data availability
        earliest_data = min(d for d in availability.values() if d)
        valid_dates = [d for d in snapshot_dates if d >= earliest_data]

        if len(valid_dates) < len(snapshot_dates):
            print(f"\nFiltered to {len(valid_dates)} dates (data starts {earliest_data})")
            snapshot_dates = valid_dates

        # Get companies
        companies = await get_companies(conn, args.limit, args.company)
        print(f"Companies to process: {len(companies)}")

        if not companies:
            print("No companies found!")
            return

        # Get existing scores for resume
        existing_scores = set()
        if resume:
            print("\nChecking existing scores...")
            existing_scores = await get_existing_scores(conn, snapshot_dates)
            print(f"Found {len(existing_scores)} existing scores")

        # Calculate estimates
        total_calculations = len(snapshot_dates) * len(companies) * len(CALCULATORS)
        to_skip = len(existing_scores) if resume else 0
        remaining = total_calculations - to_skip

        print(f"\nTotal possible calculations: {total_calculations:,}")
        print(f"Already calculated (skip): {to_skip:,}")
        print(f"Remaining to calculate: {remaining:,}")

        if remaining == 0:
            print("\nNothing to calculate - all scores exist!")
            return

        # Estimate time (rough: 50ms per calculation)
        est_seconds = remaining * 0.05
        est_minutes = est_seconds / 60
        print(f"Estimated time: {est_minutes:.0f} minutes")

        print(f"\n{'='*70}")
        print("PROCESSING")
        print(f"{'='*70}\n")

        # Process each date
        overall_stats = {'calculated': 0, 'skipped': 0, 'failed': 0, 'no_data': 0}
        start_time = time.time()

        for i, score_date in enumerate(snapshot_dates, 1):
            date_start = time.time()

            stats = await calculate_dimensions_for_date(
                conn, repo, companies, score_date, existing_scores, resume
            )

            # Update overall stats
            for key in overall_stats:
                overall_stats[key] += stats[key]

            elapsed = time.time() - date_start
            total_elapsed = time.time() - start_time

            # Progress
            pct = (i / len(snapshot_dates)) * 100
            rate = overall_stats['calculated'] / total_elapsed if total_elapsed > 0 else 0

            print(f"[{i}/{len(snapshot_dates)}] {score_date}: "
                  f"+{stats['calculated']} calculated, "
                  f"{stats['skipped']} skipped, "
                  f"{stats['no_data']} no data "
                  f"({elapsed:.1f}s, {rate:.1f}/s)")

            if args.verbose:
                print(f"         Running total: {overall_stats['calculated']:,} calculated")

        # Final summary
        total_time = time.time() - start_time

        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        print(f"Total time: {total_time/60:.1f} minutes")
        print(f"Dates processed: {len(snapshot_dates)}")
        print(f"Companies: {len(companies)}")
        print(f"Dimensions: {len(CALCULATORS)}")
        print(f"\nScores:")
        print(f"  Calculated: {overall_stats['calculated']:,}")
        print(f"  Skipped (existing): {overall_stats['skipped']:,}")
        print(f"  No data: {overall_stats['no_data']:,}")
        print(f"  Failed: {overall_stats['failed']:,}")

        if overall_stats['calculated'] > 0:
            rate = overall_stats['calculated'] / total_time
            print(f"\nRate: {rate:.1f} scores/second")

        # Verify final count
        count = await conn.fetchval("SELECT COUNT(*) FROM daily_dimension_scores")
        unique_dates = await conn.fetchval("SELECT COUNT(DISTINCT score_date) FROM daily_dimension_scores")
        print(f"\nDatabase now has: {count:,} dimension scores across {unique_dates} dates")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
