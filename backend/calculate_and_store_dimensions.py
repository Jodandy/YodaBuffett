#!/usr/bin/env python3
"""
Calculate and store dimension scores for companies.

This script:
1. Calculates all dimension scores for a company
2. Stores them in daily_dimension_scores table
3. The metadata (DuPont, Z-score, growth quality, etc.) is persisted for future LLM use

Usage:
    python calculate_and_store_dimensions.py --company "Volvo"
    python calculate_and_store_dimensions.py --all --limit 100
    python calculate_and_store_dimensions.py --sector "Industrimaskiner" --limit 50
"""

import asyncio
import asyncpg
import argparse
import time
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional

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


async def calculate_and_store_for_company(
    conn,
    repo: DimensionRepository,
    company_id: str,
    company_name: str,
    score_date: date,
    verbose: bool = True
) -> Dict[str, float]:
    """Calculate all dimensions for a company and store them."""

    results = {}

    for dim_code, calculator_class in CALCULATORS.items():
        start_time = time.time()

        try:
            calc = calculator_class(db_conn=conn)
            score = await calc.calculate(company_id, score_date)

            if score:
                # Add timing info
                elapsed_ms = int((time.time() - start_time) * 1000)
                score.computation_time_ms = elapsed_ms
                score.computed_at = datetime.now()
                score.calculator_version = "2.0.0"

                # Store to database
                await repo.store_dimension_score(score)
                results[dim_code] = score.score

                if verbose:
                    print(f"  {dim_code}: {score.score:.1f} (stored in {elapsed_ms}ms)")
            else:
                if verbose:
                    print(f"  {dim_code}: No data")

        except Exception as e:
            if verbose:
                print(f"  {dim_code}: Error - {e}")

    return results


async def get_companies(conn, sector: Optional[str] = None, limit: int = 100) -> List[Dict]:
    """Get companies to process."""

    # Include both short codes and full country names
    nordic_countries = ['SE', 'NO', 'DK', 'FI', 'Sverige', 'Norge', 'Danmark', 'Finland', 'Nordic']

    if sector:
        query = """
            SELECT id, company_name, primary_ticker, sector
            FROM company_master
            WHERE sector = $1
            AND country = ANY($2)
            ORDER BY market_cap_usd DESC NULLS LAST
            LIMIT $3
        """
        rows = await conn.fetch(query, sector, nordic_countries, limit)
    else:
        query = """
            SELECT id, company_name, primary_ticker, sector
            FROM company_master
            WHERE country = ANY($1)
            ORDER BY market_cap_usd DESC NULLS LAST
            LIMIT $2
        """
        rows = await conn.fetch(query, nordic_countries, limit)

    return [dict(row) for row in rows]


async def get_company_by_name(conn, name: str) -> Optional[Dict]:
    """Get a specific company by name."""
    row = await conn.fetchrow("""
        SELECT id, company_name, primary_ticker, sector
        FROM company_master
        WHERE company_name ILIKE $1
        LIMIT 1
    """, f"%{name}%")
    return dict(row) if row else None


async def main():
    parser = argparse.ArgumentParser(description='Calculate and store dimension scores')
    parser.add_argument('--company', help='Company name to process')
    parser.add_argument('--all', action='store_true', help='Process all companies')
    parser.add_argument('--sector', help='Process companies in a specific sector')
    parser.add_argument('--limit', type=int, default=100, help='Max companies to process')
    parser.add_argument('--date', help='Score date (YYYY-MM-DD), defaults to yesterday')
    args = parser.parse_args()

    # Parse date
    if args.date:
        score_date = date.fromisoformat(args.date)
    else:
        score_date = date.today() - timedelta(days=1)

    conn = await asyncpg.connect(DATABASE_URL)
    repo = DimensionRepository(conn)

    try:
        if args.company:
            # Single company
            company = await get_company_by_name(conn, args.company)
            if not company:
                print(f"Company not found: {args.company}")
                return

            print(f"\n{'='*60}")
            print(f"Processing: {company['company_name']} ({company['primary_ticker']})")
            print(f"Sector: {company['sector'] or 'N/A'}")
            print(f"Score date: {score_date}")
            print(f"{'='*60}")

            results = await calculate_and_store_for_company(
                conn, repo, company['id'], company['company_name'], score_date
            )

            print(f"\nStored {len(results)} dimensions to database.")

        elif args.all or args.sector:
            # Multiple companies
            companies = await get_companies(conn, args.sector, args.limit)

            print(f"\n{'='*60}")
            print(f"Processing {len(companies)} companies")
            print(f"Score date: {score_date}")
            if args.sector:
                print(f"Sector: {args.sector}")
            print(f"{'='*60}\n")

            total_scores = 0
            successful = 0

            for i, company in enumerate(companies, 1):
                print(f"[{i}/{len(companies)}] {company['company_name']}...")

                results = await calculate_and_store_for_company(
                    conn, repo, company['id'], company['company_name'], score_date,
                    verbose=False
                )

                if results:
                    successful += 1
                    total_scores += len(results)
                    print(f"  -> {len(results)} dimensions stored")
                else:
                    print(f"  -> No data")

            print(f"\n{'='*60}")
            print(f"SUMMARY")
            print(f"{'='*60}")
            print(f"Companies processed: {len(companies)}")
            print(f"Companies with scores: {successful}")
            print(f"Total dimension scores stored: {total_scores}")

        else:
            print("Specify --company, --all, or --sector")
            parser.print_help()

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
