#!/usr/bin/env python3
"""
Daily Dimensions Worker

Pre-computes dimension scores for all Nordic companies.
Scheduled to run at 4:00 AM daily via macOS LaunchAgent.

Usage:
    python -m workers.daily_dimensions_worker --dry-run    # Preview
    python -m workers.daily_dimensions_worker --run-now    # Execute now
    python -m workers.daily_dimensions_worker --date 2025-02-05  # Backfill specific date
    python -m workers.daily_dimensions_worker --dimension value  # Single dimension only
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import date, datetime
import argparse
import asyncpg

from domains.dimensions.calculators.base import calculator_registry
from domains.dimensions.repositories.dimension_repository import DimensionRepository

# Import calculators to register them
from domains.dimensions.calculators.value_calculator import ValueCalculator
from domains.dimensions.calculators.momentum_calculator import MomentumCalculator
from domains.dimensions.calculators.quality_calculator import QualityCalculator
from domains.dimensions.calculators.risk_calculator import RiskCalculator
from domains.dimensions.calculators.sentiment_calculator import SentimentCalculator
# Sophisticated fundamental calculators
from domains.dimensions.calculators.financial_health_calculator import FinancialHealthCalculator
from domains.dimensions.calculators.growth_calculator import GrowthCalculator
from domains.dimensions.calculators.profitability_calculator import ProfitabilityCalculator
from domains.dimensions.calculators.returns_calculator import ReturnsCalculator
# New specialized calculators
from domains.dimensions.calculators.earnings_quality_calculator import EarningsQualityCalculator
from domains.dimensions.calculators.capital_allocation_calculator import CapitalAllocationCalculator
from domains.dimensions.calculators.valuation_percentile_calculator import ValuationPercentileCalculator
from domains.dimensions.calculators.beneish_mscore_calculator import BeneishMScoreCalculator
from domains.dimensions.calculators.working_capital_calculator import WorkingCapitalCalculator

# Setup logging
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'daily-dimensions-worker.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DailyDimensionsWorker:
    """Worker for daily dimension score computation."""

    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

    # Default dimension weights for composite score
    # All weights equal by default - no assumptions without backtesting
    # Total: 14 dimensions, each ~7.14% weight
    COMPOSITE_WEIGHTS = {
        # === BUSINESS FUNDAMENTALS (price-independent) ===
        'profitability': 0.07,          # Margin levels and efficiency
        'growth': 0.07,                 # Revenue/earnings growth rates
        'financial_health': 0.07,       # Balance sheet strength, Altman Z
        'returns': 0.07,                # ROE/ROA/ROIC with DuPont
        'quality': 0.07,                # Overall quality metrics
        'earnings_quality': 0.07,       # Cash backing of earnings, accruals
        'capital_allocation': 0.07,     # How management deploys cash
        'working_capital': 0.07,        # Operating cycle efficiency
        'beneish_mscore': 0.07,         # Manipulation probability
        # === MARKET PERCEPTION (price-dependent) ===
        'value': 0.07,                  # Valuation vs fundamentals
        'valuation_percentile': 0.07,   # Current vs own history
        'momentum': 0.07,               # Price trends, technicals
        'risk': 0.08,                   # Volatility, drawdown
        'sentiment': 0.08,              # Document embedding analysis
    }

    def __init__(
        self,
        dry_run: bool = False,
        force_date: date = None,
        dimensions: list = None,
        skip_composite: bool = False,
    ):
        self.dry_run = dry_run
        self.score_date = force_date or date.today()
        self.dimensions = dimensions or calculator_registry.list_dimensions()
        self.skip_composite = skip_composite
        self.conn = None
        self.repository = None

    async def setup(self):
        """Initialize database connection."""
        logger.info(f"Connecting to database...")
        self.conn = await asyncpg.connect(self.DATABASE_URL)
        self.repository = DimensionRepository(self.conn)
        logger.info(f"Connected successfully")

    async def cleanup(self):
        """Close database connection."""
        if self.conn:
            await self.conn.close()

    async def get_active_companies(self) -> list:
        """Get list of active Nordic companies."""
        return await self.repository.get_active_companies(['SE', 'NO', 'DK', 'FI'])

    async def compute_dimension(
        self,
        dimension_code: str,
        company_ids: list,
    ) -> dict:
        """Compute a single dimension for all companies."""

        calculator = calculator_registry.get(dimension_code, db_conn=self.conn)

        if not calculator:
            logger.error(f"No calculator found for dimension: {dimension_code}")
            return {"error": f"Unknown dimension: {dimension_code}"}

        logger.info(f"Computing {dimension_code} for {len(company_ids)} companies...")

        # Run batch calculation
        result = await calculator.calculate_batch(company_ids, self.score_date)

        # Store results
        if not self.dry_run and result.scores:
            stored = await self.repository.store_dimension_scores(result.scores)
            logger.info(f"Stored {stored} scores for {dimension_code}")

        # Log computation
        if not self.dry_run:
            await self.repository.log_computation(result)

        return {
            'dimension': dimension_code,
            'companies_processed': result.companies_processed,
            'companies_succeeded': result.companies_succeeded,
            'companies_failed': result.companies_failed,
            'companies_skipped': result.companies_skipped,
            'avg_score': sum(s.score for s in result.scores) / len(result.scores) if result.scores else 0,
            'avg_confidence': sum(s.confidence or 0 for s in result.scores) / len(result.scores) if result.scores else 0,
            'duration_ms': result.total_duration_ms,
            'errors': result.errors,
        }

    async def compute_composite_scores(self, company_ids: list):
        """Compute composite scores combining all dimensions."""

        if self.skip_composite:
            logger.info("Skipping composite score calculation")
            return

        logger.info(f"Computing composite scores for {len(company_ids)} companies...")

        if self.dry_run:
            logger.info("DRY RUN - Would compute composite scores")
            return

        # Get all dimension scores for the date
        dimension_scores = await self.repository.get_all_dimension_scores(
            company_ids, self.score_date
        )

        composite_count = 0

        for company_id in company_ids:
            company_scores = dimension_scores.get(company_id, {})

            if len(company_scores) < 2:
                continue

            # Calculate weighted composite
            total_weight = 0
            weighted_sum = 0
            missing = []

            for dim, weight in self.COMPOSITE_WEIGHTS.items():
                if dim in company_scores:
                    weighted_sum += company_scores[dim] * weight
                    total_weight += weight
                else:
                    missing.append(dim)

            if total_weight > 0:
                composite_score = weighted_sum / total_weight

                await self.repository.store_composite_score(
                    company_id=company_id,
                    score_date=self.score_date,
                    composite_code='overall',
                    score=composite_score,
                    dimension_scores=company_scores,
                    dimension_weights={k: v for k, v in self.COMPOSITE_WEIGHTS.items() if k in company_scores},
                    confidence=total_weight / sum(self.COMPOSITE_WEIGHTS.values()),
                    missing_dimensions=missing,
                )
                composite_count += 1

        logger.info(f"Computed {composite_count} composite scores")

    async def run(self):
        """Run the daily dimensions computation."""
        start_time = datetime.now()

        logger.info("=" * 60)
        logger.info("DAILY DIMENSIONS WORKER")
        logger.info(f"Date: {self.score_date}")
        logger.info(f"Mode: {'DRY RUN' if self.dry_run else 'PRODUCTION'}")
        logger.info(f"Dimensions: {', '.join(self.dimensions)}")
        logger.info("=" * 60)

        try:
            await self.setup()

            # Get active companies
            company_ids = await self.get_active_companies()
            logger.info(f"Processing {len(company_ids)} companies")

            if self.dry_run:
                logger.info("\nDRY RUN - Would compute:")
                for dim in self.dimensions:
                    logger.info(f"  - {dim}")
                logger.info(f"  - composite scores")
                return

            # Compute each dimension
            results = []
            for dimension_code in self.dimensions:
                # Check if already computed
                if await self.repository.check_already_computed(dimension_code, self.score_date):
                    logger.info(f"Skipping {dimension_code} - already computed for {self.score_date}")
                    continue

                result = await self.compute_dimension(dimension_code, company_ids)
                results.append(result)

                # Log progress
                if 'error' not in result:
                    logger.info(
                        f"  {dimension_code}: "
                        f"{result['companies_succeeded']}/{result['companies_processed']} succeeded, "
                        f"avg score: {result['avg_score']:.1f}, "
                        f"duration: {result['duration_ms']}ms"
                    )
                    if result['errors']:
                        logger.warning(f"    Errors: {result['errors']}")

            # Compute composite scores
            await self.compute_composite_scores(company_ids)

            # Summary
            logger.info("\n" + "=" * 60)
            logger.info("COMPUTATION SUMMARY")
            logger.info("=" * 60)

            total_succeeded = 0
            total_failed = 0

            for result in results:
                if 'error' not in result:
                    total_succeeded += result['companies_succeeded']
                    total_failed += result['companies_failed']
                    logger.info(
                        f"{result['dimension']:15} | "
                        f"Score: {result['avg_score']:5.1f} | "
                        f"Conf: {result['avg_confidence']:.2f} | "
                        f"OK: {result['companies_succeeded']:4} | "
                        f"Fail: {result['companies_failed']:3}"
                    )

            duration = datetime.now() - start_time
            logger.info(f"\nTotal duration: {duration}")
            logger.info(f"Total succeeded: {total_succeeded}")
            logger.info(f"Total failed: {total_failed}")

        except Exception as e:
            logger.error(f"Worker failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            await self.cleanup()
            logger.info("Worker completed")
            logger.info("=" * 60)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Daily Dimensions Worker')
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no computation)'
    )
    parser.add_argument(
        '--run-now',
        action='store_true',
        help='Run immediately (bypass schedule check)'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Compute for specific date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--dimension',
        type=str,
        action='append',
        dest='dimensions',
        help='Compute specific dimension(s) only. Can be used multiple times.'
    )
    parser.add_argument(
        '--skip-composite',
        action='store_true',
        help='Skip composite score calculation'
    )

    args = parser.parse_args()

    # Parse date if provided
    score_date = None
    if args.date:
        score_date = date.fromisoformat(args.date)

    # Schedule check (only for automated runs)
    if not args.run_now and not args.dry_run and not args.date:
        current_hour = datetime.now().hour
        if not (4 <= current_hour <= 5):
            logger.info(f"Outside scheduled window (4-5 AM). Current hour: {current_hour}")
            logger.info("Use --run-now to force execution")
            return

    worker = DailyDimensionsWorker(
        dry_run=args.dry_run,
        force_date=score_date,
        dimensions=args.dimensions,
        skip_composite=args.skip_composite,
    )

    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
