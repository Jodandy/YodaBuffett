#!/usr/bin/env python3
"""
Data Population Script for YodaBuffett Screener

Ensures all required data is available for comprehensive screening:
- Company metadata validation
- Historical fundamentals completeness
- Price data availability
- Metric calculation validation
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import DatabaseManager
from app.services.metric_calculator import MetricCalculator
from app.services.metrics_service import MetricsService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataPopulationService:
    """Service for populating and validating screening data"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.metrics_service = None
        
    async def init(self):
        """Initialize database connections and services"""
        await self.db_manager.init_pool()
        self.metrics_service = MetricsService(self.db_manager)
        logger.info("Data population service initialized")
    
    async def run_full_population(self, days_back: int = 90):
        """Run complete data population and validation"""
        logger.info("🚀 Starting full data population for YodaBuffett Screener")
        
        try:
            # 1. Validate and clean company data
            await self.validate_company_master()
            
            # 2. Check fundamental data completeness
            await self.validate_fundamentals_data()
            
            # 3. Validate price data availability
            await self.validate_price_data()
            
            # 4. Pre-calculate metrics for recent dates
            await self.pre_calculate_metrics(days_back)
            
            # 5. Generate data quality report
            await self.generate_data_quality_report()
            
            logger.info("✅ Full data population completed successfully")
            
        except Exception as e:
            logger.error(f"❌ Data population failed: {e}")
            raise
        
        finally:
            await self.db_manager.close_pool()
    
    async def validate_company_master(self):
        """Validate and clean company master data"""
        logger.info("📊 Validating company master data...")
        
        # Check for companies with missing essential data
        query = """
        SELECT 
            primary_ticker as symbol,
            company_name,
            sector,
            industry,
            market_cap_usd,
            listing_status
        FROM company_master
        WHERE listing_status = 'active'
        AND primary_ticker IS NOT NULL
        ORDER BY market_cap_usd DESC NULLS LAST
        """
        
        companies = await self.db_manager.execute_query(query)
        logger.info(f"Found {len(companies)} active companies")
        
        # Identify companies with missing data
        companies_missing_data = []
        companies_with_data = []
        
        for company in companies:
            if not company['company_name'] or not company['sector']:
                companies_missing_data.append(company)
            else:
                companies_with_data.append(company)
        
        logger.info(f"✅ {len(companies_with_data)} companies have complete metadata")
        if companies_missing_data:
            logger.warning(f"⚠️ {len(companies_missing_data)} companies missing sector/name data")
            
            # Log first 10 companies with missing data
            for company in companies_missing_data[:10]:
                logger.warning(f"  - {company['symbol']}: missing {', '.join([k for k, v in company.items() if not v and k in ['company_name', 'sector', 'industry']])}")
    
    async def validate_fundamentals_data(self):
        """Check fundamental data completeness"""
        logger.info("📈 Validating fundamentals data...")
        
        # Check data coverage
        coverage_query = """
        SELECT 
            COUNT(DISTINCT symbol) as total_symbols,
            MIN(date) as oldest_data,
            MAX(date) as newest_data,
            COUNT(*) as total_records
        FROM historical_fundamentals_daily
        """
        
        coverage = await self.db_manager.execute_query_one(coverage_query)
        if coverage:
            logger.info(f"📊 Fundamentals coverage: {coverage['total_symbols']} symbols, {coverage['total_records']:,} records")
            logger.info(f"📅 Date range: {coverage['oldest_data']} to {coverage['newest_data']}")
        
        # Check for symbols with recent data
        recent_query = """
        SELECT 
            COUNT(DISTINCT symbol) as symbols_with_recent_data
        FROM historical_fundamentals_daily
        WHERE date >= NOW() - INTERVAL '30 days'
        """
        
        recent = await self.db_manager.execute_query_one(recent_query)
        if recent:
            logger.info(f"✅ {recent['symbols_with_recent_data']} symbols have fundamentals data within last 30 days")
    
    async def validate_price_data(self):
        """Check price data availability"""
        logger.info("💰 Validating price data...")
        
        # Check price data coverage
        price_coverage_query = """
        SELECT 
            COUNT(DISTINCT symbol) as total_symbols,
            MIN(date) as oldest_data,
            MAX(date) as newest_data,
            COUNT(*) as total_records
        FROM daily_price_data
        """
        
        price_coverage = await self.db_manager.execute_query_one(price_coverage_query)
        if price_coverage:
            logger.info(f"📊 Price coverage: {price_coverage['total_symbols']} symbols, {price_coverage['total_records']:,} records")
            logger.info(f"📅 Date range: {price_coverage['oldest_data']} to {price_coverage['newest_data']}")
        
        # Check for symbols with recent price data
        recent_prices_query = """
        SELECT 
            COUNT(DISTINCT symbol) as symbols_with_recent_prices
        FROM daily_price_data
        WHERE date >= NOW() - INTERVAL '7 days'
        """
        
        recent_prices = await self.db_manager.execute_query_one(recent_prices_query)
        if recent_prices:
            logger.info(f"✅ {recent_prices['symbols_with_recent_prices']} symbols have price data within last 7 days")
    
    async def pre_calculate_metrics(self, days_back: int):
        """Pre-calculate metrics for recent dates to speed up screening"""
        logger.info(f"🧮 Pre-calculating metrics for last {days_back} days...")
        
        # Get list of active companies
        companies_query = """
        SELECT primary_ticker as symbol
        FROM company_master 
        WHERE listing_status = 'active'
        AND primary_ticker IS NOT NULL
        ORDER BY market_cap_usd DESC NULLS LAST
        LIMIT 100
        """  # Limit to top 100 for initial population
        
        companies = await self.db_manager.execute_query(companies_query)
        symbols = [c['symbol'] for c in companies]
        
        if not symbols:
            logger.warning("No companies found for metric calculation")
            return
        
        # Get available metrics
        available_metrics = await self.metrics_service.get_available_metrics()
        metric_ids = [m.id for m in available_metrics[:20]]  # Limit to first 20 metrics
        
        logger.info(f"Calculating {len(metric_ids)} metrics for {len(symbols)} symbols")
        
        # Calculate metrics for recent dates
        calculation_dates = []
        current_date = date.today()
        
        # Weekly intervals over the past period
        for weeks_back in range(0, days_back // 7):
            calc_date = current_date - timedelta(weeks=weeks_back)
            calculation_dates.append(calc_date)
        
        successful_calculations = 0
        total_calculations = len(symbols) * len(calculation_dates)
        
        for calc_date in calculation_dates:
            try:
                results = await self.metrics_service.calculate_metrics_for_symbols(
                    symbols, metric_ids, calc_date
                )
                
                # Count successful calculations
                for symbol, metrics in results.items():
                    if any(v is not None for v in metrics.values()):
                        successful_calculations += 1
                
                logger.info(f"✅ Calculated metrics for {len(results)} symbols on {calc_date}")
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to calculate metrics for {calc_date}: {e}")
        
        success_rate = (successful_calculations / total_calculations * 100) if total_calculations > 0 else 0
        logger.info(f"📊 Metric calculation summary: {successful_calculations}/{total_calculations} ({success_rate:.1f}% success rate)")
    
    async def generate_data_quality_report(self):
        """Generate comprehensive data quality report"""
        logger.info("📋 Generating data quality report...")
        
        report = {}
        
        # Company data quality
        company_quality = await self.db_manager.execute_query_one("""
        SELECT 
            COUNT(*) as total_companies,
            COUNT(CASE WHEN sector IS NOT NULL THEN 1 END) as companies_with_sector,
            COUNT(CASE WHEN market_cap_usd IS NOT NULL THEN 1 END) as companies_with_market_cap,
            COUNT(CASE WHEN listing_status = 'active' THEN 1 END) as active_companies
        FROM company_master
        WHERE primary_ticker IS NOT NULL
        """)
        
        # Fundamental data quality
        fundamental_quality = await self.db_manager.execute_query_one("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols_with_fundamentals,
            MAX(date) as latest_fundamental_date,
            COUNT(*) as total_fundamental_records
        FROM historical_fundamentals_daily
        """)
        
        # Price data quality
        price_quality = await self.db_manager.execute_query_one("""
        SELECT 
            COUNT(DISTINCT symbol) as symbols_with_prices,
            MAX(date) as latest_price_date,
            COUNT(*) as total_price_records
        FROM daily_price_data
        """)
        
        # Data availability by symbol
        data_availability = await self.db_manager.execute_query("""
        SELECT 
            cm.primary_ticker as symbol,
            cm.company_name,
            cm.sector,
            CASE WHEN hfd.symbol IS NOT NULL THEN 'Yes' ELSE 'No' END as has_fundamentals,
            CASE WHEN dpd.symbol IS NOT NULL THEN 'Yes' ELSE 'No' END as has_prices,
            hfd.latest_fundamental,
            dpd.latest_price
        FROM company_master cm
        LEFT JOIN (
            SELECT symbol, MAX(date) as latest_fundamental
            FROM historical_fundamentals_daily
            GROUP BY symbol
        ) hfd ON cm.primary_ticker = hfd.symbol
        LEFT JOIN (
            SELECT symbol, MAX(date) as latest_price
            FROM daily_price_data  
            GROUP BY symbol
        ) dpd ON cm.primary_ticker = dpd.symbol
        WHERE cm.listing_status = 'active'
        AND cm.primary_ticker IS NOT NULL
        ORDER BY cm.market_cap_usd DESC NULLS LAST
        LIMIT 50
        """)
        
        # Print report
        logger.info("=" * 60)
        logger.info("📊 DATA QUALITY REPORT")
        logger.info("=" * 60)
        
        if company_quality:
            logger.info(f"🏢 COMPANIES:")
            logger.info(f"  Total companies: {company_quality['total_companies']:,}")
            logger.info(f"  Active companies: {company_quality['active_companies']:,}")
            logger.info(f"  With sector info: {company_quality['companies_with_sector']:,}")
            logger.info(f"  With market cap: {company_quality['companies_with_market_cap']:,}")
        
        if fundamental_quality:
            logger.info(f"📈 FUNDAMENTALS:")
            logger.info(f"  Symbols with data: {fundamental_quality['symbols_with_fundamentals']:,}")
            logger.info(f"  Total records: {fundamental_quality['total_fundamental_records']:,}")
            logger.info(f"  Latest data: {fundamental_quality['latest_fundamental_date']}")
        
        if price_quality:
            logger.info(f"💰 PRICES:")
            logger.info(f"  Symbols with data: {price_quality['symbols_with_prices']:,}")
            logger.info(f"  Total records: {price_quality['total_price_records']:,}")
            logger.info(f"  Latest data: {price_quality['latest_price_date']}")
        
        logger.info(f"🎯 TOP 20 COMPANIES DATA AVAILABILITY:")
        for i, row in enumerate(data_availability[:20], 1):
            logger.info(
                f"  {i:2d}. {row['symbol']:8s} | "
                f"Fund: {row['has_fundamentals']:3s} | "
                f"Price: {row['has_prices']:3s} | "
                f"{row['company_name'][:30]:<30s} | "
                f"{row['sector'] or 'Unknown':<15s}"
            )
        
        logger.info("=" * 60)


async def main():
    """Main entry point for data population"""
    service = DataPopulationService()
    
    try:
        await service.init()
        await service.run_full_population(days_back=90)
        
    except Exception as e:
        logger.error(f"Data population failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)