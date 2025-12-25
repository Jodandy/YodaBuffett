#!/usr/bin/env python3
"""
Validate Yahoo Finance Availability for Company Master
 
This script tests Yahoo Finance availability for all companies with Yahoo symbols
and updates the yahoo_finance_available field accordingly. This is the missing
piece that's preventing fundamentals collection for 571 companies.
"""

import asyncio
import asyncpg
import yfinance as yf
from datetime import datetime, timedelta
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YahooFinanceValidator:
    """Validates Yahoo Finance availability for company symbols."""
    
    def __init__(self):
        self.db_conn = None
        self.successful_validations = 0
        self.failed_validations = 0
        self.batch_size = 50  # Process in batches to avoid overwhelming Yahoo
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_companies_to_validate(self, limit: int = None) -> list:
        """Get companies that need Yahoo Finance validation."""
        query = """
        SELECT primary_ticker, yahoo_symbol, company_name
        FROM company_master 
        WHERE yahoo_symbol IS NOT NULL 
        AND (yahoo_finance_available IS NULL OR yahoo_finance_available = false)
        ORDER BY document_count DESC NULLS LAST, data_quality_score DESC NULLS LAST
        """
        
        if limit:
            query += f" LIMIT {limit}"
            
        rows = await self.db_conn.fetch(query)
        return [dict(row) for row in rows]
        
    def test_yahoo_symbol(self, yahoo_symbol: str) -> dict:
        """Test if a Yahoo symbol works."""
        try:
            logger.debug(f"Testing {yahoo_symbol}...")
            ticker = yf.Ticker(yahoo_symbol)
            
            # Test 1: Try to get recent price data
            hist = ticker.history(period='5d')
            
            has_price_data = not hist.empty
            
            # Test 2: Try to get company info
            try:
                info = ticker.info
                has_info = bool(info and 'symbol' in info)
                company_name = info.get('longName', '') if info else ''
            except Exception:
                has_info = False
                company_name = ''
                
            # Test 3: Try to get basic fundamentals
            has_fundamentals = False
            if has_info:
                try:
                    # Check if we can get basic fundamental data
                    pe_ratio = info.get('trailingPE')
                    market_cap = info.get('marketCap')
                    has_fundamentals = pe_ratio is not None or market_cap is not None
                except Exception:
                    pass
                    
            # Overall availability score
            availability_score = (
                (1.0 if has_price_data else 0.0) +
                (0.5 if has_info else 0.0) +
                (0.5 if has_fundamentals else 0.0)
            ) / 2.0
            
            is_available = availability_score >= 0.5  # Must have price data + something else
            
            return {
                'is_available': is_available,
                'has_price_data': has_price_data,
                'has_info': has_info,
                'has_fundamentals': has_fundamentals,
                'availability_score': availability_score,
                'yahoo_company_name': company_name,
                'price_records': len(hist) if has_price_data else 0,
                'latest_price_date': hist.index[-1].date() if has_price_data else None
            }
            
        except Exception as e:
            logger.debug(f"Error testing {yahoo_symbol}: {e}")
            return {
                'is_available': False,
                'has_price_data': False,
                'has_info': False,
                'has_fundamentals': False,
                'availability_score': 0.0,
                'yahoo_company_name': '',
                'price_records': 0,
                'latest_price_date': None,
                'error': str(e)
            }
            
    async def update_company_availability(self, primary_ticker: str, validation_result: dict):
        """Update company master with validation results."""
        query = """
        UPDATE company_master SET
            yahoo_finance_available = $1,
            yahoo_availability_score = $2,
            yahoo_company_name = $3,
            yahoo_price_records = $4,
            yahoo_latest_price_date = $5,
            yahoo_last_validated = NOW(),
            updated_at = NOW()
        WHERE primary_ticker = $6
        """
        
        await self.db_conn.execute(
            query,
            validation_result['is_available'],
            validation_result['availability_score'],
            validation_result['yahoo_company_name'],
            validation_result['price_records'],
            validation_result['latest_price_date'],
            primary_ticker
        )
        
    async def validate_batch(self, companies: list):
        """Validate a batch of companies."""
        logger.info(f"Validating batch of {len(companies)} companies...")
        
        for i, company in enumerate(companies):
            primary_ticker = company['primary_ticker']
            yahoo_symbol = company['yahoo_symbol']
            company_name = company['company_name']
            
            logger.info(f"[{i+1}/{len(companies)}] Testing {primary_ticker} ({yahoo_symbol})...")
            
            # Test Yahoo Finance availability
            validation_result = self.test_yahoo_symbol(yahoo_symbol)
            
            # Update database
            await self.update_company_availability(primary_ticker, validation_result)
            
            # Track results
            if validation_result['is_available']:
                self.successful_validations += 1
                status = "✅ AVAILABLE"
                details = f"Score: {validation_result['availability_score']:.2f}"
                if validation_result['price_records'] > 0:
                    details += f", {validation_result['price_records']} price records"
            else:
                self.failed_validations += 1
                status = "❌ NOT AVAILABLE"
                details = validation_result.get('error', 'No data available')
                
            logger.info(f"   {status} - {details}")
            
            # Be respectful to Yahoo Finance - 1 second delay between requests
            time.sleep(1)
            
    async def add_validation_columns(self):
        """Add validation columns to company_master if they don't exist."""
        columns_to_add = [
            ("yahoo_availability_score", "FLOAT"),
            ("yahoo_company_name", "TEXT"),
            ("yahoo_price_records", "INTEGER"),
            ("yahoo_latest_price_date", "DATE"),
            ("yahoo_last_validated", "TIMESTAMP")
        ]
        
        for column_name, column_type in columns_to_add:
            try:
                await self.db_conn.execute(f"""
                    ALTER TABLE company_master 
                    ADD COLUMN IF NOT EXISTS {column_name} {column_type}
                """)
                logger.debug(f"Added column {column_name}")
            except Exception as e:
                logger.debug(f"Column {column_name} already exists or error: {e}")
                
    async def validate_all_companies(self, limit: int = None, batch_size: int = None):
        """Validate Yahoo Finance availability for all companies."""
        
        # Add validation columns if needed
        await self.add_validation_columns()
        
        # Get companies to validate
        companies = await self.get_companies_to_validate(limit)
        
        if not companies:
            logger.info("No companies need validation")
            return
            
        logger.info(f"Found {len(companies)} companies to validate")
        
        # Use specified batch size or default
        actual_batch_size = batch_size or self.batch_size
        
        # Process in batches
        for i in range(0, len(companies), actual_batch_size):
            batch = companies[i:i + actual_batch_size]
            batch_num = (i // actual_batch_size) + 1
            total_batches = (len(companies) + actual_batch_size - 1) // actual_batch_size
            
            logger.info(f"\n🔄 Processing batch {batch_num}/{total_batches}")
            
            await self.validate_batch(batch)
            
            # Longer pause between batches
            if i + actual_batch_size < len(companies):
                logger.info("Pausing 10 seconds between batches...")
                time.sleep(10)
                
    async def show_validation_summary(self):
        """Show summary of validation results."""
        
        # Overall stats
        stats = await self.db_conn.fetchrow("""
            SELECT 
                COUNT(*) as total_companies,
                SUM(CASE WHEN yahoo_finance_available = true THEN 1 ELSE 0 END) as available_count,
                SUM(CASE WHEN yahoo_finance_available = false THEN 1 ELSE 0 END) as not_available_count,
                SUM(CASE WHEN yahoo_finance_available IS NULL THEN 1 ELSE 0 END) as not_tested_count,
                AVG(yahoo_availability_score) FILTER (WHERE yahoo_availability_score IS NOT NULL) as avg_score
            FROM company_master
            WHERE yahoo_symbol IS NOT NULL
        """)
        
        logger.info(f"\n📊 YAHOO FINANCE VALIDATION SUMMARY")
        logger.info(f"=" * 50)
        logger.info(f"Total companies with Yahoo symbols: {stats['total_companies']}")
        logger.info(f"Available: {stats['available_count']} ({stats['available_count']/stats['total_companies']*100:.1f}%)")
        logger.info(f"Not available: {stats['not_available_count']} ({stats['not_available_count']/stats['total_companies']*100:.1f}%)")
        logger.info(f"Not tested: {stats['not_tested_count']} ({stats['not_tested_count']/stats['total_companies']*100:.1f}%)")
        logger.info(f"Average availability score: {stats['avg_score']:.2f}" if stats['avg_score'] else "N/A")
        
        # Show some examples of available companies
        available_examples = await self.db_conn.fetch("""
            SELECT primary_ticker, yahoo_symbol, company_name, yahoo_availability_score, yahoo_price_records
            FROM company_master
            WHERE yahoo_finance_available = true
            ORDER BY yahoo_availability_score DESC, yahoo_price_records DESC
            LIMIT 10
        """)
        
        if available_examples:
            logger.info(f"\n✅ Top 10 Available Companies:")
            logger.info(f"{'Ticker':<10} {'Yahoo Symbol':<15} {'Score':<6} {'Records':<8} {'Company Name'}")
            logger.info("-" * 80)
            for company in available_examples:
                logger.info(f"{company['primary_ticker']:<10} {company['yahoo_symbol']:<15} "
                          f"{company['yahoo_availability_score']:<6.2f} {company['yahoo_price_records']:<8} "
                          f"{company['company_name'][:40]}")
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Main entry point."""
    
    validator = YahooFinanceValidator()
    
    try:
        await validator.setup()
        
        logger.info("🚀 YAHOO FINANCE AVAILABILITY VALIDATOR")
        logger.info("=" * 60)
        logger.info("This will validate Yahoo Finance availability for all companies")
        logger.info("and update the yahoo_finance_available field to enable fundamentals collection.")
        
        # Validate all companies (or specify a limit for testing)
        # Use limit=50 for testing, remove limit for full validation
        await validator.validate_all_companies(limit=100, batch_size=10)  # Test with 100 companies first
        
        # Show summary
        await validator.show_validation_summary()
        
        logger.info(f"\n✅ Validation complete!")
        logger.info(f"   Successful validations: {validator.successful_validations}")
        logger.info(f"   Failed validations: {validator.failed_validations}")
        
        logger.info(f"\n💡 Next steps:")
        logger.info(f"   1. Run the daily fundamentals worker to collect data for validated companies")
        logger.info(f"   2. Run historical fundamentals backfill for newly validated companies")
        logger.info(f"   3. Monitor fundamentals collection progress")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await validator.cleanup()

if __name__ == "__main__":
    asyncio.run(main())