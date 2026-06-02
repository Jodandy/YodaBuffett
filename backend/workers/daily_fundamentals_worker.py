#!/usr/bin/env python3
"""
Daily Fundamentals Worker

Collects ALL fundamental data from Yahoo Finance daily using the full JSONB storage approach.
Stores complete financial statements + company info - nothing is lost.

Tables populated:
- yahoo_financials: Full income statement, balance sheet, cash flow as JSONB
- yahoo_company_info: Full company info/metrics as JSONB
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import logging
from datetime import date, datetime, timedelta
import argparse
from typing import List, Tuple
import json

import yfinance as yf
import asyncpg

# Setup logging
LOG_DIR = '/Users/jdandemar/Documents/YodaBuffett/logs'
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'{LOG_DIR}/daily-fundamentals-worker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def safe_float(val) -> float | None:
    """Safely convert to float, handling NaN and None."""
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return f
    except (ValueError, TypeError):
        return None


def safe_int(val) -> int | None:
    """Safely convert to int."""
    f = safe_float(val)
    return int(f) if f is not None else None


def dataframe_to_dict(df, period) -> dict | None:
    """Convert a DataFrame column to a clean dict."""
    if df is None or df.empty:
        return None
    try:
        if period not in df.columns:
            return None
        series = df[period]
        result = {}
        for key, val in series.items():
            clean_val = safe_float(val)
            if clean_val is not None:
                clean_key = str(key).replace(' ', '_').lower()
                result[clean_key] = clean_val
        return result if result else None
    except Exception as e:
        logger.warning(f"Error converting DataFrame: {e}")
        return None


def get_earnings_date_for_period(earnings_dates, period_date) -> date | None:
    """Match a period_date to its earnings announcement date.

    Only returns dates in the past - scheduled future earnings are ignored
    to prevent look-ahead bias.
    """
    if earnings_dates is None or earnings_dates.empty:
        return None
    period_dt = period_date if isinstance(period_date, date) else period_date.date()
    today = date.today()

    for earn_dt in earnings_dates.index:
        try:
            earn_date = earn_dt.date() if hasattr(earn_dt, 'date') else earn_dt
            # Skip future dates - these are scheduled, not actual publish dates
            if earn_date > today:
                continue
            days_after = (earn_date - period_dt).days
            if 15 <= days_after <= 120:
                return earn_date
        except:
            continue
    return None


class DailyFundamentalsWorker:
    """Worker for daily fundamental data collection using full JSONB storage."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.db_conn = None

    async def setup(self):
        """Initialize database connection and create tables."""
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        await self.create_tables()

    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()

    async def create_tables(self):
        """Create tables for storing full Yahoo data."""
        # Table 1: Full financial statements
        await self.db_conn.execute("""
            CREATE TABLE IF NOT EXISTS yahoo_financials (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                period_date DATE NOT NULL,
                statement_type VARCHAR(20) NOT NULL,
                fiscal_year INT,
                fiscal_quarter INT,
                income_statement JSONB,
                balance_sheet JSONB,
                cash_flow JSONB,
                total_revenue BIGINT,
                net_income BIGINT,
                total_assets BIGINT,
                total_equity BIGINT,
                total_debt BIGINT,
                operating_cash_flow BIGINT,
                free_cash_flow BIGINT,
                goodwill BIGINT,
                other_intangible_assets BIGINT,
                net_ppe BIGINT,
                publish_date DATE,
                currency VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, period_date, statement_type)
            );
            CREATE INDEX IF NOT EXISTS idx_yahoo_fin_symbol_date
            ON yahoo_financials(symbol, period_date DESC);
            CREATE INDEX IF NOT EXISTS idx_yahoo_fin_publish_date
            ON yahoo_financials(publish_date) WHERE publish_date IS NOT NULL;
        """)

        # Table 2: Company info/metrics
        await self.db_conn.execute("""
            CREATE TABLE IF NOT EXISTS yahoo_company_info (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                fetched_date DATE NOT NULL,
                raw_info JSONB,
                market_cap BIGINT,
                enterprise_value BIGINT,
                beta FLOAT,
                trailing_pe FLOAT,
                forward_pe FLOAT,
                peg_ratio FLOAT,
                price_to_book FLOAT,
                target_mean_price FLOAT,
                target_high_price FLOAT,
                target_low_price FLOAT,
                recommendation_mean FLOAT,
                recommendation_key VARCHAR(20),
                number_of_analysts INT,
                held_percent_insiders FLOAT,
                held_percent_institutions FLOAT,
                shares_short BIGINT,
                short_ratio FLOAT,
                dividend_yield FLOAT,
                dividend_rate FLOAT,
                ex_dividend_date DATE,
                profit_margin FLOAT,
                operating_margin FLOAT,
                gross_margin FLOAT,
                return_on_equity FLOAT,
                return_on_assets FLOAT,
                currency VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, fetched_date)
            );
            CREATE INDEX IF NOT EXISTS idx_yahoo_info_symbol_date
            ON yahoo_company_info(symbol, fetched_date DESC);
        """)
        logger.info("✅ Tables ready: yahoo_financials, yahoo_company_info")

    async def get_companies(self) -> List[Tuple[str, str, str]]:
        """Get list of companies with Yahoo symbols."""
        rows = await self.db_conn.fetch("""
            SELECT primary_ticker, yahoo_symbol, company_name
            FROM company_master
            WHERE yahoo_symbol IS NOT NULL
              AND listing_status = 'active'
            ORDER BY primary_ticker
        """)
        return [(r['primary_ticker'], r['yahoo_symbol'], r['company_name']) for r in rows]

    async def check_if_already_collected(self, target_date: date) -> bool:
        """Check if we already collected company info for this date."""
        result = await self.db_conn.fetchval("""
            SELECT COUNT(*) FROM yahoo_company_info WHERE fetched_date = $1
        """, target_date)
        return result > 0

    async def fetch_and_store_company(self, symbol: str, yahoo_symbol: str) -> int:
        """Fetch all Yahoo data for a company and store it."""
        try:
            ticker = yf.Ticker(yahoo_symbol)

            # Get all financial statements
            income_annual = ticker.financials
            income_quarterly = ticker.quarterly_financials
            balance_annual = ticker.balance_sheet
            balance_quarterly = ticker.quarterly_balance_sheet
            cashflow_annual = ticker.cashflow
            cashflow_quarterly = ticker.quarterly_cashflow
            earnings_dates = ticker.earnings_dates
            info = ticker.info

            periods_processed = 0

            # Process annual statements
            if income_annual is not None and not income_annual.empty:
                for period in income_annual.columns:
                    period_date = period.date() if hasattr(period, 'date') else period
                    inc_data = dataframe_to_dict(income_annual, period)
                    bal_data = dataframe_to_dict(balance_annual, period) if balance_annual is not None else None
                    cf_data = dataframe_to_dict(cashflow_annual, period) if cashflow_annual is not None else None
                    publish_date = get_earnings_date_for_period(earnings_dates, period_date)

                    await self.db_conn.execute("""
                        INSERT INTO yahoo_financials (
                            symbol, period_date, statement_type, fiscal_year,
                            income_statement, balance_sheet, cash_flow,
                            total_revenue, net_income, total_assets, total_equity, total_debt,
                            operating_cash_flow, free_cash_flow,
                            goodwill, other_intangible_assets, net_ppe,
                            publish_date, currency
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                        ON CONFLICT (symbol, period_date, statement_type)
                        DO UPDATE SET
                            income_statement = EXCLUDED.income_statement,
                            balance_sheet = EXCLUDED.balance_sheet,
                            cash_flow = EXCLUDED.cash_flow,
                            total_revenue = EXCLUDED.total_revenue,
                            net_income = EXCLUDED.net_income,
                            total_assets = EXCLUDED.total_assets,
                            total_equity = EXCLUDED.total_equity,
                            total_debt = EXCLUDED.total_debt,
                            operating_cash_flow = EXCLUDED.operating_cash_flow,
                            free_cash_flow = EXCLUDED.free_cash_flow,
                            goodwill = EXCLUDED.goodwill,
                            other_intangible_assets = EXCLUDED.other_intangible_assets,
                            net_ppe = EXCLUDED.net_ppe,
                            publish_date = EXCLUDED.publish_date,
                            updated_at = CURRENT_TIMESTAMP
                    """,
                        symbol, period_date, 'annual',
                        period_date.year if hasattr(period_date, 'year') else None,
                        json.dumps(inc_data) if inc_data else None,
                        json.dumps(bal_data) if bal_data else None,
                        json.dumps(cf_data) if cf_data else None,
                        safe_int(inc_data.get('total_revenue')) if inc_data else None,
                        safe_int(inc_data.get('net_income')) if inc_data else None,
                        safe_int(bal_data.get('total_assets')) if bal_data else None,
                        safe_int(bal_data.get('stockholders_equity') or bal_data.get('total_equity_gross_minority_interest')) if bal_data else None,
                        safe_int(bal_data.get('total_debt')) if bal_data else None,
                        safe_int(cf_data.get('operating_cash_flow')) if cf_data else None,
                        safe_int(cf_data.get('free_cash_flow')) if cf_data else None,
                        safe_int(bal_data.get('goodwill')) if bal_data else None,
                        safe_int(bal_data.get('other_intangible_assets')) if bal_data else None,
                        safe_int(bal_data.get('net_ppe')) if bal_data else None,
                        publish_date,
                        info.get('currency')
                    )
                    periods_processed += 1

            # Process quarterly statements
            if income_quarterly is not None and not income_quarterly.empty:
                for period in income_quarterly.columns:
                    period_date = period.date() if hasattr(period, 'date') else period
                    inc_data = dataframe_to_dict(income_quarterly, period)
                    bal_data = dataframe_to_dict(balance_quarterly, period) if balance_quarterly is not None else None
                    cf_data = dataframe_to_dict(cashflow_quarterly, period) if cashflow_quarterly is not None else None
                    publish_date = get_earnings_date_for_period(earnings_dates, period_date)
                    month = period_date.month if hasattr(period_date, 'month') else 12
                    fiscal_quarter = (month - 1) // 3 + 1

                    await self.db_conn.execute("""
                        INSERT INTO yahoo_financials (
                            symbol, period_date, statement_type, fiscal_year, fiscal_quarter,
                            income_statement, balance_sheet, cash_flow,
                            total_revenue, net_income, total_assets, total_equity, total_debt,
                            operating_cash_flow, free_cash_flow,
                            goodwill, other_intangible_assets, net_ppe,
                            publish_date, currency
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                        ON CONFLICT (symbol, period_date, statement_type)
                        DO UPDATE SET
                            income_statement = EXCLUDED.income_statement,
                            balance_sheet = EXCLUDED.balance_sheet,
                            cash_flow = EXCLUDED.cash_flow,
                            total_revenue = EXCLUDED.total_revenue,
                            net_income = EXCLUDED.net_income,
                            total_assets = EXCLUDED.total_assets,
                            total_equity = EXCLUDED.total_equity,
                            total_debt = EXCLUDED.total_debt,
                            operating_cash_flow = EXCLUDED.operating_cash_flow,
                            free_cash_flow = EXCLUDED.free_cash_flow,
                            goodwill = EXCLUDED.goodwill,
                            other_intangible_assets = EXCLUDED.other_intangible_assets,
                            net_ppe = EXCLUDED.net_ppe,
                            publish_date = EXCLUDED.publish_date,
                            updated_at = CURRENT_TIMESTAMP
                    """,
                        symbol, period_date, 'quarterly',
                        period_date.year if hasattr(period_date, 'year') else None,
                        fiscal_quarter,
                        json.dumps(inc_data) if inc_data else None,
                        json.dumps(bal_data) if bal_data else None,
                        json.dumps(cf_data) if cf_data else None,
                        safe_int(inc_data.get('total_revenue')) if inc_data else None,
                        safe_int(inc_data.get('net_income')) if inc_data else None,
                        safe_int(bal_data.get('total_assets')) if bal_data else None,
                        safe_int(bal_data.get('stockholders_equity') or bal_data.get('total_equity_gross_minority_interest')) if bal_data else None,
                        safe_int(bal_data.get('total_debt')) if bal_data else None,
                        safe_int(cf_data.get('operating_cash_flow')) if cf_data else None,
                        safe_int(cf_data.get('free_cash_flow')) if cf_data else None,
                        safe_int(bal_data.get('goodwill')) if bal_data else None,
                        safe_int(bal_data.get('other_intangible_assets')) if bal_data else None,
                        safe_int(bal_data.get('net_ppe')) if bal_data else None,
                        publish_date,
                        info.get('currency')
                    )
                    periods_processed += 1

            # Store company info (daily snapshot)
            if info and info.get('symbol'):
                ex_div_date = None
                if info.get('exDividendDate'):
                    try:
                        ex_div_date = datetime.fromtimestamp(info['exDividendDate']).date()
                    except:
                        pass

                await self.db_conn.execute("""
                    INSERT INTO yahoo_company_info (
                        symbol, fetched_date, raw_info,
                        market_cap, enterprise_value, beta,
                        trailing_pe, forward_pe, peg_ratio, price_to_book,
                        target_mean_price, target_high_price, target_low_price,
                        recommendation_mean, recommendation_key, number_of_analysts,
                        held_percent_insiders, held_percent_institutions,
                        shares_short, short_ratio,
                        dividend_yield, dividend_rate, ex_dividend_date,
                        profit_margin, operating_margin, gross_margin,
                        return_on_equity, return_on_assets,
                        currency
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
                    ON CONFLICT (symbol, fetched_date) DO UPDATE SET
                        raw_info = EXCLUDED.raw_info,
                        market_cap = EXCLUDED.market_cap,
                        enterprise_value = EXCLUDED.enterprise_value,
                        beta = EXCLUDED.beta,
                        trailing_pe = EXCLUDED.trailing_pe,
                        forward_pe = EXCLUDED.forward_pe,
                        peg_ratio = EXCLUDED.peg_ratio,
                        price_to_book = EXCLUDED.price_to_book,
                        target_mean_price = EXCLUDED.target_mean_price,
                        target_high_price = EXCLUDED.target_high_price,
                        target_low_price = EXCLUDED.target_low_price,
                        recommendation_mean = EXCLUDED.recommendation_mean,
                        recommendation_key = EXCLUDED.recommendation_key,
                        number_of_analysts = EXCLUDED.number_of_analysts,
                        held_percent_insiders = EXCLUDED.held_percent_insiders,
                        held_percent_institutions = EXCLUDED.held_percent_institutions,
                        shares_short = EXCLUDED.shares_short,
                        short_ratio = EXCLUDED.short_ratio,
                        dividend_yield = EXCLUDED.dividend_yield,
                        dividend_rate = EXCLUDED.dividend_rate,
                        ex_dividend_date = EXCLUDED.ex_dividend_date,
                        profit_margin = EXCLUDED.profit_margin,
                        operating_margin = EXCLUDED.operating_margin,
                        gross_margin = EXCLUDED.gross_margin,
                        return_on_equity = EXCLUDED.return_on_equity,
                        return_on_assets = EXCLUDED.return_on_assets
                """,
                    symbol, date.today(), json.dumps(info),
                    safe_int(info.get('marketCap')),
                    safe_int(info.get('enterpriseValue')),
                    safe_float(info.get('beta')),
                    safe_float(info.get('trailingPE')),
                    safe_float(info.get('forwardPE')),
                    safe_float(info.get('pegRatio')),
                    safe_float(info.get('priceToBook')),
                    safe_float(info.get('targetMeanPrice')),
                    safe_float(info.get('targetHighPrice')),
                    safe_float(info.get('targetLowPrice')),
                    safe_float(info.get('recommendationMean')),
                    info.get('recommendationKey'),
                    safe_int(info.get('numberOfAnalystOpinions')),
                    safe_float(info.get('heldPercentInsiders')),
                    safe_float(info.get('heldPercentInstitutions')),
                    safe_int(info.get('sharesShort')),
                    safe_float(info.get('shortRatio')),
                    safe_float(info.get('dividendYield')),
                    safe_float(info.get('dividendRate')),
                    ex_div_date,
                    safe_float(info.get('profitMargins')),
                    safe_float(info.get('operatingMargins')),
                    safe_float(info.get('grossMargins')),
                    safe_float(info.get('returnOnEquity')),
                    safe_float(info.get('returnOnAssets')),
                    info.get('currency')
                )

            return periods_processed

        except Exception as e:
            logger.error(f"❌ {symbol}: {e}")
            return 0

    async def run(self):
        """Run the daily fundamentals collection."""
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("🚀 Starting Daily Fundamentals Worker (Full JSONB Mode)")
        logger.info(f"   Time: {start_time}")
        logger.info(f"   Mode: {'DRY RUN' if self.dry_run else 'PRODUCTION'}")
        logger.info(f"   Tables: yahoo_financials, yahoo_company_info")

        try:
            await self.setup()

            today = date.today()
            if await self.check_if_already_collected(today):
                logger.info(f"✅ Company info already collected for {today}")
                logger.info("   (Financial statements will still be updated if new data available)")

            companies = await self.get_companies()
            if not companies:
                logger.warning("No active companies found")
                return

            # Process all companies (financial statements are upserted, so safe to re-run)
            # But limit batch size for daily runs to avoid rate limiting
            batch_size = min(200, len(companies))
            day_offset = today.day % len(companies)
            rotated = companies[day_offset:] + companies[:day_offset]
            batch = rotated[:batch_size]

            logger.info(f"📊 Processing {len(batch)} companies (of {len(companies)} total)")

            if self.dry_run:
                logger.info("DRY RUN - Would collect full Yahoo data for:")
                for ticker, yahoo_sym, name in batch[:10]:
                    logger.info(f"  - {ticker} ({name})")
                if len(batch) > 10:
                    logger.info(f"  ... and {len(batch) - 10} more")
            else:
                successful = 0
                failed = 0
                total_periods = 0

                for i, (ticker, yahoo_symbol, name) in enumerate(batch, 1):
                    logger.info(f"[{i}/{len(batch)}] {ticker}...")
                    periods = await self.fetch_and_store_company(ticker, yahoo_symbol)

                    if periods > 0:
                        successful += 1
                        total_periods += periods
                        logger.info(f"  ✅ {periods} periods stored")
                    else:
                        failed += 1

                    await asyncio.sleep(0.5)  # Rate limiting

                # Log summary
                logger.info(f"\n📈 Collection Summary:")
                logger.info(f"   Companies processed: {successful + failed}")
                logger.info(f"   Successful: {successful}")
                logger.info(f"   Failed: {failed}")
                logger.info(f"   Financial periods stored: {total_periods}")

                # Query summary stats
                info_count = await self.db_conn.fetchval(
                    "SELECT COUNT(*) FROM yahoo_company_info WHERE fetched_date = $1", today
                )
                fin_count = await self.db_conn.fetchval(
                    "SELECT COUNT(*) FROM yahoo_financials"
                )

                row = await self.db_conn.fetchrow("""
                    SELECT
                        AVG(trailing_pe) FILTER (WHERE trailing_pe > 0) as avg_pe,
                        AVG(return_on_equity) FILTER (WHERE return_on_equity > 0) as avg_roe,
                        COUNT(*) FILTER (WHERE dividend_yield > 0) as dividend_payers
                    FROM yahoo_company_info WHERE fetched_date = $1
                """, today)

                logger.info(f"\n📊 Database Stats:")
                logger.info(f"   Company info records today: {info_count}")
                logger.info(f"   Total financial periods: {fin_count}")
                if row['avg_pe']:
                    logger.info(f"   Average P/E: {row['avg_pe']:.1f}")
                if row['avg_roe']:
                    logger.info(f"   Average ROE: {row['avg_roe']:.1%}")
                logger.info(f"   Dividend payers: {row['dividend_payers']}")

        except Exception as e:
            logger.error(f"❌ Error in fundamentals worker: {e}")
            import traceback
            traceback.print_exc()

        finally:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"\n⏱️  Duration: {duration}")
            logger.info(f"✅ Daily Fundamentals Worker completed")
            logger.info("=" * 60)
            await self.cleanup()

async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Daily Fundamentals Worker')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run in dry-run mode (no data collection)')
    parser.add_argument('--run-now', action='store_true',
                       help='Run immediately (bypass schedule check)')
    
    args = parser.parse_args()
    
    # Check if we should run (only between 3:30 AM and 4:00 AM unless forced)
    current_hour = datetime.now().hour
    current_minute = datetime.now().minute
    
    if not args.run_now:
        if not (current_hour == 3 and 30 <= current_minute <= 59):
            logger.info(f"Outside scheduled window (3:30-4:00 AM). Current time: {current_hour}:{current_minute:02d}")
            logger.info("Use --run-now to force execution")
            return
            
    worker = DailyFundamentalsWorker(dry_run=args.dry_run)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())