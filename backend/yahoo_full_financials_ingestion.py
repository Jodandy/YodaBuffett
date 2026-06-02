#!/usr/bin/env python3
"""
Yahoo Finance Full Financials Ingestion

Stores ALL data from Yahoo Finance exactly as provided - no cherry-picking fields.
Uses JSONB for flexibility so we capture everything Yahoo provides.

Tables created:
- yahoo_financials: All income statement, balance sheet, cash flow data
- yahoo_company_info: Company info/metrics (beta, PEG, analyst data, etc.)
"""

import yfinance as yf
import asyncio
import asyncpg
import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


async def create_tables(conn):
    """Create tables for storing full Yahoo data."""

    # Table 1: Full financial statements (income, balance sheet, cash flow)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS yahoo_financials (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            period_date DATE NOT NULL,
            statement_type VARCHAR(20) NOT NULL,  -- 'annual' or 'quarterly'
            fiscal_year INT,
            fiscal_quarter INT,

            -- Store complete data as JSONB
            income_statement JSONB,      -- All income statement fields
            balance_sheet JSONB,         -- All balance sheet fields
            cash_flow JSONB,             -- All cash flow fields

            -- Key fields extracted for easy querying
            total_revenue BIGINT,
            net_income BIGINT,
            total_assets BIGINT,
            total_equity BIGINT,
            total_debt BIGINT,
            operating_cash_flow BIGINT,
            free_cash_flow BIGINT,

            -- Intangibles (commonly needed)
            goodwill BIGINT,
            other_intangible_assets BIGINT,
            net_ppe BIGINT,

            -- Report date from earnings calendar
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

    # Table 2: Company info/metrics (updated periodically, not per-statement)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS yahoo_company_info (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            fetched_date DATE NOT NULL,

            -- Store complete info dict as JSONB
            raw_info JSONB,

            -- Key fields extracted for easy querying
            market_cap BIGINT,
            enterprise_value BIGINT,
            beta FLOAT,
            trailing_pe FLOAT,
            forward_pe FLOAT,
            peg_ratio FLOAT,
            price_to_book FLOAT,

            -- Analyst data
            target_mean_price FLOAT,
            target_high_price FLOAT,
            target_low_price FLOAT,
            recommendation_mean FLOAT,
            recommendation_key VARCHAR(20),
            number_of_analysts INT,

            -- Ownership
            held_percent_insiders FLOAT,
            held_percent_institutions FLOAT,

            -- Short interest
            shares_short BIGINT,
            short_ratio FLOAT,

            -- Dividends
            dividend_yield FLOAT,
            dividend_rate FLOAT,
            ex_dividend_date DATE,

            -- Margins (from Yahoo, for comparison)
            profit_margin FLOAT,
            operating_margin FLOAT,
            gross_margin FLOAT,

            -- Returns
            return_on_equity FLOAT,
            return_on_assets FLOAT,

            currency VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            UNIQUE(symbol, fetched_date)
        );

        CREATE INDEX IF NOT EXISTS idx_yahoo_info_symbol_date
        ON yahoo_company_info(symbol, fetched_date DESC);
    """)

    logger.info("✅ Tables created: yahoo_financials, yahoo_company_info")


def safe_float(val) -> Optional[float]:
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


def safe_int(val) -> Optional[int]:
    """Safely convert to int."""
    f = safe_float(val)
    return int(f) if f is not None else None


def dataframe_to_dict(df, period) -> Optional[Dict]:
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
                # Clean the key name
                clean_key = str(key).replace(' ', '_').lower()
                result[clean_key] = clean_val

        return result if result else None
    except Exception as e:
        logger.warning(f"Error converting DataFrame: {e}")
        return None


def get_earnings_date_for_period(earnings_dates, period_date) -> Optional[date]:
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

            # Earnings typically announced 15-90 days after period end
            if 15 <= days_after <= 120:
                return earn_date
        except:
            continue

    return None


async def fetch_and_store_company(conn, symbol: str, yahoo_symbol: str):
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

        # Get earnings dates for publish_date mapping
        earnings_dates = ticker.earnings_dates

        # Get company info
        info = ticker.info

        # Process annual statements
        periods_processed = 0

        if income_annual is not None and not income_annual.empty:
            for period in income_annual.columns:
                period_date = period.date() if hasattr(period, 'date') else period

                # Get data for this period from all statements
                inc_data = dataframe_to_dict(income_annual, period)
                bal_data = dataframe_to_dict(balance_annual, period) if balance_annual is not None else None
                cf_data = dataframe_to_dict(cashflow_annual, period) if cashflow_annual is not None else None

                # Get publish date
                publish_date = get_earnings_date_for_period(earnings_dates, period_date)

                # Extract key fields for easy querying
                await conn.execute("""
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
                    symbol,
                    period_date,
                    'annual',
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

                # Determine fiscal quarter
                month = period_date.month if hasattr(period_date, 'month') else 12
                fiscal_quarter = (month - 1) // 3 + 1

                await conn.execute("""
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
                    symbol,
                    period_date,
                    'quarterly',
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

        # Store company info
        if info and info.get('symbol'):
            ex_div_date = None
            if info.get('exDividendDate'):
                try:
                    ex_div_date = datetime.fromtimestamp(info['exDividendDate']).date()
                except:
                    pass

            await conn.execute("""
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
                symbol,
                date.today(),
                json.dumps(info),
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

        logger.info(f"✅ {symbol}: {periods_processed} periods stored")
        return periods_processed

    except Exception as e:
        logger.error(f"❌ {symbol}: {e}")
        return 0


async def get_companies_to_fetch(conn, limit: Optional[int] = None, only_missing: bool = False) -> List[tuple]:
    """Get list of companies with Yahoo symbols."""
    if only_missing:
        query = """
            SELECT cm.primary_ticker, cm.yahoo_symbol, cm.company_name
            FROM company_master cm
            WHERE cm.yahoo_symbol IS NOT NULL
              AND cm.listing_status = 'active'
              AND cm.primary_ticker NOT IN (SELECT DISTINCT symbol FROM yahoo_financials)
            ORDER BY cm.primary_ticker
        """
    else:
        query = """
            SELECT primary_ticker, yahoo_symbol, company_name
            FROM company_master
            WHERE yahoo_symbol IS NOT NULL
              AND listing_status = 'active'
            ORDER BY primary_ticker
        """
    if limit:
        query += f" LIMIT {limit}"

    rows = await conn.fetch(query)
    return [(r['primary_ticker'], r['yahoo_symbol'], r['company_name']) for r in rows]


async def main(limit: Optional[int] = None, symbols: Optional[List[str]] = None, only_missing: bool = False):
    """Main ingestion function."""

    conn = await asyncpg.connect(DATABASE_URL)

    try:
        # Create tables
        await create_tables(conn)

        if symbols:
            # Fetch specific symbols
            companies = []
            for sym in symbols:
                row = await conn.fetchrow("""
                    SELECT primary_ticker, yahoo_symbol, company_name
                    FROM company_master
                    WHERE primary_ticker = $1 OR yahoo_symbol ILIKE $2
                """, sym, f"%{sym}%")
                if row:
                    companies.append((row['primary_ticker'], row['yahoo_symbol'], row['company_name']))
        else:
            # Fetch all companies (or only missing ones)
            companies = await get_companies_to_fetch(conn, limit, only_missing)

        logger.info(f"📊 Fetching full Yahoo data for {len(companies)} companies")

        total_periods = 0
        successful = 0
        failed = 0

        for i, (ticker, yahoo_symbol, name) in enumerate(companies, 1):
            logger.info(f"[{i}/{len(companies)}] {ticker} ({name})...")

            periods = await fetch_and_store_company(conn, ticker, yahoo_symbol)

            if periods > 0:
                total_periods += periods
                successful += 1
            else:
                failed += 1

            # Rate limiting
            await asyncio.sleep(0.5)

        logger.info(f"""
========================================
✅ Ingestion Complete
========================================
Companies processed: {successful + failed}
Successful: {successful}
Failed: {failed}
Total periods stored: {total_periods}
========================================
        """)

        # Show sample data
        sample = await conn.fetch("""
            SELECT symbol, period_date, statement_type,
                   total_revenue, goodwill, other_intangible_assets, publish_date
            FROM yahoo_financials
            ORDER BY created_at DESC
            LIMIT 5
        """)

        if sample:
            logger.info("Sample data stored:")
            for r in sample:
                rev = f"{r['total_revenue']:,}" if r['total_revenue'] else "N/A"
                logger.info(f"  {r['symbol']} {r['period_date']} ({r['statement_type']}): "
                          f"Rev={rev}, Publish={r['publish_date']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Fetch full Yahoo Finance data')
    parser.add_argument('--limit', type=int, help='Limit number of companies')
    parser.add_argument('--symbols', nargs='+', help='Specific symbols to fetch')
    parser.add_argument('--test', action='store_true', help='Test with 5 companies')
    parser.add_argument('--only-missing', action='store_true', help='Only fetch companies not yet in yahoo_financials')

    args = parser.parse_args()

    if args.test:
        asyncio.run(main(limit=5))
    elif args.symbols:
        asyncio.run(main(symbols=args.symbols))
    else:
        asyncio.run(main(limit=args.limit, only_missing=args.only_missing))
