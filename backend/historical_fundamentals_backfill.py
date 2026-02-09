#!/usr/bin/env python3
"""
Historical Fundamentals Backfill System

Backfills historical fundamental data from Yahoo Finance:
1. Quarterly/Annual financial statements (Revenue, Earnings, etc.)
2. Balance sheet data (Assets, Debt, Equity)  
3. Cash flow statements (Operating CF, Free CF, etc.)
4. Calculated daily metrics (P/E, Market Cap, etc.) using price data

This provides historical context for backtesting fundamental strategies.
"""

import yfinance as yf
import pandas as pd
import asyncio
import asyncpg
from datetime import date, timedelta, datetime
from typing import Dict, List, Optional, Tuple
import json
import logging
from collections import defaultdict
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HistoricalFundamentalsBackfill:
    """Backfill historical fundamental data from Yahoo Finance."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection and tables."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Create historical fundamentals tables
        await self.create_historical_tables()
        
    async def create_historical_tables(self):
        """Create tables for historical fundamental data."""
        
        # 1. Quarterly/Annual financial statements
        financial_statements_query = """
        CREATE TABLE IF NOT EXISTS financial_statements (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            period_date DATE NOT NULL,
            statement_type VARCHAR(20) NOT NULL, -- 'quarterly', 'annual'
            fiscal_year INTEGER,
            fiscal_quarter INTEGER,
            
            -- Income Statement
            total_revenue BIGINT,
            gross_profit BIGINT,
            operating_income BIGINT,
            net_income BIGINT,
            ebit BIGINT,
            ebitda BIGINT,
            
            -- Per Share
            basic_eps FLOAT,
            diluted_eps FLOAT,
            
            -- Other income statement items
            research_development BIGINT,
            selling_general_administrative BIGINT,
            interest_expense BIGINT,
            tax_expense BIGINT,
            
            -- Metadata
            currency VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(symbol, period_date, statement_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_financial_statements_symbol_date 
        ON financial_statements(symbol, period_date DESC);
        """
        
        # 2. Balance sheet data  
        balance_sheet_query = """
        CREATE TABLE IF NOT EXISTS balance_sheet_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            period_date DATE NOT NULL,
            statement_type VARCHAR(20) NOT NULL,
            
            -- Assets
            total_assets BIGINT,
            current_assets BIGINT,
            cash_and_equivalents BIGINT,
            accounts_receivable BIGINT,
            inventory BIGINT,
            
            -- Liabilities  
            total_liabilities BIGINT,
            current_liabilities BIGINT,
            total_debt BIGINT,
            long_term_debt BIGINT,
            accounts_payable BIGINT,
            
            -- Equity
            total_equity BIGINT,
            retained_earnings BIGINT,
            
            -- Shares
            shares_outstanding BIGINT,
            
            -- Metadata
            currency VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(symbol, period_date, statement_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_date 
        ON balance_sheet_data(symbol, period_date DESC);
        """
        
        # 3. Cash flow data
        cash_flow_query = """
        CREATE TABLE IF NOT EXISTS cash_flow_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            period_date DATE NOT NULL,
            statement_type VARCHAR(20) NOT NULL,
            
            -- Operating Activities
            operating_cash_flow BIGINT,
            net_income BIGINT,
            depreciation_amortization BIGINT,
            
            -- Investing Activities  
            investing_cash_flow BIGINT,
            capital_expenditure BIGINT,
            
            -- Financing Activities
            financing_cash_flow BIGINT,
            dividends_paid BIGINT,
            
            -- Calculated metrics
            free_cash_flow BIGINT, -- Operating CF - CapEx
            
            -- Metadata
            currency VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(symbol, period_date, statement_type)
        );
        
        CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_date 
        ON cash_flow_data(symbol, period_date DESC);
        """
        
        # 4. Historical calculated metrics (daily P/E, etc.)
        historical_metrics_query = """
        CREATE TABLE IF NOT EXISTS historical_fundamentals_daily (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            date DATE NOT NULL,
            
            -- Market valuation (calculated from price + financial data)
            market_cap BIGINT,
            enterprise_value BIGINT,
            
            -- Ratios (calculated using latest financial data)
            pe_ratio FLOAT, -- Price / Latest EPS
            pb_ratio FLOAT, -- Price / Latest Book Value per Share
            ps_ratio FLOAT, -- Market Cap / Latest Revenue
            ev_ebitda FLOAT, -- EV / Latest EBITDA
            
            -- Per share metrics
            book_value_per_share FLOAT,
            revenue_per_share FLOAT,
            cash_per_share FLOAT,
            
            -- Financial health ratios
            debt_to_equity FLOAT,
            current_ratio FLOAT,
            
            -- Price used for calculations
            close_price FLOAT,
            
            -- Reference to source financial data
            financial_data_date DATE, -- Which financial statement was used
            
            -- Metadata
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(symbol, date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_historical_fundamentals_symbol_date 
        ON historical_fundamentals_daily(symbol, date DESC);
        """
        
        await self.db_conn.execute(financial_statements_query)
        await self.db_conn.execute(balance_sheet_query)
        await self.db_conn.execute(cash_flow_query)
        await self.db_conn.execute(historical_metrics_query)
        
        logger.info("✅ Historical fundamentals tables created")
        
    async def get_yahoo_symbol(self, primary_ticker: str) -> Optional[str]:
        """Get Yahoo symbol from company_master."""
        query = """
        SELECT yahoo_symbol, company_name 
        FROM company_master 
        WHERE primary_ticker = $1
        """
        
        row = await self.db_conn.fetchrow(query, primary_ticker)
        return row['yahoo_symbol'] if row else None
        
    async def backfill_financial_statements(self, symbol: str) -> bool:
        """Backfill quarterly and annual financial statements."""
        
        yahoo_symbol = await self.get_yahoo_symbol(symbol)
        if not yahoo_symbol:
            logger.warning(f"No Yahoo symbol found for {symbol}")
            return False
            
        try:
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get quarterly and annual financials
            statements = [
                ('quarterly', ticker.quarterly_financials),
                ('annual', ticker.financials)
            ]
            
            success_count = 0
            
            for statement_type, data in statements:
                if data.empty:
                    continue
                    
                for period_col in data.columns:
                    period_date = period_col.date()
                    
                    # Extract financial metrics
                    financial_data = {
                        'symbol': symbol,
                        'period_date': period_date,
                        'statement_type': statement_type,
                        'fiscal_year': period_date.year,
                        'fiscal_quarter': ((period_date.month - 1) // 3) + 1 if statement_type == 'quarterly' else None,
                        
                        # Income statement items
                        'total_revenue': self.safe_extract(data, 'Total Revenue', period_col),
                        'gross_profit': self.safe_extract(data, 'Gross Profit', period_col),
                        'operating_income': self.safe_extract(data, 'Operating Income', period_col),
                        'net_income': self.safe_extract(data, 'Net Income', period_col),
                        'ebit': self.safe_extract(data, 'EBIT', period_col),
                        'ebitda': self.safe_extract(data, 'EBITDA', period_col),
                        
                        # Per share metrics  
                        'basic_eps': self.safe_extract(data, 'Basic EPS', period_col),
                        'diluted_eps': self.safe_extract(data, 'Diluted EPS', period_col),
                        
                        # Other items
                        'research_development': self.safe_extract(data, 'Research And Development', period_col),
                        'selling_general_administrative': self.safe_extract(data, 'Selling General Administrative', period_col),
                        'interest_expense': self.safe_extract(data, 'Interest Expense', period_col),
                        'tax_expense': self.safe_extract(data, 'Tax Provision', period_col),
                        
                        'currency': 'SEK'  # Default for Nordic stocks
                    }
                    
                    await self.store_financial_statement(financial_data)
                    success_count += 1
                    
            logger.info(f"✅ Stored {success_count} financial statements for {symbol}")
            return success_count > 0

        except Exception as e:
            logger.error(f"Error backfilling financial statements for {symbol}: {e}")
            return False

    async def backfill_balance_sheet(self, symbol: str) -> bool:
        """Backfill balance sheet data."""
        
        yahoo_symbol = await self.get_yahoo_symbol(symbol)
        if not yahoo_symbol:
            return False
            
        try:
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get quarterly and annual balance sheets
            statements = [
                ('quarterly', ticker.quarterly_balance_sheet),
                ('annual', ticker.balance_sheet)
            ]
            
            success_count = 0
            
            for statement_type, data in statements:
                if data.empty:
                    continue
                    
                for period_col in data.columns:
                    period_date = period_col.date()
                    
                    balance_data = {
                        'symbol': symbol,
                        'period_date': period_date,
                        'statement_type': statement_type,
                        
                        # Assets
                        'total_assets': self.safe_extract(data, 'Total Assets', period_col),
                        'current_assets': self.safe_extract(data, 'Current Assets', period_col),
                        'cash_and_equivalents': self.safe_extract(data, 'Cash Cash Equivalents And Short Term Investments', period_col),
                        'accounts_receivable': self.safe_extract(data, 'Accounts Receivable', period_col),
                        'inventory': self.safe_extract(data, 'Inventory', period_col),
                        
                        # Liabilities
                        'total_liabilities': self.safe_extract(data, 'Total Liabilities Net Minority Interest', period_col),
                        'current_liabilities': self.safe_extract(data, 'Current Liabilities', period_col),
                        'total_debt': self.safe_extract(data, 'Total Debt', period_col),
                        'long_term_debt': self.safe_extract(data, 'Long Term Debt', period_col),
                        'accounts_payable': self.safe_extract(data, 'Accounts Payable', period_col),
                        
                        # Equity
                        'total_equity': self.safe_extract(data, 'Total Equity Gross Minority Interest', period_col),
                        'retained_earnings': self.safe_extract(data, 'Retained Earnings', period_col),
                        
                        # Shares
                        'shares_outstanding': self.safe_extract(data, 'Share Issued', period_col),
                        
                        'currency': 'SEK'
                    }
                    
                    await self.store_balance_sheet(balance_data)
                    success_count += 1
                    
            logger.info(f"✅ Stored {success_count} balance sheet records for {symbol}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error backfilling balance sheet for {symbol}: {e}")
            return False
            
    async def backfill_cash_flow(self, symbol: str) -> bool:
        """Backfill cash flow data."""
        
        yahoo_symbol = await self.get_yahoo_symbol(symbol)
        if not yahoo_symbol:
            return False
            
        try:
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get quarterly and annual cash flows
            statements = [
                ('quarterly', ticker.quarterly_cashflow),
                ('annual', ticker.cashflow)
            ]
            
            success_count = 0
            
            for statement_type, data in statements:
                if data.empty:
                    continue
                    
                for period_col in data.columns:
                    period_date = period_col.date()
                    
                    # Calculate free cash flow
                    operating_cf = self.safe_extract(data, 'Operating Cash Flow', period_col)
                    capex = self.safe_extract(data, 'Capital Expenditure', period_col)
                    free_cf = None
                    if operating_cf is not None and capex is not None:
                        free_cf = operating_cf + capex  # CapEx is negative
                    
                    cash_flow_data = {
                        'symbol': symbol,
                        'period_date': period_date,
                        'statement_type': statement_type,
                        
                        # Operating
                        'operating_cash_flow': operating_cf,
                        'net_income': self.safe_extract(data, 'Net Income From Continuing Operation Net Minority Interest', period_col),
                        'depreciation_amortization': self.safe_extract(data, 'Depreciation Amortization Depletion', period_col),
                        
                        # Investing
                        'investing_cash_flow': self.safe_extract(data, 'Investing Cash Flow', period_col),
                        'capital_expenditure': capex,
                        
                        # Financing
                        'financing_cash_flow': self.safe_extract(data, 'Financing Cash Flow', period_col),
                        'dividends_paid': self.safe_extract(data, 'Cash Dividends Paid', period_col),
                        
                        # Calculated
                        'free_cash_flow': free_cf,
                        
                        'currency': 'SEK'
                    }
                    
                    await self.store_cash_flow(cash_flow_data)
                    success_count += 1
                    
            logger.info(f"✅ Stored {success_count} cash flow records for {symbol}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error backfilling cash flow for {symbol}: {e}")
            return False
            
    def safe_extract(self, df: pd.DataFrame, metric: str, column) -> Optional[float]:
        """Safely extract metric from DataFrame."""
        try:
            if metric in df.index:
                value = df.loc[metric, column]
                if pd.notna(value):
                    return float(value)
        except:
            pass
        return None
        
    async def store_financial_statement(self, data: Dict):
        """Store financial statement data."""
        fields = list(data.keys())
        values = list(data.values())
        placeholders = [f"${i+1}" for i in range(len(values))]
        
        query = f"""
        INSERT INTO financial_statements ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (symbol, period_date, statement_type) DO NOTHING
        """
        
        await self.db_conn.execute(query, *values)
        
    async def store_balance_sheet(self, data: Dict):
        """Store balance sheet data."""
        fields = list(data.keys())
        values = list(data.values())
        placeholders = [f"${i+1}" for i in range(len(values))]
        
        query = f"""
        INSERT INTO balance_sheet_data ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (symbol, period_date, statement_type) DO NOTHING
        """
        
        await self.db_conn.execute(query, *values)
        
    async def store_cash_flow(self, data: Dict):
        """Store cash flow data."""
        fields = list(data.keys())
        values = list(data.values())
        placeholders = [f"${i+1}" for i in range(len(values))]
        
        query = f"""
        INSERT INTO cash_flow_data ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (symbol, period_date, statement_type) DO NOTHING
        """
        
        await self.db_conn.execute(query, *values)
        
    async def calculate_historical_metrics(self, symbol: str, start_date: date, end_date: date) -> bool:
        """Calculate daily historical metrics using price data and latest financials."""
        
        try:
            # Get price data for the period
            price_query = """
            SELECT date, close_price::NUMERIC as close_price
            FROM daily_price_data
            WHERE symbol = $1 
            AND date BETWEEN $2 AND $3
            ORDER BY date
            """
            
            price_rows = await self.db_conn.fetch(price_query, symbol, start_date, end_date)
            if not price_rows:
                return False
                
            # Check if we have any financial data for this symbol
            has_financials = await self.db_conn.fetchval(
                "SELECT COUNT(*) FROM financial_statements WHERE symbol = $1", symbol
            )
            has_balance = await self.db_conn.fetchval(
                "SELECT COUNT(*) FROM balance_sheet_data WHERE symbol = $1", symbol
            )
            
            if not has_financials and not has_balance:
                logger.warning(f"No financial data found for {symbol} to calculate metrics")
                return False
                
            success_count = 0
            
            for price_row in price_rows:
                date_val = price_row['date']
                close_price = float(price_row['close_price'])
                
                # Get financial data available AS OF this specific date (not future data!)
                financials_for_date = await self.get_latest_financials_for_date(symbol, date_val)
                balance_for_date = await self.get_latest_balance_sheet_for_date(symbol, date_val)
                
                # Only calculate metrics if we have financial data available by this date
                if not financials_for_date and not balance_for_date:
                    continue  # Skip dates with no available fundamental data
                
                # Calculate metrics
                metrics = await self.calculate_metrics_for_date(
                    symbol, date_val, close_price, financials_for_date, balance_for_date
                )
                
                if metrics:
                    await self.store_historical_metric(metrics)
                    success_count += 1
                    
            logger.info(f"✅ Calculated {success_count} daily metrics for {symbol}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error calculating historical metrics for {symbol}: {e}")
            return False
            
    async def get_latest_financials_for_date(self, symbol: str, ref_date: date) -> Optional[Dict]:
        """Get latest financial data available before reference date."""
        query = """
        SELECT * FROM financial_statements
        WHERE symbol = $1 AND period_date <= $2
        ORDER BY period_date DESC, statement_type DESC
        LIMIT 1
        """
        
        row = await self.db_conn.fetchrow(query, symbol, ref_date)
        return dict(row) if row else None
        
    async def get_latest_balance_sheet_for_date(self, symbol: str, ref_date: date) -> Optional[Dict]:
        """Get latest balance sheet data available before reference date."""
        query = """
        SELECT * FROM balance_sheet_data
        WHERE symbol = $1 AND period_date <= $2
        ORDER BY period_date DESC, statement_type DESC
        LIMIT 1
        """
        
        row = await self.db_conn.fetchrow(query, symbol, ref_date)
        return dict(row) if row else None
        
    async def calculate_metrics_for_date(self, symbol: str, date_val: date, 
                                       close_price: float, financials: Optional[Dict], 
                                       balance_sheet: Optional[Dict]) -> Optional[Dict]:
        """Calculate fundamental metrics for a specific date."""
        
        metrics = {
            'symbol': symbol,
            'date': date_val,
            'close_price': close_price
        }
        
        # Market cap (need shares outstanding)
        if balance_sheet and balance_sheet.get('shares_outstanding'):
            shares = balance_sheet['shares_outstanding']
            metrics['market_cap'] = int(close_price * shares)
            
        # P/E ratio (need EPS)
        if financials and financials.get('diluted_eps'):
            eps = financials['diluted_eps']
            if eps > 0:
                metrics['pe_ratio'] = close_price / eps
                
        # P/B ratio (need book value per share)
        if balance_sheet and balance_sheet.get('total_equity') and balance_sheet.get('shares_outstanding'):
            book_value_per_share = balance_sheet['total_equity'] / balance_sheet['shares_outstanding']
            metrics['pb_ratio'] = close_price / book_value_per_share
            metrics['book_value_per_share'] = book_value_per_share
            
        # P/S ratio (need revenue)
        if financials and financials.get('total_revenue') and balance_sheet and balance_sheet.get('shares_outstanding'):
            revenue_per_share = financials['total_revenue'] / balance_sheet['shares_outstanding']
            metrics['ps_ratio'] = close_price / revenue_per_share
            metrics['revenue_per_share'] = revenue_per_share
            
        # Financial health ratios
        if balance_sheet:
            if balance_sheet.get('total_debt') and balance_sheet.get('total_equity'):
                debt = balance_sheet['total_debt'] or 0
                equity = balance_sheet['total_equity'] or 1
                metrics['debt_to_equity'] = debt / equity
                
            if balance_sheet.get('current_assets') and balance_sheet.get('current_liabilities'):
                current_assets = balance_sheet['current_assets'] or 0
                current_liabs = balance_sheet['current_liabilities'] or 1
                metrics['current_ratio'] = current_assets / current_liabs
                
            if balance_sheet.get('cash_and_equivalents') and balance_sheet.get('shares_outstanding'):
                cash_per_share = balance_sheet['cash_and_equivalents'] / balance_sheet['shares_outstanding']
                metrics['cash_per_share'] = cash_per_share
                
        # Reference to source data
        if financials:
            metrics['financial_data_date'] = financials['period_date']
        elif balance_sheet:
            metrics['financial_data_date'] = balance_sheet['period_date']
            
        return metrics if len(metrics) > 4 else None  # Only store if we have meaningful data
        
    async def store_historical_metric(self, data: Dict):
        """Store historical metric data."""
        fields = list(data.keys())
        values = list(data.values())
        placeholders = [f"${i+1}" for i in range(len(values))]
        
        query = f"""
        INSERT INTO historical_fundamentals_daily ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (symbol, date) DO NOTHING
        """
        
        await self.db_conn.execute(query, *values)
        
    async def backfill_symbol_complete(self, symbol: str) -> Dict:
        """Complete backfill for a single symbol.

        Fetches 4 components from Yahoo Finance:
        1. Financial statements (income statement)
        2. Balance sheet
        3. Cash flow statement
        4. Earnings dates (for publication dates)
        """

        logger.info(f"🚀 Starting complete backfill for {symbol}")

        results = {
            'symbol': symbol,
            'financial_statements': False,
            'balance_sheet': False,
            'cash_flow': False,
            'earnings_dates': False,
        }

        # 1. Financial statements
        results['financial_statements'] = await self.backfill_financial_statements(symbol)
        await asyncio.sleep(1)  # Rate limiting

        # 2. Balance sheet
        results['balance_sheet'] = await self.backfill_balance_sheet(symbol)
        await asyncio.sleep(1)

        # 3. Cash flow
        results['cash_flow'] = await self.backfill_cash_flow(symbol)
        await asyncio.sleep(1)

        # 4. Earnings dates (for publication dates)
        results['earnings_dates'] = await self.backfill_earnings_dates(symbol)
        await asyncio.sleep(1)

        success_count = sum(1 for k, v in results.items() if k != 'symbol' and v is True)
        logger.info(f"✅ Backfill complete for {symbol}: {success_count}/4 successful")

        return results

    async def backfill_earnings_dates(self, symbol: str) -> bool:
        """Backfill publication dates using earnings_dates endpoint."""

        yahoo_symbol = await self.get_yahoo_symbol(symbol)
        if not yahoo_symbol:
            return False

        try:
            ticker = yf.Ticker(yahoo_symbol)
            earnings_dates = ticker.earnings_dates

            if earnings_dates is None or earnings_dates.empty:
                logger.debug(f"No earnings dates for {symbol}")
                return False

            # Convert earnings dates to a list of dates
            earnings_list = []
            for dt in earnings_dates.index:
                if pd.notna(dt):
                    earnings_list.append(dt.date() if hasattr(dt, 'date') else dt)

            if not earnings_list:
                return False

            earnings_list.sort()

            # Get periods needing publish dates from all 3 tables
            update_count = 0
            for table in ['financial_statements', 'balance_sheet_data', 'cash_flow_data']:
                rows = await self.db_conn.fetch(f"""
                    SELECT period_date FROM {table}
                    WHERE symbol = $1 AND publish_date IS NULL
                """, symbol)

                for row in rows:
                    period_date = row['period_date']
                    publish_date = self._match_earnings_date(period_date, earnings_list)

                    if publish_date:
                        await self.db_conn.execute(f"""
                            UPDATE {table}
                            SET publish_date = $1
                            WHERE symbol = $2 AND period_date = $3 AND publish_date IS NULL
                        """, publish_date, symbol, period_date)
                        update_count += 1

            if update_count > 0:
                logger.info(f"✅ Updated {update_count} publish dates for {symbol}")
            return update_count > 0

        except Exception as e:
            logger.debug(f"Error backfilling earnings dates for {symbol}: {e}")
            return False

    def _match_earnings_date(self, period_date: date, earnings_list: List[date]) -> Optional[date]:
        """Match a period to its earnings announcement date."""
        from datetime import datetime

        period_dt = datetime.combine(period_date, datetime.min.time())

        for earn_date in earnings_list:
            earn_dt = datetime.combine(earn_date, datetime.min.time()) if not isinstance(earn_date, datetime) else earn_date
            days_after = (earn_dt - period_dt).days

            # Publication should be 15-120 days after period end
            if 15 <= days_after <= 120:
                return earn_date

        return None
        
    async def get_symbols_for_backfill(self, limit: Optional[int] = None, only_missing: bool = False) -> List[str]:
        """Get symbols for backfilling.

        Args:
            limit: Max number of symbols to return
            only_missing: If True, only get companies without existing data
        """
        if only_missing:
            # Only companies that don't have financial statements yet
            query = """
                SELECT cm.primary_ticker
                FROM company_master cm
                WHERE cm.yahoo_symbol IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM financial_statements fs
                    WHERE fs.symbol = cm.primary_ticker
                )
                ORDER BY cm.primary_ticker
            """
        else:
            # All companies with yahoo symbols (for full refresh)
            query = """
                SELECT cm.primary_ticker
                FROM company_master cm
                WHERE cm.yahoo_symbol IS NOT NULL
                ORDER BY cm.primary_ticker
            """

        if limit:
            query += f" LIMIT {limit}"

        rows = await self.db_conn.fetch(query)
        return [row['primary_ticker'] for row in rows]
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Backfill all fundamentals data from Yahoo Finance."""
    import argparse
    from datetime import datetime
    import os

    parser = argparse.ArgumentParser(description='Backfill fundamentals from Yahoo Finance')
    parser.add_argument('--only-missing', action='store_true', help='Only process companies without existing data')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of companies to process')
    args = parser.parse_args()

    backfill = HistoricalFundamentalsBackfill()

    # Track results for JSON output
    run_results = {
        'start_time': datetime.now().isoformat(),
        'end_time': None,
        'mode': 'missing_only' if args.only_missing else 'full_refresh',
        'total_symbols': 0,
        'successful': [],
        'skipped_no_data': [],
        'failed': [],
        'summary': {}
    }

    try:
        await backfill.setup()

        # Get symbols to process
        symbols = await backfill.get_symbols_for_backfill(
            limit=args.limit,
            only_missing=args.only_missing
        )

        run_results['total_symbols'] = len(symbols)

        mode = "MISSING ONLY" if args.only_missing else "FULL REFRESH"
        print(f"🚀 Historical Fundamentals Backfill - {mode}")
        print("=" * 60)
        print(f"📊 Processing {len(symbols)} symbols...")

        for i, symbol in enumerate(symbols, 1):
            print(f"\n[{i}/{len(symbols)}] Processing {symbol}...")

            results = await backfill.backfill_symbol_complete(symbol)

            # Count successful components (excluding 'symbol' key)
            success_count = sum(1 for k, v in results.items() if k != 'symbol' and v is True)

            # Track result
            result_entry = {
                'symbol': symbol,
                'financial_statements': results.get('financial_statements', False),
                'balance_sheet': results.get('balance_sheet', False),
                'cash_flow': results.get('cash_flow', False),
                'earnings_dates': results.get('earnings_dates', False),
                'components_success': success_count,
                'processed_at': datetime.now().isoformat()
            }

            if success_count >= 1:
                run_results['successful'].append(result_entry)
                status = "✅ SUCCESS"
            elif success_count == 0:
                run_results['skipped_no_data'].append(result_entry)
                status = "⏭️ SKIPPED (no data)"
            else:
                run_results['failed'].append(result_entry)
                status = "❌ FAILED"

            print(f"   {status} ({success_count}/4 components)")

        # Final summary
        run_results['end_time'] = datetime.now().isoformat()
        run_results['summary'] = {
            'successful': len(run_results['successful']),
            'skipped_no_data': len(run_results['skipped_no_data']),
            'failed': len(run_results['failed'])
        }

        print(f"\n🎯 BATCH COMPLETE:")
        print(f"   Total symbols: {len(symbols)}")
        print(f"   Successful: {run_results['summary']['successful']}")
        print(f"   Skipped (no data): {run_results['summary']['skipped_no_data']}")
        print(f"   Failed: {run_results['summary']['failed']}")

        total_processed = run_results['summary']['successful'] + run_results['summary']['failed']
        if total_processed > 0:
            print(f"   Success rate: {run_results['summary']['successful']/total_processed*100:.1f}%")

        # Show what we collected
        print(f"\n📈 Total Data in Database:")

        tables = [
            ('financial_statements', 'Financial statements'),
            ('balance_sheet_data', 'Balance sheet records'),
            ('cash_flow_data', 'Cash flow records'),
        ]

        for table, description in tables:
            count = await backfill.db_conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            print(f"   {description}: {count:,}")

        # Save results to JSON
        os.makedirs('data', exist_ok=True)
        results_file = f"data/fundamentals_backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(run_results, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {results_file}")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        await backfill.cleanup()

if __name__ == "__main__":
    asyncio.run(main())