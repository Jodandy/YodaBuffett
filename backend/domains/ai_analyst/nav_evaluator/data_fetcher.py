"""
Database data fetcher for NAV-Quality Evaluator

Pulls balance sheet data for asset-backed companies and calculates required fields.
"""
import asyncpg
import json
from typing import List, Optional
from datetime import date, timedelta
from .models import CompanyAssetInput


class NAVDataFetcher:
    """
    Fetch and calculate all required data from database for NAV evaluation.

    Calculates:
    - Balance sheet items: cash, securities, receivables, property, goodwill, intangibles
    - Liabilities and equity
    - Cash burn from operating cash flow
    - Recent share issuance (dilution tracking)
    """

    def __init__(self, db_conn: asyncpg.Connection):
        self.db_conn = db_conn

    async def fetch_all_companies(
        self,
        as_of_date: Optional[date] = None,
        limit: Optional[int] = None
    ) -> List[CompanyAssetInput]:
        """
        Fetch all companies with calculated NAV inputs.

        Args:
            as_of_date: Analysis date (defaults to today)
            limit: Optional limit on number of companies

        Returns:
            List of CompanyAssetInput objects
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get all companies with price data and balance sheet data
        query = """
            SELECT DISTINCT
                cm.id,
                cm.company_name,
                cm.primary_ticker,
                cm.yahoo_symbol
            FROM company_master cm
            WHERE EXISTS (
                SELECT 1 FROM daily_price_data dpd
                WHERE dpd.symbol = cm.primary_ticker
                AND dpd.date <= $1
            )
            AND EXISTS (
                SELECT 1 FROM yahoo_financials yf
                WHERE yf.symbol = cm.primary_ticker
                AND yf.balance_sheet IS NOT NULL
                AND yf.period_date <= $1
            )
            ORDER BY cm.company_name
        """

        if limit:
            query += f" LIMIT {limit}"

        rows = await self.db_conn.fetch(query, as_of_date)

        companies = []
        seen_tickers = set()  # Dedup by ticker
        for row in rows:
            ticker = row['primary_ticker']

            # Skip duplicate tickers
            if ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)

            company = await self._fetch_company(
                company_id=row['id'],
                ticker=ticker,
                name=row['company_name'],
                yahoo_symbol=row['yahoo_symbol'],
                as_of_date=as_of_date
            )
            if company:
                companies.append(company)

        return companies

    async def _fetch_company(
        self,
        company_id: str,
        ticker: str,
        name: str,
        yahoo_symbol: str,
        as_of_date: date
    ) -> Optional[CompanyAssetInput]:
        """
        Fetch and calculate all fields for a single company.
        """
        try:
            # Get current price
            price = await self._get_price(ticker, as_of_date)
            if not price:
                return None

            # Get most recent balance sheet (point-in-time safe)
            balance_sheet = await self._get_balance_sheet(ticker, as_of_date)
            if not balance_sheet:
                return None

            # Parse balance sheet JSONB for detailed fields
            bs_data = self._parse_json_field(balance_sheet['balance_sheet'])
            if not bs_data:
                return None

            # Extract balance sheet components
            total_assets = float(balance_sheet['total_assets']) if balance_sheet['total_assets'] else 0.0
            total_liabilities = total_assets - float(balance_sheet['total_equity']) if balance_sheet['total_equity'] else 0.0
            total_equity = float(balance_sheet['total_equity']) if balance_sheet['total_equity'] else 0.0

            # Hard assets
            cash_and_equivalents = float(bs_data.get('cash_and_cash_equivalents', 0) or 0)
            marketable_securities = float(bs_data.get('available_for_sale_securities', 0) or 0)
            receivables = float(bs_data.get('accounts_receivable', 0) or 0)

            # Investment property (use 'properties' field as proxy)
            # Note: This may include operational property - Layer 2 LLM will assess quality
            investment_property = float(bs_data.get('properties', 0) or 0) if bs_data.get('properties') else None

            # Soft assets
            goodwill = float(balance_sheet['goodwill']) if balance_sheet['goodwill'] else None
            intangibles = float(balance_sheet['other_intangible_assets']) if balance_sheet['other_intangible_assets'] else None

            # Shares outstanding
            shares_outstanding = float(bs_data.get('ordinary_shares_number', 0) or 0)
            if shares_outstanding <= 0:
                # Fallback: try to get from income statement
                shares_outstanding = await self._get_shares_outstanding(ticker, as_of_date)
                if not shares_outstanding or shares_outstanding <= 0:
                    return None

            # Calculate quarterly cash burn from operating cash flow
            quarterly_cash_burn = await self._calculate_cash_burn(ticker, as_of_date)

            # Calculate recent share issuance (dilution)
            recent_share_issuance = await self._calculate_recent_dilution(ticker, as_of_date)

            return CompanyAssetInput(
                ticker=ticker,
                name=name,
                price=price,
                total_assets=total_assets,
                total_liabilities=total_liabilities,
                total_equity=total_equity,
                cash_and_equivalents=cash_and_equivalents,
                marketable_securities=marketable_securities if marketable_securities > 0 else None,
                receivables=receivables if receivables > 0 else None,
                investment_property=investment_property,
                goodwill=goodwill,
                intangibles=intangibles,
                quarterly_cash_burn=quarterly_cash_burn,
                shares_outstanding=shares_outstanding,
                recent_share_issuance=recent_share_issuance
            )

        except Exception as e:
            print(f"Warning: Failed to fetch {ticker}: {e}")
            return None

    async def _get_price(self, ticker: str, as_of_date: date) -> Optional[float]:
        """
        Get current stock price (no look-ahead).

        Returns None if latest price is too old (company likely delisted).
        """
        row = await self.db_conn.fetchrow("""
            SELECT close_price, date
            FROM daily_price_data
            WHERE symbol = $1
              AND date <= $2
            ORDER BY date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not row:
            return None

        # Filter out dead companies - require price from 2026 or later
        price_date = row['date']
        if price_date.year < 2026:
            return None

        return float(row['close_price'])

    async def _get_balance_sheet(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[dict]:
        """
        Get most recent balance sheet (point-in-time safe).

        Uses publish_date if available, otherwise period_date + 75 days fallback.
        """
        row = await self.db_conn.fetchrow("""
            SELECT
                balance_sheet,
                total_assets,
                total_equity,
                total_debt,
                goodwill,
                other_intangible_assets,
                period_date,
                publish_date
            FROM yahoo_financials
            WHERE symbol = $1
              AND balance_sheet IS NOT NULL
              AND (
                  (publish_date IS NOT NULL AND publish_date <= $2)
                  OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $2)
              )
            ORDER BY period_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not row:
            return None

        return row

    def _parse_json_field(self, field):
        """Parse JSONB field that may be string or dict."""
        if field is None:
            return None
        if isinstance(field, dict):
            return field
        if isinstance(field, str):
            try:
                return json.loads(field)
            except:
                return None
        return None

    async def _get_shares_outstanding(self, ticker: str, as_of_date: date) -> Optional[float]:
        """
        Get shares outstanding from most recent balance sheet.
        Fallback if ordinary_shares_number not in balance_sheet JSONB.
        """
        row = await self.db_conn.fetchrow("""
            SELECT balance_sheet
            FROM yahoo_financials
            WHERE symbol = $1
              AND balance_sheet IS NOT NULL
              AND (
                  (publish_date IS NOT NULL AND publish_date <= $2)
                  OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $2)
              )
            ORDER BY period_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if row and row['balance_sheet']:
            bs = self._parse_json_field(row['balance_sheet'])
            if bs:
                # Try multiple possible fields
                shares = (
                    bs.get('ordinary_shares_number') or
                    bs.get('share_issued') or
                    bs.get('common_stock_shares_outstanding')
                )
                return float(shares) if shares else None

        return None

    async def _calculate_cash_burn(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[float]:
        """
        Calculate quarterly cash burn from operating cash flow.

        Returns positive value for cash burn (absolute value of negative OCF).
        Returns None if company is cash flow positive.
        """
        # Get last 4 quarters of cash flow data
        rows = await self.db_conn.fetch("""
            SELECT operating_cash_flow, period_date, statement_type
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'quarterly'
              AND operating_cash_flow IS NOT NULL
              AND (
                  (publish_date IS NOT NULL AND publish_date <= $2)
                  OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $2)
              )
            ORDER BY period_date DESC
            LIMIT 4
        """, ticker, as_of_date)

        if not rows:
            return None

        # Average operating cash flow over available quarters
        ocf_values = [float(row['operating_cash_flow']) for row in rows]
        avg_ocf = sum(ocf_values) / len(ocf_values)

        # If OCF is negative, that's cash burn (convert to positive)
        if avg_ocf < 0:
            return abs(avg_ocf)

        # If OCF is positive, no cash burn
        return None

    async def _calculate_recent_dilution(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[float]:
        """
        Calculate recent share issuance by comparing shares outstanding over time.

        Returns:
            Fraction of dilution (e.g., 0.10 = 10% dilution in last year)
            None if data insufficient
        """
        # Get shares outstanding from 1 year ago and current
        rows = await self.db_conn.fetch("""
            SELECT
                balance_sheet,
                period_date,
                (
                    (publish_date IS NOT NULL AND publish_date <= $2)
                    OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $2)
                ) as available
            FROM yahoo_financials
            WHERE symbol = $1
              AND balance_sheet IS NOT NULL
            ORDER BY period_date DESC
            LIMIT 8
        """, ticker, as_of_date)

        if len(rows) < 2:
            return None

        # Filter only available statements (no look-ahead)
        available_rows = [r for r in rows if r['available']]
        if len(available_rows) < 2:
            return None

        # Get most recent shares outstanding
        latest_bs = self._parse_json_field(available_rows[0]['balance_sheet'])
        if not latest_bs:
            return None

        latest_shares = latest_bs.get('ordinary_shares_number')
        if not latest_shares:
            return None
        latest_shares = float(latest_shares)

        # Get shares from ~1 year ago
        # Find statement that's roughly 1 year back
        one_year_ago = as_of_date - timedelta(days=365)
        old_shares = None

        for row in available_rows[1:]:
            if row['period_date'] <= one_year_ago:
                old_bs = self._parse_json_field(row['balance_sheet'])
                if old_bs and old_bs.get('ordinary_shares_number'):
                    old_shares = float(old_bs['ordinary_shares_number'])
                    break

        if not old_shares or old_shares <= 0:
            return None

        # Calculate dilution: (new_shares - old_shares) / old_shares
        dilution = (latest_shares - old_shares) / old_shares

        # Only return if dilution is positive (share issuance)
        if dilution > 0:
            return dilution

        return None
