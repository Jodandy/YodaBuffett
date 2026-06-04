"""
Database data fetcher for Focus-Narrowing Engine

Pulls data from existing tables and calculates required fields.
"""
import asyncpg
import json
from typing import List, Optional
from datetime import date, timedelta
from .models import CompanyInput

# Exchange rates (from backend/CLAUDE.md)
EXCHANGE_RATES_TO_SEK = {
    "SEK": 1.0,
    "EUR": 11.50,
    "USD": 10.80,
    "NOK": 0.95,
    "DKK": 1.54,
}

def get_stock_currency_from_symbol(yahoo_symbol: str) -> str:
    """Determine stock trading currency from exchange suffix"""
    if yahoo_symbol.endswith('.ST'):
        return 'SEK'
    elif yahoo_symbol.endswith('.OL'):
        return 'NOK'
    elif yahoo_symbol.endswith('.CO'):
        return 'DKK'
    elif yahoo_symbol.endswith('.HE'):
        return 'EUR'
    return 'SEK'  # Default

def get_exchange_rate(from_currency: str, to_currency: str) -> float:
    """Get exchange rate between currencies via SEK"""
    if from_currency == to_currency:
        return 1.0

    # Convert through SEK
    from_to_sek = EXCHANGE_RATES_TO_SEK.get(from_currency, 1.0)
    to_to_sek = EXCHANGE_RATES_TO_SEK.get(to_currency, 1.0)

    # from -> SEK -> to
    return from_to_sek / to_to_sek


class FunnelDataFetcher:
    """
    Fetch and calculate all required data from database.

    Calculates:
    - eps_norm: Normalized earnings per share (3-year average)
    - growth_hist: Historical revenue CAGR (5-year)
    - roic: Return on invested capital
    - nav_ps: Book value per share
    - price: Current stock price
    """

    def __init__(self, db_conn: asyncpg.Connection):
        self.db_conn = db_conn

    async def fetch_all_companies(
        self,
        as_of_date: Optional[date] = None,
        limit: Optional[int] = None
    ) -> List[CompanyInput]:
        """
        Fetch all companies with calculated funnel inputs.

        Args:
            as_of_date: Analysis date (defaults to today)
            limit: Optional limit on number of companies

        Returns:
            List of CompanyInput objects
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get all companies with price data
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

            # Skip duplicate tickers (e.g., SECT B appearing twice)
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
        as_of_date: Optional[date]
    ) -> Optional[CompanyInput]:
        """
        Fetch and calculate all fields for a single company.
        """
        if as_of_date is None:
            as_of_date = date.today()

        try:
            # Get current price
            price = await self._get_price(ticker, as_of_date)
            if not price:
                return None

            # Derive EPS from P/E (currency-safe - ratio cancels currency)
            eps_norm = await self._calculate_eps_from_pe(ticker, price, as_of_date)

            # Calculate historical growth (5-year revenue CAGR)
            growth_hist = await self._calculate_growth_hist(ticker, as_of_date)

            # Calculate ROIC
            roic = await self._calculate_roic(ticker, as_of_date)

            # Derive NAV per share from P/B (currency-safe - ratio cancels currency)
            nav_ps = await self._get_nav_per_share(ticker, price, as_of_date)

            # Calculate durable compounder metrics
            growth_cagr_robust = await self._calculate_durable_growth(ticker, as_of_date)
            track_record_years = await self._get_track_record_length(ticker, as_of_date)
            growth_consistency_score = await self._calculate_growth_consistency(ticker, as_of_date)
            goodwill_fraction, organic_roic = await self._calculate_goodwill_adjusted_roic(ticker, as_of_date)

            return CompanyInput(
                ticker=ticker,
                name=name,
                price=price,
                eps_norm=eps_norm,
                growth_hist=growth_hist,
                roic=roic,
                nav_ps=nav_ps,
                div_ps=None,  # TODO: Add dividend if needed
                sector=None,  # TODO: Add sector if needed
                growth_cagr_robust=growth_cagr_robust,
                track_record_years=track_record_years,
                growth_consistency_score=growth_consistency_score,
                goodwill_fraction=goodwill_fraction,
                organic_roic=organic_roic
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
        """Get shares outstanding from most recent financials."""
        row = await self.db_conn.fetchrow("""
            SELECT income_statement
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
            ORDER BY period_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if row and row['income_statement']:
            income = self._parse_json_field(row['income_statement'])
            if not income:
                return None

            # Yahoo financials may have shares outstanding in various fields (lowercase with underscores)
            shares = (
                income.get('shares_outstanding') or
                income.get('basic_average_shares') or
                income.get('common_stock_shares_outstanding') or
                income.get('SharesOutstanding') or  # Try camelCase as fallback
                income.get('BasicAverageShares') or
                income.get('CommonStockSharesOutstanding')
            )
            return float(shares) if shares else None

        return None

    async def _calculate_eps_from_pe(
        self,
        ticker: str,
        price: float,
        as_of_date: date
    ) -> Optional[float]:
        """
        Derive EPS from P/E ratio (currency-safe method).

        P/E is a pure ratio, so currency cancels:
        price (in stock_currency) / P/E = EPS (in stock_currency)

        This sidesteps currency mislabeling in financial statements.

        Args:
            ticker: Stock ticker
            price: Current price (in stock trading currency)
            as_of_date: Analysis date

        Returns:
            Normalized EPS (in stock trading currency)
        """
        # Get trailing P/E from yahoo_company_info
        info_row = await self.db_conn.fetchrow("""
            SELECT trailing_pe, fetched_date
            FROM yahoo_company_info
            WHERE symbol = $1
              AND fetched_date <= $2
            ORDER BY fetched_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not info_row or not info_row['trailing_pe']:
            return None

        trailing_pe = float(info_row['trailing_pe'])

        # Sanity check - if P/E is absurdly high (>200) or negative, data is suspect
        if trailing_pe <= 0 or trailing_pe > 200:
            return None

        # Derive trailing EPS: price / P/E = EPS (currency-safe!)
        eps_trailing = price / trailing_pe

        # Now normalize over time to smooth cyclicals
        # Get last 3 years of P/E ratios if available
        pe_history = await self.db_conn.fetch("""
            SELECT trailing_pe, fetched_date
            FROM yahoo_company_info
            WHERE symbol = $1
              AND fetched_date <= $2
              AND trailing_pe > 0
              AND trailing_pe <= 200
            ORDER BY fetched_date DESC
            LIMIT 3
        """, ticker, as_of_date)

        if len(pe_history) >= 2:
            # Average the derived EPS over available snapshots
            eps_values = [price / float(row['trailing_pe']) for row in pe_history]
            eps_norm = sum(eps_values) / len(eps_values)
        else:
            # Only one snapshot - use trailing EPS without normalization
            eps_norm = eps_trailing

        return eps_norm

    async def _calculate_eps_norm_old(
        self,
        ticker: str,
        as_of_date: date,
        shares: float,
        yahoo_symbol: str
    ) -> Optional[float]:
        """
        Calculate normalized EPS (3-year average to smooth cyclicals).
        Applies currency conversion to match stock trading currency.

        Args:
            ticker: Stock ticker
            as_of_date: Analysis date
            shares: Shares outstanding
            yahoo_symbol: Yahoo symbol (for currency detection)

        Returns:
            Normalized EPS (3-year average, in stock trading currency)
        """
        # Determine stock trading currency from exchange suffix
        stock_currency = get_stock_currency_from_symbol(yahoo_symbol)

        # Get last 3 years of annual net income with currency
        rows = await self.db_conn.fetch("""
            SELECT net_income, currency
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND net_income IS NOT NULL
            ORDER BY period_date DESC
            LIMIT 3
        """, ticker, as_of_date)

        if len(rows) < 2:  # Need at least 2 years
            return None

        # Get report currency from latest year
        report_currency = rows[0]['currency']

        # Calculate exchange rate
        fx_rate = 1.0
        if report_currency and stock_currency and report_currency != stock_currency:
            fx_rate = get_exchange_rate(report_currency, stock_currency)

        # Average net income over available years (with currency conversion)
        net_incomes = [float(row['net_income']) * fx_rate for row in rows]
        avg_net_income = sum(net_incomes) / len(net_incomes)

        # EPS = net income / shares (both now in stock currency)
        eps_norm = avg_net_income / shares if shares > 0 else None

        return eps_norm

    async def _calculate_growth_hist(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[float]:
        """
        Calculate historical revenue CAGR over 5 years.

        Returns:
            Growth rate as decimal (e.g., 0.08 = 8%)
        """
        # Get revenue from 5 years ago and current
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_revenue
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND total_revenue IS NOT NULL
              AND total_revenue > 0
            ORDER BY period_date DESC
            LIMIT 6
        """, ticker, as_of_date)

        if len(rows) < 3:  # Need at least 3 years
            return None

        # Use oldest and newest available
        latest = rows[0]
        oldest = rows[-1]

        latest_rev = float(latest['total_revenue'])
        oldest_rev = float(oldest['total_revenue'])

        # Calculate years between
        years = (latest['period_date'] - oldest['period_date']).days / 365.25

        if years < 1 or oldest_rev <= 0:
            return None

        # CAGR = (ending / beginning)^(1/years) - 1
        cagr = ((latest_rev / oldest_rev) ** (1 / years)) - 1

        return cagr

    async def _calculate_roic(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[float]:
        """
        Calculate Return on Invested Capital.

        ROIC = NOPAT / Invested Capital
        Approximation: Net Income / (Total Equity + Total Debt)

        Returns:
            ROIC as decimal (e.g., 0.15 = 15%)
        """
        row = await self.db_conn.fetchrow("""
            SELECT net_income, total_equity, total_debt
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND net_income IS NOT NULL
              AND total_equity IS NOT NULL
            ORDER BY period_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not row:
            return None

        net_income = float(row['net_income'])
        equity = float(row['total_equity'])
        debt = float(row['total_debt']) if row['total_debt'] else 0.0

        invested_capital = equity + debt

        if invested_capital <= 0:
            return None

        roic = net_income / invested_capital

        return roic

    async def _get_nav_per_share(
        self,
        ticker: str,
        price: float,
        as_of_date: date
    ) -> Optional[float]:
        """
        Derive NAV (book value) per share from P/B ratio (currency-safe method).

        P/B is a pure ratio, so currency cancels:
        price (in stock_currency) / P/B = book value per share (in stock_currency)

        This sidesteps currency mislabeling in balance sheet, same as EPS fix.

        Args:
            ticker: Stock ticker
            price: Current price (in stock trading currency)
            as_of_date: Analysis date

        Returns:
            NAV per share (in stock trading currency)
        """
        # Get P/B from yahoo_company_info
        info_row = await self.db_conn.fetchrow("""
            SELECT price_to_book, fetched_date
            FROM yahoo_company_info
            WHERE symbol = $1
              AND fetched_date <= $2
            ORDER BY fetched_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not info_row or not info_row['price_to_book']:
            return None

        pb_ratio = float(info_row['price_to_book'])

        # Sanity check - if P/B is absurd or negative, skip
        if pb_ratio <= 0 or pb_ratio > 50:  # P/B > 50x is suspicious
            return None

        # Derive NAV per share: price / P/B = book value per share (currency-safe!)
        nav_ps = price / pb_ratio

        return nav_ps

    async def _calculate_durable_growth(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[float]:
        """
        Calculate robust multi-year revenue CAGR for durable compounders.

        Uses 5-10 years of data and checks for consistency:
        - Must have same-signed endpoints (both positive)
        - CAGR should be robust to dropping the best year
        - Bounded year-to-year volatility

        Returns:
            Robust CAGR as decimal, or None if inconsistent/insufficient data
        """
        # Get up to 10 years of annual revenue data
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_revenue
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND total_revenue IS NOT NULL
              AND total_revenue > 0
            ORDER BY period_date DESC
            LIMIT 11
        """, ticker, as_of_date)

        if len(rows) < 4:  # Need at least 4 years for durable status
            return None

        # Get oldest and newest
        latest = rows[0]
        oldest = rows[-1]

        latest_rev = float(latest['total_revenue'])
        oldest_rev = float(oldest['total_revenue'])

        # Check for same-signed endpoints (both must be positive - already filtered in query)
        if latest_rev <= 0 or oldest_rev <= 0:
            return None

        # Calculate base CAGR
        years = (latest['period_date'] - oldest['period_date']).days / 365.25
        if years < 3:  # Need significant time span (3 years for 4 data points)
            return None

        base_cagr = ((latest_rev / oldest_rev) ** (1 / years)) - 1

        # Check robustness: Calculate CAGR dropping the best year
        # If CAGR changes dramatically, growth is driven by single spike
        revenues = [float(row['total_revenue']) for row in rows]
        year_over_year_growth = []
        for i in range(len(revenues) - 1):
            if revenues[i+1] > 0:
                yoy = (revenues[i] / revenues[i+1]) - 1
                year_over_year_growth.append(yoy)

        if year_over_year_growth:
            # Find and exclude best year
            best_year_idx = year_over_year_growth.index(max(year_over_year_growth))

            # Recalculate excluding best year (use revenues before and after best year)
            if best_year_idx == 0:
                # Best year is most recent - use second newest as endpoint
                adjusted_latest_rev = revenues[1]
                adjusted_years = (rows[1]['period_date'] - oldest['period_date']).days / 365.25
            elif best_year_idx == len(year_over_year_growth) - 1:
                # Best year is oldest - use second oldest as start
                adjusted_latest_rev = latest_rev
                adjusted_oldest_rev = revenues[-2]
                adjusted_years = (latest['period_date'] - rows[-2]['period_date']).days / 365.25
                oldest_rev = adjusted_oldest_rev
            else:
                # Best year is in middle - just use base CAGR
                adjusted_years = years
                adjusted_latest_rev = latest_rev

            if adjusted_years >= 1.5:
                robust_cagr = ((adjusted_latest_rev / oldest_rev) ** (1 / adjusted_years)) - 1

                # If CAGR drops by >50% when excluding best year, it's spike-driven
                if robust_cagr < base_cagr * 0.5:
                    return None  # Not durable - single year dominates

                return robust_cagr

        return base_cagr

    async def _get_track_record_length(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[int]:
        """
        Get length of financial track record in years.

        Returns:
            Number of years of annual financial data
        """
        rows = await self.db_conn.fetch("""
            SELECT COUNT(*) as count
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND total_revenue IS NOT NULL
              AND total_revenue > 0
        """, ticker, as_of_date)

        if rows and rows[0]['count']:
            return int(rows[0]['count'])

        return None

    async def _calculate_growth_consistency(
        self,
        ticker: str,
        as_of_date: date
    ) -> Optional[float]:
        """
        Calculate growth consistency score (fraction of positive growth years).

        Returns:
            Score between 0 and 1 (1 = all years positive growth)
        """
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_revenue
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND total_revenue IS NOT NULL
              AND total_revenue > 0
            ORDER BY period_date DESC
            LIMIT 11
        """, ticker, as_of_date)

        if len(rows) < 3:
            return None

        # Calculate year-over-year growth for each period
        positive_years = 0
        total_years = 0

        for i in range(len(rows) - 1):
            current_rev = float(rows[i]['total_revenue'])
            prior_rev = float(rows[i+1]['total_revenue'])

            if prior_rev > 0:
                growth = (current_rev / prior_rev) - 1
                if growth > 0:
                    positive_years += 1
                total_years += 1

        if total_years == 0:
            return None

        return positive_years / total_years

    async def _calculate_goodwill_adjusted_roic(
        self,
        ticker: str,
        as_of_date: date
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Calculate goodwill-adjusted (organic) ROIC for serial acquirers.

        Returns:
            Tuple of (goodwill_fraction, organic_roic)
            - goodwill_fraction: Goodwill / invested capital
            - organic_roic: Net income / (invested capital - goodwill)

        Guards denominator: If invested capital ≈ goodwill, returns None
        for organic ROIC to avoid exploded values.
        """
        row = await self.db_conn.fetchrow("""
            SELECT net_income, total_equity, total_debt, goodwill
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date <= $2
              AND net_income IS NOT NULL
              AND total_equity IS NOT NULL
            ORDER BY period_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not row:
            return None, None

        net_income = float(row['net_income'])
        equity = float(row['total_equity'])
        debt = float(row['total_debt']) if row['total_debt'] else 0.0
        goodwill = float(row['goodwill']) if row['goodwill'] else 0.0

        invested_capital = equity + debt

        if invested_capital <= 0:
            return None, None

        # Calculate goodwill fraction
        goodwill_fraction = goodwill / invested_capital if invested_capital > 0 else 0.0

        # Calculate organic ROIC
        organic_capital = invested_capital - goodwill

        # Guard: If goodwill ≈ invested capital (within 10%), denominator is too small
        if organic_capital < invested_capital * 0.10:
            return goodwill_fraction, None  # Return fraction but not organic ROIC

        organic_roic = net_income / organic_capital if organic_capital > 0 else None

        return goodwill_fraction, organic_roic
