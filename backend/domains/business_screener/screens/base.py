"""
Base Screen Class

Abstract base class that all screens inherit from.
Supports point-in-time backtesting via score_date parameter.
Integrates with LLM service for Tier B and Tier C analysis.
"""

from abc import ABC, abstractmethod
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from uuid import UUID

import asyncpg

from ..models.screen_result import ScreenResult
from ..models.screen_definition import ScreenDefinition, SCREEN_DEFINITIONS

if TYPE_CHECKING:
    from ..llm.service import LLMService


class BaseScreen(ABC):
    """
    Abstract base class for all investment screens.

    Each screen implementation should:
    1. Define screen_type (1-15)
    2. Implement run_tier_a() for the SQL/math screen
    3. Optionally implement run_tier_b() for LLM analysis
    4. Optionally implement run_tier_c() for deep LLM analysis
    5. Implement calculate_score() to convert raw metrics to 0-100 score

    Backtesting:
    - All screens support a score_date parameter for point-in-time analysis
    - When score_date is provided, screens only use data available on that date
    - Financial data uses publish_date (or period_date + 75 days fallback)
    - Price data uses date <= score_date
    """

    # Must be overridden by subclass
    screen_type: int = 0

    # Default lag for financial data if publish_date is NULL (days after period_date)
    DEFAULT_PUBLISH_LAG_DAYS: int = 75

    # Maximum age of financial data (months) - prevents using stale filings
    MAX_FINANCIAL_DATA_AGE_MONTHS: int = 18

    def __init__(
        self,
        conn: asyncpg.Connection,
        score_date: date = None,
        llm_service: Optional['LLMService'] = None
    ):
        """
        Initialize the screen with a database connection.

        Args:
            conn: asyncpg database connection
            score_date: Date to run the screen as-of (None = today/latest)
            llm_service: Optional LLM service for Tier B/C analysis
        """
        self.conn = conn
        self._score_date = score_date or date.today()
        self._definition: Optional[ScreenDefinition] = None
        self._llm_service = llm_service

    @property
    def score_date(self) -> date:
        """The date this screen is being run as-of."""
        return self._score_date

    @property
    def is_backtest(self) -> bool:
        """Whether this is a historical backtest (not current date)."""
        return self._score_date < date.today()

    @property
    def definition(self) -> ScreenDefinition:
        """Get the screen definition."""
        if self._definition is None:
            self._definition = SCREEN_DEFINITIONS.get(self.screen_type)
            if self._definition is None:
                raise ValueError(f"No definition found for screen_type {self.screen_type}")
        return self._definition

    @property
    def name(self) -> str:
        """Get the screen name."""
        return self.definition.name

    @property
    def short_name(self) -> str:
        """Get the short name."""
        return self.definition.short_name

    def set_llm_service(self, llm_service: 'LLMService'):
        """
        Set the LLM service for Tier B/C analysis.

        Allows setting the service after screen initialization.

        Args:
            llm_service: The LLM service instance
        """
        self._llm_service = llm_service

    @property
    def has_llm_service(self) -> bool:
        """Check if LLM service is available."""
        return self._llm_service is not None

    # =========================================================================
    # POINT-IN-TIME HELPER METHODS
    # =========================================================================

    def get_financial_date_filter(self, table_alias: str = 'fs', include_freshness: bool = True) -> str:
        """
        Get SQL WHERE clause for point-in-time financial data filtering.

        Uses publish_date if available, otherwise falls back to period_date + 75 days.
        This ensures we only use data that was publicly available on score_date.

        Also includes a FRESHNESS check to exclude stale filings (default: 18 months).
        A company that hasn't filed in 2 years should not pass screens using old data.

        Args:
            table_alias: Table alias to use (e.g., 'fs' for financial_statements)
            include_freshness: Whether to include the freshness check (default True)

        Returns:
            SQL WHERE clause fragment
        """
        # Point-in-time filter (no look-ahead bias)
        pit_filter = f"""
            (
                ({table_alias}.publish_date IS NOT NULL AND {table_alias}.publish_date <= '{self._score_date}')
                OR ({table_alias}.publish_date IS NULL AND {table_alias}.period_date + INTERVAL '{self.DEFAULT_PUBLISH_LAG_DAYS} days' <= '{self._score_date}')
            )
        """

        if not include_freshness:
            return pit_filter

        # Freshness filter - data must be within MAX_FINANCIAL_DATA_AGE_MONTHS of score_date
        freshness_filter = f"""
            AND {table_alias}.period_date >= '{self._score_date}'::date - INTERVAL '{self.MAX_FINANCIAL_DATA_AGE_MONTHS} months'
        """

        return pit_filter + freshness_filter

    def get_price_date_filter(self, table_alias: str = 'dpd') -> str:
        """
        Get SQL WHERE clause for point-in-time price data filtering.

        Args:
            table_alias: Table alias to use

        Returns:
            SQL WHERE clause fragment
        """
        return f"{table_alias}.date <= '{self._score_date}'"

    async def get_latest_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest price on or before score_date.

        Args:
            symbol: Ticker symbol (hyphen format, e.g., 'VOLV-B')

        Returns:
            Dict with close_price, date, or None if not found
        """
        row = await self.conn.fetchrow("""
            SELECT close_price, date, volume
            FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC
            LIMIT 1
        """, symbol, self._score_date)
        return dict(row) if row else None

    async def get_latest_annual_financials(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest annual financials available on score_date.

        Uses publish_date for point-in-time accuracy.

        Args:
            symbol: Ticker symbol (space format for financials, e.g., 'VOLV B')

        Returns:
            Dict with financial data, or None if not found
        """
        row = await self.conn.fetchrow(f"""
            SELECT
                fs.symbol,
                fs.period_date,
                fs.fiscal_year,
                fs.publish_date,
                fs.currency,
                fs.total_revenue,
                fs.gross_profit,
                fs.operating_income,
                fs.net_income,
                fs.ebit,
                fs.ebitda,
                bs.total_assets,
                bs.current_assets,
                bs.cash_and_equivalents,
                bs.accounts_receivable,
                bs.inventory,
                bs.total_liabilities,
                bs.current_liabilities,
                bs.total_debt,
                bs.total_equity,
                bs.shares_outstanding,
                cf.operating_cash_flow,
                cf.free_cash_flow,
                cf.dividends_paid,
                cf.capital_expenditure
            FROM financial_statements fs
            LEFT JOIN balance_sheet_data bs
                ON fs.symbol = bs.symbol
                AND fs.period_date = bs.period_date
                AND fs.statement_type = bs.statement_type
            LEFT JOIN cash_flow_data cf
                ON fs.symbol = cf.symbol
                AND fs.period_date = cf.period_date
                AND fs.statement_type = cf.statement_type
            WHERE fs.symbol = $1
              AND fs.statement_type = 'annual'
              AND {self.get_financial_date_filter('fs')}
            ORDER BY fs.period_date DESC
            LIMIT 1
        """, symbol)
        return dict(row) if row else None

    async def get_market_cap(self, primary_ticker: str, shares_outstanding: int) -> Optional[float]:
        """
        Calculate market cap using price on score_date.

        Args:
            primary_ticker: Ticker in hyphen format
            shares_outstanding: Number of shares

        Returns:
            Market cap or None
        """
        price_data = await self.get_latest_price(primary_ticker)
        if price_data and price_data['close_price'] and shares_outstanding:
            return float(price_data['close_price']) * shares_outstanding
        return None

    async def get_ttm_financials(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get TTM (Trailing Twelve Months) financials by summing last 4 quarters.

        This provides much fresher data than annual reports:
        - Annual: Dec 2024 report → published Feb/Mar 2025 → 14mo stale by Dec 2025
        - TTM: Q3 2025 + Q2 2025 + Q1 2025 + Q4 2024 → only ~3mo stale

        Flow items (revenue, income, cash flow) are SUMMED over 4 quarters.
        Stock items (assets, equity, debt) use the LATEST quarter value.

        Args:
            symbol: Ticker symbol (space format for financials, e.g., 'VOLV B')

        Returns:
            Dict with TTM financial data, or None if insufficient quarterly data
        """
        # Get last 4 quarters available on score_date
        rows = await self.conn.fetch(f"""
            SELECT
                fs.symbol,
                fs.period_date,
                fs.publish_date,
                fs.currency,
                -- Flow items (will be summed)
                fs.total_revenue,
                fs.gross_profit,
                fs.operating_income,
                fs.net_income,
                fs.ebit,
                fs.ebitda,
                -- Balance sheet (latest only)
                bs.total_assets,
                bs.current_assets,
                bs.cash_and_equivalents,
                bs.accounts_receivable,
                bs.inventory,
                bs.total_liabilities,
                bs.current_liabilities,
                bs.total_debt,
                bs.total_equity,
                bs.shares_outstanding,
                -- Cash flow (will be summed)
                cf.operating_cash_flow,
                cf.free_cash_flow,
                cf.dividends_paid,
                cf.capital_expenditure
            FROM financial_statements fs
            LEFT JOIN balance_sheet_data bs
                ON fs.symbol = bs.symbol
                AND fs.period_date = bs.period_date
                AND fs.statement_type = bs.statement_type
            LEFT JOIN cash_flow_data cf
                ON fs.symbol = cf.symbol
                AND fs.period_date = cf.period_date
                AND fs.statement_type = cf.statement_type
            WHERE fs.symbol = $1
              AND fs.statement_type = 'quarterly'
              AND {self.get_financial_date_filter('fs', include_freshness=False)}
            ORDER BY fs.period_date DESC
            LIMIT 4
        """, symbol)

        if len(rows) < 4:
            # Not enough quarters for TTM, fall back to annual
            return None

        # Sum flow items over 4 quarters
        ttm = {
            'symbol': rows[0]['symbol'],
            'period_date': rows[0]['period_date'],  # Most recent quarter
            'currency': rows[0]['currency'],
            'is_ttm': True,
            'quarters_used': len(rows),

            # Flow items - SUM
            'total_revenue': sum(r['total_revenue'] or 0 for r in rows),
            'gross_profit': sum(r['gross_profit'] or 0 for r in rows),
            'operating_income': sum(r['operating_income'] or 0 for r in rows),
            'net_income': sum(r['net_income'] or 0 for r in rows),
            'ebit': sum(r['ebit'] or 0 for r in rows),
            'ebitda': sum(r['ebitda'] or 0 for r in rows),
            'operating_cash_flow': sum(r['operating_cash_flow'] or 0 for r in rows),
            'free_cash_flow': sum(r['free_cash_flow'] or 0 for r in rows),
            'dividends_paid': sum(r['dividends_paid'] or 0 for r in rows),
            'capital_expenditure': sum(r['capital_expenditure'] or 0 for r in rows),

            # Stock items - LATEST value only
            'total_assets': rows[0]['total_assets'],
            'current_assets': rows[0]['current_assets'],
            'cash_and_equivalents': rows[0]['cash_and_equivalents'],
            'accounts_receivable': rows[0]['accounts_receivable'],
            'inventory': rows[0]['inventory'],
            'total_liabilities': rows[0]['total_liabilities'],
            'current_liabilities': rows[0]['current_liabilities'],
            'total_debt': rows[0]['total_debt'],
            'total_equity': rows[0]['total_equity'],
            'shares_outstanding': rows[0]['shares_outstanding'],
        }

        return ttm

    def get_combined_financials_cte(self) -> str:
        """
        Get SQL CTE that prefers TTM (4 quarters) but falls back to annual.

        Priority:
        1. TTM from last 4 quarters (freshest, ~3mo old)
        2. Annual report (fallback, up to 14mo old)

        This ensures we always have data while preferring fresher TTM when available.

        Returns:
            SQL CTE fragment for combined financials
        """
        return f"""
            -- Step 1: Get TTM from last 4 quarters
            ttm_raw AS (
                SELECT
                    fs.symbol,
                    MAX(fs.period_date) AS period_date,
                    MAX(fs.currency) AS report_currency,
                    COUNT(*) AS quarters_count,
                    TRUE AS is_ttm,

                    -- Flow items - SUM
                    SUM(fs.total_revenue) AS total_revenue,
                    SUM(fs.gross_profit) AS gross_profit,
                    SUM(fs.operating_income) AS operating_income,
                    SUM(fs.net_income) AS net_income,
                    SUM(fs.ebit) AS ebit,
                    SUM(fs.ebitda) AS ebitda,

                    -- Balance sheet - LATEST
                    (ARRAY_AGG(bs.total_assets ORDER BY fs.period_date DESC))[1] AS total_assets,
                    (ARRAY_AGG(bs.current_assets ORDER BY fs.period_date DESC))[1] AS current_assets,
                    (ARRAY_AGG(bs.cash_and_equivalents ORDER BY fs.period_date DESC))[1] AS cash_and_equivalents,
                    (ARRAY_AGG(bs.accounts_receivable ORDER BY fs.period_date DESC))[1] AS accounts_receivable,
                    (ARRAY_AGG(bs.inventory ORDER BY fs.period_date DESC))[1] AS inventory,
                    (ARRAY_AGG(bs.total_liabilities ORDER BY fs.period_date DESC))[1] AS total_liabilities,
                    (ARRAY_AGG(bs.current_liabilities ORDER BY fs.period_date DESC))[1] AS current_liabilities,
                    (ARRAY_AGG(bs.total_debt ORDER BY fs.period_date DESC))[1] AS total_debt,
                    (ARRAY_AGG(bs.total_equity ORDER BY fs.period_date DESC))[1] AS total_equity,
                    (ARRAY_AGG(bs.shares_outstanding ORDER BY fs.period_date DESC))[1] AS shares_outstanding,

                    -- Cash flow - SUM
                    SUM(cf.operating_cash_flow) AS operating_cash_flow,
                    SUM(cf.free_cash_flow) AS free_cash_flow,
                    SUM(cf.capital_expenditure) AS capital_expenditure

                FROM financial_statements fs
                LEFT JOIN balance_sheet_data bs ON fs.symbol = bs.symbol
                    AND fs.period_date = bs.period_date AND fs.statement_type = bs.statement_type
                LEFT JOIN cash_flow_data cf ON fs.symbol = cf.symbol
                    AND fs.period_date = cf.period_date AND fs.statement_type = cf.statement_type
                WHERE fs.statement_type = 'quarterly'
                  AND {self.get_financial_date_filter('fs', include_freshness=False)}
                  AND fs.period_date >= '{self._score_date}'::date - INTERVAL '15 months'
                GROUP BY fs.symbol
                HAVING COUNT(*) >= 4
            ),

            -- Step 2: Get latest annual as fallback
            annual_raw AS (
                SELECT DISTINCT ON (fs.symbol)
                    fs.symbol,
                    fs.period_date,
                    fs.currency AS report_currency,
                    1 AS quarters_count,
                    FALSE AS is_ttm,
                    fs.total_revenue, fs.gross_profit, fs.operating_income, fs.net_income,
                    fs.ebit, fs.ebitda,
                    bs.total_assets, bs.current_assets, bs.cash_and_equivalents,
                    bs.accounts_receivable, bs.inventory, bs.total_liabilities,
                    bs.current_liabilities, bs.total_debt, bs.total_equity, bs.shares_outstanding,
                    cf.operating_cash_flow, cf.free_cash_flow, cf.capital_expenditure
                FROM financial_statements fs
                LEFT JOIN balance_sheet_data bs ON fs.symbol = bs.symbol
                    AND fs.period_date = bs.period_date AND fs.statement_type = bs.statement_type
                LEFT JOIN cash_flow_data cf ON fs.symbol = cf.symbol
                    AND fs.period_date = cf.period_date AND fs.statement_type = cf.statement_type
                WHERE fs.statement_type = 'annual'
                  AND {self.get_financial_date_filter('fs')}
                ORDER BY fs.symbol, fs.period_date DESC
            ),

            -- Step 3: Combine - prefer TTM, fall back to annual
            combined_financials AS (
                SELECT * FROM ttm_raw
                UNION ALL
                SELECT * FROM annual_raw a
                WHERE NOT EXISTS (SELECT 1 FROM ttm_raw t WHERE t.symbol = a.symbol)
            )
        """

    def get_yahoo_combined_financials_cte(self) -> str:
        """
        Get SQL CTE that uses yahoo_financials table with richer data.

        This provides access to:
        - Actual goodwill and intangible assets (not approximated)
        - Net tangible assets calculated by Yahoo
        - Net PPE, working capital, net debt directly available
        - Publish dates from earnings calendar

        Priority:
        1. TTM from last 4 quarters (freshest, ~3mo old)
        2. Annual report (fallback, up to 14mo old)

        Returns:
            SQL CTE fragment for yahoo-based combined financials
        """
        return f"""
            -- Step 1: Get TTM from yahoo_financials (last 4 quarters)
            yahoo_ttm_raw AS (
                SELECT
                    yf.symbol,
                    MAX(yf.period_date) AS period_date,
                    MAX(yf.currency) AS report_currency,
                    MAX(yf.publish_date) AS publish_date,
                    COUNT(*) AS quarters_count,
                    TRUE AS is_ttm,

                    -- Flow items - SUM
                    SUM((yf.income_statement->>'total_revenue')::NUMERIC::BIGINT) AS total_revenue,
                    SUM((yf.income_statement->>'gross_profit')::NUMERIC::BIGINT) AS gross_profit,
                    SUM((yf.income_statement->>'operating_income')::NUMERIC::BIGINT) AS operating_income,
                    SUM((yf.income_statement->>'net_income')::NUMERIC::BIGINT) AS net_income,
                    SUM((yf.income_statement->>'ebit')::NUMERIC::BIGINT) AS ebit,
                    SUM((yf.income_statement->>'ebitda')::NUMERIC::BIGINT) AS ebitda,

                    -- Balance sheet - LATEST non-NULL value (handles partial quarterly reports)
                    (ARRAY_AGG((yf.balance_sheet->>'total_assets')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'total_assets' IS NOT NULL))[1] AS total_assets,
                    (ARRAY_AGG((yf.balance_sheet->>'current_assets')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'current_assets' IS NOT NULL))[1] AS current_assets,
                    (ARRAY_AGG((yf.balance_sheet->>'cash_and_cash_equivalents')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'cash_and_cash_equivalents' IS NOT NULL))[1] AS cash_and_equivalents,
                    (ARRAY_AGG((yf.balance_sheet->>'accounts_receivable')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'accounts_receivable' IS NOT NULL))[1] AS accounts_receivable,
                    (ARRAY_AGG((yf.balance_sheet->>'inventory')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'inventory' IS NOT NULL))[1] AS inventory,
                    (ARRAY_AGG((yf.balance_sheet->>'total_liabilities_net_minority_interest')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'total_liabilities_net_minority_interest' IS NOT NULL))[1] AS total_liabilities,
                    (ARRAY_AGG((yf.balance_sheet->>'current_liabilities')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'current_liabilities' IS NOT NULL))[1] AS current_liabilities,
                    (ARRAY_AGG((yf.balance_sheet->>'total_debt')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'total_debt' IS NOT NULL))[1] AS total_debt,
                    (ARRAY_AGG((yf.balance_sheet->>'stockholders_equity')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'stockholders_equity' IS NOT NULL))[1] AS total_equity,
                    -- Try multiple share count fields, skip NULLs (Yahoo uses different keys depending on company)
                    (ARRAY_AGG(COALESCE(
                        (yf.balance_sheet->>'ordinary_shares_number')::NUMERIC::BIGINT,
                        (yf.balance_sheet->>'share_issued')::NUMERIC::BIGINT,
                        (yf.income_statement->>'basic_average_shares')::NUMERIC::BIGINT
                    ) ORDER BY yf.period_date DESC) FILTER (WHERE COALESCE(yf.balance_sheet->>'ordinary_shares_number', yf.balance_sheet->>'share_issued', yf.income_statement->>'basic_average_shares') IS NOT NULL))[1] AS shares_outstanding,

                    -- RICH ASSET DATA - skip NULLs from partial quarterly reports
                    (ARRAY_AGG((yf.balance_sheet->>'goodwill')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'goodwill' IS NOT NULL))[1] AS goodwill,
                    (ARRAY_AGG((yf.balance_sheet->>'other_intangible_assets')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'other_intangible_assets' IS NOT NULL))[1] AS other_intangible_assets,
                    (ARRAY_AGG((yf.balance_sheet->>'goodwill_and_other_intangible_assets')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'goodwill_and_other_intangible_assets' IS NOT NULL))[1] AS total_intangibles,
                    (ARRAY_AGG((yf.balance_sheet->>'net_tangible_assets')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'net_tangible_assets' IS NOT NULL))[1] AS net_tangible_assets,
                    (ARRAY_AGG((yf.balance_sheet->>'tangible_book_value')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'tangible_book_value' IS NOT NULL))[1] AS tangible_book_value,
                    (ARRAY_AGG((yf.balance_sheet->>'net_ppe')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'net_ppe' IS NOT NULL))[1] AS net_ppe,
                    (ARRAY_AGG((yf.balance_sheet->>'working_capital')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'working_capital' IS NOT NULL))[1] AS working_capital,
                    (ARRAY_AGG((yf.balance_sheet->>'net_debt')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'net_debt' IS NOT NULL))[1] AS net_debt,
                    (ARRAY_AGG((yf.balance_sheet->>'invested_capital')::NUMERIC::BIGINT ORDER BY yf.period_date DESC) FILTER (WHERE yf.balance_sheet->>'invested_capital' IS NOT NULL))[1] AS invested_capital,

                    -- Cash flow - SUM
                    SUM((yf.cash_flow->>'operating_cash_flow')::NUMERIC::BIGINT) AS operating_cash_flow,
                    SUM((yf.cash_flow->>'free_cash_flow')::NUMERIC::BIGINT) AS free_cash_flow,
                    SUM((yf.cash_flow->>'capital_expenditure')::NUMERIC::BIGINT) AS capital_expenditure

                FROM yahoo_financials yf
                WHERE yf.statement_type = 'quarterly'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '15 months'
                GROUP BY yf.symbol
                HAVING COUNT(*) >= 4
            ),

            -- Step 2: Get latest annual from yahoo_financials as fallback
            yahoo_annual_raw AS (
                SELECT DISTINCT ON (yf.symbol)
                    yf.symbol,
                    yf.period_date,
                    yf.currency AS report_currency,
                    yf.publish_date,
                    1 AS quarters_count,
                    FALSE AS is_ttm,

                    -- Income statement
                    (yf.income_statement->>'total_revenue')::NUMERIC::BIGINT AS total_revenue,
                    (yf.income_statement->>'gross_profit')::NUMERIC::BIGINT AS gross_profit,
                    (yf.income_statement->>'operating_income')::NUMERIC::BIGINT AS operating_income,
                    (yf.income_statement->>'net_income')::NUMERIC::BIGINT AS net_income,
                    (yf.income_statement->>'ebit')::NUMERIC::BIGINT AS ebit,
                    (yf.income_statement->>'ebitda')::NUMERIC::BIGINT AS ebitda,

                    -- Balance sheet
                    (yf.balance_sheet->>'total_assets')::NUMERIC::BIGINT AS total_assets,
                    (yf.balance_sheet->>'current_assets')::NUMERIC::BIGINT AS current_assets,
                    (yf.balance_sheet->>'cash_and_cash_equivalents')::NUMERIC::BIGINT AS cash_and_equivalents,
                    (yf.balance_sheet->>'accounts_receivable')::NUMERIC::BIGINT AS accounts_receivable,
                    (yf.balance_sheet->>'inventory')::NUMERIC::BIGINT AS inventory,
                    (yf.balance_sheet->>'total_liabilities_net_minority_interest')::NUMERIC::BIGINT AS total_liabilities,
                    (yf.balance_sheet->>'current_liabilities')::NUMERIC::BIGINT AS current_liabilities,
                    (yf.balance_sheet->>'total_debt')::NUMERIC::BIGINT AS total_debt,
                    (yf.balance_sheet->>'stockholders_equity')::NUMERIC::BIGINT AS total_equity,
                    -- Try multiple share count fields (Yahoo uses different keys depending on company)
                    COALESCE(
                        (yf.balance_sheet->>'ordinary_shares_number')::NUMERIC::BIGINT,
                        (yf.balance_sheet->>'share_issued')::NUMERIC::BIGINT,
                        (yf.income_statement->>'basic_average_shares')::NUMERIC::BIGINT
                    ) AS shares_outstanding,

                    -- RICH ASSET DATA
                    (yf.balance_sheet->>'goodwill')::NUMERIC::BIGINT AS goodwill,
                    (yf.balance_sheet->>'other_intangible_assets')::NUMERIC::BIGINT AS other_intangible_assets,
                    (yf.balance_sheet->>'goodwill_and_other_intangible_assets')::NUMERIC::BIGINT AS total_intangibles,
                    (yf.balance_sheet->>'net_tangible_assets')::NUMERIC::BIGINT AS net_tangible_assets,
                    (yf.balance_sheet->>'tangible_book_value')::NUMERIC::BIGINT AS tangible_book_value,
                    (yf.balance_sheet->>'net_ppe')::NUMERIC::BIGINT AS net_ppe,
                    (yf.balance_sheet->>'working_capital')::NUMERIC::BIGINT AS working_capital,
                    (yf.balance_sheet->>'net_debt')::NUMERIC::BIGINT AS net_debt,
                    (yf.balance_sheet->>'invested_capital')::NUMERIC::BIGINT AS invested_capital,

                    -- Cash flow
                    (yf.cash_flow->>'operating_cash_flow')::NUMERIC::BIGINT AS operating_cash_flow,
                    (yf.cash_flow->>'free_cash_flow')::NUMERIC::BIGINT AS free_cash_flow,
                    (yf.cash_flow->>'capital_expenditure')::NUMERIC::BIGINT AS capital_expenditure

                FROM yahoo_financials yf
                WHERE yf.statement_type = 'annual'
                  AND (
                      (yf.publish_date IS NOT NULL AND yf.publish_date <= '{self._score_date}')
                      OR (yf.publish_date IS NULL AND yf.period_date + INTERVAL '75 days' <= '{self._score_date}')
                  )
                  AND yf.period_date >= '{self._score_date}'::date - INTERVAL '18 months'
                ORDER BY yf.symbol, yf.period_date DESC
            ),

            -- Step 3: Combine - prefer TTM, fall back to annual
            yahoo_combined_financials AS (
                SELECT * FROM yahoo_ttm_raw
                UNION ALL
                SELECT * FROM yahoo_annual_raw a
                WHERE NOT EXISTS (SELECT 1 FROM yahoo_ttm_raw t WHERE t.symbol = a.symbol)
            )
        """

    def get_ttm_financial_cte(self, symbol_expr: str = 'c.financial_symbol') -> str:
        """
        Get SQL CTE for TTM financials that can be used in screen queries.

        This aggregates the last 4 quarters into TTM metrics.
        Use this in screen queries instead of annual-only queries for fresher data.

        Args:
            symbol_expr: SQL expression for the symbol to match

        Returns:
            SQL CTE fragment for TTM financials
        """
        return f"""
            ttm_financials AS (
                -- Calculate TTM by summing last 4 quarters
                SELECT
                    fs.symbol,
                    MAX(fs.period_date) AS period_date,
                    MAX(fs.currency) AS report_currency,
                    COUNT(*) AS quarters_count,

                    -- Flow items - SUM over 4 quarters
                    SUM(fs.total_revenue) AS total_revenue,
                    SUM(fs.gross_profit) AS gross_profit,
                    SUM(fs.operating_income) AS operating_income,
                    SUM(fs.net_income) AS net_income,
                    SUM(fs.ebit) AS ebit,
                    SUM(fs.ebitda) AS ebitda,

                    -- Balance sheet - use LATEST values (via FIRST_VALUE)
                    (ARRAY_AGG(bs.total_assets ORDER BY fs.period_date DESC))[1] AS total_assets,
                    (ARRAY_AGG(bs.current_assets ORDER BY fs.period_date DESC))[1] AS current_assets,
                    (ARRAY_AGG(bs.cash_and_equivalents ORDER BY fs.period_date DESC))[1] AS cash_and_equivalents,
                    (ARRAY_AGG(bs.accounts_receivable ORDER BY fs.period_date DESC))[1] AS accounts_receivable,
                    (ARRAY_AGG(bs.inventory ORDER BY fs.period_date DESC))[1] AS inventory,
                    (ARRAY_AGG(bs.total_liabilities ORDER BY fs.period_date DESC))[1] AS total_liabilities,
                    (ARRAY_AGG(bs.current_liabilities ORDER BY fs.period_date DESC))[1] AS current_liabilities,
                    (ARRAY_AGG(bs.total_debt ORDER BY fs.period_date DESC))[1] AS total_debt,
                    (ARRAY_AGG(bs.total_equity ORDER BY fs.period_date DESC))[1] AS total_equity,
                    (ARRAY_AGG(bs.shares_outstanding ORDER BY fs.period_date DESC))[1] AS shares_outstanding,

                    -- Cash flow - SUM over 4 quarters
                    SUM(cf.operating_cash_flow) AS operating_cash_flow,
                    SUM(cf.free_cash_flow) AS free_cash_flow,
                    SUM(cf.capital_expenditure) AS capital_expenditure

                FROM financial_statements fs
                LEFT JOIN balance_sheet_data bs
                    ON fs.symbol = bs.symbol
                    AND fs.period_date = bs.period_date
                    AND fs.statement_type = bs.statement_type
                LEFT JOIN cash_flow_data cf
                    ON fs.symbol = cf.symbol
                    AND fs.period_date = cf.period_date
                    AND fs.statement_type = cf.statement_type
                WHERE fs.statement_type = 'quarterly'
                  AND {self.get_financial_date_filter('fs', include_freshness=False)}
                  AND fs.period_date >= '{self._score_date}'::date - INTERVAL '15 months'
                GROUP BY fs.symbol
                HAVING COUNT(*) >= 4  -- Require 4 quarters for valid TTM
            )
        """

    # =========================================================================
    # ABSTRACT METHODS - Must be implemented by subclasses
    # =========================================================================

    @abstractmethod
    async def run_tier_a(self) -> List[ScreenResult]:
        """
        Run the Tier A (SQL/math) screen.

        Returns a list of ScreenResult objects for companies that pass.
        Must respect self.score_date for point-in-time accuracy.
        """
        pass

    @abstractmethod
    def calculate_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate a 0-100 score from the raw metrics.

        Args:
            metrics: Dictionary of calculated values from the screen query

        Returns:
            Score from 0-100
        """
        pass

    # =========================================================================
    # OPTIONAL METHODS - Override if screen has Tier B or C
    # =========================================================================

    async def run_tier_b(
        self,
        company_id: UUID,
        company_name: str,
        metrics: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Run Tier B (local LLM) analysis for a candidate.

        Uses the LLM service if available, otherwise returns empty dict.
        Screens with custom Tier B logic can override this method.

        Args:
            company_id: Company to analyze
            company_name: Company name for prompt generation
            metrics: Tier A metrics for this company

        Returns:
            LLM analysis results with score_adjustment
        """
        # Check if LLM service is available
        if not self._llm_service:
            self.log("No LLM service available for Tier B analysis")
            return {}

        # Check if this screen has Tier B enabled
        if not self.definition.tier_b_enabled:
            return {}

        # Get the prompt
        prompt = self.get_tier_b_prompt(company_name, metrics)
        if not prompt:
            self.log("No Tier B prompt defined for this screen")
            return {}

        # Run analysis
        result = await self._llm_service.analyze_tier_b(
            company_id=company_id,
            company_name=company_name,
            screen_type=self.screen_type,
            metrics=metrics,
            prompt=prompt,
            score_date=self._score_date
        )

        if result.success and result.parsed_response:
            return {
                "tier_b_analysis": result.parsed_response,
                "tier_b_score_adjustment": result.score_adjustment,
                "tier_b_model": result.model,
                "tier_b_latency_ms": result.latency_ms,
            }

        return {
            "tier_b_error": result.error,
            "tier_b_score_adjustment": 0,
        }

    async def run_tier_c(
        self,
        company_id: UUID,
        company_name: str,
        metrics: Dict[str, Any],
        tier_b_result: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Run Tier C (API LLM) deep analysis for a candidate.

        Uses the LLM service if available, otherwise returns empty dict.
        Should only be called for high-scoring candidates due to API cost.

        Args:
            company_id: Company to analyze
            company_name: Company name for prompt generation
            metrics: Tier A and B metrics for this company
            tier_b_result: Previous Tier B analysis result

        Returns:
            Deep analysis results with score_adjustment
        """
        # Check if LLM service is available
        if not self._llm_service:
            self.log("No LLM service available for Tier C analysis")
            return {}

        # Check if this screen has Tier C enabled
        if not self.definition.tier_c_enabled:
            return {}

        # Get the prompt
        prompt = self.get_tier_c_prompt(company_name, metrics)
        if not prompt:
            self.log("No Tier C prompt defined for this screen")
            return {}

        # Convert tier_b_result to AnalysisResult format if needed
        tier_b_analysis_result = None
        if tier_b_result and "tier_b_analysis" in tier_b_result:
            from ..llm.service import AnalysisResult
            tier_b_analysis_result = AnalysisResult(
                company_id=company_id,
                screen_type=self.screen_type,
                tier="B",
                success=True,
                raw_response="",
                parsed_response=tier_b_result.get("tier_b_analysis"),
                validation_result=None,
                score_adjustment=tier_b_result.get("tier_b_score_adjustment", 0),
                latency_ms=tier_b_result.get("tier_b_latency_ms", 0),
                model=tier_b_result.get("tier_b_model", "")
            )

        # Run analysis
        result = await self._llm_service.analyze_tier_c(
            company_id=company_id,
            company_name=company_name,
            screen_type=self.screen_type,
            metrics=metrics,
            prompt=prompt,
            tier_b_result=tier_b_analysis_result,
            score_date=self._score_date
        )

        if result.success and result.parsed_response:
            return {
                "tier_c_analysis": result.parsed_response,
                "tier_c_score_adjustment": result.score_adjustment,
                "tier_c_model": result.model,
                "tier_c_latency_ms": result.latency_ms,
                "tier_c_tokens": result.document_context_chars,
            }

        return {
            "tier_c_error": result.error,
            "tier_c_score_adjustment": 0,
        }

    def get_tier_b_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate the Tier B LLM prompt for a candidate.

        Override in subclass to provide screen-specific prompt.

        Args:
            company_name: Name of the company
            metrics: Tier A metrics

        Returns:
            Prompt string for the LLM
        """
        return ""

    def get_tier_c_prompt(self, company_name: str, metrics: Dict[str, Any]) -> str:
        """
        Generate the Tier C LLM prompt for a candidate.

        Override in subclass to provide screen-specific prompt.

        Args:
            company_name: Name of the company
            metrics: Tier A and B metrics

        Returns:
            Prompt string for the LLM
        """
        return ""

    # =========================================================================
    # CURRENCY CONVERSION
    # =========================================================================

    def get_fx_rate_sql(self, report_currency_col: str, trading_currency_col: str) -> str:
        """
        Generate SQL CASE statement to calculate FX rate from report currency to trading currency.

        Converts financial values (in report_currency) to stock price currency (trading_currency).

        Args:
            report_currency_col: Column name for report currency (e.g., 'f.currency')
            trading_currency_col: Column name for trading currency (e.g., 'c.trading_currency')

        Returns:
            SQL expression that evaluates to the FX rate
        """
        # FX rates: how many units of target currency per 1 unit of source
        # These are approximate rates - for production, would use live rates
        return f"""
            CASE
                WHEN {report_currency_col} = {trading_currency_col} THEN 1.0
                -- To SEK
                WHEN {report_currency_col} = 'EUR' AND {trading_currency_col} = 'SEK' THEN 11.50
                WHEN {report_currency_col} = 'USD' AND {trading_currency_col} = 'SEK' THEN 10.80
                WHEN {report_currency_col} = 'NOK' AND {trading_currency_col} = 'SEK' THEN 0.95
                WHEN {report_currency_col} = 'DKK' AND {trading_currency_col} = 'SEK' THEN 1.54
                WHEN {report_currency_col} = 'GBP' AND {trading_currency_col} = 'SEK' THEN 13.50
                -- To NOK
                WHEN {report_currency_col} = 'SEK' AND {trading_currency_col} = 'NOK' THEN 1.05
                WHEN {report_currency_col} = 'EUR' AND {trading_currency_col} = 'NOK' THEN 12.10
                WHEN {report_currency_col} = 'USD' AND {trading_currency_col} = 'NOK' THEN 11.35
                -- To DKK
                WHEN {report_currency_col} = 'SEK' AND {trading_currency_col} = 'DKK' THEN 0.65
                WHEN {report_currency_col} = 'EUR' AND {trading_currency_col} = 'DKK' THEN 7.45
                WHEN {report_currency_col} = 'USD' AND {trading_currency_col} = 'DKK' THEN 7.00
                -- To EUR
                WHEN {report_currency_col} = 'SEK' AND {trading_currency_col} = 'EUR' THEN 0.087
                WHEN {report_currency_col} = 'USD' AND {trading_currency_col} = 'EUR' THEN 0.94
                WHEN {report_currency_col} = 'NOK' AND {trading_currency_col} = 'EUR' THEN 0.083
                WHEN {report_currency_col} = 'DKK' AND {trading_currency_col} = 'EUR' THEN 0.134
                ELSE 1.0  -- Default: no conversion
            END
        """

    def get_stock_currency_from_ticker(self, ticker: str, yahoo_symbol: str = None) -> str:
        """
        Determine the trading currency based on exchange suffix.

        Args:
            ticker: Primary ticker
            yahoo_symbol: Yahoo symbol with exchange suffix (e.g., 'VOLV-B.ST')

        Returns:
            Currency code (e.g., 'SEK', 'NOK', 'EUR')
        """
        symbol = yahoo_symbol or ticker
        if symbol.endswith('.ST'):
            return 'SEK'
        elif symbol.endswith('.OL'):
            return 'NOK'
        elif symbol.endswith('.CO'):
            return 'DKK'
        elif symbol.endswith('.HE'):
            return 'EUR'
        return 'SEK'  # Default for Nordic

    def needs_currency_conversion(self, report_currency: str, trading_currency: str) -> bool:
        """Check if currencies differ and conversion is needed."""
        return report_currency != trading_currency and report_currency and trading_currency

    def add_currency_warning(self, flags: List[str], report_currency: str, trading_currency: str):
        """Add a warning flag if currencies don't match."""
        if self.needs_currency_conversion(report_currency, trading_currency):
            flags.append(f"CURRENCY_CONVERTED: {report_currency} -> {trading_currency}")

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def create_result(
        self,
        company_id: UUID,
        metrics: Dict[str, Any],
        tier: str = 'A',
        flags: List[str] = None
    ) -> ScreenResult:
        """
        Create a ScreenResult from metrics.

        Args:
            company_id: Company UUID
            metrics: Calculated metrics dictionary
            tier: Analysis tier ('A', 'B', 'C')
            flags: Optional warning flags

        Returns:
            ScreenResult object
        """
        score = self.calculate_score(metrics)

        # Add score_date to metrics for tracking
        metrics['score_date'] = self._score_date.isoformat()

        result = ScreenResult(
            company_id=company_id,
            screen_type=self.screen_type,
            tier=tier,
            passed=True,
            score=score,
            metrics=metrics,
            flags=flags or [],
            triggered_at=datetime.now()
        )

        # Check if Tier B/C analysis is needed
        if self.definition.tier_b_enabled and self.should_run_tier_b(metrics, score):
            result.requires_tier_b = True

        if self.definition.tier_c_enabled and self.should_run_tier_c(metrics, score):
            result.requires_tier_c = True

        return result

    def should_run_tier_b(self, metrics: Dict[str, Any], score: float) -> bool:
        """
        Determine if this candidate should go to Tier B analysis.

        Default: all Tier A passes go to Tier B if enabled.
        Override for custom logic.
        """
        return True

    def should_run_tier_c(self, metrics: Dict[str, Any], score: float) -> bool:
        """
        Determine if this candidate should go to Tier C analysis.

        Default: only high-scoring candidates (>70).
        Override for custom logic.
        """
        return score >= 70

    async def get_company_info(self, company_id: UUID) -> Optional[Dict[str, Any]]:
        """Get basic company information."""
        row = await self.conn.fetchrow("""
            SELECT
                id,
                company_name,
                primary_ticker,
                yahoo_symbol,
                sector,
                industry,
                country,
                currency
            FROM company_master
            WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    def clamp_score(self, score: float) -> float:
        """Ensure score is within 0-100 range."""
        return max(0.0, min(100.0, score))

    def log(self, message: str):
        """Log a message (for debugging/tracing)."""
        date_str = f" (as-of {self._score_date})" if self.is_backtest else ""
        print(f"[Screen {self.screen_type} - {self.short_name}{date_str}] {message}")


# Registry of all screen implementations
_screen_registry: Dict[int, type] = {}


def register_screen(screen_class: type):
    """
    Decorator to register a screen implementation.

    Usage:
        @register_screen
        class NetNetsScreen(BaseScreen):
            screen_type = 1
            ...
    """
    screen_type = getattr(screen_class, 'screen_type', 0)
    if screen_type < 1 or screen_type > 20:
        raise ValueError(f"Invalid screen_type {screen_type} for {screen_class.__name__}")
    _screen_registry[screen_type] = screen_class
    return screen_class


def get_screen_class(screen_type: int) -> Optional[type]:
    """Get the screen class for a screen type."""
    return _screen_registry.get(screen_type)


def get_all_screen_classes() -> Dict[int, type]:
    """Get all registered screen classes."""
    return _screen_registry.copy()
