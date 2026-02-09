"""
Point-in-Time Data Access Helpers

This module provides utilities to access financial data without look-ahead bias.
The core problem: financial_statements.period_date is when the fiscal period ENDED,
not when the data was PUBLISHED. Using period_date directly causes look-ahead bias.

Solution:
1. Use actual publish dates from nordic_documents where available
2. Fall back to a conservative lag from period_date (60 days default)

Usage:
    # Instead of:
    WHERE period_date <= $score_date  # WRONG: look-ahead bias!

    # Use:
    effective_date = await get_effective_date(conn, symbol, period_date)
    WHERE period_date <= $score_date AND effective_date <= $score_date
"""

from datetime import date, timedelta
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

# Conservative lag assumptions when actual publish date is unknown
# Based on typical reporting timelines:
# - Q1/Q2/Q3: ~45 days after quarter end
# - Q4/Annual: ~60-90 days after year end
DEFAULT_QUARTERLY_LAG_DAYS = 45
DEFAULT_ANNUAL_LAG_DAYS = 75


async def get_latest_available_statement(
    conn,
    symbol: str,
    score_date: date,
    statement_type: str = "annual",
    use_publish_dates: bool = True
) -> Optional[Dict]:
    """
    Get the latest financial statement that was ACTUALLY AVAILABLE on score_date.

    This is the key function to avoid look-ahead bias.

    Args:
        conn: Database connection
        symbol: Stock symbol
        score_date: Date we're calculating the score for
        statement_type: "annual" or "quarterly"
        use_publish_dates: If True, try to use actual publish dates from nordic_documents

    Returns:
        Dict with financial data, or None if nothing was available
    """

    # First, try to get statements with actual publish dates
    if use_publish_dates:
        result = await _get_statement_with_publish_date(conn, symbol, score_date, statement_type)
        if result:
            return result

    # Fallback: use conservative lag
    lag_days = DEFAULT_ANNUAL_LAG_DAYS if statement_type == "annual" else DEFAULT_QUARTERLY_LAG_DAYS
    cutoff_period_date = score_date - timedelta(days=lag_days)

    query = """
        SELECT period_date, statement_type, fiscal_year, fiscal_quarter,
               total_revenue, gross_profit, operating_income, net_income,
               ebit, ebitda, basic_eps, diluted_eps
        FROM financial_statements
        WHERE symbol = $1
        AND period_date <= $2
        AND (statement_type = $3 OR $3 IS NULL)
        ORDER BY period_date DESC
        LIMIT 1
    """

    row = await conn.fetchrow(query, symbol, cutoff_period_date, statement_type)

    if row:
        return dict(row)

    return None


async def get_available_statements(
    conn,
    symbol: str,
    score_date: date,
    years_back: int = 5
) -> list:
    """
    Get all financial statements that were ACTUALLY AVAILABLE on score_date.

    Uses conservative lag: only includes statements where period_date + lag <= score_date

    Args:
        conn: Database connection
        symbol: Stock symbol
        score_date: Date we're calculating for
        years_back: How many years of history to fetch

    Returns:
        List of available statements, most recent first
    """

    # Use 45-day lag for quarterly, 75-day for annual
    # This is conservative but prevents look-ahead bias

    query = """
        SELECT period_date, statement_type, fiscal_year, fiscal_quarter,
               total_revenue, gross_profit, operating_income, net_income,
               ebit, ebitda, basic_eps, diluted_eps,
               -- Calculate when this data would have been available
               CASE
                   WHEN statement_type = 'annual' THEN period_date + INTERVAL '75 days'
                   ELSE period_date + INTERVAL '45 days'
               END as estimated_publish_date
        FROM financial_statements
        WHERE symbol = $1
        AND period_date >= $2 - INTERVAL '%s years'
        AND (
            (statement_type = 'annual' AND period_date + INTERVAL '75 days' <= $2)
            OR
            (statement_type != 'annual' AND period_date + INTERVAL '45 days' <= $2)
        )
        ORDER BY period_date DESC
    """ % years_back

    rows = await conn.fetch(query, symbol, score_date)
    return [dict(row) for row in rows]


async def get_available_balance_sheet(
    conn,
    symbol: str,
    score_date: date
) -> Optional[Dict]:
    """
    Get the latest balance sheet that was ACTUALLY AVAILABLE on score_date.
    """

    query = """
        SELECT period_date, statement_type,
               total_assets, current_assets, cash_and_equivalents,
               total_liabilities, current_liabilities, total_debt, long_term_debt,
               total_equity, retained_earnings, shares_outstanding
        FROM balance_sheet_data
        WHERE symbol = $1
        AND (
            (statement_type = 'annual' AND period_date + INTERVAL '75 days' <= $2)
            OR
            (statement_type != 'annual' AND period_date + INTERVAL '45 days' <= $2)
        )
        ORDER BY period_date DESC
        LIMIT 1
    """

    row = await conn.fetchrow(query, symbol, score_date)
    return dict(row) if row else None


async def get_available_cash_flow(
    conn,
    symbol: str,
    score_date: date
) -> Optional[Dict]:
    """
    Get the latest cash flow statement that was ACTUALLY AVAILABLE on score_date.
    """

    query = """
        SELECT period_date, statement_type,
               operating_cash_flow, net_income, depreciation_amortization,
               investing_cash_flow, capital_expenditure,
               financing_cash_flow, dividends_paid, free_cash_flow
        FROM cash_flow_data
        WHERE symbol = $1
        AND (
            (statement_type = 'annual' AND period_date + INTERVAL '75 days' <= $2)
            OR
            (statement_type != 'annual' AND period_date + INTERVAL '45 days' <= $2)
        )
        ORDER BY period_date DESC
        LIMIT 1
    """

    row = await conn.fetchrow(query, symbol, score_date)
    return dict(row) if row else None


async def _get_statement_with_publish_date(
    conn,
    symbol: str,
    score_date: date,
    statement_type: str
) -> Optional[Dict]:
    """
    Try to get a statement using actual publish dates from nordic_documents.
    This is more accurate but only works for Swedish companies with document data.
    """

    # This requires linking financial_statements to nordic_documents
    # which is complex because:
    # 1. Not all companies have nordic_documents
    # 2. The report_period format doesn't always match period_date

    # For now, return None to use the lag-based fallback
    # TODO: Implement actual publish date lookup when data is available
    return None


def estimate_publish_date(period_date: date, statement_type: str) -> date:
    """
    Estimate when a financial statement would have been published.

    This is a conservative estimate based on typical reporting timelines.

    Args:
        period_date: The fiscal period end date
        statement_type: "annual" or "quarterly"

    Returns:
        Estimated publish date
    """
    if statement_type == "annual":
        return period_date + timedelta(days=DEFAULT_ANNUAL_LAG_DAYS)
    else:
        return period_date + timedelta(days=DEFAULT_QUARTERLY_LAG_DAYS)


def is_data_available(period_date: date, score_date: date, statement_type: str) -> bool:
    """
    Check if a financial statement would have been available on score_date.

    Uses conservative lag assumptions.
    """
    estimated_publish = estimate_publish_date(period_date, statement_type)
    return estimated_publish <= score_date
