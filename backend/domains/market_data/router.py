"""
Market Data API Router

API endpoints for price history and financial statements.
"""

import logging
from datetime import date, timedelta
from typing import Optional
from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query
import asyncpg

from .schemas import (
    PriceDataPoint,
    PriceHistoryResponse,
    IncomeStatementItem,
    BalanceSheetItem,
    CashFlowItem,
    FinancialsResponse,
    DocumentItem,
    DocumentsResponse,
    CalendarEventItem,
    CalendarEventsResponse,
    GlobalCalendarEventItem,
    GlobalCalendarResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def _to_float(val) -> Optional[float]:
    """Convert Decimal or other numeric to float."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    return float(val)


async def _get_company_name(conn: asyncpg.Connection, symbol: str) -> Optional[str]:
    """Get company name from company_master table."""
    row = await conn.fetchrow(
        """
        SELECT company_name
        FROM company_master
        WHERE primary_ticker = $1 OR yahoo_symbol = $1
        LIMIT 1
        """,
        symbol
    )
    return row['company_name'] if row else None


async def _get_nordic_company_id(conn: asyncpg.Connection, symbol: str) -> Optional[str]:
    """Get nordic_companies id from a symbol (via company_master ticker)."""
    row = await conn.fetchrow(
        """
        SELECT nc.id, nc.name
        FROM nordic_companies nc
        WHERE nc.ticker = $1
        LIMIT 1
        """,
        symbol
    )
    return row['id'] if row else None


# ============== Price History Endpoint ==============

@router.get("/prices/{symbol}", response_model=PriceHistoryResponse)
async def get_price_history(
    symbol: str,
    days: int = Query(365, ge=1, le=7300, description="Number of days of history (max 20 years)"),
):
    """
    Get price history for a symbol.

    Returns OHLCV data for the specified number of days.
    Default is 1 year (365 days).
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Calculate date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        # Get company name
        company_name = await _get_company_name(conn, symbol)

        # Query price data
        rows = await conn.fetch(
            """
            SELECT
                date,
                open_price,
                high_price,
                low_price,
                close_price,
                adjusted_close,
                volume,
                daily_return
            FROM daily_price_data
            WHERE symbol = $1
              AND date >= $2
              AND date <= $3
            ORDER BY date ASC
            """,
            symbol, start_date, end_date
        )

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No price data found for symbol: {symbol}"
            )

        # Convert to response format
        prices = []
        for row in rows:
            prices.append(PriceDataPoint(
                date=row['date'],
                open=_to_float(row['open_price']) or 0.0,
                high=_to_float(row['high_price']) or 0.0,
                low=_to_float(row['low_price']) or 0.0,
                close=_to_float(row['close_price']) or 0.0,
                volume=row['volume'] or 0,
                adjusted_close=_to_float(row['adjusted_close']),
                daily_return=_to_float(row['daily_return']),
            ))

        # Calculate summary stats
        first_price = prices[0].close if prices else 0
        last_price = prices[-1].close if prices else 0
        price_change = last_price - first_price
        price_change_percent = (price_change / first_price * 100) if first_price else 0
        period_high = max(p.high for p in prices) if prices else 0
        period_low = min(p.low for p in prices) if prices else 0

        return PriceHistoryResponse(
            symbol=symbol,
            company_name=company_name,
            currency="SEK",
            prices=prices,
            latest_price=last_price,
            price_change=price_change,
            price_change_percent=price_change_percent,
            period_high=period_high,
            period_low=period_low,
            total_records=len(prices),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching price history for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


# ============== Financials Endpoint ==============

@router.get("/financials/{symbol}", response_model=FinancialsResponse)
async def get_financials(
    symbol: str,
    statement_type: Optional[str] = Query(None, description="Filter by 'annual' or 'quarterly'"),
    limit: int = Query(20, ge=1, le=100, description="Max statements per type"),
):
    """
    Get financial statements for a symbol.

    Returns income statements, balance sheets, and cash flow statements.
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Get company name
        company_name = await _get_company_name(conn, symbol)

        # Build statement type filter
        type_filter = ""
        params = [symbol, limit]
        if statement_type:
            type_filter = "AND statement_type = $3"
            params.append(statement_type)

        # Query income statements
        income_query = f"""
            SELECT *
            FROM financial_statements
            WHERE symbol = $1 {type_filter}
            ORDER BY period_date DESC
            LIMIT $2
        """
        income_rows = await conn.fetch(income_query, *params)

        income_statements = []
        currency = "SEK"
        for row in income_rows:
            if row.get('currency'):
                currency = row['currency']
            income_statements.append(IncomeStatementItem(
                period_date=row['period_date'],
                statement_type=row['statement_type'],
                fiscal_year=row.get('fiscal_year'),
                fiscal_quarter=row.get('fiscal_quarter'),
                total_revenue=_to_float(row.get('total_revenue')),
                gross_profit=_to_float(row.get('gross_profit')),
                operating_income=_to_float(row.get('operating_income')),
                net_income=_to_float(row.get('net_income')),
                ebit=_to_float(row.get('ebit')),
                ebitda=_to_float(row.get('ebitda')),
                basic_eps=_to_float(row.get('basic_eps')),
                diluted_eps=_to_float(row.get('diluted_eps')),
                research_development=_to_float(row.get('research_development')),
                selling_general_administrative=_to_float(row.get('selling_general_administrative')),
                interest_expense=_to_float(row.get('interest_expense')),
                tax_expense=_to_float(row.get('tax_expense')),
                currency=row.get('currency'),
            ))

        # Query balance sheets
        balance_query = f"""
            SELECT *
            FROM balance_sheet_data
            WHERE symbol = $1 {type_filter}
            ORDER BY period_date DESC
            LIMIT $2
        """
        balance_rows = await conn.fetch(balance_query, *params)

        balance_sheets = []
        for row in balance_rows:
            balance_sheets.append(BalanceSheetItem(
                period_date=row['period_date'],
                statement_type=row['statement_type'],
                total_assets=_to_float(row.get('total_assets')),
                current_assets=_to_float(row.get('current_assets')),
                cash_and_equivalents=_to_float(row.get('cash_and_equivalents')),
                accounts_receivable=_to_float(row.get('accounts_receivable')),
                inventory=_to_float(row.get('inventory')),
                total_liabilities=_to_float(row.get('total_liabilities')),
                current_liabilities=_to_float(row.get('current_liabilities')),
                total_debt=_to_float(row.get('total_debt')),
                long_term_debt=_to_float(row.get('long_term_debt')),
                accounts_payable=_to_float(row.get('accounts_payable')),
                total_equity=_to_float(row.get('total_equity')),
                retained_earnings=_to_float(row.get('retained_earnings')),
                shares_outstanding=_to_float(row.get('shares_outstanding')),
                currency=row.get('currency'),
            ))

        # Query cash flow statements
        cashflow_query = f"""
            SELECT *
            FROM cash_flow_data
            WHERE symbol = $1 {type_filter}
            ORDER BY period_date DESC
            LIMIT $2
        """
        cashflow_rows = await conn.fetch(cashflow_query, *params)

        cash_flow_statements = []
        for row in cashflow_rows:
            cash_flow_statements.append(CashFlowItem(
                period_date=row['period_date'],
                statement_type=row['statement_type'],
                operating_cash_flow=_to_float(row.get('operating_cash_flow')),
                net_income=_to_float(row.get('net_income')),
                depreciation_amortization=_to_float(row.get('depreciation_amortization')),
                investing_cash_flow=_to_float(row.get('investing_cash_flow')),
                capital_expenditure=_to_float(row.get('capital_expenditure')),
                financing_cash_flow=_to_float(row.get('financing_cash_flow')),
                dividends_paid=_to_float(row.get('dividends_paid')),
                free_cash_flow=_to_float(row.get('free_cash_flow')),
                currency=row.get('currency'),
            ))

        # Check if we found any data
        if not income_statements and not balance_sheets and not cash_flow_statements:
            raise HTTPException(
                status_code=404,
                detail=f"No financial data found for symbol: {symbol}"
            )

        return FinancialsResponse(
            symbol=symbol,
            company_name=company_name,
            currency=currency,
            income_statements=income_statements,
            balance_sheets=balance_sheets,
            cash_flow_statements=cash_flow_statements,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching financials for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


# ============== Documents Endpoint ==============

@router.get("/documents/{symbol}", response_model=DocumentsResponse)
async def get_documents(
    symbol: str,
    document_type: Optional[str] = Query(None, description="Filter by type: annual_report, quarterly_report, press_release, etc."),
    limit: int = Query(50, ge=1, le=200, description="Max documents to return"),
):
    """
    Get documents for a symbol.

    Returns annual reports, quarterly reports, press releases, etc.
    Documents are sorted by publish date (most recent first).
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Get company info
        company_name = await _get_company_name(conn, symbol)
        nordic_company_id = await _get_nordic_company_id(conn, symbol)

        if not nordic_company_id:
            raise HTTPException(
                status_code=404,
                detail=f"No company found for symbol: {symbol}"
            )

        # Build document type filter
        type_filter = ""
        params = [nordic_company_id, limit]
        if document_type:
            type_filter = "AND nd.document_type = $3"
            params.append(document_type)

        # Query documents
        query = f"""
            SELECT
                nd.id,
                nd.document_type,
                nd.report_period,
                nd.title,
                nd.publish_date,
                nd.language,
                nd.source_url,
                nd.storage_path,
                nd.extraction_status,
                nd.page_count,
                nd.file_size_mb
            FROM nordic_documents nd
            WHERE nd.company_id = $1 {type_filter}
            ORDER BY nd.publish_date DESC NULLS LAST, nd.created_at DESC
            LIMIT $2
        """
        rows = await conn.fetch(query, *params)

        # Get counts
        if document_type:
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM nordic_documents WHERE company_id = $1 AND document_type = $2",
                nordic_company_id, document_type
            )
        else:
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM nordic_documents WHERE company_id = $1",
                nordic_company_id
            )

        downloaded_count = await conn.fetchval(
            "SELECT COUNT(*) FROM nordic_documents WHERE company_id = $1 AND storage_path IS NOT NULL",
            nordic_company_id
        )
        extracted_count = await conn.fetchval(
            "SELECT COUNT(*) FROM nordic_documents WHERE company_id = $1 AND extraction_status = 'completed'",
            nordic_company_id
        )

        # Convert to response format
        documents = []
        for row in rows:
            documents.append(DocumentItem(
                id=str(row['id']),
                document_type=row['document_type'],
                report_period=row.get('report_period'),
                title=row.get('title'),
                publish_date=row.get('publish_date'),
                language=row.get('language'),
                source_url=row.get('source_url'),
                has_local_file=row.get('storage_path') is not None,
                has_extracted_text=row.get('extraction_status') == 'completed',
                page_count=row.get('page_count'),
                file_size_mb=_to_float(row.get('file_size_mb')),
            ))

        return DocumentsResponse(
            symbol=symbol,
            company_name=company_name,
            documents=documents,
            total_count=total_count or 0,
            downloaded_count=downloaded_count or 0,
            extracted_count=extracted_count or 0,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching documents for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


# ============== Calendar Events Endpoint ==============

@router.get("/events/{symbol}", response_model=CalendarEventsResponse)
async def get_calendar_events(
    symbol: str,
    event_type: Optional[str] = Query(None, description="Filter by type: earnings, dividend, agm, other"),
    include_past: bool = Query(True, description="Include past events"),
    limit: int = Query(50, ge=1, le=200, description="Max events to return"),
):
    """
    Get calendar events for a symbol.

    Returns earnings dates, dividend events, AGMs, etc.
    Events are sorted by date (most recent/upcoming first).
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Get company info
        company_name = await _get_company_name(conn, symbol)
        nordic_company_id = await _get_nordic_company_id(conn, symbol)

        if not nordic_company_id:
            raise HTTPException(
                status_code=404,
                detail=f"No company found for symbol: {symbol}"
            )

        # Build filters
        filters = ["nce.company_id = $1"]
        params = [nordic_company_id]
        param_idx = 2

        if event_type:
            filters.append(f"nce.event_type = ${param_idx}")
            params.append(event_type)
            param_idx += 1

        if not include_past:
            filters.append(f"nce.event_date >= ${param_idx}")
            params.append(date.today())
            param_idx += 1

        params.append(limit)
        where_clause = " AND ".join(filters)

        # Query events
        query = f"""
            SELECT
                nce.id,
                nce.event_type,
                nce.event_date,
                nce.event_time,
                nce.title,
                nce.description,
                nce.confirmed,
                nce.webcast_url,
                nce.source_url,
                nce.dividend_amount,
                nce.dividend_currency,
                nce.ex_dividend_date,
                nce.payment_date
            FROM nordic_calendar_events nce
            WHERE {where_clause}
            ORDER BY nce.event_date DESC
            LIMIT ${param_idx}
        """
        rows = await conn.fetch(query, *params)

        # Get counts
        total_count = await conn.fetchval(
            "SELECT COUNT(*) FROM nordic_calendar_events WHERE company_id = $1",
            nordic_company_id
        )
        upcoming_count = await conn.fetchval(
            "SELECT COUNT(*) FROM nordic_calendar_events WHERE company_id = $1 AND event_date >= $2",
            nordic_company_id, date.today()
        )

        # Convert to response format
        events = []
        for row in rows:
            events.append(CalendarEventItem(
                id=str(row['id']),
                event_type=row['event_type'],
                event_date=row['event_date'],
                event_time=str(row['event_time']) if row.get('event_time') else None,
                title=row.get('title'),
                description=row.get('description'),
                confirmed=row.get('confirmed', False),
                webcast_url=row.get('webcast_url'),
                source_url=row.get('source_url'),
                dividend_amount=_to_float(row.get('dividend_amount')),
                dividend_currency=row.get('dividend_currency'),
                ex_dividend_date=row.get('ex_dividend_date'),
                payment_date=row.get('payment_date'),
            ))

        return CalendarEventsResponse(
            symbol=symbol,
            company_name=company_name,
            events=events,
            total_count=total_count or 0,
            upcoming_count=upcoming_count or 0,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching events for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()


# ============== Global Calendar Endpoint ==============

@router.get("/calendar", response_model=GlobalCalendarResponse)
async def get_global_calendar(
    event_type: Optional[str] = Query(None, description="Filter by type: earnings, dividend, agm, other"),
    start_date: Optional[date] = Query(None, description="Start date for filtering events"),
    end_date: Optional[date] = Query(None, description="End date for filtering events"),
    days_ahead: int = Query(90, ge=1, le=365, description="Days ahead to look for upcoming events (default 90)"),
    days_back: int = Query(30, ge=0, le=365, description="Days back to include past events (default 30)"),
    limit: int = Query(500, ge=1, le=1000, description="Max events to return"),
):
    """
    Get calendar events across all companies.

    Returns a global view of all financial calendar events (earnings, dividends, AGMs, etc.)
    sorted by date. Events include company information for context.
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Build date range
        if start_date is None:
            start_date = date.today() - timedelta(days=days_back)
        if end_date is None:
            end_date = date.today() + timedelta(days=days_ahead)

        # Build filters
        filters = ["nce.event_date >= $1", "nce.event_date <= $2"]
        params: list = [start_date, end_date]
        param_idx = 3

        if event_type:
            filters.append(f"nce.event_type = ${param_idx}")
            params.append(event_type)
            param_idx += 1

        params.append(limit)
        where_clause = " AND ".join(filters)

        # Query events with company info
        query = f"""
            SELECT
                nce.id,
                nce.event_type,
                nce.event_date,
                nce.event_time,
                nce.title,
                nce.description,
                nce.confirmed,
                nce.webcast_url,
                nce.source_url,
                nce.dividend_amount,
                nce.dividend_currency,
                nce.ex_dividend_date,
                nce.payment_date,
                nc.ticker as symbol,
                nc.name as company_name
            FROM nordic_calendar_events nce
            JOIN nordic_companies nc ON nce.company_id = nc.id
            WHERE {where_clause}
            ORDER BY nce.event_date ASC
            LIMIT ${param_idx}
        """
        rows = await conn.fetch(query, *params)

        # Get counts
        total_count = await conn.fetchval(
            "SELECT COUNT(*) FROM nordic_calendar_events nce WHERE nce.event_date >= $1 AND nce.event_date <= $2",
            start_date, end_date
        )
        upcoming_count = await conn.fetchval(
            "SELECT COUNT(*) FROM nordic_calendar_events WHERE event_date >= $1",
            date.today()
        )

        # Get event type counts
        type_counts_rows = await conn.fetch("""
            SELECT event_type, COUNT(*) as count
            FROM nordic_calendar_events
            WHERE event_date >= $1 AND event_date <= $2
            GROUP BY event_type
        """, start_date, end_date)
        event_type_counts = {row['event_type']: row['count'] for row in type_counts_rows}

        # Convert to response format
        events = []
        for row in rows:
            events.append(GlobalCalendarEventItem(
                id=str(row['id']),
                event_type=row['event_type'],
                event_date=row['event_date'],
                event_time=str(row['event_time']) if row.get('event_time') else None,
                title=row.get('title'),
                description=row.get('description'),
                confirmed=row.get('confirmed', False),
                webcast_url=row.get('webcast_url'),
                source_url=row.get('source_url'),
                dividend_amount=_to_float(row.get('dividend_amount')),
                dividend_currency=row.get('dividend_currency'),
                ex_dividend_date=row.get('ex_dividend_date'),
                payment_date=row.get('payment_date'),
                symbol=row['symbol'],
                company_name=row['company_name'],
            ))

        return GlobalCalendarResponse(
            events=events,
            total_count=total_count or 0,
            upcoming_count=upcoming_count or 0,
            event_type_counts=event_type_counts,
        )

    except Exception as e:
        logger.error(f"Error fetching global calendar: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await conn.close()
