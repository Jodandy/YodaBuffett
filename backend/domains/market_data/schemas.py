"""
Market Data Schemas
Pydantic models for price and financial data API responses
"""

from pydantic import BaseModel
from typing import List, Optional
from datetime import date


# ============== Price Data Schemas ==============

class PriceDataPoint(BaseModel):
    """Single price data point (OHLCV)"""
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: Optional[float] = None
    daily_return: Optional[float] = None


class PriceHistoryResponse(BaseModel):
    """Price history for a symbol"""
    symbol: str
    company_name: Optional[str] = None
    currency: str = "SEK"
    prices: List[PriceDataPoint]
    latest_price: Optional[float] = None
    price_change: Optional[float] = None
    price_change_percent: Optional[float] = None
    period_high: Optional[float] = None
    period_low: Optional[float] = None
    total_records: int


# ============== Financial Statements Schemas ==============

class IncomeStatementItem(BaseModel):
    """Income statement for a period"""
    period_date: date
    statement_type: str  # 'annual' or 'quarterly'
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    total_revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    ebit: Optional[float] = None
    ebitda: Optional[float] = None
    basic_eps: Optional[float] = None
    diluted_eps: Optional[float] = None
    research_development: Optional[float] = None
    selling_general_administrative: Optional[float] = None
    interest_expense: Optional[float] = None
    tax_expense: Optional[float] = None
    currency: Optional[str] = None


class BalanceSheetItem(BaseModel):
    """Balance sheet for a period"""
    period_date: date
    statement_type: str
    total_assets: Optional[float] = None
    current_assets: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    accounts_receivable: Optional[float] = None
    inventory: Optional[float] = None
    total_liabilities: Optional[float] = None
    current_liabilities: Optional[float] = None
    total_debt: Optional[float] = None
    long_term_debt: Optional[float] = None
    accounts_payable: Optional[float] = None
    total_equity: Optional[float] = None
    retained_earnings: Optional[float] = None
    shares_outstanding: Optional[float] = None
    currency: Optional[str] = None


class CashFlowItem(BaseModel):
    """Cash flow statement for a period"""
    period_date: date
    statement_type: str
    operating_cash_flow: Optional[float] = None
    net_income: Optional[float] = None
    depreciation_amortization: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    capital_expenditure: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    dividends_paid: Optional[float] = None
    free_cash_flow: Optional[float] = None
    currency: Optional[str] = None


class FinancialsResponse(BaseModel):
    """Complete financial data for a company"""
    symbol: str
    company_name: Optional[str] = None
    currency: str = "SEK"
    income_statements: List[IncomeStatementItem]
    balance_sheets: List[BalanceSheetItem]
    cash_flow_statements: List[CashFlowItem]


# ============== Documents Schemas ==============

class DocumentItem(BaseModel):
    """Single document record"""
    id: str
    document_type: str
    report_period: Optional[str] = None
    title: Optional[str] = None
    publish_date: Optional[date] = None
    language: Optional[str] = None
    source_url: Optional[str] = None
    has_local_file: bool = False
    has_extracted_text: bool = False
    page_count: Optional[int] = None
    file_size_mb: Optional[float] = None


class DocumentsResponse(BaseModel):
    """Documents for a company"""
    symbol: str
    company_name: Optional[str] = None
    documents: List[DocumentItem]
    total_count: int
    downloaded_count: int
    extracted_count: int


# ============== Calendar Events Schemas ==============

class CalendarEventItem(BaseModel):
    """Single calendar event"""
    id: str
    event_type: str
    event_date: date
    event_time: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    confirmed: bool = False
    webcast_url: Optional[str] = None
    source_url: Optional[str] = None
    # Dividend-specific fields
    dividend_amount: Optional[float] = None
    dividend_currency: Optional[str] = None
    ex_dividend_date: Optional[date] = None
    payment_date: Optional[date] = None


class CalendarEventsResponse(BaseModel):
    """Calendar events for a company"""
    symbol: str
    company_name: Optional[str] = None
    events: List[CalendarEventItem]
    total_count: int
    upcoming_count: int
