#!/usr/bin/env python3
"""
Quality Business Candidate Finder

Purpose: Surface companies that LOOK like quality businesses based on the numbers.
NOT a signal generator. A research funnel.

Output: "This looks like a cat, smells like a cat. You determine if it's a cat."

What this DOES:
  - Filter for businesses that appear quality on paper
  - Flag interesting characteristics worth investigating
  - Reduce the universe to research candidates

What this DOESN'T do:
  - Predict returns
  - Replace qualitative judgment
  - Tell you if the moat is real
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
import argparse
from datetime import date, timedelta
from uuid import UUID
from typing import Dict, Optional, List

from domains.dimensions.calculators.currency_utils import get_exchange_rate

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'


def get_stock_currency_from_symbol(ticker: str, yahoo_symbol: str = None) -> str:
    """
    Determine stock trading currency from exchange suffix.

    Swedish stocks (.ST) trade in SEK
    Norwegian stocks (.OL) trade in NOK
    Danish stocks (.CO) trade in DKK
    Finnish stocks (.HE) trade in EUR
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
    # Default to SEK for Swedish market
    return 'SEK'


# === CATEGORY DEFINITIONS ===

TIERS = {
    1: "Potential Compounder",
    2: "Solid Quality",
    3: "Decent Business",
    4: "Mixed Signals",
    5: "Not Quality",
}

BUSINESS_MODELS = {
    "Cash Cow": "High FCF yield, returning cash",
    "Compounder": "Reinvesting at high ROIC",
    "Caution": "Moderate cash conversion",
    "Red Flag": "Earnings not backed by cash",
    "Unclear": "Neither high FCF nor high ROIC",
    "Unknown": "Insufficient data",
}

SIZE_CATEGORIES = {
    "Micro": (0, 25e6),
    "Small": (25e6, 100e6),
    "Mid": (100e6, 1e9),
    "Large": (1e9, 10e9),
    "Mega": (10e9, float('inf')),
}

CASH_QUALITY = {
    "Excellent": 1.0,    # OCF/NI > 100%
    "Good": 0.8,         # OCF/NI > 80%
    "Moderate": 0.7,     # OCF/NI > 70%
    "Weak": 0.5,         # OCF/NI > 50%
    "Poor": 0,           # OCF/NI < 50%
}


def get_size_category(market_cap: float) -> str:
    """Categorize by market cap."""
    for name, (low, high) in SIZE_CATEGORIES.items():
        if low <= market_cap < high:
            return name
    return "Unknown"


def get_cash_quality(ocf_ni: float) -> str:
    """Categorize by cash conversion quality."""
    if ocf_ni is None:
        return "Unknown"
    if ocf_ni >= 1.0:
        return "Excellent"
    elif ocf_ni >= 0.8:
        return "Good"
    elif ocf_ni >= 0.7:
        return "Moderate"
    elif ocf_ni >= 0.5:
        return "Weak"
    else:
        return "Poor"


async def get_business_characteristics(conn, ticker: str, score_date: date) -> Optional[Dict]:
    """Extract business characteristics from financials.

    POINT-IN-TIME SAFE: Uses publish_date to avoid look-ahead bias.
    Only uses financial data that was actually available on the score_date.

    Currency handling:
    - Stock price is in stock_currency (determined by exchange: .ST=SEK, .OL=NOK, etc.)
    - Financials are in report_currency (from the currency column)
    - We convert financials to stock_currency for accurate ratio calculations
    """
    ticker_space = ticker.replace('-', ' ')
    cutoff = score_date - timedelta(days=5 * 365)

    # Get yahoo_symbol for determining stock currency
    company_info = await conn.fetchrow("""
        SELECT yahoo_symbol FROM company_master
        WHERE primary_ticker = $1 OR primary_ticker = $2
        LIMIT 1
    """, ticker, ticker_space)
    yahoo_symbol = company_info['yahoo_symbol'] if company_info else None
    stock_currency = get_stock_currency_from_symbol(ticker, yahoo_symbol)

    # Historical financials (include currency)
    # POINT-IN-TIME: Only use data that was published before score_date
    # If publish_date is NULL, assume 75-day lag from period_date (conservative for annual reports)
    financials = await conn.fetch("""
        SELECT period_date, net_income, total_revenue, operating_income,
               gross_profit, ebitda, currency
        FROM financial_statements
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND statement_type = 'annual'
          AND (
              (publish_date IS NOT NULL AND publish_date <= $4)
              OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $4)
          )
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    if len(financials) < 3:
        return None

    # Balance sheet - get multiple years for trend analysis
    # POINT-IN-TIME: Only use data that was published before score_date
    balance_sheets = await conn.fetch("""
        SELECT period_date, total_equity, total_debt, cash_and_equivalents,
               shares_outstanding, total_assets,
               accounts_receivable, inventory, accounts_payable,
               current_assets, current_liabilities, currency
        FROM balance_sheet_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND (
              (publish_date IS NOT NULL AND publish_date <= $4)
              OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $4)
          )
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    balance = balance_sheets[-1] if balance_sheets else None

    if not balance or not balance['shares_outstanding']:
        return None

    # Determine report currency and calculate fx_rate
    # Financial statements may be in different currency than stock trades
    report_currency = financials[-1].get('currency') if financials else None
    fx_rate = 1.0
    if report_currency and stock_currency and report_currency != stock_currency:
        fx_rate = get_exchange_rate(report_currency, stock_currency) or 1.0

    # Cash flow - get multiple years for averaging
    # POINT-IN-TIME: Only use data that was published before score_date
    cashflows = await conn.fetch("""
        SELECT period_date, free_cash_flow, operating_cash_flow, capital_expenditure,
               dividends_paid, depreciation_amortization
        FROM cash_flow_data
        WHERE (symbol = $1 OR symbol = $2)
          AND period_date >= $3
          AND (
              (publish_date IS NOT NULL AND publish_date <= $4)
              OR (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $4)
          )
        ORDER BY period_date
    """, ticker, ticker_space, cutoff, score_date)

    # Price
    price = await conn.fetchrow("""
        SELECT close_price FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC LIMIT 1
    """, ticker, score_date)

    if not price:
        return None

    # Calculate characteristics
    # Apply fx_rate to convert financials from report_currency to stock_currency
    latest = financials[-1]

    revenue = (float(latest['total_revenue']) if latest['total_revenue'] else 0) * fx_rate
    net_income = (float(latest['net_income']) if latest['net_income'] else 0) * fx_rate
    gross_profit = (float(latest['gross_profit']) if latest['gross_profit'] else 0) * fx_rate
    operating_income = (float(latest['operating_income']) if latest['operating_income'] else 0) * fx_rate
    ebitda = (float(latest['ebitda']) if latest['ebitda'] else operating_income * 1.15) * fx_rate

    equity = (float(balance['total_equity']) if balance['total_equity'] else 0) * fx_rate
    debt = (float(balance['total_debt']) if balance['total_debt'] else 0) * fx_rate
    cash = (float(balance['cash_and_equivalents']) if balance['cash_and_equivalents'] else 0) * fx_rate
    assets = (float(balance['total_assets']) if balance['total_assets'] else 0) * fx_rate
    shares = float(balance['shares_outstanding'])  # shares are currency-agnostic

    # Latest cash flow (convert to stock currency)
    fcf = None
    ocf = None
    if cashflows:
        latest_cf = cashflows[-1]
        fcf = (float(latest_cf['free_cash_flow']) * fx_rate) if latest_cf['free_cash_flow'] else None
        ocf = (float(latest_cf['operating_cash_flow']) * fx_rate) if latest_cf['operating_cash_flow'] else None

    # Price is already in stock_currency, market_cap will be in stock_currency
    price_val = float(price['close_price'])
    market_cap = price_val * shares
    ev = market_cap + debt - cash  # debt and cash already converted

    # Margins
    gross_margin = gross_profit / revenue if revenue > 0 else None
    operating_margin = operating_income / revenue if revenue > 0 else None
    net_margin = net_income / revenue if revenue > 0 else None

    # Historical margins for stability check
    net_margins = []
    gross_margins = []
    revenues = []
    for f in financials:
        if f['total_revenue'] and f['total_revenue'] > 0:
            revenues.append(float(f['total_revenue']))
            if f['net_income']:
                net_margins.append(float(f['net_income']) / float(f['total_revenue']))
            if f['gross_profit']:
                gross_margins.append(float(f['gross_profit']) / float(f['total_revenue']))

    # Profitability consistency
    years_profitable = sum(1 for f in financials if f['net_income'] and f['net_income'] > 0)
    pct_profitable = years_profitable / len(financials)

    # Margin stability (low CV = stable)
    margin_cv = np.std(net_margins) / abs(np.mean(net_margins)) if net_margins and np.mean(net_margins) != 0 else None

    # Revenue growth
    if len(revenues) >= 2 and revenues[0] > 0:
        years = len(revenues) - 1
        revenue_cagr = (revenues[-1] / revenues[0]) ** (1/years) - 1 if years > 0 else 0
    else:
        revenue_cagr = None

    # Returns
    roe = net_income / equity if equity > 0 else None
    roa = net_income / assets if assets > 0 else None
    roic = operating_income / (equity + debt - cash) if (equity + debt - cash) > 0 else None

    # Debt profile
    net_debt = debt - cash
    net_debt_to_ebitda = net_debt / ebitda if ebitda and ebitda > 0 else None
    debt_to_equity = debt / equity if equity > 0 else None

    # Valuation
    pe = market_cap / net_income if net_income > 0 else None
    ev_ebitda = ev / ebitda if ebitda and ebitda > 0 else None
    fcf_yield = fcf / market_cap if fcf and market_cap > 0 else None

    # === CASH CONVERSION METRICS ===

    # OCF / Net Income - Are earnings backed by cash? (before reinvestment decisions)
    ocf_to_ni = ocf / net_income if ocf and net_income and net_income > 0 else None

    # FCF / Net Income - What's left after reinvestment?
    fcf_to_ni = fcf / net_income if fcf and net_income and net_income > 0 else None

    # Average OCF/NI over multiple years (more reliable)
    ocf_ni_ratios = []
    for i, cf in enumerate(cashflows):
        if i < len(financials):
            ni = float(financials[i]['net_income']) if financials[i]['net_income'] else 0
            ocf_val = float(cf['operating_cash_flow']) if cf['operating_cash_flow'] else 0
            if ni > 0:
                ocf_ni_ratios.append(ocf_val / ni)

    avg_ocf_to_ni = np.mean(ocf_ni_ratios) if ocf_ni_ratios else None

    # === CAPITAL INTENSITY ===

    # CapEx / Revenue - how capital hungry?
    capex = None
    if cashflows:
        latest_cf = cashflows[-1]
        # Apply fx_rate to convert to stock_currency
        capex = (abs(float(latest_cf['capital_expenditure'])) * fx_rate) if latest_cf['capital_expenditure'] else None

    capex_to_revenue = capex / revenue if capex and revenue > 0 else None

    # CapEx / Depreciation - >1.5 means heavy reinvestment, <1 means harvesting
    depreciation = None
    if cashflows:
        latest_cf = cashflows[-1]
        # Apply fx_rate to convert to stock_currency
        depreciation = (float(latest_cf['depreciation_amortization']) * fx_rate) if latest_cf['depreciation_amortization'] else None
    capex_to_da = capex / depreciation if capex and depreciation and depreciation > 0 else None

    # === WORKING CAPITAL DYNAMICS ===

    receivables = float(balance['accounts_receivable']) if balance['accounts_receivable'] else 0
    inventory_val = float(balance['inventory']) if balance['inventory'] else 0
    payables = float(balance['accounts_payable']) if balance['accounts_payable'] else 0

    # Working capital (excluding cash)
    working_capital = receivables + inventory_val - payables

    # Negative working capital = customers pay before you pay suppliers (great!)
    negative_wc = working_capital < 0

    # Receivables vs Revenue growth (red flag if receivables growing faster)
    receivables_vs_revenue = None
    if len(balance_sheets) >= 2 and len(financials) >= 2:
        old_recv = float(balance_sheets[0]['accounts_receivable']) if balance_sheets[0]['accounts_receivable'] else 0
        old_rev = float(financials[0]['total_revenue']) if financials[0]['total_revenue'] else 0

        if old_recv > 0 and old_rev > 0 and receivables > 0 and revenue > 0:
            recv_growth = (receivables / old_recv) - 1
            rev_growth = (revenue / old_rev) - 1
            receivables_vs_revenue = recv_growth - rev_growth  # Positive = red flag

    # === MARGIN TRAJECTORY ===

    # Is gross margin expanding or compressing?
    gross_margin_trend = None
    if len(gross_margins) >= 3:
        # Compare first half avg to second half avg
        mid = len(gross_margins) // 2
        early_gm = np.mean(gross_margins[:mid])
        late_gm = np.mean(gross_margins[mid:])
        gross_margin_trend = late_gm - early_gm  # Positive = expanding

    # === SHAREHOLDER DILUTION ===

    # Share count growth
    share_growth = None
    if len(balance_sheets) >= 2:
        old_shares = float(balance_sheets[0]['shares_outstanding']) if balance_sheets[0]['shares_outstanding'] else 0
        if old_shares > 0 and shares > 0:
            years_span = (balance_sheets[-1]['period_date'] - balance_sheets[0]['period_date']).days / 365
            if years_span > 0:
                share_growth = ((shares / old_shares) ** (1/years_span)) - 1

    # Total shareholder yield (dividends as % of market cap - buybacks not available)
    total_yield = None
    if cashflows and market_cap > 0:
        latest_cf = cashflows[-1]
        # Apply fx_rate to convert dividends to stock_currency
        divs = (abs(float(latest_cf['dividends_paid'])) * fx_rate) if latest_cf['dividends_paid'] else 0
        total_yield = divs / market_cap if divs > 0 else None

    # Note: goodwill not available in our data, skip that metric
    goodwill_to_assets = None

    return {
        # Scale
        'market_cap': market_cap,
        'revenue': revenue,

        # Profitability
        'gross_margin': gross_margin,
        'operating_margin': operating_margin,
        'net_margin': net_margin,
        'pct_profitable': pct_profitable,
        'margin_stability': 1 - min(1, margin_cv) if margin_cv else None,  # Higher = more stable

        # Returns
        'roe': roe,
        'roa': roa,
        'roic': roic,

        # Growth
        'revenue_cagr': revenue_cagr,

        # Financial strength
        'net_debt_to_ebitda': net_debt_to_ebitda,
        'debt_to_equity': debt_to_equity,
        'net_cash': cash > debt,

        # Valuation
        'pe': pe,
        'ev_ebitda': ev_ebitda,
        'fcf_yield': fcf_yield,

        # Cash conversion
        'ocf_to_ni': ocf_to_ni,
        'fcf_to_ni': fcf_to_ni,
        'avg_ocf_to_ni': avg_ocf_to_ni,

        # Capital intensity (NEW)
        'capex_to_revenue': capex_to_revenue,    # Lower = less capital hungry
        'capex_to_da': capex_to_da,              # >1.5 = heavy reinvestment, <1 = harvesting

        # Working capital (NEW)
        'negative_wc': negative_wc,              # True = great (Amazon model)
        'receivables_vs_revenue': receivables_vs_revenue,  # Positive = red flag

        # Margin trajectory (NEW)
        'gross_margin_trend': gross_margin_trend,  # Positive = expanding

        # Shareholder treatment (NEW)
        'share_growth': share_growth,            # Negative = buybacks, positive = dilution
        'total_yield': total_yield,              # Dividends + buybacks as % of mkt cap

        # Balance sheet quality (NEW)
        'goodwill_to_assets': goodwill_to_assets,  # High = acquisition heavy

        # Data quality
        'years_of_data': len(financials),
    }


def classify_business_model(chars: Dict) -> tuple:
    """
    Classify business model based on cash conversion and reinvestment.

    Returns (model_type, explanation)

    Cash Cow: High OCF/NI + High FCF yield (returning cash)
    Compounder: High OCF/NI + Low FCF + High ROIC (reinvesting productively)
    Red Flag: Low OCF/NI (earnings not backed by cash)
    """
    ocf_ni = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
    fcf_yield = chars.get('fcf_yield')
    roic = chars.get('roic')
    fcf_ni = chars.get('fcf_to_ni')

    # Thresholds
    EARNINGS_REAL_THRESHOLD = 0.70      # OCF/NI > 70% = earnings are real
    HIGH_FCF_YIELD = 0.05               # >5% FCF yield = returning cash
    HIGH_ROIC = 0.15                    # >15% ROIC = productive reinvestment
    LOW_FCF_NI = 0.50                   # FCF/NI < 50% = reinvesting heavily

    # First check: Are earnings real?
    if ocf_ni is None:
        return ("Unknown", "Insufficient cash flow data")

    if ocf_ni < EARNINGS_REAL_THRESHOLD:
        # Earnings not backed by cash
        if ocf_ni < 0.4:
            return ("Red Flag", f"Earnings poorly backed by cash (OCF/NI: {ocf_ni:.0%})")
        else:
            return ("Caution", f"Moderate cash backing (OCF/NI: {ocf_ni:.0%})")

    # Earnings are real - now check the model
    # Is it a Cash Cow?
    if fcf_yield and fcf_yield > HIGH_FCF_YIELD:
        return ("Cash Cow", f"High FCF yield ({fcf_yield:.1%}), returning cash to shareholders")

    # Is it a Compounder?
    if roic and roic > HIGH_ROIC:
        if fcf_ni is not None and fcf_ni < LOW_FCF_NI:
            return ("Compounder", f"Reinvesting at high ROIC ({roic:.0%}), FCF/NI: {fcf_ni:.0%}")
        elif fcf_yield and fcf_yield < HIGH_FCF_YIELD:
            return ("Compounder", f"High ROIC ({roic:.0%}), reinvesting for growth")
        else:
            return ("Cash Cow", f"High ROIC ({roic:.0%}) and generating FCF")

    # Middle ground - decent but not clearly one or the other
    if fcf_yield and fcf_yield > 0.03:
        return ("Cash Cow", f"Moderate FCF yield ({fcf_yield:.1%})")
    elif roic and roic > 0.10:
        return ("Compounder", f"Decent ROIC ({roic:.0%}), appears to be reinvesting")
    else:
        return ("Unclear", f"Neither high FCF yield nor high ROIC")


def assess_quality_tier(chars: Dict) -> tuple:
    """
    Assess what tier of business this APPEARS to be based on numbers.

    Returns (tier, reasons, concerns)

    Tier 1: Looks like a potential compounder
    Tier 2: Looks like a solid quality business
    Tier 3: Looks like a decent business
    Tier 4: Mixed signals
    Tier 5: Doesn't look like a quality business
    """
    reasons = []
    concerns = []
    score = 0

    # === PROFITABILITY ===

    # Gross margin (pricing power signal)
    gm = chars.get('gross_margin')
    if gm:
        if gm > 0.50:
            score += 15
            reasons.append(f"High gross margin ({gm*100:.0f}%) suggests pricing power")
        elif gm > 0.35:
            score += 10
            reasons.append(f"Decent gross margin ({gm*100:.0f}%)")
        elif gm < 0.20:
            score -= 10
            concerns.append(f"Low gross margin ({gm*100:.0f}%) - commodity business?")

    # Operating margin (efficiency)
    om = chars.get('operating_margin')
    if om:
        if om > 0.20:
            score += 15
            reasons.append(f"Strong operating margin ({om*100:.0f}%)")
        elif om > 0.10:
            score += 8
        elif om < 0.05:
            concerns.append(f"Thin operating margin ({om*100:.0f}%)")

    # Consistency
    pct_prof = chars.get('pct_profitable')
    if pct_prof:
        if pct_prof >= 1.0:
            score += 15
            reasons.append("Profitable every year in dataset")
        elif pct_prof >= 0.8:
            score += 8
        else:
            score -= 10
            concerns.append(f"Only profitable {pct_prof*100:.0f}% of years")

    # Margin stability
    margin_stab = chars.get('margin_stability')
    if margin_stab:
        if margin_stab > 0.7:
            score += 10
            reasons.append("Stable margins over time")
        elif margin_stab < 0.4:
            concerns.append("Volatile margins")

    # === RETURNS ON CAPITAL ===

    roic = chars.get('roic')
    if roic:
        if roic > 0.20:
            score += 15
            reasons.append(f"Excellent ROIC ({roic*100:.0f}%) - capital efficient")
        elif roic > 0.12:
            score += 10
            reasons.append(f"Good ROIC ({roic*100:.0f}%)")
        elif roic < 0.08:
            concerns.append(f"Low ROIC ({roic*100:.0f}%)")

    roe = chars.get('roe')
    if roe:
        if roe > 0.20:
            score += 10
            reasons.append(f"High ROE ({roe*100:.0f}%)")
        elif roe < 0.08:
            concerns.append(f"Low ROE ({roe*100:.0f}%)")

    # === GROWTH ===

    cagr = chars.get('revenue_cagr')
    if cagr:
        if cagr > 0.15:
            score += 10
            reasons.append(f"Strong growth ({cagr*100:.0f}% CAGR)")
        elif cagr > 0.05:
            score += 5
        elif cagr < 0:
            concerns.append(f"Shrinking revenue ({cagr*100:.0f}% CAGR)")

    # === FINANCIAL STRENGTH ===

    if chars.get('net_cash'):
        score += 10
        reasons.append("Net cash position (more cash than debt)")

    nd_ebitda = chars.get('net_debt_to_ebitda')
    if nd_ebitda:
        if nd_ebitda < 1:
            score += 5
        elif nd_ebitda > 3:
            score -= 15
            concerns.append(f"High leverage ({nd_ebitda:.1f}x Net Debt/EBITDA)")
        elif nd_ebitda > 2:
            concerns.append(f"Moderate leverage ({nd_ebitda:.1f}x)")

    # === CASH CONVERSION ===

    ocf_ni = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
    if ocf_ni:
        if ocf_ni > 1.0:
            score += 10
            reasons.append(f"Excellent cash conversion (OCF/NI: {ocf_ni:.0%})")
        elif ocf_ni > 0.8:
            score += 5
            reasons.append(f"Good cash conversion (OCF/NI: {ocf_ni:.0%})")
        elif ocf_ni < 0.5:
            score -= 15
            concerns.append(f"Poor cash conversion (OCF/NI: {ocf_ni:.0%}) - earnings may not be real")
        elif ocf_ni < 0.7:
            score -= 5
            concerns.append(f"Weak cash conversion (OCF/NI: {ocf_ni:.0%})")

    # === CAPITAL INTENSITY ===

    capex_rev = chars.get('capex_to_revenue')
    if capex_rev:
        if capex_rev < 0.03:
            score += 8
            reasons.append(f"Asset-light business (CapEx/Rev: {capex_rev:.1%})")
        elif capex_rev < 0.05:
            score += 4
        elif capex_rev > 0.15:
            score -= 5
            concerns.append(f"Capital intensive (CapEx/Rev: {capex_rev:.1%})")

    capex_da = chars.get('capex_to_da')
    if capex_da:
        if capex_da > 2.0:
            concerns.append(f"Heavy reinvestment needed (CapEx/D&A: {capex_da:.1f}x)")
        elif capex_da < 0.8:
            concerns.append(f"Under-investing? (CapEx/D&A: {capex_da:.1f}x)")

    # === WORKING CAPITAL ===

    if chars.get('negative_wc'):
        score += 8
        reasons.append("Negative working capital (customers pay before you pay suppliers)")

    recv_vs_rev = chars.get('receivables_vs_revenue')
    if recv_vs_rev:
        if recv_vs_rev > 0.15:
            score -= 10
            concerns.append(f"Receivables growing faster than revenue (+{recv_vs_rev:.0%}) - potential stuffing")
        elif recv_vs_rev > 0.05:
            concerns.append(f"Receivables outpacing revenue (+{recv_vs_rev:.0%})")

    # === MARGIN TRAJECTORY ===

    gm_trend = chars.get('gross_margin_trend')
    if gm_trend:
        if gm_trend > 0.03:
            score += 5
            reasons.append(f"Gross margins expanding (+{gm_trend:.1%})")
        elif gm_trend < -0.03:
            score -= 5
            concerns.append(f"Gross margins compressing ({gm_trend:.1%})")

    # === SHAREHOLDER DILUTION ===

    share_growth = chars.get('share_growth')
    if share_growth:
        if share_growth > 0.05:
            score -= 10
            concerns.append(f"Significant share dilution ({share_growth:.1%}/yr)")
        elif share_growth > 0.02:
            concerns.append(f"Share dilution ({share_growth:.1%}/yr)")
        elif share_growth < -0.02:
            score += 5
            reasons.append(f"Buying back shares ({share_growth:.1%}/yr)")

    total_yield = chars.get('total_yield')
    if total_yield and total_yield > 0.05:
        score += 5
        reasons.append(f"High shareholder yield ({total_yield:.1%})")

    # === BALANCE SHEET QUALITY ===
    # Note: goodwill data not available in current dataset

    # === VALUATION (not for scoring, just noting) ===

    pe = chars.get('pe')
    ev_ebitda = chars.get('ev_ebitda')
    fcf_yield = chars.get('fcf_yield')

    valuation_notes = []
    if pe:
        if pe < 12:
            valuation_notes.append(f"Low P/E ({pe:.0f})")
        elif pe > 30:
            valuation_notes.append(f"High P/E ({pe:.0f})")

    if fcf_yield:
        if fcf_yield > 0.08:
            valuation_notes.append(f"High FCF yield ({fcf_yield*100:.1f}%)")

    # === DETERMINE TIER ===

    if score >= 60:
        tier = 1
        tier_desc = "Looks like a potential compounder"
    elif score >= 40:
        tier = 2
        tier_desc = "Looks like a solid quality business"
    elif score >= 20:
        tier = 3
        tier_desc = "Looks like a decent business"
    elif score >= 0:
        tier = 4
        tier_desc = "Mixed signals"
    else:
        tier = 5
        tier_desc = "Doesn't look like a quality business"

    return tier, tier_desc, reasons, concerns, valuation_notes, score


async def find_quality_candidates(
    conn,
    score_date: date = None,
    min_market_cap: float = 0,
    max_market_cap: float = float('inf'),
    tiers: List[int] = None,
    business_models: List[str] = None,
    size_categories: List[str] = None,
    cash_qualities: List[str] = None,
    profitable_only: bool = False,
    include_all: bool = True,
):
    """Find quality business candidates with optional filtering.

    Args:
        score_date: Date to evaluate (default: latest)
        min_market_cap: Minimum market cap filter
        max_market_cap: Maximum market cap filter
        tiers: Filter to specific tiers [1,2,3,4,5]
        business_models: Filter to specific models ['Cash Cow', 'Compounder', etc.]
        size_categories: Filter to sizes ['Micro', 'Small', 'Mid', 'Large', 'Mega']
        cash_qualities: Filter to cash quality ['Excellent', 'Good', 'Moderate', 'Weak', 'Poor']
        profitable_only: Only include profitable companies
        include_all: Include all companies (True) or apply original filters (False)
    """

    if score_date is None:
        score_date = await conn.fetchval("""
            SELECT MAX(date) FROM daily_price_data
        """)

    print("="*90)
    print("QUALITY BUSINESS CANDIDATE FINDER")
    print("="*90)
    print()
    print("Purpose: Surface companies that LOOK like quality businesses.")
    print("         You determine if the moat is real.")
    print()
    print(f"Date: {score_date}")
    print(f"Min market cap: ${min_market_cap/1e6:.0f}M")
    print()

    # Get all companies
    companies = await conn.fetch("""
        SELECT DISTINCT cm.id::text, cm.primary_ticker, cm.company_name
        FROM company_master cm
        JOIN daily_price_data dpd ON dpd.symbol = cm.primary_ticker
        WHERE dpd.date >= ($1::date - INTERVAL '7 days')
    """, score_date)

    print(f"Analyzing {len(companies)} companies...\n")

    candidates = []

    for i, comp in enumerate(companies):
        if i % 200 == 0:
            print(f"  {i}/{len(companies)}")

        ticker = comp['primary_ticker']

        chars = await get_business_characteristics(conn, ticker, score_date)
        if not chars:
            continue

        # Get categories
        mkt_cap = chars['market_cap']
        size_cat = get_size_category(mkt_cap)
        ocf_ni_raw = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
        cash_qual = get_cash_quality(ocf_ni_raw)
        is_profitable = chars['net_margin'] and chars['net_margin'] > 0

        # Assess quality
        try:
            tier, tier_desc, reasons, concerns, val_notes, score = assess_quality_tier(chars)
            biz_model, biz_model_reason = classify_business_model(chars)
        except Exception:
            tier, tier_desc, score = 5, "No Data", 0
            biz_model, biz_model_reason = "Unknown", "No data available"
            reasons, concerns, val_notes = [], [], []

        # === APPLY FILTERS ===

        # Market cap range
        if mkt_cap < min_market_cap or mkt_cap > max_market_cap:
            continue

        # Profitable filter
        if profitable_only and not is_profitable:
            continue

        # Tier filter
        if tiers and tier not in tiers:
            continue

        # Business model filter
        if business_models and biz_model not in business_models:
            continue

        # Size category filter
        if size_categories and size_cat not in size_categories:
            continue

        # Cash quality filter
        if cash_qualities and cash_qual not in cash_qualities:
            continue

        candidates.append({
            'ticker': ticker,
            'company': comp['company_name'],

            # === CATEGORIES (for filtering) ===
            'tier': tier,
            'tier_desc': tier_desc,
            'biz_model': biz_model,
            'size_category': size_cat,
            'cash_quality': cash_qual,
            'is_profitable': is_profitable,

            'quality_score': score,
            'biz_model_reason': biz_model_reason,

            # Key metrics
            'mkt_cap_m': round(mkt_cap / 1e6, 0),
            'gross_margin': round(chars['gross_margin'] * 100, 1) if chars['gross_margin'] else None,
            'op_margin': round(chars['operating_margin'] * 100, 1) if chars['operating_margin'] else None,
            'net_margin': round(chars['net_margin'] * 100, 1) if chars['net_margin'] else None,
            'roic': round(chars['roic'] * 100, 1) if chars['roic'] else None,
            'roe': round(chars['roe'] * 100, 1) if chars['roe'] else None,
            'cagr': round(chars['revenue_cagr'] * 100, 1) if chars['revenue_cagr'] else None,

            # Cash conversion
            'ocf_ni': round(chars['ocf_to_ni'] * 100, 0) if chars['ocf_to_ni'] else None,
            'fcf_ni': round(chars['fcf_to_ni'] * 100, 0) if chars['fcf_to_ni'] else None,

            # Capital intensity
            'capex_rev': round(chars['capex_to_revenue'] * 100, 1) if chars['capex_to_revenue'] else None,
            'capex_da': round(chars['capex_to_da'], 1) if chars['capex_to_da'] else None,

            # Working capital
            'negative_wc': chars['negative_wc'],
            'recv_vs_rev': round(chars['receivables_vs_revenue'] * 100, 0) if chars['receivables_vs_revenue'] else None,

            # Margin trajectory
            'gm_trend': round(chars['gross_margin_trend'] * 100, 1) if chars['gross_margin_trend'] else None,

            # Shareholder treatment
            'share_dilution': round(chars['share_growth'] * 100, 1) if chars['share_growth'] else None,
            'total_yield': round(chars['total_yield'] * 100, 1) if chars['total_yield'] else None,

            # Balance sheet
            'goodwill_pct': round(chars['goodwill_to_assets'] * 100, 0) if chars['goodwill_to_assets'] else None,

            # Financial strength
            'net_cash': chars['net_cash'],
            'nd_ebitda': round(chars['net_debt_to_ebitda'], 1) if chars['net_debt_to_ebitda'] else None,

            # Valuation
            'pe': round(chars['pe'], 1) if chars['pe'] else None,
            'ev_ebitda': round(chars['ev_ebitda'], 1) if chars['ev_ebitda'] else None,
            'fcf_yield': round(chars['fcf_yield'] * 100, 1) if chars['fcf_yield'] else None,

            # Qualitative flags
            'reasons': reasons,
            'concerns': concerns,
            'valuation_notes': val_notes,
        })

    if not candidates:
        print("No candidates found!")
        return pd.DataFrame()

    df = pd.DataFrame(candidates)
    df = df.sort_values(['tier', 'quality_score'], ascending=[True, False])

    # === DISPLAY BY TIER ===

    for tier in [1, 2, 3]:
        tier_df = df[df['tier'] == tier]
        if len(tier_df) == 0:
            continue

        print()
        print("="*110)
        tier_desc = tier_df.iloc[0]['tier_desc']
        print(f"TIER {tier}: {tier_desc.upper()} ({len(tier_df)} candidates)")
        print("="*110)
        print()
        print(f"{'Ticker':<10} {'Company':<20} {'Model':<12} {'MCap':>8} {'ROIC':>6} {'OCF/NI':>7} {'FCF%':>6} {'P/E':>6}")
        print("-"*95)

        for _, row in tier_df.head(25).iterrows():
            roic = f"{row['roic']:.0f}%" if row['roic'] else "-"
            ocf_ni = f"{row['ocf_ni']:.0f}%" if row['ocf_ni'] else "-"
            fcf_yld = f"{row['fcf_yield']:.1f}%" if row['fcf_yield'] else "-"
            pe = f"{row['pe']:.0f}" if row['pe'] else "-"
            model = row['biz_model'][:11] if row['biz_model'] else "-"

            print(f"{row['ticker']:<10} {row['company'][:19]:<20} {model:<12} {row['mkt_cap_m']:>7.0f}M {roic:>6} {ocf_ni:>7} {fcf_yld:>6} {pe:>6}")

        # Show detailed view of top 5
        print()
        print(f"--- TOP {min(5, len(tier_df))} TIER {tier} DETAILS ---")
        print()

        for _, row in tier_df.head(5).iterrows():
            print(f"{row['ticker']} - {row['company']}")
            print(f"  Market Cap: ${row['mkt_cap_m']:.0f}M | P/E: {row['pe']} | EV/EBITDA: {row['ev_ebitda']}")
            print(f"  Business Model: {row['biz_model']} - {row['biz_model_reason']}")

            if row['reasons']:
                print(f"  Why it looks quality:")
                for r in row['reasons'][:4]:
                    print(f"    + {r}")

            if row['concerns']:
                print(f"  Concerns to investigate:")
                for c in row['concerns'][:3]:
                    print(f"    - {c}")

            if row['valuation_notes']:
                print(f"  Valuation: {', '.join(row['valuation_notes'])}")

            print(f"  >>> YOUR JOB: Is the moat real? Why do customers stay?")
            print()

    # === SUMMARY ===

    print()
    print("="*90)
    print("SUMMARY")
    print("="*90)
    print()
    print("Tier distribution:")
    for tier in [1, 2, 3, 4, 5]:
        count = len(df[df['tier'] == tier])
        pct = count / len(df) * 100
        desc = df[df['tier'] == tier].iloc[0]['tier_desc'] if count > 0 else ""
        print(f"  Tier {tier}: {count:>4} ({pct:>5.1f}%) - {desc}")

    print()
    print("Business Model distribution:")
    for model in ['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear', 'Unknown']:
        count = len(df[df['biz_model'] == model])
        if count > 0:
            pct = count / len(df) * 100
            print(f"  {model:<12}: {count:>4} ({pct:>5.1f}%)")

    print()
    print("KEY:")
    print("  Cash Cow   = High FCF yield, returning cash to shareholders")
    print("  Compounder = Low FCF but reinvesting at high ROIC")
    print("  Caution    = Moderate cash conversion, needs investigation")
    print("  Red Flag   = Earnings not backed by cash - be careful!")

    print()
    print("REMEMBER:")
    print("  - These are CANDIDATES based on numbers")
    print("  - Numbers are backward-looking")
    print("  - The moat assessment requires YOUR judgment")
    print("  - High margins don't guarantee durability")
    print("  - This is a research funnel, not a buy list")

    # Export
    output_file = 'quality_candidates.xlsx'
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet - all key metrics with categories
        export_cols = ['ticker', 'company', 'tier', 'biz_model', 'size_category', 'cash_quality',
                       'is_profitable', 'quality_score',
                       'mkt_cap_m', 'gross_margin', 'op_margin', 'roic', 'cagr',
                       'ocf_ni', 'fcf_yield',
                       'capex_rev', 'negative_wc', 'recv_vs_rev',
                       'gm_trend', 'share_dilution', 'goodwill_pct',
                       'net_cash', 'nd_ebitda', 'pe', 'ev_ebitda']
        df_summary = df[[c for c in export_cols if c in df.columns]]
        df_summary.to_excel(writer, sheet_name='All Candidates', index=False)

        # By tier
        for tier in [1, 2, 3, 4, 5]:
            tier_df = df[df['tier'] == tier]
            if len(tier_df) > 0:
                tier_df[[c for c in export_cols if c in tier_df.columns]].to_excel(
                    writer, sheet_name=f'Tier {tier}', index=False)

        # By business model
        for model in ['Cash Cow', 'Compounder', 'Red Flag']:
            model_df = df[df['biz_model'] == model]
            if len(model_df) > 0:
                model_df[[c for c in export_cols if c in model_df.columns]].to_excel(
                    writer, sheet_name=model[:31], index=False)

        # By size category
        for size in ['Micro', 'Small', 'Mid', 'Large', 'Mega']:
            size_df = df[df['size_category'] == size]
            if len(size_df) > 0:
                size_df[[c for c in export_cols if c in size_df.columns]].to_excel(
                    writer, sheet_name=f'{size} Cap', index=False)

        # By cash quality
        for qual in ['Excellent', 'Good', 'Poor']:
            qual_df = df[df['cash_quality'] == qual]
            if len(qual_df) > 0:
                qual_df[[c for c in export_cols if c in qual_df.columns]].to_excel(
                    writer, sheet_name=f'Cash {qual}', index=False)

        # Best combos
        combo1 = df[(df['tier'] <= 2) & (df['biz_model'] == 'Cash Cow')]
        if len(combo1) > 0:
            combo1[[c for c in export_cols if c in combo1.columns]].to_excel(
                writer, sheet_name='T1-2 Cash Cow', index=False)

        combo2 = df[(df['tier'] <= 2) & (df['biz_model'] == 'Compounder')]
        if len(combo2) > 0:
            combo2[[c for c in export_cols if c in combo2.columns]].to_excel(
                writer, sheet_name='T1-2 Compounder', index=False)

        # Red flags to avoid
        red_flags = df[df['biz_model'].isin(['Red Flag', 'Caution'])]
        if len(red_flags) > 0:
            red_flags[[c for c in export_cols if c in red_flags.columns]].to_excel(
                writer, sheet_name='Caution - Check Cash', index=False)

    print(f"\nExported to {output_file}")

    return df


def print_available_categories():
    """Print all available filter categories."""
    print("\n" + "="*70)
    print("AVAILABLE FILTER CATEGORIES")
    print("="*70)

    print("\n--tier / -t (1-5):")
    for tier, desc in TIERS.items():
        print(f"  {tier}: {desc}")

    print("\n--model / -m:")
    for model, desc in BUSINESS_MODELS.items():
        print(f"  {model:<12} - {desc}")

    print("\n--size / -s:")
    for name, (low, high) in SIZE_CATEGORIES.items():
        if high == float('inf'):
            high_str = "+"
        elif high >= 1e9:
            high_str = f"${high/1e9:.0f}B"
        else:
            high_str = f"${high/1e6:.0f}M"

        if low >= 1e9:
            low_str = f"${low/1e9:.0f}B"
        elif low > 0:
            low_str = f"${low/1e6:.0f}M"
        else:
            low_str = "$0"
        print(f"  {name:<6} - {low_str} to {high_str}")

    print("\n--cash / -c (OCF/NI quality):")
    for qual, threshold in CASH_QUALITY.items():
        print(f"  {qual:<10} - OCF/NI >= {threshold:.0%}")

    print("\n--profitable / -p: Only profitable companies")
    print("--min-cap: Minimum market cap (e.g., 100M, 1B)")
    print("--max-cap: Maximum market cap (e.g., 500M, 10B)")

    print("\nEXAMPLES:")
    print("  # Tier 1-2 Cash Cows with excellent cash conversion")
    print("  python find_quality_candidates.py -t 1 2 -m 'Cash Cow' -c Excellent Good")
    print()
    print("  # All compounders in mid/large cap")
    print("  python find_quality_candidates.py -m Compounder -s Mid Large")
    print()
    print("  # Small cap red flags (potential shorts)")
    print("  python find_quality_candidates.py -m 'Red Flag' -s Small Micro")
    print()
    print("  # High quality profitable companies under $500M")
    print("  python find_quality_candidates.py -t 1 2 -p --max-cap 500M")
    print()


def parse_market_cap(value: str) -> float:
    """Parse market cap string like '100M' or '1B' to float."""
    if value is None:
        return None
    value = value.upper().strip()
    if value.endswith('B'):
        return float(value[:-1]) * 1e9
    elif value.endswith('M'):
        return float(value[:-1]) * 1e6
    elif value.endswith('K'):
        return float(value[:-1]) * 1e3
    else:
        return float(value)


async def main():
    parser = argparse.ArgumentParser(
        description='Quality Business Candidate Finder with Filtering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python find_quality_candidates.py --categories           # Show all filter options
  python find_quality_candidates.py -t 1 2                 # Tier 1-2 only
  python find_quality_candidates.py -m "Cash Cow"          # Cash Cows only
  python find_quality_candidates.py -m Compounder -t 1     # Tier 1 Compounders
  python find_quality_candidates.py -s Mid Large -p        # Mid/Large profitable
  python find_quality_candidates.py -c Excellent Good      # Excellent/Good cash quality
  python find_quality_candidates.py --min-cap 100M         # Market cap > $100M
        """
    )

    parser.add_argument('--categories', action='store_true',
                        help='Show all available filter categories')

    parser.add_argument('-t', '--tier', nargs='+', type=int, choices=[1,2,3,4,5],
                        help='Filter by tier (1-5)')

    parser.add_argument('-m', '--model', nargs='+',
                        choices=['Cash Cow', 'Compounder', 'Caution', 'Red Flag', 'Unclear', 'Unknown'],
                        help='Filter by business model')

    parser.add_argument('-s', '--size', nargs='+',
                        choices=['Micro', 'Small', 'Mid', 'Large', 'Mega'],
                        help='Filter by size category')

    parser.add_argument('-c', '--cash', nargs='+',
                        choices=['Excellent', 'Good', 'Moderate', 'Weak', 'Poor', 'Unknown'],
                        help='Filter by cash conversion quality')

    parser.add_argument('-p', '--profitable', action='store_true',
                        help='Only profitable companies')

    parser.add_argument('--min-cap', type=str, default=None,
                        help='Minimum market cap (e.g., 100M, 1B)')

    parser.add_argument('--max-cap', type=str, default=None,
                        help='Maximum market cap (e.g., 500M, 10B)')

    args = parser.parse_args()

    if args.categories:
        print_available_categories()
        return

    # Parse market cap filters
    min_cap = parse_market_cap(args.min_cap) if args.min_cap else 0
    max_cap = parse_market_cap(args.max_cap) if args.max_cap else float('inf')

    # Print active filters
    print("\n" + "="*70)
    print("ACTIVE FILTERS")
    print("="*70)
    if args.tier:
        print(f"  Tiers: {args.tier}")
    if args.model:
        print(f"  Business Models: {args.model}")
    if args.size:
        print(f"  Size Categories: {args.size}")
    if args.cash:
        print(f"  Cash Quality: {args.cash}")
    if args.profitable:
        print(f"  Profitable Only: Yes")
    if args.min_cap:
        print(f"  Min Market Cap: ${min_cap/1e6:.0f}M")
    if args.max_cap:
        print(f"  Max Market Cap: ${max_cap/1e6:.0f}M")
    if not any([args.tier, args.model, args.size, args.cash, args.profitable, args.min_cap, args.max_cap]):
        print("  (No filters - showing all)")
    print()

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await find_quality_candidates(
            conn,
            min_market_cap=min_cap,
            max_market_cap=max_cap,
            tiers=args.tier,
            business_models=args.model,
            size_categories=args.size,
            cash_qualities=args.cash,
            profitable_only=args.profitable,
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
