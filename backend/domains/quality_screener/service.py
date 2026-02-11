"""
Quality Screener Service

Core business logic for the quality screener.
Standalone - no Fat Pitch dependencies.
"""

import asyncpg
import numpy as np
from datetime import date, timedelta
from typing import List, Optional, Dict, Any

from domains.dimensions.calculators.currency_utils import get_exchange_rate

from .models import (
    QualityCandidate,
    ScreenerFilters,
    ScreenerSummary,
    CategoryOptions,
    BusinessModel,
    SizeCategory,
    CashQuality,
    QualityTier,
    SIZE_THRESHOLDS,
    CASH_THRESHOLDS,
)


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


class QualityScreenerService:
    """Service for quality screening operations."""

    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    # === CATEGORY HELPERS ===

    @staticmethod
    def get_size_category(market_cap: float) -> str:
        """Categorize by market cap."""
        for category, (low, high) in SIZE_THRESHOLDS.items():
            if low <= market_cap < high:
                return category.value
        return SizeCategory.UNKNOWN.value

    @staticmethod
    def get_cash_quality(ocf_ni: Optional[float]) -> str:
        """Categorize by cash conversion quality."""
        if ocf_ni is None:
            return CashQuality.UNKNOWN.value
        if ocf_ni >= 1.0:
            return CashQuality.EXCELLENT.value
        elif ocf_ni >= 0.8:
            return CashQuality.GOOD.value
        elif ocf_ni >= 0.7:
            return CashQuality.MODERATE.value
        elif ocf_ni >= 0.5:
            return CashQuality.WEAK.value
        else:
            return CashQuality.POOR.value

    # === BUSINESS MODEL CLASSIFICATION ===

    @staticmethod
    def classify_business_model(chars: Dict) -> tuple:
        """
        Classify business model based on cash conversion and reinvestment.

        Cash Cow: High OCF/NI + High FCF yield (returning cash)
        Compounder: High OCF/NI + Low FCF + High ROIC (reinvesting productively)
        Red Flag: Low OCF/NI (earnings not backed by cash)
        """
        ocf_ni = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
        fcf_yield = chars.get('fcf_yield')
        roic = chars.get('roic')
        fcf_ni = chars.get('fcf_to_ni')

        EARNINGS_REAL_THRESHOLD = 0.70
        HIGH_FCF_YIELD = 0.05
        HIGH_ROIC = 0.15
        LOW_FCF_NI = 0.50

        if ocf_ni is None:
            return (BusinessModel.UNKNOWN.value, "Insufficient cash flow data")

        if ocf_ni < EARNINGS_REAL_THRESHOLD:
            if ocf_ni < 0.4:
                return (BusinessModel.RED_FLAG.value, f"Earnings poorly backed by cash (OCF/NI: {ocf_ni:.0%})")
            else:
                return (BusinessModel.CAUTION.value, f"Moderate cash backing (OCF/NI: {ocf_ni:.0%})")

        if fcf_yield and fcf_yield > HIGH_FCF_YIELD:
            return (BusinessModel.CASH_COW.value, f"High FCF yield ({fcf_yield:.1%}), returning cash")

        if roic and roic > HIGH_ROIC:
            if fcf_ni is not None and fcf_ni < LOW_FCF_NI:
                return (BusinessModel.COMPOUNDER.value, f"Reinvesting at high ROIC ({roic:.0%})")
            elif fcf_yield and fcf_yield < HIGH_FCF_YIELD:
                return (BusinessModel.COMPOUNDER.value, f"High ROIC ({roic:.0%}), reinvesting for growth")
            else:
                return (BusinessModel.CASH_COW.value, f"High ROIC ({roic:.0%}) and generating FCF")

        if fcf_yield and fcf_yield > 0.03:
            return (BusinessModel.CASH_COW.value, f"Moderate FCF yield ({fcf_yield:.1%})")
        elif roic and roic > 0.10:
            return (BusinessModel.COMPOUNDER.value, f"Decent ROIC ({roic:.0%}), reinvesting")
        else:
            return (BusinessModel.UNCLEAR.value, "Neither high FCF yield nor high ROIC")

    # === QUALITY TIER ASSESSMENT ===

    @staticmethod
    def assess_quality_tier(chars: Dict) -> tuple:
        """
        Assess quality tier based on financials.

        Returns (tier, tier_desc, reasons, concerns, score)
        """
        reasons = []
        concerns = []
        score = 0

        # Gross margin
        gm = chars.get('gross_margin')
        if gm:
            if gm > 0.50:
                score += 15
                reasons.append(f"High gross margin ({gm*100:.0f}%)")
            elif gm > 0.35:
                score += 10
                reasons.append(f"Decent gross margin ({gm*100:.0f}%)")
            elif gm < 0.20:
                score -= 10
                concerns.append(f"Low gross margin ({gm*100:.0f}%)")

        # Operating margin
        om = chars.get('operating_margin')
        if om:
            if om > 0.20:
                score += 15
                reasons.append(f"Strong operating margin ({om*100:.0f}%)")
            elif om > 0.10:
                score += 8
            elif om < 0.05:
                concerns.append(f"Thin operating margin ({om*100:.0f}%)")

        # Profitability consistency
        pct_prof = chars.get('pct_profitable')
        if pct_prof:
            if pct_prof >= 1.0:
                score += 15
                reasons.append("Profitable every year")
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
                reasons.append("Stable margins")
            elif margin_stab < 0.4:
                concerns.append("Volatile margins")

        # ROIC
        roic = chars.get('roic')
        if roic:
            if roic > 0.20:
                score += 15
                reasons.append(f"Excellent ROIC ({roic*100:.0f}%)")
            elif roic > 0.12:
                score += 10
                reasons.append(f"Good ROIC ({roic*100:.0f}%)")
            elif roic < 0.08:
                concerns.append(f"Low ROIC ({roic*100:.0f}%)")

        # ROE
        roe = chars.get('roe')
        if roe:
            if roe > 0.20:
                score += 10
                reasons.append(f"High ROE ({roe*100:.0f}%)")
            elif roe < 0.08:
                concerns.append(f"Low ROE ({roe*100:.0f}%)")

        # Growth
        cagr = chars.get('revenue_cagr')
        if cagr:
            if cagr > 0.15:
                score += 10
                reasons.append(f"Strong growth ({cagr*100:.0f}% CAGR)")
            elif cagr > 0.05:
                score += 5
            elif cagr < 0:
                concerns.append(f"Shrinking revenue ({cagr*100:.0f}% CAGR)")

        # Net cash
        if chars.get('net_cash'):
            score += 10
            reasons.append("Net cash position")

        # Leverage
        nd_ebitda = chars.get('net_debt_to_ebitda')
        if nd_ebitda:
            if nd_ebitda < 1:
                score += 5
            elif nd_ebitda > 3:
                score -= 15
                concerns.append(f"High leverage ({nd_ebitda:.1f}x)")
            elif nd_ebitda > 2:
                concerns.append(f"Moderate leverage ({nd_ebitda:.1f}x)")

        # Cash conversion
        ocf_ni = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
        if ocf_ni:
            if ocf_ni > 1.0:
                score += 10
                reasons.append(f"Excellent cash conversion ({ocf_ni:.0%})")
            elif ocf_ni > 0.8:
                score += 5
                reasons.append(f"Good cash conversion ({ocf_ni:.0%})")
            elif ocf_ni < 0.5:
                score -= 15
                concerns.append(f"Poor cash conversion ({ocf_ni:.0%})")
            elif ocf_ni < 0.7:
                score -= 5
                concerns.append(f"Weak cash conversion ({ocf_ni:.0%})")

        # Capital intensity
        capex_rev = chars.get('capex_to_revenue')
        if capex_rev:
            if capex_rev < 0.03:
                score += 8
                reasons.append(f"Asset-light ({capex_rev:.1%} CapEx/Rev)")
            elif capex_rev < 0.05:
                score += 4
            elif capex_rev > 0.15:
                score -= 5
                concerns.append(f"Capital intensive ({capex_rev:.1%} CapEx/Rev)")

        # Working capital
        if chars.get('negative_wc'):
            score += 8
            reasons.append("Negative working capital")

        recv_vs_rev = chars.get('receivables_vs_revenue')
        if recv_vs_rev:
            if recv_vs_rev > 0.15:
                score -= 10
                concerns.append(f"Receivables outpacing revenue (+{recv_vs_rev:.0%})")
            elif recv_vs_rev > 0.05:
                concerns.append(f"Receivables growing faster (+{recv_vs_rev:.0%})")

        # Margin trend
        gm_trend = chars.get('gross_margin_trend')
        if gm_trend:
            if gm_trend > 0.03:
                score += 5
                reasons.append(f"Margins expanding (+{gm_trend:.1%})")
            elif gm_trend < -0.03:
                score -= 5
                concerns.append(f"Margins compressing ({gm_trend:.1%})")

        # Dilution
        share_growth = chars.get('share_growth')
        if share_growth:
            if share_growth > 0.05:
                score -= 10
                concerns.append(f"Share dilution ({share_growth:.1%}/yr)")
            elif share_growth > 0.02:
                concerns.append(f"Minor dilution ({share_growth:.1%}/yr)")
            elif share_growth < -0.02:
                score += 5
                reasons.append(f"Buying back shares ({share_growth:.1%}/yr)")

        # Determine tier
        if score >= 60:
            tier = 1
            tier_desc = "Potential Compounder"
        elif score >= 40:
            tier = 2
            tier_desc = "Solid Quality"
        elif score >= 20:
            tier = 3
            tier_desc = "Decent Business"
        elif score >= 0:
            tier = 4
            tier_desc = "Mixed Signals"
        else:
            tier = 5
            tier_desc = "Not Quality"

        return tier, tier_desc, reasons, concerns, score

    # === DATA EXTRACTION ===

    async def get_business_characteristics(self, ticker: str, score_date: date) -> Optional[Dict]:
        """Extract business characteristics from financials.

        Currency handling:
        - Stock price is in stock_currency (determined by exchange: .ST=SEK, .OL=NOK, etc.)
        - Financials are in report_currency (from the currency column)
        - We convert financials to stock_currency for accurate ratio calculations
        """
        ticker_space = ticker.replace('-', ' ')
        cutoff = score_date - timedelta(days=5 * 365)

        # Get yahoo_symbol for determining stock currency
        company_info = await self.conn.fetchrow("""
            SELECT yahoo_symbol FROM company_master
            WHERE primary_ticker = $1 OR primary_ticker = $2
            LIMIT 1
        """, ticker, ticker_space)
        yahoo_symbol = company_info['yahoo_symbol'] if company_info else None
        stock_currency = get_stock_currency_from_symbol(ticker, yahoo_symbol)

        # Historical financials (include currency)
        financials = await self.conn.fetch("""
            SELECT period_date, net_income, total_revenue, operating_income,
                   gross_profit, ebitda, currency
            FROM financial_statements
            WHERE (symbol = $1 OR symbol = $2)
              AND period_date >= $3
              AND period_date <= $4
              AND statement_type = 'annual'
            ORDER BY period_date
        """, ticker, ticker_space, cutoff, score_date)

        if len(financials) < 3:
            return None

        # Balance sheet (include currency)
        balance_sheets = await self.conn.fetch("""
            SELECT period_date, total_equity, total_debt, cash_and_equivalents,
                   shares_outstanding, total_assets,
                   accounts_receivable, inventory, accounts_payable,
                   current_assets, current_liabilities, currency
            FROM balance_sheet_data
            WHERE (symbol = $1 OR symbol = $2)
              AND period_date >= $3
              AND period_date <= $4
            ORDER BY period_date
        """, ticker, ticker_space, cutoff, score_date)

        balance = balance_sheets[-1] if balance_sheets else None
        if not balance or not balance['shares_outstanding']:
            return None

        # Determine report currency and calculate fx_rate
        report_currency = financials[-1].get('currency') if financials else None
        fx_rate = 1.0
        if report_currency and stock_currency and report_currency != stock_currency:
            fx_rate = get_exchange_rate(report_currency, stock_currency) or 1.0

        # Cash flow
        cashflows = await self.conn.fetch("""
            SELECT period_date, free_cash_flow, operating_cash_flow, capital_expenditure,
                   dividends_paid, depreciation_amortization
            FROM cash_flow_data
            WHERE (symbol = $1 OR symbol = $2)
              AND period_date >= $3
              AND period_date <= $4
            ORDER BY period_date
        """, ticker, ticker_space, cutoff, score_date)

        # Price
        price = await self.conn.fetchrow("""
            SELECT close_price FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC LIMIT 1
        """, ticker, score_date)

        if not price:
            return None

        # Calculate metrics - apply fx_rate to convert financials to stock_currency
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

        # Historical margins for analysis
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

        years_profitable = sum(1 for f in financials if f['net_income'] and f['net_income'] > 0)
        pct_profitable = years_profitable / len(financials)
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

        # Debt
        net_debt = debt - cash
        net_debt_to_ebitda = net_debt / ebitda if ebitda and ebitda > 0 else None
        debt_to_equity = debt / equity if equity > 0 else None

        # Valuation
        pe = market_cap / net_income if net_income > 0 else None
        ev_ebitda = ev / ebitda if ebitda and ebitda > 0 else None
        fcf_yield = fcf / market_cap if fcf and market_cap > 0 else None

        # Cash conversion
        ocf_to_ni = ocf / net_income if ocf and net_income and net_income > 0 else None
        fcf_to_ni = fcf / net_income if fcf and net_income and net_income > 0 else None

        # Average OCF/NI
        ocf_ni_ratios = []
        for i, cf in enumerate(cashflows):
            if i < len(financials):
                ni = float(financials[i]['net_income']) if financials[i]['net_income'] else 0
                ocf_val = float(cf['operating_cash_flow']) if cf['operating_cash_flow'] else 0
                if ni > 0:
                    ocf_ni_ratios.append(ocf_val / ni)
        avg_ocf_to_ni = np.mean(ocf_ni_ratios) if ocf_ni_ratios else None

        # Capital intensity (apply fx_rate to convert to stock_currency)
        capex = None
        if cashflows:
            latest_cf = cashflows[-1]
            capex = (abs(float(latest_cf['capital_expenditure'])) * fx_rate) if latest_cf['capital_expenditure'] else None
        capex_to_revenue = capex / revenue if capex and revenue > 0 else None

        depreciation = None
        if cashflows:
            latest_cf = cashflows[-1]
            depreciation = (float(latest_cf['depreciation_amortization']) * fx_rate) if latest_cf['depreciation_amortization'] else None
        capex_to_da = capex / depreciation if capex and depreciation and depreciation > 0 else None

        # Working capital
        receivables = float(balance['accounts_receivable']) if balance['accounts_receivable'] else 0
        inventory_val = float(balance['inventory']) if balance['inventory'] else 0
        payables = float(balance['accounts_payable']) if balance['accounts_payable'] else 0
        working_capital = receivables + inventory_val - payables
        negative_wc = working_capital < 0

        # Receivables vs Revenue growth
        receivables_vs_revenue = None
        if len(balance_sheets) >= 2 and len(financials) >= 2:
            old_recv = float(balance_sheets[0]['accounts_receivable']) if balance_sheets[0]['accounts_receivable'] else 0
            old_rev = float(financials[0]['total_revenue']) if financials[0]['total_revenue'] else 0
            if old_recv > 0 and old_rev > 0 and receivables > 0 and revenue > 0:
                recv_growth = (receivables / old_recv) - 1
                rev_growth = (revenue / old_rev) - 1
                receivables_vs_revenue = recv_growth - rev_growth

        # Gross margin trend
        gross_margin_trend = None
        if len(gross_margins) >= 3:
            mid = len(gross_margins) // 2
            early_gm = np.mean(gross_margins[:mid])
            late_gm = np.mean(gross_margins[mid:])
            gross_margin_trend = late_gm - early_gm

        # Share dilution
        share_growth = None
        if len(balance_sheets) >= 2:
            old_shares = float(balance_sheets[0]['shares_outstanding']) if balance_sheets[0]['shares_outstanding'] else 0
            if old_shares > 0 and shares > 0:
                years_span = (balance_sheets[-1]['period_date'] - balance_sheets[0]['period_date']).days / 365
                if years_span > 0:
                    share_growth = ((shares / old_shares) ** (1/years_span)) - 1

        return {
            'market_cap': market_cap,
            'revenue': revenue,
            'gross_margin': gross_margin,
            'operating_margin': operating_margin,
            'net_margin': net_margin,
            'pct_profitable': pct_profitable,
            'margin_stability': 1 - min(1, margin_cv) if margin_cv else None,
            'roe': roe,
            'roa': roa,
            'roic': roic,
            'revenue_cagr': revenue_cagr,
            'net_debt_to_ebitda': net_debt_to_ebitda,
            'debt_to_equity': debt_to_equity,
            'net_cash': cash > debt,
            'pe': pe,
            'ev_ebitda': ev_ebitda,
            'fcf_yield': fcf_yield,
            'ocf_to_ni': ocf_to_ni,
            'fcf_to_ni': fcf_to_ni,
            'avg_ocf_to_ni': avg_ocf_to_ni,
            'capex_to_revenue': capex_to_revenue,
            'capex_to_da': capex_to_da,
            'negative_wc': negative_wc,
            'receivables_vs_revenue': receivables_vs_revenue,
            'gross_margin_trend': gross_margin_trend,
            'share_growth': share_growth,
        }

    # === MAIN SCREENING ===

    async def get_candidates(
        self,
        score_date: Optional[date] = None,
        filters: Optional[ScreenerFilters] = None,
    ) -> List[QualityCandidate]:
        """Get all quality candidates with optional filtering."""

        if score_date is None:
            score_date = await self.conn.fetchval("SELECT MAX(date) FROM daily_price_data")

        if filters is None:
            filters = ScreenerFilters()

        # Get all companies with recent price data
        companies = await self.conn.fetch("""
            SELECT DISTINCT cm.id::text, cm.primary_ticker, cm.company_name
            FROM company_master cm
            JOIN daily_price_data dpd ON dpd.symbol = cm.primary_ticker
            WHERE dpd.date >= ($1::date - INTERVAL '7 days')
        """, score_date)

        candidates = []

        for comp in companies:
            ticker = comp['primary_ticker']

            chars = await self.get_business_characteristics(ticker, score_date)
            if not chars:
                continue

            # Get categories
            mkt_cap = chars['market_cap']
            size_cat = self.get_size_category(mkt_cap)
            ocf_ni_raw = chars.get('avg_ocf_to_ni') or chars.get('ocf_to_ni')
            cash_qual = self.get_cash_quality(ocf_ni_raw)
            is_profitable = chars['net_margin'] and chars['net_margin'] > 0

            # Assess quality
            try:
                tier, tier_desc, reasons, concerns, score = self.assess_quality_tier(chars)
                biz_model, biz_model_reason = self.classify_business_model(chars)
            except Exception:
                tier, tier_desc, score = 5, "No Data", 0
                biz_model, biz_model_reason = BusinessModel.UNKNOWN.value, "No data"
                reasons, concerns = [], []

            # Apply filters
            if mkt_cap < filters.min_market_cap or mkt_cap > filters.max_market_cap:
                continue
            if filters.profitable_only and not is_profitable:
                continue
            if filters.tiers and tier not in filters.tiers:
                continue
            if filters.business_models and biz_model not in filters.business_models:
                continue
            if filters.size_categories and size_cat not in filters.size_categories:
                continue
            if filters.cash_qualities and cash_qual not in filters.cash_qualities:
                continue

            candidates.append(QualityCandidate(
                ticker=ticker,
                company_name=comp['company_name'],
                tier=tier,
                tier_description=tier_desc,
                business_model=biz_model,
                business_model_reason=biz_model_reason,
                size_category=size_cat,
                cash_quality=cash_qual,
                is_profitable=bool(is_profitable),
                quality_score=score,
                market_cap=mkt_cap,
                gross_margin=chars['gross_margin'],
                operating_margin=chars['operating_margin'],
                net_margin=chars['net_margin'],
                roic=chars['roic'],
                roe=chars['roe'],
                revenue_cagr=chars['revenue_cagr'],
                ocf_to_ni=chars['ocf_to_ni'],
                fcf_to_ni=chars['fcf_to_ni'],
                fcf_yield=chars['fcf_yield'],
                capex_to_revenue=chars['capex_to_revenue'],
                capex_to_da=chars['capex_to_da'],
                negative_working_capital=bool(chars['negative_wc']),
                receivables_vs_revenue=chars['receivables_vs_revenue'],
                gross_margin_trend=chars['gross_margin_trend'],
                share_dilution=chars['share_growth'],
                net_cash=bool(chars['net_cash']),
                net_debt_to_ebitda=chars['net_debt_to_ebitda'],
                pe_ratio=chars['pe'],
                ev_to_ebitda=chars['ev_ebitda'],
                reasons=reasons or [],
                concerns=concerns or [],
            ))

        # Sort by tier, then score
        candidates.sort(key=lambda x: (x.tier, -x.quality_score))

        return candidates

    async def get_summary(self, candidates: List[QualityCandidate]) -> ScreenerSummary:
        """Get summary statistics for candidates."""
        by_tier = {}
        by_model = {}
        by_size = {}
        by_cash = {}

        for c in candidates:
            by_tier[c.tier] = by_tier.get(c.tier, 0) + 1
            by_model[c.business_model] = by_model.get(c.business_model, 0) + 1
            by_size[c.size_category] = by_size.get(c.size_category, 0) + 1
            by_cash[c.cash_quality] = by_cash.get(c.cash_quality, 0) + 1

        profitable = sum(1 for c in candidates if c.is_profitable)

        return ScreenerSummary(
            total_companies=len(candidates),
            by_tier=by_tier,
            by_business_model=by_model,
            by_size=by_size,
            by_cash_quality=by_cash,
            profitable_count=profitable,
            unprofitable_count=len(candidates) - profitable,
        )

    def get_category_options(self) -> CategoryOptions:
        """Get all available filter options."""
        return CategoryOptions(
            tiers=[
                {"value": 1, "label": "Tier 1", "description": "Potential Compounder"},
                {"value": 2, "label": "Tier 2", "description": "Solid Quality"},
                {"value": 3, "label": "Tier 3", "description": "Decent Business"},
                {"value": 4, "label": "Tier 4", "description": "Mixed Signals"},
                {"value": 5, "label": "Tier 5", "description": "Not Quality"},
            ],
            business_models=[
                {"value": "Cash Cow", "label": "Cash Cow", "description": "High FCF yield, returning cash"},
                {"value": "Compounder", "label": "Compounder", "description": "Reinvesting at high ROIC"},
                {"value": "Caution", "label": "Caution", "description": "Moderate cash conversion"},
                {"value": "Red Flag", "label": "Red Flag", "description": "Earnings not backed by cash"},
                {"value": "Unclear", "label": "Unclear", "description": "Neither high FCF nor high ROIC"},
            ],
            size_categories=[
                {"value": "Micro", "label": "Micro Cap", "description": "< $25M"},
                {"value": "Small", "label": "Small Cap", "description": "$25M - $100M"},
                {"value": "Mid", "label": "Mid Cap", "description": "$100M - $1B"},
                {"value": "Large", "label": "Large Cap", "description": "$1B - $10B"},
                {"value": "Mega", "label": "Mega Cap", "description": "> $10B"},
            ],
            cash_qualities=[
                {"value": "Excellent", "label": "Excellent", "description": "OCF/NI > 100%"},
                {"value": "Good", "label": "Good", "description": "OCF/NI > 80%"},
                {"value": "Moderate", "label": "Moderate", "description": "OCF/NI > 70%"},
                {"value": "Weak", "label": "Weak", "description": "OCF/NI > 50%"},
                {"value": "Poor", "label": "Poor", "description": "OCF/NI < 50%"},
            ],
        )
