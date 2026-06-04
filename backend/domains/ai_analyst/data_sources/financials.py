"""
Financial statements data source (yahoo_financials table)
"""
from typing import Dict, Any, Optional
from datetime import date, timedelta
from .base import BaseDataSource


class FinancialsDataSource(BaseDataSource):
    """
    Fetches financial statements from yahoo_financials table.

    Returns:
    - Annual statements (income, balance sheet, cash flow)
    - Quarterly statements
    - Key extracted metrics (revenue, net income, debt, etc.)
    """

    @property
    def source_name(self) -> str:
        return "financials"

    async def fetch(
        self,
        company_id: str,
        as_of_date: Optional[date] = None,
        years_back: int = 3
    ) -> Dict[str, Any]:
        """Fetch financial statements for a company"""

        if as_of_date is None:
            as_of_date = date.today()

        cutoff_date = as_of_date - timedelta(days=years_back * 365)

        # Get company ticker from company_master
        company = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker, yahoo_symbol
            FROM company_master
            WHERE id = $1
        """, company_id)

        if not company:
            return {"error": "Company not found"}

        ticker = company['primary_ticker']

        # Fetch annual statements
        annual = await self.db_conn.fetch("""
            SELECT
                period_date,
                statement_type,
                total_revenue,
                net_income,
                total_assets,
                total_equity,
                total_debt,
                operating_cash_flow,
                free_cash_flow,
                goodwill,
                other_intangible_assets,
                net_ppe,
                currency,
                publish_date,
                income_statement,
                balance_sheet,
                cash_flow
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'annual'
              AND period_date >= $2
              AND period_date <= $3
            ORDER BY period_date DESC
        """, ticker, cutoff_date, as_of_date)

        # Fetch quarterly statements
        quarterly = await self.db_conn.fetch("""
            SELECT
                period_date,
                fiscal_quarter,
                total_revenue,
                net_income,
                total_assets,
                total_equity,
                total_debt,
                operating_cash_flow,
                free_cash_flow,
                currency,
                publish_date
            FROM yahoo_financials
            WHERE symbol = $1
              AND statement_type = 'quarterly'
              AND period_date >= $2
              AND period_date <= $3
            ORDER BY period_date DESC
            LIMIT 8
        """, ticker, cutoff_date, as_of_date)

        # Convert to dicts and compute all metrics
        annual_list = [dict(row) for row in annual]
        quarterly_list = [dict(row) for row in quarterly]

        # Pre-compute YoY changes, trends, and metrics
        annual_analysis = self._analyze_annual_trends(annual_list)
        quarterly_analysis = self._analyze_quarterly_trends(quarterly_list)

        return {
            "company_name": company['company_name'],
            "ticker": ticker,
            "annual_statements": annual_list,
            "quarterly_statements": quarterly_list,
            "annual_analysis": annual_analysis,
            "quarterly_analysis": quarterly_analysis,
            "currency": annual[0]['currency'] if annual else None,
        }

    def _analyze_annual_trends(self, statements: list) -> Dict[str, Any]:
        """Pre-compute all YoY changes, CAGRs, and trend analysis"""
        if not statements or len(statements) < 2:
            return {"error": "Insufficient data for trend analysis"}

        latest = statements[0]
        prev_year = statements[1] if len(statements) > 1 else None

        analysis = {
            "latest_year": latest['period_date'].year if latest['period_date'] else None,
        }

        # Revenue analysis
        if latest.get('total_revenue') and prev_year and prev_year.get('total_revenue'):
            rev_current = float(latest['total_revenue'])
            rev_prev = float(prev_year['total_revenue'])
            rev_yoy_change = ((rev_current / rev_prev) - 1) * 100

            analysis['revenue'] = {
                "current": rev_current,
                "previous": rev_prev,
                "yoy_change_pct": round(rev_yoy_change, 1),
                "trend": "growth" if rev_yoy_change > 0 else "decline",
            }

            # Calculate CAGR if we have 3+ years
            if len(statements) >= 3:
                oldest = statements[-1]
                if oldest.get('total_revenue'):
                    rev_oldest = float(oldest['total_revenue'])
                    years = len(statements) - 1
                    cagr = (((rev_current / rev_oldest) ** (1 / years)) - 1) * 100
                    analysis['revenue']['cagr_pct'] = round(cagr, 1)
                    analysis['revenue']['cagr_years'] = years

        # Net Income analysis
        if latest.get('net_income') and prev_year and prev_year.get('net_income'):
            ni_current = float(latest['net_income'])
            ni_prev = float(prev_year['net_income'])

            # Handle negative values carefully
            if ni_prev != 0:
                ni_yoy_change = ((ni_current / ni_prev) - 1) * 100
                analysis['net_income'] = {
                    "current": ni_current,
                    "previous": ni_prev,
                    "yoy_change_pct": round(ni_yoy_change, 1),
                    "trend": "improved" if ni_current > ni_prev else "declined",
                }

        # Margin calculations
        if latest.get('total_revenue') and latest.get('net_income'):
            rev = float(latest['total_revenue'])
            ni = float(latest['net_income'])
            if rev > 0:
                profit_margin = (ni / rev) * 100
                analysis['profit_margin'] = {
                    "current_pct": round(profit_margin, 1),
                }

                # Compare to previous year
                if prev_year and prev_year.get('total_revenue') and prev_year.get('net_income'):
                    prev_margin = (float(prev_year['net_income']) / float(prev_year['total_revenue'])) * 100
                    margin_change = profit_margin - prev_margin
                    analysis['profit_margin']['previous_pct'] = round(prev_margin, 1)
                    analysis['profit_margin']['change_pct'] = round(margin_change, 1)
                    analysis['profit_margin']['trend'] = "expanding" if margin_change > 0 else "contracting"

        # OCF margin
        if latest.get('total_revenue') and latest.get('operating_cash_flow'):
            rev = float(latest['total_revenue'])
            ocf = float(latest['operating_cash_flow'])
            if rev > 0:
                ocf_margin = (ocf / rev) * 100
                analysis['ocf_margin'] = {
                    "current_pct": round(ocf_margin, 1),
                }

                if prev_year and prev_year.get('total_revenue') and prev_year.get('operating_cash_flow'):
                    prev_ocf_margin = (float(prev_year['operating_cash_flow']) / float(prev_year['total_revenue'])) * 100
                    margin_change = ocf_margin - prev_ocf_margin
                    analysis['ocf_margin']['previous_pct'] = round(prev_ocf_margin, 1)
                    analysis['ocf_margin']['change_pct'] = round(margin_change, 1)
                    analysis['ocf_margin']['trend'] = "improving" if margin_change > 0 else "deteriorating"

        # Balance sheet health
        if latest.get('total_debt') and latest.get('total_equity'):
            debt = float(latest['total_debt'])
            equity = float(latest['total_equity'])
            if equity > 0:
                debt_to_equity = debt / equity
                analysis['balance_sheet'] = {
                    "debt_to_equity": round(debt_to_equity, 2),
                    "financial_position": "net debt" if debt > 0 else "net cash"
                }

        # Cash flow quality (FCF vs Net Income)
        if latest.get('free_cash_flow') and latest.get('net_income'):
            fcf = float(latest['free_cash_flow'])
            ni = float(latest['net_income'])
            if ni > 0:
                fcf_conversion = (fcf / ni) * 100
                analysis['cash_flow_quality'] = {
                    "fcf_to_ni_pct": round(fcf_conversion, 1),
                    "quality": "high" if fcf_conversion > 90 else "moderate" if fcf_conversion > 70 else "low"
                }

        return analysis

    def _analyze_quarterly_trends(self, statements: list) -> Dict[str, Any]:
        """Analyze quarterly trends"""
        if not statements or len(statements) < 2:
            return {"error": "Insufficient quarterly data"}

        latest = statements[0]
        yoy_quarter = statements[4] if len(statements) > 4 else None

        analysis = {
            "latest_quarter": f"Q{latest['fiscal_quarter']} {latest['period_date'].year}" if latest.get('fiscal_quarter') else None,
        }

        # QoQ and YoY revenue growth
        if latest.get('total_revenue'):
            rev_current = float(latest['total_revenue'])
            analysis['revenue'] = {"current": rev_current}

            # YoY comparison (same quarter previous year)
            if yoy_quarter and yoy_quarter.get('total_revenue'):
                rev_yoy = float(yoy_quarter['total_revenue'])
                yoy_growth = ((rev_current / rev_yoy) - 1) * 100
                analysis['revenue']['yoy_change_pct'] = round(yoy_growth, 1)
                analysis['revenue']['trend'] = "growth" if yoy_growth > 0 else "decline"

        return analysis

    def format_for_prompt(self, data: Dict[str, Any]) -> str:
        """Format financial data for LLM prompt"""

        if "error" in data:
            return f"Financial data unavailable: {data['error']}"

        lines = [
            f"# Financial Statements - {data['company_name']} ({data['ticker']})",
            f"Currency: {data['currency']}",
            "",
            "## Annual Statements",
        ]

        for stmt in data['annual_statements']:
            lines.append(f"\n### Year: {stmt['period_date']}")
            if stmt['publish_date']:
                lines.append(f"Published: {stmt['publish_date']}")
            lines.append(f"Revenue: {stmt['total_revenue']:,}" if stmt['total_revenue'] else "Revenue: N/A")
            lines.append(f"Net Income: {stmt['net_income']:,}" if stmt['net_income'] else "Net Income: N/A")
            lines.append(f"Total Assets: {stmt['total_assets']:,}" if stmt['total_assets'] else "Total Assets: N/A")
            lines.append(f"Total Equity: {stmt['total_equity']:,}" if stmt['total_equity'] else "Total Equity: N/A")
            lines.append(f"Total Debt: {stmt['total_debt']:,}" if stmt['total_debt'] else "Total Debt: N/A")
            lines.append(f"Operating Cash Flow: {stmt['operating_cash_flow']:,}" if stmt['operating_cash_flow'] else "Operating CF: N/A")
            lines.append(f"Free Cash Flow: {stmt['free_cash_flow']:,}" if stmt['free_cash_flow'] else "Free CF: N/A")

        lines.append("\n## Recent Quarterly Results")
        for stmt in data['quarterly_statements'][:4]:  # Last 4 quarters
            lines.append(f"\n### Q{stmt['fiscal_quarter']} {stmt['period_date'].year}")
            lines.append(f"Revenue: {stmt['total_revenue']:,}" if stmt['total_revenue'] else "Revenue: N/A")
            lines.append(f"Net Income: {stmt['net_income']:,}" if stmt['net_income'] else "Net Income: N/A")

        return "\n".join(lines)
