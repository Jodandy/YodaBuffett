"""
Company info data source (yahoo_company_info table)
"""
from typing import Dict, Any, Optional
from datetime import date
from .base import BaseDataSource


class CompanyInfoDataSource(BaseDataSource):
    """
    Fetches company metrics from yahoo_company_info table.

    Returns:
    - Valuation metrics (P/E, P/B, PEG)
    - Profitability (ROE, ROA, margins)
    - Analyst data (recommendations, targets)
    - Dividend information
    """

    @property
    def source_name(self) -> str:
        return "company_info"

    async def fetch(
        self,
        company_id: str,
        as_of_date: Optional[date] = None,
        years_back: int = 3
    ) -> Dict[str, Any]:
        """Fetch company info metrics"""

        if as_of_date is None:
            as_of_date = date.today()

        # Get company ticker
        company = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker
            FROM company_master
            WHERE id = $1
        """, company_id)

        if not company:
            return {"error": "Company not found"}

        ticker = company['primary_ticker']

        # Get latest company info snapshot
        info = await self.db_conn.fetchrow("""
            SELECT
                fetched_date,
                market_cap,
                enterprise_value,
                beta,
                trailing_pe,
                forward_pe,
                peg_ratio,
                price_to_book,
                target_mean_price,
                target_high_price,
                target_low_price,
                recommendation_mean,
                recommendation_key,
                number_of_analysts,
                held_percent_insiders,
                held_percent_institutions,
                shares_short,
                short_ratio,
                dividend_yield,
                dividend_rate,
                ex_dividend_date,
                profit_margin,
                operating_margin,
                gross_margin,
                return_on_equity,
                return_on_assets,
                currency
            FROM yahoo_company_info
            WHERE symbol = $1
              AND fetched_date <= $2
            ORDER BY fetched_date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not info:
            return {"error": "No company info data available"}

        return {
            "company_name": company['company_name'],
            "ticker": ticker,
            "fetched_date": info['fetched_date'],
            "valuation": {
                "market_cap": info['market_cap'],
                "enterprise_value": info['enterprise_value'],
                "trailing_pe": info['trailing_pe'],
                "forward_pe": info['forward_pe'],
                "peg_ratio": info['peg_ratio'],
                "price_to_book": info['price_to_book'],
            },
            "profitability": {
                "profit_margin": info['profit_margin'],
                "operating_margin": info['operating_margin'],
                "gross_margin": info['gross_margin'],
                "return_on_equity": info['return_on_equity'],
                "return_on_assets": info['return_on_assets'],
            },
            "analyst_data": {
                "recommendation_key": info['recommendation_key'],
                "recommendation_mean": info['recommendation_mean'],
                "number_of_analysts": info['number_of_analysts'],
                "target_mean_price": info['target_mean_price'],
                "target_high_price": info['target_high_price'],
                "target_low_price": info['target_low_price'],
            },
            "ownership": {
                "held_percent_insiders": info['held_percent_insiders'],
                "held_percent_institutions": info['held_percent_institutions'],
            },
            "short_interest": {
                "shares_short": info['shares_short'],
                "short_ratio": info['short_ratio'],
            },
            "dividend": {
                "dividend_yield": info['dividend_yield'],
                "dividend_rate": info['dividend_rate'],
                "ex_dividend_date": info['ex_dividend_date'],
            },
            "currency": info['currency'],
        }

    def format_for_prompt(self, data: Dict[str, Any]) -> str:
        """Format company info for LLM prompt"""

        if "error" in data:
            return f"Company info unavailable: {data['error']}"

        lines = [
            f"# Company Metrics - {data['company_name']} ({data['ticker']})",
            f"As of: {data['fetched_date']}",
            f"Currency: {data['currency']}",
            "",
            "## Valuation",
        ]

        val = data['valuation']
        if val['market_cap']:
            lines.append(f"Market Cap: {val['market_cap']:,}")
        if val['enterprise_value']:
            lines.append(f"Enterprise Value: {val['enterprise_value']:,}")
        if val['trailing_pe']:
            lines.append(f"Trailing P/E: {val['trailing_pe']:.2f}")
        if val['forward_pe']:
            lines.append(f"Forward P/E: {val['forward_pe']:.2f}")
        if val['peg_ratio']:
            lines.append(f"PEG Ratio: {val['peg_ratio']:.2f}")
        if val['price_to_book']:
            lines.append(f"Price/Book: {val['price_to_book']:.2f}")

        lines.append("\n## Profitability")
        prof = data['profitability']
        if prof['profit_margin']:
            lines.append(f"Profit Margin: {prof['profit_margin']:.1%}")
        if prof['operating_margin']:
            lines.append(f"Operating Margin: {prof['operating_margin']:.1%}")
        if prof['gross_margin']:
            lines.append(f"Gross Margin: {prof['gross_margin']:.1%}")
        if prof['return_on_equity']:
            lines.append(f"ROE: {prof['return_on_equity']:.1%}")
        if prof['return_on_assets']:
            lines.append(f"ROA: {prof['return_on_assets']:.1%}")

        lines.append("\n## Analyst Sentiment")
        analyst = data['analyst_data']
        if analyst['recommendation_key']:
            lines.append(f"Recommendation: {analyst['recommendation_key']}")
        if analyst['number_of_analysts']:
            lines.append(f"Number of Analysts: {analyst['number_of_analysts']}")
        if analyst['target_mean_price']:
            lines.append(f"Price Target (Mean): {analyst['target_mean_price']:.2f}")

        div = data['dividend']
        if div['dividend_yield'] and div['dividend_yield'] > 0:
            lines.append("\n## Dividend")
            lines.append(f"Dividend Yield: {div['dividend_yield']:.2%}")
            if div['dividend_rate']:
                lines.append(f"Annual Dividend: {div['dividend_rate']:.2f}")

        return "\n".join(lines)
