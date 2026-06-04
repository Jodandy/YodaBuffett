"""
Price data source (daily_price_data table)
"""
from typing import Dict, Any, Optional
from datetime import date, timedelta
from decimal import Decimal
from .base import BaseDataSource


class PricesDataSource(BaseDataSource):
    """
    Fetches price data from daily_price_data table.

    Returns:
    - Current price
    - Historical prices
    - Simple returns (1mo, 3mo, 6mo, 1yr)
    - 52-week high/low
    """

    @property
    def source_name(self) -> str:
        return "prices"

    async def fetch(
        self,
        company_id: str,
        as_of_date: Optional[date] = None,
        years_back: int = 3
    ) -> Dict[str, Any]:
        """Fetch price data for a company"""

        if as_of_date is None:
            as_of_date = date.today()

        cutoff_date = as_of_date - timedelta(days=years_back * 365)

        # Get company ticker
        company = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker
            FROM company_master
            WHERE id = $1
        """, company_id)

        if not company:
            return {"error": "Company not found"}

        ticker = company['primary_ticker']

        # Get current price (as of as_of_date)
        current = await self.db_conn.fetchrow("""
            SELECT date, close_price, volume
            FROM daily_price_data
            WHERE symbol = $1
              AND date <= $2
            ORDER BY date DESC
            LIMIT 1
        """, ticker, as_of_date)

        if not current:
            return {"error": "No price data available"}

        current_price = float(current['close_price'])
        current_date = current['date']

        # Helper to get price at a specific lookback
        async def get_price_at_offset(days: int) -> Optional[float]:
            target_date = as_of_date - timedelta(days=days)
            row = await self.db_conn.fetchrow("""
                SELECT close_price FROM daily_price_data
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 1
            """, ticker, target_date)
            return float(row['close_price']) if row else None

        # Calculate returns
        price_1mo = await get_price_at_offset(30)
        price_3mo = await get_price_at_offset(90)
        price_6mo = await get_price_at_offset(180)
        price_1yr = await get_price_at_offset(365)

        def calc_return(old_price: Optional[float]) -> Optional[float]:
            if old_price and old_price > 0:
                return ((current_price / old_price) - 1) * 100
            return None

        # 52-week high/low
        week52_start = as_of_date - timedelta(days=365)
        high_low = await self.db_conn.fetchrow("""
            SELECT
                MAX(close_price) as high_52w,
                MIN(close_price) as low_52w
            FROM daily_price_data
            WHERE symbol = $1
              AND date >= $2
              AND date <= $3
        """, ticker, week52_start, as_of_date)

        # Get some recent daily prices for context
        recent_prices = await self.db_conn.fetch("""
            SELECT date, close_price, volume
            FROM daily_price_data
            WHERE symbol = $1
              AND date <= $2
            ORDER BY date DESC
            LIMIT 30
        """, ticker, as_of_date)

        return {
            "company_name": company['company_name'],
            "ticker": ticker,
            "current_price": current_price,
            "current_date": current_date,
            "returns": {
                "1_month_pct": calc_return(price_1mo),
                "3_month_pct": calc_return(price_3mo),
                "6_month_pct": calc_return(price_6mo),
                "1_year_pct": calc_return(price_1yr),
            },
            "52_week": {
                "high": float(high_low['high_52w']) if high_low['high_52w'] else None,
                "low": float(high_low['low_52w']) if high_low['low_52w'] else None,
            },
            "recent_prices": [
                {
                    "date": row['date'],
                    "price": float(row['close_price']),
                    "volume": int(row['volume']) if row['volume'] else None,
                }
                for row in recent_prices
            ]
        }

    def format_for_prompt(self, data: Dict[str, Any]) -> str:
        """Format price data for LLM prompt"""

        if "error" in data:
            return f"Price data unavailable: {data['error']}"

        lines = [
            f"# Price Data - {data['company_name']} ({data['ticker']})",
            f"Current Price: {data['current_price']:.2f} (as of {data['current_date']})",
            "",
            "## Returns",
        ]

        ret = data['returns']
        if ret['1_month_pct'] is not None:
            lines.append(f"1 Month: {ret['1_month_pct']:+.1f}%")
        if ret['3_month_pct'] is not None:
            lines.append(f"3 Months: {ret['3_month_pct']:+.1f}%")
        if ret['6_month_pct'] is not None:
            lines.append(f"6 Months: {ret['6_month_pct']:+.1f}%")
        if ret['1_year_pct'] is not None:
            lines.append(f"1 Year: {ret['1_year_pct']:+.1f}%")

        week52 = data['52_week']
        if week52['high'] and week52['low']:
            lines.append(f"\n## 52-Week Range")
            lines.append(f"High: {week52['high']:.2f}")
            lines.append(f"Low: {week52['low']:.2f}")
            pct_from_high = ((data['current_price'] / week52['high']) - 1) * 100
            lines.append(f"% from 52w High: {pct_from_high:+.1f}%")

        # Show last 5 trading days
        lines.append(f"\n## Recent Trading (Last 5 Days)")
        for price_data in data['recent_prices'][:5]:
            vol_str = f" | Vol: {price_data['volume']:,}" if price_data['volume'] else ""
            lines.append(f"{price_data['date']}: {price_data['price']:.2f}{vol_str}")

        return "\n".join(lines)
