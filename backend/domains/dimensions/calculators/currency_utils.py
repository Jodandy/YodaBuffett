"""
Currency Conversion Utilities

Provides exchange rate conversion for valuation calculations.
When financial statements are in a different currency than the stock price,
we need to convert to a common currency for accurate ratio calculations.

Exchange rates are approximate and should be updated periodically.
For production, consider using a real-time FX API.
"""

from typing import Optional
from datetime import date
import logging

logger = logging.getLogger(__name__)

# Approximate exchange rates to SEK (base currency for Nordic markets)
# Last updated: 2025-02
# These should be updated periodically or replaced with API calls
EXCHANGE_RATES_TO_SEK = {
    "SEK": 1.0,
    "EUR": 11.50,  # 1 EUR = 11.50 SEK
    "USD": 10.80,  # 1 USD = 10.80 SEK
    "NOK": 0.95,   # 1 NOK = 0.95 SEK
    "DKK": 1.54,   # 1 DKK = 1.54 SEK
    "GBP": 13.50,  # 1 GBP = 13.50 SEK
    "CHF": 12.00,  # 1 CHF = 12.00 SEK
    "CAD": 7.50,   # 1 CAD = 7.50 SEK
    "AUD": 6.80,   # 1 AUD = 6.80 SEK
    "PLN": 2.50,   # 1 PLN = 2.50 SEK
    "ISK": 0.075,  # 1 ISK = 0.075 SEK
    "SGD": 8.00,   # 1 SGD = 8.00 SEK
}


def convert_currency(
    amount: float,
    from_currency: str,
    to_currency: str
) -> Optional[float]:
    """
    Convert an amount from one currency to another.

    Args:
        amount: The amount to convert
        from_currency: Source currency code (e.g., "EUR")
        to_currency: Target currency code (e.g., "SEK")

    Returns:
        Converted amount, or None if conversion not possible
    """
    if not amount or not from_currency or not to_currency:
        return None

    from_currency = from_currency.upper()
    to_currency = to_currency.upper()

    # Same currency, no conversion needed
    if from_currency == to_currency:
        return amount

    # Convert via SEK as intermediate
    from_rate = EXCHANGE_RATES_TO_SEK.get(from_currency)
    to_rate = EXCHANGE_RATES_TO_SEK.get(to_currency)

    if from_rate is None:
        logger.warning(f"Unknown currency: {from_currency}")
        return None

    if to_rate is None:
        logger.warning(f"Unknown currency: {to_currency}")
        return None

    # Convert: amount in from_currency -> SEK -> to_currency
    amount_in_sek = amount * from_rate
    result = amount_in_sek / to_rate

    return result


def get_exchange_rate(from_currency: str, to_currency: str) -> Optional[float]:
    """
    Get the exchange rate between two currencies.

    Args:
        from_currency: Source currency code
        to_currency: Target currency code

    Returns:
        Exchange rate (multiply by this to convert), or None if unknown
    """
    if not from_currency or not to_currency:
        return None

    from_currency = from_currency.upper()
    to_currency = to_currency.upper()

    if from_currency == to_currency:
        return 1.0

    from_rate = EXCHANGE_RATES_TO_SEK.get(from_currency)
    to_rate = EXCHANGE_RATES_TO_SEK.get(to_currency)

    if from_rate is None or to_rate is None:
        return None

    return from_rate / to_rate


def needs_conversion(report_currency: Optional[str], stock_currency: Optional[str]) -> bool:
    """
    Check if currency conversion is needed.

    Args:
        report_currency: Currency of financial statements
        stock_currency: Currency of stock price

    Returns:
        True if conversion is needed
    """
    if not report_currency or not stock_currency:
        return False

    return report_currency.upper() != stock_currency.upper()
