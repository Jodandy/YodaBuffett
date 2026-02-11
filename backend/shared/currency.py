"""
Currency Conversion Utilities

Handles conversion of financial figures to a common currency (USD).
Used for comparing companies across Nordic markets (SEK, NOK, DKK, EUR).

IMPORTANT: These are approximate exchange rates for comparison purposes.
For precise valuations, use real-time exchange rates from a data provider.

Rates are approximate as of 2024 and should be updated periodically.
"""

from typing import Optional
from datetime import date

# Approximate exchange rates to USD (updated periodically)
# These are ballpark figures for screening/comparison purposes
# Source: Approximate rates as of late 2024
EXCHANGE_RATES_TO_USD = {
    'USD': 1.0,
    'SEK': 0.095,   # ~10.5 SEK per USD
    'NOK': 0.092,   # ~10.9 NOK per USD
    'DKK': 0.14,    # ~7.1 DKK per USD
    'EUR': 1.08,    # ~0.93 EUR per USD
    'GBP': 1.27,    # ~0.79 GBP per USD
    'CHF': 1.13,    # ~0.88 CHF per USD
    'CAD': 0.74,    # ~1.35 CAD per USD
    'AUD': 0.65,    # ~1.54 AUD per USD
    'JPY': 0.0067,  # ~150 JPY per USD
}

# Alternative: rates to SEK (for Nordic-focused comparisons)
EXCHANGE_RATES_TO_SEK = {
    'SEK': 1.0,
    'NOK': 0.97,    # ~1.03 NOK per SEK
    'DKK': 1.47,    # ~0.68 DKK per SEK
    'EUR': 11.4,    # ~0.088 EUR per SEK
    'USD': 10.5,    # ~0.095 USD per SEK
    'GBP': 13.3,    # ~0.075 GBP per SEK
}


def convert_to_usd(
    amount: Optional[float],
    from_currency: Optional[str],
    fallback_currency: str = 'SEK'
) -> Optional[float]:
    """
    Convert an amount to USD.

    Args:
        amount: The amount to convert
        from_currency: Source currency code (SEK, NOK, DKK, EUR, etc.)
        fallback_currency: Currency to assume if from_currency is None

    Returns:
        Amount in USD, or None if conversion not possible
    """
    if amount is None:
        return None

    currency = (from_currency or fallback_currency).upper()

    if currency not in EXCHANGE_RATES_TO_USD:
        # Unknown currency - assume it's already USD or use fallback
        currency = fallback_currency.upper()

    rate = EXCHANGE_RATES_TO_USD.get(currency, EXCHANGE_RATES_TO_USD[fallback_currency])
    return amount * rate


def convert_to_sek(
    amount: Optional[float],
    from_currency: Optional[str],
    fallback_currency: str = 'SEK'
) -> Optional[float]:
    """
    Convert an amount to SEK.

    Args:
        amount: The amount to convert
        from_currency: Source currency code
        fallback_currency: Currency to assume if from_currency is None

    Returns:
        Amount in SEK, or None if conversion not possible
    """
    if amount is None:
        return None

    currency = (from_currency or fallback_currency).upper()

    if currency not in EXCHANGE_RATES_TO_SEK:
        # For unknown currencies, convert via USD
        usd_amount = convert_to_usd(amount, currency, fallback_currency)
        if usd_amount is None:
            return None
        return usd_amount * EXCHANGE_RATES_TO_SEK['USD']

    rate = EXCHANGE_RATES_TO_SEK.get(currency, 1.0)
    return amount * rate


def get_currency_for_symbol(symbol: str) -> str:
    """
    Infer currency from symbol suffix.

    Args:
        symbol: Stock symbol (e.g., 'VOLV-B', 'DNB', 'NOVO-B')

    Returns:
        Likely currency code
    """
    # Norwegian stocks often have .OL suffix or specific patterns
    # Danish stocks often have .CO suffix
    # Finnish stocks often have .HE suffix
    # Swedish stocks are default

    symbol_upper = symbol.upper()

    if '.OL' in symbol_upper or symbol_upper.endswith('-OL'):
        return 'NOK'
    elif '.CO' in symbol_upper or symbol_upper.endswith('-CO'):
        return 'DKK'
    elif '.HE' in symbol_upper or symbol_upper.endswith('-HE'):
        return 'EUR'
    elif '.ST' in symbol_upper or symbol_upper.endswith('-ST'):
        return 'SEK'
    else:
        # Default to SEK for Nordic screener
        return 'SEK'


def format_currency(
    amount: Optional[float],
    currency: str = 'USD',
    abbreviated: bool = True
) -> str:
    """
    Format a currency amount for display.

    Args:
        amount: Amount to format
        currency: Currency code
        abbreviated: If True, use K/M/B suffixes

    Returns:
        Formatted string like "$1.5B" or "SEK 1,500M"
    """
    if amount is None:
        return '-'

    symbols = {
        'USD': '$',
        'EUR': '€',
        'GBP': '£',
        'SEK': 'SEK ',
        'NOK': 'NOK ',
        'DKK': 'DKK ',
    }

    symbol = symbols.get(currency.upper(), f'{currency} ')

    if abbreviated:
        if abs(amount) >= 1e12:
            return f'{symbol}{amount/1e12:.1f}T'
        elif abs(amount) >= 1e9:
            return f'{symbol}{amount/1e9:.1f}B'
        elif abs(amount) >= 1e6:
            return f'{symbol}{amount/1e6:.1f}M'
        elif abs(amount) >= 1e3:
            return f'{symbol}{amount/1e3:.1f}K'

    return f'{symbol}{amount:,.0f}'


# Convenience functions for common conversions
def sek_to_usd(amount: Optional[float]) -> Optional[float]:
    """Convert SEK to USD."""
    return convert_to_usd(amount, 'SEK')


def nok_to_usd(amount: Optional[float]) -> Optional[float]:
    """Convert NOK to USD."""
    return convert_to_usd(amount, 'NOK')


def dkk_to_usd(amount: Optional[float]) -> Optional[float]:
    """Convert DKK to USD."""
    return convert_to_usd(amount, 'DKK')


def eur_to_usd(amount: Optional[float]) -> Optional[float]:
    """Convert EUR to USD."""
    return convert_to_usd(amount, 'EUR')
