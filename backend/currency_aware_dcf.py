#!/usr/bin/env python3
"""
Currency-Aware DCF Engine

DCF engine that handles currency conversion between report currency and stock currency.
Uses real exchange rates and properly converts fair values.
"""

import asyncio
import asyncpg
from publish_date_only_dcf import PublishDateOnlyDCF, DCFParameters
from datetime import datetime
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Simple exchange rates (in a production system, you'd fetch these from an API)
EXCHANGE_RATES = {
    ('USD', 'SEK'): 11.0,   # 1 USD = 11 SEK (approximate)
    ('EUR', 'SEK'): 12.0,   # 1 EUR = 12 SEK  
    ('CHF', 'SEK'): 12.5,   # 1 CHF = 12.5 SEK
    ('DKK', 'SEK'): 1.6,    # 1 DKK = 1.6 SEK
    ('NOK', 'SEK'): 1.0,    # 1 NOK = 1 SEK (approximate)
    ('USD', 'NOK'): 11.0,   # 1 USD = 11 NOK
    ('EUR', 'NOK'): 12.0,   # 1 EUR = 12 NOK
    ('DKK', 'NOK'): 1.5,    # 1 DKK = 1.5 NOK
    ('SEK', 'NOK'): 1.0,    # 1 SEK = 1 NOK
    ('USD', 'DKK'): 7.0,    # 1 USD = 7 DKK
    ('EUR', 'DKK'): 7.5,    # 1 EUR = 7.5 DKK
}

class CurrencyAwareDCF(PublishDateOnlyDCF):
    """DCF engine with currency conversion support"""
    
    def get_exchange_rate(self, from_currency: str, to_currency: str) -> float:
        """Get exchange rate from one currency to another"""
        
        if from_currency == to_currency:
            return 1.0
        
        # Direct rate
        if (from_currency, to_currency) in EXCHANGE_RATES:
            return EXCHANGE_RATES[(from_currency, to_currency)]
        
        # Inverse rate
        if (to_currency, from_currency) in EXCHANGE_RATES:
            return 1.0 / EXCHANGE_RATES[(to_currency, from_currency)]
        
        # Cross rates via USD (most common case)
        if from_currency != 'USD' and to_currency != 'USD':
            usd_from = EXCHANGE_RATES.get(('USD', from_currency), None)
            usd_to = EXCHANGE_RATES.get(('USD', to_currency), None)
            
            if usd_from and usd_to:
                return usd_to / usd_from
        
        # Default to 1.0 if no rate found (log warning)
        logger.warning(f"No exchange rate found for {from_currency} → {to_currency}, using 1.0")
        return 1.0
    
    async def get_currency_info(self, symbol: str):
        """Get stock and report currencies for a symbol"""
        
        currency_query = """
        SELECT stock_currency, report_currency
        FROM company_master
        WHERE primary_ticker = $1
        """
        
        currency_record = await self.db_conn.fetchrow(currency_query, symbol)
        
        if currency_record:
            return {
                'stock_currency': currency_record['stock_currency'],
                'report_currency': currency_record['report_currency']
            }
        else:
            # Default to SEK if not found (most Nordic companies)
            logger.warning(f"No currency info for {symbol}, defaulting to SEK")
            return {'stock_currency': 'SEK', 'report_currency': 'SEK'}
    
    async def value_company(self, symbol: str, valuation_date: datetime, market_price: float):
        """Value company with currency conversion"""
        
        # Get base DCF result in report currency
        base_result = await super().value_company(symbol, valuation_date, market_price)
        
        if not base_result:
            return None
        
        # Get currency information
        currency_info = await self.get_currency_info(symbol)
        stock_currency = currency_info['stock_currency']
        report_currency = currency_info['report_currency']
        
        # Get exchange rate
        exchange_rate = self.get_exchange_rate(report_currency, stock_currency)
        
        # Convert fair values to stock currency
        fair_value_fields = [
            'fair_value_mean', 'fair_value_median', 'fair_value_std',
            'fair_value_p5', 'fair_value_p25', 'fair_value_p50', 
            'fair_value_p75', 'fair_value_p95'
        ]
        
        # Apply currency conversion
        result_converted = base_result.copy()
        
        for field in fair_value_fields:
            if field in result_converted:
                result_converted[field] = float(result_converted[field]) * exchange_rate
        
        # Recalculate implied return with converted fair value
        converted_fair_value = result_converted['fair_value_median']
        result_converted['implied_return'] = (converted_fair_value - market_price) / market_price
        
        # Update valuation signal based on new return
        if result_converted['implied_return'] > 0.15:
            result_converted['valuation_signal'] = 'UNDERVALUED'
        elif result_converted['implied_return'] < -0.15:
            result_converted['valuation_signal'] = 'OVERVALUED' 
        else:
            result_converted['valuation_signal'] = 'FAIR_VALUE'
        
        # Add currency metadata
        result_converted['currency_conversion'] = {
            'stock_currency': stock_currency,
            'report_currency': report_currency,
            'exchange_rate': exchange_rate,
            'conversion_applied': exchange_rate != 1.0
        }
        
        return result_converted

async def test_currency_aware_dcf():
    """Test the currency-aware DCF engine"""
    
    params = DCFParameters(num_simulations=1000)
    engine = CurrencyAwareDCF(params)
    await engine.setup()
    
    test_cases = [
        ('ABB', '2024-12-08', 650.0),    # USD → SEK conversion
        ('AAK', '2024-12-08', 270.0),    # SEK → SEK (no conversion)
        ('VOLV-B', '2024-12-08', 280.0), # Should check currency
    ]
    
    print('💱 TESTING CURRENCY-AWARE DCF ENGINE')
    print('=' * 70)
    
    for symbol, date_str, price in test_cases:
        test_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        print(f'\n📅 {symbol} on {date_str} (Price: {price})')
        
        try:
            # Get currency info first
            currency_info = await engine.get_currency_info(symbol)
            stock_curr = currency_info['stock_currency']
            report_curr = currency_info['report_currency']
            
            print(f'   Currencies: Reports in {report_curr}, trades in {stock_curr}')
            
            if report_curr != stock_curr:
                exchange_rate = engine.get_exchange_rate(report_curr, stock_curr)
                print(f'   Exchange rate: 1 {report_curr} = {exchange_rate} {stock_curr}')
            
            # Run currency-aware DCF
            result = await engine.value_company(symbol, test_date, price)
            
            if result:
                return_pct = result['implied_return'] * 100
                currency_info = result['currency_conversion']
                
                print(f'   DCF Result:')
                print(f'     Fair value: {result["fair_value_median"]:.0f} {stock_curr}')
                print(f'     Market price: {price} {stock_curr}')
                print(f'     Implied return: {return_pct:+.0f}%')
                print(f'     Signal: {result["valuation_signal"]}')
                
                if currency_info['conversion_applied']:
                    print(f'     ✅ Currency conversion applied ({report_curr} → {stock_curr})')
                else:
                    print(f'     ℹ️ No currency conversion needed')
            else:
                print(f'   ❌ DCF calculation failed')
                
        except Exception as e:
            print(f'   ❌ Error: {str(e)[:60]}...')
    
    await engine.cleanup()

if __name__ == "__main__":
    asyncio.run(test_currency_aware_dcf())