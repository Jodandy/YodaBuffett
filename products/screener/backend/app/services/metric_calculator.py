"""
Metric Calculator Service

Calculates fundamental, technical, and derived metrics from YodaBuffett database.
Handles point-in-time calculations for historical screening and backtesting.
"""

import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, date, timedelta
from decimal import Decimal
import math

from app.core.database import DatabaseManager
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class MetricCalculator:
    """
    Calculates financial metrics from YodaBuffett database
    
    Supports:
    - Fundamental ratios (P/E, P/B, ROE, etc.)
    - Technical indicators (RSI, Moving Averages) 
    - Derived metrics (Price changes, volatility)
    - Point-in-time calculations for any historical date
    """
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        
        # Cache for expensive calculations
        self._price_cache = {}
        self._fundamental_cache = {}
    
    async def calculate_fundamental_metrics(
        self, 
        symbols: List[str], 
        as_of_date: date,
        metrics: List[str]
    ) -> Dict[str, Dict[str, Union[float, None]]]:
        """
        Calculate fundamental metrics for given symbols as of specific date
        
        Returns: {symbol: {metric_id: value}}
        """
        logger.info(f"Calculating fundamental metrics for {len(symbols)} symbols as of {as_of_date}")
        
        results = {}
        
        for symbol in symbols:
            try:
                symbol_metrics = {}
                
                # Get latest fundamental data as of the screening date
                fundamental_data = await self._get_latest_fundamentals(symbol, as_of_date)
                if not fundamental_data:
                    logger.warning(f"No fundamental data for {symbol} as of {as_of_date}")
                    results[symbol] = {metric: None for metric in metrics}
                    continue
                
                # Get price as of screening date for market-based calculations
                price_data = await self._get_price_as_of_date(symbol, as_of_date)
                if not price_data:
                    logger.warning(f"No price data for {symbol} as of {as_of_date}")
                    results[symbol] = {metric: None for metric in metrics}
                    continue
                
                current_price = float(price_data['close_price'])
                shares_outstanding = fundamental_data.get('shares_outstanding')
                market_cap = current_price * shares_outstanding if shares_outstanding else None
                
                # Calculate each requested metric
                for metric_id in metrics:
                    value = None
                    
                    try:
                        if metric_id == 'pe_ratio':
                            value = self._calculate_pe_ratio(fundamental_data, current_price)
                        elif metric_id == 'pb_ratio':
                            value = self._calculate_pb_ratio(fundamental_data, market_cap)
                        elif metric_id == 'ps_ratio':
                            value = self._calculate_ps_ratio(fundamental_data, market_cap)
                        elif metric_id == 'ev_ebitda':
                            value = await self._calculate_ev_ebitda(fundamental_data, market_cap, symbol, as_of_date)
                        elif metric_id == 'roe':
                            value = self._calculate_roe(fundamental_data)
                        elif metric_id == 'roa':
                            value = self._calculate_roa(fundamental_data)
                        elif metric_id == 'current_ratio':
                            value = self._calculate_current_ratio(fundamental_data)
                        elif metric_id == 'debt_to_equity':
                            value = self._calculate_debt_to_equity(fundamental_data)
                        elif metric_id == 'gross_margin':
                            value = self._calculate_gross_margin(fundamental_data)
                        elif metric_id == 'operating_margin':
                            value = self._calculate_operating_margin(fundamental_data)
                        elif metric_id == 'net_margin':
                            value = self._calculate_net_margin(fundamental_data)
                        elif metric_id == 'revenue_growth_yoy':
                            value = await self._calculate_revenue_growth_yoy(symbol, as_of_date)
                        elif metric_id == 'earnings_growth_yoy':
                            value = await self._calculate_earnings_growth_yoy(symbol, as_of_date)
                        elif metric_id == 'market_cap':
                            value = market_cap
                        elif metric_id == 'enterprise_value':
                            value = await self._calculate_enterprise_value(fundamental_data, market_cap, symbol, as_of_date)
                        elif metric_id == 'dividend_yield':
                            value = await self._calculate_dividend_yield(symbol, as_of_date, current_price)
                        elif metric_id == 'price':
                            value = current_price
                        else:
                            # Try to get from historical_fundamentals_daily table directly
                            if metric_id in fundamental_data:
                                value = fundamental_data[metric_id]
                    
                    except Exception as e:
                        logger.error(f"Error calculating {metric_id} for {symbol}: {e}")
                        value = None
                    
                    symbol_metrics[metric_id] = value
                
                results[symbol] = symbol_metrics
                
            except Exception as e:
                logger.error(f"Error calculating metrics for {symbol}: {e}")
                results[symbol] = {metric: None for metric in metrics}
        
        return results
    
    async def calculate_technical_metrics(
        self, 
        symbols: List[str], 
        as_of_date: date,
        metrics: List[str]
    ) -> Dict[str, Dict[str, Union[float, None]]]:
        """Calculate technical indicators for given symbols as of specific date"""
        
        logger.info(f"Calculating technical metrics for {len(symbols)} symbols as of {as_of_date}")
        
        results = {}
        
        for symbol in symbols:
            try:
                symbol_metrics = {}
                
                # Get historical price data for technical analysis (need sufficient lookback)
                lookback_days = 252  # 1 year for most indicators
                start_date = as_of_date - timedelta(days=lookback_days)
                
                price_history = await self._get_price_history(symbol, start_date, as_of_date)
                if not price_history or len(price_history) < 14:  # Need minimum data
                    logger.warning(f"Insufficient price history for {symbol}")
                    results[symbol] = {metric: None for metric in metrics}
                    continue
                
                # Calculate each technical metric
                for metric_id in metrics:
                    value = None
                    
                    try:
                        if metric_id == 'rsi_14':
                            value = self._calculate_rsi(price_history, 14)
                        elif metric_id == 'rsi_30':
                            value = self._calculate_rsi(price_history, 30)
                        elif metric_id == 'sma_20':
                            value = self._calculate_sma(price_history, 20)
                        elif metric_id == 'sma_50':
                            value = self._calculate_sma(price_history, 50)
                        elif metric_id == 'sma_200':
                            value = self._calculate_sma(price_history, 200)
                        elif metric_id == 'ema_12':
                            value = self._calculate_ema(price_history, 12)
                        elif metric_id == 'ema_26':
                            value = self._calculate_ema(price_history, 26)
                        elif metric_id == 'volatility_20d':
                            value = self._calculate_volatility(price_history, 20)
                        elif metric_id == 'volatility_60d':
                            value = self._calculate_volatility(price_history, 60)
                        elif metric_id == 'beta':
                            value = await self._calculate_beta(symbol, as_of_date, price_history)
                        elif metric_id == 'avg_volume_20d':
                            value = self._calculate_avg_volume(price_history, 20)
                        elif metric_id == 'price_change_1d':
                            value = self._calculate_price_change(price_history, 1)
                        elif metric_id == 'price_change_5d':
                            value = self._calculate_price_change(price_history, 5)
                        elif metric_id == 'price_change_20d':
                            value = self._calculate_price_change(price_history, 20)
                        elif metric_id == 'distance_from_52w_high':
                            value = self._calculate_distance_from_high(price_history, 252)
                        elif metric_id == 'distance_from_52w_low':
                            value = self._calculate_distance_from_low(price_history, 252)
                    
                    except Exception as e:
                        logger.error(f"Error calculating {metric_id} for {symbol}: {e}")
                        value = None
                    
                    symbol_metrics[metric_id] = value
                
                results[symbol] = symbol_metrics
                
            except Exception as e:
                logger.error(f"Error calculating technical metrics for {symbol}: {e}")
                results[symbol] = {metric: None for metric in metrics}
        
        return results
    
    async def calculate_forward_returns(
        self,
        symbols: List[str],
        as_of_date: date, 
        periods: List[str]
    ) -> Dict[str, Dict[str, Union[float, None]]]:
        """
        Calculate forward returns for given symbols and periods
        
        periods: ['1W', '1M', '3M', '6M', '1Y', '2Y']
        """
        logger.info(f"Calculating forward returns for {len(symbols)} symbols from {as_of_date}")
        
        results = {}
        
        # Convert periods to days
        period_days = {
            '1W': 7,
            '1M': 30,
            '3M': 90,
            '6M': 180,
            '1Y': 365,
            '2Y': 730
        }
        
        for symbol in symbols:
            symbol_returns = {}
            
            try:
                # Get starting price (as of screening date)
                start_price_data = await self._get_price_as_of_date(symbol, as_of_date)
                if not start_price_data:
                    results[symbol] = {period: None for period in periods}
                    continue
                
                start_price = float(start_price_data['close_price'])
                
                # Calculate return for each period
                for period in periods:
                    if period not in period_days:
                        symbol_returns[period] = None
                        continue
                    
                    future_date = as_of_date + timedelta(days=period_days[period])
                    
                    # Get price at future date
                    end_price_data = await self._get_price_as_of_date(symbol, future_date)
                    if not end_price_data:
                        symbol_returns[period] = None
                        continue
                    
                    end_price = float(end_price_data['close_price'])
                    
                    # Calculate return
                    forward_return = (end_price - start_price) / start_price
                    symbol_returns[period] = forward_return
                
                results[symbol] = symbol_returns
                
            except Exception as e:
                logger.error(f"Error calculating forward returns for {symbol}: {e}")
                results[symbol] = {period: None for period in periods}
        
        return results
    
    # ===== PRIVATE HELPER METHODS =====
    
    async def _get_latest_fundamentals(self, symbol: str, as_of_date: date) -> Optional[Dict]:
        """Get latest fundamental data for symbol as of given date"""
        
        # First try historical_fundamentals_daily table
        query = """
        SELECT * FROM historical_fundamentals_daily
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
        """
        
        daily_result = await self.db_manager.execute_query_one(query, symbol, as_of_date)
        if daily_result:
            return daily_result
        
        # Fallback to financial statements (quarterly/annual data)
        financial_query = """
        SELECT 
            fs.total_revenue,
            fs.net_income,
            fs.basic_eps,
            bs.total_assets,
            bs.total_equity,
            bs.current_assets,
            bs.current_liabilities,
            bs.total_debt,
            bs.shares_outstanding
        FROM financial_statements fs
        LEFT JOIN balance_sheet_data bs ON (
            fs.symbol = bs.symbol 
            AND fs.period_date = bs.period_date 
            AND fs.statement_type = bs.statement_type
        )
        WHERE fs.symbol = $1 AND fs.period_date <= $2
        ORDER BY fs.period_date DESC
        LIMIT 1
        """
        
        return await self.db_manager.execute_query_one(financial_query, symbol, as_of_date)
    
    async def _get_price_as_of_date(self, symbol: str, target_date: date) -> Optional[Dict]:
        """Get price data for symbol as of specific date (or closest prior date)"""
        
        # Check cache first
        cache_key = f"{symbol}_{target_date}"
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
        
        query = """
        SELECT close_price, volume, date
        FROM daily_price_data
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
        """
        
        result = await self.db_manager.execute_query_one(query, symbol, target_date)
        
        # Cache the result
        if result:
            self._price_cache[cache_key] = result
        
        return result
    
    async def _get_price_history(self, symbol: str, start_date: date, end_date: date) -> List[Dict]:
        """Get price history for symbol in date range"""
        
        query = """
        SELECT date, open_price, high_price, low_price, close_price, volume
        FROM daily_price_data
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        ORDER BY date ASC
        """
        
        return await self.db_manager.execute_query(query, symbol, start_date, end_date)
    
    # ===== FUNDAMENTAL RATIO CALCULATIONS =====
    
    def _calculate_pe_ratio(self, fundamentals: Dict, current_price: float) -> Optional[float]:
        """Calculate Price-to-Earnings ratio"""
        eps = fundamentals.get('basic_eps')
        if not eps or eps <= 0:
            return None
        return current_price / float(eps)
    
    def _calculate_pb_ratio(self, fundamentals: Dict, market_cap: Optional[float]) -> Optional[float]:
        """Calculate Price-to-Book ratio"""
        if not market_cap:
            return None
        
        total_equity = fundamentals.get('total_equity')
        if not total_equity or total_equity <= 0:
            return None
        
        return market_cap / float(total_equity)
    
    def _calculate_ps_ratio(self, fundamentals: Dict, market_cap: Optional[float]) -> Optional[float]:
        """Calculate Price-to-Sales ratio"""
        if not market_cap:
            return None
        
        total_revenue = fundamentals.get('total_revenue')
        if not total_revenue or total_revenue <= 0:
            return None
        
        return market_cap / float(total_revenue)
    
    async def _calculate_ev_ebitda(self, fundamentals: Dict, market_cap: Optional[float], symbol: str, as_of_date: date) -> Optional[float]:
        """Calculate Enterprise Value to EBITDA ratio"""
        if not market_cap:
            return None
        
        # Get cash and debt for EV calculation
        total_debt = fundamentals.get('total_debt', 0)
        cash = fundamentals.get('cash_and_equivalents', 0)
        
        enterprise_value = market_cap + float(total_debt or 0) - float(cash or 0)
        
        ebitda = fundamentals.get('ebitda')
        if not ebitda or ebitda <= 0:
            return None
        
        return enterprise_value / float(ebitda)
    
    def _calculate_roe(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Return on Equity"""
        net_income = fundamentals.get('net_income')
        total_equity = fundamentals.get('total_equity')
        
        if not net_income or not total_equity or total_equity <= 0:
            return None
        
        return (float(net_income) / float(total_equity)) * 100  # Return as percentage
    
    def _calculate_roa(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Return on Assets"""
        net_income = fundamentals.get('net_income')
        total_assets = fundamentals.get('total_assets')
        
        if not net_income or not total_assets or total_assets <= 0:
            return None
        
        return (float(net_income) / float(total_assets)) * 100  # Return as percentage
    
    def _calculate_current_ratio(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Current Ratio"""
        current_assets = fundamentals.get('current_assets')
        current_liabilities = fundamentals.get('current_liabilities')
        
        if not current_assets or not current_liabilities or current_liabilities <= 0:
            return None
        
        return float(current_assets) / float(current_liabilities)
    
    def _calculate_debt_to_equity(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Debt-to-Equity ratio"""
        total_debt = fundamentals.get('total_debt')
        total_equity = fundamentals.get('total_equity')
        
        if not total_debt or not total_equity or total_equity <= 0:
            return None
        
        return float(total_debt) / float(total_equity)
    
    def _calculate_gross_margin(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Gross Margin percentage"""
        gross_profit = fundamentals.get('gross_profit')
        total_revenue = fundamentals.get('total_revenue')
        
        if not gross_profit or not total_revenue or total_revenue <= 0:
            return None
        
        return (float(gross_profit) / float(total_revenue)) * 100
    
    def _calculate_operating_margin(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Operating Margin percentage"""
        operating_income = fundamentals.get('operating_income')
        total_revenue = fundamentals.get('total_revenue')
        
        if not operating_income or not total_revenue or total_revenue <= 0:
            return None
        
        return (float(operating_income) / float(total_revenue)) * 100
    
    def _calculate_net_margin(self, fundamentals: Dict) -> Optional[float]:
        """Calculate Net Margin percentage"""
        net_income = fundamentals.get('net_income')
        total_revenue = fundamentals.get('total_revenue')
        
        if not net_income or not total_revenue or total_revenue <= 0:
            return None
        
        return (float(net_income) / float(total_revenue)) * 100
    
    async def _calculate_revenue_growth_yoy(self, symbol: str, as_of_date: date) -> Optional[float]:
        """Calculate Year-over-Year revenue growth"""
        
        # Get current and prior year revenue
        current_revenue_query = """
        SELECT total_revenue FROM financial_statements
        WHERE symbol = $1 AND period_date <= $2 AND statement_type = 'annual'
        ORDER BY period_date DESC LIMIT 1
        """
        
        prior_year_date = as_of_date.replace(year=as_of_date.year - 1)
        prior_revenue_query = """
        SELECT total_revenue FROM financial_statements  
        WHERE symbol = $1 AND period_date <= $2 AND statement_type = 'annual'
        ORDER BY period_date DESC LIMIT 1
        """
        
        current_result = await self.db_manager.execute_query_one(current_revenue_query, symbol, as_of_date)
        prior_result = await self.db_manager.execute_query_one(prior_revenue_query, symbol, prior_year_date)
        
        if not current_result or not prior_result:
            return None
        
        current_revenue = float(current_result['total_revenue'] or 0)
        prior_revenue = float(prior_result['total_revenue'] or 0)
        
        if prior_revenue <= 0:
            return None
        
        return ((current_revenue - prior_revenue) / prior_revenue) * 100
    
    async def _calculate_earnings_growth_yoy(self, symbol: str, as_of_date: date) -> Optional[float]:
        """Calculate Year-over-Year earnings growth"""
        # Similar to revenue growth but for net income
        # Implementation follows same pattern as revenue growth
        pass  # Implement if needed
    
    async def _calculate_enterprise_value(self, fundamentals: Dict, market_cap: Optional[float], symbol: str, as_of_date: date) -> Optional[float]:
        """Calculate Enterprise Value"""
        if not market_cap:
            return None
        
        total_debt = fundamentals.get('total_debt', 0)
        cash = fundamentals.get('cash_and_equivalents', 0)
        
        return market_cap + float(total_debt or 0) - float(cash or 0)
    
    async def _calculate_dividend_yield(self, symbol: str, as_of_date: date, current_price: float) -> Optional[float]:
        """Calculate dividend yield (if dividend data available)"""
        # This would require dividend payment data - implement if available
        return None
    
    # ===== TECHNICAL INDICATOR CALCULATIONS =====
    
    def _calculate_rsi(self, price_history: List[Dict], period: int = 14) -> Optional[float]:
        """Calculate Relative Strength Index"""
        if len(price_history) < period + 1:
            return None
        
        # Calculate daily price changes
        changes = []
        for i in range(1, len(price_history)):
            prev_close = float(price_history[i-1]['close_price'])
            curr_close = float(price_history[i]['close_price'])
            changes.append(curr_close - prev_close)
        
        if len(changes) < period:
            return None
        
        # Calculate average gains and losses
        recent_changes = changes[-period:]
        avg_gain = sum(change for change in recent_changes if change > 0) / period
        avg_loss = abs(sum(change for change in recent_changes if change < 0)) / period
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_sma(self, price_history: List[Dict], period: int) -> Optional[float]:
        """Calculate Simple Moving Average"""
        if len(price_history) < period:
            return None
        
        recent_prices = [float(p['close_price']) for p in price_history[-period:]]
        return sum(recent_prices) / len(recent_prices)
    
    def _calculate_ema(self, price_history: List[Dict], period: int) -> Optional[float]:
        """Calculate Exponential Moving Average"""
        if len(price_history) < period:
            return None
        
        prices = [float(p['close_price']) for p in price_history]
        
        # Calculate multiplier
        multiplier = 2 / (period + 1)
        
        # Initialize with SMA
        ema = sum(prices[:period]) / period
        
        # Calculate EMA for remaining prices
        for i in range(period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
        
        return ema
    
    def _calculate_volatility(self, price_history: List[Dict], period: int) -> Optional[float]:
        """Calculate price volatility (standard deviation of returns)"""
        if len(price_history) < period + 1:
            return None
        
        # Calculate daily returns
        returns = []
        for i in range(1, len(price_history)):
            prev_close = float(price_history[i-1]['close_price'])
            curr_close = float(price_history[i]['close_price'])
            daily_return = (curr_close - prev_close) / prev_close
            returns.append(daily_return)
        
        if len(returns) < period:
            return None
        
        recent_returns = returns[-period:]
        
        # Calculate standard deviation
        mean_return = sum(recent_returns) / len(recent_returns)
        variance = sum((r - mean_return) ** 2 for r in recent_returns) / len(recent_returns)
        volatility = math.sqrt(variance)
        
        # Annualize volatility
        return volatility * math.sqrt(252) * 100  # Return as percentage
    
    async def _calculate_beta(self, symbol: str, as_of_date: date, price_history: List[Dict]) -> Optional[float]:
        """Calculate Beta vs market (would need market index data)"""
        # This requires market index data - implement if available
        return None
    
    def _calculate_avg_volume(self, price_history: List[Dict], period: int) -> Optional[float]:
        """Calculate average trading volume"""
        if len(price_history) < period:
            return None
        
        recent_volumes = [float(p.get('volume', 0)) for p in price_history[-period:]]
        return sum(recent_volumes) / len(recent_volumes)
    
    def _calculate_price_change(self, price_history: List[Dict], days: int) -> Optional[float]:
        """Calculate price change over specified number of days"""
        if len(price_history) < days + 1:
            return None
        
        current_price = float(price_history[-1]['close_price'])
        past_price = float(price_history[-days-1]['close_price'])
        
        return ((current_price - past_price) / past_price) * 100
    
    def _calculate_distance_from_high(self, price_history: List[Dict], period: int) -> Optional[float]:
        """Calculate distance from period high"""
        if len(price_history) < period:
            return None
        
        recent_highs = [float(p['high_price']) for p in price_history[-period:]]
        period_high = max(recent_highs)
        current_price = float(price_history[-1]['close_price'])
        
        return ((current_price - period_high) / period_high) * 100
    
    def _calculate_distance_from_low(self, price_history: List[Dict], period: int) -> Optional[float]:
        """Calculate distance from period low"""
        if len(price_history) < period:
            return None
        
        recent_lows = [float(p['low_price']) for p in price_history[-period:]]
        period_low = min(recent_lows)
        current_price = float(price_history[-1]['close_price'])
        
        return ((current_price - period_low) / period_low) * 100