"""
Metric Calculator Service

Calculates fundamental, technical, and derived metrics from YodaBuffett database.
Handles point-in-time calculations for historical screening and backtesting.
"""

import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date, timedelta
import math

from ..database import DatabaseManager

logger = logging.getLogger(__name__)


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
        self._price_cache = {}
        self._fundamental_cache = {}

    async def calculate_fundamental_metrics(
        self,
        symbols: List[str],
        as_of_date: date,
        metrics: List[str]
    ) -> Dict[str, Dict[str, Union[float, None]]]:
        """Calculate fundamental metrics for given symbols as of specific date"""
        logger.info(f"Calculating fundamental metrics for {len(symbols)} symbols as of {as_of_date}")

        results = {}

        for symbol in symbols:
            try:
                symbol_metrics = {}

                fundamental_data = await self._get_latest_fundamentals(symbol, as_of_date)
                if not fundamental_data:
                    results[symbol] = {metric: None for metric in metrics}
                    continue

                price_data = await self._get_price_as_of_date(symbol, as_of_date)
                if not price_data:
                    results[symbol] = {metric: None for metric in metrics}
                    continue

                current_price = float(price_data['close_price'])
                shares_outstanding = fundamental_data.get('shares_outstanding')
                market_cap = current_price * shares_outstanding if shares_outstanding else None

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
                            value = self._calculate_ev_ebitda(fundamental_data, market_cap)
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
                        elif metric_id == 'market_cap':
                            value = market_cap
                        elif metric_id == 'price':
                            value = current_price
                        elif metric_id in fundamental_data:
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
                lookback_days = 252
                start_date = as_of_date - timedelta(days=lookback_days)

                price_history = await self._get_price_history(symbol, start_date, as_of_date)
                if not price_history or len(price_history) < 14:
                    results[symbol] = {metric: None for metric in metrics}
                    continue

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
        """Calculate forward returns for given symbols and periods"""
        logger.info(f"Calculating forward returns for {len(symbols)} symbols from {as_of_date}")

        results = {}
        period_days = {'1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365, '2Y': 730}

        for symbol in symbols:
            symbol_returns = {}

            try:
                start_price_data = await self._get_price_as_of_date(symbol, as_of_date)
                if not start_price_data:
                    results[symbol] = {period: None for period in periods}
                    continue

                start_price = float(start_price_data['close_price'])

                for period in periods:
                    if period not in period_days:
                        symbol_returns[period] = None
                        continue

                    future_date = as_of_date + timedelta(days=period_days[period])
                    end_price_data = await self._get_price_as_of_date(symbol, future_date)

                    if not end_price_data:
                        symbol_returns[period] = None
                        continue

                    end_price = float(end_price_data['close_price'])
                    forward_return = (end_price - start_price) / start_price
                    symbol_returns[period] = forward_return

                results[symbol] = symbol_returns

            except Exception as e:
                logger.error(f"Error calculating forward returns for {symbol}: {e}")
                results[symbol] = {period: None for period in periods}

        return results

    async def calculate_dimension_metrics(
        self,
        symbols: List[str],
        as_of_date: date,
        metrics: List[str]
    ) -> Dict[str, Dict[str, Union[float, None]]]:
        """Calculate dimension metrics for given symbols as of specific date"""
        logger.info(f"Calculating dimension metrics for {len(symbols)} symbols as of {as_of_date}")

        results = {}

        # First, get company IDs for the symbols
        symbol_to_company_id = await self._get_company_ids_for_symbols(symbols)

        for symbol in symbols:
            company_id = symbol_to_company_id.get(symbol)
            if not company_id:
                results[symbol] = {metric: None for metric in metrics}
                continue

            try:
                symbol_metrics = {}

                # Get dimension scores from company_dimension_summary view
                dimension_data = await self._get_dimension_scores(company_id, as_of_date)

                for metric_id in metrics:
                    value = None
                    try:
                        if dimension_data:
                            if metric_id == 'value_score':
                                value = dimension_data.get('value_score')
                            elif metric_id == 'momentum_score':
                                value = dimension_data.get('momentum_score')
                            elif metric_id == 'quality_score':
                                value = dimension_data.get('quality_score')
                            elif metric_id == 'sentiment_score':
                                value = dimension_data.get('sentiment_score')
                            elif metric_id == 'risk_score':
                                value = dimension_data.get('risk_score')
                            elif metric_id == 'value_percentile':
                                value = dimension_data.get('value_percentile')
                            elif metric_id == 'momentum_percentile':
                                value = dimension_data.get('momentum_percentile')
                            elif metric_id == 'quality_percentile':
                                value = dimension_data.get('quality_percentile')
                            elif metric_id == 'sentiment_percentile':
                                value = dimension_data.get('sentiment_percentile')
                            elif metric_id == 'risk_percentile':
                                value = dimension_data.get('risk_percentile')
                            elif metric_id == 'composite_score':
                                value = await self._get_composite_score(company_id, as_of_date)

                        if value is not None:
                            value = float(value)
                    except Exception as e:
                        logger.error(f"Error calculating {metric_id} for {symbol}: {e}")

                    symbol_metrics[metric_id] = value

                results[symbol] = symbol_metrics

            except Exception as e:
                logger.error(f"Error calculating dimension metrics for {symbol}: {e}")
                results[symbol] = {metric: None for metric in metrics}

        return results

    async def _get_company_ids_for_symbols(self, symbols: List[str]) -> Dict[str, str]:
        """Get company_id mapping for symbols, preferring companies with dimension scores"""
        if not symbols:
            return {}

        # Get companies with dimension score count to prefer ones with data
        query = """
        SELECT cm.id, cm.primary_ticker, cm.yahoo_symbol,
               (SELECT COUNT(*) FROM daily_dimension_scores ds WHERE ds.company_id = cm.id) as dim_count
        FROM company_master cm
        WHERE cm.primary_ticker = ANY($1) OR cm.yahoo_symbol = ANY($1)
        ORDER BY dim_count DESC
        """
        results = await self.db_manager.execute_query(query, symbols)

        symbol_to_id = {}
        for row in results:
            # Only set if not already set (first match = highest dim_count)
            if row.get('primary_ticker') in symbols and row['primary_ticker'] not in symbol_to_id:
                symbol_to_id[row['primary_ticker']] = str(row['id'])
            if row.get('yahoo_symbol') in symbols and row['yahoo_symbol'] not in symbol_to_id:
                symbol_to_id[row['yahoo_symbol']] = str(row['id'])

        return symbol_to_id

    async def _get_dimension_scores(self, company_id: str, as_of_date: date) -> Optional[Dict]:
        """Get dimension scores for a company"""
        query = """
        SELECT
            MAX(CASE WHEN dimension_code = 'value' THEN score END) as value_score,
            MAX(CASE WHEN dimension_code = 'momentum' THEN score END) as momentum_score,
            MAX(CASE WHEN dimension_code = 'quality' THEN score END) as quality_score,
            MAX(CASE WHEN dimension_code = 'sentiment' THEN score END) as sentiment_score,
            MAX(CASE WHEN dimension_code = 'risk' THEN score END) as risk_score,
            MAX(CASE WHEN dimension_code = 'value' THEN percentile_rank END) as value_percentile,
            MAX(CASE WHEN dimension_code = 'momentum' THEN percentile_rank END) as momentum_percentile,
            MAX(CASE WHEN dimension_code = 'quality' THEN percentile_rank END) as quality_percentile,
            MAX(CASE WHEN dimension_code = 'sentiment' THEN percentile_rank END) as sentiment_percentile,
            MAX(CASE WHEN dimension_code = 'risk' THEN percentile_rank END) as risk_percentile
        FROM daily_dimension_scores
        WHERE company_id = $1::uuid AND score_date <= $2
        AND score_date >= $2 - INTERVAL '7 days'
        GROUP BY company_id
        """
        return await self.db_manager.execute_query_one(query, company_id, as_of_date)

    async def _get_composite_score(self, company_id: str, as_of_date: date) -> Optional[float]:
        """Get composite score for a company"""
        query = """
        SELECT score FROM composite_scores
        WHERE company_id = $1::uuid AND score_date <= $2
        AND composite_code = 'overall'
        ORDER BY score_date DESC
        LIMIT 1
        """
        result = await self.db_manager.execute_query_one(query, company_id, as_of_date)
        return float(result['score']) if result else None

    # ===== PRIVATE HELPER METHODS =====

    async def _get_latest_fundamentals(self, symbol: str, as_of_date: date) -> Optional[Dict]:
        """Get latest fundamental data for symbol as of given date"""
        query = """
        SELECT * FROM historical_fundamentals_daily
        WHERE symbol = $1 AND date <= $2
        ORDER BY date DESC
        LIMIT 1
        """
        daily_result = await self.db_manager.execute_query_one(query, symbol, as_of_date)
        if daily_result:
            return daily_result

        # Fallback to financial statements
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
        """Get price data for symbol as of specific date"""
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
        eps = fundamentals.get('basic_eps')
        if not eps or eps <= 0:
            return None
        return current_price / float(eps)

    def _calculate_pb_ratio(self, fundamentals: Dict, market_cap: Optional[float]) -> Optional[float]:
        if not market_cap:
            return None
        total_equity = fundamentals.get('total_equity')
        if not total_equity or total_equity <= 0:
            return None
        return market_cap / float(total_equity)

    def _calculate_ps_ratio(self, fundamentals: Dict, market_cap: Optional[float]) -> Optional[float]:
        if not market_cap:
            return None
        total_revenue = fundamentals.get('total_revenue')
        if not total_revenue or total_revenue <= 0:
            return None
        return market_cap / float(total_revenue)

    def _calculate_ev_ebitda(self, fundamentals: Dict, market_cap: Optional[float]) -> Optional[float]:
        if not market_cap:
            return None
        total_debt = fundamentals.get('total_debt', 0)
        cash = fundamentals.get('cash_and_equivalents', 0)
        enterprise_value = market_cap + float(total_debt or 0) - float(cash or 0)
        ebitda = fundamentals.get('ebitda')
        if not ebitda or ebitda <= 0:
            return None
        return enterprise_value / float(ebitda)

    def _calculate_roe(self, fundamentals: Dict) -> Optional[float]:
        net_income = fundamentals.get('net_income')
        total_equity = fundamentals.get('total_equity')
        if not net_income or not total_equity or total_equity <= 0:
            return None
        return (float(net_income) / float(total_equity)) * 100

    def _calculate_roa(self, fundamentals: Dict) -> Optional[float]:
        net_income = fundamentals.get('net_income')
        total_assets = fundamentals.get('total_assets')
        if not net_income or not total_assets or total_assets <= 0:
            return None
        return (float(net_income) / float(total_assets)) * 100

    def _calculate_current_ratio(self, fundamentals: Dict) -> Optional[float]:
        current_assets = fundamentals.get('current_assets')
        current_liabilities = fundamentals.get('current_liabilities')
        if not current_assets or not current_liabilities or current_liabilities <= 0:
            return None
        return float(current_assets) / float(current_liabilities)

    def _calculate_debt_to_equity(self, fundamentals: Dict) -> Optional[float]:
        total_debt = fundamentals.get('total_debt')
        total_equity = fundamentals.get('total_equity')
        if not total_debt or not total_equity or total_equity <= 0:
            return None
        return float(total_debt) / float(total_equity)

    def _calculate_gross_margin(self, fundamentals: Dict) -> Optional[float]:
        gross_profit = fundamentals.get('gross_profit')
        total_revenue = fundamentals.get('total_revenue')
        if not gross_profit or not total_revenue or total_revenue <= 0:
            return None
        return (float(gross_profit) / float(total_revenue)) * 100

    def _calculate_operating_margin(self, fundamentals: Dict) -> Optional[float]:
        operating_income = fundamentals.get('operating_income')
        total_revenue = fundamentals.get('total_revenue')
        if not operating_income or not total_revenue or total_revenue <= 0:
            return None
        return (float(operating_income) / float(total_revenue)) * 100

    def _calculate_net_margin(self, fundamentals: Dict) -> Optional[float]:
        net_income = fundamentals.get('net_income')
        total_revenue = fundamentals.get('total_revenue')
        if not net_income or not total_revenue or total_revenue <= 0:
            return None
        return (float(net_income) / float(total_revenue)) * 100

    # ===== TECHNICAL INDICATOR CALCULATIONS =====

    def _calculate_rsi(self, price_history: List[Dict], period: int = 14) -> Optional[float]:
        if len(price_history) < period + 1:
            return None

        changes = []
        for i in range(1, len(price_history)):
            prev_close = float(price_history[i - 1]['close_price'])
            curr_close = float(price_history[i]['close_price'])
            changes.append(curr_close - prev_close)

        if len(changes) < period:
            return None

        recent_changes = changes[-period:]
        avg_gain = sum(change for change in recent_changes if change > 0) / period
        avg_loss = abs(sum(change for change in recent_changes if change < 0)) / period

        if avg_loss == 0:
            return 100

        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _calculate_sma(self, price_history: List[Dict], period: int) -> Optional[float]:
        if len(price_history) < period:
            return None
        recent_prices = [float(p['close_price']) for p in price_history[-period:]]
        return sum(recent_prices) / len(recent_prices)

    def _calculate_ema(self, price_history: List[Dict], period: int) -> Optional[float]:
        if len(price_history) < period:
            return None
        prices = [float(p['close_price']) for p in price_history]
        multiplier = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        for i in range(period, len(prices)):
            ema = (prices[i] * multiplier) + (ema * (1 - multiplier))
        return ema

    def _calculate_volatility(self, price_history: List[Dict], period: int) -> Optional[float]:
        if len(price_history) < period + 1:
            return None

        returns = []
        for i in range(1, len(price_history)):
            prev_close = float(price_history[i - 1]['close_price'])
            curr_close = float(price_history[i]['close_price'])
            daily_return = (curr_close - prev_close) / prev_close
            returns.append(daily_return)

        if len(returns) < period:
            return None

        recent_returns = returns[-period:]
        mean_return = sum(recent_returns) / len(recent_returns)
        variance = sum((r - mean_return) ** 2 for r in recent_returns) / len(recent_returns)
        volatility = math.sqrt(variance)

        return volatility * math.sqrt(252) * 100

    def _calculate_avg_volume(self, price_history: List[Dict], period: int) -> Optional[float]:
        if len(price_history) < period:
            return None
        recent_volumes = [float(p.get('volume', 0)) for p in price_history[-period:]]
        return sum(recent_volumes) / len(recent_volumes)

    def _calculate_price_change(self, price_history: List[Dict], days: int) -> Optional[float]:
        if len(price_history) < days + 1:
            return None
        current_price = float(price_history[-1]['close_price'])
        past_price = float(price_history[-days - 1]['close_price'])
        return ((current_price - past_price) / past_price) * 100

    def _calculate_distance_from_high(self, price_history: List[Dict], period: int) -> Optional[float]:
        if len(price_history) < period:
            return None
        recent_highs = [float(p['high_price']) for p in price_history[-period:]]
        period_high = max(recent_highs)
        current_price = float(price_history[-1]['close_price'])
        return ((current_price - period_high) / period_high) * 100

    def _calculate_distance_from_low(self, price_history: List[Dict], period: int) -> Optional[float]:
        if len(price_history) < period:
            return None
        recent_lows = [float(p['low_price']) for p in price_history[-period:]]
        period_low = min(recent_lows)
        current_price = float(price_history[-1]['close_price'])
        return ((current_price - period_low) / period_low) * 100
