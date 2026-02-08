"""
Metrics service - manages available metrics and their definitions
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import date

from ..database import DatabaseManager
from ..schemas import MetricDefinition
from .metric_calculator import MetricCalculator

logger = logging.getLogger(__name__)


class MetricsService:
    """Service for managing available screening metrics and calculations"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.metric_calculator = MetricCalculator(db_manager)
        self._metric_definitions = self._initialize_metric_definitions()

    def _initialize_metric_definitions(self) -> Dict[str, Dict]:
        """Initialize comprehensive metric definitions based on YodaBuffett data"""
        return {
            # === FUNDAMENTAL METRICS ===
            'pe_ratio': {
                'name': 'P/E Ratio',
                'description': 'Price-to-Earnings ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
            },
            'pb_ratio': {
                'name': 'P/B Ratio',
                'description': 'Price-to-Book ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
            },
            'ps_ratio': {
                'name': 'P/S Ratio',
                'description': 'Price-to-Sales ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
            },
            'ev_ebitda': {
                'name': 'EV/EBITDA',
                'description': 'Enterprise Value to EBITDA ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
            },
            'roe': {
                'name': 'ROE',
                'description': 'Return on Equity',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },
            'roa': {
                'name': 'ROA',
                'description': 'Return on Assets',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },
            'current_ratio': {
                'name': 'Current Ratio',
                'description': 'Current Assets / Current Liabilities',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
            },
            'debt_to_equity': {
                'name': 'Debt/Equity',
                'description': 'Total Debt to Equity ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
            },
            'gross_margin': {
                'name': 'Gross Margin',
                'description': 'Gross Profit Margin percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },
            'operating_margin': {
                'name': 'Operating Margin',
                'description': 'Operating Income Margin percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },
            'net_margin': {
                'name': 'Net Margin',
                'description': 'Net Income Margin percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },

            # === MARKET METRICS ===
            'market_cap': {
                'name': 'Market Cap',
                'description': 'Market Capitalization',
                'category': 'market',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'enterprise_value': {
                'name': 'Enterprise Value',
                'description': 'Market Cap + Total Debt - Cash',
                'category': 'market',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'price': {
                'name': 'Price',
                'description': 'Current stock price',
                'category': 'market',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },

            # === TECHNICAL METRICS ===
            'rsi_14': {
                'name': 'RSI (14)',
                'description': 'Relative Strength Index (14-day)',
                'category': 'technical',
                'data_type': 'number',
                'unit': '',
                'is_relative': False,
            },
            'rsi_30': {
                'name': 'RSI (30)',
                'description': 'Relative Strength Index (30-day)',
                'category': 'technical',
                'data_type': 'number',
                'unit': '',
                'is_relative': False,
            },
            'sma_20': {
                'name': 'SMA 20',
                'description': 'Simple Moving Average (20-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'sma_50': {
                'name': 'SMA 50',
                'description': 'Simple Moving Average (50-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'sma_200': {
                'name': 'SMA 200',
                'description': 'Simple Moving Average (200-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'ema_12': {
                'name': 'EMA 12',
                'description': 'Exponential Moving Average (12-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'ema_26': {
                'name': 'EMA 26',
                'description': 'Exponential Moving Average (26-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
            },
            'volatility_20d': {
                'name': 'Volatility (20d)',
                'description': '20-day Price Volatility (annualized)',
                'category': 'technical',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },
            'volatility_60d': {
                'name': 'Volatility (60d)',
                'description': '60-day Price Volatility (annualized)',
                'category': 'technical',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
            },
            'avg_volume_20d': {
                'name': 'Avg Volume (20d)',
                'description': '20-day Average Trading Volume',
                'category': 'technical',
                'data_type': 'number',
                'unit': 'shares',
                'is_relative': True,
            },

            # === DERIVED METRICS ===
            'price_change_1d': {
                'name': 'Price Change (1d)',
                'description': '1-day Price Change percentage',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
            },
            'price_change_5d': {
                'name': 'Price Change (5d)',
                'description': '5-day Price Change percentage',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
            },
            'price_change_20d': {
                'name': 'Price Change (20d)',
                'description': '20-day Price Change percentage',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
            },
            'distance_from_52w_high': {
                'name': 'Distance from 52W High',
                'description': 'Distance from 52-week high',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
            },
            'distance_from_52w_low': {
                'name': 'Distance from 52W Low',
                'description': 'Distance from 52-week low',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
            },

            # === DIMENSION SCORES (0-100 scale) ===
            'value_score': {
                'name': 'Value Score',
                'description': 'Measures how undervalued a company is relative to its fundamentals and peers. Higher = more undervalued.',
                'category': 'dimension',
                'data_type': 'score',
                'unit': '',
                'is_relative': True,
            },
            'momentum_score': {
                'name': 'Momentum Score',
                'description': 'Captures price and volume trends indicating directional strength. Higher = stronger positive momentum.',
                'category': 'dimension',
                'data_type': 'score',
                'unit': '',
                'is_relative': True,
            },
            'quality_score': {
                'name': 'Quality Score',
                'description': 'Assesses financial health, profitability, and operational efficiency. Higher = stronger business.',
                'category': 'dimension',
                'data_type': 'score',
                'unit': '',
                'is_relative': True,
            },
            'sentiment_score': {
                'name': 'Sentiment Score',
                'description': 'Analyzes communication patterns and document anomalies using AI. Higher = more positive sentiment.',
                'category': 'dimension',
                'data_type': 'score',
                'unit': '',
                'is_relative': True,
            },
            'risk_score': {
                'name': 'Risk Score',
                'description': 'Quantifies downside exposure (volatility, drawdowns, leverage). Higher = LOWER risk.',
                'category': 'dimension',
                'data_type': 'score',
                'unit': '',
                'is_relative': True,
            },
            'composite_score': {
                'name': 'Composite Score',
                'description': 'Weighted combination of all dimension scores. Higher = overall better investment profile.',
                'category': 'dimension',
                'data_type': 'score',
                'unit': '',
                'is_relative': True,
            },

            # === DIMENSION PERCENTILES (0-100 rank) ===
            'value_percentile': {
                'name': 'Value Percentile',
                'description': 'Percentile rank of Value Score within universe. 90 = better than 90% of companies.',
                'category': 'dimension',
                'data_type': 'percentile',
                'unit': '%ile',
                'is_relative': True,
            },
            'momentum_percentile': {
                'name': 'Momentum Percentile',
                'description': 'Percentile rank of Momentum Score within universe.',
                'category': 'dimension',
                'data_type': 'percentile',
                'unit': '%ile',
                'is_relative': True,
            },
            'quality_percentile': {
                'name': 'Quality Percentile',
                'description': 'Percentile rank of Quality Score within universe.',
                'category': 'dimension',
                'data_type': 'percentile',
                'unit': '%ile',
                'is_relative': True,
            },
            'sentiment_percentile': {
                'name': 'Sentiment Percentile',
                'description': 'Percentile rank of Sentiment Score within universe.',
                'category': 'dimension',
                'data_type': 'percentile',
                'unit': '%ile',
                'is_relative': True,
            },
            'risk_percentile': {
                'name': 'Risk Percentile',
                'description': 'Percentile rank of Risk Score within universe.',
                'category': 'dimension',
                'data_type': 'percentile',
                'unit': '%ile',
                'is_relative': True,
            }
        }

    async def get_available_metrics(
        self,
        category: Optional[str] = None,
        data_type: Optional[str] = None,
        relative_only: bool = False
    ) -> List[MetricDefinition]:
        """Get available metrics with optional filtering"""
        try:
            metrics = []

            for metric_id, definition in self._metric_definitions.items():
                if category and definition['category'] != category:
                    continue
                if data_type and definition['data_type'] != data_type:
                    continue
                if relative_only and not definition['is_relative']:
                    continue

                metric = MetricDefinition(
                    id=metric_id,
                    name=definition['name'],
                    description=definition['description'],
                    category=definition['category'],
                    data_type=definition['data_type'],
                    unit=definition.get('unit'),
                    is_relative=definition['is_relative'],
                    source_type='calculated'
                )
                metrics.append(metric)

            metrics.sort(key=lambda x: (x.category, x.name))
            logger.info(f"Retrieved {len(metrics)} available metrics")
            return metrics

        except Exception as e:
            logger.error(f"Error retrieving metrics: {e}")
            raise

    async def get_metric_definition(self, metric_id: str) -> Optional[MetricDefinition]:
        """Get detailed definition for a specific metric"""
        try:
            if metric_id not in self._metric_definitions:
                return None

            definition = self._metric_definitions[metric_id]
            return MetricDefinition(
                id=metric_id,
                name=definition['name'],
                description=definition['description'],
                category=definition['category'],
                data_type=definition['data_type'],
                unit=definition.get('unit'),
                is_relative=definition['is_relative'],
                source_type='calculated'
            )
        except Exception as e:
            logger.error(f"Error retrieving metric {metric_id}: {e}")
            raise

    async def get_metric_categories(self) -> List[str]:
        """Get list of available metric categories"""
        categories = set()
        for definition in self._metric_definitions.values():
            categories.add(definition['category'])
        return sorted(list(categories))

    async def calculate_metrics_for_symbols(
        self,
        symbols: List[str],
        metrics: List[str],
        as_of_date: date
    ) -> Dict[str, Dict[str, Any]]:
        """Calculate metric values for given symbols as of a specific date"""
        if not symbols or not metrics:
            return {}

        logger.info(f"Calculating {len(metrics)} metrics for {len(symbols)} symbols as of {as_of_date}")

        try:
            fundamental_metrics = []
            technical_metrics = []

            dimension_metrics = []

            for metric_id in metrics:
                if metric_id in self._metric_definitions:
                    category = self._metric_definitions[metric_id]['category']
                    if category in ('fundamental', 'market'):
                        fundamental_metrics.append(metric_id)
                    elif category in ('technical', 'derived'):
                        technical_metrics.append(metric_id)
                    elif category == 'dimension':
                        dimension_metrics.append(metric_id)

            results = {symbol: {} for symbol in symbols}

            if fundamental_metrics:
                fundamental_results = await self.metric_calculator.calculate_fundamental_metrics(
                    symbols, as_of_date, fundamental_metrics
                )
                for symbol in symbols:
                    if symbol in fundamental_results:
                        results[symbol].update(fundamental_results[symbol])

            if technical_metrics:
                technical_results = await self.metric_calculator.calculate_technical_metrics(
                    symbols, as_of_date, technical_metrics
                )
                for symbol in symbols:
                    if symbol in technical_results:
                        results[symbol].update(technical_results[symbol])

            if dimension_metrics:
                dimension_results = await self.metric_calculator.calculate_dimension_metrics(
                    symbols, as_of_date, dimension_metrics
                )
                for symbol in symbols:
                    if symbol in dimension_results:
                        results[symbol].update(dimension_results[symbol])

            return results

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            raise

    async def get_companies_for_screening(
        self,
        min_market_cap: Optional[float] = None,
        exclude_sectors: Optional[List[str]] = None,
        as_of_date: Optional[date] = None
    ) -> List[Dict[str, Any]]:
        """Get list of companies available for screening"""
        try:
            # Use DISTINCT ON to get one row per ticker (prefer ones with dimension scores)
            query = """
            SELECT DISTINCT ON (c.primary_ticker)
                c.id as company_id,
                c.company_name,
                c.primary_ticker as symbol,
                c.yahoo_symbol,
                c.sector,
                c.industry,
                c.currency,
                c.market_cap_usd,
                c.primary_exchange,
                (SELECT COUNT(*) FROM daily_dimension_scores ds WHERE ds.company_id = c.id) as dim_count
            FROM company_master c
            WHERE c.listing_status = 'active'
            AND c.primary_ticker IS NOT NULL
            """

            params = []
            param_idx = 1

            if min_market_cap:
                query += f" AND c.market_cap_usd >= ${param_idx}"
                params.append(min_market_cap)
                param_idx += 1

            if as_of_date:
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM daily_price_data dpd
                    WHERE dpd.symbol = c.primary_ticker
                    AND dpd.date <= ${param_idx}
                    AND dpd.date >= ${param_idx}::date - INTERVAL '30 days'
                )
                """
                params.append(as_of_date)
                param_idx += 1

            # ORDER BY must match DISTINCT ON, then prefer companies with dimension scores
            query += " ORDER BY c.primary_ticker, (SELECT COUNT(*) FROM daily_dimension_scores ds WHERE ds.company_id = c.id) DESC, c.market_cap_usd DESC NULLS LAST"

            companies = await self.db_manager.execute_query(query, *params)
            logger.info(f"Retrieved {len(companies)} companies for screening")
            return companies

        except Exception as e:
            logger.error(f"Error retrieving companies: {e}")
            raise
