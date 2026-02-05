"""
Metrics service - manages available metrics and their definitions for YodaBuffett Screener

Provides comprehensive metric definitions and handles metric availability based on
actual data in the YodaBuffett database.
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import date

from app.core.database import DatabaseManager
from app.schemas.screener import MetricDefinition
from app.services.metric_calculator import MetricCalculator

logger = logging.getLogger(__name__)


class MetricsService:
    """Service for managing available screening metrics and calculations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.metric_calculator = MetricCalculator(db_manager)
        
        # Hardcoded metric definitions based on YodaBuffett data structure
        self._metric_definitions = self._initialize_metric_definitions()
    
    def _initialize_metric_definitions(self) -> Dict[str, Dict]:
        """Initialize comprehensive metric definitions based on YodaBuffett data"""
        
        return {
            # === FUNDAMENTAL METRICS ===
            'pe_ratio': {
                'name': 'P/E Ratio',
                'description': 'Price-to-Earnings ratio (Price per share / Earnings per share)',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
                'calculation': 'price / eps'
            },
            'pb_ratio': {
                'name': 'P/B Ratio', 
                'description': 'Price-to-Book ratio (Market Cap / Total Equity)',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
                'calculation': 'market_cap / book_value'
            },
            'ps_ratio': {
                'name': 'P/S Ratio',
                'description': 'Price-to-Sales ratio (Market Cap / Total Revenue)',
                'category': 'fundamental',
                'data_type': 'ratio', 
                'unit': 'x',
                'is_relative': True,
                'calculation': 'market_cap / revenue'
            },
            'ev_ebitda': {
                'name': 'EV/EBITDA',
                'description': 'Enterprise Value to EBITDA ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
                'calculation': 'enterprise_value / ebitda'
            },
            'roe': {
                'name': 'ROE',
                'description': 'Return on Equity (Net Income / Total Equity)',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'net_income / equity * 100'
            },
            'roa': {
                'name': 'ROA',
                'description': 'Return on Assets (Net Income / Total Assets)',
                'category': 'fundamental',
                'data_type': 'percentage', 
                'unit': '%',
                'is_relative': True,
                'calculation': 'net_income / assets * 100'
            },
            'current_ratio': {
                'name': 'Current Ratio',
                'description': 'Current Assets / Current Liabilities',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
                'calculation': 'current_assets / current_liabilities'
            },
            'debt_to_equity': {
                'name': 'Debt/Equity',
                'description': 'Total Debt to Equity ratio',
                'category': 'fundamental',
                'data_type': 'ratio',
                'unit': 'x',
                'is_relative': True,
                'calculation': 'total_debt / equity'
            },
            'gross_margin': {
                'name': 'Gross Margin',
                'description': 'Gross Profit Margin percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'gross_profit / revenue * 100'
            },
            'operating_margin': {
                'name': 'Operating Margin', 
                'description': 'Operating Income Margin percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'operating_income / revenue * 100'
            },
            'net_margin': {
                'name': 'Net Margin',
                'description': 'Net Income Margin percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'net_income / revenue * 100'
            },
            'revenue_growth_yoy': {
                'name': 'Revenue Growth (YoY)',
                'description': 'Year-over-Year Revenue Growth percentage',
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': '(current_revenue - prior_revenue) / prior_revenue * 100'
            },
            'earnings_growth_yoy': {
                'name': 'Earnings Growth (YoY)',
                'description': 'Year-over-Year Earnings Growth percentage', 
                'category': 'fundamental',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': '(current_earnings - prior_earnings) / prior_earnings * 100'
            },
            
            # === MARKET METRICS ===
            'market_cap': {
                'name': 'Market Cap',
                'description': 'Market Capitalization (Price × Shares Outstanding)',
                'category': 'market',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'price * shares_outstanding'
            },
            'enterprise_value': {
                'name': 'Enterprise Value',
                'description': 'Market Cap + Total Debt - Cash',
                'category': 'market',
                'data_type': 'currency',
                'unit': 'SEK', 
                'is_relative': True,
                'calculation': 'market_cap + debt - cash'
            },
            'price': {
                'name': 'Price',
                'description': 'Current stock price',
                'category': 'market',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'close_price'
            },
            'dividend_yield': {
                'name': 'Dividend Yield',
                'description': 'Annual dividend per share / Price per share',
                'category': 'market',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'annual_dividend / price * 100'
            },
            
            # === TECHNICAL METRICS ===
            'rsi_14': {
                'name': 'RSI (14)',
                'description': 'Relative Strength Index (14-day)',
                'category': 'technical',
                'data_type': 'number',
                'unit': '',
                'is_relative': False,
                'calculation': 'rsi_14_day'
            },
            'rsi_30': {
                'name': 'RSI (30)',
                'description': 'Relative Strength Index (30-day)', 
                'category': 'technical',
                'data_type': 'number',
                'unit': '',
                'is_relative': False,
                'calculation': 'rsi_30_day'
            },
            'sma_20': {
                'name': 'SMA 20',
                'description': 'Simple Moving Average (20-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'sma_20_day'
            },
            'sma_50': {
                'name': 'SMA 50',
                'description': 'Simple Moving Average (50-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'sma_50_day'
            },
            'sma_200': {
                'name': 'SMA 200',
                'description': 'Simple Moving Average (200-day)',
                'category': 'technical',
                'data_type': 'currency', 
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'sma_200_day'
            },
            'ema_12': {
                'name': 'EMA 12',
                'description': 'Exponential Moving Average (12-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'ema_12_day'
            },
            'ema_26': {
                'name': 'EMA 26',
                'description': 'Exponential Moving Average (26-day)',
                'category': 'technical',
                'data_type': 'currency',
                'unit': 'SEK',
                'is_relative': True,
                'calculation': 'ema_26_day'
            },
            'volatility_20d': {
                'name': 'Volatility (20d)',
                'description': '20-day Price Volatility (annualized)',
                'category': 'technical',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'std_dev_returns_20d_annualized'
            },
            'volatility_60d': {
                'name': 'Volatility (60d)',
                'description': '60-day Price Volatility (annualized)',
                'category': 'technical',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': True,
                'calculation': 'std_dev_returns_60d_annualized'
            },
            'beta': {
                'name': 'Beta',
                'description': 'Beta vs market (volatility vs benchmark)',
                'category': 'technical',
                'data_type': 'ratio',
                'unit': '',
                'is_relative': True,
                'calculation': 'correlation_vs_market'
            },
            'avg_volume_20d': {
                'name': 'Avg Volume (20d)',
                'description': '20-day Average Trading Volume',
                'category': 'technical',
                'data_type': 'number',
                'unit': 'shares',
                'is_relative': True,
                'calculation': 'avg_volume_20d'
            },
            
            # === DERIVED METRICS ===
            'price_change_1d': {
                'name': 'Price Change (1d)',
                'description': '1-day Price Change percentage',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
                'calculation': 'price_change_1_day'
            },
            'price_change_5d': {
                'name': 'Price Change (5d)',
                'description': '5-day Price Change percentage',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
                'calculation': 'price_change_5_day'
            },
            'price_change_20d': {
                'name': 'Price Change (20d)',
                'description': '20-day Price Change percentage',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
                'calculation': 'price_change_20_day'
            },
            'distance_from_52w_high': {
                'name': 'Distance from 52W High',
                'description': 'Distance from 52-week high (negative percentage)',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
                'calculation': '(price - 52w_high) / 52w_high * 100'
            },
            'distance_from_52w_low': {
                'name': 'Distance from 52W Low',
                'description': 'Distance from 52-week low (positive percentage)',
                'category': 'derived',
                'data_type': 'percentage',
                'unit': '%',
                'is_relative': False,
                'calculation': '(price - 52w_low) / 52w_low * 100'
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
                # Apply filters
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
                    source_type='calculated'  # All metrics are calculated from raw data
                )
                metrics.append(metric)
            
            # Sort by category then name
            metrics.sort(key=lambda x: (x.category, x.name))
            
            logger.info(f"Retrieved {len(metrics)} available metrics (filters: category={category}, data_type={data_type}, relative_only={relative_only})")
            return metrics
            
        except Exception as e:
            logger.error(f"Error retrieving metrics: {e}")
            raise
    
    async def get_metric_definition(self, metric_id: str) -> Optional[MetricDefinition]:
        """Get detailed definition for a specific metric"""
        
        try:
            if metric_id not in self._metric_definitions:
                logger.warning(f"Metric {metric_id} not found in definitions")
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
        
        try:
            categories = set()
            for definition in self._metric_definitions.values():
                categories.add(definition['category'])
            
            sorted_categories = sorted(list(categories))
            logger.info(f"Retrieved {len(sorted_categories)} metric categories: {sorted_categories}")
            return sorted_categories
            
        except Exception as e:
            logger.error(f"Error retrieving categories: {e}")
            raise
    
    async def calculate_metrics_for_symbols(
        self,
        symbols: List[str],
        metrics: List[str],
        as_of_date: date
    ) -> Dict[str, Dict[str, Any]]:
        """
        Calculate metric values for given symbols as of a specific date
        
        Returns: {symbol: {metric_id: value}}
        """
        
        if not symbols or not metrics:
            return {}
        
        logger.info(f"Calculating {len(metrics)} metrics for {len(symbols)} symbols as of {as_of_date}")
        
        try:
            # Separate metrics by type for efficient calculation
            fundamental_metrics = []
            technical_metrics = []
            
            for metric_id in metrics:
                if metric_id in self._metric_definitions:
                    category = self._metric_definitions[metric_id]['category']
                    if category == 'fundamental' or category == 'market':
                        fundamental_metrics.append(metric_id)
                    elif category == 'technical' or category == 'derived':
                        technical_metrics.append(metric_id)
                else:
                    logger.warning(f"Unknown metric: {metric_id}")
            
            results = {}
            
            # Initialize results for all symbols
            for symbol in symbols:
                results[symbol] = {}
            
            # Calculate fundamental metrics
            if fundamental_metrics:
                fundamental_results = await self.metric_calculator.calculate_fundamental_metrics(
                    symbols, as_of_date, fundamental_metrics
                )
                
                for symbol in symbols:
                    if symbol in fundamental_results:
                        results[symbol].update(fundamental_results[symbol])
            
            # Calculate technical metrics
            if technical_metrics:
                technical_results = await self.metric_calculator.calculate_technical_metrics(
                    symbols, as_of_date, technical_metrics
                )
                
                for symbol in symbols:
                    if symbol in technical_results:
                        results[symbol].update(technical_results[symbol])
            
            # Log summary
            successful_calculations = sum(1 for symbol_data in results.values() 
                                        if any(v is not None for v in symbol_data.values()))
            
            logger.info(f"Metric calculation completed: {successful_calculations}/{len(symbols)} symbols had data")
            
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
        """Get list of companies available for screening with basic filtering"""
        
        try:
            # Base query to get active companies
            query = """
            SELECT 
                c.company_name,
                c.primary_ticker as symbol,
                c.yahoo_symbol,
                c.sector,
                c.industry,
                c.currency,
                c.market_cap_usd,
                c.primary_exchange
            FROM company_master c
            WHERE c.listing_status = 'active'
            AND c.primary_ticker IS NOT NULL
            """
            
            params = []
            
            # Add market cap filter
            if min_market_cap:
                query += " AND c.market_cap_usd >= $1"
                params.append(min_market_cap)
            
            # Add sector exclusions
            if exclude_sectors:
                param_num = len(params) + 1
                query += f" AND c.sector NOT IN ({','.join([f'${i}' for i in range(param_num, param_num + len(exclude_sectors))])})"
                params.extend(exclude_sectors)
            
            # Ensure we have recent price data (basic data availability check)
            if as_of_date:
                param_num = len(params) + 1
                query += f"""
                AND EXISTS (
                    SELECT 1 FROM daily_price_data dpd 
                    WHERE dpd.symbol = c.primary_ticker 
                    AND dpd.date <= ${param_num}
                    AND dpd.date >= ${param_num} - INTERVAL '30 days'
                )
                """
                params.append(as_of_date)
            
            query += " ORDER BY c.market_cap_usd DESC NULLS LAST"
            
            companies = await self.db_manager.execute_query(query, *params)
            
            logger.info(f"Retrieved {len(companies)} companies for screening")
            return companies
            
        except Exception as e:
            logger.error(f"Error retrieving companies: {e}")
            raise