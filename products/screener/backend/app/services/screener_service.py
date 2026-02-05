"""
Core screener service - executes screening queries with point-in-time data
"""
import asyncio
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import DatabaseManager
from app.models.screener import ScreenerResult as ScreenerResultModel
from app.schemas.screener import (
    ScreenerQuery, ScreenerResponse, ScreenerResult, 
    ResultSummary, Company, QueryCondition
)
from app.services.query_builder import QueryBuilder
from app.services.metrics_service import MetricsService

logger = logging.getLogger(__name__)


class ScreenerService:
    """Service for executing stock screening queries"""
    
    def __init__(self, db_manager: DatabaseManager, metrics_service: MetricsService):
        self.db_manager = db_manager
        self.metrics_service = metrics_service
        self.query_builder = QueryBuilder()
    
    async def execute_screen(
        self, 
        query: ScreenerQuery, 
        db_session: AsyncSession
    ) -> ScreenerResponse:
        """Execute a screening query with real YodaBuffett metrics"""
        start_time = datetime.now()
        
        try:
            # Determine the date for point-in-time screening
            screen_date = self._parse_screen_date(query.as_of_date)
            logger.info(f"Executing screen as of {screen_date} with {len(query.groups)} condition groups")
            
            # Check for cached results first
            if query.id:
                cached_response = await self.get_cached_results(str(query.id), screen_date, db_session)
                if cached_response:
                    logger.info(f"Returning cached results for query {query.id}")
                    return cached_response
            
            # Get universe of companies for screening
            companies = await self._get_screening_universe(screen_date)
            if not companies:
                logger.warning("No companies available for screening")
                return self._empty_response(query, screen_date, start_time)
            
            logger.info(f"Screening universe: {len(companies)} companies")
            
            # Get all required metrics (from conditions + display columns)
            all_required_metrics = self._get_required_metrics(query)
            logger.info(f"Required metrics: {all_required_metrics}")
            
            # Calculate metrics for all companies using the new metric calculator
            symbols = [comp['symbol'] for comp in companies]
            company_metrics = await self.metrics_service.calculate_metrics_for_symbols(
                symbols, all_required_metrics, screen_date
            )
            
            # Apply screening conditions to filter companies
            filtered_results = await self._apply_screening_conditions(
                companies, company_metrics, query
            )
            
            logger.info(f"After filtering: {len(filtered_results)} companies match conditions")
            
            # Calculate forward returns if requested
            if query.includeForwardReturns and filtered_results:
                forward_returns = await self.metrics_service.metric_calculator.calculate_forward_returns(
                    [r['symbol'] for r in filtered_results],
                    screen_date,
                    query.includeForwardReturns
                )
                
                # Add forward returns to results
                for result in filtered_results:
                    symbol = result['symbol']
                    if symbol in forward_returns:
                        result['forward_returns'] = forward_returns[symbol]
            
            # Convert to structured ScreenerResult objects
            structured_results = self._convert_to_screener_results(filtered_results, query)
            
            # Generate summary statistics
            summary = self._calculate_summary(structured_results)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            response = ScreenerResponse(
                query=query,
                results=structured_results,
                summary=summary,
                executionTime=execution_time,
                asOfDate=screen_date.isoformat(),
                totalMatches=len(structured_results)
            )
            
            # Cache results for future use
            await self._cache_results(query, response, db_session)
            
            logger.info(f"Screen executed successfully: {len(structured_results)} matches in {execution_time:.2f}s")
            return response
            
        except Exception as e:
            logger.error(f"Screening execution failed: {e}", exc_info=True)
            raise
    
    def _parse_screen_date(self, as_of_date: Optional[str]) -> date:
        """Parse and validate the screening date"""
        if as_of_date:
            try:
                return datetime.fromisoformat(as_of_date).date()
            except ValueError:
                raise ValueError(f"Invalid date format: {as_of_date}")
        else:
            # Use latest available data date
            return date.today()
    
    async def _get_screening_universe(self, screen_date: date) -> List[Dict[str, Any]]:
        """Get universe of companies available for screening"""
        try:
            # Get companies with basic filtering and data availability checks
            companies = await self.metrics_service.get_companies_for_screening(
                min_market_cap=1000000,  # 1M SEK minimum market cap
                exclude_sectors=None,
                as_of_date=screen_date
            )
            
            logger.info(f"Screening universe: {len(companies)} companies available")
            return companies
            
        except Exception as e:
            logger.error(f"Error getting screening universe: {e}")
            return []
    
    def _get_required_metrics(self, query: ScreenerQuery) -> List[str]:
        """Extract all required metrics from query conditions and display columns"""
        required_metrics = set()
        
        # Add metrics from conditions
        for group in query.groups:
            for condition in group.conditions:
                # Add left operand if it's a metric
                if isinstance(condition.left_operand, str) and not condition.left_operand.replace('.', '').isdigit():
                    required_metrics.add(condition.left_operand)
                # If it's a relative comparison (right operand is also a metric)
                if condition.is_relative and isinstance(condition.right_operand, str):
                    required_metrics.add(condition.right_operand)
        
        # Add metrics from display columns if specified
        if query.columns:
            for column in query.columns:
                # Columns are metric IDs directly (no prefix needed)
                required_metrics.add(column)
        
        return list(required_metrics)
    
    async def _apply_screening_conditions(
        self,
        companies: List[Dict[str, Any]],
        company_metrics: Dict[str, Dict[str, Any]],
        query: ScreenerQuery
    ) -> List[Dict[str, Any]]:
        """Apply screening conditions to filter companies"""
        filtered_companies = []
        
        for company in companies:
            symbol = company['symbol']
            
            # Get metrics for this company
            metrics = company_metrics.get(symbol, {})
            if not metrics:
                logger.debug(f"No metrics for {symbol}, excluding from results")
                continue
            
            # Combine company info with metrics
            company_data = {
                **company,
                'metrics': metrics
            }
            
            # Check if company passes all condition groups (OR logic between groups)
            passes_any_group = False
            
            for group in query.groups:
                if self._evaluate_condition_group(company_data, group):
                    passes_any_group = True
                    break
            
            if passes_any_group:
                filtered_companies.append(company_data)
        
        return filtered_companies
    
    def _evaluate_condition_group(self, company_data: Dict[str, Any], group) -> bool:
        """Evaluate all conditions in a group (AND logic within group)"""
        for condition in group.conditions:
            if not self._evaluate_condition(company_data, condition):
                return False
        return True
    
    def _evaluate_condition(self, company_data: Dict[str, Any], condition) -> bool:
        """Evaluate a single screening condition"""
        try:
            metrics = company_data.get('metrics', {})
            
            # Get left operand value
            left_value = metrics.get(condition.left_operand)
            if left_value is None:
                logger.debug(f"Missing metric {condition.left_operand} for {company_data.get('symbol')}")
                return False
            
            # Get right operand value
            if condition.is_relative:
                # Right operand is another metric
                right_value = metrics.get(condition.right_operand)
                if right_value is None:
                    logger.debug(f"Missing comparison metric {condition.right_operand} for {company_data.get('symbol')}")
                    return False
            else:
                # Right operand is a constant value
                right_value = condition.right_operand
            
            # Handle different operators
            if condition.operator == '>':
                return float(left_value) > float(right_value)
            elif condition.operator == '<':
                return float(left_value) < float(right_value)
            elif condition.operator == '>=':
                return float(left_value) >= float(right_value)
            elif condition.operator == '<=':
                return float(left_value) <= float(right_value)
            elif condition.operator == '=':
                return float(left_value) == float(right_value)
            elif condition.operator == '!=':
                return float(left_value) != float(right_value)
            elif condition.operator == 'between':
                # Right operand should be a list [min, max]
                if isinstance(right_value, list) and len(right_value) == 2:
                    return float(right_value[0]) <= float(left_value) <= float(right_value[1])
                return False
            elif condition.operator == 'in':
                # Check if value is in a list
                if isinstance(right_value, list):
                    return left_value in right_value
                return False
            elif condition.operator == 'not_in':
                if isinstance(right_value, list):
                    return left_value not in right_value
                return False
            else:
                logger.warning(f"Unknown operator: {condition.operator}")
                return False
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Error evaluating condition {condition.left_operand} {condition.operator} {condition.right_operand}: {e}")
            return False
    
    def _convert_to_screener_results(self, filtered_results: List[Dict[str, Any]], query: ScreenerQuery) -> List[ScreenerResult]:
        """Convert filtered company data to ScreenerResult objects"""
        structured_results = []
        
        for i, company_data in enumerate(filtered_results):
            try:
                # Create Company object
                company = Company(
                    id=str(company_data.get('company_id', i)),
                    symbol=company_data['symbol'],
                    name=company_data.get('company_name', company_data['symbol']),
                    market=company_data.get('primary_exchange', 'SE'),
                    sector=company_data.get('sector'),
                    industry=company_data.get('industry'),
                    market_cap=company_data.get('market_cap_usd'),
                    currency=company_data.get('currency', 'SEK')
                )
                
                # Extract metric values
                metrics = company_data.get('metrics', {})
                values = {}
                
                # Add requested display columns or all metrics
                if query.columns:
                    for column in query.columns:
                        # Direct metric lookup (column is metric ID)
                        if column in metrics:
                            values[column] = metrics.get(column)
                        elif column == 'company_name':
                            values['company_name'] = company.name
                        elif column == 'sector':
                            values['sector'] = company.sector
                        elif column == 'industry':
                            values['industry'] = company.industry
                        elif column == 'market_cap':
                            values['market_cap'] = company.market_cap
                else:
                    # Include all calculated metrics
                    values = metrics.copy()
                
                # Add forward returns if they exist
                forward_returns = company_data.get('forward_returns')
                
                result = ScreenerResult(
                    company=company,
                    values=values,
                    rank=i + 1,
                    forwardReturns=forward_returns
                )
                
                structured_results.append(result)
                
            except Exception as e:
                logger.error(f"Error converting company data to result: {e}")
                continue
        
        return structured_results
    
    def _empty_response(self, query: ScreenerQuery, screen_date: date, start_time: datetime) -> ScreenerResponse:
        """Create empty response when no data available"""
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return ScreenerResponse(
            query=query,
            results=[],
            summary=ResultSummary(
                count=0,
                averages={},
                medians={},
                winRates={},
                sharpeRatios={}
            ),
            executionTime=execution_time,
            asOfDate=screen_date.isoformat(),
            totalMatches=0
        )
    
    async def _execute_screening_query(self, sql_query: str) -> List[ScreenerResult]:
        """Execute the generated SQL query and convert to results"""
        try:
            rows = await self.db_manager.execute_query(sql_query)
            results = []
            
            for row in rows:
                # Convert database row to ScreenerResult
                company = Company(
                    id=str(row['company_id']),
                    symbol=row['symbol'],
                    name=row['company_name'],
                    market=row.get('market', 'SE'),  # Default to Swedish market
                    sector=row.get('sector'),
                    industry=row.get('industry'),
                    market_cap=row.get('market_cap'),
                    currency=row.get('currency', 'SEK')
                )
                
                # Extract metric values (excluding company info)
                values = {}
                for key, value in row.items():
                    if key not in ['company_id', 'symbol', 'company_name', 'market', 'sector', 'industry', 'market_cap', 'currency']:
                        values[key] = value
                
                result = ScreenerResult(
                    company=company,
                    values=values,
                    rank=len(results) + 1  # Will be recalculated if needed
                )
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    async def _add_forward_returns(
        self, 
        results: List[ScreenerResult], 
        screen_date: date, 
        periods: List[str]
    ) -> List[ScreenerResult]:
        """Add forward returns to screening results"""
        if not results:
            return results
        
        # Extract symbols for batch processing
        symbols = [result.company.symbol for result in results]
        
        # Calculate forward returns for all symbols and periods
        forward_returns_data = await self._calculate_forward_returns_batch(
            symbols, screen_date, periods
        )
        
        # Add forward returns to each result
        for result in results:
            symbol = result.company.symbol
            if symbol in forward_returns_data:
                result.forwardReturns = forward_returns_data[symbol]
        
        return results
    
    async def _calculate_forward_returns_batch(
        self, 
        symbols: List[str], 
        screen_date: date, 
        periods: List[str]
    ) -> Dict[str, Dict[str, float]]:
        """Calculate forward returns for multiple symbols efficiently"""
        
        # Build period mappings
        period_days = {
            '1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365, '2Y': 730
        }
        
        # Create query to get prices at entry date and future dates
        query = """
        WITH entry_prices AS (
            SELECT 
                symbol,
                close as entry_price
            FROM market_data_history 
            WHERE symbol = ANY($1::text[])
            AND date <= $2
            AND close IS NOT NULL
            ORDER BY symbol, date DESC
        ),
        entry_prices_latest AS (
            SELECT DISTINCT ON (symbol) 
                symbol, entry_price
            FROM entry_prices
        )
        SELECT 
            ep.symbol,
            ep.entry_price,
            mdh.date as exit_date,
            mdh.close as exit_price,
            CASE 
        """
        
        # Add CASE conditions for each period
        case_conditions = []
        for period, days in period_days.items():
            if period in periods:
                case_conditions.append(f"""
                    WHEN mdh.date BETWEEN $2::date + INTERVAL '{days} days' - INTERVAL '5 days' 
                    AND $2::date + INTERVAL '{days} days' + INTERVAL '5 days'
                    THEN '{period}'
                """)
        
        query += " ".join(case_conditions)
        query += """
                ELSE NULL
            END as period_type
        FROM entry_prices_latest ep
        JOIN market_data_history mdh ON ep.symbol = mdh.symbol
        WHERE mdh.date > $2
        AND mdh.close IS NOT NULL
        """
        query += f"AND mdh.date <= $2::date + INTERVAL '{max(period_days[p] for p in periods if p in period_days)} days' + INTERVAL '5 days'"
        query += " ORDER BY ep.symbol, mdh.date"
        
        try:
            rows = await self.db_manager.execute_query(
                query, symbols, screen_date
            )
            
            # Process results into forward returns
            returns_data = {}
            
            for row in rows:
                symbol = row['symbol']
                period = row['period_type']
                
                if not period or not row['entry_price'] or not row['exit_price']:
                    continue
                
                # Calculate return
                return_pct = (row['exit_price'] - row['entry_price']) / row['entry_price'] * 100
                
                if symbol not in returns_data:
                    returns_data[symbol] = {}
                
                # Use the closest date for each period (if multiple matches)
                if period not in returns_data[symbol]:
                    returns_data[symbol][period] = return_pct
            
            return returns_data
            
        except Exception as e:
            logger.error(f"Forward returns calculation failed: {e}")
            return {}
    
    def _calculate_summary(self, results: List[ScreenerResult]) -> ResultSummary:
        """Calculate summary statistics for screening results"""
        if not results:
            return ResultSummary(
                count=0,
                averages={},
                medians={},
                winRates={},
                sharpeRatios={}
            )
        
        # Convert results to DataFrame for easier calculation
        data = []
        for result in results:
            row = result.values.copy()
            if result.forwardReturns:
                row.update({f"fwd_{k}": v for k, v in result.forwardReturns.items()})
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Calculate averages and medians for numeric columns
        numeric_columns = df.select_dtypes(include=['number']).columns
        averages = df[numeric_columns].mean().to_dict()
        medians = df[numeric_columns].median().to_dict()
        
        # Calculate win rates for forward returns
        win_rates = {}
        sharpe_ratios = {}
        
        forward_return_cols = [col for col in df.columns if col.startswith('fwd_')]
        for col in forward_return_cols:
            if col in df.columns and not df[col].isna().all():
                returns = df[col].dropna()
                win_rates[col] = (returns > 0).mean() * 100
                
                # Simple Sharpe ratio (assuming risk-free rate = 0)
                if len(returns) > 1 and returns.std() > 0:
                    sharpe_ratios[col] = returns.mean() / returns.std()
                else:
                    sharpe_ratios[col] = 0
        
        return ResultSummary(
            count=len(results),
            averages=averages,
            medians=medians,
            winRates=win_rates,
            sharpeRatios=sharpe_ratios
        )
    
    async def _cache_results(
        self, 
        query: ScreenerQuery, 
        response: ScreenerResponse, 
        db_session: AsyncSession
    ) -> None:
        """Cache screening results for future retrieval"""
        try:
            # Only cache if query has an ID (saved query)
            if not query.id:
                return
            
            screen_date = datetime.fromisoformat(response.asOfDate).date()
            
            # Create cache entry
            cache_entry = ScreenerResultModel(
                query_id=query.id,
                as_of_date=screen_date,
                results_json=response.dict()['results'],
                summary_json=response.dict()['summary'],
                execution_time_ms=int(response.executionTime * 1000),
                total_matches=response.totalMatches
            )
            
            db_session.add(cache_entry)
            await db_session.commit()
            
        except Exception as e:
            logger.warning(f"Failed to cache results: {e}")
            # Don't fail the main operation if caching fails
    
    async def get_cached_results(
        self, 
        query_id: str, 
        as_of_date: date,
        db_session: AsyncSession
    ) -> Optional[ScreenerResponse]:
        """Retrieve cached screening results"""
        try:
            result = await db_session.execute(
                text("""
                    SELECT results_json, summary_json, execution_time_ms, total_matches, created_at
                    FROM screener_results 
                    WHERE query_id = :query_id AND as_of_date = :as_of_date
                    ORDER BY created_at DESC LIMIT 1
                """),
                {"query_id": query_id, "as_of_date": as_of_date}
            )
            
            row = result.fetchone()
            if not row:
                return None
            
            # Reconstruct response from cached data
            # This would need proper deserialization logic
            # For now, return None to force fresh calculation
            return None
            
        except Exception as e:
            logger.warning(f"Cache retrieval failed: {e}")
            return None