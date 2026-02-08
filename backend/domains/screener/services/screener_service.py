"""
Core screener service - executes screening queries with point-in-time data
"""
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ..database import DatabaseManager
from ..schemas import (
    ScreenerQuery, ScreenerResponse, ScreenerResult,
    ResultSummary, Company
)
from .query_builder import QueryBuilder
from .metrics_service import MetricsService

logger = logging.getLogger(__name__)


class ScreenerService:
    """Service for executing stock screening queries"""

    def __init__(self, db_manager: DatabaseManager, metrics_service: MetricsService):
        self.db_manager = db_manager
        self.metrics_service = metrics_service
        self.query_builder = QueryBuilder()

    async def execute_screen(self, query: ScreenerQuery) -> ScreenerResponse:
        """Execute a screening query with real YodaBuffett metrics"""
        start_time = datetime.now()

        try:
            screen_date = self._parse_screen_date(query.as_of_date)
            logger.info(f"Executing screen as of {screen_date} with {len(query.groups)} condition groups")

            companies = await self._get_screening_universe(screen_date)
            if not companies:
                return self._empty_response(query, screen_date, start_time)

            logger.info(f"Screening universe: {len(companies)} companies")

            all_required_metrics = self._get_required_metrics(query)
            logger.info(f"Required metrics: {all_required_metrics}")

            symbols = [comp['symbol'] for comp in companies]
            company_metrics = await self.metrics_service.calculate_metrics_for_symbols(
                symbols, all_required_metrics, screen_date
            )

            filtered_results = await self._apply_screening_conditions(
                companies, company_metrics, query
            )

            logger.info(f"After filtering: {len(filtered_results)} companies match conditions")

            if query.include_forward_returns and filtered_results:
                forward_returns = await self.metrics_service.metric_calculator.calculate_forward_returns(
                    [r['symbol'] for r in filtered_results],
                    screen_date,
                    query.include_forward_returns
                )
                for result in filtered_results:
                    symbol = result['symbol']
                    if symbol in forward_returns:
                        result['forward_returns'] = forward_returns[symbol]

            structured_results = self._convert_to_screener_results(filtered_results, query)
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

            logger.info(f"Screen executed: {len(structured_results)} matches in {execution_time:.2f}s")
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
            return date.today()

    async def _get_screening_universe(self, screen_date: date) -> List[Dict[str, Any]]:
        """Get universe of companies available for screening"""
        try:
            # Don't filter by market_cap - let all active companies through
            companies = await self.metrics_service.get_companies_for_screening(
                min_market_cap=None,
                as_of_date=screen_date
            )
            return companies
        except Exception as e:
            logger.error(f"Error getting screening universe: {e}")
            return []

    def _get_required_metrics(self, query: ScreenerQuery) -> List[str]:
        """Extract all required metrics from query conditions and display columns"""
        required_metrics = set()

        for group in query.groups:
            for condition in group.conditions:
                if isinstance(condition.left_operand, str) and not condition.left_operand.replace('.', '').isdigit():
                    required_metrics.add(condition.left_operand)
                if condition.is_relative and isinstance(condition.right_operand, str):
                    required_metrics.add(condition.right_operand)

        if query.columns:
            for column in query.columns:
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
            metrics = company_metrics.get(symbol, {})
            if not metrics:
                continue

            company_data = {**company, 'metrics': metrics}
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
            left_value = metrics.get(condition.left_operand)
            if left_value is None:
                return False

            if condition.is_relative:
                right_value = metrics.get(condition.right_operand)
                if right_value is None:
                    return False
            else:
                right_value = condition.right_operand

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
                if isinstance(right_value, list) and len(right_value) == 2:
                    return float(right_value[0]) <= float(left_value) <= float(right_value[1])
                return False
            elif condition.operator == 'in':
                if isinstance(right_value, list):
                    return left_value in right_value
                return False
            elif condition.operator == 'not_in':
                if isinstance(right_value, list):
                    return left_value not in right_value
                return False
            else:
                return False

        except (ValueError, TypeError) as e:
            logger.warning(f"Error evaluating condition: {e}")
            return False

    def _convert_to_screener_results(
        self,
        filtered_results: List[Dict[str, Any]],
        query: ScreenerQuery
    ) -> List[ScreenerResult]:
        """Convert filtered company data to ScreenerResult objects"""
        structured_results = []

        for i, company_data in enumerate(filtered_results):
            try:
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

                metrics = company_data.get('metrics', {})
                values = {}

                if query.columns:
                    for column in query.columns:
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
                    values = metrics.copy()

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

    def _empty_response(
        self,
        query: ScreenerQuery,
        screen_date: date,
        start_time: datetime
    ) -> ScreenerResponse:
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

        data = []
        for result in results:
            row = result.values.copy()
            if result.forward_returns:
                row.update({f"fwd_{k}": v for k, v in result.forward_returns.items()})
            data.append(row)

        df = pd.DataFrame(data)
        numeric_columns = df.select_dtypes(include=['number']).columns
        averages = df[numeric_columns].mean().to_dict()
        medians = df[numeric_columns].median().to_dict()

        win_rates = {}
        sharpe_ratios = {}

        forward_return_cols = [col for col in df.columns if col.startswith('fwd_')]
        for col in forward_return_cols:
            if col in df.columns and not df[col].isna().all():
                returns = df[col].dropna()
                win_rates[col] = (returns > 0).mean() * 100
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
