"""
Backtesting service - historical strategy validation and performance analysis
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import numpy as np

from ..database import DatabaseManager
from ..schemas import (
    BacktestRequest, BacktestResponse, BacktestResult, BacktestSummary,
    ScreenerResult
)
from .screener_service import ScreenerService

logger = logging.getLogger(__name__)


class BacktestService:
    """Service for backtesting screening strategies"""

    def __init__(self, db_manager: DatabaseManager, screener_service: ScreenerService):
        self.db_manager = db_manager
        self.screener_service = screener_service

    def validate_backtest_request(self, request: BacktestRequest) -> List[str]:
        """Validate backtest request parameters"""
        errors = []

        try:
            start_date = datetime.fromisoformat(request.start_date).date()
            end_date = datetime.fromisoformat(request.end_date).date()

            if start_date >= end_date:
                errors.append("Start date must be before end date")

            date_diff = (end_date - start_date).days
            if date_diff > 1825:
                errors.append("Backtest period cannot exceed 5 years")
            if date_diff < 30:
                errors.append("Backtest period must be at least 30 days")

            valid_periods = ['1W', '1M', '3M', '6M', '1Y', '2Y']
            for period in request.forward_periods:
                if period not in valid_periods:
                    errors.append(f"Invalid forward period: {period}")

            earliest_available = datetime(2021, 1, 1).date()
            if start_date < earliest_available:
                errors.append(f"Data not available before {earliest_available}")

        except ValueError as e:
            errors.append(f"Invalid date format: {e}")

        return errors

    async def execute_backtest(self, request: BacktestRequest) -> BacktestResponse:
        """Execute complete backtest of a screening strategy"""
        start_time = datetime.now()

        try:
            screening_dates = self._generate_screening_dates(
                request.start_date, request.end_date, request.frequency
            )

            logger.info(f"Running backtest on {len(screening_dates)} dates")

            results = []
            all_signals = 0

            for screen_date in screening_dates:
                screen_query = request.query.model_copy(deep=True)
                screen_query.as_of_date = screen_date.strftime('%Y-%m-%d')
                screen_query.include_forward_returns = request.forward_periods

                screen_response = await self.screener_service.execute_screen(screen_query)

                if screen_response.results:
                    period_result = self._analyze_period_results(
                        screen_response, screen_date, request.forward_periods
                    )
                    results.append(period_result)
                    all_signals += len(screen_response.results)

                logger.debug(f"Processed {screen_date}: {len(screen_response.results)} signals")

            summary = self._calculate_backtest_summary(results, all_signals, request.forward_periods)
            execution_time = (datetime.now() - start_time).total_seconds()

            return BacktestResponse(
                query=request.query,
                results=results,
                summary=summary,
                totalExecutionTime=execution_time
            )

        except Exception as e:
            logger.error(f"Error executing backtest: {e}")
            raise

    def _generate_screening_dates(
        self,
        start_date: str,
        end_date: str,
        frequency: str
    ) -> List[datetime]:
        """Generate list of dates for screening based on frequency"""
        start = datetime.fromisoformat(start_date).date()
        end = datetime.fromisoformat(end_date).date()

        dates = []

        if frequency == "monthly":
            current = start.replace(day=1)
            while current <= end:
                business_day = current
                while business_day.weekday() > 4:
                    business_day += timedelta(days=1)
                if business_day <= end:
                    dates.append(datetime.combine(business_day, datetime.min.time()))
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)

        elif frequency == "weekly":
            current = start
            while current.weekday() != 0:
                current += timedelta(days=1)
            while current <= end:
                dates.append(datetime.combine(current, datetime.min.time()))
                current += timedelta(weeks=1)

        else:  # daily
            current = start
            while current <= end:
                if current.weekday() < 5:
                    dates.append(datetime.combine(current, datetime.min.time()))
                current += timedelta(days=1)

        return dates

    def _analyze_period_results(
        self,
        screen_response,
        screen_date: datetime,
        forward_periods: List[str]
    ) -> BacktestResult:
        """Analyze results for a single screening period"""
        avg_returns = {}
        win_rates = {}
        sharpe_ratios = {}

        for period in forward_periods:
            returns = []
            for result in screen_response.results:
                if result.forward_returns and period in result.forward_returns:
                    returns.append(result.forward_returns[period])

            if returns:
                returns_array = np.array(returns)
                avg_returns[period] = float(np.mean(returns_array))
                win_rates[period] = float(np.mean(returns_array > 0))

                excess_returns = returns_array - 0.02 / 12
                if np.std(excess_returns) > 0:
                    sharpe_ratios[period] = float(np.mean(excess_returns) / np.std(excess_returns))
                else:
                    sharpe_ratios[period] = 0.0
            else:
                avg_returns[period] = 0.0
                win_rates[period] = 0.0
                sharpe_ratios[period] = 0.0

        top_performers = []
        if screen_response.results:
            sort_period = forward_periods[0] if forward_periods else None
            if sort_period:
                sorted_results = sorted(
                    screen_response.results,
                    key=lambda x: (x.forward_returns or {}).get(sort_period, 0),
                    reverse=True
                )
                top_performers = sorted_results[:5]
            else:
                top_performers = screen_response.results[:5]

        return BacktestResult(
            date=screen_date.strftime('%Y-%m-%d'),
            matches=len(screen_response.results),
            avgReturn=avg_returns,
            winRate=win_rates,
            sharpeRatio=sharpe_ratios,
            topPerformers=top_performers
        )

    def _calculate_backtest_summary(
        self,
        results: List[BacktestResult],
        total_signals: int,
        forward_periods: List[str]
    ) -> BacktestSummary:
        """Calculate aggregate backtest performance summary"""
        if not results:
            return BacktestSummary(
                totalSignals=0,
                avgReturns={period: 0.0 for period in forward_periods},
                winRates={period: 0.0 for period in forward_periods},
                sharpeRatios={period: 0.0 for period in forward_periods},
                bestMonth={period: {"date": "", "return": 0.0} for period in forward_periods},
                worstMonth={period: {"date": "", "return": 0.0} for period in forward_periods},
                maxDrawdown=0.0
            )

        avg_returns = {}
        win_rates = {}
        sharpe_ratios = {}
        best_months = {}
        worst_months = {}

        for period in forward_periods:
            period_returns = []
            period_win_rates = []
            period_sharpes = []

            for result in results:
                if period in result.avg_return:
                    period_returns.append(result.avg_return[period])
                if period in result.win_rate:
                    period_win_rates.append(result.win_rate[period])
                if period in result.sharpe_ratio:
                    period_sharpes.append(result.sharpe_ratio[period])

            avg_returns[period] = float(np.mean(period_returns)) if period_returns else 0.0
            win_rates[period] = float(np.mean(period_win_rates)) if period_win_rates else 0.0
            sharpe_ratios[period] = float(np.mean(period_sharpes)) if period_sharpes else 0.0

            if period_returns:
                max_idx = np.argmax(period_returns)
                min_idx = np.argmin(period_returns)
                best_months[period] = {"date": results[max_idx].date, "return": period_returns[max_idx]}
                worst_months[period] = {"date": results[min_idx].date, "return": period_returns[min_idx]}
            else:
                best_months[period] = {"date": "", "return": 0.0}
                worst_months[period] = {"date": "", "return": 0.0}

        max_drawdown = 0.0
        if "1M" in avg_returns and results:
            monthly_returns = [r.avg_return.get("1M", 0) for r in results]
            cumulative_returns = np.cumprod([1 + r / 100 for r in monthly_returns])
            running_max = np.maximum.accumulate(cumulative_returns)
            drawdowns = (cumulative_returns - running_max) / running_max
            max_drawdown = float(abs(np.min(drawdowns))) if len(drawdowns) > 0 else 0.0

        return BacktestSummary(
            totalSignals=total_signals,
            avgReturns=avg_returns,
            winRates=win_rates,
            sharpeRatios=sharpe_ratios,
            bestMonth=best_months,
            worstMonth=worst_months,
            maxDrawdown=max_drawdown
        )

    def compare_backtests(
        self,
        backtest_1: BacktestResponse,
        backtest_2: BacktestResponse
    ) -> Dict[str, Any]:
        """Compare two backtest results"""
        comparison = {
            "performance_comparison": {},
            "risk_metrics": {},
            "recommendation": {}
        }

        for period in backtest_1.summary.avg_returns.keys():
            if period in backtest_2.summary.avg_returns:
                return_diff = (
                    backtest_1.summary.avg_returns[period] -
                    backtest_2.summary.avg_returns[period]
                )
                sharpe_diff = (
                    backtest_1.summary.sharpe_ratios[period] -
                    backtest_2.summary.sharpe_ratios[period]
                )
                comparison["performance_comparison"][period] = {
                    "return_difference": return_diff,
                    "sharpe_difference": sharpe_diff,
                    "winner": "strategy_1" if return_diff > 0 else "strategy_2"
                }

        comparison["risk_metrics"] = {
            "max_drawdown_difference": (
                backtest_1.summary.max_drawdown - backtest_2.summary.max_drawdown
            )
        }

        avg_return_1 = np.mean(list(backtest_1.summary.avg_returns.values()))
        avg_return_2 = np.mean(list(backtest_2.summary.avg_returns.values()))
        avg_sharpe_1 = np.mean(list(backtest_1.summary.sharpe_ratios.values()))
        avg_sharpe_2 = np.mean(list(backtest_2.summary.sharpe_ratios.values()))

        score_1 = avg_return_1 + avg_sharpe_1 - backtest_1.summary.max_drawdown
        score_2 = avg_return_2 + avg_sharpe_2 - backtest_2.summary.max_drawdown

        comparison["recommendation"] = {
            "better_strategy": "strategy_1" if score_1 > score_2 else "strategy_2",
            "confidence": abs(score_1 - score_2) / max(abs(score_1), abs(score_2), 1),
        }

        return comparison
