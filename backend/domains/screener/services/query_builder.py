"""
SQL query builder for complex screening criteria
"""
import logging
from datetime import date
from typing import Any, Dict, List

from ..schemas import ScreenerQuery, QueryGroup, QueryCondition

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Builds SQL queries from screening criteria"""

    def __init__(self):
        self.metric_mappings = self._init_metric_mappings()

    def _init_metric_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Initialize mappings between metric IDs and database columns"""
        return {
            # Fundamental metrics from historical_fundamentals_daily
            'pe_ratio': {'table': 'hfd', 'column': 'pe_ratio', 'type': 'number'},
            'pb_ratio': {'table': 'hfd', 'column': 'pb_ratio', 'type': 'number'},
            'ps_ratio': {'table': 'hfd', 'column': 'ps_ratio', 'type': 'number'},
            'roe': {'table': 'hfd', 'column': 'roe', 'type': 'percentage'},
            'roa': {'table': 'hfd', 'column': 'roa', 'type': 'percentage'},
            'current_ratio': {'table': 'hfd', 'column': 'current_ratio', 'type': 'ratio'},
            'debt_to_equity': {'table': 'hfd', 'column': 'debt_to_equity', 'type': 'ratio'},
            'gross_margin': {'table': 'hfd', 'column': 'gross_margin', 'type': 'percentage'},
            'operating_margin': {'table': 'hfd', 'column': 'operating_margin', 'type': 'percentage'},
            'net_margin': {'table': 'hfd', 'column': 'net_margin', 'type': 'percentage'},

            # Technical metrics from daily_price_data
            'price': {'table': 'dpd', 'column': 'close_price', 'type': 'currency'},
            'volume': {'table': 'dpd', 'column': 'volume', 'type': 'number'},
            'high_price': {'table': 'dpd', 'column': 'high_price', 'type': 'currency'},
            'low_price': {'table': 'dpd', 'column': 'low_price', 'type': 'currency'},

            # Company metrics from company_master
            'market_cap': {'table': 'cm', 'column': 'market_cap_usd', 'type': 'currency'},
            'sector': {'table': 'cm', 'column': 'sector', 'type': 'text'},
            'industry': {'table': 'cm', 'column': 'industry', 'type': 'text'},

            # Additional fundamental metrics
            'ev_ebitda': {'table': 'calculated', 'column': 'ev_ebitda', 'type': 'ratio'},
            'enterprise_value': {'table': 'calculated', 'column': 'enterprise_value', 'type': 'currency'},

            # Technical indicators (calculated)
            'rsi_14': {'table': 'calculated', 'column': 'rsi_14', 'type': 'number'},
            'rsi_30': {'table': 'calculated', 'column': 'rsi_30', 'type': 'number'},
            'sma_20': {'table': 'calculated', 'column': 'sma_20', 'type': 'currency'},
            'sma_50': {'table': 'calculated', 'column': 'sma_50', 'type': 'currency'},
            'sma_200': {'table': 'calculated', 'column': 'sma_200', 'type': 'currency'},
            'ema_12': {'table': 'calculated', 'column': 'ema_12', 'type': 'currency'},
            'ema_26': {'table': 'calculated', 'column': 'ema_26', 'type': 'currency'},
            'volatility_20d': {'table': 'calculated', 'column': 'volatility_20d', 'type': 'percentage'},
            'volatility_60d': {'table': 'calculated', 'column': 'volatility_60d', 'type': 'percentage'},
            'avg_volume_20d': {'table': 'calculated', 'column': 'avg_volume_20d', 'type': 'number'},

            # Derived metrics (calculated)
            'price_change_1d': {'table': 'calculated', 'column': 'price_change_1d', 'type': 'percentage'},
            'price_change_5d': {'table': 'calculated', 'column': 'price_change_5d', 'type': 'percentage'},
            'price_change_20d': {'table': 'calculated', 'column': 'price_change_20d', 'type': 'percentage'},
            'distance_from_52w_high': {'table': 'calculated', 'column': 'distance_from_52w_high', 'type': 'percentage'},
            'distance_from_52w_low': {'table': 'calculated', 'column': 'distance_from_52w_low', 'type': 'percentage'},

            # Dimension scores (0-100 scale)
            'value_score': {'table': 'dims', 'column': 'value_score', 'type': 'score'},
            'momentum_score': {'table': 'dims', 'column': 'momentum_score', 'type': 'score'},
            'quality_score': {'table': 'dims', 'column': 'quality_score', 'type': 'score'},
            'sentiment_score': {'table': 'dims', 'column': 'sentiment_score', 'type': 'score'},
            'risk_score': {'table': 'dims', 'column': 'risk_score', 'type': 'score'},
            'composite_score': {'table': 'composite', 'column': 'score', 'type': 'score'},

            # Dimension percentiles (0-100 rank)
            'value_percentile': {'table': 'dims', 'column': 'value_percentile', 'type': 'percentile'},
            'momentum_percentile': {'table': 'dims', 'column': 'momentum_percentile', 'type': 'percentile'},
            'quality_percentile': {'table': 'dims', 'column': 'quality_percentile', 'type': 'percentile'},
            'sentiment_percentile': {'table': 'dims', 'column': 'sentiment_percentile', 'type': 'percentile'},
            'risk_percentile': {'table': 'dims', 'column': 'risk_percentile', 'type': 'percentile'}
        }

    def validate_query(self, query: ScreenerQuery) -> List[str]:
        """Validate screening query and return list of errors"""
        errors = []

        if not query.groups or all(not group.conditions for group in query.groups):
            errors.append("Query must have at least one screening condition")

        for column in query.columns:
            if column not in self.metric_mappings:
                errors.append(f"Unknown metric in columns: {column}")

        for group_idx, group in enumerate(query.groups):
            for cond_idx, condition in enumerate(group.conditions):
                condition_errors = self._validate_condition(condition)
                for error in condition_errors:
                    errors.append(f"Group {group_idx + 1}, Condition {cond_idx + 1}: {error}")

        return errors

    def _validate_condition(self, condition: QueryCondition) -> List[str]:
        """Validate a single condition"""
        errors = []

        if condition.left_operand not in self.metric_mappings:
            errors.append(f"Unknown metric: {condition.left_operand}")

        valid_operators = ['>', '>=', '<', '<=', '=', '!=', 'between', 'in', 'not_in']
        if condition.operator not in valid_operators:
            errors.append(f"Invalid operator: {condition.operator}")

        if condition.is_relative:
            if not isinstance(condition.right_operand, str):
                errors.append("Relative condition requires metric name as right operand")
            elif condition.right_operand not in self.metric_mappings:
                errors.append(f"Unknown metric in right operand: {condition.right_operand}")
        else:
            if not isinstance(condition.right_operand, (int, float, list)):
                errors.append("Absolute condition requires numeric value or list as right operand")

        return errors
