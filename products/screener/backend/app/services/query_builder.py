"""
SQL query builder for complex screening criteria
Converts visual query builder inputs into optimized SQL
"""
import logging
from datetime import date
from typing import Any, Dict, List

from app.schemas.screener import ScreenerQuery, QueryGroup, QueryCondition

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Builds SQL queries from screening criteria"""
    
    def __init__(self):
        self.metric_mappings = self._init_metric_mappings()
    
    def _init_metric_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Initialize mappings between metric IDs and database columns for YodaBuffett schema"""
        return {
            # Fundamental metrics from historical_fundamentals_daily
            'pe_ratio': {
                'table': 'hfd',
                'column': 'pe_ratio',
                'type': 'number',
                'join_required': True
            },
            'pb_ratio': {
                'table': 'hfd',
                'column': 'pb_ratio',
                'type': 'number',
                'join_required': True
            },
            'ps_ratio': {
                'table': 'hfd',
                'column': 'ps_ratio',
                'type': 'number',
                'join_required': True
            },
            'roe': {
                'table': 'hfd',
                'column': 'roe',
                'type': 'percentage',
                'join_required': True
            },
            'roa': {
                'table': 'hfd',
                'column': 'roa',
                'type': 'percentage',
                'join_required': True
            },
            'current_ratio': {
                'table': 'hfd',
                'column': 'current_ratio',
                'type': 'ratio',
                'join_required': True
            },
            'debt_to_equity': {
                'table': 'hfd',
                'column': 'debt_to_equity',
                'type': 'ratio',
                'join_required': True
            },
            'gross_margin': {
                'table': 'hfd',
                'column': 'gross_margin',
                'type': 'percentage',
                'join_required': True
            },
            'operating_margin': {
                'table': 'hfd',
                'column': 'operating_margin',
                'type': 'percentage',
                'join_required': True
            },
            'net_margin': {
                'table': 'hfd',
                'column': 'net_margin',
                'type': 'percentage',
                'join_required': True
            },
            'revenue_growth_yoy': {
                'table': 'hfd',
                'column': 'revenue_growth_yoy',
                'type': 'percentage',
                'join_required': True
            },
            
            # Technical metrics from daily_price_data
            'price': {
                'table': 'dpd',
                'column': 'close_price',
                'type': 'currency',
                'join_required': True
            },
            'volume': {
                'table': 'dpd',
                'column': 'volume',
                'type': 'number',
                'join_required': True
            },
            'high_price': {
                'table': 'dpd',
                'column': 'high_price',
                'type': 'currency',
                'join_required': True
            },
            'low_price': {
                'table': 'dpd',
                'column': 'low_price',
                'type': 'currency',
                'join_required': True
            },
            
            # Company metrics from company_master
            'market_cap': {
                'table': 'cm',
                'column': 'market_cap_usd',
                'type': 'currency',
                'join_required': False
            },
            'sector': {
                'table': 'cm',
                'column': 'sector',
                'type': 'text',
                'join_required': False
            },
            'industry': {
                'table': 'cm',
                'column': 'industry',
                'type': 'text',
                'join_required': False
            }
        }
    
    async def build_screening_query(
        self, 
        query: ScreenerQuery, 
        screen_date: date
    ) -> str:
        """Build complete SQL query from screening criteria"""
        
        # Determine which tables we need to join
        required_joins = self._determine_required_joins(query)
        
        # Build SELECT clause with requested columns
        select_clause = self._build_select_clause(query.columns, required_joins)
        
        # Build FROM clause with necessary joins
        from_clause = self._build_from_clause(required_joins, screen_date)
        
        # Build WHERE clause from conditions
        where_clause = self._build_where_clause(query.groups, query.groupLogic)
        
        # Combine into final query
        sql_query = f"""
        {select_clause}
        {from_clause}
        {where_clause}
        ORDER BY cm.market_cap_usd DESC NULLS LAST
        LIMIT 1000
        """
        
        logger.info(f"Generated screening query for date {screen_date}")
        logger.debug(f"SQL Query: {sql_query}")
        
        return sql_query
    
    def _determine_required_joins(self, query: ScreenerQuery) -> Dict[str, bool]:
        """Determine which table joins are required based on query"""
        joins = {
            'fundamentals': False,
            'market_data': False
        }
        
        # Check columns
        all_metrics = set(query.columns)
        
        # Check conditions
        for group in query.groups:
            for condition in group.conditions:
                all_metrics.add(condition.leftOperand)
                if condition.isRelative and isinstance(condition.rightOperand, str):
                    all_metrics.add(condition.rightOperand)
        
        # Determine required joins
        for metric_id in all_metrics:
            if metric_id in self.metric_mappings:
                mapping = self.metric_mappings[metric_id]
                if mapping['table'] == 'hf':
                    joins['fundamentals'] = True
                elif mapping['table'] == 'mdh':
                    joins['market_data'] = True
        
        return joins
    
    def _build_select_clause(self, columns: List[str], required_joins: Dict[str, bool]) -> str:
        """Build SELECT clause with company info and requested metrics"""
        
        # Always include company information from company_master
        select_parts = [
            "cm.company_id",
            "cm.primary_ticker as symbol",
            "cm.company_name",
            "cm.primary_exchange as market",
            "cm.sector",
            "cm.industry", 
            "cm.market_cap_usd as market_cap",
            "cm.currency"
        ]
        
        # Add requested metric columns
        for column in columns:
            if column in self.metric_mappings:
                mapping = self.metric_mappings[column]
                table_alias = mapping['table']
                db_column = mapping['column']
                select_parts.append(f"{table_alias}.{db_column} as {column}")
        
        return f"SELECT {', '.join(select_parts)}"
    
    def _build_from_clause(self, required_joins: Dict[str, bool], screen_date: date) -> str:
        """Build FROM clause with appropriate joins for point-in-time data"""
        
        from_parts = ["FROM company_master cm"]
        
        if required_joins['fundamentals']:
            # Point-in-time join for fundamentals using YodaBuffett schema
            from_parts.append(f"""
            LEFT JOIN LATERAL (
                SELECT *
                FROM historical_fundamentals_daily hfd_inner
                WHERE hfd_inner.symbol = cm.primary_ticker
                AND hfd_inner.date <= '{screen_date}'
                ORDER BY hfd_inner.date DESC
                LIMIT 1
            ) hfd ON true
            """)
        
        if required_joins['market_data']:
            # Point-in-time join for market data using YodaBuffett schema
            from_parts.append(f"""
            LEFT JOIN LATERAL (
                SELECT *
                FROM daily_price_data dpd_inner
                WHERE dpd_inner.symbol = cm.primary_ticker
                AND dpd_inner.date <= '{screen_date}'
                ORDER BY dpd_inner.date DESC
                LIMIT 1
            ) dpd ON true
            """)
        
        return " ".join(from_parts)
    
    def _build_where_clause(self, groups: List[QueryGroup], group_logic: str) -> str:
        """Build WHERE clause from query groups and conditions"""
        
        if not groups:
            return "WHERE cm.listing_status = 'active' AND cm.primary_ticker IS NOT NULL"  # Default filter
        
        group_conditions = []
        
        for group in groups:
            if group.conditions:
                condition_parts = []
                
                for condition in group.conditions:
                    condition_sql = self._build_condition(condition)
                    if condition_sql:
                        condition_parts.append(condition_sql)
                
                if condition_parts:
                    group_sql = f"({f' {group.logicalOperator} '.join(condition_parts)})"
                    group_conditions.append(group_sql)
        
        if group_conditions:
            where_clause = f"WHERE cm.listing_status = 'active' AND cm.primary_ticker IS NOT NULL AND ({f' {group_logic} '.join(group_conditions)})"
        else:
            where_clause = "WHERE cm.listing_status = 'active' AND cm.primary_ticker IS NOT NULL"
        
        return where_clause
    
    def _build_condition(self, condition: QueryCondition) -> str:
        """Build SQL condition from query condition"""
        
        try:
            if condition.isRelative:
                # Relative comparison (metric vs metric)
                return self._build_relative_condition(condition)
            else:
                # Absolute comparison (metric vs value)
                return self._build_absolute_condition(condition)
        
        except Exception as e:
            logger.error(f"Failed to build condition: {condition}, error: {e}")
            return ""
    
    def _build_relative_condition(self, condition: QueryCondition) -> str:
        """Build SQL for relative metric comparisons (e.g., P/E < Industry P/E)"""
        
        left_metric = condition.leftOperand
        right_metric = condition.rightOperand
        
        if not isinstance(right_metric, str):
            logger.error(f"Relative condition requires string right operand: {condition}")
            return ""
        
        left_mapping = self.metric_mappings.get(left_metric)
        right_mapping = self.metric_mappings.get(right_metric)
        
        if not left_mapping or not right_mapping:
            logger.error(f"Unknown metrics in relative condition: {left_metric}, {right_metric}")
            return ""
        
        left_expr = f"{left_mapping['table']}.{left_mapping['column']}"
        right_expr = f"{right_mapping['table']}.{right_mapping['column']}"
        
        # Handle NULL values
        return f"({left_expr} IS NOT NULL AND {right_expr} IS NOT NULL AND {left_expr} {condition.operator} {right_expr})"
    
    def _build_absolute_condition(self, condition: QueryCondition) -> str:
        """Build SQL for absolute value comparisons (e.g., P/E < 15)"""
        
        metric = condition.leftOperand
        value = condition.rightOperand
        
        if not isinstance(value, (int, float)):
            logger.error(f"Absolute condition requires numeric right operand: {condition}")
            return ""
        
        mapping = self.metric_mappings.get(metric)
        if not mapping:
            logger.error(f"Unknown metric in condition: {metric}")
            return ""
        
        column_expr = f"{mapping['table']}.{mapping['column']}"
        
        # Handle different operators
        if condition.operator in ['>', '>=', '<', '<=', '=', '!=']:
            return f"({column_expr} IS NOT NULL AND {column_expr} {condition.operator} {value})"
        elif condition.operator == 'between':
            # Assuming value is a list [min, max] for between
            if isinstance(value, list) and len(value) == 2:
                return f"({column_expr} IS NOT NULL AND {column_expr} BETWEEN {value[0]} AND {value[1]})"
        
        logger.error(f"Unsupported operator: {condition.operator}")
        return ""
    
    def validate_query(self, query: ScreenerQuery) -> List[str]:
        """Validate screening query and return list of errors"""
        errors = []
        
        # Check if we have any conditions
        if not query.groups or all(not group.conditions for group in query.groups):
            errors.append("Query must have at least one screening condition")
        
        # Validate metric IDs in columns
        for column in query.columns:
            if column not in self.metric_mappings:
                errors.append(f"Unknown metric in columns: {column}")
        
        # Validate conditions
        for group_idx, group in enumerate(query.groups):
            for cond_idx, condition in enumerate(group.conditions):
                condition_errors = self._validate_condition(condition)
                for error in condition_errors:
                    errors.append(f"Group {group_idx + 1}, Condition {cond_idx + 1}: {error}")
        
        return errors
    
    def _validate_condition(self, condition: QueryCondition) -> List[str]:
        """Validate a single condition"""
        errors = []
        
        # Check left operand (metric)
        if condition.leftOperand not in self.metric_mappings:
            errors.append(f"Unknown metric: {condition.leftOperand}")
        
        # Check operator
        valid_operators = ['>', '>=', '<', '<=', '=', '!=', 'between', 'in', 'not_in']
        if condition.operator not in valid_operators:
            errors.append(f"Invalid operator: {condition.operator}")
        
        # Check right operand based on condition type
        if condition.isRelative:
            if not isinstance(condition.rightOperand, str):
                errors.append("Relative condition requires metric name as right operand")
            elif condition.rightOperand not in self.metric_mappings:
                errors.append(f"Unknown metric in right operand: {condition.rightOperand}")
        else:
            if not isinstance(condition.rightOperand, (int, float, list)):
                errors.append("Absolute condition requires numeric value or list as right operand")
        
        return errors