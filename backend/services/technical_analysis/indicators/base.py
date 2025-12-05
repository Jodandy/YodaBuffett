"""
Base classes for the plugin-based indicator system.
Provides flexible architecture for technical, fundamental, and ML-based indicators.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, date
import pandas as pd
from enum import Enum
import asyncio


class IndicatorCategory(Enum):
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental" 
    SENTIMENT = "sentiment"
    ML_DERIVED = "ml_derived"


class DataType(Enum):
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"


class Timeframe(Enum):
    DAILY = "daily"
    HOUR_1 = "1h"
    MINUTE_15 = "15m"
    MINUTE_5 = "5m"
    MINUTE_1 = "1m"


class IndicatorResult:
    """Standardized result from any indicator calculation."""
    
    def __init__(
        self,
        values: Dict[date, Union[float, str, bool]],
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.values = values
        self.metadata = metadata or {}
        self.calculated_at = datetime.now()
    
    def get_latest(self) -> Union[float, str, bool, None]:
        """Get most recent value."""
        if not self.values:
            return None
        latest_date = max(self.values.keys())
        return self.values[latest_date]
    
    def get_value(self, target_date: date) -> Union[float, str, bool, None]:
        """Get value for specific date."""
        return self.values.get(target_date)


class BaseIndicator(ABC):
    """Base class for all indicators in the plugin system."""
    
    def __init__(
        self,
        name: str,
        description: str,
        category: IndicatorCategory,
        data_type: DataType,
        parameters: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.description = description
        self.category = category
        self.data_type = data_type
        self.parameters = parameters or {}
        self.dependencies: List[str] = []  # Other indicators this one depends on
    
    @abstractmethod
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """
        Calculate indicator values for given company and date range.
        
        Args:
            company_id: Company identifier
            market_data: OHLCV data as pandas DataFrame with DatetimeIndex
            start_date: Start of calculation period
            end_date: End of calculation period  
            timeframe: Data frequency
            
        Returns:
            IndicatorResult with calculated values
        """
        pass
    
    def get_required_columns(self) -> List[str]:
        """Return required market data columns (e.g., ['close', 'volume'])."""
        return ['close']  # Default: most indicators need close price
    
    def validate_data(self, market_data: pd.DataFrame) -> bool:
        """Validate that market_data has required columns."""
        required = self.get_required_columns()
        return all(col in market_data.columns for col in required)


class TechnicalIndicator(BaseIndicator):
    """Base class for technical indicators (RSI, SMA, etc.)."""
    
    def __init__(
        self,
        name: str,
        description: str,
        data_type: DataType = DataType.NUMERIC,
        parameters: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            name=name,
            description=description,
            category=IndicatorCategory.TECHNICAL,
            data_type=data_type,
            parameters=parameters
        )


class FundamentalIndicator(BaseIndicator):
    """Base class for fundamental indicators (P/E, ROE, etc.)."""
    
    def __init__(
        self,
        name: str,
        description: str,
        data_type: DataType = DataType.NUMERIC,
        parameters: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            name=name,
            description=description,
            category=IndicatorCategory.FUNDAMENTAL,
            data_type=data_type,
            parameters=parameters
        )
    
    @abstractmethod
    async def get_fundamental_data(
        self,
        company_id: int,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """Retrieve fundamental data from extracted documents/reports."""
        pass


class MLDerivedIndicator(BaseIndicator):
    """Base class for ML-derived indicators (anomaly scores, predictions, etc.)."""
    
    def __init__(
        self,
        name: str,
        description: str,
        data_type: DataType = DataType.NUMERIC,
        parameters: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            name=name,
            description=description,
            category=IndicatorCategory.ML_DERIVED,
            data_type=data_type,
            parameters=parameters
        )


class IndicatorRegistry:
    """Registry for all available indicators. Enables plugin discovery."""
    
    def __init__(self):
        self._indicators: Dict[str, BaseIndicator] = {}
    
    def register(self, indicator: BaseIndicator) -> None:
        """Register an indicator."""
        self._indicators[indicator.name] = indicator
    
    def get(self, name: str) -> Optional[BaseIndicator]:
        """Get indicator by name."""
        return self._indicators.get(name)
    
    def list_by_category(self, category: IndicatorCategory) -> List[BaseIndicator]:
        """List all indicators in a category."""
        return [
            indicator for indicator in self._indicators.values()
            if indicator.category == category
        ]
    
    def get_all(self) -> Dict[str, BaseIndicator]:
        """Get all registered indicators."""
        return self._indicators.copy()


class IndicatorEngine:
    """Main engine for calculating indicators with dependency resolution."""
    
    def __init__(self, registry: IndicatorRegistry):
        self.registry = registry
        self._cache: Dict[str, IndicatorResult] = {}
    
    async def calculate_indicator(
        self,
        indicator_name: str,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY,
        use_cache: bool = True
    ) -> IndicatorResult:
        """
        Calculate a single indicator, handling dependencies.
        
        Returns cached result if available and use_cache=True.
        """
        cache_key = f"{indicator_name}_{company_id}_{start_date}_{end_date}_{timeframe}"
        
        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]
        
        indicator = self.registry.get(indicator_name)
        if not indicator:
            raise ValueError(f"Indicator '{indicator_name}' not found in registry")
        
        # Validate market data
        if not indicator.validate_data(market_data):
            missing = set(indicator.get_required_columns()) - set(market_data.columns)
            raise ValueError(f"Missing required columns for {indicator_name}: {missing}")
        
        # Calculate dependencies first
        dependency_results = {}
        for dep_name in indicator.dependencies:
            dep_result = await self.calculate_indicator(
                dep_name, company_id, market_data, start_date, end_date, timeframe, use_cache
            )
            dependency_results[dep_name] = dep_result
        
        # Calculate the indicator
        result = await indicator.calculate(
            company_id, market_data, start_date, end_date, timeframe
        )
        
        if use_cache:
            self._cache[cache_key] = result
        
        return result
    
    async def calculate_multiple(
        self,
        indicator_names: List[str],
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> Dict[str, IndicatorResult]:
        """Calculate multiple indicators concurrently."""
        tasks = [
            self.calculate_indicator(
                name, company_id, market_data, start_date, end_date, timeframe
            )
            for name in indicator_names
        ]
        
        results = await asyncio.gather(*tasks)
        return dict(zip(indicator_names, results))
    
    def clear_cache(self) -> None:
        """Clear the indicator calculation cache."""
        self._cache.clear()


# Global registry instance
indicator_registry = IndicatorRegistry()