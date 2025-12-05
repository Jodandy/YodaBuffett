"""
Base strategy framework with ML model integration.
Supports technical, fundamental, ML-hybrid, and anomaly detection strategies.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, date
import pandas as pd
from enum import Enum
import asyncio

from ..indicators.base import IndicatorEngine, indicator_registry, IndicatorResult


class SignalType(Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    STRONG_BUY = "strong_buy"
    STRONG_SELL = "strong_sell"


class StrategyType(Enum):
    TECHNICAL = "technical"
    FUNDAMENTAL = "fundamental"
    ML_HYBRID = "ml_hybrid"
    ANOMALY_DETECTION = "anomaly_detection"
    ENSEMBLE = "ensemble"


class Signal:
    """Standardized trading signal."""
    
    def __init__(
        self,
        signal_type: SignalType,
        confidence: float,
        strength: float,
        company_id: int,
        date: date,
        contributing_factors: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.signal_type = signal_type
        self.confidence = max(0.0, min(1.0, confidence))  # Clamp to 0-1
        self.strength = strength
        self.company_id = company_id
        self.date = date
        self.contributing_factors = contributing_factors or {}
        self.metadata = metadata or {}
        self.created_at = datetime.now()
    
    def is_actionable(self, min_confidence: float = 0.6) -> bool:
        """Check if signal confidence meets minimum threshold."""
        return self.confidence >= min_confidence
    
    def get_position_size_factor(self) -> float:
        """Get position sizing factor based on confidence and strength."""
        return self.confidence * min(abs(self.strength), 1.0)


class BaseStrategy(ABC):
    """Base class for all trading strategies."""
    
    def __init__(
        self,
        name: str,
        description: str,
        strategy_type: StrategyType,
        required_indicators: List[str],
        config: Optional[Dict[str, Any]] = None
    ):
        self.name = name
        self.description = description
        self.strategy_type = strategy_type
        self.required_indicators = required_indicators
        self.config = config or {}
        self.indicator_engine = IndicatorEngine(indicator_registry)
        
        # Performance tracking
        self.last_backtest_date: Optional[date] = None
        self.performance_metrics: Dict[str, float] = {}
    
    @abstractmethod
    async def generate_signal(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        current_date: date,
        indicator_values: Dict[str, IndicatorResult]
    ) -> Optional[Signal]:
        """
        Generate trading signal for a specific company and date.
        
        Args:
            company_id: Company identifier
            market_data: Historical OHLCV data
            current_date: Date to generate signal for
            indicator_values: Pre-calculated indicator results
            
        Returns:
            Signal or None if no signal should be generated
        """
        pass
    
    async def calculate_required_indicators(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date
    ) -> Dict[str, IndicatorResult]:
        """Calculate all indicators required by this strategy."""
        return await self.indicator_engine.calculate_multiple(
            self.required_indicators,
            company_id,
            market_data,
            start_date,
            end_date
        )
    
    def validate_config(self) -> bool:
        """Validate strategy configuration parameters."""
        return True  # Override in subclasses for specific validation
    
    async def backtest_single_company(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date
    ) -> List[Signal]:
        """Run strategy backtest for a single company."""
        # Calculate indicators for entire period
        indicator_values = await self.calculate_required_indicators(
            company_id, market_data, start_date, end_date
        )
        
        # Generate signals for each date
        signals = []
        current = start_date
        while current <= end_date:
            signal = await self.generate_signal(
                company_id,
                market_data,
                current,
                indicator_values
            )
            if signal:
                signals.append(signal)
            
            # Move to next day
            current = current.replace(day=current.day + 1) if current.day < 28 else current.replace(month=current.month + 1, day=1) if current.month < 12 else current.replace(year=current.year + 1, month=1, day=1)
            if current > end_date:
                break
        
        return signals


class TechnicalStrategy(BaseStrategy):
    """Base class for technical analysis strategies."""
    
    def __init__(
        self,
        name: str,
        description: str,
        required_indicators: List[str],
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            name=name,
            description=description,
            strategy_type=StrategyType.TECHNICAL,
            required_indicators=required_indicators,
            config=config
        )


class MLStrategy(BaseStrategy):
    """Base class for ML-based strategies."""
    
    def __init__(
        self,
        name: str,
        description: str,
        required_indicators: List[str],
        ml_model_name: str,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            name=name,
            description=description,
            strategy_type=StrategyType.ML_HYBRID,
            required_indicators=required_indicators,
            config=config
        )
        self.ml_model_name = ml_model_name
    
    @abstractmethod
    async def load_ml_model(self) -> Any:
        """Load the ML model from storage."""
        pass
    
    @abstractmethod
    async def prepare_features(
        self,
        indicator_values: Dict[str, IndicatorResult],
        current_date: date
    ) -> pd.DataFrame:
        """Prepare feature vector for ML model prediction."""
        pass


class AnomalyDetectionStrategy(BaseStrategy):
    """Base class for anomaly detection strategies."""
    
    def __init__(
        self,
        name: str,
        description: str,
        required_indicators: List[str],
        anomaly_threshold: float = 0.7,
        config: Optional[Dict[str, Any]] = None
    ):
        super().__init__(
            name=name,
            description=description,
            strategy_type=StrategyType.ANOMALY_DETECTION,
            required_indicators=required_indicators,
            config=config
        )
        self.anomaly_threshold = anomaly_threshold


class StrategyRegistry:
    """Registry for all available strategies."""
    
    def __init__(self):
        self._strategies: Dict[str, BaseStrategy] = {}
    
    def register(self, strategy: BaseStrategy) -> None:
        """Register a strategy."""
        self._strategies[strategy.name] = strategy
    
    def get(self, name: str) -> Optional[BaseStrategy]:
        """Get strategy by name."""
        return self._strategies.get(name)
    
    def list_by_type(self, strategy_type: StrategyType) -> List[BaseStrategy]:
        """List all strategies of a specific type."""
        return [
            strategy for strategy in self._strategies.values()
            if strategy.strategy_type == strategy_type
        ]
    
    def get_all(self) -> Dict[str, BaseStrategy]:
        """Get all registered strategies."""
        return self._strategies.copy()


# Global strategy registry
strategy_registry = StrategyRegistry()