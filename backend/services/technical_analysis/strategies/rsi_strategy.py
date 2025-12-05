"""
RSI Mean Reversion Strategy Example
Demonstrates the technical analysis architecture with a simple RSI-based strategy.
"""

import pandas as pd
from datetime import date
from typing import Dict, Optional

from ..indicators.base import IndicatorResult
from .base import TechnicalStrategy, Signal, SignalType


class RSIMeanReversionStrategy(TechnicalStrategy):
    """
    Simple RSI mean reversion strategy.
    
    Signals:
    - BUY when RSI < oversold_threshold (default 30)
    - SELL when RSI > overbought_threshold (default 70)
    - HOLD otherwise
    """
    
    def __init__(
        self,
        oversold_threshold: float = 30.0,
        overbought_threshold: float = 70.0,
        rsi_period: int = 14,
        min_confidence: float = 0.7
    ):
        config = {
            "oversold_threshold": oversold_threshold,
            "overbought_threshold": overbought_threshold,
            "rsi_period": rsi_period,
            "min_confidence": min_confidence
        }
        
        super().__init__(
            name="rsi_mean_reversion",
            description=f"RSI Mean Reversion (RSI {rsi_period}, Buy<{oversold_threshold}, Sell>{overbought_threshold})",
            required_indicators=[f"rsi_{rsi_period}"],
            config=config
        )
        
        self.oversold_threshold = oversold_threshold
        self.overbought_threshold = overbought_threshold
        self.rsi_period = rsi_period
        self.min_confidence = min_confidence
    
    async def generate_signal(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        current_date: date,
        indicator_values: Dict[str, IndicatorResult]
    ) -> Optional[Signal]:
        """Generate RSI-based trading signal."""
        
        # Get RSI value for current date
        rsi_indicator_name = f"rsi_{self.rsi_period}"
        rsi_result = indicator_values.get(rsi_indicator_name)
        
        if not rsi_result:
            return None
        
        rsi_value = rsi_result.get_value(current_date)
        if rsi_value is None:
            return None
        
        # Determine signal based on RSI thresholds
        signal_type = None
        confidence = 0.0
        strength = 0.0
        
        if rsi_value <= self.oversold_threshold:
            # Oversold - potential buy signal
            signal_type = SignalType.BUY
            # Higher confidence the more oversold
            confidence = min(1.0, (self.oversold_threshold - rsi_value) / self.oversold_threshold + 0.5)
            strength = (self.oversold_threshold - rsi_value) / self.oversold_threshold
            
        elif rsi_value >= self.overbought_threshold:
            # Overbought - potential sell signal
            signal_type = SignalType.SELL
            # Higher confidence the more overbought
            confidence = min(1.0, (rsi_value - self.overbought_threshold) / (100 - self.overbought_threshold) + 0.5)
            strength = (rsi_value - self.overbought_threshold) / (100 - self.overbought_threshold)
            
        else:
            # In the middle zone - hold
            signal_type = SignalType.HOLD
            confidence = 0.3  # Low confidence for hold signals
            strength = 0.1
        
        # Only generate actionable signals above minimum confidence
        if confidence < self.min_confidence and signal_type != SignalType.HOLD:
            return None
        
        return Signal(
            signal_type=signal_type,
            confidence=confidence,
            strength=strength,
            company_id=company_id,
            date=current_date,
            contributing_factors={
                "rsi_value": rsi_value,
                "oversold_threshold": self.oversold_threshold,
                "overbought_threshold": self.overbought_threshold,
                "rsi_period": self.rsi_period
            },
            metadata={
                "strategy": self.name,
                "indicator_used": rsi_indicator_name
            }
        )


class EnhancedRSIStrategy(TechnicalStrategy):
    """
    Enhanced RSI strategy with volume confirmation and trend filter.
    """
    
    def __init__(
        self,
        oversold_threshold: float = 25.0,
        overbought_threshold: float = 75.0,
        rsi_period: int = 14,
        sma_period: int = 20,
        volume_sma_period: int = 20,
        volume_multiplier: float = 1.5
    ):
        config = {
            "oversold_threshold": oversold_threshold,
            "overbought_threshold": overbought_threshold,
            "rsi_period": rsi_period,
            "sma_period": sma_period,
            "volume_sma_period": volume_sma_period,
            "volume_multiplier": volume_multiplier
        }
        
        super().__init__(
            name="enhanced_rsi",
            description=f"Enhanced RSI with volume/trend filters (RSI {rsi_period}, SMA {sma_period})",
            required_indicators=[
                f"rsi_{rsi_period}",
                f"sma_{sma_period}", 
                f"volume_sma_{volume_sma_period}"
            ],
            config=config
        )
        
        self.oversold_threshold = oversold_threshold
        self.overbought_threshold = overbought_threshold
        self.rsi_period = rsi_period
        self.sma_period = sma_period
        self.volume_sma_period = volume_sma_period
        self.volume_multiplier = volume_multiplier
    
    async def generate_signal(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        current_date: date,
        indicator_values: Dict[str, IndicatorResult]
    ) -> Optional[Signal]:
        """Generate enhanced RSI signal with filters."""
        
        # Get all required indicators
        rsi_result = indicator_values.get(f"rsi_{self.rsi_period}")
        sma_result = indicator_values.get(f"sma_{self.sma_period}")
        volume_sma_result = indicator_values.get(f"volume_sma_{self.volume_sma_period}")
        
        if not all([rsi_result, sma_result, volume_sma_result]):
            return None
        
        rsi_value = rsi_result.get_value(current_date)
        sma_value = sma_result.get_value(current_date)
        volume_sma_value = volume_sma_result.get_value(current_date)
        
        if None in [rsi_value, sma_value, volume_sma_value]:
            return None
        
        # Get current price and volume from market data
        try:
            current_price = market_data.loc[market_data.index.date == current_date, 'close'].iloc[-1]
            current_volume = market_data.loc[market_data.index.date == current_date, 'volume'].iloc[-1]
        except (IndexError, KeyError):
            return None
        
        # Check volume confirmation
        volume_confirmed = current_volume > (volume_sma_value * self.volume_multiplier)
        
        # Check trend direction (price vs SMA)
        uptrend = current_price > sma_value
        downtrend = current_price < sma_value
        
        signal_type = None
        confidence = 0.0
        strength = 0.0
        contributing_factors = {
            "rsi_value": rsi_value,
            "sma_value": sma_value,
            "current_price": current_price,
            "volume_confirmed": volume_confirmed,
            "uptrend": uptrend,
            "downtrend": downtrend
        }
        
        if rsi_value <= self.oversold_threshold and uptrend and volume_confirmed:
            # Oversold in uptrend with volume - strong buy
            signal_type = SignalType.STRONG_BUY
            confidence = 0.9
            strength = (self.oversold_threshold - rsi_value) / self.oversold_threshold
            
        elif rsi_value <= self.oversold_threshold and uptrend:
            # Oversold in uptrend without volume - regular buy
            signal_type = SignalType.BUY
            confidence = 0.7
            strength = (self.oversold_threshold - rsi_value) / self.oversold_threshold * 0.8
            
        elif rsi_value >= self.overbought_threshold and downtrend and volume_confirmed:
            # Overbought in downtrend with volume - strong sell
            signal_type = SignalType.STRONG_SELL
            confidence = 0.9
            strength = (rsi_value - self.overbought_threshold) / (100 - self.overbought_threshold)
            
        elif rsi_value >= self.overbought_threshold and downtrend:
            # Overbought in downtrend without volume - regular sell
            signal_type = SignalType.SELL
            confidence = 0.7
            strength = (rsi_value - self.overbought_threshold) / (100 - self.overbought_threshold) * 0.8
            
        else:
            # No clear signal
            signal_type = SignalType.HOLD
            confidence = 0.2
            strength = 0.1
        
        return Signal(
            signal_type=signal_type,
            confidence=confidence,
            strength=strength,
            company_id=company_id,
            date=current_date,
            contributing_factors=contributing_factors,
            metadata={
                "strategy": self.name,
                "filters_applied": ["volume", "trend"]
            }
        )