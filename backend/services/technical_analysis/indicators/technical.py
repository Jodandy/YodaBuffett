"""
Technical indicators implementation.
Common indicators like RSI, SMA, Bollinger Bands, etc.
"""

import pandas as pd
import numpy as np
from datetime import date
from typing import Dict, Union, Any, Optional

from .base import TechnicalIndicator, IndicatorResult, DataType, Timeframe


class RSI(TechnicalIndicator):
    """Relative Strength Index indicator."""
    
    def __init__(self, period: int = 14):
        super().__init__(
            name=f"rsi_{period}",
            description=f"Relative Strength Index ({period}-period)",
            parameters={"period": period}
        )
        self.period = period
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate RSI values."""
        # Ensure we have enough data
        if len(market_data) < self.period + 1:
            return IndicatorResult({})
        
        # Calculate price changes
        delta = market_data['close'].diff()
        
        # Separate gains and losses
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        
        # Calculate average gains and losses
        avg_gain = gains.rolling(window=self.period, min_periods=self.period).mean()
        avg_loss = losses.rolling(window=self.period, min_periods=self.period).mean()
        
        # Calculate RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # Convert to date-indexed dict
        values = {}
        for timestamp, value in rsi.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={
                "period": self.period,
                "overbought_threshold": 70,
                "oversold_threshold": 30
            }
        )
    
    def get_required_columns(self) -> list[str]:
        return ['close']


class SMA(TechnicalIndicator):
    """Simple Moving Average indicator."""
    
    def __init__(self, period: int = 20):
        super().__init__(
            name=f"sma_{period}",
            description=f"Simple Moving Average ({period}-period)",
            parameters={"period": period}
        )
        self.period = period
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate SMA values."""
        if len(market_data) < self.period:
            return IndicatorResult({})
        
        # Calculate simple moving average
        sma = market_data['close'].rolling(window=self.period, min_periods=self.period).mean()
        
        # Convert to date-indexed dict
        values = {}
        for timestamp, value in sma.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={"period": self.period}
        )
    
    def get_required_columns(self) -> list[str]:
        return ['close']


class EMA(TechnicalIndicator):
    """Exponential Moving Average indicator."""
    
    def __init__(self, period: int = 20):
        super().__init__(
            name=f"ema_{period}",
            description=f"Exponential Moving Average ({period}-period)",
            parameters={"period": period}
        )
        self.period = period
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate EMA values."""
        if len(market_data) < self.period:
            return IndicatorResult({})
        
        # Calculate exponential moving average
        ema = market_data['close'].ewm(span=self.period, adjust=False).mean()
        
        # Convert to date-indexed dict
        values = {}
        for timestamp, value in ema.items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={"period": self.period}
        )
    
    def get_required_columns(self) -> list[str]:
        return ['close']


class BollingerBands(TechnicalIndicator):
    """Bollinger Bands indicator (returns upper band)."""
    
    def __init__(self, period: int = 20, std_dev: float = 2.0, band: str = "upper"):
        super().__init__(
            name=f"bb_{band}_{period}",
            description=f"Bollinger Bands {band.title()} ({period}-period, {std_dev} std dev)",
            parameters={"period": period, "std_dev": std_dev, "band": band}
        )
        self.period = period
        self.std_dev = std_dev
        self.band = band
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate Bollinger Band values."""
        if len(market_data) < self.period:
            return IndicatorResult({})
        
        # Calculate moving average and standard deviation
        sma = market_data['close'].rolling(window=self.period).mean()
        std = market_data['close'].rolling(window=self.period).std()
        
        # Calculate bands
        upper_band = sma + (std * self.std_dev)
        lower_band = sma - (std * self.std_dev)
        
        # Select requested band
        if self.band == "upper":
            band_values = upper_band
        elif self.band == "lower":
            band_values = lower_band
        else:  # middle
            band_values = sma
        
        # Convert to date-indexed dict
        values = {}
        for timestamp, value in band_values.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={
                "period": self.period,
                "std_dev": self.std_dev,
                "band": self.band
            }
        )
    
    def get_required_columns(self) -> list[str]:
        return ['close']


class VolumeMA(TechnicalIndicator):
    """Volume Moving Average indicator."""
    
    def __init__(self, period: int = 20):
        super().__init__(
            name=f"volume_sma_{period}",
            description=f"Volume Simple Moving Average ({period}-period)",
            parameters={"period": period}
        )
        self.period = period
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate Volume MA values."""
        if len(market_data) < self.period:
            return IndicatorResult({})
        
        # Calculate volume moving average
        vol_ma = market_data['volume'].rolling(window=self.period, min_periods=self.period).mean()
        
        # Convert to date-indexed dict
        values = {}
        for timestamp, value in vol_ma.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={"period": self.period}
        )
    
    def get_required_columns(self) -> list[str]:
        return ['volume']


class PriceChange(TechnicalIndicator):
    """Price change percentage indicator."""
    
    def __init__(self, period: int = 1):
        super().__init__(
            name=f"price_change_{period}d",
            description=f"{period}-day price change percentage",
            parameters={"period": period}
        )
        self.period = period
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate price change percentages."""
        if len(market_data) < self.period + 1:
            return IndicatorResult({})
        
        # Calculate percentage change
        pct_change = market_data['close'].pct_change(periods=self.period) * 100
        
        # Convert to date-indexed dict
        values = {}
        for timestamp, value in pct_change.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={"period": self.period}
        )
    
    def get_required_columns(self) -> list[str]:
        return ['close']


class MACD(TechnicalIndicator):
    """MACD (Moving Average Convergence Divergence) indicator."""
    
    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        super().__init__(
            name=f"macd_{fast_period}_{slow_period}_{signal_period}",
            description=f"MACD ({fast_period}, {slow_period}, {signal_period})",
            parameters={"fast_period": fast_period, "slow_period": slow_period, "signal_period": signal_period}
        )
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
    
    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate MACD values."""
        if len(market_data) < self.slow_period:
            return IndicatorResult({})
        
        # Calculate EMAs
        ema_fast = market_data['close'].ewm(span=self.fast_period).mean()
        ema_slow = market_data['close'].ewm(span=self.slow_period).mean()
        
        # Calculate MACD line
        macd_line = ema_fast - ema_slow
        
        # Calculate signal line
        signal_line = macd_line.ewm(span=self.signal_period).mean()
        
        # Calculate histogram
        histogram = macd_line - signal_line
        
        # Return MACD line values (can be extended to return all components)
        values = {}
        for timestamp, value in macd_line.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)
        
        return IndicatorResult(
            values=values,
            metadata={
                "fast_period": self.fast_period,
                "slow_period": self.slow_period,
                "signal_period": self.signal_period,
                "component": "macd_line"  # Could be "signal_line" or "histogram"
            }
        )
    
    def get_required_columns(self) -> list[str]:
        return ['close']