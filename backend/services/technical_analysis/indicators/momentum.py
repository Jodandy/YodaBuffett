"""
Momentum indicators for breakout trading.
Implements ADR, consolidation detection, momentum scoring, and volume analysis.
"""

import pandas as pd
import numpy as np
from datetime import date
from typing import Dict, Optional

from .base import TechnicalIndicator, IndicatorResult, Timeframe


class ADR(TechnicalIndicator):
    """
    Average Daily Range (Qullamaggie formula).
    ADR% = average of (High / Low - 1) * 100 over N days.

    A stock with 5% ADR means it typically swings 5% from daily low to high.
    Used for:
    - Volatility filtering (want ADR > 5%)
    - Stop loss sizing (max stop = 50% of ADR)
    """

    def __init__(self, period: int = 20):
        super().__init__(
            name=f"adr_{period}",
            description=f"Average Daily Range ({period}-period)",
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
        """Calculate ADR% values."""
        if len(market_data) < self.period:
            return IndicatorResult({})

        # Calculate daily range as percentage: (High / Low - 1) * 100
        daily_range_pct = (market_data['high'] / market_data['low'] - 1) * 100

        # Rolling average
        adr = daily_range_pct.rolling(window=self.period, min_periods=self.period).mean()

        # Convert to date-indexed dict
        values = {}
        for timestamp, value in adr.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)

        return IndicatorResult(
            values=values,
            metadata={
                "period": self.period,
                "min_threshold": 5.0,  # Minimum ADR for trading
                "formula": "avg((High/Low - 1) * 100)"
            }
        )

    def get_required_columns(self) -> list[str]:
        return ['high', 'low']


class ConsolidationRange(TechnicalIndicator):
    """
    Detects tight consolidation ranges for breakout setups.

    Measures the range (max_high - min_low) / min_low over N days.
    A "tight" consolidation is one where this range is relatively small
    compared to the stock's typical volatility (ADR).

    Returns:
    - range_pct: The percentage range of the consolidation
    - high: The consolidation high (breakout level)
    - low: The consolidation low (stop level)
    - is_tight: Boolean if range < threshold
    """

    def __init__(self, period: int = 10, tightness_threshold: float = 1.5):
        """
        Args:
            period: Number of days to look for consolidation (5-10 typical)
            tightness_threshold: Max range as multiple of ADR to be "tight"
        """
        super().__init__(
            name=f"consolidation_{period}",
            description=f"Consolidation Range ({period}-day)",
            parameters={"period": period, "tightness_threshold": tightness_threshold}
        )
        self.period = period
        self.tightness_threshold = tightness_threshold

    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate consolidation metrics."""
        if len(market_data) < self.period:
            return IndicatorResult({})

        values = {}

        # Calculate rolling consolidation metrics
        rolling_high = market_data['high'].rolling(window=self.period, min_periods=self.period).max()
        rolling_low = market_data['low'].rolling(window=self.period, min_periods=self.period).min()

        # Range as percentage
        range_pct = (rolling_high - rolling_low) / rolling_low * 100

        for timestamp in market_data.index:
            if pd.isna(rolling_high.loc[timestamp]) or pd.isna(rolling_low.loc[timestamp]):
                continue

            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = {
                    'range_pct': float(range_pct.loc[timestamp]),
                    'high': float(rolling_high.loc[timestamp]),
                    'low': float(rolling_low.loc[timestamp]),
                }

        return IndicatorResult(
            values=values,
            metadata={
                "period": self.period,
                "tightness_threshold": self.tightness_threshold
            }
        )

    def get_required_columns(self) -> list[str]:
        return ['high', 'low']


class MomentumScore(TechnicalIndicator):
    """
    Calculates momentum score based on price performance.

    Combines multiple timeframes:
    - 1-month (20 trading days) performance
    - 3-month (60 trading days) performance

    Used for universe selection - find the strongest movers.
    """

    def __init__(self, short_period: int = 20, long_period: int = 60):
        super().__init__(
            name=f"momentum_{short_period}_{long_period}",
            description=f"Momentum Score ({short_period}/{long_period}-day)",
            parameters={"short_period": short_period, "long_period": long_period}
        )
        self.short_period = short_period
        self.long_period = long_period

    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Calculate momentum scores."""
        if len(market_data) < self.long_period:
            return IndicatorResult({})

        values = {}

        # Calculate returns over different periods
        close = market_data['close']

        for i in range(self.long_period, len(market_data)):
            timestamp = market_data.index[i]
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp

            if not (start_date <= trade_date <= end_date):
                continue

            current_price = close.iloc[i]

            # 1-month return
            price_1m_ago = close.iloc[i - self.short_period]
            return_1m = (current_price / price_1m_ago - 1) * 100

            # 3-month return
            price_3m_ago = close.iloc[i - self.long_period]
            return_3m = (current_price / price_3m_ago - 1) * 100

            # Combined momentum score (simple average, can weight differently)
            momentum_score = (return_1m + return_3m) / 2

            values[trade_date] = {
                'score': float(momentum_score),
                'return_1m': float(return_1m),
                'return_3m': float(return_3m)
            }

        return IndicatorResult(
            values=values,
            metadata={
                "short_period": self.short_period,
                "long_period": self.long_period
            }
        )

    def get_required_columns(self) -> list[str]:
        return ['close']


class VolumeRatio(TechnicalIndicator):
    """
    Volume relative to moving average.

    VolumeRatio = Today's Volume / SMA(Volume, N)

    Used for breakout confirmation - want volume >= 1.5x average.
    """

    def __init__(self, period: int = 20):
        super().__init__(
            name=f"volume_ratio_{period}",
            description=f"Volume Ratio vs {period}-day average",
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
        """Calculate volume ratio values."""
        if len(market_data) < self.period:
            return IndicatorResult({})

        # Calculate volume moving average
        volume_sma = market_data['volume'].rolling(window=self.period, min_periods=self.period).mean()

        # Calculate ratio
        volume_ratio = market_data['volume'] / volume_sma

        # Convert to date-indexed dict
        values = {}
        for timestamp, value in volume_ratio.dropna().items():
            if pd.isna(value) or pd.isna(volume_sma.loc[timestamp]):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)

        return IndicatorResult(
            values=values,
            metadata={
                "period": self.period,
                "breakout_threshold": 1.5  # Minimum for breakout confirmation
            }
        )

    def get_required_columns(self) -> list[str]:
        return ['volume']


class PriorDayHigh(TechnicalIndicator):
    """
    Prior day's high price.

    Simple indicator that tracks the previous day's high,
    used as a breakout trigger level.
    """

    def __init__(self):
        super().__init__(
            name="prior_day_high",
            description="Prior Day's High",
            parameters={}
        )

    async def calculate(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        start_date: date,
        end_date: date,
        timeframe: Timeframe = Timeframe.DAILY
    ) -> IndicatorResult:
        """Get prior day's high."""
        if len(market_data) < 2:
            return IndicatorResult({})

        # Shift high by 1 to get prior day
        prior_high = market_data['high'].shift(1)

        values = {}
        for timestamp, value in prior_high.dropna().items():
            if pd.isna(value):
                continue
            trade_date = timestamp.date() if hasattr(timestamp, 'date') else timestamp
            if start_date <= trade_date <= end_date:
                values[trade_date] = float(value)

        return IndicatorResult(values=values, metadata={})

    def get_required_columns(self) -> list[str]:
        return ['high']
