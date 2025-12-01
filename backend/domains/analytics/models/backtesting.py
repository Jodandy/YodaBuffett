#!/usr/bin/env python3
"""
Core data structures and interfaces for modular backtesting framework.

Based on the vector_backtesting_framework.md design, this module provides
the foundation for building extensible trading strategies and backtesting them.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Dict, Optional, Union, Any, Tuple
from enum import Enum
import uuid


# =============================================================================
# Core Data Structures
# =============================================================================

class SignalType(Enum):
    """Types of trading signals"""
    BUY = "buy"
    SELL = "sell" 
    HOLD = "hold"
    STRONG_BUY = "strong_buy"
    STRONG_SELL = "strong_sell"


class SignalSource(Enum):
    """Source of the trading signal"""
    TEMPORAL_ANOMALY = "temporal_anomaly"
    ML_PREDICTION = "ml_prediction"
    TECHNICAL_INDICATOR = "technical_indicator"
    FUNDAMENTAL_ANALYSIS = "fundamental_analysis"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    COMBINED_STRATEGY = "combined_strategy"


@dataclass
class TradingSignal:
    """
    A standardized trading signal that any strategy can generate.
    This is the core interface between strategies and the backtesting engine.
    """
    # Core identification
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    symbol: str = ""  # Stock symbol (e.g., "VOLV.ST")
    
    # Signal details
    signal_type: SignalType = SignalType.HOLD
    signal_source: SignalSource = SignalSource.TEMPORAL_ANOMALY
    confidence: float = 0.5  # 0.0 (no confidence) to 1.0 (completely certain)
    strength: float = 0.0    # Expected return magnitude (-1.0 to +1.0)
    
    # Time horizons
    target_horizon_days: int = 30  # How many days ahead is this prediction for
    min_hold_days: int = 1         # Minimum holding period
    max_hold_days: int = 180       # Maximum holding period
    
    # Strategy-specific metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # For debugging and analysis
    strategy_name: str = ""
    strategy_version: str = "1.0"
    
    def __post_init__(self):
        """Validate signal parameters"""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {self.confidence}")
        if not -1.0 <= self.strength <= 1.0:
            raise ValueError(f"Strength must be between -1.0 and 1.0, got {self.strength}")
        if self.target_horizon_days <= 0:
            raise ValueError(f"Target horizon must be positive, got {self.target_horizon_days}")


@dataclass 
class MarketData:
    """
    Market data point for a specific symbol and timestamp.
    Used by strategies and for backtesting validation.
    """
    symbol: str
    timestamp: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int
    adjusted_close: float  # Adjusted for splits and dividends
    
    # Additional data that strategies might need
    market_cap: Optional[float] = None
    sector: Optional[str] = None
    currency: str = "SEK"  # Default for Nordic stocks
    
    def return_since(self, previous_price: float) -> float:
        """Calculate return since previous price"""
        if previous_price <= 0:
            return 0.0
        return (self.adjusted_close - previous_price) / previous_price


@dataclass
class Position:
    """
    Represents a trading position (long or short) in the portfolio.
    """
    position_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    entry_date: datetime = field(default_factory=datetime.now)
    entry_price: float = 0.0
    quantity: float = 0.0  # Positive for long, negative for short
    
    # Optional exit information
    exit_date: Optional[datetime] = None
    exit_price: Optional[float] = None
    
    # Position metadata
    signal_id: Optional[str] = None  # Link back to the signal that created this
    strategy_name: str = ""
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    
    @property
    def is_open(self) -> bool:
        """Check if position is still open"""
        return self.exit_date is None
    
    @property
    def is_long(self) -> bool:
        """Check if this is a long position"""
        return self.quantity > 0
    
    @property
    def is_short(self) -> bool:
        """Check if this is a short position"""
        return self.quantity < 0
    
    def current_return(self, current_price: float) -> float:
        """Calculate current return on position"""
        if self.entry_price <= 0:
            return 0.0
        
        price_change = current_price - self.entry_price
        if self.is_short:
            price_change = -price_change  # Invert for short positions
            
        return price_change / self.entry_price
    
    def realized_return(self) -> Optional[float]:
        """Calculate realized return if position is closed"""
        if not self.exit_price or self.entry_price <= 0:
            return None
        
        price_change = self.exit_price - self.entry_price
        if self.is_short:
            price_change = -price_change  # Invert for short positions
            
        return price_change / self.entry_price


@dataclass
class BacktestConfig:
    """
    Configuration for a backtesting run.
    """
    # Time period
    start_date: date
    end_date: date
    
    # Portfolio settings
    initial_capital: float = 1000000.0  # 1M SEK
    max_positions: int = 20
    max_position_size: float = 0.10  # 10% of portfolio per position
    min_position_size: float = 0.01  # 1% minimum position size
    
    # Risk management
    max_portfolio_risk: float = 0.20  # Maximum 20% portfolio risk
    stop_loss_threshold: Optional[float] = -0.15  # Stop loss at -15%
    take_profit_threshold: Optional[float] = 0.25   # Take profit at +25%
    
    # Transaction costs
    commission_rate: float = 0.001  # 0.1% commission
    spread_cost: float = 0.0005     # 0.05% bid-ask spread cost
    
    # Rebalancing
    rebalance_frequency_days: int = 30  # Rebalance monthly
    
    # Performance tracking
    benchmark_symbol: str = "OMXS30"  # Nordic benchmark
    
    # Strategy-specific settings
    strategy_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BacktestResults:
    """
    Results of a completed backtesting run.
    """
    # Run metadata
    backtest_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    strategy_name: str = ""
    config: Optional[BacktestConfig] = None
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    
    # Performance metrics
    total_return: float = 0.0
    annualized_return: float = 0.0
    benchmark_return: float = 0.0
    excess_return: float = 0.0
    
    # Risk metrics
    volatility: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    downside_deviation: float = 0.0
    
    # Trade statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    hit_rate: float = 0.0
    average_trade_return: float = 0.0
    average_winning_trade: float = 0.0
    average_losing_trade: float = 0.0
    
    # Detailed history
    portfolio_values: List[Tuple[datetime, float]] = field(default_factory=list)
    all_positions: List[Position] = field(default_factory=list)
    all_signals: List[TradingSignal] = field(default_factory=list)
    
    # Additional analysis
    monthly_returns: Dict[str, float] = field(default_factory=dict)
    sector_performance: Dict[str, float] = field(default_factory=dict)
    
    def profit_factor(self) -> float:
        """Calculate profit factor (total gains / total losses)"""
        total_gains = sum(p.realized_return() or 0 for p in self.all_positions 
                         if p.realized_return() and p.realized_return() > 0)
        total_losses = abs(sum(p.realized_return() or 0 for p in self.all_positions 
                              if p.realized_return() and p.realized_return() < 0))
        
        if total_losses == 0:
            return float('inf') if total_gains > 0 else 0.0
        return total_gains / total_losses


# =============================================================================
# Strategy Interface
# =============================================================================

class Strategy(ABC):
    """
    Abstract base class for all trading strategies.
    
    This defines the interface that all strategies must implement to work
    with the backtesting framework.
    """
    
    def __init__(self, name: str, version: str = "1.0", **params):
        self.name = name
        self.version = version
        self.params = params
        self._setup_complete = False
    
    @abstractmethod
    async def setup(self, config: BacktestConfig) -> None:
        """
        Initialize strategy with backtesting configuration.
        Called once before backtesting begins.
        
        Args:
            config: Backtesting configuration parameters
        """
        pass
    
    @abstractmethod
    async def generate_signals(
        self,
        current_date: datetime,
        market_data: Dict[str, MarketData],
        portfolio_state: Dict[str, Any]
    ) -> List[TradingSignal]:
        """
        Generate trading signals for the current date.
        
        Args:
            current_date: The current date in the backtest
            market_data: Dictionary of symbol -> MarketData for this date
            portfolio_state: Current portfolio information (positions, cash, etc.)
            
        Returns:
            List of trading signals to act upon
        """
        pass
    
    @abstractmethod
    async def should_exit_position(
        self,
        position: Position,
        current_date: datetime,
        market_data: MarketData,
        portfolio_state: Dict[str, Any]
    ) -> bool:
        """
        Determine if an existing position should be exited.
        
        Args:
            position: The position to evaluate
            current_date: Current date in backtest
            market_data: Current market data for the position's symbol
            portfolio_state: Current portfolio state
            
        Returns:
            True if position should be exited
        """
        pass
    
    @abstractmethod
    def get_position_size(
        self,
        signal: TradingSignal,
        portfolio_state: Dict[str, Any],
        config: BacktestConfig
    ) -> float:
        """
        Calculate position size for a trading signal.
        
        Args:
            signal: The trading signal
            portfolio_state: Current portfolio state
            config: Backtesting configuration
            
        Returns:
            Position size as fraction of portfolio (0.0 to 1.0)
        """
        pass
    
    async def teardown(self) -> None:
        """
        Clean up strategy resources after backtesting.
        Called once after backtesting completes.
        """
        pass
    
    def get_description(self) -> str:
        """Return human-readable description of the strategy"""
        return f"{self.name} v{self.version}"


# =============================================================================
# Market Data Provider Interface
# =============================================================================

class MarketDataProvider(ABC):
    """
    Abstract interface for providing market data to strategies and backtesting.
    """
    
    @abstractmethod
    async def get_market_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[MarketData]:
        """
        Get historical market data for a symbol within date range.
        
        Args:
            symbol: Stock symbol (e.g., "VOLV.ST")
            start_date: Start date for data
            end_date: End date for data
            
        Returns:
            List of MarketData points sorted by timestamp
        """
        pass
    
    @abstractmethod
    async def get_symbols_list(self) -> List[str]:
        """
        Get list of all available symbols.
        
        Returns:
            List of symbol strings
        """
        pass
    
    @abstractmethod
    async def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """
        Get metadata about a symbol (sector, market cap, etc.)
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary of symbol metadata
        """
        pass


# =============================================================================
# Utility Functions
# =============================================================================

def calculate_returns(prices: List[float]) -> List[float]:
    """Calculate period-over-period returns from price series"""
    if len(prices) < 2:
        return []
    
    returns = []
    for i in range(1, len(prices)):
        if prices[i-1] <= 0:
            returns.append(0.0)
        else:
            returns.append((prices[i] - prices[i-1]) / prices[i-1])
    
    return returns


def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.01) -> float:
    """Calculate Sharpe ratio from returns series"""
    if not returns or len(returns) < 2:
        return 0.0
    
    import numpy as np
    
    excess_returns = [r - risk_free_rate/252 for r in returns]  # Daily risk-free rate
    
    if np.std(excess_returns) == 0:
        return 0.0
    
    return np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)  # Annualized


def calculate_max_drawdown(portfolio_values: List[float]) -> float:
    """Calculate maximum drawdown from portfolio value series"""
    if len(portfolio_values) < 2:
        return 0.0
    
    peak = portfolio_values[0]
    max_dd = 0.0
    
    for value in portfolio_values[1:]:
        if value > peak:
            peak = value
        
        drawdown = (peak - value) / peak
        max_dd = max(max_dd, drawdown)
    
    return max_dd


if __name__ == "__main__":
    # Example usage and validation
    
    # Create a sample trading signal
    signal = TradingSignal(
        symbol="VOLV.ST",
        signal_type=SignalType.BUY,
        signal_source=SignalSource.TEMPORAL_ANOMALY,
        confidence=0.75,
        strength=0.12,  # Expecting 12% return
        target_horizon_days=90,
        metadata={"anomaly_type": "balance_sheet", "similarity_score": 0.23}
    )
    
    print(f"Created signal: {signal.signal_type.value} {signal.symbol} with {signal.confidence:.1%} confidence")
    
    # Create sample market data
    market_data = MarketData(
        symbol="VOLV.ST",
        timestamp=datetime.now(),
        open_price=100.0,
        high_price=102.5,
        low_price=99.0,
        close_price=101.5,
        volume=1500000,
        adjusted_close=101.5,
        sector="Automotive"
    )
    
    print(f"Market data for {market_data.symbol}: Close {market_data.close_price} SEK")
    
    # Test return calculation
    returns = calculate_returns([100, 102, 98, 105, 110])
    print(f"Sample returns: {[f'{r:.1%}' for r in returns]}")
    
    # Test Sharpe ratio calculation
    sharpe = calculate_sharpe_ratio(returns)
    print(f"Sharpe ratio: {sharpe:.2f}")
    
    print("✅ Core data structures and interfaces validated!")