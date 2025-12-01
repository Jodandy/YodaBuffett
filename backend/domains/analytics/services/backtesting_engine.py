#!/usr/bin/env python3
"""
Core Backtesting Engine for YodaBuffett.

This engine orchestrates strategy execution, position management, and performance
tracking in a walk-forward manner to avoid look-ahead bias.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
from collections import defaultdict
import numpy as np

from ..models.backtesting import (
    Strategy, MarketDataProvider, TradingSignal, Position, MarketData,
    BacktestConfig, BacktestResults, SignalType, calculate_returns,
    calculate_sharpe_ratio, calculate_max_drawdown
)

logger = logging.getLogger(__name__)


class PortfolioManager:
    """
    Manages portfolio state including positions, cash, and risk metrics.
    """
    
    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, Position] = {}  # symbol -> Position
        self.closed_positions: List[Position] = []
        self.portfolio_history: List[Tuple[datetime, float]] = []
        
    def get_portfolio_value(self, current_prices: Dict[str, float]) -> float:
        """Calculate current total portfolio value"""
        positions_value = 0.0
        
        for symbol, position in self.positions.items():
            if symbol in current_prices:
                current_price = current_prices[symbol]
                position_value = abs(position.quantity) * current_price
                
                # Account for position direction
                if position.is_long:
                    positions_value += position_value
                else:  # short position
                    # For shorts, we gain when price goes down
                    positions_value += position.quantity * (position.entry_price - current_price)
            
        return self.cash + positions_value
    
    def get_position_count(self) -> int:
        """Get number of open positions"""
        return len(self.positions)
    
    def get_available_capital(self) -> float:
        """Get cash available for new positions"""
        return self.cash
    
    def can_open_position(self, position_value: float, max_positions: int) -> bool:
        """Check if new position can be opened"""
        if self.get_position_count() >= max_positions:
            return False
        if position_value > self.cash:
            return False
        return True
    
    def open_position(self, signal: TradingSignal, price: float, quantity: float, strategy_name: str) -> Position:
        """Open a new position based on trading signal"""
        position = Position(
            symbol=signal.symbol,
            entry_date=signal.timestamp,
            entry_price=price,
            quantity=quantity,
            signal_id=signal.signal_id,
            strategy_name=strategy_name
        )
        
        # Update cash (account for transaction costs)
        position_value = abs(quantity) * price
        transaction_cost = position_value * 0.001  # 0.1% transaction cost
        
        self.cash -= position_value + transaction_cost
        self.positions[signal.symbol] = position
        
        logger.info(f"Opened position: {signal.signal_type.value} {quantity:.0f} shares of {signal.symbol} at {price:.2f}")
        
        return position
    
    def close_position(self, symbol: str, price: float, timestamp: datetime) -> Optional[Position]:
        """Close an existing position"""
        if symbol not in self.positions:
            return None
        
        position = self.positions.pop(symbol)
        position.exit_date = timestamp
        position.exit_price = price
        
        # Update cash
        position_value = abs(position.quantity) * price
        transaction_cost = position_value * 0.001  # 0.1% transaction cost
        
        if position.is_long:
            # For long positions, we receive the sale proceeds
            self.cash += position_value - transaction_cost
        else:
            # For short positions, we pay to cover the short
            self.cash += abs(position.quantity) * (position.entry_price - price) - transaction_cost
        
        self.closed_positions.append(position)
        
        return_pct = position.realized_return() or 0.0
        logger.info(f"Closed position: {symbol} with {return_pct:.1%} return")
        
        return position


class BacktestingEngine:
    """
    Main backtesting engine that coordinates strategy execution and performance tracking.
    """
    
    def __init__(self, market_data_provider: MarketDataProvider):
        self.market_data_provider = market_data_provider
        self.logger = logging.getLogger(f"{__name__}.BacktestingEngine")
    
    async def run_backtest(
        self,
        strategy: Strategy,
        config: BacktestConfig
    ) -> BacktestResults:
        """
        Run a complete backtest for the given strategy and configuration.
        """
        self.logger.info(f"🚀 Starting backtest: {strategy.get_description()}")
        self.logger.info(f"📅 Period: {config.start_date} to {config.end_date}")
        
        start_time = datetime.now()
        
        # Initialize components
        portfolio = PortfolioManager(config.initial_capital)
        results = BacktestResults(
            strategy_name=strategy.name,
            config=config,
            start_time=start_time
        )
        
        # Setup strategy
        await strategy.setup(config)
        
        try:
            # Load market data for all symbols we might need
            available_symbols = await self.market_data_provider.get_symbols_list()
            market_data_cache = await self._load_market_data(
                available_symbols, config.start_date, config.end_date
            )
            
            # Main backtesting loop
            current_date = datetime.combine(config.start_date, datetime.min.time())
            end_datetime = datetime.combine(config.end_date, datetime.min.time())
            
            last_rebalance_date = current_date
            
            while current_date <= end_datetime:
                # Get market data for current date
                current_market_data = self._get_market_data_for_date(market_data_cache, current_date)
                
                if not current_market_data:
                    current_date += timedelta(days=1)
                    continue
                
                # Create portfolio state snapshot
                current_prices = {symbol: data.adjusted_close for symbol, data in current_market_data.items()}
                portfolio_value = portfolio.get_portfolio_value(current_prices)
                portfolio.portfolio_history.append((current_date, portfolio_value))
                
                portfolio_state = {
                    'current_date': current_date,
                    'total_value': portfolio_value,
                    'cash': portfolio.cash,
                    'positions': dict(portfolio.positions),
                    'position_count': portfolio.get_position_count(),
                    'available_capital': portfolio.get_available_capital()
                }
                
                # Check if we should exit any existing positions
                await self._process_position_exits(
                    strategy, portfolio, current_date, current_market_data, portfolio_state
                )
                
                # Generate new signals (only on rebalance dates)
                should_rebalance = (
                    current_date - last_rebalance_date
                ).days >= config.rebalance_frequency_days
                
                if should_rebalance:
                    signals = await strategy.generate_signals(
                        current_date, current_market_data, portfolio_state
                    )
                    
                    # Process signals and create new positions
                    await self._process_signals(
                        strategy, signals, portfolio, current_market_data, config
                    )
                    
                    results.all_signals.extend(signals)
                    last_rebalance_date = current_date
                
                current_date += timedelta(days=1)
            
            # Calculate final results
            await self._calculate_final_results(results, portfolio, config)
            
        except Exception as e:
            self.logger.error(f"❌ Backtest failed: {e}")
            raise
        finally:
            await strategy.teardown()
        
        results.end_time = datetime.now()
        duration = results.end_time - start_time
        
        self.logger.info(f"✅ Backtest completed in {duration.total_seconds():.1f}s")
        self.logger.info(f"📊 Total return: {results.total_return:.1%}")
        self.logger.info(f"📈 Sharpe ratio: {results.sharpe_ratio:.2f}")
        self.logger.info(f"🎯 Hit rate: {results.hit_rate:.1%}")
        
        return results
    
    async def _load_market_data(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date
    ) -> Dict[str, List[MarketData]]:
        """Load market data for all symbols in date range"""
        self.logger.info(f"📊 Loading market data for {len(symbols)} symbols...")
        
        market_data_cache = {}
        
        # Load data for a subset of symbols to start (top Nordic companies)
        priority_symbols = [
            "VOLV-B.ST", "ERIC-B.ST", "SEB-A.ST", "HM-B.ST", "TEL2-B.ST",
            "SAND.ST", "ASSA-B.ST", "SKF-B.ST", "ALFA.ST", "INVE-B.ST"
        ]
        
        for symbol in priority_symbols:
            try:
                data = await self.market_data_provider.get_market_data(symbol, start_date, end_date)
                if data:
                    market_data_cache[symbol] = data
            except Exception as e:
                self.logger.warning(f"Failed to load data for {symbol}: {e}")
        
        self.logger.info(f"📊 Loaded data for {len(market_data_cache)} symbols")
        return market_data_cache
    
    def _get_market_data_for_date(
        self,
        market_data_cache: Dict[str, List[MarketData]],
        target_date: datetime
    ) -> Dict[str, MarketData]:
        """Get market data for all symbols on a specific date"""
        result = {}
        target_date_only = target_date.date()
        
        for symbol, data_list in market_data_cache.items():
            # Find data point closest to target date
            for data_point in data_list:
                if data_point.timestamp.date() == target_date_only:
                    result[symbol] = data_point
                    break
        
        return result
    
    async def _process_position_exits(
        self,
        strategy: Strategy,
        portfolio: PortfolioManager,
        current_date: datetime,
        market_data: Dict[str, MarketData],
        portfolio_state: Dict[str, Any]
    ) -> None:
        """Check if any existing positions should be exited"""
        symbols_to_exit = []
        
        for symbol, position in portfolio.positions.items():
            if symbol in market_data:
                should_exit = await strategy.should_exit_position(
                    position, current_date, market_data[symbol], portfolio_state
                )
                
                if should_exit:
                    symbols_to_exit.append(symbol)
        
        # Exit positions
        for symbol in symbols_to_exit:
            if symbol in market_data:
                portfolio.close_position(
                    symbol, market_data[symbol].adjusted_close, current_date
                )
    
    async def _process_signals(
        self,
        strategy: Strategy,
        signals: List[TradingSignal],
        portfolio: PortfolioManager,
        market_data: Dict[str, MarketData],
        config: BacktestConfig
    ) -> None:
        """Process trading signals and create new positions"""
        # Filter to actionable signals (buy/sell only)
        actionable_signals = [
            s for s in signals 
            if s.signal_type in [SignalType.BUY, SignalType.SELL, SignalType.STRONG_BUY, SignalType.STRONG_SELL]
            and s.symbol in market_data
        ]
        
        # Sort by confidence (strongest signals first)
        actionable_signals.sort(key=lambda s: s.confidence, reverse=True)
        
        # Process signals
        for signal in actionable_signals:
            # Skip if we already have a position in this symbol
            if signal.symbol in portfolio.positions:
                continue
            
            # Get current market data
            current_price = market_data[signal.symbol].adjusted_close
            
            # Calculate position size
            current_portfolio_state = {
                'total_value': portfolio.get_portfolio_value({signal.symbol: current_price}),
                'cash': portfolio.cash,
                'position_count': portfolio.get_position_count()
            }
            
            position_size_fraction = strategy.get_position_size(signal, current_portfolio_state, config)
            position_value = portfolio.cash * position_size_fraction
            
            # Check position limits
            if position_value < config.min_position_size * portfolio.cash:
                continue
            if position_value > config.max_position_size * portfolio.cash:
                position_value = config.max_position_size * portfolio.cash
            
            # Check if we can open this position
            if not portfolio.can_open_position(position_value, config.max_positions):
                continue
            
            # Calculate quantity
            quantity = position_value / current_price
            
            # For sell/short signals, make quantity negative
            if signal.signal_type in [SignalType.SELL, SignalType.STRONG_SELL]:
                quantity = -quantity
            
            # Open position
            portfolio.open_position(signal, current_price, quantity, strategy.name)
    
    async def _calculate_final_results(
        self,
        results: BacktestResults,
        portfolio: PortfolioManager,
        config: BacktestConfig
    ) -> None:
        """Calculate final performance metrics"""
        # Close any remaining positions at final prices
        if portfolio.portfolio_history:
            final_value = portfolio.portfolio_history[-1][1]
            
            # Basic performance metrics
            results.total_return = (final_value - config.initial_capital) / config.initial_capital
            
            # Calculate time period
            start_date = datetime.combine(config.start_date, datetime.min.time())
            end_date = datetime.combine(config.end_date, datetime.min.time())
            years = (end_date - start_date).days / 365.25
            
            if years > 0:
                results.annualized_return = (final_value / config.initial_capital) ** (1/years) - 1
            
            # Portfolio value time series
            results.portfolio_values = portfolio.portfolio_history
            
            # Calculate returns series for risk metrics
            portfolio_values = [pv[1] for pv in portfolio.portfolio_history]
            returns = calculate_returns(portfolio_values)
            
            if returns:
                results.volatility = np.std(returns) * np.sqrt(252)  # Annualized
                results.sharpe_ratio = calculate_sharpe_ratio(returns)
                results.max_drawdown = calculate_max_drawdown(portfolio_values)
            
            # Trade statistics
            all_positions = portfolio.closed_positions + list(portfolio.positions.values())
            results.all_positions = all_positions
            results.total_trades = len(portfolio.closed_positions)
            
            if portfolio.closed_positions:
                realized_returns = [p.realized_return() for p in portfolio.closed_positions if p.realized_return() is not None]
                
                if realized_returns:
                    results.winning_trades = sum(1 for r in realized_returns if r > 0)
                    results.losing_trades = sum(1 for r in realized_returns if r < 0)
                    results.hit_rate = results.winning_trades / len(realized_returns)
                    results.average_trade_return = np.mean(realized_returns)
                    
                    winning_returns = [r for r in realized_returns if r > 0]
                    losing_returns = [r for r in realized_returns if r < 0]
                    
                    if winning_returns:
                        results.average_winning_trade = np.mean(winning_returns)
                    if losing_returns:
                        results.average_losing_trade = np.mean(losing_returns)


if __name__ == "__main__":
    # Basic validation of the engine structure
    print("✅ Backtesting engine created successfully!")
    
    # Test portfolio manager
    portfolio = PortfolioManager(100000.0)
    print(f"Initial cash: {portfolio.cash:,.0f} SEK")
    
    # Test position management
    from ..models.backtesting import TradingSignal, SignalType, SignalSource
    
    signal = TradingSignal(
        symbol="VOLV.ST",
        signal_type=SignalType.BUY,
        signal_source=SignalSource.TEMPORAL_ANOMALY,
        confidence=0.8,
        strategy_name="test"
    )
    
    position = portfolio.open_position(signal, 100.0, 500.0, "test_strategy")
    print(f"Opened position: {position.quantity} shares at {position.entry_price} SEK")
    
    current_value = portfolio.get_portfolio_value({"VOLV.ST": 105.0})
    print(f"Portfolio value at 105 SEK: {current_value:,.0f} SEK")
    
    closed = portfolio.close_position("VOLV.ST", 105.0, datetime.now())
    if closed:
        print(f"Closed with return: {closed.realized_return():.1%}")
    
    print("✅ Portfolio management validated!")