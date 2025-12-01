#!/usr/bin/env python3
"""
CLI for running backtests with the YodaBuffett backtesting framework.

This provides a simple interface to test strategies and generate performance reports.
"""

import asyncio
import argparse
import logging
import json
from datetime import date, datetime, timedelta
from typing import Dict, Any

from services.backtesting_engine import BacktestingEngine
from services.nordic_market_data import create_market_data_provider
from services.temporal_anomaly_strategy import TemporalAnomalyStrategy
from models.backtesting import BacktestConfig, BacktestResults

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_backtest_config(
    start_date: str,
    end_date: str,
    initial_capital: float = 1000000.0,
    **kwargs
) -> BacktestConfig:
    """Create backtesting configuration from parameters"""
    
    return BacktestConfig(
        start_date=datetime.strptime(start_date, "%Y-%m-%d").date(),
        end_date=datetime.strptime(end_date, "%Y-%m-%d").date(),
        initial_capital=initial_capital,
        max_positions=kwargs.get('max_positions', 15),
        max_position_size=kwargs.get('max_position_size', 0.10),
        commission_rate=kwargs.get('commission_rate', 0.001),
        rebalance_frequency_days=kwargs.get('rebalance_days', 30)
    )


def print_backtest_results(results: BacktestResults) -> None:
    """Print formatted backtest results to console"""
    
    print("\n" + "="*80)
    print(f"🚀 BACKTEST RESULTS: {results.strategy_name}")
    print("="*80)
    
    # Basic performance
    print(f"\n📊 PERFORMANCE SUMMARY")
    print(f"   Period: {results.config.start_date} to {results.config.end_date}")
    print(f"   Initial Capital: {results.config.initial_capital:,.0f} SEK")
    print(f"   Final Value: {results.portfolio_values[-1][1]:,.0f} SEK" if results.portfolio_values else "N/A")
    print(f"   Total Return: {results.total_return:+.1%}")
    print(f"   Annualized Return: {results.annualized_return:+.1%}")
    print(f"   Benchmark Return: {results.benchmark_return:+.1%}")
    print(f"   Excess Return: {results.excess_return:+.1%}")
    
    # Risk metrics
    print(f"\n📈 RISK METRICS")
    print(f"   Volatility (annualized): {results.volatility:.1%}")
    print(f"   Sharpe Ratio: {results.sharpe_ratio:.2f}")
    print(f"   Maximum Drawdown: {results.max_drawdown:.1%}")
    
    # Trade statistics
    print(f"\n🔄 TRADE STATISTICS")
    print(f"   Total Trades: {results.total_trades}")
    print(f"   Winning Trades: {results.winning_trades}")
    print(f"   Losing Trades: {results.losing_trades}")
    print(f"   Hit Rate: {results.hit_rate:.1%}")
    print(f"   Average Trade Return: {results.average_trade_return:+.1%}")
    if results.average_winning_trade:
        print(f"   Average Winning Trade: {results.average_winning_trade:+.1%}")
    if results.average_losing_trade:
        print(f"   Average Losing Trade: {results.average_losing_trade:+.1%}")
    print(f"   Profit Factor: {results.profit_factor():.2f}")
    
    # Signal analysis
    print(f"\n🚨 SIGNAL ANALYSIS")
    print(f"   Total Signals Generated: {len(results.all_signals)}")
    
    if results.all_signals:
        buy_signals = [s for s in results.all_signals if s.signal_type.value in ['buy', 'strong_buy']]
        sell_signals = [s for s in results.all_signals if s.signal_type.value in ['sell', 'strong_sell']]
        
        print(f"   Buy Signals: {len(buy_signals)}")
        print(f"   Sell Signals: {len(sell_signals)}")
        
        if buy_signals:
            avg_buy_confidence = sum(s.confidence for s in buy_signals) / len(buy_signals)
            print(f"   Average Buy Signal Confidence: {avg_buy_confidence:.1%}")
        
        if sell_signals:
            avg_sell_confidence = sum(s.confidence for s in sell_signals) / len(sell_signals)
            print(f"   Average Sell Signal Confidence: {avg_sell_confidence:.1%}")
        
        # Show top signals
        top_signals = sorted(results.all_signals, key=lambda s: s.confidence, reverse=True)[:5]
        print(f"\n   🔝 Top 5 Signals by Confidence:")
        for i, signal in enumerate(top_signals, 1):
            print(f"      {i}. {signal.signal_type.value.upper()} {signal.symbol} "
                  f"({signal.confidence:.1%}, {signal.timestamp.strftime('%Y-%m-%d')})")
    
    # Position analysis
    if results.all_positions:
        print(f"\n💼 POSITION ANALYSIS")
        closed_positions = [p for p in results.all_positions if not p.is_open]
        
        if closed_positions:
            winning_positions = [p for p in closed_positions if p.realized_return() and p.realized_return() > 0]
            losing_positions = [p for p in closed_positions if p.realized_return() and p.realized_return() < 0]
            
            print(f"   Closed Positions: {len(closed_positions)}")
            print(f"   Winning Positions: {len(winning_positions)}")
            print(f"   Losing Positions: {len(losing_positions)}")
            
            # Best and worst trades
            if closed_positions:
                best_trade = max(closed_positions, key=lambda p: p.realized_return() or 0)
                worst_trade = min(closed_positions, key=lambda p: p.realized_return() or 0)
                
                print(f"\n   🏆 Best Trade: {best_trade.symbol} {best_trade.realized_return():+.1%}")
                print(f"   💸 Worst Trade: {worst_trade.symbol} {worst_trade.realized_return():+.1%}")
        
        open_positions = [p for p in results.all_positions if p.is_open]
        if open_positions:
            print(f"   Open Positions: {len(open_positions)}")
            for pos in open_positions[:5]:  # Show first 5
                print(f"      {pos.symbol} ({pos.entry_date.strftime('%Y-%m-%d')})")
    
    print("\n" + "="*80)


def save_results_to_file(results: BacktestResults, filename: str) -> None:
    """Save backtest results to JSON file"""
    
    # Convert results to serializable format
    results_dict = {
        'backtest_id': results.backtest_id,
        'strategy_name': results.strategy_name,
        'start_time': results.start_time.isoformat(),
        'end_time': results.end_time.isoformat() if results.end_time else None,
        'config': {
            'start_date': results.config.start_date.isoformat(),
            'end_date': results.config.end_date.isoformat(),
            'initial_capital': results.config.initial_capital,
            'max_positions': results.config.max_positions,
            'commission_rate': results.config.commission_rate
        },
        'performance': {
            'total_return': results.total_return,
            'annualized_return': results.annualized_return,
            'volatility': results.volatility,
            'sharpe_ratio': results.sharpe_ratio,
            'max_drawdown': results.max_drawdown
        },
        'trades': {
            'total_trades': results.total_trades,
            'hit_rate': results.hit_rate,
            'average_trade_return': results.average_trade_return,
            'profit_factor': results.profit_factor()
        },
        'signals_count': len(results.all_signals),
        'portfolio_history_points': len(results.portfolio_values)
    }
    
    with open(filename, 'w') as f:
        json.dump(results_dict, f, indent=2)
    
    print(f"💾 Results saved to: {filename}")


async def run_temporal_anomaly_backtest(
    start_date: str,
    end_date: str,
    initial_capital: float,
    save_file: str = None,
    strategy_params: Dict[str, Any] = None
) -> BacktestResults:
    """Run a backtest with the Temporal Anomaly Strategy"""
    
    print("🚀 YODABUFFETT TEMPORAL ANOMALY BACKTEST")
    print("="*60)
    
    # Create market data provider
    print("📊 Initializing market data provider...")
    market_data_provider = create_market_data_provider("mock")  # Use mock data for testing
    
    # Create strategy
    print("🧠 Initializing Temporal Anomaly Strategy...")
    strategy_params = strategy_params or {}
    strategy = TemporalAnomalyStrategy(**strategy_params)
    
    # Create backtest configuration
    print("⚙️ Setting up backtest configuration...")
    config = create_backtest_config(start_date, end_date, initial_capital)
    
    print(f"📅 Backtest period: {start_date} to {end_date}")
    print(f"💰 Initial capital: {initial_capital:,.0f} SEK")
    print(f"🎯 Max positions: {config.max_positions}")
    
    # Create backtesting engine
    print("🔧 Initializing backtesting engine...")
    engine = BacktestingEngine(market_data_provider)
    
    # Run backtest
    print("\n🏃 Running backtest...")
    try:
        results = await engine.run_backtest(strategy, config)
        
        # Print results
        print_backtest_results(results)
        
        # Save to file if requested
        if save_file:
            save_results_to_file(results, save_file)
        
        return results
        
    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        logger.error(f"Backtest error: {e}", exc_info=True)
        raise


async def run_quick_test():
    """Run a quick test of the backtesting framework"""
    
    print("🧪 QUICK FRAMEWORK TEST")
    print("="*40)
    
    # Short test period
    start_date = "2023-06-01"
    end_date = "2023-12-31"
    initial_capital = 500000.0  # 500K SEK
    
    # Conservative strategy parameters for testing
    strategy_params = {
        'min_confidence': 0.70,
        'anomaly_threshold': 0.25,
        'holding_period_days': 45,
        'max_position_size': 0.08
    }
    
    try:
        results = await run_temporal_anomaly_backtest(
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            strategy_params=strategy_params
        )
        
        print(f"\n✅ Quick test completed!")
        print(f"📊 Framework validation: {'PASSED' if results.total_return != 0 else 'NEEDS_REVIEW'}")
        
    except Exception as e:
        print(f"❌ Quick test failed: {e}")


def main():
    """Main CLI interface"""
    
    parser = argparse.ArgumentParser(description="YodaBuffett Backtesting CLI")
    
    parser.add_argument('command', choices=['test', 'backtest'], 
                       help="Command to run")
    
    # Backtest parameters
    parser.add_argument('--start-date', default="2022-01-01",
                       help="Start date (YYYY-MM-DD)")
    parser.add_argument('--end-date', default="2023-12-31", 
                       help="End date (YYYY-MM-DD)")
    parser.add_argument('--capital', type=float, default=1000000.0,
                       help="Initial capital in SEK")
    
    # Strategy parameters
    parser.add_argument('--min-confidence', type=float, default=0.65,
                       help="Minimum anomaly confidence threshold")
    parser.add_argument('--anomaly-threshold', type=float, default=0.30,
                       help="Maximum similarity for anomaly detection")
    parser.add_argument('--holding-days', type=int, default=60,
                       help="Target holding period in days")
    
    # Output options
    parser.add_argument('--save', type=str,
                       help="Save results to JSON file")
    parser.add_argument('--verbose', action='store_true',
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if args.command == 'test':
        asyncio.run(run_quick_test())
    
    elif args.command == 'backtest':
        strategy_params = {
            'min_confidence': args.min_confidence,
            'anomaly_threshold': args.anomaly_threshold,
            'holding_period_days': args.holding_days
        }
        
        asyncio.run(run_temporal_anomaly_backtest(
            start_date=args.start_date,
            end_date=args.end_date,
            initial_capital=args.capital,
            save_file=args.save,
            strategy_params=strategy_params
        ))


if __name__ == "__main__":
    main()