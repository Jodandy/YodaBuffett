#!/usr/bin/env python3
"""
DCF Price Analyzer

Compares daily stock prices against stored DCF valuations to generate 
investment signals. This system uses pre-computed valuations from the
dcf_valuations table rather than calculating DCF on-demand.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DCFSignal:
    """DCF-based investment signal."""
    symbol: str
    signal_date: date
    stock_price: float
    fair_value_median: float
    fair_value_p25: float
    fair_value_p75: float
    implied_return: float
    signal_strength: str  # 'STRONG_BUY', 'BUY', 'HOLD', 'SELL', 'STRONG_SELL'
    confidence: float
    report_date: date
    publish_date: date
    days_since_report: int

class DCFPriceAnalyzer:
    """Analyzes stock prices against stored DCF valuations."""
    
    def __init__(self, model_version: str = "clean_dcf_v1.0"):
        self.model_version = model_version
        self.db_conn = None
        
        # Signal thresholds
        self.strong_buy_threshold = 0.30   # 30%+ upside
        self.buy_threshold = 0.15         # 15%+ upside  
        self.sell_threshold = -0.15       # -15% downside
        self.strong_sell_threshold = -0.30 # -30% downside
        
        # Confidence requirements
        self.min_confidence = 0.20        # Minimum 20% confidence
        self.max_days_since_report = 365  # Max 1 year old valuation
    
    async def setup(self):
        """Initialize database connection."""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        logger.info(f"DCF Price Analyzer initialized with model version: {self.model_version}")
    
    async def cleanup(self):
        """Close database connection."""
        if self.db_conn:
            await self.db_conn.close()
    
    async def get_valuation_for_date(self, symbol: str, target_date: date) -> Optional[Dict]:
        """Get most recent DCF valuation available before target date."""
        
        query = """
        SELECT 
            symbol, report_date, publish_date,
            fair_value_stock_median, fair_value_stock_p25, fair_value_stock_p75,
            valuation_confidence, model_version
        FROM dcf_valuations
        WHERE symbol = $1
        AND publish_date <= $2
        AND model_version = $3
        AND valuation_confidence >= $4
        ORDER BY publish_date DESC
        LIMIT 1
        """
        
        row = await self.db_conn.fetchrow(
            query, symbol, target_date, self.model_version, self.min_confidence
        )
        
        if row:
            days_since_report = (target_date - row['publish_date']).days
            if days_since_report <= self.max_days_since_report:
                return dict(row, days_since_report=days_since_report)
        
        return None
    
    async def get_price_for_date(self, symbol: str, target_date: date) -> Optional[float]:
        """Get stock price for a specific date (with tolerance)."""
        
        query = """
        SELECT close_price
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date DESC
        LIMIT 1
        """
        
        start_date = target_date - timedelta(days=3)
        end_date = target_date + timedelta(days=1)
        
        row = await self.db_conn.fetchrow(query, symbol, start_date, end_date)
        return float(row['close_price']) if row else None
    
    def calculate_signal_strength(self, implied_return: float, confidence: float) -> str:
        """Calculate signal strength based on return and confidence."""
        
        # Adjust thresholds based on confidence
        confidence_multiplier = confidence  # Higher confidence = lower thresholds
        
        strong_buy_adj = self.strong_buy_threshold * (2 - confidence_multiplier)
        buy_adj = self.buy_threshold * (2 - confidence_multiplier)
        sell_adj = self.sell_threshold * (2 - confidence_multiplier)
        strong_sell_adj = self.strong_sell_threshold * (2 - confidence_multiplier)
        
        if implied_return >= strong_buy_adj:
            return 'STRONG_BUY'
        elif implied_return >= buy_adj:
            return 'BUY'
        elif implied_return <= strong_sell_adj:
            return 'STRONG_SELL'
        elif implied_return <= sell_adj:
            return 'SELL'
        else:
            return 'HOLD'
    
    async def generate_signal(self, symbol: str, target_date: date) -> Optional[DCFSignal]:
        """Generate DCF signal for a symbol on a specific date."""
        
        # Get valuation
        valuation = await self.get_valuation_for_date(symbol, target_date)
        if not valuation:
            return None
        
        # Get price
        stock_price = await self.get_price_for_date(symbol, target_date)
        if not stock_price:
            return None
        
        # Calculate implied return
        fair_value = valuation['fair_value_stock_median']
        if fair_value <= 0:
            return None
        
        implied_return = (fair_value - stock_price) / stock_price
        
        # Calculate signal strength
        confidence = valuation['valuation_confidence']
        signal_strength = self.calculate_signal_strength(implied_return, confidence)
        
        signal = DCFSignal(
            symbol=symbol,
            signal_date=target_date,
            stock_price=stock_price,
            fair_value_median=fair_value,
            fair_value_p25=valuation['fair_value_stock_p25'],
            fair_value_p75=valuation['fair_value_stock_p75'],
            implied_return=implied_return,
            signal_strength=signal_strength,
            confidence=confidence,
            report_date=valuation['report_date'],
            publish_date=valuation['publish_date'],
            days_since_report=valuation['days_since_report']
        )
        
        return signal
    
    async def scan_market(self, target_date: date, symbols: List[str] = None) -> List[DCFSignal]:
        """Scan market for DCF signals on a specific date."""
        
        if symbols is None:
            # Get all symbols with DCF valuations
            symbol_query = """
            SELECT DISTINCT symbol
            FROM dcf_valuations
            WHERE model_version = $1
            AND valuation_confidence >= $2
            ORDER BY symbol
            """
            rows = await self.db_conn.fetch(symbol_query, self.model_version, self.min_confidence)
            symbols = [row['symbol'] for row in rows]
        
        logger.info(f"Scanning {len(symbols)} symbols for {target_date}")
        
        signals = []
        
        for symbol in symbols:
            signal = await self.generate_signal(symbol, target_date)
            if signal:
                signals.append(signal)
        
        # Sort by implied return (best opportunities first)
        signals.sort(key=lambda x: x.implied_return, reverse=True)
        
        return signals
    
    async def backtest_signals(self, start_date: date, end_date: date, 
                              hold_days: int = 30) -> List[Dict]:
        """Backtest DCF signals over a date range."""
        
        logger.info(f"Backtesting DCF signals from {start_date} to {end_date}")
        
        # Get symbols with valuations
        symbols_query = """
        SELECT DISTINCT v.symbol
        FROM dcf_valuations v
        JOIN daily_price_data p ON v.symbol = p.symbol
        WHERE v.model_version = $1
        AND v.valuation_confidence >= $2
        AND p.date BETWEEN $3 AND $4
        LIMIT 20  -- Test with subset first
        """
        
        rows = await self.db_conn.fetch(
            symbols_query, self.model_version, self.min_confidence, start_date, end_date
        )
        symbols = [row['symbol'] for row in rows]
        
        logger.info(f"Testing {len(symbols)} symbols")
        
        backtest_results = []
        
        # Test monthly signals
        current_date = start_date
        while current_date <= end_date:
            logger.info(f"Generating signals for {current_date}")
            
            signals = await self.scan_market(current_date, symbols)
            buy_signals = [s for s in signals if s.signal_strength in ['BUY', 'STRONG_BUY']]
            
            # Test each buy signal
            for signal in buy_signals[:5]:  # Top 5 signals
                exit_date = current_date + timedelta(days=hold_days)
                if exit_date > end_date:
                    continue
                
                exit_price = await self.get_price_for_date(signal.symbol, exit_date)
                if exit_price:
                    actual_return = (exit_price - signal.stock_price) / signal.stock_price
                    
                    result = {
                        'symbol': signal.symbol,
                        'entry_date': current_date,
                        'exit_date': exit_date,
                        'entry_price': signal.stock_price,
                        'exit_price': exit_price,
                        'fair_value': signal.fair_value_median,
                        'predicted_return': signal.implied_return,
                        'actual_return': actual_return,
                        'signal_strength': signal.signal_strength,
                        'confidence': signal.confidence,
                        'success': actual_return > 0,
                        'days_since_report': signal.days_since_report
                    }
                    
                    backtest_results.append(result)
                    
                    logger.info(f"  {signal.symbol}: Predicted {signal.implied_return:+.0%}, "
                              f"Actual {actual_return:+.0%}")
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return backtest_results
    
    def analyze_backtest_results(self, results: List[Dict]):
        """Analyze backtest results and print summary."""
        
        if not results:
            logger.info("No backtest results to analyze")
            return
        
        df = pd.DataFrame(results)
        
        total_trades = len(df)
        win_rate = (df['success'].sum() / total_trades) if total_trades > 0 else 0
        avg_predicted = df['predicted_return'].mean()
        avg_actual = df['actual_return'].mean()
        
        # Performance by signal strength
        by_strength = df.groupby('signal_strength').agg({
            'actual_return': ['count', 'mean', lambda x: (x > 0).mean()],
            'predicted_return': 'mean'
        }).round(3)
        
        logger.info(f"\n📊 DCF Signal Backtest Results:")
        logger.info(f"   Total Trades: {total_trades}")
        logger.info(f"   Win Rate: {win_rate:.1%}")
        logger.info(f"   Avg Predicted Return: {avg_predicted:+.1%}")
        logger.info(f"   Avg Actual Return: {avg_actual:+.1%}")
        
        logger.info(f"\n📈 Performance by Signal Strength:")
        logger.info(f"{by_strength}")
        
        # Correlation
        if len(df) > 5:
            correlation = df['predicted_return'].corr(df['actual_return'])
            logger.info(f"   Prediction Correlation: {correlation:.2f}")

async def main():
    """Test the DCF price analyzer."""
    
    analyzer = DCFPriceAnalyzer()
    await analyzer.setup()
    
    try:
        # Test current signals
        today = date.today()
        signals = await analyzer.scan_market(today)
        
        if signals:
            logger.info(f"\n🎯 Current DCF Signals ({today}):")
            for signal in signals[:10]:  # Top 10
                logger.info(f"   {signal.symbol}: {signal.signal_strength} "
                          f"{signal.implied_return:+.0%} "
                          f"(Fair: {signal.fair_value_median:.0f}, "
                          f"Price: {signal.stock_price:.0f}, "
                          f"Conf: {signal.confidence:.0%})")
        
        # Run backtest
        logger.info(f"\n🧪 Running DCF Signal Backtest")
        results = await analyzer.backtest_signals(
            date(2024, 1, 1), 
            date(2024, 6, 1), 
            hold_days=30
        )
        
        analyzer.analyze_backtest_results(results)
        
        # Save results
        if results:
            results_df = pd.DataFrame(results)
            results_df.to_csv('dcf_signal_backtest.csv', index=False)
            logger.info(f"💾 Results saved to dcf_signal_backtest.csv")
    
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())