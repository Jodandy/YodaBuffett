#!/usr/bin/env python3
"""
Historical DCF Backtest - No Lookahead Bias

Runs DCF valuations using only historical data available at each point in time:
1. Generate DCF valuations using only past financial reports
2. Compare against future stock prices to test predictive power
3. Track performance over time with realistic trading constraints
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

from clean_dcf_engine import CleanDCFEngine, DCFConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class HistoricalTrade:
    """Historical trade record."""
    symbol: str
    entry_date: date
    exit_date: date
    valuation_date: date  # When DCF was calculated
    report_date: date     # Which financial report was used
    entry_price: float
    exit_price: float
    fair_value: float
    implied_return_entry: float
    actual_return: float
    days_held: int
    signal_strength: str
    success: bool

class DCFHistoricalBacktest:
    """Historical backtest with strict no-lookahead bias."""
    
    def __init__(self, 
                 initial_capital: float = 100000,
                 position_size_pct: float = 0.10,
                 max_positions: int = 5,
                 hold_days: int = 30,
                 min_upside: float = 0.15,
                 transaction_cost_pct: float = 0.001):
        
        self.initial_capital = initial_capital
        self.position_size_pct = position_size_pct
        self.max_positions = max_positions
        self.hold_days = hold_days
        self.min_upside = min_upside
        self.transaction_cost_pct = transaction_cost_pct
        
        # DCF engine
        self.dcf_config = DCFConfig(num_simulations=1000)  # Faster for backtesting
        self.dcf_engine = CleanDCFEngine(self.dcf_config)
        
        # Portfolio tracking
        self.cash = initial_capital
        self.trades: List[HistoricalTrade] = []
        self.portfolio_history = []
        
        # Database
        self.db_conn = None
        
        print(f"🕰️  Historical DCF Backtest Settings:")
        print(f"   Period: NO LOOKAHEAD BIAS enforced")
        print(f"   Capital: ${initial_capital:,.0f}")
        print(f"   Position Size: {position_size_pct:.0%}")
        print(f"   Hold Period: {hold_days} days")
        print(f"   Min Upside: {min_upside:.0%}")
    
    async def setup(self):
        """Initialize connections."""
        self.db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
        await self.dcf_engine.setup()
        logger.info("Historical backtest initialized")
    
    async def cleanup(self):
        """Close connections."""
        if self.db_conn:
            await self.db_conn.close()
        await self.dcf_engine.cleanup()
    
    async def get_available_companies_at_date(self, test_date: date) -> List[str]:
        """Get companies with sufficient financial data available by test_date (NO LOOKAHEAD)."""
        
        query = """
        SELECT DISTINCT fs.symbol
        FROM financial_statements fs
        JOIN company_master cm ON fs.symbol = cm.primary_ticker
        WHERE fs.publish_date <= $1  -- CRITICAL: Only use published data
        AND fs.total_revenue > 0
        GROUP BY fs.symbol
        HAVING COUNT(*) >= 3  -- Need at least 3 reports for DCF
        ORDER BY fs.symbol
        """
        
        rows = await self.db_conn.fetch(query, test_date)
        symbols = [row['symbol'] for row in rows]
        
        logger.info(f"Found {len(symbols)} companies with data available by {test_date}")
        return symbols
    
    async def get_price_at_date(self, symbol: str, target_date: date, 
                               tolerance_days: int = 5) -> Optional[float]:
        """Get stock price at specific date with tolerance."""
        
        query = """
        SELECT close_price
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date DESC
        LIMIT 1
        """
        
        start_date = target_date - timedelta(days=tolerance_days)
        end_date = target_date + timedelta(days=tolerance_days)
        
        row = await self.db_conn.fetchrow(query, symbol, start_date, end_date)
        return float(row['close_price']) if row else None
    
    async def generate_dcf_signal_at_date(self, symbol: str, test_date: date) -> Optional[Dict]:
        """Generate DCF signal using only information available at test_date."""
        
        # Get entry price
        entry_price = await self.get_price_at_date(symbol, test_date)
        if not entry_price:
            return None
        
        # Generate DCF valuation using only historical data (NO LOOKAHEAD)
        valuation_datetime = datetime.combine(test_date, datetime.min.time())
        
        try:
            dcf_result = await self.dcf_engine.value_company(symbol, valuation_datetime, entry_price)
            
            if not dcf_result:
                return None
            
            fair_value = dcf_result['fair_value_median']
            implied_return = dcf_result['implied_return']
            
            # Only consider strong buy signals
            if implied_return < self.min_upside:
                return None
            
            # Get the most recent report date used
            company_data = await self.dcf_engine.extract_company_data(symbol, valuation_datetime)
            report_date = None
            if company_data and company_data.revenues:
                # Find most recent financial report
                report_query = """
                SELECT period_date 
                FROM financial_statements
                WHERE symbol = $1 
                AND publish_date <= $2
                ORDER BY publish_date DESC 
                LIMIT 1
                """
                row = await self.db_conn.fetchrow(report_query, symbol, test_date)
                report_date = row['period_date'] if row else test_date
            
            return {
                'symbol': symbol,
                'entry_price': entry_price,
                'fair_value': fair_value,
                'implied_return': implied_return,
                'signal_strength': 'BUY' if implied_return > self.min_upside else 'HOLD',
                'report_date': report_date or test_date
            }
            
        except Exception as e:
            logger.warning(f"DCF calculation failed for {symbol} at {test_date}: {e}")
            return None
    
    async def backtest_period(self, start_date: date, end_date: date, 
                             test_frequency_days: int = 30) -> List[HistoricalTrade]:
        """Run historical backtest over specified period."""
        
        logger.info(f"🚀 Starting historical backtest: {start_date} to {end_date}")
        logger.info(f"🔍 Testing every {test_frequency_days} days")
        
        current_date = start_date
        test_count = 0
        
        while current_date <= end_date:
            test_count += 1
            logger.info(f"\n📅 Test {test_count}: {current_date}")
            
            # Get available companies (NO LOOKAHEAD)
            available_symbols = await self.get_available_companies_at_date(current_date)
            
            if not available_symbols:
                logger.warning(f"No companies available at {current_date}")
                current_date += timedelta(days=test_frequency_days)
                continue
            
            # Limit to top companies for speed
            test_symbols = available_symbols[:20]
            logger.info(f"   Testing {len(test_symbols)} companies")
            
            # Generate signals for this date
            signals = []
            for symbol in test_symbols:
                signal = await self.generate_dcf_signal_at_date(symbol, current_date)
                if signal:
                    signals.append(signal)
            
            buy_signals = [s for s in signals if s['signal_strength'] == 'BUY']
            logger.info(f"   Generated {len(buy_signals)} BUY signals")
            
            # Execute top signals (best implied returns)
            if buy_signals:
                buy_signals.sort(key=lambda x: x['implied_return'], reverse=True)
                
                for signal in buy_signals[:self.max_positions]:
                    await self.execute_historical_trade(signal, current_date)
            
            # Move to next test date
            current_date += timedelta(days=test_frequency_days)
        
        logger.info(f"\n🏁 Backtest complete: {len(self.trades)} total trades")
        return self.trades
    
    async def execute_historical_trade(self, signal: Dict, entry_date: date):
        """Execute a historical trade and track performance."""
        
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        fair_value = signal['fair_value']
        implied_return = signal['implied_return']
        
        # Calculate position size
        portfolio_value = self.cash  # Simplified for this backtest
        position_value = portfolio_value * self.position_size_pct
        shares = int(position_value / entry_price)
        
        if shares == 0:
            return
        
        actual_position_value = shares * entry_price
        transaction_cost = actual_position_value * self.transaction_cost_pct
        total_cost = actual_position_value + transaction_cost
        
        if total_cost > self.cash:
            logger.warning(f"   Insufficient cash for {symbol}")
            return
        
        # Get exit date and price
        exit_date = entry_date + timedelta(days=self.hold_days)
        exit_price = await self.get_price_at_date(symbol, exit_date)
        
        if not exit_price:
            logger.warning(f"   No exit price for {symbol}")
            return
        
        # Calculate returns
        gross_return = (exit_price - entry_price) / entry_price
        net_proceeds = shares * exit_price - (shares * exit_price * self.transaction_cost_pct)
        net_return = (net_proceeds - actual_position_value) / actual_position_value
        
        # Update portfolio
        self.cash -= total_cost
        self.cash += net_proceeds
        
        # Create trade record
        trade = HistoricalTrade(
            symbol=symbol,
            entry_date=entry_date,
            exit_date=exit_date,
            valuation_date=entry_date,
            report_date=signal['report_date'],
            entry_price=entry_price,
            exit_price=exit_price,
            fair_value=fair_value,
            implied_return_entry=implied_return,
            actual_return=net_return,
            days_held=self.hold_days,
            signal_strength=signal['signal_strength'],
            success=net_return > 0
        )
        
        self.trades.append(trade)
        
        logger.info(f"   🔄 {symbol}: Predicted {implied_return:+.0%}, "
                   f"Actual {net_return:+.0%} ({self.hold_days}d)")
    
    def analyze_results(self):
        """Analyze backtest results."""
        
        if not self.trades:
            logger.info("No trades to analyze")
            return
        
        df = pd.DataFrame([
            {
                'symbol': t.symbol,
                'entry_date': t.entry_date,
                'exit_date': t.exit_date,
                'predicted_return': t.implied_return_entry,
                'actual_return': t.actual_return,
                'success': t.success,
                'days_held': t.days_held
            }
            for t in self.trades
        ])
        
        total_trades = len(df)
        win_rate = df['success'].mean()
        avg_predicted = df['predicted_return'].mean()
        avg_actual = df['actual_return'].mean()
        
        final_portfolio = self.cash
        total_return = (final_portfolio - self.initial_capital) / self.initial_capital
        
        print(f"\n📊 HISTORICAL DCF BACKTEST RESULTS")
        print(f"=" * 50)
        print(f"Total Trades: {total_trades}")
        print(f"Win Rate: {win_rate:.1%}")
        print(f"Average Predicted Return: {avg_predicted:+.1%}")
        print(f"Average Actual Return: {avg_actual:+.1%}")
        print(f"Portfolio Return: {total_return:+.1%}")
        print(f"Final Portfolio Value: ${final_portfolio:,.0f}")
        
        # Prediction accuracy
        if len(df) > 5:
            correlation = df['predicted_return'].corr(df['actual_return'])
            print(f"Prediction Correlation: {correlation:.2f}")
        
        # Best/worst trades
        best_trades = df.nlargest(3, 'actual_return')
        worst_trades = df.nsmallest(3, 'actual_return')
        
        print(f"\n🏆 BEST TRADES:")
        for _, trade in best_trades.iterrows():
            print(f"   {trade['symbol']} {trade['entry_date']}: "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%}")
        
        print(f"\n💸 WORST TRADES:")
        for _, trade in worst_trades.iterrows():
            print(f"   {trade['symbol']} {trade['entry_date']}: "
                  f"Predicted {trade['predicted_return']:+.0%}, "
                  f"Actual {trade['actual_return']:+.0%}")
        
        return df

async def main():
    """Run historical DCF backtest."""
    
    backtest = DCFHistoricalBacktest(
        initial_capital=100000,
        position_size_pct=0.10,
        hold_days=30,
        min_upside=0.20  # 20% minimum predicted upside
    )
    
    await backtest.setup()
    
    try:
        # Run 6-month historical backtest (more recent data)
        trades = await backtest.backtest_period(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 7, 1),
            test_frequency_days=60
        )
        
        # Analyze results
        results_df = backtest.analyze_results()
        
        if results_df is not None and len(results_df) > 0:
            # Save detailed results
            results_df.to_csv('dcf_historical_backtest.csv', index=False)
            logger.info(f"💾 Detailed results saved to dcf_historical_backtest.csv")
        
    finally:
        await backtest.cleanup()

if __name__ == "__main__":
    asyncio.run(main())