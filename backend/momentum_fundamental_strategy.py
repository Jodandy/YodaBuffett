#!/usr/bin/env python3
"""
Momentum + Fundamental Strategy

Combines technical momentum with fundamental quality:
- Strong price momentum (recent returns, RSI recovery)
- Fundamental quality (profitable, reasonable valuation)
- Risk management (stop losses, position sizing based on volatility)

This addresses the value trap issue by requiring momentum confirmation.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Tuple, Optional
import logging
from dataclasses import dataclass

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class MomentumFundamentalScore:
    """Combined momentum and fundamental score."""
    symbol: str
    date: date
    close_price: float
    
    # Momentum metrics
    return_1m: Optional[float]  # 1-month return
    return_3m: Optional[float]  # 3-month return
    rsi: Optional[float]
    volatility: Optional[float]
    
    # Fundamental metrics
    pe_ratio: Optional[float]
    roe: Optional[float]
    debt_to_equity: Optional[float]
    current_ratio: Optional[float]
    
    # Calculated scores
    momentum_score: float = 0   # 0-10
    fundamental_score: float = 0  # 0-10
    combined_score: float = 0
    
    # Performance
    future_return: Optional[float] = None

class MomentumFundamentalStrategy:
    """Strategy combining momentum and fundamental analysis."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def calculate_momentum_metrics(self, symbol: str, target_date: date) -> Dict:
        """Calculate momentum metrics for a symbol."""
        
        # Get recent price data
        query = """
        SELECT date, close_price::NUMERIC as close_price, volume::NUMERIC as volume
        FROM daily_price_data
        WHERE symbol = $1 
        AND date <= $2
        AND date >= $2 - INTERVAL '120 days'
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, target_date)
        
        if len(rows) < 60:  # Need enough data
            return {}
            
        df = pd.DataFrame([{'date': r['date'], 'close': float(r['close_price']), 'volume': float(r['volume'])} for r in rows])
        df = df.sort_values('date')
        
        latest_price = df['close'].iloc[-1]
        
        # Calculate returns
        if len(df) >= 22:  # ~1 month
            return_1m = (latest_price / df['close'].iloc[-22] - 1) * 100
        else:
            return_1m = None
            
        if len(df) >= 66:  # ~3 months
            return_3m = (latest_price / df['close'].iloc[-66] - 1) * 100
        else:
            return_3m = None
            
        # Calculate RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        latest_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None
        
        # Calculate volatility (20-day)
        returns = df['close'].pct_change()
        volatility = returns.rolling(20).std().iloc[-1] * np.sqrt(252) * 100  # Annualized
        
        return {
            'return_1m': return_1m,
            'return_3m': return_3m, 
            'rsi': latest_rsi,
            'volatility': volatility if not pd.isna(volatility) else None
        }
        
    async def get_momentum_fundamental_scores(self, target_date: date) -> List[MomentumFundamentalScore]:
        """Get combined scores for all companies."""
        
        # Get companies with recent fundamental data
        fund_query = """
        WITH latest_fundamentals AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                date,
                pe_ratio,
                debt_to_equity,
                current_ratio
            FROM historical_fundamentals_daily
            WHERE date <= $1 
            AND date >= $1 - INTERVAL '60 days'
            ORDER BY symbol, date DESC
        )
        SELECT * FROM latest_fundamentals
        WHERE pe_ratio IS NOT NULL
        """
        
        fund_rows = await self.db_conn.fetch(fund_query, target_date)
        
        scores = []
        
        for fund_row in fund_rows:
            symbol = fund_row['symbol']
            
            # Get current price
            price_query = """
            SELECT close_price::NUMERIC as close_price
            FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC
            LIMIT 1
            """
            
            price_row = await self.db_conn.fetchrow(price_query, symbol, target_date)
            if not price_row:
                continue
                
            close_price = float(price_row['close_price'])
            
            # Calculate momentum metrics
            momentum_metrics = await self.calculate_momentum_metrics(symbol, target_date)
            if not momentum_metrics:
                continue
                
            # Get ROE
            roe = await self.get_latest_roe(symbol, target_date)
            
            # Create score object
            score = MomentumFundamentalScore(
                symbol=symbol,
                date=target_date,
                close_price=close_price,
                return_1m=momentum_metrics.get('return_1m'),
                return_3m=momentum_metrics.get('return_3m'),
                rsi=momentum_metrics.get('rsi'),
                volatility=momentum_metrics.get('volatility'),
                pe_ratio=fund_row['pe_ratio'],
                debt_to_equity=fund_row['debt_to_equity'],
                current_ratio=fund_row['current_ratio'],
                roe=roe
            )
            
            # Calculate scores
            self.calculate_combined_scores(score)
            scores.append(score)
            
        return scores
        
    async def get_latest_roe(self, symbol: str, target_date: date) -> Optional[float]:
        """Get latest ROE from financial statements."""
        
        income_query = """
        SELECT net_income, period_date
        FROM financial_statements
        WHERE symbol = $1 
        AND period_date <= $2
        ORDER BY period_date DESC
        LIMIT 1
        """
        
        equity_query = """
        SELECT total_equity, period_date
        FROM balance_sheet_data  
        WHERE symbol = $1
        AND period_date <= $2
        ORDER BY period_date DESC
        LIMIT 1
        """
        
        income_row = await self.db_conn.fetchrow(income_query, symbol, target_date)
        equity_row = await self.db_conn.fetchrow(equity_query, symbol, target_date)
        
        if (income_row and equity_row and 
            income_row['net_income'] and equity_row['total_equity'] and 
            equity_row['total_equity'] > 0):
            
            return (income_row['net_income'] / equity_row['total_equity']) * 100
            
        return None
        
    def calculate_combined_scores(self, score: MomentumFundamentalScore):
        """Calculate momentum, fundamental, and combined scores."""
        
        # Momentum Score (0-10)
        momentum_components = []
        
        # Recent returns (higher = better momentum)
        if score.return_1m is not None:
            # 1-month return: 10 for +20%, 0 for -10%
            ret_1m_score = max(0, min(10, (score.return_1m + 10) / 3))
            momentum_components.append(ret_1m_score)
            
        # RSI (look for oversold recovery - RSI between 40-70 ideal)
        if score.rsi is not None:
            if 40 <= score.rsi <= 70:
                rsi_score = 10
            elif 30 <= score.rsi <= 80:
                rsi_score = 7
            elif score.rsi <= 30:  # Oversold - potential opportunity
                rsi_score = 8
            else:  # Overbought
                rsi_score = 3
            momentum_components.append(rsi_score)
            
        # Volatility (moderate volatility preferred - not too high or low)
        if score.volatility is not None:
            if 15 <= score.volatility <= 35:  # Sweet spot
                vol_score = 10
            elif 10 <= score.volatility <= 50:  # Acceptable
                vol_score = 7
            else:  # Too high or too low
                vol_score = 3
            momentum_components.append(vol_score)
            
        score.momentum_score = np.mean(momentum_components) if momentum_components else 0
        
        # Fundamental Score (0-10)
        fundamental_components = []
        
        # P/E ratio (reasonable valuation)
        if score.pe_ratio is not None and score.pe_ratio > 0:
            if 8 <= score.pe_ratio <= 20:  # Reasonable valuation
                pe_score = 10
            elif 5 <= score.pe_ratio <= 30:  # Acceptable
                pe_score = 7
            elif score.pe_ratio <= 35:  # High but not extreme
                pe_score = 4
            else:  # Too high
                pe_score = 1
            fundamental_components.append(pe_score)
            
        # ROE (profitability)
        if score.roe is not None:
            if score.roe >= 15:  # Excellent ROE
                roe_score = 10
            elif score.roe >= 10:  # Good ROE
                roe_score = 8
            elif score.roe >= 5:  # Acceptable ROE
                roe_score = 5
            else:  # Poor ROE
                roe_score = 2
            fundamental_components.append(roe_score)
            
        # Debt-to-equity (financial health)
        if score.debt_to_equity is not None:
            if score.debt_to_equity <= 50:  # Low debt
                de_score = 10
            elif score.debt_to_equity <= 100:  # Moderate debt
                de_score = 7
            elif score.debt_to_equity <= 200:  # High debt
                de_score = 4
            else:  # Very high debt
                de_score = 1
            fundamental_components.append(de_score)
            
        score.fundamental_score = np.mean(fundamental_components) if fundamental_components else 0
        
        # Combined Score (weighted)
        score.combined_score = (
            score.momentum_score * 0.6 +      # 60% momentum
            score.fundamental_score * 0.4     # 40% fundamentals
        )
        
    async def screen_momentum_fundamental(self, target_date: date, min_combined_score: float = 6.0, top_n: int = 15) -> List[MomentumFundamentalScore]:
        """Screen for stocks with strong momentum and solid fundamentals."""
        
        scores = await self.get_momentum_fundamental_scores(target_date)
        
        # Filter by minimum criteria
        filtered_scores = []
        for score in scores:
            if (score.combined_score >= min_combined_score and
                score.pe_ratio is not None and score.pe_ratio > 0 and score.pe_ratio < 50 and
                score.roe is not None and score.roe > 0):  # Must be profitable
                
                filtered_scores.append(score)
                
        # Sort by combined score
        filtered_scores.sort(key=lambda x: x.combined_score, reverse=True)
        return filtered_scores[:top_n]
        
    async def add_future_returns(self, scores: List[MomentumFundamentalScore], hold_days: int = 21) -> List[MomentumFundamentalScore]:
        """Add future returns for performance evaluation."""
        
        for score in scores:
            future_price_query = """
            SELECT close_price::NUMERIC as close_price
            FROM daily_price_data
            WHERE symbol = $1 
            AND date > $2
            AND date <= $3
            ORDER BY date
            LIMIT 1
            """
            
            future_date = score.date + timedelta(days=hold_days + 5)  # Allow for weekends
            future_row = await self.db_conn.fetchrow(
                future_price_query, 
                score.symbol, 
                score.date, 
                future_date
            )
            
            if future_row:
                score.future_return = (float(future_row['close_price']) / score.close_price - 1) * 100
                
        return scores
        
    async def backtest_momentum_fundamental(self, start_date: date, end_date: date, 
                                          rebalance_days: int = 21, portfolio_size: int = 8) -> Dict:
        """Backtest the momentum + fundamental strategy."""
        
        logger.info(f"🚀 Backtesting Momentum+Fundamental Strategy")
        logger.info(f"   Period: {start_date} to {end_date}")
        logger.info(f"   Rebalance every: {rebalance_days} days")
        logger.info(f"   Portfolio size: {portfolio_size} stocks")
        
        current_date = start_date
        portfolio_returns = []
        all_selections = []
        
        while current_date < end_date:
            logger.info(f"   Screening on {current_date}")
            
            # Screen for momentum + fundamental stocks
            candidates = await self.screen_momentum_fundamental(
                current_date, 
                min_combined_score=6.0, 
                top_n=portfolio_size
            )
            
            if not candidates:
                logger.warning(f"   No candidates found for {current_date}")
                current_date += timedelta(days=rebalance_days)
                continue
                
            # Calculate returns
            candidates = await self.add_future_returns(candidates, rebalance_days)
            
            # Portfolio performance
            valid_returns = [c.future_return for c in candidates if c.future_return is not None]
            
            if valid_returns:
                period_return = np.mean(valid_returns)
                portfolio_returns.append({
                    'date': current_date,
                    'return': period_return,
                    'stocks_count': len(valid_returns)
                })
                
                # Store selections
                all_selections.extend([{
                    'date': current_date,
                    'symbol': c.symbol,
                    'combined_score': c.combined_score,
                    'momentum_score': c.momentum_score,
                    'fundamental_score': c.fundamental_score,
                    'return_1m': c.return_1m,
                    'pe_ratio': c.pe_ratio,
                    'roe': c.roe,
                    'future_return': c.future_return
                } for c in candidates])
                
            current_date += timedelta(days=rebalance_days)
            
        # Calculate performance metrics
        if portfolio_returns:
            returns = [p['return'] for p in portfolio_returns]
            total_return = np.prod([1 + r/100 for r in returns]) - 1
            avg_return = np.mean(returns)
            volatility = np.std(returns)
            sharpe_ratio = avg_return / volatility if volatility > 0 else 0
            win_rate = len([r for r in returns if r > 0]) / len(returns)
            
            results = {
                'total_return': total_return * 100,
                'average_return_per_period': avg_return,
                'volatility': volatility,
                'sharpe_ratio': sharpe_ratio,
                'win_rate': win_rate,
                'rebalance_periods': len(portfolio_returns),
                'portfolio_returns': portfolio_returns,
                'top_selections': sorted(all_selections, key=lambda x: x['combined_score'], reverse=True)[:20]
            }
            
            logger.info(f"   ✅ Results:")
            logger.info(f"      Total return: {total_return*100:.2f}%")
            logger.info(f"      Average return per period: {avg_return:.2f}%")
            logger.info(f"      Win rate: {win_rate:.1%}")
            logger.info(f"      Sharpe ratio: {sharpe_ratio:.2f}")
            
            return results
        else:
            return {'error': 'No valid returns calculated'}
            
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Test momentum + fundamental strategy."""
    
    strategy = MomentumFundamentalStrategy()
    
    try:
        await strategy.setup()
        
        print("🚀 MOMENTUM + FUNDAMENTAL STRATEGY")
        print("=" * 60)
        
        # Current screening
        current_date = date(2024, 11, 1)
        print(f"\n📊 Momentum + Fundamental Screen for {current_date}:")
        
        candidates = await strategy.screen_momentum_fundamental(current_date, min_combined_score=6.0, top_n=12)
        
        if candidates:
            print(f"{'Symbol':<10} {'Combined':<9} {'Momentum':<9} {'Fund.':<9} {'1M Ret':<8} {'P/E':<8} {'ROE':<8}")
            print("-" * 75)
            
            for candidate in candidates:
                print(f"{candidate.symbol:<10} "
                      f"{candidate.combined_score:<9.1f} "
                      f"{candidate.momentum_score:<9.1f} "
                      f"{candidate.fundamental_score:<9.1f} "
                      f"{candidate.return_1m:<8.1f} " if candidate.return_1m else f"{'N/A':<8} "
                      f"{candidate.pe_ratio:<8.1f} " if candidate.pe_ratio else f"{'N/A':<8} "
                      f"{candidate.roe:<8.1f}" if candidate.roe else f"{'N/A':<8}")
        
        # Backtest
        print(f"\n📈 Backtesting (2023-2024):")
        
        results = await strategy.backtest_momentum_fundamental(
            date(2023, 6, 1),
            date(2024, 10, 1),
            rebalance_days=21,  # Monthly rebalancing
            portfolio_size=8
        )
        
        if 'error' not in results:
            print(f"\n🎯 Backtest Results:")
            print(f"   Total Return: {results['total_return']:.2f}%")
            print(f"   Average Return per Period: {results['average_return_per_period']:.2f}%")
            print(f"   Win Rate: {results['win_rate']:.1%}")
            print(f"   Sharpe Ratio: {results['sharpe_ratio']:.2f}")
            print(f"   Rebalance Periods: {results['rebalance_periods']}")
            
            print(f"\n🏆 Top Momentum+Fundamental Picks:")
            for pick in results['top_selections'][:10]:
                print(f"   {pick['symbol']} ({pick['date']}): Score {pick['combined_score']:.1f}, 1M {pick['return_1m']:.1f}%, Return {pick['future_return']:.1f}%")
        else:
            print(f"   ❌ {results['error']}")
                
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await strategy.cleanup()

if __name__ == "__main__":
    asyncio.run(main())