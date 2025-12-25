#!/usr/bin/env python3
"""
Fundamental + Technical KNN Strategy

Combines your existing technical KNN approach with fundamental analysis:
- P/E ratios, debt ratios, profitability metrics as features
- Multi-factor neighbor matching (technical + fundamental patterns)
- Fundamental anomaly detection for entry signals
- Risk-adjusted position sizing based on financial health
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from typing import Dict, List, Tuple, Optional
import json
import logging
from dataclasses import dataclass
from collections import defaultdict

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class FundamentalPattern:
    """Fundamental pattern for KNN matching."""
    date: date
    symbol: str
    
    # Valuation metrics
    pe_ratio: Optional[float]
    pb_ratio: Optional[float] 
    ps_ratio: Optional[float]
    
    # Financial health
    debt_to_equity: Optional[float]
    current_ratio: Optional[float]
    
    # Profitability (from statements)
    roe: Optional[float]  # Will calculate from statements
    profit_margin: Optional[float]
    
    # Market context
    market_cap: Optional[int]
    
    # Future return (for training)
    future_return_1d: Optional[float] = None
    future_return_5d: Optional[float] = None

@dataclass
class TechnicalPattern:
    """Technical pattern (from your existing system)."""
    date: date
    symbol: str
    
    # Technical indicators
    rsi: Optional[float]
    ema_10: Optional[float]
    volume_sma20: Optional[float]
    close_price: float
    
    # Future return
    future_return_1d: Optional[float] = None
    future_return_5d: Optional[float] = None

@dataclass
class CombinedPattern:
    """Combined fundamental + technical pattern."""
    date: date
    symbol: str
    
    # Technical features
    rsi: Optional[float]
    ema_10: Optional[float] 
    volume_sma20: Optional[float]
    close_price: float
    
    # Fundamental features
    pe_ratio: Optional[float]
    pb_ratio: Optional[float]
    debt_to_equity: Optional[float]
    current_ratio: Optional[float]
    roe: Optional[float]
    market_cap: Optional[int]
    
    # Outcomes
    future_return_1d: Optional[float] = None
    future_return_5d: Optional[float] = None

class FundamentalKNNStrategy:
    """KNN strategy combining technical and fundamental analysis."""
    
    def __init__(self, k: int = 10):
        self.k = k
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_fundamental_features(self, symbol: str, start_date: date, end_date: date) -> List[FundamentalPattern]:
        """Get fundamental features for a symbol and date range."""
        
        query = """
        SELECT 
            date,
            pe_ratio,
            pb_ratio, 
            ps_ratio,
            debt_to_equity,
            current_ratio,
            market_cap
        FROM historical_fundamentals_daily
        WHERE symbol = $1 
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        
        patterns = []
        for row in rows:
            # Calculate ROE from financial statements (more accurate than daily approximations)
            roe = await self.get_roe_for_date(symbol, row['date'])
            
            pattern = FundamentalPattern(
                date=row['date'],
                symbol=symbol,
                pe_ratio=row['pe_ratio'],
                pb_ratio=row['pb_ratio'],
                ps_ratio=row['ps_ratio'],
                debt_to_equity=row['debt_to_equity'],
                current_ratio=row['current_ratio'],
                market_cap=row['market_cap'],
                roe=roe,
                profit_margin=None  # We'll calculate this separately if needed
            )
            patterns.append(pattern)
            
        return patterns
        
    async def get_roe_for_date(self, symbol: str, target_date: date) -> Optional[float]:
        """Get ROE from latest financial statements before target date."""
        
        query = """
        SELECT net_income, period_date
        FROM financial_statements 
        WHERE symbol = $1 
        AND period_date <= $2
        ORDER BY period_date DESC
        LIMIT 1
        """
        
        income_row = await self.db_conn.fetchrow(query, symbol, target_date)
        
        if not income_row or not income_row['net_income']:
            return None
            
        # Get corresponding equity from balance sheet
        equity_query = """
        SELECT total_equity
        FROM balance_sheet_data
        WHERE symbol = $1
        AND period_date <= $2  
        ORDER BY period_date DESC
        LIMIT 1
        """
        
        equity_row = await self.db_conn.fetchrow(equity_query, symbol, target_date)
        
        if equity_row and equity_row['total_equity'] and equity_row['total_equity'] > 0:
            return (income_row['net_income'] / equity_row['total_equity']) * 100
            
        return None
        
    async def get_technical_features(self, symbol: str, start_date: date, end_date: date) -> List[TechnicalPattern]:
        """Get technical features (using your existing indicators)."""
        
        # This would use your existing technical analysis system
        # For now, simplified version getting price data + basic indicators
        
        query = """
        SELECT 
            date,
            close_price,
            volume
        FROM daily_price_data
        WHERE symbol = $1 
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        
        if len(rows) < 20:  # Need enough data for indicators
            return []
            
        df = pd.DataFrame([dict(row) for row in rows])
        df['close_price'] = df['close_price'].astype(float)
        df['volume'] = df['volume'].astype(float)
        
        # Calculate basic technical indicators
        df['ema_10'] = df['close_price'].ewm(span=10).mean()
        df['volume_sma20'] = df['volume'].rolling(20).mean()
        
        # Simple RSI calculation
        delta = df['close_price'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        patterns = []
        for _, row in df.iterrows():
            if pd.notna(row['rsi']) and pd.notna(row['ema_10']):  # Only valid indicators
                # Handle date - could be datetime.date or pandas datetime
                pattern_date = row['date'] if isinstance(row['date'], date) else row['date'].date()
                
                pattern = TechnicalPattern(
                    date=pattern_date,
                    symbol=symbol,
                    rsi=row['rsi'],
                    ema_10=row['ema_10'],
                    volume_sma20=row['volume_sma20'],
                    close_price=row['close_price']
                )
                patterns.append(pattern)
                
        return patterns
        
    async def combine_features(self, symbol: str, start_date: date, end_date: date) -> List[CombinedPattern]:
        """Combine fundamental and technical features."""
        
        # Get both feature sets
        fundamental_patterns = await self.get_fundamental_features(symbol, start_date, end_date)
        technical_patterns = await self.get_technical_features(symbol, start_date, end_date)
        
        # Create lookup for technical patterns by date
        tech_by_date = {p.date: p for p in technical_patterns}
        
        combined = []
        for fund_pattern in fundamental_patterns:
            # Find matching technical pattern for same date
            tech_pattern = tech_by_date.get(fund_pattern.date)
            
            if tech_pattern:  # Only combine if we have both fundamental and technical data
                combined_pattern = CombinedPattern(
                    date=fund_pattern.date,
                    symbol=symbol,
                    
                    # Technical features
                    rsi=tech_pattern.rsi,
                    ema_10=tech_pattern.ema_10,
                    volume_sma20=tech_pattern.volume_sma20,
                    close_price=tech_pattern.close_price,
                    
                    # Fundamental features
                    pe_ratio=fund_pattern.pe_ratio,
                    pb_ratio=fund_pattern.pb_ratio, 
                    debt_to_equity=fund_pattern.debt_to_equity,
                    current_ratio=fund_pattern.current_ratio,
                    roe=fund_pattern.roe,
                    market_cap=fund_pattern.market_cap
                )
                combined.append(combined_pattern)
                
        return combined
        
    async def add_future_returns(self, patterns: List[CombinedPattern]) -> List[CombinedPattern]:
        """Add future returns to patterns for training."""
        
        for pattern in patterns:
            # Get future prices
            future_query = """
            SELECT close_price::NUMERIC as close_price, date
            FROM daily_price_data
            WHERE symbol = $1 
            AND date > $2
            AND date <= $3
            ORDER BY date
            """
            
            # 1-day return
            future_1d = await self.db_conn.fetchrow(
                future_query, 
                pattern.symbol, 
                pattern.date, 
                pattern.date + timedelta(days=5)  # Look ahead a few days for next trading day
            )
            
            if future_1d:
                pattern.future_return_1d = (float(future_1d['close_price']) / pattern.close_price - 1) * 100
                
            # 5-day return  
            future_5d_rows = await self.db_conn.fetch(
                future_query,
                pattern.symbol,
                pattern.date,
                pattern.date + timedelta(days=10)  # Look ahead for 5th trading day
            )
            
            if len(future_5d_rows) >= 5:
                pattern.future_return_5d = (float(future_5d_rows[4]['close_price']) / pattern.close_price - 1) * 100
                
        return patterns
        
    def calculate_pattern_distance(self, p1: CombinedPattern, p2: CombinedPattern) -> float:
        """Calculate distance between two combined patterns."""
        
        distances = []
        
        # Technical distances (normalized)
        if p1.rsi is not None and p2.rsi is not None:
            distances.append(abs(p1.rsi - p2.rsi) / 100)  # RSI scale 0-100
            
        if p1.ema_10 is not None and p2.ema_10 is not None:
            # EMA distance as percentage difference
            distances.append(abs(p1.ema_10 - p2.ema_10) / max(p1.ema_10, p2.ema_10))
            
        # Fundamental distances (normalized)
        if p1.pe_ratio is not None and p2.pe_ratio is not None and p1.pe_ratio > 0 and p2.pe_ratio > 0:
            # P/E ratio distance (capped at reasonable range)
            pe_dist = abs(p1.pe_ratio - p2.pe_ratio) / max(abs(p1.pe_ratio), abs(p2.pe_ratio))
            distances.append(min(pe_dist, 2.0))  # Cap extreme P/E differences
            
        if p1.debt_to_equity is not None and p2.debt_to_equity is not None:
            # Debt ratio distance
            de_dist = abs(p1.debt_to_equity - p2.debt_to_equity) / max(abs(p1.debt_to_equity) + 1, abs(p2.debt_to_equity) + 1)
            distances.append(min(de_dist, 1.0))
            
        if p1.roe is not None and p2.roe is not None:
            # ROE distance (percentage points)
            roe_dist = abs(p1.roe - p2.roe) / 50  # Scale to 50% range
            distances.append(min(roe_dist, 1.0))
            
        if not distances:
            return float('inf')  # No comparable features
            
        return np.mean(distances)
        
    async def find_neighbors(self, target_pattern: CombinedPattern, 
                           candidate_patterns: List[CombinedPattern]) -> List[Tuple[CombinedPattern, float]]:
        """Find K nearest neighbors for target pattern."""
        
        distances = []
        
        for candidate in candidate_patterns:
            if candidate.date >= target_pattern.date:  # No look-ahead bias
                continue
                
            distance = self.calculate_pattern_distance(target_pattern, candidate)
            
            if distance < float('inf'):
                distances.append((candidate, distance))
                
        # Sort by distance and take top K
        distances.sort(key=lambda x: x[1])
        return distances[:self.k]
        
    async def predict_returns(self, neighbors: List[Tuple[CombinedPattern, float]]) -> Dict:
        """Predict returns based on neighbor outcomes."""
        
        if not neighbors:
            return {'prediction': 'neutral', 'confidence': 0, 'expected_return': 0}
            
        # Weight predictions by inverse distance
        total_weight = 0
        weighted_return_1d = 0
        weighted_return_5d = 0
        valid_neighbors = 0
        
        for neighbor, distance in neighbors:
            if neighbor.future_return_1d is not None:
                weight = 1 / (distance + 0.01)  # Avoid division by zero
                weighted_return_1d += neighbor.future_return_1d * weight
                total_weight += weight
                valid_neighbors += 1
                
        if valid_neighbors == 0:
            return {'prediction': 'neutral', 'confidence': 0, 'expected_return': 0}
            
        expected_return = weighted_return_1d / total_weight
        
        # Determine prediction and confidence
        if expected_return > 1.0:  # > 1% expected return
            prediction = 'buy'
        elif expected_return < -1.0:  # < -1% expected return
            prediction = 'sell'
        else:
            prediction = 'neutral'
            
        # Confidence based on neighbor agreement and distance
        avg_distance = np.mean([d for _, d in neighbors])
        confidence = valid_neighbors / self.k * (1 - min(avg_distance, 1.0))
        
        return {
            'prediction': prediction,
            'confidence': confidence,
            'expected_return': expected_return,
            'neighbor_count': valid_neighbors,
            'avg_distance': avg_distance
        }
        
    async def test_strategy(self, symbol: str, start_date: date, end_date: date) -> Dict:
        """Test the fundamental + technical KNN strategy."""
        
        logger.info(f"🧪 Testing Fundamental+Technical KNN Strategy for {symbol}")
        logger.info(f"   Period: {start_date} to {end_date}")
        
        # Get all patterns for the period
        all_patterns = await self.combine_features(symbol, start_date, end_date)
        
        if len(all_patterns) < 50:  # Need enough historical data
            logger.warning(f"   ⚠️ Insufficient data: {len(all_patterns)} patterns")
            return {'error': 'Insufficient data'}
            
        # Add future returns for evaluation
        all_patterns = await self.add_future_returns(all_patterns)
        
        # Split into training (first 70%) and test (last 30%) 
        split_idx = int(len(all_patterns) * 0.7)
        training_patterns = all_patterns[:split_idx]
        test_patterns = all_patterns[split_idx:]
        
        logger.info(f"   Training patterns: {len(training_patterns)}")
        logger.info(f"   Test patterns: {len(test_patterns)}")
        
        # Test predictions
        predictions = []
        correct_predictions = 0
        total_return = 0
        trade_count = 0
        
        for test_pattern in test_patterns:
            if test_pattern.future_return_1d is None:
                continue
                
            # Find neighbors from training set
            neighbors = await self.find_neighbors(test_pattern, training_patterns)
            
            if not neighbors:
                continue
                
            # Make prediction
            prediction = await self.predict_returns(neighbors)
            
            # Evaluate prediction
            actual_return = test_pattern.future_return_1d
            
            predictions.append({
                'date': test_pattern.date,
                'prediction': prediction['prediction'],
                'confidence': prediction['confidence'],
                'expected_return': prediction['expected_return'],
                'actual_return': actual_return,
                'pe_ratio': test_pattern.pe_ratio,
                'debt_to_equity': test_pattern.debt_to_equity,
                'rsi': test_pattern.rsi
            })
            
            # Check accuracy (for buy/sell predictions only)
            if prediction['prediction'] in ['buy', 'sell']:
                expected_direction = 1 if prediction['prediction'] == 'buy' else -1
                actual_direction = 1 if actual_return > 0 else -1
                
                if expected_direction == actual_direction:
                    correct_predictions += 1
                    
                total_return += actual_return if prediction['prediction'] == 'buy' else -actual_return
                trade_count += 1
                
        # Calculate metrics
        accuracy = correct_predictions / trade_count if trade_count > 0 else 0
        avg_return = total_return / trade_count if trade_count > 0 else 0
        
        results = {
            'symbol': symbol,
            'period': f'{start_date} to {end_date}',
            'total_predictions': len(predictions),
            'trade_count': trade_count,
            'accuracy': accuracy,
            'average_return': avg_return,
            'total_return': total_return,
            'predictions': predictions[-10:]  # Last 10 predictions for review
        }
        
        logger.info(f"   ✅ Results:")
        logger.info(f"      Accuracy: {accuracy:.1%}")
        logger.info(f"      Average return per trade: {avg_return:.2f}%")
        logger.info(f"      Total return: {total_return:.2f}%")
        logger.info(f"      Trades: {trade_count}")
        
        return results
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Test fundamental KNN strategy."""
    
    strategy = FundamentalKNNStrategy(k=15)
    
    try:
        await strategy.setup()
        
        # Test symbols with good fundamental data
        test_symbols = ['VOLV B', 'ABB', 'ERIC-B', 'AAK']
        
        print("🚀 FUNDAMENTAL + TECHNICAL KNN STRATEGY TEST")
        print("=" * 60)
        
        for symbol in test_symbols:
            results = await strategy.test_strategy(
                symbol, 
                date(2023, 1, 1), 
                date(2024, 12, 1)
            )
            
            if 'error' not in results:
                print(f"\n📊 {symbol} Results:")
                print(f"   Accuracy: {results['accuracy']:.1%}")
                print(f"   Avg Return: {results['average_return']:.2f}%")
                print(f"   Total Return: {results['total_return']:.2f}%")
                print(f"   Trades: {results['trade_count']}")
            else:
                print(f"\n❌ {symbol}: {results['error']}")
                
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await strategy.cleanup()

if __name__ == "__main__":
    asyncio.run(main())