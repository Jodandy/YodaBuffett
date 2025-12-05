"""
KNN Strategy using pre-computed neighbors.
Makes predictions based on historical similar patterns.
"""

import pandas as pd
import numpy as np
from datetime import date
from typing import Dict, Optional, List
import json
import asyncpg

from ..indicators.base import IndicatorResult
from .base import MLStrategy, Signal, SignalType


class KNNStrategy(MLStrategy):
    """
    KNN strategy that uses pre-computed neighbors for prediction.
    
    - Looks up pre-computed neighbors from database
    - Averages their outcomes (weighted by distance)
    - Generates trading signals based on predictions
    """
    
    def __init__(
        self,
        model_name: str = "rsi_knn_model",
        prediction_horizon: str = "5d",  # Which horizon to use for signals
        buy_threshold: float = 0.02,     # Expected return > 2% = BUY
        sell_threshold: float = -0.02,   # Expected return < -2% = SELL
        min_confidence: float = 0.6,
        use_distance_weighting: bool = True
    ):
        config = {
            "model_name": model_name,
            "prediction_horizon": prediction_horizon,
            "buy_threshold": buy_threshold,
            "sell_threshold": sell_threshold,
            "min_confidence": min_confidence,
            "use_distance_weighting": use_distance_weighting
        }
        
        # KNN doesn't need specific indicators since it uses pre-computed neighbors
        super().__init__(
            name="knn_rsi_strategy",
            description=f"KNN strategy using {model_name} for {prediction_horizon} predictions",
            required_indicators=[],  # No indicators needed - uses pre-computed
            ml_model_name=model_name,
            config=config
        )
        
        self.prediction_horizon = prediction_horizon
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.min_confidence = min_confidence
        self.use_distance_weighting = use_distance_weighting
        
        # Database connection will be set during backtesting
        self.conn = None
        self.model_id = None
    
    async def set_connection(self, conn: asyncpg.Connection):
        """Set database connection for neighbor lookups."""
        self.conn = conn
        # Get model ID
        row = await conn.fetchrow(
            "SELECT id FROM ml_models WHERE name = $1", 
            self.ml_model_name
        )
        if row:
            self.model_id = row['id']
    
    async def load_ml_model(self):
        """KNN doesn't need to load a model - it uses pre-computed neighbors."""
        pass
    
    async def prepare_features(self, indicator_values: Dict[str, IndicatorResult], current_date: date) -> pd.DataFrame:
        """KNN doesn't prepare features - it looks up pre-computed neighbors."""
        pass
    
    async def get_neighbors_for_date(self, company_id: int, prediction_date: date) -> Optional[Dict]:
        """Retrieve pre-computed neighbors for a specific date."""
        if not self.conn or not self.model_id:
            return None
            
        query = """
        SELECT neighbors, feature_vector, num_neighbors_available
        FROM knn_neighbors
        WHERE model_id = $1 AND company_id = $2 AND prediction_date = $3
        """
        
        row = await self.conn.fetchrow(query, self.model_id, company_id, prediction_date)
        if not row:
            return None
            
        return {
            'neighbors': json.loads(row['neighbors']) if isinstance(row['neighbors'], str) else row['neighbors'],
            'feature_vector': json.loads(row['feature_vector']) if isinstance(row['feature_vector'], str) else row['feature_vector'],
            'num_neighbors_available': row['num_neighbors_available']
        }
    
    def predict_from_neighbors(self, neighbors: List[Dict]) -> Dict[str, float]:
        """
        Make prediction by averaging neighbor outcomes.
        
        Returns:
            Dictionary with predicted returns and confidence
        """
        if not neighbors:
            return {}
            
        predictions = {}
        
        # Extract returns for each horizon
        horizons = ['1d_return', '5d_return', '10d_return']
        
        for horizon in horizons:
            returns = []
            weights = []
            
            for neighbor in neighbors:
                label = neighbor.get('label', {})
                if horizon in label:
                    returns.append(label[horizon])
                    
                    if self.use_distance_weighting:
                        # Weight by inverse distance (closer = more weight)
                        distance = neighbor.get('distance', 1.0)
                        weight = 1.0 / (distance + 0.01)  # Add small epsilon to avoid division by zero
                        weights.append(weight)
                    else:
                        weights.append(1.0)
            
            if returns:
                # Weighted average
                if self.use_distance_weighting:
                    weighted_return = np.average(returns, weights=weights)
                else:
                    weighted_return = np.mean(returns)
                    
                predictions[horizon] = weighted_return
                
                # Calculate confidence based on agreement among neighbors
                std_dev = np.std(returns)
                # Higher std = lower confidence
                confidence = 1.0 / (1.0 + std_dev * 10)  # Scale factor of 10
                predictions[f"{horizon}_confidence"] = confidence
        
        return predictions
    
    async def generate_signal(
        self,
        company_id: int,
        market_data: pd.DataFrame,
        current_date: date,
        indicator_values: Dict[str, IndicatorResult]
    ) -> Optional[Signal]:
        """Generate KNN-based trading signal."""
        
        # Get pre-computed neighbors
        neighbor_data = await self.get_neighbors_for_date(company_id, current_date)
        if not neighbor_data:
            return None
            
        neighbors = neighbor_data['neighbors']
        if not neighbors:
            return None
            
        # Make predictions from neighbors
        predictions = self.predict_from_neighbors(neighbors)
        
        # Get prediction for specified horizon
        horizon_key = f"{self.prediction_horizon}_return"
        confidence_key = f"{self.prediction_horizon}_return_confidence"
        
        if horizon_key not in predictions:
            return None
            
        predicted_return = predictions[horizon_key]
        confidence = predictions.get(confidence_key, 0.5)
        
        # Check minimum confidence
        if confidence < self.min_confidence:
            return None
            
        # Determine signal type based on predicted return
        if predicted_return > self.buy_threshold:
            signal_type = SignalType.STRONG_BUY if predicted_return > self.buy_threshold * 2 else SignalType.BUY
            strength = min(predicted_return / self.buy_threshold, 2.0)
        elif predicted_return < self.sell_threshold:
            signal_type = SignalType.STRONG_SELL if predicted_return < self.sell_threshold * 2 else SignalType.SELL
            strength = min(abs(predicted_return / self.sell_threshold), 2.0)
        else:
            signal_type = SignalType.HOLD
            strength = 0.1
        
        # Build contributing factors
        contributing_factors = {
            "predicted_return": predicted_return,
            "num_neighbors": len(neighbors),
            "avg_neighbor_distance": np.mean([n['distance'] for n in neighbors]),
            "feature_vector": neighbor_data['feature_vector']
        }
        
        # Add individual neighbor info
        for i, neighbor in enumerate(neighbors[:3]):  # Top 3 neighbors
            contributing_factors[f"neighbor_{i}_date"] = neighbor['date']
            contributing_factors[f"neighbor_{i}_return"] = neighbor['label'].get(horizon_key, 'N/A')
            contributing_factors[f"neighbor_{i}_distance"] = neighbor['distance']
        
        return Signal(
            signal_type=signal_type,
            confidence=confidence,
            strength=strength,
            company_id=company_id,
            date=current_date,
            contributing_factors=contributing_factors,
            metadata={
                "strategy": self.name,
                "ml_model": self.ml_model_name,
                "prediction_horizon": self.prediction_horizon,
                "predictions": predictions
            }
        )