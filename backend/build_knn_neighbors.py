"""
Build pre-computed KNN neighbors table.
For each company/date, find K most similar historical patterns.
Respects time boundaries - no look-ahead bias.
"""

import asyncio
import asyncpg
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import json
from typing import Dict, List, Tuple, Optional
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import time

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA


class KNNNeighborBuilder:
    """Build time-aware KNN neighbors for technical analysis."""
    
    def __init__(self, k: int = 5, distance_metric: str = 'euclidean'):
        self.k = k
        self.distance_metric = distance_metric
        self.engine = None
        self.scaler = StandardScaler()
        
    async def setup_indicators(self):
        """Register required indicators."""
        indicator_registry.register(RSI(period=14))
        indicator_registry.register(SMA(period=20))
        indicator_registry.register(VolumeMA(period=20))
        
        self.engine = IndicatorEngine(indicator_registry)
        
    async def get_market_data_for_symbol(self, conn, symbol: str) -> pd.DataFrame:
        """Get all available market data for a symbol."""
        query = """
        SELECT date, 
               open_price as open, 
               high_price as high, 
               low_price as low, 
               close_price as close, 
               volume
        FROM daily_price_data 
        WHERE symbol = $1
        ORDER BY date
        """
        
        rows = await conn.fetch(query, symbol)
        if not rows:
            return pd.DataFrame()
            
        # Convert to DataFrame
        data = [dict(row) for row in rows]
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # Ensure numeric columns
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        return df
    
    async def calculate_features(self, market_data: pd.DataFrame, target_date: date) -> Optional[Dict[str, float]]:
        """Calculate feature vector for a specific date."""
        try:
            # Convert date to pandas timestamp for indexing
            target_timestamp = pd.Timestamp(target_date)
            
            # Need enough history for indicators
            if len(market_data[:target_timestamp]) < 30:
                return None
                
            # Calculate indicators up to target date
            start_date = (target_date - timedelta(days=60))
            
            # RSI
            rsi_result = await self.engine.calculate_indicator(
                "rsi_14", 0, market_data, start_date, target_date
            )
            rsi_value = rsi_result.get_value(target_date)
            if rsi_value is None:
                return None
                
            # SMA
            sma_result = await self.engine.calculate_indicator(
                "sma_20", 0, market_data, start_date, target_date
            )
            sma_value = sma_result.get_value(target_date)
            
            # Volume MA
            vol_result = await self.engine.calculate_indicator(
                "volume_sma_20", 0, market_data, start_date, target_date
            )
            vol_ma_value = vol_result.get_value(target_date)
            
            # Price-based features
            target_data = market_data.loc[market_data.index.date == target_date]
            if target_data.empty:
                return None
                
            current_price = float(target_data['close'].iloc[-1])
            current_volume = float(target_data['volume'].iloc[-1])
            
            # Calculate 5-day price change
            price_5d_ago = market_data[:target_timestamp].iloc[-6:-5]['close']
            price_change_5d = ((current_price - float(price_5d_ago.iloc[0])) / float(price_5d_ago.iloc[0]) * 100) if len(price_5d_ago) > 0 else 0.0
            
            # Additional features
            features = {
                'rsi_14': float(rsi_value),
                'price_to_sma': float(current_price / sma_value) if sma_value else 1.0,
                'volume_ratio': float(current_volume / vol_ma_value) if vol_ma_value else 1.0,
                'price_change_5d': price_change_5d
            }
            
            return features
            
        except Exception as e:
            print(f"Feature calculation error: {e}")
            return None
    
    async def get_label_for_date(self, conn, company_id: int, target_date: date) -> Optional[Dict]:
        """Get the label (future outcome) for a specific date."""
        query = """
        SELECT labels 
        FROM ml_labels 
        WHERE company_id = $1 AND date = $2 AND label_type = 'price_returns'
        LIMIT 1
        """
        
        row = await conn.fetchrow(query, company_id, target_date)
        if row:
            if isinstance(row['labels'], str):
                return json.loads(row['labels'])
            return row['labels']
        return None
    
    async def build_neighbors_for_symbol(self, conn, symbol: str) -> int:
        """Build KNN neighbors for all dates of a symbol."""
        print(f"\n  Processing symbol: {symbol}")
        
        # Get market data
        market_data = await self.get_market_data_for_symbol(conn, symbol)
        if market_data.empty or len(market_data) < 50:
            print(f"    ❌ Insufficient data")
            return 0
            
        # Use symbol hash as company_id (matching create_rsi_labels.py)
        # Use a deterministic hash that's consistent across runs
        import hashlib
        company_id = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
        
        # Get dates that have labels
        label_query = """
        SELECT DISTINCT date 
        FROM ml_labels 
        WHERE company_id = $1 AND label_type = 'price_returns'
        ORDER BY date
        """
        label_rows = await conn.fetch(label_query, company_id)
        dates_with_labels = [row['date'] for row in label_rows]
        
        if not dates_with_labels:
            print(f"    ❌ No labels found for company_id {company_id}")
            return 0
            
        print(f"    Found {len(dates_with_labels)} dates with labels")
        
        # Calculate features for all historical dates
        print(f"    Calculating features...")
        historical_features = []
        historical_dates = []
        historical_labels = []
        
        # Use only dates with labels
        for hist_date in dates_with_labels:
            features = await self.calculate_features(market_data, hist_date)
            if features:
                label = await self.get_label_for_date(conn, company_id, hist_date)
                if label:
                    historical_features.append(list(features.values()))
                    historical_dates.append(hist_date)
                    historical_labels.append(label)
        
        if len(historical_features) < self.k + 1:
            print(f"    ❌ Not enough historical data with labels")
            return 0
            
        print(f"    Found {len(historical_features)} historical points with features and labels")
        
        # Normalize features
        feature_matrix = np.array(historical_features)
        self.scaler.fit(feature_matrix)
        normalized_features = self.scaler.transform(feature_matrix)
        
        # Process each date to find its neighbors
        neighbors_to_insert = []
        
        for i, prediction_date in enumerate(historical_dates[self.k:], start=self.k):
            # Only use data before this date
            historical_mask = [j for j in range(len(historical_dates)) if historical_dates[j] < prediction_date]
            
            if len(historical_mask) < self.k:
                continue
                
            # Features available up to this date
            historical_X = normalized_features[historical_mask]
            current_X = normalized_features[i].reshape(1, -1)
            
            # Find K nearest neighbors
            nn = NearestNeighbors(n_neighbors=min(self.k, len(historical_mask)), metric=self.distance_metric)
            nn.fit(historical_X)
            distances, indices = nn.kneighbors(current_X)
            
            # Build neighbor list
            neighbors = []
            for dist, idx in zip(distances[0], indices[0]):
                actual_idx = historical_mask[idx]
                neighbor_data = {
                    "date": str(historical_dates[actual_idx]),
                    "distance": float(dist),
                    "features": {k: v for k, v in zip(features.keys(), historical_features[actual_idx])},
                    "label": historical_labels[actual_idx]
                }
                neighbors.append(neighbor_data)
            
            # Prepare for insertion
            neighbors_to_insert.append({
                'company_id': company_id,
                'prediction_date': prediction_date,
                'neighbors': neighbors,
                'feature_vector': list(features.values()),
                'num_neighbors_available': len(historical_mask)
            })
        
        # Insert into database
        if not neighbors_to_insert:
            print(f"    ❌ No neighbors to insert")
            return 0
            
        print(f"    Inserting {len(neighbors_to_insert)} neighbor sets...")
        
        # Get ML model ID (create one if needed)
        model_id = await self.get_or_create_ml_model(conn, symbol)
        
        insert_query = """
        INSERT INTO knn_neighbors 
        (model_id, company_id, prediction_date, neighbors, feature_vector, num_neighbors_available, calculation_time_ms)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (model_id, company_id, prediction_date) DO UPDATE
        SET neighbors = EXCLUDED.neighbors,
            feature_vector = EXCLUDED.feature_vector,
            num_neighbors_available = EXCLUDED.num_neighbors_available,
            calculation_time_ms = EXCLUDED.calculation_time_ms
        """
        
        inserted = 0
        for neighbor_data in neighbors_to_insert:
            start_time = time.time()
            try:
                await conn.execute(
                    insert_query,
                    model_id,
                    neighbor_data['company_id'],
                    neighbor_data['prediction_date'],
                    json.dumps(neighbor_data['neighbors']),
                    json.dumps(neighbor_data['feature_vector']),
                    neighbor_data['num_neighbors_available'],
                    int((time.time() - start_time) * 1000)
                )
                inserted += 1
            except Exception as e:
                print(f"      Insert error: {e}")
                
        print(f"    ✅ Inserted {inserted} neighbor sets")
        return inserted
    
    async def get_or_create_ml_model(self, conn, symbol: str) -> int:
        """Get or create ML model for RSI KNN."""
        # Check if model exists
        check_query = """
        SELECT id FROM ml_models 
        WHERE name = 'rsi_knn_model'
        LIMIT 1
        """
        
        row = await conn.fetchrow(check_query)
        if row:
            return row['id']
            
        # Create new model
        insert_query = """
        INSERT INTO ml_models 
        (name, model_type, description, parameters, indicator_config, output_labels)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
        """
        
        model_id = await conn.fetchval(
            insert_query,
            'rsi_knn_model',
            'knn',
            'KNN model for RSI-based price prediction',
            json.dumps({'k': self.k, 'distance': self.distance_metric}),
            json.dumps({
                'rsi': {'period': 14},
                'sma': {'period': 20},
                'volume_sma': {'period': 20}
            }),
            ['1d_return', '5d_return', '10d_return']
        )
        
        return model_id


async def main():
    """Build KNN neighbors for all symbols with labels."""
    print("🚀 Building Pre-computed KNN Neighbors")
    print("=" * 50)
    
    # Connect to database
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    try:
        # Initialize builder
        builder = KNNNeighborBuilder(k=5, distance_metric='euclidean')
        await builder.setup_indicators()
        
        # Get symbols that have labels
        print("\n1. Finding symbols with ML labels...")
        query = """
        SELECT DISTINCT metadata->>'symbol' as symbol
        FROM ml_labels
        WHERE label_type = 'price_returns'
        AND metadata->>'symbol' IS NOT NULL
        """
        
        rows = await conn.fetch(query)
        symbols = [row['symbol'] for row in rows]
        print(f"   Found {len(symbols)} symbols with labels: {symbols}")
        
        # Build neighbors for each symbol
        print(f"\n2. Building KNN neighbors (k={builder.k})...")
        total_neighbors = 0
        
        for symbol in symbols:
            neighbors_count = await builder.build_neighbors_for_symbol(conn, symbol)
            total_neighbors += neighbors_count
        
        print(f"\n🎯 Complete! Built {total_neighbors} total neighbor sets")
        
        # Show sample results
        print(f"\n📊 Sample KNN Neighbors:")
        sample_query = """
        SELECT 
            kn.company_id,
            kn.prediction_date,
            kn.num_neighbors_available,
            jsonb_array_length(kn.neighbors) as k_neighbors,
            kn.feature_vector,
            kn.calculation_time_ms
        FROM knn_neighbors kn
        ORDER BY kn.created_at DESC
        LIMIT 5
        """
        
        samples = await conn.fetch(sample_query)
        for sample in samples:
            print(f"   Company {sample['company_id']} on {sample['prediction_date']}: "
                  f"{sample['k_neighbors']} neighbors from {sample['num_neighbors_available']} available")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())