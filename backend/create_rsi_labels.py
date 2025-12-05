"""
Create ML labels for RSI14 classification.
Calculate future price movements as labels for training ML models.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from typing import Dict, Any, Optional
import json

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI


async def get_companies_with_market_data(conn) -> list:
    """Get list of companies that have sufficient market data."""
    query = """
    SELECT symbol, COUNT(*) as days
    FROM daily_price_data 
    WHERE date >= CURRENT_DATE - INTERVAL '200 days'
    GROUP BY symbol
    HAVING COUNT(*) >= 100  -- At least 100 days of data
    ORDER BY days DESC
    LIMIT 10  -- Start with 10 companies for testing
    """
    
    rows = await conn.fetch(query)
    return [row['symbol'] for row in rows]


async def get_market_data_for_symbol(conn, symbol: str, days: int = 150) -> pd.DataFrame:
    """Get market data for a specific symbol."""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    query = """
    SELECT date, 
           open_price as open, 
           high_price as high, 
           low_price as low, 
           close_price as close, 
           volume
    FROM daily_price_data 
    WHERE symbol = $1 AND date >= $2 AND date <= $3
    ORDER BY date
    """
    
    rows = await conn.fetch(query, symbol, start_date, end_date)
    
    if not rows:
        return pd.DataFrame()
    
    # Convert asyncpg Records to dict list
    data = [dict(row) for row in rows]
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Ensure numeric columns are float
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def calculate_future_returns(prices: pd.Series, horizons: list = [1, 5, 10]) -> Dict[str, pd.Series]:
    """Calculate future returns for different horizons."""
    returns = {}
    
    for horizon in horizons:
        # Calculate future returns: (price[t+horizon] - price[t]) / price[t]
        future_prices = prices.shift(-horizon)
        returns[f"{horizon}d_return"] = (future_prices - prices) / prices
        
        # Also create direction labels
        returns[f"{horizon}d_direction"] = returns[f"{horizon}d_return"].apply(
            lambda x: "up" if x > 0.02 else ("down" if x < -0.02 else "neutral") if not pd.isna(x) else None
        )
    
    return returns


async def create_labels_for_symbol(conn, symbol: str, engine: IndicatorEngine) -> int:
    """Create RSI14 labels for a single symbol."""
    
    print(f"  Processing symbol {symbol}...")
    
    # 1. Get market data
    market_data = await get_market_data_for_symbol(conn, symbol)
    if market_data.empty or len(market_data) < 50:
        print(f"    ❌ Insufficient data ({len(market_data)} days)")
        return 0
    
    # 2. Calculate RSI14 indicators
    start_date = market_data.index[20].date()  # Start after warmup period
    end_date = market_data.index[-15].date()   # End before latest (need future data for labels)
    
    print(f"    Market data shape: {market_data.shape}")
    print(f"    Date range: {start_date} to {end_date}")
    
    try:
        # Use dummy company_id for now (RSI doesn't actually need it)
        rsi_result = await engine.calculate_indicator(
            "rsi_14", 0, market_data, start_date, end_date
        )
        print(f"    RSI values calculated: {len(rsi_result.values)}")
    except Exception as e:
        print(f"    ❌ RSI calculation failed: {e}")
        return 0
    
    # 3. Calculate future returns for labels
    returns_dict = calculate_future_returns(market_data['close'], horizons=[1, 5, 10])
    
    # 4. Prepare labels for insertion
    labels_to_insert = []
    
    for date, rsi_value in rsi_result.values.items():
        if pd.isna(rsi_value):
            continue
            
        # Get future returns for this date
        labels_json = {}
        
        for horizon_key, returns_series in returns_dict.items():
            # Find the return value for this date
            try:
                date_index = pd.Timestamp(date)
                if date_index in returns_series.index:
                    return_value = returns_series[date_index]
                    if not pd.isna(return_value):
                        if "return" in horizon_key:
                            labels_json[horizon_key] = float(return_value)
                        else:  # direction
                            labels_json[horizon_key] = return_value
            except Exception:
                continue
        
        if labels_json:  # Only add if we have some labels
            labels_to_insert.append({
                'symbol': symbol,
                'date': date,
                'labels': labels_json,
                'rsi_value': float(rsi_value)  # Store for verification
            })
    
    # 5. Insert into database
    if not labels_to_insert:
        print(f"    ❌ No valid labels generated")
        return 0
    
    # For now, let's use a simple numeric ID based on symbol hash
    # This is a workaround until we fix the company_id type mismatch
    # Use a deterministic hash that's consistent across runs
    import hashlib
    company_id = int(hashlib.md5(symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    # Insert price return labels
    insert_query = """
    INSERT INTO ml_labels (company_id, date, timeframe, labels, label_type, prediction_horizons, source, metadata)
    VALUES ($1, $2, 'daily', $3, 'price_returns', $4, 'calculated', $5)
    ON CONFLICT (company_id, date, timeframe, label_type) DO NOTHING
    """
    
    inserted = 0
    for label_data in labels_to_insert:
        try:
            await conn.execute(
                insert_query,
                company_id,
                label_data['date'],
                json.dumps(label_data['labels']),  # Convert dict to JSON string
                [1, 5, 10],  # prediction horizons
                json.dumps({'rsi_value': label_data['rsi_value'], 'indicator': 'rsi_14', 'symbol': symbol})  # Convert dict to JSON string
            )
            inserted += 1
        except Exception as e:
            # Skip duplicates or other errors
            print(f"      Insert error: {e}")
            continue
    
    print(f"    ✅ Inserted {inserted} labels")
    return inserted


async def create_rsi_labels():
    """Main function to create RSI14 labels."""
    
    print("🚀 Creating RSI14 ML Labels")
    print("=" * 40)
    
    # Connect to database
    database_url = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(database_url)
    
    try:
        # 1. Set up indicator engine
        indicator_registry.register(RSI(period=14))
        engine = IndicatorEngine(indicator_registry)
        
        # 2. Get symbols with sufficient data
        print("\n1. Finding symbols with sufficient market data...")
        symbols = await get_companies_with_market_data(conn)
        print(f"   Found {len(symbols)} symbols: {symbols}")
        
        if not symbols:
            print("❌ No symbols with sufficient market data found")
            return
        
        # 3. Create labels for each symbol
        print(f"\n2. Creating labels for {len(symbols)} symbols...")
        total_labels = 0
        
        for symbol in symbols:
            labels_count = await create_labels_for_symbol(conn, symbol, engine)
            total_labels += labels_count
        
        print(f"\n🎯 Complete! Created {total_labels} total labels")
        
        # 4. Show sample results
        sample_query = """
        SELECT company_id, date, labels, metadata->>'rsi_value' as rsi_value
        FROM ml_labels 
        WHERE label_type = 'price_returns'
        ORDER BY date DESC 
        LIMIT 5
        """
        
        samples = await conn.fetch(sample_query)
        
        print(f"\n📊 Sample Labels:")
        for sample in samples:
            rsi_val = sample['rsi_value']
            # Parse the JSON string
            if isinstance(sample['labels'], str):
                labels = json.loads(sample['labels'])
            else:
                labels = sample['labels']
            print(f"   Company {sample['company_id']} on {sample['date']}: RSI={rsi_val}, Labels={labels}")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(create_rsi_labels())