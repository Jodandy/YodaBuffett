#!/usr/bin/env python3
"""
Diagnose why the document anomaly strategy isn't generating signals.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
import hashlib

from services.technical_analysis.strategies.document_anomaly_strategy import DocumentAnomalyStrategy
from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

async def diagnose_strategy():
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    print("🔍 Diagnosing Document Anomaly Strategy\n")
    
    # Setup
    indicator_registry.register(RSI(period=14))
    indicator_registry.register(SMA(period=20))
    indicator_registry.register(VolumeMA(period=20))
    indicator_engine = IndicatorEngine(indicator_registry)
    
    # Create very sensitive strategy
    strategy = DocumentAnomalyStrategy(
        anomaly_lookback_days=30,
        min_anomaly_confidence=0.3,      # Very low
        anomaly_threshold=0.6,           # High = easier
        require_technical_confirmation=False,
        rsi_oversold=45,                 # Relaxed
        rsi_overbought=55,
        volume_surge_threshold=1.2
    )
    
    await strategy.setup_db_connection()
    
    # Test with known good symbol
    test_symbol = 'ERIC-B'
    test_date = date(2024, 8, 15)
    company_id = int(hashlib.md5(test_symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    print(f"Testing {test_symbol} on {test_date}")
    print("=" * 50)
    
    # Get market data
    market_data_query = """
        SELECT date, 
               open_price as open, 
               high_price as high, 
               low_price as low, 
               close_price as close, 
               volume
        FROM daily_price_data 
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date
    """
    
    rows = await conn.fetch(
        market_data_query, 
        test_symbol, 
        test_date - timedelta(days=100), 
        test_date
    )
    
    df = pd.DataFrame([dict(row) for row in rows])
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate indicators
    indicator_values = await indicator_engine.calculate_multiple(
        ["rsi_14", "sma_20", "volume_sma_20"],
        company_id,
        df,
        test_date - timedelta(days=60),
        test_date
    )
    
    print("\n📊 Indicator Values:")
    for key, value in indicator_values.items():
        print(f"   {key}: {value}")
    
    # Check document anomaly detection directly
    print("\n📄 Checking Document Anomaly Detection:")
    
    # First, check if analyze_document_anomalies exists
    doc_anomaly = await strategy.analyze_document_anomalies(test_symbol, test_date)
    
    if doc_anomaly:
        print(f"   ✅ Document anomaly detected: {doc_anomaly}")
    else:
        print(f"   ❌ No document anomaly detected")
        
        # Debug: Check what documents exist
        print("\n   Debugging document search:")
        
        # Check for ANY documents for Ericsson
        ericsson_docs = await conn.fetch("""
            SELECT DISTINCT ed.company_name, COUNT(*) as count
            FROM extracted_documents ed
            WHERE ed.company_name ILIKE '%eric%'
            GROUP BY ed.company_name
            ORDER BY count DESC
            LIMIT 10
        """)
        
        print(f"   Found {len(ericsson_docs)} company variations:")
        for doc in ericsson_docs[:5]:
            print(f"     {doc['company_name']}: {doc['count']} docs")
    
    # Try to generate signal
    print("\n🚦 Attempting Signal Generation:")
    signal = await strategy.generate_signal(
        company_id, df, test_date, indicator_values
    )
    
    if signal:
        print(f"   ✅ Signal generated: {signal.signal_type.value}")
        print(f"      Confidence: {signal.confidence}")
        print(f"      Source: {signal.contributing_factors.get('signal_source', 'unknown')}")
    else:
        print("   ❌ No signal generated")
        
        # Check technical conditions
        print("\n   Checking technical conditions:")
        
        # Get actual RSI value
        if 'rsi_14' in indicator_values:
            rsi_result = indicator_values['rsi_14']
            # Try to extract value from IndicatorResult
            if hasattr(rsi_result, 'value'):
                rsi_value = rsi_result.value
            elif hasattr(rsi_result, 'values') and rsi_result.values:
                rsi_value = list(rsi_result.values.values())[0]
            else:
                rsi_value = None
            
            if rsi_value:
                print(f"   RSI: {rsi_value:.1f} (buy <{strategy.rsi_oversold}, sell >{strategy.rsi_overbought})")
                if rsi_value < strategy.rsi_oversold:
                    print("   → RSI suggests BUY")
                elif rsi_value > strategy.rsi_overbought:
                    print("   → RSI suggests SELL")
                else:
                    print("   → RSI is neutral")
        
        # Check volume
        if 'volume_sma_20' in indicator_values:
            current_volume = df['volume'].iloc[-1]
            vol_result = indicator_values['volume_sma_20']
            
            if hasattr(vol_result, 'value'):
                avg_volume = vol_result.value
            elif hasattr(vol_result, 'values') and vol_result.values:
                avg_volume = list(vol_result.values.values())[0]
            else:
                avg_volume = None
                
            if avg_volume:
                volume_ratio = current_volume / avg_volume
                print(f"   Volume ratio: {volume_ratio:.2f} (surge >{strategy.volume_surge_threshold})")
    
    # Test pure technical signal generation
    print("\n🔧 Testing Pure Technical Signals:")
    
    # Find dates with extreme RSI
    for i in range(len(df) - 20, len(df)):
        test_date = df.index[i].date()
        
        # Calculate RSI manually
        prices = df['close'].iloc[i-14:i+1].values
        if len(prices) >= 14:
            deltas = np.diff(prices)
            gains = deltas[deltas > 0]
            losses = -deltas[deltas < 0]
            
            if len(gains) > 0 and len(losses) > 0:
                avg_gain = np.mean(gains)
                avg_loss = np.mean(losses)
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                
                if rsi < 35 or rsi > 65:
                    print(f"   {test_date}: RSI={rsi:.1f} - should generate signal")
    
    await strategy.cleanup()
    await conn.close()
    
    print("\n\n💡 CONCLUSIONS:")
    print("The strategy may not be generating signals because:")
    print("1. Document anomaly detection requires specific company name matching")
    print("2. Technical thresholds might not be hit during the test period")
    print("3. The strategy implementation might be too restrictive")

if __name__ == "__main__":
    asyncio.run(diagnose_strategy())