#!/usr/bin/env python3
"""
Diagnose why no signals are being generated.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
import hashlib

from services.technical_analysis.indicators.base import indicator_registry, IndicatorEngine
from services.technical_analysis.indicators.technical import RSI, SMA, VolumeMA

async def diagnose_signals():
    print("🔍 Diagnosing Signal Generation Issues\n")
    
    DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Test symbol
    test_symbol = 'ERIC-B'
    test_date = date(2024, 8, 1)
    
    print(f"Testing {test_symbol} on {test_date}")
    print("=" * 50)
    
    # 1. Check market data
    print("\n1️⃣ Market Data Check:")
    market_data = await conn.fetch("""
        SELECT date, close_price, volume
        FROM daily_price_data
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        ORDER BY date DESC
        LIMIT 20
    """, test_symbol, test_date - timedelta(days=30), test_date)
    
    if market_data:
        print(f"   ✅ Found {len(market_data)} days of market data")
        latest = market_data[0]
        print(f"   Latest: {latest['date']}, close=${float(latest['close_price']):.2f}, vol={latest['volume']:,}")
    else:
        print("   ❌ No market data found")
    
    # 2. Check document data
    print("\n2️⃣ Document Data Check:")
    
    # Check if we have ANY documents with embeddings
    doc_count = await conn.fetchval("""
        SELECT COUNT(DISTINCT ed.id)
        FROM extracted_documents ed
        JOIN document_sections ds ON ed.id = ds.extracted_document_id
        JOIN section_embeddings se ON ds.id = se.document_section_id
        WHERE se.embedding_model LIKE 'local/%'
        AND ed.filing_date <= $1
    """, test_date)
    
    print(f"   Total documents with embeddings: {doc_count}")
    
    # Check documents for companies like ERIC
    company_patterns = ['%Eric%', '%ERIC%', '%Telefonaktiebolaget%']
    for pattern in company_patterns:
        docs = await conn.fetch("""
            SELECT DISTINCT ed.id, ed.company_name, ed.filing_date, ed.form_type
            FROM extracted_documents ed
            JOIN document_sections ds ON ed.id = ds.extracted_document_id
            JOIN section_embeddings se ON ds.id = se.document_section_id
            WHERE ed.company_name ILIKE $1
            AND se.embedding_model LIKE 'local/%'
            ORDER BY ed.filing_date DESC
            LIMIT 5
        """, pattern)
        
        if docs:
            print(f"\n   Documents matching '{pattern}':")
            for doc in docs:
                print(f"     {doc['company_name']}: {doc['filing_date']} ({doc['form_type']})")
    
    # 3. Check technical indicators
    print("\n3️⃣ Technical Indicators Check:")
    
    if market_data and len(market_data) >= 14:
        # Convert to dataframe
        df_data = [{'date': row['date'], 'close': float(row['close_price']), 'volume': row['volume']} 
                   for row in reversed(market_data)]
        df = pd.DataFrame(df_data)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # Calculate RSI manually
        prices = df['close'].values
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        if len(gains) >= 14:
            avg_gain = sum(gains[-14:]) / 14
            avg_loss = sum(losses[-14:]) / 14
            
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                print(f"   RSI(14): {rsi:.1f}")
                
                if rsi < 30:
                    print(f"   🟢 RSI indicates OVERSOLD - potential BUY signal")
                elif rsi > 70:
                    print(f"   🔴 RSI indicates OVERBOUGHT - potential SELL signal")
                else:
                    print(f"   🟡 RSI is neutral")
    
    # 4. Check anomaly detection setup
    print("\n4️⃣ Anomaly Detection Requirements:")
    
    # Check for historical baseline
    baseline_check = await conn.fetchval("""
        SELECT COUNT(DISTINCT ed.id)
        FROM extracted_documents ed
        JOIN document_sections ds ON ed.id = ds.extracted_document_id
        JOIN section_embeddings se ON ds.id = se.document_section_id
        WHERE se.embedding_model LIKE 'local/%'
        AND ed.filing_date BETWEEN $1 AND $2
    """, test_date - timedelta(days=1095), test_date - timedelta(days=30))
    
    print(f"   Historical documents (3yr baseline): {baseline_check}")
    
    if baseline_check < 10:
        print("   ⚠️ Insufficient historical documents for anomaly detection")
        print("   💡 Anomaly detection requires historical baseline to compare against")
    
    # 5. Check signal generation directly
    print("\n5️⃣ Direct Signal Generation Test:")
    
    # Setup indicators
    indicator_registry.register(RSI(period=14))
    indicator_registry.register(SMA(period=20))
    indicator_registry.register(VolumeMA(period=20))
    indicator_engine = IndicatorEngine(indicator_registry)
    
    # Import and test strategy
    from services.technical_analysis.strategies.document_anomaly_strategy import DocumentAnomalyStrategy
    
    # Create VERY sensitive strategy for testing
    test_strategy = DocumentAnomalyStrategy(
        anomaly_lookback_days=30,
        min_anomaly_confidence=0.3,      # Very low threshold
        anomaly_threshold=0.6,           # High threshold = easier to trigger
        require_technical_confirmation=False,
        rsi_oversold=40,                 # Less extreme
        rsi_overbought=60,
        volume_surge_threshold=1.2
    )
    
    await test_strategy.setup_db_connection()
    
    # Convert symbol to company_id
    company_id = int(hashlib.md5(test_symbol.encode()).hexdigest()[:8], 16) % 1000000
    
    # Get full market data for indicators
    full_market_data = await conn.fetch("""
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
    """, test_symbol, test_date - timedelta(days=100), test_date)
    
    if full_market_data:
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in full_market_data])
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
        
        print(f"   Indicator values calculated: {list(indicator_values.keys())}")
        
        # Try to generate signal
        signal = await test_strategy.generate_signal(
            company_id, df, test_date, indicator_values
        )
        
        if signal:
            print(f"   ✅ SIGNAL GENERATED: {signal.signal_type.value}")
            print(f"      Confidence: {signal.confidence:.2f}")
            print(f"      Source: {signal.contributing_factors.get('signal_source', 'unknown')}")
        else:
            print("   ❌ No signal generated even with sensitive parameters")
            
            # Check what's missing
            print("\n   Checking components:")
            
            # Document anomaly check
            doc_anomaly = await test_strategy.analyze_document_anomalies(test_symbol, test_date)
            if doc_anomaly:
                print(f"   📄 Document anomaly: {doc_anomaly}")
            else:
                print("   📄 No document anomaly detected")
            
            # Technical signals
            if 'rsi_14' in indicator_values:
                rsi_val = indicator_values['rsi_14']
                print(f"   📊 RSI: {rsi_val:.1f} (thresholds: <{test_strategy.rsi_oversold} or >{test_strategy.rsi_overbought})")
    
    await test_strategy.cleanup()
    await conn.close()
    
    print("\n\n📋 SUMMARY:")
    print("The strategy requires EITHER:")
    print("1. Document anomalies (needs historical baseline + recent filings)")
    print("2. Technical signals (RSI oversold/overbought, volume surges)")
    print("3. Combined signals (both document and technical align)")
    print("\nLikely issues:")
    print("- Limited document data with embeddings")
    print("- No extreme technical conditions during test period")
    print("- Conservative thresholds even when relaxed")

if __name__ == "__main__":
    asyncio.run(diagnose_signals())