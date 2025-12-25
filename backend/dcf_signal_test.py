#!/usr/bin/env python3
"""
Simple DCF signal test to validate the system works.
"""

import asyncio
import asyncpg
from datetime import datetime
from clean_dcf_engine import CleanDCFEngine, DCFConfig

async def test_dcf_signals():
    """Test DCF signal generation on a few companies."""
    
    # Setup
    db_conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    engine = CleanDCFEngine(DCFConfig(num_simulations=500))  # Faster
    await engine.setup()
    
    try:
        # Get test companies with price data
        test_symbols = ['VOLV-B', 'ABB', 'ERIC-B']
        test_date = datetime(2024, 6, 1)
        
        print(f"🧪 DCF Signal Test - {test_date.date()}")
        print("=" * 50)
        
        signals = []
        
        for symbol in test_symbols:
            print(f"\n📊 Testing {symbol}")
            
            # Get price
            price_query = """
            SELECT close_price
            FROM daily_price_data
            WHERE symbol = $1 AND date <= $2
            ORDER BY date DESC
            LIMIT 1
            """
            
            price_row = await db_conn.fetchrow(price_query, symbol, test_date.date())
            if not price_row:
                print(f"  ❌ No price data")
                continue
            
            market_price = float(price_row['close_price'])
            print(f"  Market Price: ${market_price:.2f}")
            
            # Run DCF
            try:
                dcf_result = await engine.value_company(symbol, test_date, market_price)
                
                if dcf_result:
                    fair_value = dcf_result['fair_value_median']
                    implied_return = dcf_result['implied_return']
                    signal = dcf_result['valuation_signal']
                    
                    print(f"  Fair Value: ${fair_value:.0f}")
                    print(f"  Implied Return: {implied_return:+.0%}")
                    print(f"  Signal: {signal}")
                    
                    # Generate trading signal
                    if implied_return > 0.20:  # 20% threshold
                        trading_signal = "🟢 BUY"
                    elif implied_return < -0.20:
                        trading_signal = "🔴 SELL"
                    else:
                        trading_signal = "⚪ HOLD"
                    
                    print(f"  Trading Signal: {trading_signal}")
                    
                    signals.append({
                        'symbol': symbol,
                        'fair_value': fair_value,
                        'market_price': market_price,
                        'implied_return': implied_return,
                        'signal': signal,
                        'trading_signal': trading_signal
                    })
                else:
                    print(f"  ❌ DCF calculation failed")
            
            except Exception as e:
                print(f"  ❌ Error: {e}")
        
        # Summary
        if signals:
            buy_signals = [s for s in signals if '🟢' in s['trading_signal']]
            sell_signals = [s for s in signals if '🔴' in s['trading_signal']]
            
            print(f"\n📈 SIGNAL SUMMARY:")
            print(f"  Buy Signals: {len(buy_signals)}")
            print(f"  Sell Signals: {len(sell_signals)}")
            print(f"  Hold Signals: {len(signals) - len(buy_signals) - len(sell_signals)}")
            
            if buy_signals:
                print(f"\n🟢 BUY OPPORTUNITIES:")
                for s in buy_signals:
                    print(f"  {s['symbol']}: {s['implied_return']:+.0%} upside")
            
            if sell_signals:
                print(f"\n🔴 SELL SIGNALS:")
                for s in sell_signals:
                    print(f"  {s['symbol']}: {s['implied_return']:+.0%} overvalued")
        
        print(f"\n✅ DCF Signal Test Complete")
        return signals
    
    finally:
        await engine.cleanup()
        await db_conn.close()

if __name__ == "__main__":
    asyncio.run(test_dcf_signals())