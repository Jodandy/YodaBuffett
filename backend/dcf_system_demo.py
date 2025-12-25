#!/usr/bin/env python3
"""
DCF System Demo

Demonstrates the complete DCF report-based valuation system:
1. Pre-computed DCF valuations stored per financial report
2. Price comparison system for investment signals
3. Model versioning and quality tracking
"""

import asyncio
import asyncpg
from datetime import date, timedelta
import pandas as pd

async def demo_dcf_system():
    """Demonstrate the complete DCF system."""
    
    conn = await asyncpg.connect('postgresql://yodabuffett:password@localhost:5432/yodabuffett')
    
    try:
        print("🏦 DCF Report-Based Valuation System Demo")
        print("=" * 60)
        
        # 1. Show stored DCF valuations
        print("\n📊 1. PRE-COMPUTED DCF VALUATIONS")
        
        valuations_query = """
        SELECT 
            symbol,
            report_date,
            publish_date,
            fair_value_stock_median as fair_value,
            valuation_confidence,
            data_quality_score,
            periods_used,
            model_version
        FROM dcf_valuations
        WHERE model_version = 'clean_dcf_v1.0'
        AND valuation_confidence >= 0.20
        ORDER BY computation_date DESC
        LIMIT 10
        """
        
        valuations = await conn.fetch(valuations_query)
        
        print(f"Found {len(valuations)} stored DCF valuations:")
        print(f"{'Symbol':<10} {'Report Date':<12} {'Fair Value':<12} {'Confidence':<12} {'Quality':<8}")
        print("-" * 60)
        
        for val in valuations:
            print(f"{val['symbol']:<10} {val['report_date']:<12} "
                  f"{val['fair_value']:>10.0f} SEK {val['valuation_confidence']:>9.0%} "
                  f"{val['data_quality_score']:>6.0%}")
        
        # 2. Generate current investment signals
        print("\n🎯 2. CURRENT INVESTMENT SIGNALS")
        
        # Get recent prices for comparison
        signals_query = """
        SELECT DISTINCT ON (v.symbol)
            v.symbol,
            v.fair_value_stock_median as fair_value,
            p.close_price as current_price,
            (v.fair_value_stock_median - p.close_price) / p.close_price as implied_return,
            v.valuation_confidence,
            v.publish_date,
            p.date as price_date,
            CASE 
                WHEN (v.fair_value_stock_median - p.close_price) / p.close_price > 0.20 THEN 'STRONG_BUY'
                WHEN (v.fair_value_stock_median - p.close_price) / p.close_price > 0.10 THEN 'BUY'
                WHEN (v.fair_value_stock_median - p.close_price) / p.close_price < -0.20 THEN 'STRONG_SELL'
                WHEN (v.fair_value_stock_median - p.close_price) / p.close_price < -0.10 THEN 'SELL'
                ELSE 'HOLD'
            END as signal
        FROM dcf_valuations v
        JOIN daily_price_data p ON v.symbol = p.symbol
        WHERE v.model_version = 'clean_dcf_v1.0'
        AND v.valuation_confidence >= 0.20
        AND p.date >= CURRENT_DATE - INTERVAL '7 days'
        AND v.fair_value_stock_median > 0
        ORDER BY v.symbol, p.date DESC, v.publish_date DESC
        """
        
        signals = await conn.fetch(signals_query)
        
        if signals:
            print(f"Investment signals based on recent prices:")
            print(f"{'Symbol':<10} {'Signal':<12} {'Implied Return':<15} {'Fair Value':<12} {'Current Price':<12}")
            print("-" * 75)
            
            buy_signals = []
            sell_signals = []
            
            for signal in signals:
                implied_return = signal['implied_return']
                signal_strength = signal['signal']
                
                print(f"{signal['symbol']:<10} {signal_strength:<12} "
                      f"{implied_return:>13.0%} {signal['fair_value']:>10.0f} SEK "
                      f"{signal['current_price']:>10.0f} SEK")
                
                if signal_strength in ['BUY', 'STRONG_BUY']:
                    buy_signals.append(signal)
                elif signal_strength in ['SELL', 'STRONG_SELL']:
                    sell_signals.append(signal)
            
            print(f"\n📈 Summary: {len(buy_signals)} BUY signals, {len(sell_signals)} SELL signals")
            
            if buy_signals:
                print(f"\n🟢 TOP OPPORTUNITIES:")
                sorted_buys = sorted(buy_signals, key=lambda x: x['implied_return'], reverse=True)
                for signal in sorted_buys[:3]:
                    print(f"   {signal['symbol']}: {signal['implied_return']:+.0%} upside "
                          f"(Fair: {signal['fair_value']:.0f}, Price: {signal['current_price']:.0f})")
        else:
            print("No current investment signals available")
        
        # 3. System statistics
        print("\n📊 3. SYSTEM STATISTICS")
        
        stats_query = """
        SELECT 
            COUNT(*) as total_valuations,
            COUNT(DISTINCT symbol) as companies_covered,
            AVG(valuation_confidence) as avg_confidence,
            AVG(data_quality_score) as avg_data_quality,
            MIN(publish_date) as earliest_report,
            MAX(publish_date) as latest_report,
            AVG(computation_time_ms) as avg_computation_time
        FROM dcf_valuations
        WHERE model_version = 'clean_dcf_v1.0'
        """
        
        stats = await conn.fetchrow(stats_query)
        
        print(f"Model Version: clean_dcf_v1.0")
        print(f"Total Valuations: {stats['total_valuations']:,}")
        print(f"Companies Covered: {stats['companies_covered']}")
        print(f"Date Range: {stats['earliest_report']} to {stats['latest_report']}")
        print(f"Average Confidence: {stats['avg_confidence']:.1%}")
        print(f"Average Data Quality: {stats['avg_data_quality']:.1%}")
        print(f"Average Computation Time: {stats['avg_computation_time']:.0f}ms")
        
        # 4. Show model benefits
        print(f"\n✅ 4. SYSTEM BENEFITS")
        print(f"   🚀 Performance: DCF computed once per report, not per backtest")
        print(f"   📊 Consistency: Same valuation across all price comparisons")
        print(f"   🔧 Versioning: Multiple model versions can coexist")
        print(f"   📈 Audit Trail: Complete history of valuations over time")
        print(f"   🎛️ Flexibility: Adjust signal thresholds without recalculation")
        
        print(f"\n🎯 Ready for efficient backtesting and live trading signals!")
    
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(demo_dcf_system())