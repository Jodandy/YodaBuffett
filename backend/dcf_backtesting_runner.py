#!/usr/bin/env python3
"""
DCF Backtesting Runner

Generate DCF valuations for backtesting purposes.
Clean approach - only uses published financial data (no lookahead bias).
"""

import asyncio
import asyncpg
from publish_date_only_dcf import PublishDateOnlyDCF, DCFParameters
from datetime import datetime, timedelta
import pandas as pd
import json

class DCFBacktestingRunner:
    """Generate DCF valuations for backtesting"""
    
    def __init__(self):
        self.params = DCFParameters(num_simulations=10000)
        self.engine = PublishDateOnlyDCF(self.params)
        
    async def setup(self):
        await self.engine.setup()
        
    async def cleanup(self):
        await self.engine.cleanup()
    
    async def run_historical_valuations(self, symbols=None, start_date=None, end_date=None, frequency_days=30):
        """
        Run DCF valuations at regular intervals for backtesting
        
        Args:
            symbols: List of symbols to analyze (None = all available)
            start_date: Start date for analysis (default: 2022-01-01)
            end_date: End date for analysis (default: today)
            frequency_days: Days between valuations (default: 30)
        """
        
        if start_date is None:
            start_date = datetime(2022, 1, 1)
        if end_date is None:
            end_date = datetime.now()
            
        print('🎯 DCF BACKTESTING VALUATION RUNNER')
        print('=' * 70)
        print(f'Period: {start_date.date()} to {end_date.date()}')
        print(f'Frequency: Every {frequency_days} days')
        print(f'Using ONLY published financial data (no lookahead bias)')
        
        # Get symbols to analyze
        if symbols is None:
            symbols_query = """
            SELECT DISTINCT fs.symbol
            FROM financial_statements fs
            JOIN balance_sheet_data bs ON fs.symbol = bs.symbol
            WHERE fs.publish_date IS NOT NULL
            AND bs.shares_outstanding > 0
            GROUP BY fs.symbol
            HAVING COUNT(DISTINCT fs.publish_date) >= 3
            ORDER BY fs.symbol
            """
            symbol_records = await self.engine.db_conn.fetch(symbols_query)
            symbols = [r['symbol'] for r in symbol_records]
        
        print(f'📊 Analyzing {len(symbols):,} companies')
        
        # Generate valuation dates
        valuation_dates = []
        current_date = start_date
        while current_date <= end_date:
            valuation_dates.append(current_date)
            current_date += timedelta(days=frequency_days)
        
        print(f'📅 {len(valuation_dates)} valuation dates')
        
        results = []
        total_combinations = len(symbols) * len(valuation_dates)
        processed = 0
        
        for symbol in symbols:
            for valuation_date in valuation_dates:
                processed += 1
                
                if processed % 100 == 0:
                    progress = processed / total_combinations * 100
                    print(f'   📈 {progress:.1f}% | {processed:,}/{total_combinations:,} | Processing {symbol} on {valuation_date.date()}')
                
                try:
                    # Get market price at valuation date
                    price_query = """
                    SELECT close_price
                    FROM daily_price_data
                    WHERE symbol = $1
                    AND date <= $2
                    ORDER BY date DESC
                    LIMIT 1
                    """
                    
                    price_record = await self.engine.db_conn.fetchrow(price_query, symbol, valuation_date.date())
                    
                    if not price_record:
                        continue
                        
                    market_price = float(price_record['close_price'])
                    
                    # Run DCF valuation
                    dcf_result = await self.engine.value_company(symbol, valuation_date, market_price)
                    
                    if dcf_result:
                        results.append({
                            'symbol': symbol,
                            'valuation_date': valuation_date.date(),
                            'market_price': market_price,
                            'fair_value_median': dcf_result['fair_value_median'],
                            'fair_value_mean': dcf_result['fair_value_mean'],
                            'fair_value_std': dcf_result['fair_value_std'],
                            'implied_return': dcf_result['implied_return'],
                            'valuation_signal': dcf_result['valuation_signal'],
                            'rate_sensitivity_score': dcf_result['rate_sensitivity']['rate_sensitivity_score'],
                            'wacc': dcf_result['wacc'],
                            'actual_revenue': dcf_result['actual_revenue'],
                            'actual_margin': dcf_result['actual_margin']
                        })
                        
                        # Show progress for first few results
                        if len(results) <= 20:
                            return_pct = dcf_result['implied_return'] * 100
                            print(f'      {symbol:8} | ${dcf_result["fair_value_median"]:6.0f} vs ${market_price:6.0f} ({return_pct:+5.0f}%) | {dcf_result["valuation_signal"]}')
                
                except Exception as e:
                    if processed <= 20:  # Log first few errors for debugging
                        print(f'      ❌ Error for {symbol} on {valuation_date.date()}: {str(e)[:60]}')
                    continue
        
        print(f'\n✅ BACKTESTING VALUATIONS COMPLETE')
        print(f'   Generated {len(results):,} DCF valuations')
        if results:
            print(f'   Coverage: {len(set(r["symbol"] for r in results)):,} companies')
            print(f'   Date range: {min(r["valuation_date"] for r in results)} to {max(r["valuation_date"] for r in results)}')
        else:
            print('   ⚠️ No successful DCF valuations generated')
        
        return results
    
    async def save_results(self, results, filename='dcf_backtest_results.json'):
        """Save results to JSON file"""
        
        # Convert datetime objects to strings for JSON serialization
        json_results = []
        for r in results:
            r_copy = r.copy()
            r_copy['valuation_date'] = r_copy['valuation_date'].isoformat()
            json_results.append(r_copy)
        
        with open(filename, 'w') as f:
            json.dump(json_results, f, indent=2, default=str)
        
        print(f'💾 Saved {len(results):,} results to {filename}')
        
        # Also create CSV for easier analysis
        df = pd.DataFrame(results)
        csv_filename = filename.replace('.json', '.csv')
        df.to_csv(csv_filename, index=False)
        print(f'💾 Also saved as CSV: {csv_filename}')
    
    async def quick_sample(self, symbols=['AAK', 'ABB', 'VOLV-B', 'ERIC-B', 'ASSA-B'], days_back=365):
        """Quick sample for testing"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        print(f'🔬 QUICK SAMPLE: {len(symbols)} companies, last {days_back} days')
        
        results = await self.run_historical_valuations(
            symbols=symbols, 
            start_date=start_date, 
            end_date=end_date, 
            frequency_days=60  # Every 2 months
        )
        
        if results:
            await self.save_results(results, 'dcf_quick_sample.json')
            
            # Show summary
            df = pd.DataFrame(results)
            print(f'\n📊 QUICK SAMPLE SUMMARY:')
            print(f'   Total valuations: {len(results):,}')
            print(f'   Undervalued signals: {sum(1 for r in results if r["valuation_signal"] == "UNDERVALUED"):,}')
            print(f'   Overvalued signals: {sum(1 for r in results if r["valuation_signal"] == "OVERVALUED"):,}')
            
            if len(results) > 0:
                avg_return = sum(r['implied_return'] for r in results) / len(results) * 100
                print(f'   Average implied return: {avg_return:+.1f}%')
        else:
            print(f'\n⚠️ QUICK SAMPLE: No DCF valuations generated')
            print(f'   This may indicate data availability issues or timing problems')
        
        return results

async def main():
    """Main function with usage examples"""
    
    runner = DCFBacktestingRunner()
    await runner.setup()
    
    print('🎯 DCF BACKTESTING OPTIONS:')
    print('1. Quick sample (5 companies, last year)')
    print('2. Full historical analysis (all companies, 2022-2025)')
    print('3. Custom analysis')
    
    # For now, run quick sample by default
    print('\n🔬 Running quick sample...')
    
    results = await runner.quick_sample()
    
    await runner.cleanup()
    
    print('\n🎯 TO RUN DIFFERENT ANALYSES:')
    print('# Quick sample:')
    print('python3 dcf_backtesting_runner.py')
    print('')
    print('# Full analysis (modify main() function):')
    print('results = await runner.run_historical_valuations()')
    print('await runner.save_results(results, "full_dcf_backtest.json")')

if __name__ == "__main__":
    asyncio.run(main())