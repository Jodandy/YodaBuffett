#!/usr/bin/env python3
"""
Historical DCF Analysis

Run DCF valuations for specific companies across all their available historical data
to see how fair values evolved over time and validate model performance.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dcf_monte_carlo_fixed import DCFMonteCarloFixed, DCFParameters
import logging

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

class HistoricalDCFAnalyzer:
    
    def __init__(self):
        self.dcf_engine = None
        self.db_conn = None
        
    async def setup(self):
        """Initialize connections"""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Initialize DCF engine
        params = DCFParameters(
            num_simulations=5000,  # Fewer simulations for speed
            projection_years=10,
            terminal_growth_rate=0.025,
            risk_free_rate=0.03,
            market_premium=0.07
        )
        
        self.dcf_engine = DCFMonteCarloFixed(params)
        await self.dcf_engine.setup()
        
    async def get_historical_dates(self, symbol: str) -> list:
        """Get all available dates with sufficient historical data for DCF"""
        
        # Get date range where we have both fundamentals and price data
        query = """
        WITH date_coverage AS (
            SELECT 
                hfd.date,
                hfd.close_price,
                hfd.market_cap,
                fs.period_date,
                ROW_NUMBER() OVER (PARTITION BY hfd.date ORDER BY fs.period_date DESC) as rn
            FROM historical_fundamentals_daily hfd
            LEFT JOIN financial_statements fs ON fs.symbol = hfd.symbol 
                AND fs.period_date <= hfd.date
                AND fs.period_date >= hfd.date - INTERVAL '2 years'
            WHERE hfd.symbol = $1
            AND hfd.close_price > 0
            AND hfd.market_cap > 0
        )
        SELECT 
            date,
            close_price,
            market_cap
        FROM date_coverage
        WHERE rn = 1  -- Most recent financial statement
        AND period_date IS NOT NULL  -- Must have financial data
        AND date >= '2020-01-01'  -- Start from 2020 for manageable dataset
        ORDER BY date
        """
        
        records = await self.db_conn.fetch(query, symbol)
        
        # Sample monthly to avoid too many data points
        monthly_dates = []
        last_month = None
        
        for record in records:
            current_month = (record['date'].year, record['date'].month)
            if current_month != last_month:
                monthly_dates.append({
                    'date': record['date'],
                    'price': float(record['close_price']),
                    'market_cap': float(record['market_cap'])
                })
                last_month = current_month
                
        print(f"📅 Found {len(monthly_dates)} monthly data points for {symbol}")
        return monthly_dates
        
    async def run_historical_analysis(self, symbol: str) -> pd.DataFrame:
        """Run DCF analysis across all historical dates"""
        
        print(f"\n🔍 Running historical DCF analysis for {symbol}")
        print("=" * 60)
        
        historical_dates = await self.get_historical_dates(symbol)
        
        if len(historical_dates) < 6:
            print(f"❌ Insufficient historical data for {symbol}")
            return pd.DataFrame()
            
        results = []
        successful = 0
        
        for i, date_info in enumerate(historical_dates):
            valuation_date = datetime.combine(date_info['date'], datetime.min.time())
            market_price = date_info['price']
            
            if i % 5 == 0:
                print(f"   📊 Progress: {i+1}/{len(historical_dates)} - {valuation_date.date()}")
            
            try:
                # Run DCF valuation for this historical date
                dcf_result = await self.dcf_engine.value_company(
                    symbol, 
                    valuation_date, 
                    market_price
                )
                
                if dcf_result:
                    # Save to database
                    try:
                        dcf_id = await self.dcf_engine.save_valuation(dcf_result)
                        if dcf_id:
                            logger.info(f"Saved DCF valuation {dcf_id} for {symbol} on {valuation_date.date()}")
                    except Exception as e:
                        logger.warning(f"Failed to save DCF result for {symbol} on {valuation_date.date()}: {e}")
                    
                    rate_sens = dcf_result['rate_sensitivity']
                    
                    results.append({
                        'date': valuation_date.date(),
                        'market_price': market_price,
                        'fair_value_median': dcf_result['fair_value_median'],
                        'fair_value_p25': dcf_result['fair_value_p25'],
                        'fair_value_p75': dcf_result['fair_value_p75'],
                        'implied_return': dcf_result['implied_return'] * 100,
                        'valuation_signal': dcf_result['valuation_signal'],
                        'actual_margin': dcf_result['actual_margin'] * 100,
                        'actual_interest_ratio': dcf_result['actual_interest_ratio'] * 100,
                        'rate_sensitivity_score': rate_sens['rate_sensitivity_score'],
                        'debt_category': rate_sens['debt_burden_category'],
                        'wacc': dcf_result['wacc'] * 100
                    })
                    successful += 1
                    
            except Exception as e:
                logger.warning(f"Error processing {symbol} on {valuation_date.date()}: {e}")
                
        print(f"✅ Completed {successful}/{len(historical_dates)} historical valuations")
        
        if results:
            return pd.DataFrame(results)
        else:
            return pd.DataFrame()
            
    def analyze_historical_performance(self, df: pd.DataFrame, symbol: str):
        """Analyze the historical DCF performance"""
        
        if df.empty:
            print(f"❌ No data to analyze for {symbol}")
            return
            
        print(f"\n📊 HISTORICAL DCF ANALYSIS - {symbol}")
        print("=" * 60)
        
        # Basic statistics
        total_periods = len(df)
        undervalued_periods = len(df[df['valuation_signal'] == 'UNDERVALUED'])
        overvalued_periods = len(df[df['valuation_signal'] == 'OVERVALUED'])
        fair_value_periods = len(df[df['valuation_signal'] == 'FAIR_VALUE'])
        
        print(f"\n📈 VALUATION SIGNALS OVER TIME:")
        print(f"   Total periods analyzed:    {total_periods}")
        print(f"   Undervalued periods:       {undervalued_periods} ({undervalued_periods/total_periods*100:.1f}%)")
        print(f"   Overvalued periods:        {overvalued_periods} ({overvalued_periods/total_periods*100:.1f}%)")
        print(f"   Fair value periods:        {fair_value_periods} ({fair_value_periods/total_periods*100:.1f}%)")
        
        # Price vs Fair Value evolution
        avg_implied_return = df['implied_return'].mean()
        max_undervalue = df['implied_return'].max()
        max_overvalue = df['implied_return'].min()
        
        print(f"\n💰 IMPLIED RETURNS ANALYSIS:")
        print(f"   Average implied return:    {avg_implied_return:+6.1f}%")
        print(f"   Max undervaluation:        {max_undervalue:+6.1f}%")
        print(f"   Max overvaluation:         {max_overvalue:+6.1f}%")
        print(f"   Return volatility:         {df['implied_return'].std():6.1f}%")
        
        # Interest rate sensitivity evolution
        avg_rate_sensitivity = df['rate_sensitivity_score'].mean()
        max_rate_sensitivity = df['rate_sensitivity_score'].max()
        
        print(f"\n📊 INTEREST RATE SENSITIVITY:")
        print(f"   Average rate sensitivity:  {avg_rate_sensitivity:.1f}/10")
        print(f"   Max rate sensitivity:      {max_rate_sensitivity:.1f}/10")
        print(f"   Debt evolution:            {df['debt_category'].value_counts().to_dict()}")
        
        # Operating metrics evolution
        print(f"\n🏭 OPERATING METRICS EVOLUTION:")
        print(f"   Average margin:            {df['actual_margin'].mean():.1f}%")
        print(f"   Margin range:              {df['actual_margin'].min():.1f}% to {df['actual_margin'].max():.1f}%")
        print(f"   Average interest burden:   {df['actual_interest_ratio'].mean():.1f}%")
        print(f"   Interest range:            {df['actual_interest_ratio'].min():.1f}% to {df['actual_interest_ratio'].max():.1f}%")
        
        # Recent vs Historical
        recent_data = df.tail(6)  # Last 6 months
        historical_data = df.head(-6) if len(df) > 6 else df
        
        if len(recent_data) > 0 and len(historical_data) > 0:
            print(f"\n🔄 RECENT vs HISTORICAL (Last 6 months vs Earlier):")
            print(f"   Recent avg implied return: {recent_data['implied_return'].mean():+6.1f}%")
            print(f"   Historical avg return:     {historical_data['implied_return'].mean():+6.1f}%")
            print(f"   Recent avg rate sens:      {recent_data['rate_sensitivity_score'].mean():.1f}/10")
            print(f"   Historical avg rate sens:  {historical_data['rate_sensitivity_score'].mean():.1f}/10")
        
        # Key dates analysis
        print(f"\n📅 KEY HISTORICAL MOMENTS:")
        
        # Most undervalued
        most_undervalued = df.loc[df['implied_return'].idxmax()]
        print(f"   Most undervalued:          {most_undervalued['date']} "
              f"(Market: ${most_undervalued['market_price']:.2f}, "
              f"Fair: ${most_undervalued['fair_value_median']:.2f}, "
              f"Return: {most_undervalued['implied_return']:+.1f}%)")
        
        # Most overvalued  
        most_overvalued = df.loc[df['implied_return'].idxmin()]
        print(f"   Most overvalued:           {most_overvalued['date']} "
              f"(Market: ${most_overvalued['market_price']:.2f}, "
              f"Fair: ${most_overvalued['fair_value_median']:.2f}, "
              f"Return: {most_overvalued['implied_return']:+.1f}%)")
        
        # Highest rate sensitivity
        highest_rate_sens = df.loc[df['rate_sensitivity_score'].idxmax()]
        print(f"   Highest rate sensitivity:  {highest_rate_sens['date']} "
              f"(Score: {highest_rate_sens['rate_sensitivity_score']:.1f}/10, "
              f"Interest: {highest_rate_sens['actual_interest_ratio']:.1f}%)")
        
    async def cleanup(self):
        """Close connections"""
        if self.dcf_engine:
            await self.dcf_engine.cleanup()
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run historical DCF analysis for AAK and Volvo"""
    
    companies = ['AAK', 'VOLV B']
    
    analyzer = HistoricalDCFAnalyzer()
    
    try:
        await analyzer.setup()
        
        for symbol in companies:
            # Run historical analysis
            historical_df = await analyzer.run_historical_analysis(symbol)
            
            if not historical_df.empty:
                # Analyze performance
                analyzer.analyze_historical_performance(historical_df, symbol)
                
                # Save results for further analysis if needed
                output_file = f"/Users/jdandemar/Documents/YodaBuffett/backend/historical_dcf_{symbol.lower().replace(' ', '_')}.csv"
                historical_df.to_csv(output_file, index=False)
                print(f"\n💾 Results saved to: {output_file}")
            
            print("\n" + "="*80 + "\n")
                
    except Exception as e:
        logger.error(f"Error in historical analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())