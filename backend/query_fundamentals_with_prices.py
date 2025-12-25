#!/usr/bin/env python3
"""
Query Fundamentals with Price Data

Shows how to query fundamental data with the same daily resolution as price data.
Useful for backtesting strategies that combine both.
"""

import asyncio
import asyncpg
import pandas as pd
from datetime import date, timedelta
from typing import Optional

class FundamentalsAndPriceQuery:
    """Query and analyze fundamentals with price data."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_combined_data(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """Get combined price and fundamental data."""
        
        # Query that joins price and fundamental data
        query = """
        WITH price_data AS (
            SELECT 
                date,
                open_price::NUMERIC as open,
                high_price::NUMERIC as high,
                low_price::NUMERIC as low,
                close_price::NUMERIC as close,
                volume::BIGINT as volume
            FROM daily_price_data
            WHERE symbol = $1
            AND date BETWEEN $2 AND $3
        ),
        fundamental_data AS (
            SELECT 
                date,
                market_cap,
                trailing_pe,
                forward_pe,
                price_to_book,
                price_to_sales,
                dividend_yield,
                return_on_equity,
                return_on_assets,
                profit_margin,
                current_ratio,
                total_debt_to_equity,
                peg_ratio,
                earnings_growth,
                revenue_growth,
                target_mean_price,
                recommendation_mean
            FROM daily_fundamentals
            WHERE symbol = $1
            AND date BETWEEN $2 AND $3
        )
        SELECT 
            p.*,
            f.market_cap,
            f.trailing_pe,
            f.forward_pe,
            f.price_to_book,
            f.price_to_sales,
            f.dividend_yield,
            f.return_on_equity,
            f.return_on_assets,
            f.profit_margin,
            f.current_ratio,
            f.total_debt_to_equity,
            f.peg_ratio,
            f.earnings_growth,
            f.revenue_growth,
            f.target_mean_price,
            f.recommendation_mean
        FROM price_data p
        LEFT JOIN fundamental_data f ON p.date = f.date
        ORDER BY p.date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        
        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # Forward fill fundamental data (it doesn't change daily)
            fundamental_columns = [col for col in df.columns if col not in 
                                 ['open', 'high', 'low', 'close', 'volume']]
            df[fundamental_columns] = df[fundamental_columns].fillna(method='ffill')
            
            return df
        
        return pd.DataFrame()
        
    async def analyze_pe_vs_price(self, symbol: str, days: int = 90):
        """Analyze P/E ratio changes vs price changes."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        df = await self.get_combined_data(symbol, start_date, end_date)
        
        if df.empty:
            print(f"No data found for {symbol}")
            return
            
        print(f"\n📊 P/E vs Price Analysis for {symbol}")
        print("=" * 60)
        
        # Calculate correlations
        if 'trailing_pe' in df.columns and df['trailing_pe'].notna().any():
            # Normalize for comparison
            df['price_norm'] = df['close'] / df['close'].iloc[0]
            df['pe_norm'] = df['trailing_pe'] / df['trailing_pe'].iloc[0]
            
            correlation = df['close'].corr(df['trailing_pe'])
            print(f"Price-PE Correlation: {correlation:.3f}")
            
            # Find extremes
            pe_series = df['trailing_pe'].dropna()
            if len(pe_series) > 0:
                min_pe = pe_series.min()
                max_pe = pe_series.max()
                avg_pe = pe_series.mean()
                
                print(f"\nP/E Statistics:")
                print(f"  Minimum: {min_pe:.1f}")
                print(f"  Maximum: {max_pe:.1f}") 
                print(f"  Average: {avg_pe:.1f}")
                print(f"  Current: {pe_series.iloc[-1]:.1f}")
                
                # Check if P/E is at extreme
                current_pe = pe_series.iloc[-1]
                pe_percentile = (pe_series < current_pe).sum() / len(pe_series) * 100
                print(f"  Percentile: {pe_percentile:.0f}%")
                
                if pe_percentile < 20:
                    print("  ⚡ P/E is near historical LOWS (potential value)")
                elif pe_percentile > 80:
                    print("  ⚠️  P/E is near historical HIGHS (potentially expensive)")
                    
        return df
        
    async def screen_by_fundamentals(self, min_roe: float = 0.15, 
                                   max_pe: float = 20,
                                   max_debt_equity: float = 1.0):
        """Screen stocks by fundamental criteria."""
        
        query = """
        WITH latest_fundamentals AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                date,
                trailing_pe,
                return_on_equity,
                total_debt_to_equity,
                dividend_yield,
                profit_margin,
                current_ratio,
                market_cap
            FROM daily_fundamentals
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY symbol, date DESC
        )
        SELECT 
            f.*,
            p.close_price::NUMERIC as current_price
        FROM latest_fundamentals f
        JOIN (
            SELECT DISTINCT ON (symbol)
                symbol,
                close_price
            FROM daily_price_data
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY symbol, date DESC
        ) p ON f.symbol = p.symbol
        WHERE f.return_on_equity >= $1
        AND f.trailing_pe > 0 AND f.trailing_pe <= $2
        AND f.total_debt_to_equity <= $3
        ORDER BY f.return_on_equity DESC
        """
        
        rows = await self.db_conn.fetch(query, min_roe, max_pe, max_debt_equity)
        
        print(f"\n🔍 Fundamental Screen Results")
        print(f"   Criteria: ROE >= {min_roe:.0%}, P/E <= {max_pe}, D/E <= {max_debt_equity}")
        print("=" * 80)
        
        if rows:
            print(f"{'Symbol':<10} {'Price':<10} {'P/E':<8} {'ROE':<8} {'D/E':<8} {'Yield':<8} {'Margin':<8}")
            print("-" * 80)
            
            for row in rows:
                print(f"{row['symbol']:<10} "
                      f"${row['current_price']:<9.2f} "
                      f"{row['trailing_pe']:<8.1f} "
                      f"{row['return_on_equity']:<8.1%} "
                      f"{row['total_debt_to_equity']:<8.1f} "
                      f"{row['dividend_yield'] or 0:<8.1%} "
                      f"{row['profit_margin'] or 0:<8.1%}")
                      
            print(f"\n✅ Found {len(rows)} stocks matching criteria")
        else:
            print("❌ No stocks found matching criteria")
            
    async def get_fundamental_history(self, symbol: str, metric: str, days: int = 30):
        """Get history of a specific fundamental metric."""
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        query = f"""
        SELECT date, {metric}
        FROM daily_fundamentals
        WHERE symbol = $1
        AND date BETWEEN $2 AND $3
        AND {metric} IS NOT NULL
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        
        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            print(f"\n📈 {metric.upper()} History for {symbol} ({days} days)")
            print("=" * 50)
            
            # Show recent values
            for i in range(min(5, len(df))):
                idx = -(i+1)
                print(f"{df.index[idx].strftime('%Y-%m-%d')}: {df.iloc[idx, 0]:.2f}")
                
            # Calculate change
            if len(df) >= 2:
                first_val = df.iloc[0, 0]
                last_val = df.iloc[-1, 0]
                if first_val != 0:
                    pct_change = ((last_val - first_val) / first_val) * 100
                    print(f"\nChange: {first_val:.2f} → {last_val:.2f} ({pct_change:+.1f}%)")
                    
            return df
        
        return pd.DataFrame()
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Demo fundamental + price queries."""
    
    query_tool = FundamentalsAndPriceQuery()
    
    try:
        await query_tool.setup()
        
        # Example 1: Analyze P/E vs Price
        await query_tool.analyze_pe_vs_price('VOLV-B', days=90)
        
        # Example 2: Screen by fundamentals
        print("\n" + "="*80)
        await query_tool.screen_by_fundamentals(
            min_roe=0.15,      # Minimum 15% ROE
            max_pe=25,         # Maximum P/E of 25
            max_debt_equity=1.5  # Maximum D/E of 1.5
        )
        
        # Example 3: Get specific metric history
        print("\n" + "="*80)
        await query_tool.get_fundamental_history('ERIC-B', 'trailing_pe', days=30)
        
        # Example 4: Combined data for backtesting
        print("\n" + "="*80)
        print("📊 Sample Combined Data for Backtesting")
        
        df = await query_tool.get_combined_data(
            'VOLV-B', 
            date.today() - timedelta(days=30),
            date.today()
        )
        
        if not df.empty:
            print("\nColumns available for strategy:")
            price_cols = ['open', 'high', 'low', 'close', 'volume']
            fundamental_cols = [col for col in df.columns if col not in price_cols]
            
            print(f"Price data: {price_cols}")
            print(f"Fundamental data: {fundamental_cols}")
            
            # Show sample row
            print(f"\nSample data from {df.index[-1].strftime('%Y-%m-%d')}:")
            last_row = df.iloc[-1]
            print(f"  Close: ${last_row['close']:.2f}")
            print(f"  P/E: {last_row.get('trailing_pe', 'N/A')}")
            print(f"  ROE: {last_row.get('return_on_equity', 'N/A')}")
            print(f"  Market Cap: {last_row.get('market_cap', 'N/A')}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await query_tool.cleanup()

if __name__ == "__main__":
    asyncio.run(main())