#!/usr/bin/env python3
"""
Yahoo Finance Daily Fundamentals Collector

Collects and stores fundamental data with daily resolution, matching the price data.
Integrates with existing daily workers for automated collection.
"""

import yfinance as yf
import pandas as pd
import asyncio
import asyncpg
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import json
import logging
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YahooDailyFundamentalsCollector:
    """Collects fundamental data daily from Yahoo Finance."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection and tables."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Create daily fundamentals table
        await self.create_daily_fundamentals_table()
        
    async def create_daily_fundamentals_table(self):
        """Create table for storing daily fundamental snapshots."""
        query = """
        CREATE TABLE IF NOT EXISTS daily_fundamentals (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(50) NOT NULL,
            date DATE NOT NULL,
            
            -- Valuation metrics (updated daily)
            market_cap BIGINT,
            enterprise_value BIGINT,
            trailing_pe FLOAT,
            forward_pe FLOAT,
            peg_ratio FLOAT,
            price_to_book FLOAT,
            price_to_sales FLOAT,
            ev_to_ebitda FLOAT,
            ev_to_revenue FLOAT,
            
            -- Share statistics
            shares_outstanding BIGINT,
            shares_float BIGINT,
            percent_held_insiders FLOAT,
            percent_held_institutions FLOAT,
            shares_short BIGINT,
            short_ratio FLOAT,
            short_percent_of_float FLOAT,
            
            -- Dividends & Splits
            dividend_rate FLOAT,
            dividend_yield FLOAT,
            trailing_annual_dividend_rate FLOAT,
            trailing_annual_dividend_yield FLOAT,
            five_year_avg_dividend_yield FLOAT,
            payout_ratio FLOAT,
            ex_dividend_date DATE,
            last_split_date DATE,
            last_split_factor VARCHAR(20),
            
            -- Analyst ratings (updated periodically)
            target_mean_price FLOAT,
            target_median_price FLOAT,
            target_high_price FLOAT,
            target_low_price FLOAT,
            number_of_analyst_opinions INT,
            recommendation_mean FLOAT,
            recommendation_key VARCHAR(20),
            
            -- Financial metrics (from latest reports)
            total_revenue BIGINT,
            revenue_per_share FLOAT,
            quarterly_revenue_growth FLOAT,
            gross_profit BIGINT,
            ebitda BIGINT,
            net_income BIGINT,
            diluted_eps FLOAT,
            quarterly_earnings_growth FLOAT,
            
            -- Profitability ratios
            profit_margin FLOAT,
            operating_margin FLOAT,
            gross_margin FLOAT,
            ebitda_margin FLOAT,
            
            -- Management effectiveness
            return_on_assets FLOAT,
            return_on_equity FLOAT,
            
            -- Balance sheet items
            total_cash BIGINT,
            total_cash_per_share FLOAT,
            total_debt BIGINT,
            total_debt_to_equity FLOAT,
            current_ratio FLOAT,
            book_value_per_share FLOAT,
            
            -- Cash flow
            operating_cash_flow BIGINT,
            levered_free_cash_flow BIGINT,
            
            -- Growth estimates
            earnings_growth FLOAT,
            revenue_growth FLOAT,
            
            -- Metadata
            currency VARCHAR(10),
            financial_currency VARCHAR(10),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Store full info as JSONB for flexibility
            raw_info JSONB,
            
            UNIQUE(symbol, date)
        );
        
        -- Create indexes for efficient queries
        CREATE INDEX IF NOT EXISTS idx_daily_fundamentals_symbol_date 
        ON daily_fundamentals(symbol, date DESC);
        
        CREATE INDEX IF NOT EXISTS idx_daily_fundamentals_date 
        ON daily_fundamentals(date DESC);
        
        CREATE INDEX IF NOT EXISTS idx_daily_fundamentals_pe 
        ON daily_fundamentals(trailing_pe) 
        WHERE trailing_pe IS NOT NULL;
        
        CREATE INDEX IF NOT EXISTS idx_daily_fundamentals_market_cap 
        ON daily_fundamentals(market_cap DESC) 
        WHERE market_cap IS NOT NULL;
        """
        
        await self.db_conn.execute(query)
        logger.info("✅ Daily fundamentals table ready with indexes")
        
    async def get_yahoo_symbol(self, primary_ticker: str) -> Optional[str]:
        """Get the correct Yahoo symbol from company_master table."""
        query = """
        SELECT yahoo_symbol, company_name, primary_exchange 
        FROM company_master 
        WHERE primary_ticker = $1
        """
        
        row = await self.db_conn.fetchrow(query, primary_ticker)
        
        if row and row['yahoo_symbol']:
            logger.debug(f"Found Yahoo symbol for {primary_ticker}: {row['yahoo_symbol']} "
                        f"({row['company_name']}, {row['primary_exchange']})")
            return row['yahoo_symbol']
        else:
            logger.warning(f"No Yahoo symbol found for {primary_ticker}")
            return None
        
    async def collect_fundamentals_for_symbol(self, symbol: str, 
                                            collection_date: date = None) -> Optional[Dict]:
        """Collect fundamental data for a single symbol."""
        if collection_date is None:
            collection_date = date.today()
            
        try:
            # Get the correct Yahoo symbol from company_master
            yahoo_symbol = await self.get_yahoo_symbol(symbol)
            if not yahoo_symbol:
                return None
                
            ticker = yf.Ticker(yahoo_symbol)
            
            # Get info - this contains most fundamentals
            info = ticker.info
            
            if not info or 'symbol' not in info:
                logger.warning(f"No data found for {yahoo_symbol}")
                return None
                
            # Extract comprehensive fundamentals
            fundamentals = {
                'symbol': symbol,
                'date': collection_date,
                
                # Valuation metrics
                'market_cap': info.get('marketCap'),
                'enterprise_value': info.get('enterpriseValue'),
                'trailing_pe': info.get('trailingPE'),
                'forward_pe': info.get('forwardPE'),
                'peg_ratio': info.get('pegRatio'),
                'price_to_book': info.get('priceToBook'),
                'price_to_sales': info.get('priceToSalesTrailing12Months'),
                'ev_to_ebitda': info.get('enterpriseToEbitda'),
                'ev_to_revenue': info.get('enterpriseToRevenue'),
                
                # Share statistics
                'shares_outstanding': info.get('sharesOutstanding'),
                'shares_float': info.get('floatShares'),
                'percent_held_insiders': info.get('heldPercentInsiders'),
                'percent_held_institutions': info.get('heldPercentInstitutions'),
                'shares_short': info.get('sharesShort'),
                'short_ratio': info.get('shortRatio'),
                'short_percent_of_float': info.get('shortPercentOfFloat'),
                
                # Dividends
                'dividend_rate': info.get('dividendRate'),
                'dividend_yield': info.get('dividendYield'),
                'trailing_annual_dividend_rate': info.get('trailingAnnualDividendRate'),
                'trailing_annual_dividend_yield': info.get('trailingAnnualDividendYield'),
                'five_year_avg_dividend_yield': info.get('fiveYearAvgDividendYield'),
                'payout_ratio': info.get('payoutRatio'),
                'ex_dividend_date': datetime.fromtimestamp(info['exDividendDate']).date() if info.get('exDividendDate') else None,
                'last_split_date': datetime.fromtimestamp(info['lastSplitDate']).date() if info.get('lastSplitDate') else None,
                'last_split_factor': info.get('lastSplitFactor'),
                
                # Analyst ratings
                'target_mean_price': info.get('targetMeanPrice'),
                'target_median_price': info.get('targetMedianPrice'),
                'target_high_price': info.get('targetHighPrice'),
                'target_low_price': info.get('targetLowPrice'),
                'number_of_analyst_opinions': info.get('numberOfAnalystOpinions'),
                'recommendation_mean': info.get('recommendationMean'),
                'recommendation_key': info.get('recommendationKey'),
                
                # Financial metrics
                'total_revenue': info.get('totalRevenue'),
                'revenue_per_share': info.get('revenuePerShare'),
                'quarterly_revenue_growth': info.get('quarterlyRevenueGrowth'),
                'gross_profit': info.get('grossProfits'),
                'ebitda': info.get('ebitda'),
                'net_income': info.get('netIncomeToCommon'),
                'diluted_eps': info.get('trailingEps'),
                'quarterly_earnings_growth': info.get('quarterlyEarningsGrowth'),
                
                # Profitability ratios
                'profit_margin': info.get('profitMargins'),
                'operating_margin': info.get('operatingMargins'),
                'gross_margin': info.get('grossMargins'),
                'ebitda_margin': info.get('ebitdaMargins'),
                
                # Management effectiveness
                'return_on_assets': info.get('returnOnAssets'),
                'return_on_equity': info.get('returnOnEquity'),
                
                # Balance sheet
                'total_cash': info.get('totalCash'),
                'total_cash_per_share': info.get('totalCashPerShare'),
                'total_debt': info.get('totalDebt'),
                'total_debt_to_equity': info.get('debtToEquity'),
                'current_ratio': info.get('currentRatio'),
                'book_value_per_share': info.get('bookValue'),
                
                # Cash flow
                'operating_cash_flow': info.get('operatingCashflow'),
                'levered_free_cash_flow': info.get('freeCashflow'),
                
                # Growth
                'earnings_growth': info.get('earningsGrowth'),
                'revenue_growth': info.get('revenueGrowth'),
                
                # Metadata
                'currency': info.get('currency'),
                'financial_currency': info.get('financialCurrency'),
                
                # Store raw info
                'raw_info': json.dumps(info)
            }
            
            # Count non-null metrics
            non_null_count = sum(1 for v in fundamentals.values() 
                               if v is not None and v != 'null')
            
            logger.info(f"✅ Collected {non_null_count} metrics for {symbol} on {collection_date}")
            
            return fundamentals
            
        except Exception as e:
            logger.error(f"Error collecting {symbol}: {e}")
            return None
            
    async def store_daily_fundamentals(self, fundamentals: Dict):
        """Store fundamentals in database."""
        # Build insert query dynamically
        fields = []
        values = []
        placeholders = []
        
        for key, value in fundamentals.items():
            if value is not None and key != 'raw_info':
                fields.append(key)
                values.append(value)
                placeholders.append(f"${len(values)}")
                
        # Handle raw_info separately
        if fundamentals.get('raw_info'):
            fields.append('raw_info')
            values.append(fundamentals['raw_info'])
            placeholders.append(f"${len(values)}")
            
        query = f"""
        INSERT INTO daily_fundamentals ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (symbol, date) 
        DO UPDATE SET
            {', '.join(f"{field} = EXCLUDED.{field}" for field in fields if field not in ['symbol', 'date'])},
            last_updated = CURRENT_TIMESTAMP
        """
        
        await self.db_conn.execute(query, *values)
        
    async def collect_daily_fundamentals(self, symbols: List[str], 
                                       collection_date: date = None):
        """Collect fundamentals for multiple symbols."""
        if collection_date is None:
            collection_date = date.today()
            
        logger.info(f"📊 Collecting fundamentals for {len(symbols)} symbols on {collection_date}")
        
        successful = 0
        failed = 0
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"Processing {symbol} ({i}/{len(symbols)})...")
            
            fundamentals = await self.collect_fundamentals_for_symbol(symbol, collection_date)
            
            if fundamentals:
                await self.store_daily_fundamentals(fundamentals)
                successful += 1
            else:
                failed += 1
                
            # Be respectful to Yahoo Finance
            await asyncio.sleep(1)
            
        logger.info(f"✅ Collection complete: {successful} successful, {failed} failed")
        
    async def get_historical_fundamentals(self, symbol: str, 
                                        start_date: date, 
                                        end_date: date) -> pd.DataFrame:
        """Get historical fundamentals for a symbol."""
        query = """
        SELECT * FROM daily_fundamentals
        WHERE symbol = $1 
        AND date BETWEEN $2 AND $3
        ORDER BY date
        """
        
        rows = await self.db_conn.fetch(query, symbol, start_date, end_date)
        
        if rows:
            df = pd.DataFrame([dict(row) for row in rows])
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            return df
        
        return pd.DataFrame()
        
    async def get_fundamentals_changes(self, symbol: str, days: int = 30) -> Dict:
        """Analyze fundamental changes over time."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        df = await self.get_historical_fundamentals(symbol, start_date, end_date)
        
        if df.empty:
            return {}
            
        # Calculate changes for key metrics
        metrics = ['trailing_pe', 'price_to_book', 'market_cap', 
                  'return_on_equity', 'total_debt_to_equity']
        
        changes = {}
        for metric in metrics:
            if metric in df.columns:
                series = df[metric].dropna()
                if len(series) >= 2:
                    latest = series.iloc[-1]
                    earliest = series.iloc[0]
                    if earliest != 0:
                        pct_change = ((latest - earliest) / earliest) * 100
                        changes[metric] = {
                            'latest': latest,
                            'earliest': earliest,
                            'change_pct': pct_change,
                            'days': len(series)
                        }
                        
        return changes
        
    async def get_symbols_from_company_master(self) -> List[str]:
        """Get list of symbols from company_master table that have recent price data."""
        query = """
        SELECT DISTINCT cm.primary_ticker
        FROM company_master cm
        INNER JOIN daily_price_data dpd ON cm.primary_ticker = dpd.symbol
        WHERE dpd.date >= CURRENT_DATE - INTERVAL '7 days'
        AND cm.yahoo_symbol IS NOT NULL
        AND cm.yahoo_finance_available = true
        ORDER BY cm.primary_ticker
        """
        
        rows = await self.db_conn.fetch(query)
        return [row['primary_ticker'] for row in rows]
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Test daily fundamentals collection."""
    
    collector = YahooDailyFundamentalsCollector()
    
    try:
        await collector.setup()
        
        # Option 1: Test with specific symbols (using correct symbols from company_master)
        test_symbols = ['VOLV B', 'ERIC-B', 'SAND', 'ABB', 'ATCO A']
        
        print("🚀 Testing Yahoo Finance Daily Fundamentals Collection")
        print("=" * 60)
        
        # Collect today's fundamentals
        await collector.collect_daily_fundamentals(test_symbols)
        
        # Show what we collected
        query = """
        SELECT symbol, date, market_cap, trailing_pe, 
               return_on_equity, total_debt_to_equity
        FROM daily_fundamentals
        WHERE date = $1
        ORDER BY symbol
        """
        
        rows = await collector.db_conn.fetch(query, date.today())
        
        print("\n📊 Today's Fundamentals:")
        print(f"{'Symbol':<10} {'Market Cap':<15} {'P/E':<10} {'ROE':<10} {'D/E':<10}")
        print("-" * 60)
        
        for row in rows:
            print(f"{row['symbol']:<10} "
                  f"{row['market_cap'] or 'N/A':<15} "
                  f"{row['trailing_pe'] or 'N/A':<10} "
                  f"{row['return_on_equity'] or 'N/A':<10} "
                  f"{row['total_debt_to_equity'] or 'N/A':<10}")
                  
        # Test historical analysis
        print("\n📈 Fundamental Changes (30 days) for VOLV-B:")
        changes = await collector.get_fundamentals_changes('VOLV-B', 30)
        
        for metric, data in changes.items():
            print(f"  {metric}: {data['earliest']:.2f} → {data['latest']:.2f} "
                  f"({data['change_pct']:+.1f}%)")
                  
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await collector.cleanup()

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/yahoo_fundamentals_daily.log'),
            logging.StreamHandler()
        ]
    )
    
    asyncio.run(main())