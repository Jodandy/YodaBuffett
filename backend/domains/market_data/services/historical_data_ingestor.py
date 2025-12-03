#!/usr/bin/env python3
"""
Historical Market Data Ingestion Service
Fetches and stores historical price data from Yahoo Finance into TimescaleDB
"""

import asyncio
import asyncpg
import yfinance as yf
import numpy as np
import os
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from decimal import Decimal
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalDataIngestor:
    """Service for ingesting historical market data"""
    
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv(
            'DATABASE_URL', 
            'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        )
        self.conn: Optional[asyncpg.Connection] = None
    
    async def connect(self):
        """Connect to database"""
        self.conn = await asyncpg.connect(self.database_url)
        logger.info("✅ Connected to database")
    
    async def disconnect(self):
        """Disconnect from database"""
        if self.conn:
            await self.conn.close()
            self.conn = None
    
    async def get_symbols_to_update(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get symbols that need historical data updates"""
        
        query = """
        SELECT symbol, company_name, yahoo_symbol, last_data_fetch
        FROM market_data_symbols 
        WHERE last_data_fetch IS NULL 
           OR last_data_fetch < NOW() - INTERVAL '1 day'
        ORDER BY 
            CASE WHEN last_data_fetch IS NULL THEN 0 ELSE 1 END,
            last_data_fetch ASC NULLS FIRST
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        rows = await self.conn.fetch(query)
        return [dict(row) for row in rows]
    
    async def fetch_yahoo_data(
        self, 
        yahoo_symbol: str, 
        start_date: date, 
        end_date: date
    ) -> Optional[List[Dict[str, Any]]]:
        """Fetch historical data from Yahoo Finance"""
        
        try:
            logger.info(f"📊 Fetching {yahoo_symbol} from {start_date} to {end_date}")
            
            ticker = yf.Ticker(yahoo_symbol)
            hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)
            
            if hist.empty:
                logger.warning(f"❌ No data returned for {yahoo_symbol}")
                return None
            
            # Convert to list of dictionaries
            price_data = []
            for date_idx, row in hist.iterrows():
                if not (np.isnan(row['Open']) or np.isnan(row['Close'])):
                    price_data.append({
                        'date': date_idx.date(),
                        'open_price': float(row['Open']),
                        'high_price': float(row['High']),
                        'low_price': float(row['Low']),
                        'close_price': float(row['Close']),
                        'adjusted_close': float(row['Adj Close']),
                        'volume': int(row['Volume']) if not np.isnan(row['Volume']) else None
                    })
            
            logger.info(f"✅ Retrieved {len(price_data)} price points for {yahoo_symbol}")
            return price_data
            
        except Exception as e:
            logger.error(f"❌ Error fetching {yahoo_symbol}: {e}")
            return None
    
    async def calculate_technical_indicators(
        self, 
        price_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Calculate technical indicators for price data"""
        
        if len(price_data) < 2:
            return price_data
        
        # Sort by date
        price_data.sort(key=lambda x: x['date'])
        
        # Calculate daily returns
        for i, data in enumerate(price_data):
            if i == 0:
                data['daily_return'] = None
                data['log_return'] = None
            else:
                prev_close = price_data[i-1]['close_price']
                curr_close = data['close_price']
                
                # Daily return
                daily_return = (curr_close - prev_close) / prev_close
                data['daily_return'] = float(daily_return)
                
                # Log return
                log_return = np.log(curr_close / prev_close)
                data['log_return'] = float(log_return)
        
        # Calculate rolling volatility
        for i, data in enumerate(price_data):
            # 5-day volatility
            if i >= 4:
                returns_5d = [price_data[j]['daily_return'] for j in range(i-4, i+1) 
                             if price_data[j]['daily_return'] is not None]
                if len(returns_5d) >= 3:
                    vol_5d = np.std(returns_5d) * np.sqrt(252)  # Annualized
                    data['volatility_5d'] = float(vol_5d)
            
            # 20-day volatility
            if i >= 19:
                returns_20d = [price_data[j]['daily_return'] for j in range(i-19, i+1) 
                              if price_data[j]['daily_return'] is not None]
                if len(returns_20d) >= 15:
                    vol_20d = np.std(returns_20d) * np.sqrt(252)  # Annualized
                    data['volatility_20d'] = float(vol_20d)
        
        return price_data
    
    async def store_price_data(
        self, 
        symbol: str, 
        price_data: List[Dict[str, Any]]
    ) -> int:
        """Store price data in database"""
        
        if not price_data:
            return 0
        
        insert_count = 0
        
        for data in price_data:
            try:
                await self.conn.execute("""
                    INSERT INTO daily_price_data (
                        symbol, date, open_price, high_price, low_price, 
                        close_price, adjusted_close, volume, daily_return, 
                        log_return, volatility_5d, volatility_20d, provider
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (symbol, date, provider) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        adjusted_close = EXCLUDED.adjusted_close,
                        volume = EXCLUDED.volume,
                        daily_return = EXCLUDED.daily_return,
                        log_return = EXCLUDED.log_return,
                        volatility_5d = EXCLUDED.volatility_5d,
                        volatility_20d = EXCLUDED.volatility_20d,
                        created_at = NOW()
                """, 
                    symbol,
                    data['date'],
                    Decimal(str(data['open_price'])),
                    Decimal(str(data['high_price'])),
                    Decimal(str(data['low_price'])),
                    Decimal(str(data['close_price'])),
                    Decimal(str(data['adjusted_close'])) if data['adjusted_close'] else None,
                    data['volume'],
                    Decimal(str(data['daily_return'])) if data['daily_return'] else None,
                    Decimal(str(data['log_return'])) if data['log_return'] else None,
                    Decimal(str(data['volatility_5d'])) if data.get('volatility_5d') else None,
                    Decimal(str(data['volatility_20d'])) if data.get('volatility_20d') else None,
                    'yahoo_finance'
                )
                insert_count += 1
                
            except Exception as e:
                logger.error(f"❌ Error inserting data for {symbol} on {data['date']}: {e}")
        
        return insert_count
    
    async def update_symbol_metadata(self, symbol: str):
        """Update symbol metadata after successful data fetch"""
        
        await self.conn.execute("""
            UPDATE market_data_symbols 
            SET last_data_fetch = NOW(),
                updated_at = NOW()
            WHERE symbol = $1
        """, symbol)
    
    async def calculate_performance_metrics(
        self, 
        symbol: str,
        timeframes: List[str] = ['1mo', '3mo', '6mo', '1yr']
    ):
        """Calculate and store performance metrics for different timeframes"""
        
        for timeframe in timeframes:
            # Define time period
            days_map = {'1mo': 30, '3mo': 90, '6mo': 180, '1yr': 365}
            days = days_map.get(timeframe, 30)
            
            start_date = date.today() - timedelta(days=days)
            end_date = date.today()
            
            # Get price data for period
            prices = await self.conn.fetch("""
                SELECT date, close_price 
                FROM daily_price_data 
                WHERE symbol = $1 
                  AND date >= $2 
                  AND date <= $3
                ORDER BY date ASC
            """, symbol, start_date, end_date)
            
            if len(prices) < 5:  # Need minimum data points
                continue
            
            # Calculate metrics
            start_price = float(prices[0]['close_price'])
            end_price = float(prices[-1]['close_price'])
            
            total_return = (end_price - start_price) / start_price
            annualized_return = total_return * (365 / days)
            
            # Calculate volatility from daily returns
            returns = []
            for i in range(1, len(prices)):
                prev_price = float(prices[i-1]['close_price'])
                curr_price = float(prices[i]['close_price'])
                daily_return = (curr_price - prev_price) / prev_price
                returns.append(daily_return)
            
            volatility = np.std(returns) * np.sqrt(252) if returns else 0.0
            
            # Max drawdown
            max_price = start_price
            max_drawdown = 0.0
            for price_row in prices:
                price = float(price_row['close_price'])
                if price > max_price:
                    max_price = price
                drawdown = (max_price - price) / max_price
                if drawdown > max_drawdown:
                    max_drawdown = drawdown
            
            # Sharpe ratio (assuming 2% risk-free rate)
            risk_free_rate = 0.02
            excess_return = annualized_return - risk_free_rate
            sharpe_ratio = excess_return / volatility if volatility > 0 else 0.0
            
            # Store metrics
            try:
                await self.conn.execute("""
                    INSERT INTO market_performance_metrics (
                        symbol, start_date, end_date, period_days, timeframe,
                        total_return, annualized_return, volatility, max_drawdown, sharpe_ratio,
                        start_price, end_price, trading_days
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (symbol, start_date, end_date, provider) DO UPDATE SET
                        total_return = EXCLUDED.total_return,
                        annualized_return = EXCLUDED.annualized_return,
                        volatility = EXCLUDED.volatility,
                        max_drawdown = EXCLUDED.max_drawdown,
                        sharpe_ratio = EXCLUDED.sharpe_ratio,
                        trading_days = EXCLUDED.trading_days,
                        calculated_at = NOW()
                """, 
                    symbol, start_date, end_date, days, timeframe,
                    Decimal(str(total_return)), Decimal(str(annualized_return)),
                    Decimal(str(volatility)), Decimal(str(max_drawdown)), Decimal(str(sharpe_ratio)),
                    Decimal(str(start_price)), Decimal(str(end_price)), len(prices)
                )
                
            except Exception as e:
                logger.error(f"❌ Error storing performance metrics for {symbol} {timeframe}: {e}")
    
    async def ingest_historical_data(
        self, 
        symbol: str,
        days_back: int = 730,  # 2 years default, but can be overridden
        calculate_metrics: bool = True
    ) -> bool:
        """Ingest historical data for a single symbol"""
        
        # Get symbol info
        symbol_info = await self.conn.fetchrow("""
            SELECT symbol, company_name, yahoo_symbol 
            FROM market_data_symbols 
            WHERE symbol = $1
        """, symbol)
        
        if not symbol_info:
            logger.error(f"❌ Symbol {symbol} not found in database")
            return False
        
        yahoo_symbol = symbol_info['yahoo_symbol']
        
        # Define date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        # Fetch data from Yahoo
        price_data = await self.fetch_yahoo_data(yahoo_symbol, start_date, end_date)
        
        if not price_data:
            logger.error(f"❌ No data retrieved for {symbol}")
            return False
        
        # Calculate technical indicators
        price_data = await self.calculate_technical_indicators(price_data)
        
        # Store in database
        insert_count = await self.store_price_data(symbol, price_data)
        logger.info(f"✅ Stored {insert_count} price points for {symbol}")
        
        # Update metadata
        await self.update_symbol_metadata(symbol)
        
        # Calculate performance metrics
        if calculate_metrics:
            await self.calculate_performance_metrics(symbol)
            logger.info(f"✅ Calculated performance metrics for {symbol}")
        
        return True
    
    async def ingest_all_symbols(
        self, 
        limit: Optional[int] = None, 
        delay_seconds: int = 1
    ) -> Dict[str, int]:
        """Ingest historical data for all symbols in database"""
        
        symbols = await self.get_symbols_to_update(limit)
        results = {'success': 0, 'failed': 0, 'total': len(symbols)}
        
        logger.info(f"🚀 Starting ingestion for {len(symbols)} symbols")
        
        for symbol_info in symbols:
            symbol = symbol_info['symbol']
            
            try:
                success = await self.ingest_historical_data(symbol)
                if success:
                    results['success'] += 1
                    logger.info(f"✅ Completed {symbol} ({results['success']}/{len(symbols)})")
                else:
                    results['failed'] += 1
                    logger.error(f"❌ Failed {symbol}")
                
                # Rate limiting
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)
                    
            except Exception as e:
                results['failed'] += 1
                logger.error(f"❌ Exception for {symbol}: {e}")
        
        logger.info(f"🎉 Ingestion complete! Success: {results['success']}, Failed: {results['failed']}")
        return results

# CLI functions
async def run_single_symbol(symbol: str, days_back: Optional[int] = None):
    """CLI command to ingest single symbol"""
    ingestor = HistoricalDataIngestor()
    try:
        await ingestor.connect()
        
        # Use provided days_back or default
        if days_back:
            print(f"📅 Fetching {days_back} days of history for {symbol}")
            success = await ingestor.ingest_historical_data(symbol, days_back=days_back)
        else:
            success = await ingestor.ingest_historical_data(symbol)
            
        if success:
            print(f"✅ Successfully ingested data for {symbol}")
        else:
            print(f"❌ Failed to ingest data for {symbol}")
    finally:
        await ingestor.disconnect()

async def run_all_symbols(limit: Optional[int] = None):
    """CLI command to ingest all symbols"""
    ingestor = HistoricalDataIngestor()
    try:
        await ingestor.connect()
        results = await ingestor.ingest_all_symbols(limit, delay_seconds=2)
        print(f"\n🎉 Ingestion Results:")
        print(f"   Success: {results['success']}")
        print(f"   Failed: {results['failed']}")
        print(f"   Total: {results['total']}")
    finally:
        await ingestor.disconnect()

async def show_status():
    """Show market data status"""
    ingestor = HistoricalDataIngestor()
    try:
        await ingestor.connect()
        
        # Symbol count
        symbol_count = await ingestor.conn.fetchval("SELECT COUNT(*) FROM market_data_symbols")
        
        # Data count
        data_count = await ingestor.conn.fetchval("SELECT COUNT(*) FROM daily_price_data")
        
        # Latest data
        latest = await ingestor.conn.fetch("""
            SELECT symbol, MAX(date) as latest_date, COUNT(*) as price_points
            FROM daily_price_data 
            GROUP BY symbol 
            ORDER BY latest_date DESC
            LIMIT 5
        """)
        
        print("📊 Market Data Status")
        print("=" * 25)
        print(f"Symbols: {symbol_count}")
        print(f"Price Points: {data_count}")
        print(f"\nLatest Data:")
        for row in latest:
            print(f"  {row['symbol']}: {row['latest_date']} ({row['price_points']} points)")
            
    finally:
        await ingestor.disconnect()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python historical_data_ingestor.py status")
        print("  python historical_data_ingestor.py ingest <symbol> [days_back]")
        print("  python historical_data_ingestor.py ingest-all [limit]")
        print("\nExamples:")
        print("  python historical_data_ingestor.py ingest AAK          # Default 2 years")
        print("  python historical_data_ingestor.py ingest AAK 365      # 1 year")
        print("  python historical_data_ingestor.py ingest AAK 3650     # 10 years")
        print("  python historical_data_ingestor.py ingest AAK 7300     # 20 years")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "status":
        asyncio.run(show_status())
    elif command == "ingest" and len(sys.argv) > 2:
        symbol = sys.argv[2]
        days_back = int(sys.argv[3]) if len(sys.argv) > 3 else None
        asyncio.run(run_single_symbol(symbol, days_back))
    elif command == "ingest-all":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
        asyncio.run(run_all_symbols(limit))
    else:
        print("Invalid command. Use 'status', 'ingest <symbol> [days]', or 'ingest-all [limit]'")