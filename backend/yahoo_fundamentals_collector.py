#!/usr/bin/env python3
"""
Yahoo Finance Fundamentals Collector

Collects fundamental data needed for value investing strategies:
- Book value per share
- Earnings per share  
- Return on equity
- Free cash flow
- Current assets
- Total liabilities
"""

import asyncio
import asyncpg
from datetime import datetime, timedelta
import yfinance as yf
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class YahooFundamentalsCollector:
    """Collects fundamental data from Yahoo Finance"""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        
    async def collect_fundamentals(self, symbol: str) -> bool:
        """Collect fundamental data for a symbol"""
        try:
            # Get stock info from Yahoo
            stock = yf.Ticker(symbol)
            
            # Get financial statements
            balance_sheet = stock.quarterly_balance_sheet
            income_stmt = stock.quarterly_income_stmt
            cashflow = stock.quarterly_cashflow
            info = stock.info
            
            # Get shares outstanding (most recent)
            shares_outstanding = info.get('sharesOutstanding', info.get('impliedSharesOutstanding'))
            
            # Process quarterly data
            success = await self._process_quarterly_data(
                symbol, balance_sheet, income_stmt, cashflow, shares_outstanding
            )
            
            # Also get annual data
            balance_sheet_annual = stock.balance_sheet
            income_stmt_annual = stock.income_stmt
            cashflow_annual = stock.cashflow
            
            success_annual = await self._process_annual_data(
                symbol, balance_sheet_annual, income_stmt_annual, 
                cashflow_annual, shares_outstanding
            )
            
            return success and success_annual
            
        except Exception as e:
            logger.error(f"Error collecting fundamentals for {symbol}: {e}")
            return False
    
    async def _process_quarterly_data(self, symbol: str, balance_sheet, income_stmt, 
                                     cashflow, shares_outstanding: Optional[float]) -> bool:
        """Process quarterly financial data"""
        try:
            if balance_sheet is None or balance_sheet.empty:
                return False
                
            for date in balance_sheet.columns[:4]:  # Last 4 quarters
                period_date = date.to_pydatetime().date()
                
                # Extract metrics from balance sheet
                total_assets = self._safe_get(balance_sheet, 'Total Assets', date)
                current_assets = self._safe_get(balance_sheet, 'Current Assets', date)
                total_liabilities = self._safe_get(balance_sheet, 'Total Liabilities Net Minority Interest', date)
                total_equity = self._safe_get(balance_sheet, 'Total Equity Gross Minority Interest', date)
                
                # Extract from income statement
                net_income = None
                if income_stmt is not None and not income_stmt.empty and date in income_stmt.columns:
                    net_income = self._safe_get(income_stmt, 'Net Income', date)
                
                # Extract from cash flow
                free_cash_flow = None
                if cashflow is not None and not cashflow.empty and date in cashflow.columns:
                    operating_cf = self._safe_get(cashflow, 'Operating Cash Flow', date)
                    capex = self._safe_get(cashflow, 'Capital Expenditure', date)
                    if operating_cf is not None and capex is not None:
                        free_cash_flow = operating_cf - abs(capex)
                
                # Calculate per-share metrics
                if shares_outstanding and shares_outstanding > 0:
                    # Book value per share
                    if total_equity is not None:
                        await self._store_metric(
                            symbol, 'book_value_per_share', 
                            total_equity / shares_outstanding, 
                            'quarterly', period_date
                        )
                    
                    # Earnings per share
                    if net_income is not None:
                        await self._store_metric(
                            symbol, 'earnings_per_share',
                            net_income / shares_outstanding,
                            'quarterly', period_date
                        )
                    
                    # Free cash flow per share
                    if free_cash_flow is not None:
                        await self._store_metric(
                            symbol, 'free_cash_flow_per_share',
                            free_cash_flow / shares_outstanding,
                            'quarterly', period_date
                        )
                    
                    # Current assets per share
                    if current_assets is not None:
                        await self._store_metric(
                            symbol, 'current_assets_per_share',
                            current_assets / shares_outstanding,
                            'quarterly', period_date
                        )
                    
                    # Total liabilities per share
                    if total_liabilities is not None:
                        await self._store_metric(
                            symbol, 'total_liabilities_per_share',
                            total_liabilities / shares_outstanding,
                            'quarterly', period_date
                        )
                
                # Return on equity (percentage)
                if total_equity and total_equity > 0 and net_income is not None:
                    roe = (net_income / total_equity) * 100
                    await self._store_metric(
                        symbol, 'return_on_equity',
                        roe, 'quarterly', period_date
                    )
                
                # Store shares outstanding
                if shares_outstanding:
                    await self._store_metric(
                        symbol, 'shares_outstanding',
                        shares_outstanding, 'quarterly', period_date
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing quarterly data for {symbol}: {e}")
            return False
    
    async def _process_annual_data(self, symbol: str, balance_sheet, income_stmt, 
                                  cashflow, shares_outstanding: Optional[float]) -> bool:
        """Process annual financial data"""
        try:
            if balance_sheet is None or balance_sheet.empty:
                return False
                
            for date in balance_sheet.columns[:2]:  # Last 2 years
                period_date = date.to_pydatetime().date()
                
                # Extract metrics from balance sheet
                total_assets = self._safe_get(balance_sheet, 'Total Assets', date)
                current_assets = self._safe_get(balance_sheet, 'Current Assets', date)
                total_liabilities = self._safe_get(balance_sheet, 'Total Liabilities Net Minority Interest', date)
                total_equity = self._safe_get(balance_sheet, 'Total Equity Gross Minority Interest', date)
                
                # Extract from income statement
                net_income = None
                if income_stmt is not None and not income_stmt.empty and date in income_stmt.columns:
                    net_income = self._safe_get(income_stmt, 'Net Income', date)
                
                # Extract from cash flow
                free_cash_flow = None
                if cashflow is not None and not cashflow.empty and date in cashflow.columns:
                    operating_cf = self._safe_get(cashflow, 'Operating Cash Flow', date)
                    capex = self._safe_get(cashflow, 'Capital Expenditure', date)
                    if operating_cf is not None and capex is not None:
                        free_cash_flow = operating_cf - abs(capex)
                
                # Calculate per-share metrics
                if shares_outstanding and shares_outstanding > 0:
                    # Book value per share
                    if total_equity is not None:
                        await self._store_metric(
                            symbol, 'book_value_per_share', 
                            total_equity / shares_outstanding, 
                            'annual', period_date
                        )
                    
                    # Earnings per share
                    if net_income is not None:
                        await self._store_metric(
                            symbol, 'earnings_per_share',
                            net_income / shares_outstanding,
                            'annual', period_date
                        )
                    
                    # Free cash flow per share
                    if free_cash_flow is not None:
                        await self._store_metric(
                            symbol, 'free_cash_flow_per_share',
                            free_cash_flow / shares_outstanding,
                            'annual', period_date
                        )
                    
                    # Current assets per share
                    if current_assets is not None:
                        await self._store_metric(
                            symbol, 'current_assets_per_share',
                            current_assets / shares_outstanding,
                            'annual', period_date
                        )
                    
                    # Total liabilities per share
                    if total_liabilities is not None:
                        await self._store_metric(
                            symbol, 'total_liabilities_per_share',
                            total_liabilities / shares_outstanding,
                            'annual', period_date
                        )
                
                # Return on equity (percentage)
                if total_equity and total_equity > 0 and net_income is not None:
                    roe = (net_income / total_equity) * 100
                    await self._store_metric(
                        symbol, 'return_on_equity',
                        roe, 'annual', period_date
                    )
                
                # Store shares outstanding
                if shares_outstanding:
                    await self._store_metric(
                        symbol, 'shares_outstanding',
                        shares_outstanding, 'annual', period_date
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing annual data for {symbol}: {e}")
            return False
    
    def _safe_get(self, df, key: str, date) -> Optional[float]:
        """Safely get value from DataFrame"""
        try:
            if key in df.index:
                value = df.loc[key, date]
                if value is not None and not pd.isna(value):
                    return float(value)
            return None
        except:
            return None
    
    async def _store_metric(self, symbol: str, metric: str, value: float, 
                           period_type: str, period_date) -> None:
        """Store a fundamental metric in the database"""
        try:
            await self.conn.execute("""
                INSERT INTO fundamentals (symbol, metric, value, period_type, period_end_date)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (symbol, metric, period_type, period_end_date)
                DO UPDATE SET value = $3, updated_at = CURRENT_TIMESTAMP
            """, symbol, metric, value, period_type, period_date)
        except Exception as e:
            logger.error(f"Error storing metric {metric} for {symbol}: {e}")
    
    async def collect_batch(self, symbols: List[str]) -> Dict[str, bool]:
        """Collect fundamentals for multiple symbols"""
        results = {}
        
        for symbol in symbols:
            logger.info(f"Collecting fundamentals for {symbol}")
            success = await self.collect_fundamentals(symbol)
            results[symbol] = success
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(1)
        
        return results


async def main():
    """Test the collector with sample symbols"""
    import pandas as pd
    
    # Connect to database
    conn = await asyncpg.connect(
        host='localhost',
        port=5432,
        user='yodabuffett',
        password='password',
        database='yodabuffett'
    )
    
    try:
        collector = YahooFundamentalsCollector(conn)
        
        # Test with a few symbols
        symbols = ['AAPL', 'MSFT', 'JNJ', 'BRK-B', 'GOOGL']
        
        logger.info(f"Collecting fundamentals for {len(symbols)} symbols")
        results = await collector.collect_batch(symbols)
        
        # Show results
        successful = sum(1 for v in results.values() if v)
        print(f"\nCollection Results:")
        print(f"Successful: {successful}/{len(symbols)}")
        
        for symbol, success in results.items():
            print(f"{symbol}: {'✓' if success else '✗'}")
        
        # Query and show some data
        print("\nSample data from database:")
        rows = await conn.fetch("""
            SELECT symbol, metric, value, period_type, period_end_date
            FROM fundamentals
            WHERE metric IN ('book_value_per_share', 'earnings_per_share', 'return_on_equity')
            ORDER BY symbol, period_end_date DESC
            LIMIT 10
        """)
        
        for row in rows:
            print(f"{row['symbol']} - {row['metric']}: {row['value']:.2f} ({row['period_type']} {row['period_end_date']})")
            
    finally:
        await conn.close()


if __name__ == "__main__":
    # Need pandas for yfinance
    import pandas as pd
    asyncio.run(main())