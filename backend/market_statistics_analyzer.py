#!/usr/bin/env python3
"""
Market Statistics Analyzer

Comprehensive statistics and baselines from the YodaBuffett database:
- Market-wide fundamentals analysis
- Company-specific statistics
- Historical trends and patterns
- Sector comparisons
- Risk metrics and correlations
- Trading statistics and volumes
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import json
import logging
from dataclasses import dataclass
from collections import defaultdict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class MarketStatistics:
    """Container for market-wide statistics."""
    metric_name: str
    current_value: float
    mean: float
    median: float
    std: float
    min_value: float
    max_value: float
    percentile_25: float
    percentile_75: float
    count: int

@dataclass
class CompanyProfile:
    """Container for company-specific profile."""
    symbol: str
    company_name: str
    market_cap: Optional[float]
    sector: Optional[str]
    current_price: Optional[float]
    pe_ratio: Optional[float]
    pb_ratio: Optional[float]
    dividend_yield: Optional[float]
    return_on_equity: Optional[float]
    debt_to_equity: Optional[float]
    revenue_growth: Optional[float]
    volatility: Optional[float]
    beta: Optional[float]

class MarketStatisticsAnalyzer:
    """Comprehensive market statistics and analytics engine."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_market_overview(self) -> Dict:
        """Get comprehensive market overview statistics."""
        logger.info("🏛️ Generating market overview...")
        
        # Get basic market metrics
        query = """
        SELECT 
            COUNT(DISTINCT symbol) as total_companies,
            SUM(market_cap::BIGINT) as total_market_cap,
            AVG(trailing_pe) as avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trailing_pe) as median_pe,
            AVG(price_to_book) as avg_pb,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_to_book) as median_pb,
            AVG(dividend_yield) as avg_dividend_yield,
            AVG(return_on_equity) as avg_roe,
            AVG(total_debt_to_equity) as avg_debt_equity,
            COUNT(*) as total_records
        FROM daily_fundamentals df
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        AND trailing_pe > 0 AND trailing_pe < 100  -- Filter outliers
        AND price_to_book > 0 AND price_to_book < 20
        """
        
        market_overview = await self.db_conn.fetchrow(query)
        
        # Get price statistics
        price_query = """
        SELECT 
            AVG(close_price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close_price) as median_price,
            AVG(volume) as avg_volume,
            SUM(volume * close_price) as total_dollar_volume
        FROM daily_price_data 
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        AND close_price > 0
        """
        
        price_stats = await self.db_conn.fetchrow(price_query)
        
        # Get historical fundamentals coverage
        historical_query = """
        SELECT 
            COUNT(DISTINCT symbol) as companies_with_history,
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(*) as total_historical_records
        FROM historical_fundamentals_daily
        WHERE market_cap IS NOT NULL
        """
        
        historical_stats = await self.db_conn.fetchrow(historical_query)
        
        return {
            'market_overview': dict(market_overview) if market_overview else {},
            'price_statistics': dict(price_stats) if price_stats else {},
            'historical_coverage': dict(historical_stats) if historical_stats else {}
        }
        
    async def get_fundamental_distributions(self) -> Dict[str, MarketStatistics]:
        """Get statistical distributions for key fundamental metrics."""
        logger.info("📊 Analyzing fundamental metric distributions...")
        
        metrics = {
            'trailing_pe': 'P/E Ratio',
            'price_to_book': 'P/B Ratio', 
            'price_to_sales': 'P/S Ratio',
            'dividend_yield': 'Dividend Yield',
            'return_on_equity': 'Return on Equity',
            'return_on_assets': 'Return on Assets',
            'total_debt_to_equity': 'Debt-to-Equity',
            'current_ratio': 'Current Ratio',
            'profit_margin': 'Profit Margin',
            'operating_margin': 'Operating Margin',
            'ev_to_ebitda': 'EV/EBITDA'
        }
        
        distributions = {}
        
        for metric_col, metric_name in metrics.items():
            query = f"""
            SELECT 
                {metric_col} as value
            FROM daily_fundamentals 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            AND {metric_col} IS NOT NULL
            AND {metric_col} > 0
            AND {metric_col} < CASE 
                WHEN '{metric_col}' = 'trailing_pe' THEN 100
                WHEN '{metric_col}' = 'price_to_book' THEN 20
                WHEN '{metric_col}' = 'price_to_sales' THEN 50
                WHEN '{metric_col}' = 'ev_to_ebitda' THEN 100
                WHEN '{metric_col}' = 'total_debt_to_equity' THEN 10
                ELSE 999999
            END
            """
            
            values = await self.db_conn.fetch(query)
            
            if values:
                data = [row['value'] for row in values]
                distributions[metric_name] = MarketStatistics(
                    metric_name=metric_name,
                    current_value=data[-1] if data else 0,
                    mean=np.mean(data),
                    median=np.median(data),
                    std=np.std(data),
                    min_value=np.min(data),
                    max_value=np.max(data),
                    percentile_25=np.percentile(data, 25),
                    percentile_75=np.percentile(data, 75),
                    count=len(data)
                )
                
        return distributions
        
    async def get_company_rankings(self, metric: str, limit: int = 20) -> List[Dict]:
        """Get top/bottom companies by specific metric."""
        logger.info(f"🏆 Getting company rankings by {metric}...")
        
        # Map user-friendly names to database columns
        metric_mapping = {
            'market_cap': ('market_cap', 'DESC'),
            'pe_ratio': ('trailing_pe', 'ASC'),
            'pb_ratio': ('price_to_book', 'ASC'),
            'dividend_yield': ('dividend_yield', 'DESC'),
            'roe': ('return_on_equity', 'DESC'),
            'debt_equity': ('total_debt_to_equity', 'ASC'),
            'profit_margin': ('profit_margin', 'DESC'),
            'revenue_growth': ('quarterly_revenue_growth', 'DESC')
        }
        
        if metric not in metric_mapping:
            raise ValueError(f"Unknown metric: {metric}")
            
        db_column, sort_order = metric_mapping[metric]
        
        query = f"""
        SELECT 
            df.symbol,
            cm.company_name,
            df.{db_column} as metric_value,
            df.market_cap,
            df.trailing_pe,
            df.price_to_book,
            df.dividend_yield,
            df.return_on_equity,
            dpd.close_price as current_price
        FROM daily_fundamentals df
        LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
        LEFT JOIN daily_price_data dpd ON df.symbol = dpd.symbol 
            AND dpd.date = (SELECT MAX(date) FROM daily_price_data WHERE symbol = df.symbol)
        WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
        AND df.{db_column} IS NOT NULL
        AND df.{db_column} > 0
        ORDER BY df.{db_column} {sort_order}
        LIMIT {limit}
        """
        
        results = await self.db_conn.fetch(query)
        return [dict(row) for row in results]
        
    async def get_sector_analysis(self) -> Dict[str, Dict]:
        """Analyze fundamentals by sector/industry."""
        logger.info("🏭 Performing sector analysis...")
        
        query = """
        SELECT 
            COALESCE(cm.industry, 'Unknown') as sector,
            COUNT(DISTINCT df.symbol) as company_count,
            AVG(df.market_cap::BIGINT) as avg_market_cap,
            AVG(df.trailing_pe) as avg_pe,
            AVG(df.price_to_book) as avg_pb,
            AVG(df.dividend_yield) as avg_dividend_yield,
            AVG(df.return_on_equity) as avg_roe,
            AVG(df.total_debt_to_equity) as avg_debt_equity,
            AVG(df.profit_margin) as avg_profit_margin,
            AVG(dpd.close_price) as avg_price
        FROM daily_fundamentals df
        LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
        LEFT JOIN daily_price_data dpd ON df.symbol = dpd.symbol 
            AND dpd.date = (SELECT MAX(date) FROM daily_price_data WHERE symbol = df.symbol)
        WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
        AND df.trailing_pe > 0 AND df.trailing_pe < 100
        GROUP BY COALESCE(cm.industry, 'Unknown')
        HAVING COUNT(DISTINCT df.symbol) >= 5  -- Only sectors with 5+ companies
        ORDER BY AVG(df.market_cap::BIGINT) DESC
        """
        
        sectors = await self.db_conn.fetch(query)
        
        sector_analysis = {}
        for sector in sectors:
            sector_name = sector['sector']
            sector_analysis[sector_name] = dict(sector)
            
        return sector_analysis
        
    async def get_historical_trends(self, days: int = 365) -> Dict:
        """Analyze historical trends in market fundamentals."""
        logger.info(f"📈 Analyzing {days}-day historical trends...")
        
        start_date = date.today() - timedelta(days=days)
        
        query = """
        SELECT 
            date,
            AVG(pe_ratio) as avg_pe,
            AVG(pb_ratio) as avg_pb,
            AVG(market_cap::BIGINT) as avg_market_cap,
            COUNT(DISTINCT symbol) as company_count
        FROM historical_fundamentals_daily
        WHERE date >= $1
        AND pe_ratio > 0 AND pe_ratio < 100
        AND pb_ratio > 0 AND pb_ratio < 20
        GROUP BY date
        ORDER BY date
        """
        
        historical = await self.db_conn.fetch(query, start_date)
        
        # Convert to DataFrame for analysis
        df = pd.DataFrame([dict(row) for row in historical])
        
        if df.empty:
            return {}
            
        trends = {
            'date_range': {
                'start': df['date'].min(),
                'end': df['date'].max(),
                'days': len(df)
            },
            'pe_trends': {
                'start_pe': float(df['avg_pe'].iloc[0]) if not pd.isna(df['avg_pe'].iloc[0]) else None,
                'end_pe': float(df['avg_pe'].iloc[-1]) if not pd.isna(df['avg_pe'].iloc[-1]) else None,
                'pe_change_pct': ((df['avg_pe'].iloc[-1] - df['avg_pe'].iloc[0]) / df['avg_pe'].iloc[0] * 100) if not pd.isna(df['avg_pe'].iloc[0]) else None,
                'pe_volatility': float(df['avg_pe'].std()) if not df['avg_pe'].isna().all() else None
            },
            'pb_trends': {
                'start_pb': float(df['avg_pb'].iloc[0]) if not pd.isna(df['avg_pb'].iloc[0]) else None,
                'end_pb': float(df['avg_pb'].iloc[-1]) if not pd.isna(df['avg_pb'].iloc[-1]) else None,
                'pb_change_pct': ((df['avg_pb'].iloc[-1] - df['avg_pb'].iloc[0]) / df['avg_pb'].iloc[0] * 100) if not pd.isna(df['avg_pb'].iloc[0]) else None
            },
            'market_cap_trends': {
                'start_cap': float(df['avg_market_cap'].iloc[0]) if not pd.isna(df['avg_market_cap'].iloc[0]) else None,
                'end_cap': float(df['avg_market_cap'].iloc[-1]) if not pd.isna(df['avg_market_cap'].iloc[-1]) else None,
                'cap_change_pct': ((df['avg_market_cap'].iloc[-1] - df['avg_market_cap'].iloc[0]) / df['avg_market_cap'].iloc[0] * 100) if not pd.isna(df['avg_market_cap'].iloc[0]) else None
            }
        }
        
        return trends
        
    async def get_company_profiles(self, symbols: List[str] = None, limit: int = 50) -> List[CompanyProfile]:
        """Get detailed profiles for specific companies or top companies."""
        logger.info(f"👥 Generating company profiles...")
        
        if symbols:
            symbol_filter = "AND df.symbol = ANY($1)"
            params = [symbols]
        else:
            symbol_filter = ""
            params = []
            
        query = f"""
        SELECT 
            df.symbol,
            cm.company_name,
            cm.industry as sector,
            df.market_cap,
            dpd.close_price as current_price,
            df.trailing_pe as pe_ratio,
            df.price_to_book as pb_ratio,
            df.dividend_yield,
            df.return_on_equity,
            df.total_debt_to_equity as debt_to_equity,
            df.quarterly_revenue_growth as revenue_growth
        FROM daily_fundamentals df
        LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
        LEFT JOIN daily_price_data dpd ON df.symbol = dpd.symbol 
            AND dpd.date = (SELECT MAX(date) FROM daily_price_data WHERE symbol = df.symbol)
        WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
        {symbol_filter}
        ORDER BY df.market_cap DESC
        LIMIT {limit}
        """
        
        results = await self.db_conn.fetch(query, *params)
        
        profiles = []
        for row in results:
            # Calculate volatility (30-day)
            volatility = await self._calculate_volatility(row['symbol'], 30)
            
            profiles.append(CompanyProfile(
                symbol=row['symbol'],
                company_name=row['company_name'],
                sector=row['sector'],
                market_cap=row['market_cap'],
                current_price=row['current_price'],
                pe_ratio=row['pe_ratio'],
                pb_ratio=row['pb_ratio'],
                dividend_yield=row['dividend_yield'],
                return_on_equity=row['return_on_equity'],
                debt_to_equity=row['debt_to_equity'],
                revenue_growth=row['revenue_growth'],
                volatility=volatility,
                beta=None  # Calculate if needed
            ))
            
        return profiles
        
    async def _calculate_volatility(self, symbol: str, days: int) -> Optional[float]:
        """Calculate price volatility for a symbol."""
        query = """
        SELECT close_price, LAG(close_price) OVER (ORDER BY date) as prev_price
        FROM daily_price_data 
        WHERE symbol = $1 
        AND date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY date
        """ % days
        
        prices = await self.db_conn.fetch(query, symbol)
        
        if len(prices) < 10:  # Need minimum data points
            return None
            
        returns = []
        for row in prices[1:]:  # Skip first row (no previous price)
            if row['prev_price'] and row['prev_price'] > 0:
                daily_return = (row['close_price'] - row['prev_price']) / row['prev_price']
                returns.append(daily_return)
                
        if len(returns) < 5:
            return None
            
        # Annualized volatility
        return float(np.std(returns)) * np.sqrt(252)
        
    async def get_price_statistics(self) -> Dict:
        """Get comprehensive price and trading statistics."""
        logger.info("💰 Analyzing price and trading statistics...")
        
        query = """
        SELECT 
            COUNT(DISTINCT symbol) as symbols_traded,
            AVG(close_price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close_price) as median_price,
            AVG(volume) as avg_volume,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume) as median_volume,
            SUM(volume * close_price) as total_dollar_volume,
            AVG((high_price - low_price) / close_price) as avg_daily_range_pct,
            COUNT(*) as total_trading_records
        FROM daily_price_data 
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        AND close_price > 0
        AND volume > 0
        """
        
        price_stats = await self.db_conn.fetchrow(query)
        
        # Get price distribution by ranges
        price_ranges_query = """
        SELECT 
            CASE 
                WHEN close_price < 10 THEN '<10 SEK'
                WHEN close_price < 50 THEN '10-50 SEK'
                WHEN close_price < 100 THEN '50-100 SEK'
                WHEN close_price < 500 THEN '100-500 SEK'
                ELSE '>500 SEK'
            END as price_range,
            COUNT(DISTINCT symbol) as company_count,
            AVG(volume) as avg_volume_in_range
        FROM daily_price_data 
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY price_range
        ORDER BY MIN(close_price)
        """
        
        price_ranges = await self.db_conn.fetch(price_ranges_query)
        
        return {
            'overall_stats': dict(price_stats) if price_stats else {},
            'price_ranges': [dict(row) for row in price_ranges]
        }
        
    async def generate_report(self, save_to_file: bool = True) -> Dict:
        """Generate comprehensive market statistics report."""
        logger.info("📋 Generating comprehensive market statistics report...")
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'market_overview': await self.get_market_overview(),
            'fundamental_distributions': {},
            'sector_analysis': await self.get_sector_analysis(),
            'historical_trends': await self.get_historical_trends(365),
            'price_statistics': await self.get_price_statistics(),
            'company_profiles': []
        }
        
        # Convert MarketStatistics objects to dicts
        distributions = await self.get_fundamental_distributions()
        for metric_name, stats in distributions.items():
            report['fundamental_distributions'][metric_name] = {
                'mean': stats.mean,
                'median': stats.median,
                'std': stats.std,
                'min': stats.min_value,
                'max': stats.max_value,
                'percentile_25': stats.percentile_25,
                'percentile_75': stats.percentile_75,
                'count': stats.count
            }
            
        # Get company profiles (top 30 by market cap)
        profiles = await self.get_company_profiles(limit=30)
        for profile in profiles:
            report['company_profiles'].append({
                'symbol': profile.symbol,
                'company_name': profile.company_name,
                'sector': profile.sector,
                'market_cap': profile.market_cap,
                'current_price': profile.current_price,
                'pe_ratio': profile.pe_ratio,
                'pb_ratio': profile.pb_ratio,
                'dividend_yield': profile.dividend_yield,
                'return_on_equity': profile.return_on_equity,
                'debt_to_equity': profile.debt_to_equity,
                'revenue_growth': profile.revenue_growth,
                'volatility': profile.volatility
            })
            
        if save_to_file:
            filename = f"market_statistics_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"📁 Report saved to {filename}")
            
        return report
        
    async def print_summary_report(self):
        """Print a formatted summary to console."""
        logger.info("📊 Generating market summary report...")
        
        overview = await self.get_market_overview()
        distributions = await self.get_fundamental_distributions()
        sectors = await self.get_sector_analysis()
        
        print("=" * 80)
        print("🏛️  YODABUFFETT MARKET STATISTICS SUMMARY")
        print("=" * 80)
        
        # Market Overview
        market = overview.get('market_overview', {})
        if market:
            print(f"\n📊 MARKET OVERVIEW:")
            print(f"   Total Companies: {market.get('total_companies', 0):,}")
            print(f"   Total Market Cap: ${market.get('total_market_cap', 0):,.0f}")
            print(f"   Average P/E Ratio: {market.get('avg_pe', 0):.1f}")
            print(f"   Median P/E Ratio: {market.get('median_pe', 0):.1f}")
            print(f"   Average P/B Ratio: {market.get('avg_pb', 0):.1f}")
            print(f"   Average Dividend Yield: {market.get('avg_dividend_yield', 0):.2f}")
            print(f"   Average ROE: {market.get('avg_roe', 0):.1%}")
            
        # Key Statistics
        print(f"\n📈 KEY FUNDAMENTAL STATISTICS:")
        key_metrics = ['P/E Ratio', 'P/B Ratio', 'Return on Equity', 'Dividend Yield']
        for metric in key_metrics:
            if metric in distributions:
                stats = distributions[metric]
                print(f"   {metric}:")
                print(f"     Mean: {stats.mean:.2f} | Median: {stats.median:.2f}")
                print(f"     25th-75th percentile: {stats.percentile_25:.2f} - {stats.percentile_75:.2f}")
                print(f"     Range: {stats.min_value:.2f} - {stats.max_value:.2f} | Count: {stats.count}")
                
        # Top Sectors
        print(f"\n🏭 TOP SECTORS BY AVERAGE MARKET CAP:")
        for i, (sector_name, sector_data) in enumerate(list(sectors.items())[:5]):
            print(f"   {i+1}. {sector_name}")
            print(f"      Companies: {sector_data['company_count']} | Avg Market Cap: ${sector_data['avg_market_cap']:,.0f}")
            print(f"      Avg P/E: {sector_data['avg_pe']:.1f} | Avg ROE: {sector_data['avg_roe']:.1%}")
            
        # Top Companies by Market Cap
        top_companies = await self.get_company_rankings('market_cap', 10)
        print(f"\n🏆 TOP 10 COMPANIES BY MARKET CAP:")
        for i, company in enumerate(top_companies):
            company_name = company.get('company_name', 'Unknown') or 'Unknown'
            market_cap = company.get('market_cap') or company.get('metric_value', 0)
            print(f"   {i+1:2d}. {company['symbol']:<8} ({company_name:<30}) ${market_cap:>12,.0f}")
            
        # Best Value Stocks (Low P/E)
        value_stocks = await self.get_company_rankings('pe_ratio', 10)
        print(f"\n💎 BEST VALUE STOCKS (LOWEST P/E):")
        for i, company in enumerate(value_stocks):
            company_name = company.get('company_name', 'Unknown') or 'Unknown'
            pe_ratio = company.get('metric_value', 0)
            print(f"   {i+1:2d}. {company['symbol']:<8} P/E: {pe_ratio:<6.1f} ({company_name:<25})")
            
        # High Dividend Yield
        dividend_stocks = await self.get_company_rankings('dividend_yield', 10)
        print(f"\n💰 HIGHEST DIVIDEND YIELDS:")
        for i, company in enumerate(dividend_stocks):
            if company.get('metric_value'):
                company_name = company.get('company_name', 'Unknown') or 'Unknown'
                dividend_yield = company.get('metric_value', 0)
                print(f"   {i+1:2d}. {company['symbol']:<8} Yield: {dividend_yield:<6.2f} ({company_name:<25})")
                
        print("\n" + "=" * 80)
        print("Report generated successfully! 🎉")
        print("=" * 80)
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run comprehensive market statistics analysis."""
    
    analyzer = MarketStatisticsAnalyzer()
    
    try:
        await analyzer.setup()
        
        print("🚀 Starting YodaBuffett Market Statistics Analysis")
        print("=" * 60)
        
        # Generate and display summary
        await analyzer.print_summary_report()
        
        # Generate full report
        print("\n📋 Generating detailed JSON report...")
        full_report = await analyzer.generate_report(save_to_file=True)
        
        print(f"\n✅ Analysis complete!")
        print(f"   📊 Analyzed {full_report['market_overview']['market_overview'].get('total_companies', 0)} companies")
        print(f"   📈 {len(full_report['fundamental_distributions'])} fundamental metrics")
        print(f"   🏭 {len(full_report['sector_analysis'])} sectors analyzed")
        print(f"   👥 {len(full_report['company_profiles'])} company profiles generated")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())