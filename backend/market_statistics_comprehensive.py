#!/usr/bin/env python3
"""
Market Statistics Analyzer - Comprehensive Version

Uses historical_fundamentals_daily table which has much better coverage.
Provides comprehensive market statistics and baselines.
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

class MarketStatisticsComprehensive:
    """Comprehensive market statistics using historical fundamentals data."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_market_overview(self) -> Dict:
        """Get comprehensive market overview using historical data."""
        logger.info("🏛️ Generating comprehensive market overview...")
        
        # Get the most recent date with good coverage
        recent_date_query = """
        SELECT date, COUNT(DISTINCT symbol) as company_count
        FROM historical_fundamentals_daily
        WHERE pe_ratio IS NOT NULL AND market_cap IS NOT NULL
        GROUP BY date
        HAVING COUNT(DISTINCT symbol) > 100
        ORDER BY date DESC
        LIMIT 1
        """
        
        recent_date_result = await self.db_conn.fetchrow(recent_date_query)
        
        if not recent_date_result:
            logger.warning("No recent date with sufficient coverage found")
            return {}
            
        analysis_date = recent_date_result['date']
        logger.info(f"Using data from {analysis_date} with {recent_date_result['company_count']} companies")
        
        # Get market metrics for that date
        query = """
        SELECT 
            COUNT(DISTINCT symbol) as total_companies,
            SUM(market_cap) as total_market_cap,
            AVG(pe_ratio) as avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio) as median_pe,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pe_ratio) as pe_25th,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pe_ratio) as pe_75th,
            AVG(pb_ratio) as avg_pb,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pb_ratio) as median_pb,
            AVG(debt_to_equity) as avg_debt_equity,
            AVG(current_ratio) as avg_current_ratio,
            COUNT(*) as total_records
        FROM historical_fundamentals_daily
        WHERE date = $1
        AND pe_ratio > 0 AND pe_ratio < 100
        AND pb_ratio > 0 AND pb_ratio < 20
        """
        
        market_overview = await self.db_conn.fetchrow(query, analysis_date)
        
        # Get price data statistics
        price_query = """
        SELECT 
            COUNT(DISTINCT symbol) as symbols_traded,
            AVG(close_price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close_price) as median_price,
            AVG(volume) as avg_volume,
            SUM(volume * close_price) as total_dollar_volume
        FROM daily_price_data 
        WHERE date = $1
        """
        
        price_stats = await self.db_conn.fetchrow(price_query, analysis_date)
        
        return {
            'analysis_date': str(analysis_date),
            'market_overview': dict(market_overview) if market_overview else {},
            'price_statistics': dict(price_stats) if price_stats else {}
        }
        
    async def get_fundamental_statistics(self) -> Dict:
        """Get detailed statistics for all fundamental metrics."""
        logger.info("📊 Calculating comprehensive fundamental statistics...")
        
        # Use most recent date with good coverage
        recent_date_query = """
        SELECT MAX(date) as max_date
        FROM historical_fundamentals_daily
        WHERE pe_ratio IS NOT NULL
        GROUP BY date
        HAVING COUNT(DISTINCT symbol) > 100
        ORDER BY date DESC
        LIMIT 1
        """
        
        recent_date = await self.db_conn.fetchval(recent_date_query)
        
        if not recent_date:
            return {}
            
        # Get comprehensive statistics
        query = """
        SELECT 
            -- P/E Ratio statistics
            COUNT(CASE WHEN pe_ratio IS NOT NULL THEN 1 END) as pe_count,
            AVG(pe_ratio) as pe_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio) as pe_median,
            STDDEV(pe_ratio) as pe_std,
            MIN(pe_ratio) as pe_min,
            MAX(pe_ratio) as pe_max,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pe_ratio) as pe_25th,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pe_ratio) as pe_75th,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pe_ratio) as pe_90th,
            
            -- P/B Ratio statistics
            COUNT(CASE WHEN pb_ratio IS NOT NULL THEN 1 END) as pb_count,
            AVG(pb_ratio) as pb_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pb_ratio) as pb_median,
            STDDEV(pb_ratio) as pb_std,
            MIN(pb_ratio) as pb_min,
            MAX(pb_ratio) as pb_max,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY pb_ratio) as pb_25th,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pb_ratio) as pb_75th,
            
            -- Market Cap statistics (in billions)
            COUNT(CASE WHEN market_cap IS NOT NULL THEN 1 END) as cap_count,
            AVG(market_cap)/1000000000 as cap_mean_billions,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY market_cap)/1000000000 as cap_median_billions,
            MIN(market_cap)/1000000000 as cap_min_billions,
            MAX(market_cap)/1000000000 as cap_max_billions,
            
            -- Debt/Equity statistics  
            COUNT(CASE WHEN debt_to_equity IS NOT NULL THEN 1 END) as de_count,
            AVG(debt_to_equity) as de_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY debt_to_equity) as de_median,
            
            -- Current Ratio statistics
            COUNT(CASE WHEN current_ratio IS NOT NULL THEN 1 END) as cr_count,
            AVG(current_ratio) as cr_mean,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_ratio) as cr_median
            
        FROM historical_fundamentals_daily
        WHERE date = $1
        AND pe_ratio > 0 AND pe_ratio < 100
        AND pb_ratio > 0 AND pb_ratio < 20
        """
        
        stats = await self.db_conn.fetchrow(query, recent_date)
        
        return {
            'analysis_date': str(recent_date),
            'statistics': dict(stats) if stats else {}
        }
        
    async def get_company_rankings(self, metric: str = 'market_cap', limit: int = 20) -> List[Dict]:
        """Get top companies by various metrics."""
        logger.info(f"🏆 Getting top {limit} companies by {metric}...")
        
        # Get most recent date
        recent_date = await self.db_conn.fetchval("""
            SELECT MAX(date) FROM historical_fundamentals_daily 
            WHERE market_cap IS NOT NULL
        """)
        
        # Different sorting for different metrics
        if metric == 'market_cap':
            order_by = "market_cap DESC"
            filter_condition = "market_cap IS NOT NULL"
        elif metric == 'value':  # Low P/E
            order_by = "pe_ratio ASC"
            filter_condition = "pe_ratio > 0 AND pe_ratio < 100"
        elif metric == 'growth':  # Based on P/E to growth potential
            order_by = "pe_ratio ASC"
            filter_condition = "pe_ratio > 0 AND pe_ratio < 30 AND pb_ratio < 5"
        elif metric == 'quality':  # Low debt, good ratios
            order_by = "debt_to_equity ASC"
            filter_condition = "debt_to_equity >= 0 AND debt_to_equity < 2 AND current_ratio > 1"
        else:
            order_by = "market_cap DESC"
            filter_condition = "market_cap IS NOT NULL"
            
        query = f"""
        SELECT 
            h.symbol,
            cm.company_name,
            cm.industry,
            h.market_cap,
            h.pe_ratio,
            h.pb_ratio,
            h.debt_to_equity,
            h.current_ratio,
            h.close_price,
            CASE 
                WHEN h.close_price > 0 AND h.book_value_per_share > 0 
                THEN h.close_price / h.book_value_per_share * 0.15
                ELSE NULL 
            END as estimated_roe
        FROM historical_fundamentals_daily h
        LEFT JOIN company_master cm ON h.symbol = cm.primary_ticker
        WHERE h.date = $1
        AND {filter_condition}
        ORDER BY {order_by}
        LIMIT $2
        """
        
        companies = await self.db_conn.fetch(query, recent_date, limit)
        
        return [dict(company) for company in companies]
        
    async def get_sector_analysis(self) -> Dict:
        """Analyze fundamentals by sector."""
        logger.info("🏭 Performing sector analysis...")
        
        recent_date = await self.db_conn.fetchval("""
            SELECT MAX(date) FROM historical_fundamentals_daily 
            WHERE market_cap IS NOT NULL
        """)
        
        query = """
        SELECT 
            COALESCE(cm.industry, 'Unknown') as sector,
            COUNT(DISTINCT h.symbol) as company_count,
            SUM(h.market_cap) as total_market_cap,
            AVG(h.market_cap) as avg_market_cap,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.market_cap) as median_market_cap,
            AVG(h.pe_ratio) as avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY h.pe_ratio) as median_pe,
            AVG(h.pb_ratio) as avg_pb,
            AVG(h.debt_to_equity) as avg_debt_equity,
            AVG(h.current_ratio) as avg_current_ratio
        FROM historical_fundamentals_daily h
        LEFT JOIN company_master cm ON h.symbol = cm.primary_ticker
        WHERE h.date = $1
        AND h.pe_ratio > 0 AND h.pe_ratio < 100
        AND h.market_cap > 0
        GROUP BY COALESCE(cm.industry, 'Unknown')
        HAVING COUNT(DISTINCT h.symbol) >= 3
        ORDER BY SUM(h.market_cap) DESC
        """
        
        sectors = await self.db_conn.fetch(query, recent_date)
        
        return {
            'analysis_date': str(recent_date),
            'sectors': [dict(sector) for sector in sectors]
        }
        
    async def get_historical_comparison(self, years_back: int = 3) -> Dict:
        """Compare current metrics to historical averages."""
        logger.info(f"📈 Comparing current market to {years_back} years ago...")
        
        current_date = await self.db_conn.fetchval("""
            SELECT MAX(date) FROM historical_fundamentals_daily 
            WHERE market_cap IS NOT NULL
        """)
        
        historical_date = current_date - timedelta(days=years_back * 365)
        
        comparison_query = """
        WITH current_metrics AS (
            SELECT 
                COUNT(DISTINCT symbol) as companies,
                AVG(pe_ratio) as avg_pe,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio) as median_pe,
                AVG(pb_ratio) as avg_pb,
                AVG(market_cap) as avg_market_cap,
                SUM(market_cap) as total_market_cap
            FROM historical_fundamentals_daily
            WHERE date = $1
            AND pe_ratio > 0 AND pe_ratio < 100
        ),
        historical_metrics AS (
            SELECT 
                COUNT(DISTINCT symbol) as companies,
                AVG(pe_ratio) as avg_pe,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio) as median_pe,
                AVG(pb_ratio) as avg_pb,
                AVG(market_cap) as avg_market_cap,
                SUM(market_cap) as total_market_cap
            FROM historical_fundamentals_daily
            WHERE date >= ($2::date - INTERVAL '30 days') AND date <= ($2::date + INTERVAL '30 days')
            AND pe_ratio > 0 AND pe_ratio < 100
        )
        SELECT 
            c.companies as current_companies,
            h.companies as historical_companies,
            c.avg_pe as current_pe,
            h.avg_pe as historical_pe,
            ((c.avg_pe - h.avg_pe) / h.avg_pe * 100) as pe_change_pct,
            c.median_pe as current_median_pe,
            h.median_pe as historical_median_pe,
            c.avg_pb as current_pb,
            h.avg_pb as historical_pb,
            ((c.avg_pb - h.avg_pb) / h.avg_pb * 100) as pb_change_pct,
            c.total_market_cap as current_total_cap,
            h.total_market_cap as historical_total_cap,
            ((c.total_market_cap - h.total_market_cap) / h.total_market_cap * 100) as market_cap_change_pct
        FROM current_metrics c, historical_metrics h
        """
        
        comparison = await self.db_conn.fetchrow(comparison_query, current_date, historical_date)
        
        return {
            'current_date': str(current_date),
            'historical_date': str(historical_date),
            'years_back': years_back,
            'comparison': dict(comparison) if comparison else {}
        }
        
    async def print_comprehensive_report(self):
        """Print comprehensive market statistics report."""
        print("=" * 120)
        print("🏛️  YODABUFFETT COMPREHENSIVE MARKET STATISTICS")
        print("=" * 120)
        
        # Market Overview
        overview = await self.get_market_overview()
        if overview.get('market_overview'):
            market = overview['market_overview']
            print(f"\n📊 MARKET OVERVIEW (as of {overview['analysis_date']}):")
            print(f"   Total Companies: {market.get('total_companies', 0):,}")
            print(f"   Total Market Cap: ${market.get('total_market_cap', 0):,.0f}")
            print(f"   Average P/E: {market.get('avg_pe', 0):.1f} (Median: {market.get('median_pe', 0):.1f})")
            print(f"   P/E Range: {market.get('pe_25th', 0):.1f} - {market.get('pe_75th', 0):.1f} (25th-75th percentile)")
            print(f"   Average P/B: {market.get('avg_pb', 0):.1f} (Median: {market.get('median_pb', 0):.1f})")
            print(f"   Average Debt/Equity: {market.get('avg_debt_equity', 0):.2f}")
            print(f"   Average Current Ratio: {market.get('avg_current_ratio', 0):.2f}")
            
        # Fundamental Statistics
        stats = await self.get_fundamental_statistics()
        if stats.get('statistics'):
            s = stats['statistics']
            print(f"\n📈 DETAILED FUNDAMENTAL STATISTICS:")
            print(f"   P/E Ratio Distribution ({s.get('pe_count', 0)} companies):")
            print(f"     Mean: {s.get('pe_mean', 0):.1f} | Median: {s.get('pe_median', 0):.1f} | Std: {s.get('pe_std', 0):.1f}")
            print(f"     Range: {s.get('pe_min', 0):.1f} - {s.get('pe_max', 0):.1f}")
            print(f"     Percentiles: 25th={s.get('pe_25th', 0):.1f}, 75th={s.get('pe_75th', 0):.1f}, 90th={s.get('pe_90th', 0):.1f}")
            
            print(f"   P/B Ratio Distribution ({s.get('pb_count', 0)} companies):")
            print(f"     Mean: {s.get('pb_mean', 0):.1f} | Median: {s.get('pb_median', 0):.1f}")
            print(f"     Range: {s.get('pb_min', 0):.1f} - {s.get('pb_max', 0):.1f}")
            
            print(f"   Market Cap Distribution ({s.get('cap_count', 0)} companies):")
            print(f"     Mean: ${s.get('cap_mean_billions', 0):.1f}B | Median: ${s.get('cap_median_billions', 0):.1f}B")
            print(f"     Range: ${s.get('cap_min_billions', 0):.1f}B - ${s.get('cap_max_billions', 0):.1f}B")
            
        # Top Companies
        print(f"\n🏆 TOP COMPANIES BY MARKET CAP:")
        top_companies = await self.get_company_rankings('market_cap', 10)
        for i, company in enumerate(top_companies, 1):
            name = company.get('company_name', 'Unknown') or company['symbol']
            pe_ratio = company.get('pe_ratio') if company.get('pe_ratio') is not None else 0
            print(f"   {i:2d}. {company['symbol']:<8} ({name[:30]:<30}) ${company['market_cap']:>14,.0f} | P/E: {pe_ratio:>5.1f}")
            
        # Best Value Stocks
        print(f"\n💎 BEST VALUE STOCKS (Low P/E with reasonable P/B):")
        value_stocks = await self.get_company_rankings('value', 10)
        for i, company in enumerate(value_stocks, 1):
            name = company.get('company_name', 'Unknown') or company['symbol']
            pe_ratio = company.get('pe_ratio', 0)
            pb_ratio = company.get('pb_ratio', 0)
            print(f"   {i:2d}. {company['symbol']:<8} P/E: {pe_ratio:>5.1f} | P/B: {pb_ratio:>4.1f} ({name[:25]})")
            
        # Quality Companies
        print(f"\n⭐ HIGHEST QUALITY (Low debt, strong ratios):")
        quality_stocks = await self.get_company_rankings('quality', 10)
        for i, company in enumerate(quality_stocks, 1):
            name = company.get('company_name', 'Unknown') or company['symbol']
            debt_equity = company.get('debt_to_equity', 0)
            current_ratio = company.get('current_ratio', 0)
            print(f"   {i:2d}. {company['symbol']:<8} D/E: {debt_equity:>4.2f} | Current: {current_ratio:>4.1f} ({name[:25]})")
            
        # Sector Analysis
        sector_data = await self.get_sector_analysis()
        if sector_data.get('sectors'):
            print(f"\n🏭 SECTOR ANALYSIS:")
            print(f"{'Sector':<25} {'Companies':>10} {'Market Cap':>15} {'Avg P/E':>10} {'Avg P/B':>10}")
            print("-" * 75)
            for sector in sector_data['sectors'][:10]:
                print(f"{sector['sector']:<25} {sector['company_count']:>10} "
                      f"${sector['total_market_cap']/1e9:>13.1f}B "
                      f"{sector['avg_pe']:>10.1f} {sector['avg_pb']:>10.1f}")
                      
        # Historical Comparison
        historical = await self.get_historical_comparison(3)
        if historical.get('comparison'):
            comp = historical['comparison']
            print(f"\n📊 3-YEAR HISTORICAL COMPARISON:")
            print(f"   Metric                Current    3 Years Ago    Change")
            print(f"   " + "-" * 50)
            print(f"   Companies:            {comp.get('current_companies', 0):>7}    {comp.get('historical_companies', 0):>11}")
            print(f"   Average P/E:          {comp.get('current_pe', 0):>7.1f}    {comp.get('historical_pe', 0):>11.1f}    {comp.get('pe_change_pct', 0):>+6.1f}%")
            print(f"   Median P/E:           {comp.get('current_median_pe', 0):>7.1f}    {comp.get('historical_median_pe', 0):>11.1f}")
            print(f"   Average P/B:          {comp.get('current_pb', 0):>7.1f}    {comp.get('historical_pb', 0):>11.1f}    {comp.get('pb_change_pct', 0):>+6.1f}%")
            current_cap = float(comp.get('current_total_cap', 0)) if comp.get('current_total_cap') else 0
            historical_cap = float(comp.get('historical_total_cap', 0)) if comp.get('historical_total_cap') else 0
            print(f"   Total Market Cap:     ${current_cap/1e12:>6.2f}T    ${historical_cap/1e12:>10.2f}T    {comp.get('market_cap_change_pct', 0):>+6.1f}%")
            
        print("\n" + "=" * 120)
        print("Analysis complete! 📊")
        print("=" * 120)
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run comprehensive market statistics analysis."""
    
    analyzer = MarketStatisticsComprehensive()
    
    try:
        await analyzer.setup()
        
        print("🚀 Starting Comprehensive Market Statistics Analysis")
        print("=" * 60)
        
        # Generate and display comprehensive report
        await analyzer.print_comprehensive_report()
        
        # Generate detailed JSON report
        print("\n📋 Generating detailed JSON report...")
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'market_overview': await analyzer.get_market_overview(),
            'fundamental_statistics': await analyzer.get_fundamental_statistics(),
            'top_companies': {
                'by_market_cap': await analyzer.get_company_rankings('market_cap', 20),
                'by_value': await analyzer.get_company_rankings('value', 20),
                'by_quality': await analyzer.get_company_rankings('quality', 20)
            },
            'sector_analysis': await analyzer.get_sector_analysis(),
            'historical_comparison': await analyzer.get_historical_comparison(3)
        }
        
        filename = f"comprehensive_market_statistics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
            
        print(f"✅ Detailed report saved to {filename}")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())