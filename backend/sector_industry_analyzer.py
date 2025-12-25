#!/usr/bin/env python3
"""
Sector & Industry Analyzer

Advanced sector and industry analysis:
- Sector performance comparisons
- Industry-specific metrics and benchmarks
- Sector rotation analysis
- Industry concentration and competition metrics
- Relative valuation analysis by sector
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
class SectorMetrics:
    """Container for sector-level metrics."""
    sector_name: str
    company_count: int
    total_market_cap: float
    avg_market_cap: float
    median_market_cap: float
    avg_pe: float
    avg_pb: float
    avg_roe: float
    avg_dividend_yield: float
    avg_debt_equity: float
    avg_profit_margin: float
    performance_1y: float
    volatility: float
    beta: float

class SectorIndustryAnalyzer:
    """Comprehensive sector and industry analysis engine."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_sector_overview(self) -> Dict:
        """Get comprehensive sector breakdown and overview."""
        logger.info("🏭 Generating sector overview...")
        
        query = """
        SELECT 
            COALESCE(cm.industry, 'Unknown') as sector,
            COALESCE(cm.sub_industry, 'General') as sub_industry,
            COUNT(DISTINCT df.symbol) as company_count,
            SUM(df.market_cap::BIGINT) as total_market_cap,
            AVG(df.market_cap::BIGINT) as avg_market_cap,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.market_cap::BIGINT) as median_market_cap,
            AVG(df.trailing_pe) as avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.trailing_pe) as median_pe,
            AVG(df.price_to_book) as avg_pb,
            AVG(df.dividend_yield) as avg_dividend_yield,
            AVG(df.return_on_equity) as avg_roe,
            AVG(df.total_debt_to_equity) as avg_debt_equity,
            AVG(df.profit_margin) as avg_profit_margin,
            STDDEV(df.trailing_pe) as pe_std,
            MIN(df.trailing_pe) as min_pe,
            MAX(df.trailing_pe) as max_pe
        FROM daily_fundamentals df
        LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
        WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
        AND df.trailing_pe > 0 AND df.trailing_pe < 100
        AND df.price_to_book > 0 AND df.price_to_book < 20
        GROUP BY COALESCE(cm.industry, 'Unknown'), COALESCE(cm.sub_industry, 'General')
        HAVING COUNT(DISTINCT df.symbol) >= 3  -- Only sectors with 3+ companies
        ORDER BY SUM(df.market_cap::BIGINT) DESC
        """
        
        sectors = await self.db_conn.fetch(query)
        
        sector_data = []
        for sector in sectors:
            sector_data.append(dict(sector))
            
        # Calculate sector concentration (HHI)
        total_market_cap = sum(s['total_market_cap'] for s in sector_data if s['total_market_cap'])
        
        for sector in sector_data:
            if sector['total_market_cap'] and total_market_cap > 0:
                sector['market_share'] = sector['total_market_cap'] / total_market_cap
            else:
                sector['market_share'] = 0
                
        return {
            'sector_breakdown': sector_data,
            'total_market_cap': total_market_cap,
            'sector_count': len(sector_data),
            'market_concentration': self._calculate_hhi([s['market_share'] for s in sector_data])
        }
        
    async def get_sector_performance_analysis(self, days: int = 365) -> Dict:
        """Analyze sector performance over time."""
        logger.info(f"📈 Analyzing sector performance over {days} days...")
        
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        # Get sector performance data
        query = """
        WITH sector_prices AS (
            SELECT 
                dpd.date,
                COALESCE(cm.industry, 'Unknown') as sector,
                dpd.symbol,
                dpd.close_price,
                LAG(dpd.close_price) OVER (PARTITION BY dpd.symbol ORDER BY dpd.date) as prev_price
            FROM daily_price_data dpd
            LEFT JOIN company_master cm ON dpd.symbol = cm.primary_ticker
            WHERE dpd.date >= $1 AND dpd.date <= $2
        ),
        sector_returns AS (
            SELECT 
                date,
                sector,
                symbol,
                CASE 
                    WHEN prev_price > 0 THEN (close_price - prev_price) / prev_price
                    ELSE NULL
                END as daily_return
            FROM sector_prices
            WHERE prev_price IS NOT NULL
        ),
        sector_daily_avg AS (
            SELECT 
                date,
                sector,
                AVG(daily_return) as sector_daily_return,
                COUNT(symbol) as stocks_in_sector
            FROM sector_returns
            WHERE daily_return IS NOT NULL
            GROUP BY date, sector
            HAVING COUNT(symbol) >= 3
        )
        SELECT 
            sector,
            COUNT(DISTINCT date) as trading_days,
            EXP(SUM(LN(1 + sector_daily_return))) - 1 as total_return,
            STDDEV(sector_daily_return) * SQRT(252) as annualized_volatility,
            AVG(sector_daily_return) * 252 as annualized_return,
            MIN(sector_daily_return) as worst_day,
            MAX(sector_daily_return) as best_day,
            AVG(stocks_in_sector) as avg_stocks_per_day
        FROM sector_daily_avg
        WHERE sector_daily_return IS NOT NULL
        GROUP BY sector
        HAVING COUNT(DISTINCT date) > 100  -- Require sufficient data
        ORDER BY total_return DESC
        """
        
        performance = await self.db_conn.fetch(query, start_date, end_date)
        
        sector_performance = []
        for perf in performance:
            perf_dict = dict(perf)
            
            # Calculate risk-adjusted metrics
            if perf_dict['annualized_volatility'] > 0:
                perf_dict['sharpe_ratio'] = perf_dict['annualized_return'] / perf_dict['annualized_volatility']
            else:
                perf_dict['sharpe_ratio'] = None
                
            # Calculate downside deviation
            downside_query = """
            WITH sector_returns AS (
                SELECT 
                    dpd.date,
                    COALESCE(cm.industry, 'Unknown') as sector,
                    dpd.symbol,
                    CASE 
                        WHEN LAG(dpd.close_price) OVER (PARTITION BY dpd.symbol ORDER BY dpd.date) > 0 
                        THEN (dpd.close_price - LAG(dpd.close_price) OVER (PARTITION BY dpd.symbol ORDER BY dpd.date)) / LAG(dpd.close_price) OVER (PARTITION BY dpd.symbol ORDER BY dpd.date)
                        ELSE NULL
                    END as daily_return
                FROM daily_price_data dpd
                LEFT JOIN company_master cm ON dpd.symbol = cm.primary_ticker
                WHERE dpd.date >= $1 AND COALESCE(cm.industry, 'Unknown') = $2
            )
            SELECT STDDEV(daily_return) * SQRT(252) as downside_deviation
            FROM sector_returns
            WHERE daily_return < 0 AND daily_return IS NOT NULL
            """
            
            downside_result = await self.db_conn.fetchval(downside_query, start_date, perf_dict['sector'])
            perf_dict['downside_deviation'] = downside_result
            
            if perf_dict['downside_deviation'] and perf_dict['downside_deviation'] > 0:
                perf_dict['sortino_ratio'] = perf_dict['annualized_return'] / perf_dict['downside_deviation']
            else:
                perf_dict['sortino_ratio'] = None
                
            sector_performance.append(perf_dict)
            
        return {
            'analysis_period': days,
            'sector_performance': sector_performance,
            'best_performing_sector': sector_performance[0] if sector_performance else None,
            'worst_performing_sector': sector_performance[-1] if sector_performance else None
        }
        
    async def get_sector_valuation_metrics(self) -> Dict:
        """Analyze valuation metrics by sector."""
        logger.info("💰 Analyzing sector valuations...")
        
        query = """
        SELECT 
            COALESCE(cm.industry, 'Unknown') as sector,
            COUNT(DISTINCT df.symbol) as company_count,
            
            -- P/E metrics
            AVG(df.trailing_pe) as avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.trailing_pe) as median_pe,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY df.trailing_pe) as pe_25th,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY df.trailing_pe) as pe_75th,
            
            -- P/B metrics
            AVG(df.price_to_book) as avg_pb,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.price_to_book) as median_pb,
            
            -- P/S metrics  
            AVG(df.price_to_sales) as avg_ps,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.price_to_sales) as median_ps,
            
            -- EV/EBITDA metrics
            AVG(df.ev_to_ebitda) as avg_ev_ebitda,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.ev_to_ebitda) as median_ev_ebitda,
            
            -- Dividend metrics
            AVG(df.dividend_yield) as avg_dividend_yield,
            COUNT(CASE WHEN df.dividend_yield > 0 THEN 1 END)::FLOAT / COUNT(*)::FLOAT as dividend_payer_pct,
            
            -- Quality metrics
            AVG(df.return_on_equity) as avg_roe,
            AVG(df.profit_margin) as avg_profit_margin,
            AVG(df.total_debt_to_equity) as avg_debt_equity,
            
            -- Market cap distribution
            SUM(df.market_cap::BIGINT) as total_market_cap,
            AVG(df.market_cap::BIGINT) as avg_market_cap,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.market_cap::BIGINT) as median_market_cap
            
        FROM daily_fundamentals df
        LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
        WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
        AND df.trailing_pe > 0 AND df.trailing_pe < 100
        AND df.price_to_book > 0 AND df.price_to_book < 20
        GROUP BY COALESCE(cm.industry, 'Unknown')
        HAVING COUNT(DISTINCT df.symbol) >= 5  -- Only sectors with 5+ companies
        ORDER BY SUM(df.market_cap::BIGINT) DESC
        """
        
        valuations = await self.db_conn.fetch(query)
        
        valuation_data = []
        for val in valuations:
            val_dict = dict(val)
            
            # Calculate valuation rankings (relative to market)
            # This helps identify cheap vs expensive sectors
            
            valuation_data.append(val_dict)
            
        return {
            'sector_valuations': valuation_data,
            'valuation_summary': self._calculate_valuation_summary(valuation_data)
        }
        
    def _calculate_valuation_summary(self, valuation_data: List[Dict]) -> Dict:
        """Calculate market-wide valuation summary."""
        if not valuation_data:
            return {}
            
        # Calculate market-wide averages
        total_companies = sum(v['company_count'] for v in valuation_data)
        total_market_cap = sum(v['total_market_cap'] for v in valuation_data if v['total_market_cap'])
        
        # Weight by market cap for overall market metrics
        weighted_pe = sum(v['avg_pe'] * (v['total_market_cap'] / total_market_cap) 
                         for v in valuation_data 
                         if v['avg_pe'] and v['total_market_cap']) if total_market_cap > 0 else 0
        
        weighted_pb = sum(v['avg_pb'] * (v['total_market_cap'] / total_market_cap) 
                         for v in valuation_data 
                         if v['avg_pb'] and v['total_market_cap']) if total_market_cap > 0 else 0
        
        # Find cheapest and most expensive sectors
        pe_sectors = [(v['sector'], v['avg_pe']) for v in valuation_data if v['avg_pe']]
        cheapest_pe = min(pe_sectors, key=lambda x: x[1]) if pe_sectors else None
        expensive_pe = max(pe_sectors, key=lambda x: x[1]) if pe_sectors else None
        
        return {
            'total_companies_analyzed': total_companies,
            'total_market_cap': total_market_cap,
            'market_weighted_pe': weighted_pe,
            'market_weighted_pb': weighted_pb,
            'cheapest_sector_pe': cheapest_pe,
            'most_expensive_sector_pe': expensive_pe,
            'sector_count': len(valuation_data)
        }
        
    async def get_sector_growth_analysis(self) -> Dict:
        """Analyze growth metrics by sector."""
        logger.info("📊 Analyzing sector growth metrics...")
        
        query = """
        SELECT 
            COALESCE(cm.industry, 'Unknown') as sector,
            COUNT(DISTINCT df.symbol) as company_count,
            
            -- Revenue growth
            AVG(df.quarterly_revenue_growth) as avg_quarterly_revenue_growth,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.quarterly_revenue_growth) as median_quarterly_revenue_growth,
            COUNT(CASE WHEN df.quarterly_revenue_growth > 0 THEN 1 END)::FLOAT / COUNT(*)::FLOAT as positive_revenue_growth_pct,
            
            -- Earnings growth
            AVG(df.quarterly_earnings_growth) as avg_quarterly_earnings_growth,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY df.quarterly_earnings_growth) as median_quarterly_earnings_growth,
            COUNT(CASE WHEN df.quarterly_earnings_growth > 0 THEN 1 END)::FLOAT / COUNT(*)::FLOAT as positive_earnings_growth_pct,
            
            -- Forward-looking growth (from estimates)
            AVG(df.earnings_growth) as avg_estimated_earnings_growth,
            AVG(df.revenue_growth) as avg_estimated_revenue_growth,
            
            -- Profitability trends  
            AVG(df.profit_margin) as avg_profit_margin,
            AVG(df.operating_margin) as avg_operating_margin,
            AVG(df.gross_margin) as avg_gross_margin,
            
            -- Return metrics
            AVG(df.return_on_equity) as avg_roe,
            AVG(df.return_on_assets) as avg_roa
            
        FROM daily_fundamentals df
        LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
        WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY COALESCE(cm.industry, 'Unknown')
        HAVING COUNT(DISTINCT df.symbol) >= 5
        ORDER BY AVG(df.quarterly_revenue_growth) DESC NULLS LAST
        """
        
        growth_data = await self.db_conn.fetch(query)
        
        sectors = []
        for sector in growth_data:
            sectors.append(dict(sector))
            
        # Identify growth leaders and laggards
        revenue_leaders = sorted(sectors, key=lambda x: x['avg_quarterly_revenue_growth'] or -999, reverse=True)[:3]
        earnings_leaders = sorted(sectors, key=lambda x: x['avg_quarterly_earnings_growth'] or -999, reverse=True)[:3]
        
        return {
            'sector_growth_metrics': sectors,
            'revenue_growth_leaders': revenue_leaders,
            'earnings_growth_leaders': earnings_leaders,
            'growth_summary': self._calculate_growth_summary(sectors)
        }
        
    def _calculate_growth_summary(self, sectors: List[Dict]) -> Dict:
        """Calculate overall growth summary across sectors."""
        if not sectors:
            return {}
            
        # Count sectors with positive growth
        positive_revenue_sectors = len([s for s in sectors if s['avg_quarterly_revenue_growth'] and s['avg_quarterly_revenue_growth'] > 0])
        positive_earnings_sectors = len([s for s in sectors if s['avg_quarterly_earnings_growth'] and s['avg_quarterly_earnings_growth'] > 0])
        
        # Calculate median growth rates
        revenue_growths = [s['avg_quarterly_revenue_growth'] for s in sectors if s['avg_quarterly_revenue_growth'] is not None]
        earnings_growths = [s['avg_quarterly_earnings_growth'] for s in sectors if s['avg_quarterly_earnings_growth'] is not None]
        
        return {
            'sectors_with_positive_revenue_growth': positive_revenue_sectors,
            'sectors_with_positive_earnings_growth': positive_earnings_sectors,
            'total_sectors_analyzed': len(sectors),
            'median_revenue_growth': np.median(revenue_growths) if revenue_growths else None,
            'median_earnings_growth': np.median(earnings_growths) if earnings_growths else None,
            'revenue_growth_dispersion': np.std(revenue_growths) if revenue_growths else None,
            'earnings_growth_dispersion': np.std(earnings_growths) if earnings_growths else None
        }
        
    def _calculate_hhi(self, market_shares: List[float]) -> float:
        """Calculate Herfindahl-Hirschman Index for market concentration."""
        return sum(share ** 2 for share in market_shares if share > 0)
        
    async def get_top_companies_by_sector(self, limit_per_sector: int = 5) -> Dict:
        """Get top companies in each sector by market cap."""
        logger.info(f"🏆 Getting top {limit_per_sector} companies per sector...")
        
        query = """
        WITH ranked_companies AS (
            SELECT 
                df.symbol,
                cm.company_name,
                COALESCE(cm.industry, 'Unknown') as sector,
                df.market_cap,
                df.trailing_pe,
                df.price_to_book,
                df.dividend_yield,
                df.return_on_equity,
                ROW_NUMBER() OVER (PARTITION BY COALESCE(cm.industry, 'Unknown') ORDER BY df.market_cap DESC) as rank
            FROM daily_fundamentals df
            LEFT JOIN company_master cm ON df.symbol = cm.primary_ticker
            WHERE df.date >= CURRENT_DATE - INTERVAL '7 days'
            AND df.market_cap IS NOT NULL
        )
        SELECT *
        FROM ranked_companies
        WHERE rank <= $1
        ORDER BY sector, rank
        """
        
        companies = await self.db_conn.fetch(query, limit_per_sector)
        
        # Group by sector
        sectors = defaultdict(list)
        for company in companies:
            sectors[company['sector']].append(dict(company))
            
        return dict(sectors)
        
    async def print_sector_analysis_report(self):
        """Print comprehensive sector analysis report."""
        print("=" * 120)
        print("🏭 COMPREHENSIVE SECTOR & INDUSTRY ANALYSIS")
        print("=" * 120)
        
        # Sector Overview
        overview = await self.get_sector_overview()
        print(f"\n📊 SECTOR OVERVIEW:")
        print(f"   Total Sectors: {overview['sector_count']}")
        print(f"   Total Market Cap: ${overview['total_market_cap']:,.0f}")
        print(f"   Market Concentration (HHI): {overview['market_concentration']:.3f}")
        
        print(f"\n🏭 LARGEST SECTORS BY MARKET CAP:")
        for i, sector in enumerate(overview['sector_breakdown'][:10]):
            print(f"   {i+1:2d}. {sector['sector']:<25} "
                  f"${sector['total_market_cap']:>12,.0f} "
                  f"({sector['company_count']:>3} companies) "
                  f"P/E: {sector['avg_pe']:>6.1f}")
                  
        # Sector Performance
        performance = await self.get_sector_performance_analysis(365)
        if performance['sector_performance']:
            print(f"\n📈 SECTOR PERFORMANCE (1 Year):")
            print(f"{'Sector':<25} {'Return':<10} {'Volatility':<12} {'Sharpe':<8}")
            print("-" * 60)
            
            for sector in performance['sector_performance'][:10]:
                return_str = f"{sector['total_return']:.1%}" if sector['total_return'] else "N/A"
                vol_str = f"{sector['annualized_volatility']:.1%}" if sector['annualized_volatility'] else "N/A"
                sharpe_str = f"{sector['sharpe_ratio']:.2f}" if sector['sharpe_ratio'] else "N/A"
                
                print(f"{sector['sector']:<25} {return_str:<10} {vol_str:<12} {sharpe_str:<8}")
                
        # Sector Valuations
        valuations = await self.get_sector_valuation_metrics()
        print(f"\n💰 SECTOR VALUATIONS:")
        print(f"{'Sector':<25} {'Avg P/E':<10} {'Avg P/B':<10} {'Div Yield':<12} {'ROE':<8}")
        print("-" * 70)
        
        for sector in valuations['sector_valuations'][:10]:
            pe_str = f"{sector['avg_pe']:.1f}" if sector['avg_pe'] else "N/A"
            pb_str = f"{sector['avg_pb']:.1f}" if sector['avg_pb'] else "N/A"
            div_str = f"{sector['avg_dividend_yield']:.1%}" if sector['avg_dividend_yield'] else "N/A"
            roe_str = f"{sector['avg_roe']:.1%}" if sector['avg_roe'] else "N/A"
            
            print(f"{sector['sector']:<25} {pe_str:<10} {pb_str:<10} {div_str:<12} {roe_str:<8}")
            
        # Growth Analysis
        growth = await self.get_sector_growth_analysis()
        print(f"\n📊 SECTOR GROWTH METRICS:")
        print(f"{'Sector':<25} {'Rev Growth':<12} {'Earn Growth':<12} {'Profit Margin':<15}")
        print("-" * 70)
        
        for sector in growth['sector_growth_metrics'][:10]:
            rev_str = f"{sector['avg_quarterly_revenue_growth']:.1%}" if sector['avg_quarterly_revenue_growth'] else "N/A"
            earn_str = f"{sector['avg_quarterly_earnings_growth']:.1%}" if sector['avg_quarterly_earnings_growth'] else "N/A"
            margin_str = f"{sector['avg_profit_margin']:.1%}" if sector['avg_profit_margin'] else "N/A"
            
            print(f"{sector['sector']:<25} {rev_str:<12} {earn_str:<12} {margin_str:<15}")
            
        print("\n" + "=" * 120)
        print("Sector analysis complete! 🎉")
        print("=" * 120)
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run comprehensive sector and industry analysis."""
    
    analyzer = SectorIndustryAnalyzer()
    
    try:
        await analyzer.setup()
        
        print("🚀 Starting Sector & Industry Analysis")
        print("=" * 60)
        
        # Print comprehensive report
        await analyzer.print_sector_analysis_report()
        
        # Generate detailed JSON reports
        print("\n📋 Generating detailed sector analysis reports...")
        
        overview = await analyzer.get_sector_overview()
        performance = await analyzer.get_sector_performance_analysis(365)
        valuations = await analyzer.get_sector_valuation_metrics()
        growth = await analyzer.get_sector_growth_analysis()
        top_companies = await analyzer.get_top_companies_by_sector(5)
        
        # Save comprehensive report
        comprehensive_report = {
            'generated_at': datetime.now().isoformat(),
            'sector_overview': overview,
            'sector_performance': performance,
            'sector_valuations': valuations,
            'sector_growth': growth,
            'top_companies_by_sector': top_companies
        }
        
        filename = f"sector_analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(comprehensive_report, f, indent=2, default=str)
            
        print(f"✅ Comprehensive report saved to {filename}")
        
        # Print summary statistics
        print(f"\n📈 ANALYSIS SUMMARY:")
        print(f"   Sectors analyzed: {overview['sector_count']}")
        print(f"   Total market cap: ${overview['total_market_cap']:,.0f}")
        print(f"   Performance periods: {len(performance['sector_performance'])} sectors")
        print(f"   Valuation analysis: {len(valuations['sector_valuations'])} sectors")
        print(f"   Growth analysis: {len(growth['sector_growth_metrics'])} sectors")
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())