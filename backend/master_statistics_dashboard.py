#!/usr/bin/env python3
"""
Master Statistics Dashboard

Comprehensive market statistics and analytics dashboard that orchestrates all analysis tools:
- Market overview and baselines
- Company comparison and rankings  
- Sector and industry analysis
- Historical trends and patterns
- Custom analysis requests
- Automated reporting and insights
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Union
import json
import logging
import argparse
import sys
import os

# Import our analysis modules
from market_statistics_analyzer import MarketStatisticsAnalyzer
from company_comparison_tool import CompanyComparisonTool
from sector_industry_analyzer import SectorIndustryAnalyzer
from historical_trends_analyzer import HistoricalTrendsAnalyzer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MasterStatisticsDashboard:
    """Unified dashboard for all market statistics and analytics."""
    
    def __init__(self):
        self.market_analyzer = MarketStatisticsAnalyzer()
        self.company_tool = CompanyComparisonTool() 
        self.sector_analyzer = SectorIndustryAnalyzer()
        self.trends_analyzer = HistoricalTrendsAnalyzer()
        
    async def setup(self):
        """Initialize all analysis tools."""
        await self.market_analyzer.setup()
        await self.company_tool.setup()
        await self.sector_analyzer.setup()
        await self.trends_analyzer.setup()
        
    async def generate_executive_summary(self) -> Dict:
        """Generate high-level executive summary of market conditions."""
        logger.info("🎯 Generating executive summary...")
        
        # Get key metrics from each analyzer
        market_overview = await self.market_analyzer.get_market_overview()
        sectors = await self.sector_analyzer.get_sector_overview()
        trends = await self.trends_analyzer.get_data_coverage_summary()
        
        # Get top performers
        top_market_cap = await self.market_analyzer.get_company_rankings('market_cap', 5)
        top_value = await self.market_analyzer.get_company_rankings('pe_ratio', 5)
        top_dividend = await self.market_analyzer.get_company_rankings('dividend_yield', 5)
        
        # Key market metrics
        market_metrics = market_overview.get('market_overview', {})
        
        summary = {
            'generated_at': datetime.now().isoformat(),
            'market_snapshot': {
                'total_companies': market_metrics.get('total_companies', 0),
                'total_market_cap': market_metrics.get('total_market_cap', 0),
                'average_pe': market_metrics.get('avg_pe', 0),
                'median_pe': market_metrics.get('median_pe', 0),
                'average_dividend_yield': market_metrics.get('avg_dividend_yield', 0),
                'data_coverage_days': self._calculate_data_coverage(trends)
            },
            'sector_summary': {
                'total_sectors': sectors.get('sector_count', 0),
                'largest_sector': self._get_largest_sector(sectors),
                'market_concentration': sectors.get('market_concentration', 0)
            },
            'top_performers': {
                'largest_companies': [{'symbol': c['symbol'], 'name': c.get('company_name', 'Unknown') or 'Unknown', 'market_cap': c.get('metric_value', 0)} for c in top_market_cap[:3]],
                'best_value': [{'symbol': c['symbol'], 'name': c.get('company_name', 'Unknown') or 'Unknown', 'pe_ratio': c.get('metric_value', 0)} for c in top_value[:3]],
                'highest_dividend': [{'symbol': c['symbol'], 'name': c.get('company_name', 'Unknown') or 'Unknown', 'dividend_yield': c.get('metric_value', 0)} for c in top_dividend[:3]]
            },
            'key_insights': await self._generate_key_insights(market_metrics, sectors)
        }
        
        return summary
        
    def _calculate_data_coverage(self, trends: Dict) -> int:
        """Calculate total days of data coverage."""
        if not trends.get('historical_fundamentals'):
            return 0
            
        hist = trends['historical_fundamentals']
        if hist.get('earliest_date') and hist.get('latest_date'):
            start = datetime.strptime(str(hist['earliest_date']), '%Y-%m-%d').date()
            end = datetime.strptime(str(hist['latest_date']), '%Y-%m-%d').date()
            return (end - start).days
        return 0
        
    def _get_largest_sector(self, sectors: Dict) -> Optional[Dict]:
        """Get the largest sector by market cap."""
        if not sectors.get('sector_breakdown'):
            return None
            
        largest = max(sectors['sector_breakdown'], key=lambda x: x.get('total_market_cap', 0))
        return {
            'name': largest['sector'],
            'market_cap': largest['total_market_cap'],
            'company_count': largest['company_count']
        }
        
    async def _generate_key_insights(self, market_metrics: Dict, sectors: Dict) -> List[str]:
        """Generate key insights from the data."""
        insights = []
        
        # Market valuation insights
        avg_pe = market_metrics.get('avg_pe', 0)
        if avg_pe > 0:
            if avg_pe > 20:
                insights.append(f"Market appears expensive with average P/E of {avg_pe:.1f}")
            elif avg_pe < 12:
                insights.append(f"Market appears undervalued with average P/E of {avg_pe:.1f}")
            else:
                insights.append(f"Market valuation appears reasonable with average P/E of {avg_pe:.1f}")
                
        # Dividend insights
        avg_div = market_metrics.get('avg_dividend_yield', 0)
        if avg_div > 0:
            if avg_div > 0.04:
                insights.append(f"High dividend environment with average yield of {avg_div:.1%}")
            elif avg_div < 0.02:
                insights.append(f"Low dividend environment with average yield of {avg_div:.1%}")
                
        # Market concentration
        concentration = sectors.get('market_concentration', 0)
        if concentration > 0.15:
            insights.append("Market shows high concentration among few large sectors")
        elif concentration < 0.05:
            insights.append("Market shows good diversification across sectors")
            
        return insights
        
    async def run_full_market_analysis(self, save_reports: bool = True) -> Dict:
        """Run comprehensive analysis across all tools."""
        logger.info("🚀 Running comprehensive market analysis...")
        
        results = {
            'generated_at': datetime.now().isoformat(),
            'executive_summary': {},
            'market_statistics': {},
            'sector_analysis': {},
            'historical_trends': {},
            'analysis_metadata': {}
        }
        
        try:
            # Executive Summary
            results['executive_summary'] = await self.generate_executive_summary()
            
            # Market Statistics  
            logger.info("📊 Running market statistics analysis...")
            results['market_statistics'] = await self.market_analyzer.generate_report(save_to_file=False)
            
            # Sector Analysis
            logger.info("🏭 Running sector analysis...")
            sector_overview = await self.sector_analyzer.get_sector_overview()
            sector_performance = await self.sector_analyzer.get_sector_performance_analysis(365)
            sector_valuations = await self.sector_analyzer.get_sector_valuation_metrics()
            sector_growth = await self.sector_analyzer.get_sector_growth_analysis()
            
            results['sector_analysis'] = {
                'overview': sector_overview,
                'performance': sector_performance,
                'valuations': sector_valuations,
                'growth': sector_growth
            }
            
            # Historical Trends
            logger.info("📈 Running historical trends analysis...")
            coverage = await self.trends_analyzer.get_data_coverage_summary()
            fundamental_trends = await self.trends_analyzer.analyze_fundamental_trends(4)
            price_trends = await self.trends_analyzer.analyze_price_trends(4)
            sector_rotation = await self.trends_analyzer.analyze_sector_rotation_patterns()
            
            results['historical_trends'] = {
                'data_coverage': coverage,
                'fundamental_trends': fundamental_trends,
                'price_trends': price_trends,
                'sector_rotation': sector_rotation
            }
            
            # Analysis Metadata
            results['analysis_metadata'] = {
                'total_analysis_time_seconds': None,  # Will be calculated
                'components_analyzed': ['market_statistics', 'sector_analysis', 'historical_trends'],
                'data_quality_score': self._calculate_data_quality_score(results),
                'confidence_level': self._assess_confidence_level(results)
            }
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            results['error'] = str(e)
            
        # Save comprehensive report
        if save_reports:
            filename = f"comprehensive_market_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"💾 Comprehensive report saved to {filename}")
            
        return results
        
    def _calculate_data_quality_score(self, results: Dict) -> float:
        """Calculate a data quality score based on coverage and completeness."""
        score = 0.0
        max_score = 10.0
        
        # Check market data quality
        market_data = results.get('market_statistics', {}).get('market_overview', {}).get('market_overview', {})
        if market_data.get('total_companies', 0) > 500:
            score += 2.0
        elif market_data.get('total_companies', 0) > 100:
            score += 1.0
            
        # Check historical data coverage
        trends_data = results.get('historical_trends', {}).get('data_coverage', {})
        if trends_data.get('historical_fundamentals', {}).get('total_records', 0) > 100000:
            score += 3.0
        elif trends_data.get('historical_fundamentals', {}).get('total_records', 0) > 10000:
            score += 2.0
        elif trends_data.get('historical_fundamentals', {}).get('total_records', 0) > 1000:
            score += 1.0
            
        # Check sector coverage
        sector_data = results.get('sector_analysis', {}).get('overview', {})
        if sector_data.get('sector_count', 0) > 15:
            score += 2.0
        elif sector_data.get('sector_count', 0) > 5:
            score += 1.0
            
        # Check fundamental data quality
        fund_distributions = results.get('market_statistics', {}).get('fundamental_distributions', {})
        if len(fund_distributions) > 8:
            score += 2.0
        elif len(fund_distributions) > 4:
            score += 1.0
            
        # Check price data quality
        price_stats = results.get('market_statistics', {}).get('price_statistics', {})
        if price_stats.get('overall_stats', {}).get('symbols_traded', 0) > 500:
            score += 1.0
            
        return min(score / max_score, 1.0)
        
    def _assess_confidence_level(self, results: Dict) -> str:
        """Assess confidence level in the analysis."""
        quality_score = self._calculate_data_quality_score(results)
        
        if quality_score >= 0.8:
            return "High"
        elif quality_score >= 0.6:
            return "Medium"
        else:
            return "Low"
            
    async def compare_specific_companies(self, symbols: List[str]) -> Dict:
        """Run detailed comparison for specific companies."""
        logger.info(f"🔍 Comparing companies: {', '.join(symbols)}")
        
        comparison = await self.company_tool.compare_companies(symbols, 365)
        
        # Enhance with market context
        market_overview = await self.market_analyzer.get_market_overview()
        
        enhanced_comparison = {
            **comparison,
            'market_context': {
                'market_avg_pe': market_overview.get('market_overview', {}).get('avg_pe'),
                'market_avg_pb': market_overview.get('market_overview', {}).get('avg_pb'),
                'market_avg_dividend_yield': market_overview.get('market_overview', {}).get('avg_dividend_yield')
            }
        }
        
        return enhanced_comparison
        
    async def get_investment_screening_report(self, criteria: Dict = None) -> Dict:
        """Generate investment screening report based on criteria."""
        logger.info("📋 Generating investment screening report...")
        
        if criteria is None:
            criteria = {
                'max_pe': 25,
                'min_dividend_yield': 0.02,
                'min_roe': 0.10,
                'max_debt_equity': 2.0
            }
            
        # Use market analyzer to get company rankings
        value_candidates = await self.market_analyzer.get_company_rankings('pe_ratio', 50)
        dividend_candidates = await self.market_analyzer.get_company_rankings('dividend_yield', 50)
        quality_candidates = await self.market_analyzer.get_company_rankings('roe', 50)
        
        # Apply screening criteria
        screened_companies = []
        
        for company in value_candidates:
            if (company.get('metric_value', 999) <= criteria.get('max_pe', 999) and
                company.get('dividend_yield', 0) >= criteria.get('min_dividend_yield', 0) and
                company.get('return_on_equity', 0) >= criteria.get('min_roe', 0)):
                
                screened_companies.append(company)
                
        return {
            'screening_criteria': criteria,
            'total_candidates_screened': len(value_candidates),
            'companies_passing_screen': len(screened_companies),
            'screened_companies': screened_companies[:20],  # Top 20
            'screen_summary': {
                'pass_rate': len(screened_companies) / len(value_candidates) if value_candidates else 0,
                'avg_pe_of_screened': np.mean([c.get('metric_value', 0) for c in screened_companies]) if screened_companies else 0,
                'avg_dividend_yield': np.mean([c.get('dividend_yield', 0) for c in screened_companies]) if screened_companies else 0
            }
        }
        
    async def print_dashboard_summary(self):
        """Print comprehensive dashboard summary."""
        print("=" * 140)
        print("🎯 YODABUFFETT MASTER STATISTICS DASHBOARD")
        print("=" * 140)
        
        # Executive Summary
        summary = await self.generate_executive_summary()
        
        print(f"\n📊 EXECUTIVE SUMMARY:")
        market = summary['market_snapshot']
        print(f"   Market Overview: {market['total_companies']:,} companies, ${market['total_market_cap']:,.0f} total market cap")
        print(f"   Valuation: P/E {market['average_pe']:.1f} (median {market['median_pe']:.1f}), Avg dividend yield {market['average_dividend_yield']:.1%}")
        print(f"   Data Coverage: {market['data_coverage_days']:,} days of historical data")
        
        sector = summary['sector_summary']
        print(f"   Sectors: {sector['total_sectors']} sectors, largest is {sector['largest_sector']['name'] if sector['largest_sector'] else 'N/A'}")
        
        print(f"\n🏆 TOP PERFORMERS:")
        for category, companies in summary['top_performers'].items():
            print(f"   {category.replace('_', ' ').title()}:")
            for i, company in enumerate(companies, 1):
                if category == 'largest_companies':
                    print(f"     {i}. {company['symbol']} ({company['name'][:25]}) - ${company['market_cap']:,.0f}")
                elif category == 'best_value':
                    print(f"     {i}. {company['symbol']} ({company['name'][:25]}) - P/E {company['pe_ratio']:.1f}")
                elif category == 'highest_dividend':
                    print(f"     {i}. {company['symbol']} ({company['name'][:25]}) - {company['dividend_yield']:.1%}")
                    
        print(f"\n💡 KEY INSIGHTS:")
        for insight in summary['key_insights']:
            print(f"   • {insight}")
            
        # Quick market analyzer summary
        print(f"\n📈 MARKET STATISTICS PREVIEW:")
        await self.market_analyzer.print_summary_report()
        
        print("\n" + "=" * 140)
        print("Use the following commands for detailed analysis:")
        print("   python master_statistics_dashboard.py --full-analysis")
        print("   python master_statistics_dashboard.py --compare VOLV-B,ERIC-B,ABB")
        print("   python master_statistics_dashboard.py --screen")
        print("=" * 140)
        
    async def cleanup(self):
        """Cleanup all analyzers."""
        await self.market_analyzer.cleanup()
        await self.company_tool.cleanup()
        await self.sector_analyzer.cleanup()
        await self.trends_analyzer.cleanup()

async def main():
    """Main entry point with command line interface."""
    
    parser = argparse.ArgumentParser(description='YodaBuffett Master Statistics Dashboard')
    parser.add_argument('--full-analysis', action='store_true', 
                       help='Run comprehensive analysis across all tools')
    parser.add_argument('--compare', type=str,
                       help='Compare specific companies (comma-separated symbols)')
    parser.add_argument('--screen', action='store_true',
                       help='Run investment screening report')
    parser.add_argument('--summary', action='store_true', default=True,
                       help='Show dashboard summary (default)')
    
    args = parser.parse_args()
    
    dashboard = MasterStatisticsDashboard()
    
    try:
        await dashboard.setup()
        
        if args.full_analysis:
            print("🚀 Running Full Market Analysis...")
            results = await dashboard.run_full_market_analysis()
            print(f"\n✅ Analysis complete! Data quality score: {results['analysis_metadata']['data_quality_score']:.1%}")
            print(f"   Confidence level: {results['analysis_metadata']['confidence_level']}")
            
        elif args.compare:
            symbols = [s.strip().upper() for s in args.compare.split(',')]
            print(f"🔍 Comparing Companies: {', '.join(symbols)}")
            await dashboard.company_tool.print_comparison_report(symbols, 365)
            
        elif args.screen:
            print("📋 Running Investment Screening...")
            screening = await dashboard.get_investment_screening_report()
            
            print(f"\n🎯 INVESTMENT SCREENING RESULTS:")
            print(f"   Criteria: P/E ≤ {screening['screening_criteria']['max_pe']}, "
                  f"Div Yield ≥ {screening['screening_criteria']['min_dividend_yield']:.1%}, "
                  f"ROE ≥ {screening['screening_criteria']['min_roe']:.1%}")
            print(f"   Companies screened: {screening['total_candidates_screened']}")
            print(f"   Passing screen: {screening['companies_passing_screen']} "
                  f"({screening['screen_summary']['pass_rate']:.1%} pass rate)")
            
            print(f"\n🏆 TOP SCREENED COMPANIES:")
            for i, company in enumerate(screening['screened_companies'][:10], 1):
                company_name = company.get('company_name', 'Unknown') or 'Unknown'
                print(f"   {i:2d}. {company['symbol']:<8} P/E: {company.get('metric_value', 0):>6.1f} "
                      f"Div: {company.get('dividend_yield', 0):>5.1%} "
                      f"({company_name[:30]})")
                      
        else:
            # Default: show summary
            await dashboard.print_dashboard_summary()
            
    except Exception as e:
        logger.error(f"Error in dashboard: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await dashboard.cleanup()

if __name__ == "__main__":
    asyncio.run(main())