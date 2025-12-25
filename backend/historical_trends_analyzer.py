#!/usr/bin/env python3
"""
Historical Trends Analyzer

Comprehensive historical analysis of market and fundamental trends:
- Multi-year fundamental trends (P/E, P/B, ROE evolution)
- Market cycle analysis and patterns
- Valuation expansion/contraction periods
- Interest rate impact analysis
- Economic cycle correlations
- Long-term secular trends vs cyclical movements
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
class TrendAnalysis:
    """Container for trend analysis results."""
    metric_name: str
    time_period: str
    start_value: float
    end_value: float
    change_absolute: float
    change_percentage: float
    trend_direction: str
    volatility: float
    min_value: float
    max_value: float
    min_date: date
    max_date: date

class HistoricalTrendsAnalyzer:
    """Comprehensive historical trends analysis engine."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def get_data_coverage_summary(self) -> Dict:
        """Analyze the coverage and quality of historical data."""
        logger.info("📋 Analyzing data coverage...")
        
        # Historical fundamentals coverage
        fundamentals_query = """
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(*) as total_records,
            AVG(CASE WHEN pe_ratio IS NOT NULL THEN 1.0 ELSE 0.0 END) as pe_coverage,
            AVG(CASE WHEN pb_ratio IS NOT NULL THEN 1.0 ELSE 0.0 END) as pb_coverage,
            AVG(CASE WHEN market_cap IS NOT NULL THEN 1.0 ELSE 0.0 END) as market_cap_coverage
        FROM historical_fundamentals_daily
        """
        
        fundamentals_coverage = await self.db_conn.fetchrow(fundamentals_query)
        
        # Price data coverage
        price_query = """
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(*) as total_records,
            AVG(volume) as avg_volume
        FROM daily_price_data
        """
        
        price_coverage = await self.db_conn.fetchrow(price_query)
        
        # Daily fundamentals coverage
        daily_fundamentals_query = """
        SELECT 
            MIN(date) as earliest_date,
            MAX(date) as latest_date,
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT symbol) as unique_symbols,
            COUNT(*) as total_records
        FROM daily_fundamentals
        """
        
        daily_fundamentals_coverage = await self.db_conn.fetchrow(daily_fundamentals_query)
        
        return {
            'historical_fundamentals': dict(fundamentals_coverage) if fundamentals_coverage else {},
            'daily_price_data': dict(price_coverage) if price_coverage else {},
            'daily_fundamentals': dict(daily_fundamentals_coverage) if daily_fundamentals_coverage else {}
        }
        
    async def analyze_fundamental_trends(self, years: int = 4) -> Dict:
        """Analyze long-term fundamental trends."""
        logger.info(f"📈 Analyzing fundamental trends over {years} years...")
        
        end_date = date.today()
        start_date = end_date - timedelta(days=years * 365)
        
        # Monthly aggregated fundamentals trends
        query = """
        SELECT 
            DATE_TRUNC('month', date) as month,
            COUNT(DISTINCT symbol) as company_count,
            AVG(pe_ratio) as avg_pe,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pe_ratio) as median_pe,
            AVG(pb_ratio) as avg_pb,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pb_ratio) as median_pb,
            AVG(market_cap::BIGINT) as avg_market_cap,
            SUM(market_cap::BIGINT) as total_market_cap,
            STDDEV(pe_ratio) as pe_volatility,
            COUNT(CASE WHEN pe_ratio > 0 THEN 1 END) as valid_pe_count,
            MIN(pe_ratio) as min_pe,
            MAX(pe_ratio) as max_pe
        FROM historical_fundamentals_daily
        WHERE date >= $1 
        AND pe_ratio > 0 AND pe_ratio < 100
        AND pb_ratio > 0 AND pb_ratio < 20
        GROUP BY DATE_TRUNC('month', date)
        HAVING COUNT(DISTINCT symbol) >= 10  -- Require sufficient company coverage
        ORDER BY month
        """
        
        monthly_data = await self.db_conn.fetch(query, start_date)
        
        if not monthly_data:
            return {}
            
        # Convert to DataFrame for analysis
        df = pd.DataFrame([dict(row) for row in monthly_data])
        df['month'] = pd.to_datetime(df['month'])
        
        # Calculate trends for key metrics
        trends = {}
        
        metrics = {
            'avg_pe': 'Average P/E Ratio',
            'median_pe': 'Median P/E Ratio',
            'avg_pb': 'Average P/B Ratio',
            'median_pb': 'Median P/B Ratio',
            'avg_market_cap': 'Average Market Cap',
            'total_market_cap': 'Total Market Cap'
        }
        
        for metric, description in metrics.items():
            if metric in df.columns and not df[metric].isna().all():
                trend_analysis = self._calculate_trend(df, metric, description)
                trends[metric] = trend_analysis
                
        # Identify significant periods/regimes
        regime_analysis = self._identify_valuation_regimes(df)
        
        return {
            'analysis_period': f"{start_date} to {end_date}",
            'data_points': len(df),
            'fundamental_trends': trends,
            'valuation_regimes': regime_analysis,
            'monthly_data': df.to_dict('records')
        }
        
    def _calculate_trend(self, df: pd.DataFrame, metric: str, description: str) -> Dict:
        """Calculate comprehensive trend analysis for a metric."""
        data = df[metric].dropna()
        
        if len(data) < 2:
            return {}
            
        start_value = data.iloc[0]
        end_value = data.iloc[-1]
        change_absolute = end_value - start_value
        change_percentage = (change_absolute / start_value) * 100 if start_value != 0 else 0
        
        # Determine trend direction
        if abs(change_percentage) < 5:
            trend_direction = "Stable"
        elif change_percentage > 0:
            trend_direction = "Increasing"
        else:
            trend_direction = "Decreasing"
            
        # Calculate volatility
        returns = data.pct_change().dropna()
        volatility = returns.std() * np.sqrt(12) if len(returns) > 1 else 0  # Annualized monthly volatility
        
        # Find extremes
        min_idx = data.idxmin()
        max_idx = data.idxmax()
        min_date = df.iloc[min_idx]['month'] if min_idx in df.index else None
        max_date = df.iloc[max_idx]['month'] if max_idx in df.index else None
        
        return {
            'description': description,
            'start_value': start_value,
            'end_value': end_value,
            'change_absolute': change_absolute,
            'change_percentage': change_percentage,
            'trend_direction': trend_direction,
            'volatility': volatility,
            'min_value': data.min(),
            'max_value': data.max(),
            'min_date': min_date.strftime('%Y-%m-%d') if min_date else None,
            'max_date': max_date.strftime('%Y-%m-%d') if max_date else None,
            'current_vs_min_pct': ((end_value - data.min()) / data.min() * 100) if data.min() != 0 else 0,
            'current_vs_max_pct': ((end_value - data.max()) / data.max() * 100) if data.max() != 0 else 0
        }
        
    def _identify_valuation_regimes(self, df: pd.DataFrame) -> Dict:
        """Identify different valuation regimes/periods."""
        if 'median_pe' not in df.columns or df['median_pe'].isna().all():
            return {}
            
        pe_data = df['median_pe'].dropna()
        
        if len(pe_data) < 6:
            return {}
            
        # Calculate rolling percentiles to identify regime boundaries
        pe_25th = pe_data.quantile(0.25)
        pe_75th = pe_data.quantile(0.75)
        pe_median = pe_data.median()
        
        # Classify each period
        regimes = []
        current_regime = None
        regime_start = None
        
        for idx, pe_value in pe_data.items():
            if pe_value <= pe_25th:
                regime = "Low Valuation"
            elif pe_value >= pe_75th:
                regime = "High Valuation"
            else:
                regime = "Normal Valuation"
                
            if regime != current_regime:
                if current_regime is not None:
                    # Close previous regime
                    regimes.append({
                        'regime': current_regime,
                        'start_date': df.iloc[regime_start]['month'].strftime('%Y-%m-%d'),
                        'end_date': df.iloc[idx]['month'].strftime('%Y-%m-%d'),
                        'duration_months': idx - regime_start,
                        'avg_pe': pe_data[regime_start:idx].mean()
                    })
                    
                current_regime = regime
                regime_start = idx
                
        # Close final regime
        if current_regime is not None and regime_start is not None:
            regimes.append({
                'regime': current_regime,
                'start_date': df.iloc[regime_start]['month'].strftime('%Y-%m-%d'),
                'end_date': df.iloc[-1]['month'].strftime('%Y-%m-%d'),
                'duration_months': len(df) - 1 - regime_start,
                'avg_pe': pe_data[regime_start:].mean()
            })
            
        return {
            'pe_percentiles': {
                '25th': pe_25th,
                '50th': pe_median,
                '75th': pe_75th
            },
            'regimes': regimes,
            'current_regime': regimes[-1]['regime'] if regimes else None
        }
        
    async def analyze_price_trends(self, years: int = 4) -> Dict:
        """Analyze market-wide price trends and patterns."""
        logger.info(f"💰 Analyzing price trends over {years} years...")
        
        end_date = date.today()
        start_date = end_date - timedelta(days=years * 365)
        
        # Weekly market performance
        query = """
        SELECT 
            DATE_TRUNC('week', date) as week,
            COUNT(DISTINCT symbol) as symbols_traded,
            AVG(close_price) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close_price) as median_price,
            SUM(volume * close_price) as total_dollar_volume,
            AVG(volume) as avg_volume,
            AVG((high_price - low_price) / close_price) as avg_daily_range
        FROM daily_price_data
        WHERE date >= $1 
        AND close_price > 0
        AND volume > 0
        GROUP BY DATE_TRUNC('week', date)
        HAVING COUNT(DISTINCT symbol) >= 50  -- Require sufficient market coverage
        ORDER BY week
        """
        
        weekly_data = await self.db_conn.fetch(query, start_date)
        
        if not weekly_data:
            return {}
            
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in weekly_data])
        df['week'] = pd.to_datetime(df['week'])
        
        # Calculate market index (average price evolution)
        df['market_index'] = (df['avg_price'] / df['avg_price'].iloc[0]) * 100
        
        # Calculate market returns
        df['weekly_return'] = df['market_index'].pct_change()
        df['cumulative_return'] = (df['market_index'] / df['market_index'].iloc[0]) - 1
        
        # Analyze volatility regimes
        df['rolling_volatility'] = df['weekly_return'].rolling(window=12).std() * np.sqrt(52)  # 3-month rolling vol
        
        # Calculate key statistics
        total_return = df['cumulative_return'].iloc[-1]
        annualized_return = (1 + total_return) ** (52 / len(df)) - 1  # Assuming weekly data
        
        volatility = df['weekly_return'].std() * np.sqrt(52)
        max_drawdown = self._calculate_max_drawdown(df['market_index'])
        
        # Identify bull/bear periods
        market_regimes = self._identify_market_regimes(df)
        
        return {
            'analysis_period': f"{start_date} to {end_date}",
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': annualized_return / volatility if volatility > 0 else None,
            'market_regimes': market_regimes,
            'current_market_level': df['market_index'].iloc[-1],
            'weeks_analyzed': len(df),
            'avg_weekly_volume': df['total_dollar_volume'].mean()
        }
        
    def _calculate_max_drawdown(self, price_series: pd.Series) -> float:
        """Calculate maximum drawdown from peak."""
        peak = price_series.expanding().max()
        drawdown = (price_series - peak) / peak
        return drawdown.min()
        
    def _identify_market_regimes(self, df: pd.DataFrame) -> List[Dict]:
        """Identify bull and bear market periods."""
        index_data = df['market_index']
        
        # Simple peak/trough analysis for regime identification
        # A bear market is typically a 20% decline from peak
        # A bull market is recovery and new highs
        
        regimes = []
        current_regime = None
        regime_start = 0
        peak_level = index_data.iloc[0]
        trough_level = index_data.iloc[0]
        
        for i, level in enumerate(index_data):
            if current_regime != "Bull" and level > peak_level * 1.1:  # 10% above previous peak
                if current_regime is not None:
                    # End previous regime
                    regimes.append({
                        'type': current_regime,
                        'start_date': df.iloc[regime_start]['week'].strftime('%Y-%m-%d'),
                        'end_date': df.iloc[i]['week'].strftime('%Y-%m-%d'),
                        'duration_weeks': i - regime_start,
                        'return': (level / index_data.iloc[regime_start] - 1) if regime_start < len(index_data) else 0
                    })
                    
                current_regime = "Bull"
                regime_start = i
                peak_level = level
                
            elif current_regime != "Bear" and level < peak_level * 0.8:  # 20% below peak
                if current_regime is not None:
                    regimes.append({
                        'type': current_regime,
                        'start_date': df.iloc[regime_start]['week'].strftime('%Y-%m-%d'),
                        'end_date': df.iloc[i]['week'].strftime('%Y-%m-%d'),
                        'duration_weeks': i - regime_start,
                        'return': (level / index_data.iloc[regime_start] - 1) if regime_start < len(index_data) else 0
                    })
                    
                current_regime = "Bear"
                regime_start = i
                trough_level = level
                
            # Update peaks and troughs
            if level > peak_level:
                peak_level = level
            if level < trough_level:
                trough_level = level
                
        # Close final regime
        if current_regime is not None:
            regimes.append({
                'type': current_regime,
                'start_date': df.iloc[regime_start]['week'].strftime('%Y-%m-%d'),
                'end_date': df.iloc[-1]['week'].strftime('%Y-%m-%d'),
                'duration_weeks': len(df) - 1 - regime_start,
                'return': (index_data.iloc[-1] / index_data.iloc[regime_start] - 1) if regime_start < len(index_data) else 0
            })
            
        return regimes
        
    async def analyze_sector_rotation_patterns(self) -> Dict:
        """Analyze sector rotation patterns over time."""
        logger.info("🔄 Analyzing sector rotation patterns...")
        
        # Get sector performance over time
        query = """
        WITH sector_monthly AS (
            SELECT 
                DATE_TRUNC('month', dpd.date) as month,
                COALESCE(cm.industry, 'Unknown') as sector,
                AVG((dpd.close_price / FIRST_VALUE(dpd.close_price) 
                    OVER (PARTITION BY dpd.symbol, DATE_TRUNC('month', dpd.date) 
                          ORDER BY dpd.date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) - 1) as monthly_return
            FROM daily_price_data dpd
            LEFT JOIN company_master cm ON dpd.symbol = cm.primary_ticker
            WHERE dpd.date >= CURRENT_DATE - INTERVAL '2 years'
            AND dpd.close_price > 0
            GROUP BY DATE_TRUNC('month', dpd.date), COALESCE(cm.industry, 'Unknown')
            HAVING COUNT(DISTINCT dpd.symbol) >= 5
        ),
        sector_rankings AS (
            SELECT 
                month,
                sector,
                monthly_return,
                RANK() OVER (PARTITION BY month ORDER BY monthly_return DESC) as performance_rank
            FROM sector_monthly
            WHERE monthly_return IS NOT NULL
        )
        SELECT 
            sector,
            COUNT(*) as months_in_top3,
            AVG(monthly_return) as avg_monthly_return,
            AVG(performance_rank) as avg_rank,
            STDDEV(monthly_return) as return_volatility,
            COUNT(CASE WHEN performance_rank <= 3 THEN 1 END) as top3_months,
            COUNT(CASE WHEN performance_rank > (SELECT MAX(performance_rank) * 0.7 FROM sector_rankings sr WHERE sr.month = sector_rankings.month) THEN 1 END) as bottom_months
        FROM sector_rankings
        GROUP BY sector
        HAVING COUNT(*) >= 12  -- At least 12 months of data
        ORDER BY avg_monthly_return DESC
        """
        
        rotation_data = await self.db_conn.fetch(query)
        
        if not rotation_data:
            return {}
            
        sectors = [dict(row) for row in rotation_data]
        
        # Calculate rotation metrics
        for sector in sectors:
            total_months = sector.get('months_in_top3', 0)
            if total_months > 0:
                sector['top3_frequency'] = sector['top3_months'] / total_months
                sector['consistency_score'] = 1 / sector['avg_rank'] if sector['avg_rank'] > 0 else 0
                sector['momentum_score'] = sector['avg_monthly_return'] / sector['return_volatility'] if sector['return_volatility'] > 0 else 0
                
        return {
            'sector_rotation_analysis': sectors,
            'strongest_momentum': sorted(sectors, key=lambda x: x.get('momentum_score', 0), reverse=True)[:5],
            'most_consistent': sorted(sectors, key=lambda x: x.get('consistency_score', 0), reverse=True)[:5],
            'analysis_summary': {
                'sectors_analyzed': len(sectors),
                'avg_monthly_volatility': np.mean([s['return_volatility'] for s in sectors if s['return_volatility']]),
                'rotation_intensity': np.std([s['avg_rank'] for s in sectors if s['avg_rank']])
            }
        }
        
    async def print_trends_summary_report(self):
        """Print comprehensive historical trends summary."""
        print("=" * 120)
        print("📈 HISTORICAL TRENDS & MARKET CYCLE ANALYSIS")
        print("=" * 120)
        
        # Data Coverage
        coverage = await self.get_data_coverage_summary()
        print(f"\n📋 DATA COVERAGE SUMMARY:")
        
        if coverage.get('historical_fundamentals'):
            hist_fund = coverage['historical_fundamentals']
            print(f"   Historical Fundamentals: {hist_fund['earliest_date']} to {hist_fund['latest_date']}")
            print(f"     Records: {hist_fund['total_records']:,} | Companies: {hist_fund['unique_symbols']:,}")
            print(f"     P/E Coverage: {hist_fund['pe_coverage']:.1%} | P/B Coverage: {hist_fund['pb_coverage']:.1%}")
            
        if coverage.get('daily_price_data'):
            price = coverage['daily_price_data']
            print(f"   Daily Price Data: {price['earliest_date']} to {price['latest_date']}")
            print(f"     Records: {price['total_records']:,} | Companies: {price['unique_symbols']:,}")
            
        # Fundamental Trends
        fund_trends = await self.analyze_fundamental_trends(4)
        if fund_trends.get('fundamental_trends'):
            print(f"\n📊 FUNDAMENTAL TRENDS (4 Years):")
            print(f"   Data Points: {fund_trends['data_points']} months")
            
            key_trends = ['avg_pe', 'median_pe', 'avg_pb']
            for metric in key_trends:
                if metric in fund_trends['fundamental_trends']:
                    trend = fund_trends['fundamental_trends'][metric]
                    direction_emoji = "📈" if trend['trend_direction'] == "Increasing" else "📉" if trend['trend_direction'] == "Decreasing" else "➡️"
                    print(f"   {direction_emoji} {trend['description']}: {trend['start_value']:.1f} → {trend['end_value']:.1f} ({trend['change_percentage']:+.1f}%)")
                    
        # Valuation Regimes
        if fund_trends.get('valuation_regimes') and fund_trends['valuation_regimes'].get('regimes'):
            print(f"\n💰 VALUATION REGIMES:")
            regimes = fund_trends['valuation_regimes']['regimes']
            for regime in regimes[-3:]:  # Show last 3 regimes
                print(f"   {regime['regime']}: {regime['start_date']} to {regime['end_date']} ({regime['duration_months']} months)")
                
            current = fund_trends['valuation_regimes'].get('current_regime')
            if current:
                print(f"   Current Regime: {current}")
                
        # Price Trends
        price_trends = await self.analyze_price_trends(4)
        if price_trends:
            print(f"\n💰 MARKET PERFORMANCE (4 Years):")
            print(f"   Total Return: {price_trends['total_return']:.1%}")
            print(f"   Annualized Return: {price_trends['annualized_return']:.1%}")
            print(f"   Volatility: {price_trends['volatility']:.1%}")
            print(f"   Max Drawdown: {price_trends['max_drawdown']:.1%}")
            if price_trends.get('sharpe_ratio'):
                print(f"   Sharpe Ratio: {price_trends['sharpe_ratio']:.2f}")
                
        # Market Regimes
        if price_trends.get('market_regimes'):
            print(f"\n🐻🐂 MARKET REGIMES:")
            regimes = price_trends['market_regimes']
            for regime in regimes[-3:]:  # Show last 3 regimes
                regime_emoji = "🐂" if regime['type'] == "Bull" else "🐻"
                print(f"   {regime_emoji} {regime['type']} Market: {regime['start_date']} to {regime['end_date']} ({regime['duration_weeks']} weeks, {regime['return']:+.1%})")
                
        # Sector Rotation
        rotation = await self.analyze_sector_rotation_patterns()
        if rotation.get('strongest_momentum'):
            print(f"\n🔄 SECTOR ROTATION PATTERNS:")
            print(f"   Strongest Momentum Sectors:")
            for i, sector in enumerate(rotation['strongest_momentum'][:5]):
                print(f"     {i+1}. {sector['sector']:<20} (Return: {sector['avg_monthly_return']:.1%}, Score: {sector['momentum_score']:.2f})")
                
        print("\n" + "=" * 120)
        print("Historical trends analysis complete! 📊")
        print("=" * 120)
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run comprehensive historical trends analysis."""
    
    analyzer = HistoricalTrendsAnalyzer()
    
    try:
        await analyzer.setup()
        
        print("🚀 Starting Historical Trends Analysis")
        print("=" * 60)
        
        # Print comprehensive summary report
        await analyzer.print_trends_summary_report()
        
        # Generate detailed reports
        print("\n📋 Generating detailed historical analysis reports...")
        
        coverage = await analyzer.get_data_coverage_summary()
        fundamental_trends = await analyzer.analyze_fundamental_trends(4)
        price_trends = await analyzer.analyze_price_trends(4)
        sector_rotation = await analyzer.analyze_sector_rotation_patterns()
        
        # Comprehensive report
        comprehensive_report = {
            'generated_at': datetime.now().isoformat(),
            'data_coverage': coverage,
            'fundamental_trends': fundamental_trends,
            'price_trends': price_trends,
            'sector_rotation': sector_rotation
        }
        
        filename = f"historical_trends_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(comprehensive_report, f, indent=2, default=str)
            
        print(f"✅ Comprehensive report saved to {filename}")
        
        # Summary statistics
        print(f"\n📈 ANALYSIS SUMMARY:")
        if fundamental_trends:
            print(f"   Fundamental trends: {fundamental_trends.get('data_points', 0)} months analyzed")
        if price_trends:
            print(f"   Price trends: {price_trends.get('weeks_analyzed', 0)} weeks analyzed")
        if sector_rotation:
            print(f"   Sector analysis: {len(sector_rotation.get('sector_rotation_analysis', []))} sectors analyzed")
            
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())