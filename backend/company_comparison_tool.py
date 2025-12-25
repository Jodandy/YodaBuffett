#!/usr/bin/env python3
"""
Company Comparison Tool

Deep-dive comparison analysis for specific companies:
- Head-to-head fundamental comparisons
- Historical performance analysis  
- Peer group benchmarking
- Risk-adjusted metrics
- Valuation analysis
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CompanyComparison:
    """Container for company comparison data."""
    symbol: str
    company_name: str
    fundamentals: Dict
    price_performance: Dict
    risk_metrics: Dict
    valuation_metrics: Dict
    growth_metrics: Dict

class CompanyComparisonTool:
    """Advanced company comparison and analysis tool."""
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    async def compare_companies(self, symbols: List[str], analysis_period: int = 365) -> Dict:
        """Comprehensive comparison of multiple companies."""
        logger.info(f"🔍 Comparing companies: {', '.join(symbols)}")
        
        comparisons = []
        
        for symbol in symbols:
            logger.info(f"📊 Analyzing {symbol}...")
            
            comparison = CompanyComparison(
                symbol=symbol,
                company_name=await self._get_company_name(symbol),
                fundamentals=await self._get_latest_fundamentals(symbol),
                price_performance=await self._get_price_performance(symbol, analysis_period),
                risk_metrics=await self._get_risk_metrics(symbol, analysis_period),
                valuation_metrics=await self._get_valuation_metrics(symbol),
                growth_metrics=await self._get_growth_metrics(symbol)
            )
            
            comparisons.append(comparison)
            
        # Generate comparison matrix
        comparison_matrix = self._generate_comparison_matrix(comparisons)
        
        # Generate relative rankings
        rankings = self._generate_rankings(comparisons)
        
        return {
            'symbols': symbols,
            'analysis_period_days': analysis_period,
            'generated_at': datetime.now().isoformat(),
            'individual_analysis': [self._comparison_to_dict(comp) for comp in comparisons],
            'comparison_matrix': comparison_matrix,
            'relative_rankings': rankings,
            'summary': self._generate_comparison_summary(comparisons)
        }
        
    async def _get_company_name(self, symbol: str) -> str:
        """Get company name from database."""
        query = "SELECT company_name FROM company_master WHERE primary_ticker = $1"
        result = await self.db_conn.fetchval(query, symbol)
        return result or symbol
        
    async def _get_latest_fundamentals(self, symbol: str) -> Dict:
        """Get latest fundamental metrics for a company."""
        query = """
        SELECT 
            market_cap,
            trailing_pe,
            forward_pe,
            price_to_book,
            price_to_sales,
            ev_to_ebitda,
            dividend_yield,
            dividend_rate,
            return_on_equity,
            return_on_assets,
            profit_margin,
            operating_margin,
            gross_margin,
            total_debt_to_equity,
            current_ratio,
            book_value_per_share,
            total_revenue,
            net_income,
            operating_cash_flow,
            levered_free_cash_flow,
            quarterly_revenue_growth,
            quarterly_earnings_growth,
            earnings_growth,
            revenue_growth
        FROM daily_fundamentals 
        WHERE symbol = $1 
        AND date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date DESC 
        LIMIT 1
        """
        
        result = await self.db_conn.fetchrow(query, symbol)
        return dict(result) if result else {}
        
    async def _get_price_performance(self, symbol: str, days: int) -> Dict:
        """Calculate price performance metrics."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT date, close_price, volume, high_price, low_price
        FROM daily_price_data 
        WHERE symbol = $1 
        AND date >= $2 
        ORDER BY date
        """
        
        prices = await self.db_conn.fetch(query, symbol, start_date)
        
        if len(prices) < 2:
            return {}
            
        # Calculate performance metrics
        start_price = prices[0]['close_price']
        end_price = prices[-1]['close_price']
        
        total_return = (end_price - start_price) / start_price
        
        # Calculate daily returns for volatility
        daily_returns = []
        for i in range(1, len(prices)):
            daily_return = (prices[i]['close_price'] - prices[i-1]['close_price']) / prices[i-1]['close_price']
            daily_returns.append(daily_return)
            
        volatility = np.std(daily_returns) * np.sqrt(252) if daily_returns else 0
        
        # Calculate max drawdown
        peak = start_price
        max_drawdown = 0
        for price_data in prices:
            price = price_data['close_price']
            if price > peak:
                peak = price
            drawdown = (peak - price) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                
        # Average volume and trading activity
        avg_volume = np.mean([p['volume'] for p in prices if p['volume']])
        avg_daily_range = np.mean([(p['high_price'] - p['low_price']) / p['close_price'] for p in prices])
        
        return {
            'start_price': start_price,
            'end_price': end_price,
            'total_return': total_return,
            'annualized_return': (1 + total_return) ** (365 / days) - 1,
            'volatility': volatility,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': (total_return * (365 / days)) / volatility if volatility > 0 else None,
            'avg_volume': avg_volume,
            'avg_daily_range': avg_daily_range,
            'trading_days': len(prices)
        }
        
    async def _get_risk_metrics(self, symbol: str, days: int) -> Dict:
        """Calculate risk-adjusted metrics."""
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        # Get price data for this stock
        stock_query = """
        SELECT close_price, LAG(close_price) OVER (ORDER BY date) as prev_price
        FROM daily_price_data 
        WHERE symbol = $1 AND date >= $2 
        ORDER BY date
        """
        
        stock_prices = await self.db_conn.fetch(stock_query, symbol, start_date)
        
        if len(stock_prices) < 10:
            return {}
            
        # Calculate returns
        stock_returns = []
        for row in stock_prices[1:]:
            if row['prev_price'] and row['prev_price'] > 0:
                return_val = (row['close_price'] - row['prev_price']) / row['prev_price']
                stock_returns.append(return_val)
                
        if len(stock_returns) < 5:
            return {}
            
        # Calculate beta vs market (using average of all stocks as proxy)
        market_query = """
        SELECT date, AVG(close_price) as market_price
        FROM daily_price_data 
        WHERE date >= $1 
        GROUP BY date 
        ORDER BY date
        """
        
        market_data = await self.db_conn.fetch(market_query, start_date)
        
        market_returns = []
        for i in range(1, len(market_data)):
            if market_data[i-1]['market_price'] > 0:
                market_return = (market_data[i]['market_price'] - market_data[i-1]['market_price']) / market_data[i-1]['market_price']
                market_returns.append(market_return)
                
        # Calculate beta
        beta = None
        if len(market_returns) == len(stock_returns):
            covariance = np.cov(stock_returns, market_returns)[0][1]
            market_variance = np.var(market_returns)
            if market_variance > 0:
                beta = covariance / market_variance
                
        return {
            'volatility': np.std(stock_returns) * np.sqrt(252),
            'beta': beta,
            'downside_deviation': self._calculate_downside_deviation(stock_returns),
            'var_95': np.percentile(stock_returns, 5),  # Value at Risk (95%)
            'skewness': self._calculate_skewness(stock_returns),
            'kurtosis': self._calculate_kurtosis(stock_returns)
        }
        
    def _calculate_downside_deviation(self, returns: List[float]) -> float:
        """Calculate downside deviation (volatility of negative returns)."""
        negative_returns = [r for r in returns if r < 0]
        if not negative_returns:
            return 0
        return np.std(negative_returns) * np.sqrt(252)
        
    def _calculate_skewness(self, returns: List[float]) -> float:
        """Calculate skewness of returns distribution."""
        if len(returns) < 3:
            return 0
        mean = np.mean(returns)
        std = np.std(returns)
        if std == 0:
            return 0
        return np.mean([((r - mean) / std) ** 3 for r in returns])
        
    def _calculate_kurtosis(self, returns: List[float]) -> float:
        """Calculate kurtosis of returns distribution."""
        if len(returns) < 4:
            return 0
        mean = np.mean(returns)
        std = np.std(returns)
        if std == 0:
            return 0
        return np.mean([((r - mean) / std) ** 4 for r in returns]) - 3
        
    async def _get_valuation_metrics(self, symbol: str) -> Dict:
        """Calculate advanced valuation metrics."""
        fundamentals = await self._get_latest_fundamentals(symbol)
        
        if not fundamentals:
            return {}
            
        # Get current price
        price_query = """
        SELECT close_price 
        FROM daily_price_data 
        WHERE symbol = $1 
        ORDER BY date DESC 
        LIMIT 1
        """
        
        current_price = await self.db_conn.fetchval(price_query, symbol)
        
        valuation = {}
        
        # Basic ratios from fundamentals
        valuation.update({
            'current_price': current_price,
            'market_cap': fundamentals.get('market_cap'),
            'pe_ratio': fundamentals.get('trailing_pe'),
            'pb_ratio': fundamentals.get('price_to_book'),
            'ps_ratio': fundamentals.get('price_to_sales'),
            'ev_ebitda': fundamentals.get('ev_to_ebitda')
        })
        
        # Calculate additional metrics if we have the data
        if fundamentals.get('net_income') and fundamentals.get('market_cap'):
            valuation['earnings_yield'] = fundamentals['net_income'] / fundamentals['market_cap']
            
        if fundamentals.get('levered_free_cash_flow') and fundamentals.get('market_cap'):
            valuation['fcf_yield'] = fundamentals['levered_free_cash_flow'] / fundamentals['market_cap']
            
        if fundamentals.get('book_value_per_share') and current_price:
            valuation['price_to_book_value'] = current_price / fundamentals['book_value_per_share']
            
        return valuation
        
    async def _get_growth_metrics(self, symbol: str) -> Dict:
        """Calculate growth metrics using historical data."""
        # Get historical fundamentals
        query = """
        SELECT 
            date,
            pe_ratio,
            market_cap,
            financial_data_date
        FROM historical_fundamentals_daily 
        WHERE symbol = $1 
        AND date >= CURRENT_DATE - INTERVAL '2 years'
        AND pe_ratio IS NOT NULL
        ORDER BY date
        """
        
        historical = await self.db_conn.fetch(query, symbol)
        
        if len(historical) < 10:
            return {}
            
        # Get recent fundamentals for growth rates
        recent_fundamentals = await self._get_latest_fundamentals(symbol)
        
        growth = {}
        
        # Add growth rates from recent fundamentals
        if recent_fundamentals:
            growth.update({
                'quarterly_revenue_growth': recent_fundamentals.get('quarterly_revenue_growth'),
                'quarterly_earnings_growth': recent_fundamentals.get('quarterly_earnings_growth'),
                'earnings_growth': recent_fundamentals.get('earnings_growth'),
                'revenue_growth': recent_fundamentals.get('revenue_growth')
            })
            
        # Calculate historical growth trends
        if len(historical) >= 4:
            # Market cap growth (as proxy for value growth)
            market_caps = [h['market_cap'] for h in historical if h['market_cap']]
            if len(market_caps) >= 4:
                start_cap = market_caps[0]
                end_cap = market_caps[-1]
                periods = len(market_caps) / 252  # Convert to years
                if start_cap > 0 and periods > 0:
                    growth['historical_market_cap_cagr'] = (end_cap / start_cap) ** (1/periods) - 1
                    
        return growth
        
    def _generate_comparison_matrix(self, comparisons: List[CompanyComparison]) -> Dict:
        """Generate side-by-side comparison matrix."""
        if not comparisons:
            return {}
            
        matrix = {}
        
        # Key metrics for comparison
        key_metrics = [
            'market_cap', 'trailing_pe', 'price_to_book', 'dividend_yield',
            'return_on_equity', 'total_debt_to_equity', 'profit_margin'
        ]
        
        for metric in key_metrics:
            matrix[metric] = {}
            for comp in comparisons:
                matrix[metric][comp.symbol] = comp.fundamentals.get(metric)
                
        # Performance metrics
        performance_metrics = [
            'total_return', 'volatility', 'sharpe_ratio', 'max_drawdown'
        ]
        
        for metric in performance_metrics:
            matrix[metric] = {}
            for comp in comparisons:
                matrix[metric][comp.symbol] = comp.price_performance.get(metric)
                
        return matrix
        
    def _generate_rankings(self, comparisons: List[CompanyComparison]) -> Dict:
        """Generate relative rankings for each metric."""
        rankings = {}
        
        # Metrics where lower is better
        lower_is_better = ['trailing_pe', 'price_to_book', 'total_debt_to_equity', 'volatility', 'max_drawdown']
        
        # Metrics where higher is better  
        higher_is_better = ['dividend_yield', 'return_on_equity', 'profit_margin', 'total_return', 'sharpe_ratio']
        
        # Rank each metric
        all_metrics = lower_is_better + higher_is_better
        
        for metric in all_metrics:
            values = []
            for comp in comparisons:
                value = None
                if metric in ['trailing_pe', 'price_to_book', 'total_debt_to_equity', 'dividend_yield', 
                             'return_on_equity', 'profit_margin']:
                    value = comp.fundamentals.get(metric)
                elif metric in ['volatility', 'max_drawdown', 'total_return', 'sharpe_ratio']:
                    value = comp.price_performance.get(metric)
                    
                if value is not None:
                    values.append((comp.symbol, value))
                    
            # Sort and rank
            if values:
                reverse = metric in higher_is_better
                sorted_values = sorted(values, key=lambda x: x[1], reverse=reverse)
                
                rankings[metric] = {}
                for i, (symbol, value) in enumerate(sorted_values):
                    rankings[metric][symbol] = {
                        'rank': i + 1,
                        'value': value,
                        'percentile': (len(sorted_values) - i) / len(sorted_values) * 100
                    }
                    
        return rankings
        
    def _generate_comparison_summary(self, comparisons: List[CompanyComparison]) -> Dict:
        """Generate executive summary of the comparison."""
        if not comparisons:
            return {}
            
        summary = {
            'companies_analyzed': len(comparisons),
            'analysis_highlights': {},
            'best_performers': {},
            'risk_assessment': {}
        }
        
        # Find best performer in key categories
        categories = {
            'value': 'trailing_pe',
            'growth': 'total_return', 
            'income': 'dividend_yield',
            'quality': 'return_on_equity',
            'safety': 'total_debt_to_equity'
        }
        
        for category, metric in categories.items():
            best_comp = None
            best_value = None
            
            for comp in comparisons:
                value = None
                if metric in comp.fundamentals:
                    value = comp.fundamentals[metric]
                elif metric in comp.price_performance:
                    value = comp.price_performance[metric]
                    
                if value is not None:
                    if best_comp is None:
                        best_comp = comp
                        best_value = value
                    else:
                        # Lower is better for PE and debt ratios
                        if metric in ['trailing_pe', 'total_debt_to_equity']:
                            if value < best_value:
                                best_comp = comp
                                best_value = value
                        else:  # Higher is better
                            if value > best_value:
                                best_comp = comp
                                best_value = value
                                
            if best_comp:
                summary['best_performers'][category] = {
                    'symbol': best_comp.symbol,
                    'company_name': best_comp.company_name,
                    'value': best_value,
                    'metric': metric
                }
                
        return summary
        
    def _comparison_to_dict(self, comparison: CompanyComparison) -> Dict:
        """Convert CompanyComparison to dictionary."""
        return {
            'symbol': comparison.symbol,
            'company_name': comparison.company_name,
            'fundamentals': comparison.fundamentals,
            'price_performance': comparison.price_performance,
            'risk_metrics': comparison.risk_metrics,
            'valuation_metrics': comparison.valuation_metrics,
            'growth_metrics': comparison.growth_metrics
        }
        
    async def print_comparison_report(self, symbols: List[str], days: int = 365):
        """Print formatted comparison report to console."""
        comparison = await self.compare_companies(symbols, days)
        
        print("=" * 100)
        print("🏆 COMPANY COMPARISON ANALYSIS")
        print("=" * 100)
        print(f"Companies: {', '.join(symbols)}")
        print(f"Analysis Period: {days} days")
        print(f"Generated: {comparison['generated_at']}")
        
        # Summary table
        print(f"\n📊 FUNDAMENTAL METRICS COMPARISON:")
        print(f"{'Metric':<20} " + " ".join([f"{s:>12}" for s in symbols]))
        print("-" * (20 + len(symbols) * 13))
        
        key_metrics = [
            ('Market Cap', 'market_cap', '${:,.0f}'),
            ('P/E Ratio', 'trailing_pe', '{:.1f}'),
            ('P/B Ratio', 'price_to_book', '{:.1f}'),
            ('Dividend Yield', 'dividend_yield', '{:.1%}'),
            ('ROE', 'return_on_equity', '{:.1%}'),
            ('Debt/Equity', 'total_debt_to_equity', '{:.1f}'),
            ('Profit Margin', 'profit_margin', '{:.1%}')
        ]
        
        matrix = comparison['comparison_matrix']
        
        for metric_name, metric_key, format_str in key_metrics:
            if metric_key in matrix:
                print(f"{metric_name:<20} ", end="")
                for symbol in symbols:
                    value = matrix[metric_key].get(symbol)
                    if value is not None:
                        try:
                            formatted = format_str.format(value)
                        except (ValueError, TypeError):
                            formatted = str(value)[:10]
                    else:
                        formatted = "N/A"
                    print(f"{formatted:>12} ", end="")
                print()
                
        # Performance comparison
        print(f"\n📈 PERFORMANCE METRICS COMPARISON:")
        print(f"{'Metric':<20} " + " ".join([f"{s:>12}" for s in symbols]))
        print("-" * (20 + len(symbols) * 13))
        
        perf_metrics = [
            ('Total Return', 'total_return', '{:.1%}'),
            ('Volatility', 'volatility', '{:.1%}'),
            ('Sharpe Ratio', 'sharpe_ratio', '{:.2f}'),
            ('Max Drawdown', 'max_drawdown', '{:.1%}')
        ]
        
        for metric_name, metric_key, format_str in perf_metrics:
            if metric_key in matrix:
                print(f"{metric_name:<20} ", end="")
                for symbol in symbols:
                    value = matrix[metric_key].get(symbol)
                    if value is not None:
                        try:
                            formatted = format_str.format(value)
                        except (ValueError, TypeError):
                            formatted = str(value)[:10]
                    else:
                        formatted = "N/A"
                    print(f"{formatted:>12} ", end="")
                print()
                
        # Best performers
        summary = comparison['summary']
        if 'best_performers' in summary:
            print(f"\n🏅 CATEGORY WINNERS:")
            for category, winner in summary['best_performers'].items():
                print(f"   {category.title():<12}: {winner['symbol']} ({winner['company_name']})")
                
        print("\n" + "=" * 100)
        
    async def cleanup(self):
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run company comparison analysis."""
    
    tool = CompanyComparisonTool()
    
    try:
        await tool.setup()
        
        # Example comparison - top Swedish companies
        symbols_to_compare = ['VOLV-B', 'ERIC-B', 'ABB', 'AZN', 'ATCO A']
        
        print("🚀 Starting Company Comparison Analysis")
        print("=" * 60)
        
        # Print formatted comparison report
        await tool.print_comparison_report(symbols_to_compare, 365)
        
        # Generate detailed JSON report
        print("\n📋 Generating detailed comparison report...")
        detailed_comparison = await tool.compare_companies(symbols_to_compare, 365)
        
        filename = f"company_comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(detailed_comparison, f, indent=2, default=str)
            
        print(f"✅ Detailed report saved to {filename}")
        
    except Exception as e:
        logger.error(f"Error during comparison: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await tool.cleanup()

if __name__ == "__main__":
    asyncio.run(main())