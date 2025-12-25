#!/usr/bin/env python3
"""
A/B Share Analyzer

Finds companies with both A and B share classes in the Nordic market
and analyzes price differences, trading patterns, and arbitrage opportunities.
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import json
import logging
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SharePair:
    """Represents an A/B share pair."""
    company_base: str
    a_symbol: str
    b_symbol: str
    a_price: Optional[float]
    b_price: Optional[float]
    price_ratio: Optional[float]  # A_price / B_price
    a_volume: Optional[float]
    b_volume: Optional[float]
    volume_ratio: Optional[float]  # A_volume / B_volume
    spread_pct: Optional[float]  # (A_price - B_price) / B_price

@dataclass
class HistoricalSpread:
    """Historical spread analysis for a share pair."""
    pair: SharePair
    avg_spread: float
    std_spread: float
    max_spread: float
    min_spread: float
    current_spread: float
    z_score: float  # How many standard deviations from mean
    arbitrage_signal: str  # "Buy A", "Buy B", "Neutral"

class ABShareAnalyzer:
    """
    Analyzes A and B share classes of Nordic companies.
    
    Looks for:
    - Price discrepancies between share classes
    - Trading volume differences
    - Arbitrage opportunities
    - Historical spread patterns
    """
    
    def __init__(self):
        self.db_conn = None
        
    async def setup(self):
        """Initialize database connection."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
    def extract_company_base(self, symbol: str) -> str:
        """
        Extract the company base name from symbol.
        
        Examples:
        ACRI A -> ACRI
        BEIA B -> BEIA  
        VOLV-B -> VOLV
        ATCO A -> ATCO
        """
        # Remove common A/B suffixes
        symbol = symbol.strip().upper()
        
        # Only extract if it's CLEARLY a space-separated A or B class
        if symbol.endswith(' A') or symbol.endswith(' B'):
            return symbol[:-2]  # Remove space + letter
            
        # Or dash-separated
        if symbol.endswith('-A') or symbol.endswith('-B'):
            return symbol[:-2]  # Remove dash + letter
            
        # Don't extract from single letters like ABB -> AB
        return symbol
        
    async def find_ab_pairs(self) -> List[SharePair]:
        """Find companies with both A and B share classes."""
        
        logger.info("🔍 Searching for A/B share pairs...")
        
        # Get all symbols with recent price data
        query = """
        SELECT DISTINCT 
            p.symbol,
            p.close_price,
            p.volume,
            cm.company_name
        FROM daily_price_data p
        LEFT JOIN company_master cm ON p.symbol = cm.primary_ticker
        WHERE p.date >= CURRENT_DATE - INTERVAL '7 days'
        AND p.close_price > 0
        ORDER BY p.symbol
        """
        
        symbols_data = await self.db_conn.fetch(query)
        
        # Group by company base name
        company_groups = defaultdict(list)
        
        for row in symbols_data:
            symbol = row['symbol']
            base_name = self.extract_company_base(symbol)
            
            company_groups[base_name].append({
                'symbol': symbol,
                'price': float(row['close_price']),
                'volume': float(row['volume']) if row['volume'] else 0,
                'company_name': row['company_name']
            })
            
        # Debug: Check for A and B symbols specifically
        a_symbols = []
        b_symbols = []
        
        for row in symbols_data:
            symbol = row['symbol']
            if symbol.endswith(' A') or symbol.endswith('-A') or symbol.endswith('A'):
                a_symbols.append(symbol)
            elif symbol.endswith(' B') or symbol.endswith('-B') or symbol.endswith('B'):
                b_symbols.append(symbol)
                
        logger.info(f"Found {len(a_symbols)} A symbols: {a_symbols[:10]}")
        logger.info(f"Found {len(b_symbols)} B symbols: {b_symbols[:10]}")
        
        # Debug: print some groupings
        logger.info("Sample company groupings:")
        for base, stocks in list(company_groups.items())[:10]:
            if len(stocks) > 1:
                symbols = [s['symbol'] for s in stocks]
                logger.info(f"  {base}: {symbols}")
        
        # Find pairs
        pairs = []
        
        for base_name, stocks in company_groups.items():
            if len(stocks) >= 2:
                # Look for A and B variants
                a_stocks = []
                b_stocks = []
                
                for stock in stocks:
                    symbol = stock['symbol']
                    
                    # Only count CLEAR A/B class shares
                    if symbol.endswith(' A') or symbol.endswith('-A'):
                        a_stocks.append(stock)
                    elif symbol.endswith(' B') or symbol.endswith('-B'):
                        b_stocks.append(stock)
                
                # Create pairs
                for a_stock in a_stocks:
                    for b_stock in b_stocks:
                        # Calculate metrics
                        price_ratio = a_stock['price'] / b_stock['price'] if b_stock['price'] > 0 else None
                        volume_ratio = a_stock['volume'] / b_stock['volume'] if b_stock['volume'] > 0 else None
                        spread_pct = (a_stock['price'] - b_stock['price']) / b_stock['price'] if b_stock['price'] > 0 else None
                        
                        pair = SharePair(
                            company_base=base_name,
                            a_symbol=a_stock['symbol'],
                            b_symbol=b_stock['symbol'],
                            a_price=a_stock['price'],
                            b_price=b_stock['price'],
                            price_ratio=price_ratio,
                            a_volume=a_stock['volume'],
                            b_volume=b_stock['volume'],
                            volume_ratio=volume_ratio,
                            spread_pct=spread_pct
                        )
                        
                        pairs.append(pair)
                        
        logger.info(f"📊 Found {len(pairs)} A/B share pairs")
        return pairs
        
    async def analyze_historical_spreads(self, pairs: List[SharePair], 
                                       lookback_days: int = 90) -> List[HistoricalSpread]:
        """Analyze historical price spreads for arbitrage opportunities."""
        
        logger.info(f"📈 Analyzing historical spreads over {lookback_days} days...")
        
        historical_analyses = []
        
        for pair in pairs:
            # Get historical data for both symbols
            start_date = date.today() - timedelta(days=lookback_days)
            
            query = """
            SELECT 
                date,
                symbol,
                close_price
            FROM daily_price_data
            WHERE symbol IN ($1, $2)
            AND date >= $3
            AND close_price > 0
            ORDER BY date
            """
            
            historical_data = await self.db_conn.fetch(
                query, pair.a_symbol, pair.b_symbol, start_date
            )
            
            if not historical_data:
                continue
                
            # Convert to DataFrame for easier analysis
            df = pd.DataFrame(historical_data)
            df['date'] = pd.to_datetime(df['date'])
            
            # Pivot to get A and B prices on same row
            price_df = df.pivot(index='date', columns='symbol', values='close_price')
            
            if pair.a_symbol not in price_df.columns or pair.b_symbol not in price_df.columns:
                continue
                
            # Calculate spreads
            price_df['spread_pct'] = (price_df[pair.a_symbol] - price_df[pair.b_symbol]) / price_df[pair.b_symbol] * 100
            price_df = price_df.dropna()
            
            if len(price_df) < 10:  # Need minimum data points
                continue
                
            spreads = price_df['spread_pct']
            
            # Calculate statistics
            avg_spread = spreads.mean()
            std_spread = spreads.std()
            max_spread = spreads.max()
            min_spread = spreads.min()
            current_spread = pair.spread_pct * 100 if pair.spread_pct else 0
            
            # Z-score (how far from normal is current spread?)
            z_score = (current_spread - avg_spread) / std_spread if std_spread > 0 else 0
            
            # Arbitrage signal
            if z_score > 2:  # Current spread is unusually high
                arbitrage_signal = "Buy B (sell A)" if current_spread > 0 else "Buy A (sell B)"
            elif z_score < -2:  # Current spread is unusually low
                arbitrage_signal = "Buy A (sell B)" if current_spread < 0 else "Buy B (sell A)"
            else:
                arbitrage_signal = "Neutral"
                
            historical_analysis = HistoricalSpread(
                pair=pair,
                avg_spread=avg_spread,
                std_spread=std_spread,
                max_spread=max_spread,
                min_spread=min_spread,
                current_spread=current_spread,
                z_score=z_score,
                arbitrage_signal=arbitrage_signal
            )
            
            historical_analyses.append(historical_analysis)
            
        return historical_analyses
        
    def print_analysis_report(self, pairs: List[SharePair], 
                            historical_spreads: List[HistoricalSpread]):
        """Print comprehensive A/B share analysis report."""
        
        print("\n" + "="*100)
        print("📊 NORDIC A/B SHARE CLASS ANALYSIS")
        print("="*100)
        
        # Current price comparison
        print(f"\n🏷️ CURRENT A/B SHARE PAIRS ({len(pairs)} pairs):")
        print(f"{'Company':<15} {'A Share':<12} {'B Share':<12} {'A Price':<10} {'B Price':<10} {'Spread':<10} {'A/B Ratio'}")
        print("-" * 100)
        
        for pair in sorted(pairs, key=lambda x: abs(x.spread_pct or 0), reverse=True):
            spread_str = f"{pair.spread_pct:.2%}" if pair.spread_pct else "N/A"
            ratio_str = f"{pair.price_ratio:.3f}" if pair.price_ratio else "N/A"
            
            print(f"{pair.company_base:<15} {pair.a_symbol:<12} {pair.b_symbol:<12} "
                  f"{pair.a_price:<10.2f} {pair.b_price:<10.2f} {spread_str:<10} {ratio_str}")
        
        # Volume analysis
        print(f"\n📊 TRADING VOLUME COMPARISON:")
        print(f"{'Company':<15} {'A Volume':<15} {'B Volume':<15} {'A/B Vol Ratio':<15} {'Liquidity'}")
        print("-" * 100)
        
        for pair in sorted(pairs, key=lambda x: x.volume_ratio or 0, reverse=True):
            vol_ratio_str = f"{pair.volume_ratio:.2f}" if pair.volume_ratio else "N/A"
            
            # Determine liquidity preference
            if pair.volume_ratio and pair.volume_ratio > 2:
                liquidity = "A more liquid"
            elif pair.volume_ratio and pair.volume_ratio < 0.5:
                liquidity = "B more liquid"
            else:
                liquidity = "Similar"
                
            print(f"{pair.company_base:<15} {pair.a_volume:<15,.0f} {pair.b_volume:<15,.0f} "
                  f"{vol_ratio_str:<15} {liquidity}")
        
        # Historical spread analysis
        if historical_spreads:
            print(f"\n📈 HISTORICAL SPREAD ANALYSIS (90-day):")
            print(f"{'Company':<15} {'Avg Spread':<12} {'Current':<10} {'Z-Score':<10} {'Signal':<20} {'Range'}")
            print("-" * 100)
            
            # Sort by arbitrage opportunity (highest absolute z-score)
            sorted_spreads = sorted(historical_spreads, key=lambda x: abs(x.z_score), reverse=True)
            
            for analysis in sorted_spreads:
                avg_str = f"{analysis.avg_spread:.2f}%"
                current_str = f"{analysis.current_spread:.2f}%"
                z_str = f"{analysis.z_score:.2f}"
                range_str = f"{analysis.min_spread:.1f}% to {analysis.max_spread:.1f}%"
                
                print(f"{analysis.pair.company_base:<15} {avg_str:<12} {current_str:<10} "
                      f"{z_str:<10} {analysis.arbitrage_signal:<20} {range_str}")
        
        # Arbitrage opportunities
        strong_signals = [h for h in historical_spreads if abs(h.z_score) > 2]
        if strong_signals:
            print(f"\n🎯 STRONG ARBITRAGE OPPORTUNITIES (|Z-Score| > 2):")
            
            for signal in sorted(strong_signals, key=lambda x: abs(x.z_score), reverse=True):
                direction = "UNUSUALLY HIGH" if signal.z_score > 0 else "UNUSUALLY LOW"
                confidence = "HIGH" if abs(signal.z_score) > 3 else "MODERATE"
                
                print(f"   🚨 {signal.pair.company_base}: {direction} spread ({confidence} confidence)")
                print(f"      Current: {signal.current_spread:.2f}% vs Avg: {signal.avg_spread:.2f}%")
                print(f"      Signal: {signal.arbitrage_signal}")
                print(f"      Z-Score: {signal.z_score:.2f}")
                print()
        
        # Summary statistics
        if pairs:
            spreads = [p.spread_pct for p in pairs if p.spread_pct is not None]
            ratios = [p.price_ratio for p in pairs if p.price_ratio is not None]
            
            print(f"\n📊 SUMMARY STATISTICS:")
            print(f"   Average A/B price spread: {np.mean(spreads):.2%} ± {np.std(spreads):.2%}")
            print(f"   Average A/B price ratio: {np.mean(ratios):.3f} ± {np.std(ratios):.3f}")
            print(f"   Largest spread: {max(spreads, key=abs):.2%}")
            print(f"   Most arbitrage opportunities: {len(strong_signals)} pairs")
        
        print("\n" + "="*100)
        print("Analysis complete! Monitor these spreads for trading opportunities. 📈")
        print("="*100)
        
    def save_detailed_results(self, pairs: List[SharePair], 
                            historical_spreads: List[HistoricalSpread]):
        """Save detailed results to JSON file."""
        
        # Convert to serializable format
        pairs_data = []
        for pair in pairs:
            pairs_data.append({
                'company_base': pair.company_base,
                'a_symbol': pair.a_symbol,
                'b_symbol': pair.b_symbol,
                'a_price': pair.a_price,
                'b_price': pair.b_price,
                'price_ratio': pair.price_ratio,
                'a_volume': pair.a_volume,
                'b_volume': pair.b_volume,
                'volume_ratio': pair.volume_ratio,
                'spread_pct': pair.spread_pct
            })
        
        historical_data = []
        for analysis in historical_spreads:
            historical_data.append({
                'company_base': analysis.pair.company_base,
                'avg_spread': analysis.avg_spread,
                'std_spread': analysis.std_spread,
                'max_spread': analysis.max_spread,
                'min_spread': analysis.min_spread,
                'current_spread': analysis.current_spread,
                'z_score': analysis.z_score,
                'arbitrage_signal': analysis.arbitrage_signal
            })
        
        results = {
            'analysis_date': datetime.now().isoformat(),
            'description': 'Nordic A/B share class analysis and arbitrage opportunities',
            'pairs': pairs_data,
            'historical_spreads': historical_data,
            'summary': {
                'total_pairs': len(pairs),
                'arbitrage_opportunities': len([h for h in historical_spreads if abs(h.z_score) > 2])
            }
        }
        
        filename = f"ab_share_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
            
        logger.info(f"💾 Detailed results saved to {filename}")
        return filename
    
    async def show_similar_companies(self):
        """Show analysis of companies with similar names or structures."""
        
        print("\n" + "="*80)
        print("🏢 NORDIC COMPANIES - MARKET STRUCTURE ANALYSIS")
        print("="*80)
        
        # Get current market snapshot
        query = """
        SELECT 
            p.symbol,
            cm.company_name,
            cm.industry,
            p.close_price,
            p.volume,
            h.market_cap,
            h.pe_ratio
        FROM daily_price_data p
        LEFT JOIN company_master cm ON p.symbol = cm.primary_ticker
        LEFT JOIN historical_fundamentals_daily h ON p.symbol = h.symbol 
            AND p.date = h.date
        WHERE p.date >= CURRENT_DATE - INTERVAL '3 days'
        AND p.close_price > 0
        ORDER BY h.market_cap DESC NULLS LAST
        LIMIT 30
        """
        
        companies = await self.db_conn.fetch(query)
        
        print(f"\n📈 TOP NORDIC COMPANIES BY MARKET CAP:")
        print(f"{'Symbol':<10} {'Company':<30} {'Price':<10} {'Market Cap':<15} {'P/E':<8} {'Volume'}")
        print("-" * 95)
        
        for company in companies:
            symbol = company['symbol']
            name = (company['company_name'] or 'Unknown')[:28]
            price = float(company['close_price'])
            volume = float(company['volume']) if company['volume'] else 0
            market_cap = company['market_cap']
            pe_ratio = company['pe_ratio']
            
            if market_cap:
                cap_str = f"${market_cap/1e9:.1f}B" if market_cap > 1e9 else f"${market_cap/1e6:.0f}M"
            else:
                cap_str = "N/A"
                
            pe_str = f"{pe_ratio:.1f}" if pe_ratio and pe_ratio > 0 and pe_ratio < 999 else "N/A"
            
            print(f"{symbol:<10} {name:<30} ${price:<9.2f} {cap_str:<15} {pe_str:<8} {volume:>12,.0f}")
        
        # Look for companies with interesting naming patterns
        print(f"\n🔤 COMPANIES WITH INTERESTING SYMBOLS:")
        
        all_symbols = [c['symbol'] for c in companies]
        
        # Group by potential patterns
        patterns = {
            'Single Letters': [s for s in all_symbols if len(s) <= 3],
            'Tech/Growth': [s for s in all_symbols if any(term in (s or '') for term in ['TECH', 'SOFT', 'DATA', 'AI'])],
            'Traditional Nordic': [s for s in all_symbols if any(term in (s or '') for term in ['AB', 'ASA', 'OY'])]
        }
        
        for pattern_name, symbols in patterns.items():
            if symbols:
                print(f"   {pattern_name}: {', '.join(symbols[:8])}")
        
        print("\n💡 Note: While no A/B share pairs exist in current data,")
        print("   Nordic markets have historically had dual-class structures.")
        print("   Many companies have unified their shares in recent years.")
        
        print("\n" + "="*80)
        
    async def cleanup(self):
        """Cleanup database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the comprehensive A/B share analysis."""
    
    analyzer = ABShareAnalyzer()
    
    try:
        await analyzer.setup()
        
        # Find A/B share pairs
        pairs = await analyzer.find_ab_pairs()
        
        if not pairs:
            print("ℹ️  No traditional A/B share pairs found in the current Nordic dataset.")
            print("🔍 This is common as many companies have unified their share classes.")
            print("\n📊 Let's analyze similar companies instead...")
            
            # Show companies with similar names that might be related
            await analyzer.show_similar_companies()
            return
        
        # Analyze historical spreads
        historical_spreads = await analyzer.analyze_historical_spreads(pairs)
        
        # Print comprehensive report
        analyzer.print_analysis_report(pairs, historical_spreads)
        
        # Save detailed results
        analyzer.save_detailed_results(pairs, historical_spreads)
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())