#!/usr/bin/env python3
"""
Updated A/B Share Analyzer

Uses instruments.json data to properly analyze A/B share pairs
and show price differences.
"""

import asyncio
import asyncpg
import json
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class SharePair:
    """Represents an A/B/C share pair."""
    company_name: str
    company_base: str
    shares: Dict[str, Dict]  # 'A': {ticker, price, volume}, 'B': {...}
    price_spreads: Dict[str, float]  # 'A_B': spread percentage

@dataclass
class HistoricalSpread:
    """Historical spread analysis for a share pair."""
    pair: SharePair
    avg_spread: float
    std_spread: float
    max_spread: float
    min_spread: float
    current_spread: float
    z_score: float
    arbitrage_signal: str

class UpdatedABShareAnalyzer:
    """
    Analyzes A/B/C share classes using instruments.json mapping.
    """
    
    def __init__(self):
        self.db_conn = None
        self.instruments = []
        self.multi_class_companies = {}
        
    async def setup(self):
        """Initialize database connection and load instruments."""
        DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'
        self.db_conn = await asyncpg.connect(DATABASE_URL)
        
        # Load instruments data
        with open('data/all-nordic-instruments.json', 'r') as f:
            data = json.load(f)
            self.instruments = data['instruments']
            
        logger.info(f"📋 Loaded {len(self.instruments)} instruments")
        
        # Identify multi-class companies
        self._identify_multi_class_companies()
        
    def _identify_multi_class_companies(self):
        """Find companies with multiple share classes from instruments."""
        
        company_shares = defaultdict(list)
        
        for inst in self.instruments:
            ticker = inst.get('ticker', '')
            
            # Check for share class indicators
            if ' A' in ticker or ' B' in ticker or ' C' in ticker:
                parts = ticker.rsplit(' ', 1)
                if len(parts) == 2 and parts[1] in ['A', 'B', 'C']:
                    base_name = parts[0]
                    share_class = parts[1]
                    
                    company_shares[base_name].append({
                        'class': share_class,
                        'ticker': ticker,
                        'yahoo': inst.get('yahoo', ''),
                        'name': inst.get('name', ''),
                        'isin': inst.get('isin', '')
                    })
                    
        # Filter to companies with multiple classes
        self.multi_class_companies = {
            k: v for k, v in company_shares.items() if len(v) > 1
        }
        
        logger.info(f"🔍 Found {len(self.multi_class_companies)} companies with multiple share classes")
        
    async def get_current_prices(self) -> List[SharePair]:
        """Get current prices for all multi-class shares."""
        
        logger.info("💰 Fetching current prices for multi-class companies...")
        
        # Debug: show what we're looking for
        logger.info("Looking for these companies:")
        for company_base, shares in list(self.multi_class_companies.items())[:5]:
            logger.info(f"  {company_base}:")
            for share in shares:
                logger.info(f"    {share['ticker']} -> Yahoo: {share['yahoo']}")
        
        share_pairs = []
        
        for company_base, shares in self.multi_class_companies.items():
            share_data = {}
            
            # Get price data for each share class
            for share in shares:
                ticker = share['ticker']
                share_class = share['class']
                
                # Try different format variations
                ticker_variations = [
                    ticker,                     # Original: "ERIC B"
                    ticker.replace(' ', '-'),   # With dash: "ERIC-B"
                    ticker.replace(' ', ''),    # No space: "ERICB"
                    company_base + '-' + share_class,  # Base-Class: "ERIC-B"
                    company_base + share_class,        # BaseClass: "ERICB"
                ]
                
                # Get latest price data - try different formats
                price_data = await self.db_conn.fetchrow("""
                    SELECT 
                        symbol,
                        close_price,
                        volume,
                        date
                    FROM daily_price_data
                    WHERE symbol = ANY($1)
                    AND close_price > 0
                    ORDER BY date DESC
                    LIMIT 1
                """, ticker_variations)
                
                if not price_data and company_base in ['ATCO', 'ERIC', 'ELUX']:
                    logger.info(f"    No data for {ticker}, tried: {ticker_variations}")
                
                if price_data:
                    share_data[share_class] = {
                        'ticker': ticker,
                        'actual_symbol': price_data['symbol'],  # Store the actual symbol in DB
                        'price': float(price_data['close_price']),
                        'volume': float(price_data['volume']) if price_data['volume'] else 0,
                        'date': price_data['date'],
                        'name': share['name']
                    }
            
            # Calculate spreads if we have multiple share prices
            if len(share_data) >= 2:
                price_spreads = {}
                
                # Calculate all pairwise spreads
                classes = sorted(share_data.keys())
                for i, class1 in enumerate(classes):
                    for class2 in classes[i+1:]:
                        price1 = share_data[class1]['price']
                        price2 = share_data[class2]['price']
                        
                        if price2 > 0:
                            spread = ((price1 - price2) / price2) * 100
                            price_spreads[f"{class1}_{class2}"] = spread
                
                pair = SharePair(
                    company_name=share_data[classes[0]]['name'].split()[0],  # Base company name
                    company_base=company_base,
                    shares=share_data,
                    price_spreads=price_spreads
                )
                
                share_pairs.append(pair)
                
        logger.info(f"📊 Found price data for {len(share_pairs)} multi-class companies")
        return share_pairs
        
    async def analyze_historical_spreads(self, pairs: List[SharePair], 
                                       lookback_days: int = 90) -> List[HistoricalSpread]:
        """Analyze historical price spreads for arbitrage opportunities."""
        
        logger.info(f"📈 Analyzing historical spreads over {lookback_days} days...")
        
        historical_analyses = []
        
        for pair in pairs:
            # Focus on A/B spread if exists, otherwise first available
            spread_key = 'A_B' if 'A_B' in pair.price_spreads else list(pair.price_spreads.keys())[0]
            
            # Get share classes involved
            class1, class2 = spread_key.split('_')
            ticker1 = pair.shares[class1].get('actual_symbol', pair.shares[class1]['ticker'])
            ticker2 = pair.shares[class2].get('actual_symbol', pair.shares[class2]['ticker'])
            
            # Get historical data
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
            
            historical_data = await self.db_conn.fetch(query, ticker1, ticker2, start_date)
            
            if not historical_data:
                continue
                
            # Convert to DataFrame
            df = pd.DataFrame([dict(row) for row in historical_data])
            if len(df) == 0:
                continue
            df['date'] = pd.to_datetime(df['date'])
            
            # Convert decimal to float to avoid type conflicts
            df['close_price'] = df['close_price'].astype(float)
            
            # Pivot to get both prices on same row
            price_df = df.pivot(index='date', columns='symbol', values='close_price')
            
            if ticker1 not in price_df.columns or ticker2 not in price_df.columns:
                continue
                
            # Calculate spreads
            price_df['spread_pct'] = ((price_df[ticker1] - price_df[ticker2]) / price_df[ticker2]) * 100
            price_df = price_df.dropna()
            
            if len(price_df) < 10:
                continue
                
            spreads = price_df['spread_pct']
            
            # Calculate statistics
            avg_spread = spreads.mean()
            std_spread = spreads.std()
            max_spread = spreads.max()
            min_spread = spreads.min()
            current_spread = pair.price_spreads[spread_key]
            
            # Z-score
            z_score = (current_spread - avg_spread) / std_spread if std_spread > 0 else 0
            
            # Arbitrage signal
            if z_score > 2:
                arbitrage_signal = f"Buy {class2} (sell {class1})"
            elif z_score < -2:
                arbitrage_signal = f"Buy {class1} (sell {class2})"
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
        """Print comprehensive A/B/C share analysis report."""
        
        print("\n" + "="*100)
        print("📊 NORDIC A/B/C SHARE CLASS ANALYSIS")
        print("="*100)
        
        # Current price comparison
        print(f"\n🏷️ CURRENT SHARE CLASS PRICES ({len(pairs)} companies with data):")
        print(f"{'Company':<20} {'A Price':<10} {'B Price':<10} {'C Price':<10} {'A/B Spread':<12} {'Volume Ratio'}")
        print("-" * 100)
        
        for pair in sorted(pairs, key=lambda x: abs(list(x.price_spreads.values())[0]) if x.price_spreads else 0, reverse=True):
            company = pair.company_name[:18]
            
            # Get prices
            a_price = f"${pair.shares['A']['price']:.2f}" if 'A' in pair.shares else "N/A"
            b_price = f"${pair.shares['B']['price']:.2f}" if 'B' in pair.shares else "N/A"
            c_price = f"${pair.shares['C']['price']:.2f}" if 'C' in pair.shares else "N/A"
            
            # Get A/B spread
            ab_spread = f"{pair.price_spreads.get('A_B', 0):.2f}%" if 'A_B' in pair.price_spreads else "N/A"
            
            # Volume ratio
            if 'A' in pair.shares and 'B' in pair.shares:
                a_vol = pair.shares['A']['volume']
                b_vol = pair.shares['B']['volume']
                vol_ratio = f"A:{a_vol/b_vol:.1f}x B" if b_vol > 0 else "B no volume"
            else:
                vol_ratio = "N/A"
                
            print(f"{company:<20} {a_price:<10} {b_price:<10} {c_price:<10} {ab_spread:<12} {vol_ratio}")
        
        # Trading volume analysis
        print(f"\n📊 LIQUIDITY ANALYSIS:")
        
        total_a_volume = sum(p.shares.get('A', {}).get('volume', 0) for p in pairs)
        total_b_volume = sum(p.shares.get('B', {}).get('volume', 0) for p in pairs)
        total_c_volume = sum(p.shares.get('C', {}).get('volume', 0) for p in pairs)
        
        print(f"   Total A-share volume: {total_a_volume:>15,.0f}")
        print(f"   Total B-share volume: {total_b_volume:>15,.0f}")
        print(f"   Total C-share volume: {total_c_volume:>15,.0f}")
        
        if total_b_volume > 0:
            print(f"   Overall A/B volume ratio: {total_a_volume/total_b_volume:.2f}")
            
        # Historical spread analysis
        if historical_spreads:
            print(f"\n📈 HISTORICAL SPREAD ANALYSIS (90-day):")
            print(f"{'Company':<20} {'Current':<10} {'Avg (90d)':<12} {'Std Dev':<10} {'Z-Score':<10} {'Signal'}")
            print("-" * 100)
            
            # Sort by arbitrage opportunity
            sorted_spreads = sorted(historical_spreads, key=lambda x: abs(x.z_score), reverse=True)
            
            for analysis in sorted_spreads[:15]:
                company = analysis.pair.company_name[:18]
                current_str = f"{analysis.current_spread:.2f}%"
                avg_str = f"{analysis.avg_spread:.2f}%"
                std_str = f"{analysis.std_spread:.2f}%"
                z_str = f"{analysis.z_score:.2f}"
                
                print(f"{company:<20} {current_str:<10} {avg_str:<12} {std_str:<10} {z_str:<10} {analysis.arbitrage_signal}")
        
        # Strong arbitrage signals
        strong_signals = [h for h in historical_spreads if abs(h.z_score) > 2]
        if strong_signals:
            print(f"\n🎯 ARBITRAGE OPPORTUNITIES (|Z-Score| > 2):")
            
            for signal in sorted(strong_signals, key=lambda x: abs(x.z_score), reverse=True):
                print(f"\n   🚨 {signal.pair.company_name}:")
                print(f"      Current spread: {signal.current_spread:.2f}%")
                print(f"      Historical avg: {signal.avg_spread:.2f}% ± {signal.std_spread:.2f}%")
                print(f"      Z-Score: {signal.z_score:.2f}")
                print(f"      Action: {signal.arbitrage_signal}")
                
        print("\n" + "="*100)
        print("Analysis complete! Monitor spreads for trading opportunities. 📈")
        print("="*100)
        
    async def cleanup(self):
        """Cleanup database connection."""
        if self.db_conn:
            await self.db_conn.close()

async def main():
    """Run the updated A/B share analysis."""
    
    analyzer = UpdatedABShareAnalyzer()
    
    try:
        await analyzer.setup()
        
        # Get current prices
        pairs = await analyzer.get_current_prices()
        
        if not pairs:
            print("❌ No price data found for multi-class companies.")
            print("💡 Consider updating price data from instruments.json first.")
            return
            
        # Analyze historical spreads
        historical_spreads = await analyzer.analyze_historical_spreads(pairs)
        
        # Print report
        analyzer.print_analysis_report(pairs, historical_spreads)
        
    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main())