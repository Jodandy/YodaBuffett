#!/usr/bin/env python3
"""
Fat Pitch Strategy Backtest

For each quarter:
1. Score all companies using weighted dimensions
2. Select top 20
3. Track their actual forward returns (3M, 6M, 12M)
4. Compare to market average

Usage:
    python fat_pitch_backtest.py                    # Full backtest
    python fat_pitch_backtest.py --top 10           # Top 10 instead of 20
    python fat_pitch_backtest.py --weights ml       # Use ML-optimized weights
    python fat_pitch_backtest.py --weights equal    # Equal weights baseline
    python fat_pitch_backtest.py --export           # Export full data to Excel
    python fat_pitch_backtest.py --export --weights equal --top 50  # Custom export
"""

import asyncio
import asyncpg
import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Tuple
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = 'postgresql://yodabuffett:password@localhost:5432/yodabuffett'

# Weight profiles
WEIGHT_PROFILES = {
    # ML-optimized weights based on 12M quintile analysis (2026-02-08)
    # Derived from top 20% vs bottom 20% return differences
    'ml': {
        'capital_allocation': 15,   # +14.3 diff - strongest predictor
        'beneish_mscore': 14,       # +13.6 diff - no manipulation
        'quality': 11,              # +11.0 diff
        'profitability': 11,        # +10.9 diff
        'returns': 10,              # +9.9 diff - ROE/ROIC matters
        'earnings_quality': 7,      # +6.8 diff
        'growth': 4,                # +4.2 diff
        'working_capital': 4,       # +4.2 diff
        'value': 3,                 # +2.6 diff - surprisingly weak
        'risk': 0,                  # +3.4 but counterintuitive
        'momentum': 0,              # contrarian signal - ignore or negative
        'valuation_percentile': 0,  # +1.5 diff - noise
        'sentiment': 0,             # -0.7 diff - no signal
        'financial_health': 0,      # -0.5 diff - doesn't predict
    },
    # Original Fat Pitch weights (for comparison)
    'original': {
        'profitability': 10,
        'returns': 10,
        'growth': 10,
        'financial_health': 10,
        'earnings_quality': 10,
        'capital_allocation': 10,
        'working_capital': 5,
        'beneish_mscore': 5,
        'value': 10,
        'risk': 5,
        'momentum': 5,
        'quality': 10,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Equal weights baseline
    'equal': {
        'profitability': 1,
        'returns': 1,
        'growth': 1,
        'financial_health': 1,
        'earnings_quality': 1,
        'capital_allocation': 1,
        'working_capital': 1,
        'beneish_mscore': 1,
        'value': 1,
        'risk': 1,
        'momentum': 1,
        'quality': 1,
        'valuation_percentile': 1,
        'sentiment': 1,
    },
    # Value-focused
    'value': {
        'value': 25,
        'valuation_percentile': 20,
        'earnings_quality': 15,
        'profitability': 15,
        'financial_health': 10,
        'beneish_mscore': 10,
        'quality': 5,
        'returns': 0,
        'growth': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'risk': 0,
        'momentum': 0,
        'sentiment': 0,
    },
    # Quality-focused
    'quality': {
        'quality': 20,
        'profitability': 15,
        'returns': 15,
        'earnings_quality': 15,
        'capital_allocation': 10,
        'beneish_mscore': 10,
        'financial_health': 10,
        'growth': 5,
        'working_capital': 0,
        'value': 0,
        'risk': 0,
        'momentum': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Contrarian - negative momentum (mean reversion bet)
    'contrarian': {
        'capital_allocation': 15,
        'beneish_mscore': 14,
        'quality': 11,
        'profitability': 11,
        'returns': 10,
        'earnings_quality': 7,
        'growth': 4,
        'working_capital': 4,
        'value': 3,
        'momentum': -10,            # NEGATIVE - bet against recent winners
        'risk': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
        'financial_health': 0,
    },
    # Deep contrarian - all signals reversed
    'anti': {
        'capital_allocation': -15,
        'beneish_mscore': -14,
        'quality': -11,
        'profitability': -11,
        'returns': -10,
        'earnings_quality': -7,
        'growth': -4,
        'working_capital': -4,
        'value': -3,
        'momentum': 0,
        'risk': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
        'financial_health': 0,
    },
    # Pure momentum - ignore fundamentals, just buy what's going up
    'momentum_only': {
        'momentum': 100,
        'profitability': 0,
        'returns': 0,
        'growth': 0,
        'financial_health': 0,
        'earnings_quality': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'beneish_mscore': 0,
        'value': 0,
        'risk': 0,
        'quality': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Benjamin Graham Defensive - cheap, safe, stable
    # Focus: Low P/E, low P/B, strong balance sheet, earnings stability
    'graham': {
        'value': 30,                 # P/E < 15, P/B < 1.5
        'valuation_percentile': 15,  # Historically cheap
        'financial_health': 25,      # Current ratio > 2, low debt
        'earnings_quality': 15,      # Stable earnings
        'risk': -10,                 # Prefer LOW volatility (invert)
        'profitability': 5,          # Must be profitable
        'beneish_mscore': 10,        # No manipulation
        'returns': 0,
        'growth': 0,                 # Graham didn't chase growth
        'capital_allocation': 0,
        'working_capital': 0,
        'momentum': 0,               # Ignored price trends
        'quality': 0,
        'sentiment': 0,
    },
    # Joel Greenblatt Magic Formula - high ROIC + cheap
    # Rank by: Return on Capital + Earnings Yield (inverse P/E)
    'magic_formula': {
        'returns': 40,               # High ROIC/ROE
        'value': 35,                 # High earnings yield (low P/E)
        'profitability': 15,         # Profitable operations
        'beneish_mscore': 10,        # No earnings manipulation
        'financial_health': 0,
        'earnings_quality': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'growth': 0,
        'risk': 0,
        'momentum': 0,
        'quality': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Piotroski F-Score style - improving fundamentals + cheap
    # Focus: Quality improvements, not just current state
    'piotroski': {
        'profitability': 20,         # Positive ROA, CFO
        'earnings_quality': 20,      # CFO > Net Income (accruals)
        'financial_health': 15,      # Improving leverage, liquidity
        'capital_allocation': 15,    # No dilution, good reinvestment
        'value': 20,                 # Must be cheap (low P/B originally)
        'beneish_mscore': 10,        # No manipulation
        'returns': 0,
        'growth': 0,
        'working_capital': 0,
        'risk': 0,
        'momentum': 0,
        'quality': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Warren Buffett / Quality Compounder - moat + returns + reasonable price
    'buffett': {
        'returns': 20,               # High sustainable ROE
        'profitability': 15,         # Strong margins
        'earnings_quality': 15,      # Real cash earnings
        'capital_allocation': 15,    # Good capital deployment
        'financial_health': 10,      # Conservative balance sheet
        'beneish_mscore': 10,        # Honest accounting
        'value': 10,                 # Fair price (not necessarily cheap)
        'quality': 5,                # Overall quality
        'growth': 0,                 # Sustainable, not hyper-growth
        'working_capital': 0,
        'risk': 0,
        'momentum': 0,
        'valuation_percentile': 0,
        'sentiment': 0,
    },
    # Deep Value - extremely cheap, contrarian
    'deep_value': {
        'value': 40,                 # Very cheap on P/E, P/B, EV/EBITDA
        'valuation_percentile': 30,  # At historical lows
        'financial_health': 15,      # Must survive (not bankrupt)
        'beneish_mscore': 10,        # Not fraudulent
        'earnings_quality': 5,       # Some earnings quality
        'profitability': 0,
        'returns': 0,
        'growth': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'risk': 0,
        'momentum': 0,               # Ignore (or could go negative)
        'quality': 0,
        'sentiment': 0,
    },
    # Mark Minervini SEPA / Trend Template
    # Price in uptrend, near 52-week high, strong RS, earnings acceleration
    'minervini': {
        'momentum': 35,              # Strong uptrend, price > MAs, near highs
        'growth': 25,                # Earnings acceleration
        'profitability': 15,         # Must be profitable
        'earnings_quality': 10,      # Real earnings, not accounting tricks
        'quality': 10,               # Overall quality
        'risk': -5,                  # Prefer controlled volatility (not wild)
        'value': 0,                  # Doesn't care about valuation
        'valuation_percentile': 0,
        'financial_health': 0,
        'returns': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'beneish_mscore': 0,
        'sentiment': 0,
    },
    # CANSLIM - William O'Neil
    # C=Current earnings, A=Annual earnings, N=New highs, S=Supply, L=Leader, I=Institutional, M=Market
    'canslim': {
        'growth': 30,                # C + A: Strong earnings growth (25%+ quarterly/annual)
        'momentum': 25,              # N + L: New highs, market leader, high relative strength
        'profitability': 15,         # Must be profitable with good margins
        'earnings_quality': 15,      # Quality earnings, not one-time gains
        'returns': 10,               # High ROE typical of leaders
        'quality': 5,                # Overall quality
        'value': 0,                  # O'Neil: "Don't buy cheap stocks"
        'valuation_percentile': 0,
        'financial_health': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'beneish_mscore': 0,
        'risk': 0,
        'sentiment': 0,
    },
    # Growth at Reasonable Price (GARP) - Peter Lynch style
    # PEG ratio focus: growth but not overpaying
    'garp': {
        'growth': 30,                # Strong growth
        'value': 25,                 # But reasonable valuation (PEG < 1)
        'profitability': 15,         # Profitable
        'earnings_quality': 10,      # Real earnings
        'returns': 10,               # Good returns on capital
        'quality': 5,                # Quality business
        'momentum': 5,               # Some positive momentum
        'financial_health': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'beneish_mscore': 0,
        'valuation_percentile': 0,
        'risk': 0,
        'sentiment': 0,
    },
    # High momentum + quality filter (avoid junk rallies)
    'quality_momentum': {
        'momentum': 40,              # Strong price momentum
        'quality': 20,               # Must be quality company
        'profitability': 15,         # Profitable
        'earnings_quality': 10,      # Real earnings
        'beneish_mscore': 10,        # No manipulation
        'returns': 5,                # Good returns
        'growth': 0,
        'value': 0,
        'valuation_percentile': 0,
        'financial_health': 0,
        'capital_allocation': 0,
        'working_capital': 0,
        'risk': 0,
        'sentiment': 0,
    },
    # THEORETICALLY HORRIBLE - everything we learned NOT to do
    # - Heavy deep value (shown to be INVERSE)
    # - Heavy momentum (no signal)
    # - Prefer HIGH risk/volatility (unstable)
    # - Ignore quality metrics (profitability, returns, earnings_quality)
    # - Ignore manipulation detection (beneish)
    # - Heavy sentiment (no signal)
    'horrible': {
        'value': 30,                 # Deep value = value traps
        'valuation_percentile': 25,  # Historically cheap = falling knives
        'momentum': 20,              # No signal alone
        'risk': 15,                  # HIGH risk = prefer volatile junk
        'sentiment': 10,             # No signal
        'profitability': 0,          # Ignore if profitable
        'returns': 0,                # Ignore ROE/ROIC
        'growth': 0,                 # Ignore growth
        'quality': 0,                # Ignore quality
        'earnings_quality': 0,       # Ignore earnings quality
        'beneish_mscore': 0,         # Allow manipulation
        'capital_allocation': 0,
        'working_capital': 0,
        'financial_health': 0,
    },
    # THEORETICALLY OPTIMAL - based on everything we learned
    # - GARP works: growth + reasonable value
    # - Quality metrics are strong predictors
    # - Beneish filters manipulation (top predictor in ML analysis)
    # - Small momentum to avoid falling knives
    # - Ignore sentiment (no signal)
    'optimal': {
        'growth': 20,                # Growth matters (GARP insight)
        'profitability': 18,         # Strong predictor
        'returns': 15,               # ROE/ROIC matters
        'earnings_quality': 12,      # Real earnings, not accounting tricks
        'beneish_mscore': 12,        # Filter manipulation (top ML predictor)
        'quality': 10,               # Overall quality composite
        'value': 8,                  # Some value, but not extreme
        'capital_allocation': 5,     # Good capital deployment
        'momentum': 0,               # Skip - use as veto instead
        'financial_health': 0,       # Didn't predict well
        'working_capital': 0,
        'valuation_percentile': 0,   # Historical cheapness = traps
        'risk': 0,
        'sentiment': 0,              # No signal
    },
}

DIMENSIONS = list(WEIGHT_PROFILES['ml'].keys())


class FatPitchBacktester:
    """Backtest Fat Pitch strategy across quarters."""

    def __init__(self, top_n: int = 20, weight_profile: str = 'ml', lag_days: int = 0,
                 select_bottom: bool = False, momentum_veto: int = None, min_momentum: int = None,
                 min_liquidity: float = None, max_liquidity: float = None, smart_lag: bool = False):
        self.top_n = top_n
        self.weight_profile = weight_profile
        self.weights = WEIGHT_PROFILES[weight_profile]
        self.lag_days = lag_days  # Conservative lag to avoid look-ahead bias
        self.smart_lag = smart_lag  # Use actual publish dates instead of fixed lag
        self.select_bottom = select_bottom  # Pick worst instead of best
        self.momentum_veto = momentum_veto  # Exclude companies with momentum below this threshold
        self.min_momentum = min_momentum  # Only include companies with momentum >= this (stricter)
        self.min_liquidity = min_liquidity  # Min avg daily dollar volume in millions (price * volume)
        self.max_liquidity = max_liquidity  # Max avg daily dollar volume in millions (exclude mega-caps)
        self.conn = None
        self._publish_date_cache = {}  # Cache for publish date lookups

    async def connect(self):
        self.conn = await asyncpg.connect(DATABASE_URL)

    async def close(self):
        if self.conn:
            await self.conn.close()

    async def _get_companies_with_public_financials(self, score_date: date) -> set:
        """
        Get company IDs whose financials were actually PUBLIC on score_date.

        Logic: Only include companies where the financial statement's publish_date <= score_date.
        This ensures NO LOOK-AHEAD BIAS - we only use data that was actually available.

        Falls back to earnings calendar events if publish_date is missing.
        """
        # Method 1: Use publish_date from financial_statements (most accurate)
        # Find companies whose most recent period had been published by score_date
        rows = await self.conn.fetch("""
            WITH latest_periods AS (
                -- For each symbol, get the most recent period_date <= score_date
                SELECT DISTINCT ON (symbol)
                    symbol,
                    period_date,
                    publish_date
                FROM financial_statements
                WHERE period_date <= $1
                ORDER BY symbol, period_date DESC
            )
            SELECT cm.id as company_id, lp.symbol, lp.period_date, lp.publish_date
            FROM latest_periods lp
            JOIN company_master cm ON cm.yahoo_symbol = lp.symbol
                                   OR cm.yahoo_symbol = lp.symbol || '.ST'
                                   OR cm.yahoo_symbol = lp.symbol || '.OL'
                                   OR cm.yahoo_symbol = lp.symbol || '.CO'
                                   OR cm.yahoo_symbol = lp.symbol || '.HE'
            WHERE lp.publish_date IS NOT NULL
              AND lp.publish_date <= $1
        """, score_date)

        companies_with_publish_date = {r['company_id'] for r in rows}

        # Method 2: For companies without publish_date, use earnings calendar
        # Find companies with earnings events before score_date
        calendar_rows = await self.conn.fetch("""
            WITH latest_earnings AS (
                -- For each company, get the most recent earnings event <= score_date
                SELECT DISTINCT ON (cm.id)
                    cm.id as company_id,
                    nce.event_date,
                    nce.report_period
                FROM nordic_calendar_events nce
                JOIN nordic_companies nc ON nce.company_id = nc.id
                JOIN company_master cm ON cm.yahoo_symbol LIKE nc.ticker || '.%'
                                       OR cm.yahoo_symbol = nc.ticker
                WHERE nce.event_type = 'earnings'
                  AND nce.event_date <= $1
                  AND nce.event_date >= $1 - INTERVAL '120 days'  -- Recent enough to be relevant
                ORDER BY cm.id, nce.event_date DESC
            )
            SELECT company_id FROM latest_earnings
        """, score_date)

        companies_from_calendar = {r['company_id'] for r in calendar_rows}

        # Combine both sources
        all_public = companies_with_publish_date | companies_from_calendar

        logger.debug(f"Smart lag: {len(companies_with_publish_date)} from publish_date, "
                    f"{len(companies_from_calendar)} from calendar, "
                    f"{len(all_public)} total with public financials on {score_date}")

        return all_public

    async def get_quarterly_dates(self) -> List[date]:
        """Get all quarterly dates with sufficient data."""
        rows = await self.conn.fetch('''
            SELECT score_date, COUNT(DISTINCT company_id) as companies
            FROM daily_dimension_scores
            GROUP BY score_date
            HAVING COUNT(DISTINCT company_id) >= 500
            ORDER BY score_date
        ''')
        # Exclude last 12 months (need forward returns)
        cutoff = date.today() - timedelta(days=365)
        dates = [r['score_date'] for r in rows if r['score_date'] < cutoff]
        return dates

    async def get_scored_companies(self, score_date: date) -> pd.DataFrame:
        """Get all companies with dimensions for a date, apply weighted scoring."""

        # Determine which score_date to use for dimension lookup
        if self.smart_lag:
            # Smart lag: use dimension scores from the score_date itself,
            # but filter to only include companies whose financials were PUBLIC
            actual_score_date = await self.conn.fetchval("""
                SELECT MAX(score_date) FROM daily_dimension_scores
                WHERE score_date <= $1
            """, score_date)
            if not actual_score_date:
                return pd.DataFrame()

            # Get set of companies with public financials on score_date
            public_companies = await self._get_companies_with_public_financials(score_date)
            if not public_companies:
                logger.warning(f"Smart lag: No companies with public financials on {score_date}")
                return pd.DataFrame()

        elif self.lag_days > 0:
            # Fixed lag: use dimension scores from lag_days ago
            lagged_date = score_date - timedelta(days=self.lag_days)
            actual_score_date = await self.conn.fetchval("""
                SELECT MAX(score_date) FROM daily_dimension_scores
                WHERE score_date <= $1
            """, lagged_date)
            if not actual_score_date:
                return pd.DataFrame()
            public_companies = None  # No filtering needed with fixed lag
        else:
            # No lag (not recommended for backtesting)
            actual_score_date = score_date
            public_companies = None

        pivot_cases = ",\n            ".join([
            f"MAX(CASE WHEN dimension_code = '{dim}' THEN score END) as {dim}"
            for dim in DIMENSIONS
        ])

        query = f"""
        SELECT
            dds.company_id,
            cm.company_name,
            cm.yahoo_symbol,
            {pivot_cases}
        FROM daily_dimension_scores dds
        JOIN company_master cm ON dds.company_id = cm.id
        WHERE dds.score_date = $1
        GROUP BY dds.company_id, cm.company_name, cm.yahoo_symbol
        HAVING COUNT(DISTINCT dimension_code) >= 8
        """

        rows = await self.conn.fetch(query, actual_score_date)
        df = pd.DataFrame([dict(r) for r in rows])

        if df.empty:
            return df

        # Apply smart lag filter - only keep companies with public financials
        if self.smart_lag and public_companies:
            before_count = len(df)
            df = df[df['company_id'].isin(public_companies)]
            filtered = before_count - len(df)
            if filtered > 0:
                logger.debug(f"Smart lag filtered {filtered} companies without public financials")
            if df.empty:
                return df

        # Convert to float
        for dim in DIMENSIONS:
            if dim in df.columns:
                df[dim] = pd.to_numeric(df[dim], errors='coerce')

        # Calculate weighted score
        df['weighted_score'] = 0.0
        total_weight = sum(abs(w) for w in self.weights.values())  # Use absolute for normalization

        for dim, weight in self.weights.items():
            if dim in df.columns and weight != 0:
                # Normalize dimension to 0-100, handle NaN
                df[dim] = df[dim].fillna(50)  # Neutral for missing
                # Negative weights invert the contribution (high score → low contribution)
                df['weighted_score'] += (df[dim] * weight / total_weight)

        # Apply momentum veto if set - exclude companies with poor momentum
        if self.momentum_veto is not None and 'momentum' in df.columns:
            before_count = len(df)
            df = df[df['momentum'] >= self.momentum_veto]
            vetoed = before_count - len(df)
            if vetoed > 0:
                logger.debug(f"Momentum veto removed {vetoed} companies (< {self.momentum_veto})")

        # Apply min momentum filter - only keep high momentum companies
        if self.min_momentum is not None and 'momentum' in df.columns:
            before_count = len(df)
            df = df[df['momentum'] >= self.min_momentum]
            filtered = before_count - len(df)
            if filtered > 0:
                logger.debug(f"Min momentum filter removed {filtered} companies (< {self.min_momentum})")

        # Apply liquidity filter - exclude illiquid micro-caps or mega-caps
        if (self.min_liquidity is not None or self.max_liquidity is not None) and not df.empty:
            df = await self._apply_liquidity_filter(df, actual_score_date)

        return df

    async def _apply_liquidity_filter(self, df: pd.DataFrame, score_date: date) -> pd.DataFrame:
        """Filter companies by average daily dollar volume (price * volume)."""
        # Get 20-day average dollar volume for each company around the score date
        symbols = df['yahoo_symbol'].dropna().tolist()
        if not symbols:
            return df

        # Build symbol list for query (strip exchange suffix)
        symbol_map = {}  # clean_symbol -> yahoo_symbol
        for sym in symbols:
            clean = sym.split('.')[0].replace('-', ' ') if '.' in sym else sym.replace('-', ' ')
            symbol_map[clean] = sym

        clean_symbols = list(symbol_map.keys())

        # Query average dollar volume over last 20 trading days
        lookback_start = score_date - timedelta(days=30)
        rows = await self.conn.fetch("""
            SELECT symbol, AVG(close_price * volume) as avg_dollar_volume
            FROM daily_price_data
            WHERE symbol = ANY($1)
              AND date >= $2
              AND date <= $3
              AND volume > 0 AND close_price > 0
            GROUP BY symbol
        """, clean_symbols, lookback_start, score_date)

        # Build liquidity lookup (in millions)
        liquidity = {}
        for row in rows:
            yahoo_sym = symbol_map.get(row['symbol'])
            if yahoo_sym:
                liquidity[yahoo_sym] = float(row['avg_dollar_volume']) / 1_000_000  # Convert to millions

        # Filter
        before_count = len(df)
        df['_liquidity'] = df['yahoo_symbol'].map(liquidity)

        # Apply min liquidity filter
        if self.min_liquidity is not None:
            df = df[df['_liquidity'] >= self.min_liquidity]

        # Apply max liquidity filter
        if self.max_liquidity is not None:
            df = df[df['_liquidity'] <= self.max_liquidity]

        df = df.drop(columns=['_liquidity'])
        filtered = before_count - len(df)

        if filtered > 0:
            filter_desc = []
            if self.min_liquidity is not None:
                filter_desc.append(f">= {self.min_liquidity}M")
            if self.max_liquidity is not None:
                filter_desc.append(f"<= {self.max_liquidity}M")
            logger.debug(f"Liquidity filter removed {filtered} companies ({' and '.join(filter_desc)} daily volume)")

        return df

    async def get_forward_returns(self, top_df: pd.DataFrame, entry_date: date) -> Dict:
        """Get forward returns for companies using symbol matching."""

        returns = {}

        for _, row in top_df.iterrows():
            company_id = row['company_id']
            yahoo_symbol = row.get('yahoo_symbol')

            if not yahoo_symbol:
                returns[company_id] = {'3M': None, '6M': None, '12M': None}
                continue

            # Strip exchange suffix and normalize (ATCO-B.ST -> ATCO B, MAERSK-B.CO -> MAERSK B)
            symbol = yahoo_symbol.split('.')[0] if '.' in yahoo_symbol else yahoo_symbol
            symbol = symbol.replace('-', ' ')  # daily_price_data uses spaces not hyphens

            # Get entry price by symbol
            entry_row = await self.conn.fetchrow("""
                SELECT date as entry_date, close_price as entry_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $2 + INTERVAL '7 days'
                AND close_price > 0
                ORDER BY date LIMIT 1
            """, symbol, entry_date)

            if not entry_row:
                returns[company_id] = {'3M': None, '6M': None, '12M': None}
                continue

            entry_dt = entry_row['entry_date']
            entry_price = float(entry_row['entry_price'])
            returns[company_id] = {'entry_price': entry_price}

            for horizon_name, days in [('3M', 63), ('6M', 126), ('12M', 252)]:
                target_start = entry_dt + timedelta(days=days)
                target_end = entry_dt + timedelta(days=days + 14)

                exit_row = await self.conn.fetchrow("""
                    SELECT close_price FROM daily_price_data
                    WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close_price > 0
                    ORDER BY date LIMIT 1
                """, symbol, target_start, target_end)

                if exit_row and entry_price > 0:
                    exit_price = float(exit_row['close_price'])
                    ret = ((exit_price - entry_price) / entry_price) * 100
                    # Filter likely stock splits (>500% or <-90% are suspicious)
                    if -90 <= ret <= 500:
                        returns[company_id][horizon_name] = ret
                    else:
                        returns[company_id][horizon_name] = None  # Exclude split-affected
                else:
                    returns[company_id][horizon_name] = None

        return returns

    async def get_detailed_returns(self, df: pd.DataFrame, entry_date: date) -> pd.DataFrame:
        """Get detailed returns with entry/exit dates and prices for all companies."""

        results = []

        for _, row in df.iterrows():
            company_id = row['company_id']
            yahoo_symbol = row.get('yahoo_symbol')

            result = {
                'company_id': company_id,
                'entry_date': None,
                'entry_price': None,
            }

            if not yahoo_symbol:
                for horizon in ['3M', '6M', '12M']:
                    result[f'{horizon}_exit_date'] = None
                    result[f'{horizon}_exit_price'] = None
                    result[f'{horizon}_return'] = None
                results.append(result)
                continue

            # Strip exchange suffix and normalize
            symbol = yahoo_symbol.split('.')[0] if '.' in yahoo_symbol else yahoo_symbol
            symbol = symbol.replace('-', ' ')

            # Get entry price
            entry_row = await self.conn.fetchrow("""
                SELECT date as entry_date, close_price as entry_price
                FROM daily_price_data
                WHERE symbol = $1 AND date >= $2 AND date <= $2 + INTERVAL '7 days'
                AND close_price > 0
                ORDER BY date LIMIT 1
            """, symbol, entry_date)

            if not entry_row:
                for horizon in ['3M', '6M', '12M']:
                    result[f'{horizon}_exit_date'] = None
                    result[f'{horizon}_exit_price'] = None
                    result[f'{horizon}_return'] = None
                results.append(result)
                continue

            entry_dt = entry_row['entry_date']
            entry_price = float(entry_row['entry_price'])
            result['entry_date'] = entry_dt
            result['entry_price'] = entry_price

            for horizon_name, days in [('3M', 63), ('6M', 126), ('12M', 252)]:
                target_start = entry_dt + timedelta(days=days)
                target_end = entry_dt + timedelta(days=days + 14)

                exit_row = await self.conn.fetchrow("""
                    SELECT date as exit_date, close_price as exit_price
                    FROM daily_price_data
                    WHERE symbol = $1 AND date >= $2 AND date <= $3 AND close_price > 0
                    ORDER BY date LIMIT 1
                """, symbol, target_start, target_end)

                if exit_row and entry_price > 0:
                    exit_dt = exit_row['exit_date']
                    exit_price = float(exit_row['exit_price'])
                    ret = ((exit_price - entry_price) / entry_price) * 100

                    # Flag likely stock splits (returns > 500% or < -90% are suspicious)
                    if ret > 500 or ret < -90:
                        # Mark as None - likely corporate action (split, reverse split)
                        result[f'{horizon_name}_exit_date'] = exit_dt
                        result[f'{horizon_name}_exit_price'] = exit_price
                        result[f'{horizon_name}_return'] = None  # Exclude from analysis
                        result[f'{horizon_name}_flag'] = 'SPLIT_SUSPECTED'
                    else:
                        result[f'{horizon_name}_exit_date'] = exit_dt
                        result[f'{horizon_name}_exit_price'] = exit_price
                        result[f'{horizon_name}_return'] = ret
                else:
                    result[f'{horizon_name}_exit_date'] = None
                    result[f'{horizon_name}_exit_price'] = None
                    result[f'{horizon_name}_return'] = None

            results.append(result)

        return pd.DataFrame(results)

    async def run_backtest_with_export(self) -> Dict:
        """Run backtest and collect full data for export."""
        await self.connect()

        try:
            quarters = await self.get_quarterly_dates()
            logger.info(f"Backtesting {len(quarters)} quarters for export")

            all_company_data = []

            for q_date in quarters:
                logger.info(f"Processing {q_date} for export...")

                # Get ALL scored companies (not just top N)
                df = await self.get_scored_companies(q_date)
                if df.empty:
                    continue

                # Get detailed returns for all companies
                returns_df = await self.get_detailed_returns(df, q_date)

                # Merge dimensions with returns
                merged = df.merge(returns_df, on='company_id', how='left')

                # Add quarter info and rank
                merged['quarter'] = q_date
                merged['rank'] = merged['weighted_score'].rank(ascending=False, method='min').astype(int)

                all_company_data.append(merged)

            # Combine all quarters
            if all_company_data:
                full_df = pd.concat(all_company_data, ignore_index=True)

                # Reorder columns for clarity
                dim_cols = [d for d in DIMENSIONS if d in full_df.columns]
                ordered_cols = [
                    'quarter', 'rank', 'company_name', 'yahoo_symbol', 'weighted_score',
                ] + dim_cols + [
                    'entry_date', 'entry_price',
                    '3M_exit_date', '3M_exit_price', '3M_return',
                    '6M_exit_date', '6M_exit_price', '6M_return',
                    '12M_exit_date', '12M_exit_price', '12M_return',
                ]
                # Only include columns that exist
                ordered_cols = [c for c in ordered_cols if c in full_df.columns]
                full_df = full_df[ordered_cols]

                return {
                    'data': full_df,
                    'weight_profile': self.weight_profile,
                    'weights': self.weights,
                    'top_n': self.top_n,
                    'lag_days': self.lag_days,
                    'momentum_veto': self.momentum_veto,
                    'quarters': len(quarters),
                }

            return {'data': pd.DataFrame()}

        finally:
            await self.close()

    async def run_backtest(self) -> Dict:
        """Run full backtest across all quarters."""
        await self.connect()

        try:
            quarters = await self.get_quarterly_dates()
            logger.info(f"Backtesting {len(quarters)} quarters with {self.weight_profile} weights, top {self.top_n}")

            all_results = []
            quarterly_summaries = []

            for q_date in quarters:
                logger.info(f"Processing {q_date}...")

                # Get scored companies
                df = await self.get_scored_companies(q_date)
                if df.empty:
                    continue

                # Select top N (or bottom N if testing reverse strategy)
                if self.select_bottom:
                    top_n = df.nsmallest(self.top_n, 'weighted_score')
                else:
                    top_n = df.nlargest(self.top_n, 'weighted_score')

                # Get forward returns (using symbol matching)
                returns = await self.get_forward_returns(top_n, q_date)

                # Also get market average (all companies)
                all_returns = await self.get_forward_returns(df, q_date)

                # Build results
                quarter_picks = []
                for _, row in top_n.iterrows():
                    cid = row['company_id']
                    if cid in returns:
                        pick = {
                            'quarter': q_date,
                            'company_name': row['company_name'],
                            'weighted_score': row['weighted_score'],
                            'return_3M': returns[cid].get('3M'),
                            'return_6M': returns[cid].get('6M'),
                            'return_12M': returns[cid].get('12M'),
                        }
                        quarter_picks.append(pick)
                        all_results.append(pick)

                # Calculate quarter summary
                if quarter_picks:
                    picks_df = pd.DataFrame(quarter_picks)

                    # Market median (more robust than mean - excludes outlier moonshots)
                    def robust_avg(returns_list):
                        """Use median and clip extremes for fair comparison."""
                        valid = [r for r in returns_list if r is not None and -90 < r < 200]
                        return np.median(valid) if valid else None

                    market_3m = robust_avg([r.get('3M') for r in all_returns.values()])
                    market_6m = robust_avg([r.get('6M') for r in all_returns.values()])
                    market_12m = robust_avg([r.get('12M') for r in all_returns.values()])

                    summary = {
                        'quarter': q_date,
                        'n_picks': len(picks_df),
                        'avg_score': picks_df['weighted_score'].mean(),
                        'top20_3M': picks_df['return_3M'].mean(),
                        'top20_6M': picks_df['return_6M'].mean(),
                        'top20_12M': picks_df['return_12M'].mean(),
                        'market_3M': market_3m,
                        'market_6M': market_6m,
                        'market_12M': market_12m,
                        'alpha_3M': picks_df['return_3M'].mean() - market_3m if not pd.isna(picks_df['return_3M'].mean()) else None,
                        'alpha_6M': picks_df['return_6M'].mean() - market_6m if not pd.isna(picks_df['return_6M'].mean()) else None,
                        'alpha_12M': picks_df['return_12M'].mean() - market_12m if not pd.isna(picks_df['return_12M'].mean()) else None,
                    }
                    quarterly_summaries.append(summary)

            return {
                'weight_profile': self.weight_profile,
                'weights': self.weights,
                'top_n': self.top_n,
                'lag_days': self.lag_days,
                'smart_lag': self.smart_lag,
                'momentum_veto': self.momentum_veto,
                'min_momentum': self.min_momentum,
                'min_liquidity': self.min_liquidity,
                'max_liquidity': self.max_liquidity,
                'quarters': len(quarterly_summaries),
                'all_picks': all_results,
                'quarterly_summaries': quarterly_summaries,
            }

        finally:
            await self.close()


def print_results(results: Dict):
    """Print formatted backtest results."""

    print("\n" + "=" * 100)
    print(f"FAT PITCH BACKTEST RESULTS")
    print(f"Weight Profile: {results['weight_profile'].upper()}")
    print(f"Top N: {results['top_n']}")
    if results.get('smart_lag'):
        print(f"Lag Mode: SMART LAG (using actual publish dates - NO CHEATING)")
    else:
        print(f"Lag Days: {results.get('lag_days', 0)} {'(NO LOOK-AHEAD BIAS)' if results.get('lag_days', 0) >= 60 else ''}")
    if results.get('momentum_veto'):
        print(f"Momentum Veto: < {results['momentum_veto']} excluded (AVOID VALUE TRAPS)")
    if results.get('min_momentum'):
        print(f"Min Momentum: >= {results['min_momentum']} required (QUALITY + MOMENTUM)")
    if results.get('min_liquidity') or results.get('max_liquidity'):
        liq_parts = []
        if results.get('min_liquidity'):
            liq_parts.append(f">= {results['min_liquidity']}M")
        if results.get('max_liquidity'):
            liq_parts.append(f"<= {results['max_liquidity']}M")
        print(f"Liquidity Filter: {' and '.join(liq_parts)} daily dollar volume")
    print(f"Quarters Tested: {results['quarters']}")
    print("=" * 100)

    # Print weights
    print("\nWeight Configuration:")
    for dim, weight in sorted(results['weights'].items(), key=lambda x: -x[1]):
        if weight > 0:
            bar = '█' * int(weight / 2)
            print(f"  {dim:25s} {weight:3d}% {bar}")

    # Quarterly breakdown
    print("\n" + "-" * 100)
    print(f"{'Quarter':<12} {'Top20 3M':>10} {'Mkt 3M':>10} {'Alpha':>8} │ {'Top20 6M':>10} {'Mkt 6M':>10} {'Alpha':>8} │ {'Top20 12M':>10} {'Mkt 12M':>10} {'Alpha':>8}")
    print("-" * 100)

    for q in results['quarterly_summaries']:
        def fmt(val):
            return f"{val:+.1f}%" if val is not None else "N/A"

        def fmt_alpha(val):
            if val is None:
                return "N/A"
            color = "" if val >= 0 else ""
            return f"{val:+.1f}%"

        print(f"{str(q['quarter']):<12} "
              f"{fmt(q['top20_3M']):>10} {fmt(q['market_3M']):>10} {fmt_alpha(q['alpha_3M']):>8} │ "
              f"{fmt(q['top20_6M']):>10} {fmt(q['market_6M']):>10} {fmt_alpha(q['alpha_6M']):>8} │ "
              f"{fmt(q['top20_12M']):>10} {fmt(q['market_12M']):>10} {fmt_alpha(q['alpha_12M']):>8}")

    # Overall summary
    print("-" * 100)

    summaries = pd.DataFrame(results['quarterly_summaries'])

    avg_alpha_3m = summaries['alpha_3M'].mean()
    avg_alpha_6m = summaries['alpha_6M'].mean()
    avg_alpha_12m = summaries['alpha_12M'].mean()

    win_rate_3m = (summaries['alpha_3M'] > 0).mean() * 100
    win_rate_6m = (summaries['alpha_6M'] > 0).mean() * 100
    win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

    print(f"\n{'SUMMARY':^100}")
    print("-" * 100)
    print(f"{'Metric':<30} {'3 Month':>20} {'6 Month':>20} {'12 Month':>20}")
    print("-" * 100)
    print(f"{'Avg Top 20 Return':<30} {summaries['top20_3M'].mean():>19.1f}% {summaries['top20_6M'].mean():>19.1f}% {summaries['top20_12M'].mean():>19.1f}%")
    print(f"{'Avg Market Return':<30} {summaries['market_3M'].mean():>19.1f}% {summaries['market_6M'].mean():>19.1f}% {summaries['market_12M'].mean():>19.1f}%")
    print(f"{'Avg Alpha (Top20 - Market)':<30} {avg_alpha_3m:>+18.1f}% {avg_alpha_6m:>+18.1f}% {avg_alpha_12m:>+18.1f}%")
    print(f"{'Win Rate (Alpha > 0)':<30} {win_rate_3m:>19.0f}% {win_rate_6m:>19.0f}% {win_rate_12m:>19.0f}%")
    print("-" * 100)

    # Top picks across all quarters
    print(f"\n{'TOP 10 BEST PICKS (12M Return)':^100}")
    print("-" * 100)

    picks_df = pd.DataFrame(results['all_picks'])
    if not picks_df.empty and 'return_12M' in picks_df.columns:
        top_picks = picks_df.dropna(subset=['return_12M']).nlargest(10, 'return_12M')
        print(f"{'Quarter':<12} {'Company':<40} {'Score':>8} {'12M Return':>12}")
        print("-" * 100)
        for _, pick in top_picks.iterrows():
            print(f"{str(pick['quarter']):<12} {pick['company_name'][:38]:<40} {pick['weighted_score']:>7.1f} {pick['return_12M']:>+11.1f}%")

    # Worst picks
    print(f"\n{'TOP 10 WORST PICKS (12M Return)':^100}")
    print("-" * 100)

    if not picks_df.empty and 'return_12M' in picks_df.columns:
        worst_picks = picks_df.dropna(subset=['return_12M']).nsmallest(10, 'return_12M')
        print(f"{'Quarter':<12} {'Company':<40} {'Score':>8} {'12M Return':>12}")
        print("-" * 100)
        for _, pick in worst_picks.iterrows():
            print(f"{str(pick['quarter']):<12} {pick['company_name'][:38]:<40} {pick['weighted_score']:>7.1f} {pick['return_12M']:>+11.1f}%")


def generate_charts(results: Dict, output_prefix: str):
    """Generate visualization charts for backtest results."""
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    df = results['data']
    if df.empty:
        return []

    charts = []
    top_n = results.get('top_n', 20)
    profile = results['weight_profile']

    # Set style
    plt.style.use('seaborn-v0_8-whitegrid')

    # --- Chart 1: Alpha by Quarter ---
    fig, ax = plt.subplots(figsize=(14, 6))

    # Calculate alpha per quarter
    quarters = sorted(df['quarter'].unique())
    alphas_3m, alphas_6m, alphas_12m = [], [], []

    for q in quarters:
        q_df = df[df['quarter'] == q]
        top = q_df[q_df['rank'] <= top_n]

        # Top N average vs market median
        top_12m = top['12M_return'].mean()
        mkt_12m = q_df['12M_return'].median()
        top_6m = top['6M_return'].mean()
        mkt_6m = q_df['6M_return'].median()
        top_3m = top['3M_return'].mean()
        mkt_3m = q_df['3M_return'].median()

        alphas_12m.append(top_12m - mkt_12m if pd.notna(top_12m) and pd.notna(mkt_12m) else 0)
        alphas_6m.append(top_6m - mkt_6m if pd.notna(top_6m) and pd.notna(mkt_6m) else 0)
        alphas_3m.append(top_3m - mkt_3m if pd.notna(top_3m) and pd.notna(mkt_3m) else 0)

    x = np.arange(len(quarters))
    width = 0.25

    bars1 = ax.bar(x - width, alphas_3m, width, label='3M Alpha', color='#3498db', alpha=0.8)
    bars2 = ax.bar(x, alphas_6m, width, label='6M Alpha', color='#9b59b6', alpha=0.8)
    bars3 = ax.bar(x + width, alphas_12m, width, label='12M Alpha', color='#2ecc71', alpha=0.8)

    ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax.set_xlabel('Quarter')
    ax.set_ylabel('Alpha (%)')
    ax.set_title(f'Top {top_n} Alpha vs Market by Quarter ({profile.upper()} weights)')
    ax.set_xticks(x)
    ax.set_xticklabels([str(q)[:10] for q in quarters], rotation=45, ha='right')
    ax.legend()

    # Add average line
    avg_12m = np.nanmean(alphas_12m)
    ax.axhline(y=avg_12m, color='#2ecc71', linestyle='--', linewidth=2, label=f'Avg 12M: {avg_12m:+.1f}%')

    plt.tight_layout()
    chart1 = f"{output_prefix}_alpha_by_quarter.png"
    plt.savefig(chart1, dpi=150)
    plt.close()
    charts.append(chart1)

    # --- Chart 2: Cumulative Returns ---
    fig, ax = plt.subplots(figsize=(14, 6))

    # Calculate cumulative returns for top picks vs market
    top_cumulative = [100]
    mkt_cumulative = [100]

    for q in quarters:
        q_df = df[df['quarter'] == q]
        top = q_df[q_df['rank'] <= top_n]

        # Use 3M returns for more data points
        top_ret = top['3M_return'].mean()
        mkt_ret = q_df['3M_return'].median()

        if pd.notna(top_ret):
            top_cumulative.append(top_cumulative[-1] * (1 + top_ret/100))
        else:
            top_cumulative.append(top_cumulative[-1])

        if pd.notna(mkt_ret):
            mkt_cumulative.append(mkt_cumulative[-1] * (1 + mkt_ret/100))
        else:
            mkt_cumulative.append(mkt_cumulative[-1])

    quarter_labels = ['Start'] + [str(q)[:10] for q in quarters]

    ax.plot(quarter_labels, top_cumulative, 'o-', linewidth=2, markersize=6,
            color='#2ecc71', label=f'Top {top_n} Picks')
    ax.plot(quarter_labels, mkt_cumulative, 's-', linewidth=2, markersize=6,
            color='#95a5a6', label='Market Median')

    ax.fill_between(quarter_labels, top_cumulative, mkt_cumulative,
                    alpha=0.2, color='#2ecc71')

    ax.set_xlabel('Quarter')
    ax.set_ylabel('Cumulative Value (Starting $100)')
    ax.set_title(f'Cumulative Returns: Top {top_n} vs Market ({profile.upper()} weights)')
    ax.legend()
    plt.xticks(rotation=45, ha='right')

    # Add final values
    ax.annotate(f'${top_cumulative[-1]:.0f}',
                xy=(len(quarters), top_cumulative[-1]),
                xytext=(5, 5), textcoords='offset points', fontsize=10, color='#2ecc71')
    ax.annotate(f'${mkt_cumulative[-1]:.0f}',
                xy=(len(quarters), mkt_cumulative[-1]),
                xytext=(5, -10), textcoords='offset points', fontsize=10, color='#95a5a6')

    plt.tight_layout()
    chart2 = f"{output_prefix}_cumulative_returns.png"
    plt.savefig(chart2, dpi=150)
    plt.close()
    charts.append(chart2)

    # --- Chart 3: Dimension Scores - Top vs Bottom Performers ---
    fig, ax = plt.subplots(figsize=(14, 7))

    dimensions = [d for d in DIMENSIONS if d in df.columns]

    # Get top and bottom performers by 12M return
    valid_returns = df.dropna(subset=['12M_return'])
    if len(valid_returns) > 100:
        top_performers = valid_returns.nlargest(100, '12M_return')
        bottom_performers = valid_returns.nsmallest(100, '12M_return')

        top_scores = [top_performers[d].mean() for d in dimensions]
        bottom_scores = [bottom_performers[d].mean() for d in dimensions]

        x = np.arange(len(dimensions))
        width = 0.35

        ax.bar(x - width/2, top_scores, width, label='Top 100 (by 12M return)', color='#2ecc71', alpha=0.8)
        ax.bar(x + width/2, bottom_scores, width, label='Bottom 100 (by 12M return)', color='#e74c3c', alpha=0.8)

        ax.set_xlabel('Dimension')
        ax.set_ylabel('Average Score (0-100)')
        ax.set_title('Dimension Scores: Best vs Worst Performers')
        ax.set_xticks(x)
        ax.set_xticklabels([d.replace('_', '\n') for d in dimensions], rotation=45, ha='right', fontsize=9)
        ax.legend()
        ax.axhline(y=50, color='black', linestyle='--', linewidth=0.5, alpha=0.5)

        plt.tight_layout()
        chart3 = f"{output_prefix}_dimension_comparison.png"
        plt.savefig(chart3, dpi=150)
        plt.close()
        charts.append(chart3)

    # --- Chart 4: Rank vs Return Scatter ---
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))

    for idx, (horizon, color) in enumerate([('3M', '#3498db'), ('6M', '#9b59b6'), ('12M', '#2ecc71')]):
        ax = axes[idx]
        col = f'{horizon}_return'

        # Get data with valid returns
        plot_df = df[['rank', col]].dropna()

        # Scatter plot
        ax.scatter(plot_df['rank'], plot_df[col], alpha=0.3, s=10, color=color)

        # Add trend line (rolling average by rank buckets)
        plot_df = plot_df.sort_values('rank')
        bucket_size = max(1, len(plot_df) // 50)
        rolling_avg = plot_df.groupby(plot_df['rank'] // bucket_size * bucket_size)[col].mean()
        ax.plot(rolling_avg.index, rolling_avg.values, color='red', linewidth=2, label='Trend')

        # Add reference lines
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax.axhline(y=plot_df[col].median(), color='gray', linestyle='--', linewidth=1,
                   label=f'Median: {plot_df[col].median():.1f}%')

        ax.set_xlabel('Rank (1 = Best Score)')
        ax.set_ylabel(f'{horizon} Return (%)')
        ax.set_title(f'Rank vs {horizon} Return')
        ax.legend(fontsize=9)

        # Limit y-axis for readability
        ax.set_ylim(-100, 200)

    plt.suptitle(f'Does Ranking Predict Returns? ({profile.upper()} weights)', y=1.02)
    plt.tight_layout()
    chart_scatter = f"{output_prefix}_rank_vs_return.png"
    plt.savefig(chart_scatter, dpi=150, bbox_inches='tight')
    plt.close()
    charts.append(chart_scatter)

    # --- Chart 5: Return Distribution (histogram) ---
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    for idx, (horizon, color) in enumerate([('3M', '#3498db'), ('6M', '#9b59b6'), ('12M', '#2ecc71')]):
        ax = axes[idx]
        col = f'{horizon}_return'

        # Top N picks
        top_returns = df[df['rank'] <= top_n][col].dropna()
        # All market
        all_returns = df[col].dropna()

        # Clip for visualization
        top_clipped = top_returns.clip(-100, 200)
        all_clipped = all_returns.clip(-100, 200)

        ax.hist(all_clipped, bins=50, alpha=0.5, label='All Companies', color='#95a5a6', density=True)
        ax.hist(top_clipped, bins=30, alpha=0.7, label=f'Top {top_n}', color=color, density=True)

        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.5)
        ax.axvline(x=top_returns.median(), color=color, linestyle='--', linewidth=2,
                   label=f'Top {top_n} Median: {top_returns.median():.1f}%')
        ax.axvline(x=all_returns.median(), color='#95a5a6', linestyle='--', linewidth=2,
                   label=f'Market Median: {all_returns.median():.1f}%')

        ax.set_xlabel(f'{horizon} Return (%)')
        ax.set_ylabel('Density')
        ax.set_title(f'{horizon} Return Distribution')
        ax.legend(fontsize=8)

    plt.suptitle(f'Return Distributions: Top {top_n} vs Market ({profile.upper()} weights)', y=1.02)
    plt.tight_layout()
    chart4 = f"{output_prefix}_return_distributions.png"
    plt.savefig(chart4, dpi=150, bbox_inches='tight')
    plt.close()
    charts.append(chart4)

    return charts


def export_to_excel(results: Dict, filename: str = None):
    """Export full backtest data to Excel file."""
    from datetime import datetime

    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        profile = results['weight_profile']
        filename = f"fat_pitch_export_{profile}_{timestamp}.xlsx"

    df = results['data']
    if df.empty:
        logger.error("No data to export")
        return None

    # Get unique quarters sorted
    quarters = sorted(df['quarter'].unique())
    top_n = results.get('top_n', 20)

    # Create Excel writer with multiple sheets
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Sheet 1: Full ranked data (all companies, all quarters)
        df.to_excel(writer, sheet_name='All Companies', index=False)

        # Sheet 2: Summary by quarter
        summary = df.groupby('quarter').agg({
            'company_name': 'count',
            'weighted_score': 'mean',
            '3M_return': 'mean',
            '6M_return': 'mean',
            '12M_return': 'mean',
        }).rename(columns={'company_name': 'num_companies'})
        summary.to_excel(writer, sheet_name='Summary')

        # Sheet 3: Top picks only (rank <= top_n)
        top_picks = df[df['rank'] <= top_n].copy()
        top_picks.to_excel(writer, sheet_name=f'Top {top_n} Picks', index=False)

        # Sheets 4+: Individual quarter tabs
        for q in quarters:
            q_df = df[df['quarter'] == q].copy()
            # Format quarter as sheet name (Excel limits to 31 chars)
            q_str = str(q)[:10]  # e.g., "2024-06-30"
            sheet_name = f"Q {q_str}"
            q_df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Weight configuration
        weights_df = pd.DataFrame([
            {'dimension': dim, 'weight': w}
            for dim, w in sorted(results['weights'].items(), key=lambda x: -abs(x[1]))
        ])
        weights_df.to_excel(writer, sheet_name='Weights', index=False)

        # Metadata
        meta = pd.DataFrame([{
            'weight_profile': results['weight_profile'],
            'top_n': results['top_n'],
            'lag_days': results['lag_days'],
            'quarters_tested': results['quarters'],
            'total_company_quarters': len(df),
            'export_date': datetime.now().isoformat(),
        }])
        meta.to_excel(writer, sheet_name='Metadata', index=False)

    logger.info(f"Exported to {filename}")

    # Generate charts
    output_prefix = filename.rsplit('.', 1)[0]  # Remove .xlsx extension
    charts = generate_charts(results, output_prefix)

    print(f"\n✅ Exported to: {filename}")
    print(f"   - {len(df):,} company-quarter records")
    print(f"   - {results['quarters']} quarters")
    print(f"   - {len(df['company_name'].unique()):,} unique companies")
    print(f"\nSheets:")
    print(f"   1. All Companies - Full ranked list (all quarters combined)")
    print(f"   2. Summary - Aggregated stats per quarter")
    print(f"   3. Top {top_n} Picks - Only top ranked companies")
    for i, q in enumerate(quarters, 4):
        print(f"   {i}. Q {str(q)[:10]} - All companies for that quarter")
    print(f"   {len(quarters)+4}. Weights - Dimension weights used")
    print(f"   {len(quarters)+5}. Metadata - Export configuration")

    if charts:
        print(f"\n📊 Charts generated:")
        for chart in charts:
            print(f"   - {chart}")

    return filename


async def compare_veto(weight_profile: str = 'equal', lag_days: int = 60, top_n: int = 20):
    """Compare results with and without momentum veto at different thresholds."""
    print("\n" + "=" * 100)
    print(f"MOMENTUM VETO COMPARISON - {weight_profile.upper()} weights, Top {top_n}, lag={lag_days}")
    print("=" * 100)

    thresholds = [None, 30, 40, 50, 60]  # None = no veto
    results = {}

    for threshold in thresholds:
        label = f"veto_{threshold}" if threshold else "no_veto"
        logger.info(f"Running with momentum veto = {threshold}...")
        backtester = FatPitchBacktester(
            top_n=top_n,
            weight_profile=weight_profile,
            lag_days=lag_days,
            momentum_veto=threshold
        )
        results[label] = await backtester.run_backtest()

    # Print comparison table
    print(f"\n{'Threshold':<12} {'Avg Alpha 3M':>14} {'Avg Alpha 6M':>14} {'Avg Alpha 12M':>14} {'Win Rate 12M':>14} {'Worst Pick':>12}")
    print("-" * 85)

    for threshold in thresholds:
        label = f"veto_{threshold}" if threshold else "no_veto"
        res = results[label]
        summaries = pd.DataFrame(res['quarterly_summaries'])
        picks = pd.DataFrame(res['all_picks'])

        avg_alpha_3m = summaries['alpha_3M'].mean()
        avg_alpha_6m = summaries['alpha_6M'].mean()
        avg_alpha_12m = summaries['alpha_12M'].mean()
        win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

        # Find worst pick
        worst_12m = picks['return_12M'].min() if not picks.empty and 'return_12M' in picks.columns else None

        threshold_str = f"< {threshold}" if threshold else "None"
        worst_str = f"{worst_12m:+.0f}%" if worst_12m is not None else "N/A"

        print(f"{threshold_str:<12} {avg_alpha_3m:>+13.1f}% {avg_alpha_6m:>+13.1f}% {avg_alpha_12m:>+13.1f}% {win_rate_12m:>13.0f}% {worst_str:>12}")

    # Summary
    print("-" * 85)
    print("\nInterpretation:")
    print("  - Higher Alpha = better returns vs market")
    print("  - Higher Win Rate = more consistent outperformance")
    print("  - Less negative Worst Pick = fewer value trap disasters")
    print("\nIf veto helps, you'll see: higher alpha, higher win rate, less extreme worst pick")


async def compare_strategies_slope(smart_lag: bool = True):
    """
    Compare strategies by how well their ranking predicts returns.

    Uses linear regression: rank vs 12M return
    - Negative slope = ranking predicts returns (good)
    - Steeper slope = stronger predictive power
    - R² = how much variance is explained
    """
    from scipy import stats

    print("\n" + "=" * 100)
    print(f"STRATEGY COMPARISON - RANKING PREDICTIVE POWER")
    print(f"Method: Linear regression of Rank vs 12M Return (all companies)")
    print(f"Lag Mode: {'SMART LAG (actual publish dates)' if smart_lag else 'None'}")
    print("=" * 100)

    profiles = ['equal', 'garp', 'piotroski', 'graham', 'magic_formula', 'buffett',
                'minervini', 'canslim', 'quality_momentum', 'value', 'deep_value',
                'contrarian', 'momentum_only', 'ml', 'quality', 'horrible', 'optimal']

    results = {}

    for profile in profiles:
        logger.info(f"Running {profile} strategy...")
        backtester = FatPitchBacktester(
            top_n=9999,  # Get all companies
            weight_profile=profile,
            smart_lag=smart_lag
        )

        # Use export mode to get all company data with ranks
        export_data = await backtester.run_backtest_with_export()
        df = export_data['data']

        if df.empty:
            results[profile] = {'slope': 0, 'r_squared': 0, 'p_value': 1, 'n': 0}
            continue

        # Filter to valid 12M returns
        valid = df[['rank', '12M_return']].dropna()

        if len(valid) < 100:
            results[profile] = {'slope': 0, 'r_squared': 0, 'p_value': 1, 'n': len(valid)}
            continue

        # Linear regression: rank vs return
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            valid['rank'], valid['12M_return']
        )

        results[profile] = {
            'slope': slope,
            'r_squared': r_value ** 2,
            'p_value': p_value,
            'n': len(valid),
            'intercept': intercept,
        }

    # Print results sorted by slope (most negative = best)
    print(f"\n{'Strategy':<18} {'Slope':>10} {'R²':>8} {'P-value':>12} {'N':>8} {'Interpretation':<30}")
    print("-" * 100)

    sorted_results = sorted(results.items(), key=lambda x: x[1]['slope'])

    for profile, r in sorted_results:
        slope = r['slope']
        r_sq = r['r_squared']
        p_val = r['p_value']
        n = r['n']

        # Interpretation
        if slope < -0.02 and p_val < 0.01:
            interp = "✅ STRONG predictor"
        elif slope < -0.01 and p_val < 0.05:
            interp = "✅ Good predictor"
        elif slope < 0 and p_val < 0.1:
            interp = "⚠️ Weak predictor"
        elif slope > 0:
            interp = "❌ INVERSE (bad)"
        else:
            interp = "❌ No signal"

        p_str = f"{p_val:.2e}" if p_val < 0.001 else f"{p_val:.4f}"
        print(f"{profile:<18} {slope:>+10.4f} {r_sq:>7.3f} {p_str:>12} {n:>8} {interp:<30}")

    # Explain the results
    print("-" * 100)
    print("\nInterpretation Guide:")
    print("  Slope: How much 12M return changes per rank position")
    print("         -0.02 means rank 1 has ~20% higher return than rank 1000")
    print("  R²:    Fraction of return variance explained by rank (0-1)")
    print("  P-val: Statistical significance (<0.05 = significant)")
    print("\nBest strategy = Most negative slope with low p-value")


async def run_ensemble_backtest(strategies: List[str] = None, smart_lag: bool = True, use_scores: bool = False):
    """
    Ensemble strategy: combine rankings from multiple strategies.

    Two methods:
    1. Rank sum (default): Sum ranks across strategies (lower = better)
    2. Score average (use_scores=True): Average weighted scores (higher = better)

    This is "wisdom of crowds" - companies that rank well across multiple
    good strategies are probably solid picks.
    """
    from scipy import stats

    if strategies is None:
        # Default: top 5 from slope analysis
        strategies = ['optimal', 'garp', 'quality', 'buffett', 'equal']

    method = "Average of weighted scores" if use_scores else "Sum of ranks"
    print("\n" + "=" * 100)
    print(f"ENSEMBLE STRATEGY - Combining: {', '.join(strategies)}")
    print(f"Method: {method}")
    print(f"Lag Mode: {'SMART LAG (actual publish dates)' if smart_lag else 'None'}")
    print("=" * 100)

    # Collect rankings AND scores from each strategy
    all_rankings = {}  # {strategy: {(symbol, quarter): rank}}
    all_scores = {}    # {strategy: {(symbol, quarter): weighted_score}}
    all_returns = {}   # {(symbol, quarter): 12M_return}

    for strategy in strategies:
        logger.info(f"Getting rankings from {strategy}...")
        backtester = FatPitchBacktester(
            top_n=9999,  # Get all companies
            weight_profile=strategy,
            smart_lag=smart_lag
        )

        export_data = await backtester.run_backtest_with_export()
        df = export_data['data']

        if df.empty:
            continue

        # Store rankings and scores: (symbol, quarter) -> value
        for _, row in df.iterrows():
            key = (row['yahoo_symbol'], row['quarter'])
            if strategy not in all_rankings:
                all_rankings[strategy] = {}
                all_scores[strategy] = {}
            all_rankings[strategy][key] = row['rank']
            all_scores[strategy][key] = row['weighted_score']

            # Store returns (same across strategies)
            if pd.notna(row['12M_return']):
                all_returns[key] = row['12M_return']

    # Combine based on method
    print(f"\nCombining {len(strategies)} strategy rankings...")

    combined_values = {}  # {(symbol, quarter): combined_value}

    # Get all unique (symbol, quarter) pairs
    all_keys = set()
    for strat_ranks in all_rankings.values():
        all_keys.update(strat_ranks.keys())

    for key in all_keys:
        values = []
        for strategy in strategies:
            if use_scores:
                if strategy in all_scores and key in all_scores[strategy]:
                    values.append(all_scores[strategy][key])
            else:
                if strategy in all_rankings and key in all_rankings[strategy]:
                    values.append(all_rankings[strategy][key])

        if len(values) == len(strategies):  # Only if we have values from ALL strategies
            if use_scores:
                combined_values[key] = np.mean(values)  # Average score (higher = better)
            else:
                combined_values[key] = sum(values)  # Sum of ranks (lower = better)

    print(f"Companies with complete data: {len(combined_values)}")

    # Create final ranking from combined values
    if use_scores:
        # Higher score = better, so rank descending
        sorted_keys = sorted(combined_values.keys(), key=lambda k: combined_values[k], reverse=True)
    else:
        # Lower rank sum = better, so rank ascending
        sorted_keys = sorted(combined_values.keys(), key=lambda k: combined_values[k])

    final_ranks = {key: i + 1 for i, key in enumerate(sorted_keys)}

    # Prepare data for slope analysis
    valid_data = []
    for key, final_rank in final_ranks.items():
        if key in all_returns:
            valid_data.append({
                'rank': final_rank,
                '12M_return': all_returns[key],
                'combined_value': combined_values[key]
            })

    df_ensemble = pd.DataFrame(valid_data)

    if len(df_ensemble) < 100:
        print("Not enough data for analysis")
        return

    # Linear regression
    slope, intercept, r_value, p_value, std_err = stats.linregress(
        df_ensemble['rank'], df_ensemble['12M_return']
    )

    print(f"\n{'='*60}")
    print(f"ENSEMBLE RESULTS ({method})")
    print(f"{'='*60}")
    print(f"Strategies combined: {', '.join(strategies)}")
    print(f"Sample size: {len(df_ensemble)}")
    print(f"\nSlope:   {slope:+.4f}")
    print(f"R²:      {r_value**2:.4f}")
    print(f"P-value: {p_value:.2e}")

    # Interpretation
    if slope < -0.02 and p_value < 0.01:
        print(f"\nVerdict: ✅ STRONG predictor")
    elif slope < -0.01 and p_value < 0.05:
        print(f"\nVerdict: ✅ Good predictor")
    elif slope < 0 and p_value < 0.1:
        print(f"\nVerdict: ⚠️ Weak predictor")
    elif slope > 0:
        print(f"\nVerdict: ❌ INVERSE (bad)")
    else:
        print(f"\nVerdict: ❌ No signal")

    # Compare to individual strategies
    print(f"\n{'='*60}")
    print(f"COMPARISON TO INDIVIDUAL STRATEGIES")
    print(f"{'='*60}")
    print(f"{'Strategy':<20} {'Slope':>10}")
    print(f"-" * 35)

    for strategy in strategies:
        strat_backtester = FatPitchBacktester(top_n=9999, weight_profile=strategy, smart_lag=smart_lag)
        strat_data = await strat_backtester.run_backtest_with_export()
        strat_df = strat_data['data']
        valid = strat_df[['rank', '12M_return']].dropna()
        if len(valid) >= 100:
            s, _, _, _, _ = stats.linregress(valid['rank'], valid['12M_return'])
            print(f"{strategy:<20} {s:>+10.4f}")

    print(f"-" * 35)
    print(f"{'ENSEMBLE':<20} {slope:>+10.4f}")

    return {
        'slope': slope,
        'r_squared': r_value ** 2,
        'p_value': p_value,
        'n': len(df_ensemble),
        'strategies': strategies,
        'method': 'scores' if use_scores else 'ranks'
    }


async def compare_strategies(lag_days: int = 0, select_bottom: bool = False, momentum_veto: int = None):
    """Compare different weight profiles."""
    mode = "BOTTOM" if select_bottom else "TOP"
    veto_str = f", momentum_veto={momentum_veto}" if momentum_veto else ""
    print("\n" + "=" * 100)
    print(f"STRATEGY COMPARISON - {mode} 20 (lag={lag_days} days{veto_str})")
    print("=" * 100)

    results = {}
    profiles = ['equal', 'piotroski', 'graham', 'magic_formula', 'buffett',  # Value/Quality
                'minervini', 'canslim', 'garp', 'quality_momentum', 'momentum_only',  # Growth/Momentum
                'value', 'deep_value', 'contrarian',  # Deep value
                'original', 'ml', 'quality']  # Other
    for profile in profiles:
        logger.info(f"Running {profile} strategy ({mode})...")
        backtester = FatPitchBacktester(
            top_n=20,
            weight_profile=profile,
            lag_days=lag_days,
            select_bottom=select_bottom,
            momentum_veto=momentum_veto
        )
        results[profile] = await backtester.run_backtest()

    # Comparison table - Mean (sensitive to outliers)
    print(f"\n{'Profile':<12} {'Mean Alpha 3M':>15} {'Mean Alpha 6M':>15} {'Mean Alpha 12M':>15} {'Win Rate 12M':>15}")
    print("-" * 75)

    for profile, res in results.items():
        summaries = pd.DataFrame(res['quarterly_summaries'])
        avg_alpha_3m = summaries['alpha_3M'].mean()
        avg_alpha_6m = summaries['alpha_6M'].mean()
        avg_alpha_12m = summaries['alpha_12M'].mean()
        win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

        print(f"{profile:<12} {avg_alpha_3m:>+14.1f}% {avg_alpha_6m:>+14.1f}% {avg_alpha_12m:>+14.1f}% {win_rate_12m:>14.0f}%")

    # Comparison table - Median (robust to outliers)
    print(f"\n{'Profile':<12} {'Med Alpha 3M':>15} {'Med Alpha 6M':>15} {'Med Alpha 12M':>15} {'Win Rate 12M':>15}")
    print("-" * 75)

    for profile, res in results.items():
        summaries = pd.DataFrame(res['quarterly_summaries'])
        med_alpha_3m = summaries['alpha_3M'].median()
        med_alpha_6m = summaries['alpha_6M'].median()
        med_alpha_12m = summaries['alpha_12M'].median()
        win_rate_12m = (summaries['alpha_12M'] > 0).mean() * 100

        print(f"{profile:<12} {med_alpha_3m:>+14.1f}% {med_alpha_6m:>+14.1f}% {med_alpha_12m:>+14.1f}% {win_rate_12m:>14.0f}%")


async def main():
    parser = argparse.ArgumentParser(description='Fat Pitch Strategy Backtest')
    parser.add_argument('--top', type=int, default=20, help='Number of top picks per quarter')
    parser.add_argument('--weights', type=str, default='ml',
                        choices=['ml', 'original', 'equal', 'value', 'quality', 'contrarian', 'anti',
                                 'momentum_only', 'graham', 'magic_formula', 'piotroski', 'buffett', 'deep_value',
                                 'minervini', 'canslim', 'garp', 'quality_momentum', 'horrible', 'optimal'],
                        help='Weight profile to use')
    parser.add_argument('--compare', action='store_true', help='Compare all weight profiles')
    parser.add_argument('--compare-slope', action='store_true', dest='compare_slope',
                        help='Compare strategies by ranking predictive power (slope of rank vs return)')
    parser.add_argument('--compare-veto', action='store_true', dest='compare_veto',
                        help='Compare results with different momentum veto thresholds')
    parser.add_argument('--bottom', action='store_true',
                        help='Select BOTTOM N instead of top N (test if signal works both ways)')
    parser.add_argument('--lag', type=int, default=0,
                        help='Days to lag dimension scores (60 = no look-ahead bias)')
    parser.add_argument('--smart-lag', action='store_true', dest='smart_lag',
                        help='Use actual publish dates instead of fixed lag (NO CHEATING)')
    parser.add_argument('--momentum-veto', type=int, default=None, dest='momentum_veto',
                        help='Exclude companies with momentum score below this threshold (e.g., 40)')
    parser.add_argument('--min-momentum', type=int, default=None, dest='min_momentum',
                        help='Only include companies with momentum >= this (e.g., 60 for high momentum)')
    parser.add_argument('--min-liquidity', type=float, default=None, dest='min_liquidity',
                        help='Min avg daily dollar volume in millions (e.g., 1 = 1M daily turnover)')
    parser.add_argument('--max-liquidity', type=float, default=None, dest='max_liquidity',
                        help='Max avg daily dollar volume in millions (e.g., 50 = exclude mega-caps)')
    parser.add_argument('--export', action='store_true',
                        help='Export full data to Excel (all companies, all dimensions, all returns)')
    parser.add_argument('--output', type=str, default=None,
                        help='Output filename for export (default: auto-generated)')
    parser.add_argument('--ensemble', action='store_true',
                        help='Run ensemble strategy combining top 5 strategies')
    parser.add_argument('--ensemble-strategies', type=str, default=None, dest='ensemble_strategies',
                        help='Comma-separated list of strategies to combine (default: optimal,garp,quality,buffett,equal)')
    parser.add_argument('--ensemble-scores', action='store_true', dest='ensemble_scores',
                        help='Use average of weighted scores instead of sum of ranks for ensemble')

    args = parser.parse_args()

    if args.ensemble:
        strategies = None
        if args.ensemble_strategies:
            strategies = [s.strip() for s in args.ensemble_strategies.split(',')]
        await run_ensemble_backtest(strategies=strategies, smart_lag=args.smart_lag, use_scores=args.ensemble_scores)
    elif args.compare_slope:
        await compare_strategies_slope(smart_lag=args.smart_lag)
    elif args.compare_veto:
        await compare_veto(weight_profile=args.weights, lag_days=args.lag, top_n=args.top)
    elif args.compare:
        await compare_strategies(lag_days=args.lag, select_bottom=args.bottom,
                                 momentum_veto=args.momentum_veto)
    elif args.export:
        backtester = FatPitchBacktester(
            top_n=args.top,
            weight_profile=args.weights,
            lag_days=args.lag,
            select_bottom=args.bottom,
            momentum_veto=args.momentum_veto,
            min_momentum=args.min_momentum,
            min_liquidity=args.min_liquidity,
            max_liquidity=args.max_liquidity,
            smart_lag=args.smart_lag
        )
        results = await backtester.run_backtest_with_export()
        export_to_excel(results, args.output)
    else:
        backtester = FatPitchBacktester(
            top_n=args.top,
            weight_profile=args.weights,
            lag_days=args.lag,
            select_bottom=args.bottom,
            momentum_veto=args.momentum_veto,
            min_momentum=args.min_momentum,
            min_liquidity=args.min_liquidity,
            max_liquidity=args.max_liquidity,
            smart_lag=args.smart_lag
        )
        results = await backtester.run_backtest()
        print_results(results)


if __name__ == '__main__':
    asyncio.run(main())
