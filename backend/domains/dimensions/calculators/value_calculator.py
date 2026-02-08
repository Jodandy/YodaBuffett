"""
Value Dimension Calculator

Measures how undervalued a company is relative to fundamentals and peers.
This is a MARKET dimension (price-dependent).

Metrics analyzed:
- P/E ratio (earnings yield inverse)
- P/B ratio (book value discount)
- P/S ratio (revenue multiple)
- EV/EBITDA (enterprise value multiple)
- 52-week price position (technical value)

Key features:
- Calculates ratios from raw financial data (not pre-computed)
- Sector-relative percentile rankings
- Historical valuation context
- Quality filters (excludes negative earnings, etc.)

For ML/LLM integration:
- Valuation context (vs own history, vs peers)
- Relative value ranking
- Premium/discount interpretation
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging
import numpy as np
from scipy import stats

from .base import BaseDimensionCalculator, register_calculator
from .analysis_helpers import (
    MetricAnalysis,
    DimensionAnalysis,
    HistoricalAnalyzer,
    PeerAnalyzer,
    QualityScorer,
    ScoreNormalizer,
    TrendDirection,
    get_metric_threshold,
)
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class ValueCalculator(BaseDimensionCalculator):
    """
    Sophisticated value dimension calculator.

    Measures how undervalued a company is based on:
    - Traditional valuation ratios (P/E, P/B, P/S, EV/EBITDA)
    - Price position in 52-week range
    - Sector-relative rankings
    - Historical valuation context

    Lower valuation multiples = higher value score (inverted for scoring).
    """

    # Metric weights for composite score
    METRIC_WEIGHTS = {
        "pe_ratio": 0.25,
        "pb_ratio": 0.20,
        "ps_ratio": 0.15,
        "ev_ebitda": 0.25,
        "price_52w_position": 0.15,
    }

    # Component weights for final score
    COMPONENT_WEIGHTS = {
        "raw_score": 0.40,      # Absolute valuation level
        "trend_score": 0.15,    # Valuation trend (getting cheaper?)
        "peer_score": 0.30,     # Sector comparison (most important for value)
        "stability_score": 0.15,  # Valuation consistency
    }

    @property
    def dimension_code(self) -> str:
        return "value"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Value",
            description="Measures how undervalued a company is relative to fundamentals and peers",
            category="market",  # Price-dependent
            data_sources=["financial_statements", "balance_sheet_data", "daily_price_data"],
            update_frequency="daily",
            version="2.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate value dimension for a company."""

        company_info = await self._get_company_info(company_id)
        if not company_info:
            logger.warning(f"Company not found: {company_id}")
            return None

        symbol = company_info["primary_ticker"]
        sector = company_info.get("sector")
        currency = company_info.get("report_currency", "SEK")

        # Get required data
        price_data = await self._get_price_data(symbol, score_date)
        financials = await self._get_financials(symbol, score_date)
        balance_sheet = await self._get_balance_sheet(symbol, score_date)

        if not price_data or not financials:
            logger.info(f"Insufficient data for value calculation: {symbol}")
            return None

        # Calculate market cap
        current_price = price_data.get("close_price")
        shares = balance_sheet.get("shares_outstanding") if balance_sheet else None

        if not current_price:
            logger.info(f"No price data for {symbol}")
            return None

        # Create analysis container
        analysis = DimensionAnalysis(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            sector=sector,
            currency=currency,
        )

        # Calculate valuation metrics
        valuation_metrics = await self._calculate_valuation_metrics(
            symbol, score_date, price_data, financials, balance_sheet
        )

        # Analyze each metric
        for metric_name, values in valuation_metrics.items():
            if values and metric_name in self.METRIC_WEIGHTS:
                # For valuation, lower is better (except historical context)
                higher_is_better = metric_name == "price_52w_position"  # Actually inverted later
                metric_analysis = await self._analyze_metric(
                    metric_name=metric_name,
                    values=values,
                    sector=sector,
                    score_date=score_date,
                    higher_is_better=False,  # Lower valuations are better
                )
                analysis.metrics[metric_name] = metric_analysis

        if len(analysis.metrics) < 2:
            logger.info(f"Insufficient valuation metrics for {symbol}")
            return None

        # Calculate component scores
        analysis.raw_score = self._calculate_raw_score(analysis.metrics)
        analysis.trend_score = self._calculate_trend_score(analysis.metrics)
        analysis.peer_score = self._calculate_peer_score(analysis.metrics)
        analysis.stability_score = self._calculate_stability_score(analysis.metrics)

        # Calculate composite score
        component_scores = {
            "raw_score": analysis.raw_score,
            "trend_score": analysis.trend_score,
            "peer_score": analysis.peer_score,
            "stability_score": analysis.stability_score,
        }
        analysis.composite_score = ScoreNormalizer.combine_scores(
            component_scores, self.COMPONENT_WEIGHTS
        )

        # Calculate confidence
        analysis.data_quality = self._calculate_data_quality(analysis.metrics)
        analysis.confidence = analysis.data_quality

        # Score range
        uncertainty = (1 - analysis.confidence) * 15
        analysis.score_low = max(0, analysis.composite_score - uncertainty)
        analysis.score_high = min(100, analysis.composite_score + uncertainty)

        # Build value context for LLM
        value_context = self._build_value_context(analysis, valuation_metrics)

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=analysis.composite_score,
            confidence=analysis.confidence,
            data_quality=analysis.data_quality,
            percentile_rank=None,
            score_low=analysis.score_low,
            score_high=analysis.score_high,
            metadata=self._build_metadata(analysis, company_info, value_context),
        )

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        """Get company info from company_master."""
        row = await self.db_conn.fetchrow("""
            SELECT
                id, company_name, primary_ticker, yahoo_symbol,
                sector, industry, report_currency, country
            FROM company_master
            WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_price_data(
        self,
        symbol: str,
        score_date: date
    ) -> Optional[Dict]:
        """Get current price and 52-week range."""
        row = await self.db_conn.fetchrow("""
            WITH current_price AS (
                SELECT close_price, date
                FROM daily_price_data
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC
                LIMIT 1
            ),
            price_range AS (
                SELECT
                    MAX(high_price) as high_52w,
                    MIN(low_price) as low_52w
                FROM daily_price_data
                WHERE symbol = $1
                AND date BETWEEN $2 - INTERVAL '1 year' AND $2
            )
            SELECT
                cp.close_price,
                cp.date as price_date,
                pr.high_52w,
                pr.low_52w,
                CASE
                    WHEN pr.high_52w - pr.low_52w > 0
                    THEN (cp.close_price - pr.low_52w) / (pr.high_52w - pr.low_52w)
                    ELSE 0.5
                END as price_52w_position
            FROM current_price cp, price_range pr
        """, symbol, score_date)
        return dict(row) if row else None

    async def _get_financials(
        self,
        symbol: str,
        score_date: date
    ) -> Optional[Dict]:
        """Get most recent annual financial statement."""
        row = await self.db_conn.fetchrow("""
            SELECT
                period_date,
                total_revenue,
                net_income,
                ebitda
            FROM financial_statements
            WHERE symbol = $1 AND period_date <= $2
            AND statement_type = 'annual'
            ORDER BY period_date DESC
            LIMIT 1
        """, symbol, score_date)
        return dict(row) if row else None

    async def _get_balance_sheet(
        self,
        symbol: str,
        score_date: date
    ) -> Optional[Dict]:
        """Get most recent annual balance sheet."""
        row = await self.db_conn.fetchrow("""
            SELECT
                period_date,
                total_equity,
                total_assets,
                total_debt,
                shares_outstanding,
                cash_and_equivalents
            FROM balance_sheet_data
            WHERE symbol = $1 AND period_date <= $2
            AND statement_type = 'annual'
            ORDER BY period_date DESC
            LIMIT 1
        """, symbol, score_date)
        return dict(row) if row else None

    async def _calculate_valuation_metrics(
        self,
        symbol: str,
        score_date: date,
        price_data: Dict,
        financials: Dict,
        balance_sheet: Optional[Dict]
    ) -> Dict[str, List[Tuple[date, float]]]:
        """Calculate valuation ratios over time."""

        current_price = price_data.get("close_price")
        if not current_price:
            return {}

        metrics = {
            "pe_ratio": [],
            "pb_ratio": [],
            "ps_ratio": [],
            "ev_ebitda": [],
            "price_52w_position": [],
        }

        # Get historical prices for historical valuation context
        historical_prices = await self._get_historical_prices(symbol, score_date, years=3)
        historical_financials = await self._get_historical_financials(symbol, score_date, years=3)
        historical_balance = await self._get_historical_balance(symbol, score_date, years=3)

        # Index data by period
        fin_by_period = {f["period_date"]: f for f in historical_financials}
        bs_by_period = {b["period_date"]: b for b in historical_balance}

        # Calculate ratios for each period where we have matching data
        for fin in historical_financials:
            period = fin["period_date"]
            bs = bs_by_period.get(period)

            # Find price around this period
            price_at_period = None
            for p in historical_prices:
                if abs((p["date"] - period).days) <= 30:  # Within a month
                    price_at_period = p["close_price"]
                    break

            if not price_at_period:
                continue

            shares = bs.get("shares_outstanding") if bs else None
            if not shares or shares <= 0:
                continue

            market_cap = float(price_at_period) * float(shares)

            # P/E ratio (using annual data, no annualization needed)
            net_income = fin.get("net_income")
            if net_income and net_income > 0:  # Only positive earnings
                pe = market_cap / float(net_income)
                if 0 < pe < 100:
                    metrics["pe_ratio"].append((period, pe))

            # P/B ratio
            if bs:
                equity = bs.get("total_equity")
                if equity and equity > 0:
                    pb = market_cap / float(equity)
                    if 0 < pb < 20:
                        metrics["pb_ratio"].append((period, pb))

            # P/S ratio (using annual data, no annualization needed)
            revenue = fin.get("total_revenue")
            if revenue and revenue > 0:
                ps = market_cap / float(revenue)
                if 0 < ps < 50:
                    metrics["ps_ratio"].append((period, ps))

            # EV/EBITDA (using annual data, no annualization needed)
            ebitda = fin.get("ebitda")
            if ebitda and ebitda > 0 and bs:
                debt = bs.get("total_debt") or 0
                cash = bs.get("cash_and_equivalents") or 0
                ev = market_cap + float(debt) - float(cash)
                ev_ebitda = ev / float(ebitda)
                if 0 < ev_ebitda < 50:
                    metrics["ev_ebitda"].append((period, ev_ebitda))

        # Add current 52-week position
        pos = price_data.get("price_52w_position")
        if pos is not None:
            metrics["price_52w_position"].append((score_date, pos))

        return metrics

    async def _get_historical_prices(
        self,
        symbol: str,
        score_date: date,
        years: int = 3
    ) -> List[Dict]:
        """Get historical prices."""
        start_date = score_date - timedelta(days=years * 365)
        rows = await self.db_conn.fetch("""
            SELECT date, close_price, high_price, low_price
            FROM daily_price_data
            WHERE symbol = $1
            AND date BETWEEN $2 AND $3
            ORDER BY date DESC
        """, symbol, start_date, score_date)
        return [dict(row) for row in rows]

    async def _get_historical_financials(
        self,
        symbol: str,
        score_date: date,
        years: int = 3
    ) -> List[Dict]:
        """Get historical annual financial statements."""
        start_date = score_date - timedelta(days=years * 365)
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_revenue, net_income, ebitda
            FROM financial_statements
            WHERE symbol = $1
            AND period_date BETWEEN $2 AND $3
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """, symbol, start_date, score_date)
        return [dict(row) for row in rows]

    async def _get_historical_balance(
        self,
        symbol: str,
        score_date: date,
        years: int = 3
    ) -> List[Dict]:
        """Get historical annual balance sheets."""
        start_date = score_date - timedelta(days=years * 365)
        rows = await self.db_conn.fetch("""
            SELECT period_date, total_equity, total_debt, shares_outstanding, cash_and_equivalents
            FROM balance_sheet_data
            WHERE symbol = $1
            AND period_date BETWEEN $2 AND $3
            AND statement_type = 'annual'
            ORDER BY period_date DESC
        """, symbol, start_date, score_date)
        return [dict(row) for row in rows]

    async def _analyze_metric(
        self,
        metric_name: str,
        values: List[Tuple[date, float]],
        sector: Optional[str],
        score_date: date,
        higher_is_better: bool = False,
    ) -> MetricAnalysis:
        """Analyze a valuation metric."""

        analysis = MetricAnalysis()
        analysis.data_points = len(values)

        if not values:
            return analysis

        sorted_values = sorted(values, key=lambda x: x[0], reverse=True)
        raw_values = [v[1] for v in sorted_values]

        analysis.current = raw_values[0]
        if len(raw_values) >= 2:
            analysis.ttm = sum(raw_values[:min(4, len(raw_values))]) / min(4, len(raw_values))
        if len(raw_values) >= 4:
            analysis.avg_3yr = sum(raw_values) / len(raw_values)

        analysis.min_historical = min(raw_values)
        analysis.max_historical = max(raw_values)

        # Historical percentile (where current sits in own history)
        if len(raw_values) >= 4:
            analysis.historical_percentile = PeerAnalyzer.calculate_percentile(
                analysis.current, raw_values, higher_is_better=higher_is_better
            )

        # Trend (is valuation getting cheaper or more expensive?)
        if len(values) >= 3:
            trend_values = sorted(values, key=lambda x: x[0])
            analysis.trend_direction, trend_score = \
                HistoricalAnalyzer.calculate_trend(trend_values)
            # For value metrics, DECLINING valuations are GOOD (invert)
            if not higher_is_better:
                analysis.trend_score = 100 - trend_score if trend_score else 50.0
            else:
                analysis.trend_score = trend_score

        # Volatility
        if len(raw_values) >= 3:
            analysis.volatility = HistoricalAnalyzer.calculate_volatility(raw_values)
            analysis.stability_score = HistoricalAnalyzer.calculate_stability_score(
                analysis.volatility
            )

        # Peer comparison
        if sector:
            peer_values = await self._get_sector_peer_values(metric_name, sector, score_date)
            if peer_values and len(peer_values) >= 5:
                analysis.sector_percentile = PeerAnalyzer.calculate_percentile(
                    analysis.current, peer_values, higher_is_better=higher_is_better
                )
                analysis.sector_median = float(sorted(peer_values)[len(peer_values) // 2])
                analysis.vs_sector_median = PeerAnalyzer.calculate_vs_median(
                    analysis.current, peer_values
                )

        # Normalize to raw score (lower is better for value metrics)
        thresholds = get_metric_threshold(metric_name)
        if analysis.current is not None:
            analysis.raw_score = ScoreNormalizer.normalize_metric(
                float(analysis.current),
                thresholds["low"],
                thresholds["high"],
                thresholds["higher_is_better"],  # False for most value metrics
            )

        # Data quality
        analysis.data_quality_score = QualityScorer.calculate_data_quality(
            data_points=len(values),
            expected_points=8,
            has_recent_data=True,
        )

        return analysis

    async def _get_sector_peer_values(
        self,
        metric_name: str,
        sector: str,
        score_date: date
    ) -> List[float]:
        """Get latest valuation metrics for sector peers."""

        # For each metric, we need a different calculation
        # This is simplified - in production would use subqueries

        if metric_name == "pe_ratio":
            query = """
                WITH latest_data AS (
                    SELECT DISTINCT ON (cm.id)
                        cm.id,
                        dp.close_price * bs.shares_outstanding / NULLIF(fs.net_income * 4, 0) as metric_value
                    FROM company_master cm
                    JOIN financial_statements fs ON cm.primary_ticker = fs.symbol
                    JOIN balance_sheet_data bs ON cm.primary_ticker = bs.symbol AND fs.period_date = bs.period_date
                    JOIN daily_price_data dp ON cm.primary_ticker = dp.symbol
                    WHERE cm.sector = $1
                    AND fs.period_date <= $2
                    AND fs.net_income > 0
                    AND bs.shares_outstanding > 0
                    AND dp.date <= $2
                    ORDER BY cm.id, fs.period_date DESC, dp.date DESC
                )
                SELECT metric_value FROM latest_data
                WHERE metric_value BETWEEN 1 AND 100
            """
        elif metric_name == "pb_ratio":
            query = """
                WITH latest_data AS (
                    SELECT DISTINCT ON (cm.id)
                        cm.id,
                        dp.close_price * bs.shares_outstanding / NULLIF(bs.total_equity, 0) as metric_value
                    FROM company_master cm
                    JOIN balance_sheet_data bs ON cm.primary_ticker = bs.symbol
                    JOIN daily_price_data dp ON cm.primary_ticker = dp.symbol
                    WHERE cm.sector = $1
                    AND bs.period_date <= $2
                    AND bs.total_equity > 0
                    AND bs.shares_outstanding > 0
                    AND dp.date <= $2
                    ORDER BY cm.id, bs.period_date DESC, dp.date DESC
                )
                SELECT metric_value FROM latest_data
                WHERE metric_value BETWEEN 0.1 AND 20
            """
        else:
            return []

        try:
            rows = await self.db_conn.fetch(query, sector, score_date)
            return [row["metric_value"] for row in rows if row["metric_value"]]
        except Exception as e:
            logger.warning(f"Error fetching peer values for {metric_name}: {e}")
            return []

    def _calculate_raw_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted raw score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.raw_score is not None:
                scores[name] = analysis.raw_score
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_trend_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted trend score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.trend_score is not None:
                scores[name] = analysis.trend_score
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_peer_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted peer comparison score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.sector_percentile is not None:
                scores[name] = analysis.sector_percentile
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_stability_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted stability score."""
        scores = {}
        for name, analysis in metrics.items():
            if analysis.stability_score is not None:
                scores[name] = analysis.stability_score
        if not scores:
            return 50.0
        return ScoreNormalizer.combine_scores(scores, self.METRIC_WEIGHTS)

    def _calculate_data_quality(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate overall data quality score."""
        qualities = [
            m.data_quality_score for m in metrics.values()
            if m.data_quality_score is not None
        ]
        if not qualities:
            return 0.5
        return sum(qualities) / len(qualities)

    def _build_value_context(
        self,
        analysis: DimensionAnalysis,
        valuation_metrics: Dict
    ) -> Dict[str, Any]:
        """Build value context for LLM reasoning."""

        context = {
            "valuation_summary": None,
            "vs_history": None,
            "vs_peers": None,
        }

        # Determine if cheap/expensive vs history
        historical_pcts = [
            m.historical_percentile for m in analysis.metrics.values()
            if m.historical_percentile is not None
        ]
        if historical_pcts:
            avg_hist_pct = sum(historical_pcts) / len(historical_pcts)
            if avg_hist_pct < 25:
                context["vs_history"] = "near_historical_lows"
            elif avg_hist_pct < 40:
                context["vs_history"] = "below_average"
            elif avg_hist_pct < 60:
                context["vs_history"] = "average"
            elif avg_hist_pct < 75:
                context["vs_history"] = "above_average"
            else:
                context["vs_history"] = "near_historical_highs"

        # Determine if cheap/expensive vs peers
        peer_pcts = [
            m.sector_percentile for m in analysis.metrics.values()
            if m.sector_percentile is not None
        ]
        if peer_pcts:
            avg_peer_pct = sum(peer_pcts) / len(peer_pcts)
            if avg_peer_pct > 75:  # Remember: low valuation = high percentile for "value"
                context["vs_peers"] = "significantly_cheaper"
            elif avg_peer_pct > 60:
                context["vs_peers"] = "somewhat_cheaper"
            elif avg_peer_pct > 40:
                context["vs_peers"] = "fairly_valued"
            elif avg_peer_pct > 25:
                context["vs_peers"] = "somewhat_expensive"
            else:
                context["vs_peers"] = "significantly_expensive"

        # Overall summary
        if context["vs_history"] and context["vs_peers"]:
            hist = context["vs_history"]
            peer = context["vs_peers"]
            if "cheaper" in peer and "low" in hist:
                context["valuation_summary"] = "Deep value opportunity - cheap vs peers AND own history"
            elif "cheaper" in peer:
                context["valuation_summary"] = "Relative value - cheaper than peers"
            elif "low" in hist:
                context["valuation_summary"] = "Historical value - cheaper than own history"
            elif "expensive" in peer and "high" in hist:
                context["valuation_summary"] = "Fully valued - expensive vs peers AND own history"
            else:
                context["valuation_summary"] = "Fairly valued"

        return context

    def _build_metadata(
        self,
        analysis: DimensionAnalysis,
        company_info: Dict,
        value_context: Dict
    ) -> Dict[str, Any]:
        """Build detailed metadata for the score."""

        metadata = {
            "dimension_version": "2.0.0",
            "company_name": company_info.get("company_name"),
            "sector": analysis.sector,
            "currency": analysis.currency,
            "component_scores": {
                "raw": analysis.raw_score,
                "trend": analysis.trend_score,
                "peer": analysis.peer_score,
                "stability": analysis.stability_score,
            },
            "value_context": value_context,  # Rich data for LLM reasoning
            "metrics": {},
        }

        for name, m in analysis.metrics.items():
            metadata["metrics"][name] = {
                "current": round(float(m.current), 2) if m.current else None,
                "avg_historical": round(float(m.avg_3yr), 2) if m.avg_3yr else None,
                "min_historical": round(float(m.min_historical), 2) if m.min_historical else None,
                "max_historical": round(float(m.max_historical), 2) if m.max_historical else None,
                "historical_percentile": float(m.historical_percentile) if m.historical_percentile else None,
                "sector_percentile": float(m.sector_percentile) if m.sector_percentile else None,
                "trend": m.trend_direction.value if m.trend_direction else None,
                "raw_score": float(m.raw_score) if m.raw_score else None,
                "data_points": m.data_points,
            }

        return metadata
