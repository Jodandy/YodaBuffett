"""
Returns Dimension Calculator

Measures how efficiently a company generates returns on capital.
This is a BUSINESS dimension (price-independent).

Metrics analyzed:
- ROE (Return on Equity) - shareholder returns
- ROA (Return on Assets) - asset efficiency
- ROIC (Return on Invested Capital) - capital allocation quality
- DuPont decomposition for ROE drivers

Each metric includes:
- Current level vs thresholds
- Historical trend analysis
- Sector peer comparison
- DuPont breakdown (profit margin × asset turnover × leverage)

The DuPont decomposition is particularly valuable for ML/LLM systems
as it reveals WHY returns are changing, not just that they changed.
"""

from datetime import date, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging

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
class ReturnsCalculator(BaseDimensionCalculator):
    """
    Sophisticated returns dimension calculator.

    Analyzes capital efficiency with:
    - Multi-period historical analysis
    - DuPont decomposition (profit margin × turnover × leverage)
    - Trend detection and scoring
    - Sector-relative comparisons
    - Quality-adjusted confidence

    The DuPont decomposition reveals the DRIVERS of ROE changes:
    - Profit margin improving? → Operational excellence
    - Asset turnover improving? → Better capital utilization
    - Leverage increasing? → Financial engineering (riskier)
    """

    # Metric weights for composite score
    METRIC_WEIGHTS = {
        "roe": 0.35,      # Most important for shareholders
        "roa": 0.25,      # Shows pure asset efficiency
        "roic": 0.40,     # Best measure of capital allocation
    }

    # Component weights for final score
    COMPONENT_WEIGHTS = {
        "raw_score": 0.35,      # Absolute level
        "trend_score": 0.25,    # Improvement trajectory
        "peer_score": 0.25,     # Sector comparison
        "stability_score": 0.10,  # Consistency
        "quality_bonus": 0.05,  # DuPont quality indicator
    }

    @property
    def dimension_code(self) -> str:
        return "returns"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Returns",
            description="Measures return on capital efficiency with DuPont decomposition for driver analysis",
            category="fundamental",
            data_sources=["financial_statements", "balance_sheet_data"],
            update_frequency="daily",
            version="2.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate returns dimension for a company."""

        # Get company info
        company_info = await self._get_company_info(company_id)
        if not company_info:
            logger.warning(f"Company not found: {company_id}")
            return None

        symbol = company_info["primary_ticker"]
        sector = company_info.get("sector")
        currency = company_info.get("report_currency", "SEK")

        # Get historical data
        financials = await self._get_historical_financials(symbol, score_date, years=5)
        balance_sheets = await self._get_historical_balance_sheets(symbol, score_date, years=5)

        if not financials or len(financials) < 2:
            logger.info(f"Insufficient financial data for {symbol}")
            return None
        if not balance_sheets or len(balance_sheets) < 2:
            logger.info(f"Insufficient balance sheet data for {symbol}")
            return None

        # Create analysis container
        analysis = DimensionAnalysis(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            sector=sector,
            currency=currency,
        )

        # Calculate return metrics with DuPont components
        return_metrics = await self._calculate_return_metrics(
            financials, balance_sheets, score_date
        )

        # Analyze each metric
        for metric_name, values in return_metrics.items():
            if values and metric_name in self.METRIC_WEIGHTS:
                metric_analysis = await self._analyze_metric(
                    metric_name=metric_name,
                    values=values,
                    sector=sector,
                    score_date=score_date,
                )
                analysis.metrics[metric_name] = metric_analysis

        if len(analysis.metrics) < 2:
            logger.info(f"Insufficient return metrics for {symbol}")
            return None

        # Calculate DuPont decomposition
        dupont = self._calculate_dupont_decomposition(financials, balance_sheets)

        # Calculate component scores
        analysis.raw_score = self._calculate_raw_score(analysis.metrics)
        analysis.trend_score = self._calculate_trend_score(analysis.metrics)
        analysis.peer_score = self._calculate_peer_score(analysis.metrics)
        analysis.stability_score = self._calculate_stability_score(analysis.metrics)

        # Quality bonus based on DuPont analysis
        quality_bonus = self._calculate_dupont_quality_bonus(dupont)

        # Calculate composite score
        component_scores = {
            "raw_score": analysis.raw_score,
            "trend_score": analysis.trend_score,
            "peer_score": analysis.peer_score,
            "stability_score": analysis.stability_score,
            "quality_bonus": quality_bonus,
        }
        analysis.composite_score = ScoreNormalizer.combine_scores(
            component_scores, self.COMPONENT_WEIGHTS
        )

        # Calculate confidence
        analysis.data_quality = self._calculate_data_quality(analysis.metrics)
        analysis.confidence = analysis.data_quality

        # Score range (uncertainty)
        uncertainty = (1 - analysis.confidence) * 15
        analysis.score_low = max(0, analysis.composite_score - uncertainty)
        analysis.score_high = min(100, analysis.composite_score + uncertainty)

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
            metadata=self._build_metadata(analysis, company_info, dupont),
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

    async def _get_historical_financials(
        self,
        symbol: str,
        score_date: date,
        years: int = 5
    ) -> List[Dict]:
        """Get historical financial statements."""
        start_date = score_date - timedelta(days=years * 365)

        rows = await self.db_conn.fetch("""
            SELECT
                period_date,
                statement_type,
                total_revenue,
                net_income,
                operating_income,
                ebitda,
                currency
            FROM financial_statements
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $3
            ORDER BY period_date DESC
        """, symbol, score_date, start_date)

        return [dict(row) for row in rows]

    async def _get_historical_balance_sheets(
        self,
        symbol: str,
        score_date: date,
        years: int = 5
    ) -> List[Dict]:
        """Get historical balance sheet data."""
        start_date = score_date - timedelta(days=years * 365)

        rows = await self.db_conn.fetch("""
            SELECT
                period_date,
                statement_type,
                total_assets,
                total_equity,
                total_debt,
                cash_and_equivalents,
                currency
            FROM balance_sheet_data
            WHERE symbol = $1
            AND period_date <= $2
            AND period_date >= $3
            ORDER BY period_date DESC
        """, symbol, score_date, start_date)

        return [dict(row) for row in rows]

    async def _calculate_return_metrics(
        self,
        financials: List[Dict],
        balance_sheets: List[Dict],
        score_date: date
    ) -> Dict[str, List[Tuple[date, float]]]:
        """Calculate ROE, ROA, and ROIC from financials and balance sheets."""

        # Index balance sheets by period for easy lookup
        bs_by_period = {bs["period_date"]: bs for bs in balance_sheets}

        metrics = {
            "roe": [],
            "roa": [],
            "roic": [],
        }

        for fin in financials:
            period = fin["period_date"]
            net_income = fin.get("net_income")
            operating_income = fin.get("operating_income")

            # Find matching balance sheet
            bs = bs_by_period.get(period)
            if not bs:
                continue

            total_equity = bs.get("total_equity")
            total_assets = bs.get("total_assets")
            total_debt = bs.get("total_debt") or 0
            cash = bs.get("cash_and_equivalents") or 0

            # ROE = Net Income / Shareholders' Equity
            if net_income is not None and total_equity and total_equity > 0:
                roe = float(net_income) / float(total_equity)
                # Cap extreme values
                if -2 <= roe <= 2:
                    metrics["roe"].append((period, roe))

            # ROA = Net Income / Total Assets
            if net_income is not None and total_assets and total_assets > 0:
                roa = float(net_income) / float(total_assets)
                if -1 <= roa <= 1:
                    metrics["roa"].append((period, roa))

            # ROIC = NOPAT / Invested Capital
            # NOPAT ≈ Operating Income × (1 - tax rate), assuming 22% tax
            # Invested Capital = Total Equity + Total Debt - Cash
            if operating_income is not None and total_equity:
                nopat = float(operating_income) * 0.78  # Approximate after-tax
                invested_capital = float(total_equity) + float(total_debt) - float(cash)
                if invested_capital > 0:
                    roic = nopat / invested_capital
                    if -1 <= roic <= 1:
                        metrics["roic"].append((period, roic))

        return metrics

    def _calculate_dupont_decomposition(
        self,
        financials: List[Dict],
        balance_sheets: List[Dict]
    ) -> Dict[str, Any]:
        """
        Calculate DuPont decomposition for most recent period.

        ROE = Profit Margin × Asset Turnover × Equity Multiplier
            = (Net Income/Revenue) × (Revenue/Assets) × (Assets/Equity)

        This reveals WHERE returns come from:
        - Profit margin: Operating efficiency
        - Asset turnover: Capital efficiency
        - Equity multiplier: Financial leverage
        """
        if not financials or not balance_sheets:
            return {}

        # Get most recent data
        fin = financials[0]
        bs_by_period = {bs["period_date"]: bs for bs in balance_sheets}
        bs = bs_by_period.get(fin["period_date"])

        if not bs:
            return {}

        net_income = fin.get("net_income")
        revenue = fin.get("total_revenue")
        total_assets = bs.get("total_assets")
        total_equity = bs.get("total_equity")

        if not all([net_income, revenue, total_assets, total_equity]):
            return {}
        if revenue == 0 or total_assets == 0 or total_equity == 0:
            return {}

        profit_margin = float(net_income) / float(revenue)
        asset_turnover = float(revenue) / float(total_assets)
        equity_multiplier = float(total_assets) / float(total_equity)

        # Calculate what's driving ROE changes (if we have historical data)
        historical_dupont = []
        for f in financials[:8]:  # Last 8 periods
            bs_hist = bs_by_period.get(f["period_date"])
            if bs_hist:
                ni = f.get("net_income")
                rev = f.get("total_revenue")
                ta = bs_hist.get("total_assets")
                te = bs_hist.get("total_equity")
                if all([ni, rev, ta, te]) and rev != 0 and ta != 0 and te != 0:
                    historical_dupont.append({
                        "period": str(f["period_date"]),  # Convert date to string for JSON
                        "profit_margin": float(ni) / float(rev),
                        "asset_turnover": float(rev) / float(ta),
                        "equity_multiplier": float(ta) / float(te),
                    })

        # Identify primary ROE driver
        primary_driver = self._identify_primary_driver(historical_dupont)

        return {
            "current": {
                "profit_margin": round(profit_margin, 4),
                "asset_turnover": round(asset_turnover, 4),
                "equity_multiplier": round(equity_multiplier, 4),
                "calculated_roe": round(profit_margin * asset_turnover * equity_multiplier, 4),
            },
            "historical": historical_dupont,
            "primary_driver": primary_driver,
            "driver_interpretation": self._interpret_driver(primary_driver),
        }

    def _identify_primary_driver(self, historical: List[Dict]) -> str:
        """Identify which DuPont component is driving ROE changes."""
        if len(historical) < 4:
            return "insufficient_data"

        # Calculate variance contribution of each component
        margins = [h["profit_margin"] for h in historical]
        turnovers = [h["asset_turnover"] for h in historical]
        multipliers = [h["equity_multiplier"] for h in historical]

        import numpy as np

        # Coefficient of variation for each
        def coef_var(values):
            mean = np.mean(values)
            if mean == 0:
                return 0
            return np.std(values) / abs(mean)

        variations = {
            "profit_margin": coef_var(margins),
            "asset_turnover": coef_var(turnovers),
            "equity_multiplier": coef_var(multipliers),
        }

        return max(variations, key=variations.get)

    def _interpret_driver(self, driver: str) -> str:
        """LLM-friendly interpretation of the primary ROE driver."""
        interpretations = {
            "profit_margin": "Returns primarily driven by profit margins - indicates operational efficiency focus",
            "asset_turnover": "Returns primarily driven by asset turnover - indicates capital utilization focus",
            "equity_multiplier": "Returns primarily driven by leverage - higher risk, financial engineering",
            "insufficient_data": "Insufficient historical data for driver analysis",
        }
        return interpretations.get(driver, "Unknown driver pattern")

    def _calculate_dupont_quality_bonus(self, dupont: Dict) -> float:
        """
        Calculate quality bonus based on DuPont analysis.

        Higher quality returns come from:
        - Profit margin improvements (sustainable)
        - Asset turnover improvements (operational excellence)

        Lower quality returns come from:
        - Leverage increases (risky, not sustainable)
        """
        if not dupont or "primary_driver" not in dupont:
            return 50.0  # Neutral

        driver = dupont["primary_driver"]

        # Profit margin driver = high quality (75 score)
        # Asset turnover driver = good quality (65 score)
        # Leverage driver = lower quality (35 score)
        quality_scores = {
            "profit_margin": 75.0,
            "asset_turnover": 65.0,
            "equity_multiplier": 35.0,
            "insufficient_data": 50.0,
        }

        return quality_scores.get(driver, 50.0)

    async def _analyze_metric(
        self,
        metric_name: str,
        values: List[Tuple[date, float]],
        sector: Optional[str],
        score_date: date,
    ) -> MetricAnalysis:
        """Perform full analysis on a single metric."""

        analysis = MetricAnalysis()
        analysis.data_points = len(values)

        if not values:
            return analysis

        # Sort by date (newest first)
        sorted_values = sorted(values, key=lambda x: x[0], reverse=True)
        raw_values = [v[1] for v in sorted_values]

        analysis.current = raw_values[0]
        if len(raw_values) >= 4:
            analysis.ttm = sum(raw_values[:4]) / 4

        # Historical stats
        if len(raw_values) >= 4:
            analysis.avg_3yr = sum(raw_values[:min(12, len(raw_values))]) / min(12, len(raw_values))
        if len(raw_values) >= 8:
            analysis.avg_5yr = sum(raw_values) / len(raw_values)

        analysis.min_historical = min(raw_values)
        analysis.max_historical = max(raw_values)

        # Historical percentile
        if len(raw_values) >= 4:
            analysis.historical_percentile = PeerAnalyzer.calculate_percentile(
                analysis.current, raw_values, higher_is_better=True
            )

        # Trend analysis
        if len(values) >= 4:
            trend_values = sorted(values, key=lambda x: x[0])
            analysis.trend_direction, analysis.trend_score = \
                HistoricalAnalyzer.calculate_trend(trend_values)

        # YoY change
        if len(sorted_values) >= 4:
            current_val = sorted_values[0][1]
            for d, v in sorted_values:
                days_ago = (score_date - d).days
                if 300 <= days_ago <= 400:
                    analysis.yoy_change = (current_val - v) / abs(v) if v != 0 else None
                    break

        # Volatility and stability
        if len(raw_values) >= 4:
            analysis.volatility = HistoricalAnalyzer.calculate_volatility(raw_values)
            analysis.stability_score = HistoricalAnalyzer.calculate_stability_score(
                analysis.volatility
            )

        # Peer comparison
        if sector:
            peer_values = await self._get_sector_peer_values(metric_name, sector, score_date)
            if peer_values and len(peer_values) >= 5:
                analysis.sector_percentile = PeerAnalyzer.calculate_percentile(
                    analysis.current, peer_values, higher_is_better=True
                )
                analysis.sector_median = float(sorted(peer_values)[len(peer_values) // 2])
                analysis.vs_sector_median = PeerAnalyzer.calculate_vs_median(
                    analysis.current, peer_values
                )

        # Normalize to raw score
        thresholds = get_metric_threshold(metric_name)
        if analysis.current is not None:
            analysis.raw_score = ScoreNormalizer.normalize_metric(
                analysis.current,
                thresholds["low"],
                thresholds["high"],
                thresholds["higher_is_better"],
            )

        # Data quality
        analysis.data_quality_score = QualityScorer.calculate_data_quality(
            data_points=len(values),
            expected_points=12,
            has_recent_data=True,
        )

        return analysis

    async def _get_sector_peer_values(
        self,
        metric_name: str,
        sector: str,
        score_date: date
    ) -> List[float]:
        """Get latest metric values for all companies in sector."""

        # Different queries for different metrics
        if metric_name == "roe":
            query = """
                WITH latest_data AS (
                    SELECT DISTINCT ON (fs.symbol)
                        fs.symbol,
                        fs.net_income::float / NULLIF(bs.total_equity, 0) as metric_value
                    FROM financial_statements fs
                    JOIN balance_sheet_data bs ON fs.symbol = bs.symbol AND fs.period_date = bs.period_date
                    JOIN company_master cm ON fs.symbol = cm.primary_ticker
                    WHERE cm.sector = $1
                    AND fs.period_date <= $2
                    AND bs.total_equity > 0
                    ORDER BY fs.symbol, fs.period_date DESC
                )
                SELECT metric_value FROM latest_data
                WHERE metric_value IS NOT NULL
                AND metric_value BETWEEN -1 AND 1
            """
        elif metric_name == "roa":
            query = """
                WITH latest_data AS (
                    SELECT DISTINCT ON (fs.symbol)
                        fs.symbol,
                        fs.net_income::float / NULLIF(bs.total_assets, 0) as metric_value
                    FROM financial_statements fs
                    JOIN balance_sheet_data bs ON fs.symbol = bs.symbol AND fs.period_date = bs.period_date
                    JOIN company_master cm ON fs.symbol = cm.primary_ticker
                    WHERE cm.sector = $1
                    AND fs.period_date <= $2
                    AND bs.total_assets > 0
                    ORDER BY fs.symbol, fs.period_date DESC
                )
                SELECT metric_value FROM latest_data
                WHERE metric_value IS NOT NULL
                AND metric_value BETWEEN -0.5 AND 0.5
            """
        elif metric_name == "roic":
            query = """
                WITH latest_data AS (
                    SELECT DISTINCT ON (fs.symbol)
                        fs.symbol,
                        (fs.operating_income::float * 0.78) /
                        NULLIF(bs.total_equity + COALESCE(bs.total_debt, 0) - COALESCE(bs.cash_and_equivalents, 0), 0) as metric_value
                    FROM financial_statements fs
                    JOIN balance_sheet_data bs ON fs.symbol = bs.symbol AND fs.period_date = bs.period_date
                    JOIN company_master cm ON fs.symbol = cm.primary_ticker
                    WHERE cm.sector = $1
                    AND fs.period_date <= $2
                    AND bs.total_equity > 0
                    ORDER BY fs.symbol, fs.period_date DESC
                )
                SELECT metric_value FROM latest_data
                WHERE metric_value IS NOT NULL
                AND metric_value BETWEEN -0.5 AND 0.5
            """
        else:
            return []

        rows = await self.db_conn.fetch(query, sector, score_date)
        return [row["metric_value"] for row in rows]

    def _calculate_raw_score(self, metrics: Dict[str, MetricAnalysis]) -> float:
        """Calculate weighted raw score from all metrics."""
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

    def _build_metadata(
        self,
        analysis: DimensionAnalysis,
        company_info: Dict,
        dupont: Dict
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
            "dupont_analysis": dupont,  # Rich structured data for LLM reasoning
            "metrics": {},
        }

        for name, m in analysis.metrics.items():
            metadata["metrics"][name] = {
                "current": round(m.current, 4) if m.current else None,
                "ttm": round(m.ttm, 4) if m.ttm else None,
                "avg_3yr": round(m.avg_3yr, 4) if m.avg_3yr else None,
                "trend": m.trend_direction.value if m.trend_direction else None,
                "trend_score": m.trend_score,
                "sector_percentile": m.sector_percentile,
                "vs_sector_median": round(m.vs_sector_median, 4) if m.vs_sector_median else None,
                "stability": m.stability_score,
                "raw_score": m.raw_score,
                "data_points": m.data_points,
            }

        return metadata
