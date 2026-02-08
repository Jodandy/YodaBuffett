"""
Quality Dimension Calculator

Measures financial health, profitability, and operational efficiency.
Higher scores indicate stronger, more stable businesses.

Factors (calculated from raw financial statements):
- ROE (Return on Equity) = net_income / total_equity
- Profit margin = net_income / total_revenue
- Operating margin = operating_income / total_revenue
- Debt-to-equity = total_debt / total_equity (lower is better)
- Current ratio = current_assets / current_liabilities
"""

from typing import Dict, List, Optional, Any
from datetime import date
import numpy as np
from scipy import stats
import logging

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


DEFAULT_QUALITY_FACTORS = {
    "roe": {"weight": 0.25, "higher_is_better": True, "winsorize": 0.02},
    "profit_margin": {"weight": 0.25, "higher_is_better": True, "winsorize": 0.02},
    "debt_to_equity": {"weight": 0.20, "higher_is_better": False, "winsorize": 0.02},
    "current_ratio": {"weight": 0.15, "higher_is_better": True, "winsorize": 0.02, "optimal_range": [1.5, 3.0]},
    "operating_margin": {"weight": 0.15, "higher_is_better": True, "winsorize": 0.02},
}


@register_calculator
class QualityCalculator(BaseDimensionCalculator):
    """
    Quality dimension calculator.

    Identifies financially healthy companies based on
    profitability, efficiency, and stability metrics.

    Calculates ratios from raw financial_statements and balance_sheet_data tables.
    """

    @property
    def dimension_code(self) -> str:
        return "quality"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code="quality",
            display_name="Quality",
            description="Measures financial health, profitability, and stability",
            category="fundamental",
            data_sources=["financial_statements", "balance_sheet_data"],
            update_frequency="daily",
            requires_external_api=False,
            config={"factors": list(DEFAULT_QUALITY_FACTORS.keys())},
        )

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self._version = "2.0.0"
        self.factors = config.get("factors", DEFAULT_QUALITY_FACTORS) if config else DEFAULT_QUALITY_FACTORS
        self._distribution_cache: Dict[str, np.ndarray] = {}

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        universe_data: Optional[Dict[str, Dict[str, float]]] = None,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate quality score for a company."""

        # Fetch factor values from raw financial data
        if universe_data and company_id in universe_data:
            factor_values = universe_data[company_id]
        else:
            factor_values = await self._fetch_factor_values(company_id, score_date)

        if not factor_values:
            return None

        if universe_data is None:
            universe_data = await self._fetch_universe_data(score_date)

        # Calculate normalized scores
        factor_scores = {}
        factor_metadata = {}
        available_weight = 0.0
        total_weight = sum(f["weight"] for f in self.factors.values())

        for factor_id, factor_config in self.factors.items():
            weight = factor_config["weight"]
            raw_value = factor_values.get(factor_id)

            if raw_value is None or np.isnan(raw_value):
                factor_metadata[factor_id] = {"available": False, "raw_value": None}
                continue

            # Get distribution
            universe_values = self._get_factor_distribution(factor_id, universe_data)
            if len(universe_values) < 10:
                # Still calculate with limited data
                factor_metadata[factor_id] = {"available": True, "raw_value": float(raw_value), "limited_peers": True}
                # Use a simple normalization without peer comparison
                normalized = self._simple_normalize(raw_value, factor_id, factor_config)
                factor_scores[factor_id] = normalized
                available_weight += weight
                factor_metadata[factor_id]["normalized"] = float(normalized)
                continue

            # Winsorize
            winsorize_pct = factor_config.get("winsorize", 0.02)
            if winsorize_pct > 0:
                lower = np.percentile(universe_values, winsorize_pct * 100)
                upper = np.percentile(universe_values, (1 - winsorize_pct) * 100)
                raw_value = np.clip(raw_value, lower, upper)

            # Calculate percentile
            percentile = stats.percentileofscore(universe_values, raw_value, kind='rank')

            # Invert if lower is better
            if not factor_config.get("higher_is_better", True):
                percentile = 100 - percentile

            # Special handling for current ratio (optimal range)
            if "optimal_range" in factor_config:
                opt_low, opt_high = factor_config["optimal_range"]
                original_value = factor_values.get(factor_id)
                if original_value is not None:
                    if opt_low <= original_value <= opt_high:
                        percentile = min(100, percentile * 1.2)
                    elif original_value < opt_low * 0.5 or original_value > opt_high * 2:
                        percentile = percentile * 0.8

            normalized = percentile
            factor_scores[factor_id] = normalized
            available_weight += weight

            factor_metadata[factor_id] = {
                "raw_value": float(factor_values.get(factor_id, raw_value)),
                "normalized": float(normalized),
                "percentile": float(percentile),
                "contribution": float(normalized * weight),
                "weight": weight,
                "available": True,
            }

        # Need at least 2 factors to calculate a meaningful score
        if len(factor_scores) < 2:
            return None

        # Aggregate
        if available_weight > 0:
            weighted_sum = sum(
                factor_scores[f] * self.factors[f]["weight"]
                for f in factor_scores
            )
            score = weighted_sum / available_weight * (available_weight / total_weight)
            score = score * (0.5 + 0.5 * (available_weight / total_weight))
        else:
            score = 50.0

        data_quality = available_weight / total_weight
        confidence = self._calculate_confidence(factor_scores, data_quality)

        # Uncertainty range
        if factor_scores:
            scores = list(factor_scores.values())
            std_dev = np.std(scores) if len(scores) > 1 else 15.0
            uncertainty_factor = 2.0 * (1.0 - confidence) + 1.0
            score_low = max(0, score - std_dev * uncertainty_factor)
            score_high = min(100, score + std_dev * uncertainty_factor)
        else:
            score_low, score_high = 0.0, 100.0

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=float(score),
            confidence=confidence,
            data_quality=data_quality,
            score_low=score_low,
            score_high=score_high,
            metadata=factor_metadata,
            definition_version=1,
        )

    def _simple_normalize(self, value: float, factor_id: str, factor_config: dict) -> float:
        """Simple normalization when peer data is limited."""
        # Define reasonable ranges for each factor
        ranges = {
            "roe": (-0.5, 0.5),  # -50% to 50%
            "profit_margin": (-0.3, 0.3),  # -30% to 30%
            "operating_margin": (-0.3, 0.4),  # -30% to 40%
            "debt_to_equity": (0, 3),  # 0 to 300%
            "current_ratio": (0.5, 4),  # 0.5 to 4
        }

        low, high = ranges.get(factor_id, (0, 1))

        # Clip to range
        clipped = np.clip(value, low, high)

        # Normalize to 0-100
        if factor_config.get("higher_is_better", True):
            normalized = ((clipped - low) / (high - low)) * 100
        else:
            normalized = ((high - clipped) / (high - low)) * 100

        return float(normalized)

    async def _fetch_factor_values(self, company_id: str, score_date: date) -> Dict[str, float]:
        """Fetch quality factors for a single company from raw financial data."""

        # Get the company's symbol
        symbol_row = await self.db_conn.fetchrow("""
            SELECT yahoo_symbol, primary_ticker
            FROM company_master
            WHERE id = $1
        """, company_id)

        if not symbol_row:
            return {}

        # Use primary_ticker first (matches financial_statements format),
        # then fall back to yahoo_symbol without .ST suffix
        symbol = symbol_row["primary_ticker"]
        if not symbol and symbol_row["yahoo_symbol"]:
            symbol = symbol_row["yahoo_symbol"].replace(".ST", "").replace(".OL", "").replace(".HE", "").replace(".CO", "")
        if not symbol:
            return {}

        # Fetch latest annual financial statement and balance sheet
        row = await self.db_conn.fetchrow("""
            WITH latest_financials AS (
                SELECT
                    fs.net_income,
                    fs.total_revenue,
                    fs.operating_income,
                    fs.period_date
                FROM financial_statements fs
                WHERE fs.symbol = $1
                AND fs.period_date <= $2
                AND fs.statement_type = 'annual'
                ORDER BY fs.period_date DESC
                LIMIT 1
            ),
            latest_balance AS (
                SELECT
                    bs.total_equity,
                    bs.total_debt,
                    bs.current_assets,
                    bs.current_liabilities,
                    bs.period_date
                FROM balance_sheet_data bs
                WHERE bs.symbol = $1
                AND bs.period_date <= $2
                AND bs.statement_type = 'annual'
                ORDER BY bs.period_date DESC
                LIMIT 1
            )
            SELECT
                f.net_income,
                f.total_revenue,
                f.operating_income,
                b.total_equity,
                b.total_debt,
                b.current_assets,
                b.current_liabilities
            FROM latest_financials f
            CROSS JOIN latest_balance b
        """, symbol, score_date)

        if not row:
            return {}

        result = {}

        # Calculate ROE
        if row["net_income"] is not None and row["total_equity"] and row["total_equity"] != 0:
            result["roe"] = float(row["net_income"]) / float(row["total_equity"])

        # Calculate profit margin
        if row["net_income"] is not None and row["total_revenue"] and row["total_revenue"] != 0:
            result["profit_margin"] = float(row["net_income"]) / float(row["total_revenue"])

        # Calculate operating margin
        if row["operating_income"] is not None and row["total_revenue"] and row["total_revenue"] != 0:
            result["operating_margin"] = float(row["operating_income"]) / float(row["total_revenue"])

        # Calculate debt to equity
        if row["total_debt"] is not None and row["total_equity"] and row["total_equity"] != 0:
            result["debt_to_equity"] = float(row["total_debt"]) / float(row["total_equity"])

        # Calculate current ratio
        if row["current_assets"] is not None and row["current_liabilities"] and row["current_liabilities"] != 0:
            result["current_ratio"] = float(row["current_assets"]) / float(row["current_liabilities"])

        return result

    async def _fetch_universe_data(
        self,
        score_date: date,
        company_ids: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, float]]:
        """Fetch quality factors for universe from raw financial data."""

        company_filter = ""
        params = [score_date]
        if company_ids:
            company_filter = "AND cm.id = ANY($2)"
            params.append(company_ids)

        query = f"""
            WITH latest_financials AS (
                SELECT DISTINCT ON (fs.symbol)
                    fs.symbol,
                    fs.net_income,
                    fs.total_revenue,
                    fs.operating_income
                FROM financial_statements fs
                WHERE fs.period_date <= $1
                AND fs.statement_type = 'annual'
                ORDER BY fs.symbol, fs.period_date DESC
            ),
            latest_balance AS (
                SELECT DISTINCT ON (bs.symbol)
                    bs.symbol,
                    bs.total_equity,
                    bs.total_debt,
                    bs.current_assets,
                    bs.current_liabilities
                FROM balance_sheet_data bs
                WHERE bs.period_date <= $1
                AND bs.statement_type = 'annual'
                ORDER BY bs.symbol, bs.period_date DESC
            )
            SELECT
                cm.id::text as company_id,
                f.net_income,
                f.total_revenue,
                f.operating_income,
                b.total_equity,
                b.total_debt,
                b.current_assets,
                b.current_liabilities
            FROM company_master cm
            LEFT JOIN latest_financials f ON f.symbol = cm.primary_ticker
            LEFT JOIN latest_balance b ON b.symbol = cm.primary_ticker
            WHERE cm.country IN ('SE', 'NO', 'DK', 'FI', 'Sverige', 'Norge', 'Danmark', 'Finland', 'Nordic')
            AND (f.net_income IS NOT NULL OR b.total_equity IS NOT NULL)
            {company_filter}
        """

        rows = await self.db_conn.fetch(query, *params)

        result = {}
        for row in rows:
            company_data = {}

            # Calculate ROE
            if row["net_income"] is not None and row["total_equity"] and row["total_equity"] != 0:
                company_data["roe"] = float(row["net_income"]) / float(row["total_equity"])

            # Calculate profit margin
            if row["net_income"] is not None and row["total_revenue"] and row["total_revenue"] != 0:
                company_data["profit_margin"] = float(row["net_income"]) / float(row["total_revenue"])

            # Calculate operating margin
            if row["operating_income"] is not None and row["total_revenue"] and row["total_revenue"] != 0:
                company_data["operating_margin"] = float(row["operating_income"]) / float(row["total_revenue"])

            # Calculate debt to equity
            if row["total_debt"] is not None and row["total_equity"] and row["total_equity"] != 0:
                company_data["debt_to_equity"] = float(row["total_debt"]) / float(row["total_equity"])

            # Calculate current ratio
            if row["current_assets"] is not None and row["current_liabilities"] and row["current_liabilities"] != 0:
                company_data["current_ratio"] = float(row["current_assets"]) / float(row["current_liabilities"])

            if company_data:
                result[row["company_id"]] = company_data

        return result

    def _get_factor_distribution(self, factor_id: str, universe_data: Dict) -> np.ndarray:
        """Get distribution of a factor across universe."""
        if factor_id in self._distribution_cache:
            return self._distribution_cache[factor_id]

        values = [
            data.get(factor_id)
            for data in universe_data.values()
            if data.get(factor_id) is not None and not np.isnan(data.get(factor_id, np.nan))
        ]
        arr = np.array(values) if values else np.array([])
        self._distribution_cache[factor_id] = arr
        return arr

    def _calculate_confidence(self, factor_scores: Dict, data_quality: float) -> float:
        """Calculate confidence score."""
        confidence = data_quality

        if len(factor_scores) >= 3:
            spread = np.std(list(factor_scores.values()))
            if spread < 15:
                confidence *= 1.1
            elif spread > 30:
                confidence *= 0.9

        return max(0.0, min(1.0, confidence))
