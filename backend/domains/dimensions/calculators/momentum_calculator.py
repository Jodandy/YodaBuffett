"""
Momentum Dimension Calculator

Captures price and volume trends indicating directional strength.
Higher scores suggest positive momentum.

Factors:
- RSI (Relative Strength Index)
- Price vs SMA (trend indicator)
- Recent returns (short-term momentum)
- Volume trend (confirmation)
- KNN prediction (ML-based forward look)
"""

from typing import Dict, List, Optional, Any
from datetime import date, timedelta
import numpy as np
from scipy import stats
import logging

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


@register_calculator
class MomentumCalculator(BaseDimensionCalculator):
    """
    Momentum dimension calculator.

    Combines technical indicators and ML predictions
    to assess directional strength.
    """

    @property
    def dimension_code(self) -> str:
        return "momentum"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code="momentum",
            display_name="Momentum",
            description="Captures price and volume trends indicating directional strength",
            category="technical",
            data_sources=["daily_price_data", "knn_neighbors"],
            update_frequency="daily",
            requires_external_api=False,
            config={
                "rsi_period": 14,
                "sma_period": 50,
                "return_periods": [5, 20, 60],
            },
        )

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        super().__init__(db_conn, config)
        self._version = "1.0.0"

        self.rsi_period = self.get_config("rsi_period", 14)
        self.sma_period = self.get_config("sma_period", 50)
        self.return_periods = self.get_config("return_periods", [5, 20, 60])

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> DimensionScore:
        """Calculate momentum score for a company."""

        metadata = {}
        component_scores = {}

        # 1. RSI Score
        rsi_data = await self._calculate_rsi(company_id, score_date)
        if rsi_data:
            # RSI 30-70 is neutral, below 30 is oversold (contrarian bullish), above 70 is overbought
            # For momentum: RSI 50-70 is positive momentum, 30-50 is weak, >70 is overextended
            rsi = rsi_data["rsi"]
            if 50 <= rsi <= 70:
                rsi_score = 60 + (rsi - 50) * 2  # 60-100
            elif 30 <= rsi < 50:
                rsi_score = 30 + (rsi - 30) * 1.5  # 30-60
            elif rsi > 70:
                rsi_score = max(50, 100 - (rsi - 70) * 2)  # Overbought, pulling back
            else:  # < 30
                rsi_score = rsi  # Oversold, weak momentum

            component_scores["rsi"] = rsi_score
            metadata["rsi"] = rsi_data

        # 2. Price vs SMA (trend)
        sma_data = await self._calculate_price_vs_sma(company_id, score_date)
        if sma_data:
            # Price above SMA = positive trend
            pct_above = sma_data["pct_above_sma"]
            if pct_above > 0:
                sma_score = 50 + min(50, pct_above * 5)  # Up to 100
            else:
                sma_score = 50 + max(-50, pct_above * 5)  # Down to 0

            component_scores["price_vs_sma"] = sma_score
            metadata["price_vs_sma"] = sma_data

        # 3. Recent returns
        returns_data = await self._calculate_returns(company_id, score_date)
        if returns_data:
            # Combine returns across periods
            avg_return = np.mean([
                returns_data.get(f"return_{p}d", 0)
                for p in self.return_periods
                if returns_data.get(f"return_{p}d") is not None
            ])

            # Convert return to score: +10% = 100, 0% = 50, -10% = 0
            returns_score = 50 + avg_return * 500  # 1% = 5 points
            returns_score = max(0, min(100, returns_score))

            component_scores["returns"] = returns_score
            metadata["returns"] = returns_data

        # 4. Volume trend
        volume_data = await self._calculate_volume_trend(company_id, score_date)
        if volume_data:
            # Volume confirmation: higher volume on up days = positive
            vol_ratio = volume_data.get("recent_vs_avg_volume", 1.0)
            if vol_ratio > 1.2:
                volume_score = 70 + min(30, (vol_ratio - 1.2) * 50)
            elif vol_ratio > 0.8:
                volume_score = 50 + (vol_ratio - 0.8) * 50
            else:
                volume_score = max(20, vol_ratio * 62.5)

            component_scores["volume"] = volume_score
            metadata["volume"] = volume_data

        # 5. KNN prediction (if available)
        knn_data = await self._get_knn_prediction(company_id, score_date)
        if knn_data:
            # KNN predicted return -> score
            predicted_return = knn_data.get("predicted_5d_return", 0)
            knn_score = 50 + predicted_return * 1000  # 0.1% = 1 point
            knn_score = max(0, min(100, knn_score))

            component_scores["knn_prediction"] = knn_score
            metadata["knn_prediction"] = knn_data

        # Aggregate with weights
        weights = {
            "rsi": 0.20,
            "price_vs_sma": 0.25,
            "returns": 0.30,
            "volume": 0.10,
            "knn_prediction": 0.15,
        }

        if not component_scores:
            return self._no_data_score(company_id, score_date)

        total_weight = sum(weights[k] for k in component_scores)
        weighted_sum = sum(component_scores[k] * weights[k] for k in component_scores)
        score = weighted_sum / total_weight if total_weight > 0 else 50.0

        data_quality = len(component_scores) / len(weights)
        confidence = data_quality * 0.9  # Technical indicators have inherent uncertainty

        # Uncertainty range
        if component_scores:
            std_dev = np.std(list(component_scores.values()))
            score_low = max(0, score - std_dev * 1.5)
            score_high = min(100, score + std_dev * 1.5)
        else:
            score_low, score_high = 0.0, 100.0

        metadata["component_scores"] = component_scores
        metadata["weights_used"] = {k: weights[k] for k in component_scores}

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=float(score),
            confidence=confidence,
            data_quality=data_quality,
            score_low=score_low,
            score_high=score_high,
            metadata=metadata,
            definition_version=1,
        )

    async def _calculate_rsi(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate RSI for a company."""

        query = """
        WITH prices AS (
            SELECT dpd.date, dpd.close_price,
                   dpd.close_price - LAG(dpd.close_price) OVER (ORDER BY dpd.date) as change
            FROM daily_price_data dpd
            JOIN company_master cm ON dpd.symbol = cm.primary_ticker
            WHERE cm.id = $1
            AND dpd.date <= $2
            ORDER BY dpd.date DESC
            LIMIT $3
        )
        SELECT
            AVG(CASE WHEN change > 0 THEN change ELSE 0 END) as avg_gain,
            AVG(CASE WHEN change < 0 THEN ABS(change) ELSE 0 END) as avg_loss
        FROM prices
        WHERE change IS NOT NULL
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date, self.rsi_period + 1)

            if not row or row["avg_loss"] is None:
                return None

            avg_gain = float(row["avg_gain"] or 0)
            avg_loss = float(row["avg_loss"] or 0.0001)

            rs = avg_gain / avg_loss if avg_loss > 0 else 100
            rsi = 100 - (100 / (1 + rs))

            return {
                "rsi": float(rsi),
                "period": self.rsi_period,
                "avg_gain": avg_gain,
                "avg_loss": avg_loss,
            }
        except Exception as e:
            logger.warning(f"RSI calculation failed: {e}")
            return None

    async def _calculate_price_vs_sma(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate price position relative to SMA."""

        query = """
        WITH prices AS (
            SELECT dpd.date, dpd.close_price
            FROM daily_price_data dpd
            JOIN company_master cm ON dpd.symbol = cm.primary_ticker
            WHERE cm.id = $1
            AND dpd.date <= $2
            ORDER BY dpd.date DESC
            LIMIT $3
        )
        SELECT
            (SELECT close_price FROM prices ORDER BY date DESC LIMIT 1) as current_price,
            AVG(close_price) as sma
        FROM prices
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date, self.sma_period)

            if not row or row["sma"] is None or row["current_price"] is None:
                return None

            current = float(row["current_price"])
            sma = float(row["sma"])
            pct_above = ((current - sma) / sma) * 100 if sma > 0 else 0

            return {
                "current_price": current,
                "sma": sma,
                "sma_period": self.sma_period,
                "pct_above_sma": pct_above,
            }
        except Exception as e:
            logger.warning(f"SMA calculation failed: {e}")
            return None

    async def _calculate_returns(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate returns over multiple periods."""

        result = {}

        for period in self.return_periods:
            query = """
            WITH prices AS (
                SELECT dpd.date, dpd.close_price
                FROM daily_price_data dpd
                JOIN company_master cm ON dpd.symbol = cm.primary_ticker
                WHERE cm.id = $1
                AND dpd.date <= $2
                ORDER BY dpd.date DESC
                LIMIT $3
            )
            SELECT
                (SELECT close_price FROM prices ORDER BY date DESC LIMIT 1) as current,
                (SELECT close_price FROM prices ORDER BY date ASC LIMIT 1) as past
            """

            try:
                row = await self.db_conn.fetchrow(query, company_id, score_date, period + 1)

                if row and row["current"] and row["past"]:
                    current = float(row["current"])
                    past = float(row["past"])
                    ret = (current - past) / past if past > 0 else 0
                    result[f"return_{period}d"] = ret
            except Exception as e:
                logger.warning(f"Return calculation failed for {period}d: {e}")

        return result if result else None

    async def _calculate_volume_trend(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Calculate volume trend indicator."""

        query = """
        WITH volumes AS (
            SELECT dpd.volume
            FROM daily_price_data dpd
            JOIN company_master cm ON dpd.symbol = cm.primary_ticker
            WHERE cm.id = $1
            AND dpd.date <= $2
            ORDER BY dpd.date DESC
            LIMIT 20
        )
        SELECT
            (SELECT volume FROM volumes LIMIT 1) as recent_volume,
            AVG(volume) as avg_volume
        FROM volumes
        """

        try:
            row = await self.db_conn.fetchrow(query, company_id, score_date)

            if not row or row["avg_volume"] is None:
                return None

            recent = float(row["recent_volume"] or 0)
            avg = float(row["avg_volume"] or 1)

            return {
                "recent_volume": recent,
                "avg_volume_20d": avg,
                "recent_vs_avg_volume": recent / avg if avg > 0 else 1.0,
            }
        except Exception as e:
            logger.warning(f"Volume calculation failed: {e}")
            return None

    async def _get_knn_prediction(self, company_id: str, score_date: date) -> Optional[Dict]:
        """Get KNN prediction if available."""

        query = """
        SELECT neighbors
        FROM knn_neighbors
        WHERE company_id = $1
        AND prediction_date <= $2
        ORDER BY prediction_date DESC
        LIMIT 1
        """

        try:
            row = await self.db_conn.fetchrow(query, int(company_id.replace('-', '')[:8], 16) % 1000000, score_date)

            if not row or not row["neighbors"]:
                return None

            import json
            neighbors = json.loads(row["neighbors"]) if isinstance(row["neighbors"], str) else row["neighbors"]

            if not neighbors:
                return None

            # Calculate weighted average prediction
            total_weight = 0
            weighted_return = 0

            for n in neighbors[:5]:  # Use top 5 neighbors
                distance = n.get("distance", 1.0)
                weight = 1.0 / (distance + 0.01)
                label = n.get("label", {})
                ret_5d = label.get("5d_return", 0) if isinstance(label, dict) else 0

                weighted_return += ret_5d * weight
                total_weight += weight

            predicted = weighted_return / total_weight if total_weight > 0 else 0

            return {
                "predicted_5d_return": predicted,
                "neighbors_used": min(5, len(neighbors)),
            }
        except Exception as e:
            logger.warning(f"KNN prediction lookup failed: {e}")
            return None

    def _no_data_score(self, company_id: str, score_date: date) -> DimensionScore:
        """Return neutral score when no data available."""
        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=50.0,
            confidence=0.0,
            data_quality=0.0,
            metadata={"no_data": True},
            definition_version=1,
        )
