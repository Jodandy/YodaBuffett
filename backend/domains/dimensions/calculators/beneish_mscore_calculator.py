"""
Beneish M-Score Calculator

Detects earnings manipulation using the Beneish M-Score model.
This is a BUSINESS dimension (price-independent).

Academic Basis:
Beneish, M.D. (1999). "The Detection of Earnings Manipulation"
Financial Analysts Journal, 55(5), 24-36.

The M-Score is a mathematical model that uses 8 financial ratios:
1. DSRI - Days Sales in Receivables Index
2. GMI - Gross Margin Index
3. AQI - Asset Quality Index
4. SGI - Sales Growth Index
5. DEPI - Depreciation Index
6. SGAI - SG&A Index
7. LVGI - Leverage Index
8. TATA - Total Accruals to Total Assets

M-Score = -4.84 + 0.920*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI
          + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI

Interpretation:
- M-Score > -1.78: High probability of manipulation
- M-Score < -1.78: Low probability of manipulation

Note: Coefficients are from Beneish's original research, NOT guessed.
"""

from datetime import date
from typing import Dict, List, Optional, Any
import logging
import numpy as np

from .base import BaseDimensionCalculator, register_calculator
from ..models.dimension import DimensionScore, DimensionDefinition

logger = logging.getLogger(__name__)


# Beneish's original coefficients (from 1999 paper)
BENEISH_COEFFICIENTS = {
    "intercept": -4.84,
    "dsri": 0.920,    # Days Sales in Receivables Index
    "gmi": 0.528,     # Gross Margin Index
    "aqi": 0.404,     # Asset Quality Index
    "sgi": 0.892,     # Sales Growth Index
    "depi": 0.115,    # Depreciation Index
    "sgai": -0.172,   # SG&A Index
    "tata": 4.679,    # Total Accruals to Total Assets
    "lvgi": -0.327,   # Leverage Index
}

# Manipulation threshold from Beneish's research
MANIPULATION_THRESHOLD = -1.78


@register_calculator
class BeneishMScoreCalculator(BaseDimensionCalculator):
    """
    Beneish M-Score earnings manipulation detector.

    Uses academically-validated coefficients to calculate probability
    of earnings manipulation.

    High score = Low manipulation probability (good)
    Low score = High manipulation probability (red flag)
    """

    @property
    def dimension_code(self) -> str:
        return "beneish_mscore"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(
            dimension_code=self.dimension_code,
            display_name="Beneish M-Score",
            description="Earnings manipulation probability based on Beneish (1999) model",
            category="fundamental",
            data_sources=["financial_statements", "balance_sheet_data", "cash_flow_data"],
            update_frequency="daily",
            version="1.0.0",
        )

    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> Optional[DimensionScore]:
        """Calculate Beneish M-Score."""

        company_info = await self._get_company_info(company_id)
        if not company_info:
            return None

        symbol = company_info["primary_ticker"]
        if not symbol:
            return None

        # Need current and prior year data
        current = await self._get_financial_data(symbol, score_date, offset=0)
        prior = await self._get_financial_data(symbol, score_date, offset=1)

        if not current or not prior:
            return None

        # Calculate the 8 Beneish variables
        variables = self._calculate_variables(current, prior)

        if len(variables) < 5:  # Need most variables
            return None

        # Calculate M-Score
        m_score = self._calculate_m_score(variables)

        # Convert to 0-100 score (higher = safer)
        dimension_score = self._m_score_to_dimension_score(m_score)

        # Determine manipulation probability
        manipulation_prob = self._calculate_manipulation_probability(m_score)

        return DimensionScore(
            company_id=company_id,
            score_date=score_date,
            dimension_code=self.dimension_code,
            score=dimension_score,
            confidence=len(variables) / 8,  # Based on data completeness
            data_quality=len(variables) / 8,
            score_low=max(0, dimension_score - 10),
            score_high=min(100, dimension_score + 10),
            metadata={
                "m_score": round(m_score, 3),
                "manipulation_probability": manipulation_prob,
                "threshold": MANIPULATION_THRESHOLD,
                "variables": variables,
                "interpretation": self._interpret(m_score),
                "red_flags": self._identify_red_flags(variables),
            },
            definition_version=1,
        )

    def _calculate_variables(self, current: Dict, prior: Dict) -> Dict[str, float]:
        """Calculate the 8 Beneish variables."""

        variables = {}

        # Helper to safely get float values
        def safe_float(d, key, default=0):
            val = d.get(key)
            return float(val) if val is not None else default

        # Current year values
        c_receivables = safe_float(current, "accounts_receivable")
        c_revenue = safe_float(current, "total_revenue", 1)
        c_gross_profit = safe_float(current, "gross_profit")
        c_current_assets = safe_float(current, "current_assets")
        c_ppe = safe_float(current, "total_assets") - c_current_assets  # Rough PPE proxy
        c_total_assets = safe_float(current, "total_assets", 1)
        c_depreciation = safe_float(current, "depreciation_amortization")
        c_sga = safe_float(current, "selling_general_administrative")
        c_total_debt = safe_float(current, "total_debt")
        c_net_income = safe_float(current, "net_income")
        c_ocf = safe_float(current, "operating_cash_flow")

        # Prior year values
        p_receivables = safe_float(prior, "accounts_receivable", 1)
        p_revenue = safe_float(prior, "total_revenue", 1)
        p_gross_profit = safe_float(prior, "gross_profit", 1)
        p_current_assets = safe_float(prior, "current_assets", 1)
        p_ppe = safe_float(prior, "total_assets", 1) - p_current_assets
        p_total_assets = safe_float(prior, "total_assets", 1)
        p_depreciation = safe_float(prior, "depreciation_amortization", 1)
        p_sga = safe_float(prior, "selling_general_administrative", 1)
        p_total_debt = safe_float(prior, "total_debt", 1)

        # 1. DSRI - Days Sales in Receivables Index
        # (Receivables_t / Sales_t) / (Receivables_t-1 / Sales_t-1)
        if p_revenue > 0 and c_revenue > 0 and p_receivables > 0:
            dsr_current = c_receivables / c_revenue
            dsr_prior = p_receivables / p_revenue
            if dsr_prior > 0:
                variables["dsri"] = round(dsr_current / dsr_prior, 4)

        # 2. GMI - Gross Margin Index
        # Gross_Margin_t-1 / Gross_Margin_t
        if c_revenue > 0 and p_revenue > 0:
            gm_current = c_gross_profit / c_revenue if c_revenue > 0 else 0
            gm_prior = p_gross_profit / p_revenue if p_revenue > 0 else 0
            if gm_current > 0:
                variables["gmi"] = round(gm_prior / gm_current, 4)

        # 3. AQI - Asset Quality Index
        # (1 - (CA_t + PPE_t) / TA_t) / (1 - (CA_t-1 + PPE_t-1) / TA_t-1)
        if c_total_assets > 0 and p_total_assets > 0:
            aq_current = 1 - (c_current_assets + c_ppe) / c_total_assets
            aq_prior = 1 - (p_current_assets + p_ppe) / p_total_assets
            if aq_prior != 0:
                variables["aqi"] = round(aq_current / aq_prior, 4)

        # 4. SGI - Sales Growth Index
        # Sales_t / Sales_t-1
        if p_revenue > 0:
            variables["sgi"] = round(c_revenue / p_revenue, 4)

        # 5. DEPI - Depreciation Index
        # (Depreciation_t-1 / (Depreciation_t-1 + PPE_t-1)) /
        # (Depreciation_t / (Depreciation_t + PPE_t))
        if c_depreciation + c_ppe > 0 and p_depreciation + p_ppe > 0:
            depi_current = c_depreciation / (c_depreciation + c_ppe)
            depi_prior = p_depreciation / (p_depreciation + p_ppe)
            if depi_current > 0:
                variables["depi"] = round(depi_prior / depi_current, 4)

        # 6. SGAI - SG&A Index
        # (SGA_t / Sales_t) / (SGA_t-1 / Sales_t-1)
        if c_revenue > 0 and p_revenue > 0 and c_sga > 0 and p_sga > 0:
            sga_current = c_sga / c_revenue
            sga_prior = p_sga / p_revenue
            if sga_prior > 0:
                variables["sgai"] = round(sga_current / sga_prior, 4)

        # 7. LVGI - Leverage Index
        # ((LTD_t + CL_t) / TA_t) / ((LTD_t-1 + CL_t-1) / TA_t-1)
        c_total_liab = safe_float(current, "total_liabilities", 0)
        p_total_liab = safe_float(prior, "total_liabilities", 1)

        if c_total_assets > 0 and p_total_assets > 0:
            lev_current = c_total_liab / c_total_assets
            lev_prior = p_total_liab / p_total_assets
            if lev_prior > 0:
                variables["lvgi"] = round(lev_current / lev_prior, 4)

        # 8. TATA - Total Accruals to Total Assets
        # (Net Income - OCF) / Total Assets
        if c_total_assets > 0:
            accruals = c_net_income - c_ocf
            variables["tata"] = round(accruals / c_total_assets, 4)

        return variables

    def _calculate_m_score(self, variables: Dict[str, float]) -> float:
        """Calculate the M-Score using Beneish coefficients."""

        m_score = BENEISH_COEFFICIENTS["intercept"]

        for var_name, coefficient in BENEISH_COEFFICIENTS.items():
            if var_name == "intercept":
                continue
            if var_name in variables:
                m_score += coefficient * variables[var_name]

        return m_score

    def _m_score_to_dimension_score(self, m_score: float) -> float:
        """
        Convert M-Score to 0-100 dimension score.

        M-Score < -2.5: Very safe → Score ~90
        M-Score = -1.78: Threshold → Score ~50
        M-Score > -1.0: Likely manipulation → Score ~20
        """
        # Linear transformation around the threshold
        # Range roughly -4 to 0 for M-Score
        if m_score < -3.0:
            return 95.0
        elif m_score < -2.5:
            return 85.0
        elif m_score < -2.0:
            return 70.0
        elif m_score < MANIPULATION_THRESHOLD:
            return 55.0
        elif m_score < -1.5:
            return 40.0
        elif m_score < -1.0:
            return 25.0
        else:
            return 10.0

    def _calculate_manipulation_probability(self, m_score: float) -> Dict[str, Any]:
        """Estimate manipulation probability based on M-Score."""

        if m_score > -1.0:
            return {"level": "very_high", "description": "Very high probability of manipulation"}
        elif m_score > MANIPULATION_THRESHOLD:
            return {"level": "high", "description": "High probability of manipulation"}
        elif m_score > -2.22:
            return {"level": "moderate", "description": "Moderate risk, monitor closely"}
        elif m_score > -2.5:
            return {"level": "low", "description": "Low probability of manipulation"}
        else:
            return {"level": "very_low", "description": "Very low probability of manipulation"}

    def _interpret(self, m_score: float) -> str:
        """Human-readable interpretation."""

        if m_score > MANIPULATION_THRESHOLD:
            return f"M-Score of {m_score:.2f} EXCEEDS threshold of {MANIPULATION_THRESHOLD}. Potential earnings manipulation detected."
        else:
            return f"M-Score of {m_score:.2f} is below threshold of {MANIPULATION_THRESHOLD}. No manipulation signals detected."

    def _identify_red_flags(self, variables: Dict[str, float]) -> List[str]:
        """Identify which specific variables are concerning."""

        red_flags = []

        # DSRI > 1.031 is concerning (receivables growing faster than sales)
        if variables.get("dsri", 0) > 1.2:
            red_flags.append(f"DSRI={variables['dsri']:.2f}: Receivables growing faster than sales")

        # GMI > 1.0 means margins are declining
        if variables.get("gmi", 0) > 1.1:
            red_flags.append(f"GMI={variables['gmi']:.2f}: Gross margins declining")

        # AQI > 1.0 means asset quality declining
        if variables.get("aqi", 0) > 1.2:
            red_flags.append(f"AQI={variables['aqi']:.2f}: Asset quality declining")

        # SGI > 1.4 is very high growth (can be good or manipulation)
        if variables.get("sgi", 0) > 1.5:
            red_flags.append(f"SGI={variables['sgi']:.2f}: Very high sales growth - verify")

        # TATA > 0.05 means high accruals relative to assets
        if variables.get("tata", 0) > 0.05:
            red_flags.append(f"TATA={variables['tata']:.3f}: High accruals relative to assets")

        return red_flags

    async def _get_company_info(self, company_id: str) -> Optional[Dict]:
        row = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker, yahoo_symbol
            FROM company_master WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_financial_data(
        self,
        symbol: str,
        score_date: date,
        offset: int = 0
    ) -> Optional[Dict]:
        """
        Get combined financial data for a period.
        offset=0 is most recent, offset=1 is prior year.
        """

        # Get the Nth most recent annual statement (point-in-time safe)
        fs = await self.db_conn.fetchrow("""
            SELECT
                period_date, total_revenue, gross_profit, net_income,
                selling_general_administrative
            FROM financial_statements
            WHERE symbol = $1
            AND (
                (publish_date IS NOT NULL AND publish_date <= $2)
                OR
                (publish_date IS NULL AND period_date + INTERVAL '75 days' <= $2)
            )
            AND statement_type = 'annual'
            ORDER BY period_date DESC
            OFFSET $3 LIMIT 1
        """, symbol, score_date, offset)

        if not fs:
            # No annual data available
            return None

        period_date = fs["period_date"]

        # Get matching annual balance sheet
        bs = await self.db_conn.fetchrow("""
            SELECT
                accounts_receivable, current_assets, total_assets,
                total_liabilities, total_debt
            FROM balance_sheet_data
            WHERE symbol = $1 AND period_date = $2
            AND statement_type = 'annual'
        """, symbol, period_date)

        # Get matching annual cash flow
        cf = await self.db_conn.fetchrow("""
            SELECT operating_cash_flow, depreciation_amortization
            FROM cash_flow_data
            WHERE symbol = $1 AND period_date = $2
            AND statement_type = 'annual'
        """, symbol, period_date)

        result = dict(fs)
        if bs:
            result.update(dict(bs))
        if cf:
            result.update(dict(cf))

        return result
