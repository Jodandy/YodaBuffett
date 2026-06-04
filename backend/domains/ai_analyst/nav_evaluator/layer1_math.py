"""
Layer 1 - Deterministic NAV Math

Anchors on balance sheet, not earnings.
Splits NAV into HARD (liquid, recoverable) and SOFT (goodwill, intangibles).
Filters for clean, durable asset plays with margin of safety.
"""
from typing import Dict, Any
from .models import CompanyAssetInput, Layer1Result, JudgmentFlag


class Layer1MathEngine:
    """
    Deterministic NAV calculations.

    Evaluates asset-backed companies on:
    - Hard NAV vs soft NAV
    - Discount to hard NAV (margin of safety)
    - Runway (survival duration)
    - Dilution risk
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize with configuration.

        Args:
            config: Dict with keys:
                - disc_min: Minimum discount to hard NAV
                - soft_max: Maximum soft assets fraction
                - runway_min: Minimum runway in quarters
                - issue_max: Maximum recent share issuance
                - investment_property_haircut: Haircut on investment property
        """
        self.disc_min = config['disc_min']
        self.soft_max = config['soft_max']
        self.runway_min = config['runway_min']
        self.issue_max = config['issue_max']
        self.inv_prop_haircut = config['investment_property_haircut']

    def evaluate(self, company: CompanyAssetInput) -> Layer1Result:
        """
        Run Layer 1 math on a single company.

        Returns:
            Layer1Result with all calculated fields
        """
        # Calculate NAV per share
        book_nav_ps = company.total_equity / company.shares_outstanding

        # Calculate hard assets (liquid, recoverable)
        hard_assets = company.cash_and_equivalents
        if company.marketable_securities:
            hard_assets += company.marketable_securities
        if company.receivables:
            hard_assets += company.receivables
        if company.investment_property:
            # Apply haircut to investment property
            hard_assets += company.investment_property * (1 - self.inv_prop_haircut)

        # Hard NAV per share = (hard assets - total liabilities) / shares
        hard_nav_ps = (hard_assets - company.total_liabilities) / company.shares_outstanding

        # Calculate soft assets fraction
        soft_assets = 0.0
        if company.goodwill:
            soft_assets += company.goodwill
        if company.intangibles:
            soft_assets += company.intangibles

        soft_fraction = soft_assets / company.total_assets if company.total_assets > 0 else 0.0

        # Calculate discounts
        disc_to_hard_nav = (hard_nav_ps - company.price) / hard_nav_ps if hard_nav_ps > 0 else 0.0
        disc_to_book = (book_nav_ps - company.price) / book_nav_ps if book_nav_ps > 0 else 0.0

        # Calculate runway
        runway_quarters = None
        if company.quarterly_cash_burn and company.quarterly_cash_burn > 0:
            # Burn is positive value (absolute of negative OCF)
            runway_quarters = company.cash_and_equivalents / company.quarterly_cash_burn

        # Check dilution flag
        dilution_flag = False
        if runway_quarters is not None and runway_quarters < self.runway_min:
            dilution_flag = True
        if company.recent_share_issuance and company.recent_share_issuance > self.issue_max:
            dilution_flag = True

        # Candidate filter (all conditions must pass)
        is_candidate = (
            disc_to_hard_nav >= self.disc_min and
            soft_fraction <= self.soft_max and
            (runway_quarters is None or runway_quarters >= self.runway_min) and
            not dilution_flag
        )

        return Layer1Result(
            ticker=company.ticker,
            name=company.name,
            price=company.price,
            book_nav_ps=book_nav_ps,
            hard_nav_ps=hard_nav_ps,
            soft_fraction=soft_fraction,
            disc_to_hard_nav=disc_to_hard_nav,
            disc_to_book=disc_to_book,
            runway_quarters=runway_quarters,
            dilution_flag=dilution_flag,
            is_candidate=is_candidate
        )
