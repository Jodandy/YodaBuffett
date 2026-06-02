"""
Response Parser and Validator

Parses and validates LLM JSON responses against expected schemas.
Each screen type has specific expected response formats.
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Set
from enum import Enum


class ValidationLevel(Enum):
    """How strictly to validate responses."""
    STRICT = "strict"      # All required fields must be present and valid
    LENIENT = "lenient"    # Missing fields get defaults, type coercion attempted
    MINIMAL = "minimal"    # Just check it's valid JSON


@dataclass
class ValidationResult:
    """Result of validating an LLM response."""
    is_valid: bool
    cleaned_data: Dict[str, Any]
    errors: List[str]
    warnings: List[str]

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


class ResponseParser:
    """
    Parses and validates LLM responses for each screen type.

    Each screen's get_tier_b_prompt() and get_tier_c_prompt() methods
    specify the expected JSON response format. This class validates
    responses against those schemas.
    """

    # Valid enum values for common fields
    QUALITY_LEVELS = {"HIGH", "MEDIUM", "LOW"}
    RISK_LEVELS = {"LOW", "MEDIUM", "HIGH"}
    CONFIDENCE_LEVELS = {"LOW", "MEDIUM", "HIGH"}
    YES_NO_PARTIAL = {"YES", "NO", "PARTIAL", "MOSTLY", "UNKNOWN"}
    MOAT_TYPES = {"NONE", "NARROW", "WIDE", "VERY_WIDE"}
    TRAJECTORY = {"NARROWING", "STABLE", "WIDENING"}
    BUY_RATINGS = {"STRONG_BUY", "BUY", "SPECULATIVE_BUY", "HOLD", "AVOID"}

    def __init__(self, level: ValidationLevel = ValidationLevel.LENIENT):
        self.level = level

    def validate(
        self,
        response: Dict[str, Any],
        screen_type: int,
        tier: str
    ) -> ValidationResult:
        """
        Validate an LLM response for a specific screen and tier.

        Args:
            response: Parsed JSON from LLM
            screen_type: Screen number (1-15)
            tier: 'B' or 'C'

        Returns:
            ValidationResult with cleaned data and any errors/warnings
        """
        errors = []
        warnings = []
        cleaned = {}

        # Get schema for this screen/tier
        schema = self._get_schema(screen_type, tier)

        if not schema:
            # No schema defined - just return the response as-is
            return ValidationResult(
                is_valid=True,
                cleaned_data=response,
                errors=[],
                warnings=["No schema defined for screen {screen_type} tier {tier}"]
            )

        # Validate each field
        for field_name, field_spec in schema.items():
            value = response.get(field_name)

            if value is None:
                if field_spec.get("required", False) and self.level == ValidationLevel.STRICT:
                    errors.append(f"Missing required field: {field_name}")
                else:
                    cleaned[field_name] = field_spec.get("default")
                    if field_spec.get("required", False):
                        warnings.append(f"Missing field {field_name}, using default")
                continue

            # Type validation and coercion
            expected_type = field_spec.get("type", "string")
            validated_value, type_error = self._validate_type(value, expected_type, field_spec)

            if type_error:
                if self.level == ValidationLevel.STRICT:
                    errors.append(f"Field {field_name}: {type_error}")
                else:
                    warnings.append(f"Field {field_name}: {type_error}")
                    cleaned[field_name] = field_spec.get("default")
            else:
                cleaned[field_name] = validated_value

        # Add any extra fields from response (in lenient mode)
        if self.level != ValidationLevel.STRICT:
            for key, value in response.items():
                if key not in cleaned:
                    cleaned[key] = value
                    warnings.append(f"Extra field in response: {key}")

        is_valid = len(errors) == 0

        return ValidationResult(
            is_valid=is_valid,
            cleaned_data=cleaned,
            errors=errors,
            warnings=warnings
        )

    def _validate_type(
        self,
        value: Any,
        expected_type: str,
        field_spec: Dict[str, Any]
    ) -> tuple:
        """
        Validate and coerce a value to the expected type.

        Returns:
            (validated_value, error_message or None)
        """
        if expected_type == "string":
            if isinstance(value, str):
                # Check enum constraints
                allowed = field_spec.get("enum")
                if allowed and value.upper() not in allowed:
                    # Try to match case-insensitively
                    upper_value = value.upper()
                    if upper_value in allowed:
                        return upper_value, None
                    return None, f"Invalid value '{value}', expected one of {allowed}"
                return value, None
            return str(value), None

        elif expected_type == "number":
            if isinstance(value, (int, float)):
                return float(value), None
            try:
                return float(value), None
            except (ValueError, TypeError):
                return None, f"Cannot convert '{value}' to number"

        elif expected_type == "integer":
            if isinstance(value, int):
                return value, None
            try:
                return int(float(value)), None
            except (ValueError, TypeError):
                return None, f"Cannot convert '{value}' to integer"

        elif expected_type == "boolean":
            if isinstance(value, bool):
                return value, None
            if isinstance(value, str):
                if value.lower() in ("true", "yes", "1"):
                    return True, None
                if value.lower() in ("false", "no", "0"):
                    return False, None
            return None, f"Cannot convert '{value}' to boolean"

        elif expected_type == "array":
            if isinstance(value, list):
                return value, None
            if isinstance(value, str):
                # Try to parse as comma-separated
                return [v.strip() for v in value.split(",")], None
            return [value], None

        elif expected_type == "object":
            if isinstance(value, dict):
                return value, None
            return None, f"Expected object, got {type(value).__name__}"

        return value, None

    def _get_schema(self, screen_type: int, tier: str) -> Optional[Dict[str, Any]]:
        """
        Get the validation schema for a screen/tier combination.

        Returns field specifications with:
        - type: string, number, integer, boolean, array, object
        - required: bool
        - enum: set of valid values (for strings)
        - default: default value if missing
        """
        schemas = {
            # Screen 3: Asset Plays - Tier B
            (3, "B"): {
                "receivables_quality": {"type": "string", "enum": self.QUALITY_LEVELS, "default": "MEDIUM"},
                "receivables_concerns": {"type": "string", "default": None},
                "inventory_risk": {"type": "string", "enum": self.RISK_LEVELS, "default": "MEDIUM"},
                "inventory_concerns": {"type": "string", "default": None},
                "ppe_composition": {"type": "string", "enum": {"REAL_ESTATE", "EQUIPMENT", "MIXED"}, "default": "MIXED"},
                "ppe_marketability": {"type": "string", "enum": self.QUALITY_LEVELS, "default": "MEDIUM"},
                "hidden_assets": {"type": "array", "default": []},
                "estimated_liquidation_value": {"type": "number", "default": None},
                "liquidation_vs_book": {"type": "string", "enum": {"ABOVE", "AT", "BELOW"}, "default": "AT"},
                "overall_asset_quality": {"type": "string", "enum": self.QUALITY_LEVELS, "default": "MEDIUM"},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },

            # Screen 4: Revenue Turnarounds - Tier B
            (4, "B"): {
                "revenue_decline_cause": {"type": "string", "required": True, "default": "Unknown"},
                "decline_temporary_or_structural": {"type": "string", "enum": {"TEMPORARY", "STRUCTURAL", "UNCERTAIN"}, "default": "UNCERTAIN"},
                "management_actions": {"type": "array", "default": []},
                "cost_structure_analysis": {"type": "string", "default": None},
                "competitive_position": {"type": "string", "enum": {"STRONG", "STABLE", "WEAKENING", "WEAK"}, "default": "STABLE"},
                "recovery_catalysts": {"type": "array", "default": []},
                "recovery_timeline_quarters": {"type": "integer", "default": None},
                "normalized_margin_estimate": {"type": "number", "default": None},
                "survival_probability": {"type": "string", "enum": self.QUALITY_LEVELS, "default": "MEDIUM"},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },

            # Screen 9: Holding Company Discounts - Tier B
            (9, "B"): {
                "is_holding_company": {"type": "string", "enum": {"PURE_HOLDING", "HYBRID", "OPERATING_WITH_INVESTMENTS"}, "default": "HYBRID"},
                "major_holdings": {"type": "array", "default": []},
                "calculated_nav": {"type": "number", "default": None},
                "nav_per_share": {"type": "number", "default": None},
                "true_discount_pct": {"type": "number", "default": None},
                "discount_reasons": {"type": "array", "default": []},
                "historical_discount_avg": {"type": "number", "default": None},
                "catalysts": {"type": "array", "default": []},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },

            # Screen 12: Wonderful Business - Tier C
            (12, "C"): {
                "switching_costs": {"type": "string", "enum": {"NONE", "LOW", "MEDIUM", "HIGH"}, "default": "MEDIUM"},
                "switching_costs_detail": {"type": "string", "default": None},
                "network_effects": {"type": "string", "enum": {"NONE", "WEAK", "MODERATE", "STRONG"}, "default": "NONE"},
                "network_effects_detail": {"type": "string", "default": None},
                "cost_advantages": {"type": "string", "enum": {"NONE", "SMALL", "MODERATE", "LARGE"}, "default": "NONE"},
                "cost_advantages_detail": {"type": "string", "default": None},
                "intangible_assets": {"type": "string", "enum": {"WEAK", "MODERATE", "STRONG"}, "default": "MODERATE"},
                "intangible_assets_detail": {"type": "string", "default": None},
                "efficient_scale": {"type": "string", "enum": {"NO", "PARTIAL", "YES"}, "default": "NO"},
                "efficient_scale_detail": {"type": "string", "default": None},
                "moat_trajectory": {"type": "string", "enum": self.TRAJECTORY, "default": "STABLE"},
                "disruption_risk": {"type": "string", "enum": self.RISK_LEVELS, "default": "MEDIUM"},
                "reinvestment_runway_years": {"type": "number", "default": None},
                "overall_moat_strength": {"type": "string", "enum": self.MOAT_TYPES, "default": "NARROW"},
                "confidence": {"type": "string", "enum": self.CONFIDENCE_LEVELS, "default": "MEDIUM"},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },

            # Screen 13: Crisis Bargains - Tier B
            (13, "B"): {
                "crisis_type": {"type": "string", "enum": {"EARNINGS", "LEGAL", "CUSTOMER_LOSS", "SCANDAL", "INDUSTRY", "FRAUD", "OTHER"}, "default": "OTHER"},
                "crisis_description": {"type": "string", "required": True, "default": "Unknown crisis"},
                "severity": {"type": "string", "enum": {"MINOR", "MODERATE", "SEVERE", "EXISTENTIAL"}, "default": "MODERATE"},
                "worst_case_priced_in": {"type": "string", "enum": {"YES", "PARTIALLY", "NO"}, "default": "PARTIALLY"},
                "resolution_timeline_months": {"type": "number", "default": None},
                "core_business_intact": {"type": "string", "enum": self.YES_NO_PARTIAL, "default": "UNKNOWN"},
                "survival_probability": {"type": "string", "enum": self.QUALITY_LEVELS, "default": "MEDIUM"},
                "normalized_pe_estimate": {"type": "number", "default": None},
                "potential_upside_pct": {"type": "number", "default": None},
                "key_risks": {"type": "array", "default": []},
                "buy_rating": {"type": "string", "enum": self.BUY_RATINGS, "default": "HOLD"},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },

            # Screen 14: Cyclicals - Tier B
            (14, "B"): {
                "cycle_driver": {"type": "string", "enum": {"COMMODITY", "GDP", "CAPEX", "RATES", "OTHER"}, "default": "OTHER"},
                "cycle_driver_detail": {"type": "string", "default": None},
                "current_vs_historical_trough": {"type": "string", "enum": {"WORSE", "SIMILAR", "BETTER"}, "default": "SIMILAR"},
                "recovery_catalysts": {"type": "array", "default": []},
                "expected_recovery_quarters": {"type": "number", "default": None},
                "balance_sheet_survival": {"type": "string", "enum": {"STRONG", "ADEQUATE", "AT_RISK"}, "default": "ADEQUATE"},
                "structural_decline_risk": {"type": "string", "enum": self.RISK_LEVELS, "default": "MEDIUM"},
                "is_cyclical_or_structural": {"type": "string", "enum": {"CYCLICAL", "STRUCTURAL", "UNCERTAIN"}, "default": "UNCERTAIN"},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },

            # Screen 15: Stalwarts - Tier B
            (15, "B"): {
                "decline_cause": {"type": "string", "enum": {"COMPANY_SPECIFIC", "SECTOR", "MARKET", "GUIDANCE", "ANALYST", "OTHER"}, "default": "OTHER"},
                "decline_cause_detail": {"type": "string", "default": None},
                "temporary_or_structural": {"type": "string", "enum": {"TEMPORARY", "STRUCTURAL", "UNCERTAIN"}, "default": "UNCERTAIN"},
                "fundamentals_intact": {"type": "string", "enum": {"YES", "MOSTLY", "DETERIORATING"}, "default": "MOSTLY"},
                "management_actions": {"type": "array", "default": []},
                "buyback_activity": {"type": "string", "enum": {"AGGRESSIVE", "MODERATE", "NONE"}, "default": "NONE"},
                "dividend_status": {"type": "string", "enum": {"RAISED", "MAINTAINED", "CUT"}, "default": "MAINTAINED"},
                "recovery_catalysts": {"type": "array", "default": []},
                "expected_recovery_months": {"type": "number", "default": None},
                "buy_rating": {"type": "string", "enum": self.BUY_RATINGS, "default": "HOLD"},
                "summary": {"type": "string", "required": True, "default": "Analysis not available"},
            },
        }

        return schemas.get((screen_type, tier))

    def extract_score_adjustment(
        self,
        validated_response: Dict[str, Any],
        screen_type: int
    ) -> float:
        """
        Calculate a score adjustment based on LLM analysis.

        Screens can be boosted or penalized based on LLM findings.
        Returns a value from -20 to +20 to add to the base score.

        Args:
            validated_response: Cleaned LLM response
            screen_type: Screen number

        Returns:
            Score adjustment (-20 to +20)
        """
        adjustment = 0.0

        # Common positive signals
        if validated_response.get("overall_asset_quality") == "HIGH":
            adjustment += 10
        if validated_response.get("survival_probability") == "HIGH":
            adjustment += 8
        if validated_response.get("core_business_intact") == "YES":
            adjustment += 8
        if validated_response.get("fundamentals_intact") == "YES":
            adjustment += 10
        if validated_response.get("overall_moat_strength") == "WIDE":
            adjustment += 12
        if validated_response.get("overall_moat_strength") == "VERY_WIDE":
            adjustment += 15
        if validated_response.get("buy_rating") == "STRONG_BUY":
            adjustment += 10
        if validated_response.get("buy_rating") == "BUY":
            adjustment += 5
        if validated_response.get("moat_trajectory") == "WIDENING":
            adjustment += 8
        if validated_response.get("is_cyclical_or_structural") == "CYCLICAL":
            adjustment += 5  # Confirms cyclical thesis

        # Common negative signals
        if validated_response.get("overall_asset_quality") == "LOW":
            adjustment -= 15
        if validated_response.get("survival_probability") == "LOW":
            adjustment -= 20
        if validated_response.get("core_business_intact") == "DAMAGED":
            adjustment -= 15
        if validated_response.get("fundamentals_intact") == "DETERIORATING":
            adjustment -= 15
        if validated_response.get("structural_decline_risk") == "HIGH":
            adjustment -= 15
        if validated_response.get("buy_rating") == "AVOID":
            adjustment -= 15
        if validated_response.get("moat_trajectory") == "NARROWING":
            adjustment -= 10
        if validated_response.get("is_cyclical_or_structural") == "STRUCTURAL":
            adjustment -= 15  # Not cyclical, structural decline
        if validated_response.get("severity") == "EXISTENTIAL":
            adjustment -= 20

        # Clamp to range
        return max(-20, min(20, adjustment))
