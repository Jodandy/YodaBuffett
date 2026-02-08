"""
Base dimension calculator interface.

All dimension calculators inherit from this base class.
Each calculator is a "black box" - it can use any methodology internally
(weighted factors, ML models, NLP, external APIs, etc.) but must produce
a standardized DimensionScore output.

Example implementations:
- ValueCalculator: Simple weighted fundamental ratios
- SentimentCalculator: NLP on news + embeddings + social signals
- MoatCalculator: AI classification + margin analysis + competitive dynamics
- RegulatoryRiskCalculator: Rule-based checks + filing keyword analysis
- CreditCalculator: External ratings + internal debt analysis
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Type
from datetime import date
import asyncio
import time
import logging

from ..models.dimension import DimensionScore, DimensionDefinition, ComputationResult

logger = logging.getLogger(__name__)


class BaseDimensionCalculator(ABC):
    """
    Abstract base class for all dimension calculators.

    Subclasses must implement:
    - dimension_code: The unique identifier for this dimension
    - definition: Metadata about this dimension
    - calculate(): The core calculation logic (can be anything)

    Optionally override:
    - calculate_batch(): For optimized batch processing
    - validate_inputs(): To check if calculation is possible
    """

    def __init__(self, db_conn=None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the calculator.

        Args:
            db_conn: Database connection (asyncpg connection or pool)
            config: Optional configuration overrides
        """
        self.db_conn = db_conn
        self.config = config or {}
        self._version = "1.0.0"

    @property
    @abstractmethod
    def dimension_code(self) -> str:
        """
        Unique identifier for this dimension.
        Examples: 'value', 'momentum', 'sentiment', 'moat', 'regulatory_risk'
        """
        pass

    @property
    @abstractmethod
    def definition(self) -> DimensionDefinition:
        """
        Metadata about this dimension.
        Describes what it is, not how it's calculated.
        """
        pass

    @abstractmethod
    async def calculate(
        self,
        company_id: str,
        score_date: date,
        **kwargs
    ) -> DimensionScore:
        """
        Calculate the dimension score for a single company.

        This is the core method that each dimension must implement.
        It can do anything internally - the only requirement is to
        return a DimensionScore with the standardized fields.

        Args:
            company_id: UUID of the company
            score_date: Date to calculate score for
            **kwargs: Dimension-specific parameters

        Returns:
            DimensionScore with all required fields populated
        """
        pass

    async def calculate_batch(
        self,
        company_ids: List[str],
        score_date: date,
        **kwargs
    ) -> ComputationResult:
        """
        Calculate dimension scores for multiple companies.

        Default implementation calls calculate() for each company.
        Override this for optimized batch processing (e.g., bulk data fetching).

        Args:
            company_ids: List of company UUIDs
            score_date: Date to calculate scores for
            **kwargs: Dimension-specific parameters

        Returns:
            ComputationResult with all scores and statistics
        """
        result = ComputationResult(
            dimension_code=self.dimension_code,
            score_date=score_date,
        )

        for company_id in company_ids:
            result.companies_processed += 1

            try:
                # Check if we can calculate for this company
                if not await self.validate_inputs(company_id, score_date):
                    result.companies_skipped += 1
                    continue

                # Calculate
                start_time = time.time()
                score = await self.calculate(company_id, score_date, **kwargs)

                # Handle case where calculator returns None (insufficient data)
                if score is None:
                    result.companies_skipped += 1
                    continue

                score.computation_time_ms = int((time.time() - start_time) * 1000)
                score.calculator_version = self._version

                result.scores.append(score)
                result.companies_succeeded += 1

            except Exception as e:
                result.companies_failed += 1
                result.add_error(
                    error_type=type(e).__name__,
                    company_id=company_id,
                    details=str(e)
                )
                logger.warning(f"Failed to calculate {self.dimension_code} for {company_id}: {e}")

        result.mark_completed()
        return result

    async def validate_inputs(
        self,
        company_id: str,
        score_date: date
    ) -> bool:
        """
        Check if calculation is possible for this company/date.

        Override this to add dimension-specific validation.
        Default implementation always returns True.

        Args:
            company_id: UUID of the company
            score_date: Date to calculate for

        Returns:
            True if calculation can proceed, False to skip
        """
        return True

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a configuration value."""
        return self.config.get(key, default)


class CalculatorRegistry:
    """
    Registry for dimension calculators.

    Allows dynamic registration and lookup of calculators.
    This makes it easy to add new dimensions without modifying existing code.

    Usage:
        # Register a calculator
        calculator_registry.register(ValueCalculator)

        # Get a calculator by dimension code
        calc = calculator_registry.get('value', db_conn=conn)

        # Get all registered dimensions
        dimensions = calculator_registry.list_dimensions()
    """

    def __init__(self):
        self._calculators: Dict[str, Type[BaseDimensionCalculator]] = {}

    def register(self, calculator_class: Type[BaseDimensionCalculator]) -> None:
        """
        Register a calculator class.

        Args:
            calculator_class: The calculator class to register
        """
        # Instantiate temporarily to get dimension_code
        temp_instance = calculator_class.__new__(calculator_class)
        # Call minimal init
        temp_instance.db_conn = None
        temp_instance.config = {}
        temp_instance._version = "1.0.0"

        code = temp_instance.dimension_code
        self._calculators[code] = calculator_class
        logger.info(f"Registered dimension calculator: {code}")

    def get(
        self,
        dimension_code: str,
        db_conn=None,
        config: Optional[Dict[str, Any]] = None
    ) -> Optional[BaseDimensionCalculator]:
        """
        Get an instantiated calculator by dimension code.

        Args:
            dimension_code: The dimension to get calculator for
            db_conn: Database connection to pass to calculator
            config: Optional configuration overrides

        Returns:
            Instantiated calculator, or None if not found
        """
        calculator_class = self._calculators.get(dimension_code)
        if calculator_class is None:
            return None
        return calculator_class(db_conn=db_conn, config=config)

    def list_dimensions(self) -> List[str]:
        """Get list of all registered dimension codes."""
        return list(self._calculators.keys())

    def list_definitions(self, db_conn=None) -> List[DimensionDefinition]:
        """Get definitions for all registered dimensions."""
        definitions = []
        for code, calc_class in self._calculators.items():
            calc = calc_class(db_conn=db_conn)
            definitions.append(calc.definition)
        return definitions

    def is_registered(self, dimension_code: str) -> bool:
        """Check if a dimension is registered."""
        return dimension_code in self._calculators


# Global registry instance
calculator_registry = CalculatorRegistry()


def register_calculator(cls: Type[BaseDimensionCalculator]) -> Type[BaseDimensionCalculator]:
    """
    Decorator to register a calculator class.

    Usage:
        @register_calculator
        class ValueCalculator(BaseDimensionCalculator):
            ...
    """
    calculator_registry.register(cls)
    return cls
