"""
Dimension calculators.

Each calculator is a "black box" that implements its own logic
but produces a standardized DimensionScore output.
"""

from .base import BaseDimensionCalculator, calculator_registry

__all__ = [
    "BaseDimensionCalculator",
    "calculator_registry",
]
