"""
Dimension scoring models.

These are the standardized data structures that all dimension calculators
must produce, regardless of their internal implementation.
"""

from .dimension import (
    DimensionScore,
    DimensionDefinition,
    CompositeScore,
    ScoreHistoryPoint,
    ComputationResult,
)

__all__ = [
    "DimensionScore",
    "DimensionDefinition",
    "CompositeScore",
    "ScoreHistoryPoint",
    "ComputationResult",
]
