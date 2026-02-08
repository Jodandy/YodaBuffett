"""
Dimensions Scoring Domain

A flexible, extensible system for multi-dimensional company scoring.
Each dimension is a "black box" that produces a standardized score output,
regardless of its internal methodology.

## Architecture

Dimensions can use ANY methodology internally:
- Weighted factor aggregation (Value, Quality)
- Technical indicators (Momentum)
- AI/ML models (Sentiment)
- External APIs (Credit ratings)
- Rule-based systems (Regulatory risk)
- Hybrid approaches

The only requirement is that each dimension produces a standardized
DimensionScore output with:
- score: 0-100 (higher = better, except for risk dimensions which can be inverted)
- confidence: 0-1 (how reliable is this score)
- metadata: flexible dict with dimension-specific details

## Usage

```python
from domains.dimensions.calculators import calculator_registry
from domains.dimensions.repositories import DimensionRepository

# Get a calculator
value_calc = calculator_registry.get('value', db_conn=conn)

# Calculate for a company
score = await value_calc.calculate(company_id, score_date)

# Access the score
print(f"Value score: {score.score}")
print(f"Confidence: {score.confidence}")
print(f"Factors: {score.metadata}")

# Store in database
repo = DimensionRepository(conn)
await repo.store_dimension_score(score)
```

## Adding New Dimensions

1. Create a new calculator in `calculators/`:

```python
from .base import BaseDimensionCalculator, register_calculator

@register_calculator
class MyNewCalculator(BaseDimensionCalculator):
    @property
    def dimension_code(self) -> str:
        return "my_dimension"

    @property
    def definition(self) -> DimensionDefinition:
        return DimensionDefinition(...)

    async def calculate(self, company_id, score_date, **kwargs) -> DimensionScore:
        # Your custom logic here
        return DimensionScore(...)
```

2. Import it in the worker or CLI to register it.

3. Add a row to dimension_definitions table (optional but recommended).
"""

from .models import DimensionScore, DimensionDefinition, CompositeScore
from .calculators import BaseDimensionCalculator, calculator_registry
from .repositories import DimensionRepository

__all__ = [
    "DimensionScore",
    "DimensionDefinition",
    "CompositeScore",
    "BaseDimensionCalculator",
    "calculator_registry",
    "DimensionRepository",
]
