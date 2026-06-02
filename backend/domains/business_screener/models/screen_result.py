"""
Screen Result Model

Represents the output of a screen run for a single company.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from uuid import UUID


@dataclass
class ScreenResult:
    """
    Result of running a screen on a single company.

    Attributes:
        company_id: UUID of the company from company_master
        screen_type: Screen number (1-15)
        tier: Analysis tier ('A', 'B', 'C')
        passed: Whether the company passed the screen criteria
        score: Composite score 0-100
        metrics: Dictionary of all calculated values used in the screen
        flags: List of warnings or notable conditions
        requires_tier_b: Should this candidate go to LLM analysis?
        requires_tier_c: Should this candidate go to deep LLM analysis?
        triggered_at: When this result was generated
        expires_at: When this result should be re-evaluated
    """

    company_id: UUID
    screen_type: int
    tier: str
    passed: bool
    score: float = 0.0
    metrics: Dict[str, Any] = field(default_factory=dict)
    flags: List[str] = field(default_factory=list)
    requires_tier_b: bool = False
    requires_tier_c: bool = False
    triggered_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None

    # Optional: populated when fetching from DB
    id: Optional[int] = None
    is_active: bool = True
    company_name: Optional[str] = None
    primary_ticker: Optional[str] = None

    def __post_init__(self):
        """Validate the screen result."""
        if self.screen_type < 1 or self.screen_type > 20:
            raise ValueError(f"screen_type must be 1-20, got {self.screen_type}")

        if self.tier not in ('A', 'B', 'C'):
            raise ValueError(f"tier must be 'A', 'B', or 'C', got {self.tier}")

        if self.score < 0 or self.score > 100:
            raise ValueError(f"score must be 0-100, got {self.score}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'company_id': str(self.company_id),
            'company_name': self.company_name,
            'primary_ticker': self.primary_ticker,
            'screen_type': self.screen_type,
            'tier': self.tier,
            'passed': self.passed,
            'score': self.score,
            'metrics': self.metrics,
            'flags': self.flags,
            'requires_tier_b': self.requires_tier_b,
            'requires_tier_c': self.requires_tier_c,
            'is_active': self.is_active,
            'triggered_at': self.triggered_at.isoformat() if self.triggered_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
        }

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'ScreenResult':
        """Create a ScreenResult from a database row."""
        # Parse metrics if it's a JSON string
        metrics = row.get('metrics', {})
        if isinstance(metrics, str):
            try:
                metrics = json.loads(metrics)
            except json.JSONDecodeError:
                metrics = {}

        return cls(
            id=row.get('id'),
            company_id=row['company_id'],
            screen_type=row['screen_type'],
            tier=row['tier'],
            passed=True,  # If it's in the DB, it passed
            score=float(row.get('score', 0)),
            metrics=metrics,
            flags=row.get('flags', []),
            is_active=row.get('is_active', True),
            triggered_at=row.get('triggered_at', datetime.now()),
            expires_at=row.get('expires_at'),
            company_name=row.get('company_name'),
            primary_ticker=row.get('primary_ticker'),
        )

    def add_flag(self, flag: str):
        """Add a warning or notable condition."""
        if flag not in self.flags:
            self.flags.append(flag)

    def set_tier_b_required(self, reason: str = None):
        """Mark this result as requiring Tier B LLM analysis."""
        self.requires_tier_b = True
        if reason:
            self.add_flag(f"TIER_B_REQUIRED: {reason}")

    def set_tier_c_required(self, reason: str = None):
        """Mark this result as requiring Tier C deep analysis."""
        self.requires_tier_c = True
        if reason:
            self.add_flag(f"TIER_C_REQUIRED: {reason}")
