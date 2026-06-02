"""
Screen Definition Model

Metadata about each of the 15 screen types.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ScreenDefinition:
    """
    Definition of a screen type.

    Attributes:
        screen_type: Screen number (1-15)
        name: Full name of the screen
        short_name: Abbreviated name for UI
        description: What this screen looks for
        tier_a_enabled: Has a Tier A (SQL/math) component
        tier_b_enabled: Has a Tier B (local LLM) component
        tier_c_enabled: Has a Tier C (API LLM) component
        run_frequency: How often to run ('daily', 'weekly', 'monthly', 'quarterly', 'annually')
        is_active: Whether this screen is currently enabled
    """

    screen_type: int
    name: str
    short_name: str
    description: str
    tier_a_enabled: bool = True
    tier_b_enabled: bool = False
    tier_c_enabled: bool = False
    run_frequency: str = 'weekly'
    is_active: bool = True

    def __post_init__(self):
        """Validate the screen definition."""
        if self.screen_type < 1 or self.screen_type > 20:
            raise ValueError(f"screen_type must be 1-20, got {self.screen_type}")

        valid_frequencies = ['daily', 'weekly', 'monthly', 'quarterly', 'annually']
        if self.run_frequency not in valid_frequencies:
            raise ValueError(f"run_frequency must be one of {valid_frequencies}")

    @property
    def tiers(self) -> str:
        """Return string representation of enabled tiers."""
        tiers = []
        if self.tier_a_enabled:
            tiers.append('A')
        if self.tier_b_enabled:
            tiers.append('B')
        if self.tier_c_enabled:
            tiers.append('C')
        return '+'.join(tiers) if tiers else 'None'

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'screen_type': self.screen_type,
            'name': self.name,
            'short_name': self.short_name,
            'description': self.description,
            'tier_a_enabled': self.tier_a_enabled,
            'tier_b_enabled': self.tier_b_enabled,
            'tier_c_enabled': self.tier_c_enabled,
            'tiers': self.tiers,
            'run_frequency': self.run_frequency,
            'is_active': self.is_active,
        }

    @classmethod
    def from_db_row(cls, row: dict) -> 'ScreenDefinition':
        """Create a ScreenDefinition from a database row."""
        return cls(
            screen_type=row['screen_type'],
            name=row['name'],
            short_name=row['short_name'],
            description=row.get('description', ''),
            tier_a_enabled=row.get('tier_a_enabled', True),
            tier_b_enabled=row.get('tier_b_enabled', False),
            tier_c_enabled=row.get('tier_c_enabled', False),
            run_frequency=row.get('run_frequency', 'weekly'),
            is_active=row.get('is_active', True),
        )


# Pre-defined screen definitions (matches bsd_screen_definitions table)
SCREEN_DEFINITIONS = {
    1: ScreenDefinition(1, 'Net-Nets', 'Net-Nets', 'Below liquidation value (NCAV > market cap)', True, True, False, 'weekly'),
    2: ScreenDefinition(2, 'Defensive Bargains', 'Defensive', "Graham's multi-factor safety screen", True, False, False, 'monthly'),
    3: ScreenDefinition(3, 'Asset Plays', 'Asset Plays', 'Real assets below book value', True, True, False, 'monthly'),
    4: ScreenDefinition(4, 'Revenue Turnarounds', 'Turnarounds', 'Intact unit economics at death prices', True, True, False, 'weekly'),
    5: ScreenDefinition(5, 'Distressed Stable Earners', 'Distressed', 'Temporary margin compression', True, True, False, 'monthly'),
    6: ScreenDefinition(6, 'Growth at Reasonable Prices', 'GARP', 'Demonstrated growth, not hypothetical', True, True, False, 'monthly'),
    7: ScreenDefinition(7, 'Compressed Fundamentals', 'Compressed', 'Coiled spring - temporary earnings suppression', False, True, True, 'quarterly'),
    8: ScreenDefinition(8, 'Special Situations', 'Special Sit', 'Event-driven with defined timelines', False, True, False, 'daily'),
    9: ScreenDefinition(9, 'Holding Company Discounts', 'Holdings', 'Portfolios below sum of parts', True, True, False, 'daily'),
    10: ScreenDefinition(10, 'Sum-of-Parts', 'SoTP', 'Hidden value in the footnotes', False, True, True, 'annually'),
    11: ScreenDefinition(11, 'Cannibal Companies', 'Cannibals', 'Buyback compounders', True, False, False, 'quarterly'),
    12: ScreenDefinition(12, 'Wonderful Business at Fair Price', 'Wonderful', "Munger's compounders", True, False, True, 'quarterly'),
    13: ScreenDefinition(13, 'Crisis Bargains', 'Crisis', 'Legal or regulatory overhang', True, True, False, 'daily'),
    14: ScreenDefinition(14, 'Cyclicals', 'Cyclicals', 'Inverted screen for cyclical companies', True, True, False, 'monthly'),
    15: ScreenDefinition(15, 'Stalwarts', 'Stalwarts', 'Blue chip dip buys', True, True, False, 'weekly'),
    16: ScreenDefinition(16, 'Industrial Asset Recovery', 'Asset Recovery', 'Asset-heavy industrials at liquidation valuations', True, True, False, 'quarterly'),
}
