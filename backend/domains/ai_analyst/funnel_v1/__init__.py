"""
Focus-Narrowing Engine (Funnel v1)

A single-pass triage funnel over the equity universe.
Inverts stock prices to reveal implied growth expectations and finds mispricings.
"""

from .models import CompanyInput, TriageResult, Side
from .engine import FocusNarrowingEngine
from .data_fetcher import FunnelDataFetcher
from .output import OutputWriter

__all__ = [
    'CompanyInput',
    'TriageResult',
    'Side',
    'FocusNarrowingEngine',
    'FunnelDataFetcher',
    'OutputWriter',
]
