"""
Base class for data sources
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import date
import asyncpg


class BaseDataSource(ABC):
    """
    Abstract base class for all data sources.

    Each data source knows how to fetch specific data from the database
    and format it for LLM consumption.
    """

    def __init__(self, db_conn: asyncpg.Connection):
        self.db_conn = db_conn

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier for this data source"""
        pass

    @abstractmethod
    async def fetch(
        self,
        company_id: str,
        as_of_date: Optional[date] = None,
        years_back: int = 3
    ) -> Dict[str, Any]:
        """
        Fetch data for a company.

        Args:
            company_id: Company UUID
            as_of_date: Point-in-time date (None = latest)
            years_back: How many years of historical data to include

        Returns:
            Dictionary with formatted data ready for prompt inclusion
        """
        pass

    def format_for_prompt(self, data: Dict[str, Any]) -> str:
        """
        Format the data as a string for inclusion in LLM prompt.

        Override this if you want custom formatting.
        Default: just return a structured representation.
        """
        return str(data)
