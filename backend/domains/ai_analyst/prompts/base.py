"""
Base prompt template class
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any


class BasePrompt(ABC):
    """
    Abstract base class for all prompt templates.

    Each prompt defines:
    - What data sources it needs
    - How to build the prompt from that data
    - Expected output format
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this prompt"""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this prompt does"""
        pass

    @property
    @abstractmethod
    def required_data_sources(self) -> List[str]:
        """
        List of data source names required for this prompt.
        Example: ['financials', 'prices', 'company_info']
        """
        pass

    @abstractmethod
    def build_prompt(self, data: Dict[str, Any]) -> str:
        """
        Build the actual LLM prompt from assembled data.

        Args:
            data: Dictionary with keys matching required_data_sources
                  Each value is the formatted output from that data source

        Returns:
            Complete prompt string ready to send to LLM
        """
        pass

    @property
    def system_message(self) -> str:
        """
        System message for the LLM (role/instructions).
        Override if you want custom system instructions.
        """
        return (
            "You are an expert investment analyst with deep knowledge of "
            "financial statements, valuation, and business analysis. "
            "Provide clear, objective, and actionable insights based on the data."
        )

    @property
    def output_format_instructions(self) -> str:
        """
        Instructions for how the LLM should format its response.
        Override for custom output formats.
        """
        return (
            "Structure your analysis with clear sections and specific data points. "
            "Be concise but thorough. Use bullet points where appropriate."
        )
