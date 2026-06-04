"""
Data loader with configurable column mapping

Reads CSV and maps columns to CompanyInput objects.
"""
import pandas as pd
from typing import List, Dict
from .models import CompanyInput


class DataLoader:
    """
    Load company data from CSV with configurable column mapping.
    """

    def __init__(self, column_mapping: Dict[str, str]):
        """
        Initialize with column mapping from config.

        Args:
            column_mapping: Dict mapping field name -> CSV column name
                Example: {"ticker": "Symbol", "price": "Close"}
        """
        self.column_mapping = column_mapping

    def load_from_csv(self, filepath: str) -> List[CompanyInput]:
        """
        Load companies from CSV file.

        Args:
            filepath: Path to CSV file

        Returns:
            List of CompanyInput objects
        """
        df = pd.read_csv(filepath)

        companies = []
        for _, row in df.iterrows():
            company = self._row_to_company(row)
            if company:
                companies.append(company)

        return companies

    def load_from_dataframe(self, df: pd.DataFrame) -> List[CompanyInput]:
        """
        Load companies from pandas DataFrame.

        Args:
            df: DataFrame with company data

        Returns:
            List of CompanyInput objects
        """
        companies = []
        for _, row in df.iterrows():
            company = self._row_to_company(row)
            if company:
                companies.append(company)

        return companies

    def _row_to_company(self, row) -> CompanyInput:
        """
        Convert a DataFrame row to CompanyInput using column mapping.

        Args:
            row: pandas Series (DataFrame row)

        Returns:
            CompanyInput object or None if critical fields missing
        """
        try:
            # Required fields
            ticker = self._get_field(row, 'ticker')
            name = self._get_field(row, 'name')
            price = self._get_field(row, 'price', float)

            if ticker is None or name is None or price is None:
                return None

            # Optional fields
            eps_norm = self._get_field(row, 'eps_norm', float)
            growth_hist = self._get_field(row, 'growth_hist', float)
            roic = self._get_field(row, 'roic', float)
            nav_ps = self._get_field(row, 'nav_ps', float)
            div_ps = self._get_field(row, 'div_ps', float)
            sector = self._get_field(row, 'sector', str)

            return CompanyInput(
                ticker=ticker,
                name=name,
                price=price,
                eps_norm=eps_norm,
                growth_hist=growth_hist,
                roic=roic,
                nav_ps=nav_ps,
                div_ps=div_ps,
                sector=sector
            )

        except Exception as e:
            print(f"Warning: Failed to parse row: {e}")
            return None

    def _get_field(self, row, field_name: str, field_type=None):
        """
        Get field value from row using column mapping.

        Args:
            row: pandas Series
            field_name: Field name in our model (e.g., 'ticker')
            field_type: Type to convert to (e.g., float)

        Returns:
            Field value or None if missing/invalid
        """
        # Get CSV column name from mapping
        csv_column = self.column_mapping.get(field_name)

        if csv_column is None:
            return None

        # Check if column exists in row
        if csv_column not in row.index:
            return None

        value = row[csv_column]

        # Handle NaN/missing
        if pd.isna(value):
            return None

        # Type conversion
        if field_type:
            try:
                return field_type(value)
            except (ValueError, TypeError):
                return None

        return value
