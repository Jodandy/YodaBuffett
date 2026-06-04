"""
Document fetcher for NAV-Quality Evaluator Layer 2

Retrieves financial reports from database for LLM analysis.
"""
import asyncpg
from typing import Optional
from datetime import date


class DocumentFetcher:
    """
    Fetch financial report documents from database for Layer 2 analysis.
    """

    def __init__(self, db_conn: asyncpg.Connection):
        self.db_conn = db_conn

    async def fetch_latest_report(
        self,
        ticker: str,
        as_of_date: Optional[date] = None
    ) -> Optional[str]:
        """
        Fetch most recent financial report text for a company.

        Args:
            ticker: Company ticker
            as_of_date: Analysis date (defaults to today)

        Returns:
            Extracted document text, or None if not found
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Try to find company in company_master
        company_row = await self.db_conn.fetchrow("""
            SELECT id, company_name
            FROM company_master
            WHERE primary_ticker = $1
        """, ticker)

        if not company_row:
            return None

        company_id = company_row['id']

        # Get most recent extracted document (annual or quarterly report)
        # Priority: annual reports first, then quarterly
        doc_row = await self.db_conn.fetchrow("""
            SELECT ed.extracted_text, nd.document_type, nd.document_date
            FROM extracted_documents ed
            JOIN nordic_documents nd ON ed.document_id = nd.id
            WHERE nd.company_id = $1
              AND nd.document_date <= $2
              AND nd.document_type IN ('annual_report', 'quarterly_report', 'interim_report')
              AND ed.extracted_text IS NOT NULL
            ORDER BY
                CASE
                    WHEN nd.document_type = 'annual_report' THEN 1
                    WHEN nd.document_type = 'quarterly_report' THEN 2
                    ELSE 3
                END,
                nd.document_date DESC
            LIMIT 1
        """, company_id, as_of_date)

        if not doc_row:
            return None

        return doc_row['extracted_text']

    async def fetch_balance_sheet_sections(
        self,
        ticker: str,
        as_of_date: Optional[date] = None
    ) -> Optional[str]:
        """
        Fetch balance sheet specific sections from document_sections table.

        This is more precise than fetching full report text.

        Args:
            ticker: Company ticker
            as_of_date: Analysis date

        Returns:
            Combined balance sheet sections, or None
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Get company ID
        company_row = await self.db_conn.fetchrow("""
            SELECT id FROM company_master WHERE primary_ticker = $1
        """, ticker)

        if not company_row:
            return None

        company_id = company_row['id']

        # Get most recent document with sections
        doc_row = await self.db_conn.fetchrow("""
            SELECT nd.id
            FROM nordic_documents nd
            WHERE nd.company_id = $1
              AND nd.document_date <= $2
              AND nd.document_type IN ('annual_report', 'quarterly_report')
            ORDER BY nd.document_date DESC
            LIMIT 1
        """, company_id, as_of_date)

        if not doc_row:
            return None

        document_id = doc_row['id']

        # Get balance sheet related sections
        sections = await self.db_conn.fetch("""
            SELECT section_text, section_title
            FROM document_sections
            WHERE document_id = $1
              AND (
                  section_title ILIKE '%balance%sheet%'
                  OR section_title ILIKE '%assets%'
                  OR section_title ILIKE '%liabilities%'
                  OR section_title ILIKE '%equity%'
                  OR section_title ILIKE '%financial%position%'
                  OR section_title ILIKE '%tillgångar%'  -- Swedish: assets
                  OR section_title ILIKE '%skulder%'     -- Swedish: liabilities
                  OR section_title ILIKE '%eget%kapital%' -- Swedish: equity
              )
            ORDER BY section_order
        """, document_id)

        if not sections:
            return None

        # Combine sections with titles
        combined_text = []
        for section in sections:
            combined_text.append(f"## {section['section_title']}")
            combined_text.append(section['section_text'])
            combined_text.append("")  # Blank line between sections

        return '\n'.join(combined_text)
