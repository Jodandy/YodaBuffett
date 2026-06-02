"""
Document Context Fetcher

Retrieves relevant document sections for LLM analysis.
Uses extracted documents and section embeddings to find the most
relevant content for each screen type's analysis needs.
"""

from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Any, Optional
from uuid import UUID

import asyncpg


@dataclass
class DocumentSection:
    """A section of a document with metadata."""
    section_id: UUID
    document_id: UUID
    section_type: str
    section_title: str
    content: str
    section_index: Optional[int] = None
    similarity_score: Optional[float] = None


@dataclass
class DocumentContext:
    """Context package for LLM analysis."""
    company_name: str
    document_type: str
    document_date: date
    sections: List[DocumentSection]
    total_chars: int

    def to_text(self, max_chars: int = 50000) -> str:
        """
        Convert to text for LLM prompt.

        Args:
            max_chars: Maximum characters to include

        Returns:
            Formatted text with sections
        """
        lines = [
            f"Company: {self.company_name}",
            f"Document: {self.document_type}",
            f"Date: {self.document_date}",
            "",
            "=" * 60,
            ""
        ]

        current_chars = sum(len(line) for line in lines)

        for section in self.sections:
            section_header = f"\n## {section.section_title}\n\n"
            section_content = section.content

            # Check if adding this section would exceed limit
            section_chars = len(section_header) + len(section_content)
            if current_chars + section_chars > max_chars:
                # Truncate section content
                remaining = max_chars - current_chars - len(section_header) - 100
                if remaining > 500:
                    lines.append(section_header)
                    lines.append(section_content[:remaining] + "\n\n[... truncated ...]")
                break

            lines.append(section_header)
            lines.append(section_content)
            current_chars += section_chars

        return "\n".join(lines)


class DocumentContextFetcher:
    """
    Fetches relevant document context for LLM analysis.

    Uses the document_intelligence infrastructure:
    - extracted_documents: Full document text with company_name, form_type, filing_date
    - document_sections: Parsed sections with section_title, section_content

    For each screen type, identifies the most relevant sections
    to include in the LLM prompt.
    """

    # Section types relevant for each screen
    SCREEN_SECTION_PREFERENCES = {
        # Screen 3: Asset Plays - focus on balance sheet notes
        3: ["balance_sheet", "assets", "receivables", "inventory", "property", "investments", "notes"],

        # Screen 4: Revenue Turnarounds - revenue and operations
        4: ["revenue", "operations", "segment", "management_discussion", "outlook", "strategy"],

        # Screen 7: Compressed Fundamentals - margin and earnings analysis
        7: ["income", "margin", "operations", "management_discussion", "outlook", "cost", "restructuring"],

        # Screen 8: Special Situations - events and corporate actions
        8: ["merger", "acquisition", "spin", "restructuring", "litigation", "regulatory", "management"],

        # Screen 9: Holding Companies - investments and subsidiaries
        9: ["investments", "subsidiaries", "associates", "holdings", "portfolio", "segment"],

        # Screen 10: Sum-of-Parts - segment and asset details
        10: ["segment", "business", "revenue", "operations", "assets", "investments", "subsidiaries"],

        # Screen 12: Wonderful Business - competitive position and moat
        12: ["business_description", "competition", "strategy", "market_position", "brand", "customers"],

        # Screen 13: Crisis Bargains - risk factors and recent events
        13: ["risk_factors", "legal", "litigation", "contingencies", "management_discussion", "outlook"],

        # Screen 14: Cyclicals - industry and cycle discussion
        14: ["industry", "market", "cycle", "commodity", "management_discussion", "outlook", "segment"],

        # Screen 15: Stalwarts - quality and stability indicators
        15: ["management_discussion", "strategy", "dividends", "capital_allocation", "outlook", "risk_factors"],
    }

    # Query templates for semantic search
    SCREEN_QUERIES = {
        3: "asset quality receivables inventory valuation book value liquidation real estate property",
        4: "revenue decline turnaround recovery margin operations cost structure competitive position",
        7: "margin compression temporary earnings one-time charges restructuring cost investment phase",
        8: "spin-off merger acquisition restructuring activist litigation regulatory special situation",
        9: "investment portfolio holdings subsidiaries associates net asset value discount to nav",
        10: "segment business unit division revenue profit valuation sum of parts conglomerate",
        12: "competitive advantage moat pricing power switching costs network effects brand strength",
        13: "crisis legal regulatory risk litigation management scandal earnings guidance",
        14: "cycle cyclical commodity prices industry conditions trough recovery economic sensitivity",
        15: "quality dividend stability management capital allocation long-term growth",
    }

    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_context_for_company(
        self,
        company_id: UUID,
        screen_type: int,
        score_date: date = None,
        max_sections: int = 10,
        max_chars: int = 50000
    ) -> Optional[DocumentContext]:
        """
        Get relevant document context for a company and screen type.

        Args:
            company_id: Company UUID
            screen_type: Screen number (1-15)
            score_date: Date for point-in-time (uses latest before this date)
            max_sections: Maximum number of sections to include
            max_chars: Maximum total characters

        Returns:
            DocumentContext or None if no documents found
        """
        score_date = score_date or date.today()

        # Get company info
        company = await self._get_company_info(company_id)
        if not company:
            return None

        # Try to find relevant document
        document = await self._get_latest_annual_report(company['company_name'], score_date)
        if not document:
            # Fall back to any document
            document = await self._get_latest_document(company['company_name'], score_date)

        if not document:
            return None

        # Get relevant sections
        sections = await self._get_relevant_sections(
            document_id=document['id'],
            screen_type=screen_type,
            max_sections=max_sections
        )

        if not sections:
            # Fall back to full document text (truncated)
            return await self._get_full_document_context(
                company_name=company['company_name'],
                document=document,
                max_chars=max_chars
            )

        return DocumentContext(
            company_name=company['company_name'],
            document_type=document.get('form_type', 'Annual Report'),
            document_date=document.get('filing_date', score_date),
            sections=sections,
            total_chars=sum(len(s.content) for s in sections)
        )

    async def _get_company_info(self, company_id: UUID) -> Optional[Dict[str, Any]]:
        """Get basic company info."""
        row = await self.conn.fetchrow("""
            SELECT id, company_name, primary_ticker
            FROM company_master
            WHERE id = $1
        """, company_id)
        return dict(row) if row else None

    async def _get_latest_annual_report(
        self,
        company_name: str,
        score_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Get the latest annual report for a company.

        Uses extracted_documents table which has text-extracted PDFs.
        Matches by company_name (case-insensitive).
        """
        row = await self.conn.fetchrow("""
            SELECT
                id,
                filing_date,
                form_type,
                extracted_text,
                total_pages,
                company_name
            FROM extracted_documents
            WHERE LOWER(company_name) = LOWER($1)
              AND (form_type ILIKE '%annual%' OR form_type ILIKE '%year%' OR form_type ILIKE '%årsredovisning%')
              AND filing_date <= $2
              AND extracted_text IS NOT NULL
              AND LENGTH(extracted_text) > 1000
            ORDER BY filing_date DESC
            LIMIT 1
        """, company_name, score_date)

        return dict(row) if row else None

    async def _get_latest_document(
        self,
        company_name: str,
        score_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Get any latest document for a company.

        Falls back to quarterly reports, interim reports, etc.
        """
        row = await self.conn.fetchrow("""
            SELECT
                id,
                filing_date,
                form_type,
                extracted_text,
                total_pages,
                company_name
            FROM extracted_documents
            WHERE LOWER(company_name) = LOWER($1)
              AND filing_date <= $2
              AND extracted_text IS NOT NULL
              AND LENGTH(extracted_text) > 500
            ORDER BY filing_date DESC
            LIMIT 1
        """, company_name, score_date)

        return dict(row) if row else None

    async def _get_relevant_sections(
        self,
        document_id: UUID,
        screen_type: int,
        max_sections: int = 10
    ) -> List[DocumentSection]:
        """
        Get relevant sections for a document and screen type.

        Uses section type preferences and title matching.
        """
        preferred_types = self.SCREEN_SECTION_PREFERENCES.get(screen_type, [])

        sections = []

        # First, try to get sections by type/title matching
        if preferred_types:
            type_conditions = " OR ".join([
                f"section_type ILIKE '%{t}%' OR section_title ILIKE '%{t}%'"
                for t in preferred_types
            ])
            rows = await self.conn.fetch(f"""
                SELECT
                    id,
                    extracted_document_id,
                    section_type,
                    section_title,
                    section_content,
                    section_index
                FROM document_sections
                WHERE extracted_document_id = $1
                  AND ({type_conditions})
                  AND LENGTH(section_content) > 100
                ORDER BY section_index ASC NULLS LAST
                LIMIT $2
            """, document_id, max_sections)

            for row in rows:
                sections.append(DocumentSection(
                    section_id=row['id'],
                    document_id=row['extracted_document_id'],
                    section_type=row['section_type'] or 'unknown',
                    section_title=row['section_title'] or 'Untitled Section',
                    content=row['section_content'],
                    section_index=row['section_index']
                ))

        # If we don't have enough, add more sections
        if len(sections) < max_sections:
            existing_ids = [s.section_id for s in sections]
            remaining = max_sections - len(sections)

            # Build exclusion clause
            if existing_ids:
                rows = await self.conn.fetch("""
                    SELECT
                        id,
                        extracted_document_id,
                        section_type,
                        section_title,
                        section_content,
                        section_index
                    FROM document_sections
                    WHERE extracted_document_id = $1
                      AND id != ALL($2::uuid[])
                      AND LENGTH(section_content) > 200
                    ORDER BY section_index ASC NULLS LAST
                    LIMIT $3
                """, document_id, existing_ids, remaining)
            else:
                rows = await self.conn.fetch("""
                    SELECT
                        id,
                        extracted_document_id,
                        section_type,
                        section_title,
                        section_content,
                        section_index
                    FROM document_sections
                    WHERE extracted_document_id = $1
                      AND LENGTH(section_content) > 200
                    ORDER BY section_index ASC NULLS LAST
                    LIMIT $2
                """, document_id, remaining)

            for row in rows:
                sections.append(DocumentSection(
                    section_id=row['id'],
                    document_id=row['extracted_document_id'],
                    section_type=row['section_type'] or 'unknown',
                    section_title=row['section_title'] or 'Untitled Section',
                    content=row['section_content'],
                    section_index=row['section_index']
                ))

        return sections

    async def _get_full_document_context(
        self,
        company_name: str,
        document: Dict[str, Any],
        max_chars: int
    ) -> DocumentContext:
        """
        Fall back to using full document text when sections aren't available.
        """
        text = document.get('extracted_text', '')[:max_chars]

        section = DocumentSection(
            section_id=document['id'],
            document_id=document['id'],
            section_type='full_document',
            section_title='Full Document',
            content=text
        )

        return DocumentContext(
            company_name=company_name,
            document_type=document.get('form_type', 'Document'),
            document_date=document.get('filing_date', date.today()),
            sections=[section],
            total_chars=len(text)
        )

    async def search_sections_semantic(
        self,
        company_id: UUID,
        query: str,
        top_k: int = 5,
        score_date: date = None
    ) -> List[DocumentSection]:
        """
        Search for relevant sections using semantic similarity.

        Requires section_embeddings table with vector embeddings.

        Args:
            company_id: Company UUID
            query: Search query
            top_k: Number of results
            score_date: Point-in-time date

        Returns:
            List of matching sections with similarity scores
        """
        # Get company name first
        company = await self._get_company_info(company_id)
        if not company:
            return []

        score_date = score_date or date.today()

        # Simple keyword search fallback
        keywords = query.lower().split()
        keyword_conditions = " OR ".join([
            f"LOWER(ds.section_content) LIKE '%{k}%'"
            for k in keywords[:5]
        ])

        rows = await self.conn.fetch(f"""
            SELECT
                ds.id,
                ds.extracted_document_id,
                ds.section_type,
                ds.section_title,
                ds.section_content,
                ds.section_index
            FROM document_sections ds
            JOIN extracted_documents ed ON ds.extracted_document_id = ed.id
            WHERE LOWER(ed.company_name) = LOWER($1)
              AND ed.filing_date <= $2
              AND ({keyword_conditions})
              AND LENGTH(ds.section_content) > 100
            ORDER BY ed.filing_date DESC, ds.section_index ASC
            LIMIT $3
        """, company['company_name'], score_date, top_k)

        sections = []
        for row in rows:
            sections.append(DocumentSection(
                section_id=row['id'],
                document_id=row['extracted_document_id'],
                section_type=row['section_type'] or 'unknown',
                section_title=row['section_title'] or 'Untitled Section',
                content=row['section_content'],
                section_index=row['section_index']
            ))

        return sections
