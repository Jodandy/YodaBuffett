"""
NAV-Quality Evaluator Service

Orchestrates the complete evaluation pipeline:
1. Layer 1 (deterministic math filters)
2. Layer 2 (LLM qualitative judgment) - only for Layer 1 candidates

Does NOT modify any existing components - just orchestrates them.
"""
import asyncpg
import os
from typing import Optional, List
from datetime import date
from .models import NAVEvaluationResult, JudgmentFlag
from .data_fetcher import NAVDataFetcher
from .document_fetcher import DocumentFetcher
from .layer1_math import Layer1MathEngine
from .layer2_judgment import Layer2Evaluator, extract_financial_context


class NAVEvaluatorService:
    """
    Complete NAV evaluation service combining Layer 1 and Layer 2.

    Usage:
        service = NAVEvaluatorService(db_conn, config, api_key)
        result = await service.evaluate_company("AAK")
    """

    def __init__(
        self,
        db_conn: asyncpg.Connection,
        config: dict,
        anthropic_api_key: Optional[str] = None
    ):
        """
        Initialize NAV evaluator service.

        Args:
            db_conn: Database connection
            config: Configuration dict (from config.yaml)
            anthropic_api_key: Anthropic API key for Layer 2 (optional)
                               If not provided, only Layer 1 will run.
        """
        self.db_conn = db_conn
        self.config = config

        # Initialize Layer 1 components (always available)
        self.nav_fetcher = NAVDataFetcher(db_conn)
        self.layer1_engine = Layer1MathEngine(config)

        # Initialize Layer 2 components (optional - requires API key)
        self.has_layer2 = anthropic_api_key is not None
        if self.has_layer2:
            self.document_fetcher = DocumentFetcher(db_conn)
            self.layer2_evaluator = Layer2Evaluator(anthropic_api_key)

    async def evaluate_company(
        self,
        ticker: str,
        as_of_date: Optional[date] = None,
        skip_layer2: bool = False
    ) -> Optional[NAVEvaluationResult]:
        """
        Run complete NAV evaluation on a single company.

        Args:
            ticker: Company ticker
            as_of_date: Analysis date (defaults to today)
            skip_layer2: Skip Layer 2 even if candidate (for testing)

        Returns:
            NAVEvaluationResult with Layer 1 + optional Layer 2, or None if data unavailable
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Step 1: Get company info
        company_row = await self.db_conn.fetchrow("""
            SELECT id, company_name, primary_ticker, yahoo_symbol
            FROM company_master
            WHERE primary_ticker = $1
        """, ticker)

        if not company_row:
            return None

        # Step 2: Fetch balance sheet data
        company_input = await self.nav_fetcher._fetch_company(
            company_id=company_row['id'],
            ticker=company_row['primary_ticker'],
            name=company_row['company_name'],
            yahoo_symbol=company_row['yahoo_symbol'],
            as_of_date=as_of_date
        )

        if not company_input:
            return None

        # Step 3: Run Layer 1 (deterministic math)
        layer1_result = self.layer1_engine.evaluate(company_input)

        # Step 4: If Layer 1 passed and Layer 2 available, run it
        layer2_result = None
        final_judgment = None

        if layer1_result.is_candidate and self.has_layer2 and not skip_layer2:
            # Fetch financial report documents
            # Try section-based first (more precise), fall back to full text
            financial_context = await self.document_fetcher.fetch_balance_sheet_sections(
                ticker, as_of_date
            )

            if not financial_context:
                # Fall back to full extracted text
                full_text = await self.document_fetcher.fetch_latest_report(
                    ticker, as_of_date
                )
                financial_context = extract_financial_context(ticker, full_text)

            # Run Layer 2 LLM judgment
            try:
                layer2_result = self.layer2_evaluator.evaluate(
                    company_input,
                    layer1_result,
                    financial_context
                )
                final_judgment = layer2_result.judgment_flag
            except Exception as e:
                # If Layer 2 fails, still return Layer 1 results
                # Set final judgment to DATA_SUSPECT
                print(f"Warning: Layer 2 failed for {ticker}: {e}")
                final_judgment = JudgmentFlag.DATA_SUSPECT

        # Return combined result
        return NAVEvaluationResult(
            ticker=ticker,
            name=company_row['company_name'],
            layer1=layer1_result,
            layer2=layer2_result,
            passed_layer1=layer1_result.is_candidate,
            final_judgment=final_judgment
        )

    async def evaluate_all_companies(
        self,
        limit: Optional[int] = None,
        as_of_date: Optional[date] = None,
        skip_layer2: bool = False
    ) -> List[NAVEvaluationResult]:
        """
        Run NAV evaluation on all companies.

        Args:
            limit: Optional limit on number of companies
            as_of_date: Analysis date (defaults to today)
            skip_layer2: Skip Layer 2 for all companies (faster)

        Returns:
            List of NAVEvaluationResult objects
        """
        if as_of_date is None:
            as_of_date = date.today()

        # Fetch all companies with balance sheet data
        company_inputs = await self.nav_fetcher.fetch_all_companies(
            as_of_date=as_of_date,
            limit=limit
        )

        results = []

        for company_input in company_inputs:
            # Run Layer 1
            layer1_result = self.layer1_engine.evaluate(company_input)

            # Run Layer 2 if candidate and available
            layer2_result = None
            final_judgment = None

            if layer1_result.is_candidate and self.has_layer2 and not skip_layer2:
                # Fetch documents
                financial_context = await self.document_fetcher.fetch_balance_sheet_sections(
                    company_input.ticker, as_of_date
                )

                if not financial_context:
                    full_text = await self.document_fetcher.fetch_latest_report(
                        company_input.ticker, as_of_date
                    )
                    financial_context = extract_financial_context(company_input.ticker, full_text)

                # Run Layer 2
                try:
                    layer2_result = self.layer2_evaluator.evaluate(
                        company_input,
                        layer1_result,
                        financial_context
                    )
                    final_judgment = layer2_result.judgment_flag
                except Exception as e:
                    print(f"Warning: Layer 2 failed for {company_input.ticker}: {e}")
                    final_judgment = JudgmentFlag.DATA_SUSPECT

            # Add result
            results.append(NAVEvaluationResult(
                ticker=company_input.ticker,
                name=company_input.name,
                layer1=layer1_result,
                layer2=layer2_result,
                passed_layer1=layer1_result.is_candidate,
                final_judgment=final_judgment
            ))

        return results

    async def get_final_candidates(
        self,
        limit: Optional[int] = None,
        as_of_date: Optional[date] = None
    ) -> List[NAVEvaluationResult]:
        """
        Get final candidates after both Layer 1 and Layer 2 filtering.

        Only returns companies that:
        1. Passed Layer 1 (deterministic math)
        2. Passed Layer 2 with judgment_flag = CANDIDATE

        Args:
            limit: Optional limit on companies to evaluate
            as_of_date: Analysis date

        Returns:
            List of NAVEvaluationResult objects with final_judgment = CANDIDATE
        """
        if not self.has_layer2:
            raise ValueError("Layer 2 not available - need Anthropic API key")

        # Evaluate all companies
        all_results = await self.evaluate_all_companies(
            limit=limit,
            as_of_date=as_of_date,
            skip_layer2=False
        )

        # Filter to final candidates only
        final_candidates = [
            r for r in all_results
            if r.passed_layer1 and r.final_judgment == JudgmentFlag.CANDIDATE
        ]

        # Sort by Layer 1 discount (most underpriced first)
        final_candidates.sort(key=lambda r: r.layer1.disc_to_hard_nav, reverse=True)

        return final_candidates
