"""
Layer 2 - LLM Judgment for NAV-Quality Evaluator

Qualitative assessment of NAV reality, catalysts, and value trap risk.
Runs ONLY on companies that passed Layer 1 deterministic filters.

Design principles:
- Citations required for every claim
- No numeric output from LLM (uses Layer 1 math only)
- Three questions: Is NAV real? Is there a catalyst? Why is it cheap?
"""
import anthropic
import json
from typing import Dict, Any, Optional, List
from datetime import date
from .models import (
    CompanyAssetInput,
    Layer1Result,
    Layer2Judgment,
    NAVRealityStatus,
    CatalystStatus,
    JudgmentFlag
)


class Layer2Evaluator:
    """
    LLM-based qualitative judgment for NAV candidates.

    Assesses:
    1. NAV reality (REAL | PARTLY | SUSPECT)
    2. Catalyst status (PRESENT | ABSENT)
    3. Why it's cheap (short reason with citation)

    Final flag: CANDIDATE | VALUE_TRAP_RISK | NAV_SUSPECT
    """

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-5-20250929"):
        """
        Initialize Layer 2 evaluator with Claude API.

        Args:
            api_key: Anthropic API key
            model: Claude model to use
        """
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def evaluate(
        self,
        company: CompanyAssetInput,
        layer1: Layer1Result,
        financial_context: str
    ) -> Layer2Judgment:
        """
        Run Layer 2 judgment on a Layer 1 candidate.

        Args:
            company: Company asset input data
            layer1: Layer 1 deterministic results
            financial_context: Extracted financial report sections (from database)

        Returns:
            Layer2Judgment with citations
        """
        # Build prompt with Layer 1 context + financial excerpts
        prompt = self._build_prompt(company, layer1, financial_context)

        # Call Claude API
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1000,
            temperature=0.0,  # Deterministic
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )

        # Parse response (expect JSON structure)
        response_text = response.content[0].text

        try:
            judgment_data = json.loads(response_text)
        except json.JSONDecodeError:
            # Fallback: if not JSON, treat as suspect
            return Layer2Judgment(
                ticker=company.ticker,
                nav_reality=NAVRealityStatus.SUSPECT,
                nav_reality_reason="Could not parse LLM response",
                catalyst_status=CatalystStatus.ABSENT,
                catalyst_reason="Could not parse LLM response",
                why_cheap="Could not parse LLM response",
                judgment_flag=JudgmentFlag.DATA_SUSPECT,
                sources=[]
            )

        # Validate no numeric outputs (contract test)
        self._validate_no_numbers(judgment_data)

        # Parse into Layer2Judgment
        return self._parse_judgment(company.ticker, judgment_data)

    def _build_prompt(
        self,
        company: CompanyAssetInput,
        layer1: Layer1Result,
        financial_context: str
    ) -> str:
        """
        Build structured prompt for Claude.

        Includes:
        - Layer 1 deterministic results (for context only)
        - Financial report excerpts
        - Three questions to answer
        - Citation requirements
        """
        prompt = f"""You are analyzing {company.name} ({company.ticker}) for net asset value (NAV) quality.

**Layer 1 Deterministic Analysis (Already Calculated - DO NOT RECALCULATE):**
- Price: {layer1.price:.2f} SEK
- Book NAV/share: {layer1.book_nav_ps:.2f} SEK
- Hard NAV/share: {layer1.hard_nav_ps:.2f} SEK (liquid assets - liabilities)
- Discount to hard NAV: {layer1.disc_to_hard_nav:.1%}
- Soft assets fraction: {layer1.soft_fraction:.1%}

**Financial Report Context:**
{financial_context}

**Your Task:**
Answer these THREE questions about NAV quality. You MUST cite specific evidence from the financial report for EVERY claim.

1. **Is the NAV real and recoverable?**
   - REAL: Liquid assets marked-to-market (cash, traded securities)
   - PARTLY: Mix of liquid and illiquid (property at appraisal, private investments)
   - SUSPECT: Stale appraisals, mark-to-model, or unclear valuation

2. **Is there a catalyst for discount closure?**
   - PRESENT: Active buyback, dividend policy, activist involvement, sale process, liquidation
   - ABSENT: Controlled float, entrenched management, no shareholder returns

3. **Why is it cheap?**
   - Short reason (1-2 sentences) explaining market's concern
   - Must cite evidence from reports

**CRITICAL RULES:**
- NEVER output numeric calculations (price, NAV, ratios) - these are already in Layer 1
- EVERY claim must cite a source (e.g., "per Q4 2024 balance sheet notes")
- Answer qualitatively only (e.g., "cash holdings appear liquid" not "cash = $10M")
- If insufficient evidence, say "INSUFFICIENT EVIDENCE" with reason

**Output Format (JSON):**
{{
    "nav_reality": "REAL" | "PARTLY" | "SUSPECT",
    "nav_reality_reason": "One-line reason + citation",
    "catalyst_status": "PRESENT" | "ABSENT",
    "catalyst_reason": "What it is (or why absent) + citation",
    "why_cheap": "Market concern + citation",
    "sources": ["Source 1", "Source 2", ...]
}}

Respond with ONLY the JSON object, no other text.
"""
        return prompt

    def _validate_no_numbers(self, judgment_data: Dict[str, Any]):
        """
        Contract test: Assert LLM did not output numeric calculations.

        Raises ValueError if numeric outputs found.
        """
        # Check for common numeric patterns in text fields
        text_fields = [
            judgment_data.get('nav_reality_reason', ''),
            judgment_data.get('catalyst_reason', ''),
            judgment_data.get('why_cheap', '')
        ]

        for text in text_fields:
            # Simple check: if contains "SEK", "NAV/share", or calculation symbols
            if any(pattern in text for pattern in ['SEK', '$/share', 'NAV/share', '=', 'calculate']):
                raise ValueError(
                    f"LLM output contains numeric calculation (forbidden): {text}"
                )

    def _parse_judgment(self, ticker: str, data: Dict[str, Any]) -> Layer2Judgment:
        """
        Parse LLM JSON response into Layer2Judgment dataclass.
        """
        # Determine final flag based on NAV reality + catalyst
        nav_reality = NAVRealityStatus(data['nav_reality'])
        catalyst_status = CatalystStatus(data['catalyst_status'])

        if nav_reality == NAVRealityStatus.SUSPECT:
            final_flag = JudgmentFlag.NAV_SUSPECT
        elif catalyst_status == CatalystStatus.ABSENT:
            final_flag = JudgmentFlag.VALUE_TRAP_RISK
        else:
            final_flag = JudgmentFlag.CANDIDATE

        return Layer2Judgment(
            ticker=ticker,
            nav_reality=nav_reality,
            nav_reality_reason=data['nav_reality_reason'],
            catalyst_status=catalyst_status,
            catalyst_reason=data['catalyst_reason'],
            why_cheap=data['why_cheap'],
            judgment_flag=final_flag,
            sources=data.get('sources', [])
        )


def extract_financial_context(
    ticker: str,
    extracted_text: Optional[str]
) -> str:
    """
    Extract relevant financial report sections for LLM context.

    Args:
        ticker: Company ticker
        extracted_text: Full extracted document text from database

    Returns:
        Condensed financial context (balance sheet notes, management discussion, etc.)
    """
    if not extracted_text:
        return "No financial report text available."

    # Simple extraction: look for balance sheet / assets / liabilities sections
    # In production, this would use the document_sections table with smart chunking

    lines = extracted_text.split('\n')
    relevant_sections = []

    # Keywords to identify relevant sections
    keywords = [
        'balance sheet',
        'assets',
        'liabilities',
        'equity',
        'cash and cash equivalents',
        'receivables',
        'goodwill',
        'intangible',
        'property',
        'debt',
        'shareholders equity',
        'net asset value',
        'liquidation',
        'buyback',
        'dividend',
        'share repurchase'
    ]

    # Collect lines with relevant keywords (with context)
    for i, line in enumerate(lines):
        line_lower = line.lower()
        if any(kw in line_lower for kw in keywords):
            # Add context: 2 lines before and after
            start = max(0, i - 2)
            end = min(len(lines), i + 3)
            relevant_sections.extend(lines[start:end])

    if not relevant_sections:
        return "No balance sheet or asset-related sections found in report."

    # Deduplicate and limit length
    unique_lines = list(dict.fromkeys(relevant_sections))  # Preserve order
    context = '\n'.join(unique_lines[:100])  # Limit to 100 lines

    # Add truncation notice if needed
    if len(unique_lines) > 100:
        context += f"\n\n[... {len(unique_lines) - 100} more lines omitted ...]"

    return context
