"""
Investment Memo prompt template

Generates a comprehensive investment analysis memo combining
financials, valuation metrics, and price performance.
"""
from typing import List, Dict, Any
from .base import BasePrompt


class InvestmentMemoPrompt(BasePrompt):
    """
    Full investment memo in the style of value investors.

    Analyzes:
    - Business fundamentals (revenue, profitability, cash flow)
    - Financial health (debt, liquidity)
    - Valuation (P/E, P/B, relative to history/peers)
    - Price momentum and trends
    - Risk factors
    - Investment thesis (bull/bear cases)
    """

    @property
    def name(self) -> str:
        return "investment_memo"

    @property
    def description(self) -> str:
        return "Comprehensive investment analysis memo with thesis and recommendation"

    @property
    def required_data_sources(self) -> List[str]:
        return ['financials', 'company_info', 'prices']

    @property
    def system_message(self) -> str:
        return (
            "You are a senior investment analyst writing an investment memo. "
            "Your analysis should be objective, data-driven, and balanced. "
            "Present both bull and bear cases. Focus on long-term value creation. "
            "Think like Warren Buffett, Charlie Munger, and other value investors - "
            "focus on sustainable competitive advantages, management quality, "
            "and intrinsic value relative to market price."
        )

    def build_prompt(self, data: Dict[str, Any]) -> str:
        """Build the investment memo prompt from data sources"""

        financials = data.get('financials', 'No financial data available')
        company_info = data.get('company_info', 'No company info available')
        prices = data.get('prices', 'No price data available')

        prompt = f"""
# Investment Analysis Request

You are analyzing a potential investment opportunity. Based on the data below,
write a comprehensive investment memo.

---

## Financial Data

{financials}

---

## Company Metrics & Valuation

{company_info}

---

## Price Performance

{prices}

---

# Required Analysis Sections

Please provide a structured investment memo with these sections:

## 1. Executive Summary
- One paragraph overview of the investment opportunity
- Key highlights (positive and negative)
- Preliminary recommendation (Strong Buy / Buy / Hold / Sell / Strong Sell)

## 2. Business Quality Assessment
- Revenue trends and growth analysis
- Profitability analysis (margins, ROE, ROA)
- Cash generation and capital allocation
- Balance sheet strength (debt levels, liquidity)
- Any red flags or concerns

## 3. Valuation Analysis
- Current valuation metrics (P/E, P/B, etc.)
- Is the stock cheap, fair, or expensive relative to:
  - Its own historical averages?
  - Industry peers (if you can infer)?
  - Absolute valuation benchmarks?
- Price momentum and technical picture

## 4. Investment Thesis

### Bull Case (3-5 points)
What could make this a great investment?

### Bear Case (3-5 points)
What could go wrong? What are the risks?

## 5. Key Questions / Data Gaps
What additional information would you want before making a final decision?

## 6. Conclusion
- Overall assessment
- Recommended action
- Price target range (if appropriate)

---

**Instructions:**
- Be specific with numbers from the data
- Calculate growth rates where relevant
- Be honest about limitations in the data
- Don't make up information not in the data
- If data is missing, note it explicitly
"""

        return prompt

    @property
    def output_format_instructions(self) -> str:
        return (
            "Format your response in clear markdown with headers for each section. "
            "Use bullet points for lists. Include specific numbers and percentages "
            "from the data. Be concise but thorough - aim for 800-1200 words total."
        )
