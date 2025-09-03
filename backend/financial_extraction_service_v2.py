"""
Financial Extraction Service V2 - Improved and more robust
Extract structured financial data from PDFs using local LLM with better error handling
"""
import asyncio
import asyncpg
import json
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass, asdict
import logging

# PDF processing
try:
    import PyPDF2
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("⚠️  PDF libraries not available. Install with: pip install PyPDF2 pdfplumber")

# Local LLM
import sys
sys.path.append(str(Path(__file__).parent / "research-service"))

try:
    from services.local_llm_service import OllamaService
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False
    print("⚠️  Local LLM service not available")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class FinancialMetrics:
    """Structured financial metrics with dual extraction support"""
    # Document info
    company_name: str
    report_period: str
    report_type: Optional[str] = None
    fiscal_year: Optional[int] = None
    report_date: Optional[date] = None
    
    # Revenue - dual extraction
    revenue_reported: Optional[float] = None
    revenue_adjusted: Optional[float] = None
    revenue_adjustments: Optional[str] = None
    revenue_currency: str = "SEK"
    revenue_growth_pct: Optional[float] = None
    revenue_growth_qoq_pct: Optional[float] = None
    
    # Profitability
    gross_profit: Optional[float] = None
    gross_margin_pct: Optional[float] = None
    cost_of_goods_sold: Optional[float] = None
    operating_expenses: Optional[float] = None
    
    # Operating profit - dual extraction
    operating_profit_reported: Optional[float] = None
    operating_profit_adjusted: Optional[float] = None
    operating_adjustments: Optional[str] = None
    operating_margin_pct: Optional[float] = None
    
    # EBITDA - dual extraction
    ebitda_reported: Optional[float] = None
    ebitda_adjusted: Optional[float] = None
    ebitda_adjustments: Optional[str] = None
    ebitda_margin_pct: Optional[float] = None
    
    depreciation_amortization: Optional[float] = None
    interest_expense: Optional[float] = None
    tax_expense: Optional[float] = None
    other_income: Optional[float] = None
    
    # Net income - dual extraction
    net_income_reported: Optional[float] = None
    net_income_adjusted: Optional[float] = None
    net_income_adjustments: Optional[str] = None
    net_margin_pct: Optional[float] = None
    
    # Cash flow
    operating_cash_flow: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    free_cash_flow: Optional[float] = None
    capex: Optional[float] = None
    dividends_paid: Optional[float] = None
    
    # Balance sheet
    total_assets: Optional[float] = None
    current_assets: Optional[float] = None
    non_current_assets: Optional[float] = None
    total_equity: Optional[float] = None
    retained_earnings: Optional[float] = None
    total_liabilities: Optional[float] = None
    current_liabilities: Optional[float] = None
    non_current_liabilities: Optional[float] = None
    total_debt: Optional[float] = None
    cash_and_equivalents: Optional[float] = None
    inventory: Optional[float] = None
    accounts_receivable: Optional[float] = None
    accounts_payable: Optional[float] = None
    working_capital: Optional[float] = None
    
    # Ratios
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    inventory_turnover: Optional[float] = None
    asset_turnover: Optional[float] = None
    interest_coverage: Optional[float] = None
    return_on_equity_pct: Optional[float] = None
    return_on_assets_pct: Optional[float] = None
    
    # Per share - dual extraction for EPS
    earnings_per_share_reported: Optional[float] = None
    earnings_per_share_adjusted: Optional[float] = None
    eps_adjustments: Optional[str] = None
    book_value_per_share: Optional[float] = None
    dividend_per_share: Optional[float] = None
    shares_outstanding: Optional[int] = None
    
    # Nordic-specific
    payout_ratio: Optional[float] = None
    dividend_yield_pct: Optional[float] = None
    
    # Metadata
    operational_metrics: Optional[Dict] = None
    extraction_confidence: float = 0.0
    model_used: str = "local_llm"
    extraction_notes: Optional[str] = None
    data_warnings: Optional[List[str]] = None


class ImprovedFinancialExtractor:
    """Improved financial extraction with better error handling and parsing"""
    
    def __init__(self):
        if not OLLAMA_AVAILABLE:
            raise Exception("Ollama service not available")
        self.ollama = OllamaService(default_model="llama3:latest")
    
    async def extract_metrics(
        self, 
        document_text: str, 
        company_name: str,
        document_path: str = ""
    ) -> FinancialMetrics:
        """Extract financial metrics with improved robustness"""
        
        # First, extract what we can from the document structure
        quick_extracts = self._quick_extract_numbers(document_text)
        
        # Build a focused prompt based on what we found
        prompt = self._build_smart_prompt(document_text, company_name, quick_extracts)
        
        # Get LLM response with retry logic
        extracted_data = await self._get_llm_extraction_with_retry(prompt)
        
        # Create metrics object
        metrics = self._create_metrics_from_extraction(
            extracted_data, 
            company_name, 
            document_text,
            quick_extracts
        )
        
        # Extract metadata from filename
        if document_path:
            metrics = self._extract_filename_metadata(metrics, Path(document_path).name)
        
        # Apply validation and corrections
        metrics = self._validate_and_enhance_metrics(metrics, document_text)
        
        return metrics
    
    def _quick_extract_numbers(self, text: str) -> Dict[str, Any]:
        """Quick extraction of obvious numbers from the document"""
        extracts = {}
        
        # Common patterns in financial documents
        patterns = {
            'revenue': [
                r'(?:Revenue|Net sales|Sales).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:million|MSEK)',
                r'(?:Omsättning|Nettoomsättning).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:miljoner|MSEK)',
            ],
            'operating_profit': [
                r'(?:Operating profit|EBIT).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:million|MSEK)',
                r'(?:Rörelseresultat).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:miljoner|MSEK)',
            ],
            'net_income': [
                r'(?:Net income|Profit for the period).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:million|MSEK)',
                r'(?:Periodens resultat|Resultat efter skatt).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:miljoner|MSEK)',
            ],
            'eps': [
                r'(?:Earnings per share|EPS).*?(?:SEK|kr)?\s*([\d.,]+)',
                r'(?:Vinst per aktie).*?(?:SEK|kr)?\s*([\d.,]+)',
            ],
            'cash_flow': [
                r'(?:Operating cash flow|Cash flow from operations).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:million|MSEK)',
                r'(?:Kassaflöde från löpande verksamhet).*?(?:SEK|MSEK)?\s*([\d,]+)\s*(?:miljoner|MSEK)',
            ]
        }
        
        for metric, pattern_list in patterns.items():
            for pattern in pattern_list:
                matches = re.findall(pattern, text[:10000], re.IGNORECASE)  # Search first 10k chars
                if matches:
                    # Clean and convert the number
                    number_str = matches[0].replace(',', '')
                    try:
                        value = float(number_str)
                        extracts[metric] = value
                        logger.info(f"Quick extracted {metric}: {value}")
                        break
                    except:
                        pass
        
        return extracts
    
    def _build_smart_prompt(self, text: str, company_name: str, quick_extracts: Dict) -> str:
        """Build a focused prompt based on what we found"""
        
        # Truncate text intelligently - focus on tables and key sections
        relevant_text = self._extract_relevant_sections(text)
        
        # Build extraction hints based on quick extracts
        hints = []
        if quick_extracts:
            hints.append("I found these preliminary values:")
            for key, value in quick_extracts.items():
                hints.append(f"- {key}: {value}")
        
        prompt = f"""Extract financial metrics from this {company_name} document.

{' '.join(hints) if hints else ''}

Document excerpt:
{relevant_text}

CRITICAL INSTRUCTIONS:
1. Return ONLY a JSON object - no other text
2. Use the exact field names provided below
3. Convert all amounts to full numbers (millions to actual values)
4. Use null for missing values
5. NO comments, NO calculations in JSON

Required JSON structure:
{{
    "report_period": "Q2 2024",
    "fiscal_year": 2024,
    "revenue_reported": 11300000000,
    "revenue_currency": "SEK",
    "operating_profit_reported": 912000000,
    "operating_profit_adjusted": 1162000000,
    "net_income_reported": 643000000,
    "ebitda_reported": 1123000000,
    "operating_cash_flow": 524000000,
    "earnings_per_share_reported": 2.47,
    "earnings_per_share_adjusted": 3.26,
    "total_assets": 32383000000,
    "total_equity": 18629000000,
    "total_debt": 3663000000,
    "extraction_confidence": 0.85
}}

Remember: Return ONLY the JSON object."""
        
        return prompt
    
    def _extract_relevant_sections(self, text: str, max_length: int = 8000) -> str:
        """Extract the most relevant sections for financial data"""
        
        # Look for key sections
        sections = []
        
        # Financial highlights section
        if "financial highlights" in text.lower() or "finansiella höjdpunkter" in text.lower():
            idx = text.lower().find("financial highlights")
            if idx == -1:
                idx = text.lower().find("finansiella höjdpunkter")
            if idx != -1:
                sections.append(text[idx:idx+2000])
        
        # Income statement section
        keywords = ["income statement", "resultaträkning", "profit", "revenue", "omsättning"]
        for keyword in keywords:
            if keyword in text.lower():
                idx = text.lower().find(keyword)
                sections.append(text[max(0, idx-200):idx+1500])
                break
        
        # Look for tables with numbers
        lines = text.split('\n')
        for i, line in enumerate(lines):
            if re.search(r'\d+[,.]?\d*\s*(?:million|MSEK|miljoner)', line):
                # Found a line with financial numbers, grab context
                start = max(0, i-5)
                end = min(len(lines), i+10)
                sections.append('\n'.join(lines[start:end]))
        
        # Combine sections up to max length
        combined = '\n---\n'.join(sections)
        if len(combined) > max_length:
            combined = combined[:max_length]
        
        return combined if combined else text[:max_length]
    
    async def _get_llm_extraction_with_retry(self, prompt: str, max_retries: int = 3) -> Dict:
        """Get LLM extraction with retry logic and better error handling"""
        
        for attempt in range(max_retries):
            try:
                response = await self.ollama.generate_text(
                    prompt=prompt,
                    system_prompt="You are a financial data extraction expert. Return only valid JSON.",
                    temperature=0.1
                )
                
                # Clean the response
                cleaned = self._clean_llm_response(response)
                
                # Parse JSON
                data = json.loads(cleaned)
                return data
                
            except json.JSONDecodeError as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    # Try with a simpler prompt
                    prompt = self._simplify_prompt(prompt)
                else:
                    # Return minimal valid data
                    return self._get_fallback_data()
            except Exception as e:
                logger.error(f"LLM extraction error: {e}")
                return self._get_fallback_data()
    
    def _clean_llm_response(self, response: str) -> str:
        """Clean LLM response to get valid JSON"""
        
        # Remove markdown code blocks
        response = re.sub(r'```json\s*', '', response)
        response = re.sub(r'```\s*', '', response)
        
        # Find JSON object
        start_idx = response.find('{')
        end_idx = response.rfind('}')
        
        if start_idx != -1 and end_idx != -1:
            json_str = response[start_idx:end_idx+1]
            
            # Remove comments
            json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
            json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
            
            # Fix common issues
            json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
            json_str = re.sub(r',\s*]', ']', json_str)
            
            return json_str
        
        return response
    
    def _simplify_prompt(self, prompt: str) -> str:
        """Simplify prompt for retry"""
        # Just ask for the most essential fields
        return """Extract only these key metrics and return as JSON:
{
    "revenue_reported": null,
    "operating_profit_reported": null,
    "net_income_reported": null,
    "earnings_per_share_reported": null
}

Fill in the actual numbers you find."""
    
    def _get_fallback_data(self) -> Dict:
        """Get minimal fallback data structure"""
        return {
            "extraction_confidence": 0.0,
            "extraction_notes": "LLM extraction failed - using fallback"
        }
    
    def _create_metrics_from_extraction(
        self, 
        data: Dict, 
        company_name: str,
        document_text: str,
        quick_extracts: Dict
    ) -> FinancialMetrics:
        """Create FinancialMetrics object from extracted data"""
        
        # Start with empty metrics
        metrics = FinancialMetrics(
            company_name=company_name,
            report_period=data.get("report_period", "unknown")
        )
        
        # Map extracted data to metrics fields
        field_mapping = {
            "fiscal_year": "fiscal_year",
            "revenue_reported": "revenue_reported",
            "revenue_adjusted": "revenue_adjusted",
            "revenue_currency": "revenue_currency",
            "revenue_growth_pct": "revenue_growth_pct",
            "operating_profit_reported": "operating_profit_reported",
            "operating_profit_adjusted": "operating_profit_adjusted",
            "operating_adjustments": "operating_adjustments",
            "operating_margin_pct": "operating_margin_pct",
            "ebitda_reported": "ebitda_reported",
            "ebitda_adjusted": "ebitda_adjusted",
            "net_income_reported": "net_income_reported",
            "net_income_adjusted": "net_income_adjusted",
            "operating_cash_flow": "operating_cash_flow",
            "free_cash_flow": "free_cash_flow",
            "capex": "capex",
            "total_assets": "total_assets",
            "total_equity": "total_equity",
            "total_debt": "total_debt",
            "cash_and_equivalents": "cash_and_equivalents",
            "earnings_per_share_reported": "earnings_per_share_reported",
            "earnings_per_share_adjusted": "earnings_per_share_adjusted",
            "shares_outstanding": "shares_outstanding",
            "extraction_confidence": "extraction_confidence"
        }
        
        # Apply extracted data
        for json_field, metrics_field in field_mapping.items():
            if json_field in data and data[json_field] is not None:
                setattr(metrics, metrics_field, data[json_field])
        
        # Apply quick extracts if LLM missed them
        if quick_extracts:
            if 'revenue' in quick_extracts and not metrics.revenue_reported:
                metrics.revenue_reported = quick_extracts['revenue'] * 1000000  # Convert to full amount
            if 'operating_profit' in quick_extracts and not metrics.operating_profit_reported:
                metrics.operating_profit_reported = quick_extracts['operating_profit'] * 1000000
            if 'eps' in quick_extracts and not metrics.earnings_per_share_reported:
                metrics.earnings_per_share_reported = quick_extracts['eps']
        
        # Set confidence based on extraction success
        if not metrics.extraction_confidence:
            extracted_count = sum(1 for f in field_mapping.keys() if data.get(f) is not None)
            metrics.extraction_confidence = min(extracted_count / len(field_mapping), 1.0)
        
        return metrics
    
    def _extract_filename_metadata(self, metrics: FinancialMetrics, filename: str) -> FinancialMetrics:
        """Extract metadata from filename"""
        
        # Extract date from filename (e.g., "2025-07-17-aaks-interim-report...")
        date_pattern = r'^(\d{4}-\d{2}-\d{2})'
        date_match = re.match(date_pattern, filename)
        
        if date_match:
            try:
                date_str = date_match.group(1)
                metrics.report_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                # Set fiscal year if not already set
                if not metrics.fiscal_year:
                    metrics.fiscal_year = metrics.report_date.year
                    
            except Exception as e:
                logger.warning(f"Could not parse date from filename: {e}")
        
        # Extract report type from filename
        filename_lower = filename.lower()
        if 'interim' in filename_lower or 'quarterly' in filename_lower or 'kvartal' in filename_lower:
            metrics.report_type = "quarterly"
        elif 'annual' in filename_lower or 'årsredovisning' in filename_lower:
            metrics.report_type = "annual"
        
        return metrics
    
    def _validate_and_enhance_metrics(self, metrics: FinancialMetrics, document_text: str) -> FinancialMetrics:
        """Validate and enhance extracted metrics"""
        
        # Initialize warnings list
        if not metrics.data_warnings:
            metrics.data_warnings = []
        
        # Check for Swedish million notation and fix scaling
        if "million" in document_text.lower() or "miljoner" in document_text.lower():
            # Revenue scaling check
            if metrics.revenue_reported and metrics.revenue_reported < 1000000:
                metrics.revenue_reported *= 1000000
                metrics.data_warnings.append("Applied million scaling to revenue")
            
            # Operating profit scaling
            if metrics.operating_profit_reported and metrics.operating_profit_reported < 1000000:
                metrics.operating_profit_reported *= 1000000
                metrics.data_warnings.append("Applied million scaling to operating profit")
            
            # EBITDA scaling
            if metrics.ebitda_reported and metrics.ebitda_reported < 1000000:
                metrics.ebitda_reported *= 1000000
                metrics.data_warnings.append("Applied million scaling to EBITDA")
        
        # Ensure EPS is in reasonable range (not in millions)
        if metrics.earnings_per_share_reported and metrics.earnings_per_share_reported > 1000:
            metrics.earnings_per_share_reported /= 1000000
            metrics.data_warnings.append("Corrected EPS scaling")
        
        # Add extraction metadata
        metrics.model_used = "llama3:latest"
        metrics.extraction_notes = f"Extracted with improved v2 service"
        
        return metrics


class PDFTextExtractor:
    """Extract text from PDF files"""
    
    async def extract_text(self, pdf_path: str) -> Dict[str, Any]:
        """Extract text from PDF"""
        
        if not PDF_AVAILABLE:
            raise Exception("PDF libraries not installed")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                full_text = ""
                pages_text = []
                
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    pages_text.append(page_text)
                    full_text += page_text + "\n"
            
            return {
                "full_text": full_text,
                "pages_text": pages_text,
                "total_pages": len(pages_text)
            }
            
        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            return {"full_text": "", "pages_text": [], "error": str(e)}


class FinancialExtractionServiceV2:
    """Main service for extracting financial data - Version 2"""
    
    def __init__(self):
        self.pdf_extractor = PDFTextExtractor()
        self.financial_extractor = ImprovedFinancialExtractor()
        from shared.config import settings
        self.db_url = settings.database_url
    
    async def extract_from_pdf(self, pdf_path: str, company_name: str) -> Optional[FinancialMetrics]:
        """Extract financial metrics from PDF"""
        
        logger.info(f"Extracting from {pdf_path}")
        
        # Extract text
        pdf_data = await self.pdf_extractor.extract_text(pdf_path)
        
        if not pdf_data.get("full_text"):
            logger.error(f"No text extracted from {pdf_path}")
            return None
        
        # Extract metrics
        metrics = await self.financial_extractor.extract_metrics(
            document_text=pdf_data["full_text"],
            company_name=company_name,
            document_path=pdf_path
        )
        
        return metrics
    
    async def save_metrics(self, metrics: FinancialMetrics, document_id: str) -> bool:
        """Save metrics to database"""
        
        try:
            conn = await asyncpg.connect(self.db_url)
            
            # Convert metrics to dict for easier handling
            data = asdict(metrics)
            
            # Insert with all dual extraction fields
            await conn.execute("""
                INSERT INTO financial_metrics (
                    document_id, company_name, report_period, report_type, fiscal_year, report_date,
                    revenue_reported, revenue_adjusted, revenue_adjustments, revenue_currency, revenue_growth_pct,
                    gross_profit, gross_margin_pct,
                    operating_profit_reported, operating_profit_adjusted, operating_adjustments, operating_margin_pct,
                    ebitda_reported, ebitda_adjusted, ebitda_adjustments, ebitda_margin_pct,
                    net_income_reported, net_income_adjusted, net_income_adjustments, net_margin_pct,
                    operating_cash_flow, free_cash_flow, capex,
                    total_assets, total_equity, total_debt, cash_and_equivalents,
                    debt_to_equity, current_ratio, return_on_equity_pct, return_on_assets_pct,
                    earnings_per_share_reported, earnings_per_share_adjusted, eps_adjustments, shares_outstanding,
                    operational_metrics, extraction_confidence, model_used, extraction_notes
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                    $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
                    $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44
                )
                ON CONFLICT (document_id, report_period) 
                DO UPDATE SET
                    extraction_confidence = EXCLUDED.extraction_confidence,
                    extraction_date = CURRENT_TIMESTAMP
            """, 
                document_id,
                data['company_name'], data['report_period'], data['report_type'], 
                data['fiscal_year'], data['report_date'],
                data['revenue_reported'], data['revenue_adjusted'], data['revenue_adjustments'],
                data['revenue_currency'], data['revenue_growth_pct'],
                data['gross_profit'], data['gross_margin_pct'],
                data['operating_profit_reported'], data['operating_profit_adjusted'], 
                data['operating_adjustments'], data['operating_margin_pct'],
                data['ebitda_reported'], data['ebitda_adjusted'], data['ebitda_adjustments'],
                data['ebitda_margin_pct'],
                data['net_income_reported'], data['net_income_adjusted'], data['net_income_adjustments'],
                data['net_margin_pct'],
                data['operating_cash_flow'], data['free_cash_flow'], data['capex'],
                data['total_assets'], data['total_equity'], data['total_debt'], 
                data['cash_and_equivalents'],
                data['debt_to_equity'], data['current_ratio'], data['return_on_equity_pct'],
                data['return_on_assets_pct'],
                data['earnings_per_share_reported'], data['earnings_per_share_adjusted'],
                data['eps_adjustments'], data['shares_outstanding'],
                json.dumps(data['operational_metrics'] or {}), data['extraction_confidence'],
                data['model_used'], data['extraction_notes']
            )
            
            await conn.close()
            logger.info(f"Saved metrics for {metrics.company_name} - {metrics.report_period}")
            return True
            
        except Exception as e:
            logger.error(f"Database save failed: {e}")
            return False


# Test the improved service
async def test_improved_extraction():
    """Test the improved extraction service"""
    
    print("🧪 Testing Improved Financial Extraction Service V2")
    print("=" * 60)
    
    service = FinancialExtractionServiceV2()
    
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    metrics = await service.extract_from_pdf(pdf_path, "AAK")
    
    if metrics:
        print(f"✅ Extraction successful!")
        print(f"\n📊 Key Results:")
        print(f"  Report Date: {metrics.report_date}")
        print(f"  Report Period: {metrics.report_period}")
        print(f"  Revenue: {metrics.revenue_reported:,.0f} {metrics.revenue_currency}" if metrics.revenue_reported else "  Revenue: Not extracted")
        print(f"  Operating Profit: {metrics.operating_profit_reported:,.0f}" if metrics.operating_profit_reported else "  Operating Profit: Not extracted")
        print(f"  Net Income: {metrics.net_income_reported:,.0f}" if metrics.net_income_reported else "  Net Income: Not extracted")
        print(f"  EPS: {metrics.earnings_per_share_reported}" if metrics.earnings_per_share_reported else "  EPS: Not extracted")
        print(f"  Confidence: {metrics.extraction_confidence:.1%}")
        
        if metrics.data_warnings:
            print(f"\n⚠️ Warnings: {metrics.data_warnings}")
    else:
        print("❌ Extraction failed")
    
    return metrics


if __name__ == "__main__":
    asyncio.run(test_improved_extraction())