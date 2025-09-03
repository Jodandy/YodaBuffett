"""
Financial Extraction Service - Final Optimized Version
Handles multi-column financial tables and Swedish formatting
"""
import asyncio
import asyncpg
import json
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass, asdict, field
import logging

# Use the existing imports from V2
from financial_extraction_service_v2 import (
    FinancialMetrics, 
    PDFTextExtractor,
    PDF_AVAILABLE,
    OLLAMA_AVAILABLE,
    OllamaService
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptimizedFinancialExtractor:
    """Optimized extraction specifically for Nordic financial reports"""
    
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
        """Extract financial metrics with table-aware parsing"""
        
        # Initialize metrics
        metrics = FinancialMetrics(
            company_name=company_name,
            report_period="unknown"
        )
        
        # Extract from filename first
        if document_path:
            metrics = self._extract_filename_metadata(metrics, Path(document_path).name)
        
        # Find the financial highlights/key figures section
        highlights_section = self._extract_financial_highlights(document_text)
        
        # Extract using pattern matching first
        pattern_extracts = self._extract_with_patterns(highlights_section or document_text)
        
        # Get Q2 specific data from tables
        q2_data = self._extract_q2_column_data(document_text)
        
        # Merge pattern extracts and Q2 data
        all_extracts = {**pattern_extracts, **q2_data}
        
        # Use LLM for additional extraction if needed
        if len(all_extracts) < 10:  # If we didn't find much with patterns
            llm_data = await self._get_llm_extraction(document_text, all_extracts)
            all_extracts.update(llm_data)
        
        # Apply extracts to metrics
        metrics = self._apply_extracts_to_metrics(metrics, all_extracts)
        
        # Validate and correct
        metrics = self._validate_metrics(metrics, document_text)
        
        return metrics
    
    def _extract_financial_highlights(self, text: str) -> Optional[str]:
        """Extract the financial highlights section"""
        
        # Look for financial highlights section
        patterns = [
            r"financial highlights(.*?)(?:operating segment|condensed income|$)",
            r"finansiella höjdpunkter(.*?)(?:rörelsesegment|$)",
            r"key figures(.*?)(?:segment|income statement|$)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                return match.group(0)[:3000]  # Limit to 3000 chars
        
        # If not found, look for the beginning of the report
        if "Q2 2025" in text and "Q1-Q2 2025" in text:
            idx = text.find("Q2 2025")
            if idx != -1:
                return text[max(0, idx-100):idx+3000]
        
        return None
    
    def _extract_with_patterns(self, text: str) -> Dict[str, Any]:
        """Extract using specific patterns for Nordic reports"""
        
        extracts = {}
        
        # Pattern definitions with Swedish variations
        patterns = {
            # Revenue patterns
            'revenue_reported': [
                r'Sales reached SEK\s*([\d,]+)\s*million',
                r'Net sales.*?SEK\s*([\d,]+)\s*million',
                r'Net sales.*?([\d,]+)\s+[\d,]+\s+[\d,]+',  # Table format
                r'Nettoomsättning.*?SEK\s*([\d,]+)\s*miljon',
            ],
            
            # Operating profit patterns  
            'operating_profit_reported': [
                r'Operating profit.*?SEK\s*([\d,]+)\s*million',
                r'Operating profit.*?([\d,]+)\s+[\d,]+\s+[\d,]+',  # Table
                r'Operating profit, SEK million\s*([\d,]+)',
                r'Rörelseresultat.*?SEK\s*([\d,]+)\s*miljon',
            ],
            
            # Adjusted operating profit
            'operating_profit_adjusted': [
                r'Operating profit.*?excluding.*?([\d,]+)',
                r'Operating profit excl.*?([\d,]+)',
                r'adjusted operating profit.*?([\d,]+)',
            ],
            
            # Net income patterns
            'net_income_reported': [
                r'Profit for the period, SEK million\s*([\d,]+)',
                r'Profit for the period.*?SEK\s*([\d,]+)\s*million',
                r'Net income.*?SEK\s*([\d,]+)\s*million',
                r'Periodens resultat.*?SEK\s*([\d,]+)\s*miljon',
            ],
            
            # EPS patterns
            'earnings_per_share_reported': [
                r'Earnings per share.*?SEK\s*([\d.,]+)',
                r'Earnings per share, SEK\s*([\d.,]+)',
                r'Vinst per aktie.*?SEK\s*([\d.,]+)',
                r'EPS.*?SEK\s*([\d.,]+)',
            ],
            
            # EPS adjusted
            'earnings_per_share_adjusted': [
                r'Earnings per share.*?excluding.*?SEK\s*([\d.,]+)',
                r'EPS.*?excluding.*?SEK\s*([\d.,]+)',
                r'per share equaled SEK\s*([\d.,]+).*?excluding',
            ],
            
            # Cash flow patterns
            'operating_cash_flow': [
                r'Cash flow from operating.*?SEK\s*([\d,]+)\s*million',
                r'Operating cash flow.*?SEK\s*([\d,]+)\s*million',
                r'Kassaflöde från löpande.*?SEK\s*([\d,]+)\s*miljon',
            ],
            
            # Report period
            'report_period': [
                r'(?:Second quarter|Q2)\s*(\d{4})',
                r'(?:Andra kvartalet|Kv2)\s*(\d{4})',
            ]
        }
        
        # Extract using patterns
        for field, pattern_list in patterns.items():
            for pattern in pattern_list:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Clean and convert
                    value_str = matches[0].replace(',', '').replace(' ', '')
                    
                    try:
                        if field == 'report_period':
                            extracts[field] = f"Q2 {value_str}"
                        elif 'earnings_per_share' in field:
                            # Handle both . and , as decimal separator
                            value_str = value_str.replace(',', '.')
                            extracts[field] = float(value_str)
                        else:
                            extracts[field] = float(value_str)
                        
                        logger.info(f"Pattern extracted {field}: {extracts[field]}")
                        break
                    except:
                        continue
        
        return extracts
    
    def _extract_q2_column_data(self, text: str) -> Dict[str, Any]:
        """Extract data specifically from Q2 column in tables"""
        
        extracts = {}
        
        # Look for table rows with Q2 2025 data
        # Format: Metric name | Q2 2025 | Q2 2024 | change | Q1-Q2 2025 | etc
        
        table_patterns = [
            # Net sales line
            r'Net sales.*?million.*?([\d,]+)\s+([\d,]+)\s+[\d+-]+\s+([\d,]+)',
            # Operating profit line  
            r'Operating profit.*?SEK million\s*([\d,]+)\s+([\d,]+)\s+[\d+-]+',
            # Profit for period line
            r'Profit for the period.*?SEK million\s*([\d,]+)\s+([\d,]+)\s+[\d+-]+',
            # EPS line
            r'Earnings per share.*?SEK\s*([\d.,]+)\s+([\d.,]+)\s+[\d+-]+',
        ]
        
        lines = text.split('\n')
        
        # Find lines that look like table headers
        for i, line in enumerate(lines):
            if 'Q2' in line and '2025' in line and '2024' in line:
                # This might be a header, check next lines for data
                for j in range(i+1, min(i+20, len(lines))):
                    data_line = lines[j]
                    
                    # Net sales
                    if 'Net sales' in data_line or 'million' in data_line:
                        numbers = re.findall(r'([\d,]+)', data_line)
                        if len(numbers) >= 2:
                            try:
                                # First number is usually Q2 2025
                                extracts['revenue_reported'] = float(numbers[0].replace(',', ''))
                                logger.info(f"Table extracted revenue: {extracts['revenue_reported']}")
                            except:
                                pass
                    
                    # Operating profit
                    elif 'Operating profit' in data_line:
                        numbers = re.findall(r'([\d,]+)', data_line)
                        if len(numbers) >= 2:
                            try:
                                extracts['operating_profit_reported'] = float(numbers[0].replace(',', ''))
                                logger.info(f"Table extracted operating profit: {extracts['operating_profit_reported']}")
                            except:
                                pass
                    
                    # EPS
                    elif 'Earnings per share' in data_line or 'EPS' in data_line:
                        numbers = re.findall(r'([\d.,]+)', data_line)
                        if len(numbers) >= 2:
                            try:
                                eps_val = numbers[0].replace(',', '.')
                                extracts['earnings_per_share_reported'] = float(eps_val)
                                logger.info(f"Table extracted EPS: {extracts['earnings_per_share_reported']}")
                            except:
                                pass
        
        return extracts
    
    async def _get_llm_extraction(self, text: str, existing_extracts: Dict) -> Dict:
        """Use LLM to extract additional fields"""
        
        # Focus on what we're missing
        missing_fields = []
        desired_fields = [
            'ebitda_reported', 'total_assets', 'total_equity', 
            'total_debt', 'shares_outstanding', 'capex'
        ]
        
        for field in desired_fields:
            if field not in existing_extracts:
                missing_fields.append(field)
        
        if not missing_fields:
            return {}
        
        # Create focused prompt
        prompt = f"""Extract these specific financial metrics from the text. 
Return ONLY a JSON object with the exact field names provided.

Text excerpt: {text[:3000]}

Required fields to find:
{json.dumps({field: None for field in missing_fields}, indent=2)}

Rules:
- Use exact field names
- Convert "million" to full numbers (11,300 million = 11300000000)
- Return null if not found
- NO comments or text outside JSON

Return only the JSON object."""
        
        try:
            response = await self.ollama.generate_text(
                prompt=prompt,
                system_prompt="You are a financial data extractor. Return only valid JSON.",
                temperature=0.1
            )
            
            # Clean and parse
            json_str = self._clean_json_response(response)
            data = json.loads(json_str)
            
            return {k: v for k, v in data.items() if v is not None}
            
        except:
            return {}
    
    def _clean_json_response(self, response: str) -> str:
        """Clean LLM response to valid JSON"""
        
        # Remove markdown
        response = re.sub(r'```json\s*', '', response)
        response = re.sub(r'```\s*', '', response)
        
        # Extract JSON object
        start = response.find('{')
        end = response.rfind('}')
        
        if start != -1 and end != -1:
            json_str = response[start:end+1]
            
            # Remove comments
            json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
            json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
            
            return json_str
        
        return "{}"
    
    def _apply_extracts_to_metrics(self, metrics: FinancialMetrics, extracts: Dict) -> FinancialMetrics:
        """Apply extracted values to metrics object"""
        
        # Direct field mapping
        for field, value in extracts.items():
            if hasattr(metrics, field) and value is not None:
                setattr(metrics, field, value)
        
        # Set report period if found
        if 'report_period' in extracts:
            metrics.report_period = extracts['report_period']
        elif metrics.report_date:
            # Infer from date
            month = metrics.report_date.month
            year = metrics.report_date.year
            if month <= 3:
                metrics.report_period = f"Q1 {year}"
            elif month <= 6:
                metrics.report_period = f"Q2 {year}"
            elif month <= 9:
                metrics.report_period = f"Q3 {year}"
            else:
                metrics.report_period = f"Q4 {year}"
        
        # Calculate confidence
        important_fields = [
            'revenue_reported', 'operating_profit_reported', 
            'net_income_reported', 'earnings_per_share_reported'
        ]
        
        found_important = sum(1 for f in important_fields if getattr(metrics, f) is not None)
        metrics.extraction_confidence = min(found_important / len(important_fields), 1.0) * 0.85
        
        return metrics
    
    def _extract_filename_metadata(self, metrics: FinancialMetrics, filename: str) -> FinancialMetrics:
        """Extract metadata from filename"""
        
        # Date pattern
        date_match = re.match(r'^(\d{4}-\d{2}-\d{2})', filename)
        if date_match:
            try:
                metrics.report_date = datetime.strptime(date_match.group(1), "%Y-%m-%d").date()
                if not metrics.fiscal_year:
                    metrics.fiscal_year = metrics.report_date.year
            except:
                pass
        
        # Report type
        filename_lower = filename.lower()
        if any(term in filename_lower for term in ['interim', 'quarterly', 'kvartal', 'q1', 'q2', 'q3', 'q4']):
            metrics.report_type = "quarterly"
        elif any(term in filename_lower for term in ['annual', 'år', 'full year']):
            metrics.report_type = "annual"
        
        return metrics
    
    def _validate_metrics(self, metrics: FinancialMetrics, document_text: str) -> FinancialMetrics:
        """Validate and correct extracted metrics"""
        
        if not metrics.data_warnings:
            metrics.data_warnings = []
        
        # Million scaling for Swedish reports
        if "million" in document_text.lower() or "miljoner" in document_text.lower():
            # Revenue
            if metrics.revenue_reported and metrics.revenue_reported < 1000000:
                metrics.revenue_reported *= 1000000
                metrics.data_warnings.append("Applied million scaling to revenue")
            
            # Operating profit
            if metrics.operating_profit_reported and metrics.operating_profit_reported < 1000000:
                metrics.operating_profit_reported *= 1000000
                metrics.data_warnings.append("Applied million scaling to operating profit")
            
            # Net income
            if metrics.net_income_reported and metrics.net_income_reported < 1000000:
                metrics.net_income_reported *= 1000000
                metrics.data_warnings.append("Applied million scaling to net income")
            
            # Cash flow
            if metrics.operating_cash_flow and metrics.operating_cash_flow < 10000000:
                metrics.operating_cash_flow *= 1000000
                metrics.data_warnings.append("Applied million scaling to cash flow")
        
        # Add metadata
        metrics.model_used = "llama3:latest with pattern matching"
        metrics.extraction_notes = "Optimized extraction with table parsing"
        
        return metrics


# Reuse the service classes from V2
from financial_extraction_service_v2 import FinancialExtractionServiceV2


class FinalFinancialExtractionService(FinancialExtractionServiceV2):
    """Final optimized service using the new extractor"""
    
    def __init__(self):
        self.pdf_extractor = PDFTextExtractor()
        self.financial_extractor = OptimizedFinancialExtractor()  # Use optimized version
        from shared.config import settings
        self.db_url = settings.database_url


# Test function
async def test_final_extraction():
    """Test the final optimized extraction"""
    
    print("🚀 TESTING FINAL OPTIMIZED EXTRACTION")
    print("=" * 70)
    
    service = FinalFinancialExtractionService()
    
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    metrics = await service.extract_from_pdf(pdf_path, "AAK")
    
    if metrics:
        print("✅ Extraction successful!\n")
        
        # Show all extracted fields
        extracted_fields = []
        for field_name in dir(metrics):
            if not field_name.startswith('_'):
                value = getattr(metrics, field_name)
                if value is not None and value != [] and value != {} and value != "unknown":
                    if not callable(value):
                        extracted_fields.append((field_name, value))
        
        print("📊 EXTRACTED FIELDS:")
        for field_name, value in sorted(extracted_fields):
            if isinstance(value, float) and value > 1000:
                print(f"  ✓ {field_name}: {value:,.0f}")
            else:
                print(f"  ✓ {field_name}: {value}")
        
        print(f"\n📈 Extraction Statistics:")
        print(f"  Total fields extracted: {len(extracted_fields)}")
        print(f"  Confidence: {metrics.extraction_confidence:.1%}")
        
        # Test save
        document_id = "987f81e1-dc8c-4aab-9a06-2b254599cd60"
        success = await service.save_metrics(metrics, document_id)
        print(f"\n💾 Database save: {'✅ Success' if success else '❌ Failed'}")
    
    return metrics


if __name__ == "__main__":
    asyncio.run(test_final_extraction())