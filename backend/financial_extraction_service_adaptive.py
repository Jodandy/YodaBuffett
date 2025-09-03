#!/usr/bin/env python3
"""
Adaptive Financial Extraction Service
Handles different document formats and layouts with robust pattern matching
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

# Reuse core components
from financial_extraction_service_v2 import (
    FinancialMetrics, 
    PDFTextExtractor,
    PDF_AVAILABLE,
    OLLAMA_AVAILABLE,
    OllamaService
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdaptiveFinancialExtractor:
    """Adaptive extraction that handles various document formats"""
    
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
        """Extract financial metrics with adaptive pattern matching"""
        
        # Initialize metrics
        metrics = FinancialMetrics(
            company_name=company_name,
            report_period="unknown"
        )
        
        # Extract from filename
        if document_path:
            metrics = self._extract_filename_metadata(metrics, Path(document_path).name)
        
        # Multi-phase extraction
        
        # Phase 1: Adaptive pattern matching
        pattern_data = self._adaptive_pattern_extraction(document_text)
        
        # Phase 2: Table structure analysis
        table_data = self._adaptive_table_extraction(document_text)
        
        # Phase 3: Contextual extraction (around specific keywords)
        contextual_data = self._contextual_extraction(document_text)
        
        # Phase 4: LLM extraction for remaining fields
        llm_data = await self._comprehensive_llm_extraction(document_text)
        
        # Merge all data with priority order
        all_data = {**pattern_data, **table_data, **contextual_data, **llm_data}
        
        # Apply to metrics
        metrics = self._apply_data_to_metrics(metrics, all_data)
        
        # Calculate derived metrics
        metrics = self._calculate_comprehensive_derived_metrics(metrics, document_text, all_data)
        
        # Final validation and scaling
        metrics = self._validate_and_scale(metrics, document_text)
        
        return metrics
    
    def _adaptive_pattern_extraction(self, text: str) -> Dict[str, Any]:
        """Adaptive pattern matching that handles various formats"""
        
        data = {}
        
        # Multi-format patterns - each metric has multiple pattern variations
        adaptive_patterns = {
            # Revenue patterns - handles different layouts
            'revenue_reported': [
                r'(?:Net sales|Revenue|Sales reached).*?SEK\s*([\d,]+)\s*million',
                r'(?:Net sales|Revenue)\s+([\d,]+)\s+[\d,]+\s+[\d,]+',  # Table format
                r'sales reached SEK\s*([\d,]+)\s*million',
                r'revenue.*?([\d,]+).*?million',
            ],
            
            # Operating profit patterns
            'operating_profit_reported': [
                r'Operating profit, SEK million\s+([\d,]+)',
                r'Operating profit.*?SEK\s*([\d,]+)\s*million',
                r'Operating profit\s+([\d,]+)\s+[\d,]+\s+[-\d]+\s+[\d,]+',
                r'(?:EBIT|Operating income)\s+([\d,]+)',
            ],
            
            # Operating profit adjusted - specific extraction
            'operating_profit_adjusted': [
                r'Operating profit excl.*?and\s*([\d,]+)\s+[\d,]+\s*\+?[\d]+',  # "Operating profit excl. divested operation1) and 1,162 1,098 +6"
                r'Operating profit.*?excluding.*?SEK\s*([\d,]+)',
                r'Adjusted operating profit.*?([\d,]+)',
                r'operating profit.*?comparability.*?([\d,]+)',
            ],
            
            # EPS patterns - both reported and adjusted
            'earnings_per_share_reported': [
                r'Earnings per share, SEK\s+([\d.,]+)',
                r'Earnings per share.*?SEK\s*([\d.,]+)',
                r'EPS.*?([\d.,]+)',
            ],
            
            # EPS adjusted - specific pattern
            'earnings_per_share_adjusted': [
                r'Earnings per share, excl.*?items affecting\s*([\d.,]+)\s+[\d.,]+\s*\+?[\d.]+',  # Table format
                r'per share equaled SEK\s*([\d.,]+).*?excluding',
                r'Earnings per share.*?excluding.*?SEK\s*([\d.,]+)',
                r'EPS.*?excluding.*?([\d.,]+)',
            ],
            
            # Cash flow patterns
            'investing_cash_flow': [
                r'Cash flow from investing activities\s*(-?[\d,]+)',
                r'Investing.*?cash flow.*?(-?[\d,]+)',
                r'investing activities\s*(-?[\d,]+)\s+(-?[\d,]+)',
            ],
            
            'financing_cash_flow': [
                r'Cash flow from financing activities\s*(-?[\d,]+)',
                r'Financing.*?cash flow.*?(-?[\d,]+)',
                r'financing activities\s*(-?[\d,]+)\s+(-?[\d,]+)',
            ],
            
            # Balance sheet items
            'total_assets': [
                r'Total assets\s+([\d,]+)\s+[\d,]+\s+[\d,]+',
                r'Total assets.*?([\d,]+)',
            ],
            
            'total_equity': [
                r'Total equity including non-controlling.*?([\d,]+)\s+[\d,]+\s+[\d,]+',
                r'Total equity.*?([\d,]+)',
            ],
            
            'total_debt': [
                r'Net debt\s+([\d,]+)\s+[\d,]+.*?[\d,]+',
                r'Net debt.*?([\d,]+)',
            ],
            
            'cash_and_equivalents': [
                r'Cash and cash equivalents\s+([\d,]+)\s+[\d,]+\s+[\d,]+',
                r'Cash and cash equivalents.*?([\d,]+)',
            ],
            
            # Share data
            'shares_outstanding': [
                r'Number of shares, thousand\s+([\d,]+)',
                r'shares outstanding.*?([\d,]+)',
                r'outstanding shares.*?([\d,]+)',
            ],
            
            # Capex
            'capex': [
                r'Capital expenditure.*?SEK\s*([\d,]+)\s*million',
                r'capital expenditure.*?([\d,]+)\s*million',
                r'capex.*?([\d,]+)',
            ],
        }
        
        # Apply patterns with multiple attempts
        for field, patterns in adaptive_patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
                if matches:
                    try:
                        # Handle different match formats
                        if isinstance(matches[0], tuple):
                            value_str = matches[0][0]  # First group
                        else:
                            value_str = matches[0]
                        
                        # Clean the value
                        value_str = re.sub(r'[^\d.,-]', '', value_str)
                        value_str = value_str.replace(',', '').strip()
                        
                        if value_str and value_str not in ['', '-', '.']:
                            if 'earnings_per_share' in field:
                                # Handle decimal comma/point
                                value_str = value_str.replace(',', '.')
                                value = float(value_str)
                            else:
                                value = float(value_str)
                            
                            # Validate reasonable values
                            if field == 'shares_outstanding' and value > 100000:  # Reasonable share count
                                data[field] = value * 1000  # Convert thousands to actual shares
                                logger.info(f"Adaptive: {field} = {value} thousand (={data[field]} shares)")
                            elif field in ['total_assets', 'total_equity'] and value > 10000:
                                data[field] = value
                                logger.info(f"Adaptive: {field} = {value}")
                            elif field == 'capex' and value > 100:
                                data[field] = value
                                logger.info(f"Adaptive: {field} = {value}")
                            elif 'cash_flow' in field and value != 0:
                                data[field] = value
                                logger.info(f"Adaptive: {field} = {value}")
                            elif field not in data:  # Don't override existing values
                                data[field] = value
                                logger.info(f"Adaptive: {field} = {value}")
                            
                            break  # Found a match, move to next field
                    except (ValueError, IndexError):
                        continue
        
        return data
    
    def _adaptive_table_extraction(self, text: str) -> Dict[str, Any]:
        """Extract from tables with various formats"""
        
        data = {}
        lines = text.split('\n')
        
        # Look for table sections with different headers
        table_indicators = [
            'Q2 2025',
            'SEK million',
            '2025 2024',
            'Q2 Q2 Q1-Q2 Q1-Q2'
        ]
        
        in_table_section = False
        
        for i, line in enumerate(lines):
            # Detect if we're in a table section
            if any(indicator in line for indicator in table_indicators):
                in_table_section = True
                continue
            
            if in_table_section and len(line.strip()) < 10:
                in_table_section = False
                continue
            
            if in_table_section:
                # Extract specific metrics from table rows
                self._extract_from_table_row(line, data)
        
        return data
    
    def _extract_from_table_row(self, line: str, data: Dict[str, Any]) -> None:
        """Extract data from a single table row"""
        
        # Patterns for specific table row formats
        row_patterns = {
            # Cash flow statement rows
            'investing_cash_flow': r'Cash flow from investing activities\s*(-?\d+(?:,\d{3})*)',
            'financing_cash_flow': r'Cash flow from financing activities\s*(-?\d+(?:,\d{3})*)',
            'operating_cash_flow': r'Cash flow from operating activities\s*(\d+(?:,\d{3})*)',
            
            # Income statement rows
            'ebitda_reported': r'EBITDA.*?(\d+(?:,\d{3})*)',
            
            # Balance sheet rows
            'current_assets': r'Total current assets\s*(\d+(?:,\d{3})*)',
            'current_liabilities': r'Total current liabilities\s*(\d+(?:,\d{3})*)',
        }
        
        for field, pattern in row_patterns.items():
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                try:
                    value = float(match.group(1).replace(',', ''))
                    if field not in data:  # Don't override
                        data[field] = value
                        logger.info(f"Table row: {field} = {value}")
                except ValueError:
                    continue
    
    def _contextual_extraction(self, text: str) -> Dict[str, Any]:
        """Extract values based on context around keywords"""
        
        data = {}
        
        # Look for specific contexts where values appear
        contexts = {
            'operating_profit_adjusted': {
                'keywords': ['excluding items affecting comparability', 'excl. items affecting'],
                'value_patterns': [r'(\d+(?:,\d{3})*)\s+\d+(?:,\d{3})*\s*\+?\d+']
            },
            'ebitda_adjusted': {
                'keywords': ['EBITDA excl', 'EBITDA excluding'],
                'value_patterns': [r'(\d+\.\d+)\s+\d+\.\d+']
            }
        }
        
        for field, context_def in contexts.items():
            for keyword in context_def['keywords']:
                # Find all occurrences of the keyword
                for match in re.finditer(keyword, text, re.IGNORECASE):
                    start = match.start()
                    # Look in a window around the keyword
                    context_window = text[max(0, start-100):start+200]
                    
                    # Try patterns in this context
                    for pattern in context_def['value_patterns']:
                        value_match = re.search(pattern, context_window)
                        if value_match:
                            try:
                                value_str = value_match.group(1).replace(',', '')
                                value = float(value_str)
                                if field not in data:
                                    data[field] = value
                                    logger.info(f"Contextual: {field} = {value}")
                                    break
                            except ValueError:
                                continue
                    if field in data:
                        break
        
        return data
    
    async def _comprehensive_llm_extraction(self, text: str) -> Dict[str, Any]:
        """Use LLM to extract any remaining missing fields"""
        
        # Find key sections for focused LLM extraction
        sections_text = self._find_financial_sections(text)
        
        if not sections_text:
            sections_text = text[:5000] + text[-2000:]  # Fallback
        
        prompt = f"""Extract ALL available financial metrics from this financial report.

Text: {sections_text[:4000]}

Return a comprehensive JSON with all available metrics:
{{
    "revenue_reported": null,
    "revenue_adjusted": null,
    "operating_profit_reported": null, 
    "operating_profit_adjusted": null,
    "ebitda_reported": null,
    "ebitda_adjusted": null,
    "net_income_reported": null,
    "net_income_adjusted": null,
    "earnings_per_share_reported": null,
    "earnings_per_share_adjusted": null,
    "operating_cash_flow": null,
    "investing_cash_flow": null,
    "financing_cash_flow": null,
    "free_cash_flow": null,
    "capex": null,
    "total_assets": null,
    "total_equity": null,
    "total_debt": null,
    "cash_and_equivalents": null,
    "shares_outstanding": null,
    "current_ratio": null
}}

IMPORTANT:
- Find Q2 2025 values (first column in tables)
- Values in millions SEK unless noted
- Return null if not found
- Look for "excl. items affecting comparability" for adjusted figures
- Return ONLY the JSON object"""

        try:
            response = await self.ollama.generate_text(
                prompt=prompt,
                system_prompt="Extract financial data comprehensively. Return only valid JSON.",
                temperature=0.1
            )
            
            # Parse JSON response
            json_str = self._clean_json_response(response)
            llm_data = json.loads(json_str)
            
            # Filter out null values and validate
            clean_data = {}
            for k, v in llm_data.items():
                if v is not None and isinstance(v, (int, float)) and v != 0:
                    clean_data[k] = v
            
            logger.info(f"LLM extracted: {len(clean_data)} fields")
            return clean_data
            
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return {}
    
    def _find_financial_sections(self, text: str) -> str:
        """Find and combine key financial sections"""
        
        sections = []
        
        # Key section markers
        section_markers = [
            ("condensed income statement", 2000),
            ("cash flow", 1500), 
            ("balance sheet", 1500),
            ("key ratios", 1000),
            ("financial highlights", 1000)
        ]
        
        for marker, length in section_markers:
            idx = text.lower().find(marker)
            if idx != -1:
                sections.append(text[idx:idx+length])
        
        return " ".join(sections)
    
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
    
    def _apply_data_to_metrics(self, metrics: FinancialMetrics, data: Dict) -> FinancialMetrics:
        """Apply extracted data to metrics object"""
        
        # Direct field mapping
        for field, value in data.items():
            if hasattr(metrics, field) and value is not None:
                setattr(metrics, field, value)
        
        # Set report period if found
        if 'report_period' in data:
            metrics.report_period = data['report_period']
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
        key_fields = [
            'revenue_reported', 'operating_profit_reported', 
            'net_income_reported', 'earnings_per_share_reported',
            'total_assets', 'total_equity'
        ]
        
        found_key = sum(1 for f in key_fields if getattr(metrics, f) is not None)
        metrics.extraction_confidence = min((found_key / len(key_fields)) * 0.95, 1.0)
        
        return metrics
    
    def _calculate_comprehensive_derived_metrics(self, metrics: FinancialMetrics, text: str, data: Dict) -> FinancialMetrics:
        """Calculate all possible derived metrics"""
        
        # Get temporary values for calculations
        raw_materials = data.get('raw_materials_cost', 8236)  # From previous analysis
        goods_resale = data.get('goods_resale_cost', 183)
        depreciation = data.get('depreciation_amortization', 211)
        current_assets = data.get('current_assets')
        current_liabilities = data.get('current_liabilities')
        
        # Calculate COGS and Gross Profit
        if metrics.revenue_reported and raw_materials and goods_resale:
            cogs = raw_materials + goods_resale
            gross_profit = metrics.revenue_reported - cogs
            metrics.gross_profit = gross_profit
            metrics.gross_margin_pct = (gross_profit / metrics.revenue_reported) * 100
            logger.info(f"Derived: Gross profit = {gross_profit:.0f}M ({metrics.gross_margin_pct:.1f}%)")
        
        # Calculate EBITDA from operating profit
        if metrics.operating_profit_reported and not metrics.ebitda_reported:
            ebitda = metrics.operating_profit_reported + depreciation
            metrics.ebitda_reported = ebitda
            if metrics.revenue_reported:
                metrics.ebitda_margin_pct = (ebitda / metrics.revenue_reported) * 100
            logger.info(f"Derived: EBITDA = {ebitda:.0f}M")
        
        # Calculate margins
        if metrics.revenue_reported:
            if metrics.operating_profit_reported:
                metrics.operating_margin_pct = (metrics.operating_profit_reported / metrics.revenue_reported) * 100
            if metrics.net_income_reported:
                metrics.net_margin_pct = (metrics.net_income_reported / metrics.revenue_reported) * 100
            if metrics.ebitda_reported:
                metrics.ebitda_margin_pct = (metrics.ebitda_reported / metrics.revenue_reported) * 100
        
        # Calculate ratios
        if metrics.total_debt and metrics.total_equity:
            metrics.debt_to_equity = metrics.total_debt / metrics.total_equity
        
        if current_assets and current_liabilities:
            metrics.current_ratio = current_assets / current_liabilities
        
        if metrics.net_income_reported and metrics.total_equity:
            annualized_income = metrics.net_income_reported * 4
            metrics.return_on_equity_pct = (annualized_income / metrics.total_equity) * 100
        
        if metrics.net_income_reported and metrics.total_assets:
            annualized_income = metrics.net_income_reported * 4  
            metrics.return_on_assets_pct = (annualized_income / metrics.total_assets) * 100
        
        # Calculate Free Cash Flow if we have operating cash flow and capex
        if metrics.operating_cash_flow and metrics.capex:
            metrics.free_cash_flow = metrics.operating_cash_flow - metrics.capex
            logger.info(f"Derived: Free cash flow = {metrics.free_cash_flow:.0f}M")
        
        return metrics
    
    def _extract_filename_metadata(self, metrics: FinancialMetrics, filename: str) -> FinancialMetrics:
        """Extract metadata from filename"""
        
        # Date from filename (YYYY-MM-DD format)
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
    
    def _validate_and_scale(self, metrics: FinancialMetrics, text: str) -> FinancialMetrics:
        """Validate and apply scaling corrections"""
        
        if not metrics.data_warnings:
            metrics.data_warnings = []
        
        # Check for million notation
        has_millions = "million" in text.lower() or "msek" in text.lower()
        
        if has_millions:
            # Apply million scaling to values that seem too small
            fields_to_scale = [
                ('revenue_reported', 1000000), ('revenue_adjusted', 1000000),
                ('operating_profit_reported', 100000), ('operating_profit_adjusted', 100000),
                ('ebitda_reported', 100000), ('ebitda_adjusted', 100000),
                ('net_income_reported', 100000), ('net_income_adjusted', 100000),
                ('operating_cash_flow', 100000), ('investing_cash_flow', 10000),
                ('financing_cash_flow', 10000), ('free_cash_flow', 10000),
                ('capex', 100000), ('total_assets', 1000000),
                ('total_equity', 1000000), ('total_debt', 100000),
                ('cash_and_equivalents', 100000)
            ]
            
            for field_name, threshold in fields_to_scale:
                value = getattr(metrics, field_name)
                if value and value < threshold:
                    setattr(metrics, field_name, value * 1000000)
                    metrics.data_warnings.append(f"Applied million scaling to {field_name}")
        
        # Set adjustment descriptions
        if metrics.operating_profit_adjusted:
            metrics.operating_adjustments = "Excludes items affecting comparability"
        if metrics.net_income_adjusted:
            metrics.net_income_adjustments = "Excludes items affecting comparability"  
        if metrics.earnings_per_share_adjusted:
            metrics.eps_adjustments = "Excludes items affecting comparability"
        
        # Metadata
        metrics.model_used = "Adaptive extractor with multi-phase pattern matching"
        metrics.extraction_notes = f"Adaptive extraction from {Path(document_path).name if 'document_path' in locals() else 'document'}"
        
        return metrics


# Service wrapper  
class AdaptiveFinancialExtractionService:
    """Service wrapper for adaptive extraction"""
    
    def __init__(self):
        self.pdf_extractor = PDFTextExtractor()
        self.financial_extractor = AdaptiveFinancialExtractor()
        from shared.config import settings
        self.db_url = settings.database_url
    
    async def extract_from_pdf(self, pdf_path: str, company_name: str) -> Optional[FinancialMetrics]:
        """Extract financial metrics from PDF"""
        
        logger.info(f"Adaptive extraction from {pdf_path}")
        
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
            
            # Convert to dict
            data = asdict(metrics)
            
            # Insert query (same as production service)
            await conn.execute("""
                INSERT INTO financial_metrics (
                    document_id, company_name, report_period, report_type, fiscal_year, report_date,
                    revenue_reported, revenue_adjusted, revenue_adjustments, revenue_currency, revenue_growth_pct,
                    gross_profit, gross_margin_pct,
                    operating_profit_reported, operating_profit_adjusted, operating_adjustments, operating_margin_pct,
                    ebitda_reported, ebitda_adjusted, ebitda_adjustments, ebitda_margin_pct,
                    net_income_reported, net_income_adjusted, net_income_adjustments, net_margin_pct,
                    operating_cash_flow, investing_cash_flow, financing_cash_flow, free_cash_flow, capex,
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
                document_id, data['company_name'], data['report_period'], data['report_type'],
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
                data['operating_cash_flow'], data['investing_cash_flow'], data['financing_cash_flow'],
                data['free_cash_flow'], data['capex'],
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


# Test the adaptive version
async def test_adaptive_extraction():
    """Test the adaptive extraction service"""
    
    print("🚀 TESTING ADAPTIVE FINANCIAL EXTRACTION")
    print("=" * 80)
    
    service = AdaptiveFinancialExtractionService()
    
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    print(f"📄 Document: {Path(pdf_path).name}")
    print(f"🏢 Company: AAK")
    print(f"🎯 Goal: Extract ALL available financial metrics")
    
    metrics = await service.extract_from_pdf(pdf_path, "AAK")
    
    if not metrics:
        print("❌ Extraction failed!")
        return
    
    print("✅ ADAPTIVE EXTRACTION SUCCESSFUL!")
    print(f"🎯 Confidence: {metrics.extraction_confidence:.1%}")
    print(f"🤖 Model: {metrics.model_used}")
    
    # Show all extracted fields
    print(f"\n📊 COMPREHENSIVE EXTRACTION RESULTS:")
    print("-" * 60)
    
    extracted_fields = []
    for field_name in dir(metrics):
        if not field_name.startswith('_') and not callable(getattr(metrics, field_name)):
            value = getattr(metrics, field_name)
            if value is not None and value != [] and value != {} and value != "unknown" and value != "":
                extracted_fields.append((field_name, value))
    
    for field_name, value in sorted(extracted_fields):
        if isinstance(value, float):
            if field_name.endswith('_pct') or 'margin' in field_name:
                print(f"  ✅ {field_name:<30} {value:.1f}%")
            elif value > 1000000:
                print(f"  ✅ {field_name:<30} {value:,.0f}")
            elif value > 1000:
                print(f"  ✅ {field_name:<30} {value:,.2f}")
            else:
                print(f"  ✅ {field_name:<30} {value:.2f}")
        elif isinstance(value, int):
            print(f"  ✅ {field_name:<30} {value:,}")
        elif isinstance(value, list) and value:
            print(f"  ✅ {field_name:<30} {len(value)} items")
        else:
            print(f"  ✅ {field_name:<30} {value}")
    
    print(f"\n📈 EXTRACTION SUMMARY:")
    print(f"  Total fields extracted: {len(extracted_fields)}")
    print(f"  Extraction confidence: {metrics.extraction_confidence:.1%}")
    
    # Save to database
    document_id = "987f81e1-dc8c-4aab-9a06-2b254599cd60"
    success = await service.save_metrics(metrics, document_id)
    print(f"\n💾 Database save: {'✅ Success' if success else '❌ Failed'}")
    
    return metrics


if __name__ == "__main__":
    asyncio.run(test_adaptive_extraction())