#!/usr/bin/env python3
"""
Fixed Financial Extraction Service
Addresses issues found in multi-company testing
"""
import asyncio
import asyncpg
import json
import re
import uuid
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


class FixedFinancialExtractor:
    """Fixed extraction with improved patterns and error handling"""
    
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
        """Extract financial metrics with robust error handling"""
        
        # Initialize metrics
        metrics = FinancialMetrics(
            company_name=company_name,
            report_period="unknown"
        )
        
        # Extract from filename
        if document_path:
            metrics = self._extract_filename_metadata(metrics, Path(document_path).name)
        
        # Multi-phase extraction with improved patterns
        try:
            # Phase 1: Enhanced pattern matching
            pattern_data = self._enhanced_pattern_extraction(document_text)
            
            # Phase 2: Improved table extraction
            table_data = self._improved_table_extraction(document_text)
            
            # Phase 3: Balance sheet focused extraction
            balance_sheet_data = self._extract_balance_sheet_data(document_text)
            
            # Phase 4: Robust LLM extraction
            llm_data = await self._robust_llm_extraction(document_text)
            
            # Merge all data with conflict resolution
            all_data = self._merge_extraction_data(pattern_data, table_data, balance_sheet_data, llm_data)
            
            # Apply to metrics
            metrics = self._apply_data_to_metrics(metrics, all_data)
            
            # Calculate comprehensive derived metrics
            metrics = self._calculate_comprehensive_derived_metrics(metrics, document_text, all_data)
            
            # Final validation and scaling
            metrics = self._validate_and_scale(metrics, document_text)
            
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            # Set minimum confidence for partial extraction
            metrics.extraction_confidence = 0.1
        
        return metrics
    
    def _enhanced_pattern_extraction(self, text: str) -> Dict[str, Any]:
        """Enhanced pattern matching with more variations"""
        
        data = {}
        
        # Enhanced patterns with more variations for different companies
        enhanced_patterns = {
            # Revenue patterns - multiple formats (specific patterns FIRST, then broad)
            'revenue_reported': [
                r'(?:försäljning|Försäljning).*?SEK\s*([0-9,]+)\s*.*?miljarder',  # Swedish billions: "SEK 56,1 miljarder"
                r'(?:Net sales|Sales|Revenue).*?SEK\s*([0-9.,]+)\s*\([^)]*\)\s*b\.',  # "SEK 56.1 (59.8) b." format (BILLIONS!)
                r'(?:Net sales|Sales|Revenue).*?SEK\s*([0-9.,]+)\s*b\.',  # "SEK 56.1 b." format (BILLIONS!)
                r'Intäkterna.*?MSEK\s*([0-9\s,]+)',  # Swedish: "Intäkterna nådde MSEK 7 196"
                r'Sales reached.*?([0-9,]+)\s*million',
                r'(?:Net sales|Revenue|Total revenue|Sales)\s*.*?([0-9,]+)(?:\s*million|\s*MSEK)',
                r'(?:Net sales|Revenue|Total revenue|Sales|Intäkter|Omsättning).*?MSEK\s*([0-9\s,]+)',
                r'(?:Net sales|Revenue)\s+([0-9,]+)\s+[0-9,]+\s+[0-9,]+',
                r'Total revenue.*?([0-9,]+)',
                r'Net sales.*?([0-9,]+)\s*[0-9,]+\s*[0-9,]+',  # Table format
            ],
            
            # Operating profit patterns
            'operating_profit_reported': [
                r'Operating profit.*?([0-9,]+)(?:\s*million|\s*MSEK)',
                r'Operating income.*?([0-9,]+)',
                r'EBIT.*?([0-9,]+)',
                r'Operating profit\s+([0-9,]+)\s+[0-9,]+',
                r'Operating result.*?([0-9,]+)',
            ],
            
            # Net income patterns
            'net_income_reported': [
                r'(?:Net income|Profit for the period|Net result).*?([0-9,]+)(?:\s*million|\s*MSEK)',
                r'(?:Net income|Profit)\s+([0-9,]+)\s+[0-9,]+',
                r'Profit after tax.*?([0-9,]+)',
                r'Result for the period.*?([0-9,]+)',
            ],
            
            # EPS patterns
            'earnings_per_share_reported': [
                r'(?:Earnings per share|EPS).*?([0-9.,]+)',
                r'Per share.*?([0-9.,]+)',
                r'Basic earnings per share.*?([0-9.,]+)',
            ],
            
            # Cash flow patterns
            'operating_cash_flow': [
                r'(?:Operating cash flow|Cash flow from operating activities).*?([0-9,]+)',
                r'Cash flow from operations.*?([0-9,]+)',
                r'Operating activities.*?([0-9,]+)',
            ],
            
            'investing_cash_flow': [
                r'(?:Investing cash flow|Cash flow from investing activities).*?(-?[0-9,]+)',
                r'Investment activities.*?(-?[0-9,]+)',
                r'Investing activities.*?(-?[0-9,]+)',
            ],
            
            'financing_cash_flow': [
                r'(?:Financing cash flow|Cash flow from financing activities).*?(-?[0-9,]+)',
                r'Financing activities.*?(-?[0-9,]+)',
            ],
            
            # Balance sheet patterns
            'total_assets': [
                r'Total assets.*?([0-9,]+)',
                r'Total assets\s+([0-9,]+)\s+[0-9,]+',
                r'Assets total.*?([0-9,]+)',
            ],
            
            'total_equity': [
                r'Total (?:equity|shareholders.* equity).*?([0-9,]+)',
                r'Equity total.*?([0-9,]+)',
                r'Shareholders.* equity\s+([0-9,]+)',
            ],
            
            'cash_and_equivalents': [
                r'Cash and cash equivalents.*?([0-9,]+)',
                r'Cash and bank.*?([0-9,]+)',
                r'Cash.*?([0-9,]+)',
            ],
        }
        
        # Apply enhanced patterns
        for field, patterns in enhanced_patterns.items():
            for pattern in patterns:
                try:
                    matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
                    if matches:
                        # Handle different match formats
                        value_str = matches[0] if isinstance(matches[0], str) else matches[0][0] if isinstance(matches[0], tuple) else str(matches[0])
                        
                        # Check if this is a Swedish format (has comma as decimal separator)
                        is_swedish_format = 'miljarder' in pattern or 'MSEK' in pattern
                        
                        # Clean the value
                        if is_swedish_format and re.match(r'^\d{1,3},\d{1,2}$', value_str.strip()):
                            # Swedish decimal format: "56,1" → "56.1"
                            value_str = value_str.replace(',', '.')
                        else:
                            # Standard format: remove thousands separator commas
                            value_str = re.sub(r'[^\d.,-]', '', value_str).replace(',', '').strip()
                        
                        if value_str and value_str not in ['', '-', '.']:
                            if 'earnings_per_share' in field:
                                value = float(value_str.replace(',', '.'))
                            else:
                                value = float(value_str)
                            
                            # Apply scale conversion based on pattern context
                            if r'b\.' in pattern or 'billion' in pattern.lower() or 'miljarder' in pattern.lower():
                                value = value * 1000000000  # Convert billions to actual value
                            elif 'million' in pattern.lower():
                                value = value * 1000000  # Convert millions to actual value
                            elif 'thousand' in pattern.lower():
                                value = value * 1000    # Convert thousands to actual value
                            elif 'msek' in pattern.lower():
                                value = value * 1000000  # MSEK = millions of SEK
                            
                            # Validate reasonable values
                            if self._validate_metric_value(field, value):
                                if field not in data:  # Don't override existing values
                                    data[field] = value
                                    logger.info(f"Enhanced pattern: {field} = {value}")
                                break
                except (ValueError, IndexError) as e:
                    continue
        
        return data
    
    def _improved_table_extraction(self, text: str) -> Dict[str, Any]:
        """Improved table extraction with better format handling"""
        
        data = {}
        lines = text.split('\n')
        
        # Look for different table formats
        table_formats = [
            # Format 1: Q2 2025 | Q2 2024 | Change
            r'Q2\s+2025.*?Q2\s+2024',
            # Format 2: SEK million | 2025 | 2024
            r'SEK\s+million.*?2025.*?2024',
            # Format 3: Jan-Jun 2025 | Jan-Jun 2024
            r'(?:Jan-Jun|H1).*?2025.*?2024',
        ]
        
        in_financial_table = False
        table_context = []
        
        for i, line in enumerate(lines):
            # Detect table headers
            if any(re.search(fmt, line, re.IGNORECASE) for fmt in table_formats):
                in_financial_table = True
                table_context = []
                continue
            
            # End of table detection
            if in_financial_table and (len(line.strip()) < 5 or line.startswith('Note') or line.startswith('See')):
                in_financial_table = False
                continue
            
            if in_financial_table:
                # Extract from table rows
                self._extract_from_table_row_improved(line, data, table_context)
                table_context.append(line)
        
        return data
    
    def _extract_from_table_row_improved(self, line: str, data: Dict[str, Any], context: List[str]) -> None:
        """Improved table row extraction with better number parsing"""
        
        # Patterns for different metrics in table rows
        row_patterns = {
            # Revenue/Sales patterns
            'revenue_reported': [
                r'(?:Net sales|Revenue|Total revenue).*?([0-9,]+)',
                r'Sales.*?([0-9,]+)\s+[0-9,]+',
                r'Sales reached SEK ([0-9,]+)',
                r'Net sales reached SEK ([0-9,]+)',
            ],
            
            # Profit patterns
            'operating_profit_reported': [
                r'Operating (?:profit|income).*?([0-9,]+)',
                r'EBIT.*?([0-9,]+)',
            ],
            
            'net_income_reported': [
                r'(?:Net income|Profit for the period).*?([0-9,]+)',
                r'Result for the period.*?([0-9,]+)',
            ],
            
            # Cash flow patterns
            'operating_cash_flow': [
                r'Operating activities.*?([0-9,]+)',
                r'Cash flow from operating.*?([0-9,]+)',
            ],
            
            'investing_cash_flow': [
                r'Investing activities.*?(-?[0-9,]+)',
                r'Investment activities.*?(-?[0-9,]+)',
            ],
            
            'financing_cash_flow': [
                r'Financing activities.*?(-?[0-9,]+)',
            ],
            
            # Balance sheet patterns
            'total_assets': [
                r'Total assets.*?([0-9,]+)',
            ],
            
            'total_equity': [
                r'Total (?:equity|shareholders.* equity).*?([0-9,]+)',
            ],
        }
        
        for field, patterns in row_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    try:
                        value_str = match.group(1).replace(',', '').replace(' ', '')
                        value = float(value_str)
                        
                        # Apply scale conversion based on document context
                        value = self._apply_scale_conversion(value, field, context)
                        
                        if self._validate_metric_value(field, value) and field not in data:
                            data[field] = value
                            logger.info(f"Table row improved: {field} = {value}")
                            break
                    except (ValueError, IndexError):
                        continue
    
    def _apply_scale_conversion(self, value: float, field: str, context: List[str]) -> float:
        """Apply scale conversion based on document context"""
        
        # Look for scale indicators in context (table headers)
        scale_indicators = []
        context_text = ' '.join(context[-5:]).lower()  # Last 5 lines of context
        
        # Check for explicit millions/thousands indicators
        if any(indicator in context_text for indicator in ['sek million', 'millions of sek', 'msek', 'amounts in sek million']):
            scale_indicators.append('millions')
        elif any(indicator in context_text for indicator in ['sek thousand', 'thousands of sek', 'tsek', 'amounts in sek thousand']):
            scale_indicators.append('thousands')
        
        # Smart scale detection based on field type and value range
        if not scale_indicators:
            # Revenue/assets in thousands range (10-100k) are likely in millions
            if field in ['revenue_reported', 'total_assets'] and 1000 <= value <= 100000:
                scale_indicators.append('millions')
            # Very small values for major metrics suggest millions scale
            elif field in ['revenue_reported', 'operating_profit_reported', 'total_assets'] and 1 <= value <= 1000:
                scale_indicators.append('millions')
        
        # Apply conversion
        if 'millions' in scale_indicators:
            return value * 1000000
        elif 'thousands' in scale_indicators:
            return value * 1000
        
        return value
    
    def _extract_balance_sheet_data(self, text: str) -> Dict[str, Any]:
        """Focused extraction of balance sheet data"""
        
        data = {}
        
        # Look for balance sheet section specifically
        balance_sheet_section = self._find_balance_sheet_section(text)
        
        if balance_sheet_section:
            # Balance sheet specific patterns
            balance_sheet_patterns = {
                'total_assets': [
                    r'Total assets\s+([0-9,]+)',
                    r'Sum assets.*?([0-9,]+)',
                    r'Assets total.*?([0-9,]+)',
                ],
                
                'total_equity': [
                    r'Total equity.*?([0-9,]+)',
                    r'Shareholders.* equity\s+([0-9,]+)',
                    r'Total shareholders.* equity.*?([0-9,]+)',
                ],
                
                'total_debt': [
                    r'Total (?:debt|liabilities).*?([0-9,]+)',
                    r'Net debt.*?([0-9,]+)',
                    r'Total borrowings.*?([0-9,]+)',
                ],
                
                'cash_and_equivalents': [
                    r'Cash and cash equivalents\s+([0-9,]+)',
                    r'Cash and bank.*?([0-9,]+)',
                ],
            }
            
            for field, patterns in balance_sheet_patterns.items():
                for pattern in patterns:
                    matches = re.findall(pattern, balance_sheet_section, re.IGNORECASE)
                    if matches:
                        try:
                            value = float(matches[0].replace(',', ''))
                            # Apply scale conversion for balance sheet items
                            value = self._apply_scale_conversion(value, field, [balance_sheet_section])
                            if self._validate_metric_value(field, value):
                                data[field] = value
                                logger.info(f"Balance sheet: {field} = {value}")
                                break
                        except (ValueError, IndexError):
                            continue
        
        return data
    
    def _find_balance_sheet_section(self, text: str) -> Optional[str]:
        """Find and extract balance sheet section from document"""
        
        # Look for balance sheet indicators
        balance_sheet_markers = [
            'balance sheet',
            'statement of financial position',
            'condensed balance sheet',
            'financial position'
        ]
        
        for marker in balance_sheet_markers:
            idx = text.lower().find(marker)
            if idx != -1:
                # Extract section around the marker
                section_start = max(0, idx - 200)
                section_end = min(len(text), idx + 3000)
                section = text[section_start:section_end]
                
                # Look for financial data in this section
                if re.search(r'total assets.*?[0-9,]+', section, re.IGNORECASE):
                    return section
        
        return None
    
    async def _robust_llm_extraction(self, text: str) -> Dict[str, Any]:
        """Robust LLM extraction with better error handling"""
        
        # Find key sections for focused extraction
        focused_sections = self._find_financial_sections_improved(text)
        
        if not focused_sections:
            focused_sections = text[:4000] + text[-2000:]
        
        # Simplified prompt to avoid JSON parsing issues
        prompt = f"""Extract key financial metrics from this quarterly report.

Text: {focused_sections[:3000]}

Return simple JSON format:
{{
    "revenue": null,
    "operating_profit": null,
    "net_income": null,
    "total_assets": null,
    "total_equity": null,
    "eps": null
}}

Rules:
- Use current quarter values (Q2 2025) 
- Return values in actual SEK (not millions)
- Return null if not found
- NO comments or explanations"""

        try:
            response = await self.ollama.generate_text(
                prompt=prompt,
                system_prompt="Extract financial data. Return only valid JSON without comments.",
                temperature=0.0
            )
            
            # Robust JSON cleaning
            json_str = self._robust_json_cleaning(response)
            
            if json_str:
                llm_data = json.loads(json_str)
                
                # Map simplified field names to our field names
                field_mapping = {
                    'revenue': 'revenue_reported',
                    'operating_profit': 'operating_profit_reported',
                    'net_income': 'net_income_reported',
                    'total_assets': 'total_assets',
                    'total_equity': 'total_equity',
                    'eps': 'earnings_per_share_reported'
                }
                
                clean_data = {}
                for simple_name, value in llm_data.items():
                    if value is not None and simple_name in field_mapping:
                        field_name = field_mapping[simple_name]
                        if isinstance(value, (int, float)) and value != 0:
                            if self._validate_metric_value(field_name, value):
                                clean_data[field_name] = value
                
                logger.info(f"LLM extracted: {len(clean_data)} fields")
                return clean_data
            
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
        
        return {}
    
    def _robust_json_cleaning(self, response: str) -> Optional[str]:
        """Robust JSON cleaning with multiple fallback strategies"""
        
        try:
            # Strategy 1: Direct JSON extraction
            response = response.strip()
            
            # Remove markdown
            response = re.sub(r'```(?:json)?\s*', '', response)
            
            # Extract JSON object
            start = response.find('{')
            end = response.rfind('}')
            
            if start != -1 and end != -1:
                json_str = response[start:end+1]
                
                # Remove comments and fix common issues
                json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
                json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
                json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                json_str = re.sub(r',\s*]', ']', json_str)
                
                # Test if it's valid JSON
                json.loads(json_str)
                return json_str
                
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract key-value pairs manually
        try:
            simple_data = {}
            for line in response.split('\n'):
                if ':' in line:
                    match = re.search(r'"?(\w+)"?\s*:\s*([0-9.]+|null)', line)
                    if match:
                        key, value = match.groups()
                        if value == 'null':
                            simple_data[key] = None
                        else:
                            simple_data[key] = float(value)
            
            if simple_data:
                return json.dumps(simple_data)
                
        except Exception:
            pass
        
        return None
    
    def _find_financial_sections_improved(self, text: str) -> str:
        """Find key financial sections with improved detection"""
        
        sections = []
        
        # Enhanced section markers
        section_markers = [
            ("income statement", 1500),
            ("profit and loss", 1500),
            ("cash flow", 1200),
            ("balance sheet", 1500),
            ("financial position", 1200),
            ("key figures", 800),
            ("financial highlights", 1000),
            ("summary", 800)
        ]
        
        for marker, length in section_markers:
            indices = [m.start() for m in re.finditer(marker, text, re.IGNORECASE)]
            for idx in indices[:2]:  # Take first 2 occurrences
                sections.append(text[idx:idx+length])
        
        return " ".join(sections[:4])  # Limit to avoid token limits
    
    def _validate_metric_value(self, field: str, value: float) -> bool:
        """Validate if a metric value is reasonable"""
        
        validation_rules = {
            'revenue_reported': (1000000, 1000000000000),  # 1M to 1T SEK (actual values)
            'operating_profit_reported': (10000, 500000000000),  # 10k to 500B SEK (actual values)
            'net_income_reported': (10000, 500000000000),
            'earnings_per_share_reported': (0.01, 1000),  # 1 öre to 1000 SEK (per share values)
            'total_assets': (1000000, 2000000000000),  # 1M to 2T SEK (actual values)
            'total_equity': (1000000, 2000000000000),
            'cash_and_equivalents': (100000, 500000000000),
            'operating_cash_flow': (-100000000000, 500000000000),  # Can be negative
            'investing_cash_flow': (-500000000000, 100000000000),  # Usually negative
            'financing_cash_flow': (-500000000000, 100000000000),  # Can be either
        }
        
        if field in validation_rules:
            min_val, max_val = validation_rules[field]
            return min_val <= abs(value) <= max_val
        
        return True  # Default to valid for unknown fields
    
    def _merge_extraction_data(self, *data_sources) -> Dict[str, Any]:
        """Merge data from multiple sources with conflict resolution"""
        
        merged = {}
        
        # Priority order: table > balance_sheet > pattern > llm
        for data_dict in reversed(data_sources):  # Reverse to give first source priority
            for key, value in data_dict.items():
                if value is not None:
                    if key not in merged:
                        merged[key] = value
                    elif key == 'revenue_reported' and isinstance(value, (int, float)) and isinstance(merged[key], (int, float)):
                        # For revenue, prefer larger values (scale conversion fixes typically make values larger)
                        if value > merged[key]:
                            merged[key] = value
        
        return merged
    
    def _apply_data_to_metrics(self, metrics: FinancialMetrics, data: Dict) -> FinancialMetrics:
        """Apply extracted data to metrics object with validation"""
        
        # Direct field mapping
        for field, value in data.items():
            if hasattr(metrics, field) and value is not None:
                setattr(metrics, field, value)
        
        # Set report period
        if metrics.report_date:
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
        
        # Calculate confidence based on key metrics found
        key_metrics = [
            'revenue_reported', 'operating_profit_reported', 
            'net_income_reported', 'earnings_per_share_reported',
            'total_assets', 'total_equity'
        ]
        
        found_key = sum(1 for field in key_metrics if getattr(metrics, field) is not None)
        total_key = len(key_metrics)
        
        metrics.extraction_confidence = min((found_key / total_key) * 0.95, 1.0)
        
        return metrics
    
    def _calculate_comprehensive_derived_metrics(self, metrics: FinancialMetrics, text: str, data: Dict) -> FinancialMetrics:
        """Calculate derived metrics with error handling"""
        
        try:
            # Calculate margins
            if metrics.revenue_reported:
                if metrics.operating_profit_reported:
                    metrics.operating_margin_pct = (metrics.operating_profit_reported / metrics.revenue_reported) * 100
                if metrics.net_income_reported:
                    metrics.net_margin_pct = (metrics.net_income_reported / metrics.revenue_reported) * 100
            
            # Calculate ratios
            if metrics.total_debt and metrics.total_equity:
                metrics.debt_to_equity = metrics.total_debt / metrics.total_equity
            
            if metrics.net_income_reported and metrics.total_equity:
                # Annualize for ROE
                annualized_income = metrics.net_income_reported * 2 if 'Q2' in metrics.report_period else metrics.net_income_reported * 4
                metrics.return_on_equity_pct = (annualized_income / metrics.total_equity) * 100
            
            if metrics.net_income_reported and metrics.total_assets:
                annualized_income = metrics.net_income_reported * 2 if 'Q2' in metrics.report_period else metrics.net_income_reported * 4
                metrics.return_on_assets_pct = (annualized_income / metrics.total_assets) * 100
        
        except Exception as e:
            logger.error(f"Error calculating derived metrics: {e}")
        
        return metrics
    
    def _extract_filename_metadata(self, metrics: FinancialMetrics, filename: str) -> FinancialMetrics:
        """Extract metadata from filename"""
        
        # Date from filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if date_match:
            try:
                metrics.report_date = datetime.strptime(date_match.group(1), "%Y-%m-%d").date()
                metrics.fiscal_year = metrics.report_date.year
            except:
                pass
        
        # Report type
        filename_lower = filename.lower()
        if any(term in filename_lower for term in ['interim', 'quarterly', 'q1', 'q2', 'q3', 'q4']):
            metrics.report_type = "quarterly"
        elif any(term in filename_lower for term in ['annual', 'full year']):
            metrics.report_type = "annual"
        
        return metrics
    
    def _validate_and_scale(self, metrics: FinancialMetrics, text: str) -> FinancialMetrics:
        """Validate and scale metrics with improved logic"""
        
        if not metrics.data_warnings:
            metrics.data_warnings = []
        
        # Check for million notation
        has_millions = "million" in text.lower() or "msek" in text.lower()
        
        if has_millions:
            # Smart scaling based on reasonable values
            scaling_fields = [
                ('revenue_reported', 10000, 1000000),
                ('operating_profit_reported', 1000, 1000000),
                ('net_income_reported', 1000, 1000000),
                ('total_assets', 10000, 1000000),
                ('total_equity', 10000, 1000000),
                ('operating_cash_flow', 1000, 1000000),
            ]
            
            for field_name, threshold, multiplier in scaling_fields:
                value = getattr(metrics, field_name)
                if value and value < threshold:
                    setattr(metrics, field_name, value * multiplier)
                    metrics.data_warnings.append(f"Applied million scaling to {field_name}")
        
        # Set metadata
        metrics.model_used = "Fixed adaptive extractor with robust error handling"
        metrics.extraction_notes = f"Multi-phase extraction with validation"
        
        return metrics


class FixedFinancialExtractionService:
    """Fixed service with UUID generation and better error handling"""
    
    def __init__(self):
        self.pdf_extractor = PDFTextExtractor()
        self.financial_extractor = FixedFinancialExtractor()
        from shared.config import settings
        self.db_url = settings.database_url
    
    async def extract_from_pdf(self, pdf_path: str, company_name: str) -> Optional[FinancialMetrics]:
        """Extract financial metrics from PDF with error handling"""
        
        logger.info(f"Fixed extraction from {pdf_path}")
        
        try:
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
            
        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            return None
    
    def _generate_document_uuid(self, company_name: str, report_period: str) -> str:
        """Generate a proper UUID for database storage"""
        
        # Create a deterministic UUID based on company and period
        namespace = uuid.UUID('12345678-1234-5678-1234-123456789012')
        name = f"{company_name}-{report_period}".lower()
        return str(uuid.uuid5(namespace, name))
    
    async def save_metrics(self, metrics: FinancialMetrics, document_id: Optional[str] = None) -> bool:
        """Save metrics to database with proper UUID handling"""
        
        try:
            # Generate proper UUID if needed
            if not document_id or len(document_id) < 32:
                document_id = self._generate_document_uuid(metrics.company_name, metrics.report_period)
            
            conn = await asyncpg.connect(self.db_url)
            
            # Convert to dict
            data = asdict(metrics)
            
            # Insert with ALL 80 columns (excluding auto-generated 'id')
            await conn.execute("""
                INSERT INTO financial_metrics (
                    document_id, company_name, report_period, report_type, fiscal_year, report_date,
                    revenue_reported, revenue_adjusted, revenue_adjustments, revenue_currency, revenue_growth_pct, revenue_growth_qoq_pct,
                    gross_profit, gross_margin_pct, cost_of_goods_sold, operating_expenses,
                    operating_profit_reported, operating_profit_adjusted, operating_adjustments, operating_margin_pct,
                    ebitda_reported, ebitda_adjusted, ebitda_adjustments, ebitda_margin_pct,
                    depreciation_amortization, interest_expense, tax_expense, other_income,
                    net_income_reported, net_income_adjusted, net_income_adjustments, net_margin_pct,
                    operating_cash_flow, investing_cash_flow, financing_cash_flow, free_cash_flow, capex, dividends_paid,
                    total_assets, current_assets, non_current_assets, total_equity, retained_earnings,
                    total_liabilities, current_liabilities, non_current_liabilities, total_debt, cash_and_equivalents,
                    inventory, accounts_receivable, accounts_payable, working_capital,
                    debt_to_equity, current_ratio, quick_ratio, inventory_turnover, asset_turnover, interest_coverage,
                    return_on_equity_pct, return_on_assets_pct,
                    earnings_per_share_reported, earnings_per_share_adjusted, eps_adjustments, book_value_per_share,
                    dividend_per_share, shares_outstanding, payout_ratio, dividend_yield_pct,
                    operational_metrics, extraction_method, extraction_confidence, extraction_date, model_used,
                    has_revenue, has_profitability, has_cash_flow, has_balance_sheet, data_quality_score,
                    extraction_notes, data_warnings
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
                    $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
                    $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66,
                    $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, $78, $79, $80
                )
                ON CONFLICT (document_id, report_period) 
                DO UPDATE SET
                    extraction_confidence = EXCLUDED.extraction_confidence,
                    extraction_date = CURRENT_TIMESTAMP,
                    revenue_reported = EXCLUDED.revenue_reported,
                    operating_profit_reported = EXCLUDED.operating_profit_reported,
                    net_income_reported = EXCLUDED.net_income_reported,
                    total_assets = EXCLUDED.total_assets,
                    total_equity = EXCLUDED.total_equity
            """, 
                document_id, data['company_name'], data['report_period'], data['report_type'],
                data['fiscal_year'], data['report_date'],
                data['revenue_reported'], data['revenue_adjusted'], data['revenue_adjustments'],
                data['revenue_currency'], data['revenue_growth_pct'], data.get('revenue_growth_qoq_pct'),
                data['gross_profit'], data['gross_margin_pct'], data.get('cost_of_goods_sold'), data.get('operating_expenses'),
                data['operating_profit_reported'], data['operating_profit_adjusted'],
                data['operating_adjustments'], data['operating_margin_pct'],
                data['ebitda_reported'], data['ebitda_adjusted'], data['ebitda_adjustments'],
                data['ebitda_margin_pct'],
                data.get('depreciation_amortization'), data.get('interest_expense'), data.get('tax_expense'), data.get('other_income'),
                data['net_income_reported'], data['net_income_adjusted'], data['net_income_adjustments'],
                data['net_margin_pct'],
                data['operating_cash_flow'], data['investing_cash_flow'], data['financing_cash_flow'],
                data['free_cash_flow'], data['capex'], data.get('dividends_paid'),
                data['total_assets'], data.get('current_assets'), data.get('non_current_assets'), 
                data['total_equity'], data.get('retained_earnings'),
                data.get('total_liabilities'), data.get('current_liabilities'), data.get('non_current_liabilities'), 
                data['total_debt'], data['cash_and_equivalents'],
                data.get('inventory'), data.get('accounts_receivable'), data.get('accounts_payable'), data.get('working_capital'),
                data['debt_to_equity'], data['current_ratio'], data.get('quick_ratio'), 
                data.get('inventory_turnover'), data.get('asset_turnover'), data.get('interest_coverage'),
                data['return_on_equity_pct'], data['return_on_assets_pct'],
                data['earnings_per_share_reported'], data['earnings_per_share_adjusted'],
                data['eps_adjustments'], data.get('book_value_per_share'),
                data.get('dividend_per_share'), data['shares_outstanding'], 
                data.get('payout_ratio'), data.get('dividend_yield_pct'),
                json.dumps(data['operational_metrics'] or {}), data.get('extraction_method', 'local_llm'), 
                data['extraction_confidence'], None,  # extraction_date (uses DEFAULT CURRENT_TIMESTAMP)
                data['model_used'],
                data.get('has_revenue', data['revenue_reported'] is not None),
                data.get('has_profitability', data['operating_profit_reported'] is not None),
                data.get('has_cash_flow', data['operating_cash_flow'] is not None),
                data.get('has_balance_sheet', data['total_assets'] is not None),
                data.get('data_quality_score', data['extraction_confidence']),
                data['extraction_notes'], data.get('data_warnings', [])
            )
            
            await conn.close()
            logger.info(f"Saved metrics for {metrics.company_name} - {metrics.report_period} (ID: {document_id})")
            return True
            
        except Exception as e:
            logger.error(f"Database save failed: {e}")
            return False


# Test function
async def test_fixed_extraction():
    """Test the fixed extraction service"""
    
    print("🧪 TESTING FIXED EXTRACTION SERVICE")
    print("=" * 60)
    
    service = FixedFinancialExtractionService()
    
    # Test with AAK first
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    metrics = await service.extract_from_pdf(pdf_path, "AAK")
    
    if metrics:
        print(f"✅ AAK extraction: {metrics.extraction_confidence:.1%} confidence")
        
        # Test save with existing document UUID
        existing_doc_id = "987f81e1-dc8c-4aab-9a06-2b254599cd60"  # Known AAK document from nordic_documents
        success = await service.save_metrics(metrics, existing_doc_id)
        print(f"💾 Database save: {'✅ Success' if success else '❌ Failed'}")
    
    return service


if __name__ == "__main__":
    asyncio.run(test_fixed_extraction())