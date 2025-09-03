#!/usr/bin/env python3
"""
Enhanced Financial Extraction Service - Improved Success Rate
Building on the fixed service with advanced pattern matching and validation
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


class EnhancedFinancialExtractor:
    """Enhanced extraction with improved success rate targeting 97-99%"""
    
    def __init__(self):
        if not OLLAMA_AVAILABLE:
            raise Exception("Ollama service not available")
        self.ollama = OllamaService(default_model="llama3:latest")
        
        # Enhanced Swedish financial terminology
        self.swedish_terms = {
            'revenue': [
                'nettoomsättning', 'omsättning', 'intäkter', 'försäljningsintäkter', 
                'nettointäkter', 'totala intäkter', 'rörelsens intäkter'
            ],
            'operating_profit': [
                'rörelseresultat', 'rörelsevinst', 'rörelsens resultat',
                'resultat före finansnetto', 'operativt resultat'
            ],
            'net_income': [
                'nettovinst', 'nettoresultat', 'årets resultat', 'periodens resultat',
                'resultat efter skatt', 'nettoinkomst'
            ],
            'ebitda': [
                'ebitda', 'resultat före avskrivningar', 'rörelseresultat före avskrivningar'
            ],
            'cash_flow': [
                'kassaflöde', 'kassaflöden', 'kassa', 'likvida medel',
                'kassaflöde från löpande verksamhet'
            ],
            'assets': [
                'tillgångar', 'anläggningstillgångar', 'omsättningstillgångar',
                'summa tillgångar', 'totala tillgångar'
            ],
            'equity': [
                'eget kapital', 'aktieägarnas kapital', 'moderbolagets ägare',
                'totalt eget kapital', 'summa eget kapital'
            ],
            'debt': [
                'skulder', 'lån', 'räntebärande skulder', 'nettoskuld',
                'totala skulder', 'summa skulder'
            ],
            'shares': [
                'aktier', 'antalet aktier', 'utestående aktier', 'aktier utestående'
            ]
        }
    
    async def extract_metrics(
        self, 
        document_text: str, 
        company_name: str,
        document_path: str = ""
    ) -> FinancialMetrics:
        """Enhanced extraction with improved success rate"""
        
        # Initialize metrics
        metrics = FinancialMetrics(
            company_name=company_name,
            report_period="unknown"
        )
        
        # Extract from filename
        if document_path:
            metrics = self._extract_filename_metadata(metrics, Path(document_path).name)
        
        # Multi-phase enhanced extraction
        try:
            # Phase 1: Advanced text preprocessing
            preprocessed_text = self._advanced_preprocessing(document_text)
            
            # Phase 2: Comprehensive Swedish/English pattern matching
            pattern_data = self._comprehensive_pattern_extraction(preprocessed_text)
            
            # Phase 3: Advanced table structure detection
            table_data = self._advanced_table_extraction(preprocessed_text)
            
            # Phase 4: Enhanced balance sheet extraction
            balance_sheet_data = self._enhanced_balance_sheet_extraction(preprocessed_text)
            
            # Phase 5: Optimized multi-pass LLM extraction
            llm_data = await self._multi_pass_llm_extraction(preprocessed_text)
            
            # Phase 6: Intelligent data merging with conflict resolution
            all_data = self._intelligent_merge_data(
                pattern_data, table_data, balance_sheet_data, llm_data, preprocessed_text
            )
            
            # Phase 7: Apply to metrics with validation
            metrics = self._apply_validated_data_to_metrics(metrics, all_data)
            
            # Phase 8: Enhanced derived metrics calculation
            metrics = self._calculate_enhanced_derived_metrics(metrics, preprocessed_text, all_data)
            
            # Phase 9: Advanced validation and confidence scoring
            metrics = self._advanced_validation_and_scoring(metrics, document_text, all_data)
            
        except Exception as e:
            logger.error(f"Enhanced extraction error: {e}")
            metrics.extraction_confidence = 0.1
        
        return metrics
    
    def _advanced_preprocessing(self, text: str) -> str:
        """Advanced text preprocessing for better extraction"""
        
        # Normalize Swedish characters and financial terms
        text = self._normalize_swedish_text(text)
        
        # Standardize number formats
        text = self._standardize_numbers(text)
        
        # Clean table structures
        text = self._clean_table_structures(text)
        
        # Enhance section markers
        text = self._enhance_section_markers(text)
        
        return text
    
    def _normalize_swedish_text(self, text: str) -> str:
        """Normalize Swedish text for better pattern matching"""
        
        # Common Swedish abbreviations
        swedish_normalizations = {
            'TSEK': 'thousand SEK',
            'MSEK': 'million SEK',
            'Mkr': 'million SEK',
            'Tkr': 'thousand SEK',
            'mnkr': 'million SEK',
            'ksek': 'thousand SEK',
            'msek': 'million SEK'
        }
        
        for abbrev, full in swedish_normalizations.items():
            text = re.sub(rf'\b{abbrev}\b', full, text, flags=re.IGNORECASE)
        
        return text
    
    def _standardize_numbers(self, text: str) -> str:
        """Standardize number formats for consistent extraction"""
        
        # Replace European decimal separators
        text = re.sub(r'(\d+),(\d{3})\b', r'\1\2', text)  # 1,234 -> 1234
        text = re.sub(r'(\d+) (\d{3})\b', r'\1\2', text)  # 1 234 -> 1234
        
        # Handle decimal points in Swedish format  
        text = re.sub(r'(\d+),(\d{1,2})\b(?!\d)', r'\1.\2', text)  # 1,25 -> 1.25
        
        return text
    
    def _clean_table_structures(self, text: str) -> str:
        """Clean and enhance table structures for better parsing"""
        
        # Normalize table separators
        text = re.sub(r'\s{3,}', ' | ', text)  # Multiple spaces -> pipe separator
        
        # Enhance table headers
        financial_headers = ['Q1', 'Q2', 'Q3', 'Q4', '2024', '2025', 'Jan-Jun', 'Jan-Mar']
        for header in financial_headers:
            text = re.sub(rf'\b{header}\b', f' {header} ', text)
        
        return text
    
    def _enhance_section_markers(self, text: str) -> str:
        """Enhance section markers for better section detection"""
        
        # Add clear markers around key sections
        section_patterns = {
            'INCOME_STATEMENT_START': [
                'income statement', 'profit and loss', 'resultaträkning', 
                'consolidated income statement'
            ],
            'BALANCE_SHEET_START': [
                'balance sheet', 'financial position', 'balansräkning',
                'consolidated balance sheet'
            ],
            'CASH_FLOW_START': [
                'cash flow', 'kassaflöde', 'cash flow statement'
            ]
        }
        
        for marker, patterns in section_patterns.items():
            for pattern in patterns:
                text = re.sub(
                    rf'({pattern})', 
                    f' {marker} \\1 ', 
                    text, 
                    flags=re.IGNORECASE
                )
        
        return text
    
    def _comprehensive_pattern_extraction(self, text: str) -> Dict[str, Any]:
        """Comprehensive pattern extraction with Swedish and English support"""
        
        data = {}
        
        # Build comprehensive bilingual patterns
        comprehensive_patterns = self._build_comprehensive_patterns()
        
        # Apply all patterns with smart conflict resolution
        for field, pattern_groups in comprehensive_patterns.items():
            best_value = None
            confidence_score = 0
            
            for priority, patterns in enumerate(pattern_groups):
                for pattern in patterns:
                    try:
                        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
                        if matches:
                            for match in matches[:3]:  # Check up to 3 matches
                                value_str = match if isinstance(match, str) else match[0] if isinstance(match, tuple) else str(match)
                                
                                # Clean and validate
                                cleaned_value = self._clean_numeric_value(value_str)
                                if cleaned_value and self._validate_metric_value(field, cleaned_value):
                                    # Score based on pattern priority and context
                                    pattern_confidence = self._score_pattern_match(
                                        pattern, match, text, priority
                                    )
                                    
                                    if pattern_confidence > confidence_score:
                                        best_value = cleaned_value
                                        confidence_score = pattern_confidence
                    except Exception as e:
                        continue
            
            if best_value:
                data[field] = best_value
                logger.info(f"Enhanced pattern: {field} = {best_value} (confidence: {confidence_score:.2f})")
        
        return data
    
    def _build_comprehensive_patterns(self) -> Dict[str, List[List[str]]]:
        """Build comprehensive bilingual patterns with priority ordering"""
        
        patterns = {}
        
        # Revenue patterns (highest priority first)
        patterns['revenue_reported'] = [
            # High priority: Table-based patterns
            [
                r'(?:Net sales|Nettoomsättning|Revenue|Omsättning)[\s\|]*([0-9,]+)[\s\|]*[0-9,]+',
                r'(?:Total revenue|Totala intäkter)[\s]*([0-9,]+)'
            ],
            # Medium priority: Text-based patterns  
            [
                r'(?:Net sales|Revenue|Nettoomsättning).*?([0-9,]+)(?:\s*million|\s*MSEK|\s*miljoner)',
                r'Sales reached.*?([0-9,]+)(?:\s*million|\s*MSEK)'
            ],
            # Lower priority: Context-dependent patterns
            [
                r'(?:Net sales|Revenue)[\s:]*([0-9,]+)',
                r'(?:Nettoomsättning|Omsättning)[\s:]*([0-9,]+)'
            ]
        ]
        
        # Operating profit patterns
        patterns['operating_profit_reported'] = [
            # High priority
            [
                r'(?:Operating profit|Rörelseresultat)[\s\|]*([0-9,-]+)[\s\|]*[0-9,-]+',
                r'(?:Operating income|EBIT)[\s\|]*([0-9,-]+)'
            ],
            # Medium priority
            [
                r'(?:Operating profit|Rörelseresultat).*?([0-9,-]+)(?:\s*million|\s*MSEK)',
                r'(?:Operating result|Rörelsens resultat).*?([0-9,-]+)'
            ],
            # Lower priority
            [
                r'(?:Operating profit|EBIT)[\s:]*([0-9,-]+)',
                r'(?:Rörelseresultat|Rörelsevinst)[\s:]*([0-9,-]+)'
            ]
        ]
        
        # Net income patterns
        patterns['net_income_reported'] = [
            # High priority
            [
                r'(?:Net income|Nettoresultat|Årets resultat)[\s\|]*([0-9,-]+)[\s\|]*[0-9,-]+',
                r'(?:Profit for the period|Periodens resultat)[\s\|]*([0-9,-]+)'
            ],
            # Medium priority
            [
                r'(?:Net income|Profit for the period).*?([0-9,-]+)(?:\s*million|\s*MSEK)',
                r'(?:Nettoresultat|Årets resultat).*?([0-9,-]+)(?:\s*miljoner|\s*MSEK)'
            ],
            # Lower priority
            [
                r'(?:Result for the period|Net result)[\s:]*([0-9,-]+)',
                r'(?:Periodens resultat|Nettoresultat)[\s:]*([0-9,-]+)'
            ]
        ]
        
        # Add more comprehensive patterns for other fields...
        patterns.update(self._build_balance_sheet_patterns())
        patterns.update(self._build_cash_flow_patterns())
        patterns.update(self._build_per_share_patterns())
        
        return patterns
    
    def _build_balance_sheet_patterns(self) -> Dict[str, List[List[str]]]:
        """Build comprehensive balance sheet patterns"""
        
        return {
            'total_assets': [
                [
                    r'(?:Total assets|Summa tillgångar)[\s\|]*([0-9,]+)[\s\|]*[0-9,]+',
                    r'(?:Assets total|Totala tillgångar)[\s\|]*([0-9,]+)'
                ],
                [
                    r'(?:Total assets|Summa tillgångar).*?([0-9,]+)(?:\s*million|\s*MSEK)',
                    r'Assets total.*?([0-9,]+)'
                ]
            ],
            'total_equity': [
                [
                    r'(?:Total equity|Totalt eget kapital|Eget kapital)[\s\|]*([0-9,]+)[\s\|]*[0-9,]+',
                    r'(?:Shareholders.* equity|Aktieägarnas kapital)[\s\|]*([0-9,]+)'
                ],
                [
                    r'(?:Total equity|Eget kapital).*?([0-9,]+)(?:\s*million|\s*MSEK)',
                    r'(?:Shareholders.* equity).*?([0-9,]+)'
                ]
            ],
            'cash_and_equivalents': [
                [
                    r'(?:Cash and cash equivalents|Kassa och bank)[\s\|]*([0-9,]+)[\s\|]*[0-9,]+',
                    r'(?:Cash and bank|Likvida medel)[\s\|]*([0-9,]+)'
                ]
            ]
        }
    
    def _build_cash_flow_patterns(self) -> Dict[str, List[List[str]]]:
        """Build comprehensive cash flow patterns"""
        
        return {
            'operating_cash_flow': [
                [
                    r'(?:Operating cash flow|Kassaflöde från löpande verksamhet)[\s\|]*([0-9,-]+)[\s\|]*[0-9,-]+',
                    r'(?:Cash flow from operating activities)[\s\|]*([0-9,-]+)'
                ]
            ],
            'investing_cash_flow': [
                [
                    r'(?:Investing cash flow|Kassaflöde från investeringsverksamhet)[\s\|]*([0-9,-]+)[\s\|]*[0-9,-]+',
                    r'(?:Cash flow from investing activities)[\s\|]*([0-9,-]+)'
                ]
            ],
            'financing_cash_flow': [
                [
                    r'(?:Financing cash flow|Kassaflöde från finansieringsverksamhet)[\s\|]*([0-9,-]+)[\s\|]*[0-9,-]+',
                    r'(?:Cash flow from financing activities)[\s\|]*([0-9,-]+)'
                ]
            ]
        }
    
    def _build_per_share_patterns(self) -> Dict[str, List[List[str]]]:
        """Build comprehensive per-share metric patterns"""
        
        return {
            'earnings_per_share_reported': [
                [
                    r'(?:Earnings per share|Resultat per aktie|EPS)[\s\|]*([0-9.,-]+)[\s\|]*[0-9.,-]+',
                    r'(?:Basic earnings per share)[\s\|]*([0-9.,-]+)'
                ],
                [
                    r'(?:Earnings per share|EPS).*?([0-9.,-]+)(?:\s*SEK|\s*kronor)',
                    r'(?:Resultat per aktie).*?([0-9.,-]+)'
                ]
            ],
            'shares_outstanding': [
                [
                    r'(?:Shares outstanding|Utestående aktier|Antal aktier)[\s\|]*([0-9,]+)(?:\s*million|\s*miljoner)?',
                    r'(?:Outstanding shares|Aktier utestående)[\s\|]*([0-9,]+)'
                ]
            ]
        }
    
    def _clean_numeric_value(self, value_str: str) -> Optional[float]:
        """Clean and parse numeric values with Swedish format support"""
        
        if not value_str:
            return None
            
        # Remove non-numeric characters except . , - and spaces
        cleaned = re.sub(r'[^\d.,-\s]', '', value_str).strip()
        
        if not cleaned or cleaned in ['-', '.', ',']:
            return None
        
        try:
            # Handle negative values
            is_negative = cleaned.startswith('-')
            if is_negative:
                cleaned = cleaned[1:]
            
            # Handle Swedish decimal format (1,25 = 1.25)
            if ',' in cleaned and '.' not in cleaned:
                # Check if comma is decimal separator (not thousands)
                comma_parts = cleaned.split(',')
                if len(comma_parts) == 2 and len(comma_parts[1]) <= 2:
                    cleaned = f"{comma_parts[0]}.{comma_parts[1]}"
                else:
                    cleaned = cleaned.replace(',', '')
            else:
                # Remove thousands separators
                cleaned = cleaned.replace(',', '').replace(' ', '')
            
            value = float(cleaned)
            return -value if is_negative else value
            
        except ValueError:
            return None
    
    def _score_pattern_match(self, pattern: str, match: Any, text: str, priority: int) -> float:
        """Score pattern matches based on context and reliability"""
        
        base_score = 1.0 - (priority * 0.1)  # Higher priority = higher base score
        
        # Bonus for table context
        if '|' in str(match) or 'table' in text.lower():
            base_score += 0.2
        
        # Bonus for clear section context
        section_keywords = ['income statement', 'balance sheet', 'cash flow', 'financial highlights']
        if any(keyword in text.lower() for keyword in section_keywords):
            base_score += 0.1
        
        # Penalty for unclear context
        if len(str(match)) < 2:
            base_score -= 0.3
        
        return min(base_score, 1.0)
    
    def _advanced_table_extraction(self, text: str) -> Dict[str, Any]:
        """Advanced table structure detection and extraction"""
        
        data = {}
        
        # Detect different table formats
        table_formats = self._detect_table_formats(text)
        
        for table_format in table_formats:
            table_data = self._extract_from_table_format(text, table_format)
            data.update(table_data)
        
        return data
    
    def _detect_table_formats(self, text: str) -> List[str]:
        """Detect various table formats in the document"""
        
        formats = []
        
        # Format 1: Pipe-separated tables
        if text.count('|') > 10:
            formats.append('pipe_separated')
        
        # Format 2: Quarter comparison tables (Q2 2025 | Q2 2024)
        if re.search(r'Q[1-4]\s+2025.*?Q[1-4]\s+2024', text, re.IGNORECASE):
            formats.append('quarter_comparison')
        
        # Format 3: Year comparison tables (2025 | 2024)
        if re.search(r'2025.*?2024', text):
            formats.append('year_comparison')
        
        # Format 4: Swedish formatted tables with MSEK
        if 'MSEK' in text or 'million SEK' in text:
            formats.append('swedish_msek')
        
        return formats if formats else ['generic']
    
    def _extract_from_table_format(self, text: str, table_format: str) -> Dict[str, Any]:
        """Extract data based on detected table format"""
        
        if table_format == 'quarter_comparison':
            return self._extract_quarter_comparison_table(text)
        elif table_format == 'pipe_separated':
            return self._extract_pipe_separated_table(text)
        elif table_format == 'swedish_msek':
            return self._extract_swedish_msek_table(text)
        else:
            return self._extract_generic_table(text)
    
    def _extract_generic_table(self, text: str) -> Dict[str, Any]:
        """Extract from generic table format"""
        data = {}
        
        # Simple generic extraction based on common patterns
        lines = text.split('\n')
        for line in lines:
            if any(keyword in line.lower() for keyword in ['revenue', 'sales', 'omsättning']):
                match = re.search(r'([0-9,]+)', line)
                if match:
                    value = self._clean_numeric_value(match.group(1))
                    if value and self._validate_metric_value('revenue_reported', value):
                        data['revenue_reported'] = value
                        break
        
        return data
    
    def _extract_swedish_msek_table(self, text: str) -> Dict[str, Any]:
        """Extract from Swedish MSEK formatted tables"""
        data = {}
        
        # Look for MSEK context and extract values
        msek_sections = re.findall(r'(.*?MSEK.*?)(?=\n\n|\n[A-Z]|$)', text, re.IGNORECASE | re.DOTALL)
        
        for section in msek_sections:
            # Revenue patterns
            revenue_match = re.search(r'(?:Net sales|Nettoomsättning|Revenue)\s*([0-9,]+)', section, re.IGNORECASE)
            if revenue_match:
                value = self._clean_numeric_value(revenue_match.group(1))
                if value and self._validate_metric_value('revenue_reported', value):
                    data['revenue_reported'] = value
            
            # Operating profit patterns
            op_match = re.search(r'(?:Operating profit|Rörelseresultat)\s*([0-9,-]+)', section, re.IGNORECASE)
            if op_match:
                value = self._clean_numeric_value(op_match.group(1))
                if value and self._validate_metric_value('operating_profit_reported', value):
                    data['operating_profit_reported'] = value
        
        return data
    
    def _extract_quarter_comparison_table(self, text: str) -> Dict[str, Any]:
        """Extract from quarter comparison tables (Q2 2025 vs Q2 2024)"""
        
        data = {}
        
        # Find table sections with quarter headers
        quarter_sections = re.findall(
            r'(.*?)Q[1-4]\s+2025\s*[|\s]*Q[1-4]\s+2024(.*?)(?=\n\n|\n[A-Z]|$)',
            text, 
            re.IGNORECASE | re.DOTALL
        )
        
        for section in quarter_sections:
            table_content = ' '.join(section)
            
            # Extract specific metrics from quarter comparison
            metrics_patterns = {
                'revenue_reported': r'(?:Net sales|Revenue|Nettoomsättning)\s*([0-9,]+)\s*[0-9,]+',
                'operating_profit_reported': r'(?:Operating profit|Rörelseresultat)\s*([0-9,-]+)\s*[0-9,-]+',
                'net_income_reported': r'(?:Net income|Nettoresultat)\s*([0-9,-]+)\s*[0-9,-]+'
            }
            
            for field, pattern in metrics_patterns.items():
                match = re.search(pattern, table_content, re.IGNORECASE)
                if match:
                    value = self._clean_numeric_value(match.group(1))
                    if value and self._validate_metric_value(field, value):
                        data[field] = value
                        logger.info(f"Quarter table: {field} = {value}")
        
        return data
    
    def _extract_pipe_separated_table(self, text: str) -> Dict[str, Any]:
        """Extract from pipe-separated tables"""
        
        data = {}
        
        # Find table rows with pipes
        table_rows = [line for line in text.split('\n') if line.count('|') >= 2]
        
        if not table_rows:
            return data
        
        # Try to identify header row
        header_row = None
        for row in table_rows[:5]:  # Check first 5 rows for headers
            if any(term in row.lower() for term in ['2025', '2024', 'q1', 'q2', 'msek']):
                header_row = row
                break
        
        # Extract metrics from data rows
        for row in table_rows:
            if row == header_row:
                continue
                
            cells = [cell.strip() for cell in row.split('|')]
            if len(cells) >= 3:
                metric_name = cells[0].strip().lower()
                current_value = self._clean_numeric_value(cells[1])
                
                # Map metric names to our fields
                field_mapping = self._get_field_mapping()
                
                for our_field, possible_names in field_mapping.items():
                    if any(name in metric_name for name in possible_names):
                        if current_value and self._validate_metric_value(our_field, current_value):
                            data[our_field] = current_value
                            logger.info(f"Pipe table: {our_field} = {current_value}")
                        break
        
        return data
    
    def _get_field_mapping(self) -> Dict[str, List[str]]:
        """Map various metric names to our standard field names"""
        
        return {
            'revenue_reported': ['net sales', 'revenue', 'nettoomsättning', 'omsättning', 'sales'],
            'operating_profit_reported': ['operating profit', 'operating income', 'rörelseresultat', 'ebit'],
            'net_income_reported': ['net income', 'profit for the period', 'nettoresultat', 'årets resultat'],
            'total_assets': ['total assets', 'summa tillgångar', 'assets total'],
            'total_equity': ['total equity', 'eget kapital', 'shareholders equity'],
            'operating_cash_flow': ['operating cash flow', 'kassaflöde från löpande'],
            'earnings_per_share_reported': ['earnings per share', 'eps', 'resultat per aktie']
        }
    
    def _enhanced_balance_sheet_extraction(self, text: str) -> Dict[str, Any]:
        """Enhanced balance sheet extraction with improved section detection"""
        
        data = {}
        
        # Find enhanced balance sheet section
        balance_sheet_section = self._find_enhanced_balance_sheet_section(text)
        
        if balance_sheet_section:
            # Apply comprehensive balance sheet patterns
            balance_patterns = self._get_enhanced_balance_sheet_patterns()
            
            for field, pattern_groups in balance_patterns.items():
                best_value = None
                
                for patterns in pattern_groups:
                    for pattern in patterns:
                        matches = re.findall(pattern, balance_sheet_section, re.IGNORECASE)
                        if matches:
                            value = self._clean_numeric_value(matches[0])
                            if value and self._validate_metric_value(field, value):
                                if not best_value or abs(value) > abs(best_value):  # Prefer larger absolute values
                                    best_value = value
                
                if best_value:
                    data[field] = best_value
                    logger.info(f"Enhanced balance sheet: {field} = {best_value}")
        
        return data
    
    def _find_enhanced_balance_sheet_section(self, text: str) -> Optional[str]:
        """Find balance sheet section with improved detection"""
        
        # Enhanced balance sheet markers
        markers = [
            'BALANCE_SHEET_START',  # From preprocessing
            'balance sheet', 'balansräkning', 'statement of financial position',
            'condensed balance sheet', 'financial position', 'consolidated balance sheet'
        ]
        
        best_section = None
        best_score = 0
        
        for marker in markers:
            indices = [m.start() for m in re.finditer(marker, text, re.IGNORECASE)]
            
            for idx in indices:
                # Extract larger section around marker
                section_start = max(0, idx - 500)
                section_end = min(len(text), idx + 5000)
                section = text[section_start:section_end]
                
                # Score section by financial content
                score = self._score_balance_sheet_section(section)
                
                if score > best_score:
                    best_section = section
                    best_score = score
        
        return best_section
    
    def _score_balance_sheet_section(self, section: str) -> float:
        """Score balance sheet section by content quality"""
        
        score = 0
        
        # Count balance sheet keywords
        bs_keywords = [
            'total assets', 'summa tillgångar', 'total equity', 'eget kapital',
            'current assets', 'non-current assets', 'liabilities', 'skulder'
        ]
        
        for keyword in bs_keywords:
            score += len(re.findall(keyword, section, re.IGNORECASE)) * 2
        
        # Count numbers (indicates data)
        score += len(re.findall(r'\b\d{3,}\b', section)) * 0.5
        
        # Bonus for table structure
        if section.count('|') > 5:
            score += 5
        
        return score
    
    def _get_enhanced_balance_sheet_patterns(self) -> Dict[str, List[List[str]]]:
        """Enhanced balance sheet patterns with priority"""
        
        return {
            'total_assets': [
                [
                    r'(?:Total assets|Summa tillgångar)[\s\|:]*([0-9,]+)(?:\s*[0-9,]+)?',
                    r'(?:Assets total|Totala tillgångar)[\s\|:]*([0-9,]+)'
                ],
                [
                    r'Sum assets[\s\|:]*([0-9,]+)',
                    r'Total.*assets.*?([0-9,]+)'
                ]
            ],
            'total_equity': [
                [
                    r'(?:Total equity|Totalt eget kapital|Eget kapital total)[\s\|:]*([0-9,]+)(?:\s*[0-9,]+)?',
                    r'(?:Shareholders.* equity total)[\s\|:]*([0-9,]+)'
                ],
                [
                    r'(?:Total shareholders.* equity)[\s\|:]*([0-9,]+)',
                    r'Sum.*equity.*?([0-9,]+)'
                ]
            ],
            'total_debt': [
                [
                    r'(?:Total debt|Total borrowings|Totala skulder)[\s\|:]*([0-9,]+)',
                    r'(?:Net debt|Interest-bearing liabilities)[\s\|:]*([0-9,]+)'
                ]
            ]
        }
    
    async def _multi_pass_llm_extraction(self, text: str) -> Dict[str, Any]:
        """Multi-pass LLM extraction with specialized prompts"""
        
        # Pass 1: Basic metrics extraction
        basic_metrics = await self._extract_basic_llm_metrics(text)
        
        # Pass 2: Balance sheet specific extraction
        balance_metrics = await self._extract_balance_sheet_llm_metrics(text)
        
        # Pass 3: Ratios and derived metrics
        ratio_metrics = await self._extract_ratio_llm_metrics(text)
        
        # Combine results
        combined_metrics = {**basic_metrics, **balance_metrics, **ratio_metrics}
        
        logger.info(f"Multi-pass LLM extracted: {len(combined_metrics)} total fields")
        return combined_metrics
    
    async def _extract_basic_llm_metrics(self, text: str) -> Dict[str, Any]:
        """Extract basic P&L metrics using LLM"""
        
        focused_text = self._find_income_statement_section(text)
        if not focused_text:
            focused_text = text[:4000]
        
        prompt = f"""Extract Q2 2025 financial metrics from this Swedish/English quarterly report.

Text: {focused_text[:3000]}

Return JSON with these exact fields (use null if not found):
{{
    "revenue": null,
    "operating_profit": null,
    "net_income": null,
    "eps": null
}}

Rules:
- Use Q2 2025 values only
- Values in millions SEK (ignore 'million' unit in text)
- Return null if uncertain
- NO comments or explanations
- Return only valid JSON"""

        try:
            response = await self.ollama.generate_text(
                prompt=prompt,
                system_prompt="You are a financial data extraction expert. Return only valid JSON.",
                temperature=0.0
            )
            
            json_str = self._robust_json_cleaning(response)
            if json_str:
                llm_data = json.loads(json_str)
                
                return {
                    'revenue_reported': llm_data.get('revenue'),
                    'operating_profit_reported': llm_data.get('operating_profit'),
                    'net_income_reported': llm_data.get('net_income'),
                    'earnings_per_share_reported': llm_data.get('eps')
                }
        
        except Exception as e:
            logger.error(f"Basic LLM extraction failed: {e}")
        
        return {}
    
    async def _extract_balance_sheet_llm_metrics(self, text: str) -> Dict[str, Any]:
        """Extract balance sheet metrics using specialized LLM prompt"""
        
        balance_text = self._find_enhanced_balance_sheet_section(text)
        if not balance_text:
            return {}
        
        prompt = f"""Extract balance sheet data from this text.

Text: {balance_text[:2000]}

Return JSON:
{{
    "total_assets": null,
    "total_equity": null,
    "cash_and_equivalents": null,
    "total_debt": null
}}

Rules:
- Use most recent period values
- Values in millions SEK
- Return null if not found
- NO comments"""

        try:
            response = await self.ollama.generate_text(
                prompt=prompt,
                system_prompt="Extract balance sheet data. Return only JSON.",
                temperature=0.0
            )
            
            json_str = self._robust_json_cleaning(response)
            if json_str:
                return json.loads(json_str)
        
        except Exception as e:
            logger.error(f"Balance sheet LLM extraction failed: {e}")
        
        return {}
    
    async def _extract_ratio_llm_metrics(self, text: str) -> Dict[str, Any]:
        """Extract ratios and per-share metrics using LLM"""
        
        # Find relevant sections
        per_share_text = ""
        for section in text.split('\n\n'):
            if any(term in section.lower() for term in ['per share', 'aktie', 'eps', 'shares outstanding']):
                per_share_text += section + " "
        
        if not per_share_text:
            return {}
        
        prompt = f"""Extract per-share data from this text.

Text: {per_share_text[:1500]}

Return JSON:
{{
    "earnings_per_share": null,
    "shares_outstanding": null
}}

Rules:
- Use Q2 2025 values
- EPS in SEK
- Shares in actual numbers (not millions)
- Return null if not found"""

        try:
            response = await self.ollama.generate_text(
                prompt=prompt,
                system_prompt="Extract per-share metrics. Return only JSON.",
                temperature=0.0
            )
            
            json_str = self._robust_json_cleaning(response)
            if json_str:
                llm_data = json.loads(json_str)
                return {
                    'earnings_per_share_reported': llm_data.get('earnings_per_share'),
                    'shares_outstanding': llm_data.get('shares_outstanding')
                }
        
        except Exception as e:
            logger.error(f"Ratio LLM extraction failed: {e}")
        
        return {}
    
    def _find_income_statement_section(self, text: str) -> Optional[str]:
        """Find income statement section"""
        
        markers = ['INCOME_STATEMENT_START', 'income statement', 'profit and loss', 'resultaträkning']
        
        for marker in markers:
            idx = text.lower().find(marker.lower())
            if idx != -1:
                return text[idx:idx+3000]
        
        return None
    
    def _intelligent_merge_data(self, *data_sources, text: str) -> Dict[str, Any]:
        """Intelligently merge data from multiple sources with advanced conflict resolution"""
        
        merged = {}
        confidence_scores = {}
        
        # Combine all data sources with scoring
        for i, data_dict in enumerate(data_sources):
            source_weight = [0.3, 0.4, 0.4, 0.5][i]  # Pattern, table, balance, LLM weights
            
            for key, value in data_dict.items():
                if value is not None:
                    current_confidence = confidence_scores.get(key, 0)
                    new_confidence = source_weight * self._calculate_value_confidence(key, value, text)
                    
                    if new_confidence > current_confidence:
                        merged[key] = value
                        confidence_scores[key] = new_confidence
        
        return merged
    
    def _calculate_value_confidence(self, field: str, value: Any, text: str) -> float:
        """Calculate confidence score for a specific value"""
        
        confidence = 0.8  # Base confidence
        
        # Business logic validation
        if field == 'revenue_reported' and isinstance(value, (int, float)):
            if 100 <= value <= 500000:  # Reasonable revenue range for Swedish companies
                confidence += 0.1
        
        # Context validation
        if f"{field.replace('_', ' ')}" in text.lower():
            confidence += 0.05
        
        # Value consistency check
        if isinstance(value, (int, float)) and value > 0:
            confidence += 0.05
        
        return min(confidence, 1.0)
    
    def _advanced_validation_and_scoring(self, metrics: FinancialMetrics, text: str, data: Dict) -> FinancialMetrics:
        """Advanced validation with enhanced confidence scoring"""
        
        # Count successful extractions by category
        revenue_fields = ['revenue_reported', 'revenue_adjusted']
        profit_fields = ['operating_profit_reported', 'net_income_reported', 'ebitda_reported']
        balance_fields = ['total_assets', 'total_equity', 'cash_and_equivalents']
        cash_flow_fields = ['operating_cash_flow', 'investing_cash_flow', 'financing_cash_flow']
        
        field_groups = {
            'revenue': revenue_fields,
            'profitability': profit_fields,
            'balance_sheet': balance_fields,
            'cash_flow': cash_flow_fields
        }
        
        category_scores = {}
        for category, fields in field_groups.items():
            found_fields = sum(1 for field in fields if getattr(metrics, field) is not None)
            category_scores[category] = found_fields / len(fields)
        
        # Business logic validation
        logic_score = self._validate_business_logic(metrics)
        
        # Calculate weighted confidence
        base_confidence = sum(category_scores.values()) / len(category_scores)
        enhanced_confidence = (base_confidence * 0.7) + (logic_score * 0.3)
        
        # Bonus for comprehensive extraction
        total_extracted = sum(1 for field_name in dir(metrics) 
                            if not field_name.startswith('_') 
                            and not callable(getattr(metrics, field_name))
                            and getattr(metrics, field_name) is not None
                            and getattr(metrics, field_name) not in ["", "unknown", [], {}])
        
        if total_extracted >= 20:
            enhanced_confidence = min(enhanced_confidence + 0.05, 1.0)
        
        metrics.extraction_confidence = min(enhanced_confidence, 0.99)  # Cap at 99%
        
        # Set quality flags
        metrics.has_revenue = any(getattr(metrics, field) is not None for field in revenue_fields)
        metrics.has_profitability = any(getattr(metrics, field) is not None for field in profit_fields)
        metrics.has_balance_sheet = any(getattr(metrics, field) is not None for field in balance_fields)
        metrics.has_cash_flow = any(getattr(metrics, field) is not None for field in cash_flow_fields)
        
        # Enhanced metadata
        metrics.model_used = "Enhanced multi-phase extraction with advanced validation"
        metrics.extraction_notes = f"Comprehensive extraction with {total_extracted} fields and business logic validation"
        
        return metrics
    
    def _validate_business_logic(self, metrics: FinancialMetrics) -> float:
        """Validate business logic relationships between metrics"""
        
        score = 1.0
        validations_performed = 0
        
        # Revenue >= Operating Profit (usually)
        if metrics.revenue_reported and metrics.operating_profit_reported:
            if metrics.operating_profit_reported <= metrics.revenue_reported * 1.1:  # Allow 10% margin for rounding
                score += 0.0
            else:
                score -= 0.2  # Penalty for impossible relationship
            validations_performed += 1
        
        # Operating Profit >= Net Income (usually, considering tax)
        if metrics.operating_profit_reported and metrics.net_income_reported:
            if metrics.net_income_reported <= metrics.operating_profit_reported * 1.1:
                score += 0.0
            else:
                score -= 0.15
            validations_performed += 1
        
        # Total Assets should be reasonable relative to revenue
        if metrics.total_assets and metrics.revenue_reported:
            asset_turnover = metrics.revenue_reported / metrics.total_assets if metrics.total_assets > 0 else 0
            if 0.1 <= asset_turnover <= 5.0:  # Reasonable asset turnover range
                score += 0.1
            validations_performed += 1
        
        # Return average score if validations were performed
        return max(score / max(validations_performed, 1), 0.0) if validations_performed > 0 else 1.0
    
    # Include helper methods from the fixed service
    def _validate_metric_value(self, field: str, value: float) -> bool:
        """Validate if a metric value is reasonable (reuse from fixed service)"""
        validation_rules = {
            'revenue_reported': (1, 1000000),
            'operating_profit_reported': (0.01, 500000),
            'net_income_reported': (0.01, 500000),
            'earnings_per_share_reported': (0.01, 1000),
            'total_assets': (1, 2000000),
            'total_equity': (1, 2000000),
            'cash_and_equivalents': (0.1, 500000),
            'operating_cash_flow': (-100000, 500000),
            'investing_cash_flow': (-500000, 100000),
            'financing_cash_flow': (-500000, 100000),
        }
        
        if field in validation_rules:
            min_val, max_val = validation_rules[field]
            return min_val <= abs(value) <= max_val
        
        return True
    
    def _robust_json_cleaning(self, response: str) -> Optional[str]:
        """Reuse robust JSON cleaning from fixed service"""
        try:
            response = response.strip()
            response = re.sub(r'```(?:json)?\s*', '', response)
            
            start = response.find('{')
            end = response.rfind('}')
            
            if start != -1 and end != -1:
                json_str = response[start:end+1]
                json_str = re.sub(r'//.*$', '', json_str, flags=re.MULTILINE)
                json_str = re.sub(r'/\*.*?\*/', '', json_str, flags=re.DOTALL)
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                
                json.loads(json_str)  # Validate
                return json_str
        except:
            pass
        
        return None
    
    def _extract_filename_metadata(self, metrics: FinancialMetrics, filename: str) -> FinancialMetrics:
        """Extract metadata from filename (reuse from fixed service)"""
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if date_match:
            try:
                metrics.report_date = datetime.strptime(date_match.group(1), "%Y-%m-%d").date()
                metrics.fiscal_year = metrics.report_date.year
            except:
                pass
        
        filename_lower = filename.lower()
        if any(term in filename_lower for term in ['interim', 'quarterly', 'q1', 'q2', 'q3', 'q4']):
            metrics.report_type = "quarterly"
        elif any(term in filename_lower for term in ['annual', 'full year']):
            metrics.report_type = "annual"
        
        return metrics
    
    def _apply_validated_data_to_metrics(self, metrics: FinancialMetrics, data: Dict) -> FinancialMetrics:
        """Apply data with validation (reuse pattern from fixed service)"""
        for field, value in data.items():
            if hasattr(metrics, field) and value is not None:
                if self._validate_metric_value(field, value):
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
        
        return metrics
    
    def _calculate_enhanced_derived_metrics(self, metrics: FinancialMetrics, text: str, data: Dict) -> FinancialMetrics:
        """Calculate derived metrics with enhanced logic"""
        try:
            # Calculate margins with validation
            if metrics.revenue_reported and metrics.revenue_reported > 0:
                if metrics.operating_profit_reported:
                    metrics.operating_margin_pct = (metrics.operating_profit_reported / metrics.revenue_reported) * 100
                if metrics.net_income_reported:
                    metrics.net_margin_pct = (metrics.net_income_reported / metrics.revenue_reported) * 100
            
            # Enhanced ratio calculations
            if metrics.total_debt and metrics.total_equity and metrics.total_equity > 0:
                metrics.debt_to_equity = metrics.total_debt / metrics.total_equity
            
            # Annualized returns
            if metrics.net_income_reported and metrics.total_equity and metrics.total_equity > 0:
                annualized_income = metrics.net_income_reported * (4 if 'Q' in metrics.report_period else 1)
                metrics.return_on_equity_pct = (annualized_income / metrics.total_equity) * 100
            
            if metrics.net_income_reported and metrics.total_assets and metrics.total_assets > 0:
                annualized_income = metrics.net_income_reported * (4 if 'Q' in metrics.report_period else 1)
                metrics.return_on_assets_pct = (annualized_income / metrics.total_assets) * 100
            
            # Scaling for Swedish millions
            if "million" in text.lower() or "msek" in text.lower():
                self._apply_intelligent_scaling(metrics)
        
        except Exception as e:
            logger.error(f"Error calculating enhanced derived metrics: {e}")
        
        return metrics
    
    def _apply_intelligent_scaling(self, metrics: FinancialMetrics):
        """Apply intelligent scaling based on value ranges"""
        
        scaling_fields = [
            'revenue_reported', 'operating_profit_reported', 'net_income_reported',
            'total_assets', 'total_equity', 'operating_cash_flow'
        ]
        
        for field_name in scaling_fields:
            value = getattr(metrics, field_name)
            if value and 10 <= value <= 100000:  # Likely in millions already
                # Apply scaling based on reasonable value ranges for Swedish companies
                if field_name in ['revenue_reported', 'total_assets'] and value < 1000:
                    setattr(metrics, field_name, value * 1000000)  # Scale to actual SEK
                elif value < 10000:
                    setattr(metrics, field_name, value * 1000000)
                    
                    if not metrics.data_warnings:
                        metrics.data_warnings = []
                    metrics.data_warnings.append(f"Applied intelligent scaling to {field_name}")


class EnhancedFinancialExtractionService:
    """Enhanced service using the improved extractor"""
    
    def __init__(self):
        self.pdf_extractor = PDFTextExtractor()
        self.financial_extractor = EnhancedFinancialExtractor()
        from shared.config import settings
        self.db_url = settings.database_url
    
    async def extract_from_pdf(self, pdf_path: str, company_name: str) -> Optional[FinancialMetrics]:
        """Extract financial metrics from PDF with enhanced processing"""
        
        logger.info(f"Enhanced extraction from {pdf_path}")
        
        try:
            pdf_data = await self.pdf_extractor.extract_text(pdf_path)
            
            if not pdf_data.get("full_text"):
                logger.error(f"No text extracted from {pdf_path}")
                return None
            
            metrics = await self.financial_extractor.extract_metrics(
                document_text=pdf_data["full_text"],
                company_name=company_name,
                document_path=pdf_path
            )
            
            return metrics
            
        except Exception as e:
            logger.error(f"Enhanced PDF extraction failed: {e}")
            return None
    
    def _generate_document_uuid(self, company_name: str, report_period: str) -> str:
        """Generate a proper UUID for database storage"""
        namespace = uuid.UUID('12345678-1234-5678-1234-123456789012')
        name = f"{company_name}-{report_period}".lower()
        return str(uuid.uuid5(namespace, name))
    
    async def save_metrics(self, metrics: FinancialMetrics, document_id: Optional[str] = None) -> bool:
        """Save metrics to database (reuse from fixed service)"""
        
        try:
            if not document_id or len(document_id) < 32:
                document_id = self._generate_document_uuid(metrics.company_name, metrics.report_period)
            
            conn = await asyncpg.connect(self.db_url)
            data = asdict(metrics)
            
            # Use the same complete INSERT statement as the fixed service
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
                json.dumps(data['operational_metrics'] or {}), data.get('extraction_method', 'enhanced_local_llm'), 
                data['extraction_confidence'], None,  # extraction_date uses DEFAULT
                data['model_used'],
                data.get('has_revenue', data['revenue_reported'] is not None),
                data.get('has_profitability', data['operating_profit_reported'] is not None),
                data.get('has_cash_flow', data['operating_cash_flow'] is not None),
                data.get('has_balance_sheet', data['total_assets'] is not None),
                data.get('data_quality_score', data['extraction_confidence']),
                data['extraction_notes'], data.get('data_warnings', [])
            )
            
            await conn.close()
            logger.info(f"Enhanced: Saved metrics for {metrics.company_name} - {metrics.report_period} (ID: {document_id})")
            return True
            
        except Exception as e:
            logger.error(f"Enhanced database save failed: {e}")
            return False


# Test function
async def test_enhanced_extraction():
    """Test the enhanced extraction service"""
    
    print("🚀 TESTING ENHANCED EXTRACTION SERVICE")
    print("=" * 70)
    print("Target: 97-99% extraction confidence with improved success rate")
    
    service = EnhancedFinancialExtractionService()
    
    # Test with AAK document
    pdf_path = "/Users/jdandemar/Documents/YodaBuffett/backend/data/companies/SE/A/AAK/2025/quarterly_report/2025-07-17-aaks-interim-report-for-the-second-quarter-2025.pdf"
    
    metrics = await service.extract_from_pdf(pdf_path, "AAK")
    
    if metrics:
        print(f"✅ AAK enhanced extraction: {metrics.extraction_confidence:.1%} confidence")
        
        # Count extracted fields
        extracted_count = sum(1 for field_name in dir(metrics) 
                            if not field_name.startswith('_') 
                            and not callable(getattr(metrics, field_name))
                            and getattr(metrics, field_name) is not None
                            and getattr(metrics, field_name) not in ["", "unknown", [], {}])
        
        print(f"📊 Fields extracted: {extracted_count}")
        
        # Show key metrics with null safety
        if metrics.revenue_reported:
            print(f"💰 Revenue: {metrics.revenue_reported/1000000:.0f}M SEK")
        else:
            print("💰 Revenue: Not extracted")
            
        if metrics.operating_profit_reported:
            print(f"📈 Operating Profit: {metrics.operating_profit_reported/1000000:.0f}M SEK")
        else:
            print("📈 Operating Profit: Not extracted")
            
        if metrics.net_income_reported:
            print(f"💵 Net Income: {metrics.net_income_reported/1000000:.0f}M SEK")
        else:
            print("💵 Net Income: Not extracted")
            
        if metrics.total_assets:
            print(f"🏦 Total Assets: {metrics.total_assets/1000000000:.1f}B SEK")
        else:
            print("🏦 Total Assets: Not extracted")
        
        # Test save with existing document UUID
        existing_doc_id = "987f81e1-dc8c-4aab-9a06-2b254599cd60"
        success = await service.save_metrics(metrics, existing_doc_id)
        print(f"💾 Database save: {'✅ Success' if success else '❌ Failed'}")
        
        # Success evaluation
        if metrics.extraction_confidence >= 0.97:
            print(f"\n🎯 SUCCESS TARGET ACHIEVED: {metrics.extraction_confidence:.1%} >= 97%")
            print("🟢 Enhanced extraction significantly improved success rate!")
        elif metrics.extraction_confidence >= 0.95:
            print(f"\n📈 GOOD IMPROVEMENT: {metrics.extraction_confidence:.1%} vs previous 95%")
            print("🟡 Enhanced extraction shows improvement but room for further optimization")
        else:
            print(f"\n⚠️ NEEDS MORE WORK: {metrics.extraction_confidence:.1%} < 95%")
        
    else:
        print("❌ Enhanced extraction failed!")
    
    return service


if __name__ == "__main__":
    asyncio.run(test_enhanced_extraction())