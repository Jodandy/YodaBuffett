#!/usr/bin/env python3
"""
Financial Section Parser

Intelligently parses Nordic financial reports to identify natural section boundaries
and extract complete financial sections (balance sheet, income statement, etc.)
rather than arbitrary character chunks.
"""

import re
import logging
from typing import Dict, List, Tuple, Optional, NamedTuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class SectionType(Enum):
    """Standard financial report sections"""
    BALANCE_SHEET = "balance_sheet"
    INCOME_STATEMENT = "income_statement" 
    CASH_FLOW = "cash_flow"
    EQUITY_STATEMENT = "equity_statement"
    MANAGEMENT_DISCUSSION = "management_discussion"
    STRATEGY = "strategy"
    OPERATIONS = "operations"
    MARKET_ANALYSIS = "market_analysis"
    RISK_FACTORS = "risk_factors"
    CORPORATE_GOVERNANCE = "corporate_governance"
    SUSTAINABILITY = "sustainability"
    ACCOUNTING_POLICIES = "accounting_policies"
    NOTES = "notes"
    AUDITOR_REPORT = "auditor_report"
    OTHER = "other"


@dataclass
class FinancialSection:
    """Represents a detected financial section"""
    section_type: SectionType
    title: str
    start_pos: int
    end_pos: int
    content: str
    confidence: float
    page_numbers: List[int] = None
    subsections: List['FinancialSection'] = None


class FinancialSectionParser:
    """Parses financial reports into natural sections"""
    
    def __init__(self):
        # Section header patterns for Nordic reports (Swedish, Norwegian, Danish, Finnish, English)
        self.section_patterns = {
            SectionType.BALANCE_SHEET: [
                # Swedish
                r'BALANSRÄKNING', r'KONCERNBALANSRÄKNING', r'FINANSIELL STÄLLNING',
                r'RAPPORT ÖVER FINANSIELL STÄLLNING',
                # Norwegian
                r'BALANSE', r'KONSERNBALANSE', r'FINANSIELL STILLING',
                # Danish  
                r'BALANCE', r'KONCERNBALANCE',
                # Finnish
                r'TASE', r'KONSERNITASE',
                # English
                r'BALANCE SHEET', r'CONSOLIDATED BALANCE SHEET', 
                r'STATEMENT OF FINANCIAL POSITION', r'FINANCIAL POSITION'
            ],
            
            SectionType.INCOME_STATEMENT: [
                # Swedish
                r'RESULTATRÄKNING', r'KONCERNRESULTATRÄKNING', r'RAPPORT ÖVER TOTALRESULTAT',
                r'ÖVRIGA TOTALRESULTAT',
                # Norwegian
                r'RESULTAT', r'KONSERNRESULTAT', r'TOTALRESULTAT',
                # Danish
                r'RESULTATOPGØRELSE', r'KONCERNRESULTATOPGØRELSE',
                # Finnish
                r'TULOSLASKELMA', r'KONSERNITULOSLASKELMA',
                # English
                r'INCOME STATEMENT', r'PROFIT AND LOSS', r'P&L', r'CONSOLIDATED INCOME',
                r'STATEMENT OF COMPREHENSIVE INCOME', r'COMPREHENSIVE INCOME'
            ],
            
            SectionType.CASH_FLOW: [
                # Swedish
                r'KASSAFLÖDESANALYS', r'KASSAFLÖDE', r'KONCERNKASSAFLÖDE',
                # Norwegian
                r'KONTANTSTRØM', r'KONSERNKONTANTSTRØM', r'KONTANTSTRØMOPPSTILLING',
                # Danish
                r'PENGESTRØMSOPGØRELSE', r'KONCERNPENGESTRØM',
                # Finnish
                r'RAHAVIRTALASKELMA', r'KONSERNIRAHAVIRTA',
                # English
                r'CASH FLOW', r'CASH FLOWS', r'STATEMENT OF CASH FLOWS',
                r'CONSOLIDATED CASH FLOW'
            ],
            
            SectionType.EQUITY_STATEMENT: [
                # Swedish
                r'FÖRÄNDRING.*EGET KAPITAL', r'EGET KAPITAL', r'FÖRÄNDRINGAR I EGET KAPITAL',
                # Norwegian
                r'ENDRING.*EGENKAPITAL', r'EGENKAPITAL', 
                # Danish
                r'ÆNDRING.*EGENKAPITAL', r'EGENKAPITAL',
                # Finnish
                r'OMA PÄÄOMA', r'MUUTOKSET.*OMASSA PÄÄOMASSA',
                # English
                r'CHANGES IN EQUITY', r'EQUITY', r'SHAREHOLDERS EQUITY',
                r'STATEMENT.*EQUITY'
            ],
            
            SectionType.MANAGEMENT_DISCUSSION: [
                # Swedish
                r'VD.*ORD', r'VERKSTÄLLANDE DIREKTÖR', r'FÖRVALTNINGSBERÄTTELSE',
                r'BOLAGSLEDNING', r'LEDNINGENS KOMMENTARER',
                # Norwegian
                r'ADMINISTRERENDE DIREKTØR', r'STYRETS BERETNING', r'LEDELSENS KOMMENTARER',
                # Danish
                r'ADMINISTRERENDE DIREKTØR', r'LEDELSESBERETNING', r'DIREKTØRENS BERETNING',
                # Finnish
                r'TOIMITUSJOHTAJA', r'HALLITUKSEN TOIMINTAKERTOMUS',
                # English
                r'CEO.*LETTER', r'MANAGEMENT.*DISCUSSION', r'MANAGEMENT.*ANALYSIS',
                r'BUSINESS REVIEW', r'EXECUTIVE SUMMARY', r'CHAIRMAN.*STATEMENT'
            ],
            
            SectionType.STRATEGY: [
                # Swedish
                r'STRATEGI', r'STRATEGISK.*RIKTNING', r'FRAMTID', r'MÅLSÄTTNINGAR',
                r'AFFÄRSPLAN', r'LÅNGSIKTIG.*MÅL',
                # Norwegian
                r'STRATEGI', r'STRATEGISK.*RETNING', r'FREMTID', r'MÅLSETNINGER',
                # Danish
                r'STRATEGI', r'STRATEGISK.*RETNING', r'FREMTID', r'MÅLSÆTNINGER',
                # Finnish
                r'STRATEGIA', r'STRATEGINEN.*SUUNTA', r'TULEVAISUUS',
                # English
                r'STRATEGY', r'STRATEGIC.*DIRECTION', r'FUTURE.*OUTLOOK', r'OBJECTIVES',
                r'BUSINESS STRATEGY', r'STRATEGIC PLAN'
            ],
            
            SectionType.RISK_FACTORS: [
                # Swedish
                r'RISKFAKTORER', r'RISK.*HANTERING', r'RISKER', r'RISKANALYS',
                # Norwegian
                r'RISIKOFAKTORER', r'RISIKOSTYRING', r'RISIKOER',
                # Danish
                r'RISIKOFAKTORER', r'RISIKOSTYRING', r'RISIKOER',
                # Finnish
                r'RISKITEKIJÄT', r'RISKIEN.*HALLINTA',
                # English
                r'RISK FACTORS', r'RISK MANAGEMENT', r'RISKS', r'RISK ANALYSIS'
            ],
            
            SectionType.NOTES: [
                # Swedish
                r'NOTER', r'TILLÄGGSUPPLYSNINGAR', r'NOT \d+',
                # Norwegian
                r'NOTER', r'TILLEGGSOPPLYSNINGER', r'NOTE \d+',
                # Danish
                r'NOTER', r'SUPPLERENDE OPLYSNINGER', r'NOTE \d+',
                # Finnish
                r'LIITETIEDOT', r'TILINPÄÄTÖKSEN LIITETIEDOT',
                # English
                r'NOTES?', r'NOTES? TO.*FINANCIAL STATEMENTS', r'NOTE \d+',
                r'ADDITIONAL.*INFORMATION'
            ]
        }
        
        # Financial statement line item patterns for content validation
        self.content_patterns = {
            SectionType.BALANCE_SHEET: [
                # Assets, Liabilities, Equity terms
                r'TILLGÅNGAR|ASSETS|AKTIVER|VARAT',
                r'SKULDER|LIABILITIES|GJELD|VELAT',
                r'EGET KAPITAL|EQUITY|EGENKAPITAL|OMA PÄÄOMA',
                r'BALANSOMSLUTNING|TOTAL.*ASSETS|SUM.*AKTIVA'
            ],
            
            SectionType.INCOME_STATEMENT: [
                # Revenue, Costs, Profit terms
                r'NETTOOMSÄTTNING|REVENUE|OMSETNING|LIIKEVAIHTO',
                r'RÖRELSEKOSTNADER|OPERATING.*COSTS|DRIFTSKOSTNADER',
                r'RÖRELSERESULTAT|OPERATING.*INCOME|EBIT|DRIFTSRESULTAT',
                r'RESULTAT.*SKATT|NET.*INCOME|PROFIT'
            ],
            
            SectionType.CASH_FLOW: [
                # Cash flow terms
                r'KASSAFLÖDE.*LÖPANDE|OPERATING.*CASH|DRIFTSKASSASTRØM',
                r'INVESTERINGSVERKSAMHET|INVESTING.*ACTIVITIES|INVESTERINGER',
                r'FINANSIERINGSVERKSAMHET|FINANCING.*ACTIVITIES|FINANSIERING'
            ]
        }
    
    def find_section_headers(self, text: str) -> List[Tuple[SectionType, str, int, int]]:
        """Find all potential section headers in the text"""
        headers = []
        
        # Split text into lines with position tracking
        lines = text.split('\n')
        current_pos = 0
        
        for line_num, line in enumerate(lines):
            line_start = current_pos
            line_end = current_pos + len(line)
            current_pos = line_end + 1  # +1 for newline
            
            # Skip very short lines or lines with too much lowercase (likely body text)
            if len(line.strip()) < 3 or len(line.strip()) > 100:
                continue
                
            # Look for section patterns
            for section_type, patterns in self.section_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, line.upper(), re.IGNORECASE):
                        # Additional checks for header-like formatting
                        if self._looks_like_header(line):
                            headers.append((section_type, line.strip(), line_start, line_end))
                            break
        
        return headers
    
    def _looks_like_header(self, line: str) -> bool:
        """Check if a line looks like a major section header (more restrictive)"""
        line = line.strip()
        
        # Skip very short or very long lines
        if len(line) < 8 or len(line) > 100:
            return False
            
        # Skip lines that are clearly not headers
        if (line.endswith('.') or 
            re.search(r'\d{4}[-/]\d{2}[-/]\d{2}', line) or  # Dates
            re.search(r'^\d+[\s\.]', line) or  # Starts with numbers
            re.search(r'[a-z]{20,}', line) or  # Long lowercase sequences
            re.search(r'\d+[,\.]\d+', line) or  # Financial numbers
            'SEK' in line.upper() or 'MSEK' in line.upper()):  # Financial amounts
            return False
        
        # Strong header indicators
        strong_indicators = [
            line.isupper(),  # All caps
            re.match(r'^[A-ZÅÄÖÆØÜ][A-ZÅÄÖÆØÜ\s]+$', line),  # Title case Nordic
            any(pattern in line.upper() for pattern in [
                'BALANSRÄKNING', 'RESULTATRÄKNING', 'KASSAFLÖDE', 
                'BALANCE SHEET', 'INCOME STATEMENT', 'CASH FLOW',
                'STRATEGI', 'STRATEGY', 'RISKFAKTORER', 'RISK FACTORS',
                'VD HAR ORDET', 'CEO', 'FÖRVALTNINGSBERÄTTELSE'
            ])
        ]
        
        return any(strong_indicators)
    
    def detect_section_boundaries(self, text: str) -> List[FinancialSection]:
        """Detect natural section boundaries in financial report"""
        headers = self.find_section_headers(text)
        
        if not headers:
            # No clear sections found, treat as single section
            return [FinancialSection(
                section_type=SectionType.OTHER,
                title="Complete Document",
                start_pos=0,
                end_pos=len(text),
                content=text,
                confidence=0.5
            )]
        
        raw_sections = []
        
        for i, (section_type, title, header_start, header_end) in enumerate(headers):
            # Determine section content boundaries
            content_start = header_end
            
            # Find next header or end of document
            if i + 1 < len(headers):
                content_end = headers[i + 1][2]  # Start of next header
            else:
                content_end = len(text)
            
            # Extract section content
            section_content = text[content_start:content_end].strip()
            
            # Validate section with content patterns
            confidence = self._calculate_section_confidence(section_type, section_content)
            
            # More restrictive filtering
            if (confidence > 0.5 and  # Higher confidence threshold
                len(section_content) > 500):  # Minimum section size
                raw_sections.append(FinancialSection(
                    section_type=section_type,
                    title=title,
                    start_pos=content_start,
                    end_pos=content_end,
                    content=section_content,
                    confidence=confidence
                ))
        
        # Merge adjacent sections of the same type
        merged_sections = self._merge_adjacent_sections(raw_sections)
        
        # Apply minimum sizes for financial statements
        filtered_sections = self._filter_by_minimum_sizes(merged_sections)
        
        return filtered_sections
    
    def _calculate_section_confidence(self, section_type: SectionType, content: str) -> float:
        """Calculate confidence that content matches the section type"""
        if not content or len(content) < 50:
            return 0.2  # Very short content is unreliable
        
        confidence = 0.5  # Base confidence
        
        # Check for expected content patterns
        if section_type in self.content_patterns:
            patterns = self.content_patterns[section_type]
            matches = 0
            
            for pattern in patterns:
                if re.search(pattern, content, re.IGNORECASE):
                    matches += 1
            
            # Boost confidence based on content matches
            content_confidence = min(matches / len(patterns), 1.0)
            confidence += content_confidence * 0.4
        
        # Check content length (reasonable sections should have some content)
        if len(content) > 500:
            confidence += 0.1
        if len(content) > 2000:
            confidence += 0.1
        
        # Penalize extremely long sections (might be incorrectly detected)
        if len(content) > 20000:
            confidence -= 0.2
        
        return min(confidence, 1.0)
    
    def _merge_adjacent_sections(self, sections: List[FinancialSection]) -> List[FinancialSection]:
        """Merge adjacent sections of the same type"""
        if not sections:
            return sections
        
        merged = []
        current_section = sections[0]
        
        for next_section in sections[1:]:
            # Check if sections should be merged
            if (current_section.section_type == next_section.section_type and 
                abs(current_section.end_pos - next_section.start_pos) < 1000):  # Close proximity
                
                # Merge sections
                merged_content = current_section.content + "\n\n" + next_section.content
                merged_title = f"{current_section.title} (merged)"
                avg_confidence = (current_section.confidence + next_section.confidence) / 2
                
                current_section = FinancialSection(
                    section_type=current_section.section_type,
                    title=merged_title,
                    start_pos=current_section.start_pos,
                    end_pos=next_section.end_pos,
                    content=merged_content,
                    confidence=avg_confidence
                )
            else:
                # No merge, add current to results and move to next
                merged.append(current_section)
                current_section = next_section
        
        # Don't forget the last section
        merged.append(current_section)
        return merged
    
    def _filter_by_minimum_sizes(self, sections: List[FinancialSection]) -> List[FinancialSection]:
        """Apply minimum size requirements for different section types"""
        
        # Minimum sizes for meaningful sections
        min_sizes = {
            SectionType.BALANCE_SHEET: 2000,      # Balance sheets should be substantial
            SectionType.INCOME_STATEMENT: 1500,   # Income statements should be complete
            SectionType.CASH_FLOW: 1500,          # Cash flow statements should be complete
            SectionType.EQUITY_STATEMENT: 1000,   # Equity statements can be shorter
            SectionType.MANAGEMENT_DISCUSSION: 1000,  # Management discussion should be substantial
            SectionType.STRATEGY: 800,            # Strategy sections can be shorter
            SectionType.RISK_FACTORS: 1000,       # Risk sections should be comprehensive
            SectionType.NOTES: 500,               # Notes can vary widely
            SectionType.OTHER: 200                # Other content can be short
        }
        
        filtered = []
        for section in sections:
            min_size = min_sizes.get(section.section_type, 500)
            
            if len(section.content) >= min_size:
                filtered.append(section)
            else:
                logger.debug(f"Filtered out {section.section_type.value} section (too short: {len(section.content)} < {min_size})")
        
        return filtered
    
    def parse_document(self, document_text: str, document_id: str = None) -> Dict:
        """Parse a complete financial document into sections"""
        logger.info(f"Parsing document {document_id or 'unknown'} ({len(document_text):,} characters)")
        
        sections = self.detect_section_boundaries(document_text)
        
        # Group sections by type
        section_summary = {}
        for section in sections:
            section_type_name = section.section_type.value
            if section_type_name not in section_summary:
                section_summary[section_type_name] = []
            section_summary[section_type_name].append({
                'title': section.title,
                'length': len(section.content),
                'confidence': section.confidence
            })
        
        logger.info(f"Found {len(sections)} sections: {list(section_summary.keys())}")
        
        return {
            'document_id': document_id,
            'total_sections': len(sections),
            'sections': sections,
            'section_summary': section_summary,
            'parsing_success': len(sections) > 0
        }
    
    def extract_financial_statements(self, sections: List[FinancialSection]) -> Dict[str, FinancialSection]:
        """Extract the main financial statements from parsed sections"""
        financial_statements = {}
        
        # Find the best match for each financial statement type
        statement_types = [
            SectionType.BALANCE_SHEET,
            SectionType.INCOME_STATEMENT, 
            SectionType.CASH_FLOW,
            SectionType.EQUITY_STATEMENT
        ]
        
        for statement_type in statement_types:
            candidates = [s for s in sections if s.section_type == statement_type]
            
            if candidates:
                # Pick the highest confidence candidate
                best_candidate = max(candidates, key=lambda x: x.confidence)
                financial_statements[statement_type.value] = best_candidate
        
        return financial_statements


if __name__ == "__main__":
    # For testing
    parser = FinancialSectionParser()
    
    # Test with sample text
    sample_text = """
    ÅRSRAPPORT 2024
    
    VD HAR ORDET
    Det här har varit ett transformativt år för vårt företag...
    
    BALANSRÄKNING
    TILLGÅNGAR
    Omsättningstillgångar
    Kassa och bank         150 000
    Kundfordringar        200 000
    Varulager            100 000
    Summa omsättningstillgångar  450 000
    
    SKULDER OCH EGET KAPITAL
    Kortfristiga skulder   200 000
    Eget kapital          250 000
    Summa                 450 000
    
    RESULTATRÄKNING  
    Nettoomsättning      1 000 000
    Rörelsekostnader      -800 000
    Rörelseresultat        200 000
    """
    
    result = parser.parse_document(sample_text, "test_doc")
    print("🔍 Parsing Results:")
    for section in result['sections']:
        print(f"  {section.section_type.value:20} confidence: {section.confidence:.2f} length: {len(section.content):,}")