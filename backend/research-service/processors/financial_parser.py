"""
Financial Data Parser
Extracts structured financial information from text
"""
import re
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class FinancialMetric:
    """Represents a financial metric extracted from text"""
    name: str
    value: float
    unit: str
    period: str
    confidence: float
    context: str


@dataclass
class FinancialData:
    """Collection of financial metrics from a document"""
    metrics: List[FinancialMetric]
    company_name: Optional[str]
    report_period: Optional[str]
    report_type: Optional[str]
    currency: Optional[str]


class FinancialParser:
    """Extracts structured financial data from text"""
    
    # Common financial metrics patterns (works for both English and Swedish)
    METRIC_PATTERNS = {
        'revenue': [
            r'(?:revenue|omsättning|intäkter).*?(?:SEK|MSEK|EUR|USD)?\s*([\d,\.]+)\s*(?:million|miljoner|bn|miljarder)?',
            r'(?:net sales|nettoomsättning).*?([\d,\.]+)\s*(?:MSEK|SEK|million)',
        ],
        'ebitda': [
            r'(?:EBITDA|EBITA).*?([\d,\.]+)\s*(?:MSEK|SEK|million)',
            r'(?:rörelseresultat|operating income).*?([\d,\.]+)\s*(?:MSEK|SEK|million)',
        ],
        'net_income': [
            r'(?:net income|net profit|nettovinst|resultat).*?([\d,\.]+)\s*(?:MSEK|SEK|million)',
            r'(?:årets resultat|periodens resultat).*?([\d,\.]+)\s*(?:MSEK|SEK|million)',
        ],
        'eps': [
            r'(?:EPS|earnings per share|vinst per aktie).*?([\d,\.]+)\s*(?:SEK|kr)?',
            r'(?:resultat per aktie).*?([\d,\.]+)\s*(?:SEK|kr)?',
        ],
        'margin': [
            r'(?:EBITDA margin|EBITA-marginal|rörelsemarginal).*?([\d,\.]+)\s*%',
            r'(?:operating margin|vinstmarginal).*?([\d,\.]+)\s*%',
        ]
    }
    
    # Currency patterns
    CURRENCY_PATTERNS = {
        'SEK': r'(?:SEK|MSEK|KSEK|kronor)',
        'EUR': r'(?:EUR|€|euro)',
        'USD': r'(?:USD|\$|dollar)',
        'NOK': r'(?:NOK|MNOK)',
        'DKK': r'(?:DKK|MDKK)',
    }
    
    # Period patterns
    PERIOD_PATTERNS = {
        'quarter': r'(?:Q[1-4]|första kvartalet|andra kvartalet|tredje kvartalet|fjärde kvartalet)',
        'year': r'(?:20\d{2}|FY\s*20\d{2}|helåret\s*20\d{2})',
        'month': r'(?:januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december|January|February|March|April|May|June|July|August|September|October|November|December)',
    }
    
    def __init__(self):
        self.metrics_found = []
    
    def parse_document(self, text: str, metadata: Optional[Dict] = None) -> FinancialData:
        """Parse financial data from document text"""
        self.metrics_found = []
        
        # Detect currency
        currency = self._detect_currency(text)
        
        # Extract company name
        company_name = self._extract_company_name(text, metadata)
        
        # Extract report period
        report_period = self._extract_report_period(text, metadata)
        
        # Extract report type
        report_type = self._extract_report_type(text, metadata)
        
        # Extract metrics
        for metric_name, patterns in self.METRIC_PATTERNS.items():
            self._extract_metric(text, metric_name, patterns)
        
        # Extract custom metrics based on document language
        if self._is_swedish(text):
            self._extract_swedish_specific_metrics(text)
        
        return FinancialData(
            metrics=self.metrics_found,
            company_name=company_name,
            report_period=report_period,
            report_type=report_type,
            currency=currency
        )
    
    def _detect_currency(self, text: str) -> str:
        """Detect the primary currency used in the document"""
        currency_counts = {}
        
        for currency, pattern in self.CURRENCY_PATTERNS.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            currency_counts[currency] = len(matches)
        
        if currency_counts:
            return max(currency_counts, key=currency_counts.get)
        return "SEK"  # Default for Swedish companies
    
    def _extract_company_name(self, text: str, metadata: Optional[Dict]) -> Optional[str]:
        """Extract company name from text"""
        # First check metadata
        if metadata and 'company_name' in metadata:
            return metadata['company_name']
        
        # Common patterns for company names in reports
        patterns = [
            r'(?:Delårsrapport|Interim Report|Annual Report).*?(?:för|for)\s+([A-ZÅÄÖa-zåäö\s&]+?)(?:\s+AB|\s+ASA|\s+AS|\s+Ltd)?',
            r'^([A-ZÅÄÖa-zåäö\s&]+?)(?:\s+AB|\s+ASA|\s+AS|\s+Ltd)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text[:1000], re.MULTILINE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_report_period(self, text: str, metadata: Optional[Dict]) -> Optional[str]:
        """Extract report period"""
        # Look for Q1-Q4 patterns
        quarter_match = re.search(r'(Q[1-4]\s*20\d{2})', text[:500], re.IGNORECASE)
        if quarter_match:
            return quarter_match.group(1)
        
        # Look for Swedish quarter descriptions
        swedish_quarters = {
            'första kvartalet': 'Q1',
            'andra kvartalet': 'Q2',
            'tredje kvartalet': 'Q3',
            'fjärde kvartalet': 'Q4',
        }
        
        for swedish, english in swedish_quarters.items():
            if swedish in text.lower()[:500]:
                year_match = re.search(r'20\d{2}', text[:500])
                if year_match:
                    return f"{english} {year_match.group()}"
        
        # Look for full year
        year_match = re.search(r'(?:helåret|full year|FY)\s*(20\d{2})', text[:500], re.IGNORECASE)
        if year_match:
            return f"FY {year_match.group(1)}"
        
        return None
    
    def _extract_report_type(self, text: str, metadata: Optional[Dict]) -> Optional[str]:
        """Determine report type"""
        report_types = {
            'quarterly': ['delårsrapport', 'interim report', 'quarterly report', 'kvartalsrapport'],
            'annual': ['årsredovisning', 'annual report', 'årsbokslut'],
            'press_release': ['pressmeddelande', 'press release', 'ceo comment'],
        }
        
        text_lower = text.lower()[:1000]
        
        for report_type, keywords in report_types.items():
            if any(keyword in text_lower for keyword in keywords):
                return report_type
        
        return 'other'
    
    def _extract_metric(self, text: str, metric_name: str, patterns: List[str]):
        """Extract a specific metric using patterns"""
        for pattern in patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            
            for match in matches:
                try:
                    # Extract value
                    value_str = match.group(1).replace(',', '').replace(' ', '')
                    value = float(value_str)
                    
                    # Extract context (surrounding text)
                    start = max(0, match.start() - 100)
                    end = min(len(text), match.end() + 100)
                    context = text[start:end].strip()
                    
                    # Determine unit
                    unit = self._extract_unit(match.group(0))
                    
                    # Determine period if mentioned nearby
                    period = self._extract_period_context(context)
                    
                    metric = FinancialMetric(
                        name=metric_name,
                        value=value,
                        unit=unit,
                        period=period or "unknown",
                        confidence=0.8,  # Can be improved with better heuristics
                        context=context
                    )
                    
                    self.metrics_found.append(metric)
                    
                except (ValueError, IndexError) as e:
                    logger.debug(f"Could not parse metric from match: {match.group(0)}")
                    continue
    
    def _extract_unit(self, text: str) -> str:
        """Extract unit from matched text"""
        if 'million' in text.lower() or 'msek' in text.lower():
            return 'millions'
        elif 'billion' in text.lower() or 'miljarder' in text.lower():
            return 'billions'
        elif '%' in text:
            return 'percent'
        else:
            return 'units'
    
    def _extract_period_context(self, context: str) -> Optional[str]:
        """Extract period from context"""
        # Check for quarters
        quarter_match = re.search(r'(Q[1-4]\s*20\d{2})', context, re.IGNORECASE)
        if quarter_match:
            return quarter_match.group(1)
        
        # Check for year
        year_match = re.search(r'(20\d{2})', context)
        if year_match:
            return year_match.group(1)
        
        return None
    
    def _is_swedish(self, text: str) -> bool:
        """Check if text is in Swedish"""
        swedish_indicators = ['och', 'för', 'är', 'att', 'med', 'som', 'året', 'kronor']
        text_lower = text.lower()[:1000]
        
        swedish_count = sum(1 for word in swedish_indicators if word in text_lower)
        return swedish_count >= 3
    
    def _extract_swedish_specific_metrics(self, text: str):
        """Extract metrics specific to Swedish reports"""
        # Common Swedish financial terms
        swedish_patterns = {
            'orderingång': [
                r'(?:orderingång|order intake).*?([\d,\.]+)\s*(?:MSEK|SEK|miljoner)',
            ],
            'orderstock': [
                r'(?:orderstock|order backlog).*?([\d,\.]+)\s*(?:MSEK|SEK|miljoner)',
            ],
            'soliditet': [
                r'(?:soliditet|equity ratio).*?([\d,\.]+)\s*%',
            ],
            'kassaflöde': [
                r'(?:kassaflöde|cash flow).*?([\d,\.]+)\s*(?:MSEK|SEK|miljoner)',
            ],
        }
        
        for metric_name, patterns in swedish_patterns.items():
            self._extract_metric(text, metric_name, patterns)