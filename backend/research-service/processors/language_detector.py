"""
Language Detection and Translation Support
Handles multi-language documents (Swedish/English)
"""
from typing import Dict, Optional, Tuple
import logging
from langdetect import detect, detect_langs, LangDetectException

logger = logging.getLogger(__name__)


class LanguageDetector:
    """Detects document language and provides translation mappings"""
    
    # Financial terms mapping Swedish <-> English
    FINANCIAL_TERMS = {
        'sv_to_en': {
            'omsättning': 'revenue',
            'intäkter': 'income',
            'nettoomsättning': 'net sales',
            'rörelseresultat': 'operating income',
            'resultat': 'earnings',
            'vinst': 'profit',
            'förlust': 'loss',
            'marginal': 'margin',
            'kassaflöde': 'cash flow',
            'tillgångar': 'assets',
            'skulder': 'liabilities',
            'eget kapital': 'equity',
            'orderingång': 'order intake',
            'orderstock': 'order backlog',
            'soliditet': 'equity ratio',
            'aktie': 'share',
            'utdelning': 'dividend',
            'kvartal': 'quarter',
            'helår': 'full year',
            'rapport': 'report',
            'delårsrapport': 'interim report',
            'årsredovisning': 'annual report',
            'pressmeddelande': 'press release',
            'bokslutskommuniké': 'year-end report',
        },
        'en_to_sv': {
            'revenue': 'omsättning',
            'income': 'intäkter',
            'net sales': 'nettoomsättning',
            'operating income': 'rörelseresultat',
            'earnings': 'resultat',
            'profit': 'vinst',
            'loss': 'förlust',
            'margin': 'marginal',
            'cash flow': 'kassaflöde',
            'assets': 'tillgångar',
            'liabilities': 'skulder',
            'equity': 'eget kapital',
            'order intake': 'orderingång',
            'order backlog': 'orderstock',
            'equity ratio': 'soliditet',
            'share': 'aktie',
            'dividend': 'utdelning',
            'quarter': 'kvartal',
            'full year': 'helår',
            'report': 'rapport',
            'interim report': 'delårsrapport',
            'annual report': 'årsredovisning',
            'press release': 'pressmeddelande',
            'year-end report': 'bokslutskommuniké',
        }
    }
    
    # Common Swedish company name suffixes
    SWEDISH_COMPANY_SUFFIXES = ['AB', 'AB (publ)', 'ASA', 'AS']
    
    def detect_language(self, text: str) -> Tuple[str, float]:
        """
        Detect the primary language of the text
        Returns: (language_code, confidence)
        """
        try:
            # Get language probabilities
            langs = detect_langs(text[:5000])  # Use first 5000 chars for detection
            
            if langs:
                primary_lang = langs[0]
                return primary_lang.lang, primary_lang.prob
            
        except LangDetectException as e:
            logger.warning(f"Language detection failed: {e}")
        
        # Fallback: check for Swedish characters
        if self._has_swedish_chars(text):
            return 'sv', 0.8
        
        return 'en', 0.5  # Default to English
    
    def _has_swedish_chars(self, text: str) -> bool:
        """Check if text contains Swedish-specific characters"""
        swedish_chars = set('åäöÅÄÖ')
        return any(char in swedish_chars for char in text)
    
    def is_multilingual(self, text: str) -> bool:
        """Check if document contains multiple languages"""
        try:
            langs = detect_langs(text[:5000])
            # Consider multilingual if second language has >20% probability
            return len(langs) > 1 and langs[1].prob > 0.2
        except:
            return False
    
    def translate_financial_term(self, term: str, from_lang: str, to_lang: str) -> Optional[str]:
        """Translate financial terms between languages"""
        term_lower = term.lower().strip()
        
        if from_lang == 'sv' and to_lang == 'en':
            return self.FINANCIAL_TERMS['sv_to_en'].get(term_lower)
        elif from_lang == 'en' and to_lang == 'sv':
            return self.FINANCIAL_TERMS['en_to_sv'].get(term_lower)
        
        return None
    
    def normalize_company_name(self, name: str) -> str:
        """Normalize company names (remove suffixes, etc.)"""
        # Remove common suffixes
        for suffix in self.SWEDISH_COMPANY_SUFFIXES:
            if name.endswith(f' {suffix}'):
                name = name[:-len(suffix)-1]
        
        return name.strip()
    
    def extract_bilingual_sections(self, text: str) -> Dict[str, str]:
        """
        Extract sections that might be in different languages
        Common in Swedish reports that have English summaries
        """
        sections = {
            'swedish': '',
            'english': ''
        }
        
        # Simple heuristic: look for section markers
        english_markers = ['Summary', 'Key figures', 'CEO comment', 'Financial highlights']
        swedish_markers = ['Sammanfattning', 'Nyckeltal', 'VD-kommentar', 'Finansiella höjdpunkter']
        
        lines = text.split('\n')
        current_section = None
        
        for line in lines:
            line_stripped = line.strip()
            
            # Check for section markers
            if any(marker in line_stripped for marker in english_markers):
                current_section = 'english'
            elif any(marker in line_stripped for marker in swedish_markers):
                current_section = 'swedish'
            
            # Add to appropriate section
            if current_section:
                sections[current_section] += line + '\n'
        
        return sections
    
    def get_query_language(self, query: str) -> str:
        """Detect language of a user query"""
        try:
            return detect(query)
        except:
            # Simple heuristic
            if self._has_swedish_chars(query):
                return 'sv'
            return 'en'