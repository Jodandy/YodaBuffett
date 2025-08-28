"""
Document Processor for SEC Filings
Handles PDF and HTML parsing, text extraction, and section identification.
"""

import os
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path

# PDF processing
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# HTML processing
try:
    from bs4 import BeautifulSoup
    HTML_AVAILABLE = True
except ImportError:
    HTML_AVAILABLE = False


@dataclass
class DocumentSection:
    """Represents a section of a financial document."""
    name: str
    content: str
    page_start: Optional[int] = None
    page_end: Optional[int] = None
    confidence: float = 1.0


@dataclass
class ProcessedDocument:
    """Represents a fully processed financial document."""
    filename: str
    document_type: str  # 'PDF' or 'HTML'
    company_name: Optional[str] = None
    filing_type: Optional[str] = None  # '10-K', '10-Q', etc.
    filing_date: Optional[str] = None
    full_text: str = ""
    sections: List[DocumentSection] = None
    
    def __post_init__(self):
        if self.sections is None:
            self.sections = []


class DocumentProcessor:
    """Main class for processing SEC filings and other financial documents."""
    
    # Common SEC filing section patterns
    SECTION_PATTERNS = {
        'business': [
            r'item\s*1\s*[\.\-\s]*business',
            r'business\s*overview',
            r'our\s*business'
        ],
        'risk_factors': [
            r'item\s*1a\s*[\.\-\s]*risk\s*factors',
            r'risk\s*factors',
            r'principal\s*risks'
        ],
        'mda': [
            r'item\s*2\s*[\.\-\s]*management.{0,50}discussion',
            r'management.{0,20}discussion.{0,20}analysis',
            r'md&a'
        ],
        'financial_statements': [
            r'item\s*8\s*[\.\-\s]*financial\s*statements',
            r'consolidated\s*statements',
            r'financial\s*statements'
        ],
        'controls': [
            r'item\s*9a\s*[\.\-\s]*controls',
            r'disclosure\s*controls',
            r'internal\s*controls'
        ]
    }
    
    def __init__(self):
        """Initialize the document processor."""
        self.supported_formats = []
        
        if PDF_AVAILABLE:
            self.supported_formats.append('PDF')
        if HTML_AVAILABLE:
            self.supported_formats.append('HTML')
            
        if not self.supported_formats:
            raise ImportError("No document processing libraries available. Install PyPDF2 and/or beautifulsoup4")
    
    def process_file(self, file_path: str) -> ProcessedDocument:
        """
        Process a document file and extract structured information.
        
        Args:
            file_path: Path to the document file
            
        Returns:
            ProcessedDocument with extracted content
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Determine file type
        file_extension = file_path.suffix.lower()
        
        if file_extension == '.pdf':
            if not PDF_AVAILABLE:
                raise ImportError("PyPDF2 not available for PDF processing")
            return self._process_pdf(file_path)
        elif file_extension in ['.html', '.htm']:
            if not HTML_AVAILABLE:
                raise ImportError("BeautifulSoup not available for HTML processing")
            return self._process_html(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    
    def _process_pdf(self, file_path: Path) -> ProcessedDocument:
        """Process a PDF document."""
        doc = ProcessedDocument(
            filename=file_path.name,
            document_type='PDF'
        )
        
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                # Extract text from all pages
                full_text = ""
                page_texts = []
                
                for page_num, page in enumerate(pdf_reader.pages):
                    try:
                        page_text = page.extract_text()
                        page_texts.append(page_text)
                        full_text += f"\n--- PAGE {page_num + 1} ---\n"
                        full_text += page_text
                    except Exception as e:
                        print(f"Warning: Could not extract text from page {page_num + 1}: {e}")
                        continue
                
                doc.full_text = full_text
                
                # Extract metadata
                doc.company_name = self._extract_company_name(full_text)
                doc.filing_type = self._extract_filing_type(full_text)
                doc.filing_date = self._extract_filing_date(full_text)
                
                # Identify sections
                doc.sections = self._identify_sections(full_text, page_texts)
                
        except Exception as e:
            raise RuntimeError(f"Error processing PDF: {e}")
        
        return doc
    
    def _process_html(self, file_path: Path) -> ProcessedDocument:
        """Process an HTML document."""
        doc = ProcessedDocument(
            filename=file_path.name,
            document_type='HTML'
        )
        
        try:
            # Try multiple encodings for HTML files
            encodings = ['utf-8', 'iso-8859-1', 'windows-1252']
            html_content = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as file:
                        html_content = file.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if html_content is None:
                raise RuntimeError("Could not decode HTML file with any supported encoding")
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Extract clean text
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            doc.full_text = text
            
            # Extract metadata
            doc.company_name = self._extract_company_name(text)
            doc.filing_type = self._extract_filing_type(text)
            doc.filing_date = self._extract_filing_date(text)
            
            # Identify sections
            doc.sections = self._identify_sections(text)
            
        except Exception as e:
            raise RuntimeError(f"Error processing HTML: {e}")
        
        return doc
    
    def _extract_company_name(self, text: str) -> Optional[str]:
        """Extract company name from document text."""
        # Look for common patterns in SEC filings - most specific first
        patterns = [
            # Handle PDF extraction artifacts like "3M COMP ANY"
            r'([A-Z0-9]+(?:\s+[A-Z]+)*)\s+COMP\s+ANY\s*(?:State of|\n)',
            r'([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)\s+CORP\s+OR\s+ATION\s*(?:State of|\n)',
            # Most specific patterns first
            r'Commission file number.*?\n([A-Z][A-Za-z\s&\.,]{5,35}?)(?:\s*State of|\s*I\.R\.S\.|\n)',
            r'COMPANY:\s*([A-Z][A-Za-z\s&\.,]{5,35}?)(?:\s*CIK|\s*FORM|\s*STATE|\n)',
            r'CORPORATE NAME:\s*([A-Za-z\s&\.,]{5,30}+)',
            # Company-specific patterns (add more as needed)
            r'3M\s+COMP\s+ANY',
            r'Apple\s+Inc\.?',
            r'APPLE\s+INC\.?',
            r'Microsoft\s+Corporation',
            r'Tesla,?\s+Inc\.?',
            # General patterns - more restrictive  
            r'([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+){1,3})\s+(?:INC\.?|CORP\.?|CORPORATION|COMPANY)',
            r'([A-Z][A-Za-z\s&\.,]{10,35}?)\s*(?:FORM\s*10-[KQ]|Annual Report)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text[:2000], re.IGNORECASE)  # Search first 2000 chars
            if match:
                # Some patterns don't have groups, use full match
                if match.groups():
                    company_name = match.group(1).strip()
                else:
                    company_name = match.group(0).strip()
                
                # Special handling for common PDF extraction artifacts
                if "3M COMP ANY" in company_name.upper():
                    return "3M Company"
                elif company_name.upper() == "3M":
                    return "3M Company"
                
                # Clean up common suffixes and noise
                company_name = re.sub(r'\s+(INC\.?|CORP\.?|CORPORATION|LLC\.?|LTD\.?|COMPANY)$', '', company_name, flags=re.IGNORECASE)
                company_name = re.sub(r'\s+(STATE\s+OF|DELAWARE|NEVADA|CALIFORNIA).*$', '', company_name, flags=re.IGNORECASE)
                company_name = company_name.strip(' .,;:')
                
                # Only return if it looks reasonable (1-4 words, reasonable length)
                words = company_name.split()
                if 1 <= len(words) <= 4 and 3 <= len(company_name) <= 35:
                    return company_name.title()
        
        return None
    
    def _extract_filing_type(self, text: str) -> Optional[str]:
        """Extract filing type (10-K, 10-Q, etc.) from document text."""
        patterns = [
            r'FORM\s*(10-[KQ])',
            r'(10-[KQ])\s*ANNUAL REPORT',
            r'(10-[KQ])\s*QUARTERLY REPORT',
            r'(8-K)\s*CURRENT REPORT'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text[:1000], re.IGNORECASE)
            if match:
                return match.group(1).upper()
        
        return None
    
    def _extract_filing_date(self, text: str) -> Optional[str]:
        """Extract filing date from document text."""
        patterns = [
            r'FILED\s*(?:ON|:)\s*([A-Z][a-z]+ \d{1,2}, \d{4})',
            r'FILING DATE:\s*(\d{4}-\d{2}-\d{2})',
            r'(\d{1,2}/\d{1,2}/\d{4})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text[:1000])
            if match:
                return match.group(1)
        
        return None
    
    def _identify_sections(self, full_text: str, page_texts: Optional[List[str]] = None) -> List[DocumentSection]:
        """Identify and extract document sections."""
        sections = []
        text_lower = full_text.lower()
        
        for section_name, patterns in self.SECTION_PATTERNS.items():
            for pattern in patterns:
                matches = list(re.finditer(pattern, text_lower, re.IGNORECASE | re.MULTILINE))
                
                if matches:
                    # Use the first match found
                    match = matches[0]
                    start_pos = match.start()
                    
                    # Find the end of this section (start of next major section or end of document)
                    next_section_pos = len(full_text)
                    
                    # Look for the next major section
                    for other_section, other_patterns in self.SECTION_PATTERNS.items():
                        if other_section == section_name:
                            continue
                        
                        for other_pattern in other_patterns:
                            next_matches = list(re.finditer(other_pattern, text_lower[start_pos + 100:], re.IGNORECASE))
                            if next_matches:
                                candidate_pos = start_pos + 100 + next_matches[0].start()
                                if candidate_pos < next_section_pos:
                                    next_section_pos = candidate_pos
                    
                    # Extract section content
                    section_content = full_text[start_pos:next_section_pos].strip()
                    
                    # Clean up the content
                    section_content = self._clean_section_content(section_content)
                    
                    # Only add if we have meaningful content (not just table of contents)
                    if len(section_content) > 500:  # Increased minimum for actual content
                        # Check if this looks like table of contents (lots of numbers/page refs)
                        lines = section_content.split('\n')[:10]  # First 10 lines
                        toc_indicators = sum(1 for line in lines if re.search(r'\d+\s*$', line.strip()))
                        
                        # If more than 30% of lines end with numbers, probably TOC
                        if toc_indicators / len(lines) < 0.3:
                            sections.append(DocumentSection(
                                name=section_name.replace('_', ' ').title(),
                                content=section_content,
                                confidence=0.8 if toc_indicators == 0 else 0.6
                            ))
                            break  # Stop after finding first match for this section
        
        return sections
    
    def _clean_section_content(self, content: str) -> str:
        """Clean section content by removing excessive whitespace and formatting artifacts."""
        # Remove excessive whitespace
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
        content = re.sub(r'\s+', ' ', content)
        
        # Remove page markers
        content = re.sub(r'--- PAGE \d+ ---', '', content)
        
        # Remove table of contents entries
        content = re.sub(r'\.\.\.\.\.\.\.\.\d+', '', content)
        
        return content.strip()
    
    def get_section_summary(self, doc: ProcessedDocument, section_name: str, max_words: int = 200) -> str:
        """Get a summary of a specific section."""
        section_name_lower = section_name.lower().replace(' ', '_')
        
        for section in doc.sections:
            if section.name.lower().replace(' ', '_') == section_name_lower:
                words = section.content.split()
                if len(words) <= max_words:
                    return section.content
                else:
                    return ' '.join(words[:max_words]) + '...'
        
        return f"Section '{section_name}' not found in document."
    
    def print_document_summary(self, doc: ProcessedDocument):
        """Print a summary of the processed document."""
        print(f"\n{'='*60}")
        print(f"DOCUMENT SUMMARY: {doc.filename}")
        print(f"{'='*60}")
        print(f"Type: {doc.document_type}")
        print(f"Company: {doc.company_name or 'Unknown'}")
        print(f"Filing Type: {doc.filing_type or 'Unknown'}")
        print(f"Filing Date: {doc.filing_date or 'Unknown'}")
        print(f"Total Text Length: {len(doc.full_text):,} characters")
        print(f"Sections Found: {len(doc.sections)}")
        
        for section in doc.sections:
            print(f"\n--- {section.name} ---")
            print(f"Length: {len(section.content):,} characters")
            # Show first 200 characters
            preview = section.content[:200].replace('\n', ' ')
            print(f"Preview: {preview}...")


# Example usage and testing
if __name__ == "__main__":
    processor = DocumentProcessor()
    print(f"Supported formats: {processor.supported_formats}")
    
    # Test with a sample file (you'll need to provide this)
    # doc = processor.process_file("sample_10k.pdf")
    # processor.print_document_summary(doc)