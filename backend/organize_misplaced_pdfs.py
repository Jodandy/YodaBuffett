#!/usr/bin/env python3
"""
PDF Organization Script - Fix Misplaced Company PDFs
Detects PDFs in wrong company folders and moves them to correct locations
"""
import os
import re
import json
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PDFOrganizer:
    """Organizes misplaced PDFs in company data structure"""
    
    def __init__(self, data_root: str = "data/companies/SE"):
        self.data_root = Path(data_root)
        self.moves_log = []
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Company name variations and mappings
        self.company_mappings = self._build_company_mappings()
        
    def _build_company_mappings(self) -> Dict[str, str]:
        """Build mapping from PDF name patterns to correct folder names"""
        
        # Read existing company folders to build mapping
        company_folders = {}
        
        if not self.data_root.exists():
            logger.error(f"Data root not found: {self.data_root}")
            return {}
        
        # Scan all existing company folders
        for letter_dir in self.data_root.iterdir():
            if not letter_dir.is_dir() or len(letter_dir.name) != 1:
                continue
                
            for company_dir in letter_dir.iterdir():
                if not company_dir.is_dir():
                    continue
                    
                company_name = company_dir.name
                company_folders[company_name] = str(company_dir)
                
                # Add common name variations
                variations = self._generate_name_variations(company_name)
                for variation in variations:
                    company_folders[variation] = str(company_dir)
        
        logger.info(f"Built mapping for {len(company_folders)} company name variations")
        return company_folders
    
    def _generate_name_variations(self, company_name: str) -> List[str]:
        """Generate common variations of company names for matching"""
        variations = []
        
        # Handle specific known patterns - EXACT MATCHES ONLY
        name_patterns = {
            'Atlas_Copco_AB': ['atlas-copco', 'atlas copco', 'atlascopco'],
            'Telefonaktiebolaget_LM_Ericsson': ['ericsson', 'lm-ericsson', 'telefonaktiebolaget-lm-ericsson'],
            'HM_Hennes__Mauritz_AB': ['handm', 'hm', 'hennes-mauritz', 'hennes mauritz'],
            'Volvo_Group': ['volvo'],
            'Volvo_Car': ['volvo-car'],
            'Hexatronic': ['hexatronic'],
            'BE_Group': ['be-group'],  # Remove generic 'be' - too dangerous
            'Thunderful': ['thunderful'],
            'GomSpace': ['gomspace'],
            'Genova_Property': ['genova-property'],  # Remove generic 'genova'
            'Freja_eID': ['freja-eid'],  # Remove generic 'freja'
            'Embellence_Group': ['embellence'],
            'Stillfront_Group': ['stillfront'],
            'Vitec_Software': ['vitec-software'],  # Remove generic 'vitec'
            'Scandic_Hotels': ['scandic-hotels'],  # Remove generic 'scandic'
            'Fractal_Gaming': ['fractal-gaming'],
            'Cyber_Security_1': ['cyber-security-1', 'cyber1'],  # Add correct patterns
        }
        
        if company_name in name_patterns:
            variations.extend(name_patterns[company_name])
        
        return variations
    
    def extract_company_from_filename(self, filename: str) -> Optional[str]:
        """Extract company name from PDF filename with precise matching"""
        
        # Remove date prefix and extension
        clean_name = re.sub(r'^\d{4}-\d{2}-\d{2}-', '', filename.lower())
        clean_name = clean_name.replace('.pdf', '')
        
        # Build reverse mapping: pattern -> company folder
        pattern_to_company = {}
        for letter_dir in self.data_root.iterdir():
            if not letter_dir.is_dir() or len(letter_dir.name) != 1:
                continue
            for company_dir in letter_dir.iterdir():
                if not company_dir.is_dir():
                    continue
                company_name = company_dir.name
                variations = self._generate_name_variations(company_name)
                for variation in variations:
                    pattern_to_company[variation] = company_name
        
        # Look for exact pattern matches with word boundaries
        best_match = None
        longest_match = 0
        
        for pattern, company in pattern_to_company.items():
            # Use word boundary matching for precision
            if re.search(rf'\\b{re.escape(pattern)}\\b', clean_name):
                if len(pattern) > longest_match:
                    best_match = company
                    longest_match = len(pattern)
        
        return best_match
    
    def scan_misplaced_pdfs(self) -> List[Dict]:
        """Scan all PDFs and identify misplaced ones"""
        
        misplaced_pdfs = []
        total_pdfs = 0
        
        logger.info("🔍 Scanning for misplaced PDFs...")
        
        for letter_dir in self.data_root.iterdir():
            if not letter_dir.is_dir() or len(letter_dir.name) != 1:
                continue
                
            for company_dir in letter_dir.iterdir():
                if not company_dir.is_dir():
                    continue
                    
                current_company = company_dir.name
                logger.info(f"Checking {current_company}...")
                
                # Scan all PDF files in this company's folders
                for root, dirs, files in os.walk(company_dir):
                    pdf_files = [f for f in files if f.lower().endswith('.pdf')]
                    total_pdfs += len(pdf_files)
                    
                    for pdf_file in pdf_files:
                        # Extract what company this PDF should belong to
                        detected_company = self.extract_company_from_filename(pdf_file)
                        
                        if detected_company and detected_company != current_company:
                            misplaced_pdf = {
                                'current_path': os.path.join(root, pdf_file),
                                'filename': pdf_file,
                                'current_company': current_company,
                                'detected_company': detected_company,
                                'confidence': self._calculate_confidence(pdf_file, detected_company)
                            }
                            misplaced_pdfs.append(misplaced_pdf)
                            
                            logger.info(f"❌ Found misplaced: {pdf_file}")
                            logger.info(f"   Currently in: {current_company}")
                            logger.info(f"   Should be in: {detected_company}")
        
        logger.info(f"📊 Scan Results:")
        logger.info(f"   Total PDFs scanned: {total_pdfs}")
        logger.info(f"   Misplaced PDFs found: {len(misplaced_pdfs)}")
        
        return misplaced_pdfs
    
    def _calculate_confidence(self, filename: str, detected_company: str) -> float:
        """Calculate confidence score for company detection"""
        
        # Higher confidence if company name appears multiple times
        company_lower = detected_company.lower().replace('_', '-')
        filename_lower = filename.lower()
        
        occurrences = filename_lower.count(company_lower.split('_')[0])
        
        # Base confidence
        confidence = 0.7
        
        # Boost for multiple occurrences
        if occurrences > 1:
            confidence += 0.2
        
        # Boost for exact match
        if company_lower in filename_lower:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def move_pdf_safely(self, misplaced_pdf: Dict, dry_run: bool = True) -> bool:
        """Safely move a PDF to its correct location"""
        
        current_path = Path(misplaced_pdf['current_path'])
        detected_company = misplaced_pdf['detected_company']
        
        # Find correct destination folder
        target_folder = None
        for company_name, folder_path in self.company_mappings.items():
            if Path(folder_path).name == detected_company:
                target_folder = Path(folder_path)
                break
        
        if not target_folder:
            logger.error(f"Cannot find target folder for {detected_company}")
            return False
        
        # Preserve the year/document_type structure
        relative_path = current_path.relative_to(current_path.parents[3])  # Remove SE/X/Company/ part
        target_path = target_folder / relative_path.parts[1] / relative_path.parts[2]  # year/doc_type
        
        # Create target directory
        target_path.mkdir(parents=True, exist_ok=True)
        target_file = target_path / current_path.name
        
        if target_file.exists():
            logger.warning(f"Target file already exists: {target_file}")
            return False
        
        move_record = {
            'timestamp': datetime.now().isoformat(),
            'filename': current_path.name,
            'from': str(current_path),
            'to': str(target_file),
            'from_company': misplaced_pdf['current_company'],
            'to_company': detected_company,
            'confidence': misplaced_pdf['confidence'],
            'dry_run': dry_run
        }
        
        if not dry_run:
            try:
                # Create backup log before moving
                self.moves_log.append(move_record)
                
                # Move the file
                shutil.move(str(current_path), str(target_file))
                logger.info(f"✅ Moved: {current_path.name}")
                logger.info(f"   From: {misplaced_pdf['current_company']}")
                logger.info(f"   To: {detected_company}")
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to move {current_path}: {e}")
                return False
        else:
            logger.info(f"🔄 [DRY RUN] Would move: {current_path.name}")
            logger.info(f"   From: {misplaced_pdf['current_company']}")  
            logger.info(f"   To: {detected_company}")
            self.moves_log.append(move_record)
            return True
    
    def organize_pdfs(self, min_confidence: float = 0.7, dry_run: bool = True) -> Dict:
        """Main method to organize all misplaced PDFs"""
        
        logger.info(f"🚀 PDF Organization Started (Session: {self.session_id})")
        logger.info(f"   Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
        logger.info(f"   Minimum confidence: {min_confidence}")
        
        # Scan for misplaced PDFs
        misplaced_pdfs = self.scan_misplaced_pdfs()
        
        if not misplaced_pdfs:
            logger.info("✅ No misplaced PDFs found!")
            return {'moved': 0, 'total': 0, 'session_id': self.session_id}
        
        # Filter by confidence
        high_confidence_pdfs = [pdf for pdf in misplaced_pdfs if pdf['confidence'] >= min_confidence]
        
        logger.info(f"📋 Organization Plan:")
        logger.info(f"   Total misplaced: {len(misplaced_pdfs)}")
        logger.info(f"   High confidence (≥{min_confidence}): {len(high_confidence_pdfs)}")
        
        # Move high-confidence PDFs
        moved_count = 0
        for pdf in high_confidence_pdfs:
            if self.move_pdf_safely(pdf, dry_run=dry_run):
                moved_count += 1
        
        # Save session log
        log_filename = f"pdf_organization_{self.session_id}.json"
        with open(log_filename, 'w') as f:
            json.dump({
                'session_id': self.session_id,
                'timestamp': datetime.now().isoformat(),
                'dry_run': dry_run,
                'min_confidence': min_confidence,
                'total_misplaced': len(misplaced_pdfs),
                'high_confidence': len(high_confidence_pdfs),
                'moved': moved_count,
                'moves': self.moves_log
            }, f, indent=2)
        
        logger.info(f"📄 Session log saved: {log_filename}")
        
        result = {
            'moved': moved_count,
            'total': len(misplaced_pdfs),
            'high_confidence': len(high_confidence_pdfs),
            'session_id': self.session_id,
            'log_file': log_filename
        }
        
        logger.info(f"🎯 Organization Complete!")
        logger.info(f"   Moved: {moved_count}/{len(high_confidence_pdfs)} high-confidence PDFs")
        logger.info(f"   Session: {self.session_id}")
        
        return result

def main():
    """Main execution function"""
    
    print("🗂️  PDF ORGANIZATION TOOL")
    print("=" * 50)
    
    # Initialize organizer
    organizer = PDFOrganizer()
    
    # First, run in dry-run mode to see what would be moved
    print("\\n🔍 ANALYSIS MODE - Scanning for misplaced PDFs...")
    result = organizer.organize_pdfs(min_confidence=0.8, dry_run=True)
    
    if result['high_confidence'] > 0:
        print(f"\\n📋 FOUND {result['high_confidence']} high-confidence misplaced PDFs")
        print("\\nWould you like to:")
        print("1. Execute the moves (LIVE)")
        print("2. View detailed report")
        print("3. Exit")
        
        choice = input("\\nChoice (1-3): ").strip()
        
        if choice == "1":
            print("\\n🚀 EXECUTING MOVES...")
            live_result = organizer.organize_pdfs(min_confidence=0.8, dry_run=False)
            print(f"✅ Successfully moved {live_result['moved']} PDFs")
            
        elif choice == "2":
            log_file = result['log_file']
            print(f"\\n📄 Detailed report saved to: {log_file}")
            
    else:
        print("\\n✅ No high-confidence misplaced PDFs found!")

if __name__ == "__main__":
    main()