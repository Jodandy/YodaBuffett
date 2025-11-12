#!/usr/bin/env python3
"""
Precise PDF Misplacement Detector
Focuses on finding PDFs that clearly belong to other companies based on obvious name matches
"""
import os
import re
from pathlib import Path
from collections import defaultdict

def find_misplaced_pdfs():
    """Find PDFs that are clearly in the wrong company folder"""
    
    data_root = Path("data/companies/SE")
    
    if not data_root.exists():
        print("❌ Data directory not found")
        return
    
    print("🔍 SCANNING FOR CLEARLY MISPLACED PDFs")
    print("=" * 60)
    
    # Build list of all company names
    all_companies = set()
    for letter_dir in data_root.iterdir():
        if not letter_dir.is_dir() or len(letter_dir.name) != 1:
            continue
        for company_dir in letter_dir.iterdir():
            if company_dir.is_dir():
                all_companies.add(company_dir.name.lower().replace('_', '-'))
    
    misplaced_pdfs = []
    total_pdfs = 0
    
    # Scan each company folder
    for letter_dir in data_root.iterdir():
        if not letter_dir.is_dir() or len(letter_dir.name) != 1:
            continue
            
        for company_dir in letter_dir.iterdir():
            if not company_dir.is_dir():
                continue
                
            current_company = company_dir.name
            current_company_patterns = [
                current_company.lower().replace('_', '-'),
                current_company.lower().replace('_', ' '),
                current_company.lower()
            ]
            
            print(f"Checking {current_company}...")
            
            # Scan PDFs in this company folder
            for root, dirs, files in os.walk(company_dir):
                pdf_files = [f for f in files if f.lower().endswith('.pdf')]
                total_pdfs += len(pdf_files)
                
                for pdf_file in pdf_files:
                    # Look for OTHER company names in this PDF filename
                    pdf_lower = pdf_file.lower().replace('_', '-')
                    
                    # Skip if this PDF clearly belongs to current company
                    belongs_here = False
                    for pattern in current_company_patterns:
                        if pattern in pdf_lower:
                            belongs_here = True
                            break
                    
                    if belongs_here:
                        continue
                    
                    # Check if it contains OTHER company names
                    for other_company in all_companies:
                        if other_company == current_company.lower().replace('_', '-'):
                            continue
                            
                        # Look for clear matches with word boundaries
                        if re.search(rf'\\b{re.escape(other_company)}\\b', pdf_lower):
                            # Find the actual company folder name
                            actual_company = find_company_folder_name(data_root, other_company)
                            if actual_company:
                                misplaced_pdfs.append({
                                    'filename': pdf_file,
                                    'current_folder': current_company,
                                    'should_be_in': actual_company,
                                    'path': os.path.join(root, pdf_file),
                                    'pattern_found': other_company
                                })
                                
                                print(f"  ❌ MISPLACED: {pdf_file}")
                                print(f"     Current: {current_company}")
                                print(f"     Should be: {actual_company}")
                                print(f"     Pattern: {other_company}")
                                break
    
    print(f"\\n📊 RESULTS:")
    print(f"   Total PDFs scanned: {total_pdfs}")
    print(f"   Misplaced PDFs found: {len(misplaced_pdfs)}")
    
    # Group by current folder to show problem areas
    by_folder = defaultdict(list)
    for pdf in misplaced_pdfs:
        by_folder[pdf['current_folder']].append(pdf)
    
    print(f"\\n📁 MISPLACEMENT BY FOLDER:")
    for folder, pdfs in sorted(by_folder.items()):
        print(f"   {folder}: {len(pdfs)} misplaced PDFs")
        for pdf in pdfs[:3]:  # Show first 3 examples
            print(f"      • {pdf['filename']} → should be in {pdf['should_be_in']}")
        if len(pdfs) > 3:
            print(f"      ... and {len(pdfs) - 3} more")
    
    return misplaced_pdfs

def find_company_folder_name(data_root: Path, pattern: str) -> str:
    """Find the actual company folder name for a given pattern"""
    for letter_dir in data_root.iterdir():
        if not letter_dir.is_dir() or len(letter_dir.name) != 1:
            continue
        for company_dir in letter_dir.iterdir():
            if company_dir.is_dir():
                if company_dir.name.lower().replace('_', '-') == pattern:
                    return company_dir.name
    return None

if __name__ == "__main__":
    find_misplaced_pdfs()