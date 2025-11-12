#!/usr/bin/env python3
"""
Simple Direct PDF Misplacement Detector
Checks for obvious company names that shouldn't be in certain folders
"""
import os
from pathlib import Path

def check_be_group_misplacements():
    """Check BE_Group folder for obvious misplacements"""
    
    be_group_path = Path("data/companies/SE/B/BE_Group")
    
    if not be_group_path.exists():
        print("❌ BE_Group folder not found")
        return []
    
    print("🔍 CHECKING BE_GROUP FOR OBVIOUS MISPLACEMENTS")
    print("=" * 60)
    
    # These company names should NEVER be in BE_Group
    wrong_companies = [
        'gomspace', 'thunderful', 'genova', 'freja', 'embellence',
        'vitec', 'scandic', 'fractal', 'stillfront', 'waystream',
        'dedicare', 'powercell', 'hexatronic', 'lammhults', 'proact',
        'railcare', 'transtema', 'clemondo', 'rugvista', 'mysafety',
        'nordic-flanges', 'everysport', 'infracom', 'ecoclime',
        'sensys-gatso', 'blick-global', 'wonderful-times', 'embracer',
        'suntrade', 'lagercrantz', 'antco', 'northbaze', 'mediacle',
        'rightbridge', 'embeddedart', 'taurus', 'stenvalvet'
    ]
    
    misplaced_pdfs = []
    total_pdfs = 0
    
    for root, dirs, files in os.walk(be_group_path):
        pdf_files = [f for f in files if f.lower().endswith('.pdf')]
        total_pdfs += len(pdf_files)
        
        for pdf_file in pdf_files:
            pdf_lower = pdf_file.lower()
            
            # Check for any of the wrong company names
            for wrong_company in wrong_companies:
                if wrong_company in pdf_lower:
                    misplaced_pdfs.append({
                        'filename': pdf_file,
                        'path': os.path.join(root, pdf_file),
                        'wrong_company': wrong_company,
                        'relative_path': os.path.relpath(os.path.join(root, pdf_file), be_group_path)
                    })
                    
                    print(f"❌ {pdf_file}")
                    print(f"   Contains: {wrong_company}")
                    print(f"   Path: {os.path.relpath(os.path.join(root, pdf_file), be_group_path)}")
                    print()
                    break
    
    print(f"📊 BE_GROUP SUMMARY:")
    print(f"   Total PDFs: {total_pdfs}")
    print(f"   Obviously misplaced: {len(misplaced_pdfs)}")
    print(f"   Error rate: {len(misplaced_pdfs)/total_pdfs*100:.1f}%")
    
    return misplaced_pdfs

def check_recent_misplacements():
    """Check for very recent misplacements (2025 files)"""
    
    print("\\n🕐 CHECKING FOR RECENT MISPLACEMENTS (2025 files)")
    print("=" * 60)
    
    be_group_path = Path("data/companies/SE/B/BE_Group")
    recent_misplaced = []
    
    if be_group_path.exists():
        for root, dirs, files in os.walk(be_group_path):
            pdf_files = [f for f in files if f.lower().endswith('.pdf') and f.startswith('2025-')]
            
            for pdf_file in pdf_files:
                # These are definitely recent and likely misplaced if they contain other company names
                if any(company in pdf_file.lower() for company in ['gomspace', 'thunderful', 'genova', 'freja']):
                    recent_misplaced.append({
                        'filename': pdf_file,
                        'path': os.path.join(root, pdf_file)
                    })
                    
                    print(f"🚨 RECENT MISPLACEMENT: {pdf_file}")
                    print(f"   This suggests the attribution fix isn't working!")
                    print()
    
    return recent_misplaced

def suggest_moves():
    """Suggest where misplaced PDFs should actually go"""
    
    # Simple mapping of patterns to likely correct folders
    company_suggestions = {
        'gomspace': 'GomSpace',
        'thunderful': 'Thunderful', 
        'genova': 'Genova_Property',
        'freja': 'Freja_eID',
        'embellence': 'Embellence_Group',
        'vitec': 'Vitec_Software',
        'scandic': 'Scandic_Hotels',
        'fractal': 'Fractal_Gaming',
        'stillfront': 'Stillfront_Group',
        'hexatronic': 'Hexatronic',
        'lammhults': 'Lammhults_Design',
        'powercell': 'PowerCell',
        'dedicare': 'Dedicare',
        'proact': 'Proact_IT',
        'railcare': 'Railcare',
        'transtema': 'Transtema',
    }
    
    return company_suggestions

def main():
    """Main function"""
    
    # Check BE_Group for obvious problems
    misplaced = check_be_group_misplacements()
    
    # Check for recent issues
    recent = check_recent_misplacements()
    
    if misplaced or recent:
        print("\\n💡 SUGGESTED ACTIONS:")
        print("1. Fix the ingestion attribution system (still creating new misplacements!)")
        print("2. Move the obviously misplaced PDFs to correct folders")
        
        suggestions = suggest_moves()
        print("\\n📁 WHERE THEY SHOULD GO:")
        
        patterns_found = set()
        for pdf in misplaced:
            patterns_found.add(pdf['wrong_company'])
        
        for pattern in sorted(patterns_found):
            if pattern in suggestions:
                print(f"   {pattern} files → data/companies/SE/*/{suggestions[pattern]}/")
    
    else:
        print("\\n✅ No obvious misplacements found in BE_Group")

if __name__ == "__main__":
    main()