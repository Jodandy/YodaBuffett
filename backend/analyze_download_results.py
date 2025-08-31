#!/usr/bin/env python3
"""
Analyze PDF Download Results
Quick analysis tool for PDF download batch result files
"""
import json
import glob
import sys
from datetime import datetime
from pathlib import Path

def analyze_latest_results():
    """Analyze the most recent PDF download results"""
    
    # Find most recent results file
    result_files = glob.glob("pdf_download_*.json")
    if not result_files:
        print("❌ No PDF download result files found")
        return
        
    latest_file = max(result_files, key=lambda f: f.split('_')[2].split('.')[0])
    
    print(f"📄 Analyzing: {latest_file}")
    print("="*60)
    
    try:
        with open(latest_file, 'r') as f:
            results = json.load(f)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    # Basic stats
    stats = results.get('stats', {})
    filters = results.get('filters', {})
    
    print(f"📊 OVERVIEW:")
    print(f"   Session: {results.get('session_id', 'Unknown')}")
    print(f"   Start: {results.get('start_time', 'Unknown')}")
    print(f"   Duration: {stats.get('processing_time_seconds', 0)/60:.1f} minutes")
    print(f"   Filters: Year {filters.get('target_year', 'All')}, Company: {filters.get('target_company', 'All')}")
    print(f"   Total Documents: {stats.get('total_documents', 0)}")
    
    print(f"\n✅ DOWNLOAD SUCCESS:")
    print(f"   Downloaded: {stats.get('downloaded_count', 0)}")
    print(f"   Total Size: {stats.get('total_size_mb', 0):.1f} MB")
    
    # Calculate average file size and speed
    downloaded_count = stats.get('downloaded_count', 0)
    if downloaded_count > 0:
        avg_size = stats.get('total_size_mb', 0) / downloaded_count
        print(f"   Average File Size: {avg_size:.1f} MB")
    
    processing_time = stats.get('processing_time_seconds', 0)
    if processing_time > 0:
        download_speed = stats.get('total_size_mb', 0) / (processing_time / 60)  # MB/min
        print(f"   Download Speed: {download_speed:.1f} MB/min")
    
    # Failure analysis
    failed = results.get('failed', [])
    print(f"\n❌ FAILURE ANALYSIS:")
    print(f"   Failed Downloads: {len(failed)}")
    
    if failed:
        # Group by failure reason
        failure_reasons = {}
        for failure in failed:
            reason = failure.get('failure_reason', 'unknown')
            if reason not in failure_reasons:
                failure_reasons[reason] = []
            failure_reasons[reason].append(failure)
        
        for reason, failures in failure_reasons.items():
            print(f"\n   🔴 {reason.upper().replace('_', ' ')}: {len(failures)} documents")
            
            # Show first few documents as examples
            for i, failure in enumerate(failures[:3]):
                company_name = failure.get('company_name', failure.get('document_id', 'Unknown'))
                error = failure.get('error', 'No details')[:50]
                print(f"      • {company_name}: {error}{'...' if len(failure.get('error', '')) > 50 else ''}")
            
            if len(failures) > 3:
                print(f"      ... and {len(failures) - 3} more")
    
    # Top downloads by size
    downloaded = results.get('downloaded', [])
    if downloaded:
        print(f"\n🏆 LARGEST DOWNLOADS:")
        # Sort by file size
        largest_files = sorted(downloaded, key=lambda x: x.get('file_size_mb', 0), reverse=True)[:10]
        
        for i, doc in enumerate(largest_files, 1):
            company_name = doc.get('company_name', doc.get('document_id', 'Unknown'))
            size_mb = doc.get('file_size_mb', 0)
            title = doc.get('title', 'Unknown')[:40]
            processing_time = doc.get('processing_time', 0)
            print(f"   {i:2}. {company_name}: {size_mb:.1f}MB - {title}... ({processing_time:.1f}s)")
    
    # Storage analysis
    if downloaded:
        print(f"\n📂 STORAGE ANALYSIS:")
        
        # Group by company
        companies = {}
        for doc in downloaded:
            company = doc.get('company_name', 'Unknown').split(' (')[0]  # Remove ticker
            if company not in companies:
                companies[company] = {'count': 0, 'size': 0}
            companies[company]['count'] += 1
            companies[company]['size'] += doc.get('file_size_mb', 0)
        
        # Show top companies by document count
        top_companies = sorted(companies.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        print(f"   📊 Documents per company:")
        for company, data in top_companies:
            print(f"      {company}: {data['count']} docs, {data['size']:.1f}MB")
    
    # Show current status if in progress
    in_progress = results.get('in_progress')
    if in_progress:
        print(f"\n⏳ CURRENTLY PROCESSING: {in_progress}")
    
    print(f"\n📁 LOG FILE: {latest_file.replace('.json', '.log')}")

def show_storage_structure():
    """Show the storage directory structure"""
    storage_path = Path("data/companies")
    
    if not storage_path.exists():
        print("❌ Storage directory not found: data/companies/")
        return
    
    print(f"📂 STORAGE STRUCTURE:")
    print("="*50)
    
    total_files = 0
    total_size = 0
    
    # Walk through storage structure
    for country_dir in storage_path.iterdir():
        if not country_dir.is_dir():
            continue
            
        print(f"\n🌍 {country_dir.name}/")
        
        for letter_dir in country_dir.iterdir():
            if not letter_dir.is_dir():
                continue
                
            for company_dir in letter_dir.iterdir():
                if not company_dir.is_dir():
                    continue
                
                company_files = 0
                company_size = 0
                
                # Count files in all subdirectories
                for root, dirs, files in company_dir.rglob("*"):
                    for file in files:
                        if file.endswith('.pdf'):
                            file_path = Path(root) / file
                            try:
                                size = file_path.stat().st_size
                                company_files += 1
                                company_size += size
                            except:
                                pass
                
                if company_files > 0:
                    print(f"  📁 {company_dir.name}: {company_files} PDFs, {company_size / (1024*1024):.1f}MB")
                    total_files += company_files
                    total_size += company_size
    
    print(f"\n📊 TOTAL: {total_files} PDF files, {total_size / (1024*1024):.1f}MB")

def show_failure_details():
    """Show detailed failure information"""
    result_files = glob.glob("pdf_download_*.json")
    if not result_files:
        print("❌ No PDF download result files found")
        return
        
    latest_file = max(result_files, key=lambda f: f.split('_')[2].split('.')[0])
    
    try:
        with open(latest_file, 'r') as f:
            results = json.load(f)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return
    
    failed = results.get('failed', [])
    if not failed:
        print("✅ No failures to show!")
        return
    
    print(f"❌ DETAILED FAILURE REPORT")
    print("="*60)
    
    for i, failure in enumerate(failed, 1):
        company_name = failure.get('company_name', failure.get('document_id', 'Unknown'))
        reason = failure.get('failure_reason', 'unknown')
        error = failure.get('error', 'No details')
        processing_time = failure.get('processing_time', 0)
        title = failure.get('title', 'Unknown')
        
        print(f"\n{i:2}. {company_name}")
        print(f"    Title: {title[:60]}...")
        print(f"    Reason: {reason.replace('_', ' ').title()}")
        print(f"    Error: {error}")
        print(f"    Time: {processing_time:.1f}s")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--failures":
            show_failure_details()
        elif sys.argv[1] == "--storage":
            show_storage_structure()
        else:
            print("Usage: python3 analyze_download_results.py [--failures|--storage]")
    else:
        analyze_latest_results()
        print(f"\n💡 Use --failures flag to see detailed failure information")
        print(f"💡 Use --storage flag to see storage directory structure")