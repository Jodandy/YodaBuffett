#!/usr/bin/env python3
"""
Run Historical Ingestion for Companies with Zero Documents (Non-interactive)
"""
import json
import asyncio
from datetime import datetime
import sys
import os
import subprocess

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def get_zero_document_companies():
    """Extract companies with 0 documents from latest results"""
    
    # Find the most recent ingestion results file
    result_files = [f for f in os.listdir('.') if f.startswith('historical_ingestion_') and f.endswith('.json')]
    
    if not result_files:
        print("❌ No historical ingestion results found")
        return []
    
    # Sort by timestamp and get the latest
    latest_file = sorted(result_files)[-1]
    print(f"📄 Reading from: {latest_file}")
    
    with open(latest_file, 'r') as f:
        data = json.load(f)
    
    zero_doc_companies = []
    
    # Check completed companies
    for company in data.get('completed', []):
        if company.get('documents', 0) == 0:
            zero_doc_companies.append(company['company'])
    
    # Also include failed companies (but skip "no_items_found" ones)
    for company in data.get('failed', []):
        if 'no_items_found' not in company.get('failure_reason', ''):
            zero_doc_companies.append(company['company'])
    
    print(f"📊 Found {len(zero_doc_companies)} companies with 0 documents")
    
    # Show stats about why they have 0 documents
    stats = {"high_duplicates": 0, "high_errors": 0, "no_pdfs": 0}
    
    for company in data.get('completed', []):
        if company.get('documents', 0) == 0:
            details = company.get('details', {})
            if details.get('document_duplicates', 0) > 100:
                stats["high_duplicates"] += 1
            elif details.get('document_errors', 0) > 100:
                stats["high_errors"] += 1
            else:
                stats["no_pdfs"] += 1
    
    print(f"\n📈 Zero document reasons:")
    print(f"   • High duplicates (likely unknown domains): {stats['high_duplicates']}")
    print(f"   • High errors (company name mismatch): {stats['high_errors']}")
    print(f"   • No PDFs found: {stats['no_pdfs']}")
    
    return zero_doc_companies

def main():
    """Main execution"""
    print("🎯 Historical Ingestion for Zero-Document Companies")
    print("=" * 55)
    
    # Get companies with zero documents
    zero_doc_companies = get_zero_document_companies()
    
    if not zero_doc_companies:
        print("✅ No companies with zero documents found!")
        return
    
    # Show sample of companies
    print(f"\n📋 Will process {len(zero_doc_companies)} companies:")
    for i, company in enumerate(zero_doc_companies[:5]):
        print(f"   • {company}")
    if len(zero_doc_companies) > 5:
        print(f"   ... and {len(zero_doc_companies) - 5} more")
    
    # Create company list file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    company_file = f"zero_doc_companies_{timestamp}.txt"
    
    with open(company_file, 'w') as f:
        for company in zero_doc_companies:
            f.write(f"{company}\n")
    
    print(f"\n📝 Created company list: {company_file}")
    
    # Calculate estimated time
    # 15s between companies + avg 10s per company = ~25s per company
    estimated_hours = (len(zero_doc_companies) * 25) / 3600
    print(f"⏱️  Estimated time: {estimated_hours:.1f} hours")
    
    print("\n🚀 Starting historical ingestion in 5 seconds...")
    print("   (Press Ctrl+C to cancel)")
    
    try:
        import time
        time.sleep(5)
    except KeyboardInterrupt:
        print("\n❌ Cancelled by user")
        os.remove(company_file)
        return
    
    # Run historical ingestion
    print("\n" + "=" * 55)
    
    cmd = [
        'python3', 
        'historical_ingestion_batch.py',
        '--companies', company_file
    ]
    
    try:
        result = subprocess.run(cmd, check=False)
        if result.returncode == 0:
            print("\n✅ Historical ingestion completed!")
        else:
            print(f"\n⚠️ Historical ingestion exited with code: {result.returncode}")
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
    finally:
        # Clean up temporary file
        try:
            os.remove(company_file)
            print(f"🗑️ Cleaned up: {company_file}")
        except:
            pass

if __name__ == "__main__":
    main()