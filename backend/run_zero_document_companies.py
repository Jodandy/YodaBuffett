#!/usr/bin/env python3
"""
Run Historical Ingestion for Companies with Zero Documents
Only processes companies that currently have no documents stored
"""
import json
import asyncio
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

async def get_zero_document_companies():
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
    
    # Also include failed companies (they have 0 documents by definition)
    for company in data.get('failed', []):
        # Only include if it's not a "company doesn't exist" error
        if 'no_items_found' not in company.get('failure_reason', ''):
            zero_doc_companies.append(company['company'])
    
    print(f"\n📊 Found {len(zero_doc_companies)} companies with 0 documents")
    
    return zero_doc_companies

async def create_company_list_file(companies):
    """Create a temporary file with the company list"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"zero_doc_companies_{timestamp}.txt"
    
    with open(filename, 'w') as f:
        for company in companies:
            f.write(f"{company}\n")
    
    print(f"📝 Created company list: {filename}")
    return filename

async def main():
    """Main execution"""
    print("🎯 Historical Ingestion for Zero-Document Companies")
    print("=" * 55)
    
    # Get companies with zero documents
    zero_doc_companies = await get_zero_document_companies()
    
    if not zero_doc_companies:
        print("✅ No companies with zero documents found!")
        return
    
    # Show sample of companies
    print("\n📋 Companies to process:")
    for i, company in enumerate(zero_doc_companies[:10]):
        print(f"   • {company}")
    if len(zero_doc_companies) > 10:
        print(f"   ... and {len(zero_doc_companies) - 10} more")
    
    # Ask for confirmation
    print(f"\n🤔 Process {len(zero_doc_companies)} companies with 0 documents?")
    print("   Note: This will use the new centralized company mappings")
    print("   Note: Delays are set to 3s within company, 15s between companies")
    
    response = input("\nContinue? (y/n): ").strip().lower()
    if response != 'y':
        print("❌ Cancelled")
        return
    
    # Create company list file
    company_file = await create_company_list_file(zero_doc_companies)
    
    # Run historical ingestion
    print("\n🚀 Starting historical ingestion...")
    print("=" * 55)
    
    import subprocess
    cmd = [
        'python3', 
        'historical_ingestion_batch.py',
        '--companies', company_file,
        '--delay', '3'  # Conservative delay
    ]
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n✅ Historical ingestion completed!")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Historical ingestion failed: {e}")
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
    asyncio.run(main())