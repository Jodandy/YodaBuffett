#!/usr/bin/env python3
"""
Analyze Historical Ingestion Results
Quick analysis tool for batch ingestion result files
"""
import json
import glob
import sys
from datetime import datetime

def analyze_latest_results():
    """Analyze the most recent ingestion results"""
    
    # Find most recent results file
    result_files = glob.glob("historical_ingestion_*.json")
    if not result_files:
        print("❌ No ingestion result files found")
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
    print(f"📊 OVERVIEW:")
    print(f"   Session: {results.get('session_id', 'Unknown')}")
    print(f"   Start: {results.get('start_time', 'Unknown')}")
    print(f"   Duration: {stats.get('processing_time_seconds', 0)/60:.1f} minutes")
    print(f"   Total Companies: {stats.get('total_companies', 0)}")
    
    print(f"\n✅ SUCCESS SUMMARY:")
    print(f"   Completed: {stats.get('completed_count', 0)}")
    print(f"   Total Documents: {stats.get('total_documents', 0):,}")
    print(f"   Total Calendar Events: {stats.get('total_calendar_events', 0):,}")
    
    # Failure analysis
    failed = results.get('failed', [])
    print(f"\n❌ FAILURE ANALYSIS:")
    print(f"   Failed Companies: {len(failed)}")
    
    if failed:
        # Group by failure reason
        failure_reasons = {}
        for failure in failed:
            reason = failure.get('failure_reason', 'unknown')
            if reason not in failure_reasons:
                failure_reasons[reason] = []
            failure_reasons[reason].append(failure)
        
        for reason, failures in failure_reasons.items():
            print(f"\n   🔴 {reason.upper().replace('_', ' ')}: {len(failures)} companies")
            
            # Show first few companies as examples
            for i, failure in enumerate(failures[:5]):
                company_name = failure.get('company_name', failure.get('company', 'Unknown'))
                print(f"      • {company_name}")
            
            if len(failures) > 5:
                print(f"      ... and {len(failures) - 5} more")
    
    # Top performers
    completed = results.get('completed', [])
    if completed:
        print(f"\n🏆 TOP DOCUMENT COLLECTORS:")
        # Sort by documents collected
        top_companies = sorted(completed, key=lambda x: x.get('documents', 0), reverse=True)[:10]
        
        for i, company in enumerate(top_companies, 1):
            company_name = company.get('company_name', company.get('company', 'Unknown'))
            docs = company.get('documents', 0)
            events = company.get('events', 0)
            time_taken = company.get('processing_time', 0)
            print(f"   {i:2}. {company_name}: {docs:,} docs, {events} events ({time_taken:.1f}s)")
    
    # Show current status if in progress
    in_progress = results.get('in_progress')
    if in_progress:
        print(f"\n⏳ CURRENTLY PROCESSING: {in_progress}")
    
    print(f"\n📁 LOG FILE: {latest_file.replace('.json', '.log')}")

def show_failure_details():
    """Show detailed failure information"""
    result_files = glob.glob("historical_ingestion_*.json")
    if not result_files:
        print("❌ No ingestion result files found")
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
        company_name = failure.get('company_name', failure.get('company', 'Unknown'))
        reason = failure.get('failure_reason', 'unknown')
        error = failure.get('error', 'No details')
        processing_time = failure.get('processing_time', 0)
        
        print(f"\n{i:2}. {company_name}")
        print(f"    Reason: {reason.replace('_', ' ').title()}")
        print(f"    Error: {error}")
        print(f"    Time: {processing_time:.1f}s")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--failures":
        show_failure_details()
    else:
        analyze_latest_results()
        print(f"\n💡 Use --failures flag to see detailed failure information")