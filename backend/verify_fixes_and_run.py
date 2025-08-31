#!/usr/bin/env python3
"""
Verify MFN Collector Fixes and Run Targeted Collection

Simple script to verify fixes are in place and run historical ingestion on problematic companies.
"""

import os
import sys
import subprocess
from datetime import datetime

def verify_fixes():
    """Verify that both fixes are in place"""
    
    print("🔍 Verifying MFN Collector Fixes...")
    
    mfn_file = "nordic_ingestion/collectors/aggregator/mfn_collector.py"
    
    if not os.path.exists(mfn_file):
        print(f"❌ MFN collector file not found: {mfn_file}")
        return False
    
    with open(mfn_file, 'r') as f:
        content = f.read()
    
    # Check for storage.mfn.se fix
    storage_fix_found = 'storage\\.mfn\\.se' in content
    
    # Check for slug resolution fix
    slug_fix_found = '_resolve_company_slug' in content
    
    print(f"   ✅ Storage.mfn.se fix: {'Present' if storage_fix_found else '❌ Missing'}")
    print(f"   ✅ Slug resolution fix: {'Present' if slug_fix_found else '❌ Missing'}")
    
    if storage_fix_found and slug_fix_found:
        print("   🎉 Both fixes are in place!")
        return True
    else:
        print("   ❌ Some fixes are missing!")
        return False

def get_problematic_company_slugs():
    """Get list of company slugs that likely had problems"""
    
    # Companies that likely need slug resolution or have storage.mfn.se documents
    # Based on common Swedish company naming patterns and known document hosting
    problematic_companies = [
        # Known to need slug variations
        "absolent-air-care",      # Likely needs -group
        "yubico",                 # Likely needs -ab
        "embracer-group",         # Gaming, complex hosting
        "sinch",                  # Tech company
        "kindred-group",          # Gaming/betting
        "betsson",                # Gaming/betting
        "evolution",              # Gaming
        "storytel",               # Media/streaming
        
        # Large companies likely using storage.mfn.se
        "volvo",
        "atlas-copco", 
        "sandvik",
        "ericsson",
        "hexagon",
        "skf",
        "electrolux",
        "securitas",
        "nibe-industrier",
        "getinge",
        "alfa-laval",
        
        # Companies with complex names
        "swedish-match",
        "swedish-orphan-biovitrum",
        "lundin-petroleum",
        "kinnevik",
        "investor",
        "latour",
        
        # Industrial/tech companies
        "addtech",
        "indutrade",
        "lifco",
        "bufab",
        "lagercrantz-group",
        "beijer-ref",
        "ncc",
        "skanska",
    ]
    
    return problematic_companies

def run_targeted_ingestion(company_slugs, limit=None):
    """Run historical ingestion on specific companies"""
    
    if limit:
        company_slugs = company_slugs[:limit]
    
    print(f"\n🚀 Running targeted historical ingestion on {len(company_slugs)} companies...")
    print("Companies to test:", ", ".join(company_slugs[:5]) + ("..." if len(company_slugs) > 5 else ""))
    
    # Create temporary file with company list
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_file = f"temp_companies_{timestamp}.txt"
    
    try:
        with open(temp_file, 'w') as f:
            for slug in company_slugs:
                f.write(slug + '\n')
        
        print(f"   📄 Company list saved to: {temp_file}")
        
        # Run historical ingestion with company list
        print("\n🔄 Starting historical ingestion...")
        print("=" * 50)
        
        cmd = ['python3', 'historical_ingestion_batch.py', '--companies', temp_file, '--delay', '3']
        result = subprocess.run(cmd, capture_output=False)
        
        print("\n" + "=" * 50)
        
        if result.returncode == 0:
            print("✅ Historical ingestion completed!")
            print(f"\n📊 Next steps:")
            print("1. Check the results JSON file for improvements")
            print("2. Look for storage.mfn.se URLs in the logs") 
            print("3. Check for slug resolution messages")
            print("4. Run analyze_ingestion_results.py to see statistics")
        else:
            print("⚠️  Historical ingestion had issues (check logs)")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"❌ Error running ingestion: {e}")
        return False
    
    finally:
        # Clean up temp file
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
                print(f"   🗑️  Cleaned up: {temp_file}")
        except:
            pass

def main():
    """Main execution"""
    
    print("🎯 MFN Collector Fix Verification & Targeted Retry")
    print("=" * 55)
    
    # Step 1: Verify fixes are in place
    if not verify_fixes():
        print("\n❌ Fixes are not in place. Please check the MFN collector file.")
        return
    
    # Step 2: Get problematic companies
    companies = get_problematic_company_slugs()
    
    print(f"\n📊 Identified {len(companies)} potentially problematic companies")
    print("These companies likely:")
    print("   • Need slug resolution (absolent-air-care → absolent-air-care-group)")
    print("   • Have documents on storage.mfn.se that were missed")
    print("   • Are large companies with complex document hosting")
    
    # Step 3: Ask user what to do
    print(f"\nOptions:")
    print(f"1. Test small batch (10 companies) - Quick test")
    print(f"2. Test medium batch (25 companies) - Good coverage") 
    print(f"3. Test all companies ({len(companies)}) - Complete test")
    print(f"4. Skip and run full historical ingestion")
    
    try:
        choice = input("\nEnter choice (1-4): ").strip()
        
        if choice == '1':
            success = run_targeted_ingestion(companies, limit=10)
        elif choice == '2':
            success = run_targeted_ingestion(companies, limit=25)
        elif choice == '3':
            success = run_targeted_ingestion(companies)
        elif choice == '4':
            print("\n🚀 Running full historical ingestion...")
            cmd = ['python3', 'historical_ingestion_batch.py', '--delay', '2']
            result = subprocess.run(cmd)
            success = result.returncode == 0
        else:
            print("Invalid choice")
            return
        
        # Final summary
        if success:
            print(f"\n🎉 SUCCESS!")
            print("The fixes should have improved document collection.")
            print("\nTo see the improvements:")
            print("   python3 analyze_ingestion_results.py")
            print("   # Look for:")
            print("   #   - Higher document counts")
            print("   #   - storage.mfn.se URLs in logs") 
            print("   #   - Slug resolution messages")
        else:
            print(f"\n⚠️  There were some issues.")
            print("Check the log files for specific error messages.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()