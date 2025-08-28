#!/usr/bin/env python3

from src.scraper import SwedishReportScraper
import time

def test_scraper():
    """Test the Swedish report scraper"""
    
    scraper = SwedishReportScraper()
    
    # Test companies and years
    test_cases = [
        ("Volvo", 2024),
        ("H&M", 2024),
    ]
    
    for company, year in test_cases:
        print(f"\n=== Testing {company} for {year} ===")
        
        try:
            reports = scraper.scrape_company(company, year)
            print(f"Found {len(reports)} reports")
            
            for report in reports:
                print(f"  - {report.report_type}: {report.url}")
                
            # Test downloading one report if found
            if reports and len(reports) > 0:
                print(f"\nTesting download of first report...")
                success = scraper.download_report(reports[0])
                if success:
                    print("✓ Download successful")
                else:
                    print("✗ Download failed")
                    
        except Exception as e:
            print(f"Error testing {company}: {e}")
            
        time.sleep(2)  # Be polite to servers

if __name__ == "__main__":
    test_scraper()