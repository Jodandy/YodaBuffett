#!/usr/bin/env python3

import requests
import os
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Optional
import json
import re
from urllib.parse import urlparse


@dataclass
class ReportSource:
    company_name: str
    report_type: str  # 'Q1', 'Q2', 'Q3', 'Q4', 'Annual'
    year: int
    quarter: Optional[int] = None
    url: str = ""
    language: str = 'sv'
    downloaded: bool = False
    file_path: Optional[str] = None
    download_date: Optional[datetime] = None


class ManualReportDownloader:
    def __init__(self, output_dir: str = "data/reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Load curated URLs
        self.report_urls = self._load_report_urls()
        
    def _load_report_urls(self) -> Dict[str, List[ReportSource]]:
        """Load manually curated report URLs"""
        
        # Manually curated Swedish company report URLs
        # Add your Volvo URL here!
        report_urls = {
            "Volvo": [
                ReportSource("Volvo Group", "Q2", 2025, 2, 
                           "https://www.volvogroup.com/content/dam/volvo-group/markets/master/investors/reports-and-presentations/interim-reports/2025/volvo-group-q2-2025-sve.pdf"),
            ],
            "Ericsson": [
                # ReportSource("Ericsson", "Q3", 2024, 3, "https://ericsson-url-here.pdf"),
            ],
            "H&M": [
                # ReportSource("H&M", "Q3", 2024, 3, "https://hm-url-here.pdf"),
            ],
            "IKEA": [
                # ReportSource("IKEA", "Annual", 2024, None, "https://ikea-url-here.pdf"),
            ],
            "Spotify": [
                # ReportSource("Spotify", "Q3", 2024, 3, "https://spotify-url-here.pdf"),
            ]
        }
        
        return report_urls
    
    def add_report_url(self, company: str, report_type: str, year: int, url: str, quarter: Optional[int] = None):
        """Add a new report URL to the collection"""
        if company not in self.report_urls:
            self.report_urls[company] = []
            
        report = ReportSource(
            company_name=company,
            report_type=report_type,
            year=year,
            quarter=quarter,
            url=url
        )
        
        self.report_urls[company].append(report)
        print(f"Added {company} {report_type} {year} report")
        
    def download_report(self, report: ReportSource) -> bool:
        """Download a single report PDF"""
        try:
            print(f"Downloading {report.company_name} {report.report_type} {report.year}...")
            
            response = self.session.get(report.url, stream=True)
            response.raise_for_status()
            
            # Generate filename
            filename = f"{report.company_name}_{report.year}_{report.report_type}.pdf"
            filename = re.sub(r'[^\w\-_.]', '_', filename)
            filepath = os.path.join(self.output_dir, filename)
            
            # Download file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            report.file_path = filepath
            report.download_date = datetime.now()
            report.downloaded = True
            
            file_size = os.path.getsize(filepath) / 1024 / 1024  # MB
            print(f"✓ Downloaded: {filename} ({file_size:.1f} MB)")
            return True
            
        except Exception as e:
            print(f"✗ Error downloading {report.company_name} {report.report_type}: {e}")
            return False
    
    def download_company_reports(self, company: str, year: Optional[int] = None) -> List[ReportSource]:
        """Download all reports for a specific company"""
        if company not in self.report_urls:
            print(f"No reports configured for {company}")
            return []
            
        reports_to_download = self.report_urls[company]
        
        # Filter by year if specified
        if year:
            reports_to_download = [r for r in reports_to_download if r.year == year]
            
        downloaded_reports = []
        for report in reports_to_download:
            if not report.downloaded and self.download_report(report):
                downloaded_reports.append(report)
                
        return downloaded_reports
    
    def list_available_reports(self):
        """Show all available reports"""
        print("\n=== Available Reports ===")
        for company, reports in self.report_urls.items():
            print(f"\n{company}:")
            for report in reports:
                status = "✓ Downloaded" if report.downloaded else "○ Available"
                print(f"  {status} {report.year} {report.report_type}")
                if report.downloaded and report.file_path:
                    print(f"    → {report.file_path}")
                else:
                    print(f"    → {report.url}")
    
    def save_metadata(self, reports: List[ReportSource], filename: str = "manual_reports_metadata.json"):
        """Save report metadata to JSON"""
        metadata_path = os.path.join(self.output_dir, filename)
        
        reports_data = []
        for report in reports:
            reports_data.append({
                'company_name': report.company_name,
                'report_type': report.report_type,
                'year': report.year,
                'quarter': report.quarter,
                'url': report.url,
                'language': report.language,
                'downloaded': report.downloaded,
                'download_date': report.download_date.isoformat() if report.download_date else None,
                'file_path': report.file_path
            })
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(reports_data, f, ensure_ascii=False, indent=2)
            
        print(f"Saved metadata for {len(reports)} reports to {filename}")
    
    def integrate_with_mvp1(self, report: ReportSource):
        """Integration hook for MVP1 analysis"""
        if not report.downloaded or not report.file_path:
            print(f"Report not downloaded yet: {report.company_name} {report.report_type}")
            return
            
        print(f"Ready for MVP1 analysis: {report.file_path}")
        # Here we would call MVP1's analysis functions
        # This is where you'd integrate with your existing MVP1 code
        
        return {
            'file_path': report.file_path,
            'company': report.company_name,
            'report_type': report.report_type,
            'year': report.year,
            'ready_for_analysis': True
        }


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Manual Swedish company report downloader')
    parser.add_argument('--company', type=str, help='Company name to download')
    parser.add_argument('--year', type=int, help='Year to download')
    parser.add_argument('--list', action='store_true', help='List available reports')
    parser.add_argument('--add-url', nargs=5, metavar=('COMPANY', 'TYPE', 'YEAR', 'URL', 'QUARTER'),
                       help='Add new report URL: company type year url quarter(optional)')
    
    args = parser.parse_args()
    
    downloader = ManualReportDownloader()
    
    if args.list:
        downloader.list_available_reports()
        
    elif args.add_url:
        company, report_type, year, url = args.add_url[:4]
        quarter = int(args.add_url[4]) if len(args.add_url) > 4 and args.add_url[4].isdigit() else None
        downloader.add_report_url(company, report_type, int(year), url, quarter)
        
    elif args.company:
        print(f"Downloading reports for {args.company}...")
        reports = downloader.download_company_reports(args.company, args.year)
        if reports:
            downloader.save_metadata(reports)
            print(f"\n✓ Downloaded {len(reports)} reports")
        else:
            print("No reports to download")
    else:
        print("Use --help to see available options")
        print("Example: python manual_scraper.py --add-url 'Volvo' 'Q3' '2024' 'https://your-url.pdf'")
        print("         python manual_scraper.py --company 'Volvo' --year 2024")


if __name__ == "__main__":
    main()