#!/usr/bin/env python3

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import json

@dataclass
class ReportDownload:
    company_name: str
    report_type: str
    year: int
    original_url: str
    local_file_path: Optional[str] = None
    download_method: str = "manual"  # "manual", "browser", "api"
    downloaded: bool = False
    download_date: Optional[datetime] = None
    file_size_mb: Optional[float] = None
    notes: str = ""


class BrowserAssistedDownloader:
    """
    For when automated download fails - guide user through manual download
    """
    
    def __init__(self, output_dir: str = "data/reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.downloads: List[ReportDownload] = []
        
    def add_failed_url(self, company: str, report_type: str, year: int, url: str, error_reason: str = ""):
        """Add a URL that failed automated download"""
        download = ReportDownload(
            company_name=company,
            report_type=report_type,
            year=year,
            original_url=url,
            notes=f"Failed automated download: {error_reason}"
        )
        self.downloads.append(download)
        return download
        
    def guide_manual_download(self, download: ReportDownload):
        """Guide user through manual download process"""
        
        expected_filename = f"{download.company_name}_{download.year}_{download.report_type}.pdf"
        expected_path = os.path.join(self.output_dir, expected_filename)
        
        print(f"\n{'='*60}")
        print(f"MANUAL DOWNLOAD REQUIRED: {download.company_name} {download.report_type} {download.year}")
        print(f"{'='*60}")
        print(f"URL: {download.original_url}")
        print(f"Issue: {download.notes}")
        print(f"\nSTEPS:")
        print(f"1. Open URL in your browser (preferably Chrome/Safari)")
        print(f"2. Navigate past any blocks/captchas if needed")
        print(f"3. Download the PDF manually")
        print(f"4. Save it as: {expected_filename}")
        print(f"5. Move it to: {self.output_dir}")
        print(f"6. Run: python browser_downloader.py --verify '{expected_filename}'")
        
        return expected_path
        
    def verify_manual_download(self, filename: str) -> bool:
        """Verify a manually downloaded file"""
        filepath = os.path.join(self.output_dir, filename)
        
        if not os.path.exists(filepath):
            print(f"❌ File not found: {filepath}")
            return False
            
        # Check if it's actually a PDF and not an error page
        file_size = os.path.getsize(filepath) / 1024 / 1024  # MB
        
        # Read first few bytes to check if it's HTML (error page)
        try:
            with open(filepath, 'rb') as f:
                first_bytes = f.read(100).decode('utf-8', errors='ignore')
                
            if '<!DOCTYPE html' in first_bytes or '<html' in first_bytes:
                print(f"❌ File appears to be HTML error page, not PDF")
                print(f"   First 100 chars: {first_bytes[:100]}...")
                return False
                
            if first_bytes.startswith('%PDF'):
                print(f"✅ Valid PDF file: {filename} ({file_size:.1f} MB)")
                
                # Update our tracking
                for download in self.downloads:
                    expected_name = f"{download.company_name}_{download.year}_{download.report_type}.pdf"
                    if expected_name == filename:
                        download.local_file_path = filepath
                        download.downloaded = True
                        download.download_date = datetime.now()
                        download.file_size_mb = file_size
                        download.download_method = "manual_verified"
                        break
                        
                return True
            else:
                print(f"❌ File doesn't appear to be a valid PDF")
                return False
                
        except Exception as e:
            print(f"❌ Error checking file: {e}")
            return False
            
    def list_pending_downloads(self):
        """Show downloads that need manual intervention"""
        pending = [d for d in self.downloads if not d.downloaded]
        completed = [d for d in self.downloads if d.downloaded]
        
        if pending:
            print(f"\n📋 PENDING DOWNLOADS ({len(pending)}):")
            for i, download in enumerate(pending, 1):
                print(f"{i}. {download.company_name} {download.report_type} {download.year}")
                print(f"   URL: {download.original_url}")
                print(f"   Issue: {download.notes}")
                
        if completed:
            print(f"\n✅ COMPLETED DOWNLOADS ({len(completed)}):")
            for download in completed:
                print(f"   {download.company_name} {download.report_type} {download.year} - {download.file_size_mb:.1f} MB")
                
    def save_download_log(self, filename: str = "download_log.json"):
        """Save download attempts and results"""
        log_path = os.path.join(self.output_dir, filename)
        
        downloads_data = []
        for download in self.downloads:
            downloads_data.append({
                'company_name': download.company_name,
                'report_type': download.report_type,
                'year': download.year,
                'original_url': download.original_url,
                'local_file_path': download.local_file_path,
                'download_method': download.download_method,
                'downloaded': download.downloaded,
                'download_date': download.download_date.isoformat() if download.download_date else None,
                'file_size_mb': download.file_size_mb,
                'notes': download.notes
            })
            
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(downloads_data, f, ensure_ascii=False, indent=2)
            
        print(f"💾 Download log saved: {log_path}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Browser-assisted report downloader')
    parser.add_argument('--add-failed', nargs=4, metavar=('COMPANY', 'TYPE', 'YEAR', 'URL'),
                       help='Add failed download for manual processing')
    parser.add_argument('--verify', type=str, help='Verify manually downloaded file')
    parser.add_argument('--list', action='store_true', help='List pending downloads')
    
    args = parser.parse_args()
    
    downloader = BrowserAssistedDownloader()
    
    # Example: Add the Volvo URL that failed
    if not args.add_failed and not args.verify and not args.list:
        # Add the failed Volvo download as example
        volvo_download = downloader.add_failed_url(
            "Volvo", "Q2", 2025,
            "https://www.volvogroup.com/content/dam/volvo-group/markets/master/investors/reports-and-presentations/interim-reports/2025/volvo-group-q2-2025-sve.pdf",
            "Geo-blocking or bot detection"
        )
        downloader.guide_manual_download(volvo_download)
        downloader.save_download_log()
        
    elif args.add_failed:
        company, report_type, year, url = args.add_failed
        download = downloader.add_failed_url(company, report_type, int(year), url)
        downloader.guide_manual_download(download)
        
    elif args.verify:
        success = downloader.verify_manual_download(args.verify)
        if success:
            downloader.save_download_log()
            
    elif args.list:
        downloader.list_pending_downloads()


if __name__ == "__main__":
    main()