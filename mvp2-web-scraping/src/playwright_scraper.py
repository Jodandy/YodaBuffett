#!/usr/bin/env python3

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
import json
from pathlib import Path


@dataclass
class BrowserDownload:
    company_name: str
    report_type: str
    year: int
    url: str
    success: bool = False
    file_path: Optional[str] = None
    error_message: Optional[str] = None
    download_date: Optional[datetime] = None
    file_size_mb: Optional[float] = None


class PlaywrightScraper:
    """
    Browser automation for bypassing anti-bot measures
    """
    
    def __init__(self, output_dir: str = "data/reports", headless: bool = True):
        self.output_dir = output_dir
        self.headless = headless
        os.makedirs(output_dir, exist_ok=True)
        self.downloads: List[BrowserDownload] = []
        
    async def download_with_browser(self, company: str, report_type: str, year: int, url: str) -> BrowserDownload:
        """Download PDF using real browser to bypass blocks"""
        
        download = BrowserDownload(
            company_name=company,
            report_type=report_type,
            year=year,
            url=url
        )
        
        try:
            # Dynamic import to handle missing playwright
            try:
                from playwright.async_api import async_playwright
            except ImportError:
                download.error_message = "Playwright not installed. Run: pip install playwright && playwright install"
                return download
                
            async with async_playwright() as p:
                # Launch browser (Chrome-like)
                browser = await p.chromium.launch(
                    headless=self.headless,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-web-security',
                        '--disable-features=VizDisplayCompositor'
                    ]
                )
                
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                
                page = await context.new_page()
                
                # Set up download handling
                download_path = None
                
                async def handle_download(download_event):
                    nonlocal download_path
                    filename = f"{company}_{year}_{report_type}.pdf"
                    download_path = os.path.join(self.output_dir, filename)
                    await download_event.save_as(download_path)
                    
                page.on("download", handle_download)
                
                print(f"🌐 Opening {url} in browser...")
                
                # Navigate to URL with longer timeout
                try:
                    await page.goto(url, wait_until='networkidle', timeout=30000)
                except Exception as e:
                    print(f"Navigation warning: {e}")
                    # Continue anyway, might still work
                
                # Wait a bit for page to load
                await asyncio.sleep(3)
                
                # Check if we got an error page
                page_content = await page.content()
                if 'Technical Difficulties' in page_content or 'Sorry' in page_content:
                    download.error_message = "Hit error/block page - trying manual intervention"
                    
                    if not self.headless:
                        print("❌ Hit block page. Browser is open - try to navigate manually.")
                        print("   1. Navigate past any blocks/captchas")
                        print("   2. Click download link when ready")
                        print("   3. Wait for download to complete...")
                        
                        # Wait longer for manual intervention
                        await asyncio.sleep(30)
                    else:
                        await browser.close()
                        return download
                
                # Look for PDF download links
                pdf_links = await page.locator('a[href*=".pdf"]').all()
                
                if pdf_links:
                    print(f"🔗 Found {len(pdf_links)} PDF links, clicking first...")
                    await pdf_links[0].click()
                    
                    # Wait for download
                    await asyncio.sleep(5)
                    
                elif url.endswith('.pdf'):
                    print("🔗 Direct PDF URL, waiting for download...")
                    await asyncio.sleep(5)
                    
                else:
                    download.error_message = "No PDF download link found on page"
                
                await browser.close()
                
                # Check if download succeeded
                if download_path and os.path.exists(download_path):
                    file_size = os.path.getsize(download_path) / 1024 / 1024
                    
                    # Verify it's actually a PDF
                    with open(download_path, 'rb') as f:
                        first_bytes = f.read(100)
                        
                    if first_bytes.startswith(b'%PDF'):
                        download.success = True
                        download.file_path = download_path
                        download.file_size_mb = file_size
                        download.download_date = datetime.now()
                        print(f"✅ Successfully downloaded: {os.path.basename(download_path)} ({file_size:.1f} MB)")
                    else:
                        download.error_message = f"Downloaded file is not a valid PDF (first bytes: {first_bytes[:20]})"
                        os.remove(download_path)  # Clean up invalid file
                else:
                    download.error_message = "Download did not complete - no file found"
                    
        except Exception as e:
            download.error_message = f"Browser automation error: {str(e)}"
            
        self.downloads.append(download)
        return download
    
    async def download_multiple(self, urls_data: List[tuple]) -> List[BrowserDownload]:
        """Download multiple PDFs"""
        results = []
        
        for company, report_type, year, url in urls_data:
            print(f"\n📄 Processing: {company} {report_type} {year}")
            result = await self.download_with_browser(company, report_type, year, url)
            results.append(result)
            
            if result.success:
                print(f"✅ Success: {result.file_path}")
            else:
                print(f"❌ Failed: {result.error_message}")
                
            # Be polite - wait between downloads
            await asyncio.sleep(2)
            
        return results
    
    def save_results(self, filename: str = "playwright_results.json"):
        """Save download results"""
        results_path = os.path.join(self.output_dir, filename)
        
        results_data = []
        for download in self.downloads:
            results_data.append({
                'company_name': download.company_name,
                'report_type': download.report_type,
                'year': download.year,
                'url': download.url,
                'success': download.success,
                'file_path': download.file_path,
                'error_message': download.error_message,
                'download_date': download.download_date.isoformat() if download.download_date else None,
                'file_size_mb': download.file_size_mb
            })
            
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(results_data, f, ensure_ascii=False, indent=2)
            
        print(f"💾 Results saved: {results_path}")
        
    def print_summary(self):
        """Print download summary"""
        if not self.downloads:
            print("No downloads attempted yet.")
            return
            
        successful = [d for d in self.downloads if d.success]
        failed = [d for d in self.downloads if not d.success]
        
        print(f"\n📊 DOWNLOAD SUMMARY:")
        print(f"✅ Successful: {len(successful)}")
        print(f"❌ Failed: {len(failed)}")
        
        if successful:
            total_size = sum(d.file_size_mb or 0 for d in successful)
            print(f"📁 Total downloaded: {total_size:.1f} MB")
            
        if failed:
            print(f"\n❌ Failed downloads:")
            for download in failed:
                print(f"   {download.company_name} {download.report_type} {download.year}: {download.error_message}")


async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Playwright-based PDF downloader')
    parser.add_argument('--url', type=str, help='Single URL to download')
    parser.add_argument('--company', type=str, help='Company name')
    parser.add_argument('--type', type=str, help='Report type (Q1, Q2, etc.)')
    parser.add_argument('--year', type=int, help='Report year')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--visible', action='store_true', help='Show browser (for debugging)')
    
    args = parser.parse_args()
    
    scraper = PlaywrightScraper(headless=not args.visible)
    
    if args.url and args.company and args.type and args.year:
        # Single download
        result = await scraper.download_with_browser(
            args.company, args.type, args.year, args.url
        )
        
        scraper.print_summary()
        scraper.save_results()
        
    else:
        # Test with Volvo URL
        print("🧪 Testing with Volvo URL...")
        test_urls = [
            ("Volvo", "Q2", 2025, "https://www.volvogroup.com/content/dam/volvo-group/markets/master/investors/reports-and-presentations/interim-reports/2025/volvo-group-q2-2025-sve.pdf")
        ]
        
        results = await scraper.download_multiple(test_urls)
        scraper.print_summary()
        scraper.save_results()
        
        if any(r.success for r in results):
            print("\n🎉 Success! Try running with --visible to see browser in action")
        else:
            print("\n💡 Try running with --visible to manually handle blocks")


if __name__ == "__main__":
    asyncio.run(main())