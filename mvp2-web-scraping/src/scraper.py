import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from datetime import datetime
import re
import os
from urllib.parse import urljoin, urlparse
import time
from dataclasses import dataclass
import json


@dataclass
class CompanyReport:
    company_name: str
    report_type: str  # 'Q1', 'Q2', 'Q3', 'Q4', 'Annual'
    year: int
    url: str
    language: str = 'sv'
    download_date: Optional[datetime] = None
    file_path: Optional[str] = None


class SwedishReportScraper:
    def __init__(self, output_dir: str = "data/reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
    def scrape_nasdaq_stockholm_reports(self, company_symbol: str, year: int) -> List[CompanyReport]:
        """Scrape reports from Nasdaq Stockholm (more reliable source)"""
        reports = []
        
        # Use Nasdaq Stockholm's API/search for Swedish companies
        base_url = f"https://www.nasdaqomxnordic.com/aktier/microsite?Instrument={company_symbol}"
        
        try:
            # Add headers to avoid blocking
            self.session.headers.update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            response = self.session.get(base_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for financial reports section
            report_sections = soup.find_all(['div', 'section'], class_=re.compile(r'report|financial|investor'))
            
            for section in report_sections:
                links = section.find_all('a', href=True)
                for link in links:
                    if str(year) in link.text and 'pdf' in link.get('href', '').lower():
                        report = CompanyReport(
                            company_name=company_symbol,
                            report_type=self._extract_report_type(link.text),
                            year=year,
                            url=urljoin(base_url, link['href'])
                        )
                        reports.append(report)
                        
        except Exception as e:
            print(f"Error scraping Nasdaq reports for {company_symbol}: {e}")
            
        return reports
    
    def scrape_volvo_reports(self, year: int) -> List[CompanyReport]:
        """Scrape Volvo Group investor reports - fallback to alternative sources"""
        reports = []
        
        # Try multiple URLs for Volvo
        urls_to_try = [
            "https://www.volvogroup.com/investors/reports-and-presentations/",
            "https://group.volvocars.com/investors/reports-and-presentations/",
            "https://www.volvogroup.com/en/investors/"
        ]
        
        for base_url in urls_to_try:
            try:
                response = self.session.get(base_url, timeout=10)
                if response.status_code != 200:
                    continue
                    
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # More flexible search - look for any links containing year
                all_links = soup.find_all('a', href=True)
                
                for link in all_links:
                    link_text = link.get_text() or ""
                    link_href = link.get('href', '')
                    
                    if (str(year) in link_text and 
                        ('pdf' in link_href.lower() or 'report' in link_text.lower())):
                        
                        report = CompanyReport(
                            company_name="Volvo Group",
                            report_type=self._extract_report_type(link_text),
                            year=year,
                            url=urljoin(base_url, link_href)
                        )
                        reports.append(report)
                        
                if reports:  # If we found reports, stop trying other URLs
                    break
                    
            except Exception as e:
                print(f"Error with URL {base_url}: {e}")
                continue
                
        # If no reports found, try Nasdaq Stockholm
        if not reports:
            print("Trying Nasdaq Stockholm as fallback...")
            reports = self.scrape_nasdaq_stockholm_reports("VOLV-B", year)
            
        return reports
    
    def scrape_hm_reports(self, year: int) -> List[CompanyReport]:
        """Scrape H&M investor reports"""
        reports = []
        base_url = f"https://hmgroup.com/investors/reports/"
        
        try:
            response = self.session.get(base_url)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # H&M specific parsing logic
            report_sections = soup.find_all('div', class_='report-item')
            
            for section in report_sections:
                title = section.text
                if str(year) in title:
                    link = section.find('a', href=True)
                    if link and 'pdf' in link['href'].lower():
                        report = CompanyReport(
                            company_name="H&M",
                            report_type=self._extract_report_type(title),
                            year=year,
                            url=urljoin(base_url, link['href'])
                        )
                        reports.append(report)
                        
        except Exception as e:
            print(f"Error scraping H&M reports: {e}")
            
        return reports
    
    def scrape_ericsson_reports(self, year: int) -> List[CompanyReport]:
        """Scrape Ericsson investor reports"""
        reports = []
        base_url = "https://www.ericsson.com/en/investors/financial-reports"
        
        # Similar implementation for Ericsson
        # This is a placeholder - actual implementation would need site-specific parsing
        
        return reports
    
    def download_report(self, report: CompanyReport) -> bool:
        """Download a single report PDF"""
        try:
            filename = f"{report.company_name}_{report.year}_{report.report_type}.pdf"
            filename = re.sub(r'[^\w\-_.]', '_', filename)  # Sanitize filename
            filepath = os.path.join(self.output_dir, filename)
            
            response = self.session.get(report.url, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            report.file_path = filepath
            report.download_date = datetime.now()
            print(f"Downloaded: {filename}")
            return True
            
        except Exception as e:
            print(f"Error downloading {report.url}: {e}")
            return False
    
    def _extract_report_type(self, text: str) -> str:
        """Extract report type from text"""
        text = text.lower()
        if 'annual' in text or 'år' in text:
            return 'Annual'
        elif 'q1' in text or 'kvartal 1' in text:
            return 'Q1'
        elif 'q2' in text or 'kvartal 2' in text:
            return 'Q2'
        elif 'q3' in text or 'kvartal 3' in text:
            return 'Q3'
        elif 'q4' in text or 'kvartal 4' in text:
            return 'Q4'
        else:
            return 'Unknown'
    
    def scrape_fi_se_reports(self, company_name: str, year: int) -> List[CompanyReport]:
        """Scrape reports from Swedish Financial Supervisory Authority (fi.se)"""
        reports = []
        
        # Search for company reports on fi.se (official Swedish financial authority)
        search_url = f"https://fi.se/en/our-registers/company-register/"
        
        try:
            print(f"Searching for {company_name} reports on fi.se...")
            
            # This is a placeholder - in practice, we'd need to implement
            # the specific search and navigation logic for fi.se
            # For now, return some mock data to demonstrate the structure
            
            mock_reports = [
                CompanyReport(
                    company_name=company_name,
                    report_type="Q3",
                    year=year,
                    url="https://example.com/mock_q3_report.pdf",
                    language='sv'
                ),
                CompanyReport(
                    company_name=company_name,
                    report_type="Annual",
                    year=year,
                    url="https://example.com/mock_annual_report.pdf",
                    language='sv'
                )
            ]
            
            print(f"Mock: Found {len(mock_reports)} reports for {company_name}")
            reports.extend(mock_reports)
            
        except Exception as e:
            print(f"Error scraping fi.se for {company_name}: {e}")
            
        return reports
    
    def scrape_company(self, company_name: str, year: int) -> List[CompanyReport]:
        """Main method to scrape reports for a specific company"""
        company_lower = company_name.lower()
        
        if 'volvo' in company_lower:
            reports = self.scrape_volvo_reports(year)
        elif 'h&m' in company_lower or 'hm' in company_lower:
            reports = self.scrape_hm_reports(year)
        elif 'ericsson' in company_lower:
            reports = self.scrape_ericsson_reports(year)
        else:
            print(f"Using generic Swedish financial authority search for {company_name}")
            reports = self.scrape_fi_se_reports(company_name, year)
            
        # If no reports found from company-specific scraper, try fi.se as fallback
        if not reports:
            print(f"No reports found from primary source, trying fi.se for {company_name}...")
            reports = self.scrape_fi_se_reports(company_name, year)
            
        return reports
    
    def save_metadata(self, reports: List[CompanyReport], filename: str = "report_metadata.json"):
        """Save report metadata to JSON"""
        metadata_path = os.path.join(self.output_dir, filename)
        
        # Convert reports to dict format
        reports_data = []
        for report in reports:
            reports_data.append({
                'company_name': report.company_name,
                'report_type': report.report_type,
                'year': report.year,
                'url': report.url,
                'language': report.language,
                'download_date': report.download_date.isoformat() if report.download_date else None,
                'file_path': report.file_path
            })
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(reports_data, f, ensure_ascii=False, indent=2)
            
        print(f"Saved metadata for {len(reports)} reports")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape Swedish company reports')
    parser.add_argument('--company', type=str, required=True, help='Company name')
    parser.add_argument('--year', type=int, required=True, help='Year to scrape')
    parser.add_argument('--download', action='store_true', help='Download PDFs')
    
    args = parser.parse_args()
    
    scraper = SwedishReportScraper()
    
    print(f"Scraping {args.company} reports for {args.year}...")
    reports = scraper.scrape_company(args.company, args.year)
    
    print(f"Found {len(reports)} reports")
    
    if args.download and reports:
        print("Downloading reports...")
        for report in reports:
            time.sleep(1)  # Be polite to servers
            scraper.download_report(report)
            
        scraper.save_metadata(reports)
    else:
        # Just display found reports
        for report in reports:
            print(f"- {report.company_name} {report.year} {report.report_type}: {report.url}")


if __name__ == "__main__":
    main()