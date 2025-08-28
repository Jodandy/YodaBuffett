#!/usr/bin/env python3

import feedparser
import requests
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Optional, Set
import json
import re
import time
import os
from urllib.parse import urljoin


@dataclass
class CompanyFeed:
    company_name: str
    feed_url: str
    feed_type: str  # 'rss', 'atom', 'json'
    last_checked: Optional[datetime] = None
    active: bool = True
    

@dataclass
class FeedItem:
    company_name: str
    title: str
    link: str
    published: datetime
    description: str
    item_type: str  # 'report', 'memo', 'announcement', 'unknown'
    pdf_urls: List[str]
    processed: bool = False


class SwedishCompanyRSSMonitor:
    """
    Monitor RSS feeds from Swedish companies for new reports and announcements
    """
    
    def __init__(self, output_dir: str = "data/rss_feeds"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Known Swedish company RSS feeds
        self.company_feeds = [
            # Major Swedish Companies - these are likely URLs (need verification)
            CompanyFeed("Volvo Group", "https://www.volvogroup.com/en/news.rss", "rss"),
            CompanyFeed("H&M", "https://hmgroup.com/news.rss", "rss"),
            CompanyFeed("Ericsson", "https://www.ericsson.com/en/news-and-events/news.rss", "rss"),
            CompanyFeed("Spotify", "https://newsroom.spotify.com/feeds/all.rss.xml", "rss"),
            CompanyFeed("Atlas Copco", "https://www.atlascopcogroup.com/en/media/news.rss", "rss"),
            CompanyFeed("Sandvik", "https://www.sandvik.com/en/news.rss", "rss"),
            CompanyFeed("Electrolux", "https://www.electroluxgroup.com/news.rss", "rss"),
            CompanyFeed("Tele2", "https://www.tele2.com/media/news.rss", "rss"),
            CompanyFeed("Kinnevik", "https://www.kinnevik.com/en/media/news.rss", "rss"),
            
            # Alternative: Nasdaq Stockholm RSS
            CompanyFeed("Nasdaq Stockholm", "https://www.nasdaqomxnordic.com/news/marketnotices.rss", "rss"),
            
            # Swedish Financial Authority
            CompanyFeed("Finansinspektionen", "https://fi.se/en/about-fi/press-and-reports.rss", "rss"),
        ]
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        # Keywords for identifying different types of content
        self.report_keywords = [
            'quarterly report', 'annual report', 'interim report', 'q1', 'q2', 'q3', 'q4',
            'kvartalsrapport', 'årsredovisning', 'delårsrapport', 'rapport'
        ]
        
        self.memo_keywords = [
            'memorandum', 'memo', 'announcement', 'notice', 'meddelande', 'kommuniqué'
        ]
        
        self.pdf_patterns = [
            r'https?://[^\s]+\.pdf',
            r'href=["\']([^"\']*\.pdf)["\']'
        ]
    
    def classify_item_type(self, title: str, description: str) -> str:
        """Classify feed item as report, memo, or announcement"""
        text = (title + " " + description).lower()
        
        for keyword in self.report_keywords:
            if keyword in text:
                return 'report'
                
        for keyword in self.memo_keywords:
            if keyword in text:
                return 'memo'
                
        return 'announcement'
    
    def extract_pdf_urls(self, description: str, link: str) -> List[str]:
        """Extract PDF URLs from description and main link"""
        pdf_urls = []
        
        # Check if main link is PDF
        if link.lower().endswith('.pdf'):
            pdf_urls.append(link)
            
        # Search for PDFs in description
        for pattern in self.pdf_patterns:
            matches = re.findall(pattern, description)
            pdf_urls.extend(matches)
            
        return list(set(pdf_urls))  # Remove duplicates
    
    def check_feed(self, company_feed: CompanyFeed, days_back: int = 7) -> List[FeedItem]:
        """Check a single RSS feed for new items"""
        items = []
        
        try:
            print(f"📡 Checking RSS feed: {company_feed.company_name}")
            
            response = self.session.get(company_feed.feed_url, timeout=10)
            
            # Try feedparser first
            feed = feedparser.parse(response.content)
            
            if feed.bozo and feed.bozo_exception:
                print(f"⚠️  RSS parsing warning for {company_feed.company_name}: {feed.bozo_exception}")
            
            # Filter items from last N days
            cutoff_date = datetime.now() - timedelta(days=days_back)
            
            for entry in feed.entries:
                try:
                    # Parse publication date
                    if hasattr(entry, 'published_parsed') and entry.published_parsed:
                        pub_date = datetime(*entry.published_parsed[:6])
                    elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                        pub_date = datetime(*entry.updated_parsed[:6])
                    else:
                        pub_date = datetime.now()  # Fallback to now
                    
                    if pub_date < cutoff_date:
                        continue
                        
                    # Extract content
                    title = getattr(entry, 'title', 'No title')
                    link = getattr(entry, 'link', '')
                    description = getattr(entry, 'description', '') or getattr(entry, 'summary', '')
                    
                    # Classify and extract PDFs
                    item_type = self.classify_item_type(title, description)
                    pdf_urls = self.extract_pdf_urls(description, link)
                    
                    item = FeedItem(
                        company_name=company_feed.company_name,
                        title=title,
                        link=link,
                        published=pub_date,
                        description=description[:500],  # Truncate
                        item_type=item_type,
                        pdf_urls=pdf_urls
                    )
                    
                    items.append(item)
                    
                except Exception as e:
                    print(f"⚠️  Error processing entry: {e}")
                    continue
            
            company_feed.last_checked = datetime.now()
            
        except Exception as e:
            print(f"❌ Error checking feed {company_feed.company_name}: {e}")
            
        return items
    
    def check_all_feeds(self, days_back: int = 7) -> Dict[str, List[FeedItem]]:
        """Check all configured RSS feeds"""
        all_items = {}
        
        for feed in self.company_feeds:
            if not feed.active:
                continue
                
            items = self.check_feed(feed, days_back)
            if items:
                all_items[feed.company_name] = items
                
            # Be polite to servers
            time.sleep(1)
            
        return all_items
    
    def filter_reports_only(self, all_items: Dict[str, List[FeedItem]]) -> Dict[str, List[FeedItem]]:
        """Filter to show only financial reports"""
        reports_only = {}
        
        for company, items in all_items.items():
            reports = [item for item in items if item.item_type == 'report']
            if reports:
                reports_only[company] = reports
                
        return reports_only
    
    def save_feed_data(self, all_items: Dict[str, List[FeedItem]], filename: str = None):
        """Save RSS feed data to JSON"""
        if filename is None:
            filename = f"rss_data_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
            
        filepath = os.path.join(self.output_dir, filename)
        
        # Convert to serializable format
        serializable_data = {}
        for company, items in all_items.items():
            serializable_data[company] = []
            for item in items:
                serializable_data[company].append({
                    'title': item.title,
                    'link': item.link,
                    'published': item.published.isoformat(),
                    'description': item.description,
                    'item_type': item.item_type,
                    'pdf_urls': item.pdf_urls,
                    'processed': item.processed
                })
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable_data, f, ensure_ascii=False, indent=2)
            
        print(f"💾 RSS data saved: {filepath}")
        return filepath
    
    def print_summary(self, all_items: Dict[str, List[FeedItem]]):
        """Print a nice summary of found items"""
        if not all_items:
            print("📭 No new items found in RSS feeds")
            return
            
        print(f"\n📊 RSS FEED SUMMARY")
        print(f"{'='*50}")
        
        total_items = sum(len(items) for items in all_items.values())
        total_reports = sum(len([i for i in items if i.item_type == 'report']) 
                           for items in all_items.values())
        total_pdfs = sum(len([i for i in items if i.pdf_urls]) 
                        for items in all_items.values())
        
        print(f"📈 Total items: {total_items}")
        print(f"📄 Financial reports: {total_reports}")
        print(f"🔗 Items with PDFs: {total_pdfs}")
        
        for company, items in all_items.items():
            print(f"\n🏢 {company} ({len(items)} items)")
            
            for item in items[:3]:  # Show first 3
                icon = "📊" if item.item_type == 'report' else "📝"
                pdf_info = f" ({len(item.pdf_urls)} PDFs)" if item.pdf_urls else ""
                print(f"   {icon} {item.title[:60]}...{pdf_info}")
                print(f"      📅 {item.published.strftime('%Y-%m-%d %H:%M')}")
                
            if len(items) > 3:
                print(f"   ... and {len(items) - 3} more")
    
    def generate_download_queue(self, all_items: Dict[str, List[FeedItem]]) -> List[tuple]:
        """Generate download queue for found PDFs"""
        download_queue = []
        
        for company, items in all_items.items():
            for item in items:
                if item.pdf_urls and item.item_type == 'report':
                    for pdf_url in item.pdf_urls:
                        download_queue.append((
                            company,
                            item.item_type,
                            item.published.year,
                            pdf_url,
                            item.title
                        ))
        
        return download_queue


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Swedish Company RSS Feed Monitor')
    parser.add_argument('--days', type=int, default=7, help='Days back to check (default: 7)')
    parser.add_argument('--reports-only', action='store_true', help='Show only financial reports')
    parser.add_argument('--save', action='store_true', help='Save results to JSON')
    
    args = parser.parse_args()
    
    monitor = SwedishCompanyRSSMonitor()
    
    print("🔍 Checking Swedish company RSS feeds...")
    all_items = monitor.check_all_feeds(args.days)
    
    if args.reports_only:
        all_items = monitor.filter_reports_only(all_items)
        
    monitor.print_summary(all_items)
    
    if args.save:
        monitor.save_feed_data(all_items)
        
    # Show download opportunities
    download_queue = monitor.generate_download_queue(all_items)
    if download_queue:
        print(f"\n🚀 DOWNLOAD OPPORTUNITIES ({len(download_queue)})")
        for i, (company, report_type, year, url, title) in enumerate(download_queue[:5], 1):
            print(f"{i}. {company} {year}: {title[:50]}...")
            print(f"   URL: {url}")


if __name__ == "__main__":
    main()