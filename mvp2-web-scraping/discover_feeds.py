#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict
import json


def discover_rss_feeds(url: str) -> List[Dict[str, str]]:
    """Discover RSS/Atom feeds from a website"""
    feeds = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Method 1: Look for <link> tags
        feed_links = soup.find_all('link', {
            'type': ['application/rss+xml', 'application/atom+xml', 'application/json']
        })
        
        for link in feed_links:
            href = link.get('href')
            if href:
                if not href.startswith('http'):
                    href = requests.compat.urljoin(url, href)
                    
                feeds.append({
                    'url': href,
                    'title': link.get('title', ''),
                    'type': link.get('type', ''),
                    'method': 'link_tag'
                })
        
        # Method 2: Look for common RSS URLs
        common_paths = [
            '/rss', '/rss.xml', '/feed', '/feed.xml', '/feeds/all.xml',
            '/news.rss', '/news.xml', '/atom.xml', '/feeds/news.xml',
            '/en/news.rss', '/media/news.rss', '/investor-relations.rss'
        ]
        
        for path in common_paths:
            feed_url = requests.compat.urljoin(url, path)
            feeds.append({
                'url': feed_url,
                'title': f'Discovered: {path}',
                'type': 'unknown',
                'method': 'common_path'
            })
        
        # Method 3: Search page content for RSS URLs
        rss_pattern = r'https?://[^\s"\']+\.(?:rss|xml|json)'
        matches = re.findall(rss_pattern, str(soup))
        
        for match in set(matches):
            feeds.append({
                'url': match,
                'title': 'Found in content',
                'type': 'unknown',
                'method': 'content_search'
            })
        
    except Exception as e:
        print(f"Error discovering feeds for {url}: {e}")
    
    return feeds


def verify_feed(feed_url: str) -> Dict[str, any]:
    """Verify if a URL is actually a valid RSS/Atom feed"""
    try:
        import feedparser
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        response = requests.get(feed_url, headers=headers, timeout=10)
        feed = feedparser.parse(response.content)
        
        if feed.bozo and feed.bozo_exception:
            return {
                'valid': False,
                'error': str(feed.bozo_exception),
                'entries': 0
            }
        
        return {
            'valid': len(feed.entries) > 0,
            'title': getattr(feed.feed, 'title', ''),
            'description': getattr(feed.feed, 'description', ''),
            'entries': len(feed.entries),
            'last_updated': getattr(feed.feed, 'updated', ''),
            'language': getattr(feed.feed, 'language', '')
        }
        
    except ImportError:
        print("feedparser not available - install with: pip install feedparser")
        return {'valid': False, 'error': 'feedparser not installed'}
    except Exception as e:
        return {'valid': False, 'error': str(e), 'entries': 0}


def main():
    """Discover RSS feeds for Swedish companies"""
    
    swedish_companies = [
        ('Volvo Group', 'https://www.volvogroup.com'),
        ('H&M', 'https://hmgroup.com'),
        ('Ericsson', 'https://www.ericsson.com'),
        ('Spotify', 'https://newsroom.spotify.com'),
        ('Atlas Copco', 'https://www.atlascopcogroup.com'),
        ('Sandvik', 'https://www.sandvik.com'),
        ('Electrolux', 'https://www.electroluxgroup.com'),
        ('IKEA', 'https://www.ikea.com'),
        ('Skanska', 'https://www.skanska.com'),
        ('Tele2', 'https://www.tele2.com'),
        ('Investor AB', 'https://www.investorab.com'),
        ('Kinnevik', 'https://www.kinnevik.com'),
    ]
    
    print("🔍 Discovering RSS feeds for Swedish companies...")
    all_discovered = {}
    
    for company_name, website in swedish_companies:
        print(f"\n📡 {company_name} ({website})")
        
        feeds = discover_rss_feeds(website)
        verified_feeds = []
        
        # Verify each discovered feed
        for feed in feeds:
            print(f"   Checking: {feed['url']}")
            verification = verify_feed(feed['url'])
            
            feed.update(verification)
            verified_feeds.append(feed)
            
            if verification.get('valid'):
                entries = verification.get('entries', 0)
                print(f"   ✅ Valid RSS feed ({entries} entries)")
            else:
                error = verification.get('error', 'Unknown error')
                print(f"   ❌ Invalid: {error}")
        
        all_discovered[company_name] = verified_feeds
    
    # Save results
    with open('discovered_feeds.json', 'w') as f:
        json.dump(all_discovered, f, indent=2)
    
    # Print summary
    print(f"\n📊 DISCOVERY SUMMARY")
    print(f"{'='*50}")
    
    total_valid = 0
    for company, feeds in all_discovered.items():
        valid_feeds = [f for f in feeds if f.get('valid')]
        total_valid += len(valid_feeds)
        
        if valid_feeds:
            print(f"✅ {company}: {len(valid_feeds)} valid feeds")
            for feed in valid_feeds:
                print(f"   📄 {feed['title']} ({feed['entries']} entries)")
        else:
            print(f"❌ {company}: No valid feeds found")
    
    print(f"\n🎉 Total valid feeds discovered: {total_valid}")
    print("💾 Results saved to discovered_feeds.json")


if __name__ == "__main__":
    main()