#!/usr/bin/env python3

import requests
import feedparser
from datetime import datetime
import json


def test_volvo_rss():
    """Test the actual Volvo RSS feed we found"""
    
    feed_url = "https://www.volvogroup.com/se/news-and-media/events/_jcr_content/root/responsivegrid/eventlist.feed.xml"
    
    print("🔍 Testing Volvo RSS feed...")
    print(f"URL: {feed_url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        
        # Get raw XML
        response = requests.get(feed_url, headers=headers, timeout=10)
        print(f"✅ HTTP Status: {response.status_code}")
        
        # Parse with feedparser
        feed = feedparser.parse(response.content)
        
        print(f"📄 Feed Title: {feed.feed.title}")
        print(f"📄 Feed Description: {feed.feed.description}")
        print(f"📊 Total Entries: {len(feed.entries)}")
        
        if feed.bozo:
            print(f"⚠️  Parsing warning: {feed.bozo_exception}")
        
        print(f"\n📅 UPCOMING EVENTS:")
        print("="*60)
        
        for i, entry in enumerate(feed.entries, 1):
            title = entry.title
            link = entry.link
            description = entry.description
            category = getattr(entry, 'category', 'No category')
            
            # Parse date
            if hasattr(entry, 'published_parsed'):
                pub_date = datetime(*entry.published_parsed[:6])
                date_str = pub_date.strftime('%Y-%m-%d %H:%M')
            else:
                date_str = "No date"
            
            print(f"{i}. {title}")
            print(f"   📅 {date_str}")
            print(f"   🏷️  {category}")
            print(f"   📝 {description}")
            print(f"   🔗 {link}")
            
            # Check if it's a financial report
            is_financial = 'kvartalet' in title.lower() or 'rapport' in description.lower()
            if is_financial:
                print("   💰 ← FINANCIAL REPORT!")
            
            print()
        
        # Look for patterns we can use
        financial_events = [e for e in feed.entries 
                          if 'kvartalet' in e.title.lower() or 'Financial Event' in getattr(e, 'category', '')]
        
        print(f"🎯 FINANCIAL REPORTS FOUND: {len(financial_events)}")
        
        if financial_events:
            print("\nFinancial reports we can track:")
            for event in financial_events:
                print(f"- {event.title} ({datetime(*event.published_parsed[:6]).strftime('%Y-%m-%d')})")
        
        # Save raw data for analysis
        with open('volvo_rss_sample.json', 'w', encoding='utf-8') as f:
            feed_data = {
                'feed_title': feed.feed.title,
                'feed_description': feed.feed.description,
                'entries': []
            }
            
            for entry in feed.entries:
                feed_data['entries'].append({
                    'title': entry.title,
                    'link': entry.link,
                    'description': entry.description,
                    'category': getattr(entry, 'category', ''),
                    'published': datetime(*entry.published_parsed[:6]).isoformat() if hasattr(entry, 'published_parsed') else None
                })
            
            json.dump(feed_data, f, ensure_ascii=False, indent=2)
        
        print("💾 Sample data saved to volvo_rss_sample.json")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    test_volvo_rss()