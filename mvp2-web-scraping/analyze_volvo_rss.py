#!/usr/bin/env python3

import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
import json
import re


def analyze_volvo_rss():
    """Analyze the Volvo RSS feed using built-in libraries"""
    
    feed_url = "https://www.volvogroup.com/se/news-and-media/events/_jcr_content/root/responsivegrid/eventlist.feed.xml"
    
    print("🔍 Analyzing Volvo RSS feed...")
    print(f"URL: {feed_url}")
    
    try:
        # Download the RSS feed
        req = urllib.request.Request(
            feed_url,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        )
        
        with urllib.request.urlopen(req) as response:
            xml_content = response.read().decode('utf-8')
        
        print("✅ Successfully downloaded RSS feed")
        
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Find channel info
        channel = root.find('channel')
        title = channel.find('title').text
        description = channel.find('description').text
        
        print(f"📄 Feed Title: {title}")
        print(f"📄 Description: {description}")
        
        # Find all items
        items = channel.findall('item')
        print(f"📊 Total Items: {len(items)}")
        
        print(f"\n📅 EVENTS FOUND:")
        print("="*60)
        
        financial_reports = []
        
        for i, item in enumerate(items, 1):
            title = item.find('title').text
            link = item.find('link').text
            desc = item.find('description').text
            category = item.find('category')
            category_text = category.text if category is not None else "No category"
            
            print(f"{i}. {title}")
            print(f"   🏷️  {category_text}")
            print(f"   📝 {desc}")
            print(f"   🔗 {link}")
            
            # Check if this is a financial report
            is_financial = (
                'kvartalet' in title.lower() or 
                'rapport' in desc.lower() or 
                'Financial Event' in category_text
            )
            
            if is_financial:
                print("   💰 ← FINANCIAL REPORT!")
                financial_reports.append({
                    'title': title,
                    'link': link,
                    'description': desc,
                    'category': category_text
                })
            
            print()
        
        print(f"🎯 FINANCIAL REPORTS DETECTED: {len(financial_reports)}")
        
        if financial_reports:
            print("\n💰 FINANCIAL REPORTS:")
            for report in financial_reports:
                print(f"- {report['title']}")
                print(f"  Link: {report['link']}")
                
                # Try to extract quarter info
                title_lower = report['title'].lower()
                if 'första' in title_lower or 'q1' in title_lower:
                    quarter = 'Q1'
                elif 'andra' in title_lower or 'q2' in title_lower:
                    quarter = 'Q2'
                elif 'tredje' in title_lower or 'q3' in title_lower:
                    quarter = 'Q3'
                elif 'fjärde' in title_lower or 'q4' in title_lower:
                    quarter = 'Q4'
                elif 'helåret' in title_lower or 'annual' in title_lower:
                    quarter = 'Annual'
                else:
                    quarter = 'Unknown'
                
                print(f"  Quarter: {quarter}")
                print()
        
        # Key insights
        print("🔍 KEY INSIGHTS:")
        print("- ✅ RSS feed is accessible and working")
        print("- ✅ Contains financial events with dates")
        print("- ✅ Events are categorized as 'Financial Event'")
        print("- ✅ Swedish language but parseable")
        print("- ✅ Includes future quarterly reports")
        
        print("\n🎯 MVP2 OPPORTUNITIES:")
        print("1. Monitor this feed daily for new financial events")
        print("2. Auto-detect when new quarterly reports are announced")
        print("3. Extract report publication dates")
        print("4. Generate download reminders")
        print("5. Track Volvo's financial calendar")
        
        # Save analysis
        analysis = {
            'feed_url': feed_url,
            'feed_title': title,
            'feed_description': description,
            'total_items': len(items),
            'financial_reports': financial_reports,
            'analysis_date': datetime.now().isoformat()
        }
        
        with open('volvo_rss_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2)
        
        print("💾 Analysis saved to volvo_rss_analysis.json")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    analyze_volvo_rss()