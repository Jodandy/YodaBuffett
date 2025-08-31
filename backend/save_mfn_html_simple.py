#!/usr/bin/env python3
"""
Save MFN.se HTML page using basic Python libraries
"""
import urllib.request
import urllib.error
from datetime import datetime

def save_mfn_html_simple():
    """Save the Hexagon MFN page HTML using urllib"""
    
    url = "https://mfn.se/all/a/hexagon?limit=240"
    output_file = "hexagon_mfn_page.html"
    
    print(f"🔍 Fetching: {url}")
    print(f"💾 Saving to: {output_file}")
    
    try:
        # Create request with headers
        headers = {
            'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req) as response:
            print(f"📡 HTTP Status: {response.status}")
            print(f"📄 Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
            
            if response.status != 200:
                print(f"❌ HTTP {response.status}")
                return
            
            html = response.read().decode('utf-8')
            print(f"📊 HTML length: {len(html):,} characters")
            
            # Save HTML to file with metadata
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"<!-- Saved from {url} at {datetime.now()} -->\n")
                f.write(f"<!-- HTTP Status: {response.status} -->\n") 
                f.write(f"<!-- Content-Length: {len(html):,} chars -->\n")
                f.write("<!-- ======================================== -->\n\n")
                f.write(html)
            
            print(f"✅ HTML saved to {output_file}")
            
            # Quick analysis
            link_count = html.count('<a href=')
            tr_count = html.count('<tr')
            pdf_count = html.count('.pdf')
            cision_count = html.count('cision.com')
            
            print(f"\n📊 Quick Analysis:")
            print(f"   🔗 '<a href=' tags: {link_count}")
            print(f"   📋 '<tr' tags: {tr_count}")
            print(f"   📄 '.pdf' mentions: {pdf_count}")
            print(f"   📄 'cision.com' mentions: {cision_count}")
            
            # Check for signs of pagination or dynamic content
            if 'load more' in html.lower() or 'show more' in html.lower():
                print("   ⚠️  Possible 'load more' functionality detected")
            if 'javascript' in html.lower() and ('fetch' in html.lower() or 'ajax' in html.lower()):
                print("   ⚠️  JavaScript dynamic loading detected")
            
    except urllib.error.URLError as e:
        print(f"❌ URL Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    save_mfn_html_simple()