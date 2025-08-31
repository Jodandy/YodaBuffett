#!/usr/bin/env python3
"""
Save MFN.se HTML page to file for manual inspection
"""
import asyncio
import aiohttp
from datetime import datetime

async def save_mfn_html():
    """Save the Hexagon MFN page HTML to file"""
    
    url = "https://mfn.se/all/a/hexagon?limit=240"
    headers = {
        'User-Agent': 'YodaBuffett-Research/1.0 (Financial Research; +https://yodabuffett.com)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    
    output_file = "hexagon_mfn_page.html"
    
    print(f"🔍 Fetching: {url}")
    print(f"💾 Saving to: {output_file}")
    
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as response:
                print(f"📡 HTTP Status: {response.status}")
                print(f"📄 Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
                
                if response.status != 200:
                    print(f"❌ HTTP {response.status} - {response.reason}")
                    return
                    
                html = await response.text()
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
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')
                
                # Count key elements
                all_links = soup.find_all('a', href=True)
                table_rows = soup.find_all('tr')
                
                print(f"\n📊 Quick Analysis:")
                print(f"   🔗 Total links: {len(all_links)}")
                print(f"   📋 Table rows: {len(table_rows)}")
                
                # Count document-like links
                pdf_links = sum(1 for link in all_links if '.pdf' in link.get('href', ''))
                cision_links = sum(1 for link in all_links if 'cision.com' in link.get('href', ''))
                
                print(f"   📄 PDF links: {pdf_links}")
                print(f"   📄 Cision links: {cision_links}")
                
                # Show a few sample links
                print(f"\n🔍 Sample links:")
                for i, link in enumerate(all_links[:10]):
                    href = link.get('href', '')[:80]
                    text = link.get_text(strip=True)[:40]
                    print(f"   {i+1}. {href}{'...' if len(link.get('href', '')) > 80 else ''}")
                    if text:
                        print(f"      Text: {text}{'...' if len(link.get_text(strip=True)) > 40 else ''}")
                
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        import aiohttp
        asyncio.run(save_mfn_html())
    except ImportError:
        print("❌ aiohttp not available")
        print("If you want to run this, you can install with:")
        print("python3 -m venv venv && source venv/bin/activate && pip install aiohttp beautifulsoup4")