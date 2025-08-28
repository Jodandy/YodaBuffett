#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup
import re

def debug_volvo_website():
    """Debug the Volvo website to understand its structure"""
    
    # Test different Volvo URLs
    urls_to_test = [
        "https://www.volvogroup.com/en/investors/reports-and-presentations.html",
        "https://www.volvogroup.com/investors/reports-and-presentations.html",
        "https://www.volvogroup.com/en/investors/reports-and-presentations/",
        "https://www.volvogroup.com/en/investors/"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for url in urls_to_test:
        print(f"\n=== Testing URL: {url} ===")
        
        try:
            response = requests.get(url, headers=headers)
            print(f"Status code: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for any links with 2024
                links_2024 = soup.find_all('a', href=True, string=re.compile(r'2024', re.IGNORECASE))
                print(f"Found {len(links_2024)} links with '2024' in text")
                
                # Look for any links to PDFs
                pdf_links = soup.find_all('a', href=re.compile(r'\.pdf', re.IGNORECASE))
                print(f"Found {len(pdf_links)} PDF links")
                
                # Look for text containing 2024
                text_2024 = soup.find_all(text=re.compile(r'2024'))
                print(f"Found {len(text_2024)} text nodes with '2024'")
                
                # Show first few PDF links if any
                if pdf_links:
                    print("Sample PDF links:")
                    for link in pdf_links[:3]:
                        print(f"  - {link.get('href')} : {link.text[:50]}")
                
                # Show title and some structure
                title = soup.find('title')
                if title:
                    print(f"Page title: {title.text}")
                    
                # Look for common investor relations patterns
                investor_sections = soup.find_all(['div', 'section'], class_=re.compile(r'report|investor|financial', re.IGNORECASE))
                print(f"Found {len(investor_sections)} sections with report/investor/financial classes")
                
            else:
                print(f"Failed to fetch: {response.status_code}")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    debug_volvo_website()