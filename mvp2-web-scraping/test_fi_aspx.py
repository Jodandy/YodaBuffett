#!/usr/bin/env python3

"""
Test ASPX parsing for Finansinspektionen search
"""

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta


def test_aspx_form_analysis():
    """Test ASPX form structure analysis"""
    
    url = "https://finanscentralen.fi.se/search/SearchByRegistrationDate.aspx"
    
    print("🔍 ASPX FORM ANALYSIS")
    print("="*40)
    print(f"URL: {url}")
    
    try:
        # Get the initial form page
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        
        response = session.get(url)
        print(f"Status: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ Failed to load form page")
            return False
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find ASPX-specific hidden fields
        viewstate = soup.find('input', {'name': '__VIEWSTATE'})
        viewstate_gen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'}) 
        event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})
        
        print(f"\n📋 ASPX Hidden Fields:")
        print(f"__VIEWSTATE: {'✅ Found' if viewstate else '❌ Missing'}")
        print(f"__VIEWSTATEGENERATOR: {'✅ Found' if viewstate_gen else '❌ Missing'}")
        print(f"__EVENTVALIDATION: {'✅ Found' if event_validation else '❌ Missing'}")
        
        # Find form input fields
        date_inputs = soup.find_all('input', {'type': 'text'})
        buttons = soup.find_all('input', {'type': 'submit'}) + soup.find_all('button')
        
        print(f"\n🔍 Form Elements:")
        print(f"Date inputs found: {len(date_inputs)}")
        print(f"Buttons found: {len(buttons)}")
        
        for i, input_elem in enumerate(date_inputs):
            name = input_elem.get('name', 'Unknown')
            id_attr = input_elem.get('id', 'Unknown')
            print(f"  Input {i+1}: name='{name}', id='{id_attr}'")
            
        for i, btn in enumerate(buttons):
            name = btn.get('name', 'Unknown')
            value = btn.get('value', btn.text if btn.text else 'Unknown')
            print(f"  Button {i+1}: name='{name}', value='{value}'")
        
        # Check if we can build a working form submission
        if viewstate and viewstate_gen:
            print(f"\n✅ Form appears parseable with requests + BeautifulSoup")
            return True
        else:
            print(f"\n⚠️  Complex ASPX form - may need browser automation")
            return False
            
    except Exception as e:
        print(f"❌ Error analyzing form: {e}")
        return False


def simulate_search_request():
    """Simulate what a search request would look like"""
    
    print(f"\n🎯 SEARCH REQUEST SIMULATION")
    print("="*40)
    
    # Yesterday's date for testing
    yesterday = datetime.now() - timedelta(days=1)
    search_date = yesterday.strftime('%Y-%m-%d')
    
    print(f"Search date: {search_date}")
    
    # Common ASPX form patterns
    potential_form_data = {
        '__VIEWSTATE': '[extracted_from_form]',
        '__VIEWSTATEGENERATOR': '[extracted_from_form]',
        '__EVENTVALIDATION': '[extracted_from_form]',
        
        # Potential field names for date inputs
        'ctl00$ContentPlaceHolder1$DateFrom': search_date,
        'ctl00$ContentPlaceHolder1$DateTo': search_date,
        'ctl00$ContentPlaceHolder1$SearchButton': 'Search',
        
        # Alternative field naming patterns
        'DateFrom': search_date,
        'DateTo': search_date,
        'SearchButton': 'Search'
    }
    
    print(f"\n📝 Probable form parameters:")
    for key, value in potential_form_data.items():
        if not key.startswith('__'):
            print(f"  {key}: {value}")
    
    return potential_form_data


def recommend_implementation_approach():
    """Recommend best approach based on form complexity"""
    
    approaches = {
        "Approach 1: Requests + BeautifulSoup": {
            "complexity": "Low",
            "maintenance": "Easy", 
            "success_rate": "75%",
            "description": "Parse form, extract viewstate, submit POST request",
            "pros": ["Lightweight", "Fast", "Easy to debug"],
            "cons": ["May fail on complex JS validation"]
        },
        
        "Approach 2: Playwright": {
            "complexity": "Medium",
            "maintenance": "Moderate",
            "success_rate": "95%", 
            "description": "Full browser automation with form interaction",
            "pros": ["Handles all JS", "Very reliable", "Easy to understand"],
            "cons": ["Heavier", "Slower", "More dependencies"]
        },
        
        "Approach 3: Hybrid": {
            "complexity": "Medium",
            "maintenance": "Moderate",
            "success_rate": "99%",
            "description": "Try requests first, fallback to Playwright",
            "pros": ["Best of both worlds", "Resilient"],
            "cons": ["More code complexity"]
        }
    }
    
    print(f"\n🎯 IMPLEMENTATION APPROACHES")
    print("="*50)
    
    for approach, details in approaches.items():
        print(f"\n{approach}")
        print(f"  Complexity: {details['complexity']}")
        print(f"  Success Rate: {details['success_rate']}")
        print(f"  Description: {details['description']}")
        print(f"  Pros: {', '.join(details['pros'])}")
        print(f"  Cons: {', '.join(details['cons'])}")
    
    print(f"\n🎯 RECOMMENDATION:")
    print(f"Start with Approach 1 (Requests + BeautifulSoup)")
    print(f"If that fails → Approach 2 (Playwright)")
    print(f"For production → Approach 3 (Hybrid)")
    
    return approaches


def main():
    """Main test function"""
    
    print("🏛️ FINANSINSPEKTIONEN ASPX ANALYSIS")
    print("="*50)
    
    # Test form analysis
    form_parseable = test_aspx_form_analysis()
    
    # Show what search would look like
    simulate_search_request()
    
    # Recommend approach
    recommend_implementation_approach()
    
    print(f"\n💡 NEXT STEPS:")
    print(f"1. Run this script to analyze the actual form")
    print(f"2. Try manual browser network capture") 
    print(f"3. Build requests-based prototype")
    print(f"4. Add Playwright fallback if needed")
    
    if form_parseable:
        print(f"\n✅ High confidence this will work with simple HTTP requests!")
    else:
        print(f"\n⚠️  May need browser automation - but still totally doable!")


if __name__ == "__main__":
    main()