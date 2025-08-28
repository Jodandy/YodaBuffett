# ASPX Analysis Guide - Finansinspektionen

## Step 1: Manual Network Analysis

1. **Open Browser Developer Tools**
   - Chrome: F12 → Network tab
   - Firefox: F12 → Network tab

2. **Perform Manual Search**
   - Go to https://finanscentralen.fi.se/search/SearchByRegistrationDate.aspx
   - Set yesterday's date in both fields
   - Click "Search"

3. **Capture Network Requests**
   - Look for POST request to SearchByRegistrationDate.aspx
   - Right-click → Copy → Copy as cURL
   - Note all form parameters

## Step 2: Identify Required Parameters

Typical ASPX forms need:
```
__VIEWSTATE: [long encoded string]
__VIEWSTATEGENERATOR: [shorter string]  
__EVENTVALIDATION: [validation token]
ctl00$ContentPlaceHolder1$DateFrom: 2025-01-27
ctl00$ContentPlaceHolder1$DateTo: 2025-01-27
ctl00$ContentPlaceHolder1$SearchButton: Search
```

## Step 3: Test cURL Command

```bash
curl 'https://finanscentralen.fi.se/search/SearchByRegistrationDate.aspx' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d '__VIEWSTATE=[copied_value]&__VIEWSTATEGENERATOR=[copied_value]&...'
```

## Step 4: Python Implementation Strategy

### Option A: Requests + BeautifulSoup (Preferred)
```python
import requests
from bs4 import BeautifulSoup

# 1. GET the form page to extract viewstate
response = requests.get('https://finanscentralen.fi.se/search/SearchByRegistrationDate.aspx')
soup = BeautifulSoup(response.text, 'html.parser')

# 2. Extract required hidden fields
viewstate = soup.find('input', {'name': '__VIEWSTATE'})['value']
viewstate_gen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value']
event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})['value']

# 3. Submit search with proper parameters
search_data = {
    '__VIEWSTATE': viewstate,
    '__VIEWSTATEGENERATOR': viewstate_gen,
    '__EVENTVALIDATION': event_validation,
    'ctl00$ContentPlaceHolder1$DateFrom': '2025-01-27',
    'ctl00$ContentPlaceHolder1$DateTo': '2025-01-27',
    'ctl00$ContentPlaceHolder1$SearchButton': 'Search'
}

results = requests.post(url, data=search_data, cookies=response.cookies)
```

### Option B: Selenium (If requests fails)
```python
from selenium import webdriver
from selenium.webdriver.common.by import By

driver = webdriver.Chrome()
driver.get('https://finanscentralen.fi.se/search/SearchByRegistrationDate.aspx')

# Fill form fields
date_from = driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_DateFrom")
date_from.clear()
date_from.send_keys("2025-01-27")

# Submit and parse results
search_btn.click()
results_html = driver.page_source
```

### Option C: Playwright (Modern approach)
```python
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    
    page.goto('https://finanscentralen.fi.se/search/SearchByRegistrationDate.aspx')
    page.fill('#ctl00_ContentPlaceHolder1_DateFrom', '2025-01-27')
    page.fill('#ctl00_ContentPlaceHolder1_DateTo', '2025-01-27')
    page.click('input[value="Search"]')
    
    results = page.content()
    browser.close()
```

## Step 5: Result Parsing

Once you get the results HTML:
```python
soup = BeautifulSoup(results_html, 'html.parser')

# Look for result table/grid
results_table = soup.find('table', class_='search-results')  # Adjust selector
rows = results_table.find_all('tr')[1:]  # Skip header

for row in rows:
    cells = row.find_all('td')
    company_name = cells[0].text.strip()
    document_type = cells[1].text.strip() 
    filing_date = cells[2].text.strip()
    pdf_link = cells[3].find('a')['href'] if cells[3].find('a') else None
```

## Legal Compliance Notes

✅ **This is 100% legal because:**
- Government website (Finansinspektionen)
- Public information (Offentlighetsprincipen)
- Intended for public access
- No robots.txt restrictions
- Academic/research purpose

✅ **Best practices:**
- 1 search per day maximum
- Clear User-Agent identification
- Respect any rate limits
- Log all activities for transparency